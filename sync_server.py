"""
Synchronizes files from an S3 bucket (MinIO) to a Qdrant collection.

For every new or changed file the service extracts text (file_to_text_converter_server),
chunks and vectorizes it (chunk_n_vec_server) and stores the chunks in Qdrant.
Files removed from S3 are removed from Qdrant.
"""
import asyncio
import hashlib
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from minio import Minio
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, MINIO_SECURE,
    QDRANT_HOST, QDRANT_PORT, QDRANT_API_KEY, QDRANT_COLLECTION_NAME, QDRANT_VECTOR_SIZE,
    QDRANT_USE_HTTPS, QDRANT_UPSERT_BATCH_SIZE, TEXT_CONVERTER_URL, CHUNK_N_VEC_URL,
    SERVICE_PORT, SYNC_INTERVAL_SECONDS, MAX_CONCURRENT_FILES, CHUNK_MAX_SIZE, CHUNK_OVERLAP,
    REQUEST_TIMEOUT, MAX_RETRIES, HEALTH_CHECK_INTERVAL, LOG_LEVEL, LOG_FORMAT,
)

logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

VERSION = "1.1.0"

service_status = {
    "running": True,
    "last_sync_time": None,
    "current_sync_in_progress": False,
    "files_processed": 0,
    "errors_count": 0,
}

health_cache: Dict[str, Dict] = {
    name: {"status": "unknown", "last_check": None}
    for name in ("minio", "qdrant", "text_converter", "chunk_n_vec")
}
last_health_check_time: Optional[datetime] = None

minio_client: Optional[Minio] = None
qdrant_client: Optional[QdrantClient] = None

processing_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FILES)
background_tasks = set()  # strong references to fire-and-forget tasks

# Retry only on transient network errors
network_retry = retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError)),
    reraise=True,
)


def init_clients():
    """Initialize MinIO and Qdrant clients."""
    global minio_client, qdrant_client

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    qdrant_client = QdrantClient(
        host=QDRANT_HOST,
        port=QDRANT_PORT,
        api_key=QDRANT_API_KEY or None,
        https=QDRANT_USE_HTTPS,
        timeout=30,
    )


def ensure_bucket_exists():
    """Create the S3 bucket if it does not exist."""
    if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
        minio_client.make_bucket(MINIO_BUCKET_NAME)
        logger.info(f"Created bucket: {MINIO_BUCKET_NAME}")
    else:
        logger.info(f"Bucket {MINIO_BUCKET_NAME} already exists")


def ensure_collection_exists():
    """Create the Qdrant collection if it does not exist."""
    names = [c.name for c in qdrant_client.get_collections().collections]
    if QDRANT_COLLECTION_NAME not in names:
        qdrant_client.create_collection(
            collection_name=QDRANT_COLLECTION_NAME,
            vectors_config=qdrant_models.VectorParams(
                size=QDRANT_VECTOR_SIZE,
                distance=qdrant_models.Distance.COSINE,
            ),
        )
        logger.info(f"Created collection: {QDRANT_COLLECTION_NAME}")
    else:
        logger.info(f"Collection {QDRANT_COLLECTION_NAME} already exists")


def get_s3_files_info() -> Dict[str, Dict]:
    """Return {filename: {"hash": etag, "modified": datetime, "tags": dict}} for all objects in the bucket."""
    files_info = {}
    for obj in minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True):
        try:
            tags = dict(minio_client.get_object_tags(MINIO_BUCKET_NAME, obj.object_name) or {})
        except Exception as e:  # objects without tags may raise NoSuchTagSet
            logger.debug(f"No tags for '{obj.object_name}': {e}")
            tags = {}

        files_info[obj.object_name] = {
            "hash": (obj.etag or "").strip('"'),
            "modified": obj.last_modified,
            "tags": tags,
        }

    logger.info(f"Retrieved {len(files_info)} files from S3 bucket {MINIO_BUCKET_NAME}")
    return files_info


def get_qdrant_files_info() -> Dict[str, Dict]:
    """Return {filename: {"hash": str, "modified": str, "tags": dict}} for all files stored in Qdrant."""
    files_info = {}
    offset = None

    while True:
        points, offset = qdrant_client.scroll(
            collection_name=QDRANT_COLLECTION_NAME,
            limit=100,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        for point in points:
            metadata = (point.payload or {}).get("metadata") or {}
            if "file_name" in metadata:
                files_info[metadata["file_name"]] = {
                    "hash": metadata.get("file_hash", ""),
                    "modified": metadata.get("file_modified", ""),
                    "tags": metadata.get("file_tags", {}),
                }
        if offset is None:
            break

    logger.info(f"Retrieved {len(files_info)} files from Qdrant collection {QDRANT_COLLECTION_NAME}")
    return files_info


def compare_files(s3_files: Dict, qdrant_files: Dict) -> Tuple[List[str], List[str], List[str]]:
    """Return (to_upload, to_delete, to_update)."""
    s3_names = set(s3_files)
    qdrant_names = set(qdrant_files)

    to_upload = sorted(s3_names - qdrant_names)
    to_delete = sorted(qdrant_names - s3_names)
    to_update = sorted(name for name in s3_names & qdrant_names
                       if s3_files[name]["hash"] != qdrant_files[name]["hash"])

    logger.info(f"Comparison results: Upload={len(to_upload)}, Delete={len(to_delete)}, Update={len(to_update)}")
    return to_upload, to_delete, to_update


def delete_file_from_qdrant(filename: str):
    """Delete all points of the given file from Qdrant."""
    qdrant_client.delete(
        collection_name=QDRANT_COLLECTION_NAME,
        points_selector=qdrant_models.Filter(
            must=[qdrant_models.FieldCondition(
                key="metadata.file_name",
                match=qdrant_models.MatchValue(value=filename),
            )]
        ),
    )
    logger.info(f"Deleted file '{filename}' from Qdrant")


def read_s3_object(filename: str) -> bytes:
    response = minio_client.get_object(MINIO_BUCKET_NAME, filename)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


@network_retry
async def extract_text_from_file(file_content: bytes, filename: str) -> Optional[str]:
    """Extract text from a file using the text converter service."""
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.post(
            f"{TEXT_CONVERTER_URL}/convert",
            files={"file": (filename, file_content, "application/octet-stream")},
        )

    if response.status_code != 200:
        logger.error(f"Text converter returned error {response.status_code} for {filename}: {response.text}")
        return None

    result = response.json()
    if "file_text" not in result:
        logger.error(f"Unexpected response format from text converter for {filename}")
        return None

    logger.info(f"Extracted text from {filename}, worktime: {result.get('worktime', 'unknown')}")
    return result["file_text"]


@network_retry
async def chunk_and_vectorize(text: str, filename: str) -> Optional[List[Dict]]:
    """Send text to the chunking and vectorization service."""
    payload = {"text": text, "max_chunk_size": CHUNK_MAX_SIZE, "overlap": CHUNK_OVERLAP}
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.post(f"{CHUNK_N_VEC_URL}/process", json=payload)

    if response.status_code != 200:
        logger.error(f"Chunking service returned error {response.status_code} for {filename}: {response.text}")
        return None

    result = response.json()
    if "chunks" not in result:
        logger.error(f"Unexpected response format from chunking service for {filename}")
        return None

    logger.info(
        f"Chunked and vectorized {filename}, total_chunks: {result.get('total_chunks', 0)}, "
        f"processing_time: {result.get('processing_time', 0):.3f}s"
    )
    return result["chunks"]


def upload_chunks_to_qdrant(filename: str, chunks: List[Dict], file_hash: str,
                            file_modified: datetime, file_tags: Dict):
    """Store chunks in Qdrant as points with nested metadata."""
    file_id_base = hashlib.md5(f"{filename}_{file_hash}".encode()).hexdigest()[:16]
    total_chunks = len(chunks)
    file_modified_str = file_modified.isoformat() if isinstance(file_modified, datetime) else str(file_modified)

    points = [
        qdrant_models.PointStruct(
            id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{file_id_base}_{i}_{file_hash[:8]}")),
            vector=chunk["embedding"],
            payload={
                "content": chunk["chunk_text"],
                "metadata": {
                    "file_name": filename,
                    "file_hash": file_hash,
                    "file_modified": file_modified_str,
                    "file_tags": file_tags,
                    "chunk_position": i + 1,
                    "chunk_index": i,
                    "total_chunks": total_chunks,
                },
            },
        )
        for i, chunk in enumerate(chunks)
    ]

    for i in range(0, len(points), QDRANT_UPSERT_BATCH_SIZE):
        qdrant_client.upsert(collection_name=QDRANT_COLLECTION_NAME,
                             points=points[i:i + QDRANT_UPSERT_BATCH_SIZE])

    logger.info(f"Uploaded {len(points)} chunks for file '{filename}' to Qdrant")


async def process_single_file(filename: str, operation: str, s3_files_info: Dict) -> bool:
    """Extract text, chunk, vectorize and upload one file. Returns True on success."""
    async with processing_semaphore:
        try:
            logger.info(f"Starting {operation} of file: {filename}")

            file_content = await asyncio.to_thread(read_s3_object, filename)
            file_info = s3_files_info[filename]

            text = await extract_text_from_file(file_content, filename)
            if text is None:
                logger.error(f"Text extraction failed for {filename}, skipping")
                return False
            if not text.strip():
                logger.warning(f"No text extracted from {filename}, skipping")
                return False

            chunks = await chunk_and_vectorize(text, filename)
            if not chunks:
                logger.error(f"Chunking/vectorization failed for {filename}, skipping")
                return False

            await asyncio.to_thread(
                upload_chunks_to_qdrant, filename, chunks,
                file_info["hash"], file_info["modified"], file_info["tags"],
            )

            logger.info(f"Completed {operation} of file: {filename}")
            service_status["files_processed"] += 1
            return True

        except Exception as e:
            logger.error(f"Failed to process file '{filename}' for {operation}: {e!r}")
            service_status["errors_count"] += 1
            return False


async def sync_files():
    """One synchronization cycle."""
    if service_status["current_sync_in_progress"]:
        logger.warning("Sync already in progress, skipping this cycle")
        return

    service_status["current_sync_in_progress"] = True
    logger.info("Starting synchronization cycle")

    try:
        s3_files = await asyncio.to_thread(get_s3_files_info)
        qdrant_files = await asyncio.to_thread(get_qdrant_files_info)
        to_upload, to_delete, to_update = compare_files(s3_files, qdrant_files)

        # Remove files deleted from S3 and the outdated versions of changed files
        for filename in to_delete + to_update:
            try:
                await asyncio.to_thread(delete_file_from_qdrant, filename)
            except Exception as e:
                logger.error(f"Failed to delete {filename} from Qdrant: {e!r}")
                service_status["errors_count"] += 1

        # Re-add changed files and add new ones
        tasks = [process_single_file(name, "update", s3_files) for name in to_update]
        tasks += [process_single_file(name, "upload", s3_files) for name in to_upload]
        if tasks:
            await asyncio.gather(*tasks)

        service_status["last_sync_time"] = datetime.now().isoformat()
        logger.info(f"Synchronization completed: {len(to_upload)} uploads, "
                    f"{len(to_delete)} deletions, {len(to_update)} updates")

    except Exception as e:
        logger.error(f"Sync failed: {e!r}")
        service_status["errors_count"] += 1
    finally:
        service_status["current_sync_in_progress"] = False


async def periodic_sync():
    """Run synchronization periodically."""
    while service_status["running"]:
        await sync_files()
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)


async def check_http_service(name: str, url: str, now: str):
    """Check a neighbour service's /health endpoint and store the result in the health cache."""
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{url}/health")
        if response.status_code == 200:
            health_cache[name] = {"status": "healthy", "last_check": now}
        else:
            health_cache[name] = {"status": "unhealthy", "last_check": now, "error": f"HTTP {response.status_code}"}
    except Exception as e:
        health_cache[name] = {"status": "unhealthy", "last_check": now, "error": str(e)}
        logger.error(f"{name} health check failed: {e}")


async def check_service_health() -> Dict:
    """Check all dependencies; results are cached for HEALTH_CHECK_INTERVAL seconds."""
    global last_health_check_time

    current_time = datetime.now()
    if (last_health_check_time is not None
            and (current_time - last_health_check_time).total_seconds() < HEALTH_CHECK_INTERVAL):
        return health_cache
    last_health_check_time = current_time
    now = current_time.isoformat()
    logger.info("Performing health check of dependent services")

    for name, check in (("minio", minio_client.list_buckets), ("qdrant", qdrant_client.get_collections)):
        try:
            await asyncio.to_thread(check)
            health_cache[name] = {"status": "healthy", "last_check": now}
        except Exception as e:
            health_cache[name] = {"status": "unhealthy", "last_check": now, "error": str(e)}
            logger.error(f"{name} health check failed: {e}")

    await asyncio.gather(
        check_http_service("text_converter", TEXT_CONVERTER_URL, now),
        check_http_service("chunk_n_vec", CHUNK_N_VEC_URL, now),
    )
    return health_cache


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting Sync Service...")
    init_clients()
    await asyncio.to_thread(ensure_bucket_exists)
    await asyncio.to_thread(ensure_collection_exists)

    sync_task = asyncio.create_task(periodic_sync())
    yield

    logger.info("Shutting down Sync Service...")
    service_status["running"] = False
    sync_task.cancel()
    try:
        await sync_task
    except asyncio.CancelledError:
        pass


app = FastAPI(
    title="S3 to Qdrant Sync Service",
    description="Synchronizes files from S3 (MinIO) to Qdrant vector database",
    version=VERSION,
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {
        "service": "S3 to Qdrant Sync Service",
        "version": VERSION,
        "description": "Synchronizes files from S3 bucket to Qdrant collection",
        "endpoints": {
            "/": "Service information",
            "/live": "Liveness probe (does not check dependencies)",
            "/health": "Health status of service and dependencies",
            "/status": "Service operational status",
            "/sync": "POST - trigger synchronization manually",
        },
        "configuration": {
            "s3_bucket": MINIO_BUCKET_NAME,
            "qdrant_collection": QDRANT_COLLECTION_NAME,
            "sync_interval_minutes": SYNC_INTERVAL_SECONDS // 60,
        },
    }


@app.get("/live")
async def liveness():
    return {"status": "alive"}


@app.get("/health")
async def health():
    health_status = await check_service_health()
    all_healthy = all(service["status"] == "healthy" for service in health_status.values())

    return JSONResponse(
        status_code=200 if all_healthy else 503,
        content={
            "status": "healthy" if all_healthy else "degraded",
            "services": health_status,
            "sync_status": {
                "last_sync": service_status["last_sync_time"],
                "sync_in_progress": service_status["current_sync_in_progress"],
                "files_processed_total": service_status["files_processed"],
                "errors_total": service_status["errors_count"],
            },
        },
    )


@app.get("/status")
async def status():
    return {
        "running": service_status["running"],
        "last_sync_time": service_status["last_sync_time"],
        "current_sync_in_progress": service_status["current_sync_in_progress"],
        "files_processed_since_startup": service_status["files_processed"],
        "errors_since_startup": service_status["errors_count"],
    }


@app.post("/sync")
async def trigger_sync():
    """Manually trigger synchronization."""
    if service_status["current_sync_in_progress"]:
        raise HTTPException(status_code=409, detail="Sync already in progress")

    task = asyncio.create_task(sync_files())
    background_tasks.add(task)
    task.add_done_callback(background_tasks.discard)
    return {"message": "Sync triggered successfully"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT, log_level=LOG_LEVEL.lower())
