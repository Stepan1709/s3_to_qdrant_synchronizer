import asyncio
import hashlib
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from minio import Minio
from minio.error import S3Error
from qdrant_client import QdrantClient
from qdrant_client.http import models as qdrant_models
from qdrant_client.http.exceptions import UnexpectedResponse
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET_NAME, MINIO_SECURE,
    QDRANT_HOST, QDRANT_PORT, QDRANT_API_KEY, QDRANT_COLLECTION_NAME, QDRANT_VECTOR_SIZE,
    QDRANT_USE_HTTPS, TEXT_CONVERTER_URL, CHUNK_N_VEC_URL, SERVICE_PORT, SYNC_INTERVAL_SECONDS,
    REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY_SECONDS, LOG_LEVEL, LOG_FORMAT
)

# Configure logging
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Global variables for service status
service_status = {
    "running": True,
    "last_sync_time": None,
    "current_sync_in_progress": False,
    "files_processed": 0,
    "errors_count": 0
}

# Health status cache
health_cache = {
    "minio": {"status": "unknown", "last_check": None},
    "qdrant": {"status": "unknown", "last_check": None},
    "text_converter": {"status": "unknown", "last_check": None},
    "chunk_n_vec": {"status": "unknown", "last_check": None}
}

# Initialize clients
minio_client = None
qdrant_client = None

# Semaphore for controlling concurrent file processing
processing_semaphore = asyncio.Semaphore(5)  # Process up to 5 files concurrently


def init_clients():
    """Initialize MinIO and Qdrant clients"""
    global minio_client, qdrant_client

    # Initialize MinIO client
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE
    )

    # Initialize Qdrant client
    qdrant_client = QdrantClient(
        host=QDRANT_HOST,
        port=QDRANT_PORT,
        api_key=QDRANT_API_KEY if QDRANT_API_KEY else None,
        https=QDRANT_USE_HTTPS,
        timeout=30
    )


async def ensure_bucket_exists():
    """Ensure the S3 bucket exists, create if not"""
    try:
        if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
            minio_client.make_bucket(MINIO_BUCKET_NAME)
            logger.info(f"Created bucket: {MINIO_BUCKET_NAME}")
        else:
            logger.info(f"Bucket {MINIO_BUCKET_NAME} already exists")
    except S3Error as e:
        logger.error(f"Failed to ensure bucket exists: {e}")
        raise


async def ensure_collection_exists():
    """Ensure the Qdrant collection exists, create if not"""
    try:
        collections = qdrant_client.get_collections()
        collection_names = [c.name for c in collections.collections]

        if QDRANT_COLLECTION_NAME not in collection_names:
            qdrant_client.create_collection(
                collection_name=QDRANT_COLLECTION_NAME,
                vectors_config=qdrant_models.VectorParams(
                    size=QDRANT_VECTOR_SIZE,
                    distance=qdrant_models.Distance.COSINE
                )
            )
            logger.info(f"Created collection: {QDRANT_COLLECTION_NAME}")
        else:
            logger.info(f"Collection {QDRANT_COLLECTION_NAME} already exists")
    except Exception as e:
        logger.error(f"Failed to ensure collection exists: {e}")
        raise


def get_file_hash(content: bytes) -> str:
    """Calculate MD5 hash of file content"""
    return hashlib.md5(content).hexdigest()


async def get_s3_files_info() -> Dict[str, Dict]:
    """
    Get all files and their metadata from S3 bucket
    Returns dict: {filename: {"hash": str, "modified": datetime, "tags": dict}}
    """
    files_info = {}

    try:
        objects = minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True)

        for obj in objects:
            # Get object metadata
            obj_stat = minio_client.stat_object(MINIO_BUCKET_NAME, obj.object_name)

            # Get object tags (if any)
            tags = {}
            try:
                tags_result = minio_client.get_object_tags(MINIO_BUCKET_NAME, obj.object_name)
                if tags_result:
                    tags = tags_result
            except:
                pass

            files_info[obj.object_name] = {
                "hash": obj_stat.etag.strip('"'),  # Remove quotes from ETag
                "modified": obj_stat.last_modified,
                "tags": tags
            }

        logger.info(f"Retrieved {len(files_info)} files from S3 bucket {MINIO_BUCKET_NAME}")

    except Exception as e:
        logger.error(f"Failed to get S3 files info: {e}")
        raise

    return files_info


async def get_qdrant_files_info() -> Dict[str, Dict]:
    """
    Get all files and their metadata from Qdrant collection
    Returns dict: {filename: {"hash": str, "modified": str, "tags": dict}}
    """
    files_info = {}
    offset = None
    limit = 100

    try:
        while True:
            # Scroll through all points in the collection
            points, next_offset = qdrant_client.scroll(
                collection_name=QDRANT_COLLECTION_NAME,
                limit=limit,
                offset=offset,
                with_payload=True,
                with_vectors=False
            )

            for point in points:
                payload = point.payload
                if payload and "file_name" in payload:
                    filename = payload["file_name"]
                    files_info[filename] = {
                        "hash": payload.get("file_hash", ""),
                        "modified": payload.get("file_modified", ""),
                        "tags": payload.get("file_tags", {})
                    }

            if next_offset is None:
                break
            offset = next_offset

        logger.info(f"Retrieved {len(files_info)} files from Qdrant collection {QDRANT_COLLECTION_NAME}")

    except Exception as e:
        logger.error(f"Failed to get Qdrant files info: {e}")
        raise

    return files_info


def compare_files(s3_files: Dict, qdrant_files: Dict) -> Tuple[List[str], List[str], List[str]]:
    """
    Compare S3 and Qdrant file lists
    Returns: (to_upload, to_delete, to_update)
    """
    s3_filenames = set(s3_files.keys())
    qdrant_filenames = set(qdrant_files.keys())

    # New files: in S3 but not in Qdrant
    to_upload = list(s3_filenames - qdrant_filenames)

    # Files to delete: in Qdrant but not in S3
    to_delete = list(qdrant_filenames - s3_filenames)

    # Files to update: in both but hash differs
    to_update = []
    for filename in s3_filenames & qdrant_filenames:
        if s3_files[filename]["hash"] != qdrant_files[filename]["hash"]:
            to_update.append(filename)

    logger.info(f"Comparison results: Upload={len(to_upload)}, Delete={len(to_delete)}, Update={len(to_update)}")

    return to_upload, to_delete, to_update


async def delete_file_from_qdrant(filename: str):
    """Delete all points with given filename from Qdrant"""
    try:
        qdrant_client.delete(
            collection_name=QDRANT_COLLECTION_NAME,
            points_selector=qdrant_models.Filter(
                must=[
                    qdrant_models.FieldCondition(
                        key="file_name",
                        match=qdrant_models.MatchValue(value=filename)
                    )
                ]
            )
        )
        logger.info(f"Deleted file '{filename}' from Qdrant")
    except Exception as e:
        logger.error(f"Failed to delete file '{filename}' from Qdrant: {e}")
        raise


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError))
)
async def extract_text_from_file(file_content: bytes, filename: str) -> Optional[str]:
    """Extract text from file using text converter service"""
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            files = {"file": (filename, file_content, "application/octet-stream")}
            response = await client.post(TEXT_CONVERTER_URL, files=files)

            if response.status_code != 200:
                logger.error(f"Text converter returned error {response.status_code} for {filename}: {response.text}")
                return None

            result = response.json()
            if "file_text" not in result:
                logger.error(f"Unexpected response format from text converter for {filename}")
                return None

            logger.info(f"Successfully extracted text from {filename}, worktime: {result.get('worktime', 'unknown')}")
            return result["file_text"]

    except Exception as e:
        logger.error(f"Failed to extract text from {filename}: {e}")
        raise


@retry(
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    retry=retry_if_exception_type((httpx.TimeoutException, httpx.ConnectError))
)
async def chunk_and_vectorize(text: str, filename: str) -> Optional[List[Dict]]:
    """Send text to chunking and vectorization service"""
    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            payload = {
                "text": text,
                "text_format_md": True,
                "max_chunk_size": 4000,
                "chunking_strategy": "recursive"
            }
            response = await client.post(CHUNK_N_VEC_URL, json=payload)

            if response.status_code != 200:
                logger.error(f"Chunking service returned error {response.status_code} for {filename}: {response.text}")
                return None

            result = response.json()
            if "chunks" not in result:
                logger.error(f"Unexpected response format from chunking service for {filename}")
                return None

            logger.info(
                f"Successfully chunked and vectorized {filename}, total_chunks: {result.get('total_chunks', 0)}")
            return result["chunks"]

    except Exception as e:
        logger.error(f"Failed to chunk and vectorize {filename}: {e}")
        raise


async def upload_chunks_to_qdrant(
        filename: str,
        chunks: List[Dict],
        file_hash: str,
        file_modified: datetime,
        file_tags: Dict
):
    """Upload chunks to Qdrant as points"""
    try:
        points = []
        file_id_base = hashlib.md5(f"{filename}_{file_hash}".encode()).hexdigest()[:16]
        for i, chunk in enumerate(chunks):
            point_id = uuid.uuid5(
                uuid.NAMESPACE_DNS,
                f"{file_id_base}_{i}_{file_hash[:8]}"
            )
            points.append(
                qdrant_models.PointStruct(
                    id=str(point_id),
                    vector=chunk["embedding"],
                    payload={
                        "file_name": filename,
                        "file_hash": file_hash,
                        "file_modified": file_modified.isoformat() if isinstance(file_modified, datetime) else str(
                            file_modified),
                        "file_tags": file_tags,
                        "chunk_index": i,
                        "chunk_text": chunk["chunk_text"],
                        "total_chunks": len(chunks)
                    }
                )
            )

        # Upload in batches of 100
        batch_size = 100
        for i in range(0, len(points), batch_size):
            batch = points[i:i + batch_size]
            qdrant_client.upsert(
                collection_name=QDRANT_COLLECTION_NAME,
                points=batch
            )

        logger.info(f"Uploaded {len(points)} chunks for file '{filename}' to Qdrant")

    except Exception as e:
        logger.error(f"Failed to upload chunks for '{filename}': {e}")
        raise


async def process_single_file(
        filename: str,
        operation: str,  # 'upload' or 'update'
        s3_files_info: Dict
):
    """Process a single file: extract text, chunk, vectorize, and upload to Qdrant"""
    async with processing_semaphore:
        try:
            logger.info(f"Starting {operation} of file: {filename}")

            # Get file from S3
            try:
                response = minio_client.get_object(MINIO_BUCKET_NAME, filename)
                file_content = response.read()
                response.close()
                response.release_conn()
            except Exception as e:
                logger.error(f"Failed to read file '{filename}' from S3: {e}")
                return False

            # Get file metadata
            file_info = s3_files_info[filename]
            file_hash = file_info["hash"]
            file_modified = file_info["modified"]
            file_tags = file_info["tags"]

            # Extract text from file
            extracted_text = await extract_text_from_file(file_content, filename)
            if extracted_text is None:
                logger.error(f"Text extraction failed for {filename}, skipping")
                return False

            # Chunk and vectorize
            chunks = await chunk_and_vectorize(extracted_text, filename)
            if chunks is None:
                logger.error(f"Chunking/vectorization failed for {filename}, skipping")
                return False

            # Upload to Qdrant
            await upload_chunks_to_qdrant(filename, chunks, file_hash, file_modified, file_tags)

            logger.info(f"Successfully completed {operation} of file: {filename}")
            service_status["files_processed"] += 1
            return True

        except Exception as e:
            logger.error(f"Failed to process file '{filename}' for {operation}: {e}")
            service_status["errors_count"] += 1
            return False


async def sync_files():
    """Main synchronization logic"""
    if service_status["current_sync_in_progress"]:
        logger.warning("Sync already in progress, skipping this cycle")
        return

    service_status["current_sync_in_progress"] = True
    logger.info("Starting synchronization cycle")

    try:
        # Step 1: Get files from S3
        s3_files = await get_s3_files_info()

        # Step 2: Get files from Qdrant
        qdrant_files = await get_qdrant_files_info()

        # Step 3: Compare and get lists
        to_upload, to_delete, to_update = compare_files(s3_files, qdrant_files)

        # Step 4: Process deletions first
        logger.info(f"Deleting {len(to_delete)} files")
        for filename in to_delete:
            try:
                await delete_file_from_qdrant(filename)
                logger.info(f"Deleted file: {filename}")
            except Exception as e:
                logger.error(f"Failed to delete {filename}: {e}")

        # Step 5: Process updates - delete old versions
        logger.info(f"Deleting old versions of {len(to_update)} files")
        for filename in to_update:
            try:
                await delete_file_from_qdrant(filename)
                logger.info(f"Deleted old version of {filename}")
            except Exception as e:
                logger.error(f"Failed to delete old version of {filename}: {e}")

        # Step 6: Process updates - upload new versions
        logger.info(f"Updating {len(to_update)} files")
        update_tasks = [process_single_file(filename, 'update', s3_files) for filename in to_update]
        if update_tasks:
            await asyncio.gather(*update_tasks)

        # Step 7: Process new uploads
        logger.info(f"Uploading {len(to_upload)} new files")
        upload_tasks = [process_single_file(filename, 'upload', s3_files) for filename in to_upload]
        if upload_tasks:
            await asyncio.gather(*upload_tasks)

        service_status["last_sync_time"] = datetime.now().isoformat()
        logger.info(
            f"Synchronization completed. Processed: {len(to_upload)} uploads, {len(to_delete)} deletions, {len(to_update)} updates")

    except Exception as e:
        logger.error(f"Sync failed: {e}")
        service_status["errors_count"] += 1
    finally:
        service_status["current_sync_in_progress"] = False


async def periodic_sync():
    """Run sync periodically"""
    while service_status["running"]:
        await sync_files()
        await asyncio.sleep(SYNC_INTERVAL_SECONDS)


async def check_service_health() -> Dict:
    """Check health of all dependent services"""
    current_time = datetime.now()

    # Check MinIO
    try:
        minio_client.list_buckets()
        health_cache["minio"] = {"status": "healthy", "last_check": current_time.isoformat()}
    except Exception as e:
        health_cache["minio"] = {"status": "unhealthy", "last_check": current_time.isoformat(), "error": str(e)}
        logger.error(f"MinIO health check failed: {e}")

    # Check Qdrant
    try:
        qdrant_client.get_collections()
        health_cache["qdrant"] = {"status": "healthy", "last_check": current_time.isoformat()}
    except Exception as e:
        health_cache["qdrant"] = {"status": "unhealthy", "last_check": current_time.isoformat(), "error": str(e)}
        logger.error(f"Qdrant health check failed: {e}")

    # Check Text Converter
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(TEXT_CONVERTER_URL.replace("/convert", "/health"))
            if response.status_code == 200:
                health_cache["text_converter"] = {"status": "healthy", "last_check": current_time.isoformat()}
            else:
                health_cache["text_converter"] = {"status": "unhealthy", "last_check": current_time.isoformat(),
                                                  "error": f"HTTP {response.status_code}"}
    except Exception as e:
        health_cache["text_converter"] = {"status": "unhealthy", "last_check": current_time.isoformat(),
                                          "error": str(e)}
        logger.error(f"Text converter health check failed: {e}")

    # Check Chunk & Vec
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(CHUNK_N_VEC_URL.replace("/process", "/health"))
            if response.status_code == 200:
                health_cache["chunk_n_vec"] = {"status": "healthy", "last_check": current_time.isoformat()}
            else:
                health_cache["chunk_n_vec"] = {"status": "unhealthy", "last_check": current_time.isoformat(),
                                               "error": f"HTTP {response.status_code}"}
    except Exception as e:
        health_cache["chunk_n_vec"] = {"status": "unhealthy", "last_check": current_time.isoformat(), "error": str(e)}
        logger.error(f"Chunk & vec health check failed: {e}")

    return health_cache


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for startup and shutdown events"""
    # Startup
    logger.info("Starting Sync Service...")
    init_clients()
    await ensure_bucket_exists()
    await ensure_collection_exists()

    # Start periodic sync task
    sync_task = asyncio.create_task(periodic_sync())

    yield

    # Shutdown
    logger.info("Shutting down Sync Service...")
    service_status["running"] = False
    sync_task.cancel()
    try:
        await sync_task
    except asyncio.CancelledError:
        pass


# Create FastAPI app
app = FastAPI(
    title="S3 to Qdrant Sync Service",
    description="Synchronizes files from S3 (MinIO) to Qdrant vector database",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/")
async def root():
    """Root endpoint with service information"""
    return {
        "service": "S3 to Qdrant Sync Service",
        "version": "1.0.0",
        "description": "Synchronizes files from S3 bucket to Qdrant collection",
        "endpoints": {
            "/": "Service information",
            "/health": "Health status of service and dependencies",
            "/status": "Service operational status"
        },
        "configuration": {
            "s3_bucket": MINIO_BUCKET_NAME,
            "qdrant_collection": QDRANT_COLLECTION_NAME,
            "sync_interval_minutes": SYNC_INTERVAL_SECONDS // 60
        }
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    health_status = await check_service_health()

    # Determine overall status
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
                "errors_total": service_status["errors_count"]
            }
        }
    )


@app.get("/status")
async def status():
    """Service operational status"""
    return {
        "running": service_status["running"],
        "last_sync_time": service_status["last_sync_time"],
        "current_sync_in_progress": service_status["current_sync_in_progress"],
        "files_processed_since_startup": service_status["files_processed"],
        "errors_since_startup": service_status["errors_count"]
    }


@app.post("/sync")
async def trigger_sync():
    """Manually trigger synchronization"""
    if service_status["current_sync_in_progress"]:
        raise HTTPException(status_code=409, detail="Sync already in progress")

    asyncio.create_task(sync_files())
    return {"message": "Sync triggered successfully"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=SERVICE_PORT, log_level=LOG_LEVEL.lower())