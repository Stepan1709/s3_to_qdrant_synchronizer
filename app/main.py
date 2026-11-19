"""Точка входа: сборка FastAPI-приложения и запуск периодической синхронизации."""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from minio import Minio
from qdrant_client import AsyncQdrantClient

from app.api.routers import health, sync
from app.clients.chunk_n_vec import ChunkNVecClient
from app.clients.text_converter import TextConverterClient
from app.core.config import Settings, get_settings
from app.core.exceptions import register_exception_handlers
from app.core.logging import setup_logging
from app.repositories.s3 import S3Repository
from app.repositories.vector_store import VectorStoreRepository
from app.services.health import HealthService
from app.services.scheduler import run_periodically
from app.services.state import SyncState
from app.services.sync import SyncService

logger = logging.getLogger(__name__)


def build_services(
    settings: Settings, http: httpx.AsyncClient, qdrant: AsyncQdrantClient
) -> tuple[SyncService, HealthService, SyncState, S3Repository, VectorStoreRepository]:
    s3 = S3Repository(
        Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        ),
        settings.minio_bucket_name,
    )
    store = VectorStoreRepository(
        qdrant,
        settings.qdrant_collection_name,
        settings.qdrant_vector_size,
        settings.qdrant_upsert_batch_size,
    )
    converter = TextConverterClient(
        http, settings.text_converter_base_url, settings.request_timeout, settings.max_retries
    )
    vectorizer = ChunkNVecClient(
        http,
        settings.chunk_n_vec_base_url,
        settings.request_timeout,
        settings.max_retries,
        settings.chunk_max_size,
        settings.chunk_overlap,
    )
    state = SyncState()
    sync_service = SyncService(s3, store, converter, vectorizer, state, settings.max_concurrent_files)

    async def check_s3() -> tuple[bool, str | None]:
        await s3.ping()
        return True, None

    async def check_qdrant() -> tuple[bool, str | None]:
        await store.ping()
        return True, None

    health_service = HealthService(
        {
            "minio": check_s3,
            "qdrant": check_qdrant,
            "text_converter": converter.is_healthy,
            "chunk_n_vec": vectorizer.is_healthy,
        },
        cache_ttl=settings.health_check_interval,
    )
    return sync_service, health_service, state, s3, store


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logger.info("Запуск сервиса синхронизации")
    qdrant = AsyncQdrantClient(
        host=settings.qdrant_host,
        port=settings.qdrant_port,
        api_key=settings.qdrant_api_key or None,
        https=settings.qdrant_use_https,
        timeout=30,
    )
    async with httpx.AsyncClient() as http:
        sync_service, health_service, state, s3, store = build_services(settings, http, qdrant)
        app.state.sync_service = sync_service
        app.state.health_service = health_service
        app.state.sync_state = state

        await s3.ensure_bucket()
        await store.ensure_collection()
        sync_task = asyncio.create_task(run_periodically(sync_service, settings.sync_interval_seconds))
        try:
            yield
        finally:
            logger.info("Остановка сервиса синхронизации")
            state.running = False
            sync_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sync_task
            await qdrant.close()


def create_app() -> FastAPI:
    setup_logging(get_settings().log_level)
    app = FastAPI(
        title="S3 to Qdrant Sync Service",
        description="Синхронизация файлов из S3 (MinIO) в векторную базу Qdrant",
        version=health.VERSION,
        lifespan=lifespan,
    )
    register_exception_handlers(app)
    app.include_router(health.router)
    app.include_router(sync.router)
    return app


app = create_app()
