"""Служебные эндпоинты: информация о сервисе, liveness, health и status."""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.api.deps import HealthServiceDep, SettingsDep, StateDep
from app.schemas.api import HealthResponse, LiveResponse, StatusResponse, SyncStatus

router = APIRouter(tags=["service"])

VERSION = "2.0.0"


@router.get("/")
async def root(settings: SettingsDep) -> dict[str, object]:
    return {
        "service": "S3 to Qdrant Sync Service",
        "version": VERSION,
        "description": "Синхронизирует файлы из S3 (MinIO) в векторную базу Qdrant",
        "endpoints": {
            "/": "Информация о сервисе",
            "/live": "Liveness-проба (зависимости не проверяет)",
            "/health": "Состояние сервиса и зависимостей",
            "/status": "Рабочее состояние синхронизации",
            "/sync": "POST - запустить синхронизацию вручную",
        },
        "configuration": {
            "s3_bucket": settings.minio_bucket_name,
            "qdrant_collection": settings.qdrant_collection_name,
            "sync_interval_minutes": settings.sync_interval_minutes,
        },
    }


@router.get("/live", response_model=LiveResponse)
async def liveness() -> LiveResponse:
    return LiveResponse(status="alive")


@router.get("/health", response_model=HealthResponse)
async def health(service: HealthServiceDep, state: StateDep) -> JSONResponse:
    """Состояние зависимостей (результат кешируется). Если что-то недоступно, код ответа 503."""
    services = await service.check()
    healthy = all(s.status == "healthy" for s in services.values())
    body = HealthResponse(
        status="healthy" if healthy else "degraded",
        services=services,
        sync_status=SyncStatus(
            last_sync=state.last_sync_time,
            sync_in_progress=state.in_progress,
            files_processed_total=state.files_processed,
            errors_total=state.errors_count,
        ),
    )
    return JSONResponse(status_code=200 if healthy else 503, content=body.model_dump())


@router.get("/status", response_model=StatusResponse)
async def status(state: StateDep) -> StatusResponse:
    return StatusResponse(
        running=state.running,
        last_sync_time=state.last_sync_time,
        current_sync_in_progress=state.in_progress,
        files_processed_since_startup=state.files_processed,
        errors_since_startup=state.errors_count,
    )
