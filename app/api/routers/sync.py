"""Ручной запуск синхронизации."""

from fastapi import APIRouter

from app.api.deps import SyncServiceDep
from app.schemas.api import SyncTriggerResponse

router = APIRouter(tags=["sync"])


@router.post("/sync", response_model=SyncTriggerResponse)
async def trigger_sync(service: SyncServiceDep) -> SyncTriggerResponse:
    """Запускает синхронизацию в фоне. Если она уже идёт, возвращает 409."""
    service.start_background_cycle()
    return SyncTriggerResponse(message="Синхронизация запущена")
