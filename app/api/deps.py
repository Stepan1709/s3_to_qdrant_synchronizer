"""Зависимости FastAPI: доступ к объектам, созданным в lifespan."""

from typing import Annotated

from fastapi import Depends, Request

from app.core.config import Settings, get_settings
from app.services.health import HealthService
from app.services.state import SyncState
from app.services.sync import SyncService


def get_sync_service(request: Request) -> SyncService:
    service: SyncService = request.app.state.sync_service
    return service


def get_health_service(request: Request) -> HealthService:
    service: HealthService = request.app.state.health_service
    return service


def get_state(request: Request) -> SyncState:
    state: SyncState = request.app.state.sync_state
    return state


SettingsDep = Annotated[Settings, Depends(get_settings)]
SyncServiceDep = Annotated[SyncService, Depends(get_sync_service)]
HealthServiceDep = Annotated[HealthService, Depends(get_health_service)]
StateDep = Annotated[SyncState, Depends(get_state)]
