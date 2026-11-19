"""Схемы ответов API."""

from pydantic import BaseModel


class LiveResponse(BaseModel):
    status: str


class DependencyHealth(BaseModel):
    status: str
    last_check: str | None = None
    error: str | None = None


class SyncStatus(BaseModel):
    last_sync: str | None
    sync_in_progress: bool
    files_processed_total: int
    errors_total: int


class HealthResponse(BaseModel):
    status: str
    services: dict[str, DependencyHealth]
    sync_status: SyncStatus


class StatusResponse(BaseModel):
    running: bool
    last_sync_time: str | None
    current_sync_in_progress: bool
    files_processed_since_startup: int
    errors_since_startup: int


class SyncTriggerResponse(BaseModel):
    message: str
