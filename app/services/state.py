"""Состояние сервиса синхронизации."""

import asyncio
from dataclasses import dataclass, field


@dataclass
class SyncState:
    running: bool = True
    last_sync_time: str | None = None
    files_processed: int = 0
    errors_count: int = 0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def in_progress(self) -> bool:
        return self.lock.locked()
