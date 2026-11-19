"""Периодический запуск синхронизации."""

import asyncio

from app.services.sync import SyncService


async def run_periodically(sync: SyncService, interval_seconds: float) -> None:
    """Запускает цикл синхронизации сразу, затем каждые interval_seconds. Останавливается через cancel()."""
    while True:
        await sync.run_cycle()
        await asyncio.sleep(interval_seconds)
