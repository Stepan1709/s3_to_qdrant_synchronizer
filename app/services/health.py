"""Проверка состояния зависимостей с кешированием результата."""

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from app.schemas.api import DependencyHealth

logger = logging.getLogger(__name__)

# Проверка возвращает (здоров, текст ошибки)
Check = Callable[[], Awaitable[tuple[bool, str | None]]]


class HealthService:
    def __init__(self, checks: dict[str, Check], cache_ttl: float) -> None:
        self._checks = checks
        self._cache_ttl = cache_ttl
        self._cache: dict[str, DependencyHealth] = {
            name: DependencyHealth(status="unknown") for name in checks
        }
        self._checked_at: float | None = None
        self._lock = asyncio.Lock()

    async def check(self) -> dict[str, DependencyHealth]:
        """Опрашивает зависимости не чаще, чем раз в cache_ttl секунд."""
        async with self._lock:
            if self._checked_at is not None and time.monotonic() - self._checked_at < self._cache_ttl:
                return self._cache
            logger.info("Проверка состояния зависимостей")
            names = list(self._checks)
            results = await asyncio.gather(*(self._run(name) for name in names))
            self._cache = dict(zip(names, results, strict=True))
            self._checked_at = time.monotonic()
            return self._cache

    async def _run(self, name: str) -> DependencyHealth:
        now = datetime.now(UTC).isoformat()
        try:
            healthy, error = await self._checks[name]()
        except Exception as exc:
            healthy, error = False, str(exc)
        if not healthy:
            logger.error("Зависимость %s недоступна: %s", name, error)
        return DependencyHealth(status="healthy" if healthy else "unhealthy", last_check=now, error=error)
