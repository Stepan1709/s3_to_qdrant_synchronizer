"""Проверка доступности соседних сервисов."""

import httpx


async def check_health(http: httpx.AsyncClient, base_url: str) -> tuple[bool, str | None]:
    """Проверяет GET /health соседнего сервиса. Возвращает (здоров, текст ошибки)."""
    try:
        response = await http.get(f"{base_url}/health", timeout=5.0)
    except httpx.HTTPError as exc:
        return False, str(exc)
    if response.status_code == 200:
        return True, None
    return False, f"HTTP {response.status_code}"
