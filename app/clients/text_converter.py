"""Клиент file_to_text_converter_server."""

import logging

import httpx

from app.clients.health import check_health
from app.clients.retry import network_retry
from app.core.exceptions import TextExtractionError

logger = logging.getLogger(__name__)


class TextConverterClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        timeout: float,
        max_retries: int,
        retry_wait_min: float = 4.0,
    ) -> None:
        self._http = http
        self._base_url = base_url
        self._timeout = timeout
        self._max_retries = max_retries
        self._retry_wait_min = retry_wait_min

    async def extract_text(self, content: bytes, filename: str) -> str:
        """Возвращает текст файла. При ошибке бросает TextExtractionError."""
        try:
            async for attempt in network_retry(self._max_retries, self._retry_wait_min):
                with attempt:
                    response = await self._http.post(
                        f"{self._base_url}/convert",
                        files={"file": (filename, content, "application/octet-stream")},
                        timeout=self._timeout,
                    )
        except httpx.HTTPError as exc:
            raise TextExtractionError(f"сервис конвертации недоступен: {exc!r}") from exc

        if response.status_code != 200:
            raise TextExtractionError(
                f"сервис конвертации вернул {response.status_code}: {response.text[:200]}"
            )
        try:
            result = response.json()
            text = result["file_text"]
        except (ValueError, KeyError, TypeError) as exc:
            raise TextExtractionError("неожиданный формат ответа сервиса конвертации") from exc

        logger.info("Текст из %s извлечён, время: %s", filename, result.get("worktime", "неизвестно"))
        return str(text)

    async def is_healthy(self) -> tuple[bool, str | None]:
        return await check_health(self._http, self._base_url)
