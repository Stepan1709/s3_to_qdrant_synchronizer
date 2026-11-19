"""Клиент chunk_n_vec_server."""

import logging

import httpx

from app.clients.health import check_health
from app.clients.retry import network_retry
from app.core.exceptions import VectorizationError
from app.schemas.files import Chunk

logger = logging.getLogger(__name__)


class ChunkNVecClient:
    def __init__(
        self,
        http: httpx.AsyncClient,
        base_url: str,
        timeout: float,
        max_retries: int,
        chunk_max_size: int,
        chunk_overlap: int,
        retry_wait_min: float = 4.0,
    ) -> None:
        self._http = http
        self._base_url = base_url
        self._timeout = timeout
        self._max_retries = max_retries
        self._chunk_max_size = chunk_max_size
        self._chunk_overlap = chunk_overlap
        self._retry_wait_min = retry_wait_min

    async def chunk_and_vectorize(self, text: str, filename: str) -> list[Chunk]:
        """Режет текст на чанки и векторизует их. При ошибке бросает VectorizationError."""
        payload = {"text": text, "max_chunk_size": self._chunk_max_size, "overlap": self._chunk_overlap}
        try:
            async for attempt in network_retry(self._max_retries, self._retry_wait_min):
                with attempt:
                    response = await self._http.post(
                        f"{self._base_url}/process", json=payload, timeout=self._timeout
                    )
        except httpx.HTTPError as exc:
            raise VectorizationError(f"сервис векторизации недоступен: {exc!r}") from exc

        if response.status_code != 200:
            raise VectorizationError(
                f"сервис векторизации вернул {response.status_code}: {response.text[:200]}"
            )
        try:
            result = response.json()
            chunks = [Chunk.model_validate(c) for c in result["chunks"]]
        except (ValueError, KeyError, TypeError) as exc:
            raise VectorizationError("неожиданный формат ответа сервиса векторизации") from exc

        logger.info(
            "%s: чанков %d, время векторизации %.3f с",
            filename,
            len(chunks),
            float(result.get("processing_time", 0)),
        )
        return chunks

    async def is_healthy(self) -> tuple[bool, str | None]:
        return await check_health(self._http, self._base_url)
