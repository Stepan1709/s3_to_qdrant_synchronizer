"""Повторы запросов к соседним сервисам при временных сетевых сбоях."""

import httpx
from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_exponential

TRANSIENT_ERRORS = (httpx.TimeoutException, httpx.ConnectError)


def network_retry(max_attempts: int, wait_min: float = 4.0, wait_max: float = 10.0) -> AsyncRetrying:
    return AsyncRetrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1, min=wait_min, max=wait_max),
        retry=retry_if_exception_type(TRANSIENT_ERRORS),
        reraise=True,
    )
