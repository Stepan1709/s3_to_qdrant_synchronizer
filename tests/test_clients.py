from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
import respx

from app.clients.chunk_n_vec import ChunkNVecClient
from app.clients.text_converter import TextConverterClient
from app.core.exceptions import TextExtractionError, VectorizationError

CONVERTER_URL = "http://converter.test"
CHUNK_URL = "http://chunk.test"


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as r:
        yield r


@pytest.fixture
async def http() -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient() as client:
        yield client


def converter(http: httpx.AsyncClient) -> TextConverterClient:
    return TextConverterClient(http, CONVERTER_URL, timeout=5, max_retries=3, retry_wait_min=0)


def chunker(http: httpx.AsyncClient) -> ChunkNVecClient:
    return ChunkNVecClient(
        http, CHUNK_URL, timeout=5, max_retries=3, chunk_max_size=100, chunk_overlap=10, retry_wait_min=0
    )


async def test_extract_text_success(router: respx.MockRouter, http: httpx.AsyncClient) -> None:
    router.post(f"{CONVERTER_URL}/convert").respond(json={"file_text": "привет", "worktime": "0:00:01"})

    assert await converter(http).extract_text(b"data", "a.pdf") == "привет"


async def test_extract_text_http_error(router: respx.MockRouter, http: httpx.AsyncClient) -> None:
    router.post(f"{CONVERTER_URL}/convert").respond(status_code=502, text="boom")

    with pytest.raises(TextExtractionError, match="502"):
        await converter(http).extract_text(b"data", "a.pdf")


async def test_extract_text_bad_payload(router: respx.MockRouter, http: httpx.AsyncClient) -> None:
    router.post(f"{CONVERTER_URL}/convert").respond(json={"unexpected": 1})

    with pytest.raises(TextExtractionError):
        await converter(http).extract_text(b"data", "a.pdf")


async def test_extract_text_retries_transient_errors(
    router: respx.MockRouter, http: httpx.AsyncClient
) -> None:
    route = router.post(f"{CONVERTER_URL}/convert")
    route.side_effect = [httpx.ConnectError("refused"), httpx.Response(200, json={"file_text": "ok"})]

    assert await converter(http).extract_text(b"data", "a.pdf") == "ok"
    assert route.call_count == 2


async def test_extract_text_gives_up_after_max_retries(
    router: respx.MockRouter, http: httpx.AsyncClient
) -> None:
    route = router.post(f"{CONVERTER_URL}/convert").mock(side_effect=httpx.ConnectError("refused"))

    with pytest.raises(TextExtractionError):
        await converter(http).extract_text(b"data", "a.pdf")
    assert route.call_count == 3


async def test_http_error_status_is_not_retried(router: respx.MockRouter, http: httpx.AsyncClient) -> None:
    route = router.post(f"{CONVERTER_URL}/convert").respond(status_code=500)

    with pytest.raises(TextExtractionError):
        await converter(http).extract_text(b"data", "a.pdf")
    assert route.call_count == 1


async def test_chunk_and_vectorize_success(router: respx.MockRouter, http: httpx.AsyncClient) -> None:
    route = router.post(f"{CHUNK_URL}/process").respond(
        json={
            "chunks": [{"embedding": [0.1, 0.2], "chunk_text": "a"}],
            "total_chunks": 1,
            "processing_time": 0.5,
        }
    )

    chunks = await chunker(http).chunk_and_vectorize("text", "a.pdf")

    assert [c.chunk_text for c in chunks] == ["a"]
    assert route.calls[0].request.content == b'{"text":"text","max_chunk_size":100,"overlap":10}'


async def test_chunk_and_vectorize_error(router: respx.MockRouter, http: httpx.AsyncClient) -> None:
    router.post(f"{CHUNK_URL}/process").respond(status_code=503)

    with pytest.raises(VectorizationError):
        await chunker(http).chunk_and_vectorize("text", "a.pdf")


async def test_is_healthy(router: respx.MockRouter, http: httpx.AsyncClient) -> None:
    router.get(f"{CHUNK_URL}/health").respond(200)
    router.get(f"{CONVERTER_URL}/health").respond(503)

    assert await chunker(http).is_healthy() == (True, None)
    assert await converter(http).is_healthy() == (False, "HTTP 503")
