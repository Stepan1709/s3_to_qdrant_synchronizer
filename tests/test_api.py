from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI

from app.main import create_app
from app.services.health import HealthService
from app.services.state import SyncState
from app.services.sync import SyncService
from tests.fakes import FakeExtractor, FakeS3, FakeStore, FakeVectorizer


@pytest.fixture
def state() -> SyncState:
    return SyncState()


@pytest.fixture
def healthy() -> dict[str, bool]:
    return {"minio": True, "qdrant": True}


@pytest.fixture
def app(state: SyncState, healthy: dict[str, bool]) -> FastAPI:
    application = create_app()

    def check(name: str):  # type: ignore[no-untyped-def]
        async def run() -> tuple[bool, str | None]:
            return healthy[name], None if healthy[name] else "down"

        return run

    application.state.sync_state = state
    application.state.sync_service = SyncService(
        FakeS3(),  # type: ignore[arg-type]
        FakeStore(),  # type: ignore[arg-type]
        FakeExtractor(),
        FakeVectorizer(),
        state,
        max_concurrent_files=1,
    )
    application.state.health_service = HealthService({n: check(n) for n in healthy}, cache_ttl=0)
    return application


@pytest.fixture
async def client(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as c:
        yield c


async def test_live(client: httpx.AsyncClient) -> None:
    assert (await client.get("/live")).json() == {"status": "alive"}


async def test_status(client: httpx.AsyncClient, state: SyncState) -> None:
    state.files_processed = 3
    state.errors_count = 1

    body = (await client.get("/status")).json()

    assert body["files_processed_since_startup"] == 3
    assert body["errors_since_startup"] == 1
    assert body["current_sync_in_progress"] is False


async def test_health_ok(client: httpx.AsyncClient) -> None:
    response = await client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "healthy"


async def test_health_returns_503_when_dependency_down(
    client: httpx.AsyncClient, healthy: dict[str, bool]
) -> None:
    healthy["qdrant"] = False

    response = await client.get("/health")

    assert response.status_code == 503
    assert response.json()["services"]["qdrant"]["status"] == "unhealthy"


async def test_sync_trigger_returns_409_when_running(client: httpx.AsyncClient, state: SyncState) -> None:
    async with state.lock:
        response = await client.post("/sync")

    assert response.status_code == 409


async def test_sync_trigger_starts_cycle(client: httpx.AsyncClient, state: SyncState) -> None:
    response = await client.post("/sync")

    assert response.status_code == 200
