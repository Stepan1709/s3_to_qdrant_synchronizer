from collections.abc import AsyncIterator

import pytest
from qdrant_client import AsyncQdrantClient

from app.repositories.vector_store import VectorStoreRepository
from app.schemas.files import Chunk, FileInfo

COLLECTION = "test"


@pytest.fixture
async def store() -> AsyncIterator[VectorStoreRepository]:
    client = AsyncQdrantClient(":memory:")
    repo = VectorStoreRepository(client, COLLECTION, vector_size=2, upsert_batch_size=2)
    await repo.ensure_collection()
    yield repo
    await client.close()


def chunks(n: int) -> list[Chunk]:
    return [Chunk(embedding=[1.0, float(i)], chunk_text=f"chunk {i}") for i in range(n)]


async def test_ensure_collection_is_idempotent(store: VectorStoreRepository) -> None:
    await store.ensure_collection()


async def test_upsert_and_list_files(store: VectorStoreRepository) -> None:
    info = FileInfo(hash="h1", modified="2026-01-01T00:00:00+00:00", tags={"k": "v"})
    await store.upsert_file("dir/a.pdf", info, chunks(5))  # 5 чанков при батче 2

    files = await store.list_files()

    assert files == {"dir/a.pdf": info}


async def test_upsert_is_idempotent(store: VectorStoreRepository) -> None:
    info = FileInfo(hash="h1")
    await store.upsert_file("a.pdf", info, chunks(3))
    await store.upsert_file("a.pdf", info, chunks(3))

    count = await store._client.count(COLLECTION)
    assert count.count == 3


async def test_delete_file_removes_all_points(store: VectorStoreRepository) -> None:
    await store.upsert_file("a.pdf", FileInfo(hash="h1"), chunks(3))
    await store.upsert_file("b.pdf", FileInfo(hash="h2"), chunks(2))

    await store.delete_file("a.pdf")

    assert list(await store.list_files()) == ["b.pdf"]
    assert (await store._client.count(COLLECTION)).count == 2


async def test_delete_file_keeps_new_version(store: VectorStoreRepository) -> None:
    await store.upsert_file("a.pdf", FileInfo(hash="old"), chunks(3))
    await store.upsert_file("a.pdf", FileInfo(hash="new"), chunks(2))

    await store.delete_file("a.pdf", keep_hash="new")

    assert (await store.list_files()) == {"a.pdf": FileInfo(hash="new")}
    assert (await store._client.count(COLLECTION)).count == 2


async def test_payload_format(store: VectorStoreRepository) -> None:
    await store.upsert_file("a.pdf", FileInfo(hash="h1", modified="m", tags={"t": "1"}), chunks(2))

    points, _ = await store._client.scroll(COLLECTION, limit=10, with_payload=True)
    payloads = sorted((p.payload or {} for p in points), key=lambda p: p["metadata"]["chunk_index"])

    assert payloads[1] == {
        "content": "chunk 1",
        "metadata": {
            "file_name": "a.pdf",
            "file_hash": "h1",
            "file_modified": "m",
            "file_tags": {"t": "1"},
            "chunk_position": 2,
            "chunk_index": 1,
            "total_chunks": 2,
        },
    }
