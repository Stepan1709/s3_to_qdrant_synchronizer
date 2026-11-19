import pytest

from app.core.exceptions import SyncInProgressError
from app.schemas.files import FileInfo
from app.services.state import SyncState
from app.services.sync import SyncService, compare_files
from tests.fakes import FakeExtractor, FakeS3, FakeStore, FakeVectorizer


def info(hash_: str) -> FileInfo:
    return FileInfo(hash=hash_, modified="2026-01-01T00:00:00+00:00")


def make_service(
    s3: FakeS3,
    store: FakeStore,
    extractor: FakeExtractor | None = None,
    vectorizer: FakeVectorizer | None = None,
    state: SyncState | None = None,
) -> tuple[SyncService, SyncState]:
    state = state or SyncState()
    service = SyncService(
        s3,  # type: ignore[arg-type]
        store,  # type: ignore[arg-type]
        extractor or FakeExtractor(),
        vectorizer or FakeVectorizer(),
        state,
        max_concurrent_files=2,
    )
    return service, state


def test_compare_files() -> None:
    s3 = {"new": info("1"), "same": info("2"), "changed": info("3")}
    store = {"same": info("2"), "changed": info("old"), "gone": info("4")}

    plan = compare_files(s3, store)

    assert plan.to_upload == ["new"]
    assert plan.to_update == ["changed"]
    assert plan.to_delete == ["gone"]


async def test_new_file_is_uploaded() -> None:
    s3 = FakeS3({"a.pdf": (info("h1"), b"data")})
    store = FakeStore()
    service, state = make_service(s3, store)

    await service.run_cycle()

    assert store.calls == [("upsert", "a.pdf", "h1")]
    assert state.files_processed == 1
    assert state.errors_count == 0
    assert state.last_sync_time is not None


async def test_changed_file_old_version_deleted_after_upsert() -> None:
    s3 = FakeS3({"a.pdf": (info("new"), b"data")})
    store = FakeStore({"a.pdf": info("old")})
    service, _ = make_service(s3, store)

    await service.run_cycle()

    assert store.calls == [("upsert", "a.pdf", "new"), ("delete", "a.pdf", "new")]


async def test_changed_file_keeps_old_version_when_processing_fails() -> None:
    s3 = FakeS3({"a.pdf": (info("new"), b"data")})
    store = FakeStore({"a.pdf": info("old")})
    service, state = make_service(s3, store, extractor=FakeExtractor(fail_for={"a.pdf"}))

    await service.run_cycle()

    assert store.calls == []
    assert store.files["a.pdf"].hash == "old"
    assert state.errors_count == 1
    assert state.files_processed == 0


async def test_removed_file_is_deleted_from_store() -> None:
    s3 = FakeS3()
    store = FakeStore({"gone.pdf": info("h")})
    service, _ = make_service(s3, store)

    await service.run_cycle()

    assert store.calls == [("delete", "gone.pdf", None)]
    assert store.files == {}


async def test_one_failed_file_does_not_stop_others() -> None:
    s3 = FakeS3({"bad.pdf": (info("1"), b"x"), "good.pdf": (info("2"), b"y")})
    store = FakeStore()
    service, state = make_service(s3, store, extractor=FakeExtractor(fail_for={"bad.pdf"}))

    await service.run_cycle()

    assert list(store.files) == ["good.pdf"]
    assert state.files_processed == 1
    assert state.errors_count == 1


async def test_empty_text_is_skipped_without_error() -> None:
    s3 = FakeS3({"a.pdf": (info("1"), b"x")})
    store = FakeStore()
    service, state = make_service(s3, store, extractor=FakeExtractor(text="   "))

    await service.run_cycle()

    assert store.calls == []
    assert state.errors_count == 0
    assert state.files_processed == 0


async def test_no_chunks_counts_as_error() -> None:
    s3 = FakeS3({"a.pdf": (info("1"), b"x")})
    store = FakeStore()
    service, state = make_service(s3, store, vectorizer=FakeVectorizer(chunks=[]))

    await service.run_cycle()

    assert store.calls == []
    assert state.errors_count == 1


async def test_store_failure_is_counted_and_does_not_crash() -> None:
    s3 = FakeS3({"a.pdf": (info("1"), b"x")})
    store = FakeStore()
    store.fail_upsert_for.add("a.pdf")
    service, state = make_service(s3, store)

    await service.run_cycle()

    assert state.errors_count == 1
    assert state.files_processed == 0


async def test_start_background_cycle_rejected_while_running() -> None:
    state = SyncState()
    service, _ = make_service(FakeS3(), FakeStore(), state=state)

    async with state.lock:
        with pytest.raises(SyncInProgressError):
            service.start_background_cycle()
