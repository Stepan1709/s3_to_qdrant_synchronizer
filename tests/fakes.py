"""Простые заглушки репозиториев и клиентов для тестов сервиса синхронизации."""

import asyncio

from app.core.exceptions import TextExtractionError
from app.schemas.files import Chunk, FileInfo


class FakeS3:
    def __init__(self, files: dict[str, tuple[FileInfo, bytes]] | None = None) -> None:
        self.files = files or {}

    async def list_files(self) -> dict[str, FileInfo]:
        return {name: info for name, (info, _) in self.files.items()}

    async def read_file(self, name: str) -> bytes:
        return self.files[name][1]


class FakeStore:
    """Хранит {имя файла: FileInfo} и журнал вызовов."""

    def __init__(self, files: dict[str, FileInfo] | None = None) -> None:
        self.files = files or {}
        self.calls: list[tuple[str, str, str | None]] = []
        self.fail_upsert_for: set[str] = set()

    async def list_files(self) -> dict[str, FileInfo]:
        return dict(self.files)

    async def upsert_file(self, filename: str, info: FileInfo, chunks: list[Chunk]) -> None:
        if filename in self.fail_upsert_for:
            raise RuntimeError("qdrant is down")
        self.calls.append(("upsert", filename, info.hash))
        self.files[filename] = info

    async def delete_file(self, filename: str, keep_hash: str | None = None) -> None:
        self.calls.append(("delete", filename, keep_hash))
        if keep_hash is None:
            self.files.pop(filename, None)


class FakeExtractor:
    def __init__(self, text: str = "some text", fail_for: set[str] | None = None, delay: float = 0) -> None:
        self.text = text
        self.fail_for = fail_for or set()
        self.delay = delay

    async def extract_text(self, content: bytes, filename: str) -> str:
        if self.delay:
            await asyncio.sleep(self.delay)
        if filename in self.fail_for:
            raise TextExtractionError("converter failed")
        return self.text


class FakeVectorizer:
    def __init__(self, chunks: list[Chunk] | None = None) -> None:
        self.chunks = [Chunk(embedding=[0.1, 0.2], chunk_text="c")] if chunks is None else chunks

    async def chunk_and_vectorize(self, text: str, filename: str) -> list[Chunk]:
        return self.chunks
