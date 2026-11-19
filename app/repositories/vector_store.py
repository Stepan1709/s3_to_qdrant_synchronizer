"""Репозиторий чанков в Qdrant. Знает формат точек и payload, остальной код с Qdrant не работает."""

import hashlib
import logging
import uuid

from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models

from app.schemas.files import Chunk, FileInfo

logger = logging.getLogger(__name__)

SCROLL_PAGE_SIZE = 100
FILE_NAME_KEY = "metadata.file_name"
FILE_HASH_KEY = "metadata.file_hash"
CHUNK_INDEX_KEY = "metadata.chunk_index"


def _match(key: str, value: str | int) -> models.FieldCondition:
    return models.FieldCondition(key=key, match=models.MatchValue(value=value))


class VectorStoreRepository:
    def __init__(
        self, client: AsyncQdrantClient, collection: str, vector_size: int, upsert_batch_size: int
    ) -> None:
        self._client = client
        self._collection = collection
        self._vector_size = vector_size
        self._batch_size = upsert_batch_size

    async def ensure_collection(self) -> None:
        """Создаёт коллекцию с косинусной метрикой, если её нет."""
        names = [c.name for c in (await self._client.get_collections()).collections]
        if self._collection in names:
            logger.info("Коллекция %s уже существует", self._collection)
            return
        await self._client.create_collection(
            collection_name=self._collection,
            vectors_config=models.VectorParams(size=self._vector_size, distance=models.Distance.COSINE),
        )
        logger.info("Создана коллекция %s", self._collection)

    async def list_files(self) -> dict[str, FileInfo]:
        """Возвращает {имя файла: описание}. Файл определяется по его первому чанку."""
        files: dict[str, FileInfo] = {}
        offset: models.ExtendedPointId | None = None
        first_chunk = models.Filter(must=[_match(CHUNK_INDEX_KEY, 0)])
        while True:
            points, offset = await self._client.scroll(
                collection_name=self._collection,
                scroll_filter=first_chunk,
                limit=SCROLL_PAGE_SIZE,
                offset=offset,
                with_payload=["metadata.file_name", "metadata.file_hash", "metadata.file_modified"]
                + ["metadata.file_tags"],
                with_vectors=False,
            )
            for point in points:
                metadata = (point.payload or {}).get("metadata") or {}
                if "file_name" in metadata:
                    files[metadata["file_name"]] = FileInfo(
                        hash=metadata.get("file_hash", ""),
                        modified=metadata.get("file_modified", ""),
                        tags=metadata.get("file_tags", {}),
                    )
            if offset is None:
                break
        logger.info("В коллекции %s файлов: %d", self._collection, len(files))
        return files

    async def upsert_file(self, filename: str, info: FileInfo, chunks: list[Chunk]) -> None:
        """Записывает чанки файла. Идентификаторы точек детерминированы, повторная запись безопасна."""
        points = self._build_points(filename, info, chunks)
        for i in range(0, len(points), self._batch_size):
            await self._client.upsert(
                collection_name=self._collection, points=points[i : i + self._batch_size]
            )
        logger.info("Записано чанков файла %s: %d", filename, len(points))

    async def delete_file(self, filename: str, keep_hash: str | None = None) -> None:
        """
        Удаляет точки файла.

        Если указан keep_hash, точки с этим хешем остаются: так удаляется только устаревшая версия файла.
        """
        must_not = [_match(FILE_HASH_KEY, keep_hash)] if keep_hash else None
        await self._client.delete(
            collection_name=self._collection,
            points_selector=models.Filter(must=[_match(FILE_NAME_KEY, filename)], must_not=must_not),
        )
        logger.info("Удалены точки файла %s", filename)

    async def ping(self) -> None:
        """Бросает исключение, если Qdrant недоступен."""
        await self._client.get_collections()

    @staticmethod
    def _build_points(filename: str, info: FileInfo, chunks: list[Chunk]) -> list[models.PointStruct]:
        # md5 используется только для получения стабильного идентификатора, не для защиты
        file_id = hashlib.md5(f"{filename}_{info.hash}".encode(), usedforsecurity=False).hexdigest()[:16]
        return [
            models.PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{file_id}_{i}_{info.hash[:8]}")),
                vector=chunk.embedding,
                payload={
                    "content": chunk.chunk_text,
                    "metadata": {
                        "file_name": filename,
                        "file_hash": info.hash,
                        "file_modified": info.modified,
                        "file_tags": info.tags,
                        "chunk_position": i + 1,
                        "chunk_index": i,
                        "total_chunks": len(chunks),
                    },
                },
            )
            for i, chunk in enumerate(chunks)
        ]
