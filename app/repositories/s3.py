"""Репозиторий файлов в S3 (MinIO). Клиент MinIO синхронный, поэтому вызовы выполняются в потоках."""

import asyncio
import logging

from minio import Minio
from minio.error import S3Error

from app.schemas.files import FileInfo

logger = logging.getLogger(__name__)


class S3Repository:
    def __init__(self, client: Minio, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    async def ensure_bucket(self) -> None:
        """Создаёт бакет, если его нет."""
        if await asyncio.to_thread(self._client.bucket_exists, self._bucket):
            logger.info("Бакет %s уже существует", self._bucket)
            return
        await asyncio.to_thread(self._client.make_bucket, self._bucket)
        logger.info("Создан бакет %s", self._bucket)

    async def list_files(self) -> dict[str, FileInfo]:
        """Возвращает {имя объекта: описание} для всех объектов бакета."""
        files = await asyncio.to_thread(self._list_files)
        logger.info("В бакете %s файлов: %d", self._bucket, len(files))
        return files

    async def read_file(self, name: str) -> bytes:
        return await asyncio.to_thread(self._read_file, name)

    async def ping(self) -> None:
        """Бросает исключение, если S3 недоступен."""
        await asyncio.to_thread(self._client.list_buckets)

    def _list_files(self) -> dict[str, FileInfo]:
        files: dict[str, FileInfo] = {}
        for obj in self._client.list_objects(self._bucket, recursive=True):
            name = obj.object_name
            if name is None:
                continue
            files[name] = FileInfo(
                hash=(obj.etag or "").strip('"'),
                modified=obj.last_modified.isoformat() if obj.last_modified else "",
                tags=self._get_tags(name),
            )
        return files

    def _get_tags(self, name: str) -> dict[str, str]:
        try:
            return dict(self._client.get_object_tags(self._bucket, name) or {})
        except S3Error as exc:  # у объекта нет тегов
            logger.debug("Теги объекта %s недоступны: %s", name, exc)
            return {}

    def _read_file(self, name: str) -> bytes:
        response = self._client.get_object(self._bucket, name)
        try:
            return bytes(response.read())
        finally:
            response.close()
            response.release_conn()
