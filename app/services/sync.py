"""Сценарий синхронизации S3 → Qdrant."""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Protocol

from app.core.exceptions import EmptyTextError, FileProcessingError, SyncInProgressError
from app.repositories.s3 import S3Repository
from app.repositories.vector_store import VectorStoreRepository
from app.schemas.files import Chunk, FileInfo, SyncPlan
from app.services.state import SyncState

logger = logging.getLogger(__name__)


class TextExtractor(Protocol):
    async def extract_text(self, content: bytes, filename: str) -> str: ...


class Vectorizer(Protocol):
    async def chunk_and_vectorize(self, text: str, filename: str) -> list[Chunk]: ...


def compare_files(s3_files: dict[str, FileInfo], store_files: dict[str, FileInfo]) -> SyncPlan:
    """Сравнивает содержимое S3 и Qdrant по именам и хешам файлов."""
    s3_names, store_names = set(s3_files), set(store_files)
    return SyncPlan(
        to_upload=sorted(s3_names - store_names),
        to_delete=sorted(store_names - s3_names),
        to_update=sorted(n for n in s3_names & store_names if s3_files[n].hash != store_files[n].hash),
    )


class SyncService:
    def __init__(
        self,
        s3: S3Repository,
        store: VectorStoreRepository,
        text_extractor: TextExtractor,
        vectorizer: Vectorizer,
        state: SyncState,
        max_concurrent_files: int,
    ) -> None:
        self._s3 = s3
        self._store = store
        self._text_extractor = text_extractor
        self._vectorizer = vectorizer
        self._state = state
        self._semaphore = asyncio.Semaphore(max_concurrent_files)
        self._background_tasks: set[asyncio.Task[None]] = set()

    def start_background_cycle(self) -> None:
        """Запускает цикл синхронизации в фоне. Если он уже идёт, бросает SyncInProgressError."""
        if self._state.in_progress:
            raise SyncInProgressError()
        task = asyncio.create_task(self.run_cycle())
        self._background_tasks.add(task)  # сильная ссылка, иначе задачу может собрать GC
        task.add_done_callback(self._background_tasks.discard)

    async def run_cycle(self) -> None:
        """Один цикл синхронизации. Ошибки логируются и не прерывают сервис."""
        if self._state.in_progress:
            logger.warning("Синхронизация уже выполняется, цикл пропущен")
            return
        async with self._state.lock:
            logger.info("Начало цикла синхронизации")
            try:
                s3_files = await self._s3.list_files()
                store_files = await self._store.list_files()
                plan = compare_files(s3_files, store_files)
                logger.info(
                    "План: загрузить %d, обновить %d, удалить %d",
                    len(plan.to_upload),
                    len(plan.to_update),
                    len(plan.to_delete),
                )

                for name in plan.to_delete:
                    await self._delete_file(name)

                await asyncio.gather(
                    *(self._process_file(n, s3_files[n], "update") for n in plan.to_update),
                    *(self._process_file(n, s3_files[n], "upload") for n in plan.to_upload),
                )
                self._state.last_sync_time = datetime.now(UTC).isoformat()
                logger.info("Синхронизация завершена")
            except Exception:
                logger.exception("Цикл синхронизации завершился ошибкой")
                self._state.errors_count += 1

    async def _delete_file(self, name: str) -> None:
        try:
            await self._store.delete_file(name)
        except Exception:
            logger.exception("Не удалось удалить файл %s из Qdrant", name)
            self._state.errors_count += 1

    async def _process_file(self, name: str, info: FileInfo, operation: str) -> None:
        async with self._semaphore:
            logger.info("Начало обработки файла %s (%s)", name, operation)
            try:
                content = await self._s3.read_file(name)
                text = await self._text_extractor.extract_text(content, name)
                if not text.strip():
                    raise EmptyTextError(f"из файла {name} не получено текста")
                chunks = await self._vectorizer.chunk_and_vectorize(text, name)
                if not chunks:
                    raise FileProcessingError(f"для файла {name} не получено ни одного чанка")
                await self._store.upsert_file(name, info, chunks)
                if operation == "update":
                    # Старая версия удаляется только после успешной записи новой
                    await self._store.delete_file(name, keep_hash=info.hash)
            except EmptyTextError as exc:
                logger.warning("Файл пропущен: %s", exc)
            except FileProcessingError as exc:
                logger.error("Не удалось обработать файл %s: %s", name, exc)
                self._state.errors_count += 1
            except Exception:
                logger.exception("Непредвиденная ошибка при обработке файла %s", name)
                self._state.errors_count += 1
            else:
                self._state.files_processed += 1
                logger.info("Файл %s обработан (%s)", name, operation)
