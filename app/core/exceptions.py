"""Доменные исключения и их преобразование в HTTP-ответы."""

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse


class AppError(Exception):
    """Базовое исключение сервиса."""

    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    detail = "Внутренняя ошибка сервиса"

    def __init__(self, detail: str | None = None) -> None:
        self.detail = detail or self.detail
        super().__init__(self.detail)


class SyncInProgressError(AppError):
    status_code = status.HTTP_409_CONFLICT
    detail = "Синхронизация уже выполняется"


class FileProcessingError(Exception):
    """Не удалось обработать конкретный файл. Не приводит к остановке цикла синхронизации."""


class TextExtractionError(FileProcessingError):
    """file_to_text_converter_server не смог извлечь текст."""


class EmptyTextError(FileProcessingError):
    """Из файла не удалось получить текст."""


class VectorizationError(FileProcessingError):
    """chunk_n_vec_server не смог разбить текст на чанки и векторизовать их."""


async def app_error_handler(_: Request, exc: Exception) -> JSONResponse:
    if not isinstance(exc, AppError):
        raise exc
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


def register_exception_handlers(app: FastAPI) -> None:
    app.add_exception_handler(AppError, app_error_handler)
