"""Модели файлов, чанков и плана синхронизации."""

from pydantic import BaseModel, Field


class FileInfo(BaseModel):
    """Описание файла в S3 или Qdrant. hash — ETag объекта, modified — время в формате ISO 8601."""

    hash: str
    modified: str = ""
    tags: dict[str, str] = Field(default_factory=dict)


class Chunk(BaseModel):
    embedding: list[float]
    chunk_text: str


class SyncPlan(BaseModel):
    to_upload: list[str] = Field(default_factory=list)
    to_update: list[str] = Field(default_factory=list)
    to_delete: list[str] = Field(default_factory=list)
