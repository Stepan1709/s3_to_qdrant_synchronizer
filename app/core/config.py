"""Конфигурация сервиса. Все параметры задаются переменными окружения или файлом .env."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # S3 (MinIO)
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket_name: str = "test-bucket"
    minio_secure: bool = False

    # Qdrant
    qdrant_host: str = "localhost"
    qdrant_port: int = Field(6333, ge=1, le=65535)
    qdrant_api_key: str = ""
    qdrant_collection_name: str = "test-collection"
    qdrant_vector_size: int = Field(1024, ge=1)
    qdrant_use_https: bool = False
    qdrant_upsert_batch_size: int = Field(100, ge=1)

    # Соседние сервисы (базовые URL)
    text_converter_url: str = "http://localhost:8999"
    chunk_n_vec_url: str = "http://localhost:8998"

    # Синхронизация
    service_host: str = "0.0.0.0"
    service_port: int = Field(8997, ge=1, le=65535)
    sync_interval_minutes: int = Field(30, ge=1)
    max_concurrent_files: int = Field(1, ge=1)
    chunk_max_size: int = Field(4000, ge=100)
    chunk_overlap: int = Field(500, ge=0)

    # Сеть
    request_timeout: float = Field(1200.0, gt=0)
    max_retries: int = Field(3, ge=1)
    health_check_interval: int = Field(150, ge=0)

    log_level: str = "INFO"

    @property
    def text_converter_base_url(self) -> str:
        return self.text_converter_url.rstrip("/")

    @property
    def chunk_n_vec_base_url(self) -> str:
        return self.chunk_n_vec_url.rstrip("/")

    @property
    def sync_interval_seconds(self) -> int:
        return self.sync_interval_minutes * 60


@lru_cache
def get_settings() -> Settings:
    return Settings()
