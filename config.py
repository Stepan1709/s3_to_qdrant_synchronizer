"""Конфигурация сервиса. Все параметры задаются переменными окружения."""
import os


def _bool(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# S3 (MinIO)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "test-bucket")
MINIO_SECURE = _bool("MINIO_SECURE")

# Qdrant
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY", "")
QDRANT_COLLECTION_NAME = os.getenv("QDRANT_COLLECTION_NAME", "test-collection")
QDRANT_VECTOR_SIZE = int(os.getenv("QDRANT_VECTOR_SIZE", "1024"))
QDRANT_USE_HTTPS = _bool("QDRANT_USE_HTTPS")

# Соседние сервисы (базовые URL)
TEXT_CONVERTER_URL = os.getenv("TEXT_CONVERTER_URL", "http://localhost:8999").rstrip("/")
CHUNK_N_VEC_URL = os.getenv("CHUNK_N_VEC_URL", "http://localhost:8998").rstrip("/")

# Синхронизация
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8997"))
SYNC_INTERVAL_SECONDS = int(os.getenv("SYNC_INTERVAL_MINUTES", "30")) * 60
MAX_CONCURRENT_FILES = int(os.getenv("MAX_CONCURRENT_FILES", "1"))
CHUNK_MAX_SIZE = int(os.getenv("CHUNK_MAX_SIZE", "4000"))
CHUNK_OVERLAP = int(os.getenv("CHUNK_OVERLAP", "500"))
QDRANT_UPSERT_BATCH_SIZE = 100

# Сеть
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "1200"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
HEALTH_CHECK_INTERVAL = int(os.getenv("HEALTH_CHECK_INTERVAL", "150"))

# Логирование
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
