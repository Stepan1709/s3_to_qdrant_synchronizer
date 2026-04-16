import os
from datetime import timedelta

# Try to import secrets, if not available, use environment variables or defaults
try:
    from secrets import *
except ImportError:
    # S3 configuration
    MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', '')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', '')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', '')
    MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME', 'test-bucket')
    MINIO_SECURE = os.getenv('MINIO_SECURE', 'False').lower() == 'true'

    # Qdrant configuration
    QDRANT_HOST = os.getenv('QDRANT_HOST', 'localhost')
    QDRANT_PORT = int(os.getenv('QDRANT_PORT', '6333'))
    QDRANT_API_KEY = os.getenv('QDRANT_API_KEY', '')
    QDRANT_COLLECTION_NAME = os.getenv('QDRANT_COLLECTION_NAME', 'test-collection')
    QDRANT_VECTOR_SIZE = int(os.getenv('QDRANT_VECTOR_SIZE', '1024'))
    QDRANT_USE_HTTPS = os.getenv('QDRANT_USE_HTTPS', 'False').lower() == 'true'

    # Service URLs
    TEXT_CONVERTER_URL = os.getenv('TEXT_CONVERTER_URL', '')
    CHUNK_N_VEC_URL = os.getenv('CHUNK_N_VEC_URL', '')

    # Service configuration
    SERVICE_PORT = int(os.getenv('SERVICE_PORT', '8997'))
    SYNC_INTERVAL_MINUTES = int(os.getenv('SYNC_INTERVAL_MINUTES', '30'))

# Calculate sync interval in seconds
SYNC_INTERVAL_SECONDS = SYNC_INTERVAL_MINUTES * 60

# Request timeout settings
REQUEST_TIMEOUT = 1200  # 20 minutes timeout for file processing
CONNECTION_TIMEOUT = 30  # 30 seconds for connection

# Logging configuration
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

# Retry configuration
MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 5