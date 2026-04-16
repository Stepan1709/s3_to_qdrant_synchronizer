# S3 to Qdrant Sync Service

Микросервис для синхронизации файлов из S3 хранилища (MinIO) с Qdrant векторной базой данных.

## Функциональность

- Автоматическая синхронизация файлов между S3 и Qdrant
- Поддержка добавления, удаления и обновления файлов
- Извлечение текста из файлов через сервис file_to_text_converter
- Чанкинг и векторизация через сервис chunk_n_vec
- Асинхронная обработка файлов
- Health check endpoints
- Подробное логирование

## Установка и запуск

### Через Docker

```bash
# Сборка образа
docker build -t s3-qdrant-sync .

# Запуск контейнера
docker run -d \
  --name s3-qdrant-sync \
  -p 8997:8997 \
  -e MINIO_ENDPOINT="192.168.28.246:8995" \
  -e MINIO_ACCESS_KEY="minio_root" \
  -e MINIO_SECRET_KEY="api_key_for_minio_rag" \
  -e QDRANT_HOST="localhost" \
  -e QDRANT_PORT=6333 \
  -e QDRANT_API_KEY="api_key_for_qdrant_rag" \
  -e TEXT_CONVERTER_URL="http://192.168.28.246:8999/convert" \
  -e CHUNK_N_VEC_URL="http://192.168.28.246:8998/process" \
  s3-qdrant-sync
```

API Endpoints:
```
GET / - Информация о сервисе

GET /health - Проверка здоровья сервиса и зависимостей

GET /status - Операционный статус сервиса

POST /sync - Ручной запуск синхронизации
```
### Конфигурация
Настройки в config.py (можно указать в secrets.py) или через переменные окружения:
```
MINIO_ENDPOINT - Endpoint MinIO сервера

MINIO_ACCESS_KEY - Access key для MinIO

MINIO_SECRET_KEY - Secret key для MinIO

MINIO_BUCKET_NAME - Имя бакета (по умолчанию: test-bucket)

QDRANT_HOST - Хост Qdrant

QDRANT_PORT - Порт Qdrant

QDRANT_API_KEY - API ключ Qdrant

QDRANT_COLLECTION_NAME - Имя коллекции (по умолчанию: test-collection)

SYNC_INTERVAL_MINUTES - Интервал синхронизации в минутах (по умолчанию: 30)
```
### Логирование
Сервис выводит подробные логи:

Количество файлов в S3 и Qdrant

Количество файлов на загрузку, удаление и обновление

Название обрабатываемого файла

Ошибки при обработке


## Сборка и запуск

Для сборки и запуска сервиса выполните:

```bash
# Клонируйте репозиторий с файлами
git clone <this-repo-url>
cd <repo-directory>

# Соберите Docker образ
docker build -t s3-qdrant-sync .

# Запустите контейнер
docker run -d \
  --name s3-qdrant-sync \
  -p 8997:8997 \
  s3-qdrant-sync