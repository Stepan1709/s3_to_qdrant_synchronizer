# s3_to_qdrant_synchronizer

Главный сервис RAG-пайплайна: периодически синхронизирует файлы из S3-хранилища (MinIO) с векторной базой
[Qdrant](https://qdrant.tech/). Новые и изменённые файлы превращаются в текст, режутся на чанки, векторизуются
и записываются в Qdrant; файлы, удалённые из S3, удаляются и из Qdrant.

## Архитектура

Проект состоит из четырёх контейнеризованных сервисов, которые обращаются друг к другу по HTTP:

```
                                         ┌──> file_to_text_converter_server ──┬──> Docling Serve
S3 (MinIO) ──> s3_to_qdrant_synchronizer ┤                                    ├──> PaddleOCR-VL_pdf_ocr_server ──> vLLM
               (этот сервис)             │                                    └──> vLLM (PaddleOCR-VL)
                                         ├──> chunk_n_vec_server ──> сервис эмбеддингов (vLLM, BAAI/bge-m3)
                                         └──> Qdrant
```

| Сервис                        | Репозиторий                                                                                          | Порт | Назначение                       |
|-------------------------------|------------------------------------------------------------------------------------------------------|------|----------------------------------|
| s3_to_qdrant_synchronizer     | этот                                                                                                 | 8997 | Синхронизация S3 → Qdrant        |
| file_to_text_converter_server | [Stepan1709/file_to_text_converter_server](https://github.com/Stepan1709/file_to_text_converter_server) | 8999 | Извлечение текста из документов  |
| PaddleOCR-VL_pdf_ocr_server   | [Stepan1709/PaddleOCR-VL_pdf_ocr_server](https://github.com/Stepan1709/PaddleOCR-VL_pdf_ocr_server)   | 9000 | OCR PDF-сканов                   |
| chunk_n_vec_server            | [Stepan1709/chunk_n_vec_server](https://github.com/Stepan1709/chunk_n_vec_server)                     | 8998 | Чанкинг и векторизация           |

Кроме них нужны внешние сервисы: MinIO, Qdrant, Docling Serve и vLLM (с моделями PaddleOCR-VL и эмбеддингов).

## Как работает синхронизация

Раз в `SYNC_INTERVAL_MINUTES` минут (и по `POST /sync`):

1. Из S3 читается список объектов с ETag (используется как хеш содержимого) и тегами.
2. Из Qdrant вычитывается список файлов по метаданным `metadata.file_name` / `metadata.file_hash`.
3. Списки сравниваются: **новые** (есть в S3, нет в Qdrant), **удалённые** (есть в Qdrant, нет в S3),
   **изменённые** (ETag отличается).
4. Для удалённых и изменённых файлов точки удаляются из Qdrant.
5. Для новых и изменённых файлов: чтение из S3 → `file_to_text_converter_server` (`POST /convert`) →
   `chunk_n_vec_server` (`POST /process`) → запись точек в Qdrant.

Если обработка файла не удалась, он остаётся без точек в Qdrant и будет обработан заново в следующем цикле.
Файлы, из которых не удалось извлечь текст, пропускаются (и пробуются снова в каждом цикле).

### Формат точек в Qdrant

```json
{
  "vector": [0.01, "..."],
  "payload": {
    "content": "текст чанка",
    "metadata": {
      "file_name": "folder/document.pdf",
      "file_hash": "<etag>",
      "file_modified": "2026-01-01T12:00:00+00:00",
      "file_tags": {"tag": "value"},
      "chunk_position": 1,
      "chunk_index": 0,
      "total_chunks": 12
    }
  }
}
```

Коллекция (косинусная метрика, размерность `QDRANT_VECTOR_SIZE`) и бакет S3 создаются автоматически, если их нет.

## Конфигурация

Все параметры задаются переменными окружения (шаблон — `.env.example`). Файл `.env` с ключами в git не попадает.

| Переменная               | По умолчанию            | Описание                                                              |
|--------------------------|-------------------------|-----------------------------------------------------------------------|
| `MINIO_ENDPOINT`         | `localhost:9000`        | Адрес MinIO (`host:port`)                                             |
| `MINIO_ACCESS_KEY`       | пусто                   | Access key MinIO                                                      |
| `MINIO_SECRET_KEY`       | пусто                   | Secret key MinIO                                                      |
| `MINIO_BUCKET_NAME`      | `test-bucket`           | Бакет                                                                 |
| `MINIO_SECURE`           | `false`                 | HTTPS для MinIO                                                       |
| `QDRANT_HOST`            | `localhost`             | Хост Qdrant                                                           |
| `QDRANT_PORT`            | `6333`                  | Порт Qdrant                                                           |
| `QDRANT_API_KEY`         | пусто                   | API-ключ Qdrant                                                       |
| `QDRANT_COLLECTION_NAME` | `test-collection`       | Коллекция                                                             |
| `QDRANT_VECTOR_SIZE`     | `1024`                  | Размерность векторов (для `BAAI/bge-m3` — 1024)                       |
| `QDRANT_USE_HTTPS`       | `false`                 | HTTPS для Qdrant                                                      |
| `TEXT_CONVERTER_URL`     | `http://localhost:8999` | Базовый URL file_to_text_converter_server (без `/convert`)            |
| `CHUNK_N_VEC_URL`        | `http://localhost:8998` | Базовый URL chunk_n_vec_server (без `/process`)                       |
| `SYNC_INTERVAL_MINUTES`  | `30`                    | Интервал синхронизации                                                |
| `MAX_CONCURRENT_FILES`   | `1`                     | Сколько файлов обрабатывать одновременно                              |
| `CHUNK_MAX_SIZE`         | `4000`                  | Размер чанка, символов                                                |
| `CHUNK_OVERLAP`          | `500`                   | Перекрытие чанков, символов                                           |
| `REQUEST_TIMEOUT`        | `1200`                  | Таймаут обработки файла соседними сервисами, сек                      |
| `MAX_RETRIES`            | `3`                     | Повторы при сетевых ошибках                                           |
| `HEALTH_CHECK_INTERVAL`  | `150`                   | Как часто `/health` реально опрашивает зависимости, сек               |
| `SERVICE_PORT`           | `8997`                  | Порт сервиса (в Docker-образе проброшен `8997`)                       |
| `LOG_LEVEL`              | `INFO`                  | Уровень логирования                                                   |

> `TEXT_CONVERTER_URL` и `CHUNK_N_VEC_URL` задаются **базовыми** URL. Раньше в них указывались полные пути
> (`.../convert`, `.../process`) — при обновлении старой конфигурации уберите путь.

## Запуск

### Docker Compose

```bash
git clone https://github.com/Stepan1709/s3_to_qdrant_synchronizer
cd s3_to_qdrant_synchronizer
cp .env.example .env      # заполните доступы к MinIO и Qdrant, адреса соседних сервисов
docker compose up -d --build
docker compose logs -f
```

### Docker

```bash
docker build -t s3-qdrant-sync .
docker run -d --name s3-qdrant-sync -p 8997:8997 --env-file .env --restart unless-stopped s3-qdrant-sync
```

### Локально

```bash
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
set -a; source .env; set +a                         # Windows PowerShell: задайте переменные через $env:NAME="..."
python sync_server.py
```

## API

| Метод  | Путь      | Описание                                                                                          |
|--------|-----------|---------------------------------------------------------------------------------------------------|
| `GET`  | `/`       | Информация о сервисе                                                                              |
| `GET`  | `/live`   | Liveness-проба (используется в `HEALTHCHECK`, зависимости не проверяет)                           |
| `GET`  | `/health` | Состояние MinIO, Qdrant и соседних сервисов; `503`, если что-то недоступно (результат кешируется) |
| `GET`  | `/status` | Время последней синхронизации, счётчики обработанных файлов и ошибок                              |
| `POST` | `/sync`   | Запустить синхронизацию вручную (`409`, если она уже идёт)                                        |
| `GET`  | `/docs`   | Swagger UI                                                                                        |

## Логирование

В логи пишутся: количество файлов в S3 и Qdrant, количество файлов на загрузку / обновление / удаление,
имя обрабатываемого файла, время работы соседних сервисов и ошибки обработки.

```bash
docker compose logs -f --tail 100
```
