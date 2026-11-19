from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from minio.error import S3Error

from app.repositories.s3 import S3Repository


def make_object(name: str, etag: str) -> SimpleNamespace:
    return SimpleNamespace(object_name=name, etag=etag, last_modified=datetime(2026, 1, 1, tzinfo=UTC))


async def test_list_files_collects_hash_modified_and_tags() -> None:
    client = MagicMock()
    client.list_objects.return_value = [make_object("a.pdf", '"abc"'), make_object("b.docx", "def")]
    client.get_object_tags.side_effect = [
        {"project": "x"},
        S3Error("NoSuchTagSet", "no tags", "r", "id", "host", MagicMock()),
    ]

    files = await S3Repository(client, "bucket").list_files()

    assert files["a.pdf"].hash == "abc"
    assert files["a.pdf"].modified == "2026-01-01T00:00:00+00:00"
    assert files["a.pdf"].tags == {"project": "x"}
    assert files["b.docx"].tags == {}


async def test_read_file_closes_connection() -> None:
    response = MagicMock()
    response.read.return_value = b"content"
    client = MagicMock()
    client.get_object.return_value = response

    data = await S3Repository(client, "bucket").read_file("a.pdf")

    assert data == b"content"
    response.close.assert_called_once()
    response.release_conn.assert_called_once()


async def test_ensure_bucket_creates_missing_bucket() -> None:
    client = MagicMock()
    client.bucket_exists.return_value = False

    await S3Repository(client, "bucket").ensure_bucket()

    client.make_bucket.assert_called_once_with("bucket")
