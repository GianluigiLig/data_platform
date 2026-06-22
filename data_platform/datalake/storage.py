from __future__ import annotations

import gzip
import io
import json
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from data_platform.utils.diagnostics import (
    DataValidationError,
    EmptyDataFrameError,
    ObjectDownloadError,
    ObjectNotFoundError,
    ObjectStorageError,
    ObjectUploadError,
    SerializationError,
    get_logger,
)

logger = get_logger(__name__)


class ObjectStorage(ABC):
    """Minimal object storage contract used by the data lake."""

    @property
    @abstractmethod
    def scheme(self) -> str: ...

    def uri(self, bucket: str, key: str) -> str:
        return f"{self.scheme}://{bucket}/{key}"

    @abstractmethod
    def exists(self, bucket: str, key: str) -> bool: ...

    @abstractmethod
    def download_bytes(self, bucket: str, key: str) -> bytes: ...

    @abstractmethod
    def upload_bytes(self, bucket: str, key: str, data: bytes, content_type: Optional[str] = None) -> str: ...


class LocalObjectStorage(ObjectStorage):
    """Filesystem-backed storage useful for local tests and dry runs."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    @property
    def scheme(self) -> str:
        return "file"

    def _path(self, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    def uri(self, bucket: str, key: str) -> str:
        return str(self._path(bucket, key))

    def exists(self, bucket: str, key: str) -> bool:
        return self._path(bucket, key).exists()

    def download_bytes(self, bucket: str, key: str) -> bytes:
        path = self._path(bucket, key)
        if not path.exists():
            raise ObjectNotFoundError(f"Object not found: {path}")
        try:
            return path.read_bytes()
        except Exception as exc:
            raise ObjectDownloadError(f"Unable to download object: {path}") from exc

    def upload_bytes(self, bucket: str, key: str, data: bytes, content_type: Optional[str] = None) -> str:
        path = self._path(bucket, key)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            return str(path)
        except Exception as exc:
            raise ObjectUploadError(f"Unable to upload object: {path}") from exc


class GCSObjectStorage(ObjectStorage):
    """Google Cloud Storage implementation.

    The client is injected, so this module remains importable even when GCP
    libraries are not installed in a local development environment.
    """

    def __init__(self, *, client: Any) -> None:
        self.client = client

    @property
    def scheme(self) -> str:
        return "gs"

    def get_bucket(self, bucket_name: str):
        try:
            return self.client.bucket(bucket_name)
        except Exception as exc:
            raise ObjectStorageError("Unable to connect to GCS") from exc

    def exists(self, bucket: str, key: str) -> bool:
        try:
            return self.get_bucket(bucket).blob(key).exists()
        except Exception as exc:
            raise ObjectStorageError(f"Unable to check object existence: {self.uri(bucket, key)}") from exc

    def download_bytes(self, bucket: str, key: str) -> bytes:
        uri = self.uri(bucket, key)
        blob = self.get_bucket(bucket).blob(key)
        try:
            if not blob.exists():
                raise ObjectNotFoundError(f"Object not found: {uri}")
            return blob.download_as_bytes()
        except ObjectNotFoundError:
            raise
        except Exception as exc:
            raise ObjectDownloadError(f"Failed to download object: {uri}") from exc

    def upload_bytes(self, bucket: str, key: str, data: bytes, content_type: Optional[str] = None) -> str:
        uri = self.uri(bucket, key)
        try:
            self.get_bucket(bucket).blob(key).upload_from_string(data, content_type=content_type)
            return uri
        except Exception as exc:
            raise ObjectUploadError(f"Failed to upload object: {uri}") from exc


def _json_to_bytes(payload: dict[str, Any], *, indent: int | None = None) -> bytes:
    if payload is None:
        raise DataValidationError("Payload is None")
    try:
        return json.dumps(payload, ensure_ascii=False, indent=indent, sort_keys=False, default=str).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise SerializationError("Payload is not JSON serializable") from exc


def upload_json_gz(obj_storage_client: ObjectStorage, bucket: str, key: str, payload: dict, *, indent: int | None = None) -> str:
    return obj_storage_client.upload_bytes(bucket, key, gzip.compress(_json_to_bytes(payload, indent=indent)), "application/gzip")


def upload_json(obj_storage_client: ObjectStorage, bucket: str, key: str, payload: dict, *, indent: int | None = None) -> str:
    return obj_storage_client.upload_bytes(bucket, key, _json_to_bytes(payload, indent=indent), "application/json")


def read_json_gz(obj_storage_client: ObjectStorage, bucket: str, key: str) -> dict:
    uri = obj_storage_client.uri(bucket, key)
    if not obj_storage_client.exists(bucket, key):
        raise ObjectNotFoundError(f"File not found: {uri}")
    try:
        raw_bytes = gzip.decompress(obj_storage_client.download_bytes(bucket, key))
        return json.loads(raw_bytes.decode("utf-8"))
    except ObjectStorageError:
        raise
    except Exception as exc:
        raise SerializationError(f"Invalid gzip or JSON content: {uri}") from exc


def read_json(obj_storage_client: ObjectStorage, bucket: str, key: str) -> dict:
    uri = obj_storage_client.uri(bucket, key)
    if not obj_storage_client.exists(bucket, key):
        raise ObjectNotFoundError(f"File not found: {uri}")
    try:
        return json.loads(obj_storage_client.download_bytes(bucket, key).decode("utf-8"))
    except ObjectStorageError:
        raise
    except Exception as exc:
        raise SerializationError(f"Invalid JSON content: {uri}") from exc


def upload_parquet(obj_storage_client: ObjectStorage, bucket: str, key: str, df: pd.DataFrame) -> str:
    if df is None:
        raise DataValidationError("DataFrame is None")
    if df.empty:
        raise EmptyDataFrameError("DataFrame is empty")
    try:
        buf = io.BytesIO()
        df.to_parquet(buf, engine="pyarrow", index=False)
        return obj_storage_client.upload_bytes(bucket, key, buf.getvalue(), "application/octet-stream")
    except ObjectStorageError:
        raise
    except Exception as exc:
        raise SerializationError("Failed to serialize parquet") from exc


def read_parquet(obj_storage_client: ObjectStorage, bucket: str, key: str) -> pd.DataFrame:
    uri = obj_storage_client.uri(bucket, key)
    if not obj_storage_client.exists(bucket, key):
        raise ObjectNotFoundError(f"File not found: {uri}")
    try:
        return pd.read_parquet(io.BytesIO(obj_storage_client.download_bytes(bucket, key)), engine="pyarrow")
    except ObjectStorageError:
        raise
    except Exception as exc:
        raise SerializationError(f"Failed to read parquet: {uri}") from exc
