from __future__ import annotations

import logging
from typing import Optional


class DataPlatformError(Exception):
    """Base exception for the generic data platform package."""


class ConfigurationError(DataPlatformError):
    """Invalid or missing execution/configuration value."""


class DataValidationError(DataPlatformError):
    """Input data does not respect the expected contract."""


class MissingColumnsError(DataValidationError):
    """A dataframe is missing mandatory columns."""


class EmptyDataFrameError(DataValidationError):
    """A dataframe is unexpectedly empty."""


class SerializationError(DataPlatformError):
    """Serialization/deserialization failure."""


class ObjectStorageError(DataPlatformError):
    """Generic object storage failure."""


class ObjectNotFoundError(ObjectStorageError):
    """Object not found in storage."""


class ObjectDownloadError(ObjectStorageError):
    """Object download failure."""


class ObjectUploadError(ObjectStorageError):
    """Object upload failure."""


class SourceConnectionError(DataPlatformError):
    """Unable to reach a data source."""


class SourceResponseError(DataPlatformError):
    """Unexpected source API response."""


class PipelineExecutionError(DataPlatformError):
    """Pipeline orchestration failure."""


def get_logger(name: str, level: int = logging.INFO, *, fmt: Optional[str] = None) -> logging.Logger:
    """Return a package logger with a safe default handler.

    The function is intentionally small and local to avoid spreading logger setup
    across many modules.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(fmt or "%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return logger
