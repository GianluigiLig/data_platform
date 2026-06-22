from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

import pandas as pd

from data_platform.datalake.storage import (
    ObjectStorage,
    read_json,
    read_json_gz,
    read_parquet,
    upload_json,
    upload_json_gz,
    upload_parquet,
)
from data_platform.utils.diagnostics import ConfigurationError


DEFAULT_LAKE_ROOT = "data-platform"


class MedallionLayer(str, Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass(frozen=True)
class DatasetRef:
    layer: MedallionLayer | str
    source: str
    asset: str
    dataset: str
    dt: datetime
    filename: str
    root: str = DEFAULT_LAKE_ROOT

    @property
    def layer_value(self) -> str:
        return self.layer.value if isinstance(self.layer, MedallionLayer) else str(self.layer).lower()


def build_medallion_prefix(
    *,
    root: str = DEFAULT_LAKE_ROOT,
    source: str,
    layer: MedallionLayer | str,
    asset: str | None = None,
    dt: datetime | None = None,
    dataset: str | None = None,
    asset_partition_name: str = "plant",
) -> str:
    """Build a generic medallion path prefix.

    The naming uses ``asset`` instead of ``plant`` to keep the data platform
    independent from the solar domain.
    """
    if not source:
        raise ConfigurationError("source is required")
    if dt is None:
        raise ConfigurationError("dt is required")

    layer_value = layer.value if isinstance(layer, MedallionLayer) else str(layer).lower()
    parts = [root.rstrip("/"), f"source={source}", layer_value]
    if asset:
        # ``asset`` resta il concetto generico del data platform.
        # Il nome della partizione è configurabile: per compatibilità con
        # lo storico GCS del progetto usiamo ancora ``plant=...``.
        parts.append(f"{asset_partition_name}={asset}")
    if dataset:
        parts.append(f"dataset={dataset}")
    if layer_value == MedallionLayer.GOLD.value:
        parts.extend([f"year={dt.year:04d}", f"month={dt.month:02d}"])
    else:
        parts.extend([f"year={dt.year:04d}", f"month={dt.month:02d}", f"day={dt.day:02d}"])
    return "/".join(parts) + "/"


@dataclass(frozen=True)
class BasePathsConfig:
    """Common path builder for source adapters."""

    source_name: str
    root: str = DEFAULT_LAKE_ROOT
    silver_filename_template: str = "{instrument}.parquet"
    enriched_silver_data_filename: str = "enriched_silver_data.parquet"
    kpi_calculation_filename: str = "plant-kpi-calculation-table.parquet"

    asset_partition_name: str = "plant"

    def bronze_prefix(self, *, plant: str, dt: datetime) -> str:
        return build_medallion_prefix(
            root=self.root,
            source=self.source_name,
            layer=MedallionLayer.BRONZE,
            asset=plant,
            dt=dt,
            asset_partition_name=self.asset_partition_name,
        )

    def silver_prefix(self, *, plant: str, dt: datetime) -> str:
        return build_medallion_prefix(
            root=self.root,
            source=self.source_name,
            layer=MedallionLayer.SILVER,
            asset=plant,
            dt=dt,
            asset_partition_name=self.asset_partition_name,
        )

    def gold_prefix(self, *, plant: str, dt: datetime) -> str:
        return build_medallion_prefix(
            root=self.root,
            source=self.source_name,
            layer=MedallionLayer.GOLD,
            asset=plant,
            dt=dt,
            asset_partition_name=self.asset_partition_name,
        )

    def silver_data_key(self, *, plant: str, dt: datetime, instrument: str) -> str:
        return self.silver_prefix(plant=plant, dt=dt) + self.silver_filename_template.format(instrument=instrument)

    def enriched_silver_data_key(self, *, plant: str, dt: datetime) -> str:
        return self.gold_prefix(plant=plant, dt=dt) + self.enriched_silver_data_filename

    def kpi_calculation_key(self, *, plant: str, dt: datetime) -> str:
        return self.gold_prefix(plant=plant, dt=dt) + self.kpi_calculation_filename


class DataLake:
    """Small facade around object storage used by generic pipelines."""

    def __init__(self, storage: ObjectStorage, bucket: str) -> None:
        self.storage = storage
        self.bucket = bucket

    def exists(self, key: str) -> bool:
        return self.storage.exists(self.bucket, key)

    def read_json_gz(self, key: str) -> dict:
        return read_json_gz(self.storage, self.bucket, key)

    def write_json_gz(self, key: str, payload: dict, *, indent: int | None = None) -> str:
        return upload_json_gz(self.storage, self.bucket, key, payload, indent=indent)

    def read_json(self, key: str) -> dict:
        return read_json(self.storage, self.bucket, key)

    def write_json(self, key: str, payload: dict, *, indent: int | None = None) -> str:
        return upload_json(self.storage, self.bucket, key, payload, indent=indent)

    def read_parquet(self, key: str) -> pd.DataFrame:
        return read_parquet(self.storage, self.bucket, key)

    def write_parquet(self, key: str, df: pd.DataFrame) -> str:
        return upload_parquet(self.storage, self.bucket, key, df)
