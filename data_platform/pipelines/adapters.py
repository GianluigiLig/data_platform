from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Protocol

import pandas as pd
import requests

from data_platform.datalake.lake import DatasetRef
from data_platform.utils.diagnostics import SourceConnectionError


@dataclass(frozen=True)
class PipelineContext:
    """Generic execution context passed to medallion pipelines."""

    asset: str
    source: str
    time_range: tuple
    config: Any
    raw_config: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def start_datetime(self):
        return self.time_range[0]

    @property
    def end_datetime(self):
        return self.time_range[1]

    @property
    def year(self) -> int:
        return int(self.time_range[2])

    @property
    def month(self) -> int:
        return int(self.time_range[3])

    @property
    def day(self) -> int:
        return int(self.time_range[4])


@dataclass(frozen=True)
class BronzeRequest:
    instrument: str
    fetch_parameters: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class BronzePayload:
    request: BronzeRequest
    payload: Any


@dataclass(frozen=True)
class DatasetPayload:
    name: str
    dataframe: pd.DataFrame
    key: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class DataSource(ABC):
    """Abstract API client: fetch raw payloads, never transform them."""

    auth = None
    verify = True

    def __init__(self, connection_config: dict[str, Any]) -> None:
        self.config = connection_config
        self.source_type = self.__class__.__name__

    def api_get_request(self, endpoint: str, params: dict[str, Any] | None = None) -> requests.Response:
        url = f"{self.base_url}{endpoint}"
        try:
            response = requests.get(
                url,
                headers=getattr(self, "headers", None),
                params=params,
                auth=getattr(self, "auth", None),
                timeout=getattr(self, "timeout", None),
                verify=getattr(self, "verify", True),
            )
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as exc:
            raise SourceConnectionError(f"GET {url} failed with params={params}") from exc

    @abstractmethod
    def fetch_data(self, start_timestamp: datetime, end_timestamp: datetime, parameters: dict[str, Any] | None = None) -> Any: ...

    @abstractmethod
    def get_source_name(self) -> str: ...


class BaseSourceAdapter:
    """Common source adapter behavior used by generic pipelines."""

    source_name: str
    paths: Any

    def fetch_bronze(self, client: DataSource, request: BronzeRequest, time_range: tuple) -> Any:
        start_datetime, end_datetime, *_ = time_range
        return client.fetch_data(start_timestamp=start_datetime, end_timestamp=end_datetime, parameters=request.fetch_parameters)

    def silver_key(self, *, config: dict, instrument: str, end_datetime) -> str:
        return self.paths.silver_data_key(plant=config["metadata"]["plant_name"], dt=end_datetime, instrument=instrument)

    def enriched_silver_data_key(self, *, config: dict, end_datetime) -> str:
        return self.paths.enriched_silver_data_key(plant=config["metadata"]["plant_name"], dt=end_datetime)

    def kpi_calculation_key(self, *, config: dict, end_datetime) -> str:
        return self.paths.kpi_calculation_key(plant=config["metadata"]["plant_name"], dt=end_datetime)


class SourceAdapter(Protocol):
    source_name: str

    def iter_bronze_requests(self, config: dict, time_range: tuple) -> Iterable[BronzeRequest]: ...

    def fetch_bronze(self, client: DataSource, request: BronzeRequest, time_range: tuple) -> Any: ...

    def bronze_key(self, *, config: dict, request: BronzeRequest, end_datetime) -> str: ...

    def available_silver_instruments(self, config: dict) -> list[str]: ...

    def iter_silver_requests_for_instrument(self, config: dict, instrument: str, time_range: tuple) -> Iterable[BronzeRequest]: ...

    def silver_key(self, *, config: dict, instrument: str, end_datetime) -> str: ...

    def transform_bronze_payloads_to_silver(
        self,
        *,
        config: dict,
        instrument: str,
        bronze_payloads: list[BronzePayload],
        time_range: tuple,
    ) -> pd.DataFrame: ...


class GoldProcessor(Protocol):
    """Domain-specific gold logic injected into the generic GoldPipeline."""

    def get_inputs(self, context: PipelineContext) -> dict[str, str]: ...

    def build(self, datasets: dict[str, pd.DataFrame], context: PipelineContext) -> DatasetPayload: ...

    def output_key(self, context: PipelineContext) -> str: ...
