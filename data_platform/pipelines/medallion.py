from __future__ import annotations

from typing import Any

from data_platform.datalake.lake import DataLake
from data_platform.pipelines.adapters import BronzePayload, DataSource, GoldProcessor, PipelineContext, SourceAdapter
from data_platform.utils.diagnostics import DataPlatformError, PipelineExecutionError, get_logger

logger = get_logger(__name__)


class BronzePipeline:
    """Generic bronze pipeline: fetch raw payloads and store JSON gzip."""

    def __init__(self, lake: DataLake) -> None:
        self.lake = lake

    def run(self, *, context: PipelineContext, adapter: SourceAdapter, client: DataSource) -> dict[str, int]:
        requests = list(adapter.iter_bronze_requests(context.raw_config, context.time_range))
        stats = {"processed": 0, "uploaded": 0, "empty": 0, "failed": 0}
        logger.info("Bronze started asset=%s source=%s requests=%d", context.asset, adapter.source_name, len(requests))

        for request in requests:
            stats["processed"] += 1
            try:
                response = adapter.fetch_bronze(client, request, context.time_range)
                if not response:
                    stats["empty"] += 1
                    logger.warning("Empty bronze payload instrument=%s metadata=%s", request.instrument, request.metadata)
                    continue
                key = adapter.bronze_key(config=context.raw_config, request=request, end_datetime=context.end_datetime)
                self.lake.write_json_gz(key, response)
                stats["uploaded"] += 1
            except Exception as exc:
                stats["failed"] += 1
                logger.exception("Bronze failed instrument=%s metadata=%s", request.instrument, request.metadata)
                if isinstance(exc, DataPlatformError):
                    continue
                continue
        return stats


class SilverPipeline:
    """Generic silver pipeline: read bronze payloads, delegate normalization, write parquet."""

    def __init__(self, lake: DataLake) -> None:
        self.lake = lake

    def run(self, *, context: PipelineContext, adapter: SourceAdapter) -> dict[str, int]:
        instruments = adapter.available_silver_instruments(context.raw_config)
        stats = {"processed": 0, "uploaded": 0, "empty": 0, "failed": 0}
        logger.info("Silver started asset=%s source=%s instruments=%s", context.asset, adapter.source_name, instruments)

        for instrument in instruments:
            bronze_payloads: list[BronzePayload] = []
            for request in adapter.iter_silver_requests_for_instrument(context.raw_config, instrument, context.time_range):
                stats["processed"] += 1
                bronze_key = adapter.bronze_key(config=context.raw_config, request=request, end_datetime=context.end_datetime)
                try:
                    payload = self.lake.read_json_gz(bronze_key)
                    if not payload:
                        stats["empty"] += 1
                    bronze_payloads.append(BronzePayload(request=request, payload=payload))
                except Exception:
                    stats["failed"] += 1
                    logger.exception("Unable to read bronze input for silver: %s", bronze_key)
                    # Historical behavior compatibility:
                    # the old solar silver pipeline did not drop the device when
                    # a bronze file was missing or unreadable. It still passed an
                    # empty payload to the source transform, which produced the
                    # expected timestamp x device grid filled with missing values.
                    bronze_payloads.append(BronzePayload(request=request, payload=[]))
                    continue

            try:
                df_silver = adapter.transform_bronze_payloads_to_silver(
                    config=context.raw_config,
                    instrument=instrument,
                    bronze_payloads=bronze_payloads,
                    time_range=context.time_range,
                )
                if df_silver.empty:
                    stats["empty"] += 1
                    logger.warning("Silver output empty instrument=%s", instrument)
                    continue
                key = adapter.silver_key(config=context.raw_config, instrument=instrument, end_datetime=context.end_datetime)
                self.lake.write_parquet(key, df_silver)
                stats["uploaded"] += 1
            except Exception as exc:
                stats["failed"] += 1
                logger.exception("Silver transform/upload failed instrument=%s", instrument)
                raise PipelineExecutionError(f"Silver failed for instrument={instrument}") from exc
        return stats


class GoldPipeline:
    """Generic gold pipeline: read processor inputs, call processor, write parquet."""

    def __init__(self, lake: DataLake) -> None:
        self.lake = lake

    def run(
        self,
        *,
        context: PipelineContext,
        processor: GoldProcessor,
        append_if_exists: bool = True,
        deduplicate_on: str | list[str] | tuple[str, ...] | None = None,
        deduplicate_keep: str = "last",
    ) -> dict[str, int]:
        input_keys = processor.get_inputs(context)
        datasets = {name: self.lake.read_parquet(key) for name, key in input_keys.items()}
        output = processor.build(datasets, context)
        key = output.key or processor.output_key(context)

        df_to_write = output.dataframe
        if append_if_exists and self.lake.exists(key):
            import pandas as pd

            old = self.lake.read_parquet(key)
            df_to_write = pd.concat([old, df_to_write], ignore_index=True)

            if deduplicate_on is None:
                df_to_write = df_to_write.drop_duplicates()
            else:
                subset = [deduplicate_on] if isinstance(deduplicate_on, str) else list(deduplicate_on)
                existing_subset = [col for col in subset if col in df_to_write.columns]
                if existing_subset:
                    df_to_write = df_to_write.drop_duplicates(
                        subset=existing_subset,
                        keep=deduplicate_keep,
                    )
                    df_to_write = df_to_write.sort_values(existing_subset).reset_index(drop=True)
                else:
                    logger.warning(
                        "Gold deduplication columns not found: requested=%s available=%s. Falling back to full-row deduplication.",
                        subset,
                        list(df_to_write.columns),
                    )
                    df_to_write = df_to_write.drop_duplicates()

        self.lake.write_parquet(key, df_to_write)
        logger.info("Gold uploaded asset=%s source=%s key=%s rows=%d", context.asset, context.source, key, len(df_to_write))
        return {"inputs": len(input_keys), "uploaded": 1, "rows": len(df_to_write)}
