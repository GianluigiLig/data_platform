from __future__ import annotations

from typing import Any

import pandas as pd


def build_dataframe_quality_report(df: pd.DataFrame, *, name: str | None = None, metadata: dict[str, Any] | None = None) -> dict[str, Any]:
    """Generic dataframe quality report used before/after medallion stages."""
    if df is None:
        return {"name": name, "is_none": True, "metadata": metadata or {}}

    missing_count = df.isna().sum().to_dict()
    rows = int(len(df))
    return {
        "name": name,
        "metadata": metadata or {},
        "rows": rows,
        "columns": list(df.columns),
        "column_count": int(len(df.columns)),
        "missing_count": {k: int(v) for k, v in missing_count.items()},
        "missing_ratio": {k: (float(v) / rows if rows else None) for k, v in missing_count.items()},
        "empty_columns": [k for k, v in missing_count.items() if rows and int(v) == rows],
        "duplicated_rows": int(df.duplicated().sum()) if rows else 0,
    }


def build_pre_gold_nan_quality_report(
    df: pd.DataFrame,
    *,
    plant_name: str,
    source: str,
    has_meter: bool,
    time_interval_hours: float,
) -> dict[str, Any]:
    """Compatibility wrapper for the existing solar pre-gold NaN report."""
    return build_dataframe_quality_report(
        df,
        name="pre_gold_nan_quality_report",
        metadata={
            "plant_name": plant_name,
            "source": source,
            "has_meter": has_meter,
            "time_interval_hours": time_interval_hours,
        },
    )
