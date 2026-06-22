from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable

import pandas as pd
from dateutil.tz import tzutc

from .diagnostics import DataValidationError


@dataclass(frozen=True)
class TimeWindow:
    """UTC window corresponding to one local business day."""

    start_utc: pd.Timestamp
    end_utc: pd.Timestamp
    target_date: date
    year: int
    month: int
    day: int

    def as_tuple(self) -> tuple[pd.Timestamp, pd.Timestamp, int, int, int]:
        return self.start_utc, self.end_utc, self.year, self.month, self.day


def build_day_window_utc(
    dt: datetime | date | str,
    tz_local: str = "Europe/Rome",
    tz_utc: str = "UTC",
    inclusive_end: bool = True,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Build start/end UTC timestamps for the local day containing ``dt``.

    This preserves the existing behavior: the input date is interpreted as a
    local day, then converted to UTC. DST transitions are handled by pandas
    timezone conversion.
    """
    d = pd.Timestamp(dt).date()
    start_local = pd.Timestamp(d).tz_localize(tz_local)
    end_local_excl = (pd.Timestamp(d) + pd.Timedelta(days=1)).tz_localize(tz_local)
    end_local = end_local_excl - pd.Timedelta(seconds=1) if inclusive_end else end_local_excl
    return start_local.tz_convert(tz_utc), end_local.tz_convert(tz_utc)


def get_target_date(days_back: int = 1) -> datetime:
    """Default ingestion target date: now UTC minus one day."""
    return datetime.now(tz=tzutc()) - timedelta(days=days_back)


def build_time_window(
    execution_date: datetime | date | str | None = None,
    *,
    tz_local: str = "Europe/Rome",
    inclusive_end: bool = True,
) -> TimeWindow:
    target = pd.Timestamp(execution_date).to_pydatetime() if execution_date else get_target_date()
    start, end = build_day_window_utc(target, tz_local=tz_local, inclusive_end=inclusive_end)
    d = pd.Timestamp(target).date()
    return TimeWindow(start_utc=start, end_utc=end, target_date=d, year=d.year, month=d.month, day=d.day)


def time_range(
    execution_date: datetime | date | str | None = None,
    *,
    tz_local: str = "Europe/Rome",
) -> tuple[pd.Timestamp, pd.Timestamp, int, int, int]:
    """Compatibility helper returning the old tuple shape."""
    return build_time_window(execution_date, tz_local=tz_local).as_tuple()


def split_utc_range(start_iso_z: str, end_iso_z: str, max_hours: int = 24) -> list[tuple[str, str]]:
    """Split a UTC interval into chunks with a maximum duration."""
    if max_hours <= 0:
        raise DataValidationError("max_hours must be > 0")
    start = pd.to_datetime(start_iso_z, utc=True)
    end = pd.to_datetime(end_iso_z, utc=True)
    if end < start:
        raise DataValidationError("end_timestamp must be >= start_timestamp")
    chunks: list[tuple[str, str]] = []
    cur = start
    step = pd.Timedelta(hours=max_hours)
    while cur < end:
        nxt = min(cur + step, end)
        chunks.append((cur.strftime("%Y-%m-%dT%H:%M:%SZ"), nxt.strftime("%Y-%m-%dT%H:%M:%SZ")))
        cur = nxt
    return chunks
