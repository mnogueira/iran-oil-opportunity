"""Market data utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


def rates_to_frame(rates: Any) -> pd.DataFrame:
    """Convert MT5 numpy structured rates into a pandas DataFrame."""

    if rates is None:
        return pd.DataFrame()
    frame = pd.DataFrame(rates)
    if frame.empty:
        return frame
    frame["time"] = pd.to_datetime(frame["time"], unit="s", utc=True)
    frame = frame.rename(columns={"time": "timestamp"}).set_index("timestamp").sort_index()
    return frame


def load_price_frame(path: str | Path) -> pd.DataFrame:
    """Load a CSV with `date` or `timestamp` plus price columns."""

    frame = pd.read_csv(path)
    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.set_index("timestamp")
    elif "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], utc=True)
        frame = frame.set_index("date")
    else:
        raise ValueError("Expected either a `date` or `timestamp` column.")
    return frame.sort_index()


def write_frame(frame: pd.DataFrame, path: str | Path) -> Path:
    """Persist a market-data frame to CSV."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target)
    return target


def join_spread_context(primary: pd.DataFrame, secondary: pd.DataFrame) -> pd.DataFrame:
    """Join a secondary symbol for spread-aware research."""

    if secondary.empty:
        return primary.copy()
    joined = primary.copy()
    joined["secondary_close"] = secondary["close"].reindex(joined.index).ffill()
    joined["spread"] = joined["close"] - joined["secondary_close"]
    return joined
