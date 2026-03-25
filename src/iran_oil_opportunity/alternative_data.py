"""Helpers for merging alternative event data into price bars."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd


def load_alt_data_frame(path: str | Path) -> pd.DataFrame:
    """Load an alternative-data CSV keyed by `timestamp` or `date`."""

    frame = pd.read_csv(path)
    if "timestamp" in frame.columns:
        frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
        frame = frame.set_index("timestamp")
    elif "date" in frame.columns:
        frame["date"] = pd.to_datetime(frame["date"], utc=True)
        frame = frame.set_index("date")
    else:
        raise ValueError("Expected a `timestamp` or `date` column in the alternative-data file.")
    return frame.sort_index()


def merge_alt_data(
    price_frame: pd.DataFrame,
    alt_frame: pd.DataFrame,
    *,
    tolerance: str = "12h",
) -> pd.DataFrame:
    """Backward-merge event data into price bars."""

    if alt_frame.empty:
        return price_frame.copy()
    price = price_frame.sort_index().reset_index().rename(columns={price_frame.index.name or "index": "timestamp"})
    alt = alt_frame.sort_index().reset_index().rename(columns={alt_frame.index.name or "index": "timestamp"})
    merged = pd.merge_asof(
        price,
        alt,
        on="timestamp",
        direction="backward",
        tolerance=pd.Timedelta(tolerance),
    )
    return merged.set_index("timestamp")


def combine_alt_data_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Outer-join multiple alternative-data frames on timestamp."""

    usable = [frame.sort_index() for frame in frames if frame is not None and not frame.empty]
    if not usable:
        return pd.DataFrame()
    combined = usable[0]
    for frame in usable[1:]:
        combined = combined.join(frame, how="outer")
    return combined.sort_index()


def split_alt_data_paths(paths: str | Path | Iterable[str | Path] | None) -> list[Path]:
    """Normalize one or many CSV path inputs into a flat path list."""

    if paths is None:
        return []
    raw_items = [paths] if isinstance(paths, (str, Path)) else list(paths)
    normalized: list[Path] = []
    for item in raw_items:
        for chunk in str(item).split(","):
            candidate = chunk.strip()
            if candidate:
                normalized.append(Path(candidate))
    return normalized


def load_combined_alt_data(paths: str | Path | Iterable[str | Path] | None) -> pd.DataFrame:
    """Load and outer-join any existing alternative-data CSVs."""

    frames = [load_alt_data_frame(path) for path in split_alt_data_paths(paths) if Path(path).exists()]
    return combine_alt_data_frames(frames)


def merge_alt_data_paths(
    price_frame: pd.DataFrame,
    paths: str | Path | Iterable[str | Path] | None,
    *,
    tolerance: str = "12h",
) -> pd.DataFrame:
    """Merge one or many alternative-data CSVs into the price frame."""

    combined = load_combined_alt_data(paths)
    return merge_alt_data(price_frame, combined, tolerance=tolerance)


def write_alt_data(frame: pd.DataFrame, path: str | Path) -> Path:
    """Persist an alternative-data frame to CSV."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target)
    return target
