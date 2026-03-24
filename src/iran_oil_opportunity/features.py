"""Feature engineering for oil-shock trading."""

from __future__ import annotations

import numpy as np
import pandas as pd

from iran_oil_opportunity.config import StrategyConfig


def rolling_zscore(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window).mean()
    std = series.rolling(window).std(ddof=0).replace(0.0, np.nan)
    return (series - mean) / std


def build_feature_frame(
    frame: pd.DataFrame,
    *,
    strategy_config: StrategyConfig | None = None,
) -> pd.DataFrame:
    """Build a strategy-ready feature frame from market bars."""

    cfg = strategy_config or StrategyConfig()
    result = frame.copy().sort_index()
    if "close" not in result.columns:
        raise ValueError("Price frame must contain a `close` column.")

    if "high" not in result.columns:
        result["high"] = result["close"]
    if "low" not in result.columns:
        result["low"] = result["close"]
    if "tick_volume" not in result.columns:
        result["tick_volume"] = 0.0

    result["return_1"] = result["close"].pct_change()
    result["return_3"] = result["close"].pct_change(3)
    result["return_5"] = result["close"].pct_change(5)

    true_range = (result["high"] - result["low"]) / result["close"]
    true_range = true_range.where(true_range > 0.0, result["return_1"].abs())
    result["atr_pct"] = true_range.rolling(cfg.volatility_window).mean()
    result["realized_vol"] = result["return_1"].rolling(cfg.volatility_window).std(ddof=0) * np.sqrt(
        cfg.volatility_window
    )
    result["price_zscore"] = rolling_zscore(result["close"], cfg.zscore_window)
    result["vol_zscore"] = rolling_zscore(result["realized_vol"], cfg.zscore_window).fillna(0.0)
    result["volume_zscore"] = rolling_zscore(
        np.log1p(result["tick_volume"].astype(float)),
        cfg.zscore_window,
    ).fillna(0.0)
    result["rolling_high"] = result["high"].rolling(cfg.breakout_window).max().shift(1)
    result["rolling_low"] = result["low"].rolling(cfg.breakout_window).min().shift(1)
    result["trend_gap"] = (result["close"] / result["close"].rolling(cfg.mean_window).mean()) - 1.0

    if "stress_level" not in result.columns:
        synthetic_stress = 45.0 + (12.0 * result["vol_zscore"]) + (8.0 * result["volume_zscore"])
        result["stress_level"] = synthetic_stress.clip(lower=0.0, upper=150.0)
    result["stress_change_3"] = result["stress_level"].diff(3)

    if "spread" in result.columns:
        result["spread_zscore"] = rolling_zscore(result["spread"], cfg.mean_window)

    return result

