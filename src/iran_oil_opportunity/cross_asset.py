"""Cross-asset correlation scanner for indirect oil-volatility trades."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from math import isfinite

import pandas as pd

from iran_oil_opportunity.config import CrossAssetConfig
from iran_oil_opportunity.discovery import InstrumentCandidate, discover_candidates


EXPECTED_SIGN = {
    "oil_stock": 1,
    "airline": -1,
    "fx": -1,
    "gold": 1,
    "defense": 1,
    "shipping": 1,
    "petrochemical": -1,
    "agri": 1,
    "crypto": 0,
}

LAG_HINT = {
    "oil_stock": 2,
    "airline": 1,
    "fx": 0,
    "gold": 0,
    "defense": 0,
    "shipping": 1,
    "petrochemical": 1,
    "agri": 1,
    "crypto": 0,
}


@dataclass(frozen=True, slots=True)
class CrossAssetOpportunity:
    """Ranked indirect trade candidate."""

    symbol: str
    category: str
    base_source: str | None
    asset_source: str | None
    signal: int
    opportunity_score: float
    expected_sign: int
    short_corr: float
    long_corr: float
    lead_corr: float
    base_return: float
    asset_return: float
    divergence: float
    thesis: str


def classify_symbols(symbols: list[str]) -> dict[str, str]:
    """Map symbols to the first discovered category."""

    return {candidate.symbol: candidate.category for candidate in discover_candidates(symbols)}


def analyze_cross_asset_opportunities(
    frames: dict[str, pd.DataFrame],
    *,
    base_symbol: str,
    categories: dict[str, str] | None = None,
    config: CrossAssetConfig | None = None,
) -> list[CrossAssetOpportunity]:
    """Rank indirect trades around an oil move."""

    cfg = config or CrossAssetConfig()
    if base_symbol not in frames:
        raise KeyError(f"Missing base symbol frame: {base_symbol}")
    if "close" not in frames[base_symbol].columns:
        raise ValueError(f"Base frame for {base_symbol} must contain `close`.")

    category_map = categories or classify_symbols(list(frames.keys()))
    base_source = _infer_frame_source(frames[base_symbol])
    base_close = frames[base_symbol]["close"].sort_index().astype(float)
    base_return_series = base_close.pct_change()
    opportunities: list[CrossAssetOpportunity] = []

    for symbol, frame in frames.items():
        if symbol == base_symbol or frame.empty or "close" not in frame.columns:
            continue
        category = category_map.get(symbol, "unknown")
        asset_source = _infer_frame_source(frame)
        expected_sign = EXPECTED_SIGN.get(category, 0)
        if expected_sign == 0:
            continue

        aligned = pd.concat(
            [
                base_close.rename("base_close"),
                base_return_series.rename("base_return"),
                frame["close"].sort_index().astype(float).rename("asset_close"),
            ],
            axis=1,
            join="inner",
        ).dropna()
        if len(aligned) < cfg.minimum_observations:
            continue
        aligned["asset_return"] = aligned["asset_close"].pct_change()
        aligned = aligned.dropna()
        if len(aligned) < cfg.minimum_observations:
            continue

        short = aligned.tail(cfg.short_window)
        long = aligned.tail(cfg.long_window)
        short_corr = float(short["base_return"].corr(short["asset_return"]))
        long_corr = float(long["base_return"].corr(long["asset_return"]))

        lag_bars = LAG_HINT.get(category, 0)
        lead_corr = short_corr
        if lag_bars > 0:
            lagged_corr = float(short["base_return"].shift(lag_bars).corr(short["asset_return"]))
            if isfinite(lagged_corr):
                lead_corr = lagged_corr

        base_return = float(aligned["base_close"].pct_change(cfg.signal_lookback).iloc[-1])
        asset_return = float(aligned["asset_close"].pct_change(cfg.signal_lookback).iloc[-1])
        expected_move = expected_sign * base_return
        divergence = expected_move - asset_return
        corr_shift = short_corr - long_corr

        signal = 0
        if abs(base_return) >= cfg.expected_move_threshold and abs(divergence) >= cfg.divergence_threshold:
            signal = 1 if expected_move > 0.0 else -1

        score = 0.0
        if signal != 0:
            score += abs(expected_move) * 400.0
            score += abs(divergence) * 300.0
            if expected_sign * short_corr > 0.0:
                score += 25.0
            if expected_sign * lead_corr > 0.0:
                score += 15.0
            if abs(corr_shift) >= cfg.correlation_break_threshold:
                score += 20.0

        thesis = (
            f"base_source={base_source or 'unknown'} asset_source={asset_source or 'unknown'} "
            f"{category} expected_sign={expected_sign:+d} "
            f"oil_move={base_return:.2%} asset_move={asset_return:.2%} "
            f"short_corr={short_corr:.2f} long_corr={long_corr:.2f}"
        )
        opportunities.append(
            CrossAssetOpportunity(
                symbol=symbol,
                category=category,
                base_source=base_source,
                asset_source=asset_source,
                signal=signal,
                opportunity_score=round(score, 3),
                expected_sign=expected_sign,
                short_corr=round(short_corr, 4),
                long_corr=round(long_corr, 4),
                lead_corr=round(lead_corr, 4),
                base_return=round(base_return, 6),
                asset_return=round(asset_return, 6),
                divergence=round(divergence, 6),
                thesis=thesis,
            )
        )
    return sorted(opportunities, key=lambda item: (-item.opportunity_score, item.symbol))


def opportunities_to_frame(opportunities: list[CrossAssetOpportunity]) -> pd.DataFrame:
    """Convert opportunity objects to a DataFrame."""

    if not opportunities:
        return pd.DataFrame(
            columns=[
                "symbol",
                "category",
                "base_source",
                "asset_source",
                "signal",
                "opportunity_score",
                "expected_sign",
                "short_corr",
                "long_corr",
                "lead_corr",
                "base_return",
                "asset_return",
                "divergence",
                "thesis",
            ]
        )
    return pd.DataFrame([asdict(item) for item in opportunities])


def _infer_frame_source(frame: pd.DataFrame) -> str | None:
    """Infer a single broker/source label from frame metadata."""

    source = frame.attrs.get("broker")
    if isinstance(source, str) and source:
        return source
    if "broker" not in frame.columns:
        return None
    unique = frame["broker"].dropna().astype(str).unique()
    return unique[0] if len(unique) == 1 else None
