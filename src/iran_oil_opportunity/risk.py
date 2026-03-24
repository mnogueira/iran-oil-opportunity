"""Risk controls and sizing."""

from __future__ import annotations

from dataclasses import dataclass
from math import floor
from typing import Any

from iran_oil_opportunity.config import RiskConfig


@dataclass(frozen=True, slots=True)
class PositionPlan:
    """Sizing output."""

    notional_fraction: float
    recommended_volume: float | None
    risk_budget: float


def size_notional_fraction(
    *,
    equity: float,
    stop_distance_pct: float,
    risk_config: RiskConfig,
) -> PositionPlan:
    """Size a paper position as a fraction of equity."""

    stop_distance_pct = max(stop_distance_pct, 1e-6)
    risk_budget = equity * risk_config.risk_per_trade
    notional_fraction = min(
        risk_config.max_exposure_fraction,
        risk_budget / (equity * stop_distance_pct),
    )
    return PositionPlan(
        notional_fraction=max(0.0, notional_fraction),
        recommended_volume=None,
        risk_budget=risk_budget,
    )


def round_volume(raw_volume: float, *, min_volume: float, max_volume: float, step: float) -> float:
    """Round an MT5 lot size to broker constraints."""

    if raw_volume <= 0.0:
        return 0.0
    bounded = max(min_volume, min(max_volume, raw_volume))
    if step <= 0.0:
        return round(bounded, 4)
    steps = floor((bounded - min_volume) / step)
    rounded = min_volume + max(0, steps) * step
    return round(min(max_volume, rounded), 4)


def recommend_mt5_volume(
    *,
    symbol_info: Any,
    equity: float,
    entry_price: float,
    stop_distance_pct: float,
    risk_config: RiskConfig,
) -> float | None:
    """Approximate an MT5 trade volume from equity and stop distance."""

    if symbol_info is None:
        return None
    contract_size = float(getattr(symbol_info, "trade_contract_size", 1.0) or 1.0)
    min_volume = float(getattr(symbol_info, "volume_min", 0.0) or 0.0)
    max_volume = float(getattr(symbol_info, "volume_max", 0.0) or 0.0)
    step = float(getattr(symbol_info, "volume_step", 0.0) or 0.0)
    if min_volume <= 0.0 or max_volume <= 0.0:
        return None

    risk_budget = equity * risk_config.risk_per_trade
    stop_distance_value = max(entry_price * max(stop_distance_pct, 1e-6), 1e-6)
    loss_per_lot = stop_distance_value * contract_size
    if loss_per_lot <= 0.0:
        return None

    raw_volume = risk_budget / loss_per_lot
    max_notional = equity * risk_config.max_exposure_fraction
    max_volume_by_notional = max_notional / max(entry_price * contract_size, 1e-6)
    return round_volume(
        min(raw_volume, max_volume_by_notional),
        min_volume=min_volume,
        max_volume=max_volume,
        step=step,
    )

