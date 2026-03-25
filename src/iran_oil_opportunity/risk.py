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
    return recommend_contract_quantity(
        equity=equity,
        entry_price=entry_price,
        stop_distance_pct=stop_distance_pct,
        risk_config=risk_config,
        contract_multiplier=contract_size,
        min_quantity=min_volume,
        max_quantity=max_volume,
        quantity_step=step,
    )


def recommend_contract_quantity(
    *,
    equity: float,
    entry_price: float,
    stop_distance_pct: float,
    risk_config: RiskConfig,
    contract_multiplier: float,
    min_quantity: float,
    max_quantity: float,
    quantity_step: float,
    max_position_notional: float | None = None,
) -> float | None:
    """Approximate a broker-native quantity from stop risk and contract specs."""

    if min_quantity <= 0.0 or max_quantity <= 0.0 or contract_multiplier <= 0.0:
        return None

    risk_budget = equity * risk_config.risk_per_trade
    stop_distance_value = max(entry_price * max(stop_distance_pct, 1e-6), 1e-6)
    loss_per_contract = stop_distance_value * contract_multiplier
    if loss_per_contract <= 0.0:
        return None

    raw_volume = risk_budget / loss_per_contract
    notional_cap = equity * risk_config.max_exposure_fraction if max_position_notional is None else max_position_notional
    max_volume_by_notional = notional_cap / max(entry_price * contract_multiplier, 1e-6)
    capped_volume = min(raw_volume, max_volume_by_notional, max_quantity)
    if capped_volume < min_quantity:
        return 0.0
    return round_volume(
        capped_volume,
        min_volume=min_quantity,
        max_volume=max_quantity,
        step=quantity_step,
    )


def scale_order_quantity(
    quantity: float | None,
    *,
    multiplier: float,
    instrument: Any | None = None,
) -> float | None:
    """Scale a broker-native quantity while respecting min/max/step constraints."""

    if quantity is None:
        return None
    scaled = max(0.0, float(quantity) * max(0.0, multiplier))
    min_quantity = _coerce_instrument_value(instrument, "min_quantity", "volume_min")
    max_quantity = _coerce_instrument_value(instrument, "max_quantity", "volume_max")
    quantity_step = _coerce_instrument_value(instrument, "quantity_step", "volume_step")
    if min_quantity is not None and scaled < min_quantity:
        return 0.0
    if min_quantity is None or max_quantity is None:
        return round(scaled, 4)
    step = quantity_step if quantity_step not in (None, 0.0) else min_quantity
    return round_volume(scaled, min_volume=min_quantity, max_volume=max_quantity, step=step)


def _coerce_instrument_value(instrument: Any | None, *names: str) -> float | None:
    if instrument is None:
        return None
    for name in names:
        raw_value = getattr(instrument, name, None)
        if raw_value in (None, ""):
            continue
        try:
            return float(raw_value)
        except (TypeError, ValueError):
            continue
    return None
