"""Regime-switching oil-shock strategy."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from iran_oil_opportunity.config import StrategyConfig


@dataclass(frozen=True, slots=True)
class SignalDecision:
    """Latest strategy decision."""

    signal: int
    regime: str
    conviction: float
    stop_distance_pct: float
    take_profit_pct: float
    reason: str


class IranOilShockStrategy:
    """Trade breakout expansion, then fade exhausted panic."""

    def __init__(self, config: StrategyConfig | None = None):
        self.config = config or StrategyConfig()

    def decide(self, frame: pd.DataFrame, *, symbol_role: str = "primary") -> SignalDecision:
        if frame.empty:
            return SignalDecision(0, "idle", 0.0, self.config.min_stop_pct, 0.0, "no_data")

        latest = frame.iloc[-1]
        close = float(latest["close"])
        rolling_high = latest.get("rolling_high")
        rolling_low = latest.get("rolling_low")
        stress = float(latest.get("stress_level", 0.0))
        stress_change = float(latest.get("stress_change_3", 0.0) or 0.0)
        return_1 = float(latest.get("return_1", 0.0) or 0.0)
        return_3 = float(latest.get("return_3", 0.0) or 0.0)
        raw_price_zscore = latest.get("price_zscore")
        price_zscore = 0.0 if raw_price_zscore is None or pd.isna(raw_price_zscore) else float(raw_price_zscore)
        trend_gap = float(latest.get("trend_gap", 0.0) or 0.0)
        recent_window = max(3, min(len(frame), self.config.breakout_window))
        recent_tail = frame["close"].tail(recent_window)
        recent_mean_gap = 0.0 if recent_tail.empty else (close / float(recent_tail.mean())) - 1.0
        atr_pct = float(latest.get("atr_pct", 0.0) or 0.0)
        event_score = float(latest.get("event_score", 0.0) or 0.0)
        local_news_score = float(latest.get("local_news_score", 0.0) or 0.0)
        prediction_market_score = float(latest.get("prediction_market_score", 0.0) or 0.0)
        spread_zscore = latest.get("spread_zscore")
        stop_distance_pct = max(self.config.min_stop_pct, atr_pct * self.config.stop_atr_multiple)
        take_profit_pct = stop_distance_pct * self.config.take_profit_multiple
        stretched = (
            price_zscore >= self.config.reversal_zscore_threshold
            or trend_gap >= 0.12
            or recent_mean_gap >= 0.06
        )

        spread_hint = ""
        if spread_zscore is not None and pd.notna(spread_zscore):
            spread_value = float(spread_zscore)
            if symbol_role == "brent" and spread_value > 0.75:
                spread_hint = " brent_spread_support"
            elif symbol_role == "wti" and spread_value < -0.75:
                spread_hint = " wti_spread_support"

        if (
            pd.notna(rolling_high)
            and close > float(rolling_high)
            and return_3 >= self.config.breakout_return_threshold
            and stress >= self.config.breakout_stress_threshold
            and stress_change >= 0.0
            and event_score >= self.config.breakout_event_floor
        ):
            return SignalDecision(
                signal=1,
                regime="shock_breakout",
                conviction=min(0.95, 0.55 + (stress / 200.0) + (self.config.event_weight * max(event_score, 0.0))),
                stop_distance_pct=stop_distance_pct,
                take_profit_pct=take_profit_pct,
                reason=f"fresh_upside_breakout stress={stress:.1f} event={event_score:.2f}{spread_hint}",
            )

        if (
            stretched
            and stress >= self.config.reversal_stress_threshold
            and return_1 < 0.0
            and (
                event_score <= self.config.reversal_event_ceiling
                or local_news_score < 0.0
                or prediction_market_score < 0.0
            )
        ):
            return SignalDecision(
                signal=-1,
                regime="panic_reversal",
                conviction=min(
                    0.90,
                    0.60 + (stress / 250.0) + (self.config.event_weight * max(-event_score, 0.0)),
                ),
                stop_distance_pct=stop_distance_pct,
                take_profit_pct=take_profit_pct,
                reason=(
                    f"exhausted_panic_reversal stress={stress:.1f} "
                    f"z={price_zscore:.2f} event={event_score:.2f}{spread_hint}"
                ),
            )

        if (
            pd.notna(rolling_low)
            and close < float(rolling_low)
            and return_3 <= -self.config.breakout_return_threshold
            and stress >= self.config.breakout_stress_threshold
            and stress_change >= 0.0
        ):
            return SignalDecision(
                signal=-1,
                regime="bearish_breakout",
                conviction=min(0.85, 0.50 + (stress / 220.0)),
                stop_distance_pct=stop_distance_pct,
                take_profit_pct=take_profit_pct,
                reason=f"fresh_downside_breakout stress={stress:.1f}",
            )

        return SignalDecision(
            signal=0,
            regime="flat",
            conviction=0.0,
            stop_distance_pct=stop_distance_pct,
            take_profit_pct=take_profit_pct,
            reason="no_edge",
        )
