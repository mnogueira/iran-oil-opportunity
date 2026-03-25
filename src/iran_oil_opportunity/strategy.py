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
    size_multiplier: float
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
        news_bias_2h = self._recent_local_news_mean(frame)
        polymarket_bias_6h = self._recent_prediction_market_mean(frame)
        combined_event_bias = self._combine_biases(news_bias_2h, polymarket_bias_6h)
        news_bias_hint = self._format_bias_hint(news_bias_2h)
        polymarket_bias_hint = self._format_bias_hint(polymarket_bias_6h)
        stop_distance_pct = max(self.config.min_stop_pct, atr_pct * self.config.stop_atr_multiple)
        take_profit_pct = stop_distance_pct * self.config.take_profit_multiple
        breakout_return_threshold = self.config.breakout_return_threshold
        breakout_stress_threshold = self.config.breakout_stress_threshold
        bearish_breakout_return_threshold = self.config.breakout_return_threshold
        bearish_breakout_stress_threshold = self.config.breakout_stress_threshold
        reversal_stress_threshold = self.config.reversal_stress_threshold
        reversal_zscore_threshold = self.config.reversal_zscore_threshold
        breakout_stress_change_floor = 0.0
        bullish_breakout_buffer = 0.0
        bearish_breakdown_buffer = 0.0
        bullish_event_support = max(0.0, news_bias_2h or 0.0) + (0.8 * max(0.0, polymarket_bias_6h or 0.0))
        bearish_event_support = max(0.0, -(news_bias_2h or 0.0)) + (0.9 * max(0.0, -(polymarket_bias_6h or 0.0)))
        if bullish_event_support > 0.0:
            breakout_return_threshold *= max(0.4, 1.0 - (0.45 * bullish_event_support))
            breakout_stress_threshold *= max(0.45, 1.0 - (0.35 * bullish_event_support))
            breakout_stress_change_floor = min(breakout_stress_change_floor, -12.0 * bullish_event_support)
            bullish_breakout_buffer = max(bullish_breakout_buffer, min(0.02, 0.01 + (0.01 * bullish_event_support)))
        if bearish_event_support > 0.0:
            bearish_breakout_return_threshold *= max(0.45, 1.0 - (0.4 * bearish_event_support))
            bearish_breakout_stress_threshold *= max(0.45, 1.0 - (0.3 * bearish_event_support))
            reversal_stress_threshold *= max(0.5, 1.0 - (0.3 * bearish_event_support))
            reversal_zscore_threshold *= max(0.5, 1.0 - (0.25 * bearish_event_support))
            bearish_breakdown_buffer = max(bearish_breakdown_buffer, min(0.02, 0.008 + (0.008 * bearish_event_support)))
        if news_bias_2h is not None and news_bias_2h > 0.5:
            breakout_return_threshold *= 0.7
            breakout_stress_threshold *= 0.8
            breakout_stress_change_floor = min(breakout_stress_change_floor, -15.0)
            bullish_breakout_buffer = max(bullish_breakout_buffer, 0.015)
        elif news_bias_2h is not None and news_bias_2h < 0.1:
            bearish_breakout_return_threshold *= 0.75
            bearish_breakout_stress_threshold *= 0.85
            reversal_stress_threshold *= 0.8
            reversal_zscore_threshold *= 0.85
            bearish_breakdown_buffer = max(bearish_breakdown_buffer, 0.01)
        if polymarket_bias_6h is not None and polymarket_bias_6h > 0.08:
            breakout_return_threshold *= 0.9
            breakout_stress_threshold *= 0.9
            bullish_breakout_buffer = max(bullish_breakout_buffer, 0.01)
        elif polymarket_bias_6h is not None and polymarket_bias_6h < -0.08:
            bearish_breakout_return_threshold *= 0.9
            bearish_breakout_stress_threshold *= 0.9
            reversal_stress_threshold *= 0.9
            reversal_zscore_threshold *= 0.9
            bearish_breakdown_buffer = max(bearish_breakdown_buffer, 0.008)
        stretched = (
            price_zscore >= reversal_zscore_threshold
            or trend_gap >= 0.12
            or recent_mean_gap >= 0.06
        )
        allow_long_entries = combined_event_bias is None or combined_event_bias > -0.12

        spread_hint = ""
        if spread_zscore is not None and pd.notna(spread_zscore):
            spread_value = float(spread_zscore)
            if symbol_role == "brent" and spread_value > 0.75:
                spread_hint = " brent_spread_support"
            elif symbol_role == "wti" and spread_value < -0.75:
                spread_hint = " wti_spread_support"

        if (
            allow_long_entries
            and
            pd.notna(rolling_high)
            and close >= (float(rolling_high) * (1.0 - bullish_breakout_buffer))
            and return_3 >= breakout_return_threshold
            and stress >= breakout_stress_threshold
            and stress_change >= breakout_stress_change_floor
            and event_score >= self.config.breakout_event_floor
        ):
            regime = "shock_breakout" if close > float(rolling_high) else "news_confirmed_breakout"
            size_multiplier = self._size_multiplier(
                signal=1,
                news_bias_2h=news_bias_2h,
                polymarket_bias_6h=polymarket_bias_6h,
            )
            return SignalDecision(
                signal=1,
                regime=regime,
                conviction=min(0.95, 0.55 + (stress / 200.0) + (self.config.event_weight * max(event_score, 0.0))),
                stop_distance_pct=stop_distance_pct,
                take_profit_pct=take_profit_pct,
                size_multiplier=size_multiplier,
                reason=(
                    f"fresh_upside_breakout stress={stress:.1f} event={event_score:.2f} "
                    f"news2h={news_bias_hint} poly6h={polymarket_bias_hint} sz={size_multiplier:.2f}{spread_hint}"
                ),
            )

        if (
            stretched
            and stress >= reversal_stress_threshold
            and return_1 < 0.0
            and (
                event_score <= self.config.reversal_event_ceiling
                or local_news_score < 0.0
                or prediction_market_score < 0.0
            )
        ):
            size_multiplier = self._size_multiplier(
                signal=-1,
                news_bias_2h=news_bias_2h,
                polymarket_bias_6h=polymarket_bias_6h,
            )
            return SignalDecision(
                signal=-1,
                regime="panic_reversal",
                conviction=min(
                    0.90,
                    0.60 + (stress / 250.0) + (self.config.event_weight * max(-event_score, 0.0)),
                ),
                stop_distance_pct=stop_distance_pct,
                take_profit_pct=take_profit_pct,
                size_multiplier=size_multiplier,
                reason=(
                    f"exhausted_panic_reversal stress={stress:.1f} "
                    f"z={price_zscore:.2f} event={event_score:.2f} "
                    f"news2h={news_bias_hint} poly6h={polymarket_bias_hint} sz={size_multiplier:.2f}{spread_hint}"
                ),
            )

        if (
            pd.notna(rolling_low)
            and close <= (float(rolling_low) * (1.0 + bearish_breakdown_buffer))
            and return_3 <= -bearish_breakout_return_threshold
            and stress >= bearish_breakout_stress_threshold
            and stress_change >= 0.0
        ):
            size_multiplier = self._size_multiplier(
                signal=-1,
                news_bias_2h=news_bias_2h,
                polymarket_bias_6h=polymarket_bias_6h,
            )
            return SignalDecision(
                signal=-1,
                regime="bearish_breakout",
                conviction=min(0.85, 0.50 + (stress / 220.0)),
                stop_distance_pct=stop_distance_pct,
                take_profit_pct=take_profit_pct,
                size_multiplier=size_multiplier,
                reason=(
                    f"fresh_downside_breakout stress={stress:.1f} "
                    f"news2h={news_bias_hint} poly6h={polymarket_bias_hint} sz={size_multiplier:.2f}"
                ),
            )

        return SignalDecision(
            signal=0,
            regime="flat",
            conviction=0.0,
            stop_distance_pct=stop_distance_pct,
            take_profit_pct=take_profit_pct,
            size_multiplier=0.0,
            reason="no_edge",
        )

    @classmethod
    def _recent_local_news_mean(cls, frame: pd.DataFrame) -> float | None:
        return cls._recent_signal_mean(
            frame,
            score_column="local_news_score",
            count_column="headline_count",
            lookback_hours=2,
        )

    @classmethod
    def _recent_prediction_market_mean(cls, frame: pd.DataFrame) -> float | None:
        return cls._recent_signal_mean(
            frame,
            score_column="prediction_market_score",
            count_column="prediction_market_count",
            lookback_hours=6,
        )

    @staticmethod
    def _recent_signal_mean(
        frame: pd.DataFrame,
        *,
        score_column: str,
        count_column: str,
        lookback_hours: int,
    ) -> float | None:
        if frame.empty or "local_news_score" not in frame.columns:
            return None
        if score_column not in frame.columns or count_column not in frame.columns:
            return None
        scores = pd.to_numeric(frame[score_column], errors="coerce").fillna(0.0)
        counts = pd.to_numeric(frame[count_column], errors="coerce").fillna(0.0)
        if scores.empty:
            return None
        if isinstance(frame.index, pd.DatetimeIndex) and len(frame.index) > 0:
            scores = scores.resample("1h").last().fillna(0.0).tail(lookback_hours)
            counts = counts.resample("1h").last().fillna(0.0).tail(lookback_hours)
        else:
            scores = scores.tail(lookback_hours)
            counts = counts.tail(lookback_hours)
        if counts.empty or float(counts.sum()) <= 0.0:
            return None
        return 0.0 if scores.empty else float(scores.mean())

    @staticmethod
    def _combine_biases(news_bias_2h: float | None, polymarket_bias_6h: float | None) -> float | None:
        weighted_values: list[tuple[float, float]] = []
        if news_bias_2h is not None:
            weighted_values.append((0.6, news_bias_2h))
        if polymarket_bias_6h is not None:
            weighted_values.append((0.4, polymarket_bias_6h))
        if not weighted_values:
            return None
        total_weight = sum(weight for weight, _ in weighted_values)
        return sum(weight * value for weight, value in weighted_values) / total_weight

    @classmethod
    def _size_multiplier(
        cls,
        *,
        signal: int,
        news_bias_2h: float | None,
        polymarket_bias_6h: float | None,
    ) -> float:
        combined_bias = cls._combine_biases(news_bias_2h, polymarket_bias_6h)
        if signal == 0 or combined_bias is None:
            return 1.0 if signal != 0 else 0.0
        alignment = signal * combined_bias
        return max(0.5, min(1.6, 1.0 + (0.75 * alignment)))

    @staticmethod
    def _format_bias_hint(value: float | None) -> str:
        return "na" if value is None else f"{value:.2f}"
