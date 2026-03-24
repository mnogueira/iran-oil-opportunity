"""Simple research backtester."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from iran_oil_opportunity.config import RiskConfig, StrategyConfig
from iran_oil_opportunity.features import build_feature_frame
from iran_oil_opportunity.risk import size_notional_fraction
from iran_oil_opportunity.strategy import IranOilShockStrategy


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """Backtest outputs."""

    total_return: float
    annualized_return: float
    annualized_volatility: float
    sharpe: float
    max_drawdown: float
    trades: int
    win_rate: float
    equity_curve: pd.DataFrame


def run_backtest(
    frame: pd.DataFrame,
    *,
    strategy_config: StrategyConfig | None = None,
    risk_config: RiskConfig | None = None,
) -> BacktestResult:
    """Run the event-window strategy on a price frame."""

    strategy_cfg = strategy_config or StrategyConfig()
    risk_cfg = risk_config or RiskConfig()
    strategy = IranOilShockStrategy(strategy_cfg)
    feature_frame = build_feature_frame(frame, strategy_config=strategy_cfg)

    equity = risk_cfg.initial_equity
    peak_equity = equity
    position = 0
    position_fraction = 0.0
    trades = 0
    wins = 0
    current_trade_pnl = 0.0
    rows: list[dict[str, object]] = []
    previous_close: float | None = None

    for offset, (timestamp, row) in enumerate(feature_frame.iterrows()):
        close = float(row["close"])
        bar_return = 0.0 if previous_close is None else (close / previous_close) - 1.0

        if position != 0 and previous_close is not None:
            pnl = equity * position_fraction * position * bar_return
            equity += pnl
            current_trade_pnl += pnl

        decision = strategy.decide(feature_frame.iloc[: offset + 1])
        plan = size_notional_fraction(
            equity=equity,
            stop_distance_pct=decision.stop_distance_pct,
            risk_config=risk_cfg,
        )
        target_position = decision.signal
        target_fraction = plan.notional_fraction if target_position != 0 else 0.0

        if target_position != position:
            if position != 0:
                trades += 1
                if current_trade_pnl > 0.0:
                    wins += 1
                current_trade_pnl = 0.0
            turnover = abs((target_position * target_fraction) - (position * position_fraction))
            equity -= equity * turnover * (risk_cfg.transaction_cost_bps / 10_000.0)
            position = target_position
            position_fraction = target_fraction

        peak_equity = max(peak_equity, equity)
        rows.append(
            {
                "timestamp": timestamp,
                "close": close,
                "signal": position,
                "position_fraction": position_fraction,
                "equity": equity,
                "drawdown": 1.0 - (equity / peak_equity),
                "regime": decision.regime,
                "reason": decision.reason,
            }
        )
        previous_close = close

    if position != 0:
        trades += 1
        if current_trade_pnl > 0.0:
            wins += 1

    result_frame = pd.DataFrame(rows).set_index("timestamp")
    equity_returns = result_frame["equity"].pct_change().fillna(0.0)
    sample_size = max(len(result_frame), 1)
    total_return = (equity / risk_cfg.initial_equity) - 1.0
    annualization = 252 / sample_size
    annualized_return = (1.0 + total_return) ** annualization - 1.0 if sample_size > 1 else total_return
    annualized_volatility = float(equity_returns.std(ddof=0) * np.sqrt(252))
    sharpe = 0.0 if annualized_volatility == 0.0 else annualized_return / annualized_volatility
    max_drawdown = float(result_frame["drawdown"].max()) if not result_frame.empty else 0.0
    win_rate = 0.0 if trades == 0 else wins / trades

    return BacktestResult(
        total_return=float(total_return),
        annualized_return=float(annualized_return),
        annualized_volatility=annualized_volatility,
        sharpe=float(sharpe),
        max_drawdown=max_drawdown,
        trades=trades,
        win_rate=float(win_rate),
        equity_curve=result_frame,
    )


backtest_strategy = run_backtest
