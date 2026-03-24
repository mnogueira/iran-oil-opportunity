"""Configuration contracts."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class BrokerConfig:
    """MT5 connection settings."""

    login: int | None = None
    password: str | None = None
    server: str | None = None
    path: str | None = None
    timeout_ms: int = 15_000
    magic_number: int = 260324
    deviation: int = 20
    require_demo: bool = True


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    """Oil-shock strategy parameters."""

    breakout_window: int = 5
    mean_window: int = 10
    volatility_window: int = 5
    zscore_window: int = 10
    breakout_return_threshold: float = 0.045
    breakout_stress_threshold: float = 60.0
    reversal_stress_threshold: float = 95.0
    reversal_zscore_threshold: float = 1.35
    stop_atr_multiple: float = 1.5
    take_profit_multiple: float = 2.5
    min_stop_pct: float = 0.0125


@dataclass(frozen=True, slots=True)
class RiskConfig:
    """Risk and sizing configuration."""

    initial_equity: float = 10_000.0
    risk_per_trade: float = 0.005
    max_exposure_fraction: float = 0.35
    max_drawdown: float = 0.12
    max_daily_loss: float = 0.03
    max_consecutive_losses: int = 3
    transaction_cost_bps: float = 6.0


@dataclass(frozen=True, slots=True)
class PaperServiceConfig:
    """Continuous paper-trading service settings."""

    output_dir: Path = Path(".tradebot/paper_oil_mt5")
    timeframe: str = "H1"
    bars_count: int = 750
    poll_seconds: int = 30
    submit_orders: bool = False
    symbol: str | None = None
    secondary_symbol: str | None = None
