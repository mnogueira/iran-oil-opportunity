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
    breakout_event_floor: float = -0.15
    reversal_event_ceiling: float = 0.10
    event_weight: float = 0.35
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
    alt_data_csv: Path | None = None


@dataclass(frozen=True, slots=True)
class CrossAssetConfig:
    """Rolling correlation settings for indirect plays."""

    short_window: int = 10
    long_window: int = 30
    signal_lookback: int = 3
    minimum_observations: int = 12
    expected_move_threshold: float = 0.025
    divergence_threshold: float = 0.015
    correlation_break_threshold: float = 0.20


@dataclass(frozen=True, slots=True)
class HeadlineLLMConfig:
    """Small-model headline translation and scoring settings."""

    api_key_env: str = "OPENAI_API_KEY"
    model: str = "gpt-5.4-mini"
    endpoint: str = "https://api.openai.com/v1/chat/completions"
    timeout_seconds: int = 20
    max_tokens: int = 300
    max_workers: int = 4
    reasoning_effort: str = "low"
    verbosity: str = "low"


@dataclass(frozen=True, slots=True)
class LocalNewsConfig:
    """RSS-based local-language news polling settings."""

    output_dir: Path = Path("data/processed/local_news")
    request_timeout_seconds: int = 15
    max_items_per_source: int = 20
    poll_seconds: int = 60
    half_life_minutes: int = 240


@dataclass(frozen=True, slots=True)
class PolymarketConfig:
    """Polymarket API polling settings."""

    gamma_base_url: str = "https://gamma-api.polymarket.com"
    clob_base_url: str = "https://clob.polymarket.com"
    request_timeout_seconds: int = 15
    limit: int = 80
    keywords: tuple[str, ...] = (
        "iran",
        "hormuz",
        "kharg",
        "ceasefire",
        "oil",
        "brent",
        "wti",
    )
