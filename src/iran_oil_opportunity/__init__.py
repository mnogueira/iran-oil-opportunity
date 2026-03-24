"""Iran oil volatility opportunity package."""

from iran_oil_opportunity.backtest import BacktestResult, run_backtest
from iran_oil_opportunity.config import (
    BrokerConfig,
    CrossAssetConfig,
    HeadlineLLMConfig,
    PaperServiceConfig,
    RiskConfig,
    StrategyConfig,
)
from iran_oil_opportunity.cross_asset import CrossAssetOpportunity, analyze_cross_asset_opportunities
from iran_oil_opportunity.strategy import IranOilShockStrategy, SignalDecision

__all__ = [
    "BacktestResult",
    "BrokerConfig",
    "CrossAssetConfig",
    "CrossAssetOpportunity",
    "HeadlineLLMConfig",
    "IranOilShockStrategy",
    "PaperServiceConfig",
    "RiskConfig",
    "SignalDecision",
    "StrategyConfig",
    "analyze_cross_asset_opportunities",
    "run_backtest",
]
