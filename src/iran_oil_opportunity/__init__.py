"""Iran oil volatility opportunity package."""

from iran_oil_opportunity.backtest import BacktestResult, run_backtest
from iran_oil_opportunity.broker import (
    AccountSnapshot,
    BrokerConnection,
    ContractDetailsSnapshot,
    OrderRequest,
    OrderResult,
    PositionSnapshot,
    QuoteSnapshot,
)
from iran_oil_opportunity.config import (
    BrokerConfig,
    CrossAssetConfig,
    HeadlineLLMConfig,
    IBConfig,
    PaperServiceConfig,
    RiskConfig,
    StrategyConfig,
)
from iran_oil_opportunity.cross_asset import CrossAssetOpportunity, analyze_cross_asset_opportunities
from iran_oil_opportunity.ib_client import IBGatewayClient
from iran_oil_opportunity.strategy import IranOilShockStrategy, SignalDecision

__all__ = [
    "AccountSnapshot",
    "BacktestResult",
    "BrokerConnection",
    "BrokerConfig",
    "ContractDetailsSnapshot",
    "CrossAssetConfig",
    "CrossAssetOpportunity",
    "HeadlineLLMConfig",
    "IBConfig",
    "IBGatewayClient",
    "IranOilShockStrategy",
    "OrderRequest",
    "OrderResult",
    "PaperServiceConfig",
    "PositionSnapshot",
    "QuoteSnapshot",
    "RiskConfig",
    "SignalDecision",
    "StrategyConfig",
    "analyze_cross_asset_opportunities",
    "run_backtest",
]
