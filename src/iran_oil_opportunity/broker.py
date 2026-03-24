"""Shared broker abstractions for live and paper execution."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

from iran_oil_opportunity.config import RiskConfig


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    """Normalized broker account state."""

    broker: str
    account_id: str | None
    server: str | None
    balance: float | None
    equity: float | None
    buying_power: float | None
    margin_used: float | None
    currency: str | None
    demo: bool
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ContractDetailsSnapshot:
    """Normalized instrument metadata."""

    broker: str
    symbol: str
    local_symbol: str | None = None
    exchange: str | None = None
    currency: str | None = None
    contract_month: str | None = None
    multiplier: float | None = None
    min_quantity: float | None = None
    max_quantity: float | None = None
    quantity_step: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class QuoteSnapshot:
    """Normalized top-of-book state."""

    broker: str
    symbol: str
    bid: float | None
    ask: float | None
    last: float | None
    close: float | None
    timestamp: datetime | None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OrderRequest:
    """Broker-agnostic order request."""

    symbol: str
    side: str
    quantity: float
    order_type: str = "MKT"
    limit_price: float | None = None
    stop_price: float | None = None
    tif: str = "GTC"
    tag: str = "iran_oil_opportunity"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OrderResult:
    """Normalized order placement result."""

    broker: str
    symbol: str
    order_id: str | None
    status: str | None
    filled_quantity: float | None
    remaining_quantity: float | None
    average_fill_price: float | None
    message: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PositionSnapshot:
    """Normalized open-position state."""

    broker: str
    symbol: str
    quantity: float
    average_price: float | None
    market_price: float | None
    market_value: float | None
    unrealized_pnl: float | None
    realized_pnl: float | None
    currency: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class MarketDataSubscription:
    """Handle for an active market-data stream."""

    broker: str
    symbol: str
    cancel_callback: Callable[[], None]
    handle: Any | None = None

    def cancel(self) -> None:
        self.cancel_callback()


class BrokerConnection(ABC):
    """Shared broker interface for MT5 and IB implementations."""

    broker_name: str

    @abstractmethod
    def connect(self) -> AccountSnapshot:
        """Connect to the broker and return the account snapshot."""

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the broker."""

    @abstractmethod
    def account_snapshot(self) -> AccountSnapshot:
        """Return the latest normalized account snapshot."""

    @abstractmethod
    def list_symbols(self) -> list[str]:
        """Return a lightweight symbol list or root list for discovery."""

    @abstractmethod
    def symbol_details(self, symbol: str) -> ContractDetailsSnapshot | None:
        """Return normalized symbol metadata."""

    @abstractmethod
    def fetch_rates(self, symbol: str, timeframe: str, count: int) -> pd.DataFrame:
        """Fetch historical bars normalized to the project price-frame schema."""

    @abstractmethod
    def get_quote(self, symbol: str) -> QuoteSnapshot:
        """Fetch the latest top-of-book snapshot."""

    @abstractmethod
    def subscribe_market_data(
        self,
        symbol: str,
        callback: Callable[[QuoteSnapshot], None],
    ) -> MarketDataSubscription:
        """Start a live market-data subscription."""

    @abstractmethod
    def positions(self) -> list[PositionSnapshot]:
        """Return open positions."""

    @abstractmethod
    def get_net_position(self, symbol: str) -> float:
        """Return the net signed quantity for one symbol."""

    @abstractmethod
    def place_order(self, request: OrderRequest) -> OrderResult:
        """Place an order and return the normalized result."""

    def submit_market_order(self, *, symbol: str, side: str, volume: float) -> OrderResult:
        return self.place_order(OrderRequest(symbol=symbol, side=side, quantity=volume, order_type="MKT"))

    def submit_limit_order(
        self,
        *,
        symbol: str,
        side: str,
        volume: float,
        limit_price: float,
    ) -> OrderResult:
        return self.place_order(
            OrderRequest(
                symbol=symbol,
                side=side,
                quantity=volume,
                order_type="LMT",
                limit_price=limit_price,
            )
        )

    def submit_stop_order(
        self,
        *,
        symbol: str,
        side: str,
        volume: float,
        stop_price: float,
    ) -> OrderResult:
        return self.place_order(
            OrderRequest(
                symbol=symbol,
                side=side,
                quantity=volume,
                order_type="STP",
                stop_price=stop_price,
            )
        )

    def recommend_order_size(
        self,
        *,
        symbol: str,
        equity: float,
        entry_price: float,
        stop_distance_pct: float,
        risk_config: RiskConfig,
    ) -> float | None:
        """Return a broker-native quantity recommendation when available."""

        return None
