from __future__ import annotations

import unittest
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from iran_oil_opportunity.config import IBConfig, RiskConfig
from iran_oil_opportunity.ib_client import IBGatewayClient


class FakeEvent:
    def __init__(self) -> None:
        self.handlers: list[object] = []

    def __iadd__(self, handler: object) -> FakeEvent:
        self.handlers.append(handler)
        return self

    def __isub__(self, handler: object) -> FakeEvent:
        if handler in self.handlers:
            self.handlers.remove(handler)
        return self


@dataclass
class FakeAccountValue:
    account: str
    tag: str
    value: str
    currency: str = "USD"


@dataclass
class FakeContract:
    symbol: str
    exchange: str = ""
    currency: str = "USD"
    localSymbol: str = ""
    lastTradeDateOrContractMonth: str = ""
    multiplier: str = "1000"
    secType: str = "FUT"
    conId: int = 0
    tradingClass: str = ""


@dataclass
class FakeContractDetails:
    contract: FakeContract
    minSize: float = 1.0
    sizeIncrement: float = 1.0
    minTick: float = 0.01
    marketName: str = "Crude Oil"
    longName: str = "Crude Oil Futures"


@dataclass
class FakeBar:
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    average: float
    barCount: int


@dataclass
class FakeOrderState:
    initMarginChange: str = "8200"


@dataclass
class FakeOrderStatus:
    status: str
    filled: float
    remaining: float
    avgFillPrice: float


@dataclass
class FakeTrade:
    order: object
    orderStatus: FakeOrderStatus


@dataclass
class FakePosition:
    contract: FakeContract
    position: float
    avgCost: float


class FakeTicker:
    def __init__(self, *, last: float, bid: float, ask: float, close: float) -> None:
        self.last = last
        self.bid = bid
        self.ask = ask
        self.close = close
        self.time = datetime(2026, 3, 24, 12, 0, tzinfo=UTC)
        self.updateEvent = FakeEvent()


def build_fake_ib_async(account_id: str):
    class FakeFuture(FakeContract):
        def __init__(self, symbol: str, exchange: str = "", currency: str = "USD") -> None:
            super().__init__(symbol=symbol, exchange=exchange, currency=currency)

    class FakeBaseOrder:
        orderType = "BASE"

        def __init__(self, action: str, quantity: float, price: float | None = None) -> None:
            self.action = action
            self.totalQuantity = quantity
            self.orderId: int | None = None
            self.lmtPrice = price
            self.auxPrice = price

    class FakeMarketOrder(FakeBaseOrder):
        orderType = "MKT"

        def __init__(self, action: str, quantity: float) -> None:
            super().__init__(action, quantity)

    class FakeLimitOrder(FakeBaseOrder):
        orderType = "LMT"

        def __init__(self, action: str, quantity: float, price: float) -> None:
            super().__init__(action, quantity, price)

    class FakeStopOrder(FakeBaseOrder):
        orderType = "STP"

        def __init__(self, action: str, quantity: float, price: float) -> None:
            super().__init__(action, quantity, price)

    class FakeIB:
        def __init__(self) -> None:
            self.connected = False
            self.errorEvent = FakeEvent()
            self.disconnectedEvent = FakeEvent()
            self.placed_orders: list[object] = []
            self.managedAccounts = [account_id]

        def connect(
            self,
            *,
            host: str,
            port: int,
            clientId: int,
            timeout: float,
            readonly: bool,
            account: str = "",
        ) -> None:
            self.connected = True
            self.connection_args = {
                "host": host,
                "port": port,
                "clientId": clientId,
                "timeout": timeout,
                "readonly": readonly,
                "account": account,
            }

        def isConnected(self) -> bool:
            return self.connected

        def disconnect(self) -> None:
            self.connected = False

        def accountSummary(self, _account: str = "") -> list[FakeAccountValue]:
            return [
                FakeAccountValue(account_id, "NetLiquidation", "100000", "USD"),
                FakeAccountValue(account_id, "TotalCashValue", "100000", "USD"),
                FakeAccountValue(account_id, "BuyingPower", "150000", "USD"),
                FakeAccountValue(account_id, "FullInitMarginReq", "8000", "USD"),
            ]

        def reqContractDetails(self, contract: FakeContract) -> list[FakeContractDetails]:
            if contract.symbol == "CL":
                return [
                    FakeContractDetails(
                        FakeContract(
                            symbol="CL",
                            exchange="NYMEX",
                            currency="USD",
                            localSymbol="CLM6",
                            lastTradeDateOrContractMonth="20260620",
                            multiplier="1000",
                            conId=101,
                            tradingClass="CL",
                        )
                    ),
                    FakeContractDetails(
                        FakeContract(
                            symbol="CL",
                            exchange="NYMEX",
                            currency="USD",
                            localSymbol="CLN6",
                            lastTradeDateOrContractMonth="20260720",
                            multiplier="1000",
                            conId=102,
                            tradingClass="CL",
                        )
                    ),
                ]
            if contract.symbol == "BRN":
                return []
            if contract.symbol == "BZ":
                return [
                    FakeContractDetails(
                        FakeContract(
                            symbol="BZ",
                            exchange="ICEEU",
                            currency="USD",
                            localSymbol="BZM6",
                            lastTradeDateOrContractMonth="20260630",
                            multiplier="1000",
                            conId=201,
                            tradingClass="BZ",
                        )
                    )
                ]
            return []

        def reqHistoricalData(self, contract: FakeContract, **_kwargs: object) -> list[FakeBar]:
            if contract.symbol in {"CL", "BZ"}:
                base = 80.0 if contract.symbol == "CL" else 82.0
                return [
                    FakeBar("2026-03-24 09:00:00+00:00", base, base + 1, base - 1, base + 0.2, 1000, base + 0.1, 50),
                    FakeBar("2026-03-24 10:00:00+00:00", base + 0.2, base + 1.2, base - 0.8, base + 0.4, 1100, base + 0.3, 52),
                    FakeBar("2026-03-24 11:00:00+00:00", base + 0.4, base + 1.4, base - 0.6, base + 0.8, 1200, base + 0.6, 54),
                ]
            return []

        def reqTickers(self, contract: FakeContract) -> list[FakeTicker]:
            last = 81.25 if contract.symbol == "CL" else 83.5
            return [FakeTicker(last=last, bid=last - 0.02, ask=last + 0.02, close=last - 0.1)]

        def reqMktData(self, contract: FakeContract, *_args: object) -> FakeTicker:
            return self.reqTickers(contract)[0]

        def cancelMktData(self, _contract: FakeContract) -> None:
            return None

        def positions(self) -> list[FakePosition]:
            return [
                FakePosition(
                    contract=FakeContract(
                        symbol="BZ",
                        exchange="ICEEU",
                        currency="USD",
                        localSymbol="BZM6",
                        lastTradeDateOrContractMonth="20260630",
                        multiplier="1000",
                    ),
                    position=1.0,
                    avgCost=83500.0,
                )
            ]

        def placeOrder(self, _contract: FakeContract, order: object) -> FakeTrade:
            order.orderId = len(self.placed_orders) + 1
            self.placed_orders.append(order)
            return FakeTrade(order=order, orderStatus=FakeOrderStatus("Submitted", 0.0, 1.0, 0.0))

        def whatIfOrder(self, _contract: FakeContract, _order: object) -> FakeOrderState:
            return FakeOrderState()

    class FakeUtil:
        @staticmethod
        def df(rows: list[FakeBar]) -> pd.DataFrame:
            return pd.DataFrame([row.__dict__ for row in rows])

    return SimpleNamespace(
        IB=FakeIB,
        Future=FakeFuture,
        MarketOrder=FakeMarketOrder,
        LimitOrder=FakeLimitOrder,
        StopOrder=FakeStopOrder,
        util=FakeUtil,
    )


class IBGatewayClientTests(unittest.TestCase):
    def test_connect_and_discover_oil_futures(self) -> None:
        fake_module = build_fake_ib_async("DU1234567")
        with patch.object(IBGatewayClient, "_import_ib_async", return_value=fake_module):
            client = IBGatewayClient(IBConfig())
            snapshot = client.connect()
            discovered = client.discover_oil_futures()
            self.assertTrue(snapshot.demo)
            self.assertEqual(snapshot.account_id, "DU1234567")
            self.assertIn("CL", discovered)
            self.assertIn("BRN", discovered)
            self.assertEqual(discovered["BRN"].metadata["root_symbol"], "BZ")

    def test_fetch_rates_normalizes_ib_bars(self) -> None:
        fake_module = build_fake_ib_async("DU1234567")
        with patch.object(IBGatewayClient, "_import_ib_async", return_value=fake_module):
            client = IBGatewayClient(IBConfig())
            client.connect()
            frame = client.fetch_rates("CL", "H1", 3)
            self.assertEqual(len(frame), 3)
            self.assertEqual(frame.attrs["broker"], "ib")
            self.assertEqual(frame.attrs["symbol"], "CL")
            self.assertIn("tick_volume", frame.columns)
            self.assertAlmostEqual(float(frame.iloc[-1]["close"]), 80.8)

    def test_places_market_limit_and_stop_orders(self) -> None:
        fake_module = build_fake_ib_async("DU1234567")
        with patch.object(IBGatewayClient, "_import_ib_async", return_value=fake_module):
            client = IBGatewayClient(IBConfig())
            client.connect()
            market = client.submit_market_order(symbol="CL", side="BUY", volume=1)
            client.submit_limit_order(symbol="BRN", side="SELL", volume=1, limit_price=83.25)
            client.submit_stop_order(symbol="BRN", side="SELL", volume=1, stop_price=81.5)
            self.assertEqual(market.symbol, "CL")
            self.assertEqual(client.get_net_position("BRN"), 1.0)
            self.assertEqual([order.orderType for order in client._ib.placed_orders], ["MKT", "LMT", "STP"])

    def test_recommend_order_size_uses_margin_gate_for_min_contract(self) -> None:
        fake_module = build_fake_ib_async("DU1234567")
        with patch.object(IBGatewayClient, "_import_ib_async", return_value=fake_module):
            client = IBGatewayClient(IBConfig())
            client.connect()
            size = client.recommend_order_size(
                symbol="CL",
                equity=100_000.0,
                entry_price=81.25,
                stop_distance_pct=0.015,
                risk_config=RiskConfig(),
            )
            self.assertEqual(size, 1.0)

    def test_rejects_non_paper_account(self) -> None:
        fake_module = build_fake_ib_async("U1234567")
        with patch.object(IBGatewayClient, "_import_ib_async", return_value=fake_module):
            client = IBGatewayClient(IBConfig(require_paper=True))
            with self.assertRaises(RuntimeError):
                client.connect()


if __name__ == "__main__":
    unittest.main()
