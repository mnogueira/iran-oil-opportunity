"""Thin MT5 wrapper with demo-account safeguards."""

from __future__ import annotations

from typing import Any

import pandas as pd

from iran_oil_opportunity.broker import (
    AccountSnapshot,
    BrokerConnection,
    ContractDetailsSnapshot,
    MarketDataSubscription,
    OrderRequest,
    OrderResult,
    PositionSnapshot,
    QuoteSnapshot,
)
from iran_oil_opportunity.config import BrokerConfig, RiskConfig
from iran_oil_opportunity.market_data import normalize_price_frame, rates_to_frame
from iran_oil_opportunity.risk import recommend_mt5_volume


class MT5Connection(BrokerConnection):
    """Context manager and normalized broker interface for MetaTrader 5."""

    broker_name = "mt5"

    def __init__(self, config: BrokerConfig):
        self.config = config
        self._mt5: Any | None = None

    def connect(self) -> AccountSnapshot:
        mt5 = self._import_mt5()
        kwargs: dict[str, object] = {"timeout": self.config.timeout_ms}
        if self.config.path:
            kwargs["path"] = self.config.path
        if self.config.login is not None:
            kwargs["login"] = self.config.login
        if self.config.password:
            kwargs["password"] = self.config.password
        if self.config.server:
            kwargs["server"] = self.config.server
        if not mt5.initialize(**kwargs):
            raise ConnectionError(f"MT5 initialize failed: {mt5.last_error()}")
        self._mt5 = mt5
        snapshot = self.account_snapshot()
        if self.config.require_demo and not snapshot.demo:
            raise RuntimeError("Connected MT5 account does not look like a demo environment.")
        return snapshot

    def disconnect(self) -> None:
        if self._mt5 is not None:
            self._mt5.shutdown()
        self._mt5 = None

    def __enter__(self) -> MT5Connection:
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    def account_snapshot(self) -> AccountSnapshot:
        mt5 = self._require_mt5()
        account = mt5.account_info()
        if account is None:
            raise RuntimeError("MT5 account_info() returned None.")
        demo_mode = getattr(mt5, "ACCOUNT_TRADE_MODE_DEMO", None)
        trade_mode = getattr(account, "trade_mode", None)
        server = str(getattr(account, "server", ""))
        demo = (demo_mode is not None and trade_mode == demo_mode) or ("DEMO" in server.upper())
        return AccountSnapshot(
            broker=self.broker_name,
            account_id=str(getattr(account, "login", "")) or None,
            server=server or None,
            balance=None if getattr(account, "balance", None) is None else float(account.balance),
            equity=None if getattr(account, "equity", None) is None else float(account.equity),
            buying_power=None if getattr(account, "margin_free", None) is None else float(account.margin_free),
            margin_used=None if getattr(account, "margin", None) is None else float(account.margin),
            currency=str(getattr(account, "currency", "")) or None,
            demo=bool(demo),
        )

    def terminal_info(self) -> dict[str, object]:
        terminal = self._require_mt5().terminal_info()
        if terminal is None:
            return {}
        return {
            "path": getattr(terminal, "path", None),
            "name": getattr(terminal, "name", None),
            "company": getattr(terminal, "company", None),
            "connected": getattr(terminal, "connected", None),
        }

    def list_symbols(self) -> list[str]:
        symbols = self._require_mt5().symbols_get()
        if symbols is None:
            return []
        return [str(symbol.name) for symbol in symbols]

    def symbol_info(self, symbol: str) -> Any | None:
        return self._require_mt5().symbol_info(symbol)

    def symbol_details(self, symbol: str) -> ContractDetailsSnapshot | None:
        info = self.symbol_info(symbol)
        if info is None:
            return None
        return ContractDetailsSnapshot(
            broker=self.broker_name,
            symbol=symbol,
            local_symbol=symbol,
            exchange=str(getattr(info, "exchange", "")) or None,
            currency=str(getattr(info, "currency_profit", getattr(info, "currency_base", ""))) or None,
            multiplier=None
            if getattr(info, "trade_contract_size", None) is None
            else float(info.trade_contract_size),
            min_quantity=None if getattr(info, "volume_min", None) is None else float(info.volume_min),
            max_quantity=None if getattr(info, "volume_max", None) is None else float(info.volume_max),
            quantity_step=None if getattr(info, "volume_step", None) is None else float(info.volume_step),
            metadata={
                "description": getattr(info, "description", None),
                "path": getattr(info, "path", None),
            },
        )

    def fetch_rates(self, symbol: str, timeframe: str, count: int) -> pd.DataFrame:
        mt5 = self._require_mt5()
        resolved_timeframe = self.resolve_timeframe(timeframe)
        rates = mt5.copy_rates_from_pos(symbol, resolved_timeframe, 0, count)
        return normalize_price_frame(rates_to_frame(rates), broker=self.broker_name, symbol=symbol)

    def get_quote(self, symbol: str) -> QuoteSnapshot:
        tick = self._require_mt5().symbol_info_tick(symbol)
        if tick is None:
            raise RuntimeError(f"No tick available for symbol {symbol}.")
        tick_time = getattr(tick, "time", None)
        timestamp = None if tick_time is None else pd.Timestamp(tick_time, unit="s", tz="UTC").to_pydatetime()
        return QuoteSnapshot(
            broker=self.broker_name,
            symbol=symbol,
            bid=None if getattr(tick, "bid", None) is None else float(tick.bid),
            ask=None if getattr(tick, "ask", None) is None else float(tick.ask),
            last=None if getattr(tick, "last", None) is None else float(tick.last),
            close=None if getattr(tick, "last", None) is None else float(tick.last),
            timestamp=timestamp,
        )

    def subscribe_market_data(
        self,
        symbol: str,
        callback,
    ) -> MarketDataSubscription:
        callback(self.get_quote(symbol))
        return MarketDataSubscription(broker=self.broker_name, symbol=symbol, cancel_callback=lambda: None)

    def positions(self) -> list[PositionSnapshot]:
        raw_positions = self._require_mt5().positions_get()
        if raw_positions is None:
            return []
        positions: list[PositionSnapshot] = []
        for position in raw_positions:
            side = 1.0 if int(getattr(position, "type", 0)) == 0 else -1.0
            quantity = side * float(getattr(position, "volume", 0.0) or 0.0)
            positions.append(
                PositionSnapshot(
                    broker=self.broker_name,
                    symbol=str(getattr(position, "symbol", "")),
                    quantity=quantity,
                    average_price=None if getattr(position, "price_open", None) is None else float(position.price_open),
                    market_price=None if getattr(position, "price_current", None) is None else float(position.price_current),
                    market_value=None,
                    unrealized_pnl=None if getattr(position, "profit", None) is None else float(position.profit),
                    realized_pnl=None,
                    currency=None,
                )
            )
        return positions

    def get_net_position(self, symbol: str) -> float:
        return float(
            sum(position.quantity for position in self.positions() if position.symbol.upper() == symbol.upper())
        )

    def place_order(self, request: OrderRequest) -> OrderResult:
        if request.order_type.upper() != "MKT":
            raise NotImplementedError("MT5Connection currently supports market orders only.")

        mt5 = self._require_mt5()
        symbol_info = mt5.symbol_info(request.symbol)
        if symbol_info is None:
            raise ValueError(f"Symbol not found: {request.symbol}")
        if not symbol_info.visible:
            mt5.symbol_select(request.symbol, True)
        tick = mt5.symbol_info_tick(request.symbol)
        if tick is None:
            raise RuntimeError(f"No tick available for symbol {request.symbol}")
        side = request.side.upper()
        order_type = mt5.ORDER_TYPE_BUY if side == "BUY" else mt5.ORDER_TYPE_SELL
        price = tick.ask if side == "BUY" else tick.bid
        filling_mode = getattr(symbol_info, "filling_mode", None)
        try:
            resolved_filling_mode = mt5.ORDER_FILLING_RETURN if filling_mode is None else int(filling_mode)
        except (TypeError, ValueError):
            resolved_filling_mode = mt5.ORDER_FILLING_RETURN
        payload = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": request.symbol,
            "volume": float(request.quantity),
            "type": order_type,
            "price": price,
            "deviation": self.config.deviation,
            "magic": self.config.magic_number,
            "comment": request.tag,
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": resolved_filling_mode,
        }
        result = mt5.order_send(payload)
        if result is None:
            raise RuntimeError(f"order_send returned None: {mt5.last_error()}")
        volume = float(getattr(result, "volume", request.quantity) or request.quantity)
        return OrderResult(
            broker=self.broker_name,
            symbol=request.symbol,
            order_id=None if getattr(result, "order", None) is None else str(result.order),
            status=None if getattr(result, "comment", None) is None else str(result.comment),
            filled_quantity=volume,
            remaining_quantity=0.0,
            average_fill_price=None if getattr(result, "price", None) is None else float(result.price),
            message=None if getattr(result, "retcode", None) is None else str(result.retcode),
            metadata={"retcode": getattr(result, "retcode", None)},
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
        return recommend_mt5_volume(
            symbol_info=self.symbol_info(symbol),
            equity=equity,
            entry_price=entry_price,
            stop_distance_pct=stop_distance_pct,
            risk_config=risk_config,
        )

    @staticmethod
    def resolve_timeframe(timeframe: str) -> int:
        import MetaTrader5 as mt5  # noqa: N813

        mapping = {
            "M5": mt5.TIMEFRAME_M5,
            "M15": mt5.TIMEFRAME_M15,
            "M30": mt5.TIMEFRAME_M30,
            "H1": mt5.TIMEFRAME_H1,
            "H4": mt5.TIMEFRAME_H4,
            "D1": mt5.TIMEFRAME_D1,
        }
        try:
            return mapping[timeframe.upper()]
        except KeyError as exc:
            raise ValueError(f"Unsupported timeframe: {timeframe}") from exc

    def _require_mt5(self) -> Any:
        if self._mt5 is None:
            raise RuntimeError("MT5 not connected.")
        return self._mt5

    @staticmethod
    def _import_mt5() -> Any:
        try:
            import MetaTrader5 as mt5  # noqa: N813

            return mt5
        except ImportError as exc:  # pragma: no cover
            raise ImportError("MetaTrader5 package is required.") from exc
