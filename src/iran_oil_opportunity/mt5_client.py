"""Thin MT5 wrapper with demo-account safeguards."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from iran_oil_opportunity.config import BrokerConfig
from iran_oil_opportunity.market_data import rates_to_frame


@dataclass(frozen=True, slots=True)
class AccountSnapshot:
    """Normalized MT5 account snapshot."""

    account_id: str | None
    server: str | None
    balance: float | None
    equity: float | None
    currency: str | None
    demo: bool


class MT5Connection:
    """Context manager for MetaTrader 5."""

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
            account_id=str(getattr(account, "login", "")) or None,
            server=server or None,
            balance=None if getattr(account, "balance", None) is None else float(account.balance),
            equity=None if getattr(account, "equity", None) is None else float(account.equity),
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

    def fetch_rates(self, symbol: str, timeframe: str, count: int) -> pd.DataFrame:
        mt5 = self._require_mt5()
        resolved_timeframe = self.resolve_timeframe(timeframe)
        rates = mt5.copy_rates_from_pos(symbol, resolved_timeframe, 0, count)
        return rates_to_frame(rates)

    def get_net_position(self, symbol: str) -> float:
        positions = self._require_mt5().positions_get(symbol=symbol)
        if positions is None:
            return 0.0
        total = 0.0
        for position in positions:
            side = 1.0 if int(position.type) == 0 else -1.0
            total += side * float(position.volume)
        return total

    def submit_market_order(self, *, symbol: str, side: str, volume: float) -> dict[str, object]:
        mt5 = self._require_mt5()
        symbol_info = mt5.symbol_info(symbol)
        if symbol_info is None:
            raise ValueError(f"Symbol not found: {symbol}")
        if not symbol_info.visible:
            mt5.symbol_select(symbol, True)
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            raise RuntimeError(f"No tick available for symbol {symbol}")
        order_type = mt5.ORDER_TYPE_BUY if side.upper() == "BUY" else mt5.ORDER_TYPE_SELL
        price = tick.ask if side.upper() == "BUY" else tick.bid
        filling_mode = getattr(symbol_info, "filling_mode", None)
        try:
            resolved_filling_mode = mt5.ORDER_FILLING_RETURN if filling_mode is None else int(filling_mode)
        except (TypeError, ValueError):
            resolved_filling_mode = mt5.ORDER_FILLING_RETURN
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": symbol,
            "volume": float(volume),
            "type": order_type,
            "price": price,
            "deviation": self.config.deviation,
            "magic": self.config.magic_number,
            "comment": "iran_oil_opportunity_demo",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": resolved_filling_mode,
        }
        result = mt5.order_send(request)
        if result is None:
            raise RuntimeError(f"order_send returned None: {mt5.last_error()}")
        return {
            "retcode": getattr(result, "retcode", None),
            "order": getattr(result, "order", None),
            "price": getattr(result, "price", None),
            "comment": getattr(result, "comment", None),
        }

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
