"""Interactive Brokers Gateway client built on top of ib_async."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import isfinite
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
from iran_oil_opportunity.config import IBConfig, RiskConfig
from iran_oil_opportunity.market_data import normalize_price_frame
from iran_oil_opportunity.risk import recommend_contract_quantity


@dataclass(frozen=True, slots=True)
class _FutureSearch:
    canonical_symbol: str
    roots: tuple[str, ...]
    exchanges: tuple[str, ...]


class IBGatewayClient(BrokerConnection):
    """Thin normalized client for Interactive Brokers paper trading."""

    broker_name = "ib"

    _TIMEFRAME_MAP = {
        "M1": ("1 min", timedelta(minutes=1)),
        "M5": ("5 mins", timedelta(minutes=5)),
        "M15": ("15 mins", timedelta(minutes=15)),
        "M30": ("30 mins", timedelta(minutes=30)),
        "H1": ("1 hour", timedelta(hours=1)),
        "H4": ("4 hours", timedelta(hours=4)),
        "D1": ("1 day", timedelta(days=1)),
    }

    def __init__(self, config: IBConfig):
        self.config = config
        self._api: Any | None = None
        self._ib: Any | None = None  # data connection (live, port 4001)
        self._ib_exec: Any | None = None  # execution connection (paper, port 4002)
        self._last_error: str | None = None
        self._needs_reconnect = False
        self._resolved_contracts: dict[str, tuple[Any, ContractDetailsSnapshot]] = {}

    def connect(self) -> AccountSnapshot:
        if self._ib is not None and self._is_connected():
            snapshot = self.account_snapshot()
            return snapshot

        self._api = self._import_ib_async()
        # Connect data port (live account, real-time data)
        self._connect_with_retries()
        # Connect execution port (paper account) if different
        if self.config.execution_port != self.config.data_port:
            self._connect_execution()
        snapshot = self.account_snapshot()
        return snapshot

    def _connect_execution(self) -> None:
        """Connect to the paper trading port for order execution."""
        IB = self._api.IB
        self._ib_exec = IB()
        try:
            self._ib_exec.connect(
                self.config.host,
                self.config.execution_port,
                clientId=self.config.execution_client_id,
                timeout=self.config.timeout_seconds,
            )
        except Exception:
            self._ib_exec = None

    @property
    def _exec_ib(self) -> Any:
        """Return execution connection if available, else fall back to data connection."""
        if self._ib_exec is not None and self._ib_exec.isConnected():
            return self._ib_exec
        return self._ib

    def disconnect(self) -> None:
        if self._ib_exec is not None:
            try:
                if self._ib_exec.isConnected():
                    self._ib_exec.disconnect()
            finally:
                self._ib_exec = None
        if self._ib is not None:
            try:
                if self._is_connected():
                    self._ib.disconnect()
            finally:
                self._ib = None
                self._needs_reconnect = False
                self._resolved_contracts.clear()

    def __enter__(self) -> IBGatewayClient:
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.disconnect()

    def account_snapshot(self) -> AccountSnapshot:
        self._ensure_connected()
        summary = self._account_summary_by_tag()
        account_id = self.config.account or self._discover_account_id(summary)
        net_liquidation = self._summary_float(summary, "NetLiquidation", "EquityWithLoanValue")
        balance = self._summary_float(summary, "TotalCashValue", "CashBalance")
        buying_power = self._summary_float(summary, "BuyingPower", "AvailableFunds")
        margin_used = self._summary_float(summary, "FullInitMarginReq", "InitMarginReq", "MaintMarginReq")
        currency = self._summary_currency(summary, "NetLiquidation", "TotalCashValue")
        demo = self._looks_like_paper_account(account_id, summary)
        return AccountSnapshot(
            broker=self.broker_name,
            account_id=account_id,
            server=f"{self.config.host}:{self.config.port}",
            balance=balance,
            equity=net_liquidation,
            buying_power=buying_power,
            margin_used=margin_used,
            currency=currency,
            demo=demo,
        )

    def list_symbols(self) -> list[str]:
        discovered = self.discover_oil_futures()
        symbols = [symbol for symbol in self.config.futures_roots if symbol in discovered]
        return symbols or list(self.config.futures_roots)

    def discover_oil_futures(self) -> dict[str, ContractDetailsSnapshot]:
        self._ensure_connected()
        discovered: dict[str, ContractDetailsSnapshot] = {}
        for search in self._future_searches():
            try:
                _, snapshot = self._resolve_contract(search.canonical_symbol)
            except LookupError:
                continue
            discovered[search.canonical_symbol] = snapshot
        return discovered

    def symbol_details(self, symbol: str) -> ContractDetailsSnapshot | None:
        try:
            _, snapshot = self._resolve_contract(symbol)
        except LookupError:
            return None
        return snapshot

    def fetch_rates(self, symbol: str, timeframe: str, count: int) -> pd.DataFrame:
        self._ensure_connected()
        if count <= 0:
            return pd.DataFrame()
        contract, snapshot = self._resolve_contract(symbol)
        bar_size, bar_delta = self._resolve_timeframe(timeframe)
        if self._should_chunk_history(bar_delta, count):
            return self._fetch_rates_chunked(
                contract,
                symbol=snapshot.symbol,
                bar_size=bar_size,
                bar_delta=bar_delta,
                count=count,
            )
        duration = self._duration_from_count(bar_delta, count)
        return self._request_history_frame(
            contract,
            symbol=snapshot.symbol,
            end_date_time="",
            duration=duration,
            bar_size=bar_size,
        ).tail(count)

    def get_quote(self, symbol: str) -> QuoteSnapshot:
        self._ensure_connected()
        contract, snapshot = self._resolve_contract(symbol)
        tickers = self._ib.reqTickers(contract)
        ticker = self._wait_for_ticker_prices(tickers[0] if tickers else None)
        if ticker is None or self._quote_has_no_prices(ticker):
            market_data_type = getattr(self._ib, "reqMarketDataType", None)
            if callable(market_data_type):
                market_data_type(3)
                tickers = self._ib.reqTickers(contract)
                ticker = self._wait_for_ticker_prices(tickers[0] if tickers else None)
        if ticker is None:
            raise RuntimeError(f"No market data returned for {symbol}.")
        return self._ticker_to_quote(snapshot.symbol, ticker)

    def subscribe_market_data(self, symbol: str, callback) -> MarketDataSubscription:
        self._ensure_connected()
        contract, snapshot = self._resolve_contract(symbol)
        ticker = self._ib.reqMktData(contract, "", False, False)

        def emit(*_args: object) -> None:
            callback(self._ticker_to_quote(snapshot.symbol, ticker))

        update_event = getattr(ticker, "updateEvent", None)
        if update_event is not None:
            update_event += emit
        else:
            emit()

        def cancel() -> None:
            if update_event is not None:
                try:
                    update_event -= emit
                except Exception:
                    pass
            try:
                self._ib.cancelMktData(contract)
            except Exception:
                pass

        return MarketDataSubscription(
            broker=self.broker_name,
            symbol=snapshot.symbol,
            cancel_callback=cancel,
            handle=ticker,
        )

    def positions(self) -> list[PositionSnapshot]:
        self._ensure_connected()
        positions: list[PositionSnapshot] = []
        for position in self._exec_ib.positions():
            contract = getattr(position, "contract", None)
            raw_symbol = str(getattr(contract, "symbol", ""))
            multiplier = self._coerce_float(getattr(contract, "multiplier", None))
            avg_cost = self._coerce_float(getattr(position, "avgCost", None))
            average_price = avg_cost
            if avg_cost is not None and multiplier not in (None, 0.0):
                average_price = avg_cost / multiplier
            positions.append(
                PositionSnapshot(
                    broker=self.broker_name,
                    symbol=self._canonical_symbol(raw_symbol),
                    quantity=float(getattr(position, "position", 0.0) or 0.0),
                    average_price=average_price,
                    market_price=None,
                    market_value=None,
                    unrealized_pnl=None,
                    realized_pnl=None,
                    currency=str(getattr(contract, "currency", "")) or None,
                )
            )
        return positions

    def get_net_position(self, symbol: str) -> float:
        query = self._canonical_symbol(symbol)
        total = 0.0
        for position in self.positions():
            if position.symbol.upper() == query.upper():
                total += position.quantity
        return total

    def place_order(self, request: OrderRequest) -> OrderResult:
        self._ensure_connected()
        if self.config.readonly:
            raise RuntimeError("IB client is configured in readonly mode.")
        contract, snapshot = self._resolve_contract(request.symbol)
        order = self._build_order(request)
        trade = self._exec_ib.placeOrder(contract, order)
        self._wait_for_trade_status(trade)
        status = getattr(getattr(trade, "orderStatus", None), "status", None)
        filled = self._coerce_float(getattr(getattr(trade, "orderStatus", None), "filled", None))
        remaining = self._coerce_float(getattr(getattr(trade, "orderStatus", None), "remaining", None))
        average_fill_price = self._coerce_float(getattr(getattr(trade, "orderStatus", None), "avgFillPrice", None))
        order_id = getattr(order, "orderId", None)
        if order_id is None:
            order_id = getattr(getattr(trade, "order", None), "orderId", None)
        return OrderResult(
            broker=self.broker_name,
            symbol=snapshot.symbol,
            order_id=None if order_id is None else str(order_id),
            status=None if status is None else str(status),
            filled_quantity=filled,
            remaining_quantity=remaining,
            average_fill_price=average_fill_price,
            message=self._last_error,
            metadata={
                "local_symbol": snapshot.local_symbol,
                "exchange": snapshot.exchange,
                "contract_month": snapshot.contract_month,
            },
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
        details = self.symbol_details(symbol)
        if details is None:
            return None
        min_quantity = details.min_quantity or 1.0
        quantity_step = details.quantity_step or min_quantity or 1.0
        size = recommend_contract_quantity(
            equity=equity,
            entry_price=entry_price,
            stop_distance_pct=stop_distance_pct,
            risk_config=risk_config,
            contract_multiplier=details.multiplier or 1.0,
            min_quantity=min_quantity,
            max_quantity=details.max_quantity or 1_000.0,
            quantity_step=quantity_step,
            max_position_notional=(self.account_snapshot().buying_power or equity) * 10.0,
        )
        if size and size > 0.0:
            return size
        if self._can_afford_min_contract(
            symbol=symbol,
            minimum_quantity=min_quantity,
            entry_price=entry_price,
            stop_distance_pct=stop_distance_pct,
            multiplier=details.multiplier or 1.0,
            equity=equity,
            risk_config=risk_config,
        ):
            return min_quantity
        return 0.0

    def estimate_initial_margin(self, symbol: str, *, quantity: float, side: str = "BUY") -> float | None:
        self._ensure_connected()
        method = getattr(self._ib, "whatIfOrder", None)
        if method is None:
            return None
        contract, _ = self._resolve_contract(symbol)
        order = self._build_order(OrderRequest(symbol=symbol, side=side, quantity=quantity, order_type="MKT"))
        try:
            state = method(contract, order)
        except Exception:
            return None
        if hasattr(state, "orderState"):
            state = state.orderState
        margin = self._coerce_float(getattr(state, "initMarginChange", None))
        if margin is None:
            margin = self._coerce_float(getattr(state, "initialMarginChange", None))
        return None if margin is None else abs(margin)

    def _ensure_connected(self) -> None:
        if self._ib is None or not self._is_connected() or self._needs_reconnect:
            self.connect()

    def _connect_with_retries(self) -> None:
        assert self._api is not None
        last_error: Exception | None = None
        attempts = max(1, self.config.reconnect_attempts)
        for attempt in range(1, attempts + 1):
            self._ib = self._api.IB()
            self._bind_events()
            try:
                try:
                    self._ib.connect(
                        host=self.config.host,
                        port=self.config.port,
                        clientId=self.config.client_id,
                        timeout=self.config.timeout_seconds,
                        readonly=self.config.readonly,
                        account=self.config.account or "",
                    )
                except TypeError:
                    self._ib.connect(
                        host=self.config.host,
                        port=self.config.port,
                        clientId=self.config.client_id,
                        timeout=self.config.timeout_seconds,
                        readonly=self.config.readonly,
                    )
                if not self._is_connected():
                    raise ConnectionError("ib_async connect() returned without an active session.")
                self._needs_reconnect = False
                self._last_error = None
                return
            except Exception as exc:
                last_error = exc
                self._last_error = repr(exc)
                try:
                    self._ib.disconnect()
                except Exception:
                    pass
                self._ib = None
                if attempt < attempts:
                    time.sleep(max(0.5, self.config.reconnect_backoff_seconds * attempt))
        raise ConnectionError(
            f"Unable to connect to IB Gateway at {self.config.host}:{self.config.port}: {last_error!r}"
        )

    def _bind_events(self) -> None:
        if self._ib is None:
            return
        error_event = getattr(self._ib, "errorEvent", None)
        disconnected_event = getattr(self._ib, "disconnectedEvent", None)
        if error_event is not None:
            error_event += self._on_error
        if disconnected_event is not None:
            disconnected_event += self._on_disconnect

    def _on_error(self, req_id: object, error_code: object, error_string: object, *_args: object) -> None:
        self._last_error = f"req_id={req_id} code={error_code} message={error_string}"
        if error_code in {1100, 1101, 1102, 1300}:
            self._needs_reconnect = True

    def _on_disconnect(self) -> None:
        self._needs_reconnect = True

    def _resolve_contract(self, symbol: str) -> tuple[Any, ContractDetailsSnapshot]:
        self._ensure_connected()
        query = symbol.upper()
        cached = self._resolved_contracts.get(query)
        if cached is not None:
            return cached

        for search in self._search_plan(query):
            details = self._find_contract_details(search)
            if not details:
                continue
            selected = self._choose_front_contract(details)
            if selected is None:
                continue
            contract = getattr(selected, "contract", None)
            if contract is None:
                continue
            snapshot = self._detail_to_snapshot(selected, canonical_symbol=search.canonical_symbol)
            cached_value = (contract, snapshot)
            self._cache_contract_aliases(snapshot, contract)
            self._resolved_contracts[query] = cached_value
            return cached_value
        raise LookupError(f"No IB futures contract found for {symbol}.")

    def _cache_contract_aliases(self, snapshot: ContractDetailsSnapshot, contract: Any) -> None:
        payload = (contract, snapshot)
        aliases = {
            snapshot.symbol.upper(),
            (snapshot.local_symbol or "").upper(),
            str(snapshot.metadata.get("root_symbol", "")).upper(),
        }
        for alias in aliases:
            if alias:
                self._resolved_contracts[alias] = payload

    def _search_plan(self, symbol: str) -> list[_FutureSearch]:
        if symbol in {"CL", "MCL"} or symbol.startswith("CL"):
            return [_FutureSearch(canonical_symbol="CL", roots=("CL",), exchanges=self.config.wti_exchanges)]
        if symbol in {"BRN", "BZ", "COIL"} or symbol.startswith("BRN") or symbol.startswith("BZ"):
            return [
                _FutureSearch(
                    canonical_symbol="BRN",
                    roots=self.config.brent_alias_roots,
                    exchanges=self.config.brent_exchanges,
                )
            ]
        return [
            _FutureSearch(
                canonical_symbol=symbol,
                roots=(symbol,),
                exchanges=(self.config.default_exchange,),
            )
        ]

    def _future_searches(self) -> list[_FutureSearch]:
        return [
            _FutureSearch(canonical_symbol="CL", roots=("CL",), exchanges=self.config.wti_exchanges),
            _FutureSearch(canonical_symbol="BRN", roots=self.config.brent_alias_roots, exchanges=self.config.brent_exchanges),
        ]

    def _find_contract_details(self, search: _FutureSearch) -> list[Any]:
        assert self._api is not None
        seen: set[int | str] = set()
        rows: list[Any] = []
        for root in search.roots:
            root_rows: list[Any] = []
            for exchange in search.exchanges:
                template = self._api.Future(symbol=root, exchange=exchange, currency=self.config.default_currency)
                try:
                    details = self._ib.reqContractDetails(template)
                except Exception:
                    continue
                for detail in details or []:
                    contract = getattr(detail, "contract", None)
                    if contract is None:
                        continue
                    key = getattr(contract, "conId", None) or getattr(contract, "localSymbol", None) or id(contract)
                    if key in seen:
                        continue
                    seen.add(key)
                    root_rows.append(detail)
                if root_rows:
                    break
            if root_rows:
                rows.extend(root_rows)
                break
        return rows

    def _choose_front_contract(self, details: list[Any]) -> Any | None:
        current_time = datetime.now(tz=UTC)
        ranked: list[tuple[datetime, Any]] = []
        for detail in details:
            contract = getattr(detail, "contract", None)
            if contract is None:
                continue
            expiry = self._parse_contract_month(getattr(contract, "lastTradeDateOrContractMonth", None))
            if expiry is None:
                expiry = self._parse_contract_month(getattr(detail, "realExpirationDate", None))
            if expiry is not None and expiry < current_time:
                continue
            ranked.append((expiry or datetime.max.replace(tzinfo=UTC), detail))
        if not ranked:
            return None
        ranked.sort(key=lambda item: (item[0], str(getattr(getattr(item[1], "contract", None), "localSymbol", ""))))
        return ranked[0][1]

    def _detail_to_snapshot(self, detail: Any, *, canonical_symbol: str) -> ContractDetailsSnapshot:
        contract = getattr(detail, "contract", None)
        root_symbol = str(getattr(contract, "symbol", "")) or canonical_symbol
        local_symbol = str(getattr(contract, "localSymbol", "")) or None
        contract_month = str(getattr(contract, "lastTradeDateOrContractMonth", "")) or None
        multiplier = self._coerce_float(getattr(contract, "multiplier", None))
        min_size = self._coerce_float(getattr(detail, "minSize", None))
        size_increment = self._coerce_float(getattr(detail, "sizeIncrement", None))
        return ContractDetailsSnapshot(
            broker=self.broker_name,
            symbol=canonical_symbol,
            local_symbol=local_symbol,
            exchange=str(getattr(contract, "exchange", "")) or None,
            currency=str(getattr(contract, "currency", "")) or self.config.default_currency,
            contract_month=contract_month,
            multiplier=multiplier,
            min_quantity=min_size if min_size not in (None, 0.0) else 1.0,
            max_quantity=None,
            quantity_step=size_increment if size_increment not in (None, 0.0) else 1.0,
            metadata={
                "conid": getattr(contract, "conId", None),
                "root_symbol": root_symbol,
                "trading_class": getattr(contract, "tradingClass", None),
                "market_name": getattr(detail, "marketName", None),
                "min_tick": self._coerce_float(getattr(detail, "minTick", None)),
                "long_name": getattr(detail, "longName", None),
            },
        )

    def _bars_to_frame(self, bars: Any, *, symbol: str) -> pd.DataFrame:
        if bars is None:
            return pd.DataFrame()
        if self._api is not None and hasattr(self._api, "util") and hasattr(self._api.util, "df"):
            frame = self._api.util.df(bars)
        else:
            frame = pd.DataFrame(
                {
                    "date": [getattr(bar, "date", None) for bar in bars],
                    "open": [getattr(bar, "open", None) for bar in bars],
                    "high": [getattr(bar, "high", None) for bar in bars],
                    "low": [getattr(bar, "low", None) for bar in bars],
                    "close": [getattr(bar, "close", None) for bar in bars],
                    "volume": [getattr(bar, "volume", None) for bar in bars],
                    "average": [getattr(bar, "average", None) for bar in bars],
                    "barCount": [getattr(bar, "barCount", None) for bar in bars],
                }
            )
        if frame is None:
            return pd.DataFrame()
        if frame.empty:
            return frame
        if "date" in frame.columns:
            frame["timestamp"] = pd.to_datetime(frame["date"], utc=True)
            frame = frame.drop(columns=["date"])
        elif "time" in frame.columns:
            frame["timestamp"] = pd.to_datetime(frame["time"], utc=True)
            frame = frame.drop(columns=["time"])
        else:
            raise ValueError("IB historical bars did not include a timestamp-like column.")
        frame = frame.set_index("timestamp")
        return normalize_price_frame(frame, broker=self.broker_name, symbol=symbol)

    def _ticker_to_quote(self, symbol: str, ticker: Any) -> QuoteSnapshot:
        timestamp = getattr(ticker, "time", None)
        if isinstance(timestamp, pd.Timestamp):
            timestamp_value = timestamp.to_pydatetime()
        elif isinstance(timestamp, datetime):
            timestamp_value = timestamp if timestamp.tzinfo is not None else timestamp.replace(tzinfo=UTC)
        else:
            timestamp_value = None
        return QuoteSnapshot(
            broker=self.broker_name,
            symbol=symbol,
            bid=self._coerce_float(getattr(ticker, "bid", None)),
            ask=self._coerce_float(getattr(ticker, "ask", None)),
            last=self._coerce_float(getattr(ticker, "last", None)),
            close=self._coerce_float(getattr(ticker, "close", None)),
            timestamp=timestamp_value,
        )

    @classmethod
    def _quote_has_no_prices(cls, ticker: Any) -> bool:
        return all(
            cls._coerce_float(getattr(ticker, field, None)) is None
            for field in ("bid", "ask", "last", "close")
        )

    def _wait_for_ticker_prices(self, ticker: Any, *, timeout_seconds: float = 2.0) -> Any | None:
        if ticker is None:
            return None
        deadline = time.monotonic() + max(timeout_seconds, 0.0)
        while self._quote_has_no_prices(ticker) and time.monotonic() < deadline:
            sleep_method = getattr(self._ib, "sleep", None)
            if callable(sleep_method):
                sleep_method(0.2)
            else:
                time.sleep(0.2)
        return ticker

    def _wait_for_trade_status(self, trade: Any, *, timeout_seconds: float = 5.0) -> None:
        deadline = time.monotonic() + max(timeout_seconds, 0.0)
        terminal_statuses = {"Filled", "Cancelled", "ApiCancelled", "Inactive"}
        while time.monotonic() < deadline:
            status = str(getattr(getattr(trade, "orderStatus", None), "status", "") or "")
            if status in terminal_statuses:
                return
            sleep_method = getattr(self._ib, "sleep", None)
            if callable(sleep_method):
                sleep_method(0.2)
            else:
                time.sleep(0.2)

    def _request_history_frame(
        self,
        contract: Any,
        *,
        symbol: str,
        end_date_time: str,
        duration: str,
        bar_size: str,
    ) -> pd.DataFrame:
        bars = self._ib.reqHistoricalData(
            contract,
            endDateTime=end_date_time,
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=self.config.historical_what_to_show,
            useRTH=False,
            formatDate=2,
            keepUpToDate=False,
            chartOptions=[],
        )
        return self._bars_to_frame(bars, symbol=symbol)

    def _fetch_rates_chunked(
        self,
        contract: Any,
        *,
        symbol: str,
        bar_size: str,
        bar_delta: timedelta,
        count: int,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        end_date_time = ""
        remaining = count

        for _ in range(16):
            duration = "5 D" if remaining > 7_200 else self._duration_from_count(bar_delta, remaining)
            frame = self._request_history_frame(
                contract,
                symbol=symbol,
                end_date_time=end_date_time,
                duration=duration,
                bar_size=bar_size,
            )
            if frame.empty:
                break
            frames.append(frame)
            combined = pd.concat(frames).sort_index()
            combined = combined[~combined.index.duplicated(keep="last")]
            if len(combined) >= count:
                return combined.tail(count)
            oldest = frame.index.min()
            end_date_time = (oldest - bar_delta).strftime("%Y%m%d %H:%M:%S UTC")
            remaining = count - len(combined)

        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames).sort_index()
        combined = combined[~combined.index.duplicated(keep="last")]
        return combined.tail(count)

    @staticmethod
    def _should_chunk_history(bar_delta: timedelta, count: int) -> bool:
        return bar_delta <= timedelta(minutes=1) and count > 10_000

    def _build_order(self, request: OrderRequest) -> Any:
        assert self._api is not None
        action = self._normalize_side(request.side)
        order_type = request.order_type.upper()
        if order_type == "MKT":
            order = self._api.MarketOrder(action, request.quantity)
        elif order_type == "LMT":
            if request.limit_price is None:
                raise ValueError("Limit orders require `limit_price`.")
            order = self._api.LimitOrder(action, request.quantity, request.limit_price)
        elif order_type == "STP":
            if request.stop_price is None:
                raise ValueError("Stop orders require `stop_price`.")
            order = self._api.StopOrder(action, request.quantity, request.stop_price)
        else:
            raise ValueError(f"Unsupported IB order type: {request.order_type}")
        setattr(order, "tif", request.tif)
        setattr(order, "orderRef", request.tag)
        if "outside_rth" in request.metadata:
            setattr(order, "outsideRth", bool(request.metadata["outside_rth"]))
        return order

    def _can_afford_min_contract(
        self,
        *,
        symbol: str,
        minimum_quantity: float,
        entry_price: float,
        stop_distance_pct: float,
        multiplier: float,
        equity: float,
        risk_config: RiskConfig,
    ) -> bool:
        snapshot = self.account_snapshot()
        available_funds = snapshot.buying_power or snapshot.equity or equity
        stop_risk = entry_price * max(stop_distance_pct, 1e-6) * max(multiplier, 1.0) * minimum_quantity
        if stop_risk > (equity * risk_config.max_exposure_fraction):
            return False
        margin = self.estimate_initial_margin(symbol, quantity=minimum_quantity)
        if margin is not None:
            return available_funds >= margin
        estimated_notional = entry_price * max(multiplier, 1.0) * minimum_quantity
        return available_funds >= (estimated_notional * 0.25)

    def _account_summary_by_tag(self) -> dict[str, Any]:
        summary_method = getattr(self._exec_ib, "accountSummary", None)
        if summary_method is None:
            return {}
        try:
            values = summary_method(self.config.account or "")
        except TypeError:
            values = summary_method()
        account_id = self.config.account
        if account_id is None:
            account_id = self._discover_account_id({})
        summary: dict[str, Any] = {}
        for value in values or []:
            value_account = getattr(value, "account", None)
            if account_id and value_account not in {None, "", account_id}:
                continue
            summary[str(getattr(value, "tag", ""))] = value
        return summary

    def _discover_account_id(self, summary: dict[str, Any]) -> str | None:
        for value in summary.values():
            account = str(getattr(value, "account", ""))
            if account:
                return account
        managed_accounts = getattr(self._ib, "managedAccounts", None)
        if callable(managed_accounts):
            accounts = managed_accounts()
        else:
            accounts = managed_accounts
        if isinstance(accounts, (list, tuple)) and accounts:
            return str(accounts[0])
        return self.config.account

    @staticmethod
    def _looks_like_paper_account(account_id: str | None, summary: dict[str, Any]) -> bool:
        account_upper = (account_id or "").upper()
        if account_upper.startswith("DU"):
            return True
        alias = summary.get("AccountAlias")
        alias_value = str(getattr(alias, "value", "")) if alias is not None else ""
        return "PAPER" in alias_value.upper()

    @staticmethod
    def _summary_float(summary: dict[str, Any], *tags: str) -> float | None:
        for tag in tags:
            value = summary.get(tag)
            if value is None:
                continue
            parsed = IBGatewayClient._coerce_float(getattr(value, "value", None))
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _summary_currency(summary: dict[str, Any], *tags: str) -> str | None:
        for tag in tags:
            value = summary.get(tag)
            if value is None:
                continue
            currency = str(getattr(value, "currency", ""))
            if currency:
                return currency
        return None

    def _is_connected(self) -> bool:
        if self._ib is None:
            return False
        method = getattr(self._ib, "isConnected", None)
        if callable(method):
            return bool(method())
        return False

    @classmethod
    def _resolve_timeframe(cls, timeframe: str) -> tuple[str, timedelta]:
        try:
            return cls._TIMEFRAME_MAP[timeframe.upper()]
        except KeyError as exc:
            raise ValueError(f"Unsupported IB timeframe: {timeframe}") from exc

    @staticmethod
    def _duration_from_count(bar_delta: timedelta, count: int) -> str:
        total = bar_delta * max(count + 5, 10)
        total_seconds = int(max(total.total_seconds(), 1))
        if total_seconds <= 86_400:
            return f"{total_seconds} S"
        total_days = max(1, int(total.total_seconds() // 86_400) + 1)
        if total_days <= 30:
            return f"{total_days} D"
        if total_days <= 365:
            return f"{max(1, round(total_days / 30))} M"
        return f"{max(1, round(total_days / 365))} Y"

    @staticmethod
    def _parse_contract_month(value: object) -> datetime | None:
        if value in (None, ""):
            return None
        raw = str(value).strip()
        for width, fmt in ((8, "%Y%m%d"), (6, "%Y%m")):
            if len(raw) < width:
                continue
            try:
                parsed = datetime.strptime(raw[:width], fmt)
                return parsed.replace(tzinfo=UTC)
            except ValueError:
                continue
        return None

    @staticmethod
    def _canonical_symbol(symbol: str) -> str:
        normalized = symbol.upper()
        if normalized in {"BRN", "BZ", "COIL"}:
            return "BRN"
        return "CL" if normalized.startswith("CL") else normalized

    @staticmethod
    def _normalize_side(side: str) -> str:
        normalized = side.upper()
        if normalized not in {"BUY", "SELL"}:
            raise ValueError(f"Unsupported order side: {side}")
        return normalized

    @staticmethod
    def _coerce_float(value: object) -> float | None:
        if value in (None, "", "N/A"):
            return None
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            return None
        return parsed if isfinite(parsed) else None

    @staticmethod
    def _import_ib_async() -> Any:
        try:
            import ib_async

            return ib_async
        except ImportError as exc:  # pragma: no cover
            raise ImportError("ib_async is required for Interactive Brokers support.") from exc
