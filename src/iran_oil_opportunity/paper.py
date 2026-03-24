"""Filesystem-backed paper execution."""

from __future__ import annotations

import csv
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from iran_oil_opportunity.config import RiskConfig, StrategyConfig
from iran_oil_opportunity.features import build_feature_frame
from iran_oil_opportunity.risk import recommend_mt5_volume, size_notional_fraction
from iran_oil_opportunity.strategy import IranOilShockStrategy


@dataclass(frozen=True, slots=True)
class PaperState:
    """Persistent paper state for one symbol."""

    equity: float
    peak_equity: float
    position: int
    position_fraction: float
    last_price: float | None
    last_timestamp: str | None
    daily_start_equity: float
    daily_pnl: float
    consecutive_losses: int

    @classmethod
    def initial(cls, initial_equity: float) -> PaperState:
        return cls(
            equity=initial_equity,
            peak_equity=initial_equity,
            position=0,
            position_fraction=0.0,
            last_price=None,
            last_timestamp=None,
            daily_start_equity=initial_equity,
            daily_pnl=0.0,
            consecutive_losses=0,
        )


@dataclass(frozen=True, slots=True)
class PaperSignalRecord:
    """Signal log row."""

    timestamp: str
    symbol: str
    signal: int
    regime: str
    conviction: float
    reason: str
    close: float
    stress_level: float
    equity: float
    drawdown: float
    recommended_volume: float | None


@dataclass(frozen=True, slots=True)
class PaperTradeRecord:
    """Trade log row."""

    timestamp: str
    symbol: str
    action: str
    target_position: int
    position_fraction: float
    price: float
    equity_after: float
    recommended_volume: float | None
    routed_volume_delta: float | None


class LocalPaperStore:
    """Persist paper state and CSV journals."""

    def __init__(self, root_dir: str | Path):
        self.root_dir = Path(root_dir)

    @property
    def state_path(self) -> Path:
        return self.root_dir / "state.json"

    @property
    def signals_path(self) -> Path:
        return self.root_dir / "signals.csv"

    @property
    def trades_path(self) -> Path:
        return self.root_dir / "trades.csv"

    def load_state(self, initial_equity: float) -> PaperState:
        if not self.state_path.exists():
            return PaperState.initial(initial_equity)
        payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        return PaperState(**payload)

    def save_state(self, state: PaperState) -> Path:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(asdict(state), indent=2, sort_keys=True), encoding="utf-8")
        return self.state_path

    def append_signal(self, record: PaperSignalRecord) -> Path:
        return self._append_csv(self.signals_path, asdict(record))

    def append_trade(self, record: PaperTradeRecord) -> Path:
        return self._append_csv(self.trades_path, asdict(record))

    def _append_csv(self, path: Path, row: dict[str, object]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists()
        with path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
            if write_header:
                writer.writeheader()
            writer.writerow(row)
        return path


def run_paper_step(
    *,
    symbol: str,
    frame: pd.DataFrame,
    store: LocalPaperStore,
    strategy_config: StrategyConfig,
    risk_config: RiskConfig,
    symbol_info: Any | None = None,
    broker: Any | None = None,
    submit_orders: bool = False,
) -> dict[str, object]:
    """Evaluate one paper-trading step and persist journals."""

    feature_frame = build_feature_frame(frame, strategy_config=strategy_config)
    latest = feature_frame.iloc[-1]
    timestamp = feature_frame.index[-1].isoformat()
    close = float(latest["close"])
    strategy = IranOilShockStrategy(strategy_config)
    decision = strategy.decide(feature_frame)
    state = store.load_state(risk_config.initial_equity)

    if state.last_price is not None and state.position != 0:
        mark_return = (close / state.last_price) - 1.0
        pnl = state.equity * state.position_fraction * state.position * mark_return
        next_equity = state.equity + pnl
        next_daily_pnl = state.daily_pnl + pnl
        loss_streak = state.consecutive_losses
        if pnl < 0.0:
            loss_streak += 1
        elif pnl > 0.0:
            loss_streak = 0
        state = PaperState(
            equity=next_equity,
            peak_equity=max(state.peak_equity, next_equity),
            position=state.position,
            position_fraction=state.position_fraction,
            last_price=state.last_price,
            last_timestamp=state.last_timestamp,
            daily_start_equity=state.daily_start_equity,
            daily_pnl=next_daily_pnl,
            consecutive_losses=loss_streak,
        )

    drawdown = 1.0 - (state.equity / max(state.peak_equity, 1e-6))
    halt_reason: str | None = None
    if drawdown >= risk_config.max_drawdown:
        halt_reason = "max_drawdown"
    elif abs(state.daily_pnl) >= (risk_config.initial_equity * risk_config.max_daily_loss):
        halt_reason = "max_daily_loss"
    elif state.consecutive_losses >= risk_config.max_consecutive_losses:
        halt_reason = "max_consecutive_losses"

    if halt_reason is not None:
        decision_signal = 0
        decision_reason = f"risk_halt:{halt_reason}"
    else:
        decision_signal = decision.signal
        decision_reason = decision.reason

    plan = size_notional_fraction(
        equity=state.equity,
        stop_distance_pct=decision.stop_distance_pct,
        risk_config=risk_config,
    )
    recommended_volume = recommend_mt5_volume(
        symbol_info=symbol_info,
        equity=state.equity,
        entry_price=close,
        stop_distance_pct=decision.stop_distance_pct,
        risk_config=risk_config,
    )
    target_fraction = plan.notional_fraction if decision_signal != 0 else 0.0
    action = "hold"
    routed_volume_delta: float | None = None
    if decision_signal != state.position or abs(target_fraction - state.position_fraction) > 0.05:
        action = {
            (0, 1): "open_long",
            (0, -1): "open_short",
            (1, 0): "close_long",
            (-1, 0): "close_short",
            (1, -1): "reverse_to_short",
            (-1, 1): "reverse_to_long",
        }.get((state.position, decision_signal), "rebalance")

        if submit_orders and broker is not None and recommended_volume is not None:
            current_volume = float(broker.get_net_position(symbol))
            desired_volume = float(decision_signal * recommended_volume)
            routed_volume_delta = desired_volume - current_volume
            if abs(routed_volume_delta) >= 1e-9:
                if routed_volume_delta > 0.0:
                    broker.submit_market_order(symbol=symbol, side="BUY", volume=abs(routed_volume_delta))
                else:
                    broker.submit_market_order(symbol=symbol, side="SELL", volume=abs(routed_volume_delta))

    state = PaperState(
        equity=state.equity,
        peak_equity=max(state.peak_equity, state.equity),
        position=decision_signal,
        position_fraction=target_fraction,
        last_price=close,
        last_timestamp=timestamp,
        daily_start_equity=state.daily_start_equity,
        daily_pnl=state.daily_pnl,
        consecutive_losses=state.consecutive_losses,
    )
    store.save_state(state)
    store.append_signal(
        PaperSignalRecord(
            timestamp=timestamp,
            symbol=symbol,
            signal=decision_signal,
            regime=decision.regime,
            conviction=decision.conviction,
            reason=decision_reason,
            close=close,
            stress_level=float(latest.get("stress_level", 0.0) or 0.0),
            equity=state.equity,
            drawdown=drawdown,
            recommended_volume=recommended_volume,
        )
    )
    if action != "hold":
        store.append_trade(
            PaperTradeRecord(
                timestamp=timestamp,
                symbol=symbol,
                action=action,
                target_position=decision_signal,
                position_fraction=target_fraction,
                price=close,
                equity_after=state.equity,
                recommended_volume=recommended_volume,
                routed_volume_delta=routed_volume_delta,
            )
        )

    return {
        "timestamp": timestamp,
        "symbol": symbol,
        "signal": decision_signal,
        "regime": decision.regime,
        "reason": decision_reason,
        "action": action,
        "equity": round(state.equity, 2),
        "drawdown": round(drawdown, 4),
        "close": close,
        "recommended_volume": recommended_volume,
        "routed_volume_delta": routed_volume_delta,
    }
