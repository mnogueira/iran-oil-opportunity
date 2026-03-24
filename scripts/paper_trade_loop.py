"""Continuous oil paper-trading loop."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.alternative_data import load_alt_data_frame, merge_alt_data
from iran_oil_opportunity.config import BrokerConfig, PaperServiceConfig, RiskConfig, StrategyConfig
from iran_oil_opportunity.discovery import choose_brent_wti_pair, choose_primary_oil_symbol
from iran_oil_opportunity.market_data import join_spread_context
from iran_oil_opportunity.monitoring import append_jsonl, build_paper_service_paths, write_json_atomic
from iran_oil_opportunity.mt5_client import MT5Connection
from iran_oil_opportunity.paper import LocalPaperStore, run_paper_step


DEFAULT_LOCAL_NEWS_SCORES = REPO_ROOT / "data" / "processed" / "local_news_scores.csv"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the oil paper-trading loop.")
    parser.add_argument("--output-dir", default=".tradebot/paper_oil_mt5")
    parser.add_argument("--timeframe", default="H1")
    parser.add_argument("--bars-count", type=int, default=750)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--symbol")
    parser.add_argument("--secondary-symbol")
    parser.add_argument("--alt-data-csv")
    parser.add_argument("--submit-orders", action="store_true")
    parser.add_argument("--kill-switch-path")
    parser.add_argument("--mt5-login", type=int)
    parser.add_argument("--mt5-password")
    parser.add_argument("--mt5-server")
    parser.add_argument("--mt5-path")
    parser.add_argument("--allow-non-demo", action="store_true")
    return parser


def resolve_alt_data_path(raw_path: str | None) -> Path | None:
    if raw_path:
        return Path(raw_path)
    return DEFAULT_LOCAL_NEWS_SCORES


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    alt_data_path = resolve_alt_data_path(args.alt_data_csv)
    service_cfg = PaperServiceConfig(
        output_dir=Path(args.output_dir),
        timeframe=args.timeframe,
        bars_count=args.bars_count,
        poll_seconds=args.poll_seconds,
        submit_orders=args.submit_orders,
        symbol=args.symbol,
        secondary_symbol=args.secondary_symbol,
        alt_data_csv=alt_data_path,
    )
    broker_cfg = BrokerConfig(
        login=args.mt5_login,
        password=args.mt5_password,
        server=args.mt5_server,
        path=args.mt5_path,
        require_demo=not args.allow_non_demo,
    )
    paths = build_paper_service_paths(
        service_cfg.output_dir,
        kill_switch_path=(None if args.kill_switch_path is None else Path(args.kill_switch_path)),
    )
    paths.output_dir.mkdir(parents=True, exist_ok=True)
    write_json_atomic(
        paths.status_path,
        {"runner_state": "started", "started_at": datetime.now(tz=UTC).isoformat()},
    )

    try:
        with MT5Connection(broker_cfg) as connection:
            symbols = connection.list_symbols()
            primary_symbol = service_cfg.symbol or choose_primary_oil_symbol(symbols, preferred="brent")
            brent, wti = choose_brent_wti_pair(symbols)
            secondary_symbol = service_cfg.secondary_symbol
            if secondary_symbol is None and primary_symbol == brent:
                secondary_symbol = wti
            elif secondary_symbol is None and primary_symbol == wti:
                secondary_symbol = brent
            if primary_symbol is None:
                raise RuntimeError("No oil-like MT5 symbol was discovered.")

            store = LocalPaperStore(service_cfg.output_dir / primary_symbol)
            strategy_cfg = StrategyConfig()
            risk_cfg = RiskConfig()

            while True:
                if paths.kill_switch_path.exists():
                    write_json_atomic(
                        paths.status_path,
                        {
                            "runner_state": "stopped",
                            "stop_reason": "kill_switch",
                            "last_symbol": primary_symbol,
                        },
                    )
                    return 0

                primary_frame = connection.fetch_rates(primary_symbol, service_cfg.timeframe, service_cfg.bars_count)
                if primary_frame.empty:
                    raise RuntimeError(f"No data returned for {primary_symbol}.")
                if secondary_symbol:
                    secondary_frame = connection.fetch_rates(secondary_symbol, service_cfg.timeframe, service_cfg.bars_count)
                else:
                    secondary_frame = primary_frame.iloc[0:0].copy()
                combined = join_spread_context(primary_frame, secondary_frame)
                if service_cfg.alt_data_csv is not None and service_cfg.alt_data_csv.exists():
                    combined = merge_alt_data(combined, load_alt_data_frame(service_cfg.alt_data_csv))

                result = run_paper_step(
                    symbol=primary_symbol,
                    frame=combined,
                    store=store,
                    strategy_config=strategy_cfg,
                    risk_config=risk_cfg,
                    symbol_info=connection.symbol_info(primary_symbol),
                    broker=connection,
                    submit_orders=service_cfg.submit_orders,
                )
                heartbeat = {
                    "timestamp": datetime.now(tz=UTC).isoformat(),
                    "runner_state": "running",
                    "symbol": primary_symbol,
                    "secondary_symbol": secondary_symbol,
                    "last_signal": result["signal"],
                    "last_action": result["action"],
                    "last_reason": result["reason"],
                }
                append_jsonl(paths.event_log, result)
                write_json_atomic(paths.heartbeat_path, heartbeat)
                write_json_atomic(
                    paths.status_path,
                    {
                        "runner_state": "running",
                        "last_heartbeat_at": heartbeat["timestamp"],
                        "last_signal": result["signal"],
                        "last_action": result["action"],
                        "last_reason": result["reason"],
                        "last_symbol": primary_symbol,
                    },
                )
                time.sleep(max(1, service_cfg.poll_seconds))
    except Exception as exc:  # pragma: no cover - operational path
        write_json_atomic(paths.status_path, {"runner_state": "failed", "failure": repr(exc)})
        raise


if __name__ == "__main__":
    raise SystemExit(main())
