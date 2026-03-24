"""CLI for research and paper-trading tasks."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass

from iran_oil_opportunity.backtest import run_backtest
from iran_oil_opportunity.config import BrokerConfig, RiskConfig, StrategyConfig
from iran_oil_opportunity.discovery import choose_brent_wti_pair, choose_primary_oil_symbol, discover_candidates
from iran_oil_opportunity.market_data import load_price_frame, write_frame
from iran_oil_opportunity.mt5_client import MT5Connection


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Iran oil opportunity CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    probe = subparsers.add_parser("probe-mt5", help="Probe MT5 connectivity and symbol exposure.")
    _add_broker_args(probe)

    discover = subparsers.add_parser("discover-symbols", help="Discover broker oil symbols.")
    _add_broker_args(discover)

    collect = subparsers.add_parser("collect-history", help="Collect historical bars from MT5.")
    _add_broker_args(collect)
    collect.add_argument("--symbol")
    collect.add_argument("--timeframe", default="H1")
    collect.add_argument("--bars", type=int, default=5000)
    collect.add_argument("--output", default="data/mt5/primary.csv")

    backtest = subparsers.add_parser("backtest", help="Backtest the strategy on a CSV input.")
    backtest.add_argument("--input", required=True)
    backtest.add_argument("--json", action="store_true")
    return parser


def _add_broker_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--mt5-login", type=int)
    parser.add_argument("--mt5-password")
    parser.add_argument("--mt5-server")
    parser.add_argument("--mt5-path")
    parser.add_argument("--allow-non-demo", action="store_true")


def _broker_config_from_args(args: argparse.Namespace) -> BrokerConfig:
    return BrokerConfig(
        login=args.mt5_login,
        password=args.mt5_password,
        server=args.mt5_server,
        path=args.mt5_path,
        require_demo=not args.allow_non_demo,
    )


def handle_probe(args: argparse.Namespace) -> int:
    with MT5Connection(_broker_config_from_args(args)) as connection:
        payload = {
            "account": asdict(connection.account_snapshot()),
            "terminal": connection.terminal_info(),
            "oil_symbols": discover_candidates(connection.list_symbols()),
        }
        print(json.dumps(payload, indent=2, default=_serialize_dataclass))
    return 0


def handle_discover(args: argparse.Namespace) -> int:
    with MT5Connection(_broker_config_from_args(args)) as connection:
        symbols = connection.list_symbols()
        brent, wti = choose_brent_wti_pair(symbols)
        payload = {
            "primary_preferred_brent": choose_primary_oil_symbol(symbols, preferred="brent"),
            "primary_preferred_wti": choose_primary_oil_symbol(symbols, preferred="wti"),
            "brent": brent,
            "wti": wti,
            "classified": [asdict(candidate) for candidate in discover_candidates(symbols)],
        }
        print(json.dumps(payload, indent=2))
    return 0


def handle_collect(args: argparse.Namespace) -> int:
    with MT5Connection(_broker_config_from_args(args)) as connection:
        symbol = args.symbol or choose_primary_oil_symbol(connection.list_symbols(), preferred="brent")
        if not symbol:
            raise RuntimeError("No oil-like symbol was discovered in MT5.")
        frame = connection.fetch_rates(symbol, args.timeframe, args.bars)
        frame["symbol"] = symbol
        target = write_frame(frame, args.output)
        print(json.dumps({"symbol": symbol, "rows": len(frame), "output": str(target)}, indent=2))
    return 0


def handle_backtest(args: argparse.Namespace) -> int:
    frame = load_price_frame(args.input)
    result = run_backtest(frame, strategy_config=StrategyConfig(), risk_config=RiskConfig())
    payload = {
        "total_return": result.total_return,
        "annualized_return": result.annualized_return,
        "annualized_volatility": result.annualized_volatility,
        "sharpe": result.sharpe,
        "max_drawdown": result.max_drawdown,
        "trades": result.trades,
        "win_rate": result.win_rate,
    }
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        for key, value in payload.items():
            print(f"{key}: {value}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "probe-mt5": handle_probe,
        "discover-symbols": handle_discover,
        "collect-history": handle_collect,
        "backtest": handle_backtest,
    }
    return handlers[args.command](args)


def _serialize_dataclass(item: object) -> object:
    if is_dataclass(item):
        return asdict(item)
    raise TypeError(f"Object of type {type(item).__name__} is not JSON serializable")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
