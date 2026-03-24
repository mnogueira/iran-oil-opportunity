"""Run the strategy backtest and optionally write artifacts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.backtest import run_backtest
from iran_oil_opportunity.config import RiskConfig, StrategyConfig
from iran_oil_opportunity.market_data import load_price_frame


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest the oil-shock strategy.")
    parser.add_argument(
        "--input",
        default="data/reference/fred_brent_ovx_q1_2026.csv",
        help="CSV input with at least date/timestamp and close columns.",
    )
    parser.add_argument("--equity-output", default="artifacts/backtest/equity_curve.csv")
    parser.add_argument("--summary-output", default="artifacts/backtest/summary.json")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    frame = load_price_frame(args.input)
    result = run_backtest(frame, strategy_config=StrategyConfig(), risk_config=RiskConfig())

    equity_target = Path(args.equity_output)
    summary_target = Path(args.summary_output)
    equity_target.parent.mkdir(parents=True, exist_ok=True)
    summary_target.parent.mkdir(parents=True, exist_ok=True)
    result.equity_curve.to_csv(equity_target)

    summary = {
        "input": args.input,
        "total_return": result.total_return,
        "annualized_return": result.annualized_return,
        "annualized_volatility": result.annualized_volatility,
        "sharpe": result.sharpe,
        "max_drawdown": result.max_drawdown,
        "trades": result.trades,
        "win_rate": result.win_rate,
        "equity_output": str(equity_target),
    }
    summary_target.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
