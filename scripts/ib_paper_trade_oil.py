"""Run the oil-shock paper trader directly against IB Gateway."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.alternative_data import load_alt_data_frame, merge_alt_data
from iran_oil_opportunity.config import IBConfig, RiskConfig, StrategyConfig
from iran_oil_opportunity.ib_client import IBGatewayClient
from iran_oil_opportunity.market_data import join_spread_context
from iran_oil_opportunity.paper import LocalPaperStore, run_paper_step


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Paper trade CL/Brent futures through IB Gateway.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4002)
    parser.add_argument("--client-id", type=int, default=1)
    parser.add_argument("--account")
    parser.add_argument("--allow-live", action="store_true")
    parser.add_argument("--output-dir", default=".tradebot/paper_oil_ib")
    parser.add_argument("--timeframe", default="H1")
    parser.add_argument("--bars-count", type=int, default=750)
    parser.add_argument("--poll-seconds", type=int, default=30)
    parser.add_argument("--symbol", help="Canonical root symbol, usually CL or BRN.")
    parser.add_argument("--secondary-symbol", help="Optional spread context symbol, usually BRN or CL.")
    parser.add_argument("--alt-data-csv")
    parser.add_argument("--submit-orders", action="store_true")
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = IBGatewayClient(
        IBConfig(
            host=args.host,
            port=args.port,
            client_id=args.client_id,
            account=args.account,
            require_paper=not args.allow_live,
        )
    )
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with client:
        discovered = client.discover_oil_futures()
        if not discovered:
            raise RuntimeError("No CL/Brent futures contracts were discovered via IB Gateway.")

        primary_symbol = args.symbol or ("CL" if "CL" in discovered else next(iter(discovered)))
        secondary_symbol = args.secondary_symbol
        if secondary_symbol is None:
            secondary_symbol = "CL" if primary_symbol == "BRN" and "CL" in discovered else None
            if primary_symbol == "CL" and "BRN" in discovered:
                secondary_symbol = "BRN"

        alt_frame = None
        if args.alt_data_csv:
            alt_frame = load_alt_data_frame(args.alt_data_csv)

        store = LocalPaperStore(output_dir / primary_symbol)
        strategy_cfg = StrategyConfig()
        risk_cfg = RiskConfig()

        while True:
            primary_frame = client.fetch_rates(primary_symbol, args.timeframe, args.bars_count)
            if primary_frame.empty:
                raise RuntimeError(f"No IB data returned for {primary_symbol}.")
            if secondary_symbol is not None:
                secondary_frame = client.fetch_rates(secondary_symbol, args.timeframe, args.bars_count)
            else:
                secondary_frame = primary_frame.iloc[0:0].copy()

            combined = join_spread_context(primary_frame, secondary_frame)
            if alt_frame is not None and not alt_frame.empty:
                combined = merge_alt_data(combined, alt_frame)

            result = run_paper_step(
                symbol=primary_symbol,
                frame=combined,
                store=store,
                strategy_config=strategy_cfg,
                risk_config=risk_cfg,
                broker=client,
                submit_orders=args.submit_orders,
            )
            print(json.dumps(result, indent=2))

            if args.once:
                break
            time.sleep(max(1, args.poll_seconds))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
