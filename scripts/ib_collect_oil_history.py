"""Collect oil futures history from Interactive Brokers Gateway."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.config import IBConfig
from iran_oil_opportunity.ib_client import IBGatewayClient
from iran_oil_opportunity.market_data import write_frame


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download CL/Brent futures history from IB Gateway.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4002)
    parser.add_argument("--client-id", type=int, default=260325)
    parser.add_argument("--account")
    parser.add_argument("--allow-live", action="store_true")
    parser.add_argument("--timeframe", default="H1")
    parser.add_argument("--bars", type=int, default=1500)
    parser.add_argument("--output-dir", default="data/ib")
    parser.add_argument("--symbols", nargs="*", default=["CL", "BRN"])
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
        account = client.account_snapshot()
        discovered = client.discover_oil_futures()
        series: list[dict[str, object]] = []
        for symbol in args.symbols:
            details = client.symbol_details(symbol)
            if details is None:
                series.append({"symbol": symbol, "status": "not_found"})
                continue
            frame = client.fetch_rates(symbol, args.timeframe, args.bars)
            target = write_frame(frame, output_dir / f"{symbol.lower()}_{args.timeframe.lower()}.csv")
            series.append(
                {
                    "symbol": symbol,
                    "rows": len(frame),
                    "output": str(target),
                    "local_symbol": details.local_symbol,
                    "contract_month": details.contract_month,
                    "exchange": details.exchange,
                }
            )

    print(
        json.dumps(
            {
                "account": asdict(account),
                "discovered": {symbol: asdict(snapshot) for symbol, snapshot in discovered.items()},
                "series": series,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
