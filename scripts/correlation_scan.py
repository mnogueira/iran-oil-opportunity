"""Scan indirect oil-volatility trades from local CSVs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.cross_asset import analyze_cross_asset_opportunities, opportunities_to_frame
from iran_oil_opportunity.discovery import discover_candidates
from iran_oil_opportunity.market_data import load_price_frame


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan cross-asset oil shock opportunities.")
    parser.add_argument("--base-symbol", required=True)
    parser.add_argument("--output", default="artifacts/cross_asset/opportunities.csv")
    parser.add_argument(
        "--series",
        nargs="+",
        required=True,
        help="Pairs like SYMBOL=path/to/file.csv",
    )
    return parser


def parse_series(items: list[str]) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    for item in items:
        symbol, separator, raw_path = item.partition("=")
        if not separator:
            raise ValueError(f"Expected SYMBOL=path syntax, got: {item}")
        mapping[symbol] = Path(raw_path)
    return mapping


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    frames = {symbol: load_price_frame(path) for symbol, path in parse_series(args.series).items()}
    categories = {candidate.symbol: candidate.category for candidate in discover_candidates(list(frames.keys()))}
    opportunities = analyze_cross_asset_opportunities(frames, base_symbol=args.base_symbol, categories=categories)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    frame = opportunities_to_frame(opportunities)
    frame.to_csv(output, index=False)
    print(json.dumps({"rows": len(frame), "output": str(output)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
