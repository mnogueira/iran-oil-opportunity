"""Fetch Polymarket-derived event scores into CSV."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.alternative_data import write_alt_data
from iran_oil_opportunity.prediction_markets import fetch_polymarket_markets, summarize_oil_event_bias


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll Polymarket for oil-war probabilities.")
    parser.add_argument("--markets-output", default="data/processed/polymarket_markets.csv")
    parser.add_argument("--scores-output", default="data/processed/polymarket_scores.csv")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    rows, summary = summarize_oil_event_bias(fetch_polymarket_markets())
    markets_target = write_alt_data(pd.DataFrame([row.__dict__ for row in rows]), args.markets_output)
    scores_target = write_alt_data(summary, args.scores_output)
    print(
        {
            "markets": len(rows),
            "markets_output": str(markets_target),
            "scores_output": str(scores_target),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
