"""Fetch Polymarket-derived event scores into CSV."""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.alternative_data import write_alt_data
from iran_oil_opportunity.monitoring import append_jsonl, write_json_atomic
from iran_oil_opportunity.prediction_markets import fetch_polymarket_markets, summarize_oil_event_bias


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll Polymarket for oil-war probabilities.")
    parser.add_argument("--markets-output", default="data/processed/polymarket_markets.csv")
    parser.add_argument("--scores-output", default="data/processed/polymarket_scores.csv")
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--status-output")
    parser.add_argument("--heartbeat-output")
    parser.add_argument("--event-log-output")
    parser.add_argument("--kill-switch-path")
    parser.add_argument("--once", action="store_true")
    return parser


def run_once(args: argparse.Namespace) -> dict[str, object]:
    rows, summary = summarize_oil_event_bias(fetch_polymarket_markets())
    markets_target = write_alt_data(pd.DataFrame([asdict(row) for row in rows]), args.markets_output)
    scores_target = write_alt_data(summary, args.scores_output)
    prediction_market_score = 0.0 if summary.empty else float(summary.iloc[-1]["prediction_market_score"])
    return {
        "markets": len(rows),
        "markets_output": str(markets_target),
        "scores_output": str(scores_target),
        "prediction_market_score": round(prediction_market_score, 4),
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    _publish_service_state(args, runner_state="started", last_action="startup", last_reason="boot")
    while True:
        if _kill_switch_is_set(args):
            _publish_service_state(args, runner_state="stopped", last_action="stop", last_reason="kill_switch")
            return 0
        try:
            summary = run_once(args)
            print(summary)
            _publish_service_state(
                args,
                runner_state="running",
                last_action="poll",
                last_reason="success",
                summary=summary,
            )
        except Exception as exc:  # pragma: no cover
            failure_payload = {
                "error": repr(exc),
                "markets_output": args.markets_output,
                "scores_output": args.scores_output,
            }
            print(failure_payload)
            _publish_service_state(
                args,
                runner_state="running",
                last_action="poll",
                last_reason="error",
                summary=failure_payload,
                failure=repr(exc),
            )
            if args.once:
                return 1
        if args.once:
            return 0
        time.sleep(max(30, args.poll_seconds))


def _kill_switch_is_set(args: argparse.Namespace) -> bool:
    if not args.kill_switch_path:
        return False
    return Path(args.kill_switch_path).exists()


def _publish_service_state(
    args: argparse.Namespace,
    *,
    runner_state: str,
    last_action: str,
    last_reason: str,
    summary: dict[str, object] | None = None,
    failure: str | None = None,
) -> None:
    timestamp = datetime.now(tz=UTC).isoformat()
    payload = {
        "timestamp": timestamp,
        "pid": os.getpid(),
        "runner_state": runner_state,
        "last_action": last_action,
        "last_reason": last_reason,
    }
    if summary:
        payload.update(summary)
    if failure is not None:
        payload["failure"] = failure
    if args.status_output:
        write_json_atomic(Path(args.status_output), payload)
    if args.heartbeat_output:
        write_json_atomic(Path(args.heartbeat_output), payload)
    if args.event_log_output:
        append_jsonl(Path(args.event_log_output), payload)


if __name__ == "__main__":
    raise SystemExit(main())
