"""Fetch and score local-language headlines into CSV outputs."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from iran_oil_opportunity.alternative_data import write_alt_data
from iran_oil_opportunity.config import HeadlineLLMConfig
from iran_oil_opportunity.headline_llm import KeywordHeadlineScorer, OpenAIHeadlineScorer
from iran_oil_opportunity.local_news import (
    aggregate_headline_scores,
    fetch_recent_headlines_with_status,
    headlines_to_frame,
    score_headlines,
    source_statuses_to_frame,
    write_headlines,
    write_source_statuses,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll local-language oil-war headlines.")
    parser.add_argument("--headlines-output", default="data/processed/local_news_headlines.csv")
    parser.add_argument("--scores-output", default="data/processed/local_news_scores.csv")
    parser.add_argument("--source-status-output", default="data/processed/local_news_source_status.csv")
    parser.add_argument("--poll-seconds", type=int, default=300)
    parser.add_argument("--max-items-per-source", type=int, default=20)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--model", default="gpt-5.4-mini")
    parser.add_argument("--heuristic-only", action="store_true")
    return parser


def build_scorer(args: argparse.Namespace) -> KeywordHeadlineScorer | OpenAIHeadlineScorer:
    if args.heuristic_only or not os.getenv("OPENAI_API_KEY"):
        return KeywordHeadlineScorer()
    return OpenAIHeadlineScorer(HeadlineLLMConfig(model=args.model))


def run_once(args: argparse.Namespace) -> dict[str, object]:
    scorer = build_scorer(args)
    raw, statuses = fetch_recent_headlines_with_status(max_items_per_source=args.max_items_per_source)
    scored = score_headlines(raw, scorer)
    headline_frame = headlines_to_frame(scored)
    score_frame = aggregate_headline_scores(scored)
    status_frame = source_statuses_to_frame(statuses)
    headlines_target = write_headlines(headline_frame, args.headlines_output)
    scores_target = write_alt_data(score_frame, args.scores_output)
    source_status_target = write_source_statuses(status_frame, args.source_status_output)
    return {
        "headlines": len(headline_frame),
        "headline_output": str(headlines_target),
        "score_rows": len(score_frame),
        "score_output": str(scores_target),
        "sources_checked": len(statuses),
        "sources_ok": sum(1 for status in statuses if status.ok),
        "sources_failed": sum(1 for status in statuses if not status.ok),
        "source_status_output": str(source_status_target),
        "scorer": scorer.__class__.__name__,
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    while True:
        summary = run_once(args)
        print(summary)
        if args.once:
            return 0
        time.sleep(max(30, args.poll_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
