"""Prediction-market polling for de-escalation and disruption probabilities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

import pandas as pd
import requests


@dataclass(frozen=True, slots=True)
class MarketSnapshot:
    """Normalized Polymarket-like market row."""

    question: str
    yes_probability: float
    event_score: float


def fetch_polymarket_markets(*, limit: int = 200, session: requests.Session | None = None) -> list[dict[str, object]]:
    """Fetch active markets from Polymarket's public gamma API."""

    http = session or requests.Session()
    response = http.get(
        "https://gamma-api.polymarket.com/markets",
        params={"active": "true", "closed": "false", "limit": str(limit)},
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, list) else []


def summarize_oil_event_bias(markets: list[dict[str, object]]) -> tuple[list[MarketSnapshot], pd.DataFrame]:
    """Convert market questions into an oil-direction score."""

    rows: list[MarketSnapshot] = []
    for market in markets:
        question = str(market.get("question") or market.get("title") or "").strip()
        if not question:
            continue
        normalized = question.lower()
        if not any(token in normalized for token in ("iran", "hormuz", "ceasefire", "oil", "kharg")):
            continue
        yes_probability = _extract_yes_probability(market)
        event_score = _question_to_event_score(question, yes_probability)
        rows.append(
            MarketSnapshot(
                question=question,
                yes_probability=round(yes_probability, 4),
                event_score=round(event_score, 4),
            )
        )
    summary = pd.DataFrame(
        [
            {
                "timestamp": datetime.now(tz=UTC),
                "prediction_market_score": 0.0 if not rows else sum(item.event_score for item in rows) / len(rows),
                "prediction_market_count": len(rows),
            }
        ]
    ).set_index("timestamp")
    return rows, summary


def _extract_yes_probability(payload: dict[str, object]) -> float:
    for key in ("outcomePrices", "outcome_prices"):
        raw = payload.get(key)
        if raw is None:
            continue
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                continue
        else:
            parsed = raw
        if isinstance(parsed, list) and parsed:
            try:
                return float(parsed[0])
            except (TypeError, ValueError):
                continue
    for key in ("yes_price", "probability"):
        raw = payload.get(key)
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return 0.5


def _question_to_event_score(question: str, yes_probability: float) -> float:
    normalized = question.lower()
    if any(token in normalized for token in ("ceasefire", "negotiation", "productive talks", "deal")):
        return -(yes_probability - 0.5) * 2.0
    if any(token in normalized for token in ("hormuz", "closure", "kharg", "strike", "regime fall")):
        return (yes_probability - 0.5) * 2.0
    return 0.0
