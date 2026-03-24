"""Broker symbol discovery helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass


BRENT_ALIASES = (
    "BRENT",
    "UKOIL",
    "XBRUSD",
    "BRN",
    "BRENTCMD",
    "BRENTM",
)
WTI_ALIASES = (
    "WTI",
    "USOIL",
    "XTIUSD",
    "WTICMD",
    "LIGHT",
    "USCRUDE",
    "CL",
)
OIL_ETF_ALIASES = ("USO", "BNO", "XLE", "OIH")
OIL_STOCK_ALIASES = ("XOM", "CVX", "PBR", "SHEL", "BP", "COP", "PETR4")
AIRLINE_ALIASES = ("JETS", "UAL", "DAL", "AAL", "AZUL4", "GOLL4")
CATEGORY_PRIORITY = {
    "brent": 0,
    "wti": 1,
    "oil_etf": 2,
    "oil_stock": 3,
    "airline": 4,
}


@dataclass(frozen=True, slots=True)
class InstrumentCandidate:
    """Ranked symbol candidate."""

    symbol: str
    category: str
    score: int
    reason: str


def normalize_symbol(symbol: str) -> str:
    """Normalize broker symbol names so aliases compare more reliably."""

    return re.sub(r"[^A-Z0-9]", "", symbol.upper())


def _score_alias(symbol: str, aliases: tuple[str, ...], *, category: str) -> InstrumentCandidate | None:
    normalized = normalize_symbol(symbol)
    best_score = -1
    best_reason = ""
    for alias in aliases:
        if normalized == alias:
            return InstrumentCandidate(symbol=symbol, category=category, score=100, reason=f"exact:{alias}")
        if normalized.startswith(alias):
            best_score = max(best_score, 85)
            best_reason = f"prefix:{alias}"
        elif alias in normalized:
            best_score = max(best_score, 70)
            best_reason = f"contains:{alias}"
    if best_score < 0:
        return None
    return InstrumentCandidate(symbol=symbol, category=category, score=best_score, reason=best_reason)


def discover_candidates(symbols: list[str]) -> list[InstrumentCandidate]:
    """Classify a raw broker symbol list."""

    candidates: list[InstrumentCandidate] = []
    for symbol in symbols:
        for category, aliases in (
            ("brent", BRENT_ALIASES),
            ("wti", WTI_ALIASES),
            ("oil_etf", OIL_ETF_ALIASES),
            ("oil_stock", OIL_STOCK_ALIASES),
            ("airline", AIRLINE_ALIASES),
        ):
            candidate = _score_alias(symbol, aliases, category=category)
            if candidate is not None:
                candidates.append(candidate)
                break
    return sorted(
        candidates,
        key=lambda item: (CATEGORY_PRIORITY.get(item.category, 99), -item.score, item.symbol),
    )


def choose_primary_oil_symbol(symbols: list[str], *, preferred: str = "brent") -> str | None:
    """Choose the best oil symbol for the primary strategy."""

    candidates = discover_candidates(symbols)
    preferred = preferred.lower()
    preferred_categories = ("brent", "wti") if preferred == "brent" else ("wti", "brent")
    for category in preferred_categories:
        for candidate in candidates:
            if candidate.category == category:
                return candidate.symbol
    return None


def choose_brent_wti_pair(symbols: list[str]) -> tuple[str | None, str | None]:
    """Choose a Brent/WTI pair if the broker exposes both."""

    candidates = discover_candidates(symbols)
    brent = next((candidate.symbol for candidate in candidates if candidate.category == "brent"), None)
    wti = next((candidate.symbol for candidate in candidates if candidate.category == "wti"), None)
    return brent, wti
