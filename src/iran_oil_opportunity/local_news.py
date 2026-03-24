"""Local-language news feed polling and aggregation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree

import pandas as pd
import requests

from iran_oil_opportunity.headline_llm import HeadlineAssessment, HeadlineScorer


@dataclass(frozen=True, slots=True)
class NewsSource:
    """Configured local-language source."""

    name: str
    language: str
    feed_url: str


@dataclass(frozen=True, slots=True)
class RawHeadline:
    """Fetched headline before scoring."""

    source: str
    language: str
    title: str
    link: str | None
    published_at: datetime


@dataclass(frozen=True, slots=True)
class ScoredHeadline:
    """Scored headline enriched by a small-model worker."""

    source: str
    language: str
    title: str
    link: str | None
    published_at: datetime
    translation: str
    escalation_score: float
    confidence: float
    entities: tuple[str, ...]


DEFAULT_LOCAL_NEWS_SOURCES = (
    NewsSource("IRNA", "fa", "https://www.irna.ir/rss"),
    NewsSource("Tasnim", "fa", "https://www.tasnimnews.com/fa/rss"),
    NewsSource("Mehr", "fa", "https://www.mehrnews.com/rss"),
    NewsSource("Khabar Online", "fa", "https://www.khabaronline.ir/rss"),
    NewsSource("Shafaq", "ar", "https://www.shafaq.com/ar/rss"),
)


def fetch_recent_headlines(
    *,
    sources: Iterable[NewsSource] = DEFAULT_LOCAL_NEWS_SOURCES,
    timeout_seconds: int = 15,
    session: requests.Session | None = None,
    max_items_per_source: int = 20,
) -> list[RawHeadline]:
    """Fetch headlines from configured RSS feeds."""

    http = session or requests.Session()
    headlines: list[RawHeadline] = []
    for source in sources:
        try:
            response = http.get(source.feed_url, timeout=timeout_seconds)
            response.raise_for_status()
            root = ElementTree.fromstring(response.text)
            items = root.findall(".//item") or root.findall(".//entry")
            for item in items[:max_items_per_source]:
                title = _find_text(item, ("title",))
                if not title:
                    continue
                headlines.append(
                    RawHeadline(
                        source=source.name,
                        language=source.language,
                        title=title.strip(),
                        link=_find_link(item),
                        published_at=_coerce_timestamp(_find_text(item, ("pubDate", "published", "updated"))),
                    )
                )
        except (requests.RequestException, ElementTree.ParseError, ValueError):
            continue
    deduped: dict[tuple[str, str], RawHeadline] = {}
    for headline in headlines:
        deduped[(headline.source, headline.title)] = headline
    return sorted(deduped.values(), key=lambda item: item.published_at, reverse=True)


def score_headlines(headlines: Iterable[RawHeadline], scorer: HeadlineScorer) -> list[ScoredHeadline]:
    """Apply translation and escalation scoring to fetched headlines."""

    scored: list[ScoredHeadline] = []
    for headline in headlines:
        assessment = scorer.score(text=headline.title, language=headline.language)
        scored.append(
            ScoredHeadline(
                source=headline.source,
                language=headline.language,
                title=headline.title,
                link=headline.link,
                published_at=headline.published_at,
                translation=assessment.translation,
                escalation_score=assessment.escalation_score,
                confidence=assessment.confidence,
                entities=assessment.entities,
            )
        )
    return scored


def aggregate_headline_scores(
    headlines: Iterable[ScoredHeadline],
    *,
    frequency: str = "1H",
) -> pd.DataFrame:
    """Bucket scored headlines into a time series for the trading engine."""

    rows = [
        {
            "timestamp": headline.published_at,
            "local_news_score": headline.escalation_score,
            "local_news_confidence": headline.confidence,
            "headline_count": 1,
        }
        for headline in headlines
    ]
    if not rows:
        return pd.DataFrame(columns=["local_news_score", "local_news_confidence", "headline_count"])
    frame = pd.DataFrame(rows).set_index("timestamp").sort_index()
    aggregated = frame.resample(frequency).agg(
        {
            "local_news_score": "mean",
            "local_news_confidence": "mean",
            "headline_count": "sum",
        }
    )
    return aggregated.dropna(how="all")


def headlines_to_frame(headlines: Iterable[ScoredHeadline]) -> pd.DataFrame:
    """Convert scored headlines to a row-level DataFrame."""

    rows = [
        {
            "timestamp": headline.published_at,
            "source": headline.source,
            "language": headline.language,
            "title": headline.title,
            "translation": headline.translation,
            "link": headline.link,
            "escalation_score": headline.escalation_score,
            "confidence": headline.confidence,
            "entities": ",".join(headline.entities),
        }
        for headline in headlines
    ]
    if not rows:
        return pd.DataFrame(
            columns=[
                "source",
                "language",
                "title",
                "translation",
                "link",
                "escalation_score",
                "confidence",
                "entities",
            ]
        )
    return pd.DataFrame(rows).set_index("timestamp").sort_index()


def write_headlines(frame: pd.DataFrame, path: str | Path) -> Path:
    """Persist scored headlines to CSV."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target)
    return target


def _find_link(item: ElementTree.Element) -> str | None:
    if "href" in item.attrib:
        return item.attrib["href"]
    for child in item:
        if child.tag.endswith("link") and child.attrib.get("href"):
            return child.attrib["href"]
        if child.tag.endswith("link") and child.text:
            return child.text.strip()
    return None


def _find_text(item: ElementTree.Element, names: tuple[str, ...]) -> str | None:
    for child in item:
        tag = child.tag.split("}")[-1]
        if tag in names and child.text:
            return child.text
    return None


def _coerce_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.now(tz=UTC)
    try:
        return parsedate_to_datetime(value).astimezone(UTC)
    except (TypeError, ValueError):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
