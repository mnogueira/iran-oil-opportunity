"""Local-language news feed polling and aggregation."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import quote_plus
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
    source_type: str = "rss"
    query: str | None = None
    api_key_env: str | None = None
    params: tuple[tuple[str, str], ...] = ()


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


@dataclass(frozen=True, slots=True)
class SourceFetchStatus:
    """Per-source fetch diagnostics for the monitor."""

    source: str
    source_type: str
    checked_at: datetime
    ok: bool
    headline_count: int
    status_code: int | None
    error: str | None
    endpoint: str


def build_google_news_rss_url(query: str) -> str:
    """Build a Google News RSS search URL for a keyword query."""

    return f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"


DEFAULT_LOCAL_NEWS_SOURCES = (
    NewsSource(
        "Google News Iran Oil War",
        "en",
        build_google_news_rss_url('"Iran oil war" OR "Iran oil" OR "Strait of Hormuz" OR Kharg ceasefire'),
    ),
    NewsSource(
        "Google News Reuters Iran Oil",
        "en",
        build_google_news_rss_url('(Iran OR Hormuz OR Kharg) (oil OR crude OR Brent OR WTI) site:reuters.com'),
    ),
    NewsSource(
        "Google News AP Iran Oil",
        "en",
        build_google_news_rss_url('(Iran OR Hormuz OR Kharg) (oil OR crude OR Brent OR WTI) site:apnews.com'),
    ),
    NewsSource(
        "NewsAPI Iran Oil",
        "en",
        "https://newsapi.org/v2/everything",
        source_type="newsapi",
        query='("Iran" OR "Strait of Hormuz" OR Kharg OR ceasefire) AND (oil OR crude OR Brent OR WTI)',
        api_key_env="NEWSAPI_API_KEY",
        params=(
            ("domains", "reuters.com,apnews.com,ft.com,wsj.com,bloomberg.com"),
            ("sortBy", "publishedAt"),
            ("searchIn", "title,description"),
        ),
    ),
    NewsSource(
        "X Iran Oil",
        "en",
        "https://api.x.com/2/tweets/search/recent",
        source_type="x_recent_search",
        query="(Iran OR Hormuz OR Kharg OR ceasefire) (oil OR crude OR Brent OR WTI) lang:en -is:retweet",
        api_key_env="X_BEARER_TOKEN",
        params=(("tweet.fields", "created_at,lang"),),
    ),
)


def fetch_recent_headlines(
    *,
    sources: Iterable[NewsSource] = DEFAULT_LOCAL_NEWS_SOURCES,
    timeout_seconds: int = 15,
    session: requests.Session | None = None,
    max_items_per_source: int = 20,
) -> list[RawHeadline]:
    """Fetch headlines from configured RSS feeds and optional JSON APIs."""

    headlines, _ = fetch_recent_headlines_with_status(
        sources=sources,
        timeout_seconds=timeout_seconds,
        session=session,
        max_items_per_source=max_items_per_source,
    )
    return headlines


def fetch_recent_headlines_with_status(
    *,
    sources: Iterable[NewsSource] = DEFAULT_LOCAL_NEWS_SOURCES,
    timeout_seconds: int = 15,
    session: requests.Session | None = None,
    max_items_per_source: int = 20,
) -> tuple[list[RawHeadline], list[SourceFetchStatus]]:
    """Fetch headlines together with per-source diagnostics."""

    http = session or requests.Session()
    headlines: list[RawHeadline] = []
    statuses: list[SourceFetchStatus] = []
    for source in sources:
        source_headlines, status = _fetch_source_headlines(
            source=source,
            http=http,
            timeout_seconds=timeout_seconds,
            max_items_per_source=max_items_per_source,
        )
        headlines.extend(source_headlines)
        statuses.append(status)
    deduped: dict[tuple[str, str], RawHeadline] = {}
    for headline in headlines:
        dedupe_key = (
            (headline.link or "").strip().lower(),
            " ".join(headline.title.split()).lower(),
        )
        deduped[dedupe_key] = headline
    return sorted(deduped.values(), key=lambda item: item.published_at, reverse=True), statuses


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


def source_statuses_to_frame(statuses: Iterable[SourceFetchStatus]) -> pd.DataFrame:
    """Convert source diagnostics into a DataFrame."""

    rows = [
        {
            "timestamp": status.checked_at,
            "source": status.source,
            "source_type": status.source_type,
            "ok": status.ok,
            "headline_count": status.headline_count,
            "status_code": status.status_code,
            "error": status.error,
            "endpoint": status.endpoint,
        }
        for status in statuses
    ]
    if not rows:
        return pd.DataFrame(
            columns=[
                "source",
                "source_type",
                "ok",
                "headline_count",
                "status_code",
                "error",
                "endpoint",
            ]
        )
    return pd.DataFrame(rows).set_index("timestamp").sort_index()


def write_source_statuses(frame: pd.DataFrame, path: str | Path) -> Path:
    """Persist source diagnostics to CSV."""

    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target)
    return target


def _fetch_source_headlines(
    *,
    source: NewsSource,
    http: requests.Session,
    timeout_seconds: int,
    max_items_per_source: int,
) -> tuple[list[RawHeadline], SourceFetchStatus]:
    checked_at = datetime.now(tz=UTC)
    try:
        if source.source_type == "rss":
            headlines, status_code = _fetch_rss_headlines(
                source=source,
                http=http,
                timeout_seconds=timeout_seconds,
                max_items_per_source=max_items_per_source,
            )
        elif source.source_type == "newsapi":
            headlines, status_code = _fetch_newsapi_headlines(
                source=source,
                http=http,
                timeout_seconds=timeout_seconds,
                max_items_per_source=max_items_per_source,
            )
        elif source.source_type == "x_recent_search":
            headlines, status_code = _fetch_x_recent_search_headlines(
                source=source,
                http=http,
                timeout_seconds=timeout_seconds,
                max_items_per_source=max_items_per_source,
            )
        else:
            raise ValueError(f"Unsupported source_type={source.source_type!r}")
        return headlines, SourceFetchStatus(
            source=source.name,
            source_type=source.source_type,
            checked_at=checked_at,
            ok=True,
            headline_count=len(headlines),
            status_code=status_code,
            error=None,
            endpoint=source.feed_url,
        )
    except Exception as exc:
        return [], SourceFetchStatus(
            source=source.name,
            source_type=source.source_type,
            checked_at=checked_at,
            ok=False,
            headline_count=0,
            status_code=getattr(getattr(exc, "response", None), "status_code", None),
            error=f"{exc.__class__.__name__}: {exc}",
            endpoint=source.feed_url,
        )


def _fetch_rss_headlines(
    *,
    source: NewsSource,
    http: requests.Session,
    timeout_seconds: int,
    max_items_per_source: int,
) -> tuple[list[RawHeadline], int | None]:
    response = http.get(
        source.feed_url,
        timeout=timeout_seconds,
        headers={"User-Agent": "iran-oil-opportunity/0.1"},
    )
    response.raise_for_status()
    root = ElementTree.fromstring(response.text)
    items = root.findall(".//item") or root.findall(".//entry")
    headlines: list[RawHeadline] = []
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
    return headlines, response.status_code


def _fetch_newsapi_headlines(
    *,
    source: NewsSource,
    http: requests.Session,
    timeout_seconds: int,
    max_items_per_source: int,
) -> tuple[list[RawHeadline], int | None]:
    api_key = _require_api_key(source)
    params = {
        "q": source.query or "",
        "language": source.language,
        "pageSize": str(max_items_per_source),
    }
    params.update(dict(source.params))
    response = http.get(
        source.feed_url,
        params=params,
        timeout=timeout_seconds,
        headers={"X-Api-Key": api_key, "User-Agent": "iran-oil-opportunity/0.1"},
    )
    response.raise_for_status()
    payload = response.json()
    articles = payload.get("articles", [])
    headlines = [
        RawHeadline(
            source=source.name,
            language=source.language,
            title=str(article.get("title") or "").strip(),
            link=_coerce_optional_string(article.get("url")),
            published_at=_coerce_timestamp(_coerce_optional_string(article.get("publishedAt"))),
        )
        for article in articles[:max_items_per_source]
        if str(article.get("title") or "").strip() not in {"", "[Removed]"}
    ]
    return headlines, response.status_code


def _fetch_x_recent_search_headlines(
    *,
    source: NewsSource,
    http: requests.Session,
    timeout_seconds: int,
    max_items_per_source: int,
) -> tuple[list[RawHeadline], int | None]:
    bearer_token = _require_api_key(source)
    params = {"query": source.query or "", "max_results": str(min(100, max_items_per_source))}
    params.update(dict(source.params))
    response = http.get(
        source.feed_url,
        params=params,
        timeout=timeout_seconds,
        headers={
            "Authorization": f"Bearer {bearer_token}",
            "User-Agent": "iran-oil-opportunity/0.1",
        },
    )
    response.raise_for_status()
    payload = response.json()
    tweets = payload.get("data", [])
    headlines = [
        RawHeadline(
            source=source.name,
            language=str(tweet.get("lang") or source.language),
            title=" ".join(str(tweet.get("text") or "").split()),
            link=f"https://x.com/i/web/status/{tweet['id']}",
            published_at=_coerce_timestamp(_coerce_optional_string(tweet.get("created_at"))),
        )
        for tweet in tweets[:max_items_per_source]
        if str(tweet.get("text") or "").strip() and tweet.get("id")
    ]
    return headlines, response.status_code


def _require_api_key(source: NewsSource) -> str:
    if not source.api_key_env:
        raise RuntimeError(f"{source.name} requires api_key_env to be configured.")
    api_key = os.getenv(source.api_key_env)
    if not api_key:
        raise RuntimeError(f"Missing {source.api_key_env} for {source.name}.")
    return api_key


def _coerce_optional_string(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


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
