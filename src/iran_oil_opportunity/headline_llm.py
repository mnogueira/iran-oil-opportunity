"""Headline translation and escalation scoring."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Protocol

import requests

from iran_oil_opportunity.config import HeadlineLLMConfig


@dataclass(frozen=True, slots=True)
class HeadlineAssessment:
    """Structured headline output."""

    translation: str
    escalation_score: float
    confidence: float
    entities: tuple[str, ...]
    source_language: str


class HeadlineScorer(Protocol):
    """Protocol for scoring local-language headlines."""

    def score(self, *, text: str, language: str) -> HeadlineAssessment:
        """Return translation and escalation metadata."""


class KeywordHeadlineScorer:
    """Offline heuristic fallback for tests and blocked environments."""

    ESCALATION_TERMS = (
        "حمله",
        "جنگ",
        "هرمز",
        "موشک",
        "attack",
        "war",
        "strike",
        "closure",
        "Hormuz",
    )
    DEESCALATION_TERMS = (
        "آتش‌بس",
        "آتش بس",
        "مذاکره",
        "توافق",
        "ceasefire",
        "talks",
        "negotiation",
        "deal",
    )

    def score(self, *, text: str, language: str) -> HeadlineAssessment:
        normalized = text.lower()
        score = 0.0
        if any(term.lower() in normalized for term in self.ESCALATION_TERMS):
            score += 0.65
        if any(term.lower() in normalized for term in self.DEESCALATION_TERMS):
            score -= 0.65
        entities = tuple(
            entity
            for entity in ("Iran", "Hormuz", "Oil", "Israel", "US")
            if entity.lower() in normalized
        )
        return HeadlineAssessment(
            translation=text,
            escalation_score=max(-1.0, min(1.0, score)),
            confidence=0.35 if score == 0.0 else 0.6,
            entities=entities,
            source_language=language,
        )


class OpenAIHeadlineScorer:
    """Small-model worker call for translation and sentiment in one step."""

    def __init__(self, config: HeadlineLLMConfig | None = None):
        self.config = config or HeadlineLLMConfig()

    def score(self, *, text: str, language: str) -> HeadlineAssessment:
        api_key = os.getenv(self.config.api_key_env)
        if not api_key:
            raise RuntimeError(f"Missing {self.config.api_key_env} for OpenAI headline scoring.")

        payload = {
            "model": self.config.model,
            "temperature": 0,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a fast geopolitical news worker. Return strict JSON with keys "
                        "translation, escalation_score, confidence, entities. "
                        "Use escalation_score in [-1,1] where +1 is strongly bullish for oil "
                        "because of escalation or supply disruption, and -1 is strongly bearish "
                        "for oil because of de-escalation or ceasefire progress."
                    ),
                },
                {
                    "role": "user",
                    "content": f"Language: {language}\nHeadline: {text}",
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "headline_assessment",
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "translation": {"type": "string"},
                            "escalation_score": {"type": "number"},
                            "confidence": {"type": "number"},
                            "entities": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                        },
                        "required": ["translation", "escalation_score", "confidence", "entities"],
                    },
                },
            },
            "max_tokens": self.config.max_tokens,
        }
        response = requests.post(
            self.config.endpoint,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=self.config.timeout_seconds,
        )
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        parsed = json.loads(content)
        return HeadlineAssessment(
            translation=str(parsed["translation"]),
            escalation_score=max(-1.0, min(1.0, float(parsed["escalation_score"]))),
            confidence=max(0.0, min(1.0, float(parsed["confidence"]))),
            entities=tuple(str(item) for item in parsed["entities"]),
            source_language=language,
        )
