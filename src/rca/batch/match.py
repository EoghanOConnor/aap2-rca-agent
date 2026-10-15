"""Shared similarity scoring for pre-analysis and post-analysis matching."""

from __future__ import annotations

from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Literal

Confidence = Literal["high", "medium", "low"]
MatchStage = Literal["pre_analysis", "post_analysis"]

_MAX_TEXT_LENGTH = 5000
_CONFIDENCE_RANK = {"high": 3, "medium": 2, "low": 1}
_POST_THRESHOLDS = {
    "high": 0.85,
    "medium": 0.70,
    "low": 0.55,
}
_POST_SEMANTIC_THRESHOLDS = {
    "high": 0.50,
    "medium": 0.40,
    "low": 0.30,
}
_GENERIC_TEXT_TOKENS = {"failed", "failure", "error", "exception", "timeout"}


@dataclass(frozen=True)
class MatchSignals:
    """Signals available to the shared scorer at a particular pipeline stage."""

    catalog_item: str | None
    text: str | None
    category: str | None = None


@dataclass(frozen=True)
class ScoreResult:
    """A normalized match decision and the evidence that produced it."""

    confidence: Confidence | None
    score: float
    evidence: tuple[dict[str, Any], ...]


def text_similarity(left: str | None, right: str | None) -> float:
    """Return a bounded text similarity ratio, treating missing text as empty."""

    left_text = (left or "")[:_MAX_TEXT_LENGTH]
    right_text = (right or "")[:_MAX_TEXT_LENGTH]
    if not left_text or not right_text:
        return 0.0
    return SequenceMatcher(None, left_text, right_text).ratio()


def _significant_tokens(text: str) -> set[str]:
    return {
        token
        for token in text.casefold().split()
        if len(token) > 2 and token not in _GENERIC_TEXT_TOKENS
    }


def _pre_analysis_text_is_specific(
    left_text: str, right_text: str, *, same_catalog: bool
) -> bool:
    left_tokens = _significant_tokens(left_text)
    right_tokens = _significant_tokens(right_text)
    minimum_tokens = 1 if same_catalog else 3
    return len(left_tokens) >= minimum_tokens and len(right_tokens) >= minimum_tokens


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    candidate = str(value).strip()
    return candidate or None


def _evidence(statement_type: str, description: str, source: str) -> dict[str, str]:
    return {
        "statement_type": statement_type,
        "description": description,
        "source": source,
    }


def score_similarity(
    left: MatchSignals,
    right: MatchSignals,
    stage: MatchStage,
    *,
    semantic_confidence: Confidence | None = None,
    semantic_reasoning: str | None = None,
) -> ScoreResult:
    """Score two result signals using one implementation and stage policies.

    Pre-analysis is intentionally conservative because a high-confidence result
    skips analysis. Post-analysis can use semantic judgment from the batch
    aggregator, but deterministic summary and category signals still bound the
    decision.
    """

    left_text = _clean(left.text)
    right_text = _clean(right.text)
    if left_text is None or right_text is None:
        return ScoreResult(None, 0.0, ())

    similarity = text_similarity(left_text, right_text)
    left_catalog = _clean(left.catalog_item)
    right_catalog = _clean(right.catalog_item)
    same_catalog = left_catalog is not None and left_catalog == right_catalog
    left_category = _clean(left.category)
    right_category = _clean(right.category)
    categories_compatible = not (
        left_category is not None
        and right_category is not None
        and left_category != right_category
    )

    evidence: list[dict[str, str]] = [
        _evidence("observed", f"Text similarity is {similarity:.0%}.", "shared_scorer")
    ]
    if same_catalog:
        evidence.append(
            _evidence(
                "observed",
                f"Both results use catalog item {left_catalog}.",
                "shared_scorer",
            )
        )
    if left_category is not None and right_category is not None:
        evidence.append(
            _evidence(
                "observed",
                f"Both results use root-cause category {left_category}.",
                "shared_scorer",
            )
        )

    if stage == "pre_analysis":
        threshold = 0.75 if same_catalog else 0.90
        if not _pre_analysis_text_is_specific(left_text, right_text, same_catalog=same_catalog):
            evidence.append(
                _evidence(
                    "observed",
                    "At least one error text is too generic for a pre-analysis match.",
                    "shared_scorer",
                )
            )
            return ScoreResult(None, similarity, tuple(evidence))
        if similarity < threshold:
            return ScoreResult(None, similarity, tuple(evidence))
        return ScoreResult("high", similarity, tuple(evidence))

    if not categories_compatible:
        evidence.append(
            _evidence(
                "observed",
                "Root-cause categories differ, so the results are not linked.",
                "shared_scorer",
            )
        )
        return ScoreResult(None, similarity, tuple(evidence))

    semantic_reasoning_text = _clean(semantic_reasoning)
    normalized_semantic_confidence = (
        semantic_confidence
        if isinstance(semantic_confidence, str) and semantic_confidence in _CONFIDENCE_RANK
        else None
    )
    if normalized_semantic_confidence is not None and semantic_reasoning_text is not None:
        evidence.append(
            _evidence("inference", semantic_reasoning_text, "semantic_aggregation")
        )
        threshold = _POST_SEMANTIC_THRESHOLDS[normalized_semantic_confidence]
        if similarity >= threshold:
            return ScoreResult(semantic_confidence, similarity, tuple(evidence))
        return ScoreResult(None, similarity, tuple(evidence))

    adjusted_score = min(1.0, similarity + (0.05 if same_catalog else 0.0))
    for confidence, threshold in _POST_THRESHOLDS.items():
        if adjusted_score >= threshold:
            if same_catalog:
                evidence.append(
                    _evidence(
                        "observed",
                        "Catalog agreement increased the deterministic score.",
                        "shared_scorer",
                    )
                )
            return ScoreResult(confidence, adjusted_score, tuple(evidence))
    return ScoreResult(None, adjusted_score, tuple(evidence))


def weakest_confidence(values: list[Confidence | None]) -> Confidence | None:
    """Return the weakest non-null confidence, or None when any value is None."""

    present = [value for value in values if value is not None]
    if not present or len(present) != len(values):
        return None
    return min(present, key=lambda value: _CONFIDENCE_RANK[value])
