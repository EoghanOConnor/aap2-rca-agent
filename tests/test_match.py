from rca.batch.match import (
    MatchSignals,
    ScoreResult,
    score_similarity,
    weakest_confidence,
)


def test_pre_analysis_uses_stricter_cross_catalog_threshold() -> None:
    left = MatchSignals(catalog_item="widget", text="Worker 7 timed out")
    same_catalog = MatchSignals(catalog_item="widget", text="Worker 7 timed out now")
    different_catalog = MatchSignals(catalog_item="other", text="Worker 7 timed out slightly")

    assert score_similarity(left, same_catalog, "pre_analysis").confidence == "high"
    assert score_similarity(left, different_catalog, "pre_analysis").confidence is None


def test_pre_analysis_rejects_generic_cross_catalog_errors() -> None:
    result = score_similarity(
        MatchSignals(catalog_item="widget", text="Failed"),
        MatchSignals(catalog_item="other", text="Failed"),
        "pre_analysis",
    )

    assert result.confidence is None
    assert any("too generic" in item["description"] for item in result.evidence)


def test_post_analysis_semantic_judgment_is_bounded_by_category() -> None:
    left = MatchSignals(
        catalog_item="widget",
        text="The worker exhausted capacity.",
        category="infrastructure",
    )
    same_category = MatchSignals(
        catalog_item="other",
        text="Worker capacity was exhausted.",
        category="infrastructure",
    )
    different_category = MatchSignals(
        catalog_item="widget",
        text="The worker exhausted capacity.",
        category="application_bug",
    )

    semantic = score_similarity(
        left,
        same_category,
        "post_analysis",
        semantic_confidence="high",
        semantic_reasoning="Both identify worker capacity exhaustion.",
    )
    assert semantic.confidence == "high"
    assert any(
        item["source"] == "semantic_aggregation" for item in semantic.evidence
    )

    assert (
        score_similarity(
            left,
            different_category,
            "post_analysis",
            semantic_confidence="high",
            semantic_reasoning="The categories differ.",
        ).confidence
        is None
    )


def test_weakest_confidence_requires_every_pair_to_match() -> None:
    assert weakest_confidence(["high", "medium", "low"]) == "low"
    assert weakest_confidence(["high", None]) is None


def test_missing_text_never_matches() -> None:
    result = score_similarity(
        MatchSignals(catalog_item="widget", text=None),
        MatchSignals(catalog_item="widget", text="failure"),
        "pre_analysis",
    )
    assert isinstance(result, ScoreResult)
    assert result.confidence is None
