"""Models for semantic aggregation and the final batch report schemas."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BeforeValidator, Field

from rca.models.base import JsonInteger, SchemaModel, normalize_json_integer
from rca.models.summary import Category, Confidence

# Constraints precede the validator so model_json_schema retains JSON bounds.
NonNegativeInteger = Annotated[int, Field(ge=0), BeforeValidator(normalize_json_integer)]
PositiveInteger = Annotated[int, Field(ge=1), BeforeValidator(normalize_json_integer)]


class SemanticEvidence(SchemaModel):
    statement_type: Literal["observed", "inference"]
    description: str
    kind: str = None
    source: str = None


class SemanticMatch(SchemaModel):
    matched_result_id: JsonInteger
    similarity_reasoning: str
    confidence: Confidence = None
    evidence: list[SemanticEvidence] = None


class HistoricalMatches(SchemaModel):
    job_id: str
    matches: list[SemanticMatch]


class SemanticPattern(SchemaModel):
    pattern: str
    jobs: list[str]
    description: str
    shared_github_path: str = None
    confidence: Confidence = None
    evidence: list[SemanticEvidence] = None


class BatchSemanticOutput(SchemaModel):
    historical_matches: list[HistoricalMatches]
    cross_job_patterns: list[SemanticPattern]


class AgentCompletion(SchemaModel):
    duration_ms: NonNegativeInteger
    status: Literal["completed", "failed", "timeout"]


class BatchTiming(SchemaModel):
    agent_spawn: str
    agent_completion: dict[str, AgentCompletion]
    wall_clock_total_ms: NonNegativeInteger
    aggregation_completed_at: str


class CategoryBreakdown(SchemaModel):
    count: PositiveInteger
    job_ids: list[str]
    description: str


class ConfidenceBreakdown(SchemaModel):
    high: NonNegativeInteger
    medium: NonNegativeInteger
    low: NonNegativeInteger


class PriorityRecommendation(SchemaModel):
    rank: Annotated[int, Field(ge=1, le=5), BeforeValidator(normalize_json_integer)]
    priority: Confidence
    action: str
    affects_jobs: list[str]
    category: Category
    details: str
    github_path: str = None


class ReportHistoricalMatch(SemanticMatch):
    recurrence_count: PositiveInteger
    pattern_id: str = None


class JobSummary(SchemaModel):
    job_id: str
    status: Literal["analyzed", "failed", "matched_known_issue"]
    result_id: JsonInteger = None
    root_cause_category: Category = None
    confidence: Confidence = None
    root_cause_summary: str = None
    catalog_item: str = None
    platform: str = None
    job_duration_seconds: NonNegativeInteger = None
    analysis_duration_ms: NonNegativeInteger = None
    failing_role: str = None
    failing_github_path: str = None
    analysis_path: str = None
    historical_matches: list[ReportHistoricalMatch] = None


class FailedAnalysis(SchemaModel):
    job_id: str
    error: str
    stage: Literal[
        "agent_spawn", "analysis_pipeline", "skill_execution", "step5_missing", "upload"
    ] = None


class ReportPattern(SemanticPattern):
    source: Literal["current_batch", "historical"]
    pattern_id: str = None


class BatchReport(SchemaModel):
    batch_id: str
    generated_at: str
    total_jobs_requested: NonNegativeInteger
    total_jobs_analyzed: NonNegativeInteger
    total_jobs_failed: NonNegativeInteger
    timing: BatchTiming
    root_cause_category_breakdown: dict[str, CategoryBreakdown]
    confidence_breakdown: ConfidenceBreakdown
    high_priority_recommendations: Annotated[list[PriorityRecommendation], Field(max_length=5)]
    job_summaries: list[JobSummary]
    failed_analyses: list[FailedAnalysis]
    cross_job_patterns: list[ReportPattern]
