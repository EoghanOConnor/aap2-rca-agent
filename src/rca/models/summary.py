"""Typed counterparts of the analysis summary and stored root-cause schemas."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BeforeValidator, Field

from rca.models.base import (
    ClosedSchemaModel,
    DateTimeString,
    SchemaModel,
    normalize_json_integer,
)

Category = Literal[
    "application_bug",
    "infrastructure",
    "configuration",
    "dependency",
    "network",
    "resource",
    "cloud_api",
    "secrets",
    "unknown",
]
Confidence = Literal["high", "medium", "low"]
EvidenceSource = Literal["aap_job", "splunk_ocp", "agnosticv_config", "agnosticd_code"]
Relationship = Literal["direct_cause", "contributing_factor"]
# Constraints precede the validator so model_json_schema retains JSON bounds.
EvidenceRef = Annotated[int, Field(ge=0), BeforeValidator(normalize_json_integer)]
StepNumber = Annotated[int, Field(ge=1), BeforeValidator(normalize_json_integer)]


class SummaryRootCause(SchemaModel):
    summary: str = Field(description="One-line description of the root cause")
    category: Category
    confidence: Confidence


class SummaryIdentifiers(SchemaModel):
    guid: str = None
    namespace: str = None
    pod_name: str = None


class SummaryTimeOverlap(SchemaModel):
    aap_job_start: str = None
    aap_job_end: str = None
    splunk_first_error: str = None
    splunk_last_error: str = None
    overlap_confirmed: bool = None


class SummaryCorrelation(SchemaModel):
    method: str = Field(description="How AAP job was correlated to Splunk logs")
    identifiers: SummaryIdentifiers
    time_overlap: SummaryTimeOverlap = None


class SummaryEvidence(SchemaModel):
    source: EvidenceSource
    timestamp: str
    message: str
    github_path: str = Field(
        default=None,
        description=(
            "REQUIRED when source is agnosticv_config or agnosticd_code. Format: "
            "'owner/repo:path/to/file.yml:line'. Extract from step4 "
            "github_fetches[].location.parsed or fetched_configs/fetched_workload paths."
        ),
    )


class SummaryFix(ClosedSchemaModel):
    status: Literal["proposed"] = Field(description="Always 'proposed': model-authored, untested")
    base_sha: str = Field(
        description=(
            "The sha of the fetched file this diff applies to, from "
            "step4_github_fetch_history.json's fetched_configs/fetched_workload"
        )
    )
    diff: str | None = Field(default=None, description="Unified diff against the file at base_sha")
    pr_link: str | None = None


class SummaryRecommendation(ClosedSchemaModel):
    priority: Confidence = None
    action: str = None
    github_path: str = Field(
        default=None,
        description="GitHub reference 'owner/repo:path/to/file.yml:line'. Extract from step4 data.",
    )
    details: str = None
    evidence_ref: EvidenceRef | None = Field(
        default=None,
        description="0-based index into this file's evidence[] array that this recommendation addresses",
    )
    fix: SummaryFix | None = Field(
        default=None,
        description=(
            "null unless a concrete, model-authored patch is proposed. "
            "Never set without both status and base_sha."
        ),
    )


class SummaryCausalStep(ClosedSchemaModel):
    step: StepNumber
    relationship: Relationship
    statement: str
    evidence_ref: EvidenceRef | None = Field(
        default=None, description="0-based index into this file's evidence[] array supporting this step"
    )


class SummaryMisidentification(ClosedSchemaModel):
    theory: str
    why_suspected: str
    why_ruled_out: str
    evidence_ref: EvidenceRef | None = Field(
        default=None,
        description="0-based index into this file's evidence[] array that ruled this theory out",
    )


class AnalysisSummary(SchemaModel):
    """Root cause summary from correlation analysis with AgnosticD/V configuration context."""

    job_id: str
    analyzed_at: DateTimeString = None
    root_cause: SummaryRootCause
    correlation: SummaryCorrelation
    evidence: Annotated[list[SummaryEvidence], Field(min_length=1)]
    recommendations: list[SummaryRecommendation] = None
    causal_chain: list[SummaryCausalStep] = Field(
        default=None,
        description=(
            "Ordered account of how the failure unfolded: direct causes and contributing "
            "factors, each tied back to an evidence entry"
        ),
    )
    misidentifications: list[SummaryMisidentification] = Field(
        default=None,
        description=(
            "Theories considered and ruled out during investigation. "
            "Only include genuine dead ends, not a list of everything checked."
        ),
    )


class RootCauseEvidence(ClosedSchemaModel):
    source: Literal[
        "aap_job", "splunk_ocp", "agnosticv_config", "agnosticd_code", "related_job"
    ]
    job_id: str
    timestamp: str | None
    message: str
    github_path: str | None


class RootCauseFix(ClosedSchemaModel):
    status: Literal["proposed", "applied"]
    base_sha: str
    diff: str | None = None
    pr_link: str | None = None


class RootCauseRecommendation(ClosedSchemaModel):
    priority: Confidence
    action: str
    github_path: str | None
    details: str | None
    evidence_ref: EvidenceRef | None
    fix: RootCauseFix | None


class RootCauseCausalStep(ClosedSchemaModel):
    step: StepNumber
    relationship: Relationship
    statement: str
    evidence_ref: EvidenceRef | None


class RootCauseMisidentification(ClosedSchemaModel):
    theory: str
    why_suspected: str
    why_ruled_out: str
    evidence_ref: EvidenceRef | None


class RootCauseDetail(SchemaModel):
    """The root-cause detail payload stored as JSONB."""

    summary: str
    platform: str | None = None
    failing_role: str | None = None
    failing_github_path: str | None = None
    analysis_path: str | None = None
    evidence: list[RootCauseEvidence]
    causal_chain: list[RootCauseCausalStep]
    misidentifications: list[RootCauseMisidentification]
    recommendations: list[RootCauseRecommendation]
