"""Models for job, correlation, and GitHub context artifacts.

Non-nullable fields with a ``None`` default may be omitted, but reject an
explicit JSON null. Use ``model_dump(exclude_unset=True)`` to preserve omission.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BeforeValidator, Field, ValidationError

from rca.models.base import DateTimeString, JsonInteger, JsonNumber, SchemaModel


class JobTimeWindow(SchemaModel):
    started: DateTimeString = None
    finished: DateTimeString = None
    duration_seconds: JsonNumber = None


class FailedTask(SchemaModel):
    task: str = None
    play: str = None
    role: str = None
    timestamp: str = None
    error_message: str = None
    task_path: str = None
    task_action: str = None
    duration: JsonNumber = None


class PodReference(SchemaModel):
    pod_name: str = None
    task: str = None
    timestamp: str = None


class JobContext(SchemaModel):
    job_id: str
    time_window: JobTimeWindow
    job_name: str = None
    status: Literal["successful", "failed", "canceled", "running"] = None
    guid: str = None
    namespace: str = None
    cluster: str = None
    cloud_provider: str = None
    env_type: str = None
    action: Literal["provision", "destroy", "stop", "start"] = None
    failed_tasks: list[FailedTask] = None
    pod_references: list[PodReference] = None
    plays: list[str] = None
    roles: list[str] = None
    total_events: JsonInteger = None
    host_status_counts: dict[str, Any] = None
    parsed_at: DateTimeString = None
    source_file: str = None


class CorrelationTimeRange(SchemaModel):
    start: str = None
    end: str = None
    duration_seconds: JsonNumber = None


class CorrelationIdentifiers(SchemaModel):
    guid: str | None = None
    namespace: str | None = None


class CorrelationTimeOverlap(SchemaModel):
    aap_job_start: str = None
    aap_job_end: str = None
    splunk_first_log: str | None = None
    splunk_last_log: str | None = None
    overlap_confirmed: bool = None


class CorrelationDetails(SchemaModel):
    method: Literal[
        "namespace_time_match", "guid_time_match", "pod_name_match", "identifier_match", "none"
    ]
    confidence: Literal["high", "medium", "low", "none"]
    identifiers: CorrelationIdentifiers = None
    time_overlap: CorrelationTimeOverlap = None
    matching_pods: list[str] = None


class TimelineEvent(SchemaModel):
    timestamp: str
    source: Literal["aap_job", "splunk_ocp"]
    event_type: Literal["task_failed", "pod_error", "warning", "info"]
    summary: str
    details: dict[str, Any] = None


class CorrelationSummary(SchemaModel):
    aap_failed_tasks: JsonInteger = None
    splunk_error_logs: JsonInteger = None
    total_timeline_events: JsonInteger = None
    pods_involved: list[str] = None


class CorrelationTimeline(SchemaModel):
    job_id: str
    correlation: CorrelationDetails
    timeline_events: list[TimelineEvent]
    generated_at: DateTimeString = None
    time_range: CorrelationTimeRange = None
    summary: CorrelationSummary = None


class GitHubJobMetadata(SchemaModel):
    platform: str = None
    catalog_item: str = None
    env: str = None
    guid: str = None
    warnings: list[str] = None


class GitHubParsedLocation(SchemaModel):
    owner: str = None
    repo: str = None
    file_path: str = None
    line_number: JsonInteger = None


class GitHubLocation(SchemaModel):
    original_path: str = None
    parsed: GitHubParsedLocation = None


class FetchAttempt(SchemaModel):
    path: str
    status: Literal["success", "404"]


class FailedFetchAttempt(SchemaModel):
    path: str
    status: Literal["404"]


class FetchedFile(SchemaModel):
    path: str
    content: str
    sha: str
    size: JsonNumber
    paths_tried: list[FetchAttempt] = None


class FileNotFound(SchemaModel):
    error: Literal["not_found"]
    path: str


class AllPathsFailed(SchemaModel):
    error: Literal["all_paths_failed"]
    paths_tried: list[FailedFetchAttempt]
    mcp_verified: bool = None
    mcp_findings: str = None


def _reject_overlapping_fetch_results(value: Any) -> Any:
    """JSON Schema oneOf rejects objects matching multiple open alternatives."""
    matches = 0
    for model in (FetchedFile, FileNotFound, AllPathsFailed):
        try:
            model.model_validate(value)
        except ValidationError:
            continue
        matches += 1
    if matches > 1:
        raise ValueError("fetch result must match exactly one fetched-file schema")
    return value


def _fetch_result_schema(schema: dict[str, Any]) -> None:
    schema["oneOf"] = schema.pop("anyOf")


FetchResult = Annotated[
    FetchedFile | FileNotFound | AllPathsFailed,
    BeforeValidator(_reject_overlapping_fetch_results),
    Field(json_schema_extra=_fetch_result_schema),
]


class GitHubFetch(SchemaModel):
    task_path: str
    location: GitHubLocation
    fetched_configs: dict[str, FetchResult]
    fetched_workload: dict[str, FetchResult]
    task: str = None


class GitHubContext(SchemaModel):
    job_id: str
    job_metadata: GitHubJobMetadata
    github_fetches: list[GitHubFetch]
