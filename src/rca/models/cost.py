"""Typed contract for the Step 4c cost estimate artifact (step4c_cost_estimate.json)."""

from __future__ import annotations

from typing import Literal

from pydantic import model_validator

from rca.models.base import SchemaModel
from rca.models.summary import Confidence


class CostEstimate(SchemaModel):
    source: Literal["comparison", "catalog_baseline"]
    n: int
    confidence: Confidence
    median_per_hr: float
    min_per_hr: float
    max_per_hr: float
    cost_mid: float
    cost_low: float
    cost_high: float
    skewed: bool


class CostEstimateArtifact(SchemaModel):
    """Either a skipped result (``skipped`` + ``reason``) or a full estimate.

    ``estimate`` is only set for a usable estimate; every no-estimate outcome is
    ``skipped`` so consumers never see a null estimate that is not marked skipped.
    """

    skipped: bool = None
    reason: str = None
    provision: dict = None
    hours_failing: float = None
    hours_failing_basis: str = None
    window_days: int = None
    baseline: dict | None = None
    estimate: CostEstimate | None = None
    note: str = None
    disclaimer: str = None

    @model_validator(mode="after")
    def _skipped_or_estimate(self) -> CostEstimateArtifact:
        if self.skipped:
            if not self.reason:
                raise ValueError("skipped result requires a reason")
        elif self.estimate is None:
            raise ValueError("non-skipped result requires an estimate")
        return self
