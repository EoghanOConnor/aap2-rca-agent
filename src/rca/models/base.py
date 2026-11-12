"""Shared types for JSON artifact models.

An optional, non-nullable field uses a non-nullable annotation with a None
default. Defaults are not validated: omission is accepted, but an explicit
null is rejected. Use model_dump(exclude_unset=True) to preserve omissions.
Runtime output validation keeps the original dictionaries to avoid changing
their representation or adding defaults.
"""

from __future__ import annotations

from typing import Annotated, Any

from jsonschema import Draft7Validator
from jsonschema.exceptions import FormatError
from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict, Field, ValidationError


def normalize_json_integer(value: Any) -> Any:
    # JSON Schema permits 1.0 as an integer, but never bools or numeric strings.
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _date_time(value: str) -> str:
    # Keep the existing RFC3339 checker and the supplied string representation.
    try:
        Draft7Validator.FORMAT_CHECKER.check(value, "date-time")
    except FormatError as exc:
        raise ValueError("must be a valid RFC3339 date-time") from exc
    return value


JsonInteger = Annotated[int, BeforeValidator(normalize_json_integer)]
JsonNumber = float
DateTimeString = Annotated[
    str, AfterValidator(_date_time), Field(json_schema_extra={"format": "date-time"})
]


class SchemaModel(BaseModel):
    """Strict JSON field types with schema-permitted extra properties."""

    model_config = ConfigDict(strict=True, extra="allow", validate_default=False)


class ClosedSchemaModel(SchemaModel):
    """An object whose schema sets additionalProperties to false."""

    model_config = ConfigDict(extra="forbid")


def format_validation_errors(error: ValidationError) -> str:
    """Render nested locations in the existing output-validation error style."""
    return "; ".join(
        f"{'.'.join(str(part) for part in item['loc']) or '$'}: {item['msg']}"
        for item in error.errors(include_url=False)
    )
