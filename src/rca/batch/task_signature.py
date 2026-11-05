"""Normalized failed-task signatures for the pre-analysis gate."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


_UUID_PATTERN = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b",
    re.IGNORECASE,
)
_TIMESTAMP_PATTERN = re.compile(
    r"\b\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?Z?)?\b",
    re.IGNORECASE,
)
_PATH_LINE_SUFFIX = re.compile(r":\d+$")


def _normalize_text(value: Any) -> str:
    return " ".join(str(value or "").casefold().split())


def _normalize_path(value: Any) -> str:
    normalized = _normalize_text(value).replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return _PATH_LINE_SUFFIX.sub("", normalized)


def _normalize_error(value: Any) -> str:
    normalized = _normalize_text(value)
    normalized = _UUID_PATTERN.sub("<uuid>", normalized)
    return _TIMESTAMP_PATTERN.sub("<timestamp>", normalized)


@dataclass(frozen=True)
class TaskSignature:
    """A failed Ansible task's stable identity and normalized error."""

    play: str
    role: str
    task: str
    task_action: str
    task_path: str
    error_message: str

    @property
    def identity_key(self) -> tuple[str, str, str, str, str]:
        return (
            _normalize_text(self.play),
            _normalize_text(self.role),
            _normalize_text(self.task),
            _normalize_text(self.task_action),
            _normalize_path(self.task_path),
        )

    @property
    def normalized_error(self) -> str:
        return _normalize_error(self.error_message)

    @property
    def is_complete(self) -> bool:
        play, _role, task, task_action, task_path = self.identity_key
        return all((play, task, task_action, task_path, self.normalized_error))


def build_task_signature(context: Mapping[str, Any] | None) -> TaskSignature | None:
    """Build a signature from the first failed task in a job context."""

    if not context:
        return None
    if not isinstance(context, Mapping):
        return None

    failed_tasks = context.get("failed_tasks")
    if not isinstance(failed_tasks, list) or not failed_tasks:
        return None

    indexed_tasks = [
        (index, task)
        for index, task in enumerate(failed_tasks)
        if isinstance(task, Mapping)
    ]
    if not indexed_tasks:
        return None

    failed_task = indexed_tasks[0][1]
    return TaskSignature(
        play=str(failed_task.get("play") or ""),
        role=str(failed_task.get("role") or ""),
        task=str(failed_task.get("task") or ""),
        task_action=str(failed_task.get("task_action") or ""),
        task_path=str(failed_task.get("task_path") or ""),
        error_message=str(failed_task.get("error_message") or ""),
    )
