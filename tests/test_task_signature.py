"""Tests for normalized pre-analysis task signatures."""

from rca.batch.task_signature import build_task_signature


def _failed_task(**overrides: object) -> dict[str, object]:
    task = {
        "task": "Install dependencies",
        "play": "Setup",
        "role": "common",
        "timestamp": "2026-01-01T00:00:00Z",
        "error_message": "Package not found",
        "task_path": "/roles/common/tasks/main.yml",
        "task_action": "yum",
        "duration": 1.0,
    }
    task.update(overrides)
    return task


def test_build_task_signature_selects_the_first_failed_task_in_event_order() -> None:
    signature = build_task_signature(
        {
            "failed_tasks": [
                _failed_task(
                    task="Original failure",
                    error_message="Original error",
                ),
                _failed_task(
                    timestamp="",
                    task="Cascading failure",
                    error_message="Cascading error",
                ),
            ]
        }
    )

    assert signature is not None
    assert signature.task == "Original failure"
    assert signature.error_message == "Original error"


def test_task_signature_normalizes_volatile_error_values() -> None:
    signature = build_task_signature(
        {
            "failed_tasks": [
                _failed_task(
                    error_message=(
                        "Job 123 failed at 2026-01-01T00:00:00Z "
                        "for 123e4567-e89b-12d3-a456-426614174000"
                    )
                )
            ]
        }
    )

    assert signature is not None
    assert signature.normalized_error == (
        "job 123 failed at <timestamp> for <uuid>"
    )


def test_play_level_task_without_role_is_complete() -> None:
    signature = build_task_signature({"failed_tasks": [_failed_task(role="")]})

    assert signature is not None
    assert signature.is_complete


def test_task_signature_ignores_task_path_line_numbers() -> None:
    first = build_task_signature(
        {"failed_tasks": [_failed_task(task_path="/roles/common/tasks/main.yml:10")]}
    )
    second = build_task_signature(
        {"failed_tasks": [_failed_task(task_path="/roles/common/tasks/main.yml:42")]}
    )

    assert first is not None
    assert second is not None
    assert first.identity_key == second.identity_key


def test_incomplete_task_signature_is_reported_as_incomplete() -> None:
    signature = build_task_signature({"failed_tasks": [_failed_task(play="")]})

    assert signature is not None
    assert not signature.is_complete
