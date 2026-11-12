"""Regression tests for optional MLflow import initialization."""

import sys
from importlib.util import module_from_spec, spec_from_file_location
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock

import pytest

from rca.analysis import tracing


def _load_tracing() -> ModuleType:
    """Import a fresh module without changing existing decorated callers."""
    spec = spec_from_file_location("isolated_tracing", tracing.__file__)
    assert spec is not None and spec.loader is not None
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_successful_import_enables_mlflow_tracing(monkeypatch: pytest.MonkeyPatch) -> None:
    mlflow = ModuleType("mlflow")
    entities = ModuleType("mlflow.entities")
    entities.SpanType = SimpleNamespace(CHAIN="CHAIN")

    def pipeline_step(value: str, *, suffix: str) -> str:
        return value + suffix

    traced_step = Mock(wraps=pipeline_step)
    decorator = Mock(return_value=traced_step)
    mlflow.trace = Mock(return_value=decorator)
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)
    monkeypatch.setitem(sys.modules, "mlflow.entities", entities)

    module = _load_tracing()

    assert module.HAS_MLFLOW is True
    assert module.mlflow is mlflow
    assert module.SpanType is entities.SpanType
    wrapped = module.trace("Pipeline step", span_type=module.SpanType.CHAIN)(pipeline_step)
    assert wrapped("value", suffix="!") == "value!"
    mlflow.trace.assert_called_once_with(name="Pipeline step", span_type="CHAIN")
    decorator.assert_called_once_with(pipeline_step)
    traced_step.assert_called_once_with("value", suffix="!")


@pytest.mark.parametrize("missing_module", ["mlflow", "mlflow.entities"])
def test_import_failure_preserves_noop_tracing(
    monkeypatch: pytest.MonkeyPatch, missing_module: str
) -> None:
    mlflow = ModuleType("mlflow")
    mlflow.trace = Mock()
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)
    monkeypatch.setitem(sys.modules, missing_module, None)

    module = _load_tracing()

    assert module.HAS_MLFLOW is False
    assert module.mlflow is None
    assert module.SpanType is None

    def pipeline_step() -> str:
        return "ok"

    assert module.trace("Pipeline step")(pipeline_step) is pipeline_step
    mlflow.trace.assert_not_called()
