"""Optional MLflow tracing support. No-ops when mlflow is not installed."""

from functools import wraps

try:
    import mlflow
    from mlflow.entities import SpanType

    HAS_MLFLOW = True
except ImportError:
    mlflow = None  # noqa: N816
    SpanType = None
    HAS_MLFLOW = False

_pipeline_tracing_enabled = True


def set_pipeline_tracing_enabled(enabled: bool) -> None:
    """Enable or disable pipeline-level spans without changing Agent SDK traces."""
    global _pipeline_tracing_enabled
    _pipeline_tracing_enabled = enabled


def trace(name, span_type=None):
    """Decorator that traces with mlflow if available, otherwise no-op."""
    if HAS_MLFLOW:
        def decorator(fn):
            traced = mlflow.trace(name=name, span_type=span_type)(fn)

            @wraps(fn)
            def wrapper(*args, **kwargs):
                if _pipeline_tracing_enabled:
                    return traced(*args, **kwargs)
                return fn(*args, **kwargs)

            return wrapper

        return decorator

    return lambda fn: fn
