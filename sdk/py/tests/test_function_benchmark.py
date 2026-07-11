from __future__ import annotations

import importlib.util
from pathlib import Path

_PATH = Path(__file__).resolve().parents[1] / "function_benchmark.py"
_SPEC = importlib.util.spec_from_file_location("function_benchmark", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
benchmark = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(benchmark)


def test_percentile_interpolates_and_does_not_mutate() -> None:
    values = [40, 10, 30, 20]
    assert benchmark.percentile(values, 50) == 25
    assert benchmark.percentile(values, 95) == 38.5
    assert values == [40, 10, 30, 20]


def test_summarize_preserves_raw_samples_and_all_timing_metrics() -> None:
    samples = [
        {"call_id": "a", "wall_ms": 10, "queue_ms": 1, "startup_ms": 6, "execution_ms": 3},
        {"call_id": "b", "wall_ms": 30, "queue_ms": 3, "startup_ms": 18, "execution_ms": 9},
    ]
    result = benchmark.summarize(samples)
    assert result["count"] == 2
    assert result["samples"] == samples
    assert result["summary"] == {
        "wall_ms": {"p50": 20, "p95": 29},
        "queue_ms": {"p50": 2, "p95": 2.9},
        "startup_ms": {"p50": 12, "p95": 17.4},
        "execution_ms": {"p50": 6, "p95": 8.7},
    }
