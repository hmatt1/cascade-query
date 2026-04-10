from __future__ import annotations

import os
from pathlib import Path

import pytest

from benchmarks.performance_suite import (
    PERFORMANCE_ASSERTION_EXCLUDED_SCENARIOS,
    assert_parallel_speedup_scenario_threshold,
    assert_report_thresholds,
    run_performance_scenario,
    run_performance_suite,
    write_performance_report,
)


def test_performance_regressions() -> None:
    report = run_performance_suite()
    assert_report_thresholds(report)


def test_default_perf_gate_excludes_parallel_speedup_scenario() -> None:
    # Keep the regular perf gate stable by excluding the most VM-sensitive
    # scenario, which is asserted in a dedicated test and CI step.
    assert "compute-many-parallel-speedup" in PERFORMANCE_ASSERTION_EXCLUDED_SCENARIOS


def test_compute_many_parallel_speedup_scenario() -> None:
    if os.environ.get("CASCADE_QUERY_SKIP_PARALLEL_PERF") == "1":
        pytest.skip("CASCADE_QUERY_SKIP_PARALLEL_PERF=1")
    retries = max(0, int(os.environ.get("CASCADE_QUERY_PARALLEL_PERF_RETRIES", "0")))
    for attempt in range(retries + 1):
        result = run_performance_scenario("compute-many-parallel-speedup")
        try:
            assert_parallel_speedup_scenario_threshold(result)
            return
        except AssertionError:
            if attempt >= retries:
                raise


def test_performance_report_is_published(tmp_path: Path) -> None:
    report = {
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "python_version": "3.14.0 free-threading build",
        "platform": "test-platform",
        "all_passed": True,
        "results": [
            {
                "name": "cache-hit-speedup",
                "concern": "cache validation cost",
                "metric_name": "cold_to_hot_speedup",
                "metric_value": 4.2,
                "unit": "x",
                "threshold": 3.0,
                "comparator": ">=",
                "passed": True,
                "details": {"cold_median_ms": 10.0, "hot_median_ms": 2.4},
            }
        ],
    }
    json_path, md_path = write_performance_report(report, tmp_path)
    assert json_path.exists()
    assert md_path.exists()
