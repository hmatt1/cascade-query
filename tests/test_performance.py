from __future__ import annotations

from pathlib import Path

from benchmarks.performance_suite import (
    assert_report_thresholds,
    run_performance_suite,
    write_performance_report,
)


def test_performance_regressions() -> None:
    report = run_performance_suite()
    assert_report_thresholds(report)


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
