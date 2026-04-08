from __future__ import annotations

import argparse
import json
import platform
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cascade import Engine


@dataclass(frozen=True)
class ScenarioResult:
    name: str
    concern: str
    metric_name: str
    metric_value: float
    unit: str
    threshold: float
    comparator: str
    passed: bool
    details: dict[str, Any]


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.median(values))


def _scenario_cache_hit_speedup() -> ScenarioResult:
    engine = Engine()
    rounds = 8
    cold_ms: list[float] = []
    hot_ms: list[float] = []
    base_rows = "\n".join(f"symbol_{i} = {i}" for i in range(2500))

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def analyze(name: str) -> int:
        total = 0
        for row in source(name).splitlines():
            left, right = row.split("=", maxsplit=1)
            total += (len(left.strip()) * 17) + int(right.strip())
        return total

    for idx in range(rounds):
        source.set("main", f"{base_rows}\nmarker = {idx}")
        start = time.perf_counter()
        analyze("main")
        cold_ms.append((time.perf_counter() - start) * 1000.0)

        start = time.perf_counter()
        analyze("main")
        hot_ms.append((time.perf_counter() - start) * 1000.0)

    cold_med = _median(cold_ms)
    hot_med = _median(hot_ms)
    speedup = cold_med / max(hot_med, 1e-9)
    threshold = 3.0
    passed = speedup >= threshold
    return ScenarioResult(
        name="cache-hit-speedup",
        concern="Cache lookup and dependency verification must stay much cheaper than recompute.",
        metric_name="cold_to_hot_speedup",
        metric_value=speedup,
        unit="x",
        threshold=threshold,
        comparator=">=",
        passed=passed,
        details={
            "rounds": rounds,
            "cold_median_ms": cold_med,
            "hot_median_ms": hot_med,
        },
    )


def _scenario_dedup_contention() -> ScenarioResult:
    engine = Engine()
    callers = 32
    lock = threading.Lock()
    runs = 0

    @engine.input
    def base() -> int:
        return 0

    @engine.query
    def expensive() -> int:
        nonlocal runs
        with lock:
            runs += 1
        time.sleep(0.05)
        return base() + 1

    base.set(41)
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=callers) as pool:
        futures = [pool.submit(expensive) for _ in range(callers)]
        values = [future.result(timeout=5.0) for future in futures]
    wall_ms = (time.perf_counter() - start) * 1000.0

    if values != [42] * callers:
        raise AssertionError("dedup scenario produced unexpected query values")

    collapse_ratio = callers / max(runs, 1)
    threshold = 16.0
    passed = runs == 1 and collapse_ratio >= threshold and wall_ms <= 500.0
    return ScenarioResult(
        name="dedup-under-contention",
        concern="Concurrent identical queries should collapse to one in-flight compute.",
        metric_name="caller_to_compute_collapse",
        metric_value=collapse_ratio,
        unit="x",
        threshold=threshold,
        comparator=">=",
        passed=passed,
        details={
            "callers": callers,
            "actual_computes": runs,
            "wall_time_ms": wall_ms,
            "wall_time_budget_ms": 500.0,
        },
    )


def _run_compute_many_once(workers: int, calls_count: int, sleep_seconds: float) -> float:
    engine = Engine()

    @engine.input
    def base(i: int) -> int:
        return 0

    @engine.query
    def job(i: int) -> int:
        time.sleep(sleep_seconds)
        return base(i) + 10

    for i in range(calls_count):
        base.set(i, i)

    start = time.perf_counter()
    result = engine.compute_many([(job, (i,)) for i in range(calls_count)], workers=workers)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    expected = [i + 10 for i in range(calls_count)]
    if result != expected:
        raise AssertionError("compute_many scenario produced unexpected values")
    return elapsed_ms


def _scenario_parallel_scheduler_speedup() -> ScenarioResult:
    calls_count = 48
    sleep_seconds = 0.008
    serial_samples = [
        _run_compute_many_once(workers=1, calls_count=calls_count, sleep_seconds=sleep_seconds) for _ in range(3)
    ]
    parallel_samples = [
        _run_compute_many_once(workers=8, calls_count=calls_count, sleep_seconds=sleep_seconds) for _ in range(3)
    ]
    serial_ms = _median(serial_samples)
    parallel_ms = _median(parallel_samples)
    speedup = serial_ms / max(parallel_ms, 1e-9)
    threshold = 2.5
    passed = speedup >= threshold
    return ScenarioResult(
        name="compute-many-parallel-speedup",
        concern="Work-stealing scheduling should provide meaningful overlap on GIL-releasing tasks.",
        metric_name="workers8_vs_workers1_speedup",
        metric_value=speedup,
        unit="x",
        threshold=threshold,
        comparator=">=",
        passed=passed,
        details={
            "calls": calls_count,
            "sleep_seconds_per_task": sleep_seconds,
            "serial_ms": serial_ms,
            "parallel_ms": parallel_ms,
        },
    )


def run_performance_suite() -> dict[str, Any]:
    results = [
        _scenario_cache_hit_speedup(),
        _scenario_dedup_contention(),
        _scenario_parallel_scheduler_speedup(),
    ]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "results": [asdict(item) for item in results],
        "all_passed": all(item.passed for item in results),
    }


def assert_report_thresholds(report: dict[str, Any]) -> None:
    failed = [row for row in report["results"] if not row["passed"]]
    if not failed:
        return
    lines = ["Performance regression thresholds failed:"]
    for row in failed:
        lines.append(
            f"- {row['name']}: {row['metric_name']}={row['metric_value']:.3f}{row['unit']} "
            f"(expected {row['comparator']} {row['threshold']:.3f}{row['unit']})"
        )
    raise AssertionError("\n".join(lines))


def render_markdown_report(report: dict[str, Any]) -> str:
    header = [
        "# Performance Report",
        "",
        f"- Generated (UTC): `{report['generated_at_utc']}`",
        f"- Python: `{report['python_version']}`",
        f"- Platform: `{report['platform']}`",
        "",
        "## Covered concerns",
        "",
        "1. Cache hit and dependency verification cost versus full recomputation.",
        "2. In-flight query deduplication behavior during heavy contention.",
        "3. Work-stealing scheduler throughput on GIL-releasing concurrent tasks.",
        "",
        "## Scenario results",
        "",
        "| Scenario | Metric | Observed | Threshold | Status |",
        "| --- | --- | ---: | ---: | --- |",
    ]

    rows: list[str] = []
    for result in report["results"]:
        status = "PASS" if result["passed"] else "FAIL"
        observed = f"{result['metric_value']:.3f}{result['unit']}"
        threshold = f"{result['comparator']} {result['threshold']:.3f}{result['unit']}"
        rows.append(
            f"| {result['name']} | {result['metric_name']} | {observed} | {threshold} | {status} |"
        )

    lines = header + rows + ["", "## Raw details", "", "```json", json.dumps(report, indent=2), "```", ""]
    return "\n".join(lines)


def write_performance_report(report: dict[str, Any], report_dir: Path) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    json_path = report_dir / "performance-report.json"
    md_path = report_dir / "performance-report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    md_path.write_text(render_markdown_report(report), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run query-cascade performance suite.")
    parser.add_argument("--report-dir", type=Path, default=Path("artifacts/performance"))
    parser.add_argument("--assert-thresholds", action="store_true")
    args = parser.parse_args()

    report = run_performance_suite()
    json_path, md_path = write_performance_report(report, args.report_dir)
    print(f"Wrote performance report JSON: {json_path}")
    print(f"Wrote performance report Markdown: {md_path}")
    if args.assert_thresholds:
        assert_report_thresholds(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
