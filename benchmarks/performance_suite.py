from __future__ import annotations

import argparse
import json
import platform
import statistics
import sys
import sysconfig
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
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


def _build_fanout_chain_pipeline(
    engine: Engine,
    *,
    depth: int,
    fanout: int,
    counts: dict[str, int] | None = None,
) -> tuple[Any, list[Any], Any]:
    if depth < 1 or fanout < 1:
        raise ValueError("depth and fanout must be >= 1")
    call_counts = counts if counts is not None else defaultdict(int)

    @engine.input
    def leaf(branch: int) -> int:
        return 0

    levels: list[Any] = []
    prev = None
    for level in range(depth):

        def _make_level(level_index: int, prev_level: Any) -> Any:
            def level_query(branch: int) -> int:
                call_counts[f"level_{level_index}"] += 1
                base = leaf(branch) if prev_level is None else prev_level(branch)
                return base + (level_index + 1)

            level_query.__name__ = f"bench_level_{level_index}"
            level_query.__qualname__ = f"bench_synthetic_level_{level_index}"
            return engine.query(level_query)

        current = _make_level(level, prev)
        levels.append(current)
        prev = current

    top = levels[-1]

    def aggregate_query() -> int:
        call_counts["aggregate"] += 1
        return sum(top(branch) for branch in range(fanout))

    aggregate_query.__name__ = "bench_aggregate_query"
    aggregate_query.__qualname__ = "bench_synthetic_aggregate_query"
    aggregate = engine.query(aggregate_query)
    return leaf, levels, aggregate


def _measure_median_ms(fn: Any, *, rounds: int) -> float:
    samples: list[float] = []
    for _ in range(rounds):
        start = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - start) * 1000.0)
    return _median(samples)


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


def _cpu_burn(seed: int, *, iterations: int) -> int:
    value = seed + 1
    for step in range(iterations):
        value = (value * 1_664_525 + 1_013_904_223 + step) & 0xFFFFFFFF
    return value


def _run_compute_many_once(workers: int, calls_count: int, iterations: int) -> float:
    engine = Engine()

    @engine.input
    def base(i: int) -> int:
        return 0

    @engine.query
    def job(i: int) -> int:
        return _cpu_burn(i, iterations=iterations) + base(i)

    for i in range(calls_count):
        base.set(i, i)

    start = time.perf_counter()
    result = engine.compute_many([(job, (i,)) for i in range(calls_count)], workers=workers)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    expected = [_cpu_burn(i, iterations=iterations) + i for i in range(calls_count)]
    if result != expected:
        raise AssertionError("compute_many scenario produced unexpected values")
    return elapsed_ms


def _scenario_parallel_scheduler_speedup() -> ScenarioResult:
    calls_count = 32
    iterations = 250_000
    workers = 8
    serial_samples = [
        _run_compute_many_once(workers=1, calls_count=calls_count, iterations=iterations) for _ in range(4)
    ]
    parallel_samples = [
        _run_compute_many_once(workers=workers, calls_count=calls_count, iterations=iterations) for _ in range(4)
    ]
    serial_ms = _median(serial_samples)
    parallel_ms = _median(parallel_samples)
    speedup = serial_ms / max(parallel_ms, 1e-9)
    threshold = 1.15
    passed = speedup >= threshold
    return ScenarioResult(
        name="compute-many-parallel-speedup",
        concern="Work-stealing scheduling should provide meaningful overlap on CPU-bound tasks under free-threaded Python.",
        metric_name="workers8_vs_workers1_speedup",
        metric_value=speedup,
        unit="x",
        threshold=threshold,
        comparator=">=",
        passed=passed,
        details={
            "calls": calls_count,
            "iterations_per_task": iterations,
            "workers": workers,
            "serial_ms": serial_ms,
            "parallel_ms": parallel_ms,
        },
    )


def _scenario_giant_graph_targeted_mutation_latency() -> ScenarioResult:
    engine = Engine()
    depth = 10
    fanout = 256
    rounds = 5
    leaf, _, aggregate = _build_fanout_chain_pipeline(engine, depth=depth, fanout=fanout)
    values = [index * 7 for index in range(fanout)]
    for branch, value in enumerate(values):
        leaf.set(branch, value)
    aggregate()

    targeted_ms: list[float] = []
    full_ms: list[float] = []
    for round_index in range(rounds):
        target = (round_index * 13) % fanout
        values[target] += 1
        leaf.set(target, values[target])
        start = time.perf_counter()
        aggregate()
        targeted_ms.append((time.perf_counter() - start) * 1000.0)

        for branch in range(fanout):
            values[branch] += 1
            leaf.set(branch, values[branch])
        start = time.perf_counter()
        aggregate()
        full_ms.append((time.perf_counter() - start) * 1000.0)

    targeted_med = _median(targeted_ms)
    full_med = _median(full_ms)
    speedup = full_med / max(targeted_med, 1e-9)
    threshold = 3.0
    passed = speedup >= threshold
    return ScenarioResult(
        name="giant-graph-targeted-mutation-latency",
        concern="Small edits should recompute only a narrow dependency cone instead of full graph rebuild.",
        metric_name="full_rebuild_vs_targeted_speedup",
        metric_value=speedup,
        unit="x",
        threshold=threshold,
        comparator=">=",
        passed=passed,
        details={
            "rounds": rounds,
            "depth": depth,
            "fanout": fanout,
            "targeted_median_ms": targeted_med,
            "full_rebuild_median_ms": full_med,
        },
    )


def _scenario_mark_green_depth_overhead() -> ScenarioResult:
    rounds = 7
    depths = [8, 24, 48]
    medians: dict[int, float] = {}

    for depth in depths:
        engine = Engine()

        @engine.input
        def leaf() -> int:
            return 0

        @engine.input
        def noise(step: int) -> int:
            return 0

        previous = None
        for level in range(depth):

            def _make_level(level_index: int, prev_level: Any) -> Any:
                def level_query() -> int:
                    base = leaf() if prev_level is None else prev_level()
                    return base + (level_index + 1)

                level_query.__name__ = f"depth_probe_{depth}_{level_index}"
                level_query.__qualname__ = f"depth_probe_{depth}_{level_index}"
                return engine.query(level_query)

            previous = _make_level(level, previous)

        if previous is None:  # pragma: no cover - safety belt
            raise AssertionError("expected non-empty chain")
        top = previous

        leaf.set(3)
        assert top() > 0

        verify_ms: list[float] = []
        for step in range(rounds):
            noise.set(step, step)
            start = time.perf_counter()
            top()
            verify_ms.append((time.perf_counter() - start) * 1000.0)
        medians[depth] = _median(verify_ms)

    shallow = medians[depths[0]]
    deep = medians[depths[-1]]
    growth = deep / max(shallow, 1e-9)
    threshold = 1.4
    passed = growth >= threshold
    return ScenarioResult(
        name="mark-green-depth-overhead",
        concern="Dependency verification overhead should reflect dependency depth and remain measurable at scale.",
        metric_name=f"depth{depths[-1]}_vs_depth{depths[0]}_verification_growth",
        metric_value=growth,
        unit="x",
        threshold=threshold,
        comparator=">=",
        passed=passed,
        details={
            "depth_medians_ms": {str(depth): medians[depth] for depth in depths},
            "rounds_per_depth": rounds,
        },
    )


def _scenario_prune_runtime_scaling() -> ScenarioResult:
    rounds = 5
    depth = 8
    workloads = {
        "small": 120,
        "medium": 360,
        "large": 900,
    }
    medians: dict[str, float] = {}
    memo_counts: dict[str, int] = {}

    for label, fanout in workloads.items():
        samples: list[float] = []
        for round_index in range(rounds):
            engine = Engine()
            leaf, levels, aggregate = _build_fanout_chain_pipeline(engine, depth=depth, fanout=fanout)
            for idx in range(fanout):
                leaf.set(idx, idx + round_index)
            aggregate()
            memo_counts[label] = engine.inspect_graph()["memo_count"]
            root = ("query", levels[-1].id, (fanout - 1,))
            start = time.perf_counter()
            engine.prune([root])
            samples.append((time.perf_counter() - start) * 1000.0)
        medians[label] = _median(samples)

    small = medians["small"]
    medium = medians["medium"]
    large = medians["large"]
    monotonic = small < medium < large
    node_scale = memo_counts["large"] / max(memo_counts["small"], 1)
    runtime_scale = large / max(small, 1e-9)
    threshold = node_scale * 2.2
    passed = monotonic and runtime_scale <= threshold
    return ScenarioResult(
        name="prune-runtime-scaling",
        concern="Prune should scale with graph size without pathological blowups.",
        metric_name="large_vs_small_runtime_scale",
        metric_value=runtime_scale,
        unit="x",
        threshold=threshold,
        comparator="<=",
        passed=passed,
        details={
            "rounds": rounds,
            "depth": depth,
            "fanouts": workloads,
            "memo_counts": memo_counts,
            "median_ms": medians,
            "monotonic_small_medium_large": monotonic,
            "node_scale_large_vs_small": node_scale,
        },
    )


def run_performance_suite() -> dict[str, Any]:
    gil_probe = getattr(sys, "_is_gil_enabled", None)
    gil_enabled = bool(gil_probe()) if callable(gil_probe) else None
    if gil_enabled is None:
        runtime_mode = "unknown"
    elif gil_enabled:
        runtime_mode = "gil-enabled"
    else:
        runtime_mode = "gil-disabled"

    results = [
        _scenario_cache_hit_speedup(),
        _scenario_dedup_contention(),
        _scenario_parallel_scheduler_speedup(),
        _scenario_giant_graph_targeted_mutation_latency(),
        _scenario_mark_green_depth_overhead(),
        _scenario_prune_runtime_scaling(),
    ]
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "py_gil_disabled": sysconfig.get_config_var("Py_GIL_DISABLED"),
        "gil_enabled": gil_enabled,
        "runtime_mode": runtime_mode,
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
    runtime_mode = report.get("runtime_mode", "unknown")
    py_gil_disabled = report.get("py_gil_disabled", "unknown")
    gil_enabled = report.get("gil_enabled", "unknown")
    header = [
        "# Performance Report",
        "",
        f"- Generated (UTC): `{report['generated_at_utc']}`",
        f"- Python: `{report['python_version']}`",
        f"- Platform: `{report['platform']}`",
        f"- Runtime mode: `{runtime_mode}`",
        f"- Build flag `Py_GIL_DISABLED`: `{py_gil_disabled}`",
        f"- Runtime GIL enabled: `{gil_enabled}`",
        "",
        "## Covered concerns",
        "",
        "1. Cache hit and dependency verification cost versus full recomputation.",
        "2. In-flight query deduplication behavior during heavy contention.",
        "3. Work-stealing scheduler throughput on CPU-bound concurrent tasks under free-threaded Python.",
        "4. Targeted giant-graph mutation latency versus full-graph rebuild latency.",
        "5. Mark-green verification overhead as dependency depth increases.",
        "6. Prune runtime scaling from small to large memo graphs.",
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
