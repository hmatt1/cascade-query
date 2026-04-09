from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import sysconfig
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from cascade import Engine
from cascade._synthetic_graph import build_fanout_chain_pipeline


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


def _effective_cpu_count() -> int:
    count = max(1, os.cpu_count() or 1)
    if hasattr(os, "sched_getaffinity"):
        count = min(count, max(1, len(os.sched_getaffinity(0))))
    cpu_max = Path("/sys/fs/cgroup/cpu.max")
    if cpu_max.exists():
        raw = cpu_max.read_text(encoding="utf-8").strip().split()
        if len(raw) >= 2 and raw[0] != "max":
            quota = int(raw[0])
            period = max(int(raw[1]), 1)
            count = min(count, max(1, quota // period))
    return count


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    return float(statistics.median(values))


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

    # Baseline one-owner compute cost on current hardware.
    base.set(41)
    start = time.perf_counter()
    baseline_value = expensive()
    owner_compute_ms = (time.perf_counter() - start) * 1000.0
    if baseline_value != 42:
        raise AssertionError("dedup baseline produced unexpected query value")

    # Force one fresh recompute that all contenders should collapse onto.
    base.set(42)
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=callers) as pool:
        futures = [pool.submit(expensive) for _ in range(callers)]
        values = [future.result(timeout=5.0) for future in futures]
    wall_ms = (time.perf_counter() - start) * 1000.0

    if values != [43] * callers:
        raise AssertionError("dedup scenario produced unexpected query values")

    contention_computes = max(runs - 1, 0)
    collapse_ratio = callers / max(contention_computes, 1)
    threshold = 16.0
    # Scale wall-time budget by measured owner compute to reduce false negatives
    # on slower CI/VM hardware while keeping a strict collapse expectation.
    wall_budget_ms = max(500.0, owner_compute_ms * 8.0)
    passed = contention_computes == 1 and collapse_ratio >= threshold and wall_ms <= wall_budget_ms
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
            "actual_computes": contention_computes,
            "wall_time_ms": wall_ms,
            "wall_time_budget_ms": wall_budget_ms,
            "single_owner_baseline_ms": owner_compute_ms,
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
    available_cpus = _effective_cpu_count()
    workers = min(8, max(2, available_cpus))
    rounds = 4
    serial_samples: list[float] = []
    parallel_samples: list[float] = []
    pair_speedups: list[float] = []
    # Interleaving serial and parallel rounds dampens host-load drift noise.
    for _ in range(rounds):
        serial_ms = _run_compute_many_once(workers=1, calls_count=calls_count, iterations=iterations)
        parallel_ms = _run_compute_many_once(workers=workers, calls_count=calls_count, iterations=iterations)
        serial_samples.append(serial_ms)
        parallel_samples.append(parallel_ms)
        pair_speedups.append(serial_ms / max(parallel_ms, 1e-9))
    serial_ms = _median(serial_samples)
    parallel_ms = _median(parallel_samples)
    speedup = _median(pair_speedups)
    threshold = 1.1
    passed = speedup >= threshold
    return ScenarioResult(
        name="compute-many-parallel-speedup",
        concern="Work-stealing scheduling should provide meaningful overlap on CPU-bound tasks under free-threaded Python.",
        metric_name=f"workers{workers}_vs_workers1_speedup",
        metric_value=speedup,
        unit="x",
        threshold=threshold,
        comparator=">=",
        passed=passed,
        details={
            "calls": calls_count,
            "iterations_per_task": iterations,
            "rounds": rounds,
            "workers": workers,
            "available_cpus": available_cpus,
            "serial_ms": serial_ms,
            "parallel_ms": parallel_ms,
        },
    )


def _scenario_giant_graph_targeted_mutation_latency() -> ScenarioResult:
    engine = Engine()
    depth = 10
    fanout = 256
    rounds = 5
    leaf, _, aggregate = build_fanout_chain_pipeline(
        engine,
        depth=depth,
        fanout=fanout,
        name_prefix="bench_synthetic",
    )
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
            leaf, levels, aggregate = build_fanout_chain_pipeline(
                engine,
                depth=depth,
                fanout=fanout,
                name_prefix="bench_synthetic",
            )
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


_SCENARIO_FACTORIES: tuple[tuple[str, Callable[[], ScenarioResult]], ...] = (
    ("cache-hit-speedup", _scenario_cache_hit_speedup),
    ("dedup-under-contention", _scenario_dedup_contention),
    ("compute-many-parallel-speedup", _scenario_parallel_scheduler_speedup),
    ("giant-graph-targeted-mutation-latency", _scenario_giant_graph_targeted_mutation_latency),
    ("mark-green-depth-overhead", _scenario_mark_green_depth_overhead),
    ("prune-runtime-scaling", _scenario_prune_runtime_scaling),
)
_SCENARIO_BY_NAME: dict[str, Callable[[], ScenarioResult]] = dict(_SCENARIO_FACTORIES)
PARALLEL_SPEEDUP_SCENARIO = "compute-many-parallel-speedup"
# The parallel scheduler speedup probe is intentionally asserted in a dedicated
# test/CI step because it is the most VM-load-sensitive scenario.
PERFORMANCE_ASSERTION_EXCLUDED_SCENARIOS: frozenset[str] = frozenset({PARALLEL_SPEEDUP_SCENARIO})


def _selected_scenarios(
    *,
    include_scenarios: set[str] | None = None,
    exclude_scenarios: set[str] | None = None,
) -> list[Callable[[], ScenarioResult]]:
    known = set(_SCENARIO_BY_NAME)
    unknown_includes = (include_scenarios or set()) - known
    unknown_excludes = (exclude_scenarios or set()) - known
    unknown = unknown_includes | unknown_excludes
    if unknown:
        raise ValueError(f"Unknown performance scenario(s): {', '.join(sorted(unknown))}")

    selected: list[Callable[[], ScenarioResult]] = []
    for name, factory in _SCENARIO_FACTORIES:
        if include_scenarios is not None and name not in include_scenarios:
            continue
        if exclude_scenarios is not None and name in exclude_scenarios:
            continue
        selected.append(factory)
    return selected


def run_performance_scenario(name: str) -> ScenarioResult:
    if name not in _SCENARIO_BY_NAME:
        raise ValueError(f"Unknown performance scenario: {name}")
    return _SCENARIO_BY_NAME[name]()


def run_performance_suite() -> dict[str, Any]:
    return run_performance_suite_with_filters()


def run_performance_suite_with_filters(
    *,
    include_scenarios: set[str] | None = None,
    exclude_scenarios: set[str] | None = None,
) -> dict[str, Any]:
    gil_probe = getattr(sys, "_is_gil_enabled", None)
    gil_enabled = bool(gil_probe()) if callable(gil_probe) else None
    if gil_enabled is None:
        runtime_mode = "unknown"
    elif gil_enabled:
        runtime_mode = "gil-enabled"
    else:
        runtime_mode = "gil-disabled"

    results = [factory() for factory in _selected_scenarios(include_scenarios=include_scenarios, exclude_scenarios=exclude_scenarios)]
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


def assert_report_thresholds(
    report: dict[str, Any],
    *,
    excluded_scenarios: set[str] | frozenset[str] | None = None,
) -> None:
    excluded = (
        set(PERFORMANCE_ASSERTION_EXCLUDED_SCENARIOS) if excluded_scenarios is None else set(excluded_scenarios)
    )
    failed = [row for row in report["results"] if not row["passed"] and row["name"] not in excluded]
    if not failed:
        return
    lines = ["Performance regression thresholds failed:"]
    for row in failed:
        lines.append(
            f"- {row['name']}: {row['metric_name']}={row['metric_value']:.3f}{row['unit']} "
            f"(expected {row['comparator']} {row['threshold']:.3f}{row['unit']})"
        )
    raise AssertionError("\n".join(lines))


def assert_parallel_speedup_scenario_threshold(result: ScenarioResult) -> None:
    if result.name != PARALLEL_SPEEDUP_SCENARIO:
        raise AssertionError(
            f"Expected scenario '{PARALLEL_SPEEDUP_SCENARIO}', got '{result.name}'."
        )
    if result.passed:
        return
    raise AssertionError(
        "Performance regression thresholds failed:\n"
        f"- {result.name}: {result.metric_name}={result.metric_value:.3f}{result.unit} "
        f"(expected {result.comparator} {result.threshold:.3f}{result.unit})"
    )


def assert_parallel_scheduler_speedup_threshold(report: dict[str, Any]) -> None:
    for row in report["results"]:
        if row["name"] != PARALLEL_SPEEDUP_SCENARIO:
            continue
        if row["passed"]:
            return
        raise AssertionError(
            "Performance regression thresholds failed:\n"
            f"- {row['name']}: {row['metric_name']}={row['metric_value']:.3f}{row['unit']} "
            f"(expected {row['comparator']} {row['threshold']:.3f}{row['unit']})"
        )
    raise AssertionError(f"Missing performance scenario: {PARALLEL_SPEEDUP_SCENARIO}")


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
    parser.add_argument("--include-scenarios", nargs="*", default=None)
    parser.add_argument("--exclude-scenarios", nargs="*", default=None)
    args = parser.parse_args()

    include_scenarios = set(args.include_scenarios) if args.include_scenarios else None
    exclude_scenarios = set(args.exclude_scenarios) if args.exclude_scenarios else None
    report = run_performance_suite_with_filters(
        include_scenarios=include_scenarios,
        exclude_scenarios=exclude_scenarios,
    )
    json_path, md_path = write_performance_report(report, args.report_dir)
    print(f"Wrote performance report JSON: {json_path}")
    print(f"Wrote performance report Markdown: {md_path}")
    if args.assert_thresholds:
        assert_report_thresholds(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
