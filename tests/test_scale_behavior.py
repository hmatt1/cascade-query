from __future__ import annotations

import concurrent.futures
import threading
import time
from collections import defaultdict
from pathlib import Path

import pytest

from cascade import Engine, QueryCancelled
from tests.scale_helpers import (
    build_fanout_chain_pipeline,
    cached_query_args,
    expected_fanout_chain_total,
)


def test_giant_graph_selective_invalidation_counts() -> None:
    # Guards against regressions where a tiny input change recomputes the full graph.
    engine = Engine(trace_limit=200_000)
    depth = 10
    fanout = 64
    counts: dict[str, int] = defaultdict(int)
    leaf, _, aggregate = build_fanout_chain_pipeline(engine, depth=depth, fanout=fanout, counts=counts)

    values = [index * 3 for index in range(fanout)]
    for branch, value in enumerate(values):
        leaf.set(branch, value)
    assert aggregate() == expected_fanout_chain_total(depth=depth, fanout=fanout, values=values)

    baseline = dict(counts)
    mutated_branch = fanout // 2
    values[mutated_branch] += 11
    leaf.set(mutated_branch, values[mutated_branch])
    assert aggregate() == expected_fanout_chain_total(depth=depth, fanout=fanout, values=values)

    for level in range(depth):
        key = f"level_{level}"
        assert counts[key] - baseline.get(key, 0) == 1
    assert counts["aggregate"] - baseline.get("aggregate", 0) == 1

    no_change_baseline = dict(counts)
    leaf.set(mutated_branch, values[mutated_branch])
    assert aggregate() == expected_fanout_chain_total(depth=depth, fanout=fanout, values=values)
    for level in range(depth):
        key = f"level_{level}"
        assert counts[key] - no_change_baseline.get(key, 0) == 0
    assert counts["aggregate"] - no_change_baseline.get("aggregate", 0) == 0


def test_dependency_churn_rewrites_dynamic_edges() -> None:
    # Guards against stale dependency edges surviving dynamic branch rewrites.
    engine = Engine()
    counts_lock = threading.Lock()

    @engine.input
    def mode() -> int:
        return 0

    @engine.input
    def source(index: int) -> int:
        return 0

    @engine.query
    def dynamic_value(index: int) -> int:
        # Free-threaded builds can execute query bodies truly concurrently, so
        # shared test counters need synchronization to avoid lost updates.
        with counts_lock:
            counts["dynamic"] += 1
        pivot = index if mode() == 0 else index + 1_000
        return source(pivot) * 2

    width = 48
    counts: dict[str, int] = defaultdict(int)
    for idx in range(width):
        source.set(idx, idx)
        source.set(idx + 1_000, idx + 100)

    mode.set(0)
    assert engine.compute_many([(dynamic_value, (idx,)) for idx in range(width)], workers=6) == [idx * 2 for idx in range(width)]
    assert counts["dynamic"] == width

    mode.set(1)
    assert engine.compute_many([(dynamic_value, (idx,)) for idx in range(width)], workers=6) == [
        (idx + 100) * 2 for idx in range(width)
    ]
    assert counts["dynamic"] == width * 2

    # After switching to mode=1, old branch inputs must no longer invalidate.
    source.set(0, 999)
    assert dynamic_value(0) == 200
    assert counts["dynamic"] == width * 2

    # New branch inputs should invalidate and force recompute.
    source.set(1_000, 777)
    assert dynamic_value(0) == 1_554
    assert counts["dynamic"] == width * 2 + 1


@pytest.mark.slow
def test_prune_stress_keeps_consistent_reachable_subgraph() -> None:
    # Guards against prune leaving orphan links or unreachable memo entries.
    engine = Engine()
    depth = 9
    fanout = 180
    leaf, levels, aggregate = build_fanout_chain_pipeline(engine, depth=depth, fanout=fanout)

    for idx in range(fanout):
        leaf.set(idx, idx)
    assert aggregate() == expected_fanout_chain_total(depth=depth, fanout=fanout, values=list(range(fanout)))

    root_branch = fanout - 1
    engine.prune([("query", levels[-1].id, (root_branch,))])
    graph = engine.inspect_graph()

    assert graph["memo_count"] == depth
    for level in levels:
        args = cached_query_args(graph, level.id)
        assert args == {(root_branch,)}

    node_set = set(graph["nodes"])
    for parent, child in graph["edges"]:
        assert parent in node_set
        if child.startswith("query:"):
            assert child in node_set


@pytest.mark.slow
def test_persistence_scale_roundtrip_preserves_hot_cache(tmp_path: Path) -> None:
    # Guards against persistence/load paths dropping large memo state and forcing full recompute.
    db_path = tmp_path / "large-roundtrip.db"
    depth = 8
    fanout = 120

    engine_a = Engine()
    leaf_a, _, aggregate_a = build_fanout_chain_pipeline(engine_a, depth=depth, fanout=fanout)
    values = [(idx * 5) % 97 for idx in range(fanout)]
    for idx, value in enumerate(values):
        leaf_a.set(idx, value)
    assert aggregate_a() == expected_fanout_chain_total(depth=depth, fanout=fanout, values=values)
    engine_a.save(str(db_path))

    counts_b: dict[str, int] = defaultdict(int)
    engine_b = Engine()
    leaf_b, _, aggregate_b = build_fanout_chain_pipeline(engine_b, depth=depth, fanout=fanout, counts=counts_b)
    engine_b.load(str(db_path))

    assert aggregate_b() == expected_fanout_chain_total(depth=depth, fanout=fanout, values=values)
    assert counts_b == {}

    changed_branch = fanout // 3
    values[changed_branch] += 9
    leaf_b.set(changed_branch, values[changed_branch])
    assert aggregate_b() == expected_fanout_chain_total(depth=depth, fanout=fanout, values=values)
    for level in range(depth):
        assert counts_b[f"level_{level}"] == 1
    assert counts_b["aggregate"] == 1


def test_eviction_policy_lru_behavior_under_churn() -> None:
    # Guards against eviction degenerating into random drops under heavy cache churn.
    max_entries = 96
    engine = Engine(max_entries=max_entries)

    @engine.input
    def source(index: int) -> int:
        return 0

    @engine.query
    def value(index: int) -> int:
        return source(index) + 1

    initial = 128
    for idx in range(initial):
        source.set(idx, idx)
        assert value(idx) == idx + 1

    hot = list(range(16))
    for idx in hot:
        assert value(idx) == idx + 1

    for idx in range(initial, initial + 64):
        source.set(idx, idx)
        assert value(idx) == idx + 1

    graph = engine.inspect_graph()
    cached = cached_query_args(graph, value.id)
    assert len(cached) <= max_entries
    assert all((idx,) in cached for idx in hot)
    evicted_cold = sum((idx,) not in cached for idx in range(16, initial))
    assert evicted_cold >= 48

    for idx in hot:
        assert value(idx) == idx + 1
    for idx in range(initial + 64, initial + 128):
        source.set(idx, idx)
        assert value(idx) == idx + 1

    graph_after = engine.inspect_graph()
    cached_after = cached_query_args(graph_after, value.id)
    hot_survivors = sum((idx,) in cached_after for idx in hot)
    assert hot_survivors >= 12
    assert len(cached_after) <= max_entries


@pytest.mark.slow
def test_concurrency_stress_submit_compute_many_and_writes() -> None:
    # Guards against deadlocks and cancellation regressions under mixed concurrent workloads.
    engine = Engine()
    width = 24

    @engine.input
    def cell(index: int) -> int:
        return 0

    @engine.query
    def read_cell(index: int) -> int:
        time.sleep(0.0015)
        return cell(index)

    @engine.query
    def sum_cells() -> int:
        return sum(read_cell(index) for index in range(width))

    for round_index in range(3):
        for idx in range(width):
            cell.set(idx, round_index)

        snapshot = engine.snapshot()
        baseline_total = round_index * width

        def churn_inputs() -> None:
            for idx in range(width):
                cell.set(idx, round_index + 100)
                time.sleep(0.0008)

        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
            submitted = [engine.submit(sum_cells, executor=pool) for _ in range(8)]
            frozen_reads_future = pool.submit(
                lambda: engine.compute_many([(read_cell, (idx,)) for idx in range(width)], workers=4, snapshot=snapshot)
            )
            writer = threading.Thread(target=churn_inputs, daemon=True)
            writer.start()

            frozen_values = frozen_reads_future.result(timeout=6.0)
            writer.join(timeout=6.0)
            assert not writer.is_alive()

            cancellations = 0
            successful: list[int] = []
            for future in submitted:
                try:
                    successful.append(future.result(timeout=6.0))
                except QueryCancelled:
                    cancellations += 1

        assert frozen_values == [round_index] * width
        assert cancellations >= 1
        assert all(result == baseline_total for result in successful)
        assert sum_cells() == (round_index + 100) * width
