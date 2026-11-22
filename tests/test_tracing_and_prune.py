from __future__ import annotations

import concurrent.futures


from cascade import Engine
from cascade._state import Dependency, MemoEntry
from cascade._store import GraphStore
from tests.scale_helpers import run_prune_with_timeout


def _run_trace_workload(engine: Engine) -> list[str]:
    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def compute() -> int:
        return source() + 1

    for value in range(5):
        source.set(value)
        assert compute() == value + 1
        assert compute() == value + 1

    return [event.event for event in engine.traces()]


def test_trace_limit_keeps_the_most_recent_events() -> None:
    full = Engine(trace_limit=10_000)
    limited = Engine(trace_limit=9)

    full_events = _run_trace_workload(full)
    limited_events = _run_trace_workload(limited)

    assert len(limited_events) == 9
    assert limited_events == full_events[-9:]


def test_eviction_and_prune() -> None:
    engine = Engine(max_entries=2)

    @engine.input
    def source(i: int) -> int:
        return 0

    @engine.query
    def inc(i: int) -> int:
        return source(i) + 1

    for i in range(4):
        source.set(i, i)
        assert inc(i) == i + 1
    graph = engine.inspect_graph()
    assert graph["memo_count"] <= 2

    roots = [("query", inc.id, (3,))]
    run_prune_with_timeout(engine, roots, timeout=0.5)
    graph_after = engine.inspect_graph()
    assert all(node.endswith("(3,)") for node in graph_after["nodes"])


def test_tracing_captures_events() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def compute() -> int:
        return source() + 2

    source.set(1)
    assert compute() == 3
    assert compute() == 3
    events = engine.traces()
    kinds = {e.event for e in events}
    assert "input_set" in kinds
    assert "recompute_start" in kinds
    assert "cache_hit" in kinds or "cache_green" in kinds


def test_trace_event_sequence_for_recompute_then_cache_hit() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def compute() -> int:
        return source() + 2

    source.set(5)
    assert compute() == 7
    assert compute() == 7

    events = [event.event for event in engine.traces()]
    # Deterministic happy-path trace ordering for one recompute then cache hit.
    assert events == [
        "input_set",
        "recompute_start",
        "input_read",
        "recompute_done",
        "cache_hit",
    ]


def test_prune_keeps_transitively_reachable_query_chain_only() -> None:
    engine = Engine()

    @engine.input
    def source(index: int) -> int:
        return 0

    @engine.query
    def leaf(index: int) -> int:
        return source(index) + 1

    @engine.query
    def middle(index: int) -> int:
        return leaf(index) + 10

    @engine.query
    def top(index: int) -> int:
        return middle(index) + 100

    @engine.query
    def side(index: int) -> int:
        return source(index) + 1_000

    source.set(1, 5)
    source.set(2, 8)
    assert top(1) == 116
    assert top(2) == 119
    assert side(2) == 1_008

    root = ("query", top.id, (1,))
    run_prune_with_timeout(engine, [root], timeout=0.5)
    graph = engine.inspect_graph()

    top_node = f"query:{top.id}(1,)"
    middle_node = f"query:{middle.id}(1,)"
    leaf_node = f"query:{leaf.id}(1,)"
    assert set(graph["nodes"]) == {top_node, middle_node, leaf_node}
    assert graph["memo_count"] == 3

    query_edges = {
        (parent, child)
        for parent, child in graph["edges"]
        if child.startswith("query:")
    }
    assert query_edges == {(top_node, middle_node), (middle_node, leaf_node)}


def test_prune_ignores_missing_root_without_skipping_valid_roots() -> None:
    engine = Engine()

    @engine.input
    def source(index: int) -> int:
        return 0

    @engine.query
    def leaf(index: int) -> int:
        return source(index) + 1

    @engine.query
    def middle(index: int) -> int:
        return leaf(index) + 10

    @engine.query
    def top(index: int) -> int:
        return middle(index) + 100

    source.set(0, 2)
    source.set(1, 3)
    assert top(0) == 113
    assert top(1) == 114

    missing_root = ("query", top.id, (999,))
    valid_root = ("query", top.id, (1,))
    run_prune_with_timeout(engine, [missing_root, valid_root], timeout=0.5)
    graph = engine.inspect_graph()

    expected_nodes = {
        f"query:{top.id}(1,)",
        f"query:{middle.id}(1,)",
        f"query:{leaf.id}(1,)",
    }
    assert set(graph["nodes"]) == expected_nodes
    assert graph["memo_count"] == 3


def test_prune_terminates_for_non_empty_root_set() -> None:
    engine = Engine()

    @engine.input
    def source(index: int) -> int:
        return 0

    @engine.query
    def leaf(index: int) -> int:
        return source(index) + 1

    @engine.query
    def top(index: int) -> int:
        return leaf(index) + 10

    source.set(0, 1)
    assert top(0) == 12
    root = ("query", top.id, (0,))

    # prune() should not spin indefinitely on valid non-empty roots.
    run_prune_with_timeout(engine, [root], timeout=0.5)


def test_prune_uses_query_only_reachability_for_dependency_expansion() -> None:
    store = GraphStore(max_entries=32, trace_limit=64)

    q_root = ("query", "q_root", ())
    q_child = ("query", "q_child", ())
    i_leaf = ("input", "i_leaf", ())

    store.memos[q_root] = MemoEntry(
        value=1,
        value_hash="root",
        changed_at=1,
        verified_at=1,
        deps=(Dependency(key=q_child, observed_changed_at=1),),
        effects={},
        last_access=1,
    )
    store.memos[q_child] = MemoEntry(
        value=2,
        value_hash="child",
        changed_at=1,
        verified_at=1,
        deps=(Dependency(key=i_leaf, observed_changed_at=1),),
        effects={},
        last_access=2,
    )

    store.prune([q_root])

    assert set(store.memos) == {q_root, q_child}


def test_prune_does_not_infinite_loop_with_query_cycle_when_root_reachable() -> None:
    store = GraphStore(max_entries=32, trace_limit=64)
    q_a = ("query", "q_a", ())
    q_b = ("query", "q_b", ())

    store.memos[q_a] = MemoEntry(
        value="a",
        value_hash="a",
        changed_at=1,
        verified_at=1,
        deps=(Dependency(key=q_b, observed_changed_at=1),),
        effects={},
        last_access=1,
    )
    store.memos[q_b] = MemoEntry(
        value="b",
        value_hash="b",
        changed_at=1,
        verified_at=1,
        deps=(Dependency(key=q_a, observed_changed_at=1),),
        effects={},
        last_access=2,
    )

    finished = concurrent.futures.Future[None]()

    def run_prune() -> None:
        try:
            store.prune([q_a])
            finished.set_result(None)
        except BaseException as exc:  # pragma: no cover - defensive
            finished.set_exception(exc)

    worker = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        worker.submit(run_prune)
        finished.result(timeout=0.5)
    finally:
        worker.shutdown(wait=False, cancel_futures=True)

    assert set(store.memos) == {q_a, q_b}


def test_prune_terminates_even_with_query_cycle_in_memo_graph() -> None:
    store = GraphStore(max_entries=32, trace_limit=64)

    q_a = ("query", "q_a", ())
    q_b = ("query", "q_b", ())
    q_c = ("query", "q_c", ())

    store.memos[q_a] = MemoEntry(
        value=1,
        value_hash="a",
        changed_at=1,
        verified_at=1,
        deps=(Dependency(key=q_b, observed_changed_at=1),),
        effects={},
        last_access=1,
    )
    store.memos[q_b] = MemoEntry(
        value=2,
        value_hash="b",
        changed_at=1,
        verified_at=1,
        deps=(Dependency(key=q_a, observed_changed_at=1),),
        effects={},
        last_access=2,
    )
    store.memos[q_c] = MemoEntry(
        value=3,
        value_hash="c",
        changed_at=1,
        verified_at=1,
        deps=(),
        effects={},
        last_access=3,
    )

    done = concurrent.futures.Future[None]()

    def run_prune() -> None:
        try:
            store.prune([q_a])
            done.set_result(None)
        except BaseException as exc:  # pragma: no cover - defensive
            done.set_exception(exc)

    worker = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        worker.submit(run_prune)
        done.result(timeout=0.5)
    finally:
        worker.shutdown(wait=False, cancel_futures=True)

    assert set(store.memos) == {q_a, q_b}
