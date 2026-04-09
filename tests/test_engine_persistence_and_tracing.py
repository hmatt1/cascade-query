from __future__ import annotations

import concurrent.futures
import pickle
import sqlite3
from pathlib import Path

import pytest

import cascade._persistence as persistence_mod
from cascade import Engine
from cascade._state import Dependency, MemoEntry
from cascade._store import GraphStore
from tests.scale_helpers import run_prune_with_timeout


def test_persistence_save_and_load(tmp_path: Path) -> None:
    db_path = tmp_path / "cascade.db"

    engine_a = Engine()

    @engine_a.input
    def source(name: str) -> str:
        return ""

    @engine_a.query
    def count_lines(name: str) -> int:
        return len([line for line in source(name).splitlines() if line.strip()])

    source.set("main", "a\nb\n")
    assert count_lines("main") == 2
    engine_a.save(str(db_path))

    engine_b = Engine()

    @engine_b.input
    def source(name: str) -> str:
        return ""

    @engine_b.query
    def count_lines(name: str) -> int:
        return len([line for line in source(name).splitlines() if line.strip()])

    engine_b.load(str(db_path))
    assert count_lines("main") == 2


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


def test_load_applies_current_trace_limit_to_persisted_trace(tmp_path: Path) -> None:
    db_path = tmp_path / "trace-limit.db"

    engine_a = Engine(trace_limit=10_000)
    full_events = _run_trace_workload(engine_a)
    engine_a.save(str(db_path))

    engine_b = Engine(trace_limit=4)
    _run_trace_workload(engine_b)
    engine_b.load(str(db_path))
    loaded_events = [event.event for event in engine_b.traces()]

    assert len(loaded_events) == 4
    assert loaded_events == full_events[-4:]


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


def test_persistence_round_trip_robustness(tmp_path: Path) -> None:
    db_path = tmp_path / "roundtrip.db"

    def build_pipeline(counter: dict[str, int] | None = None) -> tuple[Engine, object, object]:
        engine = Engine()

        @engine.input
        def source(name: str) -> str:
            return ""

        @engine.query
        def parse(name: str) -> tuple[str, ...]:
            if counter is not None:
                counter["parse"] += 1
            return tuple(line.strip() for line in source(name).splitlines() if line.strip())

        @engine.query
        def symbol_count(name: str) -> int:
            if counter is not None:
                counter["count"] += 1
            return len(parse(name))

        return engine, source, symbol_count

    engine_a, source_a, symbol_count_a = build_pipeline()

    source_a.set("a", "one\ntwo")
    source_a.set("b", "x")
    assert symbol_count_a("a") == 2
    assert symbol_count_a("b") == 1
    engine_a.save(str(db_path))

    calls = {"parse": 0, "count": 0}
    engine_b, source_b, symbol_count_b = build_pipeline(calls)

    engine_b.load(str(db_path))
    assert symbol_count_b("a") == 2
    assert symbol_count_b("b") == 1
    assert calls == {"parse": 0, "count": 0}

    source_b.set("a", "one\ntwo\nthree")
    assert symbol_count_b("a") == 3
    assert symbol_count_b("b") == 1
    assert calls["parse"] == 1
    assert calls["count"] == 1

    engine_b.save(str(db_path))

    calls_c = {"parse": 0, "count": 0}
    engine_c, _, symbol_count_c = build_pipeline(calls_c)

    engine_c.load(str(db_path))
    assert symbol_count_c("a") == 3
    assert symbol_count_c("b") == 1
    assert calls_c == {"parse": 0, "count": 0}


def test_load_missing_state_table_raises_operational_error(tmp_path: Path) -> None:
    db_path = tmp_path / "missing-table.db"
    conn = sqlite3.connect(db_path)
    try:
        # Intentionally leave out cascade_state table.
        conn.execute("create table if not exists unrelated (id integer primary key)")
        conn.commit()
    finally:
        conn.close()

    engine = Engine()
    with pytest.raises(sqlite3.OperationalError, match="no such table: cascade_state"):
        engine.load(str(db_path))


def test_load_corrupt_payload_raises_and_keeps_existing_state(tmp_path: Path) -> None:
    db_path = tmp_path / "corrupt.db"

    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def value() -> int:
        return source() + 1

    source.set(10)
    assert value() == 11
    revision_before = engine.revision
    snapshot_before = engine.snapshot()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute("create table if not exists cascade_state (id integer primary key, payload blob not null)")
        conn.execute("delete from cascade_state")
        conn.execute("insert into cascade_state(id, payload) values (1, ?)", (b"not-a-pickle",))
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(Exception):
        engine.load(str(db_path))

    # Failed load must not clobber in-memory state.
    assert engine.revision == revision_before
    assert value(snapshot=snapshot_before) == 11
    assert value() == 11


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
    assert events == ["input_set", "recompute_start", "input_read", "recompute_done", "cache_hit"]


def test_load_clears_transient_in_flight_state(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    persisted = Engine()
    persisted.save(str(db_path))

    engine = Engine()
    stale_future: concurrent.futures.Future[object] = concurrent.futures.Future()
    stale_future.set_result("stale")
    stale_key = ("query", "tests:stale", ())
    engine._in_flight[(stale_key, 123)] = stale_future  # noqa: SLF001
    assert len(engine._in_flight) == 1  # noqa: SLF001

    engine.load(str(db_path))

    # in_flight contains process-local synchronization primitives and must not
    # leak across load boundaries.
    assert engine._in_flight == {}  # noqa: SLF001


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

    query_edges = {(parent, child) for parent, child in graph["edges"] if child.startswith("query:")}
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


def test_save_payload_uses_highest_protocol_and_stable_sql_sequence(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"revision": 7, "data": {"k": [1, 2, 3]}}
    expected_blob = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    db_path = "tests:fake-path.db"
    executed: list[tuple[str, tuple[object, ...]]] = []
    commits = 0
    closes = 0

    class FakeConn:
        def execute(self, sql: str, params: tuple[object, ...] = ()) -> "FakeConn":
            executed.append((sql, params))
            return self

        def commit(self) -> None:
            nonlocal commits
            commits += 1

        def close(self) -> None:
            nonlocal closes
            closes += 1

    def fake_connect(path: str) -> FakeConn:
        assert path == db_path
        return FakeConn()

    monkeypatch.setattr(persistence_mod.sqlite3, "connect", fake_connect)
    persistence_mod.save_payload(db_path, payload)

    assert commits == 1
    assert closes == 1
    assert executed == [
        ("create table if not exists cascade_state (id integer primary key, payload blob not null)", ()),
        ("delete from cascade_state", ()),
        ("insert into cascade_state(id, payload) values (1, ?)", (expected_blob,)),
    ]


def test_save_payload_passes_explicit_highest_protocol_to_pickle(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"important": ("state", 1)}
    sentinel_blob = b"blob-by-contract"

    def fake_dumps(obj: object, *, protocol: int | None = None) -> bytes:
        assert obj is payload
        assert protocol == pickle.HIGHEST_PROTOCOL
        return sentinel_blob

    executed: list[tuple[str, tuple[object, ...]]] = []

    class FakeConn:
        def execute(self, sql: str, params: tuple[object, ...] = ()) -> "FakeConn":
            executed.append((sql, params))
            return self

        def commit(self) -> None:
            return None

        def close(self) -> None:
            return None

    monkeypatch.setattr(persistence_mod.pickle, "dumps", fake_dumps)
    monkeypatch.setattr(persistence_mod.sqlite3, "connect", lambda _: FakeConn())

    persistence_mod.save_payload("tests:fake-protocol.db", payload)

    assert executed[-1] == ("insert into cascade_state(id, payload) values (1, ?)", (sentinel_blob,))


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
