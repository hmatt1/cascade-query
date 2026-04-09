from __future__ import annotations

import concurrent.futures
import sqlite3
from pathlib import Path

import pytest

from cascade import Engine


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
    engine.prune(roots)
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
