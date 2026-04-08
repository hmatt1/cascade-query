from __future__ import annotations

import concurrent.futures
import sqlite3
import threading
import time
from pathlib import Path

import pytest

from cascade import CycleError, Engine, QueryCancelled


def test_smart_recalculation_and_selective_updates() -> None:
    engine = Engine()
    calls = {"parse": 0, "names": 0}

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        calls["parse"] += 1
        return tuple(line.strip() for line in source(name).splitlines() if line.strip())

    @engine.query
    def names(name: str) -> tuple[str, ...]:
        calls["names"] += 1
        return tuple(item.split("=")[0].strip() for item in parse(name))

    source.set("main", "a = 1\nb = 2")
    assert names("main") == ("a", "b")
    assert calls == {"parse": 1, "names": 1}

    # Demand-driven: querying parse directly does not force names to recompute.
    assert parse("main") == ("a = 1", "b = 2")
    assert calls == {"parse": 1, "names": 1}

    # Whitespace-only change backdates parse result hash, so parent remains green.
    source.set("main", "a = 1  \nb = 2")
    assert names("main") == ("a", "b")
    assert calls["parse"] == 2
    assert calls["names"] == 1


def test_cycle_detection() -> None:
    engine = Engine()

    @engine.query
    def a(x: int) -> int:
        return b(x)

    @engine.query
    def b(x: int) -> int:
        return a(x)

    with pytest.raises(CycleError):
        a(1)


def test_snapshot_isolation_mvcc() -> None:
    engine = Engine()

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        return tuple(source(name).splitlines())

    source.set("main", "a\nb")
    snap = engine.snapshot()
    source.set("main", "x\ny")

    assert parse("main", snapshot=snap) == ("a", "b")
    assert parse("main") == ("x", "y")


def test_query_dedup_concurrent_requests() -> None:
    engine = Engine()
    compute_counter = 0
    lock = threading.Lock()
    started = threading.Event()

    @engine.input
    def base() -> int:
        return 0

    @engine.query
    def expensive() -> int:
        nonlocal compute_counter
        with lock:
            compute_counter += 1
        started.set()
        time.sleep(0.12)
        return base() + 1

    base.set(41)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futs = [pool.submit(expensive) for _ in range(6)]
        started.wait(timeout=1.0)
        values = [f.result() for f in futs]
    assert values == [42] * 6
    assert compute_counter == 1


def test_query_dedup_failure_propagates_and_recovers() -> None:
    engine = Engine()
    runs = 0
    lock = threading.Lock()
    started = threading.Event()

    @engine.input
    def mode() -> int:
        return 0

    @engine.query
    def flaky() -> int:
        nonlocal runs
        with lock:
            runs += 1
        started.set()
        time.sleep(0.06)
        if mode() == 1:
            raise RuntimeError("planned failure")
        return mode() + 40

    mode.set(1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(flaky) for _ in range(8)]
        started.wait(timeout=1.0)
        errors = []
        for future in futures:
            with pytest.raises(RuntimeError, match="planned failure") as exc:
                future.result(timeout=2.0)
            errors.append(exc.value)

    assert len(errors) == 8
    assert runs == 1
    assert ("query", flaky.id, ()) not in engine._in_flight  # noqa: SLF001

    mode.set(2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        values = [future.result(timeout=2.0) for future in [pool.submit(flaky) for _ in range(6)]]

    assert values == [42] * 6
    assert runs == 2


def test_safe_task_cancellation_after_input_mutation() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def slow() -> int:
        total = 0
        for _ in range(12):
            _ = source()
            total += 1
            time.sleep(0.015)
        return total

    source.set(1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        future = engine.submit(slow, executor=pool)
        time.sleep(0.04)
        source.set(2)  # cancels older epoch
        with pytest.raises(QueryCancelled):
            future.result(timeout=2.0)


def test_side_effect_replay_on_cache_hits() -> None:
    engine = Engine()
    warns = engine.accumulator("warnings")

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def lint() -> int:
        text = source()
        if "todo" in text:
            warns.push("contains todo")
        return len(text)

    source.set("todo item")
    effects_1: dict[str, list[str]] = {}
    assert lint(effects=effects_1) == len("todo item")
    assert effects_1["warnings"] == ["contains todo"]

    effects_2: dict[str, list[str]] = {}
    assert lint(effects=effects_2) == len("todo item")
    assert effects_2["warnings"] == ["contains todo"]


def test_nested_side_effect_replay_propagates_to_parent_effects() -> None:
    engine = Engine()
    warns = engine.accumulator("warnings")
    events = {"child": 0}

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def child() -> int:
        events["child"] += 1
        text = source()
        if "todo" in text:
            warns.push("child warned")
        return len(text)

    @engine.query
    def parent() -> int:
        return child() + 1

    source.set("todo")
    effects_1: dict[str, list[str]] = {}
    assert parent(effects=effects_1) == len("todo") + 1
    assert effects_1["warnings"] == ["child warned"]
    assert events["child"] == 1

    # Child is cached; parent recomputes due to an independent input dependency
    # while still receiving replayed child effects transitively.
    @engine.input
    def toggle() -> int:
        return 0

    @engine.query
    def parent_with_toggle() -> int:
        _ = toggle()
        return child() + 2

    toggle.set(0)
    effects_2: dict[str, list[str]] = {}
    assert parent_with_toggle(effects=effects_2) == len("todo") + 2
    assert effects_2["warnings"] == ["child warned"]
    assert events["child"] == 1

    toggle.set(1)
    effects_3: dict[str, list[str]] = {}
    assert parent_with_toggle(effects=effects_3) == len("todo") + 2
    assert effects_3["warnings"] == ["child warned"]
    # Still a single concrete child execution; subsequent effects came from replay.
    assert events["child"] == 1


def test_dynamic_graph_expansion() -> None:
    engine = Engine()
    expand_calls = 0

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def expand() -> tuple[str, ...]:
        nonlocal expand_calls
        expand_calls += 1
        lines = []
        for row in source().splitlines():
            if row.startswith("macro "):
                lines.append(row.replace("macro ", "fn ").strip())
            else:
                lines.append(row.strip())
        return tuple(lines)

    @engine.query
    def lowered() -> tuple[str, ...]:
        return tuple(line.lower() for line in expand())

    source.set("macro Add\nmacro Sub")
    assert lowered() == ("fn add", "fn sub")
    source.set("macro Add\nmacro Mul")
    assert lowered() == ("fn add", "fn mul")
    assert expand_calls >= 2


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


def test_work_stealing_compute_many() -> None:
    engine = Engine()
    thread_ids: set[int] = set()
    lock = threading.Lock()

    @engine.input
    def base(i: int) -> int:
        return 0

    @engine.query
    def job(i: int) -> int:
        with lock:
            thread_ids.add(threading.get_ident())
        time.sleep(0.01)
        return base(i) + 10

    for i in range(20):
        base.set(i, i)

    calls = [(job, (i,)) for i in range(20)]
    result = engine.compute_many(calls, workers=4)
    assert result == [i + 10 for i in range(20)]
    assert len(thread_ids) > 1


def test_multi_cpu_parallel_overlap_proof() -> None:
    engine = Engine()
    active = 0
    max_active = 0
    lock = threading.Lock()
    thread_ids: set[int] = set()

    @engine.input
    def base(i: int) -> int:
        return 0

    @engine.query
    def job(i: int) -> int:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
            thread_ids.add(threading.get_ident())
        # Sleep-based overlap should remain observable on free-threaded CPython.
        time.sleep(0.02)
        with lock:
            active -= 1
        return base(i) + 1

    for i in range(32):
        base.set(i, i)

    result = engine.compute_many([(job, (i,)) for i in range(32)], workers=8)
    assert result == [i + 1 for i in range(32)]
    assert len(thread_ids) >= 2
    assert max_active >= 2


def test_query_dedup_heavy_contention_stress() -> None:
    engine = Engine()
    runs = 0
    lock = threading.Lock()
    started = threading.Event()
    release = threading.Event()

    @engine.input
    def base() -> int:
        return 0

    @engine.query
    def expensive() -> int:
        nonlocal runs
        with lock:
            runs += 1
        started.set()
        release.wait(timeout=2.0)
        time.sleep(0.01)
        return base() + 1

    base.set(41)
    with concurrent.futures.ThreadPoolExecutor(max_workers=24) as pool:
        futures = [pool.submit(expensive) for _ in range(80)]
        started.wait(timeout=1.0)
        time.sleep(0.03)
        release.set()
        values = [future.result(timeout=2.0) for future in futures]

    assert values == [42] * 80
    assert runs == 1
    assert sum(1 for event in engine.traces() if event.event == "dedup_wait") >= 1


def test_cancellation_race_stress() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def slow_sum() -> int:
        total = 0
        for _ in range(60):
            total += source()
            time.sleep(0.002)
        return total

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for seed in range(5):
            source.set(seed)
            futures = [engine.submit(slow_sum, executor=pool) for _ in range(8)]
            time.sleep(0.01)
            source.set(seed + 1000)
            for future in futures:
                with pytest.raises(QueryCancelled):
                    future.result(timeout=2.0)
            assert slow_sum() == (seed + 1000) * 60


def test_snapshot_reads_stable_during_concurrent_writes() -> None:
    engine = Engine()

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def parse() -> tuple[str, ...]:
        return tuple(row.strip() for row in source().splitlines() if row.strip())

    source.set("stable")
    snap = engine.snapshot()
    stop = threading.Event()
    errors: list[tuple[str, ...]] = []

    def writer() -> None:
        for idx in range(80):
            source.set(f"row-{idx}")
            time.sleep(0.001)
        stop.set()

    def reader() -> None:
        while not stop.is_set():
            value = parse(snapshot=snap)
            if value != ("stable",):
                errors.append(value)
                stop.set()
            time.sleep(0.0005)

    t_writer = threading.Thread(target=writer, daemon=True)
    t_reader = threading.Thread(target=reader, daemon=True)
    t_writer.start()
    t_reader.start()
    t_writer.join(timeout=3.0)
    t_reader.join(timeout=3.0)

    assert errors == []
    assert parse() == ("row-79",)


def test_long_chain_cycle_detection_behavior() -> None:
    engine = Engine()
    chain_length = 25
    nodes: list = []

    def make_node(index: int):
        def node(x: int) -> int:
            if index == chain_length - 1:
                return nodes[0](x)
            return nodes[index + 1](x)

        node.__name__ = f"node_{index}"
        node.__qualname__ = f"node_{index}"
        return engine.query(node)

    for index in range(chain_length):
        nodes.append(make_node(index))

    with pytest.raises(CycleError) as exc:
        nodes[0](1)

    message = str(exc.value)
    assert "cycle detected:" in message
    assert "node_0" in message
    assert f"node_{chain_length - 1}" in message


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


def test_misc_api_edges_and_default_input_paths(tmp_path: Path) -> None:
    engine = Engine()
    warnings = engine.accumulator("warnings")

    @engine.input
    def number(x: int) -> int:
        return x * 2

    @engine.query
    def plus_one(x: int) -> int:
        return number(x) + 1

    # Unset input falls back to its default function and initializes a version.
    assert plus_one(3) == 7
    assert engine.revision == 1
    assert number.id in repr(number)
    assert plus_one.id in repr(plus_one)
    assert repr(warnings) == "<Accumulator warnings>"

    with pytest.raises(TypeError):
        number.set()

    with pytest.raises(RuntimeError):
        warnings.push("outside query")

    # Empty dispatch is explicitly supported.
    assert engine.compute_many([]) == []

    # submit() without an external executor takes the owned-executor path.
    assert engine.submit(plus_one, 3).result(timeout=2.0) == 7

    # clear_traces() resets the event buffer.
    assert engine.traces()
    engine.clear_traces()
    assert engine.traces() == []

    # load() gracefully handles an empty persistence table.
    empty_path = tmp_path / "empty.db"
    conn = sqlite3.connect(empty_path)
    try:
        conn.execute("create table if not exists cascade_state (id integer primary key, payload blob not null)")
        conn.commit()
    finally:
        conn.close()
    engine.load(str(empty_path))

    # prune() tolerates unreachable roots and keeps memo graph consistent.
    engine.prune([("query", plus_one.id, (999,))])
    assert isinstance(engine.inspect_graph(), dict)


def test_compute_many_propagates_worker_exceptions() -> None:
    engine = Engine()

    @engine.query
    def boom(i: int) -> int:
        if i == 2:
            raise ValueError("boom")
        return i

    with pytest.raises(ValueError, match="boom"):
        engine.compute_many([(boom, (0,)), (boom, (1,)), (boom, (2,)), (boom, (3,))], workers=3)


def test_internal_dependency_version_helpers() -> None:
    engine = Engine()

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        return tuple(source(name).splitlines())

    # Internal helper boundaries are stable and testable.
    assert engine._latest_input_version((source.id, ("missing",))) is None
    assert engine._input_version_at((source.id, ("missing",)), revision=0) is None
    assert engine._input_version_at((source.id, ("main",)), revision=0) is None

    source.set("main", "a\nb")
    snap = engine.snapshot()
    assert parse("main", snapshot=snap) == ("a", "b")

    dep_key = ("query", parse.id, ("main",))
    assert engine._dependency_changed_at(dep_key, snap) == snap.revision


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

