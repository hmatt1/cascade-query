from __future__ import annotations

import concurrent.futures
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
        # Sleep releases the GIL and makes overlap directly observable.
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

