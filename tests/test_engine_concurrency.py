from __future__ import annotations

import concurrent.futures
import threading
import time

import pytest

from cascade import Engine, QueryCancelled


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


def test_query_dedup_replays_effects_to_all_concurrent_callers() -> None:
    engine = Engine()
    warnings = engine.accumulator("warnings")
    runs = 0
    lock = threading.Lock()
    started = threading.Event()
    release = threading.Event()

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def lint_len() -> int:
        nonlocal runs
        with lock:
            runs += 1
        started.set()
        release.wait(timeout=2.0)
        text = source()
        if "todo" in text:
            warnings.push("contains todo")
        time.sleep(0.01)
        return len(text)

    source.set("todo")
    effects_by_call: list[dict[str, list[str]]] = [{} for _ in range(24)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        futures = [pool.submit(lint_len, effects=effects) for effects in effects_by_call]
        assert started.wait(timeout=1.0)
        time.sleep(0.03)
        release.set()
        values = [future.result(timeout=2.0) for future in futures]

    assert values == [len("todo")] * 24
    assert runs == 1
    assert any(event.event == "dedup_wait" for event in engine.traces())
    for effects in effects_by_call:
        assert effects["warnings"] == ["contains todo"]


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
    assert any(event.event == "dedup_wait" for event in engine.traces())

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


def test_submit_replays_effects_on_cache_hit() -> None:
    engine = Engine()
    warnings = engine.accumulator("warnings")

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def lint_len() -> int:
        text = source()
        if "warn" in text:
            warnings.push("has warn")
        return len(text)

    source.set("warn me")

    effects_1: dict[str, list[str]] = {}
    assert engine.submit(lint_len, effects=effects_1).result(timeout=2.0) == len("warn me")
    assert effects_1["warnings"] == ["has warn"]

    effects_2: dict[str, list[str]] = {}
    assert engine.submit(lint_len, effects=effects_2).result(timeout=2.0) == len("warn me")
    assert effects_2["warnings"] == ["has warn"]


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


def test_in_flight_dedup_does_not_cross_snapshot_boundaries() -> None:
    engine = Engine()
    first_started = threading.Event()
    release_first = threading.Event()
    lock = threading.Lock()
    calls = 0

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def read_source() -> int:
        nonlocal calls
        with lock:
            calls += 1
            invocation = calls
        # Keep the first (stale-snapshot) call in flight so a newer-snapshot
        # caller for the same key overlaps with it.
        if invocation == 1:
            first_started.set()
            release_first.wait(timeout=2.0)
        return source()

    source.set(1)
    stale_snapshot = engine.snapshot()
    source.set(2)

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        stale_future = pool.submit(read_source, snapshot=stale_snapshot)
        assert first_started.wait(timeout=1.0)
        live_future = pool.submit(read_source)
        time.sleep(0.03)
        release_first.set()

        stale_value = stale_future.result(timeout=2.0)
        live_value = live_future.result(timeout=2.0)

    assert stale_value == 1
    assert live_value == 2
    assert calls == 2


def test_compute_many_uses_single_snapshot_for_all_calls() -> None:
    engine = Engine()
    gate = threading.Event()
    started = threading.Event()

    @engine.input
    def cell(i: int) -> int:
        return 0

    @engine.query
    def read_cell(i: int) -> int:
        started.set()
        gate.wait(timeout=2.0)
        return cell(i)

    width = 10
    for i in range(width):
        cell.set(i, i)

    result_box: list[list[int]] = []

    def run_batch() -> None:
        result_box.append(engine.compute_many([(read_cell, (i,)) for i in range(width)], workers=4))

    worker = threading.Thread(target=run_batch, daemon=True)
    worker.start()
    started.wait(timeout=1.0)

    for i in range(width):
        cell.set(i, i + 1000)
    gate.set()
    worker.join(timeout=3.0)
    assert not worker.is_alive()
    assert result_box == [list(range(width))]
    assert engine.compute_many([(read_cell, (i,)) for i in range(width)], workers=4) == [i + 1000 for i in range(width)]


def test_submit_replays_cached_effects_with_shared_external_executor() -> None:
    engine = Engine()
    warnings = engine.accumulator("warnings")
    runs = 0

    @engine.input
    def source() -> str:
        return ""

    @engine.query
    def lint() -> int:
        nonlocal runs
        runs += 1
        text = source()
        if "todo" in text:
            warnings.push("contains todo")
        return len(text)

    source.set("todo")
    effects_1: dict[str, list[str]] = {}
    effects_2: dict[str, list[str]] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        assert engine.submit(lint, effects=effects_1, executor=pool).result(timeout=2.0) == len("todo")
        assert engine.submit(lint, effects=effects_2, executor=pool).result(timeout=2.0) == len("todo")

    assert effects_1["warnings"] == ["contains todo"]
    assert effects_2["warnings"] == ["contains todo"]
    assert runs == 1


def test_compute_many_deduplicates_identical_calls_within_batch() -> None:
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
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
        started.set()
        release.wait(timeout=2.0)
        time.sleep(0.01)
        return base() + 1

    base.set(41)
    result_box: list[list[int]] = []
    error_box: list[BaseException] = []

    def run_batch() -> None:
        try:
            result_box.append(engine.compute_many([(expensive, ()) for _ in range(40)], workers=12))
        except BaseException as exc:  # pragma: no cover - defensive
            error_box.append(exc)

    runner = threading.Thread(target=run_batch, daemon=True)
    runner.start()

    assert started.wait(timeout=1.0)
    time.sleep(0.03)
    release.set()

    runner.join(timeout=3.0)
    assert not runner.is_alive()
    assert not error_box
    assert result_box == [[42] * 40]
    assert runs == 1
    assert any(event.event == "dedup_wait" for event in engine.traces())


def test_compute_many_shared_dependency_recomputes_once_per_invalidation_wave() -> None:
    engine = Engine()
    width = 36
    lock = threading.Lock()
    runs = {"shared": 0, "parent": 0}

    @engine.input
    def base() -> int:
        return 0

    @engine.query
    def shared_root() -> int:
        with lock:
            runs["shared"] += 1
        # Keep the shared compute in flight long enough for many parent calls
        # to contend on the same memo key.
        time.sleep(0.02)
        return base() * 100

    @engine.query
    def parent(index: int) -> int:
        with lock:
            runs["parent"] += 1
        return shared_root() + index

    calls = [(parent, (index,)) for index in range(width)]

    base.set(1)
    first = engine.compute_many(calls, workers=12)
    assert first == [100 + index for index in range(width)]
    assert runs["parent"] == width
    assert runs["shared"] == 1

    base.set(2)
    second = engine.compute_many(calls, workers=12)
    assert second == [200 + index for index in range(width)]
    assert runs["parent"] == width * 2
    assert runs["shared"] == 2
    assert any(event.event == "dedup_wait" for event in engine.traces())
