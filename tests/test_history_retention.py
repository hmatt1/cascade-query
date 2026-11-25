"""Input history retention: no-op sets, revision pinning, trimming, log compaction."""

from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import pickle
import threading
from typing import Any

import pytest

from cascade import CascadeList, CascadeSet, Engine, Snapshot, SnapshotExpiredError


def _store(engine: Engine) -> Any:
    return engine._internals._store  # noqa: SLF001


def _versions(engine: Engine, handle: Any, *args: Any) -> list[Any]:
    return list(_store(engine).inputs.get((handle.id, args), []))


def _counter_pipeline() -> tuple[Engine, Any, Any, dict[str, int]]:
    engine = Engine()
    runs = {"double": 0}

    @engine.input
    def source(name: str) -> int:
        return 0

    @engine.query
    def double(name: str) -> int:
        runs["double"] += 1
        return source(name) * 2

    return engine, source, double, runs


def test_same_value_set_is_a_full_noop() -> None:
    engine, source, double, runs = _counter_pipeline()
    source.set("a", 5)
    assert double("a") == 10
    store = _store(engine)
    revision, epoch = engine.revision, store.cancel_epoch
    traces = len(engine.traces())

    assert source.set("a", 5) == revision
    assert engine.revision == revision
    assert store.cancel_epoch == epoch
    assert len(engine.traces()) == traces
    assert len(_versions(engine, source, "a")) == 1
    assert store.memos[("query", double.id, ("a",))].head_clean
    assert double("a") == 10
    assert runs["double"] == 1


def test_transaction_with_only_unchanged_values_does_not_bump() -> None:
    engine, source, _, _ = _counter_pipeline()
    source.set("a", 1)
    source.set("b", 2)
    revision = engine.revision
    with engine.transaction():
        source.set("a", 1)
        source.set("b", 2)
    assert engine.revision == revision


def test_transaction_bumps_once_and_skips_unchanged_inputs() -> None:
    engine, source, _, _ = _counter_pipeline()
    source.set("a", 1)
    source.set("b", 2)
    revision = engine.revision
    with engine.transaction():
        source.set("a", 1)
        source.set("b", 3)
    assert engine.revision == revision + 1
    assert [v.value for v in _versions(engine, source, "a")][-1] == 1
    assert _versions(engine, source, "a")[-1].revision < engine.revision
    assert _versions(engine, source, "b")[-1].value == 3


def test_transaction_that_returns_to_the_original_value_is_a_noop() -> None:
    engine, source, double, runs = _counter_pipeline()
    source.set("a", 1)
    assert double("a") == 2
    revision = engine.revision
    with engine.transaction():
        source.set("a", 7)
        source.set("a", 1)
    assert engine.revision == revision
    assert double("a") == 2
    assert runs["double"] == 1


def test_collection_mutations_in_one_transaction_store_one_version() -> None:
    engine = Engine()
    xs = CascadeList(engine, [])
    handle = xs._core.handle  # noqa: SLF001
    before = len(_versions(engine, handle))
    with engine.transaction():
        for i in range(10):
            xs.append(i)
    assert len(_versions(engine, handle)) <= before + 1
    assert _versions(engine, handle)[-1].value == xs._core.log.head  # noqa: SLF001


# --- revision pinning and trimming ---


def _pins(engine: Engine) -> dict[int, int]:
    store = _store(engine)
    with store.lock:
        store._drain_unpins_locked()  # noqa: SLF001
        return dict(store._pins)  # noqa: SLF001


def test_without_snapshots_each_input_keeps_one_version() -> None:
    engine, source, double, _ = _counter_pipeline()
    for value in range(50):
        source.set("a", value)
        assert double("a") == value * 2
    assert len(_versions(engine, source, "a")) == 1
    assert _pins(engine) == {}


def test_held_snapshot_keeps_history_until_released() -> None:
    engine, source, double, _ = _counter_pipeline()
    source.set("a", 1)
    snap = engine.snapshot()
    for value in range(2, 6):
        source.set("a", value)
    assert double("a", snapshot=snap) == 2
    assert double("a") == 10
    assert source("a", snapshot=snap) == 1

    snap.release()
    snap.release()  # idempotent
    source.set("a", 6)
    assert len(_versions(engine, source, "a")) == 1
    with pytest.raises(SnapshotExpiredError):
        double("a", snapshot=snap)
    # Not cached as the query's result: the live value still computes.
    assert double("a") == 12
    assert _pins(engine) == {}


def test_with_block_releases_the_snapshot() -> None:
    engine, source, double, _ = _counter_pipeline()
    source.set("a", 1)
    with engine.snapshot() as snap:
        source.set("a", 2)
        assert double("a", snapshot=snap) == 2
    assert _pins(engine) == {}
    source.set("a", 3)
    assert len(_versions(engine, source, "a")) == 1


def test_garbage_collected_snapshot_releases_its_pin() -> None:
    engine, source, _, _ = _counter_pipeline()
    source.set("a", 1)
    snap = engine.snapshot()
    source.set("a", 2)
    assert len(_versions(engine, source, "a")) == 2
    del snap
    gc.collect()
    source.set("a", 3)
    assert len(_versions(engine, source, "a")) == 1
    assert _pins(engine) == {}


def test_hand_built_snapshot_below_the_floor_raises() -> None:
    engine, source, double, _ = _counter_pipeline()
    source.set("a", 1)
    old = engine.revision
    source.set("a", 2)
    with pytest.raises(SnapshotExpiredError):
        double("a", snapshot=Snapshot(revision=old))
    assert double("a") == 4


def test_snapshot_value_semantics() -> None:
    engine, _, _, _ = _counter_pipeline()
    with engine.snapshot() as snap:
        same = Snapshot(revision=snap.revision)
        assert snap == same and hash(snap) == hash(same)
        assert snap != Snapshot(revision=snap.revision + 1)
        assert bool(snap)
        assert repr(snap) == f"Snapshot(revision={snap.revision})"
        copy = pickle.loads(pickle.dumps(snap))
        assert copy == snap
        copy.release()  # an unowned copy holds no pin
    assert _pins(engine) == {}


def test_sweep_trims_every_key_after_release() -> None:
    engine, source, _, _ = _counter_pipeline()
    names = [f"k{i}" for i in range(100)]
    for name in names:
        source.set(name, 0)
    snap = engine.snapshot()
    for value in range(1, 11):
        for name in names:
            source.set(name, value)
    assert all(len(_versions(engine, source, n)) == 11 for n in names)
    snap.release()
    assert all(len(_versions(engine, source, n)) == 1 for n in names)


def test_release_while_holding_the_store_lock_is_deferred() -> None:
    engine, source, _, _ = _counter_pipeline()
    source.set("a", 1)
    snap = engine.snapshot()
    store = _store(engine)
    with store.lock:
        snap.release()
        assert store._pins  # noqa: SLF001
        assert store._deferred_unpins  # noqa: SLF001
    source.set("a", 2)
    assert not store._deferred_unpins  # noqa: SLF001
    assert store._pins == {}  # noqa: SLF001
    assert len(_versions(engine, source, "a")) == 1


def test_disk_fingerprint_falls_back_when_verified_revision_expired() -> None:
    engine, source, _, _ = _counter_pipeline()
    source.set("a", 1)
    old = engine.revision
    source.set("a", 2)
    latest = _versions(engine, source, "a")[-1]
    evaluator = engine._internals._evaluator  # noqa: SLF001
    fingerprint = evaluator._observed_dep_fingerprint(  # noqa: SLF001
        ("input", source.id, ("a",)), latest.changed_at, old - 1
    )
    assert fingerprint == latest.value_hash


# --- no pin leaks ---


def test_no_pins_leak_from_submit_and_compute_many() -> None:
    engine, source, double, _ = _counter_pipeline()
    source.set("a", 1)
    assert engine.submit(double, "a").result(timeout=5) == 2

    gate = threading.Event()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        blocker = pool.submit(gate.wait)
        queued = engine.submit(double, "a", executor=pool)
        assert queued.cancel()
        gate.set()
        blocker.result(timeout=5)
    finally:
        pool.shutdown(wait=True)

    assert engine.compute_many([(double, ("a",)), (double, ("b",))]) == [2, 0]
    stream = engine.compute_many_stream([(double, ("a",)), (double, ("b",))])
    next(stream)
    stream.close()
    assert sorted(v for _, v, _ in engine.compute_many_stream([(double, ("a",))])) == [
        2
    ]
    engine.shutdown()
    assert _pins(engine) == {}


def test_no_pins_leak_from_errors_and_fixed_point_restarts() -> None:
    engine = Engine()

    @engine.input
    def limit() -> int:
        return 3

    @engine.query(fixed_point=0)
    def grow() -> int:
        return min(grow() + 1, limit())

    @engine.query
    def boom() -> int:
        raise ValueError("boom")

    assert grow() == 3
    with pytest.raises(ValueError):
        boom()
    limit.set(5)
    assert grow() == 5
    assert _pins(engine) == {}


def test_no_pins_leak_from_async_calls_and_cancellation() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 1

    @engine.query
    async def slow() -> int:
        await asyncio.sleep(10)
        return source()

    @engine.query
    async def fast() -> int:
        return source() + 1

    async def main() -> None:
        assert await fast() == 2
        task = asyncio.ensure_future(slow())
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(main())
    engine.shutdown()
    assert _pins(engine) == {}


# --- collection log compaction ---


def _log(collection: Any) -> Any:
    return collection._core.log  # noqa: SLF001


def test_collection_log_stays_bounded_under_churn() -> None:
    engine = Engine()
    xs = CascadeSet(engine)
    for i in range(20_000):
        xs.add(i % 10)
        xs.discard(i % 10)
    assert len(_log(xs).entries) <= 2 * 1024
    assert set(xs) == set()


def test_collection_snapshot_reads_survive_compaction() -> None:
    engine = Engine()
    xs = CascadeSet(engine, range(5))

    @engine.query
    def total() -> int:
        return sum(v for v in xs)

    assert total() == 10
    with engine.snapshot() as snap:
        for i in range(5_000):
            xs.add(100 + i % 3)
            xs.discard(100 + i % 3)
        xs.add(50)
        assert total() == 60
        assert total(snapshot=snap) == 10
    for i in range(5_000):
        xs.add(200)
        xs.discard(200)
    assert total() == 60
    assert len(_log(xs).entries) <= 2 * 1024


def test_pipeline_that_keeps_up_replays_incrementally(monkeypatch: Any) -> None:
    engine = Engine()
    xs = CascadeSet(engine, range(5))
    builds = {"n": 0}
    runtime = engine._incremental_rt  # noqa: SLF001
    original = runtime._build_state  # noqa: SLF001

    def counting_build(*args: Any, **kwargs: Any) -> Any:
        builds["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(runtime, "_build_state", counting_build)

    @engine.query
    def total() -> int:
        return sum(v for v in xs)

    assert total() == 10
    for i in range(5_000):
        xs.add(100)
        xs.discard(100)
        if i % 50 == 0:
            assert total() == 10
    assert total() == 10
    assert builds["n"] == 1
    assert len(_log(xs).entries) <= 2 * 1024


def test_pipeline_that_lags_far_behind_rebuilds_correctly() -> None:
    engine = Engine()
    xs = CascadeSet(engine, range(5))

    @engine.query
    def total() -> int:
        return sum(v for v in xs)

    assert total() == 10
    for i in range(10_000):
        xs.add(100 + i % 7)
        xs.discard(100 + i % 7)
    xs.add(42)
    assert _log(xs).base_rev > 0
    assert total() == 52


def test_contents_below_the_log_base_raise() -> None:
    engine = Engine()
    xs = CascadeSet(engine)
    for i in range(3_000):
        xs.add(i % 5)
        xs.discard(i % 5)
    log = _log(xs)
    assert log.base_rev > 0
    with pytest.raises(RuntimeError, match="not available"):
        log.contents_at(log.base_rev - 1)


def test_named_collection_reloads_after_memory_and_disk_compaction(
    tmp_path: Any,
) -> None:
    cache_dir = tmp_path / "cache"
    engine = Engine(cache_dir=cache_dir, cache_backend="sqlite")
    xs = CascadeSet(engine, name="tags")
    for i in range(3_000):
        xs.add(i % 13)
        if i % 3 == 0:
            xs.discard(i % 13)
    expected = set(xs)
    assert _log(xs).base_rev > 0
    engine.flush_disk()
    engine.shutdown()

    engine2 = Engine(cache_dir=cache_dir, cache_backend="sqlite")
    ys = CascadeSet(engine2, name="tags")
    assert set(ys) == expected
    engine2.shutdown()


def test_concurrent_snapshots_and_writers_never_expire_live_reads() -> None:
    engine = Engine()

    @engine.input
    def src(i: int) -> int:
        return 0

    @engine.query
    def leaf(i: int) -> int:
        return src(i) * 2

    @engine.query
    def total() -> int:
        return sum(leaf(i) for i in range(4))

    for i in range(4):
        src.set(i, 0)
    stop = threading.Event()
    errors: list[str] = []

    def fail(message: str) -> None:
        errors.append(message)
        stop.set()

    def writer() -> None:
        value = 0
        while not stop.is_set():
            value += 1
            with engine.transaction():
                for i in range(4):
                    src.set(i, value)

    def live_reader() -> None:
        while not stop.is_set():
            try:
                if total() % 8:
                    fail("inconsistent live total")
                src(0)
            except Exception as exc:  # noqa: BLE001
                fail(f"live read failed: {exc!r}")

    def snapshot_reader() -> None:
        while not stop.is_set():
            try:
                with engine.snapshot() as snap:
                    first = total(snapshot=snap)
                    for _ in range(3):
                        if total(snapshot=snap) != first:
                            fail("snapshot read changed")
                    if first % 8 or src(0, snapshot=snap) * 8 != first:
                        fail("inconsistent snapshot total")
            except Exception as exc:  # noqa: BLE001
                fail(f"snapshot read failed: {exc!r}")

    threads = (
        [threading.Thread(target=writer) for _ in range(2)]
        + [threading.Thread(target=live_reader) for _ in range(2)]
        + [threading.Thread(target=snapshot_reader) for _ in range(3)]
    )
    for t in threads:
        t.start()
    stop.wait(timeout=1.5)
    stop.set()
    for t in threads:
        t.join(timeout=10)
    assert errors == []
    assert _pins(engine) == {}
    store = _store(engine)
    assert all(len(v) == 1 for v in store.inputs.values())


def test_overlapping_snapshots_trim_once_the_last_is_released() -> None:
    engine, source, _, _ = _counter_pipeline()
    source.set("a", 0)
    first = engine.snapshot()
    for value in range(1, 6):
        source.set("a", value)
    second = engine.snapshot()
    for value in range(6, 11):
        source.set("a", value)
    first.release()
    assert len(_versions(engine, source, "a")) == 6
    second.release()
    assert len(_versions(engine, source, "a")) == 1


# --- the Snapshot object ---


def test_snapshot_revision_cannot_be_changed() -> None:
    engine = Engine()
    with engine.snapshot() as snapshot:
        revision = snapshot.revision
        with pytest.raises(AttributeError, match="read-only"):
            snapshot.revision = revision + 1  # type: ignore[misc]
        assert snapshot.revision == revision


def test_snapshots_are_equal_when_their_revisions_are() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 0

    with engine.snapshot() as first, engine.snapshot() as same:
        x.set(1)
        with engine.snapshot() as later:
            assert first == same
            assert hash(first) == hash(same)
            assert first != later
            assert first != first.revision
            assert len({first, same, later}) == 2


# --- pins taken and released by the engine itself ---


def test_pin_taken_under_the_lock_keeps_history_until_it_is_released() -> None:
    engine, source, double, _ = _counter_pipeline()
    source.set("a", 1)
    store = _store(engine)
    with store.lock:
        pinned = store.pin_current_locked()
    assert pinned == engine.revision
    source.set("a", 2)
    source.set("a", 3)
    # Like a snapshot, the pin keeps the value of its revision readable.
    assert double("a", snapshot=Snapshot(revision=pinned)) == 2
    assert len(_versions(engine, source, "a")) == 3
    store.unpin(pinned)
    assert len(_versions(engine, source, "a")) == 1
    with pytest.raises(SnapshotExpiredError):
        double("a", snapshot=Snapshot(revision=pinned))


def test_releasing_a_snapshot_also_releases_the_collected_ones() -> None:
    engine, source, _, _ = _counter_pipeline()
    store = _store(engine)
    source.set("a", 1)
    dropped = engine.snapshot()
    source.set("a", 2)
    kept = engine.snapshot()
    source.set("a", 3)
    # A collected snapshot cannot take the lock, so its release is queued ...
    del dropped
    gc.collect()
    assert len(_versions(engine, source, "a")) == 3
    # ... and applied by the next release, with nothing else in between.
    kept.release()
    assert store._pins == {}  # noqa: SLF001
    assert len(_versions(engine, source, "a")) == 1


def test_call_that_ends_applies_the_releases_queued_while_it_ran() -> None:
    engine, source, _, _ = _counter_pipeline()
    store = _store(engine)
    source.set("a", 1)
    held = [engine.snapshot()]
    source.set("a", 2)

    @engine.query
    def drops_the_snapshot() -> int:
        held.clear()  # collected while the call holds a pin of its own
        gc.collect()
        return source("a")

    assert len(_versions(engine, source, "a")) == 2
    assert drops_the_snapshot() == 2
    assert store._pins == {}  # noqa: SLF001
    assert len(_versions(engine, source, "a")) == 1


def test_async_call_closed_inside_a_critical_section_is_unpinned_later() -> None:
    engine = Engine()
    store = _store(engine)

    @engine.input
    def source() -> int:
        return 1

    @engine.query
    async def waits() -> int:
        await asyncio.sleep(0)
        return source()

    async def main() -> None:
        call = waits()
        call.send(None)  # runs up to the await in the body
        assert store._pins == {engine.revision: 1}  # noqa: SLF001
        with store.lock:
            # What the garbage collector does to an abandoned coroutine, here
            # while this thread is inside one of the store's critical
            # sections: the pins must not change under that section.
            call.close()
            assert store._pins == {engine.revision: 1}  # noqa: SLF001
            assert list(store._deferred_unpins) == [engine.revision]  # noqa: SLF001

    asyncio.run(main())
    source.set(2)
    assert store._pins == {}  # noqa: SLF001
    assert not store._deferred_unpins  # noqa: SLF001

    async def call_again() -> int:
        return await waits()

    assert asyncio.run(call_again()) == 2
    engine.shutdown()


def test_no_pin_leaks_when_the_executor_refuses_the_job() -> None:
    engine, source, double, _ = _counter_pipeline()
    source.set("a", 1)
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    pool.shutdown()
    with pytest.raises(RuntimeError, match="after shutdown"):
        engine.submit(double, "a", executor=pool)
    assert _store(engine)._pins == {}  # noqa: SLF001
    # Nothing holds the old revision, so history is trimmed as usual.
    source.set("a", 2)
    source.set("a", 3)
    assert len(_versions(engine, source, "a")) == 1
