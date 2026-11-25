"""Multi-writer collection concurrency stress tests under free-threaded execution.

Exercises CascadeList, CascadeSet, and CascadeDict with concurrent writers,
rewritten-pipeline readers, live snapshots, transactions (commit and rollback),
and in-memory log compaction.
"""

from __future__ import annotations

import threading
from typing import Any

from cascade import CascadeDict, CascadeList, CascadeSet, Engine


def _log(collection: Any) -> Any:
    return collection._core.log  # noqa: SLF001


def _run_workers(targets: list[Any]) -> None:
    errors: list[BaseException] = []
    barrier = threading.Barrier(len(targets))

    def wrap(fn: Any) -> None:
        try:
            barrier.wait(timeout=10.0)
            fn()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=wrap, args=(fn,)) for fn in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30.0)
    alive = [t for t in threads if t.is_alive()]
    assert not alive, f"{len(alive)} worker thread(s) did not finish in time"
    if errors:
        raise errors[0]


def test_list_multi_writer_concurrent_rewritten_readers_snapshots_compaction() -> None:
    engine = Engine()
    xs = CascadeList(engine, list(range(20)))

    @engine.query
    def rewritten_even_sum() -> int:
        return sum(v for v in xs if v % 2 == 0)

    @engine.query
    def rewritten_count() -> int:
        return sum(1 for _ in xs)

    @engine.query
    def direct_stats() -> tuple[int, int]:
        items = list(xs)
        return len(items), sum(v for v in items if v % 2 == 0)

    # Prime once so rewritten pipelines are registered before threads start.
    assert rewritten_even_sum() == sum(v for v in range(20) if v % 2 == 0)
    assert rewritten_count() == 20
    assert len(engine.inspect_pipelines()) >= 2

    stop = threading.Event()
    writers_remaining = 3
    writers_lock = threading.Lock()

    def writer(wid: int) -> None:
        nonlocal writers_remaining
        try:
            base = wid * 10_000
            for step in range(40):
                # Bursts of extends + appends + pops trigger compaction checks.
                xs.extend([base + step * 30 + k for k in range(30)])
                xs.append(base + 500_000 + step)
                if step % 5 == 0 and len(xs) > 10:
                    try:
                        xs.pop()
                    except IndexError:
                        pass
        finally:
            with writers_lock:
                writers_remaining -= 1
                if writers_remaining == 0:
                    stop.set()

    def reader() -> None:
        while not stop.is_set():
            _ = rewritten_even_sum()
            _ = rewritten_count()
            snap = engine.snapshot()
            try:
                r_count = rewritten_count(snapshot=snap)
                r_sum = rewritten_even_sum(snapshot=snap)
                d_count, d_sum = direct_stats(snapshot=snap)
                assert r_count == d_count
                assert r_sum == d_sum
                # Re-read the same snapshot after concurrent writes may have progressed.
                assert rewritten_even_sum(snapshot=snap) == r_sum
                assert rewritten_count(snapshot=snap) == r_count
            finally:
                snap.release()

    _run_workers([lambda i=i: writer(i) for i in range(3)] + [reader, reader, reader])

    final = list(xs)
    assert rewritten_count() == len(final)
    assert rewritten_even_sum() == sum(v for v in final if v % 2 == 0)
    assert direct_stats() == (len(final), sum(v for v in final if v % 2 == 0))

    # With all snapshots released, further churn compacts the log.
    xs.extend(range(2_500))
    assert _log(xs).base_rev > 0
    final_after = list(xs)
    assert rewritten_count() == len(final_after)
    assert rewritten_even_sum() == sum(v for v in final_after if v % 2 == 0)


def test_set_multi_writer_concurrent_rewritten_readers_snapshots_compaction() -> None:
    engine = Engine()
    s = CascadeSet(engine, range(20))

    @engine.query
    def rewritten_even_sum() -> int:
        return sum(v for v in s if v % 2 == 0)

    @engine.query
    def rewritten_count() -> int:
        return sum(1 for _ in s)

    @engine.query
    def direct_stats() -> tuple[int, int]:
        items = set(s)
        return len(items), sum(v for v in items if v % 2 == 0)

    assert rewritten_even_sum() == sum(v for v in range(20) if v % 2 == 0)
    assert rewritten_count() == 20
    assert len(engine.inspect_pipelines()) >= 2

    stop = threading.Event()
    writers_remaining = 3
    writers_lock = threading.Lock()

    def writer(wid: int) -> None:
        nonlocal writers_remaining
        try:
            base = wid * 10_000
            for step in range(40):
                batch = [base + step * 25 + k for k in range(25)]
                s.update(batch)
                s.add(base + 500_000 + step)
                if step % 4 == 0:
                    s.discard(batch[0])
        finally:
            with writers_lock:
                writers_remaining -= 1
                if writers_remaining == 0:
                    stop.set()

    def reader() -> None:
        while not stop.is_set():
            _ = rewritten_even_sum()
            _ = rewritten_count()
            snap = engine.snapshot()
            try:
                r_count = rewritten_count(snapshot=snap)
                r_sum = rewritten_even_sum(snapshot=snap)
                d_count, d_sum = direct_stats(snapshot=snap)
                assert r_count == d_count
                assert r_sum == d_sum
                assert rewritten_even_sum(snapshot=snap) == r_sum
                assert rewritten_count(snapshot=snap) == r_count
            finally:
                snap.release()

    _run_workers([lambda i=i: writer(i) for i in range(3)] + [reader, reader, reader])

    final = set(s)
    assert rewritten_count() == len(final)
    assert rewritten_even_sum() == sum(v for v in final if v % 2 == 0)
    assert direct_stats() == (len(final), sum(v for v in final if v % 2 == 0))

    s.update(range(1_000_000, 1_002_500))
    assert _log(s).base_rev > 0
    final_after = set(s)
    assert rewritten_count() == len(final_after)
    assert rewritten_even_sum() == sum(v for v in final_after if v % 2 == 0)


def test_dict_multi_writer_concurrent_rewritten_readers_snapshots_compaction() -> None:
    engine = Engine()
    d = CascadeDict(engine, {i: i * 2 for i in range(20)})

    @engine.query
    def rewritten_val_sum() -> int:
        return sum(v for v in d.values() if v % 2 == 0)

    @engine.query
    def rewritten_key_count() -> int:
        return sum(1 for _ in d)

    @engine.query
    def direct_stats() -> tuple[int, int]:
        items = dict(d)
        return len(items), sum(v for v in items.values() if v % 2 == 0)

    assert rewritten_val_sum() == sum(i * 2 for i in range(20))
    assert rewritten_key_count() == 20
    assert len(engine.inspect_pipelines()) >= 2

    stop = threading.Event()
    writers_remaining = 3
    writers_lock = threading.Lock()

    def writer(wid: int) -> None:
        nonlocal writers_remaining
        try:
            base = wid * 10_000
            for step in range(40):
                d.update({base + step * 25 + k: step + k for k in range(25)})
                d[base + 500_000 + step] = step * 2
                if step % 4 == 0:
                    d.pop(base + step * 25, None)
        finally:
            with writers_lock:
                writers_remaining -= 1
                if writers_remaining == 0:
                    stop.set()

    def reader() -> None:
        while not stop.is_set():
            _ = rewritten_val_sum()
            _ = rewritten_key_count()
            snap = engine.snapshot()
            try:
                r_count = rewritten_key_count(snapshot=snap)
                r_sum = rewritten_val_sum(snapshot=snap)
                d_count, d_sum = direct_stats(snapshot=snap)
                assert r_count == d_count
                assert r_sum == d_sum
                assert rewritten_val_sum(snapshot=snap) == r_sum
                assert rewritten_key_count(snapshot=snap) == r_count
            finally:
                snap.release()

    _run_workers([lambda i=i: writer(i) for i in range(3)] + [reader, reader, reader])

    final = dict(d)
    assert rewritten_key_count() == len(final)
    assert rewritten_val_sum() == sum(v for v in final.values() if v % 2 == 0)
    assert direct_stats() == (len(final), sum(v for v in final.values() if v % 2 == 0))

    d.update({1_000_000 + k: k for k in range(2_500)})
    assert _log(d).base_rev > 0
    final_after = dict(d)
    assert rewritten_key_count() == len(final_after)
    assert rewritten_val_sum() == sum(v for v in final_after.values() if v % 2 == 0)


def test_multi_collection_atomic_transactions_under_concurrent_readers() -> None:
    """Transactions updating list, set, and dict together are never observed half-applied."""
    engine = Engine()
    xs = CascadeList(engine, [])
    s = CascadeSet(engine, [])
    d = CascadeDict(engine, {})

    @engine.query
    def sizes() -> tuple[int, int, int]:
        return (
            sum(1 for _ in xs),
            sum(1 for _ in s),
            sum(1 for _ in d),
        )

    assert sizes() == (0, 0, 0)
    assert len(engine.inspect_pipelines()) >= 3

    stop = threading.Event()
    tx_lock = threading.Lock()
    writers_remaining = 3
    writers_lock = threading.Lock()

    def writer(wid: int) -> None:
        nonlocal writers_remaining
        try:
            base = wid * 1_000
            for i in range(60):
                token = base + i
                with tx_lock:
                    if i % 5 == 0:
                        try:
                            with engine.transaction():
                                xs.append(token)
                                s.add(token)
                                raise RuntimeError("abort")
                        except RuntimeError:
                            pass
                    else:
                        with engine.transaction():
                            xs.append(token)
                            s.add(token)
                            d[token] = token
        finally:
            with writers_lock:
                writers_remaining -= 1
                if writers_remaining == 0:
                    stop.set()

    def reader() -> None:
        while not stop.is_set():
            snap = engine.snapshot()
            try:
                lx, ls, ld = sizes(snapshot=snap)
                assert lx == ls == ld
            finally:
                snap.release()

    _run_workers([lambda i=i: writer(i) for i in range(3)] + [reader, reader, reader])

    lx, ls, ld = sizes()
    expected = 3 * 48  # 60 iterations * 4/5 committed per writer
    assert (lx, ls, ld) == (expected, expected, expected)
