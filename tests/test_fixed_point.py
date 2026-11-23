"""Fixed points: ``fixed_point=`` queries that reach themselves (G7, D2)."""

from __future__ import annotations

import asyncio
import itertools
import threading
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from cascade import Engine
from cascade.engine import CycleError

# How long a call that must not block is given before it counts as hung.
HANG_TIMEOUT = 20.0


def _store(engine: Engine) -> Any:
    return engine._internals._store  # noqa: SLF001


def _run_with_timeout(fn: Any, timeout: float = HANG_TIMEOUT) -> Any:
    """Run ``fn`` on a daemon thread so a hang fails the test instead of it."""
    outcome: list[Any] = []

    def target() -> None:
        try:
            outcome.append(("ok", fn()))
        except BaseException as exc:  # noqa: BLE001
            outcome.append(("raised", exc))

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    assert not thread.is_alive(), "call did not return"
    kind, value = outcome[0]
    if kind == "raised":
        raise value
    return value


def test_fixed_point_basic():
    engine = Engine()

    @engine.input
    def input_val():
        return 1

    @engine.query(fixed_point=0)
    def cell_a():
        return cell_b() + input_val()

    @engine.query
    def cell_b():
        return cell_a() // 2

    assert cell_a() == 1
    assert cell_b() == 0

    input_val.set(value=4)
    assert cell_a() == 7
    assert cell_b() == 3


def test_fixed_point_no_default_raises():
    engine = Engine()

    @engine.query
    def cell_a():
        return cell_b()

    @engine.query
    def cell_b():
        return cell_a()

    with pytest.raises(CycleError):
        cell_a()


def _pair(engine: Engine, runs: Counter[str] | None = None) -> tuple[Any, Any, Any]:
    """``a = b + inp`` and ``b = a // 2``: the fixed point is ``a = 2 * inp - 1``."""
    counts = Counter() if runs is None else runs

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    def a() -> int:
        counts["a"] += 1
        return b() + inp()

    @engine.query
    def b() -> int:
        counts["b"] += 1
        return a() // 2

    return inp, a, b


# --- snapshots and the revision ----------------------------------------------


def test_fixed_point_read_through_a_snapshot_uses_the_snapshot_inputs() -> None:
    engine = Engine()
    inp, a, b = _pair(engine)

    inp.set(1)
    assert a() == 1
    first = engine.snapshot()
    inp.set(4)
    second = engine.snapshot()
    inp.set(6)

    # Not yet computed at the newest revision: the snapshot reads come first.
    assert a(snapshot=second) == 7
    assert a(snapshot=first) == 1
    assert a() == 11
    assert a(snapshot=first) == 1
    assert a(snapshot=second) == 7
    # The other member of the cycle, and both in one read.
    assert b(snapshot=second) == 3
    assert b(snapshot=first) == 0
    assert b() == 5
    assert (a(snapshot=second), b(snapshot=second)) == (7, 3)


def test_fixed_point_member_first_read_through_a_snapshot() -> None:
    engine = Engine()
    inp, a, b = _pair(engine)

    inp.set(1)
    assert a() == 1
    snapshot = engine.snapshot()
    inp.set(4)
    assert b() == 3
    assert b(snapshot=snapshot) == 0
    assert a(snapshot=snapshot) == 1
    assert a() == 7


def test_evaluating_a_fixed_point_leaves_the_revision_alone() -> None:
    engine = Engine()
    inp, a, b = _pair(engine)

    @engine.query
    def plain() -> int:
        return inp() * 100

    inp.set(1)
    assert plain() == 100
    snapshot = engine.snapshot()
    inp.set(40)  # 40 iterations to converge

    store = _store(engine)
    revision = engine.revision
    cancel_epoch = store.cancel_epoch
    history_floor = store.history_floor
    stored_versions = {key: list(versions) for key, versions in store.inputs.items()}

    assert a() == 79
    assert b() == 39

    assert engine.revision == revision
    assert store.cancel_epoch == cancel_epoch
    assert store.history_floor == history_floor
    assert {k: list(v) for k, v in store.inputs.items()} == stored_versions
    # Nothing but the outside snapshot is still pinned.
    assert store._pins == {snapshot.revision: 1}  # noqa: SLF001
    assert plain(snapshot=snapshot) == 100
    assert a(snapshot=snapshot) == 1
    assert engine.revision == revision


def test_fixed_point_does_not_cancel_or_disturb_submitted_work() -> None:
    engine = Engine()
    inp, a, b = _pair(engine)
    started = threading.Event()
    release = threading.Event()

    @engine.input
    def other() -> int:
        return 5

    @engine.query
    def slow() -> int:
        value = other()
        started.set()
        release.wait(HANG_TIMEOUT)
        return value + other()

    inp.set(30)
    other.set(5)
    try:
        future = engine.submit(slow)
        assert started.wait(HANG_TIMEOUT)
        # A whole fixed-point evaluation while the submitted query is running.
        assert _run_with_timeout(a) == 59
        release.set()
        assert future.result(timeout=HANG_TIMEOUT) == 10
    finally:
        release.set()
        engine.shutdown()


def test_self_recursive_fixed_point_through_a_snapshot() -> None:
    engine = Engine()

    @engine.input
    def limit() -> int:
        return 3

    @engine.query(fixed_point=0)
    def grow() -> int:
        return min(grow() + 1, limit())

    limit.set(3)
    snapshot = engine.snapshot()
    limit.set(8)
    assert grow() == 8
    assert grow(snapshot=snapshot) == 3
    assert grow() == 8


# --- what the fixed point depends on -----------------------------------------


def test_fixed_point_follows_an_input_read_only_by_another_member() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def near() -> int:
        return 1

    @engine.input
    def far() -> int:
        return 0

    @engine.query(fixed_point=0)
    def head() -> int:
        runs["head"] += 1
        return min(member() + near(), 50)

    @engine.query
    def member() -> int:
        return head() // 2 + far()

    @engine.query
    def top() -> tuple[int, int]:
        return head(), member()

    near.set(1)
    far.set(0)
    assert top() == (1, 0)
    # Only the member reads ``far``; the head must still notice.
    far.set(10)
    assert top() == (21, 20)
    assert head() == 21
    # An unrelated revision change re-runs nothing.
    runs.clear()
    near.set(1)

    @engine.input
    def unrelated() -> int:
        return 0

    unrelated.set(1)
    assert top() == (21, 20)
    assert runs == {}


def test_fixed_point_with_a_stale_result_read_before_the_cycle_is_entered() -> None:
    engine = Engine()

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    def head() -> int:
        # ``side`` is read first. On a later run it is stale and depends on a
        # member of the cycle that is running: it has to be run again inside
        # the cycle, not verified against it.
        return min(side() + through() + inp(), 100)

    @engine.query
    def through() -> int:
        return member()

    @engine.query
    def member() -> int:
        return head() // 4

    @engine.query
    def side() -> int:
        return member()

    inp.set(1)
    assert _run_with_timeout(head) == 1
    inp.set(4)
    assert _run_with_timeout(head) == 6
    assert (side(), through(), member()) == (1, 1, 1)
    inp.set(9)
    assert _run_with_timeout(side) == 3
    assert head() == 15


def _nested(engine: Engine) -> tuple[Any, Any, Any]:
    @engine.input
    def x() -> int:
        return 0

    @engine.input
    def y() -> int:
        return 0

    @engine.query(fixed_point=0)
    def outer() -> int:
        return min(inner() + x(), 40)

    @engine.query(fixed_point=0)
    def inner() -> int:
        return min(shared() + y(), 40)

    @engine.query
    def shared() -> int:
        return (outer() + inner()) // 3

    @engine.query
    def top() -> tuple[int, int, int]:
        return outer(), inner(), shared()

    return x, y, top


def test_nested_fixed_points_match_a_fresh_engine_after_every_change() -> None:
    engine = Engine()
    x, y, top = _nested(engine)
    x.set(0)
    y.set(0)
    for x_value, y_value in itertools.product(range(7), range(4)):
        x.set(x_value)
        y.set(y_value)
        revision = engine.revision
        got = _run_with_timeout(top)
        assert engine.revision == revision

        fresh = Engine()
        fresh_x, fresh_y, fresh_top = _nested(fresh)
        fresh_x.set(x_value)
        fresh_y.set(y_value)
        assert got == fresh_top(), (x_value, y_value)

        outer_value, inner_value, shared_value = got
        assert outer_value == min(inner_value + x_value, 40)
        assert inner_value == min(shared_value + y_value, 40)
        assert shared_value == (outer_value + inner_value) // 3


def test_intermediate_results_never_reach_the_store() -> None:
    engine = Engine()
    seen_in_store: list[bool] = []
    member_values: list[int] = []

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    def head() -> int:
        value = member()
        member_values.append(value)
        seen_in_store.append(("query", member.id, ()) in _store(engine).memos)
        return value + inp()

    @engine.query
    def member() -> int:
        return head() // 2

    @engine.query
    def reader() -> int:
        return member() * 10

    inp.set(1)
    assert head() == 1
    inp.set(6)
    seen_in_store.clear()
    member_values.clear()
    assert head() == 11
    # The member took several values on the way; none of them was stored
    # while the cycle was still being iterated.
    assert len(set(member_values)) > 2
    assert seen_in_store == [False] * len(seen_in_store)
    assert _store(engine).memos[("query", member.id, ())].value == 5
    assert reader() == 50


def test_dependents_outside_the_cycle_follow_it() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()
    inp, a, b = _pair(engine)

    @engine.query
    def uses_head() -> int:
        runs["uses_head"] += 1
        return a() * 10

    @engine.query
    def uses_member() -> int:
        runs["uses_member"] += 1
        return b() * 10

    inp.set(1)
    assert (uses_head(), uses_member()) == (10, 0)
    inp.set(4)
    assert (uses_member(), uses_head()) == (30, 70)
    assert runs == {"uses_head": 2, "uses_member": 2}


def test_fixed_point_with_an_unmemoized_member() -> None:
    engine = Engine()

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    def head() -> int:
        return member() + inp()

    @engine.query(memoize=False)
    def member() -> int:
        return head() // 2

    inp.set(4)
    assert head() == 7
    assert member() == 3
    inp.set(6)
    assert member() == 5
    assert head() == 11


def test_cold_entry_through_a_member_without_fixed_point_raises() -> None:
    # A limit, by design: the query that is reached again must be the one
    # with ``fixed_point=``. Entering a cycle that was never computed through
    # another member reaches that member again first.
    engine = Engine()
    inp, a, b = _pair(engine)
    inp.set(4)
    with pytest.raises(CycleError, match="cycle detected"):
        b()
    assert a() == 7
    assert b() == 3


# --- errors and cycles that do not converge ----------------------------------


def test_fixed_point_that_does_not_converge_raises_instead_of_hanging() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.query(fixed_point=0)
    def up() -> int:
        runs["up"] += 1
        return up() + 1

    @engine.query
    def other() -> int:
        return 5

    with pytest.raises(CycleError, match="did not converge after 1000 iterations"):
        _run_with_timeout(up)
    assert runs == {"up": 1000}
    # Nothing is cached for it and nothing is left behind.
    with pytest.raises(CycleError, match="did not converge"):
        _run_with_timeout(up)
    assert runs == {"up": 2000}
    store = _store(engine)
    assert not store.in_flight
    assert store._pins == {}  # noqa: SLF001
    assert ("query", up.id, ()) not in store.memos
    assert other() == 5


def test_fixed_point_that_oscillates_between_members_raises() -> None:
    engine = Engine()

    @engine.query(fixed_point=False, max_iterations=50)
    def flip() -> bool:
        return not flop()

    @engine.query
    def flop() -> bool:
        return flip()

    @engine.query
    def caller() -> bool:
        return flip()

    with pytest.raises(CycleError, match="did not converge after 50 iterations"):
        _run_with_timeout(flip)
    # Like every CycleError it is not cached, and it reaches callers.
    with pytest.raises(CycleError, match="did not converge after 50 iterations"):
        _run_with_timeout(caller)
    store = _store(engine)
    assert ("query", flop.id, ()) not in store.memos
    assert ("query", caller.id, ()) not in store.memos
    assert not store.in_flight


def test_max_iterations_bounds_the_number_of_runs() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def limit() -> int:
        return 3

    @engine.query(fixed_point=0, max_iterations=5)
    def grow() -> int:
        runs["grow"] += 1
        return min(grow() + 1, limit())

    limit.set(4)  # runs see 0, 1, 2, 3, 4: the fifth returns its own guess
    assert grow() == 4
    assert runs == {"grow": 5}
    runs.clear()
    limit.set(5)  # needs a sixth run
    with pytest.raises(CycleError, match="did not converge after 5 iterations"):
        grow()
    assert runs == {"grow": 5}
    limit.set(2)
    assert grow() == 2


def test_max_iterations_is_validated() -> None:
    engine = Engine()
    with pytest.raises(TypeError, match="fixed_point"):
        engine.query(max_iterations=5)
    with pytest.raises(ValueError, match="at least 1"):
        engine.query(fixed_point=0, max_iterations=0)
    with pytest.raises(TypeError, match="int"):
        engine.query(fixed_point=0, max_iterations=2.5)  # type: ignore[arg-type]


def test_fixed_point_head_that_raises_a_cached_error() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    def head() -> int:
        runs["head"] += 1
        value = member() + inp()
        if value > 5:
            raise ValueError("too big")
        return value

    @engine.query
    def member() -> int:
        return head() // 2

    inp.set(4)
    with pytest.raises(ValueError, match="too big"):
        head()
    count = runs["head"]
    with pytest.raises(ValueError, match="too big"):
        head()
    assert runs["head"] == count  # cached
    # The member was computed from a guess that never became the result.
    assert ("query", member.id, ()) not in _store(engine).memos
    inp.set(1)
    assert head() == 1
    assert member() == 0


def test_fixed_point_recovers_after_an_uncached_error() -> None:
    engine = Engine()
    fail = [True]

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0, cache_exceptions=False)
    def head() -> int:
        value = member() + inp()
        if fail[0] and value > 5:
            raise RuntimeError("flaky")
        return value

    @engine.query
    def member() -> int:
        return head() // 2

    inp.set(4)
    with pytest.raises(RuntimeError, match="flaky"):
        head()
    store = _store(engine)
    assert not store.in_flight
    assert ("query", member.id, ()) not in store.memos
    fail[0] = False
    assert head() == 7
    assert member() == 3


def test_caller_that_catches_a_failed_fixed_point_is_cached_normally() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0, cache_exceptions=False)
    def head() -> int:
        value = member() + inp()
        if value > 5:
            raise RuntimeError("too big")
        return value

    @engine.query
    def member() -> int:
        return head() // 2

    @engine.query
    def safe() -> int:
        runs["safe"] += 1
        try:
            return head()
        except RuntimeError:
            return -1

    inp.set(4)
    assert safe() == -1
    assert safe() == -1
    # The failed cycle is over: its caller holds an ordinary result.
    assert runs == {"safe": 1}
    assert ("query", safe.id, ()) in _store(engine).memos
    inp.set(1)
    assert safe() == 1
    assert runs == {"safe": 2}


# --- threads -----------------------------------------------------------------


def test_threads_requesting_one_fixed_point_get_one_result() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()
    inp, a, b = _pair(engine, runs)
    inp.set(20)

    single = Engine()
    single_runs: Counter[str] = Counter()
    single_inp, single_a, _ = _pair(single, single_runs)
    single_inp.set(20)
    assert single_a() == 39

    threads = 8
    barrier = threading.Barrier(threads)
    results: list[Any] = []

    def worker() -> None:
        try:
            barrier.wait(HANG_TIMEOUT)
            results.append(a())
        except BaseException as exc:  # noqa: BLE001
            results.append(exc)

    workers = [threading.Thread(target=worker, daemon=True) for _ in range(threads)]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join(HANG_TIMEOUT)
    assert not any(thread.is_alive() for thread in workers), "a thread hung"
    assert results == [39] * threads
    # One thread iterated; the others waited for it.
    assert runs == single_runs
    assert b() == 19
    assert not _store(engine).in_flight


def test_threads_reading_a_fixed_point_at_different_revisions() -> None:
    engine = Engine()
    inp, a, b = _pair(engine)
    inp.set(3)
    # (snapshot, expected a) pairs, one per value the input has held.
    expected = []
    for value, next_value in ((3, 5), (5, 8), (8, 13), (13, 21)):
        expected.append((engine.snapshot(), 2 * value - 1))
        inp.set(next_value)

    errors: list[Any] = []
    barrier = threading.Barrier(len(expected) + 1)

    def reader(snapshot: Any, want: int) -> None:
        try:
            barrier.wait(HANG_TIMEOUT)
            for _ in range(20):
                got = a(snapshot=snapshot)
                if got != want:
                    errors.append((snapshot.revision, got, want))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def head_reader() -> None:
        try:
            barrier.wait(HANG_TIMEOUT)
            for _ in range(20):
                got = a()
                if got != 41:
                    errors.append(("head", got, 41))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    workers = [
        threading.Thread(target=reader, args=pair, daemon=True) for pair in expected
    ]
    workers.append(threading.Thread(target=head_reader, daemon=True))
    revision = engine.revision
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join(HANG_TIMEOUT)
    assert not any(thread.is_alive() for thread in workers), "a thread hung"
    assert errors == []
    assert engine.revision == revision


# --- async and disk ----------------------------------------------------------


def test_async_fixed_point_honours_snapshots_and_the_revision() -> None:
    engine = Engine()

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    async def a() -> int:
        return await b() + inp()

    @engine.query
    async def b() -> int:
        return await a() // 2

    async def main() -> None:
        inp.set(1)
        assert await a() == 1
        snapshot = engine.snapshot()
        inp.set(4)
        revision = engine.revision
        assert await a() == 7
        assert await a(snapshot=snapshot) == 1
        assert await b() == 3
        assert await b(snapshot=snapshot) == 0
        assert engine.revision == revision

    asyncio.run(asyncio.wait_for(main(), HANG_TIMEOUT))


def test_async_fixed_point_that_does_not_converge_raises() -> None:
    engine = Engine()

    @engine.query(fixed_point=0, max_iterations=25)
    async def up() -> int:
        return await up() + 1

    async def main() -> None:
        with pytest.raises(CycleError, match="did not converge after 25 iterations"):
            await up()

    asyncio.run(asyncio.wait_for(main(), HANG_TIMEOUT))


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_fixed_point_with_a_disk_cache(tmp_path: Path, backend: str) -> None:
    runs: Counter[str] = Counter()

    def open_engine() -> tuple[Engine, Any, Any, Any, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.input
        def near() -> int:
            return 1

        @engine.input
        def far() -> int:
            return 0

        @engine.query(fixed_point=0)
        def head() -> int:
            runs["head"] += 1
            return min(member() + near(), 50)

        @engine.query
        def member() -> int:
            runs["member"] += 1
            return head() // 2 + far()

        return engine, near, far, head, member

    engine, near, far, head, member = open_engine()
    try:
        near.set(1)
        far.set(10)
        assert head() == 21
        assert member() == 20
    finally:
        engine.shutdown()

    # Same inputs: the stored fixed point is reused without running the head.
    runs.clear()
    engine, near, far, head, member = open_engine()
    try:
        near.set(1)
        far.set(10)
        assert head() == 21
        assert runs["head"] == 0
        assert member() == 20
        # Changed within the session, through the member only.
        far.set(3)
        assert head() == 7
        assert member() == 6
    finally:
        engine.shutdown()

    # An input only the member reads differs from the stored run: the stored
    # fixed point must not be loaded.
    engine, near, far, head, member = open_engine()
    try:
        near.set(1)
        far.set(0)
        assert head() == 1
        assert member() == 0
    finally:
        engine.shutdown()
