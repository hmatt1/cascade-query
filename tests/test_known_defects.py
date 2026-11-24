"""Regression tests for the defects listed in docs/guarantees.md (D1-D31).

Every test here asserts the CORRECT behaviour. A test for an open defect is
marked ``xfail(strict=True)``: it fails today, so the suite stays green, and
it starts failing the suite the moment the defect is fixed. Remove the marker
(and move the test next to the feature it covers, if you like) as part of
the fix. Tests without the marker are for defects that have been fixed.

Where the right behaviour is a design choice, the test accepts every outcome
that would be correct and says so in a comment.
"""

from __future__ import annotations

import asyncio
import contextlib
import heapq
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest

import cascade
from cascade import (
    CascadeList,
    CascadeSet,
    Engine,
    PersistentCacheError,
    QueryCancelled,
)
from cascade import _disk_cache

BACKENDS = ("mdbx", "sqlite")

# How long a call that must not block is given before it counts as hung.
HANG_TIMEOUT = 3.0


def _defect(number: int, description: str) -> pytest.MarkDecorator:
    return pytest.mark.xfail(
        strict=True,
        reason=f"D{number}: {description}, see docs/guarantees.md",
    )


def _run_with_timeout(fn: Any, timeout: float = HANG_TIMEOUT) -> tuple[bool, list]:
    """Run ``fn`` on a daemon thread; return (finished, [outcome])."""
    outcome: list[Any] = []

    def target() -> None:
        try:
            outcome.append(("ok", fn()))
        except BaseException as exc:  # noqa: BLE001
            outcome.append(("raised", exc))

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    return not thread.is_alive(), outcome


# --- Wrong results ----------------------------------------------------------


def test_d1_stage_reading_an_already_read_input_is_invalidated() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])

    @engine.input
    def factor() -> int:
        return 2

    @engine.query
    def total() -> int:
        f = factor()  # noqa: F841
        return sum(x * factor() for x in xs)

    assert total() == 12
    factor.set(10)
    assert total() == 60


def test_d2_fixed_point_query_honours_snapshot() -> None:
    engine = Engine()

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    def a() -> int:
        return b() + inp()

    @engine.query
    def b() -> int:
        return a() // 2

    assert a() == 1
    snapshot = engine.snapshot()
    inp.set(4)
    assert a() == 7
    assert a(snapshot=snapshot) == 1


def test_d3_expired_ttl_dependency_refreshes_through_its_dependent() -> None:
    engine = Engine()
    runs = {"n": 0}

    @engine.query(ttl=0.05)
    def external() -> int:
        runs["n"] += 1
        return runs["n"]

    @engine.query
    def parent() -> int:
        return external() * 10

    assert parent() == 10
    time.sleep(0.2)
    assert parent() == 20


@pytest.mark.parametrize("fan_out", ["compute_many", "submit"])
def test_d4_fan_out_inside_a_query_tracks_dependencies(fan_out: str) -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def leaf() -> int:
        return x()

    @engine.query
    def parent() -> int:
        if fan_out == "compute_many":
            return engine.compute_many([(leaf, ())])[0]
        return engine.submit(leaf).result(timeout=10)

    try:
        # Design choice: the fix may instead refuse thread fan-out inside a
        # query with a clear error. Either outcome is correct; a silently
        # untracked dependency is not.
        try:
            first = parent()
        except Exception:  # noqa: BLE001
            return
        assert first == 1
        x.set(5)
        assert parent() == 5
    finally:
        engine.shutdown()


def test_d5_set_union_inside_a_query_sees_later_mutations() -> None:
    engine = Engine()
    s = CascadeSet(engine, [1])

    @engine.query
    def q() -> list[int]:
        return sorted(s | {99})

    assert q() == [1, 99]
    s.add(2)
    assert q() == [1, 2, 99]


def test_d6_dict_order_change_reaches_dependents() -> None:
    engine = Engine()

    @engine.input
    def order() -> str:
        return "ab"

    @engine.query
    def d() -> dict[str, int]:
        return {k: 1 for k in order()}

    @engine.query
    def keys() -> list[str]:
        return list(d())

    assert keys() == ["a", "b"]
    order.set("ba")
    assert keys() == ["b", "a"]


def test_d7_rewritten_float_sum_matches_builtin() -> None:
    engine = Engine()
    floats = CascadeList(engine, [0.1] * 10)

    @engine.query
    def total() -> float:
        return sum(x for x in floats)

    assert total() == sum([0.1] * 10)


def test_d7_rewritten_sorted_reverse_is_stable() -> None:
    engine = Engine()
    pairs = CascadeList(engine, [(1, "a"), (1, "b")])

    @engine.query
    def ordered() -> list[tuple[int, str]]:
        return sorted((p for p in pairs), key=lambda p: p[0], reverse=True)

    assert ordered() == sorted([(1, "a"), (1, "b")], key=lambda p: p[0], reverse=True)


def test_d7_rewritten_any_short_circuits() -> None:
    engine = Engine()
    zs = CascadeList(engine, [1, 0])

    @engine.query
    def any_reciprocal() -> bool:
        return any(1 / z for z in zs)

    # The builtin stops at the first truthy element and never divides by 0.
    assert any_reciprocal() is True


@_defect(29, "a CascadeSet iterates in another order when the body is not rewritten")
def test_d29_set_order_is_the_same_rewritten_or_not() -> None:
    engine = Engine()
    s = CascadeSet(engine, [30, 1, 20])

    @engine.query
    def rewritten() -> list[int]:
        return [x for x in s]

    @engine.query(incremental=False)
    def unrewritten() -> list[int]:
        return [x for x in s]

    # Either order would do, as long as it is one order.
    assert rewritten() == unrewritten()


@_defect(30, "an assignment expression in a rewritten comprehension is lost")
def test_d30_walrus_in_a_comprehension_binds_in_the_query() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])

    @engine.query
    def last_seen() -> Any:
        last = None
        out = [(last := x) for x in xs]
        return (last, out)

    assert last_seen() == (3, [1, 2, 3])


# The accumulator is reached through a global: a stage function that closes
# over one cannot be fingerprinted, so its pipeline is rebuilt on every run
# and the defect does not show.
_D31: dict[str, Any] = {}


def _d31_note(x: int) -> int:
    _D31["seen"].push(x)
    return x


@_defect(31, "effects pushed in a rewritten stage are not replayed for held elements")
def test_d31_effects_pushed_per_element_survive_an_incremental_update() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])
    _D31["seen"] = engine.accumulator("seen")

    @engine.query
    def total() -> int:
        return sum(_d31_note(x) for x in xs)

    first: dict[str, list[Any]] = {}
    assert total(effects=first) == 6
    assert first == {"seen": [1, 2, 3]}
    xs.append(4)
    second: dict[str, list[Any]] = {}
    assert total(effects=second) == 10
    assert second == {"seen": [1, 2, 3, 4]}


def test_d8_builtin_shadowed_in_enclosing_scope_is_not_rewritten() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2])

    def outer() -> Any:
        sum = lambda it: "shadowed"  # noqa: E731, A001

        @engine.query
        def q() -> Any:
            return sum([x for x in xs])

        return q

    assert outer()() == "shadowed"


def test_d9_factory_made_queries_do_not_share_results() -> None:
    engine = Engine()

    def make(k: int) -> Any:
        @engine.query
        def q() -> int:
            return k

        return q

    q1 = make(1)
    assert q1() == 1
    # Design choice: the fix may give each closure its own identity, or
    # refuse the second registration with a clear error. Either is correct;
    # silently serving q1's result for q2 is not.
    try:
        q2 = make(2)
    except Exception:  # noqa: BLE001
        return
    assert q2() == 2


@pytest.mark.parametrize("backend", BACKENDS)
def test_d10_two_engines_on_one_named_collection(tmp_path: Path, backend: str) -> None:
    engines: list[Engine] = []

    def open_engine() -> Engine:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)
        engines.append(engine)
        return engine

    try:
        first = open_engine()
        xs1 = CascadeList(first, [1, 2, 3], name="xs")
        # Design choice: the fix may make the second writer fail with a clear
        # error (one writer per cache_dir), or merge both writers' changes.
        # Either is correct; silently dropping one writer's append is not.
        try:
            second = open_engine()
            xs2 = CascadeList(second, name="xs")
            xs1.append("from-1")
            xs2.append("from-2")
        except PersistentCacheError:
            return
        for engine in engines:
            engine.shutdown()
        engines.clear()

        restored = list(CascadeList(open_engine(), name="xs"))
        assert "from-1" in restored
        assert "from-2" in restored
    finally:
        for engine in engines:
            engine.shutdown()


def test_d22_reregistered_query_reaches_dependents_at_the_same_revision() -> None:
    engine = Engine()

    def base_v1() -> int:
        return 1

    def base_v2() -> int:
        return 10

    base_v2.__qualname__ = base_v1.__qualname__
    base = engine.query(base_v1)

    @engine.query
    def top() -> int:
        return base() + 1

    assert top() == 2
    # Changed code, registered again; no input changes in between.
    base = engine.query(base_v2)
    assert top() == 11


@_defect(26, "effects of abandoned fixed-point runs reach the caller")
def test_d26_fixed_point_effects_are_those_of_the_final_run() -> None:
    engine = Engine()
    log = engine.accumulator("log")

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        log.push("head")
        return member() + inp()

    @engine.query
    def member() -> int:
        return head() // 2

    @engine.query
    def top() -> int:
        return head()

    inp.set(4)
    effects: dict[str, list[Any]] = {}
    assert top(effects=effects) == 7  # four runs of the head
    assert effects == {"log": ["head"]}


@_defect(25, "a dependent of an evicted cycle member raises CycleError")
def test_d25_evicting_a_cycle_member_does_not_change_the_outcome() -> None:
    engine = Engine()

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    def head() -> int:
        return member() + inp()

    @engine.query
    def member() -> int:
        return head() // 2

    @engine.query
    def top() -> int:
        return member() * 10

    inp.set(4)
    assert head() == 7
    assert top() == 30
    # What LRU eviction, prune() or sweep_unaccessed() do to one result.
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store.drop_memo_locked(("query", member.id, ()))
    inp.set(6)
    # With the member's result still cached this returns 50.
    assert top() == 50


# --- Data loss and corruption -----------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
def test_d11_clear_disk_cache_keeps_live_named_collections(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        xs = CascadeList(engine, [1, 2, 3], name="xs")
        engine.clear_disk_cache()
        xs.append(4)
        live = list(xs)
    finally:
        engine.shutdown()

    reopened = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert list(CascadeList(reopened, name="xs")) == live == [1, 2, 3, 4]
    finally:
        reopened.shutdown()


def test_d12_snapshot_read_survives_compaction_with_an_older_pin() -> None:
    engine = Engine()
    before_collection = engine.snapshot()
    xs = CascadeList(engine, [1])

    @engine.query
    def n() -> int:
        return len(xs)

    snapshot = engine.snapshot()
    for i in range(3000):
        xs.append(i)
    assert n(snapshot=snapshot) == 1
    before_collection.release()


def test_d12_rollback_undoes_everything_after_compaction_in_the_block() -> None:
    engine = Engine()
    xs: CascadeList | None = None
    with pytest.raises(RuntimeError, match="abort"):
        with engine.transaction():
            xs = CascadeList(engine, [])
            for i in range(3000):
                xs.append(i)
            raise RuntimeError("abort")
    assert xs is not None
    assert len(xs) == 0


def test_d13_reused_transaction_object_does_not_roll_back_committed_data() -> None:
    engine = Engine()
    xs = CascadeList(engine, [])
    tx = engine.transaction()
    with tx:
        xs.append("committed-1")
    xs.append("committed-2")
    # Design choice: a transaction object may be reusable, or single-use and
    # refuse to be entered twice. Either way committed data must survive.
    try:
        with tx:
            xs.append("doomed")
            raise RuntimeError("abort")
    except RuntimeError:
        pass
    assert list(xs) == ["committed-1", "committed-2"]


def test_d14_failed_set_update_leaves_storage_and_queries_in_agreement() -> None:
    engine = Engine()
    s = CascadeSet(engine, [])

    @engine.query
    def members() -> list[int]:
        return sorted(x for x in s)

    assert members() == []
    with pytest.raises(TypeError):
        s.update([1, [2]])
    # Design choice: the update may be atomic (nothing applied), or behave
    # like the builtin and keep the elements before the bad one. Either way
    # queries must see exactly what the set holds.
    assert members() == sorted(set.__iter__(s))


_D15_CHILD = """
import sys
from cascade import Engine

mode, cache_dir, count = sys.argv[1], sys.argv[2], int(sys.argv[3])
engine = Engine(cache_dir=cache_dir, cache_backend="mdbx", max_entries=100_000)
runs = [0]

@engine.query
def square(i):
    runs[0] += 1
    return i * i

for i in range(count):
    square(i)
if mode == "read":
    print(runs[0])
    engine.shutdown()
# mode == "write": fall off the end without shutdown() or flush_disk().
"""


def _run_d15_child(mode: str, cache_dir: Path, count: int) -> str:
    env = dict(os.environ)
    src = str(Path(cascade.__file__).resolve().parent.parent)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (src, env.get("PYTHONPATH", "")) if p
    )
    done = subprocess.run(
        [sys.executable, "-c", _D15_CHILD, mode, str(cache_dir), str(count)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    return done.stdout.strip()


def test_d15_results_survive_a_normal_exit_without_shutdown(tmp_path: Path) -> None:
    count = 500
    _run_d15_child("write", tmp_path, count)
    rerun = int(_run_d15_child("read", tmp_path, count))
    assert rerun == 0, f"{rerun} of {count} results were not on disk"


@pytest.mark.parametrize("backend", BACKENDS)
def test_d16_format_change_does_not_silently_drop_collections(
    tmp_path: Path, backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        CascadeList(engine, [1, 2, 3], name="xs")
    finally:
        engine.shutdown()

    monkeypatch.setattr(_disk_cache, "DISK_FORMAT", _disk_cache.DISK_FORMAT + 1)
    # Design choice: the fix may migrate or keep the collection log, or refuse
    # to open the directory with a clear error. Either is correct; coming
    # back empty with no error is not, because collection logs are source
    # data that cannot be recomputed.
    try:
        reopened = Engine(cache_dir=tmp_path, cache_backend=backend)
    except PersistentCacheError:
        return
    try:
        try:
            restored = list(CascadeList(reopened, name="xs"))
        except PersistentCacheError:
            return
        assert restored == [1, 2, 3]
    finally:
        reopened.shutdown()


def test_d17_rollback_keeps_another_threads_collection_change() -> None:
    engine = Engine()
    xs = CascadeList(engine, ["base"])
    other_error: list[BaseException] = []

    def other_thread() -> None:
        try:
            xs.append("other")
        except BaseException as exc:  # noqa: BLE001
            other_error.append(exc)

    with pytest.raises(RuntimeError, match="abort"):
        with engine.transaction():
            xs.append("tx")
            thread = threading.Thread(target=other_thread, daemon=True)
            thread.start()
            thread.join(10)
            raise RuntimeError("abort")

    # Design choice: the other thread's append may succeed and survive the
    # rollback, or be refused with an error while the transaction holds the
    # collection. Losing it silently is not acceptable.
    if other_error:
        assert list(xs) == ["base"]
    else:
        assert list(xs) == ["base", "other"]


def test_d17_task_created_in_a_transaction_can_set_after_it_commits() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    async def main() -> int:
        gate = asyncio.Event()

        async def later() -> None:
            await gate.wait()
            x.set(5)

        with engine.transaction():
            task = asyncio.create_task(later())
        gate.set()
        # Design choice: the late set() may apply, or raise because its
        # transaction is over. Dropping it silently is not acceptable.
        try:
            await task
        except RuntimeError:
            return 5
        return x()

    assert asyncio.run(main()) == 5


@_defect(28, "C code that changes a collection's storage directly logs no diff")
def test_d28_heapq_on_a_cascade_list_reaches_queries() -> None:
    engine = Engine()
    xs = CascadeList(engine, [5, 3])

    @engine.query
    def n() -> int:
        return len(xs)

    assert n() == 2
    heapq.heappush(xs, 1)
    # Either the push is seen, or it is refused; never silently missed.
    assert n() == 3


@_defect(27, "a commit that fails on an input value keeps collection changes")
def test_d27_commit_that_fails_on_an_input_applies_nothing() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    xs = CascadeList(engine, [1])

    @engine.query
    def n() -> int:
        return len(xs)

    assert (x(), n()) == (1, 1)
    with pytest.raises(TypeError):
        with engine.transaction():
            xs.append(2)
            x.set(object())  # cannot be hashed, so the commit fails
    # Nothing applied: the collection is back, and queries agree with it.
    assert list(xs) == [1]
    assert (x(), n()) == (1, 1)


# --- Hangs and misleading errors --------------------------------------------


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_d18_flush_disk_returns_after_a_writer_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")

    def locked(entries: Any) -> None:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(engine._disk, "store_entry_many", locked)  # noqa: SLF001

    @engine.query
    def q(i: int) -> int:
        return i

    assert q(1) == 1
    # Never call flush_disk() on this thread: today it would not return.
    # Returning or raising a clear error are both fine; hanging is not.
    finished, _ = _run_with_timeout(engine.flush_disk)
    if finished:
        finished, _ = _run_with_timeout(engine.shutdown)
    else:
        # shutdown() would hang the same way; just release the file.
        with contextlib.suppress(Exception):
            engine._disk.close()  # noqa: SLF001
    assert finished, "flush_disk()/shutdown() did not return after a writer error"


def test_d19_direct_caller_is_not_cancelled_by_a_submitted_run() -> None:
    engine = Engine()
    started = threading.Event()
    go = threading.Event()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def leaf() -> int:
        return 0

    @engine.query
    def q() -> int:
        value = x()
        started.set()
        go.wait(10)
        return value + leaf()  # the nested call notices the cancellation

    direct: list[Any] = []

    def direct_call() -> None:
        try:
            direct.append(q())
        except BaseException as exc:  # noqa: BLE001
            direct.append(exc)

    try:
        future = engine.submit(q)
        assert started.wait(10)
        thread = threading.Thread(target=direct_call, daemon=True)
        thread.start()
        # Wait until the direct caller has joined the submitted run.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with engine._internals.lock:  # noqa: SLF001
                slots = list(engine._internals.in_flight.values())  # noqa: SLF001
            if any(slot.future is not None for slot in slots):
                break
            time.sleep(0.005)
        x.set(2)
        go.set()
        thread.join(10)
        assert not thread.is_alive()
        with pytest.raises(QueryCancelled):
            future.result(timeout=10)
    finally:
        go.set()
        engine.shutdown()

    # Only submit() cancels. The direct caller started when x was 1 and must
    # get that revision's answer, not the submitted run's cancellation.
    assert direct == [1]


@pytest.mark.parametrize("backend", BACKENDS)
def test_d23_async_query_result_is_reused_by_a_new_engine(
    tmp_path: Path, backend: str
) -> None:
    runs = {"n": 0}

    def session() -> int:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.input
        def x() -> int:
            return 3

        @engine.query
        async def double() -> int:
            runs["n"] += 1
            return x() * 2

        async def main() -> int:
            return await double()

        try:
            return asyncio.run(main())
        finally:
            engine.shutdown()

    assert session() == 6
    assert session() == 6
    assert runs["n"] == 1


def test_d24_threads_entering_a_cycle_from_both_ends_do_not_deadlock() -> None:
    engine = Engine()
    in_head = threading.Event()
    in_member = threading.Event()

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        in_head.set()
        in_member.wait(HANG_TIMEOUT)
        return member() + inp()

    @engine.query
    def member() -> int:
        in_member.set()
        in_head.wait(HANG_TIMEOUT)
        return head() // 2

    inp.set(4)
    outcomes: dict[str, Any] = {}

    def call(name: str, query: Any) -> None:
        try:
            outcomes[name] = query()
        except BaseException as exc:  # noqa: BLE001
            outcomes[name] = exc

    # Each thread now runs one of the two queries and asks for the other.
    threads = [
        threading.Thread(target=call, args=("head", head), daemon=True),
        threading.Thread(target=call, args=("member", member), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(HANG_TIMEOUT)
    # Design choice: both may get the fixed point, or the thread that entered
    # through the member may get a CycleError, as it does on one thread.
    # Waiting for each other for ever is not acceptable.
    assert not any(thread.is_alive() for thread in threads), "threads deadlocked"
    assert outcomes["head"] == 7
    assert outcomes["member"] == 3 or isinstance(outcomes["member"], cascade.CycleError)


def _plain_object() -> Any:
    class Plain:
        pass

    return Plain()


def test_d20_unencodable_result_is_returned_or_reported_clearly() -> None:
    engine = Engine()
    value = _plain_object()

    @engine.query
    def q() -> Any:
        return value

    # Design choice: an engine without cache_dir may return the value, or
    # reject it. A rejection must say which query produced the value (or use
    # a Cascade error type), not surface as a bare TypeError from the codec.
    try:
        result = q()
    except Exception as exc:  # noqa: BLE001
        assert type(exc) is not TypeError or q.id in str(exc)
    else:
        assert result is value


def test_d20_exception_with_unencodable_args_reaches_the_caller() -> None:
    engine = Engine()

    @engine.query
    def q() -> Any:
        raise ValueError(_plain_object())

    with pytest.raises(ValueError):
        q()


def test_d21_sync_query_on_the_loop_does_not_deadlock_on_an_async_run() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    async def slow() -> int:
        value = x()
        await asyncio.sleep(0.2)
        return value

    @engine.query
    def dependent() -> int:
        return slow() + 1

    # Off the loop the sync query drives the async one and depends on it.
    assert dependent() == 2
    x.set(2)

    async def main() -> Any:
        task = asyncio.create_task(slow())
        await asyncio.sleep(0.05)  # slow() is now running on this loop
        try:
            # Re-verifying dependent() waits, synchronously and on the loop
            # thread, for the run of slow() that only this loop can finish.
            return dependent()
        finally:
            await task

    # Runs on a daemon thread because today it never returns. A value or a
    # clear error are both acceptable; blocking the loop forever is not.
    finished, _ = _run_with_timeout(lambda: asyncio.run(main()))
    assert finished, "sync call on the event-loop thread deadlocked"
