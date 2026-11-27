"""Regression tests for the defects listed in docs/guarantees.md (D1-D37).

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
    CascadeDict,
    CascadeList,
    CascadeSet,
    CollectionBypassedError,
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


def test_d29_set_order_is_the_same_rewritten_or_not() -> None:
    engine = Engine()
    s = CascadeSet(engine, [30, 1, 20])

    @engine.query
    def rewritten() -> list[int]:
        return [x for x in s]

    @engine.query(incremental_per_element=False)
    def unrewritten() -> list[int]:
        return [x for x in s]

    # Either order would do, as long as it is one order.
    assert rewritten() == unrewritten()


def test_d30_walrus_in_a_comprehension_binds_in_the_query() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])

    @engine.query
    def last_seen() -> Any:
        last = None
        out = [(last := x) for x in xs]
        return (last, out)

    assert last_seen() == (3, [1, 2, 3])


def test_d32_nested_pipeline_sees_the_loop_variable_of_a_kept_comprehension() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2])
    ys = CascadeList(engine, [10, 20])

    @engine.query
    def scaled() -> Any:
        # Two ``for`` clauses: the outer comprehension stays as written.
        return [sum(y * x for y in ys) for x in xs for _ in (0,)]

    @engine.query
    def shadowed() -> Any:
        x = 100
        return [sum(y * x for y in ys) for x in xs for _ in (0,)], x

    assert scaled() == [30, 60]  # was NameError: name 'x' is not defined
    assert shadowed() == ([30, 60], 100)  # was ([3000, 3000], 100)


# The accumulator is reached through a global: a stage function that closes
# over one cannot be fingerprinted, so its pipeline is rebuilt on every run
# and the defect does not show.
_D31: dict[str, Any] = {}


def _d31_note(x: int) -> int:
    _D31["seen"].push(x)
    return x


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


def test_d28_heapq_on_a_cascade_list_reaches_queries() -> None:
    engine = Engine()
    xs = CascadeList(engine, [5, 3])

    @engine.query
    def n() -> int:
        return len(xs)

    assert n() == 2
    heapq.heappush(xs, 1)
    # Either the push is seen, or it is refused; never silently missed.
    # Refused: C code wrote the storage and recorded nothing. The next use
    # of the collection undoes the push and raises, and queries agree with
    # what it holds.
    with pytest.raises(CollectionBypassedError, match="heapq"):
        xs.append(7)
    assert list(xs) == [5, 3]
    assert n() == 2


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


def test_d33_caller_the_owner_just_stopped_waiting_for_does_not_run_the_query_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Thread B runs total(), which needs part(), and waits for thread A's run
    # of part(). A finishes part() and asks for total() itself. B has its
    # answer by then. It used to stay recorded as waiting for A until it had
    # taken the engine's lock again, and A, asking in between, concluded
    # that the two waited for each other and ran total() a second time (G5).
    #
    # Under the interpreter lock A got there first nearly every time. The
    # test holds B where it used to remove its record, so that A always asks
    # in between, on any build.
    engine = Engine()
    evaluator = engine._evaluator  # noqa: SLF001
    part_running = threading.Event()
    total_runs: list[int] = []

    def waiting() -> int:
        with engine._internals.lock:  # noqa: SLF001
            return len(engine._store.waiting_on)  # noqa: SLF001

    def poll(condition: Any) -> bool:
        deadline = time.monotonic() + HANG_TIMEOUT
        while not condition():
            if time.monotonic() > deadline:
                return False
            time.sleep(0.002)
        return True

    @engine.query
    def part() -> int:
        part_running.set()
        assert poll(lambda: waiting() >= 1)  # B waits for this run
        return 1

    @engine.query
    def total() -> int:
        total_runs.append(1)
        return part() + 1

    def a_went_on() -> bool:
        # A waits for B's run of total(), or runs it itself (the defect).
        waits = [t.event for t in engine.traces()].count("dedup_wait")
        return len(total_runs) > 1 or waits > 1

    held = threading.Event()
    leave_wait = evaluator._leave_wait  # noqa: SLF001

    def leave_wait_late(agent: Any, slot: Any) -> None:
        if not held.is_set():
            held.set()
            poll(a_went_on)
        leave_wait(agent, slot)

    monkeypatch.setattr(evaluator, "_leave_wait", leave_wait_late)
    outcomes: dict[str, Any] = {}

    def call(name: str, fn: Any) -> None:
        try:
            outcomes[name] = fn()
        except BaseException as exc:  # noqa: BLE001
            outcomes[name] = exc

    def a() -> int:
        part()
        return total()

    def b() -> int:
        assert part_running.wait(HANG_TIMEOUT)
        return total()

    threads = [
        threading.Thread(target=call, args=("a", a), daemon=True),
        threading.Thread(target=call, args=("b", b), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(2 * HANG_TIMEOUT)
    assert not any(thread.is_alive() for thread in threads), "threads hung"
    assert outcomes == {"a": 2, "b": 2}
    assert len(total_runs) == 1
    assert "dedup_cycle" not in [t.event for t in engine.traces()]


def test_d34_cancelling_a_waiting_task_leaves_the_owner_and_other_waiters_alone() -> (
    None
):
    # Three tasks ask for one query. The first runs it, the other two wait
    # for that run. Cancelling one of the two must cancel that one only: the
    # three used to share one future, which the cancelled task cancelled.
    engine = Engine()
    runs: list[int] = []
    state: dict[str, Any] = {}

    def waiting() -> int:
        with engine._internals.lock:  # noqa: SLF001
            return len(engine._store.waiting_on)  # noqa: SLF001

    async def poll(condition: Any) -> None:
        deadline = time.monotonic() + HANG_TIMEOUT
        while not condition():
            assert time.monotonic() < deadline, "timed out"
            await asyncio.sleep(0.002)

    @engine.query
    async def slow() -> int:
        runs.append(1)
        await state["go"].wait()
        return 42

    async def main() -> list[Any]:
        state["go"] = asyncio.Event()
        owner = asyncio.create_task(slow())
        await poll(lambda: runs)
        cancelled = asyncio.create_task(slow())
        other = asyncio.create_task(slow())
        await poll(lambda: waiting() == 2)
        cancelled.cancel()
        await asyncio.gather(cancelled, return_exceptions=True)
        state["go"].set()
        return await asyncio.gather(owner, cancelled, other, return_exceptions=True)

    owner, cancelled, other = asyncio.run(asyncio.wait_for(main(), 4 * HANG_TIMEOUT))
    assert isinstance(cancelled, asyncio.CancelledError)
    assert owner == 42
    assert other == 42
    assert len(runs) == 1


def test_d34_sync_query_keeps_its_value_when_a_task_waiting_for_its_run_is_cancelled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A thread runs outer(), a sync query that calls the async query slow().
    # A task on another loop asks for slow() as well, waits for the thread's
    # run of it and is cancelled. outer() used to raise InvalidStateError
    # where the run set its result, and that error was cached: later calls
    # raised it again without running anything.
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
    runs = {"slow": 0, "outer": 0}

    def waiting() -> int:
        with engine._internals.lock:  # noqa: SLF001
            return len(engine._store.waiting_on)  # noqa: SLF001

    @engine.query
    async def slow() -> int:
        runs["slow"] += 1
        started.set()
        deadline = time.monotonic() + 4 * HANG_TIMEOUT
        while not release.is_set():
            assert time.monotonic() < deadline, "never released"
            await asyncio.sleep(0.002)
        return 42

    @engine.query
    def outer() -> int:
        runs["outer"] += 1
        return slow() + 1

    outcome: list[Any] = []

    def call_outer() -> None:
        try:
            outcome.append(outer())
        except BaseException as exc:  # noqa: BLE001
            outcome.append(exc)

    thread = threading.Thread(target=call_outer, daemon=True)
    thread.start()
    assert started.wait(HANG_TIMEOUT)

    async def cancelled_waiter() -> Any:
        task = asyncio.create_task(slow())
        deadline = time.monotonic() + HANG_TIMEOUT
        while waiting() < 1:
            assert time.monotonic() < deadline, "the task never waited"
            await asyncio.sleep(0.002)
        task.cancel()
        return (await asyncio.gather(task, return_exceptions=True))[0]

    try:
        # The task's loop is closed by the time the run ends.
        assert isinstance(asyncio.run(cancelled_waiter()), asyncio.CancelledError)
    finally:
        release.set()
    thread.join(HANG_TIMEOUT)
    assert not thread.is_alive(), "outer() hung"
    assert outcome == [43]
    assert outer() == 43
    assert runs == {"slow": 1, "outer": 1}
    # The run ended after the cancelled task's loop was closed, and has to
    # cope with that quietly.
    assert not [r for r in caplog.records if r.name == "concurrent.futures"]


def test_d35_two_threads_asking_for_a_dropped_cycle_member_both_get_its_value() -> None:
    # The member of a fixed-point cycle has lost its result and an input has
    # changed (D25). A caller that asks for the member runs it, reaches the
    # member again through head(), and starts over by entering the cycle
    # through head(). That went wrong when a second caller had taken over the
    # member in between: the first one's head() waited for that run, which
    # failed as the first one's own had (it reached itself). The head's run
    # gave up there, as a verification does, head() was never brought up to
    # date, and the first caller got CycleError for a cycle that converges.
    #
    # On a free-threaded build six threads got there by themselves about one
    # time in ten. Three holds make it certain on any build.
    engine = Engine()
    first_running = threading.Event()
    state = {"held": False}
    runs = {"head": 0, "member": 0}
    counting = threading.Lock()

    def count(name: str) -> int:
        with counting:
            runs[name] += 1
            return runs[name]

    def waiting() -> int:
        with engine._internals.lock:  # noqa: SLF001
            return len(engine._store.waiting_on)  # noqa: SLF001

    def poll(condition: Any) -> bool:
        deadline = time.monotonic() + HANG_TIMEOUT
        while not condition():
            if time.monotonic() > deadline:
                return False
            time.sleep(0.002)
        return True

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        if state["held"] and count("head") == 2:
            # The first caller enters the cycle through head(): not before
            # the second caller has taken over the member.
            assert poll(lambda: runs["member"] == 2)
        return member() + inp()

    @engine.query
    def member() -> int:
        if state["held"]:
            run = count("member")
            if run == 1:
                first_running.set()
                assert poll(lambda: waiting() >= 1)  # the second caller joined
            elif run == 2:
                # The second caller's own run: not before the first one's
                # head() waits for it.
                assert poll(lambda: waiting() >= 1)
        return head() // 2

    assert (head(), member()) == (7, 3)
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store.drop_memo_locked(("query", member.id, ()))
    inp.set(6)
    state["held"] = True
    outcomes: dict[str, Any] = {}

    def call(name: str, after: threading.Event | None) -> None:
        try:
            if after is not None:
                assert after.wait(HANG_TIMEOUT)
            outcomes[name] = member()
        except BaseException as exc:  # noqa: BLE001
            outcomes[name] = exc

    threads = [
        threading.Thread(target=call, args=("first", None), daemon=True),
        threading.Thread(target=call, args=("second", first_running), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(4 * HANG_TIMEOUT)
    assert not any(thread.is_alive() for thread in threads), "threads hung"
    # head = head // 2 + 6 converges to 11.
    assert outcomes == {"first": 5, "second": 5}
    assert (head(), member()) == (11, 5)


def test_d35_two_tasks_asking_for_a_dropped_cycle_member_both_get_its_value() -> None:
    # The same with two tasks on one loop.
    engine = Engine()
    state = {"held": False}
    runs = {"head": 0, "member": 0}

    def waiting() -> int:
        with engine._internals.lock:  # noqa: SLF001
            return len(engine._store.waiting_on)  # noqa: SLF001

    async def poll(condition: Any) -> None:
        deadline = time.monotonic() + HANG_TIMEOUT
        while not condition():
            assert time.monotonic() < deadline, "timed out"
            await asyncio.sleep(0.002)

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    async def head() -> int:
        if state["held"]:
            runs["head"] += 1
            if runs["head"] == 2:
                await poll(lambda: runs["member"] == 2)
        return await member() + inp()

    @engine.query
    async def member() -> int:
        if state["held"]:
            runs["member"] += 1
            if runs["member"] in (1, 2):
                await poll(lambda: waiting() >= 1)
        return await head() // 2

    async def warm() -> tuple[int, int]:
        return await head(), await member()

    async def two_callers() -> list[Any]:
        first = asyncio.create_task(member())
        await poll(lambda: runs["member"] == 1)
        second = asyncio.create_task(member())
        return await asyncio.gather(first, second, return_exceptions=True)

    assert asyncio.run(warm()) == (7, 3)
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store.drop_memo_locked(("query", member.id, ()))
    inp.set(6)
    state["held"] = True
    outcomes = asyncio.run(asyncio.wait_for(two_callers(), 8 * HANG_TIMEOUT))
    assert outcomes == [5, 5]
    assert asyncio.run(warm()) == (11, 5)


def _d36_bodies(d: Any) -> list[Any]:
    def comp() -> Any:
        return {x: 1 for x in d}

    def comp_same() -> Any:
        return {x: x for x in d}

    def comp_filtered() -> Any:
        return {x: -x for x in d if x}

    def from_generator() -> Any:
        return dict((x, 1) for x in d)

    def from_map() -> Any:
        return dict(map(lambda x: (x, 2), d))

    def unpacking_keys() -> Any:
        # The keys are ints: plain Python cannot unpack them.
        return {p: q for p, q in d}

    def bare() -> Any:
        return dict(d)

    def over_items() -> Any:
        return {p: q for p, q in d.items()}

    return [comp, comp_same, comp_filtered, from_generator, from_map,
            unpacking_keys, bare, over_items]  # fmt: skip


def _d36_outcome(fn: Any) -> Any:
    try:
        return ("value", fn())
    except Exception as exc:  # noqa: BLE001
        return ("raises", type(exc).__name__, str(exc))


def test_d36_dict_built_from_a_cascade_dict_iterates_its_keys() -> None:
    """D36: ``dict`` over stages of a ``CascadeDict`` saw its items.

    Iterating a dict gives its keys. The rewritten ``{x: 1 for x in d}``
    iterated ``(key, value)`` pairs instead, because the runtime read a
    ``CascadeDict`` under the ``dict`` reducer as its items, which is right
    only for ``dict(d)`` itself.
    """
    engine = Engine()
    d = CascadeDict(engine, {0: 5, 1: 6})
    plain = {0: 5, 1: 6}
    queries = [engine.query(fn, name=fn.__name__) for fn in _d36_bodies(d)]
    for query, reference in zip(queries, _d36_bodies(plain), strict=True):
        assert _d36_outcome(query) == _d36_outcome(reference), query.id
    d[2] = 7
    plain[2] = 7
    del d[0]
    del plain[0]
    for query, reference in zip(queries, _d36_bodies(plain), strict=True):
        assert _d36_outcome(query) == _d36_outcome(reference), query.id


def _d37_bodies(xs: Any) -> list[Any]:
    def any_of_list() -> Any:
        return any([1 // x for x in xs])

    def all_of_list() -> Any:
        return all([x - 1 // x for x in xs])

    def any_of_map_of_list() -> Any:
        return any(map(abs, [1 // x for x in xs]))

    def first_error() -> Any:
        # The list is built in full first: 10 // 0 raises before the outer
        # stage divides by zero on the first element.
        return sum(map(lambda v: 1 / (v - 10), [10 // x for x in xs]))

    def len_of_list() -> Any:
        return len([1 // x for x in xs if x is not None])

    def any_of_generator() -> Any:
        return any(1 // x for x in xs)  # lazy in Python too

    return [any_of_list, all_of_list, any_of_map_of_list, first_error,
            len_of_list, any_of_generator]  # fmt: skip


def test_d37_list_comprehension_is_built_in_full_before_it_is_read() -> None:
    """D37: a rewritten list comprehension was evaluated lazily.

    ``any([1 // x for x in xs])`` over ``[1, 0]`` raises in Python, because
    the list is built before ``any`` reads it. The rewritten expression fell
    back to a lazy plain evaluation when an element raised, and ``any``
    stopped at the first element: it returned True.
    """
    engine = Engine()
    xs = CascadeList(engine, [1, 0])
    plain = [1, 0]
    queries = [engine.query(fn, name=fn.__name__) for fn in _d37_bodies(xs)]
    for query, reference in zip(queries, _d37_bodies(plain), strict=True):
        assert _d36_outcome(query) == _d36_outcome(reference), query.id
    xs.append(0)
    plain.append(0)
    xs[1] = 2
    plain[1] = 2
    for query, reference in zip(queries, _d37_bodies(plain), strict=True):
        assert _d36_outcome(query) == _d36_outcome(reference), query.id


def test_d37_dict_of_a_list_of_cascade_dict_keys_iterates_the_keys() -> None:
    """With D36: ``dict([p for p in d])`` has no stage but the list's own."""
    engine = Engine()
    d = CascadeDict(engine, {0: 5})

    @engine.query
    def copied() -> Any:
        return dict([p for p in d])

    with pytest.raises(TypeError):
        dict([p for p in {0: 5}])
    with pytest.raises(TypeError):
        copied()


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


@pytest.mark.parametrize("backend", BACKENDS)
def test_d38_rolled_back_creation_of_a_named_collection_writes_nothing(
    tmp_path: Path, backend: str
) -> None:
    # Creating a named collection stamped an empty record of it on disk at
    # once, outside the transaction's held writes. After a rollback that
    # record stayed: the next construction under the name restored it and
    # dropped its own initial contents, and another kind was refused.
    class Abort(Exception):
        pass

    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    with pytest.raises(Abort):
        with engine.transaction():
            CascadeList(engine, [1, 2, 3], name="seeded")
            CascadeList(engine, [1], name="either")
            raise Abort
    engine.shutdown()
    reopened = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert list(CascadeList(reopened, [1, 2, 3], name="seeded")) == [1, 2, 3]
        assert set(CascadeSet(reopened, {1, 2}, name="either")) == {1, 2}
    finally:
        reopened.shutdown()


def _d39_engine() -> tuple[Engine, Any, Any, dict[str, int]]:
    engine = Engine()
    runs = {"double": 0}

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def double() -> int:
        runs["double"] += 1
        return 2 * x()

    return engine, x, double, runs


def test_d39_submitted_call_on_a_thread_that_copied_a_query_context() -> None:
    # Where threads copy the context of the thread that starts them
    # (free-threaded 3.14), a pool worker started from inside a running
    # query keeps that query's runtime. A call submitted to it later ran as
    # part of that finished query: at its old revision, never cancelled.
    # The executor here imitates such a worker on any build.
    import concurrent.futures
    import contextvars

    engine, x, double, _runs = _d39_engine()
    captured: dict[str, contextvars.Context] = {}

    @engine.query
    def capture() -> int:
        captured["ctx"] = contextvars.copy_context()
        return x()

    assert capture() == 1
    x.set(5)

    class InheritingExecutor(concurrent.futures.ThreadPoolExecutor):
        def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
            return super().submit(captured["ctx"].run, fn, *args, **kwargs)

    with InheritingExecutor(max_workers=1) as pool:
        assert engine.submit(double, executor=pool).result(HANG_TIMEOUT) == 10
    assert double() == 10
    engine.shutdown()


@pytest.mark.skipif(
    not getattr(sys.flags, "thread_inherit_context", False),
    reason="threads start with an empty context on this build",
)
def test_d39_submit_after_an_async_query_started_the_pool(tmp_path: Path) -> None:
    # The path met on a free-threaded build: an async query loaded from disk
    # checks the default of a sync input on the engine's pool, whose worker
    # thread starts inside the async query's context; a later submit reuses
    # that worker.
    def build() -> tuple[Engine, Any, Any, Any, dict[str, int]]:
        engine = Engine(cache_dir=tmp_path)
        runs = {"double": 0}

        @engine.input
        def x() -> int:
            return 1

        @engine.query
        def double() -> int:
            runs["double"] += 1
            return 2 * x()

        @engine.query
        async def outer() -> int:
            return 3 * x()

        return engine, x, double, outer, runs

    async def read(outer: Any) -> int:
        return await outer()

    first, _x, _double, outer, _runs = build()
    assert asyncio.run(read(outer)) == 3
    first.shutdown()
    engine, x, double, outer, runs = build()
    assert asyncio.run(read(outer)) == 3  # loaded: x's default ran on the pool
    x.set(5)
    assert engine.submit(double).result(HANG_TIMEOUT) == 10
    # Cancellation works on that worker too.
    release = threading.Event()
    reached = threading.Event()

    @engine.query
    def held() -> int:
        reached.set()
        assert release.wait(HANG_TIMEOUT)
        return x()

    future = engine.submit(held)
    assert reached.wait(HANG_TIMEOUT)
    x.set(6)
    release.set()
    with pytest.raises(QueryCancelled):
        future.result(HANG_TIMEOUT)
    assert held() == 6
    assert runs["double"] == 1
    engine.shutdown()


def test_d40_edited_body_under_a_wrapping_decorator_is_recomputed(
    tmp_path: Path,
) -> None:
    # The stored hash covered the bytecode of the registered function, which
    # under a decorator is the wrapper; the body it wraps was not hashed, so
    # an edit to it between runs left the old stored result in use.
    import textwrap

    def program(value: int) -> Any:
        namespace: dict[str, Any] = {"__name__": "program"}
        source = f"""
        import functools

        def logged(fn):
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                return fn(*args, **kwargs)
            return wrapper

        @logged
        def q():
            return {value}
        """
        exec(textwrap.dedent(source), namespace)
        return namespace["q"]

    for value in (1, 1, 2):
        engine = Engine(cache_dir=tmp_path)
        try:
            assert engine.query(program(value), name="q")() == value
        finally:
            engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("then_append", [True, False], ids=["tail", "snapshot-only"])
def test_d41_held_back_record_does_not_overwrite_another_engines_collection(
    tmp_path: Path, backend: str, then_append: bool
) -> None:
    # The fix for D38 holds a new named collection's record back to the
    # commit. Written then without looking, it replaced the compacted
    # snapshot another engine had written to the name in the meantime.
    a = Engine(cache_dir=tmp_path, cache_backend=backend)
    b = Engine(cache_dir=tmp_path, cache_backend=backend)
    with a.transaction():
        CascadeList(a, name="xs")
        xs = CascadeList(b, name="xs")
        xs.extend([1, 2])
        xs.compact()
        if then_append:  # a snapshot and a tail on disk, or the snapshot alone
            xs.append(3)
    a.shutdown()
    b.shutdown()
    reopened = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        expected = [1, 2, 3] if then_append else [1, 2]
        assert list(CascadeList(reopened, name="xs")) == expected
    finally:
        reopened.shutdown()


def test_d45_dict_items_keep_the_key_the_dict_holds_after_an_equal_key_update() -> None:
    # A dict keeps the key it was first given: ``d[1.0] = "b"`` where ``1``
    # is a key replaces the value under ``1``. A rewritten pipeline over
    # ``d.items()``, ``dict(d)`` among them, took the key from the update
    # and gave ``{1.0: "b"}``; a restart, which rebuilds the pipeline from
    # the contents, gave ``{1: "b"}``. Found by test_rewrite_programs.py
    # once its keys included equal values of other types.
    engine = Engine()
    d = CascadeDict(engine, {1: "a", 0: "z", "k": "v"})

    def body(source: Any) -> Any:
        return (
            dict(source),
            dict(source.items()),
            {k: v for k, v in source.items()},
            sorted(source.items(), key=str),
            [k for k, _ in source.items()],
        )

    @engine.query
    def q() -> Any:
        return (
            dict(d),
            dict(d.items()),
            {k: v for k, v in d.items()},
            sorted(d.items(), key=str),
            [k for k, _ in d.items()],
        )

    assert getattr(q.raw, "__cascade_rewritten__", False)
    assert repr(q()) == repr(body(dict(d)))
    d[1.0] = "b"
    d[False] = "y"
    d.update({True: "c"})
    assert repr(q()) == repr(body(dict(d)))
    assert repr(dict(d)) == "{1: 'c', 0: 'y', 'k': 'v'}"
    del d[1.0]
    d[1.0] = "d"
    assert repr(q()) == repr(body(dict(d)))
    assert repr(dict(d)) == "{0: 'y', 'k': 'v', 1.0: 'd'}"


@pytest.mark.parametrize("backend", BACKENDS)
def test_d50_nan_key_removed_before_a_restart_stays_removed(
    tmp_path: Path, backend: str
) -> None:
    # A dict holds a NaN key, and a set a NaN element, by identity: NaN is
    # equal to nothing, itself included, so ``del d[nan]`` finds the key
    # only as the same object. The log on disk had the key twice, in the
    # write and in the removal, and decoding made two NaN objects of them:
    # replayed at the next start, the removal found nothing to remove.
    # Diffs now name entries by id (collection format 3).
    # Found by test_rewrite_programs.py once its values included NaN.
    nan = float("nan")
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        d = CascadeDict(engine, name="d")
        s = CascadeSet(engine, name="s")
        d[nan] = 1
        d["a"] = 2
        del d[nan]
        s.add(nan)
        s.discard(nan)
        assert (dict(d), set(s)) == ({"a": 2}, set())
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        d = CascadeDict(engine, name="d")
        s = CascadeSet(engine, name="s")
        assert (dict(d), set(s)) == ({"a": 2}, set())
    finally:
        engine.shutdown()


def test_d46_a_nan_captured_by_a_pipeline_is_one_object_in_its_result() -> None:
    # Every NaN has the same digest, so a pipeline whose element reads a
    # NaN made anew on each run kept its state across runs and mixed the
    # NaN of an earlier run with this run's: ``set([m for _ in d.values()])``
    # held two NaN objects after a change, where Python's has one (a set
    # tells NaN objects apart). Found by test_rewrite_programs.py once its
    # values included NaN.
    engine = Engine()
    d = CascadeDict(engine, {0: 1, 1: 2})

    @engine.query
    def q() -> Any:
        m = float("nan")  # a new object on every run
        return set([m for _ in d.values()]), [m for _ in d.values()]

    assert getattr(q.raw, "__cascade_rewritten__", False)
    assert len(q()[0]) == 1
    d[0] = 5
    nans, listed = q()
    assert len(nans) == 1
    assert listed[0] is listed[1]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("compact", [False, True])
def test_d50_two_nan_keys_stay_two_and_each_is_removed_on_its_own(
    tmp_path: Path, backend: str, compact: bool
) -> None:
    # Two NaN objects are two keys of a dict and two elements of a set. After
    # a restart each is still there, in its place, and a removal of one (by
    # the object a restarted engine hands back) removes that one only, also
    # once a compaction has folded the earlier diffs into the snapshot.
    first, second = float("nan"), float("nan")
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        d = CascadeDict(engine, name="d")
        s = CascadeSet(engine, name="s")
        d.update({"a": 0, first: 1, second: 2})
        s.update([first, "b", second])
        d[1] = "one"
        d[1.0] = "uno"  # the entry keeps the key 1
        del d[first]
        s.discard(first)
        if compact:
            d.compact()
            s.compact()
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        d = CascadeDict(engine, name="d")
        s = CascadeSet(engine, name="s")
        assert repr(dict(d)) == "{'a': 0, nan: 2, 1: 'uno'}"
        assert type(next(k for k in d if k != "a" and k == k)) is int
        assert repr(list(s)) == "['b', nan]"
        nan_key = next(k for k in d if k != k)
        nan_element = next(v for v in s if v != v)
        del d[nan_key]
        s.remove(nan_element)
        s.add(float("nan"))
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert dict(CascadeDict(engine, name="d")) == {"a": 0, 1: "uno"}
        assert repr(list(CascadeSet(engine, name="s"))) == "['b', nan]"
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("awaited", [False, True])
def test_d51_setting_the_default_after_a_result_reused_from_disk_is_no_change(
    tmp_path: Path, backend: str, awaited: bool
) -> None:
    # A result reused from disk checked a never-set input against its
    # default without keeping it, as a direct read keeps it (G3). A later
    # ``set`` to the default was then a change: the revision moved and a
    # submitted call was cancelled (G6). Found by
    # test_transaction_properties.py, whose model expected no change.
    runs: list[str] = []

    def session() -> tuple[Engine, Any, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.input(name="x")
        def x() -> int:
            return 0

        if awaited:

            @engine.query(name="q")
            async def q() -> int:
                runs.append("q")
                return x() + 1

        else:

            @engine.query(name="q")
            def q() -> int:
                runs.append("q")
                return x() + 1

        return engine, x, q

    def call(q: Any) -> int:
        if awaited:
            # Awaited on a running loop: the async path checks the disk.
            async def main() -> int:
                return await q()

            return asyncio.run(main())
        return q()

    engine, _, q = session()
    try:
        assert call(q) == 1
    finally:
        engine.shutdown()
    assert runs == ["q"]

    engine, x, q = session()
    try:
        assert call(q) == 1
        assert runs == ["q"]  # served from disk
        revision = engine.revision
        x.set(0)
        assert engine.revision == revision
        assert engine.submit(q).result(timeout=10) == 1
        assert call(q) == 1
        assert runs == ["q"]
        x.set(5)
        assert engine.revision == revision + 1
        assert call(q) == 6
    finally:
        engine.shutdown()


@pytest.mark.parametrize("waiter", ["task", "thread"])
def test_d52_cancelling_the_task_that_runs_a_query_does_not_cancel_its_waiters(
    waiter: str,
) -> None:
    # A task runs slow() and another caller waits for that run (G5): a
    # second task on the same loop, or a thread. Cancelling the task that
    # runs it used to hand its CancelledError to every waiter: the second
    # task ended cancelled although nobody cancelled it, and the thread
    # raised asyncio.CancelledError out of a plain call. A waiter is not
    # cancelled with the owner (G5, G6): it runs the query itself.
    engine = Engine()
    runs: list[int] = []
    gates: list[asyncio.Event] = []

    @engine.input
    def x() -> int:
        return 4

    @engine.query
    async def slow() -> int:
        runs.append(1)
        gate = asyncio.Event()
        gates.append(gate)
        if len(runs) == 1:
            await gate.wait()  # the first run is cancelled here
        return x() * 10

    def waiting() -> int:
        with engine._internals.lock:  # noqa: SLF001
            return len(engine._store.waiting_on)  # noqa: SLF001

    async def poll(condition: Any) -> None:
        deadline = time.monotonic() + HANG_TIMEOUT
        while not condition():
            assert time.monotonic() < deadline, "timed out"
            await asyncio.sleep(0.002)

    outcome: list[Any] = []

    def call_in_thread() -> None:
        try:
            outcome.append(slow())
        except BaseException as exc:  # noqa: BLE001
            outcome.append(exc)

    async def main() -> Any:
        owner = asyncio.create_task(slow())
        await poll(lambda: runs)
        if waiter == "task":
            other: Any = asyncio.create_task(slow())
        else:
            other = threading.Thread(target=call_in_thread, daemon=True)
            other.start()
        await poll(lambda: waiting() == 1)
        owner.cancel()
        cancelled = (await asyncio.gather(owner, return_exceptions=True))[0]
        if waiter == "task":
            got = (await asyncio.gather(other, return_exceptions=True))[0]
        else:
            await asyncio.to_thread(other.join, HANG_TIMEOUT)
            assert not other.is_alive(), "the waiting thread hung"
            got = outcome[0]
        return cancelled, got

    finished, outcome_of_main = _run_with_timeout(
        lambda: asyncio.run(asyncio.wait_for(main(), 4 * HANG_TIMEOUT)),
        6 * HANG_TIMEOUT,
    )
    assert finished, "the test's event loop hung"
    status, value = outcome_of_main[0]
    assert status == "ok", value
    cancelled, got = value
    assert isinstance(cancelled, asyncio.CancelledError)
    assert got == 40
    assert len(runs) == 2
    assert engine._internals.in_flight == {}  # noqa: SLF001
    assert waiting() == 0


# --- Found by test_async_differential.py --------------------------------------


@pytest.mark.parametrize("awaited", [False, True])
def test_d55_effects_of_a_dependency_rerun_to_an_equal_value_reach_its_dependents(
    awaited: bool,
) -> None:
    # A dependent stores the effects replayed to it with its own result. A
    # dependency that ran again and gave an equal value with other effects
    # kept its stamp (early bail-out, G2), so the dependent was verified
    # unchanged and kept replaying the old effects, where a fresh run gives
    # the new ones (G1, G12). An equal value now keeps the stamp only if the
    # effects are equal too.
    engine = Engine()
    log = engine.accumulator("log")

    @engine.input(name="a")
    def a() -> int:
        return 1

    if awaited:

        @engine.query(name="child")
        async def child() -> int:
            log.push(("child", a()))
            return 0

        @engine.query(name="parent")
        async def parent() -> int:
            await child()
            return 1

    else:

        @engine.query(name="child")
        def child() -> int:
            log.push(("child", a()))
            return 0

        @engine.query(name="parent")
        def parent() -> int:
            child()
            return 1

    def read() -> dict[str, list[Any]]:
        effects: dict[str, list[Any]] = {}
        if awaited:

            async def main() -> int:
                return await parent(effects=effects)

            assert asyncio.run(main()) == 1
        else:
            assert parent(effects=effects) == 1
        return effects

    assert read() == {"log": [("child", 1)]}
    a.set(2)
    assert read() == {"log": [("child", 2)]}
    a.set(2)
    assert read() == {"log": [("child", 2)]}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("awaited", [False, True])
def test_d55_stored_dependent_is_not_reused_when_a_dependency_has_other_effects(
    tmp_path: Path, backend: str, awaited: bool
) -> None:
    # The same across a restart: a stored result recorded its query
    # dependencies by the digest of their values only, so a new engine
    # reused it with the effects of the old session. The digest of a
    # dependency's effects is now part of what is recorded (G15, G16).
    runs: list[str] = []

    def session() -> tuple[Engine, Any, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)
        log = engine.accumulator("log")

        @engine.input(name="a")
        def a() -> int:
            return 1

        if awaited:

            @engine.query(name="child")
            async def child() -> int:
                log.push(("child", a()))
                return 0

            @engine.query(name="parent")
            async def parent() -> int:
                runs.append("parent")
                await child()
                return 1

        else:

            @engine.query(name="child")
            def child() -> int:
                log.push(("child", a()))
                return 0

            @engine.query(name="parent")
            def parent() -> int:
                runs.append("parent")
                child()
                return 1

        return engine, a, parent

    def read(parent: Any) -> dict[str, list[Any]]:
        effects: dict[str, list[Any]] = {}
        if awaited:

            async def main() -> int:
                return await parent(effects=effects)

            assert asyncio.run(main()) == 1
        else:
            assert parent(effects=effects) == 1
        return effects

    engine, _, parent = session()
    try:
        assert read(parent) == {"log": [("child", 1)]}
    finally:
        engine.shutdown()
    engine, a, parent = session()
    try:
        assert read(parent) == {"log": [("child", 1)]}
        assert runs == ["parent"]  # served from disk
    finally:
        engine.shutdown()
    engine, a, parent = session()
    try:
        a.set(2)
        assert read(parent) == {"log": [("child", 2)]}
        assert runs == ["parent", "parent"]
    finally:
        engine.shutdown()


def test_d57_threads_entering_a_cycle_of_async_queries_from_both_ends_do_not_hang() -> (
    None
):
    # Two worker threads of compute_many each ran an async query of one
    # fixed-point cycle from plain code: each thread blocks in an event loop
    # of its own while the task on it awaits the other thread's query. The
    # record of waits had the tasks' waits but not that each thread waits
    # for its task, so neither saw the cycle (G7) and both waited for ever.
    for _ in range(10):
        engine = Engine()

        @engine.input(name="limit")
        def limit() -> int:
            return 3

        @engine.query(name="head", fixed_point=0)
        async def head() -> int:
            return min(await member(), limit())

        @engine.query(name="member")
        async def member() -> int:
            return await head() + 1

        finished, outcome = _run_with_timeout(
            lambda: engine.compute_many([(head, ()), (member, ())])
        )
        try:
            assert finished, "compute_many hung"
            kind, value = outcome[0]
            # Entered through the member while the other thread iterates the
            # head: the fixed point or a CycleError (G7).
            assert (kind, value) == ("ok", [3, 4]) or isinstance(
                value, cascade.CycleError
            ), outcome
        finally:
            engine.shutdown(wait=False)


@pytest.mark.parametrize("cached", [True, False])
def test_d58_submitted_run_that_raises_after_an_input_change_is_cancelled(
    cached: bool,
) -> None:
    # A submitted call whose inputs change before it finishes raises
    # QueryCancelled and stores nothing (G6). A body that raised instead of
    # returning was not checked: the error of the stale run reached the
    # caller, and a cached one was stored.
    engine = Engine()
    entered, release = threading.Event(), threading.Event()
    runs: list[int] = []

    @engine.input(name="x")
    def x() -> int:
        return 0

    @engine.query(name="q", cache_exceptions=True if cached else (KeyError,))
    def q() -> int:
        value = x()
        runs.append(value)
        if len(runs) == 1:
            entered.set()
            release.wait(10)
        raise ValueError(value)

    future = engine.submit(q)
    try:
        assert entered.wait(10)
        x.set(1)
        release.set()
        with pytest.raises(QueryCancelled):
            future.result(10)
        with pytest.raises(ValueError) as raised:
            q()
        assert raised.value.args == (1,)
        assert runs == [0, 1]
    finally:
        release.set()
        engine.shutdown()


def _call_quietly(fn: Any) -> None:
    with contextlib.suppress(ValueError):
        fn()


@pytest.mark.parametrize("callers", ["tasks", "threads"])
def test_d56_caller_that_joined_a_run_failing_uncached_gets_its_effects(
    callers: str,
) -> None:
    # A query that raises keeps what it pushed, and a caller that catches
    # the error has it (G12). A caller that joined the run instead of
    # running it got the error without those effects when the error was
    # not cached (G5: waiters get the run's result, effects and exception).
    engine = Engine()
    log = engine.accumulator("log")

    if callers == "tasks":

        @engine.query(name="failing", cache_exceptions=(KeyError,))
        async def failing() -> int:
            log.push("failing")
            await gate.wait()
            raise ValueError("no")

        @engine.query(name="top")
        async def top() -> int:
            try:
                await failing()
            except ValueError:
                return 1
            return 0

        async def main() -> tuple[Any, dict[str, list[Any]]]:
            owner = asyncio.ensure_future(failing())
            await asyncio.sleep(0)  # the owner runs up to the gate
            effects: dict[str, list[Any]] = {}
            joined = asyncio.ensure_future(top(effects=effects))
            for _ in range(5):
                await asyncio.sleep(0)  # top joins the owner's run
            assert engine._store.waiting_on  # noqa: SLF001
            gate.set()
            with pytest.raises(ValueError):
                await owner
            return await joined, effects

        gate: asyncio.Event
        runner = asyncio.Runner()
        try:

            async def make_gate() -> asyncio.Event:
                return asyncio.Event()

            gate = runner.run(make_gate())
            value, effects = runner.run(main())
        finally:
            runner.close()
    else:
        entered, release = threading.Event(), threading.Event()

        @engine.query(name="failing", cache_exceptions=(KeyError,))
        def failing() -> int:
            log.push("failing")
            entered.set()
            release.wait(10)
            raise ValueError("no")

        @engine.query(name="top")
        def top() -> int:
            try:
                failing()
            except ValueError:
                return 1
            return 0

        owner = threading.Thread(target=_call_quietly, args=(failing,), daemon=True)
        owner.start()
        assert entered.wait(10)
        effects = {}
        result: list[Any] = []
        joined = threading.Thread(
            target=lambda: result.append(top(effects=effects)), daemon=True
        )
        joined.start()
        deadline = time.monotonic() + 10
        while not engine._store.waiting_on and time.monotonic() < deadline:  # noqa: SLF001
            time.sleep(0.001)
        release.set()
        owner.join(10)
        joined.join(10)
        value = result[0]
    assert value == 1
    assert effects == {"log": ["failing"]}
    engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_d59_result_that_read_a_default_kept_meanwhile_at_a_newer_revision_is_stored(
    tmp_path: Path, backend: str
) -> None:
    # A task at an older revision read an input's default (nothing is kept
    # there, G3); before it finished, a task at the newest revision read the
    # input and kept the default. Storing the first task's result then found
    # a version that did not match what it had read and stored nothing, so
    # a new engine ran it again (G15). What it read was the default: that is
    # what is recorded now.
    runs: list[str] = []

    def session() -> tuple[Engine, Any, Any, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.input(name="x")
        def x() -> int:
            return 7

        @engine.input(name="y")
        def y() -> int:
            return 0

        @engine.query(name="old")
        async def old() -> int:
            runs.append("old")
            value = x()
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            return value

        @engine.query(name="new")
        async def new() -> int:
            return x() + y()

        return engine, y, old, new

    engine, y, old, new = session()
    try:
        snapshot = engine.snapshot()
        y.set(1)

        async def main() -> list[int]:
            return list(await asyncio.gather(old(snapshot=snapshot), new()))

        assert asyncio.run(main()) == [7, 8]
        snapshot.release()
    finally:
        engine.shutdown()
    assert runs == ["old"]
    engine, _, old, _ = session()
    try:
        assert old() == 7
        assert runs == ["old"]  # served from disk
    finally:
        engine.shutdown()


@pytest.mark.parametrize("callers", ["tasks", "threads"])
def test_d56_caller_that_joined_a_run_failing_uncached_depends_on_what_it_read(
    callers: str,
) -> None:
    # A caller that catches an error which is not cached, or caches its
    # own error from it, still depends on what the failed query read (G8).
    # The owner's caller took that over; a caller that joined the run got
    # only the error, so it stored its own result with no dependency on
    # it and served it for ever, also after the input changed (G1, G5).
    engine = Engine()

    @engine.input(name="x")
    def x() -> int:
        return 0

    if callers == "tasks":

        @engine.query(name="failing", cache_exceptions=(KeyError,))
        async def failing() -> int:
            await asyncio.sleep(0)  # the other task joins this run here
            if x() == 0:
                raise ValueError("zero")
            return x()

        @engine.query(name="top")
        async def top() -> int:
            return await failing() + 1

        async def both() -> list[Any]:
            async def one(handle: Any) -> Any:
                try:
                    return await handle()
                except ValueError as exc:
                    return exc.args

            return list(await asyncio.gather(one(failing), one(top)))

        assert asyncio.run(both()) == [("zero",), ("zero",)]
        x.set(5)
        assert asyncio.run(both()) == [5, 6]
    else:
        entered, release = threading.Event(), threading.Event()

        @engine.query(name="failing", cache_exceptions=(KeyError,))
        def failing() -> int:
            if not entered.is_set():
                entered.set()
                release.wait(10)
            if x() == 0:
                raise ValueError("zero")
            return x()

        @engine.query(name="top")
        def top() -> int:
            return failing() + 1

        owner = threading.Thread(target=_call_quietly, args=(failing,), daemon=True)
        owner.start()
        assert entered.wait(10)
        outcome: list[Any] = []

        def joined() -> None:
            try:
                outcome.append(top())
            except ValueError as exc:
                outcome.append(exc.args)

        waiter = threading.Thread(target=joined, daemon=True)
        waiter.start()
        deadline = time.monotonic() + 10
        while not engine._store.waiting_on and time.monotonic() < deadline:  # noqa: SLF001
            time.sleep(0.001)
        release.set()
        owner.join(10)
        waiter.join(10)
        assert outcome == [("zero",)]
        x.set(5)
        assert top() == 6
    engine.shutdown()


def test_d52_cancelling_the_task_that_runs_a_query_leaves_its_waiters_to_run_it() -> (
    None
):
    # A task runs slow(); another task and a thread wait for that run. The
    # task that runs it is cancelled. That is its own business (G5): the
    # waiters used to get its asyncio.CancelledError as the outcome of the
    # run, so the other task ended as if it had been cancelled and the
    # thread's plain call raised CancelledError. They must take the run over
    # and get the value; one of them runs slow() again, the other waits.
    engine = Engine()
    runs: list[int] = []
    release = threading.Event()
    from_thread: list[Any] = []

    def waiting() -> int:
        with engine._internals.lock:  # noqa: SLF001
            return len(engine._store.waiting_on)  # noqa: SLF001

    async def poll(condition: Any) -> None:
        deadline = time.monotonic() + HANG_TIMEOUT
        while not condition():
            assert time.monotonic() < deadline, "timed out"
            await asyncio.sleep(0.002)

    @engine.query
    async def slow() -> int:
        runs.append(1)
        await poll(release.is_set)
        return 42

    def call_from_thread() -> None:
        try:
            from_thread.append(slow())
        except BaseException as exc:  # noqa: BLE001
            from_thread.append(exc)

    async def main() -> list[Any]:
        owner = asyncio.create_task(slow())
        await poll(lambda: runs)
        waiter = asyncio.create_task(slow())
        thread = threading.Thread(target=call_from_thread, daemon=True)
        thread.start()
        await poll(lambda: waiting() == 2)
        owner.cancel()
        cancelled = (await asyncio.gather(owner, return_exceptions=True))[0]
        release.set()
        other = (await asyncio.gather(waiter, return_exceptions=True))[0]
        await asyncio.to_thread(thread.join, HANG_TIMEOUT)
        return [cancelled, other]

    cancelled, other = asyncio.run(asyncio.wait_for(main(), 4 * HANG_TIMEOUT))
    assert isinstance(cancelled, asyncio.CancelledError)
    assert other == 42
    assert from_thread == [42]
    assert len(runs) == 2
    assert slow() == 42
    assert len(runs) == 2
    assert not engine._store.waiting_on  # noqa: SLF001


def test_d53_a_caller_that_joined_a_run_blocked_on_an_event_loop_runs_it_itself() -> (
    None
):
    # bridge() is a sync query calling the async aval(). Called from a
    # coroutine, on the thread of a running loop, its run cannot run aval()
    # and raises EventLoopBlockedError (G14). A thread that joined that run
    # used to get the same error, though it waits on a thread with no loop,
    # where aval() runs fine. It must run bridge() itself and get the value.
    engine = Engine()
    entered = threading.Event()
    release = threading.Event()
    from_thread: list[Any] = []

    @engine.query
    async def aval() -> int:
        return 5

    @engine.query
    def bridge() -> int:
        entered.set()
        assert release.wait(HANG_TIMEOUT)
        return aval() + 1

    def waiting() -> int:
        with engine._internals.lock:  # noqa: SLF001
            return len(engine._store.waiting_on)  # noqa: SLF001

    def call_from_thread() -> None:
        assert entered.wait(HANG_TIMEOUT)
        try:
            from_thread.append(bridge())
        except BaseException as exc:  # noqa: BLE001
            from_thread.append(exc)

    def release_once_joined() -> None:
        deadline = time.monotonic() + HANG_TIMEOUT
        while waiting() < 1 and time.monotonic() < deadline:
            time.sleep(0.002)
        release.set()

    thread = threading.Thread(target=call_from_thread, daemon=True)
    thread.start()
    threading.Thread(target=release_once_joined, daemon=True).start()

    async def on_the_loop() -> Any:
        try:
            return bridge()
        except cascade.EventLoopBlockedError as exc:
            return exc

    assert isinstance(asyncio.run(on_the_loop()), cascade.EventLoopBlockedError)
    thread.join(HANG_TIMEOUT)
    assert not thread.is_alive(), "the joined caller hung"
    assert from_thread == [6]
    assert bridge() == 6


# (reading before, reading after, the run that drops it, the fixed point)
_OUTSIDE_READ_CASES = [
    # From 0 with -2: 0, -2 (dropped here, now 0), then -1 from the guess
    # -2 used to be taken as converged on: -1 // 2 + 0 == -1.
    (-2, 0, 2, 0),
    # From 0 with 11: 11, 16 (dropped here, now 8): 16 // 2 + 8 == 16, the
    # guess, so that run used to count as converged though it read 8 and
    # the guess came from 11.
    (11, 8, 3, 15),
]


@pytest.mark.parametrize("awaited", [False, True])
@pytest.mark.parametrize(("before", "after", "on_run", "expected"), _OUTSIDE_READ_CASES)
def test_d54_a_fixed_point_whose_outside_read_changes_while_it_iterates_starts_again(
    awaited: bool, before: int, after: int, on_run: int, expected: int
) -> None:
    # head = member + reading and member = head // 2, iterated from the guess
    # 0. reading is a TTL query. Its result is dropped while the head
    # iterates (sweep_unaccessed) and runs again with a new value. The head
    # used to go on from the guesses computed with the old value and stop
    # at a fixed point of head = head // 2 + after that no run from the
    # guess 0 reaches. It must start again from the guess and give the
    # fixed point a run with the new value gives. Found by the state
    # machine (evict_while_a_head_runs, reading_dropped_while_a_head_iterates),
    # sync and async alike.
    engine = Engine()
    value = {"now": before}
    runs = {"head": 0}

    @engine.query(ttl=60)
    def reading() -> int:
        return value["now"]

    def drop_on_a_later_run() -> None:
        runs["head"] += 1
        if runs["head"] == on_run:
            engine.sweep_unaccessed(engine.access_id)
            value["now"] = after

    if awaited:

        @engine.query(fixed_point=0)
        async def head() -> int:
            drop_on_a_later_run()
            return (await member()) + reading()

        @engine.query
        async def member() -> int:
            return (await head()) // 2

        async def main() -> int:
            return await head()

        result = asyncio.run(main())
    else:

        @engine.query(fixed_point=0)
        def head() -> int:
            drop_on_a_later_run()
            return member() + reading()

        @engine.query
        def member() -> int:
            return head() // 2

        result = head()
    assert result == expected
    assert "outside read" in [t.detail for t in engine.traces()]


def test_d60_closures_made_in_a_comprehension_see_its_last_loop_value() -> None:
    # A comprehension's loop variable is one variable: lambdas and
    # generators made in its element and called afterwards all see its last
    # value. The rewriter moved the element into a helper function, where
    # the loop variable is a new variable for each element, so each closure
    # kept its own: [10, 20] where Python gives [20, 20], on the first run.
    # Found by test_rewrite_programs.py once it made closures in elements.
    engine = Engine()
    ys = CascadeList(engine, [10, 20])

    def body(source: Any) -> Any:
        fs = [lambda v: v + y for y in source]
        gs = [(v + y for v in (0, 1)) for y in source]
        return [f(0) for f in fs], [list(g) for g in gs]

    @engine.query
    def q() -> Any:
        fs = [lambda v: v + y for y in ys]
        gs = [(v + y for v in (0, 1)) for y in ys]
        return [f(0) for f in fs], [list(g) for g in gs]

    assert q() == body(list(ys)) == ([20, 20], [[20, 21], [20, 21]])
    ys.append(30)
    assert q() == body(list(ys))


def test_d61_a_comprehension_that_changes_a_local_list_does_so_for_every_element() -> (
    None
):
    # A pipeline runs its element, ``if``, ``map``, ``filter`` and ``key=``
    # functions for the elements that changed only.
    # ``[seen.append(x) for x in xs]`` appended every element on the first
    # run and, after ``xs.append(4)``, only ``4`` to the new list ``seen``:
    # [4] where Python gives [1, 2, 3, 4]. An expression whose functions call
    # a method that changes a list, set or dict the query assigns is now
    # left as written. A helper function that changes an object is not seen;
    # an accumulator is the way to collect effects.
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])

    @engine.query
    def q() -> Any:
        seen: list[int] = []
        [seen.append(x) for x in xs]
        found: set[int] = set()
        total = sum(x for x in xs if found.add(x) is None)
        mapped: list[int] = []
        doubled = list(map(lambda v: mapped.append(v) or 2 * v, xs))
        keys: dict[int, int] = {}
        ordered = sorted(xs, key=lambda v: keys.setdefault(v, -v))
        return seen, sorted(found), total, mapped, doubled, keys, ordered

    assert q() == (
        [1, 2, 3],
        [1, 2, 3],
        6,
        [1, 2, 3],
        [2, 4, 6],
        {1: -1, 2: -2, 3: -3},
        [3, 2, 1],
    )
    xs.append(4)
    assert q() == (
        [1, 2, 3, 4],
        [1, 2, 3, 4],
        10,
        [1, 2, 3, 4],
        [2, 4, 6, 8],
        {1: -1, 2: -2, 3: -3, 4: -4},
        [4, 3, 2, 1],
    )


def test_d55_a_caller_replays_the_effects_of_a_run_that_gave_the_same_value() -> None:
    # A query that ran again and gave the same value kept its old
    # ``changed_at`` (early cutoff), so a caller was not run again and
    # replayed the effects it had stored, which held the dependency's old
    # ones: ``p`` collected [1] after ``n`` became 2, where ``q`` itself
    # collected [2]. A result now keeps its ``changed_at`` only if its
    # effects are the same too. Found by test_rewrite_programs.py once it
    # collected effects through a query over two bodies.
    engine = Engine()
    log = engine.accumulator("log")
    runs: list[str] = []

    @engine.input
    def n() -> int:
        return 1

    @engine.query
    def q() -> int:
        log.push(n())
        return 0

    @engine.query
    def p() -> int:
        runs.append("p")
        return q()

    for value in (1, 2, 1.0, 1.0):
        n.set(value=value)
        mine: dict[str, list[Any]] = {}
        above: dict[str, list[Any]] = {}
        assert q(effects=mine) == 0
        assert p(effects=above) == 0
        assert repr(above) == repr(mine) == repr({"log": [value]})
    # Run again for each change of the effects (1.0 is not 1), not for the
    # last ``set``, which changed nothing.
    assert runs.count("p") == 3

    # The same for a cached error: a caller that catches it collects the
    # effects the failed run pushed.
    @engine.query
    def fails() -> int:
        log.push(n())
        raise ValueError("no")

    @engine.query
    def catches() -> str:
        try:
            fails()
        except ValueError:
            return "caught"
        return "not caught"  # pragma: no cover

    for value in (3, 4):
        n.set(value=value)
        above = {}
        assert catches(effects=above) == "caught"
        assert above == {"log": [value]}


def test_d62_a_value_made_by_a_stage_is_new_on_every_run() -> None:
    # A pipeline keeps the values its stages made and hands the same objects
    # out on the next run. A body that changed one, or used one up, found it
    # so on the next run, where Python makes a new one: after
    # ``xs.append(3)``, ``rows[0]`` was [1, 99, 99] and the second generator
    # was empty. A pipeline whose stages make a value that can be changed in
    # place (anything but numbers, strings, bytes, None, and tuples and
    # frozensets of those) is now rebuilt on every run. Found by
    # test_rewrite_programs.py on 3.14t, with generators made in elements.
    engine = Engine()
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def q() -> Any:
        rows = [[x] for x in xs]
        rows[0].append(99)
        gens = [(v for v in (1, 2)) for x in xs]
        return rows, [list(g) for g in gens], max([x] for x in xs)

    assert q() == ([[1, 99], [2]], [[1, 2], [1, 2]], [2])
    xs.append(3)
    assert q() == ([[1, 99], [2], [3]], [[1, 2], [1, 2], [1, 2]], [3])


def test_d63_a_nan_made_by_a_stage_is_a_new_object_each_time() -> None:
    # Python makes a new NaN object for each ``v + 1`` over a NaN, and a set
    # tells NaN objects apart. A pipeline called twice in one run handed out
    # the NaN its state held both times: ``{min(map(f, xs)) for _ in (0, 1)}``
    # held one NaN where Python's holds two. A stage that makes a NaN now
    # makes the pipeline rebuild on every call, as a captured NaN does (D46).
    # Found by the soak of test_rewrite_programs.py on 3.14t (15,000
    # examples), behind the 3.14 message difference of the plain set model.
    engine = Engine()
    xs = CascadeList(engine, [float("nan")])

    @engine.query
    def q() -> Any:
        return len({min(map(lambda v: v + 1, xs)) for _ in (0, 1)})

    assert getattr(q.raw, "__cascade_rewritten__", False)
    assert q() == 2
    xs.append(1.0)
    assert q() == 2


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("callers", ["threads", "tasks"])
def test_d64_head_whose_member_another_caller_finished_keeps_its_last_runs_effects(
    tmp_path: Path, backend: str, callers: str
) -> None:
    # Caller A runs the head and waits for the member, which caller B runs.
    # B, checking the member's stored result, reaches the head, finds A
    # waiting for it, iterates the cycle on its own stack (G7) and stores
    # head and member. A then took B's member result: the head stored an
    # ordinary result that depends on its member, with the member's
    # effects, which replay the head's last run, before its own push. Every
    # reader got that run twice where a fresh run gives it once (G12).
    # Found by test_async_differential.py, three compute_many threads on
    # sqlite. B is held inside the disk check here so that A waits first.
    from types import SimpleNamespace

    from cascade import _evaluator

    asynchronous = callers == "tasks"
    ns = SimpleNamespace()

    def limit() -> int:
        return 2

    if asynchronous:

        async def head() -> int:
            result = min(await ns.member(), ns.limit())
            ns.log.push(("head", result))
            return result

        async def member() -> int:
            result = await ns.head() + 1
            ns.log.push(("member", result))
            return result

    else:

        def head() -> int:
            result = min(ns.member(), ns.limit())
            ns.log.push(("head", result))
            return result

        def member() -> int:
            result = ns.head() + 1
            ns.log.push(("member", result))
            return result

    def session() -> Engine:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)
        ns.log = engine.accumulator("log")
        ns.limit = engine.input(name="limit")(limit)
        ns.head = engine.query(name="head", fixed_point=0)(head)
        ns.member = engine.query(name="member")(member)
        return engine

    def call(handle: Any, effects: dict[str, list[Any]] | None = None) -> Any:
        if asynchronous:

            async def main() -> Any:
                return await handle(effects=effects)

            return asyncio.run(main())
        return handle(effects=effects)

    engine = session()
    call(ns.head)
    engine.shutdown()
    engine = session()
    call(ns.head)  # loaded from disk, without its member
    call(ns.member)  # so the member runs alone, stored as reading the head
    engine.shutdown()

    engine = session()
    ns.limit.set(1)  # the stored head no longer matches
    held = threading.Event()
    go = threading.Event()
    check, check_async = (
        _evaluator.Evaluator._verify_disk_deps,
        _evaluator.Evaluator._verify_disk_deps_async,
    )

    def hold(key: Any) -> bool:
        if key[1] == "member" and not held.is_set():
            held.set()
            return True
        return False

    def held_check(self: Any, key: Any, record: Any, runtime: Any) -> Any:
        if hold(key):
            go.wait(10)
        return check(self, key, record, runtime)

    async def held_check_async(self: Any, key: Any, record: Any, runtime: Any) -> Any:
        if hold(key):
            while not go.is_set():
                await asyncio.sleep(0.001)
        return await check_async(self, key, record, runtime)

    def a_waits() -> None:
        deadline = time.monotonic() + 10
        while not engine._store.waiting_on and time.monotonic() < deadline:  # noqa: SLF001
            time.sleep(0.001)
        go.set()

    _evaluator.Evaluator._verify_disk_deps = held_check  # type: ignore[method-assign]
    _evaluator.Evaluator._verify_disk_deps_async = held_check_async  # type: ignore[method-assign]
    try:
        if asynchronous:

            async def both() -> list[Any]:
                b = asyncio.ensure_future(ns.member())
                while not held.is_set():
                    await asyncio.sleep(0)
                a = asyncio.ensure_future(ns.head())
                releaser = threading.Thread(target=a_waits, daemon=True)
                releaser.start()
                return list(await asyncio.gather(a, b))

            values = asyncio.run(both())
        else:
            out: dict[str, Any] = {}
            b = threading.Thread(target=lambda: out.setdefault("member", ns.member()))
            b.start()
            assert held.wait(10)
            a = threading.Thread(target=lambda: out.setdefault("head", ns.head()))
            a.start()
            a_waits()
            a.join(10)
            b.join(10)
            values = [out.get("head"), out.get("member")]
    finally:
        _evaluator.Evaluator._verify_disk_deps = check  # type: ignore[method-assign]
        _evaluator.Evaluator._verify_disk_deps_async = check_async  # type: ignore[method-assign]
    try:
        assert go.is_set() and values == [1, 2]
        effects: dict[str, list[Any]] = {}
        assert call(ns.head, effects) == 1
        assert effects == {"log": [("member", 2), ("head", 1)]}
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("awaited", [False, True])
def test_d64_head_rerun_at_a_snapshot_does_not_serve_its_member_run_alone(
    tmp_path: Path, backend: str, awaited: bool
) -> None:
    # One thread, the same mistake as above. The member, read through a
    # snapshot, ran alone on the head loaded from disk; a read at the
    # newest revision then kept the head's row there, so a read through
    # the snapshot ran the head again. Its run called the member, whose
    # row was current at the snapshot, and served it: the head's run did
    # not reach itself, stored an ordinary result depending on the member,
    # and replayed the member's effects, which hold the head's last run,
    # before its own push (G12). A head's run now runs every query that
    # depends on that head again, as a member, on its guess.
    from types import SimpleNamespace

    ns = SimpleNamespace()

    def step() -> int:
        return 1

    if awaited:

        async def head() -> int:
            result = min(await ns.member(), 2)
            ns.log.push(("head", result))
            return result

        async def member() -> int:
            result = await ns.head() + ns.step()
            ns.log.push(("member", result))
            return result

        async def top() -> int:
            return await ns.head()

    else:

        def head() -> int:
            result = min(ns.member(), 2)
            ns.log.push(("head", result))
            return result

        def member() -> int:
            result = ns.head() + ns.step()
            ns.log.push(("member", result))
            return result

        def top() -> int:
            return ns.head()

    def session() -> Engine:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)
        ns.log = engine.accumulator("log")
        ns.step = engine.input(name="step")(step)
        ns.head = engine.query(name="head", fixed_point=0)(head)
        ns.member = engine.query(name="member")(member)
        ns.top = engine.query(name="top")(top)
        return engine

    def read(handle: Any, snapshot: Any = None) -> tuple[Any, Any]:
        effects: dict[str, list[Any]] = {}
        if awaited:

            async def main() -> Any:
                return await handle(snapshot=snapshot, effects=effects)

            value = asyncio.run(main())
        else:
            value = handle(snapshot=snapshot, effects=effects)
        return value, effects["log"]

    engine = session()
    read(ns.head)
    engine.shutdown()
    engine = session()
    try:
        snapshot = engine.snapshot()
        ns.step.set(1)  # its default: a new version with the same value
        assert read(ns.member, snapshot)[0] == 3
        assert read(ns.head) == (2, [("member", 3), ("head", 2)])
        assert read(ns.top, snapshot) == (2, [("member", 3), ("head", 2)])
        snapshot.release()
    finally:
        engine.shutdown()
