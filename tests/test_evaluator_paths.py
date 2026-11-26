"""Evaluator paths the rest of the suite does not reach.

Mostly the async evaluator: bringing a stale result up to date (the
verification walk) when it meets a query that is running, a provisional
result or a loop of stored results; cycles entered through a member; joined
runs that end on a guess; and loading results from disk whose dependencies
fail, cycle, are cancelled or are no longer registered. Each test checks the
value against what the graph gives when computed from scratch (a fresh engine
or plain Python), and the run counts or the error where they matter.

Threads and tasks are held with events and polling, never with sleeps that
hope for an ordering; every join has a timeout.
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from cascade import CycleError, Engine, MutatedValueError, QueryCancelled
from cascade._runtime import RuntimeFrame, RuntimeState

TIMEOUT = 10.0


def _waiters(engine: Engine) -> int:
    with engine._internals.lock:  # noqa: SLF001
        return len(engine._store.waiting_on)  # noqa: SLF001


def _until(predicate: Any) -> None:
    deadline = time.monotonic() + TIMEOUT
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.002)


def _arun(handle: Any, *args: Any) -> Any:
    """Await ``handle(*args)`` on a new event loop (the async path)."""

    async def main() -> Any:
        return await handle(*args)

    return asyncio.run(asyncio.wait_for(main(), TIMEOUT))


async def _until_async(predicate: Any) -> None:
    deadline = time.monotonic() + TIMEOUT
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        await asyncio.sleep(0.002)


def _red_details(engine: Engine) -> list[str]:
    return [t.detail for t in engine.traces() if t.event == "cache_red"]


def _memo_keys(engine: Engine) -> set[Any]:
    with engine._internals.lock:  # noqa: SLF001
        return set(engine._internals.memos)  # noqa: SLF001


# --- async verification meets a query that is running ----------------------


def _running_head_graph(engine: Engine, runs: Counter[str]) -> Any:
    """r() = min(x(), 10) once ``flag`` is set, x() = d() + 1, d() = r() + 1."""

    @engine.input
    def flag() -> bool:
        return False

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    async def r() -> int:
        runs["r"] += 1
        if flag():
            return min(await x(), 10)
        return inp()

    @engine.query
    async def x() -> int:
        runs["x"] += 1
        return await d() + 1

    @engine.query
    async def d() -> int:
        runs["d"] += 1
        return await r() + 1

    return flag, r, x, d


def _plain_running_head() -> tuple[int, int, int]:
    guess = 0
    while True:
        d = guess + 1
        x = d + 1
        r = min(x, 10)
        if r == guess:
            return r, x, d
        guess = r


def test_async_verification_reaching_the_running_head_recomputes_instead() -> None:
    # x() and d() were stored while r() did not call x(). Now r() does: its
    # run asks for x(), whose verification walks to d() and from there to
    # r(), which is running. The walk from d()'s shadow runtime cannot tell
    # (the error is not its own and goes up); x()'s verification, in r()'s
    # runtime, owns r() and counts the dependency as changed. x() and d()
    # then run inside the cycle, where d()'s own check meets r() on the stack.
    engine = Engine()
    runs: Counter[str] = Counter()
    flag, r, x, d = _running_head_graph(engine, runs)

    assert _arun(x) == 3  # r() = 1, d() = 2
    flag.set(True)
    engine.clear_traces()

    async def after() -> tuple[int, int, int]:
        return await r(), await x(), await d()

    assert asyncio.run(after()) == _plain_running_head() == (10, 12, 11)
    details = _red_details(engine)
    assert "dependency is running" in details
    # d()'s check inside the cycle found r() on the stack: a changed dependency.
    assert any(r.id in detail for detail in details)

    fresh_runs: Counter[str] = Counter()
    fresh = Engine()
    fresh_flag, fresh_r, fresh_x, fresh_d = _running_head_graph(fresh, fresh_runs)
    fresh_flag.set(True)

    async def fresh_after() -> tuple[int, int, int]:
        return await fresh_r(), await fresh_x(), await fresh_d()

    assert asyncio.run(fresh_after()) == (10, 12, 11)


def test_async_verification_of_a_result_that_reads_a_provisional_member() -> None:
    # helper() was stored outside the cycle and reads member(). When head()
    # runs again, member() holds a provisional result in head()'s runtime;
    # verifying helper() against it would compare with a guess, so helper()
    # runs instead, on each iteration.
    def build(engine: Engine, runs: Counter[str]) -> Any:
        @engine.input
        def base() -> int:
            return 1

        @engine.query(fixed_point=0)
        async def head() -> int:
            runs["head"] += 1
            b = base()
            m = await member()
            if b > 1:
                await helper()
            return b if m == b else min(b, m + 1)

        @engine.query
        async def member() -> int:
            return await head()

        @engine.query
        async def helper() -> int:
            runs["helper"] += 1
            return await member() + 100

        return base, head, member, helper

    runs: Counter[str] = Counter()
    engine = Engine()
    base, head, member, helper = build(engine, runs)

    async def read() -> tuple[int, int, int]:
        return await head(), await member(), await helper()

    assert asyncio.run(read()) == (1, 1, 101)
    base.set(2)
    engine.clear_traces()
    runs.clear()
    assert asyncio.run(read()) == (2, 2, 102)
    # Three iterations of head() (guesses 0, 1, 2), helper() run in each.
    assert runs == {"head": 3, "helper": 3}
    # helper()'s check counted member() as changed: its result there was
    # provisional, nothing to compare with.
    assert any(member.id in detail for detail in _red_details(engine))

    fresh_runs: Counter[str] = Counter()
    fresh = Engine()
    fresh_base, fresh_head, fresh_member, fresh_helper = build(fresh, fresh_runs)
    fresh_base.set(2)

    async def fresh_read() -> tuple[int, int, int]:
        return await fresh_head(), await fresh_member(), await fresh_helper()

    assert asyncio.run(fresh_read()) == (2, 2, 102)


def test_async_verification_of_stored_results_that_depend_on_each_other() -> None:
    # As test_cross_thread_cycles.py::test_verifying_stored_results_that_
    # depend_on_each_other_in_a_loop, on the async path: the walk that
    # brings m2() up to date comes back to head() while verifying it, and
    # must count it as changed rather than recurse.
    from cascade._state import Dependency

    engine = Engine()

    @engine.input
    def base() -> int:
        return 4

    @engine.query(fixed_point=0)
    async def head() -> int:
        return await m1() // 2 + base()

    @engine.query
    async def m1() -> int:
        return await m2() + 1

    @engine.query
    async def m2() -> int:
        return await head()

    assert _arun(head) == 8
    memos = engine._internals.memos  # noqa: SLF001
    head_key, m1_key = ("query", head.id, ()), ("query", m1.id, ())
    with engine._internals.lock:  # noqa: SLF001
        stored = memos[head_key]
        stored.deps = (Dependency(m1_key, memos[m1_key].changed_at), *stored.deps)
        stored.cycle_nodes = ()
        engine._store.dependents[m1_key].add(head_key)  # noqa: SLF001
    base.set(5)

    async def read() -> tuple[int, int, int]:
        return await head(), await m1(), await m2()

    # head = (head + 1) // 2 + 5 converges to 10 from the guess 0.
    assert _arun(m2) == 10
    assert asyncio.run(read()) == (10, 11, 10)


def test_cancellation_during_an_async_verification_walk_is_raised() -> None:
    # A submitted async query asks for x(), whose dependency d() has to run
    # again to verify it. An input changes while d() runs: the submitted call
    # is cancelled, and the cancellation comes out of the verification as it
    # is, instead of counting as "dependency raised" and running x().
    engine = Engine()
    runs: Counter[str] = Counter()
    state = {"hold": False}
    started = threading.Event()
    proceed = threading.Event()

    @engine.input
    def inp() -> int:
        return 1

    @engine.input
    def other() -> int:
        return 0

    @engine.query
    async def d() -> int:
        runs["d"] += 1
        if state["hold"]:
            started.set()
            assert proceed.wait(TIMEOUT)
        return inp() * 2

    @engine.query
    async def x() -> int:
        runs["x"] += 1
        return await d() + 1

    @engine.query
    async def driver() -> int:
        return await x()

    try:
        assert _arun(x) == 3
        inp.set(2)
        state["hold"] = True
        future = engine.submit(driver)
        assert started.wait(TIMEOUT)
        other.set(1)
        proceed.set()
        with pytest.raises(QueryCancelled):
            future.result(TIMEOUT)
        assert runs == {"d": 2, "x": 1}  # x() itself never ran
        state["hold"] = False
        assert _arun(x) == 5
        assert runs == {"d": 3, "x": 2}
    finally:
        proceed.set()
        engine.shutdown()


# --- fan-out from a query to an async query that calls back ----------------


def test_fan_out_to_an_async_query_that_calls_back_into_the_fanning_query() -> None:
    # a() fans out to b(0) and b(1), which await a(). a() is async, so it
    # fans out from a thread (``asyncio.to_thread``), which carries its
    # runtime. The worker threads that warm b() must not wait for a(): a()
    # waits for them. They refuse on the async path (b's body awaits a())
    # with CycleError, which the warm-up drops; a()'s own calls of b() then
    # meet the cycle on a()'s stack, where a() is a fixed-point head.
    engine = Engine()
    runs: Counter[str] = Counter()
    refused: list[str] = []

    @engine.query
    async def b(i: int) -> int:
        runs[f"b{i}"] += 1
        try:
            return await a() + i
        except CycleError as exc:
            refused.append(str(exc))
            raise

    @engine.query(fixed_point=0)
    async def a() -> int:
        runs["a"] += 1
        values = await asyncio.to_thread(engine.compute_many, [(b, (0,)), (b, (1,))])
        return min(values[0] + 1, 3)

    # a = min(a + 1, 3) from the guess 0: 1, 2, 3, 3.
    assert _arun(a) == 3
    assert runs["a"] == 4
    # Every refusal is a worker's, naming the fan-out; none reached a().
    assert refused
    assert all("fans out to a query that depends on it" in m for m in refused)
    assert "dedup_wait" not in [t.event for t in engine.traces()]

    async def read() -> tuple[int, int, int]:
        return await a(), await b(0), await b(1)

    assert asyncio.run(read()) == (3, 3, 4)
    assert runs["a"] == 4


# --- entering a cycle through a member, async -----------------------------


def _member_first_graph(engine: Engine, fail_calls: list[int]) -> Any:
    @engine.input
    def mode() -> str:
        return "ok"

    @engine.query(fixed_point=0, max_iterations=2, cache_exceptions=False)
    async def head() -> int:
        m = mode()
        if m == "diverge":
            return await member() + 1
        if m == "fail":
            fail_calls[0] += 1
            # The first run reaches member() again, which starts over through
            # head(); that second run fails.
            if fail_calls[0] >= 2:
                raise RuntimeError("head boom")
        return await member()

    @engine.query(cache_exceptions=False)
    async def member() -> int:
        return await head()

    return mode, head, member


def test_async_member_entering_a_head_that_does_not_converge() -> None:
    engine = Engine()
    fail_calls = [0]
    mode, head, member = _member_first_graph(engine, fail_calls)

    async def both() -> tuple[int, int]:
        return await head(), await member()

    assert asyncio.run(both()) == (0, 0)
    engine.prune([], vacuum_disk=False)
    mode.set("diverge")
    # member() starts over through head(), whose run hits the iteration cap:
    # that is swallowed there and met again by member()'s own run.
    with pytest.raises(CycleError):
        _arun(member)
    assert not _memo_keys(engine) & {("query", member.id, ()), ("query", head.id, ())}
    # Never cached: back on a converging cycle, the same call works.
    mode.set("ok")
    assert _arun(member) == 0


def test_async_member_entering_a_head_that_raises_an_uncached_error() -> None:
    engine = Engine()
    fail_calls = [0]
    mode, head, member = _member_first_graph(engine, fail_calls)

    async def both() -> tuple[int, int]:
        return await head(), await member()

    assert asyncio.run(both()) == (0, 0)
    engine.prune([], vacuum_disk=False)
    mode.set("fail")
    with pytest.raises(RuntimeError, match="head boom"):
        _arun(member)
    # Once from member()'s first run, once entering through head() (dropped
    # there), once in member()'s second run.
    assert fail_calls[0] == 3
    mode.set("ok")
    assert asyncio.run(both()) == (0, 0)


def test_async_member_entering_a_head_is_cancelled_with_the_submitted_call() -> None:
    engine = Engine()
    started = threading.Event()
    proceed = threading.Event()
    calls_at_2 = [0]

    @engine.input
    def x() -> int:
        return 1

    @engine.query(fixed_point=0)
    async def head() -> int:
        val = x()
        if val == 2:
            calls_at_2[0] += 1
            # The first run reaches member() again; the second is the one
            # entering through head(), held until the inputs change.
            if calls_at_2[0] >= 2:
                started.set()
                assert proceed.wait(TIMEOUT)
            return await member()
        return await member() if val == 1 else val

    @engine.query
    async def member() -> int:
        return await head()

    @engine.query
    async def driver() -> int:
        return await member()

    try:
        assert _arun(head) == 0
        assert _arun(member) == 0
        engine.prune([], vacuum_disk=False)
        x.set(2)
        future = engine.submit(driver)
        assert started.wait(TIMEOUT)
        x.set(3)
        proceed.set()
        with pytest.raises(QueryCancelled):
            future.result(TIMEOUT)
        assert calls_at_2[0] == 2  # not run a third time after the cancellation
        # head() returns 3 without the cycle; member() follows it.
        assert _arun(member) == 3
    finally:
        proceed.set()
        engine.shutdown()


def test_async_enter_cycle_through_head_it_already_runs_does_nothing() -> None:
    # Not reachable through the public API: the member's run that reached
    # itself through head() has unwound head() by the time it starts over,
    # and head() cannot be on the stack twice. So called directly, as
    # test_evaluator_edge_coverage.py does for the sync twin: a runtime that
    # runs head() itself must not start another run of it.
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.query(fixed_point=0)
    async def head() -> int:
        runs["head"] += 1
        return 1

    head_key = ("query", head.id, ())
    runtime = RuntimeState(
        snapshot=engine.snapshot(),
        stack=[RuntimeFrame(key=head_key)],
        root_effects=None,
        staged_root_effects={},
        cancel_epoch=None,
        snapshot_explicit=False,
        is_async=True,
    )
    evaluator = engine._evaluator  # noqa: SLF001
    entered = asyncio.run(evaluator._enter_cycle_through_head_async(head_key, runtime))  # noqa: SLF001
    assert entered is False
    assert runs == {}
    assert head_key not in _memo_keys(engine)


# --- a task joins a run that ends on a guess -------------------------------


def test_task_joining_a_provisional_run_whose_head_then_fails() -> None:
    # The first task iterates head(); the second joins its run of member(),
    # which ends on head()'s guess. The second then asks for head() itself;
    # head()'s next run fails with an error that is not cached. That failure
    # is left to the second task's own run of member(), which meets it too.
    engine = Engine()
    state = {"fail": False}
    runs: Counter[str] = Counter()

    @engine.query(fixed_point=0, cache_exceptions=False)
    async def head() -> int:
        runs["head"] += 1
        if state["fail"]:
            raise ValueError("head failed")
        val = await member()
        state["fail"] = True
        return val + 1

    @engine.query(cache_exceptions=False)
    async def member() -> int:
        runs["member"] += 1
        if runs["member"] == 1:
            await _until_async(lambda: _waiters(engine) >= 1)
        return await head()

    async def main() -> list[Any]:
        first = asyncio.create_task(head())
        await _until_async(lambda: runs["member"] == 1)
        second = asyncio.create_task(member())
        return await asyncio.gather(first, second, return_exceptions=True)

    first, second = asyncio.run(asyncio.wait_for(main(), TIMEOUT))
    assert isinstance(first, ValueError)
    assert isinstance(second, ValueError)
    assert str(second) == "head failed"
    assert not engine._internals.in_flight  # noqa: SLF001
    assert _waiters(engine) == 0
    assert not _memo_keys(engine)


def test_task_joining_a_provisional_run_is_cancelled_with_the_submitted_call() -> None:
    # The same inside a submitted call whose inputs change while head()
    # iterates: the second task, asking for head() after its joined run
    # ended on a guess, raises the cancellation instead of dropping it.
    engine = Engine()
    runs: Counter[str] = Counter()
    second_run = threading.Event()
    proceed = threading.Event()
    outcomes: dict[str, Any] = {}

    @engine.input
    def tick() -> int:
        return 0

    @engine.query(fixed_point=0)
    async def head() -> int:
        runs["head"] += 1
        val = await member()
        if runs["head"] == 2:
            second_run.set()
            assert proceed.wait(TIMEOUT)
        return min(val + 1, 3)

    @engine.query
    async def member() -> int:
        runs["member"] += 1
        if runs["member"] == 1:
            await _until_async(lambda: _waiters(engine) >= 1)
        return await head()

    @engine.query
    async def driver() -> int:
        first = asyncio.create_task(head())
        await _until_async(lambda: runs["member"] == 1)
        second = asyncio.create_task(member())
        outcomes["tasks"] = await asyncio.gather(first, second, return_exceptions=True)
        return 0

    try:
        future = engine.submit(driver)
        assert second_run.wait(TIMEOUT)
        tick.set(1)
        proceed.set()
        with pytest.raises(QueryCancelled):
            future.result(TIMEOUT)
        first, second = outcomes["tasks"]
        assert isinstance(first, QueryCancelled)
        assert isinstance(second, QueryCancelled)
        assert runs["head"] == 2  # the second task ran nothing of head()
        assert not engine._internals.in_flight  # noqa: SLF001
        # head = min(head + 1, 3) converges to 3.
        assert _arun(head) == 3
    finally:
        proceed.set()
        engine.shutdown()


# --- a fixed-point head whose run ends in a cached error, async ------------


def _cached_error_head(engine: Engine, runs: Counter[str], is_async: bool) -> Any:
    if is_async:

        @engine.query(fixed_point=0)
        async def head() -> int:
            runs["head"] += 1
            m = await member()
            if m >= 2:
                raise ValueError(f"too big: {m}")
            return m + 1

        @engine.query
        async def member() -> int:
            runs["member"] += 1
            return await head()

    else:

        @engine.query(fixed_point=0)
        def head() -> int:
            runs["head"] += 1
            m = member()
            if m >= 2:
                raise ValueError(f"too big: {m}")
            return m + 1

        @engine.query
        def member() -> int:
            runs["member"] += 1
            return head()

    return head, member


@pytest.mark.parametrize("is_async", [True, False], ids=["async", "sync"])
def test_fixed_point_head_that_reads_its_guess_and_raises_a_cached_error(
    is_async: bool,
) -> None:
    # Guesses 0, 1, 2: the third run reads 2 and raises. The error is cached
    # as head()'s result and ends the iteration; member()'s results, computed
    # from guesses, are not stored. Async and sync agree.
    engine = Engine()
    runs: Counter[str] = Counter()
    head, member = _cached_error_head(engine, runs, is_async)

    def call(handle: Any) -> Any:
        if is_async:
            return _arun(handle)
        return handle()

    with pytest.raises(ValueError, match="too big: 2") as first:
        call(head)
    assert runs == {"head": 3, "member": 3}
    assert ("query", member.id, ()) not in _memo_keys(engine)
    with pytest.raises(ValueError, match="too big: 2") as again:
        call(head)
    assert again.value is first.value  # the cached error, not a new run
    assert runs == {"head": 3, "member": 3}
    # member() reads head()'s cached error.
    with pytest.raises(ValueError, match="too big: 2"):
        call(member)
    assert runs == {"head": 3, "member": 4}


# --- a fixed-point head that changed a cached value in place ---------------


def test_fixed_point_head_that_mutates_a_cached_value_fails_and_stores_nothing() -> (
    None
):
    engine = Engine(check_mutations=True)
    runs: Counter[str] = Counter()
    state = {"mutate": True}

    @engine.query
    def rows() -> list[int]:
        runs["rows"] += 1
        return [1, 2]

    @engine.query(fixed_point=0)
    def head() -> int:
        runs["head"] += 1
        found = rows()
        if state["mutate"]:
            found.append(0)
        return min(member() + 1, 2)

    @engine.query
    def member() -> int:
        return head()

    rows_key = ("query", rows.id, ())
    with pytest.raises(MutatedValueError) as info:
        head()
    assert info.value.keys == (rows_key,)
    # Neither the head nor the members computed from its guesses are stored,
    # and the changed result was dropped.
    assert not _memo_keys(engine) & {
        rows_key,
        ("query", head.id, ()),
        ("query", member.id, ()),
    }
    assert runs == {"rows": 1, "head": 3}  # guesses 0, 1, 2

    state["mutate"] = False
    assert (head(), member(), rows()) == (2, 2, [1, 2])
    assert runs["rows"] == 2  # computed again after being dropped


# --- disk records whose dependencies are gone or fail ----------------------


class _Source:
    """Calls a handle the query's code hash does not see (``obj.method()``)."""

    def __init__(self, handle: Any) -> None:
        self.handle = handle

    def read(self) -> Any:
        return self.handle()


def test_stored_result_whose_input_is_no_longer_registered_runs_again(
    tmp_path: Path,
) -> None:
    # The stored reader() depends on input "a". The next session reads "b"
    # through the same code and registers no "a": the record cannot be
    # verified, so reader() runs instead of returning the stale 10.
    runs: Counter[str] = Counter()

    def session(name: str, default: int) -> int:
        engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
        try:
            source = _Source(None)

            @engine.input(name=name)
            def value() -> int:
                return default

            source.handle = value

            @engine.query(name="reader")
            def reader() -> int:
                runs["reader"] += 1
                return source.read() * 10

            return reader()
        finally:
            engine.shutdown()

    assert session("a", 1) == 10
    assert session("a", 1) == 10
    assert runs["reader"] == 1  # loaded from disk the second time
    assert session("b", 2) == 20
    assert runs["reader"] == 2


@pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
def test_stored_result_whose_query_is_no_longer_registered_runs_again(
    tmp_path: Path, is_async: bool
) -> None:
    runs: Counter[str] = Counter()

    def session(name: str, base: int) -> int:
        engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
        try:
            source = _Source(None)
            if is_async:

                @engine.query(name=name)
                async def helper() -> int:
                    return base

                @engine.query(name="reader")
                async def reader() -> int:
                    runs["reader"] += 1
                    return await source.read() * 10

                source.handle = helper
                return _arun(reader)

            @engine.query(name=name)
            def sync_helper() -> int:
                return base

            @engine.query(name="reader")
            def sync_reader() -> int:
                runs["reader"] += 1
                return source.read() * 10

            source.handle = sync_helper
            return sync_reader()
        finally:
            engine.shutdown()

    assert session("helper_a", 1) == 10
    assert session("helper_a", 1) == 10
    assert runs["reader"] == 1
    assert session("helper_b", 2) == 20
    assert runs["reader"] == 2


def test_async_load_from_disk_whose_dependency_now_raises(tmp_path: Path) -> None:
    runs: Counter[str] = Counter()

    def session(value: int | None) -> Any:
        engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
        try:

            @engine.input
            def inp() -> int:
                return 1

            @engine.query(cache_exceptions=False)
            async def dep() -> int:
                runs["dep"] += 1
                v = inp()
                if v < 0:
                    raise ValueError(f"negative: {v}")
                return v * 2

            @engine.query
            async def top() -> int:
                runs["top"] += 1
                return await dep() + 1

            if value is not None:
                inp.set(value)
            try:
                return _arun(top)
            except ValueError as exc:
                return exc
        finally:
            engine.shutdown()

    assert session(None) == 3
    assert session(None) == 3
    assert runs == {"dep": 1, "top": 1}
    failed = session(-1)
    assert isinstance(failed, ValueError)
    assert str(failed) == "negative: -1"
    assert runs == {"dep": 3, "top": 2}  # dep() checked, then run by top()


def test_async_load_from_disk_whose_dependency_now_cycles(tmp_path: Path) -> None:
    # top() catches the CycleError of d(). Loading top() from disk has to
    # bring d() up to date, which now raises CycleError: the record does not
    # match, and top() runs and returns its fallback, as on a fresh engine.
    runs: Counter[str] = Counter()

    def session(cyclic: bool, cache_dir: Path) -> int:
        engine = Engine(cache_dir=cache_dir, cache_backend="sqlite")
        try:

            @engine.input
            def flag() -> bool:
                return False

            @engine.query
            async def d() -> int:
                if flag():
                    return await d2()
                return 1

            @engine.query
            async def d2() -> int:
                return await d()

            @engine.query
            async def top() -> int:
                runs["top"] += 1
                try:
                    return await d()
                except CycleError:
                    return -1

            if cyclic:
                flag.set(True)
            return _arun(top)
        finally:
            engine.shutdown()

    assert session(False, tmp_path / "a") == 1
    assert session(False, tmp_path / "a") == 1
    assert runs["top"] == 1
    assert session(True, tmp_path / "a") == -1
    assert runs["top"] == 2
    assert session(True, tmp_path / "fresh") == -1


def test_async_load_from_disk_cancelled_while_checking_a_dependency(
    tmp_path: Path,
) -> None:
    runs: Counter[str] = Counter()
    started = threading.Event()
    proceed = threading.Event()
    state = {"hold": False}

    def make(engine: Engine) -> Any:
        @engine.input
        def inp() -> int:
            return 1

        @engine.input
        def other() -> int:
            return 0

        @engine.query
        async def dep() -> int:
            runs["dep"] += 1
            if state["hold"]:
                started.set()
                assert proceed.wait(TIMEOUT)
            return inp() * 2

        @engine.query
        async def top() -> int:
            runs["top"] += 1
            return await dep() + 1

        @engine.query
        async def driver() -> int:
            return await top()

        return inp, other, top, driver

    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
    try:
        _, _, top, _ = make(engine)
        assert _arun(top) == 3
    finally:
        engine.shutdown()

    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
    try:
        inp, other, top, driver = make(engine)
        inp.set(5)
        state["hold"] = True
        future = engine.submit(driver)
        assert started.wait(TIMEOUT)
        other.set(1)
        proceed.set()
        with pytest.raises(QueryCancelled):
            future.result(TIMEOUT)
        assert runs == {"dep": 2, "top": 1}  # top() did not run in between
        state["hold"] = False
        assert _arun(top) == 11
        assert runs == {"dep": 3, "top": 2}
    finally:
        proceed.set()
        engine.shutdown()


def test_async_load_from_disk_reaching_the_running_head(tmp_path: Path) -> None:
    # Session one stores q() = d() + 1, d() = r() + 1, r() = 1. In session two
    # r() calls q(), making a cycle with r() as head. Loading q() from disk
    # checks d(), whose own record points at r(), which is running: neither
    # record can be used, and q() and d() run inside the cycle.
    def make(engine: Engine, runs: Counter[str]) -> Any:
        @engine.input
        def flag() -> bool:
            return False

        @engine.input
        def inp() -> int:
            return 1

        @engine.query(fixed_point=0)
        async def r() -> int:
            runs["r"] += 1
            if flag():
                return min(await q(), 10)
            return inp()

        @engine.query
        async def q() -> int:
            runs["q"] += 1
            return await d() + 1

        @engine.query
        async def d() -> int:
            runs["d"] += 1
            return await r() + 1

        return flag, r, q, d

    runs: Counter[str] = Counter()
    engine = Engine(cache_dir=tmp_path / "a", cache_backend="sqlite")
    try:
        _, _, q, _ = make(engine, runs)
        assert _arun(q) == 3
    finally:
        engine.shutdown()

    def second_session(cache_dir: Path) -> tuple[int, int, int]:
        engine = Engine(cache_dir=cache_dir, cache_backend="sqlite")
        try:
            flag, r, q, d = make(engine, Counter())
            flag.set(True)

            async def read() -> tuple[int, int, int]:
                return await r(), await q(), await d()

            return asyncio.run(asyncio.wait_for(read(), TIMEOUT))
        finally:
            engine.shutdown()

    # r = min(r + 2, 10) from the guess 0: 2, 4, ..., 10.
    assert second_session(tmp_path / "a") == (10, 12, 11)
    assert second_session(tmp_path / "fresh") == (10, 12, 11)


def test_load_from_disk_reaching_the_running_head(tmp_path: Path) -> None:
    # The same on the sync path. While r() runs, loading q() from disk checks
    # d() in a shadow runtime; loading d() checks r() in another one, which
    # refuses because r() runs in the runtime that started the check. The
    # check of d() does not own r() and passes the refusal up; the check of
    # q() does, and counts the dependency as not matching. No thread is
    # involved, so this is deterministic.
    def make(engine: Engine, runs: Counter[str]) -> Any:
        @engine.input
        def flag() -> bool:
            return False

        @engine.input
        def inp() -> int:
            return 1

        @engine.query(fixed_point=0)
        def r() -> int:
            runs["r"] += 1
            if flag():
                return min(q(), 10)
            return inp()

        @engine.query
        def q() -> int:
            runs["q"] += 1
            return d() + 1

        @engine.query
        def d() -> int:
            runs["d"] += 1
            return r() + 1

        return flag, r, q, d

    runs: Counter[str] = Counter()
    engine = Engine(cache_dir=tmp_path / "a", cache_backend="sqlite")
    try:
        _, _, q, _ = make(engine, runs)
        assert q() == 3
    finally:
        engine.shutdown()
    assert runs == {"r": 1, "q": 1, "d": 1}

    def second_session(cache_dir: Path) -> tuple[tuple[int, int, int], Any, Any]:
        engine = Engine(cache_dir=cache_dir, cache_backend="sqlite")
        session_runs: Counter[str] = Counter()
        try:
            flag, r, q, d = make(engine, session_runs)
            flag.set(True)
            values = (r(), q(), d())
            hits = {t.key for t in engine.traces() if t.event == "disk_hit"}
            return values, session_runs, hits
        finally:
            engine.shutdown()

    # r = min(r + 2, 10) from the guess 0: 2, 4, ..., 10.
    values, session_runs, hits = second_session(tmp_path / "a")
    assert values == (10, 12, 11)
    # Six iterations of the cycle (guesses 0, 2, ..., 10); nothing was loaded.
    assert session_runs == {"r": 6, "q": 6, "d": 6}
    assert not hits
    fresh_values, fresh_runs, _ = second_session(tmp_path / "fresh")
    assert (fresh_values, fresh_runs) == (values, session_runs)
