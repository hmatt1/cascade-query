"""Threads and tasks entering one cycle through different queries (G7, D24).

Each runs one query of the cycle and asks for another's; waiting for each
other would never end. The engine sees the cycle in its record of who waits
for whom and runs the query on the caller's own stack instead, where the
cycle is handled as on one thread. With ``fixed_point=`` whoever entered
through the head gets the fixed point and the others get it too or a
``CycleError``; without it, everyone gets ``CycleError``. Nothing hangs:
every thread is a daemon joined with a timeout.
"""

from __future__ import annotations

import asyncio
import gc
import random
import threading
import time
from typing import Any

import pytest

from cascade import CycleError, Engine

TIMEOUT = 10.0


def _waiters(engine: Engine) -> int:
    with engine._internals.lock:  # noqa: SLF001
        return len(engine._store.waiting_on)  # noqa: SLF001


def _until(predicate: Any) -> None:
    deadline = time.monotonic() + TIMEOUT
    while not predicate():
        assert time.monotonic() < deadline, "timed out"
        time.sleep(0.002)


def _run_threads(calls: dict[str, Any]) -> dict[str, Any]:
    outcomes: dict[str, Any] = {}

    def call(name: str, fn: Any) -> None:
        try:
            outcomes[name] = fn()
        except BaseException as exc:  # noqa: BLE001
            outcomes[name] = exc

    threads = [
        threading.Thread(target=call, args=(name, fn), daemon=True)
        for name, fn in calls.items()
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
    assert not any(thread.is_alive() for thread in threads), "threads deadlocked"
    return outcomes


class _TwoQueryCycle:
    """``head`` and ``member`` call each other; the order of the calls is set."""

    def __init__(self, engine: Engine, *, fixed_point: bool, first: str) -> None:
        self.inside = {"head": threading.Event(), "member": threading.Event()}

        def gate(me: str) -> None:
            if self.inside[me].is_set():
                return  # only the first run of each query is held
            self.inside[me].set()
            other = "member" if me == "head" else "head"
            assert self.inside[other].wait(TIMEOUT)
            if me != first:
                # Call only once the other thread waits for this one.
                _until(lambda: _waiters(engine) >= 1)

        @engine.input
        def inp() -> int:
            return 4

        options: dict[str, Any] = {"fixed_point": 0} if fixed_point else {}

        @engine.query(**options)
        def head() -> int:
            gate("head")
            return member() + inp()

        @engine.query
        def member() -> int:
            gate("member")
            return head() // 2

        self.inp, self.head, self.member = inp, head, member


@pytest.mark.parametrize("first", ["head", "member"])
def test_fixed_point_entered_from_both_ends_converges(first: str) -> None:
    engine = Engine()
    cycle = _TwoQueryCycle(engine, fixed_point=True, first=first)
    outcomes = _run_threads({"head": cycle.head, "member": cycle.member})
    assert outcomes["head"] == 7
    assert outcomes["member"] == 3 or isinstance(outcomes["member"], CycleError)
    assert "dedup_cycle" in [t.event for t in engine.traces()]
    # Settled afterwards: both read the fixed point from one thread.
    assert cycle.head() == 7
    assert cycle.member() == 3
    assert not engine._internals.in_flight  # noqa: SLF001
    assert _waiters(engine) == 0


@pytest.mark.parametrize("first", ["head", "member"])
def test_plain_cycle_entered_from_both_ends_raises_for_both(first: str) -> None:
    engine = Engine()
    cycle = _TwoQueryCycle(engine, fixed_point=False, first=first)
    outcomes = _run_threads({"head": cycle.head, "member": cycle.member})
    assert isinstance(outcomes["head"], CycleError), outcomes
    assert isinstance(outcomes["member"], CycleError), outcomes
    assert not engine._internals.in_flight  # noqa: SLF001
    assert _waiters(engine) == 0


def test_three_threads_entering_a_three_query_cycle() -> None:
    engine = Engine()
    names = ("head", "a", "b")
    inside = {name: threading.Event() for name in names}

    def gate(me: str) -> None:
        inside[me].set()
        for name in names:
            assert inside[name].wait(TIMEOUT)

    @engine.input
    def inp() -> int:
        return 6

    @engine.query(fixed_point=0)
    def head() -> int:
        gate("head")
        return a() + inp()

    @engine.query
    def a() -> int:
        gate("a")
        return b() // 2

    @engine.query
    def b() -> int:
        gate("b")
        return head() // 2

    # head = head // 4 + 6 converges to 7 (0 -> 6 -> 7 -> 7).
    outcomes = _run_threads({"head": head, "a": a, "b": b})
    assert outcomes["head"] == 7
    assert outcomes["a"] == 1 or isinstance(outcomes["a"], CycleError)
    assert outcomes["b"] == 3 or isinstance(outcomes["b"], CycleError)
    assert (head(), a(), b()) == (7, 1, 3)


def test_async_tasks_entering_a_fixed_point_from_both_ends() -> None:
    engine = Engine()

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    async def head() -> int:
        await asyncio.sleep(0.01)
        return await member() + inp()

    @engine.query
    async def member() -> int:
        await asyncio.sleep(0.01)
        return await head() // 2

    async def main() -> list[Any]:
        return await asyncio.gather(head(), member(), return_exceptions=True)

    outcomes = _run_threads({"main": lambda: asyncio.run(main())})["main"]
    head_value, member_value = outcomes
    assert head_value == 7
    assert member_value == 3 or isinstance(member_value, CycleError)


def test_async_tasks_entering_a_plain_cycle_from_both_ends() -> None:
    engine = Engine()

    @engine.query
    async def ping() -> int:
        await asyncio.sleep(0.01)
        return await pong() + 1

    @engine.query
    async def pong() -> int:
        await asyncio.sleep(0.01)
        return await ping() + 1

    async def main() -> list[Any]:
        return await asyncio.gather(ping(), pong(), return_exceptions=True)

    outcomes = _run_threads({"main": lambda: asyncio.run(main())})["main"]
    assert all(isinstance(o, CycleError) for o in outcomes), outcomes


def test_a_task_and_a_thread_entering_one_cycle() -> None:
    engine = Engine()
    inside = {"head": threading.Event(), "member": threading.Event()}

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        inside["head"].set()
        assert inside["member"].wait(TIMEOUT)
        return member() + inp()

    @engine.query
    async def member() -> int:
        inside["member"].set()
        while not inside["head"].is_set():
            await asyncio.sleep(0.002)
        await asyncio.sleep(0.02)  # let the thread ask for member() first
        # A synchronous call from the task: it blocks the loop's thread.
        return head() // 2

    async def main() -> Any:
        return await member()

    outcomes = _run_threads({"head": head, "member": lambda: asyncio.run(main())})
    assert outcomes["head"] == 7
    assert outcomes["member"] == 3 or isinstance(outcomes["member"], CycleError)


def test_many_threads_entering_a_cycle_at_random_while_inputs_change() -> None:
    engine = Engine()

    @engine.input
    def base() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        time.sleep(random.random() * 0.002)
        return m1() // 2 + base()

    @engine.query
    def m1() -> int:
        time.sleep(random.random() * 0.002)
        return m2() + 1

    @engine.query
    def m2() -> int:
        time.sleep(random.random() * 0.002)
        return head()

    def expected(b: int) -> int:
        h = 0
        while True:
            nxt = (h + 1) // 2 + b
            if nxt == h:
                return h
            h = nxt

    stop = threading.Event()
    errors: list[Any] = []
    entries = {"head": head, "m1": m1, "m2": m2}

    def worker(seed: int) -> None:
        rng = random.Random(seed)
        while not stop.is_set():
            name = rng.choice(list(entries))
            snap = engine.snapshot()
            try:
                value = entries[name](snapshot=snap)
                h = expected(base(snapshot=snap))
                want = {"head": h, "m1": h + 1, "m2": h}[name]
                if value != want:
                    errors.append((name, value, want))
            except CycleError:
                if name == "head":
                    errors.append(("head raised", name))
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
            finally:
                snap.release()

    threads = [
        threading.Thread(target=worker, args=(i,), daemon=True) for i in range(6)
    ]
    for thread in threads:
        thread.start()
    try:
        for b in range(5, 25):
            base.set(b)
            time.sleep(0.02)
    finally:
        stop.set()
        for thread in threads:
            thread.join(TIMEOUT)
    assert not any(thread.is_alive() for thread in threads), "threads deadlocked"
    assert errors == []


def test_verifying_stored_results_that_depend_on_each_other_in_a_loop() -> None:
    # Threads reading one fixed-point cycle at different revisions can leave
    # results behind that depend on each other in a loop: the head's result
    # read the member's cached result, which read the head's. Build that state
    # directly and check that verifying it neither recurses without end nor
    # caches a RecursionError.
    from cascade._state import Dependency

    engine = Engine()

    @engine.input
    def base() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        return m1() // 2 + base()

    @engine.query
    def m1() -> int:
        return m2() + 1

    @engine.query
    def m2() -> int:
        return head()

    assert head() == 8
    memos = engine._internals.memos  # noqa: SLF001
    head_key, m1_key = ("query", head.id, ()), ("query", m1.id, ())
    with engine._internals.lock:  # noqa: SLF001
        stored = memos[head_key]
        stored.deps = (Dependency(m1_key, memos[m1_key].changed_at), *stored.deps)
        stored.cycle_nodes = ()
        engine._store.dependents[m1_key].add(head_key)  # noqa: SLF001
    base.set(5)
    outcomes = _run_threads({"m2": m2})
    assert outcomes["m2"] == 10
    assert (head(), m1(), m2()) == (10, 11, 10)


# --- joining a run that ends on a guess ---
#
# While a fixed point is iterated, the queries in its cycle return results
# computed from a guess. Such a result is good for the iterating thread only,
# so a caller that joined the run gets none: it is told which fixed points
# the result hangs on, and waits for those. The random test above gets a
# caller into that place only when its threads happen to meet; these put one
# there on purpose.
#
# A caller that iterates one of those fixed points itself runs the query on
# its own guess instead. Two callers iterate one fixed point at one revision
# only when the owner of its run gives up part way while another runs it on
# its own stack. The last test of this group gets tasks there by cancelling
# the owner; no test gets threads there. (Before D33 was fixed, callers also
# got there whenever a waiter was slow to say that it had stopped waiting.)


def _dedup_waits(engine: Engine) -> int:
    return [t.event for t in engine.traces()].count("dedup_wait")


def test_thread_joining_a_run_that_ends_on_a_guess_waits_for_the_fixed_point() -> None:
    engine = Engine()
    member_running = threading.Event()
    ran: dict[str, set[int]] = {"head": set(), "member": set()}
    head_runs: list[int] = []

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        ran["head"].add(threading.get_ident())
        head_runs.append(1)
        if len(head_runs) == 2:
            # Not before the joiner has gone on from the member's run to
            # waiting for this one: its second wait.
            _until(lambda: _dedup_waits(engine) >= 2)
        return member() + inp()

    @engine.query
    def member() -> int:
        ran["member"].add(threading.get_ident())
        if not member_running.is_set():
            member_running.set()
            _until(lambda: _waiters(engine) >= 1)  # the other thread has joined
        return head() // 2

    def join_member() -> int:
        assert member_running.wait(TIMEOUT)
        return member()

    outcomes = _run_threads({"head": head, "member": join_member})
    assert outcomes == {"head": 7, "member": 3}
    # The joiner waited for the member's run, then for the fixed point, and
    # read the member's final result: it ran nothing itself.
    assert _dedup_waits(engine) == 2
    assert len(ran["head"]) == 1
    assert ran["member"] == ran["head"]
    assert not engine._internals.in_flight  # noqa: SLF001
    assert _waiters(engine) == 0


def test_task_joining_a_run_that_ends_on_a_guess_waits_for_the_fixed_point() -> None:
    engine = Engine()
    ran: dict[str, set[Any]] = {"head": set(), "member": set()}
    runs = {"head": 0, "member": 0}

    async def until(predicate: Any) -> None:
        deadline = time.monotonic() + TIMEOUT
        while not predicate():
            assert time.monotonic() < deadline, "timed out"
            await asyncio.sleep(0.002)

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    async def head() -> int:
        ran["head"].add(asyncio.current_task())
        runs["head"] += 1
        if runs["head"] == 2:
            await until(lambda: _dedup_waits(engine) >= 2)
        return await member() + inp()

    @engine.query
    async def member() -> int:
        ran["member"].add(asyncio.current_task())
        runs["member"] += 1
        if runs["member"] == 1:
            await until(lambda: _waiters(engine) >= 1)
        return await head() // 2

    async def main() -> list[Any]:
        iterating = asyncio.create_task(head())
        await until(lambda: runs["member"] == 1)
        return await asyncio.gather(iterating, member(), return_exceptions=True)

    outcomes = _run_threads({"main": lambda: asyncio.run(main())})["main"]
    assert outcomes == [7, 3]
    assert _dedup_waits(engine) == 2
    assert len(ran["head"]) == 1
    assert ran["member"] == ran["head"]
    assert not engine._internals.in_flight  # noqa: SLF001
    assert _waiters(engine) == 0


def test_task_iterating_a_fixed_point_runs_a_joined_member_on_its_own_guess(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Three tasks. One enters the cycle through side() and runs it. The
    # second owns the run of head() and waits for side(), so the first, when
    # it asks for head(), runs it on its own stack. The second is cancelled
    # there, which frees the run of head() while the first still iterates
    # it. A third task claims the run and joins the first one's run of
    # member(). That run ends on the first task's guess, which is of no use
    # to the third: it runs member() itself, on its own.
    engine = Engine()
    runs = {"member": 0, "side": 0}
    state = {"owner_cancelled": False}

    async def until(predicate: Any) -> None:
        deadline = time.monotonic() + TIMEOUT
        while not predicate():
            assert time.monotonic() < deadline, "timed out"
            await asyncio.sleep(0.002)

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    async def head() -> int:
        m = await member()
        s = await side()
        return m + s + inp()

    @engine.query
    async def member() -> int:
        runs["member"] += 1
        if runs["member"] == 2:
            # The first task's run, inside its own head(). Held until the
            # owner is gone and the third task has joined this run.
            await until(lambda: state["owner_cancelled"] and _waiters(engine) >= 1)
        return await head() // 2

    @engine.query
    async def side() -> int:
        runs["side"] += 1
        if runs["side"] == 1:
            await until(lambda: _waiters(engine) >= 1)  # the owner of head() waits
        return await head() // 4

    async def main() -> list[Any]:
        first = asyncio.create_task(side())
        await until(lambda: runs["side"] == 1)
        owner = asyncio.create_task(head())
        await until(lambda: runs["member"] == 2)
        owner.cancel()
        cancelled = (await asyncio.gather(owner, return_exceptions=True))[0]
        state["owner_cancelled"] = True
        third = asyncio.create_task(head())
        rest = await asyncio.gather(third, first, return_exceptions=True)
        return [cancelled, *rest]

    async def settled() -> tuple[int, int, int]:
        return await head(), await member(), await side()

    outcomes = _run_threads({"main": lambda: asyncio.run(main())})["main"]
    cancelled, third, first = outcomes
    assert isinstance(cancelled, asyncio.CancelledError)
    # head = head // 2 + head // 4 + 4 converges to 11.
    assert third == 11
    # Entered through side(), as when a thread enters through a member.
    assert first == 2 or isinstance(first, CycleError)
    assert asyncio.run(settled()) == (11, 5, 2)
    assert not engine._internals.in_flight  # noqa: SLF001
    assert _waiters(engine) == 0
    # The cancelled task waited for a run that then failed: nothing of that
    # may be left behind for asyncio to report.
    del outcomes, cancelled, third, first
    gc.collect()
    reported = [r.getMessage() for r in caplog.records if r.name == "asyncio"]
    assert not [m for m in reported if "never retrieved" in m or "callback" in m]


# --- the record of who waits for whom ---


def test_releasing_one_run_leaves_the_waits_for_other_runs_on_record() -> None:
    # A run that is released drops the records of the callers that waited for
    # it (D33). Every other record has to stay: two threads in one cycle are
    # kept from waiting for each other only by the record of the first wait.
    engine = Engine()
    member_running = threading.Event()
    unrelated_running = threading.Event()
    unrelated_released = threading.Event()

    @engine.input
    def inp() -> int:
        return 4

    @engine.query
    def unrelated() -> int:
        unrelated_running.set()
        # Until head()'s thread waits for member() and a second caller for this.
        _until(lambda: _waiters(engine) >= 2)
        return 1

    @engine.query(fixed_point=0)
    def head() -> int:
        return member() + inp()

    @engine.query
    def member() -> int:
        member_running.set()
        assert unrelated_released.wait(TIMEOUT)
        return head() // 2

    def call_head() -> int:
        assert member_running.wait(TIMEOUT)
        return head()

    def run_unrelated() -> int:
        assert member_running.wait(TIMEOUT)
        value = unrelated()
        unrelated_released.set()
        return value

    def join_unrelated() -> int:
        assert unrelated_running.wait(TIMEOUT)
        return unrelated()

    outcomes = _run_threads(
        {
            "member": member,
            "head": call_head,
            "unrelated": run_unrelated,
            "joined": join_unrelated,
        }
    )
    assert (outcomes["unrelated"], outcomes["joined"]) == (1, 1)
    # As when two threads enter the cycle from both ends, see above.
    assert outcomes["head"] == 7
    assert outcomes["member"] == 3 or isinstance(outcomes["member"], CycleError)
    assert "dedup_cycle" in [t.event for t in engine.traces()]
    assert not engine._internals.in_flight  # noqa: SLF001
    assert _waiters(engine) == 0
