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
