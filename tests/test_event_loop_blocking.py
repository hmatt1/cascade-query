"""Synchronous calls on an event loop's thread never hang (G14, D21).

A synchronous query called on the thread that runs an asyncio event loop
cannot wait for an ``async def`` query that has to run, or that is running
as a task on that loop: either wait would block the loop the async query
needs. Such a call raises ``EventLoopBlockedError`` at once, naming the
query and what to do, and nothing is cached. Cached results are served as
usual. Everything runs on daemon threads with a timeout.
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any

import pytest

from cascade import Engine, EventLoopBlockedError

TIMEOUT = 10.0


def _run(fn: Any, timeout: float = TIMEOUT) -> Any:
    """Run ``fn`` on a daemon thread; return its value or raise its error."""
    out: list[tuple[str, Any]] = []

    def target() -> None:
        try:
            out.append(("ok", fn()))
        except BaseException as exc:  # noqa: BLE001
            out.append(("raised", exc))

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    assert not thread.is_alive(), "the call hung"
    kind, value = out[0]
    if kind == "raised":
        raise value
    return value


def _graph(engine: Engine) -> Any:
    class G:
        pass

    g = G()

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

    g.x, g.slow, g.dependent = x, slow, dependent
    return g


def test_waiting_for_a_task_on_the_same_loop_raises_at_once() -> None:
    engine = Engine()
    g = _graph(engine)
    assert g.dependent() == 2
    g.x.set(2)

    async def main() -> Any:
        task = asyncio.create_task(g.slow())
        await asyncio.sleep(0.05)  # slow() is running on this loop
        try:
            with pytest.raises(EventLoopBlockedError) as info:
                g.dependent()
            message = str(info.value)
            assert g.slow.id in message
            assert "to_thread" in message
        finally:
            assert await task == 2
        # Now slow() is cached at this revision, so the same call works.
        return g.dependent()

    assert _run(lambda: asyncio.run(main())) == 3
    # Nothing was cached from the refused call.
    assert g.dependent() == 3


def test_the_suggested_to_thread_call_works_while_the_task_runs() -> None:
    engine = Engine()
    g = _graph(engine)
    assert g.dependent() == 2
    g.x.set(5)

    async def main() -> Any:
        task = asyncio.create_task(g.slow())
        await asyncio.sleep(0.05)
        value = await asyncio.to_thread(g.dependent)
        await task
        return value

    assert _run(lambda: asyncio.run(main())) == 6


def test_sync_query_calling_an_uncached_async_query_on_the_loop() -> None:
    engine = Engine()
    runs = {"dependent": 0}

    @engine.query
    async def fetch(i: int) -> int:
        await asyncio.sleep(0)
        return i * 10

    @engine.query
    def dependent(i: int) -> int:
        runs["dependent"] += 1
        return fetch(i) + 1

    async def main() -> Any:
        # Before the fix: fetch() handed the sync body a coroutine, and the
        # body failed with a TypeError that was cached.
        with pytest.raises(EventLoopBlockedError, match=r"fetch\(1,\)"):
            dependent(1)
        assert await fetch(1) == 10
        return dependent(1)  # cached now: served without blocking

    assert _run(lambda: asyncio.run(main())) == 11
    assert runs["dependent"] == 2  # the error was not cached
    assert dependent(1) == 11


def test_sync_query_on_the_loop_reads_a_cached_async_query() -> None:
    engine = Engine()

    @engine.query
    async def fetch() -> str:
        return "data"

    @engine.query
    def shout() -> str:
        return fetch().upper()

    async def main() -> Any:
        await fetch()
        return shout()

    assert _run(lambda: asyncio.run(main())) == "DATA"


def test_async_input_default_read_by_a_sync_query_on_the_loop() -> None:
    engine = Engine()

    @engine.input
    async def setting() -> str:
        return "default"

    @engine.query
    def reader() -> str:
        return setting() + "!"

    async def main() -> Any:
        with pytest.raises(EventLoopBlockedError, match="setting"):
            reader()
        setting.set("given")
        return reader()  # a stored value needs no coroutine

    assert _run(lambda: asyncio.run(main())) == "given!"


def test_sync_query_owned_by_a_task_is_run_here_instead() -> None:
    engine = Engine()
    gate = threading.Event()
    entered = threading.Event()
    runs = {"n": 0}

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def sync_child() -> int:
        runs["n"] += 1
        if runs["n"] == 2:  # the task's run parks in its executor thread
            entered.set()
            gate.wait(TIMEOUT)
        return x() * 3

    @engine.query
    async def parent() -> int:
        return sync_child() + 1

    async def main() -> Any:
        assert await parent() == 4
        x.set(2)
        # Verifying parent() makes this task own sync_child's run, whose
        # body runs in an executor thread and parks there.
        task = asyncio.create_task(parent())
        while not entered.is_set():
            await asyncio.sleep(0.005)
        try:
            # A synchronous call on the loop thread cannot wait for that
            # task, so it runs the (synchronous) query itself.
            return sync_child()
        finally:
            gate.set()
            assert await task == 7

    assert _run(lambda: asyncio.run(main())) == 6
    assert "dedup_cycle" in [t.event for t in engine.traces()]


def test_waiting_for_a_task_on_another_loop_is_fine() -> None:
    engine = Engine()
    g = _graph(engine)
    assert g.dependent() == 2
    g.x.set(7)
    started = threading.Event()

    def other_loop() -> Any:
        async def run() -> int:
            task = asyncio.create_task(g.slow())
            await asyncio.sleep(0.01)
            started.set()
            return await task

        return asyncio.run(run())

    worker = threading.Thread(target=other_loop, daemon=True)
    worker.start()
    assert started.wait(TIMEOUT)

    async def main() -> Any:
        # Blocks this loop briefly, but the other loop finishes the run.
        return g.dependent()

    assert _run(lambda: asyncio.run(main())) == 8
    worker.join(TIMEOUT)
    assert not worker.is_alive()


def test_async_queries_still_await_each_other_on_one_loop() -> None:
    engine = Engine()

    @engine.query
    async def leaf(i: int) -> int:
        await asyncio.sleep(0.01)
        return i

    @engine.query
    async def total() -> int:
        values = await asyncio.gather(*(leaf(i) for i in range(5)))
        return sum(values)

    async def main() -> Any:
        a, b = await asyncio.gather(total(), total())
        return a, b

    assert _run(lambda: asyncio.run(main())) == (10, 10)
