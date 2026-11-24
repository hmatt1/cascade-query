"""Only the submitted call is cancelled, never a caller that joined it (G6, D19).

Callers that ask for the same query at the same revision share one run (G5).
When that run belongs to ``engine.submit`` and is cancelled, the callers that
joined it run the query themselves at their own revision, unless they are
cancelled too. Every wait has a timeout.
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

from cascade import Engine, QueryCancelled

TIMEOUT = 10.0


def _wait_for_joiner(engine: Engine) -> None:
    """Block until a second caller has joined an in-flight run."""
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        with engine._internals.lock:  # noqa: SLF001
            slots = list(engine._internals.in_flight.values())  # noqa: SLF001
        if any(slot.future is not None for slot in slots):
            return
        time.sleep(0.002)
    raise AssertionError("no caller joined the run")


class _Gate:
    """A query body that parks until released, then reads a nested query."""

    def __init__(self, engine: Engine) -> None:
        self.started = threading.Event()
        self.go = threading.Event()
        self.runs = 0

        @engine.input
        def x() -> int:
            return 1

        @engine.query
        def leaf() -> int:
            return 0

        @engine.query
        def q() -> int:
            self.runs += 1
            value = x()
            self.started.set()
            self.go.wait(TIMEOUT)
            return value + leaf()  # notices a cancellation

        self.x, self.q = x, q


def _in_thread(fn: Any) -> tuple[threading.Thread, list[Any]]:
    out: list[Any] = []

    def target() -> None:
        try:
            out.append(fn())
        except BaseException as exc:  # noqa: BLE001
            out.append(exc)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread, out


def test_direct_caller_gets_the_value_of_its_revision() -> None:
    engine = Engine()
    gate = _Gate(engine)
    try:
        future = engine.submit(gate.q)
        assert gate.started.wait(TIMEOUT)
        thread, out = _in_thread(gate.q)
        _wait_for_joiner(engine)
        gate.x.set(2)
        gate.go.set()
        thread.join(TIMEOUT)
        assert not thread.is_alive()
        with pytest.raises(QueryCancelled):
            future.result(timeout=TIMEOUT)
        assert out == [1]
        assert gate.runs == 2  # the joiner ran it again, at its revision
        assert "dedup_retry" in [t.event for t in engine.traces()]
        assert gate.q() == 2  # the newest revision is unaffected
    finally:
        gate.go.set()
        engine.shutdown()


def test_compute_many_caller_is_not_cancelled_either() -> None:
    engine = Engine()
    gate = _Gate(engine)
    try:
        future = engine.submit(gate.q)
        assert gate.started.wait(TIMEOUT)
        thread, out = _in_thread(lambda: engine.compute_many([(gate.q, ())]))
        _wait_for_joiner(engine)
        gate.x.set(5)
        gate.go.set()
        thread.join(TIMEOUT)
        assert not thread.is_alive()
        assert out == [[1]]
        with pytest.raises(QueryCancelled):
            future.result(timeout=TIMEOUT)
    finally:
        gate.go.set()
        engine.shutdown()


def test_a_later_submit_on_the_same_snapshot_survives_an_earlier_cancellation() -> None:
    engine = Engine()
    gate = _Gate(engine)
    snap = engine.snapshot()
    try:
        first = engine.submit(gate.q, snapshot=snap)
        assert gate.started.wait(TIMEOUT)
        gate.x.set(2)  # cancels the first submit, not ones made after it
        second = engine.submit(gate.q, snapshot=snap)
        _wait_for_joiner(engine)
        gate.go.set()
        with pytest.raises(QueryCancelled):
            first.result(timeout=TIMEOUT)
        assert second.result(timeout=TIMEOUT) == 1
    finally:
        gate.go.set()
        snap.release()
        engine.shutdown()


def test_a_joined_submit_that_is_stale_itself_is_cancelled() -> None:
    engine = Engine()
    gate = _Gate(engine)
    snap = engine.snapshot()
    try:
        first = engine.submit(gate.q, snapshot=snap)
        assert gate.started.wait(TIMEOUT)
        second = engine.submit(gate.q, snapshot=snap)
        _wait_for_joiner(engine)
        gate.x.set(2)  # both submits predate the change
        gate.go.set()
        for future in (first, second):
            with pytest.raises(QueryCancelled):
                future.result(timeout=TIMEOUT)
    finally:
        gate.go.set()
        snap.release()
        engine.shutdown()


def test_nested_caller_inside_another_query_is_not_cancelled() -> None:
    engine = Engine()
    gate = _Gate(engine)

    @engine.query
    def outer() -> int:
        return gate.q() * 10

    try:
        future = engine.submit(gate.q)
        assert gate.started.wait(TIMEOUT)
        thread, out = _in_thread(outer)
        _wait_for_joiner(engine)
        gate.x.set(2)
        gate.go.set()
        thread.join(TIMEOUT)
        assert not thread.is_alive()
        assert out == [10]
        with pytest.raises(QueryCancelled):
            future.result(timeout=TIMEOUT)
        assert outer() == 20
    finally:
        gate.go.set()
        engine.shutdown()


def test_async_caller_joining_a_cancelled_submit_gets_the_value() -> None:
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
    async def q() -> int:
        value = x()
        started.set()
        while not go.is_set():
            await asyncio.sleep(0.005)
        return value + leaf()

    async def direct() -> int:
        return await q()

    try:
        future = engine.submit(q)
        assert started.wait(TIMEOUT)
        thread, out = _in_thread(lambda: asyncio.run(direct()))
        _wait_for_joiner(engine)
        x.set(2)
        go.set()
        thread.join(TIMEOUT)
        assert not thread.is_alive()
        assert out == [1]
        with pytest.raises(QueryCancelled):
            future.result(timeout=TIMEOUT)
    finally:
        go.set()
        engine.shutdown()


def test_many_direct_callers_and_submits_under_changing_inputs() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 0

    @engine.query
    def slow() -> int:
        time.sleep(0.001)
        return x() * 2

    @engine.query
    def top() -> int:
        return slow() + 1

    stop = threading.Event()
    errors: list[BaseException] = []

    def direct() -> None:
        while not stop.is_set():
            try:
                value = top()
                assert value % 2 == 1
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
                return

    def submitter() -> None:
        while not stop.is_set():
            try:
                engine.submit(top).result(timeout=TIMEOUT)
            except QueryCancelled:
                pass
            except BaseException as exc:  # noqa: BLE001
                errors.append(exc)
                return

    threads = [threading.Thread(target=direct, daemon=True) for _ in range(3)]
    threads += [threading.Thread(target=submitter, daemon=True) for _ in range(3)]
    try:
        for thread in threads:
            thread.start()
        for i in range(1, 60):
            x.set(i)
            time.sleep(0.002)
    finally:
        stop.set()
        for thread in threads:
            thread.join(TIMEOUT)
        engine.shutdown()
    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
