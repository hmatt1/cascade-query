"""Reading an input that was never set does not move the revision (G3).

The first read at the newest revision keeps the default as the input's value,
so the default function is not called again, but a read is not a change:
``engine.revision`` stays put and nothing is invalidated or cancelled.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

import pytest

from cascade import Engine

BACKENDS = ("mdbx", "sqlite")


def test_first_read_keeps_the_default_without_a_new_revision() -> None:
    engine = Engine()
    calls = {"n": 0}

    @engine.input
    def width(i: int) -> int:
        calls["n"] += 1
        return 10 * i

    @engine.query
    def area(i: int) -> int:
        return width(i) * width(i)

    epoch = engine._internals.cancel_epoch  # noqa: SLF001
    assert width(1) == 10
    assert area(2) == 400
    assert engine.revision == 0
    assert engine._internals.cancel_epoch == epoch  # noqa: SLF001
    assert width(1) == 10 and area(2) == 400
    assert calls["n"] == 2  # once per argument: the default was kept
    assert [t.event for t in engine.traces()].count("input_default") == 2


def test_set_after_a_default_read_is_a_change_only_if_the_value_differs() -> None:
    engine = Engine()
    runs = {"n": 0}

    @engine.input
    def width() -> int:
        return 10

    @engine.query
    def doubled() -> int:
        runs["n"] += 1
        return 2 * width()

    assert doubled() == 20
    width.set(10)
    assert engine.revision == 0
    assert doubled() == 20 and runs["n"] == 1
    width.set(11)
    assert engine.revision == 1
    assert doubled() == 22 and runs["n"] == 2


def test_async_first_read_does_not_move_the_revision() -> None:
    engine = Engine()

    @engine.input
    async def remote() -> int:
        await asyncio.sleep(0)
        return 5

    @engine.query
    async def plus() -> int:
        return await remote() + 1

    async def main() -> int:
        return await plus()

    assert asyncio.run(main()) == 6
    assert engine.revision == 0


def test_reads_through_a_snapshot_of_the_head_keep_the_same_default() -> None:
    engine = Engine()
    values = iter([1, 2, 3, 4])

    @engine.input
    def counter() -> int:
        return next(values)

    @engine.query
    def read() -> int:
        return counter()

    with engine.snapshot() as snap:
        assert read(snapshot=snap) == 1
        assert counter() == 1  # the head sees what the snapshot read kept
        assert read() == 1
    assert engine.revision == 0

    @engine.input
    def other() -> int:
        return 0

    with engine.snapshot() as old:
        other.set(1)  # the head moves on
        assert counter(snapshot=old) == 1  # kept at the old revision
        assert engine.revision == 1


def test_threads_reading_a_default_first_agree_on_one_value() -> None:
    engine = Engine()
    counter = iter(range(1000))
    lock = threading.Lock()

    @engine.input
    def token() -> int:
        with lock:
            return next(counter)

    @engine.query
    def seen(i: int) -> int:
        return token()

    start = threading.Barrier(8)
    results: list[int] = []

    def worker(i: int) -> None:
        start.wait()
        results.append(seen(i))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert engine.revision == 0
    kept = token()
    # Every query either read the kept value, or read its own default and
    # verifies against the kept one: at the head they all agree now.
    assert {seen(i) for i in range(8)} == {kept}


@pytest.mark.parametrize("backend", BACKENDS)
def test_results_read_from_a_default_are_reused_by_a_new_engine(
    tmp_path: Path, backend: str
) -> None:
    runs = {"n": 0}

    def session() -> tuple[Engine, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.input(name="defaults.width")
        def width() -> int:
            return 7

        @engine.query(name="defaults.area")
        def area() -> int:
            runs["n"] += 1
            return width() * width()

        return engine, area

    engine, area = session()
    assert area() == 49
    assert engine.revision == 0
    engine.shutdown()
    engine, area = session()
    try:
        assert area() == 49
        assert runs["n"] == 1
    finally:
        engine.shutdown()
