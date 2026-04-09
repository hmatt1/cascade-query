from __future__ import annotations

import time
import threading

import pytest

from cascade import Engine
from cascade._scheduler import WorkStealingExecutor


def test_compute_many_snapshot_freezes_input_reads() -> None:
    engine = Engine()

    @engine.input
    def cell(index: int) -> int:
        return 0

    @engine.query
    def read_cell(index: int) -> int:
        return cell(index)

    for index in range(6):
        cell.set(index, index)
    snapshot = engine.snapshot()
    for index in range(6):
        cell.set(index, index + 100)

    frozen = engine.compute_many([(read_cell, (index,)) for index in range(6)], snapshot=snapshot)
    live = engine.compute_many([(read_cell, (index,)) for index in range(6)])
    assert frozen == [0, 1, 2, 3, 4, 5]
    assert live == [100, 101, 102, 103, 104, 105]


def test_compute_many_propagates_worker_exceptions() -> None:
    engine = Engine()

    @engine.query
    def boom(i: int) -> int:
        if i == 2:
            raise ValueError("boom")
        return i

    with pytest.raises(ValueError, match="boom"):
        engine.compute_many([(boom, (0,)), (boom, (1,)), (boom, (2,)), (boom, (3,))], workers=3)


def test_compute_many_preserves_call_order_with_mixed_durations() -> None:
    engine = Engine()

    @engine.input
    def base(i: int) -> int:
        return 0

    @engine.query
    def delayed(i: int) -> int:
        time.sleep(0.002 * (8 - i))
        return base(i) * 10

    for i in range(8):
        base.set(i, i)

    calls = [(delayed, (i,)) for i in range(8)]
    result = engine.compute_many(calls, workers=4)
    assert result == [i * 10 for i in range(8)]


def test_compute_many_with_zero_workers_falls_back_to_default_worker_selection() -> None:
    engine = Engine()

    @engine.input
    def base(i: int) -> int:
        return 0

    @engine.query
    def plus(i: int) -> int:
        return base(i) + 1

    for i in range(6):
        base.set(i, i)

    # workers=0 uses the default worker-count branch.
    result = engine.compute_many([(plus, (i,)) for i in range(6)], workers=0)
    assert result == [i + 1 for i in range(6)]


def test_scheduler_bootstrap_invariants_for_compute_many_path() -> None:
    # Keep scheduler defaults explicit: starts open (not shutdown) and with a real lock.
    scheduler = WorkStealingExecutor(workers=1)
    assert scheduler._workers == 1  # noqa: SLF001
    assert scheduler._shutdown is False  # noqa: SLF001
    assert scheduler._lock is not None  # noqa: SLF001
    assert hasattr(scheduler._lock, "acquire")  # noqa: SLF001
    assert hasattr(scheduler._lock, "release")  # noqa: SLF001
