from __future__ import annotations

import time

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


def test_compute_many_can_collect_accumulator_effects() -> None:
    engine = Engine()
    progress = engine.accumulator("progress")

    @engine.query
    def job(name: str) -> str:
        progress.push(("start", name))
        progress.push(("done", name))
        return name.upper()

    effects: dict[str, list[object]] = {}
    result = engine.compute_many([(job, ("a",)), (job, ("b",)), (job, ("c",))], workers=3, effects=effects)
    assert result == ["A", "B", "C"]
    assert effects == {
        "progress": [
            ("start", "a"),
            ("done", "a"),
            ("start", "b"),
            ("done", "b"),
            ("start", "c"),
            ("done", "c"),
        ]
    }


def test_compute_many_stream_yields_in_completion_order() -> None:
    engine = Engine()

    @engine.query
    def delayed(i: int) -> int:
        time.sleep(0.004 * (4 - i))
        return i

    calls = [(delayed, (i,)) for i in range(4)]
    items = list(engine.compute_many_stream(calls, workers=4))
    # i=3 sleeps least, so it should complete first.
    assert [idx for idx, _value, _effects in items] == [3, 2, 1, 0]
    assert sorted(value for _idx, value, _effects in items) == [0, 1, 2, 3]


def test_compute_many_stream_can_yield_and_collect_accumulator_effects() -> None:
    engine = Engine()
    progress = engine.accumulator("progress")

    @engine.query
    def job(name: str, seconds: float) -> str:
        progress.push(("start", name))
        time.sleep(seconds)
        progress.push(("done", name))
        return name.upper()

    calls = [(job, ("a", 0.03)), (job, ("b", 0.01)), (job, ("c", 0.02))]
    effects: dict[str, list[object]] = {}
    items = list(engine.compute_many_stream(calls, workers=3, effects=effects))

    # Stream yields in completion order (b, c, a).
    assert [value for _idx, value, _call_effects in items] == ["B", "C", "A"]
    # Each yielded item includes just that call's effects.
    by_value = {value: call_effects["progress"] for _idx, value, call_effects in items}
    assert by_value["A"] == [("start", "a"), ("done", "a")]
    assert by_value["B"] == [("start", "b"), ("done", "b")]
    assert by_value["C"] == [("start", "c"), ("done", "c")]
    # Aggregate effects dict is merged in call order, matching compute_many.
    assert effects["progress"] == [
        ("start", "a"),
        ("done", "a"),
        ("start", "b"),
        ("done", "b"),
        ("start", "c"),
        ("done", "c"),
    ]


def test_work_stealing_run_raises_first_recorded_task_error() -> None:
    scheduler = WorkStealingExecutor(workers=2)

    def failing() -> None:
        raise ValueError("task failed")

    def succeeding() -> int:
        return 1

    scheduler.submit_indexed(0, failing)
    scheduler.submit_indexed(1, succeeding)
    with pytest.raises(ValueError, match="task failed"):
        scheduler.run(2)


def test_scheduler_bootstrap_invariants_for_compute_many_path() -> None:
    # Keep scheduler defaults explicit: idle (no pending work) and with a real lock.
    scheduler = WorkStealingExecutor(workers=1)
    assert scheduler._workers == 1  # noqa: SLF001
    assert scheduler._pending == 0  # noqa: SLF001
    assert scheduler._lock is not None  # noqa: SLF001
    assert hasattr(scheduler._lock, "acquire")  # noqa: SLF001
    assert hasattr(scheduler._lock, "release")  # noqa: SLF001
