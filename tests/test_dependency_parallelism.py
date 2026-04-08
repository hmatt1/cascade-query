from __future__ import annotations

import threading
import time

from cascade import Engine


def test_compute_many_dependency_gate_allows_independent_overlap() -> None:
    engine = Engine()
    width = 8
    release = threading.Event()
    lock = threading.Lock()

    prereq_started = [threading.Event() for _ in range(width)]
    independent_started_before_release = threading.Event()

    runs = {"prereq": 0}
    active_prereq = 0
    max_active_prereq = 0
    prereq_done_at: dict[int, float] = {}
    dependent_after_at: dict[int, float] = {}

    @engine.input
    def base(index: int) -> int:
        return 0

    @engine.query
    def prerequisite(index: int) -> int:
        nonlocal active_prereq, max_active_prereq
        with lock:
            runs["prereq"] += 1
            active_prereq += 1
            max_active_prereq = max(max_active_prereq, active_prereq)
        prereq_started[index].set()
        release.wait(timeout=2.0)
        with lock:
            prereq_done_at[index] = time.perf_counter()
            active_prereq -= 1
        return base(index) * 2

    @engine.query
    def dependent(index: int) -> int:
        value = prerequisite(index)
        with lock:
            dependent_after_at[index] = time.perf_counter()
        return value + 1

    @engine.query
    def independent(index: int) -> int:
        if not release.is_set():
            independent_started_before_release.set()
        time.sleep(0.002)
        return base(index) + 100

    for index in range(width):
        base.set(index, index)

    calls = (
        [(dependent, (index,)) for index in range(width)]
        + [(prerequisite, (index,)) for index in range(width)]
        + [(independent, (index,)) for index in range(width)]
    )

    result_box: list[list[int]] = []
    error_box: list[BaseException] = []

    def run_batch() -> None:
        try:
            result_box.append(engine.compute_many(calls, workers=width * 4))
        except BaseException as exc:  # pragma: no cover - defensive
            error_box.append(exc)

    runner = threading.Thread(target=run_batch, daemon=True)
    runner.start()

    for event in prereq_started:
        assert event.wait(timeout=2.0)
    assert independent_started_before_release.wait(timeout=2.0)
    release.set()

    runner.join(timeout=6.0)
    assert not runner.is_alive()
    assert not error_box
    result = result_box[0]

    assert runs["prereq"] == width
    assert max_active_prereq >= 2

    for index in range(width):
        assert prereq_done_at[index] <= dependent_after_at[index]

    expected = (
        [index * 2 + 1 for index in range(width)]
        + [index * 2 for index in range(width)]
        + [index + 100 for index in range(width)]
    )
    assert result == expected


def test_compute_many_layered_dependencies_run_once_per_key() -> None:
    engine = Engine()
    width = 6
    release_stage1 = threading.Event()
    lock = threading.Lock()

    stage1_started = [threading.Event() for _ in range(width)]
    independent_started_before_release = threading.Event()

    runs = {"stage1": 0, "stage2": 0, "stage3": 0}
    stage1_done_at: dict[int, float] = {}
    stage2_after_at: dict[int, float] = {}
    stage3_after_at: dict[int, float] = {}

    @engine.input
    def base(index: int) -> int:
        return 0

    @engine.query
    def stage1(index: int) -> int:
        with lock:
            runs["stage1"] += 1
        stage1_started[index].set()
        release_stage1.wait(timeout=2.0)
        with lock:
            stage1_done_at[index] = time.perf_counter()
        return base(index) + 10

    @engine.query
    def stage2(index: int) -> int:
        with lock:
            runs["stage2"] += 1
        value = stage1(index) + 20
        with lock:
            stage2_after_at[index] = time.perf_counter()
        return value

    @engine.query
    def stage3(index: int) -> int:
        with lock:
            runs["stage3"] += 1
        value = stage2(index) + 30
        with lock:
            stage3_after_at[index] = time.perf_counter()
        return value

    @engine.query
    def independent(index: int) -> int:
        if not release_stage1.is_set():
            independent_started_before_release.set()
        time.sleep(0.0015)
        return base(index) - 1

    for index in range(width):
        base.set(index, index * 3)

    calls = []
    for index in range(width):
        calls.extend(
            [
                (stage3, (index,)),
                (stage2, (index,)),
                (stage1, (index,)),
                (independent, (index,)),
            ]
        )

    result_box: list[list[int]] = []
    error_box: list[BaseException] = []

    def run_batch() -> None:
        try:
            result_box.append(engine.compute_many(calls, workers=width * 4))
        except BaseException as exc:  # pragma: no cover - defensive
            error_box.append(exc)

    runner = threading.Thread(target=run_batch, daemon=True)
    runner.start()

    for event in stage1_started:
        assert event.wait(timeout=2.0)
    assert independent_started_before_release.wait(timeout=2.0)
    release_stage1.set()

    runner.join(timeout=6.0)
    assert not runner.is_alive()
    assert not error_box
    result = result_box[0]

    assert runs["stage1"] == width
    assert runs["stage2"] == width
    assert runs["stage3"] == width

    for index in range(width):
        assert stage1_done_at[index] <= stage2_after_at[index] <= stage3_after_at[index]

    expected: list[int] = []
    for index in range(width):
        base_value = index * 3
        expected.extend(
            [
                base_value + 60,
                base_value + 30,
                base_value + 10,
                base_value - 1,
            ]
        )
    assert result == expected
