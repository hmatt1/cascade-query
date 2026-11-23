"""A rewritten stage that reads tracked state follows that state (D1).

Each query reads an input, a query or another collection inside the element
function of a rewritten pipeline, after the body has already read the same
thing. The cached per-element results must not outlive a change.
"""

from __future__ import annotations

import pytest

from cascade import CascadeDict, CascadeList, CascadeSet, Engine


@pytest.fixture
def engine() -> Engine:
    return Engine()


def test_map_stage_reading_an_input_the_body_read_first(engine: Engine) -> None:
    xs = CascadeList(engine, [1, 2, 3])
    factor = engine.input(lambda: 2, name="factor")

    @engine.query
    def total() -> int:
        f = factor()  # noqa: F841
        return sum(x * factor() for x in xs)

    assert total() == 12
    factor.set(10)
    assert total() == 60
    xs.append(4)
    assert total() == 100
    factor.set(1)
    assert total() == 10


def test_filter_clause_reading_an_input_the_body_read_first(engine: Engine) -> None:
    xs = CascadeList(engine, [1, 2, 3, 4])
    limit = engine.input(lambda: 2, name="limit")

    @engine.query
    def small() -> list[int]:
        if limit() < 0:
            return []
        return [x for x in xs if x <= limit()]

    assert small() == [1, 2]
    limit.set(3)
    assert small() == [1, 2, 3]
    xs.append(0)
    assert small() == [1, 2, 3, 0]
    limit.set(0)
    assert small() == [0]


def test_map_and_filter_builtins_with_functions_that_read(engine: Engine) -> None:
    xs = CascadeList(engine, [1, 2, 3, 4])
    limit = engine.input(lambda: 2, name="limit")

    def keep(x: int) -> bool:
        return x <= limit()

    def scale(x: int) -> int:
        return x * limit()

    @engine.query
    def out() -> list[int]:
        limit()
        return list(map(scale, filter(keep, xs)))

    assert out() == [2, 4]
    limit.set(3)
    assert out() == [3, 6, 9]


def test_nested_comprehension_reading_an_input_the_body_read_first(
    engine: Engine,
) -> None:
    xs = CascadeList(engine, [1, 2])
    ys = CascadeList(engine, [10, 20])
    factor = engine.input(lambda: 1, name="factor")

    @engine.query
    def grid() -> list[int]:
        factor()
        return [sum(x * y * factor() for y in ys) for x in xs]

    assert grid() == [30, 60]
    factor.set(2)
    assert grid() == [60, 120]
    ys.append(30)
    assert grid() == [120, 240]
    xs.append(3)
    assert grid() == [120, 240, 360]


def test_nested_pipeline_over_a_collection_the_body_read_first(engine: Engine) -> None:
    xs = CascadeList(engine, [1, 2])
    ys = CascadeSet(engine, [1])

    @engine.query
    def hits() -> list[bool]:
        if not len(ys):
            return []
        return [any(y == x for y in ys) for x in xs]

    assert hits() == [True, False]
    ys.add(2)
    assert hits() == [True, True]
    ys.discard(1)
    assert hits() == [False, True]


@pytest.mark.parametrize("reducer", ["sorted", "min", "max"])
def test_key_function_reading_an_input_the_body_read_first(
    engine: Engine, reducer: str
) -> None:
    xs = CascadeList(engine, [1, 2, 3])
    sign = engine.input(lambda: 1, name="sign")

    def by_sign(x: int) -> int:
        return x * sign()

    if reducer == "sorted":

        @engine.query
        def q():
            sign()
            return sorted((x for x in xs), key=by_sign)

        def plain():
            return sorted(list(xs), key=lambda x: x * sign())

    elif reducer == "min":

        @engine.query
        def q():
            sign()
            return min((x for x in xs), key=by_sign)

        def plain():
            return min(list(xs), key=lambda x: x * sign())

    else:

        @engine.query
        def q():
            sign()
            return max((x for x in xs), key=by_sign)

        def plain():
            return max(list(xs), key=lambda x: x * sign())

    assert q() == plain()
    sign.set(-1)
    assert q() == plain()
    xs.append(4)
    assert q() == plain()
    sign.set(1)
    assert q() == plain()


def test_stage_reading_a_query_the_body_read_first(engine: Engine) -> None:
    xs = CascadeList(engine, [1, 2, 3])
    base = engine.input(lambda: 1, name="base")

    @engine.query
    def rate() -> int:
        return base() * 2

    @engine.query
    def total() -> int:
        r = rate()  # noqa: F841
        return sum(x * rate() for x in xs)

    assert total() == 12
    base.set(5)
    assert total() == 60
    xs.pop()
    assert total() == 30


def test_stage_reading_a_ttl_query(
    engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [1000.0]
    monkeypatch.setattr("cascade._evaluator.time.time", lambda: now[0])
    xs = CascadeList(engine, [1, 2, 3])

    @engine.query(ttl=10.0)
    def rate() -> float:
        return now[0]

    @engine.query
    def total() -> float:
        r = rate()  # noqa: F841
        return sum(x * rate() for x in xs)

    assert total() == 6000.0
    now[0] = 1005.0
    assert total() == 6000.0  # within the TTL
    now[0] = 1020.0
    assert total() == 6 * 1020.0
    xs.append(4)
    assert total() == 10 * 1020.0
    now[0] = 1040.0
    assert total() == 10 * 1040.0


def test_stage_that_reads_only_for_some_elements(engine: Engine) -> None:
    xs = CascadeList(engine, [1, 2])
    factor = engine.input(lambda: 2, name="factor")

    @engine.query
    def total() -> int:
        factor()
        return sum(x * factor() if x > 5 else x for x in xs)

    assert total() == 3
    factor.set(3)
    assert total() == 3
    xs.append(10)  # the first element whose result depends on the input
    assert total() == 33
    factor.set(4)
    assert total() == 43


def test_stage_reading_through_a_snapshot(engine: Engine) -> None:
    xs = CascadeList(engine, [1, 2, 3])
    factor = engine.input(lambda: 2, name="factor")

    @engine.query
    def total() -> int:
        factor()
        return sum(x * factor() for x in xs)

    assert total() == 12
    snap = engine.snapshot()
    factor.set(10)
    assert total() == 60
    assert total(snapshot=snap) == 12
    assert total() == 60


def test_dict_stage_reading_an_input_the_body_read_first(engine: Engine) -> None:
    d = CascadeDict(engine, {"a": 1, "b": 2})
    bonus = engine.input(lambda: 0, name="bonus")

    @engine.query
    def scored() -> dict[str, int]:
        bonus()
        return {k: v + bonus() for k, v in d.items()}

    assert scored() == {"a": 1, "b": 2}
    bonus.set(10)
    assert scored() == {"a": 11, "b": 12}
    d["c"] = 3
    assert scored() == {"a": 11, "b": 12, "c": 13}


def test_stage_that_catches_an_uncached_error_of_a_query_it_calls(
    engine: Engine,
) -> None:
    xs = CascadeList(engine, [1, 2])
    divisor = engine.input(lambda: 0, name="divisor")

    @engine.query(cache_exceptions=False)
    def share(x: int) -> float:
        return x / divisor()

    def safe(x: int) -> float:
        try:
            return share(x)
        except ZeroDivisionError:
            return -1.0

    @engine.query
    def shares() -> list[float]:
        divisor()
        return [safe(x) for x in xs]

    assert shares() == [-1.0, -1.0]
    divisor.set(2)
    assert shares() == [0.5, 1.0]


def test_pure_stage_stays_incremental_when_the_body_reads_an_input(
    engine: Engine,
) -> None:
    # Only stages that read something themselves give up incremental updates.
    xs = CascadeList(engine, list(range(100)))
    factor = engine.input(lambda: 2, name="factor")
    calls: list[int] = []

    def double(x: int) -> int:
        calls.append(x)
        return x * 2

    @engine.query
    def total() -> int:
        return factor() + sum(double(x) for x in xs)

    assert total() == 2 + 9900
    calls.clear()
    xs.append(100)
    assert total() == 2 + 10100
    assert calls == [100]
    factor.set(3)
    calls.clear()
    assert total() == 3 + 10100
    assert calls == []
