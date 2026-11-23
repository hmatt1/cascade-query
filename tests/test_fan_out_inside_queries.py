"""compute_many / compute_many_stream / submit called from inside a query (D4).

Inside a query, compute_many and compute_many_stream make each requested
query a dependency of the caller, exactly like a direct call. submit is
refused there. Tests that could block run on a daemon thread with a timeout.
"""

from __future__ import annotations

import threading
from typing import Any

import pytest

from cascade import CycleError, Engine

TIMEOUT = 20.0


def _within_timeout(fn: Any) -> Any:
    outcome: list[Any] = []

    def target() -> None:
        try:
            outcome.append(("ok", fn()))
        except BaseException as exc:  # noqa: BLE001
            outcome.append(("raised", exc))

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(TIMEOUT)
    assert not thread.is_alive(), "call did not finish in time"
    kind, value = outcome[0]
    if kind == "raised":
        raise value
    return value


def _fan_out(engine: Engine, api: str, calls: list, **kwargs: Any) -> list[Any]:
    if api == "compute_many":
        return engine.compute_many(calls, **kwargs)
    results: list[Any] = [None] * len(calls)
    for idx, value, _effects in engine.compute_many_stream(calls, **kwargs):
        results[idx] = value
    return results


APIS = ("compute_many", "compute_many_stream")


@pytest.mark.parametrize("api", APIS)
def test_fan_out_records_every_requested_query_as_a_dependency(api: str) -> None:
    engine = Engine()
    runs: dict[int, int] = {}
    threads: set[int] = set()

    @engine.input
    def base(i: int) -> int:
        return i

    @engine.query
    def leaf(i: int) -> int:
        runs[i] = runs.get(i, 0) + 1
        threads.add(threading.get_ident())
        return base(i) * 10

    @engine.query
    def parent() -> int:
        return sum(_fan_out(engine, api, [(leaf, (i,)) for i in range(4)]))

    assert _within_timeout(parent) == 60
    assert runs == {0: 1, 1: 1, 2: 1, 3: 1}
    # The leaves ran on worker threads, not on the caller's.
    assert threading.get_ident() not in threads

    for i in range(4):
        base.set(i, 100)
        expected = sum(1000 if j <= i else j * 10 for j in range(4))
        assert _within_timeout(parent) == expected
    # One changed input re-ran one leaf each time.
    assert runs == {0: 2, 1: 2, 2: 2, 3: 2}
    graph = engine.inspect_graph()
    parent_edges = {dep for node, dep in graph["edges"] if "parent" in node}
    assert len([e for e in parent_edges if "leaf" in e]) == 4


@pytest.mark.parametrize("api", APIS)
def test_fan_out_honours_the_calling_querys_snapshot(api: str) -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def leaf(i: int) -> int:
        return x() + i

    @engine.query
    def parent() -> list[int]:
        return _fan_out(engine, api, [(leaf, (0,)), (leaf, (1,))])

    assert parent() == [1, 2]
    with engine.snapshot() as old:
        x.set(10)
        assert _within_timeout(parent) == [10, 11]
        assert _within_timeout(lambda: parent(snapshot=old)) == [1, 2]
        assert _within_timeout(parent) == [10, 11]


@pytest.mark.parametrize("api", APIS)
def test_fan_out_effects_are_collected_and_reach_the_root(api: str) -> None:
    engine = Engine()
    log = engine.accumulator("log")
    inner: dict[str, list[Any]] = {}

    @engine.query
    def leaf(i: int) -> int:
        log.push(f"leaf-{i}")
        return i

    @engine.query
    def parent() -> int:
        inner.clear()
        log.push("before")
        total = sum(
            _fan_out(engine, api, [(leaf, (i,)) for i in range(3)], effects=inner)
        )
        log.push("after")
        return total

    for _ in range(2):  # the second call replays from the cache
        outer: dict[str, list[Any]] = {}
        assert _within_timeout(lambda: parent(effects=outer)) == 3  # noqa: B023
        assert sorted(outer["log"]) == sorted(
            ["before", "leaf-0", "leaf-1", "leaf-2", "after"]
        )
        assert outer["log"][0] == "before"
        assert outer["log"][-1] == "after"
    # Per-call effects are merged in call order, as outside a query.
    assert inner == {"log": ["leaf-0", "leaf-1", "leaf-2"]}


@pytest.mark.parametrize("api", APIS)
def test_handled_error_from_fan_out_is_still_a_dependency(api: str) -> None:
    engine = Engine()

    @engine.input
    def divisor() -> int:
        return 0

    @engine.query
    def leaf(i: int) -> float:
        return i / divisor()

    @engine.query
    def parent() -> Any:
        try:
            return _fan_out(engine, api, [(leaf, (1,)), (leaf, (2,))])
        except ZeroDivisionError:
            return "failed"

    assert _within_timeout(parent) == "failed"
    divisor.set(2)
    assert _within_timeout(parent) == [0.5, 1.0]


@pytest.mark.parametrize("api", APIS)
def test_uncached_error_from_fan_out_propagates_and_recovers(api: str) -> None:
    engine = Engine()
    fail = [True]
    runs = [0]

    @engine.query(cache_exceptions=False)
    def leaf(i: int) -> int:
        runs[0] += 1
        if fail[0]:
            raise ValueError("not yet")
        return i

    @engine.query(cache_exceptions=False)
    def parent() -> list[int]:
        return _fan_out(engine, api, [(leaf, (1,)), (leaf, (2,))])

    with pytest.raises(ValueError, match="not yet"):
        _within_timeout(parent)
    fail[0] = False
    assert _within_timeout(parent) == [1, 2]


@pytest.mark.parametrize("api", APIS)
def test_cycle_through_fan_out_raises_instead_of_hanging(api: str) -> None:
    engine = Engine()

    @engine.query
    def a() -> int:
        return sum(_fan_out(engine, api, [(b, (0,)), (b, (1,))]))

    @engine.query
    def b(i: int) -> int:
        return a() + i

    with pytest.raises(CycleError):
        _within_timeout(a)


@pytest.mark.parametrize("api", APIS)
def test_fan_out_over_unmemoized_and_repeated_calls(api: str) -> None:
    engine = Engine()
    runs = {"plain": 0, "volatile": 0}

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def plain(i: int) -> int:
        runs["plain"] += 1
        return x() + i

    @engine.query(memoize=False)
    def volatile(i: int) -> int:
        runs["volatile"] += 1
        return x() * i

    @engine.query
    def parent() -> list[int]:
        return _fan_out(
            engine,
            api,
            [(plain, (1,)), (volatile, (2,)), (plain, (1,)), (volatile, (3,))],
        )

    assert _within_timeout(parent) == [2, 2, 2, 3]
    # The repeated call is one run; unmemoized bodies are not run twice.
    assert runs == {"plain": 1, "volatile": 2}
    x.set(5)
    assert _within_timeout(parent) == [6, 10, 6, 15]


def test_nested_fan_out_two_levels_deep() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def leaf(i: int, j: int) -> int:
        return x() * (10 * i + j)

    @engine.query
    def middle(i: int) -> int:
        return sum(engine.compute_many([(leaf, (i, j)) for j in range(3)]))

    @engine.query
    def top() -> int:
        return sum(engine.compute_many([(middle, (i,)) for i in range(3)]))

    expected = sum(10 * i + j for i in range(3) for j in range(3))
    assert _within_timeout(top) == expected
    x.set(2)
    assert _within_timeout(top) == 2 * expected


def test_compute_many_outside_a_query_can_run_queries_that_fan_out() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def leaf(i: int) -> int:
        return x() + i

    @engine.query
    def parent(k: int) -> int:
        return k * sum(engine.compute_many([(leaf, (0,)), (leaf, (1,))]))

    calls = [(parent, (k,)) for k in range(4)]
    assert _within_timeout(lambda: engine.compute_many(calls)) == [0, 3, 6, 9]
    x.set(2)
    assert _within_timeout(lambda: engine.compute_many(calls)) == [0, 5, 10, 15]
    try:
        future = engine.submit(parent, 2)
        assert future.result(timeout=TIMEOUT) == 10
    finally:
        engine.shutdown()


def test_unhashable_arguments_in_fan_out_raise_the_usual_error() -> None:
    engine = Engine()

    @engine.query
    def leaf(i: Any) -> Any:
        return i

    @engine.query
    def parent() -> Any:
        return engine.compute_many([(leaf, ([1],)), (leaf, (2,))])

    with pytest.raises(TypeError):
        _within_timeout(parent)


def test_stream_consumed_after_its_query_ended_still_gives_correct_values() -> None:
    engine = Engine()
    streams: list[Any] = []

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def leaf(i: int) -> int:
        return x() + i

    @engine.query
    def parent() -> int:
        stream = engine.compute_many_stream([(leaf, (0,)), (leaf, (1,))])
        first = next(stream)
        streams.append(stream)
        return first[1]

    assert _within_timeout(parent) in (1, 2)
    rest = _within_timeout(lambda: list(streams[0]))
    assert len(rest) == 1
    assert rest[0][1] == leaf(rest[0][0])


def test_submit_inside_a_query_is_refused() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def leaf() -> int:
        return x()

    @engine.query(cache_exceptions=False)
    def parent() -> int:
        return engine.submit(leaf).result(timeout=TIMEOUT)

    try:
        with pytest.raises(RuntimeError, match="cannot be called from inside a query"):
            parent()
        # Outside a query nothing changed.
        assert engine.submit(leaf).result(timeout=TIMEOUT) == 1
    finally:
        engine.shutdown()
