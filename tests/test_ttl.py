import asyncio
import unittest.mock
from collections import Counter
from pathlib import Path
from typing import Any

import pytest

from cascade import Engine


def test_ttl_invalidation_basic():
    engine = Engine()

    clock = 0.0
    engine._store.monotonic_seconds = lambda: clock

    compute_count = 0

    @engine.query(ttl=2.0)
    def my_query():
        nonlocal compute_count
        compute_count += 1
        return "value"

    with unittest.mock.patch("cascade._evaluator.time.time", side_effect=lambda: clock):
        assert my_query() == "value"
        assert compute_count == 1

        # Still within TTL
        clock = 1.0
        assert my_query() == "value"
        assert compute_count == 1

        # Exceed TTL
        clock = 3.0
        assert my_query() == "value"
        assert compute_count == 2

        # Still within new TTL
        clock = 4.0
        assert my_query() == "value"
        assert compute_count == 2


def test_ttl_invalidation_async():
    engine = Engine()

    clock = 0.0
    engine._store.monotonic_seconds = lambda: clock

    compute_count = 0

    @engine.query(ttl=1.5)
    async def my_async_query():
        nonlocal compute_count
        compute_count += 1
        return "async_value"

    async def run():
        nonlocal clock
        with unittest.mock.patch(
            "cascade._evaluator.time.time", side_effect=lambda: clock
        ):
            assert await my_async_query() == "async_value"
            assert compute_count == 1

            clock = 1.0
            assert await my_async_query() == "async_value"
            assert compute_count == 1

            clock = 2.0
            assert await my_async_query() == "async_value"
            assert compute_count == 2

    asyncio.run(run())


# --- TTL reached through dependents (D3) -------------------------------------
#
# A query that depends on a TTL query, however far above it, must give what a
# fresh run would give once the TTL has elapsed, with no revision change.


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr("cascade._evaluator.time.time", fake)
    return fake


def _memo(engine: Engine, handle: Any, *args: Any) -> Any:
    return engine._internals.memos[("query", handle.id, args)]  # noqa: SLF001


def test_expired_ttl_refreshes_a_chain_of_dependents(clock: _Clock) -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.query(ttl=10.0)
    def external() -> float:
        runs["external"] += 1
        return clock.now

    @engine.query
    def first() -> float:
        runs["first"] += 1
        return external() + 1

    @engine.query
    def second() -> float:
        runs["second"] += 1
        return first() + 1

    @engine.query
    def third() -> float:
        runs["third"] += 1
        return second() + 1

    assert third() == 1003.0
    revision = engine.revision

    clock.now = 1005.0  # inside the TTL: nothing runs
    assert third() == 1003.0
    assert second() == 1002.0
    assert runs == {"external": 1, "first": 1, "second": 1, "third": 1}

    clock.now = 1011.0  # past it: every level is refreshed
    assert third() == 1014.0
    assert second() == 1013.0
    assert first() == 1012.0
    assert external() == 1011.0
    assert runs == {"external": 2, "first": 2, "second": 2, "third": 2}
    assert engine.revision == revision


def test_expired_ttl_with_an_equal_result_does_not_rerun_dependents(
    clock: _Clock,
) -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.query(ttl=10.0)
    def external() -> str:
        runs["external"] += 1
        return "same"

    @engine.query
    def parent() -> str:
        runs["parent"] += 1
        return external().upper()

    @engine.query
    def top() -> str:
        runs["top"] += 1
        return parent() + "!"

    assert top() == "SAME!"
    clock.now += 11.0
    assert top() == "SAME!"
    assert runs == {"external": 2, "parent": 1, "top": 1}

    # The dependents now expire with the refreshed result, not the old one.
    clock.now += 5.0
    assert top() == "SAME!"
    assert runs == {"external": 2, "parent": 1, "top": 1}
    clock.now += 6.0
    assert top() == "SAME!"
    assert runs == {"external": 3, "parent": 1, "top": 1}


def test_expired_ttl_refreshes_a_dependent_read_through_a_snapshot(
    clock: _Clock,
) -> None:
    engine = Engine()

    @engine.input
    def offset() -> int:
        return 0

    @engine.query(ttl=10.0)
    def external() -> float:
        return clock.now

    @engine.query
    def parent() -> float:
        return external() + offset()

    offset.set(1)
    snapshot = engine.snapshot()
    assert parent(snapshot=snapshot) == 1001.0
    offset.set(2)
    assert parent(snapshot=snapshot) == 1001.0

    clock.now = 1020.0
    revision = engine.revision
    # The offset is the snapshot one; the TTL result is fresh.
    assert parent(snapshot=snapshot) == 1021.0
    assert parent() == 1022.0
    assert parent(snapshot=snapshot) == 1021.0
    assert engine.revision == revision


def test_dependent_expires_with_the_shortest_ttl_beneath_it(clock: _Clock) -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.query(ttl=100.0)
    def slow() -> float:
        runs["slow"] += 1
        return clock.now

    @engine.query(ttl=10.0)
    def fast() -> float:
        runs["fast"] += 1
        return clock.now

    @engine.query
    def both() -> tuple[float, float]:
        return slow(), fast()

    assert both() == (1000.0, 1000.0)
    assert _memo(engine, both).expires_at == 1010.0
    clock.now = 1011.0
    assert both() == (1000.0, 1011.0)
    assert _memo(engine, both).expires_at == 1021.0
    clock.now = 1101.0
    assert both() == (1101.0, 1101.0)
    assert runs == {"slow": 2, "fast": 3}


def test_ttl_query_that_depends_on_a_ttl_query(clock: _Clock) -> None:
    engine = Engine()

    @engine.query(ttl=10.0)
    def inner() -> float:
        return clock.now

    @engine.query(ttl=100.0)
    def outer() -> float:
        return inner()

    assert outer() == 1000.0
    clock.now = 1011.0  # the outer TTL has not elapsed, the inner one has
    assert outer() == 1011.0


def test_queries_without_a_ttl_beneath_them_carry_no_expiry(clock: _Clock) -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    def plain() -> int:
        return x() + 1

    @engine.query
    def plain_parent() -> int:
        return plain() + 1

    @engine.query(ttl=10.0)
    def external() -> int:
        return 1

    @engine.query
    def tainted() -> int:
        return external() + plain()

    assert plain_parent() == 3
    assert tainted() == 3
    assert _memo(engine, plain).expires_at is None
    assert _memo(engine, plain_parent).expires_at is None
    assert _memo(engine, external).expires_at == 1010.0
    assert _memo(engine, tainted).expires_at == 1010.0


def test_expired_ttl_reaches_a_caller_that_caught_a_dependency_error(
    clock: _Clock,
) -> None:
    engine = Engine()

    @engine.query(ttl=10.0)
    def external() -> float:
        return clock.now

    @engine.query(cache_exceptions=False)
    def uncached_failure() -> float:
        if external() < 1010.0:
            raise ValueError("too early")
        return external()

    @engine.query
    def cached_failure() -> float:
        if external() < 1010.0:
            raise ValueError("too early")
        return external()

    @engine.query
    def catches_uncached() -> float:
        try:
            return uncached_failure()
        except ValueError:
            return -1.0

    @engine.query
    def catches_cached() -> float:
        try:
            return cached_failure()
        except ValueError:
            return -1.0

    assert catches_uncached() == -1.0
    assert catches_cached() == -1.0
    with pytest.raises(ValueError):
        cached_failure()
    clock.now = 1011.0
    assert catches_uncached() == 1011.0
    assert catches_cached() == 1011.0
    assert cached_failure() == 1011.0


def test_expired_ttl_refreshes_through_memoize_false_and_compute_many(
    clock: _Clock,
) -> None:
    engine = Engine()

    @engine.query(ttl=10.0)
    def external() -> float:
        return clock.now

    @engine.query(memoize=False)
    def unmemoized() -> float:
        return external() + 1

    @engine.query
    def top(i: int) -> float:
        return unmemoized() + i

    try:
        assert engine.compute_many([(top, (0,)), (top, (1,))]) == [1001.0, 1002.0]
        clock.now = 1011.0
        assert engine.compute_many([(top, (0,)), (top, (1,))]) == [1012.0, 1013.0]
    finally:
        engine.shutdown()


def test_expired_ttl_refreshes_async_dependents(clock: _Clock) -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.query(ttl=10.0)
    async def external() -> float:
        runs["external"] += 1
        return clock.now

    @engine.query
    async def parent() -> float:
        runs["parent"] += 1
        return await external() + 1

    @engine.query
    async def top() -> float:
        runs["top"] += 1
        return await parent() + 1

    @engine.query
    def sync_top() -> float:
        return external() + 100

    async def main() -> None:
        assert await top() == 1002.0
        clock.now = 1005.0
        assert await top() == 1002.0
        assert runs == {"external": 1, "parent": 1, "top": 1}
        clock.now = 1011.0
        assert await top() == 1013.0
        assert runs == {"external": 2, "parent": 2, "top": 2}

    asyncio.run(main())
    # A sync query above an async TTL query, called off the loop.
    assert sync_top() == 1111.0
    clock.now = 1030.0
    assert sync_top() == 1130.0


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_expired_ttl_refreshes_dependents_loaded_from_disk(
    clock: _Clock, tmp_path: Path, backend: str
) -> None:
    runs: Counter[str] = Counter()

    def open_engine() -> tuple[Engine, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.query(ttl=10.0)
        def external() -> float:
            runs["external"] += 1
            return clock.now

        @engine.query
        def parent() -> float:
            runs["parent"] += 1
            return external() + 1

        @engine.query
        def top() -> float:
            runs["top"] += 1
            return parent() + 1

        return engine, top

    engine, top = open_engine()
    try:
        assert top() == 1002.0
    finally:
        engine.shutdown()
    assert runs == {"external": 1, "parent": 1, "top": 1}

    # A new session inside the TTL loads everything from disk...
    clock.now = 1004.0
    engine, top = open_engine()
    try:
        assert top() == 1002.0
        assert runs == {"external": 1, "parent": 1, "top": 1}
        # ...and what it loaded still expires with the stored TTL result.
        clock.now = 1011.0
        assert top() == 1013.0
        assert runs == {"external": 2, "parent": 2, "top": 2}
    finally:
        engine.shutdown()

    # A session that starts after the TTL has elapsed loads none of it.
    clock.now = 1030.0
    engine, top = open_engine()
    try:
        assert top() == 1032.0
        assert runs == {"external": 3, "parent": 3, "top": 3}
    finally:
        engine.shutdown()
