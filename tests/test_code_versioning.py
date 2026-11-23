import asyncio
from collections import Counter
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from cascade.engine import Engine


def test_memory_cache_invalidation_on_code_change():
    engine = Engine()

    call_count = 0

    def my_query(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    query_handle = engine.query(my_query)

    # First call: cache miss
    assert query_handle(5) == 10
    assert call_count == 1

    # Second call: cache hit
    assert query_handle(5) == 10
    assert call_count == 1

    # Now redefine the function with different logic but same name.
    # In a real environment, developers modify the file and hot-reload.
    # We simulate this by defining a new function with the same __qualname__ and __module__.

    def my_query_v2(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 3

    my_query_v2.__module__ = my_query.__module__
    my_query_v2.__qualname__ = my_query.__qualname__

    # Re-register
    query_handle_v2 = engine.query(my_query_v2)

    assert query_handle_v2.id == query_handle.id

    # Third call: cache miss because function hash changed
    assert query_handle_v2(5) == 15
    assert call_count == 2

    # Fourth call: cache hit on new logic
    assert query_handle_v2(5) == 15
    assert call_count == 2


def test_disk_cache_invalidation_on_code_change(tmp_path):
    engine1 = Engine(cache_dir=str(tmp_path))

    call_count_1 = 0

    def disk_query(x: int) -> int:
        nonlocal call_count_1
        call_count_1 += 1
        return x + 10

    qh1 = engine1.query(disk_query)

    # Populate disk cache
    assert qh1(5) == 15
    assert call_count_1 == 1

    engine1.shutdown()

    # Start a new engine pointing to the same disk cache
    engine2 = Engine(cache_dir=str(tmp_path))

    call_count_2 = 0

    def disk_query_v2(x: int) -> int:
        nonlocal call_count_2
        call_count_2 += 1
        return x + 20  # changed logic

    disk_query_v2.__module__ = disk_query.__module__
    disk_query_v2.__qualname__ = disk_query.__qualname__

    qh2 = engine2.query(disk_query_v2)

    # The disk cache has a record for disk_query(5) -> 15.
    # But because disk_query_v2 has a different bytecode hash, it should ignore the disk cache.
    assert qh2(5) == 25
    assert call_count_2 == 1


def test_input_cache_invalidation_on_code_change():
    engine = Engine()

    call_count = 0

    def my_input(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2

    input_handle = engine.input(my_input)

    assert input_handle(5) == 10
    assert call_count == 1

    assert input_handle(5) == 10
    assert call_count == 1

    def my_input_v2(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 3

    my_input_v2.__module__ = my_input.__module__
    my_input_v2.__qualname__ = my_input.__qualname__

    input_handle_v2 = engine.input(my_input_v2)

    # Input has changed hash, should clear from state
    assert input_handle_v2(5) == 15
    assert call_count == 2


# --- Dependents of re-registered code (D22) ----------------------------------
#
# Registering changed code must reach every result computed from the old
# code, also when no input changes in between: the revision stands still, so
# nothing else would make the dependents look again.


def _versions(*fns: Callable[..., Any]) -> None:
    """Give the functions one identity, as edits of one function have."""
    for fn in fns[1:]:
        fn.__module__ = fns[0].__module__
        fn.__qualname__ = fns[0].__qualname__
        fn.__name__ = fns[0].__name__


def _drop_memo(engine: Engine, handle: Any, *args: Any) -> None:
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store.drop_memo_locked(("query", handle.id, args))


def test_reregistered_query_reaches_a_chain_of_dependents_at_the_same_revision() -> (
    None
):
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def a() -> int:
        return 1

    def base_v1() -> int:
        return a()

    def base_v2() -> int:
        return a() + 10

    _versions(base_v1, base_v2)
    base = engine.query(base_v1)

    @engine.query
    def middle() -> int:
        runs["middle"] += 1
        return base() * 2

    @engine.query
    def top() -> int:
        runs["top"] += 1
        return middle() + 1

    a.set(1)  # stored up front, so the first read does not move the revision
    assert top() == 3
    revision = engine.revision
    base = engine.query(base_v2)
    assert top() == 23
    assert middle() == 22
    assert engine.revision == revision
    assert runs == {"middle": 2, "top": 2}


def test_reregistered_query_with_an_equal_result_stops_at_its_first_dependent() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    def base_v1() -> int:
        return 4

    def base_v2() -> int:
        return int("4")

    _versions(base_v1, base_v2)
    base = engine.query(base_v1)

    @engine.query
    def middle() -> int:
        runs["middle"] += 1
        return base() * 2

    @engine.query
    def top() -> int:
        runs["top"] += 1
        return middle() + 1

    assert top() == 9
    base = engine.query(base_v2)
    assert top() == 9
    # The result that depended on the old code is checked by running it; it
    # comes out equal, so nothing above it runs.
    assert runs == {"middle": 2, "top": 1}


def test_reregistered_query_reaches_dependents_read_through_a_snapshot() -> None:
    engine = Engine()

    @engine.input
    def a() -> int:
        return 1

    def base_v1() -> int:
        return a()

    def base_v2() -> int:
        return a() + 10

    _versions(base_v1, base_v2)
    base = engine.query(base_v1)

    @engine.query
    def top() -> int:
        return base() * 2

    a.set(1)
    snapshot = engine.snapshot()
    assert top(snapshot=snapshot) == 2
    a.set(5)
    assert top() == 10
    assert top(snapshot=snapshot) == 2
    base = engine.query(base_v2)
    assert top(snapshot=snapshot) == 22
    assert top() == 30
    assert top(snapshot=snapshot) == 22


@pytest.mark.parametrize("dropped", ["base", "middle"])
def test_reregistered_query_reaches_dependents_after_a_result_was_evicted(
    dropped: str,
) -> None:
    engine = Engine()

    def base_v1() -> int:
        return 1

    def base_v2() -> int:
        return 11

    _versions(base_v1, base_v2)
    base = engine.query(base_v1)

    @engine.query
    def middle() -> int:
        return base() * 2

    @engine.query
    def top() -> int:
        return middle() + 1

    assert top() == 3
    # An evicted result leaves its dependents in place, still pointing at it.
    _drop_memo(engine, base if dropped == "base" else middle)
    base = engine.query(base_v2)
    assert top() == 23


def test_reregistered_input_default_reaches_dependents_at_the_same_revision() -> None:
    engine = Engine()

    def limit_v1() -> int:
        return 1

    def limit_v2() -> int:
        return 50

    _versions(limit_v1, limit_v2)
    limit = engine.input(limit_v1)

    @engine.query
    def reader() -> int:
        return limit() + 1

    @engine.query
    def top() -> int:
        return reader() * 2

    assert top() == 4
    limit = engine.input(limit_v2)
    assert top() == 102
    assert reader() == 51


def test_reregistered_input_default_reaches_dependents_that_never_stored_it() -> None:
    engine = Engine()

    @engine.input
    def other() -> int:
        return 0

    def limit_v1() -> int:
        return 1

    def limit_v2() -> int:
        return 50

    _versions(limit_v1, limit_v2)
    limit = engine.input(limit_v1)

    @engine.query
    def reader() -> int:
        return limit() + 1

    @engine.query
    def top() -> int:
        return reader() * 2

    other.set(1)
    # A read through an explicit snapshot uses the default without storing
    # it, so before and after the change the input has no stored version.
    snapshot = engine.snapshot()
    assert top(snapshot=snapshot) == 4
    limit = engine.input(limit_v2)
    assert top(snapshot=snapshot) == 102


def test_reregistered_query_reaches_async_dependents() -> None:
    engine = Engine()

    def base_v1() -> int:
        return 1

    def base_v2() -> int:
        return 11

    _versions(base_v1, base_v2)
    base = engine.query(base_v1)

    @engine.query
    async def middle() -> int:
        return base() * 2

    @engine.query
    async def top() -> int:
        return await middle() + 1

    async def main() -> tuple[int, int]:
        nonlocal base
        first = await top()
        base = engine.query(base_v2)
        return first, await top()

    assert asyncio.run(main()) == (3, 23)


def test_reregistering_with_another_ttl_applies_it_to_cached_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = [1000.0]
    monkeypatch.setattr("cascade._evaluator.time.time", lambda: now[0])
    engine = Engine()
    runs: Counter[str] = Counter()

    def external() -> float:
        runs["external"] += 1
        return now[0]

    handle = engine.query(ttl=100.0)(external)

    @engine.query
    def parent() -> float:
        return handle() + 1

    assert parent() == 1001.0
    handle = engine.query(ttl=10.0)(external)
    now[0] = 1011.0  # past the new TTL, inside the old one
    assert parent() == 1012.0
    assert runs == {"external": 2}


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_reregistered_query_reaches_dependents_with_a_disk_cache(
    tmp_path: Path, backend: str
) -> None:
    runs: Counter[str] = Counter()

    def base_v1() -> int:
        return 1

    def base_v2() -> int:
        return 11

    _versions(base_v1, base_v2)

    def open_engine(*versions: Callable[..., Any]) -> tuple[Engine, Any, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)
        for version in versions:
            base = engine.query(version)

        @engine.query
        def middle() -> int:
            runs["middle"] += 1
            return base() * 2

        @engine.query
        def top() -> int:
            runs["top"] += 1
            return middle() + 1

        return engine, middle, top

    # Changed in a running session: the results are in memory and on disk.
    engine, middle, top = open_engine(base_v1)
    try:
        assert top() == 3
        engine.flush_disk()
        base = engine.query(base_v2)  # noqa: F841 - registration is the point
    finally:
        engine.shutdown()

    # A later session with only the new code must not load what the old code
    # produced, for the query or for its dependents.
    engine, middle, top = open_engine(base_v2)
    try:
        assert top() == 23
        assert middle() == 22
    finally:
        engine.shutdown()

    # Old code registered and replaced before anything runs: the dependents
    # exist only on disk, written from the new code's result.
    runs.clear()
    engine, middle, top = open_engine(base_v1, base_v2)
    try:
        assert top() == 23
        assert runs == {}
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_reregistered_query_reaches_in_memory_dependents_with_a_disk_cache(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)

    def base_v1() -> int:
        return 1

    def base_v2() -> int:
        return 11

    _versions(base_v1, base_v2)
    base = engine.query(base_v1)

    @engine.query
    def middle() -> int:
        return base() * 2

    @engine.query
    def top() -> int:
        return middle() + 1

    try:
        assert top() == 3
        engine.flush_disk()
        base = engine.query(base_v2)
        assert top() == 23
        # With the in-memory results gone, disk must not bring the old ones back.
        engine.flush_disk()
        _drop_memo(engine, top)
        _drop_memo(engine, middle)
        assert top() == 23
    finally:
        engine.shutdown()
