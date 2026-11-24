"""A collection changed behind its back says so (D28).

C code that writes a list, set or dict subclass's storage directly logs no
diff. Cascade cannot intercept it; it notices on the next use of the
collection (a read, inside a query or not, a mutation, a rewritten
pipeline) that the storage's size no longer matches what it recorded,
undoes the change and raises ``CollectionBypassedError``. A change that
keeps the size is not noticed: that limit is pinned at the end.
"""

from __future__ import annotations

import bisect
import heapq
import threading
from pathlib import Path
from typing import Any, Callable

import pytest

from cascade import (
    CascadeDict,
    CascadeList,
    CascadeSet,
    CollectionBypassedError,
    Engine,
)

BACKENDS = ("mdbx", "sqlite")

# (make collection, bypass it, contents before the bypass)
SIZE_CHANGES: dict[str, tuple[Callable[[Engine], Any], Callable[[Any], Any], Any]] = {
    "heappush": (
        lambda e: CascadeList(e, [5, 3]),
        lambda c: heapq.heappush(c, 1),
        [5, 3],
    ),
    "heappop": (lambda e: CascadeList(e, [1, 3]), lambda c: heapq.heappop(c), [1, 3]),
    "list.append": (lambda e: CascadeList(e, [1]), lambda c: list.append(c, 2), [1]),
    "list.pop": (lambda e: CascadeList(e, [1, 2]), lambda c: list.pop(c), [1, 2]),
    "list.extend": (
        lambda e: CascadeList(e, [1]),
        lambda c: list.extend(c, [2, 3]),
        [1],
    ),
    "set.add": (lambda e: CascadeSet(e, [1]), lambda c: set.add(c, 2), [1]),
    "set.discard": (
        lambda e: CascadeSet(e, [1, 2]),
        lambda c: set.discard(c, 1),
        [1, 2],
    ),
    "dict.__setitem__": (
        lambda e: CascadeDict(e, {"a": 1}),
        lambda c: dict.__setitem__(c, "b", 2),
        {"a": 1},
    ),
    "dict.pop": (
        lambda e: CascadeDict(e, {"a": 1, "b": 2}),
        lambda c: dict.pop(c, "a"),
        {"a": 1, "b": 2},
    ),
}

USES: dict[str, Callable[[Any], Any]] = {
    "len": len,
    "iter": list,
    "repr": repr,
    "contains": lambda c: 1 in c,
    "copy": lambda c: c.copy(),
    "equality": lambda c: c == c.copy(),
}


def _contents(c: Any) -> Any:
    return dict(c) if isinstance(c, dict) else list(c)


def _mutate(c: Any) -> None:
    if isinstance(c, CascadeList):
        c.append(99)
    elif isinstance(c, CascadeSet):
        c.add(99)
    else:
        c["z"] = 99


@pytest.mark.parametrize("case", sorted(SIZE_CHANGES))
@pytest.mark.parametrize("use", sorted(USES))
def test_next_read_outside_a_query_raises_and_undoes(case: str, use: str) -> None:
    make, bypass, before = SIZE_CHANGES[case]
    c = make(Engine())
    bypass(c)
    with pytest.raises(CollectionBypassedError, match="without going through"):
        USES[use](c)
    assert _contents(c) == before
    USES[use](c)  # repaired: works again


@pytest.mark.parametrize("case", sorted(SIZE_CHANGES))
def test_next_mutation_raises_and_changes_nothing(case: str) -> None:
    make, bypass, before = SIZE_CHANGES[case]
    engine = Engine()
    c = make(engine)

    @engine.query
    def contents() -> Any:
        return _contents(c)

    assert contents() == before
    bypass(c)
    with pytest.raises(CollectionBypassedError):
        _mutate(c)
    assert _contents(c) == before
    assert contents() == before
    _mutate(c)  # the collection works normally again
    assert contents() == _contents(c)
    assert 99 in (c.values() if isinstance(c, dict) else c)


@pytest.mark.parametrize("case", sorted(SIZE_CHANGES))
def test_query_that_runs_raises_once_and_is_not_cached(case: str) -> None:
    make, bypass, before = SIZE_CHANGES[case]
    engine = Engine()
    c = make(engine)

    @engine.input
    def salt() -> int:
        return 0

    runs: list[int] = []

    @engine.query
    def contents() -> Any:
        runs.append(salt())
        return _contents(c)

    assert contents() == before
    bypass(c)
    salt.set(1)  # makes the query run again and read the collection
    with pytest.raises(CollectionBypassedError):
        contents()
    # Not cached: the collection was repaired, and the next call reads it.
    assert contents() == before
    assert runs == [0, 1, 1]


def test_rewritten_pipeline_notices() -> None:
    engine = Engine()
    xs = CascadeList(engine, [3, 1])

    @engine.input
    def salt() -> int:
        return 0

    @engine.query
    def total() -> int:
        return sum(x * 2 for x in xs) + salt()

    assert getattr(total.raw, "__cascade_rewritten__", False)
    assert total() == 8
    heapq.heappush(xs, 0)
    salt.set(1)
    with pytest.raises(CollectionBypassedError):
        total()
    assert total() == 9
    xs.append(5)
    assert total() == 19


def test_bisect_insort_goes_through_insert_and_is_tracked() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 3, 5])

    @engine.query
    def items() -> list[int]:
        return list(xs)

    assert items() == [1, 3, 5]
    bisect.insort(xs, 4)
    bisect.insort_left(xs, 0)
    assert items() == [0, 1, 3, 4, 5]


def test_bypass_inside_a_transaction_rolls_the_block_back() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1])

    @engine.query
    def items() -> list[int]:
        return list(xs)

    assert items() == [1]
    with pytest.raises(CollectionBypassedError):
        with engine.transaction():
            xs.append(2)
            heapq.heappush(xs, 0)
            xs.append(3)
    assert list(xs) == [1]
    assert items() == [1]


@pytest.mark.parametrize("backend", BACKENDS)
def test_named_collection_on_disk_keeps_what_was_recorded(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        xs = CascadeList(engine, [4, 2], name="heap")
        heapq.heappush(xs, 1)
        with pytest.raises(CollectionBypassedError):
            xs.append(9)
        xs.append(8)
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert list(CascadeList(engine, name="heap")) == [4, 2, 8]
    finally:
        engine.shutdown()


def test_concurrent_writers_and_readers_see_no_false_alarm() -> None:
    engine = Engine()
    xs = CascadeList(engine)
    s = CascadeSet(engine)
    d = CascadeDict(engine)
    errors: list[BaseException] = []
    barrier = threading.Barrier(6)

    @engine.query
    def sizes() -> tuple[int, int, int]:
        return (len(xs), len(s), len(d))

    @engine.query
    def contents() -> tuple[int, int, int]:
        # Iterated inside a query: each read is of one recorded state.
        return (len(list(xs)), len(list(s)), len(dict(d)))

    def writer(k: int) -> None:
        barrier.wait()
        try:
            for i in range(300):
                xs.append(i)
                s.add((k, i))
                d[(k, i)] = i
                if i % 4 == 0:
                    xs.pop()
                    s.discard((k, i))
                    d.pop((k, i))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def reader() -> None:
        barrier.wait()
        try:
            for _ in range(300):
                # Outside a query only sizes: iterating live storage while
                # other threads change it races as it does for the builtins.
                len(xs), len(s), len(d)
                sizes()
                contents()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(k,)) for k in range(3)]
    threads += [threading.Thread(target=reader) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert not any(thread.is_alive() for thread in threads)
    assert errors == []
    assert sizes() == (len(xs), len(s), len(d)) == (675, 675, 675)


# --- the limit ---------------------------------------------------------------

SAME_SIZE_CHANGES: dict[str, tuple[Callable[[Engine], Any], Callable[[Any], Any]]] = {
    "heapq.heapify": (lambda e: CascadeList(e, [3, 1, 2]), heapq.heapify),
    "heapq.heapreplace": (
        lambda e: CascadeList(e, [1, 3]),
        lambda c: heapq.heapreplace(c, 5),
    ),
    "heapq.heappushpop": (
        lambda e: CascadeList(e, [1, 3]),
        lambda c: heapq.heappushpop(c, 5),
    ),
    "list.sort": (lambda e: CascadeList(e, [3, 1, 2]), list.sort),
    "list.reverse": (lambda e: CascadeList(e, [3, 1, 2]), list.reverse),
    "list.__setitem__": (
        lambda e: CascadeList(e, [3, 1]),
        lambda c: list.__setitem__(c, 0, 9),
    ),
    "dict.__setitem__ of a present key": (
        lambda e: CascadeDict(e, {"a": 1}),
        lambda c: dict.__setitem__(c, "a", 2),
    ),
    "set.remove then set.add": (
        lambda e: CascadeSet(e, [1]),
        lambda c: (set.remove(c, 1), set.add(c, 2)),
    ),
}


@pytest.mark.parametrize("case", sorted(SAME_SIZE_CHANGES))
def test_limit_a_change_that_keeps_the_size_is_not_noticed(case: str) -> None:
    make, bypass = SAME_SIZE_CHANGES[case]
    engine = Engine()
    c = make(engine)

    @engine.query
    def contents() -> Any:
        return _contents(c)

    before = contents()
    bypass(c)
    # Nothing is raised, and the cached result is what it was: queries
    # never learn of the change. Documented in G21.
    assert contents() == before
    len(c)
    _mutate(c)


def test_limit_a_cached_result_is_served_until_the_collection_is_used() -> None:
    engine = Engine()
    xs = CascadeList(engine, [5, 3])

    @engine.query
    def n() -> int:
        return len(xs)

    assert n() == 2
    heapq.heappush(xs, 1)
    assert n() == 2  # a cache hit: nothing has looked at the collection yet
    with pytest.raises(CollectionBypassedError):
        len(xs)
    assert n() == 2 == len(xs)
