"""A commit that fails on an input value applies nothing (D27).

Input values are hashed when the transaction commits. A value that cannot be
encoded used to fail there after the block's collection changes were kept
(and, for named collections, written to disk) but never published to
queries. Every value is now checked before anything is applied.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cascade import (
    CascadeDict,
    CascadeList,
    CascadeSet,
    Engine,
    UnencodableValueError,
)

BACKENDS = ("mdbx", "sqlite")


def _engine_with_input(engine: Engine) -> Any:
    @engine.input
    def x() -> int:
        return 1

    return x


def test_failed_commit_keeps_every_kind_of_collection_and_input() -> None:
    engine = Engine()
    x = _engine_with_input(engine)

    @engine.input
    def y() -> int:
        return 10

    rows = CascadeList(engine, [1])
    tags = CascadeSet(engine, {"a"})
    meta = CascadeDict(engine, {"k": 1})

    @engine.query
    def view() -> Any:
        return (x(), y(), list(rows), sorted(tags), dict(meta))

    before = view()
    revision = engine.revision
    with pytest.raises(UnencodableValueError) as info:
        with engine.transaction():
            y.set(20)
            rows.append(2)
            rows[0] = 0
            tags.add("b")
            tags.discard("a")
            meta["k"] = 2
            meta["new"] = 3
            x.set(object())
    assert "x" in str(info.value)
    assert engine.revision == revision
    assert (list(rows), set(tags), dict(meta)) == ([1], {"a"}, {"k": 1})
    assert view() == before
    # The engine is usable afterwards, and a good commit applies everything.
    with engine.transaction():
        rows.append(5)
        y.set(30)
    assert view() == (1, 30, [1, 5], ["a"], {"k": 1})


def test_failed_commit_is_reported_with_the_original_error() -> None:
    engine = Engine()
    x = _engine_with_input(engine)
    xs = CascadeList(engine, [1])
    with pytest.raises(TypeError) as info:
        with engine.transaction():
            xs.append(2)
            x.set({"inner": object()})
    assert isinstance(info.value, UnencodableValueError)
    assert "object" in str(info.value)
    assert list(xs) == [1]


def test_unhashable_input_argument_fails_the_whole_commit() -> None:
    engine = Engine()

    @engine.input
    def keyed(k: Any) -> int:
        return 0

    xs = CascadeList(engine, [1])

    @engine.query
    def n() -> int:
        return len(xs)

    assert n() == 1
    with pytest.raises(TypeError):
        with engine.transaction():
            xs.append(2)
            keyed.set([1], 5)  # a list cannot be an argument
    assert list(xs) == [1]
    assert n() == 1


def test_later_set_of_a_good_value_does_not_rescue_an_earlier_bad_one() -> None:
    # Last write wins: only the final value of each input is checked.
    engine = Engine()
    x = _engine_with_input(engine)
    xs = CascadeList(engine, [1])
    with engine.transaction():
        xs.append(2)
        x.set(object())
        x.set(7)
    assert (x(), list(xs)) == (7, [1, 2])


def test_collection_created_in_a_failed_commit_reads_as_rolled_back() -> None:
    engine = Engine()
    x = _engine_with_input(engine)
    made: list[CascadeList] = []
    with pytest.raises(UnencodableValueError):
        with engine.transaction():
            made.append(CascadeList(engine, [1, 2, 3]))
            x.set(object())
    assert list(made[0]) == []
    made[0].append(9)

    @engine.query
    def contents() -> list[int]:
        return list(made[0])

    assert contents() == [9]


def test_queries_running_inside_the_block_are_not_disturbed() -> None:
    engine = Engine()
    x = _engine_with_input(engine)
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def total() -> int:
        return sum(v for v in xs) + x()

    assert total() == 4
    with pytest.raises(UnencodableValueError):
        with engine.transaction():
            xs.append(10)
            assert total() == 4  # the engine as it was before the block
            x.set(object())
    assert total() == 4
    xs.append(10)
    assert total() == 14


@pytest.mark.parametrize("backend", BACKENDS)
def test_failed_commit_writes_nothing_for_named_collections(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        x = _engine_with_input(engine)
        xs = CascadeList(engine, [1], name="xs")
        tags = CascadeSet(engine, {"a"}, name="tags")
        with pytest.raises(UnencodableValueError):
            with engine.transaction():
                xs.append(2)
                tags.add("b")
                x.set(object())
        assert list(xs) == [1]
        xs.append(3)
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert list(CascadeList(engine, name="xs")) == [1, 3]
        assert set(CascadeSet(engine, name="tags")) == {"a"}
    finally:
        engine.shutdown()


def test_failed_commit_releases_the_collection_for_other_threads() -> None:
    import threading

    engine = Engine()
    x = _engine_with_input(engine)
    xs = CascadeList(engine, [1])
    with pytest.raises(UnencodableValueError):
        with engine.transaction():
            xs.append(2)
            x.set(object())
    errors: list[BaseException] = []

    def other() -> None:
        try:
            xs.append(3)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    thread = threading.Thread(target=other)
    thread.start()
    thread.join(5)
    assert not thread.is_alive()
    assert errors == []
    assert list(xs) == [1, 3]
