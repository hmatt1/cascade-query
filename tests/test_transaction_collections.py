"""A transaction applies its collection changes only if it succeeds.

Input sets inside a failed transaction were always dropped; collection
changes used to stay in the object (invisible to queries), get committed by
the next unrelated change, and reach disk for named collections.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cascade import CascadeDict, CascadeList, CascadeSet, Engine, PersistentCacheError


class Abort(Exception):
    pass


def _abort(engine: Engine, body: Any) -> None:
    with pytest.raises(Abort):
        with engine.transaction():
            body()
            raise Abort


def _disk_engine(cache_dir: Path) -> Engine:
    return Engine(cache_dir=cache_dir, cache_backend="sqlite")


def test_failed_transaction_rolls_back_collections_and_inputs() -> None:
    engine = Engine()
    tags = CascadeSet(engine, [1, 2])
    rows = CascadeList(engine, ["a", "b", "c"])
    meta = CascadeDict(engine, {"k": 1})

    @engine.input
    def factor() -> int:
        return 1

    @engine.query
    def total() -> int:
        return sum(v * factor() for v in tags)

    assert total() == 3
    heads = [c._core.log.head for c in (tags, rows, meta)]  # noqa: SLF001

    def body() -> None:
        factor.set(100)
        tags.add(50)
        tags.discard(1)
        rows.insert(1, "x")
        rows[0] = "z"
        rows.remove("c")
        meta["k"] = 2
        meta["new"] = 3
        assert 50 in tags and rows == ["z", "x", "b"]  # visible inside the block

    _abort(engine, body)
    assert set(tags) == {1, 2}
    assert list(rows) == ["a", "b", "c"]
    assert dict(meta) == {"k": 1}
    assert factor() == 1
    assert total() == 3
    assert [c._core.log.head for c in (tags, rows, meta)] == heads  # noqa: SLF001

    # A later change does not bring the aborted ones back.
    tags.add(7)
    rows.append("d")
    assert total() == 10
    assert list(rows) == ["a", "b", "c", "d"]
    rows.insert(0, "first")
    del rows[1]
    assert list(rows) == ["first", "b", "c", "d"]


def test_successful_transaction_commits_collection_changes() -> None:
    engine = Engine()
    tags = CascadeSet(engine, [1])

    @engine.query
    def total() -> int:
        return sum(v for v in tags)

    assert total() == 1
    revision = engine.revision
    with engine.transaction():
        tags.add(2)
        tags.add(3)
    assert engine.revision == revision + 1
    assert total() == 6


def test_named_collection_writes_to_disk_only_on_commit(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    engine = _disk_engine(cache_dir)
    tags = CascadeSet(engine, [1, 2], name="tags")
    disk = engine._disk  # noqa: SLF001
    _, tail_before = disk.collection_load("tags")

    _abort(engine, lambda: tags.add(50))

    with engine.transaction():
        tags.add(3)
        tags.discard(1)
        _, tail_during = disk.collection_load("tags")
        assert len(tail_during) == len(tail_before)  # held until commit
    assert set(tags) == {2, 3}
    engine.flush_disk()
    engine.shutdown()

    reloaded_engine = _disk_engine(cache_dir)
    reloaded = CascadeSet(reloaded_engine, name="tags")
    assert set(reloaded) == {2, 3}
    reloaded_engine.shutdown()


def test_unpersistable_value_in_transaction_fails_at_the_call(tmp_path: Path) -> None:
    engine = _disk_engine(tmp_path / "cache")
    rows = CascadeList(engine, [1], name="rows")
    with pytest.raises(PersistentCacheError):
        with engine.transaction():
            rows.append(2)
            try:
                rows.append(object())
            except PersistentCacheError:
                assert list(rows) == [1, 2]  # only the failed call was undone
                raise
    assert list(rows) == [1]
    rows.append(3)
    assert list(rows) == [1, 3]
    engine.shutdown()


def test_aborted_transaction_does_not_trigger_disk_compaction(tmp_path: Path) -> None:
    cache_dir = tmp_path / "cache"
    engine = _disk_engine(cache_dir)
    tags = CascadeSet(engine, name="tags")
    tags._core.compact_every = 4  # noqa: SLF001

    def body() -> None:
        for i in range(10):
            tags.add(i)

    _abort(engine, body)
    assert set(tags) == set()
    engine.flush_disk()
    engine.shutdown()

    reloaded_engine = _disk_engine(cache_dir)
    assert set(CascadeSet(reloaded_engine, name="tags")) == set()
    reloaded_engine.shutdown()


def test_transaction_object_cannot_be_entered_twice() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 0

    xs = CascadeList(engine, [])
    tx = engine.transaction()
    with tx:
        x.set(1)
        xs.append("first")
    x.set(2)
    xs.append("between")

    with pytest.raises(RuntimeError, match="single-use"):
        with tx:
            raise AssertionError("the block must not run")  # pragma: no cover
    # Nothing was replayed (x would be 1 again) and nothing was rolled back.
    assert x() == 2
    assert list(xs) == ["first", "between"]
    # The refused entry left no transaction open in this context.
    with engine.transaction():
        x.set(3)
    assert x() == 3


def test_transaction_object_cannot_be_reused_after_a_rollback() -> None:
    engine = Engine()
    xs = CascadeList(engine, ["base"])
    tx = engine.transaction()
    with pytest.raises(ValueError, match="abort"):
        with tx:
            xs.append("doomed")
            raise ValueError("abort")
    xs.append("kept")
    with pytest.raises(RuntimeError, match="single-use"):
        tx.__enter__()
    assert list(xs) == ["base", "kept"]


def test_transaction_refused_as_nested_is_still_usable_later() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 0

    inner = engine.transaction()
    with engine.transaction():
        with pytest.raises(RuntimeError, match="Nested"):
            inner.__enter__()
    with inner:
        x.set(7)
    assert x() == 7
