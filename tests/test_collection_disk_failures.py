"""Named collections when a disk write fails (G25, G26).

A named collection writes its log to disk: each change as it is made, a
transaction's changes in one batch at commit, and a snapshot of the contents
when the log is compacted. These tests make one of those writes fail. The
caller gets ``PersistentCacheError`` (the disk's own, or one that carries the
original error as its cause), nothing is half applied, and the collection
goes on working once the disk does.

The last test is about the write lock (D10): a second object for a name the
engine is already writing is refused when its copy is out of date, and the
refusal must not take the lock away from the engine.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cascade import CascadeList, CascadeSet, Engine, PersistentCacheError

BACKENDS = ("mdbx", "sqlite")

# What the disk raises: its own error (the map is full), or anything else.
FAILURES: dict[str, type[Exception]] = {
    "the cache's own error": PersistentCacheError,
    "an error from below": OSError,
}


class _FailingWrite:
    """Stands in for one write method of the engine's disk object."""

    def __init__(
        self, monkeypatch: pytest.MonkeyPatch, engine: Engine, method: str, exc: Any
    ) -> None:
        disk = engine._disk  # noqa: SLF001
        self.real = getattr(disk, method)
        self.exc = exc
        self.failing = True
        monkeypatch.setattr(disk, method, self)

    def __call__(self, *args: Any) -> Any:
        if self.failing:
            raise self.exc
        return self.real(*args)


def _assert_reported(info: Any, exc: Exception, wrapped_as: str) -> None:
    """The cache's own error arrives as it is, any other one as its cause."""
    if isinstance(exc, PersistentCacheError):
        assert info.value is exc
    else:
        assert info.value.__cause__ is exc
        assert wrapped_as in str(info.value)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("failure", sorted(FAILURES))
def test_commit_whose_disk_write_fails_applies_nothing(
    tmp_path: Path, backend: str, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    exc = FAILURES[failure]("no space left")
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        xs = CascadeList(engine, [1], name="xs")

        @engine.query
        def total() -> int:
            return sum(x for x in xs)

        assert total() == 1
        writes = _FailingWrite(monkeypatch, engine, "collection_append_many", exc)
        with pytest.raises(PersistentCacheError) as info:
            with engine.transaction():
                xs.append(2)
                xs.append(3)
        _assert_reported(info, exc, "'xs' could not persist its changes")
        assert list(xs) == [1]
        assert total() == 1

        # The collection is not left reserved or out of step with its log.
        writes.failing = False
        with engine.transaction():
            xs.append(4)
        xs.append(5)
        assert total() == 10
    finally:
        engine.shutdown()

    with Engine(cache_dir=tmp_path, cache_backend=backend) as reopened:
        assert list(CascadeList(reopened, name="xs")) == [1, 4, 5]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("failure", sorted(FAILURES))
def test_compact_whose_disk_write_fails_loses_nothing(
    tmp_path: Path, backend: str, failure: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    exc = FAILURES[failure]("no space left")
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        tags = CascadeSet(engine, ["a", "b"], name="tags")
        _FailingWrite(monkeypatch, engine, "collection_snapshot", exc)
        with pytest.raises(PersistentCacheError) as info:
            tags.compact()
        _assert_reported(info, exc, "'tags'")
        assert list(tags) == ["a", "b"]
        tags.add("c")  # changes are still written, as diffs
    finally:
        engine.shutdown()

    # No snapshot was written, so the log is still whole on disk.
    with Engine(cache_dir=tmp_path, cache_backend=backend) as reopened:
        tags = CascadeSet(reopened, name="tags")
        assert list(tags) == ["a", "b", "c"]
        tags.compact()  # and it can be compacted where the disk works
    with Engine(cache_dir=tmp_path, cache_backend=backend) as again:
        tags = CascadeSet(again, name="tags")
        assert list(tags) == ["a", "b", "c"]
        assert tags._core.log.base_rev == 3  # noqa: SLF001 - from the snapshot


@pytest.mark.parametrize("backend", BACKENDS)
def test_compact_of_an_element_changed_in_place_into_something_unencodable(
    tmp_path: Path, backend: str
) -> None:
    with Engine(cache_dir=tmp_path, cache_backend=backend) as engine:
        rows = CascadeList(engine, name="rows")
        row = ["a"]
        rows.append(row)
        rows.append(["b"])
        # Behind the collection's back: no diff, and nothing the codec knows.
        row.append(object())
        with pytest.raises(PersistentCacheError, match="'rows' holds a value") as info:
            rows.compact()
        assert isinstance(info.value.__cause__, TypeError)

    # The log holds what was recorded, and no snapshot replaced it.
    with Engine(cache_dir=tmp_path, cache_backend=backend) as reopened:
        assert list(CascadeList(reopened, name="rows")) == [["a"], ["b"]]


@pytest.mark.parametrize("backend", BACKENDS)
def test_out_of_date_second_object_is_refused_and_the_engine_keeps_its_lock(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    other = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        first = CascadeList(engine, name="xs")
        second = CascadeList(engine, name="xs")  # loaded while the log was empty
        first.append(1)  # takes the write lock for this engine

        # ``second`` numbers its changes from an empty log: writing would
        # overwrite what ``first`` wrote.
        with pytest.raises(PersistentCacheError, match="out of date"):
            second.append(2)
        assert list(second) == []

        # The lock was the engine's before ``second`` asked for it, and still
        # is: ``first`` goes on writing, and no other engine can start.
        first.append(3)
        theirs = CascadeList(other, name="xs")
        assert list(theirs) == [1, 3]
        with pytest.raises(PersistentCacheError, match="another Engine"):
            theirs.append(4)
        assert list(theirs) == [1, 3]
    finally:
        other.shutdown()
        engine.shutdown()

    with Engine(cache_dir=tmp_path, cache_backend=backend) as reopened:
        assert list(CascadeList(reopened, name="xs")) == [1, 3]


def test_mdbx_non_map_full_errors_propagate_through_collection_writes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mdbx = pytest.importorskip("mdbx")
    with Engine(cache_dir=tmp_path, cache_backend="mdbx") as engine:
        xs = CascadeList(engine, [1], name="xs")
        disk = engine._disk  # noqa: SLF001
        real_begin = disk._begin  # noqa: SLF001
        boom = mdbx.MDBXErrorExc(-30796, "MDBX_CORRUPTED")

        def fail_write(*, write: bool = False) -> Any:
            if write:
                raise boom
            return real_begin(write=write)

        monkeypatch.setattr(disk, "_begin", fail_write)
        with pytest.raises(mdbx.MDBXErrorExc) as append_info:
            xs.append(2)
        assert append_info.value is boom

        with pytest.raises(mdbx.MDBXErrorExc) as snap_info:
            xs.compact()
        assert snap_info.value is boom


def test_transaction_releases_remaining_collections_if_after_commit_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as engine:
        first = CascadeList(engine, name="first", compact_every=1)
        second = CascadeList(engine, name="second")
        _FailingWrite(monkeypatch, engine, "collection_snapshot", OSError("disk full"))
        with pytest.raises(PersistentCacheError):
            with engine.transaction():
                first.append(1)
                second.append(2)
        # ``second`` had not run ``after_commit()`` yet when ``first`` failed
        # during compaction, so ``release_transaction`` must have freed it.
        second.append(3)
        assert list(second) == [2, 3]
