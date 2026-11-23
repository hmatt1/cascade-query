"""Format changes on disk: results are dropped, collections are kept (G20, G26).

Cached query results are derived data, so a change of ``DISK_FORMAT`` empties
them. A named collection's log is the only copy of its contents; it has its
own ``COLLECTION_FORMAT`` and is never emptied. A directory whose collection
data cannot be read is refused and left untouched.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

import cascade._disk_cache as disk_cache_mod
from cascade import (
    CascadeDict,
    CascadeList,
    CascadeSet,
    Engine,
    PersistentCacheError,
)

BACKENDS = ("mdbx", "sqlite")
FORMAT_KEY = disk_cache_mod._FORMAT_KEY  # noqa: SLF001
COLLECTION_KEY = disk_cache_mod._COLLECTION_FORMAT_KEY  # noqa: SLF001


def _session(
    cache: Path, backend: str, runs: dict[str, int]
) -> tuple[Engine, Any, Any]:
    """An engine with the same input and query in every session."""
    engine = Engine(cache_dir=cache, cache_backend=backend)

    @engine.input(name="base")
    def base() -> int:
        return 10

    @engine.query(name="doubled")
    def doubled() -> int:
        runs["doubled"] = runs.get("doubled", 0) + 1
        return base() * 2

    return engine, base, doubled


def _set_stamp(cache: Path, backend: str, key: bytes, value: int | None) -> None:
    """Write (or with None, delete) a format stamp behind the library's back."""
    stamp = None if value is None else value.to_bytes(4, "big")
    if backend == "sqlite":
        conn = sqlite3.connect(cache / "cache.db")
        try:
            if stamp is None:
                conn.execute("delete from sys where k = ?", (key,))
            else:
                conn.execute(
                    "insert or replace into sys (k, v) values (?, ?)", (key, stamp)
                )
            conn.commit()
        finally:
            conn.close()
        return
    # Open the store without the format check, which would refuse or repair
    # the very stamps this helper is planting.
    cls = disk_cache_mod.MdbxDiskCache
    check = cls._ensure_format  # noqa: SLF001
    cls._ensure_format = lambda self: None  # type: ignore[method-assign]  # noqa: SLF001
    try:
        disk = cls(cache, map_size=1 << 24)
    finally:
        cls._ensure_format = check  # type: ignore[method-assign]  # noqa: SLF001
    try:
        with disk._begin(write=True) as txn:  # noqa: SLF001
            if stamp is None:
                disk._sys.delete(txn, key)  # noqa: SLF001
            else:
                disk._sys.put(txn, key, stamp)  # noqa: SLF001
    finally:
        disk.close()


def _fill(cache: Path, backend: str) -> None:
    engine, _, doubled = _session(cache, backend, {})
    try:
        assert doubled() == 20
        CascadeList(engine, [1, 2, 3], name="xs")
        CascadeSet(engine, ["a", "b"], name="tags")
        d = CascadeDict(engine, name="by_id")
        d["z"] = 1
        d["a"] = 2
    finally:
        engine.shutdown()


def _check_collections(engine: Engine) -> None:
    assert list(CascadeList(engine, name="xs")) == [1, 2, 3]
    assert set(CascadeSet(engine, name="tags")) == {"a", "b"}
    assert dict(CascadeDict(engine, name="by_id")) == {"z": 1, "a": 2}


@pytest.mark.parametrize("backend", BACKENDS)
def test_same_format_reuses_results_and_collections(
    tmp_path: Path, backend: str
) -> None:
    _fill(tmp_path, backend)
    runs: dict[str, int] = {}
    engine, _, doubled = _session(tmp_path, backend, runs)
    try:
        assert doubled() == 20
        assert runs == {}
        _check_collections(engine)
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_format_bump_recomputes_results_and_keeps_collections(
    tmp_path: Path, backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _fill(tmp_path, backend)
    monkeypatch.setattr(disk_cache_mod, "DISK_FORMAT", disk_cache_mod.DISK_FORMAT + 1)

    runs: dict[str, int] = {}
    engine, _, doubled = _session(tmp_path, backend, runs)
    try:
        # The stored result is gone, not served and not an error.
        assert doubled() == 20
        assert runs == {"doubled": 1}
        _check_collections(engine)
        xs = CascadeList(engine, name="xs2")
        xs.extend([7, 8])
    finally:
        engine.shutdown()

    # The new format is now the directory's format: results written under
    # it are reused, and collections written before and after are there.
    runs.clear()
    engine, _, doubled = _session(tmp_path, backend, runs)
    try:
        assert doubled() == 20
        assert runs == {}
        _check_collections(engine)
        assert list(CascadeList(engine, name="xs2")) == [7, 8]
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_format_bump_keeps_a_compacted_collection_and_its_tail(
    tmp_path: Path, backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        xs = CascadeList(engine, name="xs", compact_every=8)
        for i in range(30):
            xs.append(i)
        xs.remove(4)
        xs[0] = "first"
        expected = list(xs)
    finally:
        engine.shutdown()

    monkeypatch.setattr(disk_cache_mod, "DISK_FORMAT", disk_cache_mod.DISK_FORMAT + 5)
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        xs = CascadeList(engine, name="xs", compact_every=8)
        assert list(xs) == expected
        xs.append("after")

        @engine.query
        def n() -> int:
            return len(xs)

        assert n() == len(expected) + 1
    finally:
        engine.shutdown()

    # And back again: a downgrade of the result cache keeps them as well.
    monkeypatch.undo()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert list(CascadeList(engine, name="xs")) == [*expected, "after"]
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_directory_written_before_collections_had_a_stamp_is_kept(
    tmp_path: Path, backend: str
) -> None:
    _fill(tmp_path, backend)
    # What the previous release left behind: result stamp 2, no collection stamp.
    _set_stamp(tmp_path, backend, FORMAT_KEY, 2)
    _set_stamp(tmp_path, backend, COLLECTION_KEY, None)

    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        _check_collections(engine)
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_unreadable_collection_format_is_refused_and_nothing_is_dropped(
    tmp_path: Path, backend: str
) -> None:
    _fill(tmp_path, backend)
    _set_stamp(tmp_path, backend, COLLECTION_KEY, disk_cache_mod.COLLECTION_FORMAT + 1)

    for _ in range(2):  # a refused open leaves the directory openable again
        with pytest.raises(PersistentCacheError, match="collection format"):
            Engine(cache_dir=tmp_path, cache_backend=backend)

    _set_stamp(tmp_path, backend, COLLECTION_KEY, disk_cache_mod.COLLECTION_FORMAT)
    runs: dict[str, int] = {}
    engine, _, doubled = _session(tmp_path, backend, runs)
    try:
        _check_collections(engine)
        # The refused opens did not touch the cached results either.
        assert doubled() == 20
        assert runs == {}
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_unstamped_collections_from_an_unknown_format_are_refused(
    tmp_path: Path, backend: str
) -> None:
    _fill(tmp_path, backend)
    _set_stamp(tmp_path, backend, FORMAT_KEY, 999)
    _set_stamp(tmp_path, backend, COLLECTION_KEY, None)

    with pytest.raises(PersistentCacheError, match="disk format 999"):
        Engine(cache_dir=tmp_path, cache_backend=backend)

    # Still all there for the version that can read it.
    _set_stamp(tmp_path, backend, FORMAT_KEY, disk_cache_mod.DISK_FORMAT)
    _set_stamp(tmp_path, backend, COLLECTION_KEY, disk_cache_mod.COLLECTION_FORMAT)
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        _check_collections(engine)
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_unstamped_directory_without_collections_just_recomputes(
    tmp_path: Path, backend: str
) -> None:
    runs: dict[str, int] = {}
    engine, _, doubled = _session(tmp_path, backend, runs)
    assert doubled() == 20
    engine.shutdown()
    _set_stamp(tmp_path, backend, FORMAT_KEY, 999)
    _set_stamp(tmp_path, backend, COLLECTION_KEY, None)

    runs.clear()
    engine, _, doubled = _session(tmp_path, backend, runs)
    try:
        assert doubled() == 20
        assert runs == {"doubled": 1}
    finally:
        engine.shutdown()
