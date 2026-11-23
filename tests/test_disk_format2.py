"""Disk format 2: single-pass canonical encoding, cached key encodings, and
small values stored inline in their meta record."""

from __future__ import annotations

import dataclasses
import datetime
import sqlite3
from pathlib import Path
from typing import Any, NamedTuple

import msgpack
import pytest

import cascade._canonical as canonical
import cascade._disk_cache as disk_cache_mod
from cascade import Engine


@dataclasses.dataclass(frozen=True)
class Point:
    x: int
    y: tuple[int, ...]


class Pair(NamedTuple):
    left: int
    right: list[Any]


ROUND_TRIP_VALUES: list[Any] = [
    (),
    (1, "a", (2, None)),
    [(1,), [2], []],
    {3, 1, 2},
    {"b", "a"},
    {1, "a", (2, 3), None},
    frozenset({"b", "a"}),
    {"z": 1, "a": (2, {"k": {4, 5}})},
    {1: "x", (2, 3): [4]},
    -(2**80),
    2**64,
    datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
    datetime.date(2024, 1, 2),
    datetime.time(3, 4, 5),
    datetime.timedelta(days=1, seconds=2, microseconds=3),
    datetime.timezone(datetime.timedelta(hours=-5), "EST"),
    Point(1, (2, 3)),
    Pair(1, [Point(2, ())]),
    bytearray(b"ab"),
]


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, bytearray):
        return b == bytes(a)
    if type(a) is not type(b):
        return False
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(_same(a[k], b[k]) for k in a)
    return a == b


# Ids are positional: ``repr`` of a set of strings changes with the hash seed,
# so pytest-xdist workers collected different ids and refused to run.
@pytest.mark.parametrize(
    "value",
    ROUND_TRIP_VALUES,
    ids=[f"{i}-{type(v).__name__}" for i, v in enumerate(ROUND_TRIP_VALUES)],
)
def test_round_trip_preserves_value_and_type(value: Any) -> None:
    assert _same(value, canonical.decode(canonical.encode(value)))


def test_container_subclasses_encode_as_their_base_type() -> None:
    class MyTuple(tuple):  # type: ignore[type-arg]
        pass

    class MyList(list):  # type: ignore[type-arg]
        pass

    class MyDict(dict):  # type: ignore[type-arg]
        pass

    assert canonical.encode(MyTuple((1, 2))) == canonical.encode((1, 2))
    assert canonical.encode(MyList([1, 2])) == canonical.encode([1, 2])
    assert canonical.encode(MyDict(a=1)) == canonical.encode({"a": 1})


def test_exception_round_trip() -> None:
    out = canonical.decode(canonical.encode(ValueError("boom", (1, 2))))
    assert type(out) is ValueError
    assert out.args == ("boom", (1, 2))


def test_encoding_is_independent_of_insertion_order() -> None:
    a = {"x": {3, 1, 2}, "y": {(1, 2): "p", "k": "q"}, "z": frozenset({"b", "a"})}
    b = {"z": frozenset({"a", "b"}), "y": {"k": "q", (1, 2): "p"}, "x": {2, 3, 1}}
    assert canonical.encode(a) == canonical.encode(b)
    mixed_a = {1, "a", (2, 3), None, 1.5}
    mixed_b = {None, 1.5, (2, 3), "a", 1}
    assert canonical.encode(mixed_a) == canonical.encode(mixed_b)


def test_list_tuple_and_marker_shapes_do_not_collide() -> None:
    assert canonical.encode([1]) != canonical.encode((1,))
    assert canonical.encode([]) != canonical.encode(())
    assert canonical.encode({"a": 1}) != canonical.encode({("a",): 1})


def test_value_is_packed_with_a_single_packb_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"n": 0}
    real_packb = msgpack.packb

    def counting_packb(*args: Any, **kwargs: Any) -> Any:
        calls["n"] += 1
        return real_packb(*args, **kwargs)

    monkeypatch.setattr(canonical.msgpack, "packb", counting_packb)
    nested = ("q", (1, (2, (3, [(4,), {"k": (5, 6)}]))), {"a": ("b",)})
    canonical.encode(nested)
    assert calls["n"] == 1


def test_unsupported_type_still_raises_type_error() -> None:
    class Opaque:
        pass

    with pytest.raises(TypeError, match="cannot serialize"):
        canonical.encode((1, Opaque()))


def test_foreign_extension_payload_is_rejected() -> None:
    # Format 1 wrapped tagged values in extensions carrying a packed payload.
    legacy_tuple = bytes.fromhex("c70302920103")
    with pytest.raises(ValueError, match="not disk format 2"):
        canonical.decode(legacy_tuple)
    unknown_code = msgpack.packb(msgpack.ExtType(99, b""))
    with pytest.raises(ValueError, match="not disk format 2"):
        canonical.decode(unknown_code)


# --- engine-level behavior ---------------------------------------------------


def _blob_count(cache: Path) -> int:
    conn = sqlite3.connect(cache / "cache.db")
    try:
        return conn.execute("select count(*) from blobs").fetchone()[0]
    finally:
        conn.close()


def _build(cache: Path, runs: dict[str, int], size: int) -> tuple[Engine, Any, Any]:
    engine = Engine(cache_dir=cache, cache_backend="sqlite")

    @engine.input
    def seed() -> int:
        return 3

    @engine.query
    def payload() -> str:
        runs["payload"] = runs.get("payload", 0) + 1
        return "x" * size * seed()

    @engine.query
    def length() -> int:
        runs["length"] = runs.get("length", 0) + 1
        return len(payload())

    return engine, payload, length


def _close(engine: Engine) -> None:
    engine.shutdown()
    assert engine._disk is not None  # noqa: SLF001
    engine._disk.close()  # noqa: SLF001


@pytest.mark.parametrize(
    ("size", "expected_blobs"),
    [(4, 0), (disk_cache_mod.INLINE_VALUE_MAX_BYTES, 1)],
    ids=["small-values-inline", "large-value-in-blob-store"],
)
def test_small_values_are_inlined_and_large_ones_use_blobs(
    tmp_path: Path, size: int, expected_blobs: int
) -> None:
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}
    engine_a, _, length_a = _build(cache, runs, size)
    assert length_a() == size * 3
    _close(engine_a)
    assert _blob_count(cache) == expected_blobs

    runs.clear()
    engine_b, payload_b, length_b = _build(cache, runs, size)
    assert length_b() == size * 3
    assert payload_b() == "x" * size * 3
    assert runs == {}
    _close(engine_b)


def test_inline_hit_never_reads_the_blob_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}
    engine_a, _, length_a = _build(cache, runs, 4)
    assert length_a() == 12
    _close(engine_a)

    runs.clear()
    engine_b, _, length_b = _build(cache, runs, 4)

    def no_blob_reads(value_hash: str) -> bytes | None:
        raise AssertionError(f"unexpected blob read for {value_hash}")

    monkeypatch.setattr(engine_b._disk, "load_blob", no_blob_reads)  # noqa: SLF001
    assert length_b() == 12
    assert runs == {}
    _close(engine_b)


def test_corrupt_inline_value_falls_back_to_recompute(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}
    engine_a, _, length_a = _build(cache, runs, 4)
    assert length_a() == 12
    _close(engine_a)

    conn = sqlite3.connect(cache / "cache.db")
    rows = conn.execute("select k, v from meta").fetchall()
    assert rows
    for key, raw in rows:
        record = canonical.decode(bytes(raw))
        assert isinstance(record["value"], bytes)
        record["value"] = canonical.encode("tampered")
        conn.execute(
            "update meta set v = ? where k = ?", (canonical.encode(record), key)
        )
    conn.commit()
    conn.close()

    runs.clear()
    engine_b, _, length_b = _build(cache, runs, 4)
    assert length_b() == 12
    assert runs == {"payload": 1, "length": 1}
    _close(engine_b)


def test_each_key_is_encoded_once_per_session(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = Engine(cache_dir=tmp_path / "cache", cache_backend="sqlite")

    @engine.input
    def leaf(i: int) -> int:
        return i

    @engine.query
    def mid(i: int) -> int:
        return leaf(i) + 1

    @engine.query
    def top(i: int) -> int:
        return mid(i) + mid(i + 1)

    encoded_args: list[Any] = []
    real_encode = canonical.encode

    def spy(value: Any) -> bytes:
        if isinstance(value, tuple):
            encoded_args.append(value)
        return real_encode(value)

    monkeypatch.setattr(canonical, "encode", spy)
    assert [top(i) for i in range(5)] == [2 * i + 3 for i in range(5)]
    engine.flush_disk()
    # mid(i) is hydrated, recorded as a dependency by top(i) and top(i - 1),
    # and persisted; its args tuple must still be encoded only once per key.
    evaluator = engine._internals._evaluator  # noqa: SLF001
    assert len(encoded_args) == len(evaluator._key_blobs)  # noqa: SLF001
    _close(engine)


def test_key_and_args_caches_are_bounded(tmp_path: Path) -> None:
    engine = Engine(cache_dir=tmp_path / "cache", cache_backend="sqlite", max_entries=1)
    evaluator = engine._internals._evaluator  # noqa: SLF001
    limit = evaluator._key_cache_limit()  # noqa: SLF001
    for i in range(limit + 10):
        args_blob, _ = evaluator._key_blobs_for(("query", "q", (i,)))  # noqa: SLF001
        assert evaluator._args_from_blob(args_blob) == (i,)  # noqa: SLF001
    assert len(evaluator._key_blobs) <= limit  # noqa: SLF001
    assert len(evaluator._args_by_blob) <= limit  # noqa: SLF001
    # A blob that is not a tuple is returned for the caller to reject, uncached.
    not_args = canonical.encode([1])
    assert evaluator._args_from_blob(not_args) == [1]  # noqa: SLF001
    assert not_args not in evaluator._args_by_blob  # noqa: SLF001
    _close(engine)


@pytest.mark.parametrize("backend", ["sqlite", "mdbx"])
def test_format_change_keeps_collection_logs(tmp_path: Path, backend: str) -> None:
    cache = tmp_path / "cache"

    def open_disk() -> Any:
        return disk_cache_mod.DiskCache(cache, map_size=1 << 24, cache_backend=backend)

    disk = open_disk()
    disk.collection_append_many("col", [(1, {"diff": (1, 2)})])
    assert disk.collection_load("col") == (None, [(1, {"diff": (1, 2)})])
    disk.close()

    # The result-cache format number does not cover collection logs: they
    # are data nothing can recompute, with a format number of their own.
    old_format = disk_cache_mod.DISK_FORMAT
    disk_cache_mod.DISK_FORMAT = old_format + 1
    try:
        disk = open_disk()
        assert disk.collection_load("col") == (None, [(1, {"diff": (1, 2)})])
        disk.close()
    finally:
        disk_cache_mod.DISK_FORMAT = old_format


def test_unknown_cache_backend_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown cache_backend 'lmdb'"):
        Engine(cache_dir=tmp_path / "cache", cache_backend="lmdb")
