from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, NamedTuple

import pytest

import cascade._canonical as canonical
import cascade._disk_cache as disk_cache_mod
from cascade import Engine, PersistentCacheError


@dataclass(frozen=True)
class _Ast:
    name: str
    children: tuple[int, ...]


class _Point(NamedTuple):
    x: int
    y: int


# --- canonical serde ---


def test_encode_is_deterministic_for_dict_and_set_ordering() -> None:
    assert canonical.encode({"z": 1, "a": 2}) == canonical.encode({"a": 2, "z": 1})
    assert canonical.encode({3, 1, 2}) == canonical.encode({2, 3, 1})
    assert canonical.encode(frozenset(("b", "a"))) == canonical.encode(frozenset(("a", "b")))
    assert canonical.value_digest({"k": [1, 2]}) == canonical.value_digest({"k": [1, 2]})
    assert canonical.value_digest([1, 2]) != canonical.value_digest([2, 1])


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        False,
        0,
        -17,
        2**64 - 1,
        -(2**63),
        2**100,
        -(2**100),
        3.5,
        float("inf"),
        "text",
        b"raw",
        (1, "a", (2,)),
        [1, [2, 3]],
        {"a": 1, "b": {"c": (1, 2)}},
        {1: "int key", (2, 3): "tuple key"},
        {"s", "e", "t"},
        frozenset((1, 2)),
        _Ast("root", (1, 2)),
        _Point(3, 4),
        {"nested": [_Ast("leaf", ()), {_Point(0, 0): {"deep"}}]},
    ],
)
def test_round_trip(value: Any) -> None:
    assert canonical.decode(canonical.encode(value)) == value


def test_round_trip_preserves_container_types() -> None:
    out = canonical.decode(canonical.encode((1, [2], {3}, frozenset((4,)))))
    assert type(out) is tuple
    assert type(out[1]) is list
    assert type(out[2]) is set
    assert type(out[3]) is frozenset
    ast = canonical.decode(canonical.encode(_Ast("n", (1,))))
    assert type(ast) is _Ast
    point = canonical.decode(canonical.encode(_Point(1, 2)))
    assert type(point) is _Point


def test_bytearray_collapses_to_bytes_and_nan_round_trips() -> None:
    assert canonical.decode(canonical.encode(bytearray(b"ab"))) == b"ab"
    out = canonical.decode(canonical.encode(float("nan")))
    assert out != out  # NaN


def test_unsupported_type_raises_type_error_with_guidance() -> None:
    with pytest.raises(TypeError, match="persistent cache"):
        canonical.encode(object())


def test_entry_key_is_deterministic_and_arg_sensitive() -> None:
    args_a = canonical.encode(("pkg",))
    args_b = canonical.encode(("other",))
    key_1 = disk_cache_mod.entry_key("query", "mod:fn", args_a)
    key_2 = disk_cache_mod.entry_key("query", "mod:fn", args_a)
    assert key_1 == key_2
    assert key_1 != disk_cache_mod.entry_key("query", "mod:fn", args_b)
    assert key_1 != disk_cache_mod.entry_key("query", "mod:other_fn", args_a)


# --- missing dependency errors ---


def test_missing_lmdb_raises_with_install_hint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(disk_cache_mod, "lmdb", None)
    with pytest.raises(PersistentCacheError, match="pip install lmdb"):
        Engine(cache_dir=tmp_path / "cache")


def test_missing_msgpack_raises_with_install_hint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(canonical, "msgpack", None)
    with pytest.raises(PersistentCacheError, match="pip install msgpack"):
        Engine(cache_dir=tmp_path / "cache")


# --- helpers for cross-session scenarios ---


def _build_file_pipeline(cache_dir: Path, runs: dict[str, int]) -> tuple[Engine, Any, Any, Any]:
    engine = Engine(cache_dir=cache_dir)
    warnings = engine.accumulator("warnings")

    @engine.input
    def source_text(path: str) -> str:
        return Path(path).read_text()

    @engine.query
    def normalized(path: str) -> str:
        runs["normalized"] = runs.get("normalized", 0) + 1
        text = source_text(path)
        if "\t" in text:
            warnings.push({"code": "tabs", "path": path})
        return " ".join(text.split())

    @engine.query
    def word_count(path: str) -> int:
        runs["word_count"] = runs.get("word_count", 0) + 1
        return len(normalized(path).split())

    return engine, source_text, normalized, word_count


def _session(cache_dir: Path, runs: dict[str, int]) -> tuple[Engine, Any, Any, Any]:
    return _build_file_pipeline(cache_dir, runs)


# --- cross-session behavior ---


def test_cross_session_hit_skips_all_query_bodies(tmp_path: Path) -> None:
    data = tmp_path / "a.txt"
    data.write_text("one two three")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    assert word_count_a(str(data)) == 3
    assert runs == {"normalized": 1, "word_count": 1}
    engine_a.shutdown()

    engine_b, _, _, word_count_b = _session(cache, runs)
    assert word_count_b(str(data)) == 3
    assert runs == {"normalized": 1, "word_count": 1}
    events = [t.event for t in engine_b.traces()]
    assert events.count("disk_hit") == 2
    engine_b.shutdown()


def test_file_edit_invalidates_only_its_chain(tmp_path: Path) -> None:
    file_a = tmp_path / "a.txt"
    file_b = tmp_path / "b.txt"
    file_a.write_text("alpha beta")
    file_b.write_text("gamma")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    assert word_count_a(str(file_a)) == 2
    assert word_count_a(str(file_b)) == 1
    engine_a.shutdown()

    file_a.write_text("alpha beta gamma delta")

    runs.clear()
    engine_b, _, _, word_count_b = _session(cache, runs)
    assert word_count_b(str(file_a)) == 4
    assert word_count_b(str(file_b)) == 1
    assert runs == {"normalized": 1, "word_count": 1}
    engine_b.shutdown()


def test_early_cutoff_survives_across_sessions(tmp_path: Path) -> None:
    data = tmp_path / "a.txt"
    data.write_text("one two\tthree")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    assert word_count_a(str(data)) == 3
    engine_a.shutdown()

    # Whitespace-only edit: normalized output is unchanged, so word_count must
    # keep hitting even though normalized recomputes.
    data.write_text("one    two\tthree")

    runs.clear()
    engine_b, _, _, word_count_b = _session(cache, runs)
    assert word_count_b(str(data)) == 3
    assert runs == {"normalized": 1}
    engine_b.shutdown()

    # The refreshed fingerprint was re-persisted; a third session is a pure hit.
    runs.clear()
    engine_c, _, _, word_count_c = _session(cache, runs)
    assert word_count_c(str(data)) == 3
    assert runs == {}
    engine_c.shutdown()


def test_accumulator_effects_replay_on_disk_hit(tmp_path: Path) -> None:
    data = tmp_path / "a.txt"
    data.write_text("uses\ttabs")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    effects_a: dict[str, list[Any]] = {}
    assert word_count_a(str(data), effects=effects_a) == 2
    assert effects_a == {"warnings": [{"code": "tabs", "path": str(data)}]}
    engine_a.shutdown()

    runs.clear()
    engine_b, _, _, word_count_b = _session(cache, runs)
    effects_b: dict[str, list[Any]] = {}
    assert word_count_b(str(data), effects=effects_b) == 2
    assert runs == {}
    assert effects_b == effects_a
    engine_b.shutdown()


def test_set_based_inputs_hit_across_sessions_when_values_match(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    def build() -> tuple[Engine, Any, Any]:
        engine = Engine(cache_dir=cache)

        @engine.input
        def config() -> dict:
            return {}

        @engine.query
        def strictness() -> bool:
            runs["strictness"] = runs.get("strictness", 0) + 1
            return bool(config().get("strict", False))

        return engine, config, strictness

    engine_a, config_a, strictness_a = build()
    config_a.set({"strict": True, "version": 1})
    assert strictness_a() is True
    engine_a.shutdown()

    runs.clear()
    engine_b, config_b, strictness_b = build()
    config_b.set({"version": 1, "strict": True})  # same value, different key order
    assert strictness_b() is True
    assert runs == {}
    engine_b.shutdown()

    runs.clear()
    engine_c, config_c, strictness_c = build()
    config_c.set({"strict": False, "version": 1})
    assert strictness_c() is False
    assert runs == {"strictness": 1}
    engine_c.shutdown()


def test_in_session_invalidation_still_works_after_hydration(tmp_path: Path) -> None:
    data = tmp_path / "a.txt"
    data.write_text("one two")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    assert word_count_a(str(data)) == 2
    engine_a.shutdown()

    runs.clear()
    engine_c, source_c, _, word_count_c = _session(cache, runs)
    assert word_count_c(str(data)) == 2
    assert runs == {}
    # Hydrated entries must react to in-session input updates like normal ones.
    source_c.set(str(data), "a b c d")
    assert word_count_c(str(data)) == 4
    assert runs == {"normalized": 1, "word_count": 1}
    engine_c.shutdown()


def test_disk_serves_entries_evicted_from_memory(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}
    engine = Engine(cache_dir=cache, max_entries=1)

    @engine.input
    def seed(name: str) -> int:
        return {"a": 1, "b": 2}[name]

    @engine.query
    def doubled(name: str) -> int:
        runs[name] = runs.get(name, 0) + 1
        return seed(name) * 2

    assert doubled("a") == 2
    assert doubled("b") == 4  # evicts "a" from the in-memory LRU
    assert doubled("a") == 2  # rehydrated from disk instead of recomputed
    assert runs == {"a": 1, "b": 1}
    events = [t.event for t in engine.traces()]
    assert "disk_hit" in events
    engine.shutdown()


def test_clear_disk_cache_forces_recompute_next_session(tmp_path: Path) -> None:
    data = tmp_path / "a.txt"
    data.write_text("one")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    assert word_count_a(str(data)) == 1
    engine_a.clear_disk_cache()
    engine_a.shutdown()

    runs.clear()
    engine_b, _, _, word_count_b = _session(cache, runs)
    assert word_count_b(str(data)) == 1
    assert runs == {"normalized": 1, "word_count": 1}
    engine_b.shutdown()


def test_clear_disk_cache_without_cache_dir_raises() -> None:
    engine = Engine()
    with pytest.raises(PersistentCacheError, match="cache_dir"):
        engine.clear_disk_cache()
    engine.shutdown()


def test_corrupt_blob_falls_back_to_recompute(tmp_path: Path) -> None:
    data = tmp_path / "a.txt"
    data.write_text("one two")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    assert word_count_a(str(data)) == 2
    engine_a.shutdown()

    disk = disk_cache_mod.DiskCache(cache, map_size=1 << 24)
    with disk._env.begin(write=True) as txn:  # noqa: SLF001
        cursor = txn.cursor(db=disk._blobs)  # noqa: SLF001
        keys = [bytes(key) for key, _ in cursor]
        for key in keys:
            txn.put(key, b"corrupted", db=disk._blobs)  # noqa: SLF001
    disk.close()

    runs.clear()
    engine_b, _, _, word_count_b = _session(cache, runs)
    assert word_count_b(str(data)) == 2
    assert runs == {"normalized": 1, "word_count": 1}
    engine_b.shutdown()


def test_format_bump_wipes_stale_cache(tmp_path: Path) -> None:
    data = tmp_path / "a.txt"
    data.write_text("one two")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    assert word_count_a(str(data)) == 2
    engine_a.shutdown()

    disk = disk_cache_mod.DiskCache(cache, map_size=1 << 24)
    with disk._env.begin(write=True) as txn:  # noqa: SLF001
        txn.put(b"format", (999).to_bytes(4, "big"), db=disk._sys)  # noqa: SLF001
    disk.close()

    runs.clear()
    engine_b, _, _, word_count_b = _session(cache, runs)
    assert word_count_b(str(data)) == 2
    assert runs == {"normalized": 1, "word_count": 1}
    engine_b.shutdown()


def test_unserializable_query_arg_computes_but_skips_disk(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    runs = {"n": 0}

    def build() -> Callable[..., Any]:
        engine = Engine(cache_dir=cache)

        @engine.query
        def describe(marker: Any) -> str:
            runs["n"] += 1
            return type(marker).__name__

        return describe

    marker = object()
    describe_a = build()
    assert describe_a(marker) == "object"
    assert describe_a(marker) == "object"  # in-memory hit still works
    assert runs["n"] == 1

    describe_b = build()
    assert describe_b(marker) == "object"  # nothing was persisted for this key
    assert runs["n"] == 2


def test_unserializable_query_value_raises_with_query_context(tmp_path: Path) -> None:
    engine = Engine(cache_dir=tmp_path / "cache")

    @engine.query
    def bad() -> Any:
        return object()

    with pytest.raises(TypeError, match="persistent cache"):
        bad()
    engine.shutdown()


def test_dataclass_values_round_trip_through_disk(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    runs = {"n": 0}

    def build() -> Any:
        engine = Engine(cache_dir=cache)

        @engine.input
        def name() -> str:
            return "root"

        @engine.query
        def build_ast() -> _Ast:
            runs["n"] += 1
            return _Ast(name(), (1, 2, 3))

        return engine, build_ast

    engine_a, build_ast_a = build()
    assert build_ast_a() == _Ast("root", (1, 2, 3))
    engine_a.shutdown()

    engine_b, build_ast_b = build()
    result = build_ast_b()
    assert result == _Ast("root", (1, 2, 3))
    assert type(result) is _Ast
    assert runs["n"] == 1
    engine_b.shutdown()


def test_two_engines_share_one_cache_dir(tmp_path: Path) -> None:
    data = tmp_path / "a.txt"
    data.write_text("one two three four")
    cache = tmp_path / "cache"
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    engine_b, _, _, word_count_b = _session(cache, runs)
    assert word_count_a(str(data)) == 4
    assert word_count_b(str(data)) == 4
    # Engine B was a separate session reading the same store: it hydrates
    # rather than recomputing.
    assert runs == {"normalized": 1, "word_count": 1}
    engine_a.shutdown()
    engine_b.shutdown()


def test_in_memory_engine_untouched_by_feature(tmp_path: Path) -> None:
    engine = Engine()
    assert engine.cache_dir is None

    @engine.input
    def seed() -> int:
        return 1

    @engine.query
    def doubled() -> int:
        return seed() * 2

    assert doubled() == 2
    events = [t.event for t in engine.traces()]
    assert not any(event.startswith("disk") for event in events)
    engine.shutdown()


def test_compute_many_parallel_with_disk_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"
    files = []
    for i in range(20):
        path = tmp_path / f"f{i}.txt"
        path.write_text(f"file {i} has five words")
        files.append(str(path))
    runs: dict[str, int] = {}

    engine_a, _, _, word_count_a = _session(cache, runs)
    results = engine_a.compute_many([(word_count_a, (p,)) for p in files], workers=8)
    assert results == [5] * 20
    assert sum(runs.values()) == 40
    engine_a.shutdown()

    runs.clear()
    engine_b, _, _, word_count_b = _session(cache, runs)
    results = engine_b.compute_many([(word_count_b, (p,)) for p in files], workers=8)
    assert results == [5] * 20
    assert runs == {}
    engine_b.shutdown()


def test_pinned_snapshot_isolation_holds_with_disk_cache(tmp_path: Path) -> None:
    cache = tmp_path / "cache"

    def build() -> tuple[Engine, Any, Any]:
        engine = Engine(cache_dir=cache)

        @engine.input
        def seed() -> int:
            return 0

        @engine.query
        def doubled() -> int:
            return seed() * 2

        return engine, seed, doubled

    engine_a, seed_a, doubled_a = build()
    seed_a.set(5)
    assert doubled_a() == 10
    engine_a.shutdown()

    engine_b, seed_b, doubled_b = build()
    pinned = engine_b.snapshot()
    seed_b.set(5)
    # The pinned snapshot predates the set(), so it must see the default seed
    # regardless of what the disk cache holds for the post-set value.
    assert doubled_b(snapshot=pinned) == 0
    assert doubled_b() == 10
    engine_b.shutdown()
