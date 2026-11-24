"""Dict insertion order is part of a value (G2, G3, G18, G19; D6).

Two dicts that differ only in insertion order are ``==``, but a query that
iterates one sees the difference. So the digest behind "did this change"
tells them apart, in memory and on disk, and a dict read back from disk has
the order it was stored with. Sets have no order and are not affected.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any, NamedTuple

import pytest

import cascade._canonical as canonical
from cascade import CascadeDict, Engine
from cascade._serde import stable_value_digest

BACKENDS = ("mdbx", "sqlite")
# The digest of an engine without cache_dir, and of one with.
DIGESTS = [stable_value_digest, canonical.value_digest]


@dataclasses.dataclass(frozen=True)
class _Box:
    label: str
    data: dict = dataclasses.field(hash=False, compare=True)


class _Pair(NamedTuple):
    left: Any
    right: Any


def _engine(tmp_path: Path, backend: str | None) -> Engine:
    if backend is None:
        return Engine()
    return Engine(cache_dir=tmp_path, cache_backend=backend)


ALL_ENGINES = [None, *BACKENDS]


# --- the digests -------------------------------------------------------------


@pytest.mark.parametrize("digest", DIGESTS)
def test_digest_tells_dict_orders_apart_at_any_depth(digest: Any) -> None:
    ab, ba = {"a": 1, "b": 2}, {"b": 2, "a": 1}
    assert ab == ba
    assert digest(ab) != digest(ba)
    assert digest(ab) == digest({"a": 1, "b": 2})
    # Non-string keys take another encoding path.
    assert digest({1: "x", (2, 3): "y"}) != digest({(2, 3): "y", 1: "x"})
    assert digest({1: "x", (2, 3): "y"}) == digest({1: "x", (2, 3): "y"})
    # Nested in every container that can hold a dict.
    assert digest([ab]) != digest([ba])
    assert digest((ab,)) != digest((ba,))
    assert digest({"k": ab}) != digest({"k": ba})
    assert digest(_Box("x", ab)) != digest(_Box("x", ba))
    assert digest(_Pair(ab, 1)) != digest(_Pair(ba, 1))
    assert digest(ValueError(ab)) != digest(ValueError(ba))
    # An empty or one-item dict has one order.
    assert digest({}) == digest(dict())
    assert digest({"a": 1}) == digest(dict(a=1))


@pytest.mark.parametrize("digest", DIGESTS)
def test_digest_of_sets_ignores_how_they_were_built(digest: Any) -> None:
    assert digest({3, 1, 2}) == digest({2, 3, 1})
    assert digest(frozenset("abc")) == digest(frozenset("cba"))
    assert digest({1, "a", (2, 3), None, 1.5}) == digest({None, 1.5, (2, 3), "a", 1})
    # Many elements, inserted in opposite orders (different table layouts).
    forward, backward = set(), set()
    for i in range(500):
        forward.add(f"item-{i}")
        backward.add(f"item-{499 - i}")
    assert digest(forward) == digest(backward)
    assert digest({"k": forward}) == digest({"k": backward})
    assert digest(frozenset({(1, 2), (3, 4)})) == digest(frozenset({(3, 4), (1, 2)}))
    # Still type-sensitive.
    assert digest({1, 2}) != digest(frozenset({1, 2}))


def test_decoded_dict_keeps_its_order() -> None:
    for value in (
        {"z": 1, "a": 2, "m": 3},
        {3: "c", 1: "a", (0, 0): "t", "s": None},
        {"outer": {"z": 1, "a": 2}, "list": [{"b": 1, "a": 2}]},
    ):
        decoded = canonical.decode(canonical.encode(value))
        assert decoded == value
        assert list(decoded) == list(value)
    nested = canonical.decode(canonical.encode({"o": {"z": 1, "a": 2}}))
    assert list(nested["o"]) == ["z", "a"]


# --- early bail-out ----------------------------------------------------------


@pytest.mark.parametrize("backend", ALL_ENGINES)
def test_reordered_result_reruns_dependents_and_equal_order_does_not(
    tmp_path: Path, backend: str | None
) -> None:
    engine = _engine(tmp_path, backend)
    runs = {"keys": 0}
    try:

        @engine.input
        def order() -> str:
            return "ab"

        @engine.query
        def d() -> dict[str, int]:
            return {k: 1 for k in order().strip()}

        @engine.query
        def keys() -> list[str]:
            runs["keys"] += 1
            return list(d())

        assert keys() == ["a", "b"]
        order.set("ba")
        assert keys() == ["b", "a"]
        assert runs["keys"] == 2
        # Same dict in the same order: the bail-out still works.
        order.set(" ba ")
        assert keys() == ["b", "a"]
        assert runs["keys"] == 2
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", ALL_ENGINES)
def test_reordered_nested_and_non_string_keyed_results_reach_dependents(
    tmp_path: Path, backend: str | None
) -> None:
    engine = _engine(tmp_path, backend)
    try:

        @engine.input
        def flip() -> bool:
            return False

        @engine.query
        def table() -> list[Any]:
            pairs = [(1, "x"), ((2, 3), "y")]
            if flip():
                pairs.reverse()
            return [{"inner": dict(pairs)}]

        @engine.query
        def first_key() -> Any:
            return next(iter(table()[0]["inner"]))

        assert first_key() == 1
        flip.set(True)
        assert first_key() == (2, 3)
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", ALL_ENGINES)
def test_set_results_built_in_another_order_still_stop_propagation(
    tmp_path: Path, backend: str | None
) -> None:
    engine = _engine(tmp_path, backend)
    runs = {"size": 0}
    try:

        @engine.input
        def items() -> tuple[int, ...]:
            return (1, 2, 3)

        @engine.query
        def as_set() -> set[int]:
            return set(items())

        @engine.query
        def frozen() -> frozenset[int]:
            return frozenset(items())

        @engine.query
        def size() -> int:
            runs["size"] += 1
            return len(as_set()) + len(frozen())

        assert size() == 6
        items.set((3, 2, 1))
        assert size() == 6
        assert runs["size"] == 1
    finally:
        engine.shutdown()


# --- inputs ------------------------------------------------------------------


@pytest.mark.parametrize("backend", ALL_ENGINES)
def test_setting_an_input_to_a_reordered_dict_is_a_change(
    tmp_path: Path, backend: str | None
) -> None:
    engine = _engine(tmp_path, backend)
    try:

        @engine.input
        def config() -> dict[str, int]:
            return {}

        @engine.query
        def names() -> list[str]:
            return list(config())

        config.set({"a": 1, "b": 2})
        assert names() == ["a", "b"]
        revision = engine.revision
        config.set({"a": 1, "b": 2})
        assert engine.revision == revision  # same order: a no-op (G3)
        config.set({"b": 2, "a": 1})
        assert engine.revision == revision + 1
        assert names() == ["b", "a"]
    finally:
        engine.shutdown()


def test_equal_hashable_arguments_share_a_result_whatever_their_dict_order() -> None:
    """Arguments that are ``==`` and of one type share a key; dict order is not seen."""
    engine = Engine()
    runs = {"q": 0}

    @engine.query
    def q(box: _Box) -> list[str]:
        runs["q"] += 1
        return list(box.data)

    with pytest.raises(TypeError, match="unhashable"):
        q({"a": 1, "b": 2})  # a plain dict cannot be an argument at all
    runs["q"] = 0
    first = _Box("x", {"a": 1, "b": 2})
    second = _Box("x", {"b": 2, "a": 1})
    assert first == second and hash(first) == hash(second)
    assert q(first) == ["a", "b"]
    # One memo for equal arguments: the first call's result is served.
    assert q(second) == ["a", "b"]
    assert runs["q"] == 1


# --- across sessions ---------------------------------------------------------


def _order_session(cache: Path, backend: str, runs: dict[str, int]) -> Any:
    engine = Engine(cache_dir=cache, cache_backend=backend)

    @engine.input(name="order")
    def order() -> str:
        return "ab"

    @engine.query(name="d")
    def d() -> dict[str, int]:
        runs["d"] = runs.get("d", 0) + 1
        return {k: 1 for k in order()}

    @engine.query(name="keys")
    def keys() -> list[str]:
        runs["keys"] = runs.get("keys", 0) + 1
        return list(d())

    return engine, order, d, keys


@pytest.mark.parametrize("backend", BACKENDS)
def test_stored_dict_comes_back_in_the_order_it_was_returned(
    tmp_path: Path, backend: str
) -> None:
    runs: dict[str, int] = {}
    engine, order, d, keys = _order_session(tmp_path, backend, runs)
    order.set("zma")
    assert keys() == ["z", "m", "a"]
    engine.shutdown()

    runs.clear()
    engine, order, d, keys = _order_session(tmp_path, backend, runs)
    try:
        order.set("zma")
        assert list(d()) == ["z", "m", "a"]
        assert keys() == ["z", "m", "a"]
        assert runs == {}  # both served from disk
        # A reordered result in this session is seen against the stored one.
        order.set("amz")
        assert keys() == ["a", "m", "z"]
        assert runs == {"d": 1, "keys": 1}
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_results_stored_with_sorted_dicts_are_dropped_and_collections_kept(
    tmp_path: Path, backend: str
) -> None:
    """The step from disk format 2 (dict keys sorted) to 3 (insertion order)."""
    from tests.test_format_bump import FORMAT_KEY, _set_stamp

    runs: dict[str, int] = {}
    engine, order, d, keys = _order_session(tmp_path, backend, runs)
    order.set("ba")
    assert keys() == ["b", "a"]
    by_id = CascadeDict(engine, name="by_id")
    by_id["z"] = 1
    by_id["a"] = 2
    engine.shutdown()

    # Make the directory look like one a format 2 release wrote.
    _set_stamp(tmp_path, backend, FORMAT_KEY, 2)

    runs.clear()
    engine, order, d, keys = _order_session(tmp_path, backend, runs)
    try:
        order.set("ba")
        assert keys() == ["b", "a"]
        assert runs == {"d": 1, "keys": 1}  # recomputed, not read from format 2
        restored = CascadeDict(engine, name="by_id")
        assert dict(restored) == {"z": 1, "a": 2}
        assert list(restored) == ["z", "a"]
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_compacted_named_dict_restores_its_insertion_order(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        d = CascadeDict(engine, name="d", compact_every=4)
        for key in ("z", "m", "a", "q", "b", "y"):
            d[key] = key.upper()
        d.compact()
        expected = list(d.items())
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert list(CascadeDict(engine, name="d").items()) == expected
    finally:
        engine.shutdown()
