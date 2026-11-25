"""The roots ``Engine.prune`` and ``Engine.subgraph`` accept, and the ones they ignore."""

from __future__ import annotations

from typing import Any

import pytest

from cascade import Engine


def _pipeline() -> tuple[Engine, Any, Any, Any]:
    engine = Engine()

    @engine.query
    def half(x: float) -> float:
        return x / 2

    @engine.query
    def total() -> float:
        return half(1.0) + half(3.0)

    @engine.query
    def other() -> int:
        return 5

    assert total() == 2.0
    assert other() == 5
    return engine, half, total, other


def _cached(engine: Engine) -> set[Any]:
    return set(engine._internals.memos)  # noqa: SLF001


def test_roots_may_be_keys_as_the_engine_holds_them() -> None:
    engine, half, _, _ = _pipeline()
    as_called = ("query", half.id, (1.0,))
    # A float argument is held tagged with its type, which tells it apart
    # from 1 and True.
    as_held = ("query", half.id, ((float, 1.0),))
    assert as_held in _cached(engine) and as_called not in _cached(engine)
    assert len(engine.subgraph([as_held])["nodes"]) == 1
    assert engine.subgraph([as_held]) == engine.subgraph([as_called])
    engine.prune([as_held])
    assert _cached(engine) == {as_held}


def test_root_whose_arguments_the_query_cannot_take_is_ignored() -> None:
    engine, half, total, other = _pipeline()
    misfit = ("query", half.id, (1.0, 2.0))  # half takes one argument
    assert engine.subgraph([misfit]) == engine.subgraph([])
    kept = ("query", other.id, ())
    assert engine.subgraph([misfit, kept]) == engine.subgraph([kept])
    engine.prune([misfit, kept])
    assert _cached(engine) == {kept}
    assert total() == 2.0


@pytest.mark.parametrize(
    "root",
    [
        ("query", "tests.roots:half"),  # no arguments
        ("query", "tests.roots:half", 1.0),  # arguments that are no tuple
        "query:tests.roots:half(1.0,)",  # a node name, as subgraph() takes
        42,
        None,
    ],
)
def test_prune_ignores_a_root_that_is_not_a_key(root: Any) -> None:
    engine, _, total, other = _pipeline()
    kept = ("query", other.id, ())
    # Like a key nothing is cached under: no error, and it keeps nothing.
    engine.prune([root, kept])
    assert _cached(engine) == {kept}
    engine.prune([root])
    assert _cached(engine) == set()
    assert (total(), other()) == (2.0, 5)
