"""The slow verification path must not recurse once per dependency level.

Chains are built bottom-up so the initial compute never recurses deeply; only
verification could. Each case would overflow Python's default recursion limit
(1000 frames) with the recursive walk.
"""

from __future__ import annotations

import sys
from collections import Counter
from typing import Any

import pytest

from cascade import Engine

DEPTH = 3000


def _chain(engine: Engine, *, ttl_bottom: bool, runs: Counter[str]) -> tuple[Any, ...]:
    @engine.input
    def leaf() -> int:
        return 1

    @engine.input
    def noise() -> int:
        return 0

    def bottom_body() -> int:
        runs["bottom"] += 1
        return leaf()

    bottom = (
        engine.query(ttl=3600.0)(bottom_body)
        if ttl_bottom
        else engine.query(bottom_body)
    )

    @engine.query
    def level(n: int) -> int:
        runs["level"] += 1
        return (bottom() if n == 0 else level(n - 1)) + 1

    leaf.set(1)
    noise.set(0)
    for n in range(DEPTH):
        level(n)
    runs.clear()
    return leaf, noise, level


@pytest.fixture(autouse=True)
def _default_recursion_limit() -> Any:
    old = sys.getrecursionlimit()
    sys.setrecursionlimit(1000)
    yield
    sys.setrecursionlimit(old)


def test_deep_chain_above_ttl_memo_verifies_without_recursion() -> None:
    runs: Counter[str] = Counter()
    engine = Engine(max_entries=DEPTH * 2)
    _, noise, level = _chain(engine, ttl_bottom=True, runs=runs)
    noise.set(1)  # unrelated bump; the TTL memo keeps everything above it dirty
    assert level(DEPTH - 1) == DEPTH + 1
    assert runs == {}


def test_deep_chain_read_at_pinned_snapshot() -> None:
    runs: Counter[str] = Counter()
    engine = Engine(max_entries=DEPTH * 2)
    leaf, _, level = _chain(engine, ttl_bottom=False, runs=runs)
    old = engine.snapshot()
    leaf.set(2)
    assert level(DEPTH - 1) == DEPTH + 2
    runs.clear()
    # Every memo was verified at the newer revision, so the old snapshot read
    # walks (and, since leaf differs there, recomputes) the whole chain.
    assert level(DEPTH - 1, snapshot=old) == DEPTH + 1
    assert runs == {"bottom": 1, "level": DEPTH}


def test_deep_chain_changed_leaf_recomputes_every_level() -> None:
    runs: Counter[str] = Counter()
    engine = Engine(max_entries=DEPTH * 2)
    leaf, _, level = _chain(engine, ttl_bottom=True, runs=runs)
    leaf.set(5)
    assert level(DEPTH - 1) == DEPTH + 5
    assert runs == {"bottom": 1, "level": DEPTH}


def test_deep_chain_with_evicted_middle_memo() -> None:
    runs: Counter[str] = Counter()
    engine = Engine(max_entries=DEPTH * 2)
    _, noise, level = _chain(engine, ttl_bottom=True, runs=runs)
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store.drop_memo_locked(("query", level.id, (DEPTH // 2,)))
    noise.set(1)
    assert level(DEPTH - 1) == DEPTH + 1
    # The dropped level is recomputed. With no previous memo to compare
    # against it cannot backdate, so the level above reruns once, finds the
    # same value, backdates, and nothing further up reruns.
    assert runs == {"level": 2}


def test_verification_stops_at_first_changed_dependency() -> None:
    """A dependency read after one that changed must not be verified: the
    recomputation may no longer read it, and verifying it could fail."""
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def flag() -> bool:
        return True

    @engine.input
    def x() -> int:
        return 1

    @engine.query(ttl=3600.0)
    def gate() -> bool:
        return flag()

    @engine.query
    def fragile() -> int:
        runs["fragile"] += 1
        if x() < 0:
            raise ValueError("fragile read with negative x")
        return x()

    @engine.query
    def top() -> int:
        runs["top"] += 1
        return fragile() if gate() else 0

    flag.set(True)
    x.set(1)
    assert top() == 1
    runs.clear()
    with engine.transaction():
        flag.set(False)
        x.set(-1)
    assert top() == 0
    assert runs == {"top": 1}


def test_cycle_memos_fall_back_to_the_normal_path() -> None:
    engine = Engine()

    @engine.input
    def limit() -> int:
        return 3

    @engine.input
    def noise() -> int:
        return 0

    @engine.query(fixed_point=0)
    def grow() -> int:
        return min(grow() + 1, limit())

    @engine.query
    def wrap(n: int) -> int:
        return grow() if n == 0 else wrap(n - 1)

    limit.set(3)
    noise.set(0)
    for n in range(50):
        wrap(n)
    noise.set(1)
    assert wrap(49) == 3
    limit.set(6)
    assert wrap(49) == 6
