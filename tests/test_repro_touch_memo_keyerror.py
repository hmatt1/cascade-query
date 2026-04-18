"""Regression: nested ``try_mark_green`` must not LRU-evict memos mid-verify.

Previously, ``compute_or_get_memo`` could touch a parent memo, call
``try_mark_green`` (which recursively recomputed deps), and nested
``evict_if_needed_locked`` could evict that parent before the post-verify
``touch_memo_locked`` → ``KeyError``.  Query memos under verification are now
refcount-pinned in :class:`~cascade._store.GraphStore` so LRU skips them until
``unpin_memo_verification_locked``.
"""

from __future__ import annotations

from cascade import Engine


def _run_nested_verify_under_pressure(*, max_entries: int = 6) -> None:
    engine = Engine(max_entries=max_entries, trace_limit=2_000)
    inp = engine.input(lambda: 0)

    @engine.query
    def aux(i: int) -> int:
        return i + 1

    @engine.query
    def leaf(i: int) -> int:
        return aux(i)

    @engine.query
    def root() -> int:
        return sum(leaf(j) for j in range(3))

    assert root() == 6
    inp.set(1)
    assert root() == 6


def test_nested_verify_with_small_memo_table_no_keyerror() -> None:
    """Seven query keys, ``max_entries == 6``: first run evicts; second run verifies."""
    _run_nested_verify_under_pressure(max_entries=6)


def test_same_graph_succeeds_with_smaller_cache() -> None:
    _run_nested_verify_under_pressure(max_entries=5)


def test_many_fillers_after_revision_before_root_still_computes(max_entries: int = 12) -> None:
    engine = Engine(max_entries=max_entries, trace_limit=10_000)
    inp = engine.input(lambda: 0)

    @engine.query
    def filler(n: int) -> int:
        return n

    @engine.query
    def aux(i: int) -> int:
        return i + 1

    @engine.query
    def leaf(i: int) -> int:
        return aux(i)

    @engine.query
    def root() -> int:
        return sum(leaf(j) for j in range(3))

    assert root() == 6
    for n in range(400):
        filler(n)
    inp.set(1)
    assert root() == 6
