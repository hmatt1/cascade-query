from __future__ import annotations

import threading
from collections import OrderedDict
from unittest.mock import patch

import pytest

from cascade import Engine, QueryCancelled
from tests.scale_helpers import assert_internal_dependents_consistent


def test_internal_api_policy_lists_remain_explicit() -> None:
    # Keep policy lists explicit and stable so private coupling is deliberate.
    assert Engine._INTERNAL_TEST_API == ("_internals",)


def test_internal_dependency_helpers_and_indexes_are_consistent() -> None:
    engine = Engine()

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        return tuple(source(name).splitlines())

    @engine.query
    def symbol_count(name: str) -> int:
        return len(parse(name))

    # Invariant probes flow through the dedicated _internals object.
    internals = engine._internals  # noqa: SLF001
    assert internals.latest_input_version((source.id, ("missing",))) is None
    assert internals.input_version_at((source.id, ("main",)), revision=0) is None

    source.set("main", "a\nb")
    latest = internals.latest_input_version((source.id, ("main",)))
    assert latest is not None
    assert latest.value == "a\nb"
    assert (
        internals.input_version_at((source.id, ("main",)), revision=engine.revision)
        is latest
    )
    snap = engine.snapshot()
    assert symbol_count("main", snapshot=snap) == 2

    dep_key = ("query", parse.id, ("main",))
    # A query's changed_at is a change stamp, not a revision.
    assert (
        internals.dependency_changed_at(dep_key, snap)
        == internals.memos[dep_key].changed_at
    )
    assert_internal_dependents_consistent(engine)


def test_private_set_input_default_still_bumps_cancel_epoch() -> None:
    engine = Engine()

    before = engine._internals.cancel_epoch  # noqa: SLF001
    engine._set_input("tests:manual_input", (), 1)  # noqa: SLF001
    assert engine._internals.cancel_epoch == before + 1  # noqa: SLF001


def test_cache_hit_updates_access_order_monotonically() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def value() -> int:
        return source() + 1

    source.set(10)
    assert value() == 11
    memo_key = ("query", value.id, ())
    internals = engine._internals  # noqa: SLF001
    first_access = internals.memos[memo_key].last_access
    access_id_before_hit = internals.next_access_id

    # Cache-hit path should advance access ordering by exactly one step.
    assert value() == 11
    assert internals.next_access_id == access_id_before_hit + 1
    second_access = internals.memos[memo_key].last_access
    assert second_access == internals.next_access_id
    assert second_access > first_access


def test_push_memo_lru_locked_is_safe_when_memo_missing() -> None:
    engine = Engine()
    store = engine._internals._store  # noqa: SLF001
    ghost = ("query", "no.such.fn", ())
    with store.lock:
        store.push_memo_lru_locked(ghost)
    assert ghost not in store.memos


def test_lru_eviction_recovers_when_lru_contains_only_stale_entries() -> None:
    engine = Engine(max_entries=1)

    @engine.input
    def s() -> int:
        return 0

    @engine.query
    def a() -> int:
        return s()

    @engine.query
    def b() -> int:
        return s() + 1

    s.set(1)
    assert a() == 1
    store = engine._internals._store  # noqa: SLF001
    stale = ("query", "ghost:fn", ())
    with store.lock:
        store._lru = OrderedDict([(stale, None)])
    assert b() == 2
    assert len(store.memos) == 1


def test_pop_lru_victim_returns_none_when_lru_empty() -> None:
    engine = Engine()
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store._lru.clear()
        assert store._pop_lru_victim_key_locked() is None


def test_drop_memo_tolerates_absent_dependent_buckets() -> None:
    engine = Engine()

    @engine.input
    def s() -> int:
        return 0

    @engine.query
    def q() -> int:
        return s()

    s.set(1)
    assert q() == 1
    store = engine._internals._store  # noqa: SLF001
    qkey = ("query", q.id, ())
    memo = store.memos[qkey]
    dep_key = memo.deps[0].key
    with store.lock:
        store.dependents.pop(dep_key, None)
        store.drop_memo_locked(qkey)
    assert qkey not in store.memos


def test_evict_rebuilds_lru_and_uses_min_fallback_when_pop_finds_no_victim() -> None:
    engine = Engine(max_entries=5)

    @engine.input
    def s() -> int:
        return 0

    @engine.query
    def a() -> int:
        return s()

    @engine.query
    def b() -> int:
        return s() + 1

    @engine.query
    def c() -> int:
        return s() + 2

    s.set(1)
    assert a() == 1 and b() == 2 and c() == 3
    store = engine._internals._store  # noqa: SLF001
    store.max_entries = 1
    with patch.object(store, "_pop_lru_victim_key_locked", return_value=None):
        with store.lock:
            store.evict_if_needed_locked()
    assert len(store.memos) == 1


def test_internals_probe_shows_inputs_queries_and_the_entry_limit() -> None:
    engine = Engine(max_entries=7)

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query(incremental=False)
    def shout(name: str) -> str:
        return source(name).upper()

    source.set("main", "a")
    assert shout("main") == "A"
    internals = engine._internals  # noqa: SLF001
    assert [v.value for v in internals.inputs[(source.id, ("main",))]] == ["a"]
    assert internals.queries[shout.id] is shout.raw
    assert internals.queries[shout.id]("main") == "A"
    assert internals.max_entries == 7


@pytest.mark.parametrize("bump", [True, False])
def test_private_set_input_cancels_running_queries_only_on_request(bump: bool) -> None:
    engine = Engine()
    started, release = threading.Event(), threading.Event()

    @engine.input
    def knob() -> int:
        return 0

    @engine.query
    def leaf() -> int:
        return 1

    @engine.query
    def slow() -> int:
        started.set()
        assert release.wait(20)
        return leaf()  # every call of a query checks for cancellation first

    try:
        future = engine.submit(slow)
        assert started.wait(20)
        internals = engine._internals  # noqa: SLF001
        epoch, revision = internals.cancel_epoch, engine.revision
        returned = engine._set_input(  # noqa: SLF001
            knob.id, (), 5, bump_cancel_epoch=bump
        )
        # The value changes either way, at a new revision.
        assert returned == engine.revision == revision + 1
        assert knob() == 5
        assert internals.cancel_epoch == epoch + (1 if bump else 0)
        release.set()
        if bump:
            with pytest.raises(QueryCancelled):
                future.result(timeout=20)
        else:
            assert future.result(timeout=20) == 1
    finally:
        release.set()
        engine.shutdown()


def test_private_set_inputs_applies_a_batch_as_one_revision() -> None:
    engine = Engine()
    runs = {"total": 0}

    @engine.input
    def knob(name: str) -> int:
        return 0

    @engine.query
    def total() -> int:
        runs["total"] += 1
        return knob("a") + knob("b")

    knob.set("a", 1)
    assert total() == 1
    internals = engine._internals  # noqa: SLF001
    revision, epoch = engine.revision, internals.cancel_epoch

    returned = engine._set_inputs(  # noqa: SLF001
        [(knob.id, ("a",), 1), (knob.id, ("b",), 2), (knob.id, ("b",), 3)]
    )
    # One revision for the batch; the later update of "b" wins, and "a",
    # given the value it has, keeps its version.
    assert returned == engine.revision == revision + 1
    assert internals.cancel_epoch == epoch + 1
    assert (knob("a"), knob("b")) == (1, 3)
    assert internals.latest_input_version((knob.id, ("a",))).revision == revision
    assert internals.latest_input_version((knob.id, ("b",))).revision == returned
    assert total() == 4

    # A batch that changes nothing is no revision at all.
    same_values = [(knob.id, ("a",), 1), (knob.id, ("b",), 3)]
    assert engine._set_inputs(same_values) == returned  # noqa: SLF001
    assert (engine.revision, internals.cancel_epoch) == (returned, epoch + 1)
    assert total() == 4
    assert runs["total"] == 2

    # The cancel epoch can be left alone here too.
    quiet = engine._set_inputs(  # noqa: SLF001
        [(knob.id, ("a",), 10)], bump_cancel_epoch=False
    )
    assert quiet == engine.revision == returned + 1
    assert internals.cancel_epoch == epoch + 1
    assert total() == 13
