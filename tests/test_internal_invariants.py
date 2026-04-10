from __future__ import annotations

from unittest.mock import patch

from cascade import Engine
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
    assert internals.input_version_at((source.id, ("main",)), revision=engine.revision) is latest
    snap = engine.snapshot()
    assert symbol_count("main", snapshot=snap) == 2

    dep_key = ("query", parse.id, ("main",))
    assert internals.dependency_changed_at(dep_key, snap) == snap.revision
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


def test_lru_eviction_recovers_when_heap_contains_only_stale_entries() -> None:
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
        store._lru_heap = [(0, stale)]
    assert b() == 2
    assert len(store.memos) == 1


def test_pop_lru_victim_returns_none_when_heap_empty() -> None:
    engine = Engine()
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store._lru_heap.clear()
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


def test_evict_rebuilds_heap_and_uses_min_fallback_when_pop_finds_no_victim() -> None:
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
