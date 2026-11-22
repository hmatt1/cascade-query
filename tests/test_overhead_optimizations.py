"""Regression tests for the hot-path bookkeeping optimizations.

Covers the lazy trace rows, the OrderedDict LRU, lazily allocated dedup
futures, scalar digest fast paths, and the latest-version input lookup.
"""

from __future__ import annotations

import hashlib
import json
import threading

import pytest

from cascade import Engine, _canonical, _serde
from cascade._state import InFlight


def _slow_json_digest(value: object) -> str:
    blob = json.dumps(
        _serde._to_jsonable(value),  # noqa: SLF001
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.blake2b(blob, digest_size=20).hexdigest()


SCALARS = [
    None,
    True,
    False,
    0,
    -1,
    2**63,
    -(2**70),
    "",
    "plain",
    'quote" backslash\\ newline\n nul\x00 sep ',
    "日本語 ☃",
]


@pytest.mark.parametrize("value", SCALARS)
def test_json_digest_fast_path_matches_general_path(value: object) -> None:
    # Digests are persisted by save()/load(), so the fast path must be
    # byte-identical to the general JSON encoding, not merely deterministic.
    assert _serde.stable_value_digest(value) == _slow_json_digest(value)


@pytest.mark.parametrize(
    "value", [*SCALARS, 1.5, float("nan"), b"raw", 2**64 - 1, 2**64, -(2**63) - 1]
)
def test_msgpack_digest_fast_path_matches_general_path(value: object) -> None:
    expected = _canonical.digest_bytes(_canonical.encode(value))
    assert _canonical.value_digest(value) == expected


def test_lru_tracks_memos_without_growing_on_cache_hits() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 1

    @engine.query
    def doubled() -> int:
        return source() * 2

    for _ in range(1_000):
        assert doubled() == 2
    store = engine._internals._store  # noqa: SLF001
    assert list(store._lru) == list(store.memos)  # noqa: SLF001


def test_lru_evicts_least_recently_used_first() -> None:
    engine = Engine(max_entries=2)

    @engine.input
    def source(name: str) -> int:
        return 0

    @engine.query
    def read(name: str) -> int:
        return source(name)

    read("a")
    read("b")
    read("a")  # refresh "a" so "b" becomes the LRU victim
    read("c")
    store = engine._internals._store  # noqa: SLF001
    assert set(store.memos) == {("query", read.id, ("a",)), ("query", read.id, ("c",))}


def test_lru_victim_skips_pinned_rows() -> None:
    engine = Engine()

    @engine.query
    def value(n: int) -> int:
        return n

    value(1)
    value(2)
    store = engine._internals._store  # noqa: SLF001
    first = ("query", value.id, (1,))
    with store.lock:
        store.pin_memo_verification_locked(first)
        assert store._pop_lru_victim_key_locked() == ("query", value.id, (2,))  # noqa: SLF001
        store.unpin_memo_verification_locked(first)
        assert store._pop_lru_victim_key_locked() == first  # noqa: SLF001


def test_uncontended_compute_never_allocates_a_dedup_future() -> None:
    engine = Engine()
    seen: list[InFlight] = []
    store = engine._internals._store  # noqa: SLF001

    @engine.query
    def probe() -> int:
        seen.extend(store.in_flight.values())
        return 1

    assert probe() == 1
    assert len(seen) == 1
    assert seen[0].future is None
    assert store.in_flight == {}


def test_waiter_receives_owner_result_and_exception() -> None:
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
    fail = {"value": False}

    @engine.query
    def slow() -> int:
        started.set()
        release.wait(timeout=5)
        if fail["value"]:
            raise ValueError("boom")
        return 7

    for should_fail in (False, True):
        fail["value"] = should_fail
        started.clear()
        release.clear()
        if should_fail:
            # Force a fresh computation for the second round.
            engine._internals._store.memos.clear()  # noqa: SLF001
        results: dict[str, object] = {}

        def call(label: str) -> None:
            try:
                results[label] = slow()
            except ValueError as exc:
                results[label] = exc

        owner = threading.Thread(target=call, args=("owner",))
        owner.start()
        assert started.wait(timeout=5)
        waiter = threading.Thread(target=call, args=("waiter",))
        waiter.start()
        # Wait until the waiter has attached its future to the owner's slot.
        store = engine._internals._store  # noqa: SLF001
        for _ in range(500):
            with store.lock:
                slots = list(store.in_flight.values())
            if slots and slots[0].future is not None:
                break
            threading.Event().wait(0.01)
        release.set()
        owner.join(timeout=5)
        waiter.join(timeout=5)
        if should_fail:
            assert isinstance(results["owner"], ValueError)
            assert isinstance(results["waiter"], ValueError)
        else:
            assert results == {"owner": 7, "waiter": 7}
        assert store.in_flight == {}


def test_release_does_not_unregister_a_newer_owner() -> None:
    engine = Engine()
    evaluator = engine._internals._evaluator  # noqa: SLF001
    store = engine._internals._store  # noqa: SLF001
    key = (("query", "q", ()), 0)
    with store.lock:
        old_slot, _ = evaluator._claim_in_flight_locked(key)  # noqa: SLF001
        # Simulate engine.load() clearing in_flight, then a new owner claiming.
        store.in_flight.clear()
        new_slot, waiter = evaluator._claim_in_flight_locked(key)  # noqa: SLF001
    assert waiter is None
    evaluator._release_in_flight(key, old_slot, result=None)  # noqa: SLF001
    assert store.in_flight[key] is new_slot


def test_trace_limit_zero_records_nothing() -> None:
    engine = Engine(trace_limit=0)

    @engine.query
    def value() -> int:
        return 1

    value()
    assert engine.traces() == []


def test_traces_format_keys_on_read() -> None:
    engine = Engine()

    @engine.input
    def source(name: str) -> int:
        return 0

    source.set("a", 3)
    events = [ev for ev in engine.traces() if ev.event == "input_set"]
    assert events[-1].key == f"input:{source.id}('a',)"
    assert events[-1].detail.startswith("changed_at=")


def test_input_version_at_returns_historical_versions() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    revisions: list[int] = []
    for value in (10, 20, 30):
        source.set(value)
        revisions.append(engine.revision)
    store = engine._internals._store  # noqa: SLF001
    input_key = (source.id, ())
    assert store.input_version_at(input_key, revisions[0] - 1) is None
    for revision, value in zip(revisions, (10, 20, 30)):
        version = store.input_version_at(input_key, revision)
        assert version is not None and version.value == value
    latest = store.input_version_at(input_key, revisions[-1] + 100)
    assert latest is not None and latest.value == 30


@pytest.mark.parametrize("stats", [False, True])
def test_disk_and_persistence_counters_respect_stats_flag(stats: bool) -> None:
    store = Engine(stats=stats)._internals._store  # noqa: SLF001
    store.record_disk_hit()
    store.record_disk_miss()
    store.record_disk_store()
    store.record_persistence_load()
    store.record_persistence_save()
    summary = store.stats_summary()
    expected = 1 if stats else 0
    for name in (
        "disk_hits",
        "disk_misses",
        "disk_stores",
        "persistence_loads",
        "persistence_saves",
    ):
        assert summary[name] == expected


def test_nested_pins_release_one_level_at_a_time() -> None:
    engine = Engine()

    @engine.query
    def value(n: int) -> int:
        return n

    value(1)
    value(2)
    store = engine._internals._store  # noqa: SLF001
    first = ("query", value.id, (1,))
    with store.lock:
        store.pin_memo_verification_locked(first)
        store.pin_memo_verification_locked(first)
        store.unpin_memo_verification_locked(first)
        # Still pinned once: the fallback scan must skip it too.
        assert store._unpinned_lru_fallback_key_locked() == (  # noqa: SLF001
            "query",
            value.id,
            (2,),
        )
        store.unpin_memo_verification_locked(first)
        assert store._unpinned_lru_fallback_key_locked() == first  # noqa: SLF001


def test_eviction_stops_when_every_row_is_pinned() -> None:
    engine = Engine()

    @engine.query
    def value(n: int) -> int:
        return n

    value(1)
    value(2)
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        for key in list(store.memos):
            store.pin_memo_verification_locked(key)
        store.max_entries = 0
        store.evict_if_needed_locked()
        assert len(store.memos) == 2
