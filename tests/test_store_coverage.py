import threading
from typing import Any

import pytest

from cascade._store import GraphStore
from cascade.engine import Engine


def test_store_stats_eviction_recent_cap() -> None:
    engine = Engine(stats=True, max_entries=2)

    # populate cache
    @engine.query
    def q(x: int) -> int:
        return x

    q(1)
    q(2)
    q(3)  # evicts 1

    assert engine.stats_summary()["evictions_total"] == 1
    assert len(engine.stats_summary()["evictions_recent"]) == 1

    # modify cap to 0
    engine._store.set_stats_eviction_recent_cap(0)
    assert len(engine.stats_summary()["evictions_recent"]) == 0

    # modify cap to 5
    engine._store.set_stats_eviction_recent_cap(5)

    q(4)  # evicts 2
    assert engine.stats_summary()["evictions_total"] == 2
    assert len(engine.stats_summary()["evictions_recent"]) == 1


def test_engine_negative_clock() -> None:
    clock_val = 100.0

    def clock():
        nonlocal clock_val
        val = clock_val
        clock_val -= 10.0  # goes backwards!
        return val

    engine = Engine(stats=True, stats_clock=clock)

    @engine.query
    def q(x: int) -> int:
        return x

    q(1)
    stats = engine.stats_summary()
    key = list(stats["by_key"].keys())[0]
    assert stats["by_key"][key] == 0.0  # clamped from negative to 0.0


def test_body_time_is_recorded_only_while_stats_are_on() -> None:
    store = GraphStore(max_entries=10, trace_limit=0, stats=False)
    key = ("query", "mod:q", (1,))
    store.record_query_body_time(key, 1.5)
    assert store.stats_summary()["by_key"] == {}

    store.set_stats_enabled(True)
    store.record_query_body_time(key, 1.5)
    store.record_query_body_time(key, 0.5)
    assert store.stats_summary()["by_key"] == {"query:mod:q(1,)": 2.0}

    store.set_stats_enabled(False)
    store.record_query_body_time(key, 4.0)
    assert store.stats_summary()["by_key"] == {"query:mod:q(1,)": 2.0}


class _AnnouncingLock:
    """The store's lock, telling the test when a thread comes to take it."""

    def __init__(self, lock: Any) -> None:
        self._lock = lock
        self.wanted = threading.Event()

    def __enter__(self) -> Any:
        self.wanted.set()
        return self._lock.__enter__()

    def __exit__(self, *exc: Any) -> Any:
        return self._lock.__exit__(*exc)


@pytest.mark.parametrize(
    ("record", "counter"),
    [
        ("record_disk_hit", "disk_hits"),
        ("record_disk_miss", "disk_misses"),
        ("record_disk_store", "disk_stores"),
    ],
)
def test_disk_counter_stays_put_once_stats_are_off(record: str, counter: str) -> None:
    # The counters look at the stats switch before they take the lock. A
    # thread that got past that look and then waited for the lock, while
    # stats were switched off, must not count after the switch.
    store = GraphStore(max_entries=10, trace_limit=0, stats=True)
    lock = store.lock
    announcing = _AnnouncingLock(lock)
    worker = threading.Thread(target=getattr(store, record), daemon=True)
    with lock:
        store.lock = announcing  # type: ignore[assignment]
        worker.start()
        assert announcing.wanted.wait(20)
        store._stats_enabled = False  # noqa: SLF001 - set_stats_enabled, lock held
    worker.join(20)
    assert not worker.is_alive()
    store.lock = lock
    assert store.stats_summary()[counter] == 0

    # With stats on from start to end the same call counts.
    store.set_stats_enabled(True)
    getattr(store, record)()
    assert store.stats_summary()[counter] == 1
