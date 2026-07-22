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
