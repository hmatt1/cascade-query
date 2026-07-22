from cascade import Engine


def test_sweep_unaccessed() -> None:
    engine = Engine()

    @engine.input
    def root() -> int:
        return 1

    @engine.query
    def query_a() -> int:
        return root() + 1

    @engine.query
    def query_b() -> int:
        return query_a() + 1

    # Phase 1: Compute both. Both 'query_a' and 'query_b' are accessed.
    assert query_b() == 3
    stats = engine.stats_summary()
    assert stats["memo_count"] == 2

    # Record access_id before the next "epoch"
    epoch = engine.access_id

    # Phase 2: Compute only a (query_b is unaccessed this pass)
    assert query_a() == 2

    engine.sweep_unaccessed(epoch)

    # query_b should be evicted, query_a should remain
    stats_after = engine.stats_summary()
    assert stats_after["memo_count"] == 1
