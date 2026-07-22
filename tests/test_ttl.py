from cascade import Engine


def test_ttl_invalidation_basic():
    engine = Engine()

    clock = 0.0
    engine._store.monotonic_seconds = lambda: clock

    compute_count = 0

    @engine.query(ttl=2.0)
    def my_query():
        nonlocal compute_count
        compute_count += 1
        return "value"

    assert my_query() == "value"
    assert compute_count == 1

    # Still within TTL
    clock = 1.0
    assert my_query() == "value"
    assert compute_count == 1

    # Exceed TTL
    clock = 3.0
    assert my_query() == "value"
    assert compute_count == 2

    # Still within new TTL
    clock = 4.0
    assert my_query() == "value"
    assert compute_count == 2


def test_ttl_invalidation_async():
    import asyncio

    engine = Engine()

    clock = 0.0
    engine._store.monotonic_seconds = lambda: clock

    compute_count = 0

    @engine.query(ttl=1.5)
    async def my_async_query():
        nonlocal compute_count
        compute_count += 1
        return "async_value"

    async def run():
        nonlocal clock
        assert await my_async_query() == "async_value"
        assert compute_count == 1

        clock = 1.0
        assert await my_async_query() == "async_value"
        assert compute_count == 1

        clock = 2.0
        assert await my_async_query() == "async_value"
        assert compute_count == 2

    asyncio.run(run())
