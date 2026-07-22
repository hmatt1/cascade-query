import asyncio
import time
import pytest
from cascade.engine import Engine


def test_async_query_basic():
    engine = Engine()

    @engine.query
    async def get_data(x):
        await asyncio.sleep(0.01)
        return x * 2

    @engine.query
    async def compute(x):
        a = await get_data(x)
        b = await get_data(x + 1)
        return a + b

    result = compute(5)
    assert result == 10 + 12


def test_mixed_graph_sync_async():
    engine = Engine()

    @engine.query
    async def async_node(x):
        await asyncio.sleep(0.01)
        return x * 2

    @engine.query
    def sync_node(x):
        return async_node(x) + 5

    @engine.query
    async def root_async(x):
        a = await async_node(x)
        b = await asyncio.get_running_loop().run_in_executor(None, sync_node, x)
        return a + b

    @engine.query
    def root_sync(x):
        return sync_node(x)

    assert root_sync(5) == 15
    assert root_async(5) == 10 + 15


@pytest.mark.asyncio
async def test_engine_called_from_user_loop():
    engine = Engine()

    @engine.query
    async def get_data(x):
        await asyncio.sleep(0.01)
        return x * 2

    @engine.query
    def sync_data(x):
        time.sleep(0.01)
        return x * 3

    # If user invokes it from an event loop, the wrapper returns a coroutine
    # for async nodes but blocks on sync nodes (which we could offload, but directly calling sync is blocking)
    res = await get_data(10)
    assert res == 20

    # Sync query called from async loop executes synchronously.
    res2 = sync_data(10)
    assert res2 == 30


def test_dedup_yields_loop():
    engine = Engine()

    events = []

    @engine.query
    async def slow_node():
        events.append("slow_start")
        await asyncio.sleep(0.05)
        events.append("slow_end")
        return "slow"

    @engine.query
    async def fast_node():
        events.append("fast_start")
        await asyncio.sleep(0.01)
        events.append("fast_end")
        return "fast"

    @engine.query
    async def root():
        # Start both at the same time. They should run concurrently.
        # slow_node will take 0.05s, fast_node will take 0.01s.
        coros = [slow_node(), slow_node(), fast_node()]
        results = await asyncio.gather(*coros)
        return results

    res = root()
    assert res == ["slow", "slow", "fast"]

    # fast_end should happen before slow_end
    assert events == ["slow_start", "fast_start", "fast_end", "slow_end"]
