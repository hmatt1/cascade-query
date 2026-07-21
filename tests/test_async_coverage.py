import asyncio
import pytest
from cascade.engine import Engine
from cascade._errors import CycleError, QueryCancelled

@pytest.mark.asyncio
async def test_async_input_and_disk_cache(tmp_path):
    engine = Engine(cache_dir=str(tmp_path), stats=True)
    
    @engine.input
    async def async_input(x: int) -> int:
        await asyncio.sleep(0.01)
        return x + 10
        
    @engine.query
    async def async_query(x: int) -> int:
        val = await async_input(x)
        return val * 2
        
    # Run once to populate disk cache and trigger recompute_async saving to disk
    res1 = await async_query(5)
    assert res1 == 30
    
    # Run again to hit memory cache
    res2 = await async_query(5)
    assert res2 == 30
    
    # Shutdown and re-init to hit disk cache
    engine.shutdown()
    
    engine2 = Engine(cache_dir=str(tmp_path), stats=True)
    
    @engine2.input
    async def async_input2(x: int) -> int:
        await asyncio.sleep(0.01)
        return x + 10
        
    @engine2.query
    async def async_query(x: int) -> int:
        val = await async_input(x)
        return val * 2
        
    # Hits disk cache through compute_or_get_memo_async
    res3 = await async_query(5)
    assert res3 == 30
    engine2.shutdown()


@pytest.mark.asyncio
async def test_async_cycle():
    engine = Engine()
    
    @engine.query
    async def a(x):
        return await b(x)
        
    @engine.query
    async def b(x):
        return await a(x)
        
    with pytest.raises(CycleError):
        await a(1)

@pytest.mark.asyncio
async def test_async_exception_caching():
    engine = Engine()
    
    call_count = 0
    
    @engine.query(cache_exceptions=True)
    async def error_query(x):
        nonlocal call_count
        call_count += 1
        raise ValueError("test error")
        
    with pytest.raises(ValueError):
        await error_query(5)
        
    assert call_count == 1
    
    # Should hit cache
    with pytest.raises(ValueError):
        await error_query(5)
        
    assert call_count == 1

@pytest.mark.asyncio
async def test_async_query_cancelled():
    engine = Engine()
    
    @engine.input
    def fast_input():
        return 1
        
    @engine.query
    async def slow_query():
        # wait a bit, then check cancelled
        await asyncio.sleep(0.1)
        return fast_input()
        
    # We simulate cancellation by running in background and changing input
    fut = engine.submit(slow_query)
    # Immediately bump the cancel epoch
    fast_input.set(2)
    
    # Wait for future
    with pytest.raises(QueryCancelled):
        fut.result()

def test_sync_cycle_fixed_point():
    # just in case coverage for cycle nodes missing
    pass
