from cascade.engine import Engine

def test_memory_cache_invalidation_on_code_change():
    engine = Engine()
    
    call_count = 0
    
    def my_query(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2
        
    query_handle = engine.query(my_query)
    
    # First call: cache miss
    assert query_handle(5) == 10
    assert call_count == 1
    
    # Second call: cache hit
    assert query_handle(5) == 10
    assert call_count == 1
    
    # Now redefine the function with different logic but same name.
    # In a real environment, developers modify the file and hot-reload.
    # We simulate this by defining a new function with the same __qualname__ and __module__.
    
    def my_query_v2(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 3
        
    my_query_v2.__module__ = my_query.__module__
    my_query_v2.__qualname__ = my_query.__qualname__
    
    # Re-register
    query_handle_v2 = engine.query(my_query_v2)
    
    assert query_handle_v2.id == query_handle.id
    
    # Third call: cache miss because function hash changed
    assert query_handle_v2(5) == 15
    assert call_count == 2
    
    # Fourth call: cache hit on new logic
    assert query_handle_v2(5) == 15
    assert call_count == 2


def test_disk_cache_invalidation_on_code_change(tmp_path):
    engine1 = Engine(cache_dir=str(tmp_path))
    
    call_count_1 = 0
    
    def disk_query(x: int) -> int:
        nonlocal call_count_1
        call_count_1 += 1
        return x + 10
        
    qh1 = engine1.query(disk_query)
    
    # Populate disk cache
    assert qh1(5) == 15
    assert call_count_1 == 1
    
    engine1.shutdown()
    
    # Start a new engine pointing to the same disk cache
    engine2 = Engine(cache_dir=str(tmp_path))
    
    call_count_2 = 0
    
    def disk_query_v2(x: int) -> int:
        nonlocal call_count_2
        call_count_2 += 1
        return x + 20  # changed logic
        
    disk_query_v2.__module__ = disk_query.__module__
    disk_query_v2.__qualname__ = disk_query.__qualname__
    
    qh2 = engine2.query(disk_query_v2)
    
    # The disk cache has a record for disk_query(5) -> 15.
    # But because disk_query_v2 has a different bytecode hash, it should ignore the disk cache.
    assert qh2(5) == 25
    assert call_count_2 == 1

def test_input_cache_invalidation_on_code_change():
    engine = Engine()
    
    call_count = 0
    
    def my_input(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 2
        
    input_handle = engine.input(my_input)
    
    assert input_handle(5) == 10
    assert call_count == 1
    
    assert input_handle(5) == 10
    assert call_count == 1
    
    def my_input_v2(x: int) -> int:
        nonlocal call_count
        call_count += 1
        return x * 3
        
    my_input_v2.__module__ = my_input.__module__
    my_input_v2.__qualname__ = my_input.__qualname__
    
    input_handle_v2 = engine.input(my_input_v2)
    
    # Input has changed hash, should clear from state
    assert input_handle_v2(5) == 15
    assert call_count == 2
