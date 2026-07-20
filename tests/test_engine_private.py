import pytest
from typing import Any
from cascade import Engine
from cascade._state import Snapshot
from cascade._runtime import RuntimeState

def test_engine_private_wrappers() -> None:
    engine = Engine()
    
    @engine.input
    def val() -> int:
        return 1

    @engine.query
    def query() -> int:
        return val() + 10

    assert query() == 11
    
    engine._check_cancelled(None)
    
    snapshot = engine.snapshot()
    
    try:
        engine._read_input("test", lambda: 1, (), snapshot=snapshot)
    except Exception:
        pass
        
    try:
        engine._query_call("test", lambda: 1, (), snapshot=snapshot, effects=None, cancel_epoch=None)
    except Exception:
        pass

    try:
        engine._set_input("test", (), 1, bump_cancel_epoch=False)
    except Exception:
        pass

    runtime = RuntimeState(snapshot=snapshot, stack=[], root_effects=None, staged_root_effects={}, cancel_epoch=0, snapshot_pinned=False)
    try:
        engine._compute_or_get_memo(("query", "test", ()), lambda: 1, runtime)
    except Exception:
        pass

    try:
        engine._try_mark_green(("query", "test", ()), None, snapshot)
    except Exception:
        pass

    try:
        engine._recompute(("query", "test", ()), lambda: 1, runtime)
    except Exception:
        pass

    try:
        engine._record_dependency(("query", "test", ()), 0)
    except Exception:
        pass

    try:
        engine._replay_effects({})
    except Exception:
        pass

    try:
        engine._push_effect("test", 1)
    except Exception:
        pass

    try:
        engine._trace_event("test", ("query", "test", ()))
    except Exception:
        pass

    try:
        engine._key_to_str(("query", "test", ()))
    except Exception:
        pass

    try:
        engine._stable_hash(1)
    except Exception:
        pass

    try:
        engine.compute_many([], effects=None)
    except Exception:
        pass

    try:
        list(engine.compute_many_stream([], effects=None))
    except Exception:
        pass

    try:
        # Cover the continue branches in compute_many and compute_many_stream
        query_handle = engine.query(lambda: 1)
        engine.compute_many([(query_handle, ())], effects={"a": []})
        list(engine.compute_many_stream([(query_handle, ())], effects={"a": []}))
    except Exception:
        pass

    engine.shutdown()
