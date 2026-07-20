from pathlib import Path
from cascade import Engine

def test_prune_query_dep(tmp_path: Path) -> None:
    engine = Engine(cache_dir=tmp_path / "cache")
    
    @engine.query
    def q1() -> int:
        return 1
        
    @engine.query
    def q2() -> int:
        return q1() + 1
        
    q2()
    engine.prune([("query", q2.id, ())], vacuum_disk=True)
    
    engine._evaluator.prune_disk_cache([])
    engine._disk = None
    engine._evaluator.prune_disk_cache([])
    
    engine.shutdown()

def test_store_methods() -> None:
    engine = Engine()
    try:
        engine._store.latest_input_version(("input", ()))
    except Exception:
        pass
        
    try:
        engine._store.input_version_at(("input", ()), 0)
    except Exception:
        pass

    engine.shutdown()
