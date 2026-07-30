import pytest
from pathlib import Path
from cascade.engine import Engine

def setup_pipeline(engine: Engine):
    runs = {}
    
    @engine.input
    def get_seed() -> int:
        return 10
        
    @engine.query
    def multiply_seed(multiplier: int) -> int:
        runs["multiply_seed"] = runs.get("multiply_seed", 0) + 1
        return get_seed() * multiplier
        
    return multiply_seed, runs

def test_permutation_incremental_mdbx(tmp_path: Path):
    """Permutation A: Incremental write-behind cache with MDBX."""
    cache_dir = tmp_path / "cache"
    
    # Session 1
    engine1 = Engine(cache_dir=cache_dir, cache_backend="mdbx")
    mult1, runs1 = setup_pipeline(engine1)
    assert mult1(5) == 50
    assert runs1 == {"multiply_seed": 1}
    engine1.flush_disk()
    engine1.shutdown()
    
    # Session 2
    engine2 = Engine(cache_dir=cache_dir, cache_backend="mdbx")
    mult2, runs2 = setup_pipeline(engine2)
    assert mult2(5) == 50
    assert runs2 == {}  # Hydrated from disk, no recompute
    engine2.shutdown()

def test_permutation_incremental_lmdb(tmp_path: Path):
    """Permutation A2: Incremental write-behind cache with LMDB."""
    cache_dir = tmp_path / "cache"
    
    # Session 1
    engine1 = Engine(cache_dir=cache_dir, cache_backend="lmdb")
    mult1, runs1 = setup_pipeline(engine1)
    assert mult1(5) == 50
    assert runs1 == {"multiply_seed": 1}
    engine1.flush_disk()
    engine1.shutdown()
    
    # Session 2
    engine2 = Engine(cache_dir=cache_dir, cache_backend="lmdb")
    mult2, runs2 = setup_pipeline(engine2)
    assert mult2(5) == 50
    assert runs2 == {}  # Hydrated from disk, no recompute
    engine2.shutdown()

@pytest.mark.parametrize("backend", ["sqlite", "mdbx", "lmdb"])
def test_permutation_snapshot(tmp_path: Path, backend: str):
    """Permutation B: Pure in-memory with manual save()/load() to a backend container."""
    db_file = tmp_path / ("state.db" if backend == "sqlite" else "state_dir")
    
    # Session 1
    engine1 = Engine(cache_dir=None)
    mult1, runs1 = setup_pipeline(engine1)
    assert mult1(5) == 50
    assert runs1 == {"multiply_seed": 1}
    
    # Save snapshot
    engine1.save(str(db_file), backend=backend)
    engine1.shutdown()
    
    # Session 2
    engine2 = Engine(cache_dir=None)
    mult2, runs2 = setup_pipeline(engine2)
    
    # Load snapshot BEFORE executing
    engine2.load(str(db_file), backend=backend)
    
    assert mult2(5) == 50
    assert runs2 == {}  # Hydrated from snapshot, no recompute
    engine2.shutdown()

def test_permutation_in_memory_only():
    """Permutation C: Pure in-memory, no persistence across sessions."""
    # Session 1
    engine1 = Engine(cache_dir=None)
    mult1, runs1 = setup_pipeline(engine1)
    assert mult1(5) == 50
    assert runs1 == {"multiply_seed": 1}
    engine1.shutdown()
    
    # Session 2
    engine2 = Engine(cache_dir=None)
    mult2, runs2 = setup_pipeline(engine2)
    assert mult2(5) == 50
    assert runs2 == {"multiply_seed": 1}  # No cache, must recompute
    engine2.shutdown()
