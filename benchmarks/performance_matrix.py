import argparse
import itertools
import tempfile
import time
from pathlib import Path
from typing import Any

from cascade import Engine
from cascade._synthetic_graph import build_fanout_chain_pipeline

BACKENDS = ["sqlite", "lmdb", "mdbx"]

def run_matrix(depth: int, fanout: int) -> list[dict[str, Any]]:
    results = []
    
    for stats_enabled, backend in itertools.product([False, True], BACKENDS):
        # 1. Baseline Memory (No Disk Cache) + State Save/Load
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            engine = Engine(stats=stats_enabled)
            leaf, _, aggregate = build_fanout_chain_pipeline(engine, depth=depth, fanout=fanout)
            
            for i in range(fanout):
                leaf.set(i, i)
            
            # Cold compute
            start = time.perf_counter()
            aggregate()
            cold_time = time.perf_counter() - start
            
            # Hot compute
            start = time.perf_counter()
            aggregate()
            hot_time = time.perf_counter() - start
            
            # Save state
            save_path = Path(tmp) / f"state_{backend}"
            start = time.perf_counter()
            engine.save(str(save_path), backend=backend) # type: ignore
            save_time = time.perf_counter() - start
            
            # Load state
            engine2 = Engine(stats=stats_enabled)
            start = time.perf_counter()
            engine2.load(str(save_path), backend=backend) # type: ignore
            load_time = time.perf_counter() - start
            
            engine.shutdown()
            engine2.shutdown()
            
            results.append({
                "Backend": backend,
                "Stats": stats_enabled,
                "Mode": "Memory + Save/Load",
                "Cold (s)": cold_time,
                "Hot (s)": hot_time,
                "Save (s)": save_time,
                "Load (s)": load_time,
                "Disk Cache Hit (s)": None,
            })
            
        # 2. Disk Cache Enabled
        with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
            cache_dir = Path(tmp) / f"cache_{backend}"
            engine = Engine(stats=stats_enabled, cache_dir=cache_dir, cache_backend=backend)
            leaf, _, aggregate = build_fanout_chain_pipeline(engine, depth=depth, fanout=fanout)
            
            for i in range(fanout):
                leaf.set(i, i)
                
            # Cold compute (writes to cache)
            start = time.perf_counter()
            aggregate()
            cold_time = time.perf_counter() - start
            
            engine.shutdown()
            if hasattr(engine, "_disk") and engine._disk:
                engine._disk.close()
            
            # Re-instantiate Engine to test Disk Cache Hit
            engine_hot = Engine(stats=stats_enabled, cache_dir=cache_dir, cache_backend=backend)
            leaf_hot, _, aggregate_hot = build_fanout_chain_pipeline(engine_hot, depth=depth, fanout=fanout)
            for i in range(fanout):
                leaf_hot.set(i, i)
                
            start = time.perf_counter()
            aggregate_hot()
            disk_cache_hit_time = time.perf_counter() - start
            
            engine_hot.shutdown()
            if hasattr(engine_hot, "_disk") and engine_hot._disk:
                engine_hot._disk.close()
            
            results.append({
                "Backend": backend,
                "Stats": stats_enabled,
                "Mode": "Disk Cache",
                "Cold (s)": cold_time,
                "Hot (s)": None,
                "Save (s)": None,
                "Load (s)": None,
                "Disk Cache Hit (s)": disk_cache_hit_time,
            })

    return results

def _format_time(t: float | None) -> str:
    if t is None:
        return "-"
    return f"{t:.4f}"

def main() -> None:
    parser = argparse.ArgumentParser(description="Run query-cascade performance matrix.")
    parser.add_argument("--depth", type=int, default=8, help="Depth of the synthetic graph")
    parser.add_argument("--fanout", type=int, default=256, help="Fanout of the synthetic graph")
    args = parser.parse_args()

    print(f"Running performance matrix with depth={args.depth}, fanout={args.fanout}...")
    results = run_matrix(args.depth, args.fanout)
    
    print("\n# Performance Matrix Results")
    print("\n| Backend | Stats | Mode | Cold (s) | Hot (s) | Save (s) | Load (s) | Disk Cache Hit (s) |")
    print("| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |")
    for r in results:
        print(f"| {r['Backend']} | {r['Stats']} | {r['Mode']} | "
              f"{_format_time(r['Cold (s)'])} | "
              f"{_format_time(r['Hot (s)'])} | "
              f"{_format_time(r['Save (s)'])} | "
              f"{_format_time(r['Load (s)'])} | "
              f"{_format_time(r['Disk Cache Hit (s)'])} |")

if __name__ == "__main__":
    main()
