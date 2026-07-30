# Performance Matrix Analysis

## Overview
This document summarizes the results of running the performance matrix benchmark on the `cascade-query` engine. The goal was to test and harden the library by evaluating the performance characteristics of different backend persistence options (`sqlite`, `lmdb`, `mdbx`), caching configurations (memory vs. disk cache), state persistence (save/load), and engine stats.

## The Benchmark
The benchmark was executed using `benchmarks/performance_matrix.py` against a synthetic graph of `depth=50` and `fanout=5000` (roughly 250,000 computed nodes). It evaluates all combinations of:
- **Backends:** `sqlite`, `lmdb`, `mdbx`
- **Stats:** Enabled (`True`), Disabled (`False`)
- **Modes:** 
  - **Memory + Save/Load**: Evaluates cold/hot compute times purely in memory, as well as explicit state dump and state hydration operations.
  - **Disk Cache Enabled**: Evaluates cold compute time (which inherently includes background serialization and writing to disk cache) and a secondary "Disk Cache Hit" time (hydrating on an empty engine reading directly from disk cache).

## Results

| Backend | Stats | Mode | Cold (s) | Hot (s) | Save (s) | Load (s) | Disk Cache Hit (s) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| sqlite | False | Memory + Save/Load | 5.6357 | 0.0000 | 0.8740 | 0.5484 | - |
| sqlite | False | Disk Cache | 30.3790 | - | - | - | 13.3672 |
| lmdb | False | Memory + Save/Load | 6.3892 | 0.0000 | 0.9992 | 0.6750 | - |
| lmdb | False | Disk Cache | 12.1140 | - | - | - | 11.0339 |
| mdbx | False | Memory + Save/Load | 6.8195 | 0.0000 | 1.4244 | 1.0451 | - |
| mdbx | False | Disk Cache | 26.9976 | - | - | - | 20.9087 |
| sqlite | True | Memory + Save/Load | 7.6900 | 0.0000 | 1.3127 | 0.9004 | - |
| sqlite | True | Disk Cache | 34.0900 | - | - | - | 14.7107 |
| lmdb | True | Memory + Save/Load | 7.9378 | 0.0001 | 1.2986 | 0.9307 | - |
| lmdb | True | Disk Cache | 14.2435 | - | - | - | 12.1503 |
| mdbx | True | Memory + Save/Load | 7.8589 | 0.0000 | 1.5756 | 1.2119 | - |
| mdbx | True | Disk Cache | 26.4062 | - | - | - | 21.3382 |

## Key Takeaways

1. **LMDB Disk Cache dominates**: LMDB's `Disk Cache` run took only `12.11s` for Cold and `11.03s` for a hit. Compare this to SQLite taking `30.3s` to initially dump the cache to disk and `13.3s` to hydrate it. MDBX was slightly worse at `26s` for Cold and `20s` for a cache hit. LMDB is highly optimized for this library's massive on-disk caches.
2. **SQLite is great for Memory + Save/Load dumps**: If you're purely running in memory and then explicitly saving/loading state (rather than continuously disk caching as nodes execute), SQLite actually performs the best (saving in `0.87s`, loading in `0.54s`).
3. **Engine Stats Overhead**: `Stats: True` shows a measurable overhead at this scale (e.g., Memory Cold jumped from `5.6s` to `7.6s` with SQLite). Tracking the graph statistics takes about a 20-30% execution toll on massive graphs.
4. **Hot compute is flawless (0.0000s)**: The hot memory checks consistently take `0.0000s` and scale beautifully. This happens because the engine tracks a global `revision` that only increments when inputs change. When no inputs change, the engine checks the top-level root query's `memo.verified_at` against the current global `revision`. Since they match, it knows immediately that no dependencies could have changed, returning the cached result in O(1) time without traversing the graph.
