# Performance Matrix Analysis

## Overview
This document summarizes the results of running the performance matrix benchmark on the `cascade-query` engine. It compares the persistent backends (`sqlite`, `mdbx`), caching configurations (memory vs. disk cache), state persistence (save/load), and engine stats.

## The Benchmark
The benchmark was executed using `benchmarks/performance_matrix.py` against a synthetic graph of `depth=50` and `fanout=5000` (roughly 250,000 computed nodes). It evaluates all combinations of:
- **Backends:** `sqlite`, `mdbx`
- **Stats:** Enabled (`True`), Disabled (`False`)
- **Modes:**
  - **Memory + Save/Load**: Evaluates cold/hot compute times purely in memory, as well as explicit state dump and state hydration operations.
  - **Disk Cache Enabled**: Evaluates cold compute time (which inherently includes background serialization and writing to disk cache) and a secondary "Disk Cache Hit" time (hydrating on an empty engine reading directly from disk cache).

> **Rerun 2026-11-22**, after the hot-path, disk format 2, dirty propagation and
> iterative verification changes, on Python 3.12 (GIL build). `max_entries` is
> sized to the graph, so nothing is evicted. Each cell is a single run, so
> differences of 10-20% between rows are noise.
>
> Earlier versions of this document used the `Engine` default
> `max_entries=10_000`, so the LRU evicted throughout and the Save/Load columns
> covered only ~10,000 memos. The `lmdb` backend has since been removed: its
> bindings segfaulted under parallel load on free-threaded Python.

## Results

| Backend | Stats | Mode | Cold (s) | Hot (s) | Save (s) | Load (s) | Disk Cache Hit (s) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| sqlite | False | Memory + Save/Load | 3.0393 | 0.0000 | 11.2403 | 6.8214 | - |
| sqlite | False | Disk Cache | 13.9717 | - | - | - | 6.3743 |
| mdbx | False | Memory + Save/Load | 3.1325 | 0.0000 | 14.5201 | 8.3251 | - |
| mdbx | False | Disk Cache | 9.2042 | - | - | - | 8.8382 |
| sqlite | True | Memory + Save/Load | 4.0400 | 0.0000 | 14.0403 | 8.4426 | - |
| sqlite | True | Disk Cache | 14.5709 | - | - | - | 6.8078 |
| mdbx | True | Memory + Save/Load | 4.7854 | 0.0000 | 14.3367 | 10.2117 | - |
| mdbx | True | Disk Cache | 10.9934 | - | - | - | 8.4986 |

## Incremental updates

The matrix only measures cold and fully-cached runs. Updates after an input
change are where dirty propagation matters; `benchmarks/engine_overhead.py`
measures them on a 20,000-node graph (`depth=20`, `fanout=1000`, median of 5
runs, Python 3.12):

| Workload | Before dirty propagation (ms) | Now (ms) |
| --- | ---: | ---: |
| Change one leaf, recompute | 85.7 | 3.1 |
| Set an input to the same value, re-verify | 68.2 | 0.01 |
| Memory cold | 185 | 170 |

A memo stays marked clean until an input it transitively read changes value, so
only that dependency cone is re-verified. Memos with a TTL, fixed-point cycle
members, reads at an older pinned snapshot, and state restored by `load()` still
use the full dependency walk, which is iterative and handles chains of any depth.

## Key Takeaways

1. **Incremental updates are now proportional to what changed**: changing one leaf of a 20,000-node graph recomputes its 21-node cone in about `3ms`, down from `86ms` when every revision bump re-walked the graph. Re-verifying after a same-value input set is effectively free.
2. **The disk cache is now the expensive path**: a cold run that populates the cache takes `9-14s` against `3s` in memory, and a cache hit on a fresh engine (`6-9s`) is still slower than recomputing this graph. The query bodies here are trivial, so this is all serialization and storage overhead; real workloads with expensive queries benefit far more.
3. **SQLite is the faster backend for cache hits, MDBX for populating the cache**: SQLite hits took `6.4s` against `8.8s` for MDBX, while MDBX's cold run took `9.2s` against `14.0s` for SQLite.
4. **`engine.save()`/`load()` is expensive at full graph size**: with all ~250,000 memos retained, a save takes `11-15s` and a load `7-10s` on both backends, several times the `3s` it takes to compute the graph in memory. For graphs this cheap to compute, the disk cache or plain recomputation beats explicit save/load.
5. **Engine stats cost about 1-1.7s of a 3s cold compute in this run**: memory cold rose from `3.0s` to `4.0s` (SQLite) and `3.1s` to `4.8s` (MDBX) with `Stats: True`. Earlier runs showed less difference; with a single run per cell, treat this as indicative.
6. **Hot compute is O(1) (0.0000s)**: The engine tracks a global `revision` that only increments when inputs change. When no inputs change, the top-level query's `memo.verified_at` matches the current `revision`, so the cached result is returned without traversing the graph.
