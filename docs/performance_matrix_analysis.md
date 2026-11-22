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

> **Rerun 2026-11-21.** The original run used the `Engine` default
> `max_entries=10_000` against this ~250,000-node graph, so the LRU evicted
> throughout and its timings mixed in eviction churn (and its Save/Load columns
> covered only ~10,000 memos). `performance_matrix.py` now sizes `max_entries`
> to the graph. The numbers below are from that fixed benchmark, after the
> hot-path and disk format 2 optimizations, on Python 3.12 (GIL build). They are
> a single run per cell, so differences of 10-20% between rows are noise. For
> repeatable per-node engine overhead, see `benchmarks/engine_overhead.py`.

## Results

| Backend | Stats | Mode | Cold (s) | Hot (s) | Save (s) | Load (s) | Disk Cache Hit (s) |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| sqlite | False | Memory + Save/Load | 3.1604 | 0.0000 | 11.8244 | 7.0730 | - |
| sqlite | False | Disk Cache | 16.4262 | - | - | - | 6.2496 |
| lmdb | False | Memory + Save/Load | 4.4442 | 0.0000 | 12.3047 | 8.0137 | - |
| lmdb | False | Disk Cache | 6.3084 | - | - | - | 6.1989 |
| mdbx | False | Memory + Save/Load | 4.0676 | 0.0000 | 14.7320 | 10.6485 | - |
| mdbx | False | Disk Cache | 12.1854 | - | - | - | 9.8778 |
| sqlite | True | Memory + Save/Load | 3.6770 | 0.0000 | 13.3678 | 8.8858 | - |
| sqlite | True | Disk Cache | 16.7904 | - | - | - | 7.3988 |
| lmdb | True | Memory + Save/Load | 3.6228 | 0.0000 | 12.9674 | 8.8238 | - |
| lmdb | True | Disk Cache | 6.7653 | - | - | - | 6.4712 |
| mdbx | True | Memory + Save/Load | 4.3847 | 0.0000 | 15.1434 | 10.8973 | - |
| mdbx | True | Disk Cache | 12.7424 | - | - | - | 11.0355 |

## Key Takeaways

1. **LMDB is fastest for the disk cache on the GIL build, but is not safe on free-threaded Python**: LMDB took `6.3s` cold and `6.2s` for a hit, against `16.4s`/`6.2s` for SQLite and `12.2s`/`9.9s` for MDBX. However, the LMDB bindings segfault under parallel load on Python 3.14t, so prefer `sqlite` or `mdbx` there.
2. **SQLite hits are as fast as LMDB; its cost is the cold write path**: a SQLite cache hit (`6.2s`) matches LMDB, but the cold run that populates the cache is about 2.6x slower.
3. **`engine.save()`/`load()` is expensive at full graph size**: with all ~250,000 memos retained, a save takes `12-15s` and a load `7-11s` on every backend, several times the `3-4s` it takes to compute the graph in memory. The earlier sub-second figures only covered the ~10,000 memos that survived eviction. For graphs this cheap to compute, the disk cache or plain recomputation beats explicit save/load.
4. **Engine stats overhead is now within noise**: `Stats: True` no longer shows a consistent cost (e.g. SQLite memory cold `3.2s` vs `3.7s`, LMDB `4.4s` vs `3.6s`).
5. **Hot compute is O(1) (0.0000s)**: The engine tracks a global `revision` that only increments when inputs change. When no inputs change, the top-level query's `memo.verified_at` matches the current `revision`, so the cached result is returned without traversing the graph.
