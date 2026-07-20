# Disk Cache Vacuuming

The `engine.prune(roots, vacuum_disk=True)` method allows for fine-grained garbage collection of the persistent LMDB disk cache, reclaiming logical space by deleting cache entries and blobs that are no longer reachable from the provided roots.

While this implementation handles logical cleanup gracefully, there are several known limitations and gaps to be aware of:

## 1. Multi-Process Hostility
LMDB enables multiple Cascade `Engine` instances (across different Python processes) to safely share the same `.cascade_cache` directory. However, `vacuum_disk=True` is a **global, destructive operation**. If Process A triggers a vacuum using its own graph roots, it has no knowledge of Process B's graph roots and will aggressively delete Process B's cached entries. There is no cross-process reference counting.
**Recommendation:** Do not use `vacuum_disk=True` if multiple independent processes actively rely on the same cache directory with disparate query graphs.

## 2. Intra-Process Concurrency Races
Currently, the disk cache traversal runs *outside* of the `GraphStore`'s strict lock to avoid halting all engine activity for long periods. If another thread is actively evaluating new queries (e.g., via `compute_many` or `submit`) concurrently with a vacuum, it might write new queries to disk that the vacuum traversal misses. The trailing sweep will then delete those newly written queries. While safe (no corruption occurs), it results in wasted computation.
**Recommendation:** Vacuuming should ideally be done when the engine is quiescent.

## 3. Physical File Shrinkage (Fragmentation)
Deleting records in an LMDB database marks the pages as free to be reused by future writes, preventing the cache from growing infinitely. However, it **does not shrink the physical `data.mdb` file size** on the OS disk. To truly return bytes to the operating system, LMDB requires doing an environment copy to a new compacted database, which this pruning method does not perform. 
**Recommendation:** To completely recover physical disk space, simply stop all engine instances and delete the `data.mdb` file or the entire cache directory.

## 4. Memory Footprint During Traversal
For astronomically large disk caches, the graph traversal builds up the sets of reachable entries and blobs entirely in memory before executing the sweeps. For most users this is trivial, but for caches with millions of entries, it could cause an unexpected memory spike during the vacuum operation.
