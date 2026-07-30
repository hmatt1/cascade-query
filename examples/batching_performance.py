import time
import shutil
import os
from cascade import Engine, CascadeDict


def main():
    """
    This example demonstrates the performance difference between individually
    assigning items to a CascadeDict versus batching assignments using .update().

    Because CascadeDict persists changes to disk (MDBX), individual assignments
    via __setitem__ open and commit a separate disk transaction per item.
    Using .update() batches all operations into a single disk transaction,
    making bulk-inserts nearly instantaneous.
    """
    cache_dir = "./my_cascade_cache_perf"

    # Clean up the cache directory if it exists for a fresh test
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)

    engine = Engine(cache_dir=cache_dir)

    # Number of items to insert. We use 10,000 for a quick demo, but in real
    # scenarios (like AST parsing), this could be 150,000+.
    NUM_ITEMS = 10000

    print(f"=== Demonstrating {NUM_ITEMS} insertions ===")

    # --- 1. The Slow Way (Unbatched) ---
    slow_dict = CascadeDict(engine, name="slow_dict")

    print("\nStep 1. Starting unbatched insertions (Slow)...")
    start_time = time.time()

    # Modifying the dict directly in a loop triggers a disk sync on EVERY iteration
    for i in range(NUM_ITEMS):
        slow_dict[f"key_{i}"] = i

    slow_duration = time.time() - start_time
    print(f"Unbatched insertions took: {slow_duration:.2f} seconds")

    # --- 2. The Fast Way (Batched via update) ---
    fast_dict = CascadeDict(engine, name="fast_dict")

    print("\nStep 2. Starting batched insertions (Fast)...")
    start_time = time.time()

    # Buffer the changes in a standard Python dictionary in memory
    buffered_dict = {}
    for i in range(NUM_ITEMS):
        buffered_dict[f"key_{i}"] = i

    # .update() applies all changes inside a SINGLE disk transaction
    fast_dict.update(buffered_dict)

    fast_duration = time.time() - start_time
    print(f"Batched insertions took: {fast_duration:.2f} seconds")

    # Show results
    if fast_duration > 0:
        speedup = slow_duration / fast_duration
        print(f"\nResult: Batching was {speedup:.1f}x faster!")

    print("\nExample complete.")


if __name__ == "__main__":
    main()
