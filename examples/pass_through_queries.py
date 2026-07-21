import cascade

def run_example():
    engine = cascade.Engine()

    @engine.input
    def raw_data() -> list[int]:
        print("Fetching raw data...")
        return list(range(1000000))

    # We use memoize=False for this intermediate mapping step
    # because it produces a large output that we don't want to keep in the LRU cache,
    # but we DO want it to participate in the dependency graph.
    @engine.query(memoize=False)
    def mapped_data() -> list[int]:
        print("Computing mapped data...")
        data = raw_data()
        return [x * 2 for x in data]

    @engine.query
    def aggregated_result() -> int:
        print("Computing aggregated result...")
        return sum(mapped_data())

    print("Step 1: First Run ---")
    print("Result:", aggregated_result())

    print("\nStep 2: Second Run (Cache Hit) ---")
    # This will NOT recompute mapped_data() because aggregated_result() is cached
    # and its dependencies (raw_data) haven't changed.
    print("Result:", aggregated_result())

    print("\nStep 3: Third Run (Updating Input) ---")
    raw_data.set([1, 2, 3])
    # This WILL recompute mapped_data() because raw_data changed.
    # Since mapped_data() is memoize=False, it executes to generate its output
    # but doesn't store the output in the LRU cache.
    print("Result:", aggregated_result())

if __name__ == "__main__":
    run_example()
