from __future__ import annotations

import concurrent.futures
import time

from cascade import Engine, QueryCancelled


def run_concurrency_demo() -> None:
    print("=== Concurrent dedup + cancellation example ===")
    engine = Engine()
    counters = {"expensive": 0}

    @engine.input
    def base() -> int:
        return 0

    @engine.query
    def expensive() -> int:
        counters["expensive"] += 1
        # Simulated expensive work.
        time.sleep(0.08)
        return base() + 1

    print("Step 1: Initial result with base=41.")
    base.set(41)
    print("Result:", expensive())
    print("Counters:", counters)

    print("Step 2: Multiple concurrent calls for the same query key.")
    base.set(42)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(expensive) for _ in range(6)]
        values = [future.result() for future in futures]
    print("Concurrent values:", values)
    print("Counters (single computation shared by all callers):", counters)

    print("Step 3: Background query cancellation.")
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        base.set(10)
        stale = engine.submit(expensive, executor=pool)
        time.sleep(0.02)
        base.set(20)
        try:
            stale.result(timeout=2.0)
            print("Error: stale computation did not cancel.")
        except QueryCancelled:
            print("Success: stale computation cancelled after input changed.")
        print("Fresh value after mutation:", expensive())

    dedup_waits = sum(1 for event in engine.traces() if event.event == "dedup_wait")
    print("Trace summary:", {"dedup_wait_events": dedup_waits, "total_trace_events": len(engine.traces())})
    print("Example complete.")


if __name__ == "__main__":
    run_concurrency_demo()
