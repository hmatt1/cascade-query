from __future__ import annotations

import concurrent.futures
import time

from cascade import Engine


def run_demo() -> None:
    print("=== Large rules engine with concurrent duplicate requests example ===")
    print("Goal: deduplicate in-flight work for identical evaluations.")

    engine = Engine()
    calls = {"evaluate": 0}

    @engine.input
    def facts(subject: str) -> tuple[str, ...]:
        return ()

    @engine.input
    def policy_version() -> int:
        return 0

    @engine.query
    def evaluate(subject: str, action: str, resource: str) -> bool:
        calls["evaluate"] += 1
        # Simulate expensive policy graph traversal.
        time.sleep(0.07)
        token = f"{action}:{resource}"
        return token in facts(subject) and policy_version() >= 1

    print("Step 1: Seed baseline policy data and warm cache.")
    facts.set("user:alice", ("read:repoA", "write:repoA"))
    policy_version.set(1)
    print("Warm result:", evaluate("user:alice", "read", "repoA"))
    print("Counter:", calls)

    print("Step 2: Invalidate and send many duplicate concurrent requests.")
    policy_version.set(2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(evaluate, "user:alice", "read", "repoA") for _ in range(12)]
        values = [future.result() for future in futures]
    print("Concurrent values:", values)
    print("Counter after deduped recompute:", calls)

    print("Step 3: Mix duplicate and unique keys concurrently.")
    facts.set("user:bob", ("read:repoA",))
    policy_version.set(3)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        mixed_futures = [
            pool.submit(evaluate, "user:alice", "read", "repoA"),
            pool.submit(evaluate, "user:alice", "read", "repoA"),
            pool.submit(evaluate, "user:bob", "read", "repoA"),
            pool.submit(evaluate, "user:bob", "write", "repoA"),
            pool.submit(evaluate, "user:bob", "write", "repoA"),
        ]
        mixed_values = [future.result() for future in mixed_futures]
    print("Mixed results:", mixed_values)
    print("Counter after mixed batch:", calls)

    dedup_waits = sum(1 for event in engine.traces() if event.event == "dedup_wait")
    print("Trace summary:", {"dedup_wait_events": dedup_waits, "total_events": len(engine.traces())})
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
