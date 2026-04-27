from __future__ import annotations

import os
import sys
import time
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
_src = _repo_root / "src"
if _src.exists():
    sys.path.insert(0, str(_src))

from cascade import Engine  # noqa: E402


def _make_engine_and_calls(durations: dict[str, float]) -> tuple[Engine, list[tuple[object, tuple[object, ...]]]]:
    engine = Engine()
    progress = engine.accumulator("progress")

    @engine.query
    def sleep_job(name: str, seconds: float) -> str:
        progress.push(("start", name, seconds))
        time.sleep(seconds)
        progress.push(("done", name, seconds))
        return f"{name} slept {seconds:.2f}s"

    calls = [(sleep_job, (name, seconds)) for name, seconds in durations.items()]
    return engine, calls


def main() -> None:
    print("=== compute_many vs compute_many_stream accumulator example ===")

    # Keep the demo visually obvious in real usage, but make tests fast.
    # (The test runner executes all examples.)
    if os.environ.get("PYTEST_CURRENT_TEST"):
        durations = {"A": 0.06, "B": 0.02, "C": 0.04}
    else:
        durations = {"A": 6.0, "B": 2.0, "C": 4.0}

    engine, calls = _make_engine_and_calls(durations)

    effects_batch: dict[str, list[object]] = {}
    print("Step 1: Run 3 independent jobs in parallel.")
    t0 = time.perf_counter()
    results = engine.compute_many(calls, workers=3, effects=effects_batch)
    dt = time.perf_counter() - t0

    print("Step 2: Print results and collected side-effects.")
    print(f"compute_many returned {len(results)} results in {dt:.3f}s")
    print("results:", results)
    print()
    print("Accumulator output (call order):")
    for item in effects_batch.get("progress", []):
        print(" ", item)

    print()
    print("Step 3: Run the same jobs via streaming results.")
    # Important: use a fresh engine/graph so the sleeps are not retrieved from cache.
    engine_stream, calls_stream = _make_engine_and_calls(durations)
    effects_stream: dict[str, list[object]] = {}
    t0 = time.perf_counter()
    finished = 0
    total = len(calls_stream)
    for idx, value, call_effects in engine_stream.compute_many_stream(
        calls_stream, workers=3, effects=effects_stream
    ):
        finished += 1
        name = calls_stream[idx][1][0]
        dt = time.perf_counter() - t0
        print(f"finished {finished}/{total}: {name} ({dt:.2f}s) -> {value}")
        for item in call_effects.get("progress", []):
            print("  effect:", item)

    print("\nStep 4: Aggregated side-effects for the streaming run (call order).")
    for item in effects_stream.get("progress", []):
        print(" ", item)

    print("\nExample complete.")


if __name__ == "__main__":
    main()

