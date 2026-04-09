from __future__ import annotations

import argparse
import os
import statistics
import sys
import sysconfig
import time

from cascade import Engine


def _cpu_heavy_mix(seed: int, rounds: int) -> int:
    """Pure-Python integer mixing loop that stays CPU-bound."""
    value = seed & 0xFFFFFFFF
    for i in range(rounds):
        value = ((value * 1664525 + 1013904223) ^ i) & 0xFFFFFFFF
        value ^= value >> 13
        value = (value * 1274126177) & 0xFFFFFFFF
    return value


def _measure_compute_many(task_count: int, rounds: int, workers: int, repeats: int) -> tuple[list[float], int]:
    engine = Engine()

    @engine.input
    def nonce() -> int:
        return 0

    @engine.query
    def cpu_job(task_id: int) -> int:
        # Depend on nonce so each trial invalidates memoized values and recomputes.
        return _cpu_heavy_mix(task_id ^ nonce(), rounds)

    calls = [(cpu_job, (task_id,)) for task_id in range(task_count)]

    nonce.set(1)
    engine.compute_many(calls, workers=workers)

    durations: list[float] = []
    checksum = 0
    for run_idx in range(repeats):
        nonce.set(run_idx + 2)
        started = time.perf_counter()
        values = engine.compute_many(calls, workers=workers)
        durations.append(time.perf_counter() - started)
        checksum ^= sum(values) & 0xFFFFFFFF
    return durations, checksum


def _gil_state() -> str:
    if hasattr(sys, "_is_gil_enabled"):
        return "enabled" if sys._is_gil_enabled() else "disabled"
    return "unknown"


def main() -> None:
    cpu_count = os.cpu_count() or 2
    default_workers = min(8, max(2, cpu_count))
    default_tasks = default_workers * 12

    parser = argparse.ArgumentParser(
        description=(
            "Benchmark CPU-bound threaded compute_many work. "
            "Run this script once with python3.14 (GIL on) and once with "
            "python3.14t + PYTHON_GIL=0 to compare."
        )
    )
    parser.add_argument("--tasks", type=int, default=default_tasks, help="Number of independent query keys to run.")
    parser.add_argument("--rounds", type=int, default=300_000, help="CPU loop iterations per task.")
    parser.add_argument("--workers", type=int, default=default_workers, help="Worker threads for the parallel run.")
    parser.add_argument("--repeats", type=int, default=3, help="How many measured runs per configuration.")
    args = parser.parse_args()

    if args.tasks < 1:
        raise ValueError("--tasks must be >= 1")
    if args.rounds < 1:
        raise ValueError("--rounds must be >= 1")
    if args.workers < 1:
        raise ValueError("--workers must be >= 1")
    if args.repeats < 1:
        raise ValueError("--repeats must be >= 1")

    print("=== GIL parallel speedup example ===")
    print(f"python: {sys.version.split()[0]}")
    print(f"executable: {sys.executable}")
    print(f"Py_GIL_DISABLED build flag: {sysconfig.get_config_var('Py_GIL_DISABLED')}")
    print(f"runtime GIL state: {_gil_state()}")
    print(
        "workload:",
        {"tasks": args.tasks, "rounds": args.rounds, "workers": args.workers, "repeats": args.repeats},
    )
    print()

    serial_times, serial_checksum = _measure_compute_many(
        task_count=args.tasks,
        rounds=args.rounds,
        workers=1,
        repeats=args.repeats,
    )
    parallel_times, parallel_checksum = _measure_compute_many(
        task_count=args.tasks,
        rounds=args.rounds,
        workers=args.workers,
        repeats=args.repeats,
    )
    if serial_checksum != parallel_checksum:
        raise RuntimeError("Sanity check failed: checksums differ between serial and parallel runs.")

    serial_median = statistics.median(serial_times)
    parallel_median = statistics.median(parallel_times)
    speedup = serial_median / parallel_median

    print(f"serial times (workers=1):    {[round(value, 3) for value in serial_times]}")
    print(f"parallel times (workers={args.workers}): {[round(value, 3) for value in parallel_times]}")
    print(f"median serial seconds:   {serial_median:.3f}")
    print(f"median parallel seconds: {parallel_median:.3f}")
    print(f"threaded speedup in this runtime: {speedup:.2f}x")
    print()
    print(
        "Run the same command under python3.14 and python3.14t (with PYTHON_GIL=0), "
        "then compare the median parallel seconds and speedup lines."
    )


if __name__ == "__main__":
    main()
