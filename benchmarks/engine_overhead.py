"""Absolute-time benchmark of engine bookkeeping overhead.

Unlike ``performance_suite`` (pass/fail ratio gates), this reports raw medians
so individual optimizations can be compared run to run. Query bodies in the
synthetic graph are trivial, so the numbers are almost entirely engine cost.

Usage::

    python -m benchmarks.engine_overhead --depth 20 --fanout 1000 --rounds 5
    python -m benchmarks.engine_overhead --disk sqlite mdbx
"""

from __future__ import annotations

import argparse
import statistics
import tempfile
import time
from pathlib import Path
from typing import Any, Callable

from cascade import Engine
from cascade._synthetic_graph import build_fanout_chain_pipeline


def _build(depth: int, fanout: int, **engine_kwargs: Any) -> tuple[Engine, Any, Any]:
    # Size the LRU to hold the whole graph so eviction never skews the timings.
    engine_kwargs.setdefault("max_entries", depth * fanout + 16)
    engine = Engine(**engine_kwargs)
    leaf, _, aggregate = build_fanout_chain_pipeline(engine, depth=depth, fanout=fanout)
    for branch in range(fanout):
        leaf.set(branch, branch)
    return engine, leaf, aggregate


def _timed(fn: Callable[[], Any]) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def _close(engine: Engine) -> None:
    engine.shutdown()
    disk = getattr(engine, "_disk", None)
    if disk is not None:
        disk.close()


def run_memory(depth: int, fanout: int) -> dict[str, float]:
    engine, leaf, aggregate = _build(depth, fanout)
    cold = _timed(aggregate)
    hot = _timed(aggregate)
    leaf.set(0, 10_000)
    one_leaf = _timed(aggregate)

    def no_op_bump() -> None:
        # Same value: the input backdates, but the revision still advances,
        # so every memo must be re-verified.
        leaf.set(1, 1)
        aggregate()

    reverify = _timed(no_op_bump)
    _close(engine)
    return {
        "cold": cold,
        "hot": hot,
        "one_leaf_mutation": one_leaf,
        "noop_revision_reverify": reverify,
    }


def run_disk(depth: int, fanout: int, backend: str) -> dict[str, float]:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        cache_dir = Path(tmp) / "cache"
        engine, _, aggregate = _build(
            depth, fanout, cache_dir=cache_dir, cache_backend=backend
        )
        cold = _timed(aggregate)
        flush = _timed(engine._evaluator.flush_disk)  # noqa: SLF001
        _close(engine)

        engine, _, aggregate = _build(
            depth, fanout, cache_dir=cache_dir, cache_backend=backend
        )
        hit = _timed(aggregate)
        _close(engine)
    return {"cold": cold, "flush": flush, "hit": hit}


def _median_of(rounds: int, fn: Callable[[], dict[str, float]]) -> dict[str, float]:
    samples: dict[str, list[float]] = {}
    for _ in range(rounds):
        for name, value in fn().items():
            samples.setdefault(name, []).append(value)
    return {name: statistics.median(values) for name, values in samples.items()}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Absolute-time benchmark of engine bookkeeping overhead."
    )
    parser.add_argument("--depth", type=int, default=20)
    parser.add_argument("--fanout", type=int, default=1000)
    parser.add_argument("--rounds", type=int, default=5)
    parser.add_argument(
        "--disk",
        nargs="*",
        default=[],
        choices=["sqlite", "mdbx"],
        help="also benchmark these persistent cache backends",
    )
    args = parser.parse_args()

    nodes = args.depth * args.fanout
    print(
        f"depth={args.depth} fanout={args.fanout} (~{nodes} nodes), "
        f"median of {args.rounds} rounds"
    )
    print("\n| Workload | Time (ms) |\n| --- | ---: |")
    memory = _median_of(args.rounds, lambda: run_memory(args.depth, args.fanout))
    for name, value in memory.items():
        print(f"| memory {name} | {value * 1000:.2f} |")
    for backend in args.disk:
        disk = _median_of(
            args.rounds, lambda b=backend: run_disk(args.depth, args.fanout, b)
        )
        for name, value in disk.items():
            print(f"| {backend} {name} | {value * 1000:.2f} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
