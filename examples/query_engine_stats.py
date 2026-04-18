"""How to use Engine stats: per-query body time and LRU eviction visibility.

``stats_summary()`` answers two operational questions without external profilers:

1. **Where did query bodies spend time?** — ``by_key`` aggregates **real** wall time
   (``time.perf_counter`` by default) for each successful *recompute*. Cache hits are
   not timed. Timers wrap the **entire** Python callable: if a query calls another
   query, the **parent’s** duration is **inclusive** of the child’s work. Rows are
   **not** mutually exclusive, so you cannot sum ``by_key`` values to get “total CPU
   across the graph.”

2. **Is the memo cache under pressure?** — When ``len(memos) > max_entries``,
   the engine evicts LRU memo entries. ``evictions_total`` and
   ``evictions_recent`` count and remember those evictions (not ``prune``).

This script uses small ``time.sleep`` calls so differences show up in ``by_key`` on a
normal machine (total runtime is still well under a second).

Run: ``python examples/query_engine_stats.py`` (from repo root, or with ``src`` on ``PYTHONPATH``).
"""

from __future__ import annotations

import time

from cascade import Engine

# Small sleeps so by_key differs clearly; wide assert bands tolerate scheduler jitter.
_SLEEP_EXPENSIVE = 0.06
_SLEEP_CHEAP = 0.012
_SLEEP_AGG_PAD = 0.018
_SLEEP_LEAF_A = 0.045
_SLEEP_LEAF_B = 0.014


def _print_by_key_sorted(engine: Engine) -> None:
    by = engine.stats_summary()["by_key"]
    for key, sec in sorted(by.items(), key=lambda kv: (-kv[1], kv[0])):
        short = key if len(key) < 72 else key[:32] + "…" + key[-32:]
        print(f"  {sec:8.3f}s  {short}")


def _by_key_subset(engine: Engine, needle: str) -> float:
    by = engine.stats_summary()["by_key"]
    return next(v for k, v in by.items() if needle in k)


def run_flat_queries_real_time() -> None:
    print(
        "=== 1a) Each query run on its own — by_key tracks real sleep in the body ===\n"
        "(Default stats use time.perf_counter; no custom stats_clock.)"
    )
    engine = Engine(stats=True)

    @engine.input
    def limit() -> int:
        return 100

    @engine.query
    def expensive() -> int:
        time.sleep(_SLEEP_EXPENSIVE)
        return limit() + 1

    @engine.query
    def cheap() -> int:
        time.sleep(_SLEEP_CHEAP)
        return limit() + 2

    limit.set(10)
    assert expensive() == 11
    assert cheap() == 12
    _print_by_key_sorted(engine)

    t_exp = _by_key_subset(engine, "expensive")
    t_ch = _by_key_subset(engine, "cheap")
    assert t_exp > 0.04 and t_ch > 0.008, "sleep should dominate body time"
    assert t_exp > t_ch * 2, "heavier query should have larger by_key"

    before = dict(engine.stats_summary()["by_key"])
    assert expensive() == 11 and cheap() == 12
    assert engine.stats_summary()["by_key"] == before
    print("Second calls: cache hits — by_key unchanged.\n")


def run_nested_queries_real_time() -> None:
    print(
        "=== 1b) Nested queries — parent row *includes* child sleep time ===\n"
        "by_key for aggregate is inclusive (do not sum all rows for 'total work')."
    )
    engine = Engine(stats=True)

    @engine.input
    def lim() -> int:
        return 0

    @engine.query
    def leaf_a() -> int:
        time.sleep(_SLEEP_LEAF_A)
        return lim() + 1

    @engine.query
    def leaf_b() -> int:
        time.sleep(_SLEEP_LEAF_B)
        return lim() + 1

    @engine.query
    def aggregate() -> int:
        time.sleep(_SLEEP_AGG_PAD)
        return leaf_a() + leaf_b()

    lim.set(0)
    assert aggregate() == 2
    _print_by_key_sorted(engine)

    t_agg = _by_key_subset(engine, "aggregate")
    t_a = _by_key_subset(engine, "leaf_a")
    t_b = _by_key_subset(engine, "leaf_b")
    assert t_a > 0.03 and t_b > 0.008
    assert t_agg > t_a and t_agg > t_b, "parent timer covers pad + both children"
    rough_inclusive = _SLEEP_AGG_PAD + _SLEEP_LEAF_A + _SLEEP_LEAF_B
    assert t_agg > rough_inclusive * 0.75, "aggregate should reflect stacked sleeps"
    print()


def run_lru_evictions() -> None:
    print("=== 2) LRU eviction counters (small max_entries) ===")
    engine = Engine(max_entries=2, stats=True, stats_eviction_recent_cap=5)

    @engine.input
    def seed() -> int:
        return 0

    @engine.query
    def node_a() -> int:
        return seed() + 1

    @engine.query
    def node_b() -> int:
        return seed() + 2

    @engine.query
    def node_c() -> int:
        return seed() + 3

    seed.set(0)
    node_a()
    node_b()
    assert engine.inspect_graph()["memo_count"] == 2

    node_c()
    s = engine.stats_summary()
    assert s["memo_count"] == 2
    assert s["evictions_total"] >= 1
    print(f"memo_count={s['memo_count']} max_entries={s['max_entries']}")
    print(f"evictions_total={s['evictions_total']}")
    print("evictions_recent (most recent last):")
    for key in s["evictions_recent"]:
        print(f"  - {key}")
    print()


def run_runtime_toggle() -> None:
    print("=== 3) Turning stats on after the fact ===")
    engine = Engine(stats=False)

    @engine.input
    def i() -> int:
        return 0

    @engine.query
    def q() -> int:
        return i() + 1

    i.set(1)
    q()
    assert engine.stats_summary()["by_key"] == {}

    engine.enable_stats(True)
    i.set(2)  # invalidate so q recomputes
    q()
    assert engine.stats_summary()["by_key"], "expected one timed recompute after enable_stats"
    print("After enable_stats and input bump, by_key is non-empty.\n")


def main() -> None:
    print("Step 1: Flat queries — by_key tracks real sleep in each body.")
    run_flat_queries_real_time()
    print("Step 2: Nested queries — parent by_key includes child time.")
    run_nested_queries_real_time()
    print("Step 3: LRU eviction counters with a tiny memo table.")
    run_lru_evictions()
    print("Step 4: Enable stats after cold runs, then bump input and time one recompute.")
    run_runtime_toggle()
    print("Example complete.")


if __name__ == "__main__":
    main()
