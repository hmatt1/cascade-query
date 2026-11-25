"""Async parity tests on a running event loop.

Every async test here awaits queries on a running asyncio loop so execution
takes the actual async evaluator paths (``query_call_async``,
``compute_or_get_memo_async``, ``try_mark_green_async``, ``recompute_async``,
``_try_hydrate_from_disk_async``) rather than the synchronous wrapper.
"""

from __future__ import annotations

import asyncio
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from cascade import (
    CascadeDict,
    CascadeList,
    CycleError,
    Engine,
    QueryCancelled,
)

TIMEOUT = 10.0


# ---------------------------------------------------------------------------
# 1. Accumulator effects (hit replay, nested async/sync, rollback, waiter)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_accumulator_hit_replay_memory_and_disk(tmp_path: Path) -> None:
    engine = Engine(cache_dir=tmp_path)
    try:
        log = engine.accumulator("log")
        runs = 0

        @engine.input
        async def x() -> int:
            return 5

        @engine.query
        async def compute() -> int:
            nonlocal runs
            runs += 1
            await asyncio.sleep(0)
            v = await x()
            log.push(f"computed:{v}")
            return v * 2

        eff1: dict[str, list[Any]] = {}
        assert await compute(effects=eff1) == 10
        assert runs == 1
        assert eff1 == {"log": ["computed:5"]}

        # Memory cache hit must replay effects once (not zero, not twice).
        eff2: dict[str, list[Any]] = {}
        assert await compute(effects=eff2) == 10
        assert runs == 1
        assert eff2 == {"log": ["computed:5"]}
    finally:
        engine.shutdown()

    # Reopen from disk: async hydration must replay effects once.
    engine2 = Engine(cache_dir=tmp_path)
    try:
        log2 = engine2.accumulator("log")
        runs2 = 0

        @engine2.input
        async def x() -> int:
            return 5

        @engine2.query
        async def compute() -> int:
            nonlocal runs2
            runs2 += 1
            await asyncio.sleep(0)
            v = await x()
            log2.push(f"computed:{v}")
            return v * 2

        eff3: dict[str, list[Any]] = {}
        assert await compute(effects=eff3) == 10
        assert runs2 == 0
        assert eff3 == {"log": ["computed:5"]}
    finally:
        engine2.shutdown()


@pytest.mark.asyncio
async def test_async_accumulator_nested_async_and_sync_replay() -> None:
    engine = Engine()
    log = engine.accumulator("log")
    runs = {"inner_async": 0, "inner_sync": 0, "outer": 0}

    @engine.input
    def flag() -> int:
        return 1

    @engine.query
    async def inner_async() -> int:
        runs["inner_async"] += 1
        await asyncio.sleep(0)
        log.push("inner_async")
        return 10

    @engine.query
    def inner_sync() -> int:
        runs["inner_sync"] += 1
        log.push("inner_sync")
        return 20

    @engine.query
    async def outer() -> int:
        runs["outer"] += 1
        log.push("outer_start")
        a = await inner_async()
        b = inner_sync()
        _ = flag()
        log.push("outer_end")
        return a + b

    eff1: dict[str, list[Any]] = {}
    assert await outer(effects=eff1) == 30
    assert eff1 == {"log": ["outer_start", "inner_async", "inner_sync", "outer_end"]}
    assert runs == {"inner_async": 1, "inner_sync": 1, "outer": 1}

    # Full cache hit replays all nested effects in order.
    eff2: dict[str, list[Any]] = {}
    assert await outer(effects=eff2) == 30
    assert eff2 == {"log": ["outer_start", "inner_async", "inner_sync", "outer_end"]}
    assert runs == {"inner_async": 1, "inner_sync": 1, "outer": 1}

    # Changing flag forces outer to recompute while inner_async & inner_sync are green.
    flag.set(2)
    eff3: dict[str, list[Any]] = {}
    assert await outer(effects=eff3) == 30
    assert eff3 == {"log": ["outer_start", "inner_async", "inner_sync", "outer_end"]}
    assert runs == {"inner_async": 1, "inner_sync": 1, "outer": 2}


@pytest.mark.asyncio
async def test_async_accumulator_failed_query_and_fixed_point_rollback() -> None:
    engine = Engine()
    log = engine.accumulator("log")

    @engine.query
    async def failing_child() -> int:
        await asyncio.sleep(0)
        log.push("child_before_error")
        raise RuntimeError("boom")

    @engine.query
    async def failing_root() -> int:
        log.push("root_before_child")
        return await failing_child()

    eff_fail: dict[str, list[Any]] = {}
    with pytest.raises(RuntimeError, match="boom"):
        await failing_root(effects=eff_fail)
    # Failed root query must not commit staged effects.
    assert eff_fail == {}

    # Fixed-point async query: effects from abandoned iterations are retracted.
    @engine.query(fixed_point=0)
    async def fp_grow() -> int:
        await asyncio.sleep(0)
        prev = await fp_grow()
        log.push(f"iter:{prev}")
        return min(prev + 1, 3)

    eff_fp: dict[str, list[Any]] = {}
    assert await fp_grow(effects=eff_fp) == 3
    assert eff_fp == {"log": ["iter:3"]}

    # Cached hit replays only the converged iteration's effects.
    eff_fp_hit: dict[str, list[Any]] = {}
    assert await fp_grow(effects=eff_fp_hit) == 3
    assert eff_fp_hit == {"log": ["iter:3"]}


@pytest.mark.asyncio
async def test_async_accumulator_concurrent_waiter_replay() -> None:
    engine = Engine()
    log = engine.accumulator("log")
    started = asyncio.Event()
    release = asyncio.Event()
    runs = 0

    @engine.query
    async def shared() -> int:
        nonlocal runs
        runs += 1
        started.set()
        await release.wait()
        log.push("shared_effect")
        return 42

    eff_owner: dict[str, list[Any]] = {}
    eff_waiter: dict[str, list[Any]] = {}

    t1 = asyncio.create_task(shared(effects=eff_owner))
    await asyncio.wait_for(started.wait(), timeout=TIMEOUT)
    t2 = asyncio.create_task(shared(effects=eff_waiter))
    # Yield so t2 enters dedup wait before releasing t1.
    await asyncio.sleep(0.01)
    release.set()

    r1, r2 = await asyncio.wait_for(asyncio.gather(t1, t2), timeout=TIMEOUT)
    assert (r1, r2) == (42, 42)
    assert runs == 1
    assert eff_owner == {"log": ["shared_effect"]}
    assert eff_waiter == {"log": ["shared_effect"]}


# ---------------------------------------------------------------------------
# 2. Early bail-out, run counts, and memoize=False on async chains
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_chain_early_bailout_and_run_counts() -> None:
    engine = Engine()
    runs = {"norm": 0, "mid": 0, "top": 0}

    @engine.input
    def raw() -> int:
        return 2

    @engine.query
    async def norm() -> int:
        runs["norm"] += 1
        await asyncio.sleep(0)
        return raw() % 2

    @engine.query
    async def mid() -> int:
        runs["mid"] += 1
        await asyncio.sleep(0)
        return (await norm()) + 10

    @engine.query
    async def top() -> int:
        runs["top"] += 1
        await asyncio.sleep(0)
        return (await mid()) * 3

    assert await top() == 30
    assert runs == {"norm": 1, "mid": 1, "top": 1}

    # Same parity: norm recomputes with equal value; mid and top bail out green.
    raw.set(4)
    assert await top() == 30
    assert runs == {"norm": 2, "mid": 1, "top": 1}

    # Different parity: entire chain recomputes.
    raw.set(5)
    assert await top() == 33
    assert runs == {"norm": 3, "mid": 2, "top": 2}


@pytest.mark.asyncio
async def test_async_memoize_false_recomputes_and_skips_disk_hydration(
    tmp_path: Path,
) -> None:
    engine = Engine(cache_dir=tmp_path)
    try:
        runs = {"unmemo": 0, "dependent": 0}

        @engine.input
        def x() -> int:
            return 3

        @engine.query(memoize=False)
        async def unmemo() -> int:
            runs["unmemo"] += 1
            await asyncio.sleep(0)
            return x() * 10

        @engine.query
        async def dependent() -> int:
            runs["dependent"] += 1
            await asyncio.sleep(0)
            return (await unmemo()) + 1

        # Direct awaits on memoize=False always recompute (need_value=True).
        assert await unmemo() == 30
        assert await unmemo() == 30
        assert runs["unmemo"] == 2

        # Dependent runs unmemo once, then at same revision dependent hits cache.
        assert await dependent() == 31
        assert runs == {"unmemo": 3, "dependent": 1}
        assert await dependent() == 31
        assert runs == {"unmemo": 3, "dependent": 1}

        # Unrelated input bump: verifying dependent checks unmemo with need_value=False,
        # which can mark unmemo green without recomputing it.
        @engine.input
        def unrelated() -> int:
            return 0

        unrelated.set(1)
        assert await dependent() == 31
        assert runs["dependent"] == 1
        assert runs["unmemo"] == 3

        # Direct await of unmemo after green revision still recomputes (not cached).
        assert await unmemo() == 30
        assert runs["unmemo"] == 4
    finally:
        engine.shutdown()


# ---------------------------------------------------------------------------
# 3. Eviction (max_entries), prune, and sweep_unaccessed with async & FP
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_eviction_max_entries_and_fixed_point() -> None:
    engine = Engine(max_entries=3)
    runs: dict[int, int] = {}

    @engine.input
    def step() -> int:
        return 1

    @engine.query
    async def leaf(i: int) -> int:
        runs[i] = runs.get(i, 0) + 1
        await asyncio.sleep(0)
        return i * 10

    @engine.query(fixed_point=0)
    async def head() -> int:
        await asyncio.sleep(0)
        return min((await member()) + step(), 4)

    @engine.query
    async def member() -> int:
        await asyncio.sleep(0)
        return await head()

    # Fill and exceed max_entries=3 so earlier entries are evicted.
    for i in range(6):
        assert await leaf(i) == i * 10
    assert len(engine._internals.memos) <= 3  # noqa: SLF001

    # leaf(0) was evicted and must recompute.
    assert await leaf(0) == 0
    assert runs[0] == 2

    # Run async fixed point, then evict member by running other queries.
    assert await head() == 4
    assert await member() == 4
    for i in range(10, 14):
        assert await leaf(i) == i * 10

    # Asking for member after eviction still converges via head.
    step.set(2)
    assert await member() == 4
    assert await head() == 4


@pytest.mark.asyncio
async def test_async_prune_and_sweep_unaccessed() -> None:
    engine = Engine()
    runs = {"a": 0, "b": 0}

    @engine.input
    def base() -> int:
        return 1

    @engine.query
    async def qa(i: int) -> int:
        runs["a"] += 1
        await asyncio.sleep(0)
        return i + base()

    @engine.query
    async def qb() -> int:
        runs["b"] += 1
        await asyncio.sleep(0)
        return await qa(1) + 10

    @engine.query(fixed_point=0)
    async def fp_head() -> int:
        await asyncio.sleep(0)
        return min((await fp_member()) + base(), 5)

    @engine.query
    async def fp_member() -> int:
        await asyncio.sleep(0)
        return await fp_head()

    assert await qa(1) == 2
    assert await qa(2) == 3
    assert await qb() == 12
    assert await fp_head() == 5
    assert await fp_member() == 5

    # Prune everything except qb (which keeps qa(1) as its dependency).
    engine.prune([("query", qb.id, ())])
    assert await qb() == 12
    assert runs == {"a": 2, "b": 1}

    # qa(2) was pruned so it recomputes.
    assert await qa(2) == 3
    assert runs["a"] == 3

    # Re-populate fixed point, record access_id, touch qa(1), then sweep.
    assert await fp_head() == 5
    assert await fp_member() == 5
    watermark = engine.access_id
    assert await qa(1) == 2  # touched after watermark
    engine.sweep_unaccessed(watermark)

    # qa(1) stayed cached; swept fixed-point member still converges cleanly.
    before_a = runs["a"]
    assert await qa(1) == 2
    assert runs["a"] == before_a
    base.set(2)
    assert await fp_member() == 5
    assert await fp_head() == 5


# ---------------------------------------------------------------------------
# 4. Async snapshots under concurrent writes & mixed sync<->async fan-out
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_snapshots_under_concurrent_input_and_collection_writes() -> None:
    engine = Engine()

    @engine.input
    def offset() -> int:
        return 0

    xs = CascadeList(engine, [1, 2, 3])
    d = CascadeDict(engine, {"k": 10})

    @engine.query
    async def summary() -> tuple[int, int, int]:
        await asyncio.sleep(0)
        return offset(), sum(v for v in xs), d["k"]

    # Prime initial state and capture pinned snapshots.
    snap0 = engine.snapshot()
    assert await summary(snapshot=snap0) == (0, 6, 10)

    stop = threading.Event()
    writer_errors: list[BaseException] = []

    def writer() -> None:
        try:
            for i in range(1, 80):
                offset.set(i)
                xs.append(i)
                d["k"] = 10 + i
        except BaseException as exc:  # noqa: BLE001
            writer_errors.append(exc)
        finally:
            stop.set()

    t = threading.Thread(target=writer, daemon=True)
    t.start()

    try:
        while not stop.is_set():
            # Old snapshot must remain completely stable despite concurrent writes.
            assert await summary(snapshot=snap0) == (0, 6, 10)
            snap = engine.snapshot()
            try:
                first = await summary(snapshot=snap)
                await asyncio.sleep(0)
                second = await summary(snapshot=snap)
                assert first == second
            finally:
                snap.release()
    finally:
        t.join(timeout=TIMEOUT)
        snap0.release()

    assert not writer_errors
    assert await summary() == (79, 6 + sum(range(1, 80)), 89)


@pytest.mark.asyncio
async def test_async_mixed_sync_async_fan_out() -> None:
    engine = Engine()
    runs = {"sync_leaf": 0, "async_leaf": 0, "root": 0}

    @engine.input
    def factor() -> int:
        return 2

    xs = CascadeList(engine, [1, 2, 3, 4])

    @engine.query
    def sync_leaf(i: int) -> int:
        runs["sync_leaf"] += 1
        return i * factor()

    @engine.query
    async def async_leaf(i: int) -> int:
        runs["async_leaf"] += 1
        await asyncio.sleep(0)
        # Call sync query inside async query (offloaded via executor/sync call).
        return sync_leaf(i) + 1

    @engine.query
    async def root() -> int:
        runs["root"] += 1
        vals = list(xs)
        results = await asyncio.gather(*(async_leaf(v) for v in vals))
        return sum(results)

    assert await root() == sum(v * 2 + 1 for v in [1, 2, 3, 4])
    assert runs == {"sync_leaf": 4, "async_leaf": 4, "root": 1}

    # Cache hit: no recomputations.
    assert await root() == sum(v * 2 + 1 for v in [1, 2, 3, 4])
    assert runs == {"sync_leaf": 4, "async_leaf": 4, "root": 1}

    # Mutate collection: only new item computes its leaves.
    xs.append(5)
    assert await root() == sum(v * 2 + 1 for v in [1, 2, 3, 4, 5])
    assert runs["sync_leaf"] == 5
    assert runs["async_leaf"] == 5
    assert runs["root"] == 2

    # Mutate factor: all leaves recompute.
    factor.set(3)
    assert await root() == sum(v * 3 + 1 for v in [1, 2, 3, 4, 5])
    assert runs["sync_leaf"] == 10
    assert runs["async_leaf"] == 10
    assert runs["root"] == 3


# ---------------------------------------------------------------------------
# 5. Pin _retry_after verification rule (sync + async)
# ---------------------------------------------------------------------------


def test_retry_after_verification_does_not_retry_joined_cycle_error_sync() -> None:
    """When verifying a dependency that joins a run ending in CycleError,
    verification must propagate CycleError rather than retrying the dependency
    (``_retry_after`` returns False when ``runtime.verifying`` is True).
    """
    engine = Engine()
    dep_entered = threading.Event()
    release_dep = threading.Event()
    dep_runs = 0
    dep_lock = threading.Lock()

    @engine.input
    def mode() -> str:
        return "ok"

    @engine.query
    def dep() -> int:
        nonlocal dep_runs
        with dep_lock:
            dep_runs += 1
            run_no = dep_runs
        if mode() == "cycle":
            if run_no == 2:
                dep_entered.set()
                assert release_dep.wait(timeout=TIMEOUT)
            return dep()
        return 10

    @engine.query
    def parent() -> int:
        try:
            return dep() + 1
        except CycleError:
            return -1

    # Rev 0: parent caches with dep() as its dependency.
    assert parent() == 11
    assert dep_runs == 1

    # Rev 1: dep() becomes a true cycle.
    mode.set("cycle")
    engine.clear_traces()

    def run_dep_directly() -> None:
        with pytest.raises(CycleError):
            dep()

    t_dep = threading.Thread(target=run_dep_directly, daemon=True)
    t_dep.start()
    assert dep_entered.wait(timeout=TIMEOUT)

    parent_result: list[int] = []
    parent_error: list[BaseException] = []

    def run_parent() -> None:
        try:
            parent_result.append(parent())
        except BaseException as exc:  # noqa: BLE001
            parent_error.append(exc)

    t_parent = threading.Thread(target=run_parent, daemon=True)
    t_parent.start()

    # Wait until parent's verification has joined t_dep's in-flight run of dep().
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        with engine._internals.lock:  # noqa: SLF001
            if len(engine._store.waiting_on) >= 1:  # noqa: SLF001
                break
        time.sleep(0.002)
    else:
        pytest.fail("parent verification did not join dep's in-flight run")

    release_dep.set()
    t_dep.join(timeout=TIMEOUT)
    t_parent.join(timeout=TIMEOUT)
    assert not t_dep.is_alive() and not t_parent.is_alive()
    assert not parent_error
    assert parent_result == [-1]

    # dep ran once at rev 0, once for t_dep at rev 1, and once when parent ran
    # for real at rev 1 (= 3 total). If verification had retried dep() on the
    # joined CycleError, dep_runs would be 4 and trace would contain dedup_retry.
    assert dep_runs == 3
    assert not any(ev.event == "dedup_retry" for ev in engine.traces())


@pytest.mark.asyncio
async def test_retry_after_verification_does_not_retry_joined_cycle_error_async() -> (
    None
):
    """Async twin of the ``_retry_after`` verification rule."""
    engine = Engine()
    dep_entered = asyncio.Event()
    release_dep = asyncio.Event()
    dep_runs = 0

    @engine.input
    def mode() -> str:
        return "ok"

    @engine.query
    async def dep() -> int:
        nonlocal dep_runs
        dep_runs += 1
        run_no = dep_runs
        if mode() == "cycle":
            if run_no == 2:
                dep_entered.set()
                await asyncio.wait_for(release_dep.wait(), timeout=TIMEOUT)
            return await dep()
        return 10

    @engine.query
    async def parent() -> int:
        try:
            return (await dep()) + 1
        except CycleError:
            return -1

    assert await parent() == 11
    assert dep_runs == 1

    mode.set("cycle")
    engine.clear_traces()

    async def run_dep_directly() -> None:
        with pytest.raises(CycleError):
            await dep()

    t_dep = asyncio.create_task(run_dep_directly())
    await asyncio.wait_for(dep_entered.wait(), timeout=TIMEOUT)
    t_parent = asyncio.create_task(parent())

    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        with engine._internals.lock:  # noqa: SLF001
            if len(engine._store.waiting_on) >= 1:  # noqa: SLF001
                break
        await asyncio.sleep(0.002)
    else:
        pytest.fail("async parent verification did not join dep's in-flight run")

    release_dep.set()
    await asyncio.wait_for(t_dep, timeout=TIMEOUT)
    assert await asyncio.wait_for(t_parent, timeout=TIMEOUT) == -1

    assert dep_runs == 3
    assert not any(ev.event == "dedup_retry" for ev in engine.traces())


# ---------------------------------------------------------------------------
# 6. Targeting condition/operator survivors in compute_or_get_memo_async
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_async_provisional_hit_within_fixed_point_iteration() -> None:
    """Calling a cycle member twice in one async fixed-point iteration hits the
    provisional overlay on the second call instead of recomputing it."""
    engine = Engine()
    member_runs = 0

    @engine.query(fixed_point=0)
    async def head() -> int:
        await asyncio.sleep(0)
        a = await member()
        b = await member()
        assert a == b
        return min(a + 1, 3)

    @engine.query
    async def member() -> int:
        nonlocal member_runs
        member_runs += 1
        await asyncio.sleep(0)
        return await head()

    assert await head() == 3
    # 4 iterations (guess=0,1,2,3 -> converges on 3); member must run once per
    # iteration (4 times), NOT twice per iteration (8 times).
    assert member_runs == 4


@pytest.mark.asyncio
async def test_async_waiter_retries_after_joined_submitted_run_cancelled() -> None:
    """A non-cancellable async task that joins a submitted run which is then
    cancelled must retry via ``_joined_run_failed`` and succeed."""
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
    runs = 0
    runs_lock = threading.Lock()

    @engine.input
    def x() -> int:
        return 1

    @engine.query
    async def slow() -> int:
        nonlocal runs
        with runs_lock:
            runs += 1
            run_no = runs
        if run_no == 1:
            started.set()
            while not release.is_set():
                await asyncio.sleep(0.005)
        return x() * 10

    # Start a cancellable submitted run at rev 0.
    snap0 = engine.snapshot()
    try:
        fut = engine.submit(slow)
        assert await asyncio.to_thread(started.wait, TIMEOUT)

        # Async waiter on our running loop joins the in-flight run at rev 0.
        waiter = asyncio.create_task(slow(snapshot=snap0))
        deadline = time.monotonic() + TIMEOUT
        while time.monotonic() < deadline:
            with engine._internals.lock:  # noqa: SLF001
                if len(engine._store.waiting_on) >= 1:  # noqa: SLF001
                    break
            await asyncio.sleep(0.002)
        else:
            pytest.fail("waiter did not join submitted run")

        # Mutate input to cancel the submitted run, then let it finish its await.
        x.set(2)
        release.set()

        with pytest.raises(QueryCancelled):
            await asyncio.to_thread(fut.result, TIMEOUT)

        # Waiter at snap0 must retry and return rev 0's value (10), not QueryCancelled.
        assert await asyncio.wait_for(waiter, timeout=TIMEOUT) == 10
        assert any(ev.event == "dedup_retry" for ev in engine.traces())
    finally:
        snap0.release()


@pytest.mark.asyncio
async def test_async_reentry_refused_during_verification() -> None:
    """When async verification of a dependency reaches a query running on the
    outer stack, ``_refuse_reentry`` abandons verification and recomputes cleanly."""
    engine = Engine()

    @engine.input
    def flip() -> bool:
        return False

    @engine.query(fixed_point=0)
    async def a() -> int:
        await asyncio.sleep(0)
        if not flip():
            return (await b()) + 1
        return min((await b()) + 1, 3)

    @engine.query
    async def b() -> int:
        await asyncio.sleep(0)
        if not flip():
            return 10
        return await a()

    assert await a() == 11
    flip.set(True)
    # Verifying a's old dep b reaches a (which is now in a's verification),
    # forcing a real recompute that converges to 3.
    assert await a() == 3


@pytest.mark.asyncio
async def test_async_enter_through_head_propagates_through_intermediate() -> None:
    """When a dropped cycle member calls an intermediate helper which reaches
    the head and back to the member, ``_EnterThroughHead`` must propagate
    through the helper (``exc.member != key``) and redirect at the member."""
    engine = Engine()

    @engine.input
    def step() -> int:
        return 1

    @engine.query(fixed_point=0)
    async def head() -> int:
        await asyncio.sleep(0)
        return min((await member()) + step(), 5)

    @engine.query
    async def helper() -> int:
        await asyncio.sleep(0)
        return await head()

    @engine.query
    async def member() -> int:
        await asyncio.sleep(0)
        return await helper()

    assert await head() == 5
    assert await member() == 5

    # Drop member and helper, bump input, then enter through member.
    engine.prune([("query", head.id, ())])
    step.set(2)
    assert await member() == 5
    assert await head() == 5


@pytest.mark.asyncio
async def test_async_input_explicit_snapshot() -> None:
    engine = Engine()

    @engine.input
    async def cfg() -> int:
        await asyncio.sleep(0)
        return 1

    snap0 = engine.snapshot()
    try:
        assert await cfg(snapshot=snap0) == 1
        cfg.set(2)
        assert await cfg(snapshot=snap0) == 1
        assert await cfg() == 2
    finally:
        snap0.release()


@pytest.mark.asyncio
async def test_async_disk_hydration_large_blob_and_cached_error(tmp_path: Path) -> None:
    payload = "z" * 16_384

    def register(eng: Engine, runs: dict[str, int]) -> tuple[Any, Any]:
        @eng.query
        async def big() -> str:
            runs["big"] += 1
            await asyncio.sleep(0)
            return payload

        @eng.query(cache_exceptions=True)
        async def boom() -> int:
            runs["boom"] += 1
            await asyncio.sleep(0)
            raise ValueError("persisted boom")

        return big, boom

    engine = Engine(cache_dir=tmp_path)
    try:
        runs1 = {"big": 0, "boom": 0}
        big1, boom1 = register(engine, runs1)
        assert await big1() == payload
        with pytest.raises(ValueError, match="persisted boom"):
            await boom1()
        assert runs1 == {"big": 1, "boom": 1}
    finally:
        engine.shutdown()

    engine2 = Engine(cache_dir=tmp_path)
    try:
        runs2 = {"big": 0, "boom": 0}
        big2, boom2 = register(engine2, runs2)
        assert await big2() == payload
        with pytest.raises(ValueError, match="persisted boom"):
            await boom2()
        assert runs2 == {"big": 0, "boom": 0}
    finally:
        engine2.shutdown()


@pytest.mark.asyncio
async def test_async_fixed_point_unencodable_result() -> None:
    from cascade import UnencodableValueError

    engine = Engine()

    class Unencodable:
        pass

    @engine.query(fixed_point=0)
    async def bad_fp() -> Any:
        await asyncio.sleep(0)
        _ = await bad_fp()
        return Unencodable()

    with pytest.raises(UnencodableValueError):
        await bad_fp()
