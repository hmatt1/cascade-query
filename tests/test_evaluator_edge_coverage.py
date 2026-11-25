"""Targeted coverage for remaining edge paths in ``cascade._evaluator``."""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

import pytest

from cascade import (
    CascadeList,
    CycleError,
    Engine,
    QueryCancelled,
    UnencodableValueError,
)
from cascade._state import InFlight


def test_disk_writer_caps_batch_at_1000_and_skips_mutated_value(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as engine:
        writer = engine._evaluator._writer  # noqa: SLF001
        assert writer is not None
        real_write_batch = writer.write_batch
        release = threading.Event()
        entered = threading.Event()
        batch_sizes: list[int] = []

        def gated_write_batch(batch: list[Any]) -> None:
            entered.set()
            release.wait(timeout=5.0)
            batch_sizes.append(len(batch))
            real_write_batch(batch)

        monkeypatch.setattr(writer, "write_batch", gated_write_batch)

        @engine.query
        def make_list(i: int) -> list[int]:
            return [i]

        # First call wakes the writer thread and parks it on ``release``.
        first = make_list(0)
        assert entered.wait(timeout=5.0)
        # Mutate the returned list before the writer encodes it.
        first.append(999)

        # Queue 1,001 more items so the next batch hits the 1,000-item cap.
        for i in range(1, 1002):
            make_list(i)

        release.set()
        engine.flush_disk()

        assert any(size == 1000 for size in batch_sizes)
        traces = engine.traces()
        assert any(
            ev.event == "disk_skip" and ev.detail == "value mutated before persist"
            for ev in traces
        )


def test_run_sync_in_executor_with_keyword_arguments() -> None:
    engine = Engine()

    @engine.input
    def base() -> int:
        return 1

    @engine.query
    def sync_kw(*, factor: int = 1) -> int:
        return base() * factor * 10

    @engine.query
    async def caller() -> int:
        return sync_kw(factor=3)

    async def main() -> tuple[int, int]:
        first = await caller()
        # Changing ``base`` forces ``caller``'s async verification to recompute
        # the sync query ``sync_kw`` off the event loop via ``_run_sync_in_executor``.
        base.set(2)
        second = await caller()
        return first, second

    assert asyncio.run(main()) == (30, 60)


def test_keep_default_race_returns_version_stored_by_peer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 42

    store = engine._store  # noqa: SLF001
    real_store_default = store.store_default

    def peer_won(
        input_key: tuple[str, tuple[Any, ...]], default: Any, revision: int
    ) -> Any:
        # Simulate another thread storing the default at ``revision`` first.
        real_store_default(input_key, default, revision)
        return None

    monkeypatch.setattr(store, "store_default", peer_won)
    assert x() == 42


def test_enter_wait_locked_handles_unrelated_wait_cycle() -> None:
    engine = Engine()
    store = engine._store  # noqa: SLF001
    evaluator = engine._evaluator  # noqa: SLF001

    slot_a = InFlight("agent_a", 1)
    slot_b = InFlight("agent_b", 2)
    with store.lock:
        store.waiting_on["agent_a"] = slot_b
        store.waiting_on["agent_b"] = slot_a
        try:
            ok = evaluator._enter_wait_locked(  # noqa: SLF001
                ("query", "q", ()),
                slot_a,
                "agent_c",
                3,
                blocking=True,
            )
            assert ok is True
        finally:
            store.waiting_on.clear()


def test_provisional_dependency_breaks_settle_and_owns() -> None:
    """A cached query depending on a provisional cycle member is re-run, not settled."""
    engine = Engine()

    @engine.input
    def base() -> int:
        return 1

    @engine.query
    def member() -> int:
        return head()

    @engine.query
    def helper() -> int:
        return member() + 100

    @engine.query
    def mid_helper() -> int:
        return member() + 10

    @engine.query
    def outer_helper() -> int:
        return mid_helper() + 1

    @engine.query(fixed_point=0)
    def head() -> int:
        b = base()
        m = member()
        if b > 1:
            # ``helper`` and ``outer_helper`` are cached outside ``head.cycle_nodes``
            # and depend on ``member`` (now in ``runtime.provisional``).
            _ = helper()
            _ = outer_helper()
        return b if m == b else min(b, m + 1)

    assert head() == 1
    # Populate ``helper``, ``mid_helper`` and ``outer_helper`` outside ``head``'s
    # cycle so they stay in ``store.memos`` when ``head`` recomputes.
    assert helper() == 101
    assert outer_helper() == 12

    base.set(2)
    assert head() == 2
    assert helper() == 102
    assert outer_helper() == 13


def test_stored_on_disk_absent_and_io_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as engine:

        @engine.query(fixed_point=0)
        def head() -> int:
            return member()

        @engine.query
        def member() -> int:
            return head()

        # Cold call through member with nothing on disk hits ``record is None``.
        with pytest.raises(CycleError):
            member()

        # Failing disk read during ``_stored_on_disk`` (the lookup for ``head``
        # after ``member`` and ``head`` both missed hydration) falls back to CycleError.
        disk = engine._disk  # noqa: SLF001
        real_load = disk.load_entry
        calls = [0]

        def fail_on_stored_check(entry_key: bytes) -> Any:
            calls[0] += 1
            if calls[0] >= 3:
                raise OSError("io boom")
            return real_load(entry_key)

        monkeypatch.setattr(disk, "load_entry", fail_on_stored_check)
        with pytest.raises(CycleError):
            member()


def test_enter_cycle_through_head_edge_outcomes() -> None:
    engine = Engine()
    fail_calls = [0]

    @engine.input
    def mode() -> str:
        return "ok"

    @engine.query(fixed_point=0, max_iterations=2, cache_exceptions=False)
    def head() -> int:
        m = mode()
        if m == "diverge":
            return member() + 1
        if m == "fail":
            fail_calls[0] += 1
            # Call 1 comes from ``member`` and must reach ``member`` again so
            # ``_EnterThroughHead`` triggers ``_enter_cycle_through_head``;
            # call 2 runs inside ``_enter_cycle_through_head`` and raises.
            if fail_calls[0] >= 2:
                raise RuntimeError("head boom")
            return member()
        return member()

    @engine.query
    def member() -> int:
        return head()

    # Warm up so ``member`` knows ``head`` is its cycle head.
    assert head() == 0
    assert member() == 0

    # 1. Head fails to converge (CycleError) when member is dropped.
    engine.prune([], vacuum_disk=False)
    mode.set("diverge")
    with pytest.raises(CycleError):
        member()

    # 2. Head raises an uncached Exception inside ``_enter_cycle_through_head``.
    engine.prune([], vacuum_disk=False)
    mode.set("fail")
    with pytest.raises(RuntimeError, match="head boom"):
        member()

    # 3. Direct check for ``_owns(runtime, head)`` returning False.
    from cascade._runtime import RuntimeFrame, RuntimeState

    head_key = ("query", head.id, ())
    rt = RuntimeState(
        snapshot=engine.snapshot(),
        stack=[RuntimeFrame(key=head_key, deps={}, effects={})],
        root_effects=None,
        staged_root_effects={},
        cancel_epoch=None,
        snapshot_explicit=False,
    )
    assert engine._evaluator._enter_cycle_through_head(head_key, rt) is False  # noqa: SLF001


def test_enter_cycle_through_head_propagates_cancellation() -> None:
    engine = Engine()
    started = threading.Event()
    proceed = threading.Event()
    calls_at_2 = [0]

    @engine.input
    def x() -> int:
        return 1

    @engine.query(fixed_point=0)
    def head() -> int:
        val = x()
        if val == 2:
            calls_at_2[0] += 1
            # Call 1 reaches ``member`` again to trigger ``_enter_cycle_through_head``;
            # call 2 blocks inside ``_enter_cycle_through_head`` until cancelled.
            if calls_at_2[0] == 1:
                return member()
            started.set()
            proceed.wait(timeout=5.0)
            return member()
        return member() if val == 1 else val

    @engine.query
    def member() -> int:
        return head()

    assert head() == 0
    assert member() == 0
    engine.prune([], vacuum_disk=False)
    x.set(2)

    fut = engine.submit(member)
    assert started.wait(timeout=5.0)
    x.set(3)
    proceed.set()
    with pytest.raises(QueryCancelled):
        fut.result(timeout=5.0)
    engine.shutdown()


def test_retract_counts_skips_untouched_accumulator() -> None:
    engine = Engine()
    before_acc = engine.accumulator("before")
    pipe_acc = engine.accumulator("pipe")
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def q() -> list[int]:
        before_acc.push("keep")

        def step(v: int) -> int:
            pipe_acc.push(v)
            return v * 2

        return [step(v) for v in xs]

    effects: dict[str, list[Any]] = {}
    assert q(effects=effects) == [2, 4]
    assert effects == {"before": ["keep"], "pipe": [1, 2]}


def test_shadow_outer_with_non_empty_async_stack() -> None:
    engine = Engine()

    @engine.input
    def x() -> int:
        return 1

    @engine.input
    def y() -> int:
        return 10

    @engine.query
    async def leaf() -> int:
        return y() * 2

    @engine.query
    async def mid() -> int:
        return await leaf() + 1

    @engine.query
    async def top() -> int:
        # ``top`` reads ``x`` directly so changing ``x`` forces ``top`` onto
        # ``runtime.stack`` before it awaits ``mid``, which then verifies
        # ``leaf`` via ``_dependency_entry_shadowed_async`` -> ``_shadow_outer``.
        return x() + await mid()

    async def run() -> tuple[int, int]:
        first = await top()
        # Changing both ``x`` and ``y`` forces ``top`` to recompute (putting
        # ``top`` on ``runtime.stack``) AND makes ``mid`` dirty so ``await mid()``
        # enters ``try_mark_green_async`` -> ``_dependency_entry_shadowed_async``
        # -> ``_shadow_outer(runtime)`` with a non-empty stack.
        with engine.transaction():
            x.set(2)
            y.set(11)
        second = await top()
        return first, second

    assert asyncio.run(run()) == (22, 25)


def test_await_cycle_heads_cancellation_and_error() -> None:
    engine = Engine()

    # 1. Head raises an uncached error while another thread awaits it.
    entered_member = threading.Event()
    release_member = threading.Event()
    fail_now = [False]

    @engine.query(fixed_point=0, cache_exceptions=False)
    def head_err() -> int:
        if fail_now[0]:
            raise ValueError("head failed")
        val = member_err()
        fail_now[0] = True
        return val + 1

    @engine.query
    def member_err() -> int:
        entered_member.set()
        release_member.wait(timeout=5.0)
        return head_err()

    t1_exc: list[BaseException] = []

    def run_head() -> None:
        try:
            head_err()
        except BaseException as exc:  # noqa: BLE001
            t1_exc.append(exc)

    t1 = threading.Thread(target=run_head)
    t1.start()
    assert entered_member.wait(timeout=5.0)

    # Thread 2 joins ``member_err`` (which returns a provisional result),
    # then calls ``_await_cycle_heads({head_err})`` which catches ValueError.
    t2_exc: list[BaseException] = []

    def run_member() -> None:
        try:
            member_err()
        except BaseException as exc:  # noqa: BLE001
            t2_exc.append(exc)

    t2 = threading.Thread(target=run_member)
    t2.start()
    # Give t2 a moment to join member_err's in-flight slot before releasing.
    threading.Event().wait(0.05)
    release_member.set()
    t1.join(timeout=5.0)
    t2.join(timeout=5.0)
    assert t1_exc and isinstance(t1_exc[0], ValueError)
    assert t2_exc

    # 2. Control-flow error (QueryCancelled) propagates out of ``_await_cycle_heads``.
    @engine.input
    def tick() -> int:
        return 1

    @engine.query
    def slow_head() -> int:
        tick.set(2)
        return tick()

    from cascade._runtime import RuntimeState

    rt = RuntimeState(
        snapshot=engine.snapshot(),
        stack=[],
        root_effects=None,
        staged_root_effects={},
        cancel_epoch=engine._store.cancel_epoch,  # noqa: SLF001
        snapshot_explicit=False,
    )
    with pytest.raises(QueryCancelled):
        engine._evaluator._await_cycle_heads(  # noqa: SLF001
            frozenset({("query", slow_head.id, ())}), rt
        )


def test_inherit_failed_deps_inside_fixed_point_cycle() -> None:
    """Uncached errors caught inside a fixed-point cycle inherit cycle_heads."""
    engine = Engine()

    @engine.query(cache_exceptions=False)
    def failing_a() -> int:
        _ = head()
        raise ValueError("a")

    @engine.query(cache_exceptions=False)
    def failing_b() -> int:
        _ = head()
        raise ValueError("b")

    @engine.query
    def member() -> int:
        # First caught error sets ``caller.cycle_heads`` from None;
        # second caught error updates the existing ``caller.cycle_heads``.
        try:
            failing_a()
        except ValueError:
            pass
        try:
            failing_b()
        except ValueError:
            pass
        return head()

    @engine.query(fixed_point=7)
    def head() -> int:
        return member()

    assert head() == 7


def test_fail_unencodable_discards_provisional_cycle_entries(tmp_path: Path) -> None:
    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as engine:
        calls = [0]

        @engine.query
        def member() -> int:
            return head()

        @engine.query(fixed_point=0)
        def head() -> Any:
            calls[0] += 1
            if calls[0] == 1:
                # Iteration 1 recurses and sets ``frame.cycle_guess = 1``.
                _ = member()
                return 1
            # Iteration 2 does not recurse (``is_cycle_root`` is False) and
            # returns an unencodable value, reaching ``_finish_recompute`` ->
            # ``_fail_unencodable`` with ``frame.cycle_guess is not UNSET``.
            return object()

        with pytest.raises(UnencodableValueError):
            head()


def test_fixed_point_head_stops_recursing_shares_input_and_inherits_member_ttl() -> (
    None
):
    engine = Engine()
    calls = [0]

    @engine.input
    def shared() -> int:
        return 5

    @engine.query(ttl=60.0)
    def timed() -> int:
        return shared()

    @engine.query
    def member() -> int:
        # Reads both ``shared`` (also read by ``head``) and ``timed`` (TTL).
        return head() + shared() - shared() + timed() - timed()

    @engine.query(fixed_point=0)
    def head() -> int:
        s = shared()
        calls[0] += 1
        if calls[0] == 1:
            # Iteration 1 reads ``member`` -> recursive guess -> returns s.
            _ = member()
            return s
        # Iteration 2 does NOT call ``member`` -> ``frame.cycle_heads`` is None.
        return s

    assert head() == 5

    # Also test a normal converging cycle where member reads ``shared`` and ``timed``.
    @engine.query
    def m2() -> int:
        return shared() + timed() + (h2() * 0)

    @engine.query(fixed_point=10)
    def h2() -> int:
        _ = shared()
        return m2()

    assert h2() == 10
    memo = engine._store.memos[("query", h2.id, ())]  # noqa: SLF001
    assert memo.expires_at is not None


def test_disk_hydration_cycle_missing_nodes_and_async_input_default(
    tmp_path: Path,
) -> None:
    # Shared helpers keep bytecode identical across engines so disk hydration
    # actually runs ``_verify_disk_deps`` instead of short-circuiting on fn_hash.
    def make_root_cycle(eng: Engine, dep: Any) -> Any:
        @eng.query(name="root_cycle")
        def root_cycle() -> int:
            try:
                return dep() + 1
            except CycleError:
                return -1

        return root_cycle

    def make_root_missing_in(eng: Engine, inp: Any) -> Any:
        @eng.query(name="root_missing_in")
        def root_missing_in() -> int:
            return inp() + 1 if inp is not None else 123

        return root_missing_in

    def make_root_missing_q(eng: Engine, dep: Any) -> Any:
        @eng.query(name="root_missing_q")
        def root_missing_q() -> int:
            return dep() + 2 if dep is not None else 456

        return root_missing_q

    # 1. Hydrating a query whose dependency now raises CycleError, or is unregistered.
    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as e1:

        @e1.input(name="missing_in")
        def missing_in() -> int:
            return 10

        @e1.query(name="dep_q")
        def dep_q() -> int:
            return 20

        rc1 = make_root_cycle(e1, dep_q)
        rmi1 = make_root_missing_in(e1, missing_in)
        rmq1 = make_root_missing_q(e1, dep_q)

        assert rc1() == 21
        assert rmi1() == 11
        assert rmq1() == 22

    # Engine 2: ``dep_q`` is a cycle; ``missing_in`` is not registered.
    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as e2:

        @e2.query(name="dep_q")
        def dep_q2() -> int:
            return dep_q2()

        rc2 = make_root_cycle(e2, dep_q2)
        rmi2 = make_root_missing_in(e2, None)

        assert rc2() == -1
        assert rmi2() == 123

    # Engine 3: ``dep_q`` is not registered at all -> ``_current_query_state`` hits KeyError.
    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as e3:
        rmq3 = make_root_missing_q(e3, None)
        assert rmq3() == 456

    # 2. Hydrating a sync query that depends on an async input default.
    def make_uses_async_in(eng: Engine, inp: Any, runs: list[int]) -> Any:
        @eng.query(name="uses_async_in")
        def uses_async_in() -> int:
            runs[0] += 1
            return inp() + 1

        return uses_async_in

    async_dir = tmp_path / "async_default"
    with Engine(cache_dir=async_dir, cache_backend="sqlite") as e4:
        runs4 = [0]

        @e4.input(name="async_in")
        async def async_in4() -> int:
            return 77

        q4 = make_uses_async_in(e4, async_in4, runs4)
        assert q4() == 78
        assert runs4[0] == 1

    with Engine(cache_dir=async_dir, cache_backend="sqlite") as e5:
        runs5 = [0]

        @e5.input(name="async_in")
        async def async_in5() -> int:
            return 77

        q5 = make_uses_async_in(e5, async_in5, runs5)
        assert q5() == 78
        assert runs5[0] == 0


def test_verify_disk_deps_propagates_cancellation(tmp_path: Path) -> None:
    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as e1:

        @e1.input(name="x")
        def x1() -> int:
            return 1

        @e1.query(name="inner")
        def inner1() -> int:
            return x1() * 2

        @e1.query(name="outer")
        def outer1() -> int:
            return inner1() + 1

        assert outer1() == 3

    with Engine(cache_dir=tmp_path, cache_backend="sqlite") as e2:
        started = threading.Event()
        release = threading.Event()

        @e2.input(name="x")
        def x2() -> int:
            return 1

        @e2.query(name="inner")
        def inner2() -> int:
            started.set()
            release.wait(timeout=5.0)
            return x2() * 2

        @e2.query(name="outer")
        def outer2() -> int:
            return inner2() + 1

        # Change x2 so inner2 must recompute during outer2's disk verification.
        x2.set(2)
        fut = e2.submit(outer2)
        assert started.wait(timeout=5.0)
        x2.set(3)
        release.set()
        with pytest.raises(QueryCancelled):
            fut.result(timeout=5.0)


def test_in_memory_disk_methods_shutdown_no_wait_and_diamond_vacuum(
    tmp_path: Path,
) -> None:
    # In-memory engine: flush_disk, flush_disk_at_exit, prune_disk_cache are no-ops.
    mem = Engine()
    mem.flush_disk()
    mem._evaluator.flush_disk_at_exit()  # noqa: SLF001
    mem._evaluator.prune_disk_cache([])  # noqa: SLF001
    mem.prune([], vacuum_disk=True)

    # Disk-backed engine: diamond query graph vacuum + shutdown(wait=False).
    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
    try:

        @engine.input
        def seed() -> int:
            return 1

        @engine.query
        def leaf() -> int:
            return seed() + 1

        @engine.query
        def left() -> int:
            return leaf() * 2

        @engine.query
        def right() -> int:
            return leaf() * 3

        @engine.query
        def top() -> int:
            return left() + right()

        @engine.query
        def orphan() -> int:
            return seed() + 99

        assert top() == 10
        assert orphan() == 100
        engine.flush_disk()

        # Diamond dependency (top -> left/right -> leaf) exercises both
        # ``dep_ekey not in wanted_entries`` and already-visited branches.
        engine.prune([("query", top.id, ())], vacuum_disk=True)
    finally:
        thread = engine._evaluator._disk_thread  # noqa: SLF001
        engine.shutdown(wait=False)
        if thread is not None:
            thread.join(timeout=5.0)
        if engine._disk is not None:  # noqa: SLF001
            engine._disk.close()  # noqa: SLF001
