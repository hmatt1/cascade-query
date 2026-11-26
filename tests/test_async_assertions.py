"""The async evaluator asserted as closely as the sync one.

An ``async def`` query called from plain code takes the sync path; only a
call awaited on a running event loop reaches ``compute_or_get_memo_async``
and the other ``*_async`` functions. The twins here write each query body
once, as a coroutine, and run it either way:

* ``sync``: registered as a plain function that drives the coroutine to its
  end without a loop (the body must not suspend), and called from plain
  code;
* ``async``: registered as the coroutine function and awaited on a loop
  that lives for the whole test.

Both modes then meet the same assertions: run counts, values, trace events
with their keys, the fields of the stored memo, effects, dependency edges.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import threading
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from cascade import CycleError, Engine, QueryCancelled, UnencodableValueError

TIMEOUT = 10.0


async def get(value: Any) -> Any:
    """What a query handle returned: awaited in async mode, as is in sync mode."""
    if inspect.isawaitable(value):
        return await value
    return value


def _run_to_end(body: Callable[..., Any]) -> Callable[..., Any]:
    """A plain function that runs ``body``'s coroutine without a loop."""

    @functools.wraps(body)
    def run(*args: Any) -> Any:
        coro = body(*args)
        try:
            coro.send(None)
        except StopIteration as stop:
            return stop.value
        coro.close()
        raise AssertionError(f"{body.__name__} suspended in sync mode")

    return run


class Mode:
    """Registers and calls queries the sync way or the async way."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.is_async = name == "async"
        self._runner = asyncio.Runner() if self.is_async else None

    def query(self, engine: Engine, **options: Any) -> Callable[..., Any]:
        def register(body: Callable[..., Any]) -> Any:
            fn = body if self.is_async else _run_to_end(body)
            return engine.query(name=body.__name__, **options)(fn)

        return register

    def call(self, handle: Any, *args: Any, **kwargs: Any) -> Any:
        if self._runner is None:
            return handle(*args, **kwargs)

        async def awaited() -> Any:
            return await handle(*args, **kwargs)

        return self._runner.run(asyncio.wait_for(awaited(), TIMEOUT))

    async def pause(self) -> None:
        """Let other tasks run (async mode only; a sync body cannot suspend)."""
        if self.is_async:
            await asyncio.sleep(0)

    def close(self) -> None:
        if self._runner is not None:
            self._runner.close()


@pytest.fixture(params=["sync", "async"])
def mode(request: pytest.FixtureRequest) -> Iterator[Mode]:
    m = Mode(request.param)
    try:
        yield m
    finally:
        m.close()


def events(engine: Engine, *kinds: str) -> list[tuple[str, str]]:
    """(event, key) of the traces, only the kinds named if any are."""
    return [(e.event, e.key) for e in engine.traces() if not kinds or e.event in kinds]


def memo_of(engine: Engine, handle: Any, *args: Any) -> Any:
    return engine._internals.memos[("query", handle.id, args)]  # noqa: SLF001


def assert_settled(engine: Engine) -> None:
    """No run is left registered as in flight, and no caller as waiting."""
    internals = engine._internals  # noqa: SLF001
    with internals.lock:
        assert internals.in_flight == {}
        assert engine._store.waiting_on == {}  # noqa: SLF001


# ---------------------------------------------------------------------------
# Hits and verification: traces, memo fields, effects
# ---------------------------------------------------------------------------


def _chain(engine: Engine, mode: Mode, runs: dict[str, int]) -> tuple[Any, ...]:
    """``top`` reads ``flag``, which reads input ``a``; ``other`` is unrelated."""
    log = engine.accumulator("log")

    @engine.input
    def a() -> int:
        return 1

    @engine.input
    def other() -> int:
        return 0

    @mode.query(engine)
    async def flag() -> bool:
        runs["flag"] += 1
        log.push("flag")
        return a() > 0

    @mode.query(engine)
    async def top() -> str:
        runs["top"] += 1
        await mode.pause()
        log.push("top")
        return "yes" if await get(flag()) else "no"

    @mode.query(engine)
    async def side() -> int:
        return other()

    return a, other, flag, top, side


def test_hit_is_traced_and_stamps_the_memo(mode: Mode) -> None:
    engine = Engine()
    runs = {"flag": 0, "top": 0}
    _a, other, _flag, top, side = _chain(engine, mode, runs)

    assert mode.call(top) == "yes"
    engine.clear_traces()
    memo = memo_of(engine, top)
    first_access = memo.last_access

    effects: dict[str, list[Any]] = {}
    assert mode.call(top, effects=effects) == "yes"
    assert runs == {"flag": 1, "top": 1}
    # A hit replays the effects of the stored run, once.
    assert effects == {"log": ["top", "flag"]}
    assert events(engine) == [("cache_hit", "query:top()")]
    assert memo.last_access > first_access

    # A hit at a newer revision (nothing top read changed) moves verified_at.
    assert mode.call(side) == 0
    other.set(1)
    revision = engine.revision
    assert memo.verified_at < revision
    engine.clear_traces()
    assert mode.call(top) == "yes"
    assert memo_of(engine, top) is memo
    assert memo.verified_at == revision
    assert events(engine, "cache_hit", "cache_green") == [("cache_hit", "query:top()")]
    assert_settled(engine)


def test_verified_result_is_traced_stamped_and_replays_its_effects(mode: Mode) -> None:
    engine = Engine()
    runs = {"flag": 0, "top": 0}
    a, _other, _flag, top, _side = _chain(engine, mode, runs)

    assert mode.call(top) == "yes"
    memo = memo_of(engine, top)
    a.set(2)  # flag runs again and returns True again: top is unchanged
    revision = engine.revision
    engine.clear_traces()

    effects: dict[str, list[Any]] = {}
    assert mode.call(top, effects=effects) == "yes"
    assert runs == {"flag": 2, "top": 1}
    assert effects == {"log": ["top", "flag"]}
    assert events(engine, "cache_hit", "cache_green", "recompute_start") == [
        ("recompute_start", "query:flag()"),
        ("cache_green", "query:top()"),
    ]
    assert memo_of(engine, top) is memo
    assert memo.verified_at == revision
    assert memo.head_clean

    # Verified at this revision: the next call is a plain hit, no walk.
    engine.clear_traces()
    effects = {}
    assert mode.call(top, effects=effects) == "yes"
    assert effects == {"log": ["top", "flag"]}
    assert events(engine) == [("cache_hit", "query:top()")]
    assert_settled(engine)


def test_verified_result_at_an_older_snapshot_is_stamped_for_it(mode: Mode) -> None:
    engine = Engine()
    runs = {"flag": 0, "top": 0}
    a, _other, _flag, top, _side = _chain(engine, mode, runs)

    assert mode.call(top) == "yes"
    a.set(2)
    old = engine.snapshot()
    try:
        a.set(3)
        # At the pinned snapshot the memo is not clean: verified by a walk.
        assert mode.call(top, snapshot=old) == "yes"
        memo = memo_of(engine, top)
        assert memo.verified_at == old.revision
        engine.clear_traces()
        # Verified at that revision now: a hit, with no second walk.
        assert mode.call(top, snapshot=old) == "yes"
        assert events(engine) == [("cache_hit", "query:top()")]
        assert runs["top"] == 1
        assert_settled(engine)
    finally:
        old.release()


# ---------------------------------------------------------------------------
# memoize=False: run on every call, but verified like any query beneath others
# ---------------------------------------------------------------------------


def test_unmemoized_query_runs_per_call_and_is_only_verified_beneath_others(
    mode: Mode,
) -> None:
    engine = Engine()
    runs = {"flag": 0, "volatile": 0, "top": 0}

    @engine.input
    def a() -> int:
        return 1

    @mode.query(engine)
    async def flag() -> bool:
        runs["flag"] += 1
        return a() > 0

    @mode.query(engine, memoize=False)
    async def volatile() -> str:
        runs["volatile"] += 1
        await mode.pause()
        return "yes" if await get(flag()) else "no"

    @mode.query(engine)
    async def top() -> str:
        runs["top"] += 1
        return await get(volatile()) + "!"

    assert mode.call(top) == "yes!"
    assert runs == {"flag": 1, "volatile": 1, "top": 1}

    # Called directly, it runs every time, even with a current row.
    engine.clear_traces()
    assert mode.call(volatile) == "yes"
    assert mode.call(volatile) == "yes"
    assert runs == {"flag": 1, "volatile": 3, "top": 1}
    # A current row is found, and its dependencies walked, but the body runs.
    assert events(engine, "cache_hit", "cache_green", "recompute_start") == 2 * [
        ("cache_hit", "query:volatile()"),
        ("cache_green", "query:volatile()"),
        ("recompute_start", "query:volatile()"),
        ("cache_hit", "query:flag()"),
    ]

    # Beneath top it is only verified: a current row is enough...
    engine.clear_traces()
    assert mode.call(top) == "yes!"
    assert runs == {"flag": 1, "volatile": 3, "top": 1}
    assert events(engine) == [("cache_hit", "query:top()")]

    # ...and so is a row whose dependencies turn out unchanged.
    a.set(2)
    engine.clear_traces()
    assert mode.call(top) == "yes!"
    assert runs == {"flag": 2, "volatile": 3, "top": 1}
    assert events(engine, "cache_hit", "cache_green", "recompute_start") == [
        ("recompute_start", "query:flag()"),
        ("cache_green", "query:volatile()"),
        ("cache_green", "query:top()"),
    ]

    # A changed dependency runs it while top is verified, and again when
    # top runs and calls it.
    a.set(-1)
    assert mode.call(top) == "no!"
    assert runs == {"flag": 3, "volatile": 5, "top": 2}
    assert_settled(engine)


# ---------------------------------------------------------------------------
# Errors that are not cached: nothing is left behind for the next caller
# ---------------------------------------------------------------------------


def test_uncached_error_runs_again_on_the_next_call(mode: Mode) -> None:
    engine = Engine()
    runs = {"boom": 0}

    @mode.query(engine, cache_exceptions=False)
    async def boom() -> int:
        runs["boom"] += 1
        await mode.pause()
        raise ValueError(f"boom {runs['boom']}")

    with pytest.raises(ValueError, match="boom 1"):
        mode.call(boom)
    with pytest.raises(ValueError, match="boom 2"):
        mode.call(boom)
    assert runs == {"boom": 2}
    assert engine._internals.in_flight == {}  # noqa: SLF001


# ---------------------------------------------------------------------------
# Tasks that join a run on the same loop
# ---------------------------------------------------------------------------


async def _until(condition: Callable[[], bool]) -> None:
    deadline = asyncio.get_running_loop().time() + TIMEOUT
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("condition not reached")
        await asyncio.sleep(0.001)


def _waiting(engine: Engine) -> int:
    with engine._internals.lock:  # noqa: SLF001
        return len(engine._store.waiting_on)  # noqa: SLF001


@pytest.mark.asyncio
async def test_task_joining_a_run_gets_its_value_effects_and_trace() -> None:
    engine = Engine()
    log = engine.accumulator("log")
    release = asyncio.Event()
    runs = 0

    @engine.query(name="shared")
    async def shared() -> int:
        nonlocal runs
        runs += 1
        log.push("ran")
        await release.wait()
        return 42

    owner_effects: dict[str, list[Any]] = {}
    waiter_effects: dict[str, list[Any]] = {}
    owner = asyncio.create_task(shared(effects=owner_effects))
    await _until(lambda: runs == 1)
    waiter = asyncio.create_task(shared(effects=waiter_effects))
    await _until(lambda: _waiting(engine) == 1)
    release.set()
    assert await asyncio.wait_for(asyncio.gather(owner, waiter), TIMEOUT) == [42, 42]
    assert runs == 1
    assert owner_effects == waiter_effects == {"log": ["ran"]}
    assert events(engine, "dedup_wait", "recompute_start", "cache_hit") == [
        ("recompute_start", "query:shared()"),
        ("dedup_wait", "query:shared()"),
    ]
    assert _waiting(engine) == 0
    assert engine._internals.in_flight == {}  # noqa: SLF001


@pytest.mark.asyncio
async def test_task_joining_a_run_that_raises_gets_the_same_error() -> None:
    engine = Engine()
    release = asyncio.Event()
    runs = 0

    @engine.query(name="shared", cache_exceptions=False)
    async def shared() -> int:
        nonlocal runs
        runs += 1
        await release.wait()
        raise ValueError(f"boom {runs}")

    owner = asyncio.create_task(shared())
    await _until(lambda: runs == 1)
    waiter = asyncio.create_task(shared())
    await _until(lambda: _waiting(engine) == 1)
    release.set()
    results = await asyncio.wait_for(
        asyncio.gather(owner, waiter, return_exceptions=True), TIMEOUT
    )
    assert [type(r) for r in results] == [ValueError, ValueError]
    assert [str(r) for r in results] == ["boom 1", "boom 1"]
    assert runs == 1
    assert _waiting(engine) == 0
    assert engine._internals.in_flight == {}  # noqa: SLF001

    # Not cached: the next call runs it again, and does not find a stale run.
    with pytest.raises(ValueError, match="boom 2"):
        await asyncio.wait_for(shared(), TIMEOUT)


@pytest.mark.asyncio
async def test_cancelled_waiting_task_leaves_no_record_that_it_waits() -> None:
    engine = Engine()
    release = asyncio.Event()
    runs = 0

    @engine.query(name="shared")
    async def shared() -> int:
        nonlocal runs
        runs += 1
        await release.wait()
        return 7

    owner = asyncio.create_task(shared())
    await _until(lambda: runs == 1)
    waiter = asyncio.create_task(shared())
    await _until(lambda: _waiting(engine) == 1)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    # The owner is still running; the record of the wait is gone already.
    assert not owner.done()
    assert _waiting(engine) == 0
    release.set()
    assert await asyncio.wait_for(owner, TIMEOUT) == 7
    assert runs == 1


# ---------------------------------------------------------------------------
# A result loaded from disk while another task waits for it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_joining_a_load_from_disk_gets_the_loaded_result(
    tmp_path: Path,
) -> None:
    """The owner loads ``total`` from disk; to check it, it runs ``count``
    (memoize=False, so not on disk), which waits. A second task asks for
    ``total`` meanwhile and joins the load."""

    def register(engine: Engine, gate: asyncio.Event | None) -> dict[str, Any]:
        runs = {"count": 0, "total": 0}
        log = engine.accumulator("log")

        @engine.input
        def n() -> int:
            return 3

        @engine.query(name="count", memoize=False)
        async def count() -> int:
            runs["count"] += 1
            if gate is not None:
                await gate.wait()
            return n()

        @engine.query(name="total")
        async def total() -> int:
            runs["total"] += 1
            log.push("total")
            return 10 * await count()

        return {"runs": runs, "total": total}

    engine = Engine(cache_dir=tmp_path)
    try:
        first = register(engine, None)
        assert await first["total"]() == 30
        assert first["runs"] == {"count": 1, "total": 1}
    finally:
        engine.shutdown()

    gate = asyncio.Event()
    engine = Engine(cache_dir=tmp_path)
    try:
        second = register(engine, gate)
        total = second["total"]
        owner_effects: dict[str, list[Any]] = {}
        waiter_effects: dict[str, list[Any]] = {}
        owner = asyncio.create_task(total(effects=owner_effects))
        await _until(lambda: second["runs"]["count"] == 1)
        waiter = asyncio.create_task(total(effects=waiter_effects))
        await _until(lambda: _waiting(engine) == 1)
        gate.set()
        assert await asyncio.wait_for(asyncio.gather(owner, waiter), TIMEOUT) == [
            30,
            30,
        ]
        # Loaded, not run, and the waiter got the loaded result with its effects.
        assert second["runs"] == {"count": 1, "total": 0}
        assert owner_effects == waiter_effects == {"log": ["total"]}
        assert ("dedup_wait", "query:total()") in events(engine)
        assert engine._internals.in_flight == {}  # noqa: SLF001
    finally:
        engine.shutdown()


# ---------------------------------------------------------------------------
# A result being verified is kept from eviction, and only while it is
# ---------------------------------------------------------------------------


def test_result_is_kept_from_eviction_only_while_it_is_verified(mode: Mode) -> None:
    """While ``top`` is verified, ``spread`` runs again and stores one more
    new result than there are old ones to evict, so ``top``, the oldest entry
    left, would be next. It is pinned, so ``flag`` goes instead. Once
    verified, ``top`` is an ordinary entry and can be evicted."""
    engine = Engine(max_entries=5)
    runs = {"top": 0}

    @engine.input
    def x() -> int:
        return 1

    @mode.query(engine)
    async def flag() -> bool:
        return x() > 0

    @mode.query(engine)
    async def part(v: int, i: int) -> int:
        return 0

    @mode.query(engine)
    async def spread() -> int:
        v = x()
        total = 0
        for i in range(v + 1):
            total += await get(part(v, i))
        return total

    @mode.query(engine)
    async def top() -> tuple[bool, int]:
        runs["top"] += 1
        await mode.pause()
        return (await get(flag()), await get(spread()))

    @mode.query(engine)
    async def filler(i: int) -> int:
        return i

    key = ("query", top.id, ())
    memos = engine._internals.memos  # noqa: SLF001
    assert mode.call(top) == (True, 0)
    x.set(2)
    engine.clear_traces()
    assert mode.call(top) == (True, 0)
    assert events(engine, "evict")[:3] == [
        ("evict", "query:part(1, 0)"),
        ("evict", "query:part(1, 1)"),
        ("evict", "query:flag()"),
    ]
    assert ("evict", "query:top()") not in events(engine)
    assert key in memos
    if mode.is_async:
        # The sync walk reads flag again after settling the dependencies,
        # finds it evicted and runs it: a new stamp, so top runs too.
        assert ("cache_green", "query:top()") in events(engine)
        assert runs == {"top": 1}

    # Verified and unpinned: new entries push it out like any other.
    for i in range(8):
        assert mode.call(filler, i) == i
    assert key not in memos
    assert_settled(engine)


# ---------------------------------------------------------------------------
# A result another task stored while this one was verifying
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_finds_the_result_another_task_stored_while_it_verified() -> None:
    """Tasks A and B both verify ``top`` after ``x`` changed. Both walk to
    ``dep``, which A runs and B waits for. When ``dep`` is done, A runs
    ``top`` to the end before B wakes (``top``'s body does not suspend). B
    then finds ``top`` red, but A's new result is stored and verified at this
    revision: B uses it, and does not run ``top`` a second time."""
    engine = Engine()
    gate = asyncio.Event()
    runs = {"dep": 0, "top": 0}

    @engine.input
    def x() -> int:
        return 1

    @engine.query(name="dep")
    async def dep() -> int:
        runs["dep"] += 1
        if runs["dep"] > 1:
            await gate.wait()
        return x()

    @engine.query(name="top")
    async def top() -> int:
        runs["top"] += 1
        value = 10 * await dep()
        log.push(f"top {value}")
        return value

    log = engine.accumulator("log")
    assert await top() == 10
    x.set(2)
    engine.clear_traces()
    a_effects: dict[str, list[Any]] = {}
    b_effects: dict[str, list[Any]] = {}
    task_a = asyncio.create_task(top(effects=a_effects))
    await _until(lambda: runs["dep"] == 2)
    task_b = asyncio.create_task(top(effects=b_effects))
    await _until(lambda: _waiting(engine) == 1)
    gate.set()
    assert await asyncio.wait_for(asyncio.gather(task_a, task_b), TIMEOUT) == [20, 20]
    assert runs == {"dep": 2, "top": 2}
    assert a_effects == b_effects == {"log": ["top 20"]}
    assert events(engine, "recompute_start", "dedup_wait", "cache_hit") == [
        ("recompute_start", "query:dep()"),
        ("dedup_wait", "query:dep()"),
        ("recompute_start", "query:top()"),
        ("cache_hit", "query:dep()"),  # A, in top's body
        ("cache_hit", "query:top()"),  # B, finding A's result
    ]
    assert engine._internals.in_flight == {}  # noqa: SLF001


@pytest.mark.asyncio
async def test_task_runs_an_unmemoized_query_another_task_finished_meanwhile() -> None:
    """The same with ``top`` unmemoized: a result A stored meanwhile is no
    use to B, which needs a value of its own, so B runs ``top`` too."""
    engine = Engine()
    gate = asyncio.Event()
    runs = {"dep": 0, "top": 0}

    @engine.input
    def x() -> int:
        return 1

    @engine.query(name="dep")
    async def dep() -> int:
        runs["dep"] += 1
        if runs["dep"] > 1:
            await gate.wait()
        return x()

    @engine.query(name="top", memoize=False)
    async def top() -> int:
        runs["top"] += 1
        return 10 * await dep()

    assert await top() == 10
    x.set(2)
    task_a = asyncio.create_task(top())
    await _until(lambda: runs["dep"] == 2)
    task_b = asyncio.create_task(top())
    await _until(lambda: _waiting(engine) == 1)
    engine.clear_traces()
    gate.set()
    assert await asyncio.wait_for(asyncio.gather(task_a, task_b), TIMEOUT) == [20, 20]
    assert runs == {"dep": 2, "top": 3}
    assert events(engine, "recompute_start") == 2 * [("recompute_start", "query:top()")]
    assert_settled(engine)


def test_thread_finds_the_result_another_thread_stored_while_it_verified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The sync twin of the test above, with threads. A thread cannot be
    held at a point of its own choosing, so B's walk is wrapped to return
    only once A has stored its result."""
    engine = Engine()
    gate = threading.Event()
    a_done = threading.Event()
    runs = {"dep": 0, "top": 0}
    log = engine.accumulator("log")

    @engine.input
    def x() -> int:
        return 1

    @engine.query(name="dep")
    def dep() -> int:
        runs["dep"] += 1
        if runs["dep"] > 1:
            assert gate.wait(TIMEOUT)
        return x()

    @engine.query(name="top")
    def top() -> int:
        runs["top"] += 1
        value = 10 * dep()
        log.push(f"top {value}")
        return value

    assert top() == 10
    x.set(2)
    evaluator = engine._evaluator  # noqa: SLF001
    walk = evaluator.try_mark_green

    def b_walk(key: Any, entry: Any, snapshot: Any) -> bool:
        green = walk(key, entry, snapshot)
        if threading.current_thread().name == "B" and key[1] == "top":
            assert a_done.wait(TIMEOUT)
        return green

    monkeypatch.setattr(evaluator, "try_mark_green", b_walk)
    engine.clear_traces()
    effects: dict[str, dict[str, list[Any]]] = {"A": {}, "B": {}}
    values: dict[str, int] = {}

    def call(name: str) -> None:
        values[name] = top(effects=effects[name])
        if name == "A":
            a_done.set()

    a = threading.Thread(target=call, args=("A",), name="A", daemon=True)
    b = threading.Thread(target=call, args=("B",), name="B", daemon=True)
    a.start()
    deadline = time.monotonic() + TIMEOUT
    while runs["dep"] < 2 and time.monotonic() < deadline:
        time.sleep(0.001)
    b.start()
    while _waiting(engine) < 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert _waiting(engine) == 1
    gate.set()
    a.join(TIMEOUT)
    b.join(TIMEOUT)
    assert values == {"A": 20, "B": 20}
    assert runs == {"dep": 2, "top": 2}
    assert effects == {"A": {"log": ["top 20"]}, "B": {"log": ["top 20"]}}
    assert events(engine, "recompute_start", "dedup_wait", "cache_hit") == [
        ("recompute_start", "query:dep()"),
        ("dedup_wait", "query:dep()"),
        ("recompute_start", "query:top()"),
        ("cache_hit", "query:dep()"),
        ("cache_hit", "query:top()"),
    ]
    assert_settled(engine)


# ---------------------------------------------------------------------------
# Expiry on the fast path: a result is current up to its expiry, inclusive
# ---------------------------------------------------------------------------


def test_result_with_an_expiry_beneath_it_is_a_hit_up_to_that_moment(
    mode: Mode, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = [1000.0]
    monkeypatch.setattr("cascade._evaluator.time.time", lambda: now[0])
    engine = Engine()
    runs = {"stamp": 0, "top": 0}

    @mode.query(engine, ttl=10.0)
    async def stamp() -> float:
        runs["stamp"] += 1
        return now[0]

    @mode.query(engine)
    async def top() -> float:
        runs["top"] += 1
        await mode.pause()
        return await get(stamp())

    assert mode.call(top) == 1000.0
    assert memo_of(engine, top).expires_at == 1010.0

    # Same revision, exactly at the expiry: still a plain hit, no walk.
    now[0] = 1010.0
    engine.clear_traces()
    assert mode.call(top) == 1000.0
    assert events(engine) == [("cache_hit", "query:top()")]

    # Past it: walked, the TTL result runs again, and so does top.
    now[0] = 1010.5
    engine.clear_traces()
    assert mode.call(top) == 1010.5
    assert runs == {"stamp": 2, "top": 2}
    assert ("cache_hit", "query:top()") not in events(engine)
    assert_settled(engine)


# ---------------------------------------------------------------------------
# Fixed points: a member asked for twice in one iteration
# ---------------------------------------------------------------------------


def test_member_asked_for_twice_in_one_iteration_runs_once_and_replays(
    mode: Mode,
) -> None:
    engine = Engine()
    runs = {"member": 0, "head": 0}
    log = engine.accumulator("log")

    @mode.query(engine, fixed_point=0)
    async def head() -> int:
        runs["head"] += 1
        await mode.pause()
        first = await get(member())
        second = await get(member())  # the provisional result of this iteration
        log.push(f"head {first} {second}")
        return min(first + 1, 3)

    @mode.query(engine)
    async def member() -> int:
        runs["member"] += 1
        guess = await get(head())
        log.push(f"member {guess}")
        return guess

    effects: dict[str, list[Any]] = {}
    assert mode.call(head, effects=effects) == 3
    assert runs == {"member": 4, "head": 4}
    # Only the last iteration's effects are kept; the second call to member
    # in it replays member's effects as a call does.
    assert effects == {"log": ["member 3", "member 3", "head 3 3"]}

    effects = {}
    assert mode.call(member, effects=effects) == 3
    assert effects == {"log": ["member 3"]}
    assert runs == {"member": 4, "head": 4}
    assert_settled(engine)


def test_member_asked_for_again_through_another_query_runs_once(mode: Mode) -> None:
    """The same, with the second call made by ``via``, a query the head runs
    in the same iteration. In async mode ``via``'s body runs in a runtime of
    its own, which must see the provisional results of the iteration."""
    engine = Engine()
    runs = {"member": 0, "head": 0, "via": 0}
    log = engine.accumulator("log")

    @mode.query(engine, fixed_point=0)
    async def head() -> int:
        runs["head"] += 1
        await mode.pause()
        first = await get(member())
        second = await get(via())
        log.push(f"head {first} {second}")
        return min(first + 1, 3)

    @mode.query(engine)
    async def member() -> int:
        runs["member"] += 1
        guess = await get(head())
        log.push(f"member {guess}")
        return guess

    @mode.query(engine)
    async def via() -> int:
        runs["via"] += 1
        return await get(member())

    effects: dict[str, list[Any]] = {}
    assert mode.call(head, effects=effects) == 3
    assert runs == {"member": 4, "head": 4, "via": 4}
    assert effects == {"log": ["member 3", "member 3", "head 3 3"]}
    assert_settled(engine)


@pytest.mark.parametrize("limit", [3, 4])
def test_fixed_point_gets_exactly_max_iterations_runs(mode: Mode, limit: int) -> None:
    """``head = min(head + 1, 3)`` from the guess 0 returns 1, 2, 3 and then
    3 again: four runs, the last one equal to its guess."""
    engine = Engine()
    runs = {"head": 0}

    @mode.query(engine, fixed_point=0, max_iterations=limit, cache_exceptions=False)
    async def head() -> int:
        runs["head"] += 1
        await mode.pause()
        return min(await get(head()) + 1, 3)

    if limit == 4:
        assert mode.call(head) == 3
    else:
        with pytest.raises(CycleError, match="did not converge after 3 iterations"):
            mode.call(head)
    assert runs == {"head": limit}
    assert_settled(engine)


@pytest.mark.parametrize("change", ["noise", "detour"])
def test_unmemoized_head_falls_back_from_its_cached_members_once(
    mode: Mode, change: str
) -> None:
    """As ``test_an_async_head_falls_back_like_a_sync_one`` in
    test_fixed_point_memoize_false.py, with the runs, effects and traces
    asserted. The run on the cached members gives another value ("noise")
    or reaches the head again through a query that is not a member
    ("detour"); it is abandoned once, and the head iterates from the guess."""
    engine = Engine()
    runs = {"head": 0, "mid": 0, "detour": 0}
    state: dict[str, Any] = {}
    notes = engine.accumulator("notes")

    @engine.input
    def cap() -> int:
        return 10

    @mode.query(engine, fixed_point=0, memoize=False)
    async def head() -> int:
        runs["head"] += 1
        notes.push("head")
        await mode.pause()
        if state.get("detour"):
            return min(await get(detour()) + 2, cap())
        return min(await get(mid()) + 2, cap() + state.get("noise", 0))

    @mode.query(engine)
    async def mid() -> int:
        runs["mid"] += 1
        notes.push("mid")
        return await get(head())

    @mode.query(engine)
    async def detour() -> int:
        runs["detour"] += 1
        notes.push("detour")
        return await get(head())

    assert mode.call(head) == 10
    state.update({"noise": -1} if change == "noise" else {"detour": True})
    for name in runs:
        runs[name] = 0
    engine.clear_traces()
    effects: dict[str, list[Any]] = {}
    if change == "noise":
        # 0, 2, 4, 6, 8, 9, 9: six runs of mid, seven of head with the one
        # on the cached members.
        assert mode.call(head, effects=effects) == 9
        assert runs == {"head": 7, "mid": 6, "detour": 0}
        assert effects == {"notes": ["head", "mid"]}
    else:
        assert mode.call(head, effects=effects) == 10
        assert runs == {"head": 7, "mid": 0, "detour": 7}
        assert effects == {"notes": ["head", "detour"]}
    assert events(engine, "cycle_restart") == [("cycle_restart", "query:head()")]
    assert_settled(engine)


def test_caller_catching_an_unencodable_fixed_point_depends_on_what_it_read(
    mode: Mode,
) -> None:
    """A fixed point that ends on a value with no digest raises
    ``UnencodableValueError`` and stores nothing. A caller that catches it
    and returns a fallback must still depend on what the head read."""
    engine = Engine()
    runs = {"caller": 0}

    @engine.input
    def a() -> int:
        return -1

    @mode.query(engine, fixed_point=0)
    async def head() -> Any:
        limit = a()
        guess = await get(head())
        if limit < 0:
            return object()
        return min(guess + 1, limit)

    @mode.query(engine)
    async def caller() -> int:
        runs["caller"] += 1
        await mode.pause()
        try:
            return await get(head())
        except UnencodableValueError:
            return -1

    assert mode.call(caller) == -1
    a.set(2)
    assert mode.call(caller) == 2
    assert runs == {"caller": 2}
    assert_settled(engine)


# ---------------------------------------------------------------------------
# Two tasks entering one fixed point from both ends
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_running_a_member_instead_of_waiting_keeps_its_effects_once() -> (
    None
):
    """The member's task waits for the head's run; the head's task then asks
    for the member, whose owner waits for it. Waiting would never end, so the
    head's task runs the member on its own stack, once per iteration. Its
    effects reach the head's caller once, as from a run of its own."""
    engine = Engine()
    log = engine.accumulator("log")
    go = asyncio.Event()
    runs = {"head": 0, "member": 0}

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(name="head", fixed_point=0)
    async def head() -> int:
        runs["head"] += 1
        if runs["head"] == 1:
            await go.wait()
        value = await member() + inp()
        log.push(f"head {value}")
        return value

    @engine.query(name="member")
    async def member() -> int:
        runs["member"] += 1
        value = await head() // 2
        log.push(f"member {value}")
        return value

    head_effects: dict[str, list[Any]] = {}
    member_effects: dict[str, list[Any]] = {}
    head_task = asyncio.create_task(head(effects=head_effects))
    await _until(lambda: runs["head"] == 1)
    member_task = asyncio.create_task(member(effects=member_effects))
    await _until(lambda: _waiting(engine) == 1)
    go.set()
    # head = head // 2 + 4 from the guess 0: 4, 6, 7, 7.
    assert await asyncio.wait_for(asyncio.gather(head_task, member_task), TIMEOUT) == [
        7,
        3,
    ]
    assert head_effects == {"log": ["member 3", "head 7"]}
    assert member_effects == {"log": ["member 3", "head 7", "member 3"]}
    assert runs == {"head": 4, "member": 5}
    assert events(engine, "dedup_wait", "dedup_cycle") == [
        ("dedup_wait", "query:head()"),
        *4 * [("dedup_cycle", "query:member()")],
    ]
    assert engine._internals.in_flight == {}  # noqa: SLF001
    assert _waiting(engine) == 0


def test_thread_running_a_member_instead_of_waiting_keeps_its_effects_once() -> None:
    """The sync twin of the test above, with threads."""
    engine = Engine()
    log = engine.accumulator("log")
    go = threading.Event()
    runs = {"head": 0, "member": 0}

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(name="head", fixed_point=0)
    def head() -> int:
        runs["head"] += 1
        if runs["head"] == 1:
            assert go.wait(TIMEOUT)
        value = member() + inp()
        log.push(f"head {value}")
        return value

    @engine.query(name="member")
    def member() -> int:
        runs["member"] += 1
        value = head() // 2
        log.push(f"member {value}")
        return value

    effects: dict[str, dict[str, list[Any]]] = {"head": {}, "member": {}}
    values: dict[str, int] = {}

    def call(name: str, handle: Any) -> None:
        values[name] = handle(effects=effects[name])

    head_thread = threading.Thread(target=call, args=("head", head), daemon=True)
    member_thread = threading.Thread(target=call, args=("member", member), daemon=True)
    head_thread.start()
    deadline = time.monotonic() + TIMEOUT
    while runs["head"] < 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    member_thread.start()
    while _waiting(engine) < 1 and time.monotonic() < deadline:
        time.sleep(0.001)
    assert _waiting(engine) == 1
    go.set()
    head_thread.join(TIMEOUT)
    member_thread.join(TIMEOUT)
    assert values == {"head": 7, "member": 3}
    assert effects == {
        "head": {"log": ["member 3", "head 7"]},
        "member": {"log": ["member 3", "head 7", "member 3"]},
    }
    assert runs == {"head": 4, "member": 5}
    assert events(engine, "dedup_wait", "dedup_cycle") == [
        ("dedup_wait", "query:head()"),
        *4 * [("dedup_cycle", "query:member()")],
    ]
    assert_settled(engine)


@pytest.mark.asyncio
async def test_task_running_a_joined_member_on_its_own_guess_keeps_its_effects_once() -> (
    None
):
    """As ``test_task_iterating_a_fixed_point_runs_a_joined_member_on_its_own_guess``
    in test_cross_thread_cycles.py, with a fixed point that converges at its
    first iteration (everything is 0), so the effects of the member the third
    task runs on its own stack are the ones its caller gets."""
    engine = Engine()
    log = engine.accumulator("log")
    runs = {"member": 0, "side": 0}
    owner_cancelled = asyncio.Event()

    @engine.input
    def inp() -> int:
        return 0

    @engine.query(name="head", fixed_point=0)
    async def head() -> int:
        value = await member() + await side() + inp()
        log.push(f"head {value}")
        return value

    @engine.query(name="member")
    async def member() -> int:
        runs["member"] += 1
        if runs["member"] == 2:
            # The first task's run, inside its own head(). Held until the
            # owner is gone and the third task has joined this run.
            await owner_cancelled.wait()
            await _until(lambda: _waiting(engine) >= 1)
        value = await head() // 2
        log.push(f"member {value}")
        return value

    @engine.query(name="side")
    async def side() -> int:
        runs["side"] += 1
        if runs["side"] == 1:
            await _until(lambda: _waiting(engine) >= 1)  # the owner of head() waits
        value = await head() // 4
        log.push(f"side {value}")
        return value

    first = asyncio.create_task(side())
    await _until(lambda: runs["side"] == 1)
    owner = asyncio.create_task(head())
    await _until(lambda: runs["member"] == 2)
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    owner_cancelled.set()
    third_effects: dict[str, list[Any]] = {}
    third = asyncio.create_task(head(effects=third_effects))
    third_value, first_outcome = await asyncio.wait_for(
        asyncio.gather(third, first, return_exceptions=True), TIMEOUT
    )
    assert third_value == 0
    assert first_outcome == 0 or isinstance(first_outcome, CycleError)
    assert third_effects == {"log": ["member 0", "side 0", "head 0"]}
    assert runs == {"member": 3, "side": 2}
    assert events(engine, "dedup_wait", "dedup_cycle") == [
        ("dedup_wait", "query:side()"),
        ("dedup_cycle", "query:head()"),
        ("dedup_wait", "query:member()"),
    ]
    assert engine._internals.in_flight == {}  # noqa: SLF001
    assert _waiting(engine) == 0


# ---------------------------------------------------------------------------
# A task whose joined run was cancelled runs the query itself
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_task_whose_joined_run_was_cancelled_retries_and_says_why() -> None:
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
    runs = 0

    @engine.input
    def x() -> int:
        return 1

    @engine.query(name="slow")
    async def slow() -> int:
        nonlocal runs
        runs += 1
        if runs == 1:
            # The submitted call, on a worker thread with a loop of its own.
            started.set()
            while not release.is_set():
                await asyncio.sleep(0.002)
        return x() * 10

    snap0 = engine.snapshot()
    try:
        submitted = engine.submit(slow)
        assert await asyncio.to_thread(started.wait, TIMEOUT)
        waiter = asyncio.create_task(slow(snapshot=snap0))
        await _until(lambda: _waiting(engine) == 1)
        x.set(2)  # cancels the submitted call
        release.set()
        with pytest.raises(QueryCancelled):
            await asyncio.to_thread(submitted.result, TIMEOUT)
        # The waiter was not cancelled: it runs slow at its own revision.
        assert await asyncio.wait_for(waiter, TIMEOUT) == 10
    finally:
        snap0.release()
    assert runs == 2
    retries = [(e.key, e.detail) for e in engine.traces() if e.event == "dedup_retry"]
    assert retries == [("query:slow()", "joined run raised QueryCancelled")]
    assert _waiting(engine) == 0
    assert engine._internals.in_flight == {}  # noqa: SLF001


@pytest.mark.asyncio
async def test_task_retrying_an_unmemoized_query_runs_it_for_its_value() -> None:
    """As the test above, for a query that keeps no value: the row left by
    an earlier run is current at the waiter's revision, but the waiter needs
    a value, so its retry runs the query."""
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
    runs = 0

    @engine.input
    def x() -> int:
        return 1

    @engine.query(name="slow", memoize=False)
    async def slow() -> int:
        nonlocal runs
        runs += 1
        if runs == 2:
            started.set()
            while not release.is_set():
                await asyncio.sleep(0.002)
        return x() * 10

    snap0 = engine.snapshot()
    try:
        assert await slow() == 10  # leaves a row, current at snap0
        submitted = engine.submit(slow)
        assert await asyncio.to_thread(started.wait, TIMEOUT)
        waiter = asyncio.create_task(slow(snapshot=snap0))
        await _until(lambda: _waiting(engine) == 1)
        x.set(2)  # cancels the submitted call
        release.set()
        with pytest.raises(QueryCancelled):
            await asyncio.to_thread(submitted.result, TIMEOUT)
        assert await asyncio.wait_for(waiter, TIMEOUT) == 10
    finally:
        snap0.release()
    assert runs == 3
    assert [e.detail for e in engine.traces() if e.event == "dedup_retry"] == [
        "joined run raised QueryCancelled"
    ]
    assert_settled(engine)


@pytest.mark.parametrize("memoize", [True, False])
def test_thread_whose_joined_run_was_cancelled_retries_and_says_why(
    memoize: bool,
) -> None:
    """The sync twin of the two tests above, with a thread as the waiter."""
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
    runs = 0

    @engine.input
    def x() -> int:
        return 1

    @engine.query(name="slow", memoize=memoize)
    def slow() -> int:
        nonlocal runs
        runs += 1
        if runs == 2:
            started.set()
            assert release.wait(TIMEOUT)
        return x() * 10

    snap0 = engine.snapshot()
    try:
        assert slow() == 10
        if memoize:
            engine.prune([])  # the submitted call has to run it
        submitted = engine.submit(slow)
        assert started.wait(TIMEOUT)
        outcome: list[Any] = []
        waiter = threading.Thread(
            target=lambda: outcome.append(slow(snapshot=snap0)), daemon=True
        )
        waiter.start()
        deadline = time.monotonic() + TIMEOUT
        while _waiting(engine) < 1 and time.monotonic() < deadline:
            time.sleep(0.001)
        assert _waiting(engine) == 1
        x.set(2)  # cancels the submitted call
        release.set()
        with pytest.raises(QueryCancelled):
            submitted.result(TIMEOUT)
        waiter.join(TIMEOUT)
        assert outcome == [10]
    finally:
        snap0.release()
    assert runs == 3
    assert [(e.key, e.detail) for e in engine.traces() if e.event == "dedup_retry"] == [
        ("query:slow()", "joined run raised QueryCancelled")
    ]
    assert_settled(engine)


def test_submitted_call_cancelled_while_a_body_runs_stores_nothing(
    mode: Mode,
) -> None:
    """The input changes while ``inner``'s body runs and reads nothing more:
    the body finishes, but neither result is stored. ``submit`` calls
    ``outer`` from plain code, so in async mode ``outer`` takes the sync
    path and runs its body on a loop of its own, where awaiting ``inner``
    takes the async path."""
    engine = Engine()
    started = threading.Event()
    release = threading.Event()
    runs = {"inner": 0, "outer": 0}

    @engine.input
    def x() -> int:
        return 1

    @mode.query(engine)
    async def inner() -> int:
        runs["inner"] += 1
        value = x() * 10
        if runs["inner"] == 1:
            # The submitted call, on a worker thread: blocking is fine there.
            started.set()
            assert release.wait(TIMEOUT)
        return value

    @mode.query(engine)
    async def outer() -> int:
        runs["outer"] += 1
        return await get(inner()) + 1

    snap0 = engine.snapshot()
    try:
        submitted = engine.submit(outer)
        assert started.wait(TIMEOUT)
        x.set(2)
        release.set()
        with pytest.raises(QueryCancelled):
            submitted.result(TIMEOUT)
        assert engine._internals.memos == {}  # noqa: SLF001
        assert mode.call(outer, snapshot=snap0) == 11
        assert runs == {"inner": 2, "outer": 2}
    finally:
        snap0.release()
    assert mode.call(outer) == 21
    assert_settled(engine)


# ---------------------------------------------------------------------------
# The persistent cache is read only for a result missing from memory
# ---------------------------------------------------------------------------

# What a load from disk traces (the writer thread traces "disk_store" too,
# whenever it gets round to a result).
DISK_READS = ("disk_hit", "disk_miss", "disk_red", "disk_red_ttl", "disk_unkeyed")


def test_disk_is_read_only_for_a_memoized_result_missing_from_memory(
    mode: Mode, tmp_path: Path
) -> None:
    engine = Engine(cache_dir=tmp_path)
    try:
        runs = {"kept": 0, "volatile": 0}

        @engine.input
        def a() -> int:
            return 1

        @mode.query(engine)
        async def kept() -> int:
            runs["kept"] += 1
            await mode.pause()
            return a() * 10

        @mode.query(engine, memoize=False)
        async def volatile() -> int:
            runs["volatile"] += 1
            return a() + 1

        assert mode.call(kept) == 10
        assert mode.call(volatile) == 2
        engine.flush_disk()
        # Back to the stored input: the record on disk would verify, but the
        # result in memory is red and runs again; nothing is read from disk.
        a.set(2)
        a.set(1)
        engine.clear_traces()
        assert mode.call(kept) == 10
        assert mode.call(volatile) == 2
        assert runs == {"kept": 2, "volatile": 2}
        assert events(engine, *DISK_READS) == []
        assert_settled(engine)
    finally:
        engine.shutdown()

    # A new engine has nothing in memory: the memoized result is loaded.
    engine = Engine(cache_dir=tmp_path)
    try:
        runs = {"kept": 0, "volatile": 0}

        @engine.input
        def a() -> int:  # noqa: F811 - registered again on the new engine
            return 1

        @mode.query(engine)
        async def kept() -> int:  # noqa: F811
            runs["kept"] += 1
            await mode.pause()
            return a() * 10

        assert mode.call(kept) == 10
        assert runs == {"kept": 0, "volatile": 0}
        assert ("disk_hit", "query:kept()") in events(engine)
    finally:
        engine.shutdown()


# ---------------------------------------------------------------------------
# A body run during a verification is still part of that verification
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_body_run_while_verifying_does_not_retry_a_joined_cycle_error() -> None:
    """As ``test_retry_after_verification_does_not_retry_joined_cycle_error_async``
    in test_async_parity.py, one level down: the walk for ``top`` runs
    ``parent`` again, and it is ``parent``'s body that joins the run of
    ``dep`` ending in ``CycleError``. That body runs in a runtime of its own,
    which must still say that it verifies: the error is not retried."""
    engine = Engine()
    dep_entered = asyncio.Event()
    release_dep = asyncio.Event()
    runs = {"dep": 0, "parent": 0, "top": 0}

    @engine.input
    def mode() -> str:
        return "ok"

    @engine.query(name="dep", cache_exceptions=False)
    async def dep() -> int:
        runs["dep"] += 1
        if mode() == "cycle":
            if runs["dep"] == 2:
                dep_entered.set()
                await release_dep.wait()
            return await dep()
        return 10

    @engine.query(name="parent")
    async def parent() -> int:
        runs["parent"] += 1
        mode()
        try:
            return await dep() + 1
        except CycleError:
            return -1

    @engine.query(name="top")
    async def top() -> int:
        runs["top"] += 1
        return await parent() * 2

    assert await top() == 22
    mode.set("cycle")
    engine.clear_traces()

    async def dep_directly() -> None:
        with pytest.raises(CycleError):
            await dep()

    dep_task = asyncio.create_task(dep_directly())
    await asyncio.wait_for(dep_entered.wait(), TIMEOUT)
    top_task = asyncio.create_task(top())
    await _until(lambda: _waiting(engine) == 1)
    release_dep.set()
    await asyncio.wait_for(dep_task, TIMEOUT)
    assert await asyncio.wait_for(top_task, TIMEOUT) == -2
    assert runs == {"dep": 2, "parent": 2, "top": 2}
    assert events(engine, "dedup_retry") == []
    assert ("dedup_wait", "query:dep()") in events(engine)
    assert_settled(engine)


def test_body_run_while_verifying_refuses_a_query_running_above(mode: Mode) -> None:
    """``top`` runs and asks for ``x``; verifying ``x`` runs ``y`` again, and
    ``y`` now asks for ``top``, which is running in the caller. The
    verification is abandoned (``_ReenteredQuery``) and ``x`` runs on
    ``top``'s stack, where ``top`` is a fixed-point head. ``y``'s body runs
    in a runtime of its own in async mode, which must keep the chain of
    runtimes the check walks."""
    engine = Engine()
    runs = {"top": 0, "x": 0, "y": 0}

    @engine.input
    def flip() -> bool:
        return False

    @mode.query(engine, fixed_point=0)
    async def top() -> int:
        runs["top"] += 1
        flip()
        await mode.pause()
        return min(await get(x()) + 1, 3)

    @mode.query(engine)
    async def x() -> int:
        runs["x"] += 1
        return await get(y())

    @mode.query(engine)
    async def y() -> int:
        runs["y"] += 1
        if flip():
            return await get(top())
        return 0

    assert mode.call(top) == 1
    flip.set(True)
    engine.clear_traces()
    # top = min(top + 1, 3) from the guess 0: 1, 2, 3, 3.
    assert mode.call(top) == 3
    assert runs == {"top": 5, "x": 5, "y": 9}
    assert events(engine, "dedup_cycle", "dedup_wait") == []
    assert_settled(engine)


# ---------------------------------------------------------------------------
# An unmemoized cycle member that lost its result, entered through itself
# ---------------------------------------------------------------------------


def _unmemoized_member_cycle(
    engine: Engine, mode: Mode, runs: dict[str, int]
) -> tuple[Any, ...]:
    @engine.input
    def step() -> int:
        return 1

    @mode.query(engine, fixed_point=0)
    async def head() -> int:
        runs["head"] += 1
        await mode.pause()
        return min(await get(member()) + step(), 5)

    @mode.query(engine, memoize=False)
    async def member() -> int:
        runs["member"] += 1
        notes.push("member")
        return await get(head())

    @mode.query(engine)
    async def top() -> int:
        runs["top"] += 1
        return await get(member()) * 10

    notes = engine.accumulator("notes")
    return step, head, member, top


def test_dropped_unmemoized_member_called_directly_runs_for_its_value(
    mode: Mode,
) -> None:
    """The member's run reaches its head and starts over through the head
    (``_EnterThroughHead``); the call then looks the member up again. The
    row the head's run left keeps no value: the member runs for it."""
    engine = Engine()
    runs = {"head": 0, "member": 0, "top": 0}
    step, head, member, _top = _unmemoized_member_cycle(engine, mode, runs)

    # member = head, head = min(member + 1, 5) from the guess 0: 1 to 5, 5.
    assert mode.call(head) == 5
    assert runs == {"head": 6, "member": 6, "top": 0}
    engine.prune([("query", head.id, ())])  # keeps head, drops member
    assert memo_of(engine, head) is not None
    step.set(2)
    engine.clear_traces()
    # The run that reached the head is abandoned and its push taken back:
    # what is left is the member's last run and, replayed from the head's
    # result, the member's run inside the head.
    effects: dict[str, list[Any]] = {}
    assert mode.call(member, effects=effects) == 5
    assert effects == {"notes": ["member", "member"]}
    assert runs == {"head": 11, "member": 12, "top": 0}
    # Looked up again after the head's run: found, verified, and run.
    assert events(engine, "cache_hit", "cache_green")[-3:] == [
        ("cache_hit", "query:member()"),
        ("cache_green", "query:member()"),
        ("cache_hit", "query:head()"),
    ]
    assert_settled(engine)


def test_dropped_unmemoized_member_beneath_a_result_is_verified_not_run(
    mode: Mode,
) -> None:
    """The same, reached by the walk that verifies ``top``: there the member
    is only verified, so after the head's run the row it left is enough."""
    engine = Engine()
    runs = {"head": 0, "member": 0, "top": 0}
    step, head, _member, top = _unmemoized_member_cycle(engine, mode, runs)

    assert mode.call(head) == 5
    assert mode.call(top) == 50
    assert runs == {"head": 6, "member": 7, "top": 1}
    mark = engine._internals.next_access_id  # noqa: SLF001
    assert mode.call(head) == 5
    assert mode.call(top) == 50
    engine.sweep_unaccessed(mark)  # drops member only
    assert memo_of(engine, top) is not None
    step.set(2)
    assert mode.call(top) == 50
    assert runs == {"head": 11, "member": 13, "top": 2}
    assert_settled(engine)
