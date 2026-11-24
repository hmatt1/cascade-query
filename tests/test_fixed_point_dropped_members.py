"""Dropping a fixed-point cycle member's result changes nothing (D25, G13).

A member's cached result, when it is stale, is brought up to date by
verifying its dependency on the head first, which enters the cycle through
the head. A member whose result was dropped used to run its own body
instead, enter the cycle from the wrong end and raise ``CycleError``. Every
test compares with a fresh engine that enters the cycle through the head.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any, Callable

import pytest

from cascade import Engine

BACKENDS = ("mdbx", "sqlite")
HANG_TIMEOUT = 10.0


def _build(engine: Engine, runs: list[str] | None = None) -> dict[str, Any]:
    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    def head() -> int:
        if runs is not None:
            runs.append("head")
        value = inp()
        if value == 13:
            raise ValueError("unlucky")
        if value >= 100:
            return value  # the member drops out of the cycle
        return member() + value

    @engine.query
    def member() -> int:
        if runs is not None:
            runs.append("member")
        return head() // 2

    @engine.query
    def top() -> int:
        return member() * 10

    return {"inp": inp, "head": head, "member": member, "top": top}


def _outcome(fn: Callable[[], Any]) -> Any:
    try:
        return ("ok", fn())
    except Exception as exc:  # noqa: BLE001
        return ("raised", type(exc).__name__, str(exc))


def _fresh(value: int) -> tuple[Any, Any, Any]:
    """(head, member, top) from a fresh engine that enters through the head."""
    q = _build(Engine())
    q["inp"].set(value)
    return (_outcome(q["head"]), _outcome(q["member"]), _outcome(q["top"]))


def _drop(engine: Engine, handle: Any) -> None:
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store.drop_memo_locked(("query", handle.id, ()))


def _cached(engine: Engine, handle: Any) -> bool:
    return ("query", handle.id, ()) in engine._internals.memos  # noqa: SLF001


def _warm(q: dict[str, Any], value: int = 4) -> None:
    q["inp"].set(value)
    assert q["head"]() == 7
    assert q["top"]() == 30


@pytest.mark.parametrize("new_value", [4, 6, 13, 100, 250])
def test_dependent_of_a_dropped_member_matches_a_fresh_engine(new_value: int) -> None:
    engine = Engine()
    q = _build(engine)
    _warm(q)
    _drop(engine, q["member"])
    q["inp"].set(new_value)
    assert _outcome(q["top"]) == _fresh(new_value)[2]
    assert _outcome(q["member"]) == _fresh(new_value)[1]
    assert _outcome(q["head"]) == _fresh(new_value)[0]


@pytest.mark.parametrize("new_value", [6, 13, 100])
def test_dropped_member_called_directly_matches_a_fresh_engine(new_value: int) -> None:
    engine = Engine()
    q = _build(engine)
    _warm(q)
    _drop(engine, q["member"])
    q["inp"].set(new_value)
    assert _outcome(q["member"]) == _fresh(new_value)[1]


def test_head_and_member_both_dropped() -> None:
    engine = Engine()
    q = _build(engine)
    _warm(q)
    _drop(engine, q["member"])
    _drop(engine, q["head"])
    q["inp"].set(6)
    assert _outcome(q["top"]) == _fresh(6)[2] == ("ok", 50)


def test_dropped_member_without_an_input_change() -> None:
    runs: list[str] = []
    engine = Engine()
    q = _build(engine, runs)
    _warm(q)
    _drop(engine, q["member"])
    _drop(engine, q["top"])
    runs.clear()
    assert q["top"]() == 30
    # The head was current: entering through it costs no run of the head.
    assert runs == ["member"]


def test_prune_that_drops_the_member() -> None:
    engine = Engine()
    q = _build(engine)
    _warm(q)
    # The head does not depend on its members, so they are not kept.
    engine.prune([("query", q["head"].id, ())])
    assert not _cached(engine, q["member"])
    q["inp"].set(6)
    assert q["top"]() == 50


def test_sweep_unaccessed_that_drops_the_member() -> None:
    engine = Engine()
    q = _build(engine)
    _warm(q)
    mark = engine.access_id
    assert q["head"]() == 7
    assert q["top"]() == 30  # a cache hit: the member is not touched
    engine.sweep_unaccessed(mark)
    assert not _cached(engine, q["member"])
    q["inp"].set(6)
    assert q["top"]() == 50


def test_lru_eviction_that_drops_the_member() -> None:
    engine = Engine(max_entries=3)
    q = _build(engine)

    @engine.query
    def filler() -> int:
        return 0

    _warm(q)
    assert q["head"]() == 7
    assert q["top"]() == 30
    filler()
    assert not _cached(engine, q["member"])
    q["inp"].set(6)
    assert q["top"]() == 50


def test_dropped_member_read_through_a_snapshot() -> None:
    engine = Engine()
    q = _build(engine)
    _warm(q)
    q["inp"].set(6)
    with engine.snapshot() as before:
        q["inp"].set(100)
        _drop(engine, q["member"])
        assert q["top"](snapshot=before) == 50
        assert q["top"]() == _fresh(100)[2][1]


def test_threads_reading_a_dependent_of_a_dropped_member() -> None:
    engine = Engine()
    q = _build(engine)
    _warm(q)
    _drop(engine, q["member"])
    q["inp"].set(6)
    barrier = threading.Barrier(6)
    results: list[Any] = []
    lock = threading.Lock()

    def reader() -> None:
        barrier.wait()
        outcome = _outcome(q["top"])
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=reader, daemon=True) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(HANG_TIMEOUT)
    assert not any(thread.is_alive() for thread in threads)
    assert results == [("ok", 50)] * 6


def test_async_dependent_of_a_dropped_member() -> None:
    engine = Engine()

    @engine.input
    def inp() -> int:
        return 1

    @engine.query(fixed_point=0)
    async def head() -> int:
        return await member() + inp()

    @engine.query
    async def member() -> int:
        return await head() // 2

    @engine.query
    async def top() -> int:
        return await member() * 10

    async def main() -> None:
        inp.set(4)
        assert await head() == 7
        assert await top() == 30
        _drop(engine, member)
        inp.set(6)
        assert await top() == 50

    asyncio.run(asyncio.wait_for(main(), HANG_TIMEOUT))


@pytest.mark.parametrize("backend", BACKENDS)
def test_dropped_member_with_a_disk_cache(tmp_path: Path, backend: str) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        q = _build(engine)
        _warm(q)
        _drop(engine, q["member"])
        q["inp"].set(6)
        assert q["top"]() == 50
    finally:
        engine.shutdown()
    # A new engine whose inputs match reuses the head and recomputes the
    # member from it, without a cached member to route through.
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        q = _build(engine)
        q["inp"].set(6)
        assert q["top"]() == 50
        _drop(engine, q["member"])
        q["inp"].set(4)
        assert q["top"]() == 30
    finally:
        engine.shutdown()


def _build_three(engine: Engine) -> dict[str, Any]:
    @engine.input
    def k() -> int:
        return 1

    @engine.query(fixed_point=0)
    def h() -> int:
        return min(m1() + k(), 40)

    @engine.query
    def m1() -> int:
        return m2() + 1

    @engine.query
    def m2() -> int:
        return h() // 2

    @engine.query
    def out() -> tuple[int, int, int]:
        return (m2(), m1(), h())

    return {"k": k, "h": h, "m1": m1, "m2": m2, "out": out}


@pytest.mark.parametrize("dropped", [("m1",), ("m2",), ("m1", "m2"), ("m1", "m2", "h")])
@pytest.mark.parametrize("disk", [False, True])
def test_longer_cycle_with_dropped_members(
    tmp_path: Path, dropped: tuple[str, ...], disk: bool
) -> None:
    fresh = _build_three(Engine())
    fresh["k"].set(7)
    fresh["h"]()
    expected = fresh["out"]()

    def open_engine() -> Engine:
        if disk:
            return Engine(cache_dir=tmp_path, cache_backend="sqlite")
        return Engine()

    engine = open_engine()
    try:
        q = _build_three(engine)
        q["k"].set(3)
        q["h"]()
        q["out"]()
        if disk:
            # A new engine holds the head from disk and no members.
            engine.shutdown()
            engine = open_engine()
            q = _build_three(engine)
            q["k"].set(3)
            q["h"]()
        for name in dropped:
            _drop(engine, q[name])
        q["k"].set(7)
        assert q["out"]() == expected
    finally:
        engine.shutdown()


def test_cold_entry_through_a_member_still_raises() -> None:
    # Nothing of the cycle was ever computed: entering through a member is
    # the documented CycleError, not a redirect.
    from cascade import CycleError

    engine = Engine()
    q = _build(engine)
    q["inp"].set(4)
    with pytest.raises(CycleError, match="cycle detected"):
        q["top"]()
    assert q["head"]() == 7
    assert q["top"]() == 30


def test_effects_of_a_dropped_member_are_not_doubled() -> None:
    engine = Engine()
    log = engine.accumulator("log")

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        return member() + inp()

    @engine.query
    def member() -> int:
        log.push("member")
        return head() // 2

    @engine.query
    def top() -> int:
        log.push("top")
        return member() * 10

    assert head() == 7
    assert top() == 30
    _drop(engine, member)
    _drop(engine, top)  # so that top's own run meets the missing member
    inp.set(6)
    effects: dict[str, list[Any]] = {}
    assert top(effects=effects) == 50
    # As with the member still cached: the member's run that reached itself
    # and started again from the head leaves nothing behind.
    fresh = Engine()
    fresh_log = fresh.accumulator("log")

    @fresh.input
    def inp2() -> int:
        return 6

    @fresh.query(fixed_point=0)
    def head2() -> int:
        return member2() + inp2()

    @fresh.query
    def member2() -> int:
        fresh_log.push("member")
        return head2() // 2

    @fresh.query
    def top2() -> int:
        fresh_log.push("top")
        return member2() * 10

    head2()
    expected: dict[str, list[Any]] = {}
    assert top2(effects=expected) == 50
    assert effects == expected
