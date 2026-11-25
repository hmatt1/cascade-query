"""Accumulator effects of a fixed-point cycle are those of its last run (D26).

A head re-runs until a run returns the guess it was computed from. Every
earlier run is abandoned, and so is what it pushed: the head, its members
and every caller above see the pushes of the run that converged, once, on
the computing call, on cache hits and from disk alike.

The cycle below: with ``inp = 4`` and guess ``g`` the head returns
``g // 2 + 4``, so the guesses run 0, 4, 6, 7, 7: four runs, the last one
with guess 7 and member value 3.
"""

from __future__ import annotations

import asyncio
import threading
from pathlib import Path
from typing import Any

import pytest

from cascade import Engine

BACKENDS = ("mdbx", "sqlite")
HANG_TIMEOUT = 10.0

# What the converged run pushes, in order: the head starts, the member runs
# on the guess 7, the head ends with the member's 3.
FINAL_RUN = [("head", "start"), ("member", 7), ("head", 3)]


def _build(engine: Engine, runs: list[str] | None = None) -> dict[str, Any]:
    log = engine.accumulator("log")

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    def head() -> int:
        if runs is not None:
            runs.append("head")
        log.push(("head", "start"))
        m = member()
        log.push(("head", m))
        return m + inp()

    @engine.query
    def member() -> int:
        guess = head()
        log.push(("member", guess))
        return guess // 2

    @engine.query
    def top() -> int:
        log.push(("top", "start"))
        value = head()
        log.push(("top", value))
        return value

    return {"inp": inp, "head": head, "member": member, "top": top, "log": log}


def _call(handle: Any) -> tuple[Any, list[Any]]:
    effects: dict[str, list[Any]] = {}
    value = handle(effects=effects)
    return value, effects.get("log", [])


def test_caller_above_the_head_sees_the_final_run_once() -> None:
    runs: list[str] = []
    q = _build(Engine(), runs)
    assert _call(q["top"]) == (7, [("top", "start"), *FINAL_RUN, ("top", 7)])
    assert runs == ["head"] * 4
    # Replayed from the cache, unchanged.
    assert _call(q["top"]) == (7, [("top", "start"), *FINAL_RUN, ("top", 7)])
    assert runs == ["head"] * 4


def test_head_called_directly_sees_the_final_run_once() -> None:
    q = _build(Engine())
    assert _call(q["head"]) == (7, FINAL_RUN)
    assert _call(q["head"]) == (7, FINAL_RUN)
    assert _call(q["top"]) == (7, [("top", "start"), *FINAL_RUN, ("top", 7)])


def test_member_keeps_its_push_from_the_final_run() -> None:
    q = _build(Engine())
    q["head"]()
    assert _call(q["member"]) == (3, [("member", 7)])


def test_effects_follow_an_input_change() -> None:
    q = _build(Engine())
    _call(q["top"])
    q["inp"].set(10)
    # Guesses 0, 10, 15, 17, 18, 19, 19: the last run has guess 19.
    expected = [("head", "start"), ("member", 19), ("head", 9)]
    assert _call(q["top"]) == (19, [("top", "start"), *expected, ("top", 19)])


def test_head_whose_last_run_raises_a_cached_error() -> None:
    engine = Engine()
    log = engine.accumulator("log")

    @engine.query(fixed_point=0)
    def head() -> int:
        guess = member()
        log.push(guess)
        if guess >= 3:
            raise ValueError("too big")
        return guess + 1

    @engine.query
    def member() -> int:
        return head()

    @engine.query
    def top() -> Any:
        try:
            return head()
        except ValueError as exc:
            return str(exc)

    for _ in range(2):  # computed, then replayed
        effects: dict[str, list[Any]] = {}
        assert top(effects=effects) == "too big"
        assert effects == {"log": [3]}


def test_nested_heads_keep_one_run_of_each() -> None:
    engine = Engine()
    log = engine.accumulator("log")

    @engine.query(fixed_point=0)
    def outer() -> int:
        log.push("outer")
        return min(inner() + 1, 6)

    @engine.query(fixed_point=0)
    def inner() -> int:
        log.push("inner")
        return min(max(inner(), outer()), 6)

    @engine.query
    def top() -> int:
        return outer()

    effects: dict[str, list[Any]] = {}
    assert top(effects=effects) == 6
    assert effects == {"log": ["outer", "inner"]}


def test_compute_many_sees_the_final_run_once() -> None:
    engine = Engine()
    q = _build(engine)
    effects: dict[str, list[Any]] = {}
    assert engine.compute_many([(q["top"], ()), (q["head"], ())], effects=effects) == [
        7,
        7,
    ]
    assert effects == {"log": [("top", "start"), *FINAL_RUN, ("top", 7), *FINAL_RUN]}


def test_threads_requesting_the_cycle_each_see_one_run() -> None:
    engine = Engine()
    q = _build(engine)
    barrier = threading.Barrier(6)
    seen: list[Any] = []
    lock = threading.Lock()

    def reader(i: int) -> None:
        barrier.wait()
        outcome = _call(q["top"] if i % 2 else q["head"])
        with lock:
            seen.append((i % 2, outcome))

    threads = [
        threading.Thread(target=reader, args=(i,), daemon=True) for i in range(6)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(HANG_TIMEOUT)
    assert not any(thread.is_alive() for thread in threads)
    for is_top, outcome in seen:
        if is_top:
            assert outcome == (7, [("top", "start"), *FINAL_RUN, ("top", 7)])
        else:
            assert outcome == (7, FINAL_RUN)


def test_async_cycle_sees_the_final_run_once() -> None:
    engine = Engine()
    log = engine.accumulator("log")

    @engine.input
    def inp() -> int:
        return 4

    @engine.query(fixed_point=0)
    async def head() -> int:
        log.push(("head", "start"))
        m = await member()
        log.push(("head", m))
        return m + inp()

    @engine.query
    async def member() -> int:
        guess = await head()
        log.push(("member", guess))
        return guess // 2

    @engine.query
    async def top() -> int:
        log.push(("top", "start"))
        value = await head()
        log.push(("top", value))
        return value

    async def main() -> None:
        for _ in range(2):
            effects: dict[str, list[Any]] = {}
            assert await top(effects=effects) == 7
            assert effects == {"log": [("top", "start"), *FINAL_RUN, ("top", 7)]}

    asyncio.run(asyncio.wait_for(main(), HANG_TIMEOUT))


@pytest.mark.parametrize("backend", BACKENDS)
def test_effects_replayed_from_disk_are_the_final_run(
    tmp_path: Path, backend: str
) -> None:
    expected = (7, [("top", "start"), *FINAL_RUN, ("top", 7)])
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        # A list here too: ``runs`` is a value the queries capture, and a
        # captured None in one session and a list in the next would be
        # different code to the stored hash.
        assert _call(_build(engine, [])["top"]) == expected
    finally:
        engine.shutdown()
    runs: list[str] = []
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        q = _build(engine, runs)
        assert _call(q["top"]) == expected
        assert _call(q["head"]) == (7, FINAL_RUN)
        assert runs == []  # both loaded from disk
    finally:
        engine.shutdown()
