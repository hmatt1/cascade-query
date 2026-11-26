"""A memoize=False fixed-point head called again runs once on its cached members.

A memoize=False query keeps no value, so every call runs its body. For the
head of a fixed-point cycle each run used to drop the members and iterate
from the guess, though nothing had changed. When the head's stored row is
current, one run over the cached members gives the value the cycle
converged to; anything else (a member that must run, a different value)
falls back to iterating from the guess. Every value is compared with a fresh
engine, which always iterates from the guess.
"""

from __future__ import annotations

import asyncio
from collections import Counter
from typing import Any

import pytest

from cascade import Engine


def _cycle(engine: Engine, state: dict[str, Any], runs: Counter[str]) -> Any:
    """head = min(mid + step, cap); mid = low = head. Converges to cap."""

    @engine.input
    def cap() -> int:
        return 10

    @engine.query(fixed_point=0, memoize=False)
    def head() -> int:
        runs["head"] += 1
        notes.push("head")
        if state.get("detour"):
            # Reads a query that also depends on the head, which the last
            # fixed point did not have as a member.
            return min(detour() + state["step"], cap())
        return min(mid() + state["step"], cap() + state.get("noise", 0))

    @engine.query
    def mid() -> int:
        runs["mid"] += 1
        return low()

    @engine.query
    def low() -> int:
        runs["low"] += 1
        return head()

    @engine.query
    def detour() -> int:
        runs["detour"] += 1
        return head()

    @engine.query
    def reader() -> int:
        runs["reader"] += 1
        return head() * 10

    notes = engine.accumulator("notes")
    return cap, head, mid, reader


def _fresh(state: dict[str, Any], cap_value: int | None = None) -> int:
    engine = Engine()
    cap, head, _mid, _reader = _cycle(engine, state, Counter())
    if cap_value is not None:
        cap.set(cap_value)
    return head()


def test_a_second_call_runs_the_head_once_and_no_member() -> None:
    state = {"step": 2}
    engine = Engine(trace_limit=1000)
    runs: Counter[str] = Counter()
    cap, head, mid, reader = _cycle(engine, state, runs)
    assert head() == 10
    assert runs == {"head": 6, "mid": 6, "low": 6}
    runs.clear()
    effects: dict[str, list[Any]] = {}
    assert head(effects=effects) == 10
    assert runs == {"head": 1}
    # The effects are those of the one run, as of a full one's last run.
    assert effects == {"notes": ["head"]}
    assert any(t.event == "cycle_reused" for t in engine.traces())
    runs.clear()
    assert reader() == 100
    assert runs == {"reader": 1, "head": 1}
    runs.clear()
    assert mid() == 10
    assert runs == {}


def test_an_input_change_iterates_from_the_guess_as_before() -> None:
    state = {"step": 2}
    engine = Engine()
    runs: Counter[str] = Counter()
    cap, head, mid, _reader = _cycle(engine, state, runs)
    assert head() == 10
    cap.set(20)
    runs.clear()
    assert head() == 20 == _fresh(state, 20)
    assert runs == {"head": 11, "mid": 11, "low": 11}
    runs.clear()
    assert head() == 20
    assert runs == {"head": 1}
    assert mid() == 20


def test_a_different_value_on_the_cached_members_iterates_from_the_guess() -> None:
    # A body whose value does not follow from what it read (here a hidden
    # global): the one run disagrees with the stored row, so the head
    # iterates from the guess, as a fresh engine does.
    state = {"step": 3}
    engine = Engine(trace_limit=1000)
    runs: Counter[str] = Counter()
    cap, head, mid, _reader = _cycle(engine, state, runs)
    cap.set(11)
    assert head() == 11
    state["noise"] = -1
    runs.clear()
    assert head() == _fresh(state, 11)
    assert runs["mid"] > 0
    assert any(t.event == "cycle_restart" for t in engine.traces())
    assert mid() == _fresh(state, 11)


def test_reaching_the_head_again_iterates_from_the_guess_and_takes_back_effects() -> (
    None
):
    state = {"step": 2}
    engine = Engine(trace_limit=1000)
    runs: Counter[str] = Counter()
    cap, head, _mid, _reader = _cycle(engine, state, runs)
    assert head() == 10
    state["detour"] = True
    runs.clear()
    effects: dict[str, list[Any]] = {}
    assert head(effects=effects) == _fresh(state) == 10
    assert runs["detour"] >= 1
    assert any(t.event == "cycle_restart" for t in engine.traces())
    # Only the run that converged pushed what the caller sees.
    assert effects == {"notes": ["head"]}


def test_a_dropped_member_is_not_reused() -> None:
    state = {"step": 2}
    engine = Engine()
    runs: Counter[str] = Counter()
    cap, head, mid, _reader = _cycle(engine, state, runs)
    assert head() == 10
    store = engine._internals._store  # noqa: SLF001
    with store.lock:
        store.drop_memo_locked(("query", mid.id, ()))
    runs.clear()
    assert head() == 10
    assert runs == {"head": 6, "mid": 6, "low": 6}


def test_a_cycle_with_two_fixed_points_keeps_the_one_iterating_gives() -> None:
    # head = head // 2 + b has two fixed points, 2b - 1 and 2b. From the
    # guess 0 the iteration reaches 2b - 1 for b > 0; starting from an older
    # fixed point it could reach 2b. Reuse happens only when nothing changed.
    engine = Engine()

    @engine.input
    def b() -> int:
        return 6

    @engine.query(fixed_point=0, memoize=False)
    def head() -> int:
        return member() + b()

    @engine.query
    def member() -> int:
        return head() // 2

    def plain(bias: int) -> int:
        h = 0
        while (n := h // 2 + bias) != h:
            h = n
        return h

    for bias in (6, 6, 20, 20, 3, 3, 20):
        b.set(bias)
        assert head() == plain(bias)
        assert head() == plain(bias)


def test_an_async_head_runs_once_too() -> None:
    engine = Engine()
    runs: Counter[str] = Counter()

    @engine.input
    def cap() -> int:
        return 10

    @engine.query(fixed_point=0, memoize=False)
    async def head() -> int:
        runs["head"] += 1
        return min(await mid() + 2, cap())

    @engine.query
    async def mid() -> int:
        runs["mid"] += 1
        return await head()

    async def call() -> int:
        return await head()

    assert asyncio.run(call()) == 10
    runs.clear()
    assert asyncio.run(call()) == 10
    assert runs == {"head": 1}
    cap.set(4)
    assert asyncio.run(call()) == 4


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_with_a_disk_cache_the_next_engine_gets_the_same_value(
    tmp_path: Any, backend: str
) -> None:
    state = {"step": 2}
    for _ in range(2):
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)
        runs: Counter[str] = Counter()
        _cap, head, mid, _reader = _cycle(engine, state, runs)
        assert head() == 10
        assert head() == 10
        assert mid() == 10
        engine.shutdown()


def test_an_expired_row_is_not_reused(monkeypatch: pytest.MonkeyPatch) -> None:
    import cascade._evaluator as evaluator_module

    class Clock:
        now = 1000.0

        def time(self) -> float:
            return self.now

    clock = Clock()
    monkeypatch.setattr(evaluator_module, "time", clock)
    engine = Engine()
    runs: Counter[str] = Counter()
    level = {"value": 7}

    @engine.query(fixed_point=0, memoize=False)
    def head() -> int:
        runs["head"] += 1
        return min(mid() + 1, sensor())

    @engine.query
    def mid() -> int:
        runs["mid"] += 1
        return head()

    @engine.query(ttl=5.0)
    def sensor() -> int:
        return level["value"]

    assert head() == 7
    runs.clear()
    assert head() == 7
    assert runs == {"head": 1}
    level["value"] = 3
    clock.now += 10.0
    runs.clear()
    assert head() == 3
    assert runs["mid"] > 0


def test_a_reused_run_that_raises_iterates_from_the_guess() -> None:
    state: dict[str, Any] = {"raise": False}

    def build() -> Any:
        engine = Engine()

        @engine.query(fixed_point=0, memoize=False, cache_exceptions=True)
        def head() -> int:
            value = min(mid() + 1, 4)
            if state["raise"]:
                raise ValueError(value)
            return value

        @engine.query
        def mid() -> int:
            return head()

        return head

    head = build()
    assert head() == 4
    state["raise"] = True
    with pytest.raises(ValueError) as reused:
        head()
    with pytest.raises(ValueError) as fresh:
        build()()
    assert reused.value.args == fresh.value.args


def test_a_reused_run_with_an_unencodable_value_iterates_from_the_guess(
    tmp_path: Any,
) -> None:
    from cascade import UnencodableValueError

    state = {"odd": False}
    engine = Engine(cache_dir=tmp_path)

    @engine.query(fixed_point=0, memoize=False)
    def head() -> Any:
        value = min(mid() + 1, 4)
        return object() if state["odd"] and value == 4 else value

    @engine.query
    def mid() -> int:
        return head()

    assert head() == 4
    state["odd"] = True
    # On the cached members the run gives an object, which has no digest;
    # iterating from the guess then meets it as a fresh engine does.
    with pytest.raises(UnencodableValueError):
        head()
    engine.shutdown()


def _async_cycle(state: dict[str, Any], runs: Counter[str]) -> Any:
    engine = Engine(trace_limit=1000)

    @engine.input
    def cap() -> int:
        return 10

    @engine.query(fixed_point=0, memoize=False)
    async def head() -> int:
        runs["head"] += 1
        if state.get("detour"):
            return min(await detour() + 2, cap())
        return min(await mid() + 2, cap() + state.get("noise", 0))

    @engine.query
    async def mid() -> int:
        runs["mid"] += 1
        return await head()

    @engine.query
    async def detour() -> int:
        runs["detour"] += 1
        return await head()

    async def call() -> int:
        return await head()

    return engine, call


@pytest.mark.parametrize("change", ["noise", "detour"])
def test_an_async_head_falls_back_like_a_sync_one(change: str) -> None:
    runs: Counter[str] = Counter()
    state: dict[str, Any] = {}
    engine, call = _async_cycle(state, runs)
    assert asyncio.run(call()) == 10
    state.update({"noise": -1} if change == "noise" else {"detour": True})
    runs.clear()
    _fresh_engine, fresh_call = _async_cycle(state, Counter())
    assert asyncio.run(call()) == asyncio.run(fresh_call())
    assert any(t.event == "cycle_restart" for t in engine.traces())
