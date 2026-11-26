"""Fixed-point members expire with their head, and go when it goes.

A member reads the head's guess, which carries no expiry, so a member of a
cycle whose head reads a TTL query kept its value for good at one revision,
though the head's value had expired: read directly after the TTL elapsed, it
gave the old value.

And a head whose result was dropped (LRU, ``prune``, ``sweep_unaccessed``)
while its members were kept found them cached on its next run: it never
reached itself, so it did not iterate. With a TTL query in the cycle that
was run again (expired, or dropped too), the head added the new reading to
the old members and returned one step of the iteration, not a fixed point.

Both found by the state machine (``tests/test_stateful_engine_invariants.py``,
LRU bound 3). A member now takes the head's expiry when the cycle converges,
and dropping a head's result drops its members.
"""

from __future__ import annotations

from typing import Any

import pytest

import cascade._evaluator as evaluator_module
from cascade import Engine


class _Clock:
    now = 1000.0

    def time(self) -> float:
        return self.now


def _cycle(monkeypatch: pytest.MonkeyPatch, memoize: bool) -> Any:
    clock = _Clock()
    monkeypatch.setattr(evaluator_module, "time", clock)
    engine = Engine()
    level = {"value": 0}

    @engine.query(ttl=10.0)
    def sensor() -> int:
        return level["value"]

    # head = head // 2 + sensor, iterated from 0: 0 for 0, -2 for -1.
    @engine.query(fixed_point=0, memoize=memoize)
    def head() -> int:
        return member() + sensor()

    @engine.query
    def member() -> int:
        return head() // 2

    store = engine._internals._store  # noqa: SLF001

    def drop(query: Any) -> None:
        with store.lock:
            store.drop_memo_locked(("query", query.id, ()))

    return clock, level, head, member, sensor, drop


@pytest.mark.parametrize("memoize", [True, False])
def test_a_member_expires_with_the_head(
    monkeypatch: pytest.MonkeyPatch, memoize: bool
) -> None:
    clock, level, head, member, _sensor, _drop = _cycle(monkeypatch, memoize)
    assert head() == 0
    level["value"] = -1
    clock.now += 11.0
    assert member() == -1
    assert head() == -2


@pytest.mark.parametrize("memoize", [True, False])
def test_a_dropped_head_iterates_after_its_reading_expired(
    monkeypatch: pytest.MonkeyPatch, memoize: bool
) -> None:
    clock, level, head, member, _sensor, drop = _cycle(monkeypatch, memoize)
    assert head() == 0
    drop(head)
    assert head() == 0
    level["value"] = -1
    clock.now += 11.0
    assert head() == -2
    assert member() == -1


@pytest.mark.parametrize("memoize", [True, False])
def test_a_dropped_head_iterates_after_its_reading_was_dropped(
    monkeypatch: pytest.MonkeyPatch, memoize: bool
) -> None:
    # Within the TTL a dropped reading may come back with the new value, and
    # then the fixed point must be the new one, not a mix.
    _clock, level, head, _member, sensor, drop = _cycle(monkeypatch, memoize)
    assert head() == 0
    level["value"] = -1
    drop(head)
    drop(sensor)
    assert head() == -2
