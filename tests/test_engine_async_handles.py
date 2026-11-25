"""Async inputs and queries called through their handles.

A handle of an async function returns a value when it is called from
synchronous code, and an awaitable on the thread of a running event loop. It
finds out which through ``asyncio._get_running_loop`` where the interpreter
has it, and through ``asyncio.get_running_loop`` where it does not.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import Any

import pytest

import cascade.engine as engine_module
from cascade import Engine


def _pipeline() -> tuple[Any, Any, dict[str, int]]:
    engine = Engine()
    runs = {"default": 0, "body": 0}

    @engine.input
    async def base(k: int) -> int:
        runs["default"] += 1
        return k * 10

    @engine.query
    async def plus_one(k: int) -> int:
        runs["body"] += 1
        return (await base(k)) + 1

    return base, plus_one, runs


@pytest.fixture(params=["private loop lookup", "public loop lookup"])
def loop_lookup(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> str:
    if request.param == "public loop lookup":
        # An interpreter without asyncio._get_running_loop.
        monkeypatch.setattr(engine_module, "_get_loop", None)
    else:
        assert engine_module._get_loop is not None  # noqa: SLF001
    return request.param


def test_async_handles_return_values_to_synchronous_code(loop_lookup: str) -> None:
    base, plus_one, runs = _pipeline()
    assert base(2) == 20
    assert plus_one(2) == 21
    # The default was kept and the result cached: nothing runs again.
    assert (base(2), plus_one(2)) == (20, 21)
    assert runs == {"default": 1, "body": 1}
    base.set(2, 7)
    assert (base(2), plus_one(2)) == (7, 8)
    assert runs == {"default": 1, "body": 2}


def test_async_handles_return_awaitables_on_a_running_loop(loop_lookup: str) -> None:
    base, plus_one, runs = _pipeline()

    async def main() -> list[int]:
        pending = [base(3), plus_one(3), plus_one(4)]
        assert all(inspect.isawaitable(p) for p in pending)
        first = [await p for p in pending]
        # Cached now, and still awaitables.
        return first + [await base(3), await plus_one(3)]

    assert asyncio.run(main()) == [30, 31, 41, 30, 31]
    assert runs == {"default": 2, "body": 2}
    # What the loop computed is there for synchronous code afterwards.
    assert (base(4), plus_one(4)) == (40, 41)
    assert runs == {"default": 2, "body": 2}
