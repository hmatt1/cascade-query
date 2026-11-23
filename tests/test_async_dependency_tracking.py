"""Sync queries evaluated on behalf of async queries must track dependencies.

Async verification runs sync query bodies (and sync input defaults) on an
executor thread. Without the caller's context the body runs outside any
query, records no dependencies, and its memo goes permanently stale.
"""

from __future__ import annotations

import asyncio

from cascade import Engine


def _deps(engine: Engine, handle: object) -> list[tuple]:
    store = engine._internals._store  # noqa: SLF001
    memo = store.memos[("query", handle.id, ())]  # type: ignore[attr-defined]
    return [dep.key for dep in memo.deps]


def test_sync_dependency_of_async_query_stays_fresh() -> None:
    engine = Engine()

    @engine.input
    def src() -> int:
        return 1

    @engine.query
    def plain() -> int:
        return src() * 10

    @engine.query
    async def outer() -> int:
        return plain() + 1

    async def main() -> list[int]:
        seen = [await outer()]
        for value in (2, 3, 4):
            src.set(value)
            seen.append(await outer())
        return seen

    assert asyncio.run(main()) == [11, 21, 31, 41]
    assert plain() == 40
    assert ("input", src.id, ()) in _deps(engine, plain)


def test_sync_chain_under_async_query_records_every_edge() -> None:
    engine = Engine()

    @engine.input
    def src() -> int:
        return 1

    @engine.query
    def inner() -> int:
        return src() + 1

    @engine.query
    def middle() -> int:
        return inner() * 2

    @engine.query
    async def outer() -> int:
        return middle()

    async def main() -> list[int]:
        seen = [await outer()]
        for value in (5, 9):
            src.set(value)
            seen.append(await outer())
        return seen

    assert asyncio.run(main()) == [4, 12, 20]
    assert ("query", inner.id, ()) in _deps(engine, middle)
    assert ("input", src.id, ()) in _deps(engine, inner)
    assert middle() == 20


def test_unchanged_inputs_still_serve_async_results_from_cache() -> None:
    engine = Engine()
    runs = {"plain": 0}

    @engine.input
    def src() -> int:
        return 1

    @engine.input
    def unrelated() -> int:
        return 0

    @engine.query
    def plain() -> int:
        runs["plain"] += 1
        return src() * 10

    @engine.query
    async def outer() -> int:
        return plain() + 1

    async def main() -> None:
        assert await outer() == 11
        src.set(2)
        assert await outer() == 21
        unrelated.set(1)
        assert await outer() == 21

    asyncio.run(main())
    assert runs["plain"] == 2
