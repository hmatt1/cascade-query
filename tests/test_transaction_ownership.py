"""Who is inside a transaction (G25, D17).

A transaction belongs to the asyncio task that entered it, or to the thread
when it was entered outside any task. Other threads and tasks are outside
it, also when they inherited a copy of the block's context: their input
updates apply at once, and they may not change a collection the transaction
has uncommitted changes on.
"""

from __future__ import annotations

import asyncio
import contextvars
import threading
from typing import Any, Callable

import pytest

from cascade import CascadeDict, CascadeList, CascadeSet, Engine

JOIN_TIMEOUT = 30.0


def _in_thread(fn: Callable[[], Any], *, inherit: bool) -> tuple[str, Any]:
    """Run ``fn`` on a new thread; return ("ok", value) or ("raised", exc).

    ``inherit`` runs it in a copy of the caller's context, which is what a
    new thread gets by default on free-threaded builds and what
    ``asyncio.to_thread`` does everywhere.
    """
    outcome: list[tuple[str, Any]] = []

    def target() -> None:
        try:
            outcome.append(("ok", fn()))
        except BaseException as exc:  # noqa: BLE001
            outcome.append(("raised", exc))

    if inherit:
        context = contextvars.copy_context()
        thread = threading.Thread(target=context.run, args=(target,), daemon=True)
    else:
        thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(JOIN_TIMEOUT)
    assert not thread.is_alive(), "the other thread hung"
    return outcome[0]


def _engine_with_inputs() -> tuple[Engine, Any, Any]:
    engine = Engine()

    @engine.input
    def a() -> int:
        return 0

    @engine.input
    def b() -> int:
        return 0

    a.set(0)
    b.set(0)
    return engine, a, b


# --- threads -----------------------------------------------------------------


@pytest.mark.parametrize("inherit", [False, True])
def test_set_from_another_thread_is_outside_the_transaction(inherit: bool) -> None:
    engine, a, b = _engine_with_inputs()
    with engine.transaction():
        a.set(1)
        assert _in_thread(lambda: b.set(7), inherit=inherit)[0] == "ok"
        # The other thread's update is applied at once; ours is still queued.
        assert b() == 7
        assert a() == 0
    assert (a(), b()) == (1, 7)


@pytest.mark.parametrize("inherit", [False, True])
def test_set_from_another_thread_survives_a_rollback(inherit: bool) -> None:
    engine, a, b = _engine_with_inputs()
    with pytest.raises(RuntimeError, match="abort"):
        with engine.transaction():
            a.set(1)
            _in_thread(lambda: b.set(7), inherit=inherit)
            raise RuntimeError("abort")
    assert (a(), b()) == (0, 7)


@pytest.mark.parametrize("inherit", [False, True])
@pytest.mark.parametrize("commit", [True, False])
def test_other_thread_cannot_change_a_collection_the_transaction_changed(
    inherit: bool, commit: bool
) -> None:
    engine = Engine()
    xs = CascadeList(engine, ["base"])
    untouched = CascadeList(engine, ["base"])

    @engine.query
    def seen() -> list[str]:
        return list(xs)

    assert seen() == ["base"]

    def block() -> None:
        with engine.transaction():
            xs.append("tx")
            status, error = _in_thread(lambda: xs.append("other"), inherit=inherit)
            assert status == "raised"
            assert isinstance(error, RuntimeError)
            assert "another thread or task" in str(error)
            # A collection the transaction has not changed is free.
            assert _in_thread(lambda: untouched.append("other"), inherit=inherit) == (
                "ok",
                None,
            )
            # The refused change left nothing behind, and queries still see
            # the engine as it was before the block.
            assert list(xs) == ["base", "tx"]
            assert seen() == ["base"]
            xs.append("tx2")
            if not commit:
                raise RuntimeError("abort")

    if commit:
        block()
        expected = ["base", "tx", "tx2"]
    else:
        with pytest.raises(RuntimeError, match="abort"):
            block()
        expected = ["base"]
    assert list(xs) == expected
    assert seen() == expected
    assert list(untouched) == ["base", "other"]

    # Once the transaction has ended, the other thread is welcome.
    assert _in_thread(lambda: xs.append("later"), inherit=inherit)[0] == "ok"
    assert list(xs) == [*expected, "later"]
    assert seen() == [*expected, "later"]


@pytest.mark.parametrize("kind", ["set", "dict"])
def test_reserved_set_and_dict_refuse_other_threads(kind: str) -> None:
    engine = Engine()
    if kind == "set":
        coll: Any = CascadeSet(engine, [1])

        def mine() -> None:
            coll.add(2)

        def theirs() -> None:
            coll.discard(1)
    else:
        coll = CascadeDict(engine, {1: 1})

        def mine() -> None:
            coll[2] = 2

        def theirs() -> None:
            coll.clear()

    with pytest.raises(RuntimeError, match="abort"):
        with engine.transaction():
            mine()
            status, error = _in_thread(theirs, inherit=False)
            assert status == "raised" and isinstance(error, RuntimeError)
            raise RuntimeError("abort")
    assert sorted(coll) == [1]
    assert _in_thread(theirs, inherit=False)[0] == "ok"
    assert len(coll) == 0


def test_two_transactions_cannot_hold_one_collection() -> None:
    engine = Engine()
    xs = CascadeList(engine, [])

    def other_transaction() -> None:
        with engine.transaction():
            xs.append("second")

    with engine.transaction():
        xs.append("first")
        status, error = _in_thread(other_transaction, inherit=False)
        assert status == "raised" and isinstance(error, RuntimeError)
    assert list(xs) == ["first"]
    assert _in_thread(other_transaction, inherit=False)[0] == "ok"
    assert list(xs) == ["first", "second"]


@pytest.mark.parametrize("inherit", [False, True])
def test_thread_started_in_a_block_can_open_its_own_transaction(inherit: bool) -> None:
    engine, a, b = _engine_with_inputs()

    def other_transaction() -> int:
        with engine.transaction():
            b.set(3)
            b.set(4)
        return engine.revision

    with engine.transaction():
        a.set(1)
        before = engine.revision
        assert _in_thread(other_transaction, inherit=inherit) == ("ok", before + 1)
        assert b() == 4
    assert (a(), b()) == (1, 4)


def test_nested_transaction_in_the_same_thread_still_raises() -> None:
    engine = Engine()
    other = Engine()
    with engine.transaction():
        with pytest.raises(RuntimeError, match="Nested"):
            with engine.transaction():
                pass
        with pytest.raises(RuntimeError, match="Nested"):
            with other.transaction():
                pass


def test_context_copied_in_a_block_is_harmless_after_the_block() -> None:
    engine, a, _ = _engine_with_inputs()
    with engine.transaction():
        a.set(1)
        context = contextvars.copy_context()
    assert a() == 1
    # Same thread, the block's context, but the transaction is over.
    context.run(a.set, 9)
    assert a() == 9

    def new_transaction() -> None:
        with engine.transaction():
            a.set(10)

    context.run(new_transaction)
    assert a() == 10


def test_failed_commit_does_not_leave_the_collection_reserved() -> None:
    engine, a, _ = _engine_with_inputs()
    xs = CascadeList(engine, [])
    with pytest.raises(TypeError):
        with engine.transaction():
            xs.append(1)
            a.set(object())  # cannot be hashed: the commit fails
    assert _in_thread(lambda: xs.append(2), inherit=False)[0] == "ok"


def test_threads_with_their_own_transactions_and_a_shared_collection() -> None:
    engine, a, _ = _engine_with_inputs()
    shared = CascadeList(engine, [])
    rounds = 150
    workers = 4
    kept: list[list[int]] = [[] for _ in range(workers)]
    refused = [0] * workers
    errors: list[BaseException] = []
    start = threading.Barrier(workers)

    def worker(index: int) -> None:
        own = CascadeList(engine, [])
        try:
            start.wait(JOIN_TIMEOUT)
            for i in range(rounds):
                item = index * rounds + i
                abort = i % 3 == 0
                try:
                    with engine.transaction():
                        own.append(item)
                        a.set(item)
                        shared.append(item)
                        if abort:
                            raise KeyError("abort")
                except KeyError:
                    continue
                except RuntimeError as exc:
                    # Another worker's transaction holds ``shared``. Our
                    # block raised, so all of it was rolled back.
                    assert "another thread or task" in str(exc)
                    refused[index] += 1
                    continue
                kept[index].append(item)
            assert list(own) == kept[index]
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(i,), daemon=True) for i in range(workers)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(JOIN_TIMEOUT)
    assert not any(t.is_alive() for t in threads), "a worker hung"
    assert errors == []
    # Every committed item is there exactly once; nothing rolled back is.
    assert sorted(shared) == sorted(item for items in kept for item in items)

    @engine.query
    def count() -> int:
        return len(shared)

    assert count() == len(shared)


# --- asyncio tasks -----------------------------------------------------------


def test_task_running_during_the_block_is_outside_the_transaction() -> None:
    engine, a, b = _engine_with_inputs()

    async def main() -> tuple[int, int, int, int]:
        async def other() -> None:
            b.set(7)

        with engine.transaction():
            a.set(1)
            await asyncio.wait_for(asyncio.create_task(other()), JOIN_TIMEOUT)
            during = (a(), b())
        return (*during, a(), b())

    assert asyncio.run(main()) == (0, 7, 1, 7)


def test_task_set_after_a_rolled_back_block_is_applied() -> None:
    engine, a, b = _engine_with_inputs()

    async def main() -> tuple[int, int]:
        gate = asyncio.Event()

        async def later() -> None:
            await gate.wait()
            b.set(5)

        task: asyncio.Task[None] | None = None
        with pytest.raises(RuntimeError, match="abort"):
            with engine.transaction():
                a.set(1)
                task = asyncio.create_task(later())
                raise RuntimeError("abort")
        gate.set()
        assert task is not None
        await asyncio.wait_for(task, JOIN_TIMEOUT)
        return a(), b()

    assert asyncio.run(main()) == (0, 5)


@pytest.mark.parametrize("commit", [True, False])
def test_other_task_cannot_change_a_collection_the_transaction_changed(
    commit: bool,
) -> None:
    engine = Engine()
    xs = CascadeList(engine, ["base"])

    async def main() -> list[str]:
        async def other() -> None:
            xs.append("other")

        try:
            with engine.transaction():
                xs.append("tx")
                with pytest.raises(RuntimeError, match="another thread or task"):
                    await asyncio.wait_for(asyncio.create_task(other()), JOIN_TIMEOUT)
                # A coroutine awaited directly runs in this task: inside.
                await other()
                if not commit:
                    raise KeyError("abort")
        except KeyError:
            pass
        await asyncio.wait_for(asyncio.create_task(other()), JOIN_TIMEOUT)
        return list(xs)

    expected = ["base", "tx", "other", "other"] if commit else ["base", "other"]
    assert asyncio.run(main()) == expected


def test_two_tasks_each_with_a_transaction_do_not_mix() -> None:
    engine, a, b = _engine_with_inputs()

    async def main() -> list[int]:
        revisions: list[int] = []
        first_inside = asyncio.Event()
        second_done = asyncio.Event()

        async def first() -> None:
            with engine.transaction():
                a.set(1)
                first_inside.set()
                await second_done.wait()
                a.set(2)
            revisions.append(engine.revision)

        async def second() -> None:
            await first_inside.wait()
            # Not "nested": the first task's transaction is not ours.
            with engine.transaction():
                b.set(10)
                b.set(20)
            revisions.append(engine.revision)
            assert (a(), b()) == (0, 20)
            second_done.set()

        async def spawned_inside() -> None:
            with engine.transaction():
                tasks = [asyncio.create_task(first()), asyncio.create_task(second())]
            await asyncio.wait_for(asyncio.gather(*tasks), JOIN_TIMEOUT)

        await spawned_inside()
        return revisions

    start = engine.revision
    assert asyncio.run(main()) == [start + 1, start + 2]
    assert (a(), b()) == (2, 20)


def test_to_thread_inside_a_block_is_outside_the_transaction() -> None:
    engine, a, b = _engine_with_inputs()
    xs = CascadeList(engine, [])

    async def main() -> None:
        with engine.transaction():
            a.set(1)
            xs.append("tx")
            await asyncio.wait_for(asyncio.to_thread(b.set, 7), JOIN_TIMEOUT)
            assert (a(), b()) == (0, 7)
            with pytest.raises(RuntimeError, match="another thread or task"):
                await asyncio.wait_for(
                    asyncio.to_thread(xs.append, "other"), JOIN_TIMEOUT
                )

    asyncio.run(main())
    assert (a(), b()) == (1, 7)
    assert list(xs) == ["tx"]


def test_event_loop_run_inside_a_thread_owned_block_is_inside() -> None:
    engine, a, b = _engine_with_inputs()
    xs = CascadeList(engine, [])

    async def populate() -> None:
        a.set(1)
        await asyncio.sleep(0)
        b.set(2)
        xs.append("in")

    start = engine.revision
    with engine.transaction():
        asyncio.run(populate())
        assert (a(), b()) == (0, 0)
    assert (a(), b()) == (1, 2)
    assert list(xs) == ["in"]
    assert engine.revision == start + 1
