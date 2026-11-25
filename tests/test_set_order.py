"""A CascadeSet iterates in insertion order, rewritten or not (D29).

The order is the one a dict's keys have: the order in which the elements
now present were added; removing one and adding it again moves it to the
end. Every Python-level way of iterating the set gives it: ``for`` loops,
comprehensions (rewritten or not), helper functions, ``list(s)``, ``repr``,
snapshots, a restart from disk.
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Callable

import pytest

from cascade import CascadeSet, Engine

BACKENDS = ("mdbx", "sqlite")


def _helper(s: Any) -> list[Any]:
    return [x for x in s]


def _bodies(s: Any) -> dict[str, Callable[[], Any]]:
    def listcomp():
        return [x for x in s]

    def for_loop():
        out = []
        for x in s:
            out.append(x)
        return out

    def helper():
        return _helper(s)

    def list_call():
        return list(s)

    def join():
        return ",".join(str(x) for x in s)

    def float_sum():
        # Float addition is not associative: the order shows in the result.
        return sum(x * 0.1 for x in s)

    def first():
        return next(iter(s), None)

    def stable_sort_ties():
        return sorted(s, key=lambda x: 0)

    def min_with_key_ties():
        return min(s, key=lambda x: 0, default=None)

    def dict_comp():
        return {x: i for i, x in enumerate(s)}

    def repr_():
        return repr(s)

    return {
        fn.__name__: fn
        for fn in [
            listcomp,
            for_loop,
            helper,
            list_call,
            join,
            float_sum,
            first,
            stable_sort_ties,
            min_with_key_ties,
            dict_comp,
            repr_,
        ]
    }


STEPS: list[Callable[[Any], Any]] = [
    lambda s: None,
    lambda s: s.add(5),
    lambda s: s.discard(30),
    lambda s: s.add(30),  # back, at the end
    lambda s: s.update([7, 1, 8]),
    lambda s: s.difference_update([20, 99]),
    lambda s: s.intersection_update([1, 5, 30, 8, 7]),
    lambda s: s.symmetric_difference_update([5, 2, 3]),
    lambda s: s.__ior__({40}),
    lambda s: s.__isub__({1}),
    lambda s: s.pop(),
    lambda s: s.clear(),
    lambda s: s.update([3, 2, 1]),
]


def _expected(s: Any) -> list[Any]:
    return list(s._core.log.contents_at(s._core.log.head))  # noqa: SLF001


def test_the_pinned_example() -> None:
    engine = Engine()
    s = CascadeSet(engine, [30, 1, 20])
    assert list(s) == [30, 1, 20]
    assert repr(s) == "CascadeSet({30, 1, 20})"


@pytest.mark.parametrize("body_name", sorted(_bodies(None)))
def test_every_way_of_iterating_agrees(body_name: str) -> None:
    engine = Engine()
    s = CascadeSet(engine, [30, 1, 20])
    rewritten = engine.query(_bodies(s)[body_name], name=f"rw:{body_name}")
    plain = engine.query(
        _bodies(s)[body_name], name=f"plain:{body_name}", incremental_per_element=False
    )
    for step in STEPS:
        step(s)
        order = _expected(s)
        assert list(s) == order  # outside a query
        assert rewritten() == plain()
        if body_name in ("listcomp", "for_loop", "helper", "list_call"):
            assert plain() == order


def test_snapshot_sees_the_order_it_had() -> None:
    engine = Engine()
    s = CascadeSet(engine, [3, 1, 2])

    @engine.query
    def items() -> list[int]:
        return list(s)

    @engine.query
    def shown() -> str:
        return repr(s)

    with engine.snapshot() as before:
        s.discard(3)
        s.add(3)
        s.add(0)
        assert items(snapshot=before) == [3, 1, 2]
        assert shown(snapshot=before) == "CascadeSet({3, 1, 2})"
        assert items() == [1, 2, 3, 0]


def test_equal_values_keep_the_first_and_its_place() -> None:
    engine = Engine()
    s = CascadeSet(engine, [1, 2])
    s.add(1.0)
    s.add(True)
    assert list(s) == [1, 2]
    assert type(list(s)[0]) is int


def test_rolled_back_transaction_restores_the_order() -> None:
    engine = Engine()
    s = CascadeSet(engine, [3, 1, 2])
    with pytest.raises(ZeroDivisionError):
        with engine.transaction():
            s.discard(3)
            s.add(3)
            s.add(9)
            1 / 0  # noqa: B018
    assert list(s) == [3, 1, 2]


@pytest.mark.parametrize("backend", BACKENDS)
def test_named_set_keeps_its_order_across_a_restart(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        s = CascadeSet(engine, [30, 1, 20], name="tags")
        s.discard(30)
        s.add(30)
        s.add(5)
        before = list(s)
        assert before == [1, 20, 30, 5]
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        s = CascadeSet(engine, name="tags")
        assert list(s) == before
        s.compact()
        s.add(2)
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        s = CascadeSet(engine, name="tags")

        @engine.query
        def items() -> list[int]:
            return [x for x in s]

        assert items() == [*before, 2]
    finally:
        engine.shutdown()


def test_threads_adding_keep_iteration_and_log_in_step() -> None:
    engine = Engine()
    s = CascadeSet(engine)
    barrier = threading.Barrier(4)

    def writer(base: int) -> None:
        barrier.wait()
        for i in range(200):
            s.add(base + i)
            if i % 3 == 0:
                s.discard(base + i - 1)

    threads = [threading.Thread(target=writer, args=(k * 1000,)) for k in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
    assert not any(thread.is_alive() for thread in threads)
    assert list(s) == _expected(s)
    assert set(list(s)) == set.copy(s)

    @engine.query
    def items() -> list[int]:
        return [x for x in s]

    assert items() == list(s)


def test_effects_follow_the_order_too() -> None:
    engine = Engine()
    log = engine.accumulator("log")
    s = CascadeSet(engine, [30, 1, 20])
    seen: list[str] = []

    def note(x: int) -> int:
        log.push(x)
        return x

    def body():
        return sum(note(x) for x in s)

    rewritten = engine.query(body, name="effects_rw")
    plain = engine.query(body, name="effects_plain", incremental_per_element=False)
    for step in STEPS[:6]:
        step(s)
        got: dict[str, list[Any]] = {}
        want: dict[str, list[Any]] = {}
        assert rewritten(effects=got) == plain(effects=want)
        assert got == want == {"log": _expected(s)}
        seen.append("ok")
    assert len(seen) == 6
