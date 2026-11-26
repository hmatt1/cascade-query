"""Queries written as methods (G1).

``@engine.query`` on a function in a class body makes a query whose first
argument is ``self``: read from an instance, the handle binds it, as a
function does. ``self`` is part of the cache key like any other argument.
"""

from __future__ import annotations

import asyncio
import dataclasses
import types
from pathlib import Path
from typing import Any

import pytest

from cascade import Engine, register_type

BACKENDS = ("mdbx", "sqlite")


def test_method_binds_self_and_caches_per_instance() -> None:
    engine = Engine()
    runs: list[Any] = []

    @dataclasses.dataclass(frozen=True)
    class Scaler:
        k: int

        @engine.query
        def scale(self, x: int, offset: int = 0) -> int:
            runs.append((self, x, offset))
            return self.k * x + offset

    a, b = Scaler(2), Scaler(3)
    assert a.scale(5) == 10
    assert a.scale(5) == 10
    assert a.scale(x=5) == 10
    assert Scaler.scale(a, 5, offset=0) == 10  # through the class: same key
    assert b.scale(5) == 15
    assert a.scale(5, offset=1) == 11
    assert [(obj, x) for obj, x, _ in runs] == [(a, 5), (b, 5), (a, 5)]
    assert "bound QueryHandle" in repr(a.scale)
    assert a.scale.id == Scaler.scale.id


def test_method_follows_the_inputs_it_reads() -> None:
    engine = Engine()

    @engine.input
    def rate() -> int:
        return 2

    @dataclasses.dataclass(frozen=True)
    class Account:
        balance: int

        @engine.query
        def interest(self) -> int:
            return self.balance * rate()

    acct = Account(100)
    assert acct.interest() == 200
    rate.set(3)
    assert acct.interest() == 300
    # Equal instances of one class share a result.
    assert Account(100).interest() == 300


def test_unhashable_self_is_refused_clearly_and_nothing_runs() -> None:
    engine = Engine()
    runs = {"n": 0}

    class Mutable:
        def __eq__(self, other: object) -> bool:  # makes the class unhashable
            return self is other

        @engine.query
        def value(self) -> int:
            runs["n"] += 1
            return 1

    with pytest.raises(TypeError, match="is a method, and its self"):
        Mutable().value()
    with pytest.raises(TypeError, match="is a method, and its self"):
        engine.compute_many([(Mutable().value, ())])
    assert runs["n"] == 0


def test_static_and_class_methods() -> None:
    engine = Engine()

    class Tools:
        @staticmethod
        @engine.query
        def double(x: int) -> int:
            return 2 * x

        @classmethod
        @engine.query
        def label(cls, x: int) -> str:
            return f"{cls.__name__}:{x}"

    class More(Tools):
        pass

    assert Tools.double(2) == 4 and Tools().double(2) == 4
    assert Tools.label(1) == "Tools:1"
    assert More.label(1) == "More:1"  # the class is an argument

    with pytest.raises(TypeError, match="put @staticmethod above @engine.query"):

        class Wrong:
            @engine.query
            @staticmethod
            def f(x: int) -> int:
                return x


def test_handle_assigned_to_a_class_is_not_bound() -> None:
    engine = Engine()

    @engine.query
    def plain(x: int) -> int:
        return x + 1

    class Holder:
        q = plain

    assert Holder().q(1) == 2  # a module-level query stays a plain function


def test_bound_methods_work_with_submit_compute_many_and_async() -> None:
    engine = Engine()

    @dataclasses.dataclass(frozen=True)
    class Item:
        n: int

        @engine.query
        def square(self, extra: int = 0) -> int:
            return self.n * self.n + extra

        @engine.query
        async def later(self) -> int:
            await asyncio.sleep(0)
            return -self.n

    items = [Item(i) for i in range(4)]
    try:
        assert engine.compute_many([(item.square, ()) for item in items]) == [
            0,
            1,
            4,
            9,
        ]
        assert engine.compute_many([(items[2].square, (), {"extra": 1})]) == [5]
        assert engine.submit(items[3].square, extra=1).result() == 10

        async def main() -> list[int]:
            return [await item.later() for item in items]

        assert asyncio.run(main()) == [0, -1, -2, -3]
    finally:
        engine.shutdown()


@dataclasses.dataclass(frozen=True)
class Shape:
    sides: int
    length: float

    def perimeter_plain(self) -> float:
        return self.sides * self.length


class Circle:
    def __init__(self, r: float) -> None:
        self.r = r

    def __eq__(self, other: object) -> bool:
        return type(other) is Circle and other.r == self.r

    def __hash__(self) -> int:
        return hash(self.r)


register_type(
    Circle, encode=lambda c: c.r, decode=Circle, name="tests.test_query_methods:Circle"
)


@pytest.mark.parametrize("backend", BACKENDS)
def test_methods_on_encodable_instances_are_reused_from_disk(
    tmp_path: Path, backend: str
) -> None:
    runs: list[str] = []

    def session() -> tuple[Engine, Any, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        class Geometry:
            @engine.query(name="methods.perimeter")
            def perimeter(self: Shape) -> float:
                runs.append("perimeter")
                return self.perimeter_plain()

            @engine.query(name="methods.area")
            def area(self: Circle, scale: float = 1.0) -> float:
                runs.append("area")
                return 3.0 * self.r * self.r * scale

        return engine, Geometry.perimeter, Geometry.area

    engine, perimeter, area = session()
    assert perimeter(Shape(3, 2.0)) == 6.0
    assert area(Circle(2.0), scale=2.0) == 24.0
    engine.shutdown()
    assert runs == ["perimeter", "area"]
    engine, perimeter, area = session()
    try:
        assert perimeter(Shape(3, 2.0)) == 6.0
        assert area(Circle(2.0), 2.0) == 24.0
        assert runs == ["perimeter", "area"]
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_methods_on_instances_that_cannot_be_encoded_are_computed_but_not_stored(
    tmp_path: Path, backend: str
) -> None:
    runs = {"n": 0}

    def session() -> Engine:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        class Counter:
            # Its own __eq__ and __hash__, so it may be a key; no encoding,
            # so its results stay in memory.
            def __eq__(self, other: object) -> bool:
                return self is other

            def __hash__(self) -> int:
                return id(self)

            @engine.query(name="methods.count")
            def count(self) -> int:
                runs["n"] += 1
                return 1

        counter = Counter()
        assert counter.count() == 1
        assert counter.count() == 1
        return engine

    session().shutdown()
    session().shutdown()
    assert runs["n"] == 2


def test_bound_method_handles_compare_by_instance_and_method() -> None:
    engine = Engine()

    class Shape:
        @engine.query
        def area(self) -> int:
            return 1

        @engine.query
        def edges(self) -> int:
            return 4

    a, b = Shape(), Shape()
    # Reading the attribute makes a new handle each time, as for a method.
    assert a.area is not a.area
    assert a.area == a.area
    assert hash(a.area) == hash(a.area)
    assert a.area != b.area
    assert a.area != a.edges
    assert a.area != Shape.area
    assert len({a.area, a.area, b.area, a.edges}) == 3
    assert a.area.raw is Shape.area.raw


def test_method_on_an_object_keyed_by_identity_is_refused() -> None:
    """Its attributes could change without the key changing (a stale result)."""
    engine = Engine()
    runs: list[int] = []

    class Doc:
        def __init__(self, text: str) -> None:
            self.text = text

        @engine.query
        def words(self) -> int:
            runs.append(1)
            return len(self.text.split())

    doc = Doc("a b")
    with pytest.raises(TypeError, match="keyed by identity"):
        doc.words()
    with pytest.raises(TypeError, match="keyed by identity"):
        Doc.words(doc)  # through the class: an ordinary argument
    assert runs == []


def test_object_keyed_by_identity_is_refused_as_any_argument() -> None:
    engine = Engine()

    class Config:
        factor = 2

    @engine.query
    def scaled(config: Any, x: int) -> int:
        return config.factor * x

    @engine.input
    def setting(config: Any) -> int:
        return 0

    with pytest.raises(TypeError, match="keyed by identity"):
        scaled(Config(), 3)
    with pytest.raises(TypeError, match="keyed by identity"):
        scaled(x=3, config=Config())
    with pytest.raises(TypeError, match="keyed by identity"):
        setting(Config())
    with pytest.raises(TypeError, match="keyed by identity"):
        setting.set(Config(), 1)
    with pytest.raises(TypeError, match="keyed by identity"):
        scaled((1, Config()), 3)  # also inside a tuple
    # Classes and functions are what they are: fine as arguments. So is a
    # bare object() used as a sentinel, which has no state.
    assert scaled(Config, 3) == 6
    assert setting(object()) == 0

    @engine.query
    def name_of(thing: Any) -> str:
        return thing.__name__

    def helper() -> None:
        pass

    assert name_of(helper) == "helper"
    assert name_of(types) == "types"
