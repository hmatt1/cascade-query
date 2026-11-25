"""Values Cascade cannot encode are reported clearly, never masked (G8, D20).

With ``cache_dir``, a query result that cannot be encoded raises
``UnencodableValueError`` (a ``TypeError``) naming the query, the path to
the bad part and its type; it is not cached. An exception raised by a query
reaches the caller as itself, even when its ``args`` cannot be encoded; it
is then not cached. Without ``cache_dir`` such results and exceptions are
kept as they are (``test_opaque_values.py``). Input values and fixed-point
guesses must be encodable on every engine.
"""

from __future__ import annotations

import asyncio
import collections
import dataclasses
import decimal
import enum
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from cascade import Engine, UnencodableValueError, register_type
from cascade._serde import _children

from .value_helpers import Point

BACKENDS = ("mdbx", "sqlite")


class Plain:
    pass


@dataclasses.dataclass
class Box:
    label: str
    payload: Any


class Pair(NamedTuple):
    left: Any
    right: Any


@pytest.fixture(params=[None, *BACKENDS])
def engine(request: pytest.FixtureRequest, tmp_path: Path) -> Any:
    if request.param is None:
        eng = Engine()
    else:
        eng = Engine(cache_dir=tmp_path, cache_backend=request.param)
    yield eng
    eng.shutdown()


@pytest.fixture(params=BACKENDS)
def disk_engine(request: pytest.FixtureRequest, tmp_path: Path) -> Any:
    """Results must be encodable only where they are written to disk."""
    eng = Engine(cache_dir=tmp_path, cache_backend=request.param)
    yield eng
    eng.shutdown()


@pytest.mark.parametrize(
    ("value", "path"),
    [
        (Plain(), "result "),
        ([1, 2, Plain()], "result[2] "),
        ({"a": 1, "b": (0, Plain())}, "result['b'][1] "),
        (Box("x", [Plain()]), "result.payload[0] "),
        (Pair(1, {"k": Plain()}), "result.right['k'] "),
        ({Plain(): 1}, "result key "),
        (ValueError(Plain()), "result.args[0] "),
        (collections.deque([1, Plain()]), "result[1] "),
        (frozenset({Plain()}), "result element "),
        ({1, Plain()}, "result element "),
    ],
)
def test_unencodable_result_names_the_query_path_and_type(
    disk_engine: Engine, value: Any, path: str
) -> None:
    engine = disk_engine
    runs = {"n": 0}

    @engine.query
    def make(i: int) -> Any:
        runs["n"] += 1
        return value

    with pytest.raises(UnencodableValueError) as info:
        make(7)
    message = str(info.value)
    assert make.id in message and "(7,)" in message
    assert path in message
    assert "Plain" in message
    assert isinstance(info.value, TypeError)
    # Not cached: the next call runs the body again and raises again.
    with pytest.raises(UnencodableValueError):
        make(7)
    assert runs["n"] == 2


def test_exception_with_unencodable_args_reaches_the_caller_uncached(
    disk_engine: Engine,
) -> None:
    engine = disk_engine
    runs = {"n": 0}
    raised: list[BaseException] = []

    @engine.query
    def boom() -> Any:
        runs["n"] += 1
        exc = KeyError(Plain(), "context")
        raised.append(exc)
        raise exc

    with pytest.raises(KeyError) as info:
        boom()
    assert info.value is raised[0]
    with pytest.raises(KeyError) as info:
        boom()
    assert info.value is raised[1]
    assert runs["n"] == 2
    assert "error_not_cached" in [t.event for t in engine.traces()]


def test_encodable_exceptions_are_still_cached(engine: Engine) -> None:
    runs = {"n": 0}

    @engine.query
    def boom() -> Any:
        runs["n"] += 1
        raise ValueError("plain", 3)

    for _ in range(3):
        with pytest.raises(ValueError, match="plain"):
            boom()
    assert runs["n"] == 1


def test_caller_that_catches_the_error_still_tracks_what_failed(
    disk_engine: Engine,
) -> None:
    engine = disk_engine

    @engine.input
    def mode() -> str:
        return "bad"

    @engine.query
    def child() -> Any:
        return Plain() if mode() == "bad" else "fine"

    @engine.query
    def parent() -> str:
        try:
            return child()
        except UnencodableValueError as exc:
            assert child.id in str(exc)
            return "fallback"

    @engine.query
    def raw_parent() -> Any:
        return child()

    assert parent() == "fallback"
    with pytest.raises(UnencodableValueError, match=child.id):
        raw_parent()
    mode.set("good")
    assert parent() == "fine"
    assert raw_parent() == "fine"


def test_unencodable_async_result_is_reported(disk_engine: Engine) -> None:
    engine = disk_engine

    @engine.query
    async def make() -> Any:
        return {"deep": [Plain()]}

    async def main() -> Any:
        return await make()

    with pytest.raises(UnencodableValueError, match=r"result\['deep'\]\[0\]"):
        asyncio.run(main())


def test_unencodable_exception_from_an_async_query_reaches_the_caller(
    engine: Engine,
) -> None:
    @engine.query
    async def boom() -> Any:
        raise RuntimeError(Plain())

    async def main() -> Any:
        return await boom()

    with pytest.raises(RuntimeError) as info:
        asyncio.run(main())
    assert isinstance(info.value.args[0], Plain)


def test_unencodable_input_value_names_the_input(engine: Engine) -> None:
    @engine.input
    def setting() -> Any:
        return 0

    with pytest.raises(UnencodableValueError) as info:
        setting.set([1, Box("b", Plain())])
    message = str(info.value)
    assert setting.id in message
    assert "value[1].payload" in message
    assert setting() == 0


def test_unencodable_input_default_names_the_input() -> None:
    engine = Engine()

    @engine.input
    def setting() -> Any:
        return Plain()

    @engine.query
    def reader() -> Any:
        return setting()

    with pytest.raises(UnencodableValueError, match=setting.id):
        reader()


def test_unencodable_fixed_point_results_are_reported_not_hung() -> None:
    engine = Engine()

    @engine.query(fixed_point=Plain())
    def bad_guess() -> int:
        bad_guess()  # reads the guess, which cannot be compared
        return 1

    with pytest.raises(UnencodableValueError, match="fixed_point= guess"):
        bad_guess()

    @engine.query(fixed_point=0)
    def bad_result() -> Any:
        bad_result()
        return Plain()

    with pytest.raises(UnencodableValueError, match=bad_result.id):
        bad_result()
    # Nothing provisional or in flight is left behind.
    assert not engine._internals.in_flight  # noqa: SLF001


def test_compute_many_reports_the_query(disk_engine: Engine) -> None:
    engine = disk_engine

    @engine.query
    def item(i: int) -> Any:
        return Plain() if i == 2 else i

    with pytest.raises(UnencodableValueError, match=r"\(2,\)"):
        engine.compute_many([(item, (i,)) for i in range(4)])


def test_a_caught_unencodable_guess_leaves_the_caller_consistent() -> None:
    engine = Engine()
    guess = Plain()

    @engine.query(fixed_point=guess)
    def bad_guess() -> int:
        bad_guess()
        return 1

    @engine.query
    def outer() -> Any:
        seen = []
        for _ in range(2):
            try:
                seen.append(bad_guess())
            except UnencodableValueError:
                seen.append("refused")
        return seen

    # The second call must be refused the same way, not see the first run
    # still on the stack and get its guess back.
    assert outer() == ["refused", "refused"]


# --- which part of a value an error names ------------------------------------


class Ring(collections.deque):
    """The codecs take exactly ``deque``, not a subclass of it."""


class Sensor(enum.Enum):
    """Takes any reading: one it has no member for becomes an unnamed member.

    An unnamed member has no name to store, so the codecs store its value.
    """

    OFF = 0

    @classmethod
    def _missing_(cls, value: object) -> Sensor:
        member = object.__new__(cls)
        member._name_ = None
        member._value_ = value
        return member


class Moody:
    """Registered with an encode function that fails ``refusals`` times."""

    def __init__(self, refusals: int) -> None:
        self.refusals = refusals


def _encode_moody(obj: Moody) -> str:
    if obj.refusals:
        obj.refusals -= 1
        raise RuntimeError("not now")
    return "moody"


def _decode_moody(_: str) -> Moody:
    return Moody(0)


register_type(
    Moody,
    encode=_encode_moody,
    decode=_decode_moody,
    name="tests.test_unencodable_values:Moody",
)


def test_refused_container_whose_items_are_fine_is_named_itself(
    disk_engine: Engine,
) -> None:
    engine = disk_engine

    @engine.query
    def make() -> Any:
        return {"ring": Ring([1, 2])}

    with pytest.raises(UnencodableValueError) as info:
        make()
    # Not an item: every item can be encoded. The container is what to fix.
    assert f"result['ring'] is a {Ring.__module__}.Ring," in str(info.value)


def test_enum_member_stored_by_value_names_its_value(disk_engine: Engine) -> None:
    engine = disk_engine

    @engine.query
    def make() -> Any:
        return [Sensor.OFF, Sensor(Plain())]

    with pytest.raises(UnencodableValueError) as info:
        make()
    assert f"result[1].value is a {Plain.__module__}.Plain," in str(info.value)


def test_part_that_fails_for_another_reason_is_not_the_one_named(
    engine: Engine,
) -> None:
    # A lone surrogate cannot be written as UTF-8, so encoding the first
    # item fails too, with a UnicodeEncodeError. That is not a type the
    # codec refuses, and it is not what made the call raise.
    @engine.input
    def setting() -> Any:
        return 0

    with pytest.raises(UnencodableValueError) as info:
        setting.set(["\ud800", Plain()])
    assert f"value[1] is a {Plain.__module__}.Plain," in str(info.value)
    assert setting() == 0


def test_refusal_that_does_not_repeat_is_reported_for_the_whole_value(
    engine: Engine,
) -> None:
    # The error is built by encoding the value again, part by part. When
    # that second attempt succeeds there is no part to name; the error still
    # says which input, and why the first attempt failed.
    @engine.input
    def setting() -> Any:
        return 0

    moody = Moody(refusals=1)
    with pytest.raises(UnencodableValueError) as info:
        setting.set(moody)
    message = str(info.value)
    assert setting.id in message
    assert "value cannot be encoded" in message
    assert "RuntimeError: not now" in message
    assert setting() == 0
    # Nothing was left half set: the same object is accepted now.
    setting.set(moody)
    assert setting() is moody


def test_unencodable_input_value_names_the_arguments_of_the_input(
    engine: Engine,
) -> None:
    @engine.input
    def rate(currency: str, amount: Any) -> Any:
        return 0

    with pytest.raises(UnencodableValueError) as info:
        rate.set("EUR", decimal.Decimal("1.50"), Plain())
    # As the call was written, though the key holds the Decimal wrapped.
    assert f"input {rate.id}('EUR', Decimal('1.50')) " in str(info.value)


@pytest.mark.parametrize(
    ("value", "parts"),
    [
        pytest.param(Pair(1, "b"), [(".left", 1), (".right", "b")], id="namedtuple"),
        pytest.param(
            Box("x", [2]), [(".label", "x"), (".payload", [2])], id="dataclass"
        ),
        pytest.param([5, 6], [("[0]", 5), ("[1]", 6)], id="list"),
        pytest.param((5, 6), [("[0]", 5), ("[1]", 6)], id="tuple"),
        pytest.param(collections.deque([7, 8]), [("[0]", 7), ("[1]", 8)], id="deque"),
        pytest.param({"k": 1}, [(" key 'k'", "k"), ("['k']", 1)], id="dict"),
        pytest.param({9}, [(" element 9", 9)], id="set"),
        pytest.param(frozenset({"s"}), [(" element 's'", "s")], id="frozenset"),
        pytest.param(
            KeyError("k", 3), [(".args[0]", "k"), (".args[1]", 3)], id="exception"
        ),
        # A named member is stored by its name: there is no part to look in.
        pytest.param(Sensor.OFF, [], id="enum-by-name"),
        pytest.param(Sensor(5), [(".value", 5)], id="enum-by-value"),
        pytest.param(Point(1, "y"), [(" (as encoded)", (1, "y"))], id="registered"),
        pytest.param(Plain(), [], id="unknown-class"),
        pytest.param("text", [], id="scalar"),
    ],
)
def test_parts_of_a_value_and_how_a_message_names_them(
    value: Any, parts: list[tuple[str, Any]]
) -> None:
    """Every kind of container, and the path suffix of each of its parts.

    The suffixes are joined to ``result`` or ``value`` in the message, so
    ``result.payload[0]`` is field ``payload`` and then index 0.
    """
    assert list(_children(value)) == parts
