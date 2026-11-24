"""Values Cascade cannot encode are reported clearly, never masked (G8, D20).

A query result that cannot be encoded raises ``UnencodableValueError`` (a
``TypeError``) naming the query, the path to the bad part and its type; it
is not cached. An exception raised by a query reaches the caller as itself,
even when its ``args`` cannot be encoded; it is then not cached.
"""

from __future__ import annotations

import asyncio
import dataclasses
from pathlib import Path
from typing import Any, NamedTuple

import pytest

from cascade import Engine, UnencodableValueError

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
    ],
)
def test_unencodable_result_names_the_query_path_and_type(
    engine: Engine, value: Any, path: str
) -> None:
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
    engine: Engine,
) -> None:
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


def test_caller_that_catches_the_error_still_tracks_what_failed(engine: Engine) -> None:
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


def test_unencodable_async_result_is_reported(engine: Engine) -> None:
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


def test_compute_many_reports_the_query(engine: Engine) -> None:
    @engine.query
    def item(i: int) -> Any:
        return Plain() if i == 2 else i

    with pytest.raises(UnencodableValueError, match=r"\(2,\)"):
        engine.compute_many([(item, (i,)) for i in range(4)])
