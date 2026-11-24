"""Standard-library value types and registered types as values (G18).

Each type is used as a query argument, an input value and a query result,
in memory and on both disk backends, and read back by a new engine on the
same directory with its type intact.
"""

from __future__ import annotations

import collections
import datetime
import decimal
import fractions
import pathlib
import uuid
from pathlib import Path
from typing import Any

import pytest

from cascade import Engine, UnencodableValueError, register_type
from cascade import _canonical as canonical
from cascade._serde import stable_value_digest

from .value_helpers import Box, Color, Level, Mode, Pair, Perm, Point, same_value

BACKENDS = ("mdbx", "sqlite")

VALUES: dict[str, Any] = {
    "enum": Color.BLUE,
    "int_enum": Level.HIGH,
    "str_enum": Mode.SLOW,
    "flag_member": Perm.W,
    "flag_composite": Perm.R | Perm.X,
    "decimal": decimal.Decimal("-1.50"),
    "decimal_nan": decimal.Decimal("NaN"),
    "fraction": fractions.Fraction(-7, 3),
    "complex": complex(1.5, -0.0),
    "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
    "pure_posix_path": pathlib.PurePosixPath("src/a.py"),
    "pure_windows_path": pathlib.PureWindowsPath(r"C:\Src\A.py"),
    "path": pathlib.Path("src") / "b.txt",
    "bytearray": bytearray(b"\x00raw"),
    "range": range(3, 30, 4),
    "deque": collections.deque([1, "two", 3.0], maxlen=5),
    "datetime_fold": datetime.datetime(2024, 11, 3, 1, 30, fold=1),
    "registered": Point(1, (2, "three")),
    "nested": {
        Color.RED: [decimal.Decimal("2.0"), fractions.Fraction(1, 3)],
        "box": Box("p", (pathlib.PurePosixPath("x"), uuid.UUID(int=7))),
        "pair": Pair(range(2), collections.deque([Point(0, 0)])),
    },
}

HASHABLE = {
    name: value
    for name, value in VALUES.items()
    if name not in ("bytearray", "deque", "nested", "decimal_nan")
}


def _engine(tmp_path: Path, backend: str | None) -> Engine:
    if backend is None:
        return Engine()
    return Engine(cache_dir=tmp_path, cache_backend=backend)


@pytest.mark.parametrize("name", sorted(VALUES))
def test_codec_round_trips_with_the_type(name: str) -> None:
    value = VALUES[name]
    assert same_value(canonical.decode(canonical.encode(value)), value)


@pytest.mark.parametrize("backend", [None, *BACKENDS])
@pytest.mark.parametrize("name", sorted(VALUES))
def test_result_and_input_value(tmp_path: Path, backend: str | None, name: str) -> None:
    value = VALUES[name]
    runs = {"result": 0, "reader": 0}
    engine = _engine(tmp_path, backend)
    try:

        @engine.input
        def setting() -> Any:
            return None

        @engine.query
        def result() -> Any:
            runs["result"] += 1
            return value

        @engine.query
        def reader() -> Any:
            runs["reader"] += 1
            return setting()

        assert same_value(result(), value)
        assert same_value(result(), value)
        assert runs["result"] == 1

        setting.set(value)
        assert same_value(reader(), value)
        revision = engine.revision
        # Setting the same value again is a no-op (G3).
        setting.set(canonical.decode(canonical.encode(value)))
        assert engine.revision == revision
        assert same_value(reader(), value)
        assert runs["reader"] == 1
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", [None, *BACKENDS])
@pytest.mark.parametrize("name", sorted(HASHABLE))
def test_argument(tmp_path: Path, backend: str | None, name: str) -> None:
    value = HASHABLE[name]
    seen: list[Any] = []
    engine = _engine(tmp_path, backend)
    try:

        @engine.query
        def echo(x: Any) -> Any:
            seen.append(x)
            return x

        assert same_value(echo(value), value)
        assert same_value(echo(value), value)
        assert len(seen) == 1 and seen[0] is value
    finally:
        engine.shutdown()


def _session(
    cache: Path, backend: str, runs: dict[str, int], value: Any
) -> tuple[Engine, Any, Any, Any]:
    engine = Engine(cache_dir=cache, cache_backend=backend)

    @engine.input(name="value_types.setting")
    def setting() -> Any:
        return None

    @engine.query(name="value_types.result")
    def result() -> Any:
        runs["result"] = runs.get("result", 0) + 1
        return value

    @engine.query(name="value_types.reader")
    def reader() -> Any:
        runs["reader"] = runs.get("reader", 0) + 1
        return [setting(), "read"]

    @engine.query(name="value_types.echo")
    def echo(x: Any) -> Any:
        runs["echo"] = runs.get("echo", 0) + 1
        return [x]

    return engine, setting, result, (reader, echo)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("name", sorted(VALUES))
def test_new_engine_reads_the_value_back_with_its_type(
    tmp_path: Path, backend: str, name: str
) -> None:
    value = VALUES[name]
    hashable = name in HASHABLE
    runs: dict[str, int] = {}
    engine, setting, result, (reader, echo) = _session(tmp_path, backend, runs, value)
    setting.set(value)
    result()
    reader()
    if hashable:
        echo(value)
    engine.shutdown()

    runs.clear()
    engine, setting, result, (reader, echo) = _session(tmp_path, backend, runs, value)
    try:
        setting.set(canonical.decode(canonical.encode(value)))
        assert same_value(result(), value)
        assert same_value(reader(), [value, "read"])
        if hashable:
            assert same_value(echo(value), [value])
        # Nothing ran: every result came from disk, with its type.
        assert runs == {}
    finally:
        engine.shutdown()


def test_look_alike_results_are_changes_for_dependents() -> None:
    """A result that changes only in type or representation re-runs dependents."""
    engine = Engine()
    pairs = [
        (decimal.Decimal("1.0"), decimal.Decimal("1.00")),
        (Level.LOW, 1),
        (pathlib.PureWindowsPath("A"), pathlib.PureWindowsPath("a")),
        (complex(0.0, 0.0), complex(0.0, -0.0)),
        (range(0), range(1, 1)),
        (bytearray(b"x"), b"x"),
    ]

    @engine.input
    def which() -> int:
        return 0

    @engine.input
    def index() -> int:
        return 0

    @engine.query
    def middle() -> Any:
        return pairs[index()][which()]

    seen: list[Any] = []

    @engine.query
    def top() -> str:
        value = middle()
        seen.append(value)
        return repr(value)

    for i, (a, b) in enumerate(pairs):
        index.set(i)
        which.set(0)
        assert top() == repr(a)
        which.set(1)
        assert top() == repr(b), (a, b)
        assert same_value(seen[-1], b)


# --- register_type -----------------------------------------------------------


class Celsius:
    def __init__(self, degrees: float) -> None:
        self.degrees = degrees

    def __eq__(self, other: object) -> bool:
        return type(other) is Celsius and other.degrees == self.degrees

    def __hash__(self) -> int:
        return hash(self.degrees)


class Unregistered:
    pass


def test_register_type_refuses_native_types_and_conflicts() -> None:
    with pytest.raises(ValueError, match="natively"):
        register_type(decimal.Decimal, encode=str, decode=decimal.Decimal)
    with pytest.raises(TypeError, match="class"):
        register_type("Point", encode=str, decode=str)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="callable"):
        register_type(Celsius, encode=None, decode=None)  # type: ignore[arg-type]

    def enc(c: Celsius) -> float:
        return c.degrees

    register_type(Celsius, encode=enc, decode=Celsius, name="test:Celsius")
    # The same registration again is fine (a module imported twice).
    register_type(Celsius, encode=enc, decode=Celsius, name="test:Celsius")
    with pytest.raises(ValueError, match="already registered"):
        register_type(Celsius, encode=lambda c: c.degrees, decode=Celsius)
    with pytest.raises(ValueError, match="already registered for"):
        register_type(Unregistered, encode=str, decode=str, name="test:Celsius")
    # Engine.register_type is the same process-wide registry.
    Engine.register_type(Celsius, encode=enc, decode=Celsius, name="test:Celsius")
    assert same_value(
        canonical.decode(canonical.encode([Celsius(21.5)]))[0].degrees, 21.5
    )


def test_registered_type_with_an_unencodable_part_names_the_path(
    tmp_path: Path,
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
    try:

        @engine.query
        def make() -> Any:
            return [Point(1, Unregistered())]

        with pytest.raises(
            UnencodableValueError, match=r"result\[0\] \(as encoded\)\[1\]"
        ):
            make()
    finally:
        engine.shutdown()


def test_failing_encode_function_is_reported_as_unencodable(tmp_path: Path) -> None:
    class Fragile:
        pass

    def explode(_: Any) -> Any:
        raise RuntimeError("no")

    register_type(
        Fragile, encode=explode, decode=lambda _: Fragile(), name="test:Fragile"
    )
    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
    try:

        @engine.query
        def make() -> Any:
            return Fragile()

        with pytest.raises(UnencodableValueError, match="RuntimeError: no"):
            make()
    finally:
        engine.shutdown()


def test_value_stored_under_an_unregistered_name_is_not_decoded() -> None:
    blob = canonical.encode(Point(1, 2)).replace(
        b"tests.value_helpers:Point", b"tests.value_helpers:Gone!"
    )
    with pytest.raises(ValueError, match="not registered"):
        canonical.decode(blob)


@pytest.mark.parametrize("backend", BACKENDS)
def test_result_stored_under_an_unregistered_name_is_recomputed(
    tmp_path: Path, backend: str
) -> None:
    class Later:
        def __init__(self, v: int) -> None:
            self.v = v

        def __eq__(self, other: object) -> bool:
            return type(other) is Later and other.v == self.v

    runs = {"n": 0}

    def session() -> tuple[Engine, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.query(name="later.make")
        def make() -> Any:
            runs["n"] += 1
            return Later(5)

        return engine, make

    # The first session can encode Later only once it is registered; the
    # second reads a record whose name it cannot resolve (simulated by
    # registering Later under a name that is then edited in the record).
    register_type(
        Later, encode=lambda x: x.v, decode=lambda v: Later(v), name=f"later/{backend}"
    )
    engine, make = session()
    assert make() == Later(5)
    engine.shutdown()
    from cascade import _types

    reg = _types._BY_NAME.pop(f"later/{backend}")  # noqa: SLF001
    try:
        engine, make = session()
        try:
            assert make() == Later(5)
            assert runs["n"] == 2  # recomputed, not decoded wrongly
        finally:
            engine.shutdown()
    finally:
        _types._BY_NAME[f"later/{backend}"] = reg  # noqa: SLF001


# --- numpy ---------------------------------------------------------------------


@pytest.mark.parametrize("backend", [None, *BACKENDS])
def test_numpy_arrays_and_scalars(tmp_path: Path, backend: str | None) -> None:
    np = pytest.importorskip("numpy")
    arrays = [
        np.arange(6, dtype=np.int32).reshape(2, 3),
        np.asfortranarray(np.arange(6.0).reshape(2, 3)),
        np.array([1.5, np.nan, -0.0], dtype=">f8"),
        np.array(["a", "bc"]),
        np.array([], dtype=np.int64),
        np.array(np.datetime64("2024-01-02", "D")),
        np.float32(1.5),
        np.int8(-3),
        np.bool_(True),
    ]
    for value in arrays:
        assert same_value(canonical.decode(canonical.encode(value)), value)
    assert stable_value_digest(np.float32(1.5)) != stable_value_digest(np.float64(1.5))
    assert stable_value_digest(np.arange(3)) != stable_value_digest(
        np.arange(3).reshape(3, 1)
    )
    runs = {"n": 0}

    def session() -> tuple[Engine, Any]:
        engine = _engine(tmp_path, backend)

        @engine.query(name="np.make")
        def make(i: int) -> Any:
            runs["n"] += 1
            return arrays[i]

        return engine, make

    engine, make = session()
    for i, value in enumerate(arrays):
        assert same_value(make(i), value)
    engine.shutdown()
    if backend is not None:
        engine, make = session()
        try:
            for i, value in enumerate(arrays):
                assert same_value(make(i), value)
        finally:
            engine.shutdown()
    assert runs["n"] == len(arrays)


def test_numpy_object_arrays_are_not_encodable() -> None:
    np = pytest.importorskip("numpy")
    with pytest.raises(TypeError):
        canonical.encode(np.array([object()], dtype=object))


@pytest.mark.parametrize("backend", BACKENDS)
def test_named_collection_restores_every_value_type(
    tmp_path: Path, backend: str
) -> None:
    from cascade import CascadeDict, CascadeList

    items = [VALUES[name] for name in sorted(VALUES)]
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        xs = CascadeList(engine, name="value_types.list")
        xs.extend(items)
        d = CascadeDict(engine, name="value_types.dict")
        d.update({name: VALUES[name] for name in sorted(HASHABLE)})
    finally:
        engine.shutdown()
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert same_value(list(CascadeList(engine, name="value_types.list")), items)
        restored = dict(CascadeDict(engine, name="value_types.dict"))
        assert same_value(restored, {name: VALUES[name] for name in sorted(HASHABLE)})
    finally:
        engine.shutdown()
