"""Argument keys: by type, and with keyword arguments bound (G1).

``q(1)``, ``q(True)`` and ``q(1.0)`` are equal and hash alike, but a body can
tell them apart, so each gets its own result, in memory and on disk. Calls
that bind to the same parameters share one: ``q(1)``, ``q(x=1)`` and
``q(1, y=2)`` when ``y=2`` is the default.
"""

from __future__ import annotations

import collections
import dataclasses
import decimal
import fractions
import math
import pathlib
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from cascade import Engine
from cascade import _canonical as canonical
from cascade import _keys

from .test_codec_properties import hashable_scalars
from .value_helpers import Level, Mode, Pair, same_value

BACKENDS = ("mdbx", "sqlite")


@dataclasses.dataclass(frozen=True)
class Frozen:
    a: Any
    b: Any = 0


# Each group: values that are == (and hash alike) but a body can tell apart.
LOOK_ALIKE_GROUPS: list[list[Any]] = [
    [1, True, 1.0, decimal.Decimal(1), fractions.Fraction(1), Level.LOW, complex(1)],
    [0, False, 0.0, -0.0],
    ["fast", Mode.FAST],
    [(1, 2), (1, 2.0), (True, 2), Pair(1, 2)],
    [frozenset({1}), frozenset({True}), frozenset({1.0})],
    [decimal.Decimal("1.0"), decimal.Decimal("1.00")],
    [pathlib.PureWindowsPath("A"), pathlib.PureWindowsPath("a")],
    [range(0), range(1, 1)],
    [Frozen(1), Frozen(1.0), Frozen(True)],
    [(0.0,), (-0.0,)],
]


def _repr_type(x: Any) -> str:
    return f"{type(x).__name__}:{x!r}"


@pytest.mark.parametrize("group", LOOK_ALIKE_GROUPS, ids=lambda g: repr(g[0]))
def test_look_alike_arguments_get_a_result_each(group: list[Any]) -> None:
    engine = Engine()
    seen: list[Any] = []

    @engine.query
    def describe(x: Any) -> str:
        seen.append(x)
        return _repr_type(x)

    for x in group:
        assert describe(x) == _repr_type(x)
    for x in group:
        assert describe(x) == _repr_type(x)  # cached, still its own
    assert len(seen) == len(group)
    for got, want in zip(seen, group):
        assert same_value(got, want)


def test_equal_arguments_of_one_type_still_share_a_result() -> None:
    engine = Engine()
    runs = {"n": 0}
    nan = float("nan")

    @engine.query
    def q(x: Any) -> int:
        runs["n"] += 1
        return 1

    q(1.5)
    q(float("1.5"))
    q((1, 2.5))
    q((1, float("2.5")))
    q(nan)
    q(nan)  # the same object: one result, as for a plain tuple key
    q(Frozen(1.0, "x"))
    q(Frozen(1.0, "x"))
    assert runs["n"] == 4


@pytest.mark.parametrize("backend", BACKENDS)
def test_look_alike_arguments_are_separate_on_disk(
    tmp_path: Path, backend: str
) -> None:
    group = [1, True, 1.0, -0.0, 0.0, (1, 2.0), (1, 2), decimal.Decimal("1.0")]
    runs: list[Any] = []

    def session() -> tuple[Engine, Any, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.query(name="keys.describe")
        def describe(x: Any) -> str:
            runs.append(x)
            return _repr_type(x)

        @engine.query(name="keys.all")
        def describe_all() -> list[str]:
            return [describe(x) for x in group]

        return engine, describe, describe_all

    engine, describe, describe_all = session()
    expected = [_repr_type(x) for x in group]
    assert describe_all() == expected
    engine.shutdown()
    assert len(runs) == len(group)

    # In the reverse order, so that no argument can borrow another's entry.
    runs.clear()
    engine, describe, describe_all = session()
    try:
        assert [describe(x) for x in reversed(group)] == list(reversed(expected))
        assert describe_all() == expected
        assert runs == []
    finally:
        engine.shutdown()


# --- keyword arguments ---------------------------------------------------------


def test_keyword_and_positional_calls_share_a_result() -> None:
    engine = Engine()
    calls: list[tuple[Any, ...]] = []

    @engine.query
    def area(width: int, height: int = 2, *, scale: int = 1) -> int:
        calls.append((width, height, scale))
        return width * height * scale

    assert area(3) == 6
    assert area(3, 2) == 6
    assert area(width=3) == 6
    assert area(height=2, width=3) == 6
    assert area(3, scale=1) == 6
    assert calls == [(3, 2, 1)]
    assert area(3, height=4) == 12
    assert area(3, 4) == 12
    assert area(3, scale=2) == 12
    assert area(3, 2, scale=2) == 12
    assert calls == [(3, 2, 1), (3, 4, 1), (3, 2, 2)]
    # A keyword argument is typed like a positional one.
    assert area(3, scale=True) == 6
    assert len(calls) == 4


def test_varargs_kwargs_and_positional_only_parameters() -> None:
    engine = Engine()
    calls: list[Any] = []

    @engine.query
    def f(a: int, /, b: int = 0, *rest: int, k: int = 5, **extra: int) -> tuple:
        calls.append((a, b, rest, k, extra))
        return (a, b, rest, k, tuple(sorted(extra.items())))

    assert f(1) == (1, 0, (), 5, ())
    assert f(1, b=0, k=5) == (1, 0, (), 5, ())
    assert f(1, 2, 3, 4) == (1, 2, (3, 4), 5, ())
    assert f(1, z=9, y=8) == (1, 0, (), 5, (("y", 8), ("z", 9)))
    assert f(1, y=8, z=9) == (1, 0, (), 5, (("y", 8), ("z", 9)))
    assert len(calls) == 3
    # ``a`` is positional-only, so a=... is an extra keyword, as in Python:
    # f(1, a=2) passes a=2 in **extra.
    assert f(1, a=2) == (1, 0, (), 5, (("a", 2),))


def test_binding_errors_are_raised_before_the_body_and_not_cached() -> None:
    engine = Engine()
    runs = {"n": 0}

    @engine.query
    def q(x: int, y: int = 1) -> int:
        runs["n"] += 1
        return x + y

    with pytest.raises(TypeError, match="missing a required argument: 'x'"):
        q()
    with pytest.raises(TypeError, match="unexpected keyword argument 'z'"):
        q(1, z=2)
    with pytest.raises(TypeError, match="multiple values for argument 'x'"):
        q(1, x=2)
    with pytest.raises(TypeError, match="too many positional arguments"):
        q(1, 2, 3)
    assert runs["n"] == 0
    assert q.id in str(pytest.raises(TypeError, q).value)


def test_defaults_that_cannot_be_keys_are_left_out() -> None:
    engine = Engine()
    missing = object()
    calls: list[Any] = []

    @engine.query
    def q(x: int, opts: list[int] = [], default: Any = missing, z: int = 3) -> Any:  # noqa: B006
        calls.append((x, opts, default, z))
        return x + z

    assert q(1) == 4
    assert q(1, z=3) == 4
    assert q(x=1) == 4
    assert len(calls) == 1
    assert q(1, z=4) == 5
    assert calls[-1] == (1, [], missing, 4)
    assert len(calls) == 2
    assert q(1, default=7) == 4
    assert calls[-1] == (1, [], 7, 3)
    with pytest.raises(TypeError, match="unhashable"):
        q(1, [1])


@pytest.mark.parametrize("backend", BACKENDS)
def test_keyword_calls_and_sentinel_defaults_are_reused_from_disk(
    tmp_path: Path, backend: str
) -> None:
    missing = object()
    runs = {"n": 0}

    def session() -> tuple[Engine, Any, Any]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)

        @engine.query(name="kw.scaled")
        def scaled(x: float, *, factor: float = 2.0, tag: Any = missing) -> float:
            runs["n"] += 1
            return x * factor

        @engine.query(name="kw.total")
        def total() -> float:
            return scaled(1.5) + scaled(x=1.5, factor=3.0) + scaled(2, factor=True)

        return engine, scaled, total

    engine, scaled, total = session()
    assert total() == 3.0 + 4.5 + 2
    engine.shutdown()
    assert runs["n"] == 3
    engine, scaled, total = session()
    try:
        assert total() == 9.5
        assert scaled(factor=3.0, x=1.5) == 4.5
        assert runs["n"] == 3
    finally:
        engine.shutdown()


def test_reserved_keyword_only_parameters_are_refused_at_decoration() -> None:
    engine = Engine()
    with pytest.raises(TypeError, match="'snapshot'"):

        @engine.query
        def q(x: int, *, snapshot: int = 0) -> int:
            return x

    with pytest.raises(TypeError, match="'value'"):

        @engine.input
        def setting(*, value: int = 0) -> int:
            return value

    # As positional parameters they are fine; pass them positionally.
    @engine.query
    def r(effects: int) -> int:
        return effects

    assert r(3) == 3


def test_inputs_key_their_arguments_by_type_and_bind_keywords() -> None:
    engine = Engine()

    @engine.input
    def weight(item: Any, unit: str = "kg") -> float:
        return 0.0

    @engine.query
    def read(item: Any) -> float:
        return weight(item)

    weight.set(1, 5.0)
    weight.set(True, value=7.0)
    weight.set(item=1.0, unit="kg", value=9.0)
    assert weight(1) == 5.0
    assert weight(True) == 7.0
    assert weight(item=1.0) == 9.0
    assert weight(1, "kg") == 5.0
    assert read(1) == 5.0 and read(True) == 7.0 and read(1.0) == 9.0
    with engine.transaction():
        weight.set(True, unit="kg", value=8.0)
    assert read(True) == 8.0 and read(1) == 5.0


def test_compute_many_and_submit_take_keyword_arguments() -> None:
    engine = Engine()
    calls: list[Any] = []

    @engine.query
    def q(x: Any, y: int = 0) -> str:
        calls.append((x, y))
        return _repr_type(x) + str(y)

    try:
        assert engine.compute_many(
            [(q, (1,)), (q, (True,)), (q, (), {"x": 1.0, "y": 2})]
        ) == [
            "int:10",
            "bool:True0",
            "float:1.02",
        ]
        assert engine.submit(q, x=1, y=0).result() == "int:10"
        assert engine.submit(q, True).result() == "bool:True0"
        stream = sorted(engine.compute_many_stream([(q, (), {"x": 2})]))
        assert [value for _, value, _ in stream] == ["int:20"]
        assert len(calls) == 4
    finally:
        engine.shutdown()


def test_traces_and_graph_show_the_arguments_as_written() -> None:
    engine = Engine()

    @engine.query
    def q(x: Any, *, k: int = 0) -> int:
        return 1

    q(1.5, k=2)
    q(True)
    nodes = engine.inspect_graph()["nodes"]
    assert f"query:{q.id}(1.5, **{{'k': 2}})" in nodes
    assert f"query:{q.id}(True, **{{'k': 0}})" in nodes


def test_prune_roots_are_keyed_like_calls() -> None:
    engine = Engine()

    @engine.query
    def q(x: Any, y: int = 0) -> int:
        return 1

    q(1.0)
    q(1)
    engine.prune([("query", q.id, (1.0,))])
    assert len(engine._internals.memos) == 1  # noqa: SLF001


# --- properties --------------------------------------------------------------

hashable_values = st.recursive(
    hashable_scalars,
    lambda children: st.one_of(
        st.lists(children, max_size=3).map(tuple),
        st.frozensets(hashable_scalars, max_size=3),
        st.builds(Pair, children, children),
        st.builds(Frozen, children, children),
    ),
    max_leaves=8,
)


def _has_nan(value: Any) -> bool:
    if isinstance(value, float):
        return math.isnan(value)
    if isinstance(value, complex):
        return math.isnan(value.real) or math.isnan(value.imag)
    if isinstance(value, decimal.Decimal):
        return value.is_nan()
    if isinstance(value, (tuple, frozenset)):
        return any(_has_nan(x) for x in value)
    if isinstance(value, Frozen):
        return _has_nan(value.a) or _has_nan(value.b)
    return False


@settings(max_examples=300, deadline=None)
@given(hashable_values, hashable_values)
def test_equal_keys_only_for_arguments_no_body_can_tell_apart(a: Any, b: Any) -> None:
    if _keys.key_args((a,)) == _keys.key_args((b,)):
        assert same_value(a, b), (a, b)


@settings(max_examples=300, deadline=None)
@given(
    st.lists(hashable_values, max_size=3).map(tuple),
    st.dictionaries(st.sampled_from(["k", "m"]), hashable_values, max_size=2),
)
def test_keys_survive_the_trip_through_the_disk_codec(
    args: tuple[Any, ...], kwargs: dict[str, Any]
) -> None:
    key = _keys.key_args(args) + ((_keys.KwArgs(kwargs),) if kwargs else ())
    plain = _keys.plain_args(key)
    decoded = canonical.decode(canonical.encode(plain))
    if not _has_nan((args, tuple(kwargs.values()))):
        assert _keys.key_from_plain(decoded) == key
    positional, keywords = _keys.call_parts(key)
    assert same_value(positional, args)
    assert same_value(keywords, dict(sorted(kwargs.items())))


def test_call_passes_the_original_objects() -> None:
    marker = collections.namedtuple("marker", "v")(1.5)
    key = _keys.key_args((1.5, True, marker, -0.0)) + (_keys.KwArgs({"k": 2.0}),)
    got: list[Any] = []
    _keys.call(lambda *a, **kw: got.append((a, kw)), key)
    ((args, kwargs),) = got
    assert args[2] is marker
    assert same_value(args, (1.5, True, marker, -0.0)) and kwargs == {"k": 2.0}
