from __future__ import annotations

import datetime
import math
from dataclasses import dataclass
from typing import NamedTuple

import pytest

from cascade._serde import _to_jsonable as to_j
from cascade._serde import stable_value_digest


@dataclass(frozen=True)
class _SampleDC:
    a: int
    b: str


class _SampleNT(NamedTuple):
    x: int
    y: int


def test_stable_digest_deterministic_for_collections_and_dataclass() -> None:
    # A dict's insertion order is part of its value; a set has no order.
    assert stable_value_digest({"z": 1, "a": 2}) == stable_value_digest(
        {"z": 1, "a": 2}
    )
    assert stable_value_digest({"z": 1, "a": 2}) != stable_value_digest(
        {"a": 2, "z": 1}
    )
    assert stable_value_digest(_SampleDC(1, "x")) == stable_value_digest(
        _SampleDC(1, "x")
    )
    assert stable_value_digest(frozenset((3, 1, 2))) == stable_value_digest(
        frozenset((1, 2, 3))
    )
    assert stable_value_digest({3, 1, 2}) == stable_value_digest({1, 2, 3})


def test_stable_digest_float_specials_and_bytearray() -> None:
    h_nan = stable_value_digest(float("nan"))
    assert h_nan == stable_value_digest(math.copysign(float("nan"), 1.0))
    assert stable_value_digest(float("inf")) != stable_value_digest(float("-inf"))
    # A bytearray round-trips as itself (disk format 4), so it is not bytes.
    assert stable_value_digest(bytearray(b"ab")) != stable_value_digest(b"ab")


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ((1, 2), [1, 2]),
        ((3, 4), _SampleNT(3, 4)),
        ({1, 2}, frozenset((1, 2))),
        (_SampleDC(1, "x"), _SampleDC(2, "x")),
        (1, 1.0),
        (1, "1"),
        (b"ab", "ab"),
        (ValueError("x"), KeyError("x")),
        (datetime.date(2024, 1, 2), datetime.datetime(2024, 1, 2)),
    ],
)
def test_stable_digest_distinguishes_types_and_values(
    left: object, right: object
) -> None:
    assert stable_value_digest(left) != stable_value_digest(right)


def test_to_jsonable_rejects_arbitrary_object() -> None:
    class PlainClass:
        pass

    with pytest.raises(TypeError, match="unsupported type"):
        to_j(PlainClass())
