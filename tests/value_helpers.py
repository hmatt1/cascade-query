"""Value types for the codec tests, and an exact comparison of decoded values.

``same_value(a, b)`` is true when no query could tell ``a`` and ``b`` apart by
their contents: same types all the way down, same items, dict order
included, floats down to the sign of zero, NaN equal to NaN. ``==`` is too
weak for that (``1 == 1.0 == True``, ``{"a": 1, "b": 2} == {"b": 2, "a": 1}``)
and too strong for NaN.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import math
import pathlib
import sys
from typing import Any, NamedTuple

from cascade import register_type


class Color(enum.Enum):
    RED = 1
    GREEN = "green"
    BLUE = (2, 3)


class Level(enum.IntEnum):
    LOW = 1
    HIGH = 2


class Perm(enum.Flag):
    R = 4
    W = 2
    X = 1


class Mode(str, enum.Enum):
    FAST = "fast"
    SLOW = "slow"


class Pair(NamedTuple):
    left: Any
    right: Any


@dataclasses.dataclass(frozen=True)
class Box:
    label: str
    payload: Any


class Point:
    """A plain class, made encodable with ``register_type``."""

    __slots__ = ("x", "y")

    def __init__(self, x: Any, y: Any) -> None:
        self.x = x
        self.y = y

    def __eq__(self, other: object) -> bool:
        return type(other) is Point and (self.x, self.y) == (other.x, other.y)

    def __hash__(self) -> int:
        return hash((self.x, self.y))

    def __repr__(self) -> str:
        return f"Point({self.x!r}, {self.y!r})"


register_type(
    Point,
    encode=lambda p: (p.x, p.y),
    decode=lambda fields: Point(*fields),
    name="tests.value_helpers:Point",
)


def same_value(a: Any, b: Any) -> bool:
    t = type(a)
    if t is not type(b):
        return False
    if t is float:
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return a == b and math.copysign(1.0, a) == math.copysign(1.0, b)
    if t is complex:
        return same_value(a.real, b.real) and same_value(a.imag, b.imag)
    if t is decimal.Decimal:
        return str(a) == str(b)
    if t is collections.deque:
        return a.maxlen == b.maxlen and _same_seq(list(a), list(b))
    if t is list or isinstance(a, tuple):
        return _same_seq(list(a), list(b))
    if isinstance(a, dict):
        return _same_seq(list(a.items()), list(b.items()))
    if isinstance(a, (set, frozenset)):
        if len(a) != len(b):
            return False
        rest = list(b)
        for x in a:
            for i, y in enumerate(rest):
                if same_value(x, y):
                    del rest[i]
                    break
            else:
                return False
        return True
    if dataclasses.is_dataclass(a):
        return all(
            same_value(getattr(a, f.name), getattr(b, f.name))
            for f in dataclasses.fields(a)
        )
    if t is Point:
        return same_value(a.x, b.x) and same_value(a.y, b.y)
    if isinstance(a, BaseException):
        return _same_seq(list(a.args), list(b.args))
    if t is range:
        # ``range(0) == range(1, 1)``, but their ``start`` differs.
        return (a.start, a.stop, a.step) == (b.start, b.stop, b.step)
    if isinstance(a, pathlib.PurePath):
        # ``==`` ignores case for Windows paths; a query can see it.
        return str(a) == str(b)
    if isinstance(a, (datetime.datetime, datetime.time)):
        return a == b and a.utcoffset() == b.utcoffset() and a.fold == b.fold
    np = sys.modules.get("numpy")
    if np is not None and t is np.ndarray:
        return a.dtype == b.dtype and a.shape == b.shape and a.tobytes() == b.tobytes()
    if np is not None and isinstance(a, np.generic):
        return a.dtype == b.dtype and a.tobytes() == b.tobytes()
    return bool(a == b)


def _same_seq(a: list[Any], b: list[Any]) -> bool:
    return len(a) == len(b) and all(same_value(x, y) for x, y in zip(a, b))
