"""Hypothesis properties of the two value codecs (G18, G19).

The msgpack codec (``_canonical``, used with ``cache_dir``) must decode every
supported value to one no query could tell from the original. Both it and
the JSON digest (``_serde``, used without ``cache_dir``) must give two
values the same bytes only if no query could tell them apart.
"""

from __future__ import annotations

import collections
import datetime
import decimal
import fractions
import pathlib
import uuid
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from cascade import _canonical as canonical
from cascade._serde import stable_value_digest

from .value_helpers import Box, Color, Level, Mode, Pair, Perm, Point, same_value

_offsets = st.integers(min_value=-23 * 60, max_value=23 * 60).map(
    lambda m: datetime.timezone(datetime.timedelta(minutes=m))
)
_tz = st.one_of(st.none(), _offsets)

hashable_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.integers(min_value=-(2**70), max_value=2**70),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(max_size=6),
    st.binary(max_size=6),
    st.complex_numbers(allow_nan=False, allow_infinity=True),
    # A signaling NaN cannot be hashed; it is a value, not a key.
    st.decimals(allow_nan=True, allow_infinity=True).filter(lambda d: not d.is_snan()),
    st.fractions(),
    st.uuids(),
    st.sampled_from(
        [
            Color.RED,
            Color.GREEN,
            Color.BLUE,
            Level.LOW,
            Level.HIGH,
            Perm.R,
            Perm.R | Perm.W,
            Perm.R | Perm.W | Perm.X,
            Perm(0),
            Mode.FAST,
        ]
    ),
    st.text(max_size=6).map(pathlib.PurePosixPath),
    st.text(alphabet="abcXYZ/\\:.", max_size=6).map(pathlib.PureWindowsPath),
    st.text(alphabet="abc/.", max_size=6).map(pathlib.Path),
    st.datetimes(timezones=_tz),
    st.dates(),
    st.times(timezones=_tz),
    st.timedeltas(),
    st.builds(
        range, st.integers(-5, 5), st.integers(-5, 5), st.sampled_from([1, 2, -1])
    ),
)

scalars = st.one_of(
    hashable_scalars,
    st.binary(max_size=6).map(bytearray),
    st.just(decimal.Decimal("sNaN")),
    st.just(decimal.Decimal("-sNaN1")),
)


def _containers(children: st.SearchStrategy[Any]) -> st.SearchStrategy[Any]:
    keys = st.one_of(hashable_scalars, st.tuples(hashable_scalars, hashable_scalars))
    return st.one_of(
        st.lists(children, max_size=4),
        st.lists(children, max_size=4).map(tuple),
        st.dictionaries(keys, children, max_size=4),
        st.dictionaries(st.text(max_size=3), children, max_size=4),
        st.frozensets(hashable_scalars, max_size=4),
        st.sets(hashable_scalars, max_size=4),
        st.builds(
            collections.deque,
            st.lists(children, max_size=4),
            st.one_of(st.none(), st.integers(4, 6)),
        ),
        st.builds(Pair, children, children),
        st.builds(Box, st.text(max_size=3), children),
        st.builds(Point, children, children),
        st.builds(ValueError, children),
    )


values = st.recursive(scalars, _containers, max_leaves=12)


@settings(max_examples=300, deadline=None)
@given(values)
def test_every_supported_value_decodes_to_the_same_value(value: Any) -> None:
    decoded = canonical.decode(canonical.encode(value))
    assert same_value(decoded, value), (value, decoded)
    # Decoding is stable: the decoded value encodes to the same bytes.
    assert canonical.encode(decoded) == canonical.encode(value)


@settings(max_examples=300, deadline=None)
@given(values, values)
def test_equal_encodings_mean_values_no_query_can_tell_apart(a: Any, b: Any) -> None:
    if canonical.encode(a) == canonical.encode(b):
        assert same_value(a, b), (a, b)
    if stable_value_digest(a) == stable_value_digest(b):
        assert same_value(a, b), (a, b)


@settings(max_examples=200, deadline=None)
@given(values)
def test_a_value_and_its_decoded_copy_have_one_digest(value: Any) -> None:
    decoded = canonical.decode(canonical.encode(value))
    assert canonical.value_digest(decoded) == canonical.value_digest(value)
    assert stable_value_digest(decoded) == stable_value_digest(value)


@settings(max_examples=200, deadline=None)
@given(st.lists(hashable_scalars, max_size=6), st.randoms())
def test_set_encoding_ignores_insertion_order(items: list[Any], rnd: Any) -> None:
    a = frozenset(items)
    shuffled = list(a)
    rnd.shuffle(shuffled)
    b = frozenset(shuffled)
    assert canonical.encode(a) == canonical.encode(b)
    assert stable_value_digest(a) == stable_value_digest(b)


# Look-alikes: equal under ``==`` (or nearly), but a query can tell them apart.
LOOK_ALIKES = [
    (decimal.Decimal("1.0"), decimal.Decimal("1.00")),
    (decimal.Decimal("0"), decimal.Decimal("-0")),
    (decimal.Decimal(1), 1),
    (fractions.Fraction(1, 2), 0.5),
    (complex(1, 0), 1.0),
    (complex(0.0, 0.0), complex(0.0, -0.0)),
    (Level.LOW, 1),
    (Mode.FAST, "fast"),
    (Perm.R | Perm.W, Perm(6)),  # one member: no difference expected
    (pathlib.PurePosixPath("a"), pathlib.PureWindowsPath("a")),
    (pathlib.PureWindowsPath("A"), pathlib.PureWindowsPath("a")),
    (bytearray(b"ab"), b"ab"),
    (collections.deque([1, 2]), [1, 2]),
    (collections.deque([1, 2], 3), collections.deque([1, 2])),
    (range(0, 3), (0, 1, 2)),
    (range(0), range(1, 1)),
    (uuid.UUID(int=1), uuid.UUID(int=1).bytes),
    (Point(1, 2), (1, 2)),
    (Point(1, 2.0), Point(1, 2)),
]


def test_look_alikes_get_different_encodings_and_digests() -> None:
    for a, b in LOOK_ALIKES:
        if same_value(a, b):
            assert canonical.encode(a) == canonical.encode(b), (a, b)
            continue
        assert canonical.encode(a) != canonical.encode(b), (a, b)
        assert stable_value_digest(a) != stable_value_digest(b), (a, b)
