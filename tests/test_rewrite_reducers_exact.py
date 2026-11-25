"""Rewritten reducers give exactly what the builtins give (D7, G22).

Example tests for each way the reducers used to differ, and for which of
them still update incrementally afterwards. The broad check is the
Hypothesis comparison in ``test_rewrite_differential.py``.
"""

from __future__ import annotations

import math
from fractions import Fraction
from typing import Any

import pytest

from cascade import CascadeDict, CascadeList, CascadeSet, Engine


@pytest.fixture
def engine() -> Engine:
    return Engine()


class _Calls:
    """A stage function that records what it was called with."""

    def __init__(self, fn: Any = None) -> None:
        self.seen: list[Any] = []
        self.fn = fn or (lambda v: v)

    def __call__(self, value: Any) -> Any:
        self.seen.append(value)
        return self.fn(value)


# --- sum ---------------------------------------------------------------------


def test_float_sum_equals_the_builtin_through_updates(engine: Engine) -> None:
    values = [0.1] * 10
    xs = CascadeList(engine, values)

    @engine.query
    def total() -> float:
        return sum(x for x in xs)

    assert total() == sum(values) == 1.0
    for v in (0.2, 1e16, 0.3, -1e16, 1e-9):
        xs.append(v)
        values.append(v)
        assert total() == sum(values)
    for index in (0, 5, -1, 3):
        del xs[index]
        del values[index]
        assert total() == sum(values)
    xs[2] = 7.7
    values[2] = 7.7
    assert total() == sum(values)
    xs.insert(0, 1e16)
    values.insert(0, 1e16)
    assert total() == sum(values)


def test_float_sum_refolds_without_running_the_stage_again(engine: Engine) -> None:
    xs = CascadeList(engine, [0.1, 0.2, 0.3])
    half = _Calls(lambda v: v / 2)

    @engine.query
    def total() -> float:
        return sum(half(x) for x in xs)

    assert total() == sum(v / 2 for v in [0.1, 0.2, 0.3])
    half.seen.clear()
    xs.append(0.4)
    assert total() == sum(v / 2 for v in [0.1, 0.2, 0.3, 0.4])
    assert half.seen == [0.4]


def test_int_sum_goes_back_to_a_running_total_once_floats_are_gone(
    engine: Engine,
) -> None:
    xs = CascadeList(engine, [1, 2, 0.5])

    @engine.query
    def total() -> Any:
        return sum(x for x in xs)

    assert total() == 3.5
    xs.remove(0.5)
    assert total() == 3 and type(total()) is int
    xs.append(True)
    assert total() == 4 and type(total()) is int
    xs.append(2**70)
    assert total() == 4 + 2**70


def test_sum_of_nan_inf_and_negative_zero(engine: Engine) -> None:
    xs = CascadeList(engine, [-0.0])

    @engine.query
    def total() -> float:
        return sum(x for x in xs)

    assert math.copysign(1.0, total()) == math.copysign(1.0, sum([-0.0]))
    xs.append(float("inf"))
    assert total() == math.inf
    xs.append(float("-inf"))
    assert math.isnan(total())
    xs.pop()
    assert total() == math.inf
    xs.pop()
    assert math.copysign(1.0, total()) == math.copysign(1.0, sum([-0.0]))


def test_sum_error_is_the_builtins_and_the_first_in_list_order(engine: Engine) -> None:
    xs = CascadeList(engine, [1, 2])

    @engine.query(cache_exceptions=False)
    def total() -> Any:
        return sum(x for x in xs)

    assert total() == 3
    # Diff order is None then "a"; list order is "a" then None.
    xs.append(None)
    xs.insert(0, "a")
    with pytest.raises(TypeError) as got:
        total()
    with pytest.raises(TypeError) as want:
        sum(x for x in ["a", 1, 2, None])
    assert str(got.value) == str(want.value)
    del xs[0]
    with pytest.raises(TypeError, match="'int' and 'NoneType'"):
        total()
    xs.pop()
    assert total() == 3


def test_sum_of_a_type_that_cannot_be_subtracted(engine: Engine) -> None:
    xs = CascadeList(engine, [[1], [2]])

    @engine.query(cache_exceptions=False)
    def total() -> Any:
        return sum(x for x in xs)

    with pytest.raises(TypeError, match="unsupported operand"):
        total()
    xs.clear()
    assert total() == 0


# --- any / all -----------------------------------------------------------------


def test_any_stops_at_the_first_truthy_element(engine: Engine) -> None:
    zs = CascadeList(engine, [1, 0])

    @engine.query(cache_exceptions=False)
    def any_reciprocal() -> bool:
        return any(1 / z for z in zs)

    assert any_reciprocal() is True
    zs.append(5)
    assert any_reciprocal() is True
    zs[0] = 0  # now the first element raises
    with pytest.raises(ZeroDivisionError):
        any_reciprocal()
    zs.insert(0, 2)
    assert any_reciprocal() is True
    del zs[0]
    del zs[0]
    with pytest.raises(ZeroDivisionError):
        any_reciprocal()
    del zs[0]
    assert any_reciprocal() is True
    zs.clear()
    assert any_reciprocal() is False


def test_all_stops_at_the_first_falsy_element(engine: Engine) -> None:
    zs = CascadeList(engine, [0, "x"])

    @engine.query(cache_exceptions=False)
    def all_small() -> bool:
        return all(z < 5 and z for z in zs)

    assert all_small() is False  # never compares "x" with 5
    zs[0] = 1
    with pytest.raises(TypeError):
        all_small()
    zs.pop()
    assert all_small() is True
    zs.clear()
    assert all_small() is True


def test_any_stays_incremental_when_nothing_raises(engine: Engine) -> None:
    xs = CascadeList(engine, list(range(100)))
    negative = _Calls(lambda v: v < 0)

    @engine.query
    def any_negative() -> bool:
        return any(negative(x) for x in xs)

    assert any_negative() is False
    negative.seen.clear()
    xs.append(-1)
    assert any_negative() is True
    assert negative.seen == [-1]


def test_pipeline_survives_an_element_that_raises(engine: Engine) -> None:
    xs = CascadeList(engine, list(range(1, 101)))
    recip = _Calls(lambda v: 1 / v)

    @engine.query(cache_exceptions=False)
    def total() -> float:
        return sum(recip(x) for x in xs)

    @engine.query(cache_exceptions=False)
    def some() -> bool:
        return any(recip(x) > 0.5 for x in xs)

    values = list(range(1, 101))
    assert total() == sum(1 / v for v in values)
    assert some() is True
    recip.seen.clear()

    xs.append(0)  # raises in the stage
    with pytest.raises(ZeroDivisionError):
        total()
    assert some() is True  # Python stops at the first element
    # Each pipeline tried the new element once. Then plain evaluation ran:
    # total() went through every element, some() through the first only.
    assert recip.seen.count(0) == 3 and recip.seen.count(1) == 2
    assert recip.seen.count(2) == 1

    xs.pop()  # the offender is gone: the stored results are in use again
    recip.seen.clear()
    assert total() == sum(1 / v for v in values)
    assert some() is True
    assert recip.seen == []
    xs.append(4)
    values.append(4)
    assert total() == sum(1 / v for v in values)
    assert recip.seen == [4]

    xs[0] = 0  # replaced in place by one that raises, then by one that works
    with pytest.raises(ZeroDivisionError):
        total()
    with pytest.raises(ZeroDivisionError):
        some()
    xs[0] = 8
    values[0] = 8
    recip.seen.clear()
    assert total() == sum(1 / v for v in values)
    assert some() is False
    assert recip.seen == [8, 8]


def test_any_with_a_truth_value_that_raises(engine: Engine) -> None:
    class Moody:
        def __bool__(self) -> bool:
            raise ValueError("no truth")

    moody = Moody()
    xs: CascadeList = CascadeList(engine, [0])

    @engine.query(cache_exceptions=False)
    def flag() -> int:
        return 1 if any(x for x in xs) else 0

    assert flag() == 0
    xs.append(moody)
    with pytest.raises(ValueError, match="no truth"):
        flag()
    xs.insert(0, 1)
    assert flag() == 1


# --- min / max -----------------------------------------------------------------


def _same(a: Any, b: Any) -> bool:
    return type(a) is type(b) and repr(a) == repr(b)


def test_min_and_max_return_the_first_of_equal_elements(engine: Engine) -> None:
    values: list[Any] = [1, True, 1.0]
    xs = CascadeList(engine, values)

    @engine.query
    def lo() -> Any:
        return min(x for x in xs)

    @engine.query
    def hi() -> Any:
        return max(x for x in xs)

    @engine.query
    def lo_reversed() -> Any:
        return min(reversed(xs))

    @engine.query
    def hi_reversed() -> Any:
        return max(reversed(xs))

    def check() -> None:
        assert _same(lo(), min(values))
        assert _same(hi(), max(values))
        assert _same(lo_reversed(), min(reversed(values)))
        assert _same(hi_reversed(), max(reversed(values)))

    check()
    for op in (
        lambda c: c.insert(0, True),
        lambda c: c.insert(0, 1.0),
        lambda c: c.append(1),
        lambda c: c.__setitem__(0, 1),
        lambda c: c.pop(0),
        lambda c: c.insert(2, 0.0),
        lambda c: c.insert(1, False),
        lambda c: c.append(-0.0),
        lambda c: c.reverse(),
        lambda c: c.sort(),
        lambda c: c.__delitem__(slice(0, 2)),
    ):
        op(xs)
        op(values)
        check()


def test_tie_formed_by_changing_an_inserted_element_in_place(engine: Engine) -> None:
    # The inserted 0 ties with nothing; replaced by True it ties with the 1
    # behind it, and it is the first of the two.
    values: list[Any] = [1]
    xs = CascadeList(engine, values)

    @engine.query
    def lo() -> Any:
        return min(x for x in xs)

    @engine.query
    def hi() -> Any:
        return max(x for x in xs)

    @engine.query
    def ordered() -> list[Any]:
        return sorted((x for x in xs), reverse=True)

    def check() -> None:
        assert _same(lo(), min(values))
        assert _same(hi(), max(values))
        assert repr(ordered()) == repr(sorted(values, reverse=True))

    check()
    for op in (
        lambda c: c.insert(0, 0),
        lambda c: c.__setitem__(0, True),
        lambda c: c.append(1.0),
        lambda c: c.insert(1, 2),
        lambda c: c.__setitem__(1, 1),
        lambda c: c.append(True),
        lambda c: c.__setitem__(0, 1.0),
        lambda c: c.insert(0, 1),
        lambda c: c.append(1),
        lambda c: c.insert(3, True),
        lambda c: c.append(1.0),
    ):
        op(xs)
        op(values)
        check()


def test_min_and_max_with_nan_follow_the_builtin_scan(engine: Engine) -> None:
    nan = float("nan")
    values: list[Any] = [nan, 1.0, 0.0]
    xs = CascadeList(engine, values)

    @engine.query
    def lo() -> Any:
        return min(x for x in xs)

    @engine.query
    def hi() -> Any:
        return max(x for x in xs)

    def check() -> None:
        assert _same(lo(), min(values))
        assert _same(hi(), max(values))

    check()  # nan comes first, so it wins both
    for op in (
        lambda c: c.append(5.0),
        lambda c: c.pop(0),  # the nan is gone: an ordinary min again
        lambda c: c.append(nan),
        lambda c: c.insert(0, 9.0),
        lambda c: c.insert(0, nan),
    ):
        op(xs)
        op(values)
        check()


def test_min_of_values_that_do_not_compare(engine: Engine) -> None:
    xs: CascadeList = CascadeList(engine, [3, 1])

    @engine.query(cache_exceptions=False)
    def lo() -> Any:
        return min(x for x in xs)

    assert lo() == 1
    xs.append("a")
    with pytest.raises(TypeError) as got:
        lo()
    with pytest.raises(TypeError) as want:
        min(x for x in [3, 1, "a"])
    assert str(got.value) == str(want.value)
    xs.pop()
    assert lo() == 1
    xs.clear()
    with pytest.raises(ValueError) as got_empty:
        lo()
    with pytest.raises(ValueError) as want_empty:
        min(x for x in [])
    assert str(got_empty.value) == str(want_empty.value)


def test_min_of_partially_ordered_values(engine: Engine) -> None:
    # Sets are ordered by inclusion: most pairs are neither < nor >.
    values = [frozenset({1}), frozenset({2}), frozenset()]
    xs = CascadeList(engine, values)

    @engine.query
    def lo() -> Any:
        return min(x for x in xs)

    @engine.query
    def hi() -> Any:
        return max(x for x in xs)

    assert lo() == min(values) and hi() == max(values)
    xs.insert(0, frozenset({2, 3}))
    values.insert(0, frozenset({2, 3}))
    assert lo() == min(values) and hi() == max(values)


def test_min_stays_incremental_for_ordinary_numbers(engine: Engine) -> None:
    xs = CascadeList(engine, list(range(1, 200)))
    seen = _Calls()

    @engine.query
    def lo() -> int:
        return min(seen(x) for x in xs)

    assert lo() == 1
    seen.seen.clear()
    xs.append(0)
    assert lo() == 0
    xs.insert(0, 0)  # a tie, inserted at the front
    assert lo() == 0
    xs.remove(0)
    xs.remove(0)
    assert lo() == 1
    assert seen.seen == [0, 0]


def test_min_max_with_default_and_key_match_the_builtin(engine: Engine) -> None:
    xs = CascadeList(engine)
    pairs = CascadeList(engine, [(1, "b"), (1, "a")])

    @engine.query
    def lo_default() -> Any:
        return min((x for x in xs), default="none")

    @engine.query
    def hi_key() -> Any:
        return max(pairs, key=lambda p: p[0])

    @engine.query
    def total_start() -> Any:
        return sum((x for x in xs), 0.5)

    assert lo_default() == "none"
    assert total_start() == 0.5
    assert hi_key() == (1, "b")
    xs.append(2)
    pairs.insert(0, (1, "c"))
    assert lo_default() == 2
    assert total_start() == 2.5
    assert hi_key() == (1, "c")


# --- sorted --------------------------------------------------------------------


def test_sorted_is_stable_in_both_directions_through_updates(engine: Engine) -> None:
    values = [(1, "a"), (1, "b"), (0, "c")]
    pairs = CascadeList(engine, values)

    def first(p: Any) -> Any:
        return p[0]

    @engine.query
    def ascending() -> list[Any]:
        return sorted((p for p in pairs), key=first)

    @engine.query
    def descending() -> list[Any]:
        return sorted((p for p in pairs), key=first, reverse=True)

    @engine.query
    def reversed_descending() -> list[Any]:
        return sorted(reversed(pairs), key=first, reverse=True)

    def check() -> None:
        assert ascending() == sorted(values, key=first)
        assert descending() == sorted(values, key=first, reverse=True)
        assert reversed_descending() == sorted(
            reversed(values), key=first, reverse=True
        )

    check()
    assert descending()[:2] == [(1, "a"), (1, "b")]
    for op in (
        lambda c: c.append((1, "d")),
        lambda c: c.insert(0, (1, "e")),
        lambda c: c.insert(2, (0, "f")),
        lambda c: c.__setitem__(1, (1.0, "g")),
        lambda c: c.__setitem__(0, (0, "h")),
        lambda c: c.pop(3),
        lambda c: c.reverse(),
        lambda c: c.extend([(True, "i"), (False, "j")]),
        lambda c: c.__setitem__(slice(1, 3), [(1, "k")]),
    ):
        op(pairs)
        op(values)
        check()


def test_ties_stay_in_position_order_after_a_group_was_reordered(
    engine: Engine,
) -> None:
    # Sorting out one group of equal keys must not leave its members with
    # ranks that are wrong against elements that join the group later.
    # Enough other keys that one group is repaired on its own.
    values = [(1, "a"), (2, "b")] + [(k, "z") for k in range(10, 60)]
    pairs = CascadeList(engine, values)

    def first(p: Any) -> Any:
        return p[0]

    @engine.query
    def ordered() -> list[Any]:
        return sorted((p for p in pairs), key=first)

    @engine.query
    def lowest() -> Any:
        return min(first(p) for p in pairs)

    def check() -> None:
        assert ordered() == sorted(values, key=first)
        assert lowest() == min(first(p) for p in values)

    check()
    for op in (
        lambda c: c.insert(0, (1, "f")),  # ties with "a", in front of it
        lambda c: c.pop(0),  # and leaves again
        lambda c: c.__setitem__(1, (1, "b")),  # "b" joins the group, behind "a"
        lambda c: c.append((1, "c")),
        lambda c: c.insert(1, (1, "d")),
        lambda c: c.__setitem__(0, (1, "e")),
    ):
        op(pairs)
        op(values)
        check()
        assert ordered() == sorted(values, key=first)


def test_sorted_runs_the_key_once_per_changed_element(engine: Engine) -> None:
    xs = CascadeList(engine, [3, 1, 2] * 50)
    key = _Calls(lambda v: v % 2)

    @engine.query
    def ordered() -> list[int]:
        return sorted((x for x in xs), key=key)

    values = [3, 1, 2] * 50
    assert ordered() == sorted(values, key=lambda v: v % 2)
    key.seen.clear()
    xs.insert(0, 7)  # ties with every odd element, and is not at the end
    values.insert(0, 7)
    assert ordered() == sorted(values, key=lambda v: v % 2)
    xs.append(8)
    values.append(8)
    assert ordered() == sorted(values, key=lambda v: v % 2)
    assert key.seen == [7, 8]


def test_tie_that_is_gone_before_the_next_run_needs_no_reordering(
    engine: Engine,
) -> None:
    # An element inserted in front of its equal makes a tie whose order has
    # to be looked up in the list. Here the other one leaves before the query
    # runs again, so there is nothing to look up, and nothing to run again.
    values = [(k, "old") for k in range(20)]
    pairs = CascadeList(engine, values)
    key = _Calls(lambda p: p[0])

    @engine.query
    def ordered() -> list[Any]:
        return sorted(pairs, key=key)

    def check() -> None:
        assert ordered() == sorted(values, key=lambda p: p[0])

    check()
    key.seen.clear()
    for op in (
        lambda c: c.insert(0, (5, "new")),
        lambda c: c.remove((5, "old")),
    ):
        op(pairs)
        op(values)
    check()
    pairs.append((5, "last"))  # a tie again, behind the inserted one
    values.append((5, "last"))
    check()
    assert key.seen == [(5, "new"), (5, "last")]


def test_bytes_keys_are_ordered_like_strings_and_apart_from_them(
    engine: Engine,
) -> None:
    values: list[Any] = [b"pear", b"apple", b"fig"]
    xs = CascadeList(engine, values)
    seen = _Calls()

    @engine.query(cache_exceptions=False)
    def ordered() -> list[Any]:
        return sorted(seen(x) for x in xs)

    @engine.query(cache_exceptions=False)
    def lowest() -> Any:
        return min(x for x in xs)

    def check() -> None:
        assert ordered() == sorted(values)
        assert lowest() == min(values)

    check()
    seen.seen.clear()
    for op in (
        lambda c: c.append(b"date"),
        lambda c: c.insert(0, b"fig"),  # a tie, in front
        lambda c: c.remove(b"apple"),
    ):
        op(xs)
        op(values)
        check()
    assert seen.seen == [b"date", b"fig"]
    # Bytes have a total order, so every key is in the reducers' sorted
    # structure. (With keys outside it the results above are the same, by
    # way of the builtin over all stored keys; only this tells the two apart.)
    states = engine._incremental_rt._states.values()  # noqa: SLF001
    assert [state.reducer.outside for state in states] == [0, 0]

    xs.append("fig")  # bytes and str do not compare
    for query, builtin in ((ordered, sorted), (lowest, min)):
        with pytest.raises(TypeError) as got:
            query()
        with pytest.raises(TypeError) as want:
            builtin([*values, "fig"])
        assert str(got.value) == str(want.value)
    xs.pop()
    check()


def test_sorted_with_keys_that_have_no_total_order(engine: Engine) -> None:
    nan = float("nan")
    values: list[Any] = [2.0, nan, 1.0, nan, 0.5]
    xs = CascadeList(engine, values)

    @engine.query(cache_exceptions=False)
    def ordered() -> list[Any]:
        return sorted(x for x in xs)

    def check() -> None:
        assert repr(ordered()) == repr(sorted(values))

    check()
    xs.append(0.1)
    values.append(0.1)
    check()
    xs.insert(0, 3.0)
    values.insert(0, 3.0)
    check()
    xs.append("s")
    with pytest.raises(TypeError) as got:
        ordered()
    with pytest.raises(TypeError) as want:
        sorted([*values, "s"])
    assert str(got.value) == str(want.value)
    xs.pop()
    check()


def test_sorted_with_unusual_arguments_is_left_to_the_builtin(engine: Engine) -> None:
    xs = CascadeList(engine, [(1, "a"), (2, "b"), (1, "c")])
    plain = [(1, "a"), (2, "b"), (1, "c")]

    def first(p: Any) -> Any:
        return p[0]

    @engine.query(cache_exceptions=False)
    def odd_reverse() -> list[Any]:
        return sorted((x for x in xs), key=first, reverse="yes")  # type: ignore[call-overload]

    @engine.query(cache_exceptions=False)
    def bad_key() -> list[Any]:
        return sorted((x for x in xs), key=5)  # type: ignore[call-overload]

    def outcome(fn: Any) -> Any:
        try:
            return fn()
        except TypeError as exc:
            return str(exc)

    want_reverse = outcome(lambda: sorted(plain, key=first, reverse="yes"))  # type: ignore[call-overload]
    assert outcome(odd_reverse) == want_reverse
    with pytest.raises(TypeError) as got:
        bad_key()
    with pytest.raises(TypeError) as want:
        sorted(plain, key=5)  # type: ignore[call-overload]
    assert str(got.value) == str(want.value)


def test_sorted_direction_taken_from_a_value_that_changes(engine: Engine) -> None:
    xs = CascadeList(engine, [(1, "a"), (1, "b"), (2, "c")])
    descending = engine.input(lambda: False, name="descending")

    @engine.query
    def ordered() -> list[Any]:
        return sorted((x for x in xs), key=lambda p: p[0], reverse=descending())

    assert ordered() == [(1, "a"), (1, "b"), (2, "c")]
    descending.set(True)
    assert ordered() == [(2, "c"), (1, "a"), (1, "b")]
    xs.append((2, "d"))
    assert ordered() == [(2, "c"), (2, "d"), (1, "a"), (1, "b")]
    descending.set(False)
    assert ordered() == [(1, "a"), (1, "b"), (2, "c"), (2, "d")]


# --- len -----------------------------------------------------------------------


@pytest.mark.parametrize("which", ["map", "filter", "reversed", "generator"])
def test_len_of_an_iterator_is_a_type_error(engine: Engine, which: str) -> None:
    xs = CascadeList(engine, [1, 2])
    plain = [1, 2]

    if which == "map":

        @engine.query(cache_exceptions=False)
        def q() -> int:
            return len(map(str, xs))  # type: ignore[arg-type]

        def want() -> int:
            return len(map(str, plain))  # type: ignore[arg-type]

    elif which == "filter":

        @engine.query(cache_exceptions=False)
        def q() -> int:
            return len(filter(None, xs))  # type: ignore[arg-type]

        def want() -> int:
            return len(filter(None, plain))  # type: ignore[arg-type]

    elif which == "reversed":

        @engine.query(cache_exceptions=False)
        def q() -> int:
            return len(reversed(xs))  # type: ignore[arg-type]

        def want() -> int:
            return len(reversed(plain))  # type: ignore[arg-type]

    else:

        @engine.query(cache_exceptions=False)
        def q() -> int:
            return len(x for x in xs)  # type: ignore[arg-type]

        def want() -> int:
            return len(x for x in plain)  # type: ignore[arg-type]

    with pytest.raises(TypeError) as got:
        q()
    with pytest.raises(TypeError) as expected:
        want()
    assert str(got.value) == str(expected.value)


def test_len_of_an_iterator_over_a_plain_list_is_a_type_error(engine: Engine) -> None:
    plain = [1, 2]

    @engine.query(cache_exceptions=False)
    def q() -> int:
        return len(map(str, plain))  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="has no len"):
        q()


def test_len_of_a_list_comprehension_stays_incremental(engine: Engine) -> None:
    xs = CascadeList(engine, list(range(50)))
    even = _Calls(lambda v: v % 2 == 0)

    @engine.query
    def n() -> int:
        return len([x for x in xs if even(x)])

    assert n() == 25
    even.seen.clear()
    xs.append(50)
    assert n() == 26
    assert even.seen == [50]


# --- set / dict / join ---------------------------------------------------------


def test_set_and_dict_keep_the_first_of_equal_values(engine: Engine) -> None:
    values: list[Any] = [1, True, 1.0]
    xs = CascadeList(engine, values)

    @engine.query
    def as_set() -> set[Any]:
        return {x for x in xs}

    @engine.query
    def as_dict() -> dict[Any, Any]:
        return {x: x for x in xs}

    def check() -> None:
        assert repr(as_set()) == repr({x for x in values})
        assert repr(as_dict()) == repr({x: x for x in values})

    check()
    for op in (
        lambda c: c.insert(0, True),
        lambda c: c.__setitem__(0, 1.0),
        lambda c: c.pop(0),
        lambda c: c.reverse(),
    ):
        op(xs)
        op(values)
        check()


def test_unhashable_and_malformed_elements_raise_the_builtins_errors(
    engine: Engine,
) -> None:
    xs: CascadeList = CascadeList(engine, [1])

    @engine.query(cache_exceptions=False)
    def as_set() -> set[Any]:
        return {x for x in xs}

    @engine.query(cache_exceptions=False)
    def as_dict() -> dict[Any, Any]:
        return dict(x for x in xs)

    @engine.query(cache_exceptions=False)
    def joined() -> str:
        return ",".join(x for x in xs)

    plain: list[Any] = [1]
    for query, fn in (
        (as_dict, lambda: dict(x for x in plain)),
        (joined, lambda: ",".join(x for x in plain)),
    ):
        with pytest.raises(TypeError) as got:
            query()
        with pytest.raises(TypeError) as want:
            fn()
        assert str(got.value) == str(want.value)
    xs.append([2])
    plain.append([2])
    with pytest.raises(TypeError) as got:
        as_set()
    with pytest.raises(TypeError) as want:
        {x for x in plain}  # noqa: B018
    assert str(got.value) == str(want.value)


# --- evaluation order ----------------------------------------------------------


def test_function_expressions_are_evaluated_before_the_source(engine: Engine) -> None:
    order: list[str] = []
    xs = CascadeList(engine, [1, 2, 3])

    def note(name: str, value: Any) -> Any:
        order.append(name)
        return value

    def plain() -> list[int]:
        return sorted(
            filter(note("filter fn", bool), map(note("map fn", abs), note("src", ys))),
            key=note("key", None),
            reverse=note("reverse", False),
        )

    @engine.query
    def q() -> list[int]:
        return sorted(
            filter(note("filter fn", bool), map(note("map fn", abs), note("src", xs))),
            key=note("key", None),
            reverse=note("reverse", False),
        )

    ys = [1, 2, 3]
    assert plain() == [1, 2, 3]
    expected, order[:] = list(order), []
    assert q() == [1, 2, 3]
    assert order == expected == ["filter fn", "map fn", "src", "key", "reverse"]


def test_the_first_missing_name_is_the_one_reported(engine: Engine) -> None:
    @engine.query(cache_exceptions=False)
    def q() -> int:
        return sum(map(no_such_function, no_such_source))  # type: ignore[name-defined]  # noqa: F821

    with pytest.raises(NameError, match="no_such_function"):
        q()


# --- other sources -------------------------------------------------------------


def test_order_dependent_reducers_over_a_dict(engine: Engine) -> None:
    values: dict[str, Any] = {"a": 1, "b": True, "c": 0.5}
    d = CascadeDict(engine, values)

    @engine.query
    def lowest() -> Any:
        return min(d.values())

    @engine.query
    def by_value() -> list[Any]:
        return sorted(d.items(), key=lambda kv: kv[1], reverse=True)

    @engine.query
    def total() -> Any:
        return sum(v for v in d.values())

    def check() -> None:
        assert _same(lowest(), min(values.values()))
        assert by_value() == sorted(values.items(), key=lambda kv: kv[1], reverse=True)
        assert _same(total(), sum(values.values()))

    check()
    for key, value in (("a", True), ("d", 1.0), ("b", 0.5), ("c", 1)):
        d[key] = value
        values[key] = value
        check()
    del d["a"]
    del values["a"]
    check()
    d["a"] = 0.5  # back, now at the end
    values["a"] = 0.5
    check()


def test_set_source_is_visited_in_insertion_order(engine: Engine) -> None:
    s = CascadeSet(engine, [30, 1, 20])

    @engine.query
    def halves() -> list[float]:
        return [x / 2 for x in s]

    @engine.query
    def total() -> float:
        return sum(x / 3 for x in s)

    assert halves() == [15.0, 0.5, 10.0]
    assert total() == sum(x / 3 for x in [30, 1, 20])
    s.discard(30)
    s.add(7)
    s.add(30)
    assert halves() == [0.5, 10.0, 3.5, 15.0]
    assert total() == sum(x / 3 for x in [1, 20, 7, 30])


# --- at an older revision ------------------------------------------------------
#
# A query run through a snapshot reduces the collection as it was then, while
# the collection itself has moved on. Where a result depends on the order of
# the elements, that is the order at the snapshot's revision.


def test_tie_order_through_a_snapshot_is_the_order_the_snapshot_saw(
    engine: Engine,
) -> None:
    values = [(k, "old") for k in range(20)]
    pairs = CascadeList(engine, values)

    def first(p: Any) -> Any:
        return p[0]

    @engine.query
    def ordered() -> list[Any]:
        return sorted(pairs, key=first)

    assert ordered() == sorted(values, key=first)
    pairs.insert(0, (5, "new"))  # ties with (5, "old"), in front of it
    values.insert(0, (5, "new"))
    snapshot = engine.snapshot()
    pairs.reverse()  # now behind it, which the snapshot does not see

    then = ordered(snapshot=snapshot)
    assert then == sorted(values, key=first)
    assert then[5:7] == [(5, "new"), (5, "old")]
    values.reverse()
    now = ordered()
    assert now == sorted(values, key=first)
    assert now[5:7] == [(5, "old"), (5, "new")]
    snapshot.release()


def test_set_through_a_snapshot_is_reduced_in_the_order_it_had_then(
    engine: Engine,
) -> None:
    # Fractions are sorted by the builtin over the elements in iteration
    # order; the elements, and their order, are the snapshot's.
    half, third, quarter = Fraction(1, 2), Fraction(1, 3), Fraction(1, 4)
    tags = CascadeSet(engine, [half, third])

    @engine.query
    def ordered() -> list[Fraction]:
        return sorted(tags)

    assert ordered() == [third, half]
    tags.add(Fraction(3, 4))
    snapshot = engine.snapshot()
    tags.discard(half)
    tags.add(quarter)
    assert ordered(snapshot=snapshot) == [third, half, Fraction(3, 4)]
    assert ordered() == [quarter, third, Fraction(3, 4)]
    snapshot.release()


def test_dict_through_a_snapshot_keeps_the_order_it_had_then(engine: Engine) -> None:
    d = CascadeDict(engine, {"a": 1, "b": -1, "c": 3})

    @engine.query
    def positives() -> list[int]:
        return [v for v in d.values() if v > 0]

    assert positives() == [1, 3]
    d["b"] = 2  # passes the filter now, in its old place between a and c
    snapshot = engine.snapshot()
    del d["a"]
    assert positives(snapshot=snapshot) == [1, 2, 3]
    assert positives() == [2, 3]
    snapshot.release()


def _first(pair: Any) -> Any:
    return pair[0]


def test_ordered_reducers_take_in_an_element_that_starts_to_pass_the_filter(
    engine: Engine,
) -> None:
    # An element the filter kept out has no place in the order yet. When it
    # is replaced by one that passes, with a key that is already there, it
    # has to land where the plain expression puts it: after its twin, since
    # it comes later in the list.
    xs = CascadeList(engine, [(1, "a"), (0, "out"), (2, "b"), (0, "out too")])

    @engine.query
    def kept() -> list[Any]:
        return sorted((p for p in xs if p[0] > 0), key=_first)

    @engine.query
    def smallest() -> Any:
        return min((p for p in xs if p[0] > 0), key=_first)

    @engine.query
    def largest() -> Any:
        return max((p for p in xs if p[0] > 0), key=_first)

    def plain() -> tuple[Any, Any, Any]:
        passing = [p for p in list(xs) if p[0] > 0]
        return (
            sorted(passing, key=_first),
            min(passing, key=_first),
            max(passing, key=_first),
        )

    assert kept.raw.__cascade_rewritten__
    assert (kept(), smallest(), largest()) == plain()
    xs[1] = (1, "z")
    assert kept() == [(1, "a"), (1, "z"), (2, "b")]
    assert (kept(), smallest(), largest()) == plain()
    xs[3] = (2, "y")
    assert kept() == [(1, "a"), (1, "z"), (2, "b"), (2, "y")]
    assert (kept(), smallest(), largest()) == plain()
    # And out again.
    xs[1] = (0, "out")
    assert (kept(), smallest(), largest()) == plain()
