"""Rewritten reducers against the builtins, after every mutation (D7, G22).

``_list_bodies`` and friends define each query body once, as a plain
function over a closure variable. Called with a cascade collection and an
engine, the functions are registered as queries (and rewritten); called with
a plain ``list`` or ``dict`` they are the reference. Hypothesis drives a
random sequence of mutations through both, and after each one every query
must give exactly what its plain twin gives: the same value down to types,
``-0.0`` and NaN, or the same exception with the same message.
"""

from __future__ import annotations

import math
from typing import Any, Callable

from hypothesis import given, settings
from hypothesis import strategies as st

from cascade import CascadeDict, CascadeList, CascadeSet, Engine


def _first(v: Any) -> Any:
    return v[0] if type(v) is tuple else v


def _second(v: Any) -> Any:
    return v[1]


def _zero(v: Any) -> int:
    return 0


def _recip(v: Any) -> Any:
    return 1 / v


def _list_bodies(xs: Any) -> list[Callable[[], Any]]:
    # --- sum
    def sum_gen():
        return sum(x for x in xs)

    def sum_listcomp():
        return sum([x for x in xs])

    def sum_bare():
        return sum(xs)

    def sum_reversed():
        return sum(reversed(xs))

    def sum_filtered():
        return sum(x for x in xs if x)

    def sum_mapped():
        return sum(map(_first, xs))

    def sum_start():
        return sum((x for x in xs), 10)

    def sum_float_start():
        return sum(xs, 0.5)

    # --- len
    def len_bare():
        return len(xs)

    def len_listcomp():
        return len([x for x in xs if x])

    def len_of_map():
        return len(map(str, xs))

    def len_of_filter():
        return len(filter(None, xs))

    def len_of_reversed():
        return len(reversed(xs))

    def len_of_generator():
        return len(x for x in xs)

    def len_list_filter():
        return len(list(filter(None, xs)))

    # --- min / max
    def min_bare():
        return min(xs)

    def max_bare():
        return max(xs)

    def min_gen():
        return min(x for x in xs)

    def max_gen():
        return max(x for x in xs)

    def min_reversed():
        return min(reversed(xs))

    def max_reversed():
        return max(reversed(xs))

    def min_mapped():
        return min(map(_first, xs))

    def max_filtered():
        return max(x for x in xs if x)

    def min_default():
        return min(xs, default=None)

    def max_default():
        return max((x for x in xs), default=0)

    def min_key():
        return min(xs, key=_first)

    def max_key_default():
        return max(xs, key=_first, default=None)

    # --- any / all
    def any_bare():
        return any(xs)

    def all_bare():
        return all(xs)

    def any_gen():
        return any(x for x in xs)

    def all_gen():
        return all(x for x in xs)

    def any_recip():
        return any(1 / x for x in xs)

    def all_recip():
        return all(1 / x for x in xs)

    def any_recip_reversed():
        return any(_recip(x) for x in reversed(xs))

    def any_recip_map():
        return any(map(_recip, xs))

    def all_filtered_recip():
        return all(1 / x for x in xs if x != 2)

    # --- sorted
    def sorted_bare():
        return sorted(xs)

    def sorted_desc():
        return sorted(xs, reverse=True)

    def sorted_key():
        return sorted((x for x in xs), key=_first)

    def sorted_key_desc():
        return sorted(xs, key=_first, reverse=True)

    def sorted_reversed_key():
        return sorted(reversed(xs), key=_first)

    def sorted_reversed_key_desc():
        return sorted(reversed(xs), key=_first, reverse=True)

    def sorted_all_ties():
        return sorted(xs, key=_zero)

    def sorted_all_ties_desc():
        return sorted(xs, key=_zero, reverse=True)

    def sorted_key_none():
        return sorted(xs, key=None)

    def sorted_filtered():
        return sorted([x for x in xs if x], key=_first)

    def sorted_reverse_int():
        return sorted(xs, reverse=1)

    def sorted_lambda_key():
        return sorted((x for x in xs), key=lambda v: _first(v), reverse=True)

    # --- list / set / dict / join
    def list_bare():
        return list(xs)

    def listcomp():
        return [x for x in xs]

    def listcomp_filtered():
        return [x for x in xs if x]

    def list_reversed():
        return list(reversed(xs))

    def list_mapped():
        return list(map(_first, xs))

    def set_bare():
        return set(xs)

    def setcomp():
        return {x for x in xs}

    def setcomp_mapped():
        return {_first(x) for x in xs}

    def set_reversed():
        return set(reversed(xs))

    def dictcomp():
        return {x: _first(x) for x in xs}

    def dict_of_pairs():
        return dict((x, x) for x in xs)

    def dict_bare():
        return dict(xs)

    def join_bare():
        return ",".join(xs)

    def join_str():
        return ",".join(str(x) for x in xs)

    def join_reversed():
        return ",".join(map(str, reversed(xs)))

    return [v for v in locals().values() if callable(v)]


# Bodies of _list_bodies that plain Python leaves alone (not rewritten).
_NOT_REWRITTEN = {
    "sum_start",
    "sum_float_start",
    "len_of_map",
    "len_of_filter",
    "len_of_reversed",
    "len_of_generator",
    "min_default",
    "max_default",
    "min_key",
    "max_key_default",
}


def _dict_bodies(d: Any) -> list[Callable[[], Any]]:
    def sum_values():
        return sum(d.values())

    def sum_values_gen():
        return sum(v for v in d.values())

    def min_keys():
        return min(d)

    def max_values():
        return max(d.values())

    def min_values_gen():
        return min(v for v in d.values())

    def min_values_reversed():
        return min(reversed(d.values()))

    def sorted_items_by_value():
        return sorted(d.items(), key=_second)

    def sorted_items_by_value_desc():
        return sorted(d.items(), key=_second, reverse=True)

    def sorted_values_desc():
        return sorted(d.values(), reverse=True)

    def sorted_keys_all_ties():
        return sorted(d, key=_zero, reverse=True)

    def dictcomp_items():
        return {k: v for k, v in d.items()}

    def dictcomp_swapped():
        return {v: k for k, v in d.items()}

    def keys_list():
        return [k for k in d]

    def values_list():
        return list(d.values())

    def reversed_keys():
        return list(reversed(d))

    def any_recip_values():
        return any(1 / v for v in d.values())

    def all_values():
        return all(d.values())

    def join_keys():
        return ",".join(str(k) for k in d)

    def set_values():
        return set(d.values())

    def len_truthy():
        return len([k for k, v in d.items() if v])

    def dict_bare():
        return dict(d)

    def dict_items():
        return dict(d.items())

    def len_bare():
        return len(d)

    return [v for v in locals().values() if callable(v)]


def _set_bodies(xs: Any) -> list[Callable[[], Any]]:
    # Rewritten reducers only: they visit a CascadeSet in insertion order.
    def sum_gen():
        return sum(x for x in xs)

    def len_filtered():
        return len([x for x in xs if x])

    def min_gen():
        return min(x for x in xs)

    def max_gen():
        return max(x for x in xs)

    def min_mapped():
        return min(map(_first, xs))

    def any_recip():
        return any(1 / x for x in xs)

    def all_gen():
        return all(x for x in xs)

    def sorted_key():
        return sorted((x for x in xs), key=_first)

    def sorted_all_ties_desc():
        return sorted((x for x in xs), key=_zero, reverse=True)

    def listcomp():
        return [x for x in xs]

    def setcomp_mapped():
        return {_first(x) for x in xs}

    def dictcomp():
        return {_first(x): x for x in xs}

    def join_str():
        return ",".join(str(x) for x in xs)

    return [v for v in locals().values() if callable(v)]


# --- exact comparison ---------------------------------------------------------


def _canon(value: Any) -> Any:
    """A value that compares equal only for results no code could tell apart."""
    kind = type(value)
    if kind is float:
        return ("float", repr(value), math.copysign(1.0, value))
    if kind in (list, tuple):
        return (kind.__name__, tuple(_canon(v) for v in value))
    if kind is set:
        return ("set", frozenset(_canon(v) for v in value))
    if kind is dict:
        return ("dict", tuple((_canon(k), _canon(v)) for k, v in value.items()))
    return (kind.__name__, value)


def _outcome(fn: Callable[[], Any]) -> Any:
    try:
        return ("value", _canon(fn()))
    except Exception as exc:  # noqa: BLE001
        return ("raises", type(exc).__name__, str(exc))


def _register(engine: Engine, bodies: list[Callable[[], Any]]) -> dict[str, Any]:
    return {fn.__name__: engine.query(fn, name=fn.__name__) for fn in bodies}


def _assert_same(queries: dict[str, Any], plain: list[Callable[[], Any]], state: Any):
    for fn in plain:
        want = _outcome(fn)
        got = _outcome(queries[fn.__name__])
        assert got == want, f"{fn.__name__} over {state!r}: {got} != {want}"


# --- strategies -----------------------------------------------------------------

_INTS = st.integers(-3, 3)
_BOOLS = st.booleans()
_FLOATS = st.one_of(
    st.sampled_from(
        [0.0, -0.0, 1.0, -1.0, 0.1, 0.2, 0.3, 2.5, 1e16, -1e16, 1e-16, 1e308]
        + [float("inf"), float("-inf"), float("nan")]
    ),
    st.floats(allow_nan=True, allow_infinity=True),
)
_STRS = st.sampled_from(["", "a", "b", "ab", "B", "0"])
_TUPLES = st.one_of(
    st.tuples(st.integers(0, 2), st.sampled_from(["a", "b"])),
    st.tuples(st.sampled_from([1, 1.0, True, 0, 0.0]), st.integers(0, 1)),
    st.tuples(st.integers(0, 1)),
    st.tuples(st.integers(0, 1), st.sampled_from([1, "a", 2.0, float("nan")])),
)
_FAMILIES = [
    _INTS,
    st.one_of(_INTS, _BOOLS),
    _FLOATS,
    st.one_of(_INTS, _BOOLS, _FLOATS),
    _STRS,
    _TUPLES,
    st.one_of(_INTS, _BOOLS, _FLOATS, _STRS, _TUPLES, st.none()),
]


# Few distinct values: long lists of these are mostly ties.
_TIE_FAMILIES = [
    st.one_of(st.integers(0, 2), _BOOLS, st.sampled_from([0.0, -0.0, 1.0, 2.0])),
    st.tuples(st.integers(0, 1), st.sampled_from(["a", "b", "c"])),
    st.sampled_from(["a", "b", "c"]),
    st.sampled_from([0.5, 1.5, float("nan"), 1, True]),
]


@st.composite
def _list_scenario(
    draw: Any, families: Any = None, min_size: int = 0, max_size: int = 6
) -> tuple[list[Any], list[tuple[Any, ...]]]:
    values = draw(st.sampled_from(families or _FAMILIES))
    initial = draw(st.lists(values, min_size=min_size, max_size=max_size))
    index = st.integers(-8, 8) if max_size <= 6 else st.integers(-max_size, max_size)
    ops = draw(
        st.lists(
            st.one_of(
                st.tuples(st.just("append"), values),
                st.tuples(st.just("insert"), index, values),
                st.tuples(st.just("set"), index, values),
                st.tuples(st.just("del"), index),
                st.tuples(st.just("pop")),
                st.tuples(st.just("remove"), values),
                st.tuples(st.just("extend"), st.lists(values, max_size=3)),
                st.tuples(st.just("reverse")),
                st.tuples(st.just("sort")),
                st.tuples(st.just("clear")),
                st.tuples(
                    st.just("setslice"), index, index, st.lists(values, max_size=2)
                ),
                st.tuples(st.just("delslice"), index, index),
            ),
            max_size=8,
        )
    )
    return initial, ops


def _apply_list_op(target: Any, op: tuple[Any, ...]) -> Any:
    name = op[0]
    try:
        if name == "append":
            target.append(op[1])
        elif name == "insert":
            target.insert(op[1], op[2])
        elif name == "set":
            target[op[1]] = op[2]
        elif name == "del":
            del target[op[1]]
        elif name == "pop":
            target.pop()
        elif name == "remove":
            target.remove(op[1])
        elif name == "extend":
            target.extend(op[1])
        elif name == "reverse":
            target.reverse()
        elif name == "sort":
            target.sort()
        elif name == "clear":
            target.clear()
        elif name == "setslice":
            target[op[1] : op[2]] = op[3]
        elif name == "delslice":
            del target[op[1] : op[2]]
    except Exception as exc:  # noqa: BLE001
        return type(exc)
    return None


@settings(max_examples=400, deadline=None)
@given(_list_scenario())
def test_list_reducers_equal_the_builtins_after_every_mutation(scenario: Any) -> None:
    initial, ops = scenario
    engine = Engine()
    xs = CascadeList(engine, initial)
    model = list(initial)
    queries = _register(engine, _list_bodies(xs))
    plain = _list_bodies(model)
    _assert_same(queries, plain, model)
    for op in ops:
        raised = _apply_list_op(model, op)
        assert _apply_list_op(xs, op) is raised
        if raised is not None and op[0] == "sort":
            # A failed list.sort() leaves a plain list in an unspecified
            # order, and a CascadeList unchanged; start again from there.
            model[:] = list(xs)
        _assert_same(queries, plain, model)


@settings(max_examples=120, deadline=None)
@given(_list_scenario(_TIE_FAMILIES, min_size=20, max_size=45))
def test_list_reducers_on_longer_lists_full_of_ties(scenario: Any) -> None:
    # Long enough that one group of equal keys is put in order on its own
    # (short lists are renumbered whole), with groups of more than eight.
    initial, ops = scenario
    engine = Engine()
    xs = CascadeList(engine, initial)
    model = list(initial)
    queries = _register(engine, _list_bodies(xs))
    plain = _list_bodies(model)
    _assert_same(queries, plain, model)
    for op in ops:
        raised = _apply_list_op(model, op)
        assert _apply_list_op(xs, op) is raised
        if raised is not None and op[0] == "sort":
            model[:] = list(xs)
        _assert_same(queries, plain, model)


@settings(max_examples=40, deadline=None)
@given(_list_scenario())
def test_list_reducers_through_a_snapshot(scenario: Any) -> None:
    initial, ops = scenario
    engine = Engine()
    xs = CascadeList(engine, initial)
    queries = _register(engine, _list_bodies(xs))
    frozen = list(initial)
    plain = _list_bodies(frozen)
    snap = engine.snapshot()
    for op in ops:
        _apply_list_op(xs, op)
        for fn in plain:
            want = _outcome(fn)
            query = queries[fn.__name__]
            assert _outcome(lambda: query(snapshot=snap)) == want, fn.__name__
        _assert_same(queries, _list_bodies(list(xs)), list(xs))


_KEYS = st.sampled_from(["a", "b", "c", "d"])


@st.composite
def _dict_scenario(draw: Any) -> tuple[dict[str, Any], list[tuple[Any, ...]]]:
    values = draw(st.sampled_from(_FAMILIES))
    initial = draw(st.dictionaries(_KEYS, values, max_size=4))
    ops = draw(
        st.lists(
            st.one_of(
                st.tuples(st.just("set"), _KEYS, values),
                st.tuples(st.just("del"), _KEYS),
                st.tuples(st.just("pop"), _KEYS),
                st.tuples(
                    st.just("update"), st.dictionaries(_KEYS, values, max_size=3)
                ),
                st.tuples(st.just("setdefault"), _KEYS, values),
                st.tuples(st.just("popitem")),
                st.tuples(st.just("clear")),
            ),
            max_size=8,
        )
    )
    return initial, ops


def _apply_dict_op(target: Any, op: tuple[Any, ...]) -> Any:
    name = op[0]
    try:
        if name == "set":
            target[op[1]] = op[2]
        elif name == "del":
            del target[op[1]]
        elif name == "pop":
            target.pop(op[1], None)
        elif name == "update":
            target.update(op[1])
        elif name == "setdefault":
            target.setdefault(op[1], op[2])
        elif name == "popitem":
            target.popitem()
        elif name == "clear":
            target.clear()
    except Exception as exc:  # noqa: BLE001
        return type(exc)
    return None


@settings(max_examples=100, deadline=None)
@given(_dict_scenario())
def test_dict_reducers_equal_the_builtins_after_every_mutation(scenario: Any) -> None:
    initial, ops = scenario
    engine = Engine()
    d = CascadeDict(engine, initial)
    model = dict(initial)
    queries = _register(engine, _dict_bodies(d))
    plain = _dict_bodies(model)
    _assert_same(queries, plain, model)
    for op in ops:
        assert _apply_dict_op(d, op) is _apply_dict_op(model, op)
        _assert_same(queries, plain, model)


@st.composite
def _set_scenario(draw: Any) -> tuple[list[Any], list[tuple[Any, ...]]]:
    values = draw(st.sampled_from(_FAMILIES))
    some = st.lists(values, max_size=3)
    initial = draw(st.lists(values, max_size=5))
    ops = draw(
        st.lists(
            st.one_of(
                st.tuples(st.just("add"), values),
                st.tuples(st.just("discard"), values),
                st.tuples(st.just("update"), some),
                st.tuples(st.just("difference_update"), some),
                st.tuples(st.just("intersection_update"), some),
                st.tuples(st.just("clear")),
            ),
            max_size=8,
        )
    )
    return initial, ops


def _apply_to_ordered_model(model: dict[Any, None], op: tuple[Any, ...]) -> None:
    # A dict used as an insertion-ordered set: the order a CascadeSet
    # promises to rewritten queries.
    name = op[0]
    if name == "add":
        model.setdefault(op[1])
    elif name == "discard":
        model.pop(op[1], None)
    elif name == "update":
        for v in op[1]:
            model.setdefault(v)
    elif name == "difference_update":
        for v in op[1]:
            model.pop(v, None)
    elif name == "intersection_update":
        keep = set(op[1])
        for v in [v for v in model if v not in keep]:
            del model[v]
    elif name == "clear":
        model.clear()


@settings(max_examples=100, deadline=None)
@given(_set_scenario())
def test_set_reducers_equal_the_builtins_in_insertion_order(scenario: Any) -> None:
    initial, ops = scenario
    engine = Engine()
    s = CascadeSet(engine, initial)
    model: dict[Any, None] = dict.fromkeys(initial)
    queries = _register(engine, _set_bodies(s))
    _assert_same(queries, _set_bodies(list(model)), list(model))
    for op in ops:
        getattr(s, op[0])(*op[1:])
        _apply_to_ordered_model(model, op)
        _assert_same(queries, _set_bodies(list(model)), list(model))


def test_the_bodies_under_test_are_rewritten() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2])
    queries = _register(engine, _list_bodies(xs))
    for query in queries.values():
        _outcome(query)
    sites = {p["site"].split(":")[0] for p in engine.inspect_pipelines()}
    assert sites == set(queries) - _NOT_REWRITTEN

    engine_d = Engine()
    d = CascadeDict(engine_d, {"a": 1})
    queries = _register(engine_d, _dict_bodies(d))
    for query in queries.values():
        _outcome(query)
    assert {p["site"].split(":")[0] for p in engine_d.inspect_pipelines()} == set(
        queries
    )
    engine_s = Engine()
    s = CascadeSet(engine_s, [1])
    queries = _register(engine_s, _set_bodies(s))
    for query in queries.values():
        _outcome(query)
    assert {p["site"].split(":")[0] for p in engine_s.inspect_pipelines()} == set(
        queries
    )
