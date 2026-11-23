"""Every read of a collection inside a query is tracked (D5, G21).

The reads run in a helper function, so the AST rewriter never sees them:
what is tested is the collection's own read path. Each read is compared with
the same expression over a plain ``list``/``set``/``dict`` holding the same
contents: after creation, after two mutations, and through a snapshot taken
before them (a read that bypasses the collection's methods sees live data
whatever the revision, so the snapshot check catches it too).

The reads CPython gives a subclass no way to see are at the bottom, pinned
as the documented limit.
"""

from __future__ import annotations

import copy
import heapq
import json
import operator
import pickle
import threading
from typing import Any, Callable

import pytest

from cascade import CascadeDict, CascadeList, CascadeSet, Engine

Read = Callable[[Any], Any]


def _extend_into(target: Any, source: Any, method: str) -> Any:
    getattr(target, method)(source)
    return target


LIST_READS: dict[str, Read] = {
    "iter": lambda c: [x for x in c],
    "list()": lambda c: list(c),
    "tuple()": lambda c: tuple(c),
    "sorted()": lambda c: sorted(c),
    "set()": lambda c: set(c),
    "len": lambda c: len(c),
    "bool": lambda c: bool(c),
    "not": lambda c: not c,
    "in": lambda c: 9 in c,
    "not in": lambda c: 3 not in c,
    "index": lambda c: c[0],
    "negative index": lambda c: c[-1],
    "slice": lambda c: c[1:],
    "step slice": lambda c: c[::-2],
    "reversed": lambda c: list(reversed(c)),
    "copy()": lambda c: c.copy(),
    "copy.copy": lambda c: copy.copy(c),
    "copy.deepcopy": lambda c: copy.deepcopy(c),
    "index()": lambda c: c.index(2),
    "count()": lambda c: c.count(9),
    "xs + list": lambda c: c + [0],
    "list + xs": lambda c: [0] + c,
    "xs + xs": lambda c: c + c,
    "xs * 2": lambda c: c * 2,
    "2 * xs": lambda c: 2 * c,
    "xs * 0": lambda c: c * 0,
    "==": lambda c: c == [3, 1, 2],
    "reflected ==": lambda c: [3, 1, 2] == c,
    "!=": lambda c: c != [3, 1, 2],
    "reflected !=": lambda c: [3, 1, 2] != c,
    "xs == xs": lambda c: c == c,
    "<": lambda c: c < [3, 1, 2, 5],
    "<=": lambda c: c <= [3, 1, 2],
    ">": lambda c: c > [3, 1, 2],
    ">=": lambda c: c >= [3, 1, 2, 5],
    "reflected <": lambda c: [3, 1, 2] < c,
    "reflected >": lambda c: [3, 1, 2, 5] > c,
    "operator.lt": lambda c: operator.lt(c, [3, 1, 2, 5]),
    "repr": lambda c: repr(c),
    "str": lambda c: str(c),
    "f-string": lambda c: f"{c}",
    "format": lambda c: "{}".format(c),
    "percent": lambda c: "%s" % (c,),
    "json.dumps": lambda c: json.dumps(c),
    "json.dumps nested": lambda c: json.dumps({"k": c}),
    "[*xs]": lambda c: [*c],
    "(*xs,)": lambda c: (*c,),
    "f(*xs)": lambda c: (lambda *a: a)(*c),
    "unpack": lambda c: (lambda first, *rest: (first, rest))(*c),
    "plain.extend(xs)": lambda c: _extend_into([], c, "extend"),
    "plain += xs": lambda c: _extend_into([7], c, "__iadd__"),
    "sum": lambda c: sum(c),
    "min": lambda c: min(c),
    "max": lambda c: max(c),
    "any": lambda c: any(x > 8 for x in c),
    "join": lambda c: ",".join(map(str, c)),
    "enumerate": lambda c: list(enumerate(c)),
    "zip": lambda c: list(zip(c, c)),
    "nlargest": lambda c: heapq.nlargest(2, c),
    "dict.fromkeys": lambda c: dict.fromkeys(c),
    "frozenset()": lambda c: frozenset(c),
    "set.union(xs)": lambda c: {0}.union(c),
    "for loop": lambda c: _loop_sum(c),
}

SET_READS: dict[str, Read] = {
    "iter": lambda c: sorted(x for x in c),
    "list()": lambda c: sorted(list(c)),
    "sorted()": lambda c: sorted(c),
    "tuple()": lambda c: sorted(tuple(c)),
    "len": lambda c: len(c),
    "bool": lambda c: bool(c),
    "in": lambda c: 2 in c,
    "not in": lambda c: 1 not in c,
    "copy()": lambda c: c.copy(),
    "copy.copy": lambda c: copy.copy(c),
    "copy.deepcopy": lambda c: copy.deepcopy(c),
    "s | x": lambda c: c | {99},
    "x | s": lambda c: {99} | c,
    "s & x": lambda c: c & {1, 2},
    "x & s": lambda c: {1, 2} & c,
    "s - x": lambda c: c - {1},
    "x - s": lambda c: {1, 2, 3} - c,
    "s ^ x": lambda c: c ^ {2, 3},
    "x ^ s": lambda c: {2, 3} ^ c,
    "s | s": lambda c: c | c,
    "s | frozenset": lambda c: c | frozenset({99}),
    "==": lambda c: c == {1},
    "reflected ==": lambda c: {1} == c,
    "!=": lambda c: c != {1},
    "reflected !=": lambda c: {1} != c,
    "s == frozenset": lambda c: c == frozenset({1}),
    "s == s": lambda c: c == c,
    "<": lambda c: c < {1, 2},
    "<=": lambda c: c <= {1},
    ">": lambda c: c > {1},
    ">=": lambda c: c >= {1, 2},
    "reflected <=": lambda c: {1, 2} <= c,
    "reflected >=": lambda c: {1} >= c,
    "union": lambda c: c.union({5}, [6]),
    "union generator": lambda c: c.union(x for x in (5, 6)),
    "intersection": lambda c: c.intersection({1, 2}),
    "difference": lambda c: c.difference([1]),
    "symmetric_difference": lambda c: c.symmetric_difference({2, 3}),
    "issubset": lambda c: c.issubset({1}),
    "issubset list": lambda c: c.issubset([1, 2]),
    "issuperset": lambda c: c.issuperset({1, 2}),
    "isdisjoint": lambda c: c.isdisjoint({2}),
    "union with no arguments": lambda c: c.union(),
    "sum": lambda c: sum(c),
    "min": lambda c: min(c),
    "json.dumps": lambda c: json.dumps(sorted(c)),
    "dict.fromkeys": lambda c: sorted(dict.fromkeys(c)),
    "[*s]": lambda c: sorted([*c]),
    "keys & s": lambda c: {1: 0, 2: 0}.keys() & c,
    "keys == s": lambda c: {1: 0, 2: 0}.keys() == c,
    "set comprehension": lambda c: {x + 1 for x in c},
}

DICT_READS: dict[str, Read] = {
    "iter": lambda c: [k for k in c],
    "list()": lambda c: list(c),
    "sorted()": lambda c: sorted(c),
    "set()": lambda c: set(c),
    "len": lambda c: len(c),
    "bool": lambda c: bool(c),
    "in": lambda c: "b" in c,
    "getitem": lambda c: c["a"],
    "get": lambda c: c.get("b"),
    "get default": lambda c: c.get("b", 0),
    "reversed": lambda c: list(reversed(c)),
    "dict()": lambda c: dict(c),
    "dict(d, extra=)": lambda c: dict(c, z=0),
    "{**d}": lambda c: {**c},
    "f(**d)": lambda c: (lambda **k: k)(**c),
    "copy()": lambda c: c.copy(),
    "copy.copy": lambda c: copy.copy(c),
    "copy.deepcopy": lambda c: copy.deepcopy(c),
    "==": lambda c: c == {"a": 1},
    "reflected ==": lambda c: {"a": 1} == c,
    "!=": lambda c: c != {"a": 1},
    "reflected !=": lambda c: {"a": 1} != c,
    "d == d": lambda c: c == c,
    "d | x": lambda c: c | {"z": 0},
    "x | d": lambda c: {"z": 0, "a": 5} | c,
    "d | d": lambda c: c | c,
    "repr": lambda c: repr(c),
    "str": lambda c: str(c),
    "f-string": lambda c: f"{c}",
    "json.dumps": lambda c: json.dumps(c),
    "json.dumps sorted": lambda c: json.dumps(c, sort_keys=True, indent=1),
    "plain.update(d)": lambda c: _extend_into({}, c, "update"),
    "plain |= d": lambda c: _extend_into({"q": 0}, c, "__ior__"),
    "format_map": lambda c: "{a}".format_map(c) + str(len(c)),
    "percent": lambda c: "%(a)s" % c,
    "list(keys)": lambda c: list(c.keys()),
    "list(values)": lambda c: list(c.values()),
    "list(items)": lambda c: list(c.items()),
    "sorted(items)": lambda c: sorted(c.items()),
    "dict(items)": lambda c: dict(c.items()),
    "len(keys)": lambda c: len(c.keys()),
    "in keys": lambda c: "b" in c.keys(),
    "in values": lambda c: 2 in c.values(),
    "in items": lambda c: ("b", 2) in c.items(),
    "reversed(items)": lambda c: list(reversed(c.items())),
    "keys ==": lambda c: c.keys() == {"a"},
    "reflected keys ==": lambda c: {"a"} == c.keys(),
    "keys !=": lambda c: c.keys() != {"a"},
    "keys == keys": lambda c: c.keys() == c.keys(),
    "items == items": lambda c: c.items() == {"a": 1}.items(),
    "items == own items": lambda c: c.items() == c.items(),
    "keys &": lambda c: c.keys() & {"a", "b"},
    "reflected keys &": lambda c: {"a", "b"} & c.keys(),
    "keys |": lambda c: c.keys() | {"q"},
    "reflected keys |": lambda c: {"q"} | c.keys(),
    "keys -": lambda c: c.keys() - {"a"},
    "reflected keys -": lambda c: {"a", "b", "q"} - c.keys(),
    "keys ^": lambda c: c.keys() ^ {"a", "q"},
    "keys <=": lambda c: c.keys() <= {"a"},
    "keys >": lambda c: c.keys() > {"a"},
    "keys isdisjoint": lambda c: c.keys().isdisjoint({"b"}),
    "items &": lambda c: c.items() & {("a", 1), ("b", 2)},
    "values ==": lambda c: c.values() == [1],
    "max by value": lambda c: max(c, key=c.get),
    "sum values": lambda c: sum(c.values()),
}

KINDS: dict[str, tuple[Any, Any, Any, list[Callable[[Any], Any]]]] = {
    "list": (
        CascadeList,
        list,
        [3, 1, 2],
        [lambda c: c.append(9), lambda c: c.remove(3)],
    ),
    "set": (
        CascadeSet,
        set,
        [1],
        [lambda c: c.add(2), lambda c: c.discard(1)],
    ),
    "dict": (
        CascadeDict,
        dict,
        {"a": 1},
        [lambda c: c.__setitem__("b", 2), lambda c: c.__setitem__("a", 7)],
    ),
}


def _loop_sum(c: Any) -> int:
    total = 0
    for x in c:
        total += x
    return total


def _cases() -> list[Any]:
    out = []
    for kind, reads in (("list", LIST_READS), ("set", SET_READS), ("dict", DICT_READS)):
        for name, read in reads.items():
            out.append(pytest.param(kind, read, id=f"{kind}: {name}"))
    return out


@pytest.mark.parametrize(("kind", "read"), _cases())
def test_read_inside_a_query_follows_the_collection(kind: str, read: Read) -> None:
    cascade_type, plain_type, initial, mutations = KINDS[kind]
    engine = Engine()
    coll = cascade_type(engine, initial)
    plain = plain_type(initial)

    def helper() -> Any:
        return read(coll)

    @engine.query
    def q() -> Any:
        return helper()

    def check(got: Any, want: Any) -> None:
        assert got == want
        assert type(got) is type(want)

    check(q(), read(plain))
    snap = engine.snapshot()
    before = read(plain)
    for mutate in mutations:
        mutate(coll)
        mutate(plain)
        check(q(), read(plain))
        check(q(snapshot=snap), before)
        check(q(), read(plain))
    # Outside a query the same expression gives the same value.
    check(read(coll), read(plain))


@pytest.mark.parametrize(("kind", "read"), _cases())
def test_read_gives_a_plain_builtin(kind: str, read: Read) -> None:
    cascade_type, plain_type, initial, _ = KINDS[kind]
    engine = Engine()
    got = read(cascade_type(engine, initial))
    assert not isinstance(got, (CascadeList, CascadeSet, CascadeDict))
    assert type(got) is type(read(plain_type(initial)))


# --- two collections in one expression: both are tracked ---------------------


def test_both_operands_of_a_set_operator_are_tracked() -> None:
    engine = Engine()
    a = CascadeSet(engine, [1])
    b = CascadeSet(engine, [2])

    def helper() -> Any:
        return (a | b, a & b, a - b, a ^ b, a == b, a <= b, a.union(b), b.issubset(a))

    @engine.query
    def q() -> Any:
        return helper()

    def want() -> Any:
        x, y = set(a.copy()), set(b.copy())
        return (x | y, x & y, x - y, x ^ y, x == y, x <= y, x.union(y), y.issubset(x))

    assert q() == want()
    b.add(1)
    assert q() == want()
    a.add(2)
    assert q() == want()
    b.discard(2)
    assert q() == want()


def test_both_operands_of_list_and_dict_operators_are_tracked() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1])
    ys = CascadeList(engine, [1])
    d1 = CascadeDict(engine, {"a": 1})
    d2 = CascadeDict(engine, {"a": 1})

    def helper() -> Any:
        return (
            xs + ys,
            xs == ys,
            xs < ys,
            d1 | d2,
            d1 == d2,
            d1.keys() == d2.keys(),
            d1.keys() & d2.keys(),
            d1.items() - d2.items(),
        )

    @engine.query
    def q() -> Any:
        return helper()

    def want() -> Any:
        x, y, p, r = xs.copy(), ys.copy(), d1.copy(), d2.copy()
        return (
            x + y,
            x == y,
            x < y,
            p | r,
            p == r,
            p.keys() == r.keys(),
            p.keys() & r.keys(),
            p.items() - r.items(),
        )

    assert q() == want()
    ys.append(2)
    assert q() == want()
    d2["b"] = 2
    assert q() == want()
    xs.append(5)
    d1["a"] = 3
    assert q() == want()


def test_set_and_dict_view_mixed_operators() -> None:
    engine = Engine()
    s = CascadeSet(engine, ["a"])
    d = CascadeDict(engine, {"a": 1, "b": 2})

    def helper() -> Any:
        return (s & d.keys(), d.keys() & s, s == d.keys(), d.keys() - s, s.union(d))

    @engine.query
    def q() -> Any:
        return helper()

    def want() -> Any:
        x, p = s.copy(), d.copy()
        return (x & p.keys(), p.keys() & x, x == p.keys(), p.keys() - x, x.union(p))

    assert q() == want()
    s.add("b")
    assert q() == want()
    d["c"] = 3
    assert q() == want()


def test_d5_examples_from_the_audit() -> None:
    engine = Engine()
    s = CascadeSet(engine, [1])
    xs = CascadeList(engine, [1])
    d = CascadeDict(engine, {"a": 1})

    @engine.query
    def union() -> list[int]:
        return sorted(s | {99})

    @engine.query
    def as_set() -> set[int]:
        return set(s)

    @engine.query
    def equal() -> tuple[bool, bool]:
        return (s == {1}, d == {"a": 1})

    @engine.query
    def subset() -> bool:
        return s.issubset({1})

    @engine.query
    def concat() -> list[int]:
        return xs + [0]

    @engine.query
    def shown() -> str:
        return f"{xs!r} {s!r} {d!r} {d.keys()!r}"

    assert union() == [1, 99]
    assert as_set() == {1}
    assert equal() == (True, True)
    assert subset() is True
    assert concat() == [1, 0]
    assert shown() == "[1] CascadeSet({1}) {'a': 1} cascade_keys(['a'])"
    s.add(2)
    xs.append(2)
    d["b"] = 2
    assert union() == [1, 2, 99]
    assert as_set() == {1, 2}
    assert equal() == (False, False)
    assert subset() is False
    assert concat() == [1, 2, 0]
    assert shown() == (
        "[1, 2] CascadeSet({1, 2}) {'a': 1, 'b': 2} cascade_keys(['a', 'b'])"
    )


def test_set_repr_names_the_class_at_every_revision() -> None:
    engine = Engine()
    s = CascadeSet(engine)

    @engine.query
    def shown() -> str:
        return repr(s)

    assert repr(s) == "CascadeSet()"
    assert shown() == "CascadeSet()"
    snap = engine.snapshot()
    s.add(1)
    assert shown() == "CascadeSet({1})"
    assert shown(snapshot=snap) == "CascadeSet()"


# --- operand types the builtins refuse, and reflected methods ----------------


def _error(fn: Callable[[], Any]) -> tuple[type, str]:
    with pytest.raises(Exception) as info:  # noqa: PT011
        fn()
    return type(info.value), str(info.value)


@pytest.mark.parametrize(
    "expr",
    [
        lambda c: c + (1,),
        lambda c: (1,) + c,
        lambda c: c + 1,
        lambda c: c * "a",
        lambda c: c * 1.5,
        lambda c: "a" * c,
        lambda c: c < 5,
        lambda c: c < (1,),
        lambda c: c.index(99),
        lambda c: c[10],
    ],
)
def test_list_operand_errors_match_the_builtin(expr: Read) -> None:
    engine = Engine()
    got = _error(lambda: expr(CascadeList(engine, [1, 2])))
    want = _error(lambda: expr([1, 2]))
    assert got[0] is want[0]
    assert got[1].replace("CascadeList", "list") == want[1]


@pytest.mark.parametrize(
    "expr",
    [
        lambda c: c | [1],
        lambda c: [1] | c,
        lambda c: c & 5,
        lambda c: c - "a",
        lambda c: c < [1],
        lambda c: c.union(5),
        lambda c: c.issubset(5),
        lambda c: c.union([[1]]),
        lambda c: hash(c),
    ],
)
def test_set_operand_errors_match_the_builtin(expr: Read) -> None:
    engine = Engine()
    got = _error(lambda: expr(CascadeSet(engine, [1, 2])))
    want = _error(lambda: expr({1, 2}))
    assert got[0] is want[0]
    assert got[1].replace("CascadeSet", "set") == want[1]


@pytest.mark.parametrize(
    "expr",
    [
        lambda c: c | 5,
        lambda c: [("a", 1)] | c,
        lambda c: c["zz"],
        lambda c: hash(c),
        lambda c: c.values() & {1},
        lambda c: c.keys() < 5,
    ],
)
def test_dict_operand_errors_match_the_builtin(expr: Read) -> None:
    engine = Engine()
    got = _error(lambda: expr(CascadeDict(engine, {"a": 1})))
    want = _error(lambda: expr({"a": 1}))
    assert got[0] is want[0]
    assert (
        got[1]
        .replace("CascadeDict", "dict")
        .replace("_CascadeDictView", "dict_values")
        .replace("dict_values' and 'int", "dict_keys' and 'int")
        == want[1]
    )


def test_comparison_with_another_type_is_false_not_an_error() -> None:
    engine = Engine()
    assert (CascadeList(engine, [1]) == (1,)) is False
    assert (CascadeSet(engine, [1]) == [1]) is False
    assert (CascadeDict(engine, {1: 1}) == [(1, 1)]) is False
    assert (CascadeList(engine, [1]) != 5) is True
    assert (CascadeDict(engine, {1: 1}).keys() == [1]) is False


def test_the_other_operands_reflected_method_still_runs() -> None:
    class Greedy:
        def __radd__(self, other: Any) -> str:
            return "radd"

        def __rmul__(self, other: Any) -> str:
            return "rmul"

        def __ror__(self, other: Any) -> str:
            return "ror"

        def __eq__(self, other: Any) -> bool:
            return True

        __hash__ = None  # type: ignore[assignment]

    engine = Engine()
    xs = CascadeList(engine, [1])
    s = CascadeSet(engine, [1])
    d = CascadeDict(engine, {1: 1})
    assert xs + Greedy() == [1] + Greedy() == "radd"
    assert xs * Greedy() == [1] * Greedy() == "rmul"
    assert s | Greedy() == {1} | Greedy() == "ror"
    assert d | Greedy() == {1: 1} | Greedy() == "ror"
    assert (xs == Greedy()) is True
    assert (s == Greedy()) is True
    assert (d == Greedy()) is True


def test_collections_are_unhashable_like_the_builtins() -> None:
    engine = Engine()
    for coll in (
        CascadeList(engine),
        CascadeSet(engine),
        CascadeDict(engine),
        CascadeDict(engine).keys(),
    ):
        with pytest.raises(TypeError, match="unhashable"):
            hash(coll)


def test_copy_module_gives_plain_copies_and_pickle_refuses() -> None:
    engine = Engine()
    nested = [1]
    xs = CascadeList(engine, [nested])
    shallow = copy.copy(xs)
    deep = copy.deepcopy(xs)
    assert type(shallow) is list and shallow[0] is nested
    assert type(deep) is list and deep == [[1]] and deep[0] is not nested
    # The copies are detached: they are not second handles on the collection.
    shallow.append(2)
    assert list(xs) == [[1]]
    for coll in (xs, CascadeSet(engine, [1]), CascadeDict(engine, {1: 1})):
        with pytest.raises(TypeError, match="cannot be pickled"):
            pickle.dumps(coll)


def test_opposite_operand_order_on_two_threads_does_not_deadlock() -> None:
    # ``a | b`` reads b before it takes a's lock, and ``b | a`` the reverse:
    # neither thread ever holds both.
    engine = Engine()
    a = CascadeSet(engine, range(50))
    b = CascadeSet(engine, range(25, 75))
    stop = threading.Event()
    errors: list[BaseException] = []

    def reader(left: CascadeSet, right: CascadeSet, name: str) -> None:
        @engine.query(name=name)
        def q() -> int:
            return len(_union(left, right))

        try:
            while not stop.is_set():
                assert q() >= 50
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def writer() -> None:
        i = 100
        try:
            while not stop.is_set():
                a.add(i)
                b.add(i + 1)
                a.discard(i)
                b.discard(i + 1)
                i += 2
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [
        threading.Thread(target=reader, args=(a, b, "ab"), daemon=True),
        threading.Thread(target=reader, args=(b, a, "ba"), daemon=True),
        threading.Thread(target=writer, daemon=True),
    ]
    for t in threads:
        t.start()
    timer = threading.Timer(0.5, stop.set)
    timer.start()
    for t in threads:
        t.join(10)
    timer.cancel()
    stop.set()
    assert not any(t.is_alive() for t in threads), "deadlock"
    assert not errors


def _union(left: Any, right: Any) -> Any:
    return left | right


# --- the limit: C code that reads a set subclass's storage itself ------------
#
# For any ``set`` subclass CPython's set and frozenset code reads the other
# operand's hash table directly; no method of the subclass runs. These reads
# record nothing and see live contents. They are pinned here so that a change
# (in CPython or in Cascade) shows up; docs/guarantees.md G21 lists them.

UNTRACKED_SET_READS: dict[str, Read] = {
    "set(s) outside a rewritten body": lambda c: set(c),
    "frozenset(s)": lambda c: frozenset(c),
    "{*s}": lambda c: {*c},
    "plain.union(s)": lambda c: {5}.union(c),
    "plain.issubset(s)": lambda c: {1, 2}.issubset(c),
    "plain |= s": lambda c: _extend_into(set(), c, "__ior__"),
    "plain.update(s)": lambda c: _extend_into(set(), c, "update"),
    "frozenset | s": lambda c: frozenset({99}) | c,
    "frozenset == s": lambda c: frozenset({1}) == c,
}

TRACKED_SPELLINGS: dict[str, Read] = {
    "set(s) outside a rewritten body": lambda c: c.copy(),
    "frozenset(s)": lambda c: frozenset(c.copy()),
    "{*s}": lambda c: {*c.copy()},
    "plain.union(s)": lambda c: c.union({5}),
    "plain.issubset(s)": lambda c: c.issuperset({1, 2}),
    "plain |= s": lambda c: _extend_into(set(), c.copy(), "__ior__"),
    "plain.update(s)": lambda c: _extend_into(set(), iter(c), "update"),
    "frozenset | s": lambda c: frozenset(c | frozenset({99})),
    "frozenset == s": lambda c: c == frozenset({1}),
}


@pytest.mark.parametrize("name", list(UNTRACKED_SET_READS))
def test_limit_set_read_by_c_code_is_not_tracked(name: str) -> None:
    read = UNTRACKED_SET_READS[name]
    spelling = TRACKED_SPELLINGS[name]
    engine = Engine()
    s = CascadeSet(engine, [1])

    def helper() -> Any:
        return read(s)

    def tracked_helper() -> Any:
        return spelling(s)

    @engine.query
    def q() -> Any:
        return helper()

    @engine.query
    def tracked() -> Any:
        return tracked_helper()

    first = q()
    assert first == read({1}) == tracked()
    s.add(2)
    assert q() == first  # the limit: stale
    assert q() != read({1, 2})
    assert tracked() == read({1, 2})  # the documented spelling is tracked


def test_set_call_in_a_rewritten_query_body_is_tracked() -> None:
    engine = Engine()
    s = CascadeSet(engine, [1])

    @engine.query
    def q() -> set[int]:
        return set(s)

    assert q() == {1}
    s.add(2)
    assert q() == {1, 2}
