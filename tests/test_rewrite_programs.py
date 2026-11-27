"""Generated query bodies against plain Python (G22, test gap 4).

``test_rewrite_differential.py`` checks each reducer in a body written by
hand. Here Hypothesis writes the bodies: several rewritten expressions in one
query, comprehensions inside the elements and conditions of others (also
inside a two-``for`` comprehension that stays as written), ``:=``, lambdas,
closures, plain ``for`` loops and ``if`` statements, locals used as sources,
an input read inside an element, a query called inside an element, and
elements that raise.

Each body is compiled twice from the same source: once in a namespace of
cascade collections, an engine input and an engine query, registered as a
query (and so rewritten), and once in a namespace of plain lists, an
insertion-ordered dict for the set, a plain dict and plain functions, called
as is. Two bodies share each example's data and changes. After every change
to the collections and to the input, each query must give what its plain
function gives: the same value down to types (``1``, ``1.0`` and ``True``
apart, ``0.0`` and ``-0.0`` apart, NaN equal to NaN), or the same exception
with the same message. A third query that returns both results must give
the same, and must run again only when one of them changed (early cutoff, in
both directions).

Each example draws one family of values for the collections, the input, the
closure variable and the literals in the body: small ints; ints with floats
(``-0.0``, NaN, infinities); values that are equal but of other types (``0``,
``0.0``, ``-0.0``, ``False``); ints beyond 64 bits; strings and bytes; a mix
with ``None``; tuples, frozensets, lists and dicts. One example in five runs
on a disk backend and restarts the engine in between: the contents and the
stored results come back from disk and must still agree.

Each body uses some of the sources, reducers, kinds of expression and kinds
of change only (swarm testing), always has a reducer or two right over a
source, and mostly catches what each statement raises, so that one part
that raises does not hide the others. Without that the faults seeded into
how a pipeline reads its diffs needed thousands of examples.

Four more parts of the grammar are each in some bodies only:

* ``key=`` functions for ``sorted``, ``min`` and ``max`` that raise for
  some elements or return keys of types that cannot be compared, so that
  the error, and the effects pushed before it, must be Python's;
* accumulator effects pushed from elements, filters, ``key=`` functions,
  a query called per element and statements: what a call collects must be
  what the plain function pushes, in its order, after every change, on a
  cache hit and through a snapshot (and nothing when the call raises);
  also a local list appended to from elements, filters and keys;
* functions defined in the body: used as stages, filters and keys, with
  defaults, holding comprehensions over the collections themselves, and
  defined inside a loop over the loop variable;
* closures: lambdas and generators made in a comprehension's element over
  its loop variable (all of them see its last value), and lambdas over a
  local computed from the collections, so it changes between runs.

One body in four is ``async def``, with ``await`` of an async query between
statements and in comprehensions, and ``async for``. The rewriter leaves
async queries as written; such a body, awaited on a running loop, must
still give what the plain coroutine gives. Being left as written, it reads
``set(s)`` without a trace (G21): an async body takes ``set(iter(s))``.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import linecache
import math
import os
import shutil
import tempfile
from collections import Counter
from typing import Any, Callable, NamedTuple

import pytest
from hypothesis import HealthCheck, assume, event, given, note, settings
from hypothesis import strategies as st

from cascade import CascadeDict, CascadeList, CascadeSet, Engine
from tests.test_rewrite_differential import (
    _apply_dict_op,
    _apply_list_op,
    _apply_to_ordered_model,
)

# --- the values -----------------------------------------------------------------

# One NaN object throughout. ``x in xs`` and ``xs.remove(x)`` match NaN by
# identity, so a second NaN object would make the plain model and the
# collections disagree about values that are each other's copies.
NAN = float("nan")
INF = float("inf")

SMALL = st.integers(-3, 3)
EQUAL_VALUES = (0, 1, 2, 0.0, -0.0, 1.0, 2.0, True, False)
# NaN three times: it has to meet min, max and sorted with other numbers.
FLOAT_VALUES = (0.5, -1.5, 0.0, -0.0, 2.0, NAN, NAN, NAN, INF, -INF, 1e16, 0.1)
BIG_VALUES = (2**63, -(2**63) - 1, 2**64, 2**64 - 1, 2**100, -(2**70))
TEXT_VALUES = ("", "a", "b", "ab", "0", b"", b"a", b"ab")
HASHABLE_CONTAINERS = (
    (),
    (1,),
    (0, "a"),
    (1, 1.0),
    (True, -0.0),
    (NAN,),
    ((1, 2), "a"),
    frozenset(),
    frozenset({1, "a"}),
    frozenset({0.0, 2}),
)
# Values of a list or of the dict only: a set or a key raises for them.
UNHASHABLE = ([], [1, "a"], [[1]], {"a": 1}, {1: [2]}, (1, [2]))


class Family(NamedTuple):
    """The values one example uses, and the literals its body may contain."""

    name: str
    values: Any  # elements of the lists, values of the dict, the input
    hashables: Any  # elements of the set
    keys: Any  # keys of the dict
    literals: tuple[Any, ...]

    def on_disk(self) -> Family:
        """No NaN in the collections.

        NaN is equal only to itself, and what comes back from disk is a new
        object for each time it was written. A NaN key removed before a
        restart comes back after it (see ``test_known_defects.py``), and a
        list that held one NaN twice holds two after it: ``set(xs)`` read
        from the stored result and computed again differ.
        """
        return _ON_DISK[self.name]


def _no_nan(value: Any) -> bool:
    if type(value) is float:
        return value == value
    if type(value) is tuple or type(value) is frozenset:
        return all(_no_nan(v) for v in value)
    return True


def _family(
    name: str,
    values: tuple[Any, ...],
    unhashable: tuple[Any, ...] = (),
    keys: Any = None,
) -> Family:
    hashable = st.one_of(SMALL, st.sampled_from(values)) if values else SMALL
    every = st.one_of(hashable, st.sampled_from(unhashable)) if unhashable else hashable
    if keys is None:
        # Few keys, so that a change mostly replaces a value.
        keys = st.one_of(st.integers(0, 2), st.integers(0, 2), hashable)
    return Family(name, every, hashable, keys, (-1, 0, 1, 2, *values))


FAMILIES = (
    _family("ints", ()),
    _family("floats", FLOAT_VALUES),
    # Keys equal to each other in three types: ``d[1.0] = v`` keeps key ``1``.
    _family("equal", EQUAL_VALUES, keys=st.sampled_from(EQUAL_VALUES)),
    _family("big", BIG_VALUES),
    _family("text", TEXT_VALUES),
    _family("mixed", (None, *EQUAL_VALUES[3:], NAN, INF, "", "a", b"a", 2**64)),
    _family("containers", HASHABLE_CONTAINERS, UNHASHABLE),
)
_ON_DISK = {
    f.name: f._replace(
        values=f.values.filter(_no_nan),
        hashables=f.hashables.filter(_no_nan),
        keys=f.keys.filter(_no_nan),
    )
    for f in FAMILIES
}
# Floats and equal values of three types count twice: they raise little,
# so most of each body is computed by the pipelines, and they are where
# equal values differ.
FAMILY = st.sampled_from((FAMILIES[1], FAMILIES[2], *FAMILIES))


def _lit(value: Any) -> str:
    """Source text that evaluates to ``value`` (``nan`` and ``inf`` are names)."""
    kind = type(value)
    if kind is float:
        if value != value:
            return "nan"
        if math.isinf(value):
            return "inf" if value > 0 else "(-inf)"
        return repr(value)
    if kind is tuple:
        inner = ", ".join(_lit(v) for v in value)
        return f"({inner},)" if len(value) == 1 else f"({inner})"
    if kind is frozenset:
        if not value:
            return "frozenset()"
        return "frozenset({" + ", ".join(_lit(v) for v in value) + "})"
    if kind is list:
        return "[" + ", ".join(_lit(v) for v in value) + "]"
    if kind is dict:
        return "{" + ", ".join(f"{_lit(k)}: {_lit(v)}" for k, v in value.items()) + "}"
    return repr(value)


# --- the grammar ----------------------------------------------------------------

LOOP_NAMES = ("x", "y", "z")
LIST_SOURCES = ("xs", "ys")
SOURCES = ("xs", "ys", "s", "d", "d.values()", "d.keys()", "d.items()")
REDUCERS = ("sum", "len", "min", "max", "any", "all", "count")
# Swarm testing: each body uses some of each of these only, so that what it
# does use comes up often enough to meet the change that breaks it.
EXPR_KINDS = ("literal", "binop", "factor", "cond", "generic")
PIPE_KINDS = ("comp", "map", "filter", "reversed", "nested", "two_for", "eager")
TOP_KINDS = (
    *("value", "list", "sorted", "set", "setcomp", "dict", "join"),
    *("fold", "decided"),
)
# Elements that raise for some values and not for others.
PARTIALS = (
    "(1 // {v})",
    "({v} + 1)",
    "inc({v})",
    "({v} < 1)",
    "{v}[0]",
    "({v} % 2)",
    "neg({v})",
    "({o} // {v})",
    "({v} - {o})",
    "int({v})",
    "(10, 20, 30)[{v}]",
)
HELPERS = ("inc", "neg", "half", "kind", "tag")
PREDICATES = ("is_odd", "is_pos", "is_int")
MAX_DEPTH = 2
# Parts of the grammar that only some bodies use (swarm testing).
FEATURES = ("keys", "effects", "defs", "closures")
# ``key=`` functions the old grammar had, and those of the "keys" part:
# some raise for some elements, some give keys of types that cannot be
# compared with each other, one is a query.
OLD_KEYS = ("neg", "lambda v: v % 3", "kind")
KEYS = (
    *OLD_KEYS,
    "inc",
    "str",
    "tag",
    "lambda v: 1 // v",
    "lambda v: v if is_int(v) else kind(v)",
    "lambda v: (is_pos(v), v)",
    "lambda v: -v if is_pos(v) else v",
    # Frozensets are ordered by inclusion only: many ties, some pairs
    # neither smaller nor larger, and no error.
    "lambda v: frozenset({kind(v)})",
)
# Functions that push accumulator effects: for every element, for some
# only, as a filter, as a key, and a query called per element.
EFFECT_HELPERS = ("noted", "noted_odd", "ntag")
EFFECT_PREDICATES = ("seen",)
EFFECT_KEYS = ("keyed",)


class _Writer:
    """Draws the source text of one query body.

    ``names`` are the names in scope that hold a single value: loop
    variables, the closure variable ``k`` and locals assigned earlier;
    ``lists`` are locals that hold a plain list and can be a source.

    Each body uses some of the sources, reducers, helpers, raising elements
    and kinds of expression only (swarm testing): a body that sums
    ``d.values()`` in three places finds what one sum among seven kinds of
    reducer over seven sources rarely does.
    """

    def __init__(
        self,
        draw: Callable[..., Any],
        literals: tuple[Any, ...],
        *,
        is_async: bool = False,
    ) -> None:
        self.draw = draw
        self.literals = tuple(_lit(v) for v in literals)
        self.sources = _some(draw, SOURCES)
        self.reducers = _some(draw, REDUCERS)
        self.helpers = list(_some(draw, HELPERS))
        self.partials = _some(draw, PARTIALS)
        self.expr_kinds = ("name", *_some(draw, EXPR_KINDS))
        self.pipe_kinds = ("source", *_some(draw, PIPE_KINDS))
        self.top_kinds = _some(draw, TOP_KINDS)
        self.features = _some(draw, FEATURES, at_least=0)
        self.is_async = is_async
        self.predicates = list(PREDICATES)
        self.keys = list(_some(draw, KEYS) if "keys" in self.features else OLD_KEYS)
        if "effects" in self.features:
            self.helpers += _some(draw, EFFECT_HELPERS)
            self.predicates += EFFECT_PREDICATES
            self.keys += EFFECT_KEYS
        # Functions defined in the body so far.
        self.defs: list[str] = []
        self.n_locals = 0
        self.n_walrus = 0
        self.lists: list[str] = []
        self.walrus_used = False

    def choice(self, options: Any) -> Any:
        return self.draw(_sampler(tuple(options)))

    # --- expressions ---

    def expr(self, names: tuple[str, ...], depth: int) -> str:
        kinds = [*self.expr_kinds, "name"]
        if depth < MAX_DEPTH:
            # Calls that push effects or call a function of the body; not
            # deeper, so that expressions stay small.
            if "effects" in self.features:
                kinds.append("noted")
            if self.defs:
                kinds.append("call")
            kinds += ["reduce", "reduce"]
        kind = self.choice(kinds)
        if kind == "name" and names:
            return self.choice(names)
        if kind == "binop":
            op = self.choice(("+", "-", "*", "//", "%"))
            left = self.expr(names, depth + 1)
            if op in ("//", "%") and len(names) > 1 and self.draw(st.booleans()):
                # By the innermost name, often a loop variable: an element
                # that raises for some values only, the case where lazy and
                # eager evaluation differ (D37).
                right = names[-1]
            else:
                right = self.expr(names, depth + 1)
            return f"({left} {op} {right})"
        if kind == "factor":
            return "factor()"
        if kind == "noted":
            return f"noted({self.expr(names, depth + 1)})"
        if kind == "call":
            # A function defined in the body, called in an element.
            return f"{self.choice(self.defs)}({self.expr(names, depth + 1)})"
        if kind == "cond":
            return (
                f"({self.expr(names, depth + 1)} if {self.cond(names, depth + 1)}"
                f" else {self.expr(names, depth + 1)})"
            )
        if kind == "reduce":
            return self.reduce(names, depth + 1)
        if kind == "generic":
            # Defined for every value: the values themselves get through to
            # the results, whatever their type.
            form = self.choice(("pair", "str", "tag", "eq", "kind"))
            first = self.expr(names, depth + 1)
            if form == "pair":
                return f"({first}, {self.expr(names, depth + 1)})"
            if form == "eq":
                return f"({first} == {self.expr(names, depth + 1)})"
            return f"{form}({first})"
        return self.choice(self.literals)

    def partial(self, var: str, names: tuple[str, ...]) -> str:
        """An element that raises for some values and not for others."""
        other = self.choice(names) if names else "k"
        return self.choice(self.partials).format(v=var, o=other)

    def eager(self, names: tuple[str, ...], depth: int) -> str:
        """A list comprehension over such elements.

        Python builds it in full first: an element that raises does so even
        where ``any()`` above it is already decided, and before the stages
        outside it see the first element (D37).
        """
        var = self.choice(LOOP_NAMES)
        if depth < MAX_DEPTH and self.draw(st.booleans()):
            source = self.pipe(names, depth + 1)
        else:
            source = self.source()
        return f"[{self.partial(var, (*names, var))} for {var} in {source}]"

    def cond(self, names: tuple[str, ...], depth: int) -> str:
        kind = self.choice(("cmp", "pred", "in", "any_all"))
        if kind == "pred" and names:
            return f"{self.choice(self.predicates)}({self.choice(names)})"
        if kind == "in":
            return f"{self.expr(names, depth + 1)} in {self.source()}"
        if kind == "any_all" and depth < MAX_DEPTH:
            name = self.choice(("any", "all"))
            if self.draw(st.booleans()):
                return f"{name}({self.eager(names, depth)})"
            inner = self.pipe(names, depth + 1, generator=self.draw(st.booleans()))
            return f"{name}({inner})"
        op = self.choice(("<", ">", "==", "!="))
        left = self.expr(names, depth + 1)
        return f"{left} {op} {self.expr(names, depth + 1)}"

    def element(self, names: tuple[str, ...], depth: int) -> str:
        """An element or ``if`` clause body, sometimes with ``:=``."""
        expr = self.expr(names, depth)
        if self.draw(st.integers(0, 9)) == 0:
            self.n_walrus += 1
            self.walrus_used = True
            return f"(w{self.n_walrus} := {expr})"
        return expr

    def source(self) -> str:
        return self.choice((*self.sources, *self.lists))

    def pipe(
        self, names: tuple[str, ...], depth: int, *, generator: bool = False
    ) -> str:
        """An iterable: a source, or stages over one."""
        kinds = self.pipe_kinds
        if depth >= MAX_DEPTH:
            kinds = tuple(k for k in kinds if k not in ("nested", "two_for", "eager"))
        kind = self.choice(kinds)
        if kind == "source":
            return self.source()
        if kind == "eager":
            return self.eager(names, depth)
        if kind == "reversed":
            return f"reversed({self.choice((*LIST_SOURCES, *self.lists))})"
        if kind == "map":
            inner = self.pipe(names, depth + 1)
            if self.draw(st.booleans()):
                return f"map({self.choice(self.helpers)}, {inner})"
            var = self.choice(LOOP_NAMES)
            body = self.expr((*names, var), depth + 1)
            return f"map(lambda {var}: {body}, {inner})"
        if kind == "filter":
            inner = self.pipe(names, depth + 1)
            fn = self.choice((*self.predicates, "None"))
            return f"filter({fn}, {inner})"
        var = self.choice(LOOP_NAMES)
        inner_names = (*names, var)
        if kind == "two_for":
            # Stays as written. A pipeline in its element that uses its loop
            # variables must stay inside it too (D32).
            other = self.choice([n for n in LOOP_NAMES if n != var])
            both = (*names, var, other)
            first = self.source()
            second = self.pipe(inner_names, depth + 1)
            if self.draw(st.booleans()):
                third = next(n for n in LOOP_NAMES if n not in (var, other))
                elt = (
                    f"sum({third} * {var} for {third} in {self.source()}"
                    f" if {third} != {other})"
                )
            else:
                elt = self.element(both, depth + 1)
            return f"[{elt} for {var} in {first} for {other} in {second}]"
        iterable = self.pipe(names, depth + 1) if kind == "nested" else self.source()
        elt = self.element(inner_names, depth + 1)
        clause = ""
        if self.draw(st.booleans()):
            clause = f" if {self.element(inner_names, depth + 1)}"
        if generator or self.draw(st.booleans()):
            return f"({elt} for {var} in {iterable}{clause})"
        return f"[{elt} for {var} in {iterable}{clause}]"

    def reduce(self, names: tuple[str, ...], depth: int) -> str:
        reducer = self.choice(self.reducers)
        if reducer == "len":
            var = self.choice(LOOP_NAMES)
            if self.draw(st.booleans()):
                return f"len({self.source()})"
            elt = self.element((*names, var), depth + 1)
            return f"len([{elt} for {var} in {self.pipe(names, depth + 1)}])"
        if reducer == "count":
            return f"sum(1 for _ in {self.pipe(names, depth + 1)})"
        if reducer in ("any", "all") and self.draw(st.booleans()):
            return f"{reducer}({self.eager(names, depth)})"
        inner = self.pipe(names, depth + 1, generator=self.draw(st.booleans()))
        if reducer in ("min", "max") and self.draw(st.booleans()):
            # With ``default=`` it is left as written.
            return f"{reducer}({inner}, default={self.choice(self.literals)})"
        return f"{reducer}({inner})"

    def top(self, names: tuple[str, ...]) -> str:
        """A top-level expression of any shape."""
        kind = self.choice(self.top_kinds)
        if kind == "fold":
            return self.fold()
        if kind == "decided":
            return f"{self.choice(('any', 'all'))}({self.eager(names, 1)})"
        if kind == "list":
            return f"list({self.pipe(names, 1)})"
        if kind == "sorted":
            return self.keyed(names, self.pipe(names, 1))
        if kind == "set":
            return self.set_of(self.pipe(names, 1))
        if kind == "setcomp":
            var = self.choice(LOOP_NAMES)
            return f"{{{self.element((*names, var), 1)} for {var} in {self.source()}}}"
        if kind == "dict":
            # Iterating ``d`` gives its keys; only ``dict(d)`` copies (D36).
            var = self.choice(LOOP_NAMES)
            elt = self.element((*names, var), 1)
            source = self.choice(("d", "d", "d.keys()", *self.sources, *self.lists))
            form = self.choice(
                ("comp", "comp", "gen", "map", "items", "items", "copy", "copy")
            )
            if form == "items":
                return self.choice(("{p: q for p, q in d.items()}", "dict(d.items())"))
            if form == "copy":
                return "dict(d)"
            if form == "gen":
                return f"dict(({var}, {elt}) for {var} in {source})"
            if form == "map":
                return f"dict(map(lambda {var}: ({var}, {elt}), {source}))"
            return f"{{{var}: {elt} for {var} in {source}}}"
        if kind == "join":
            return f'",".join(map(str, {self.pipe(names, 1)}))'
        return self.expr(names, 0)

    def key(self, names: tuple[str, ...]) -> str:
        """A ``key=`` function: a name, or a lambda over the names in scope."""
        if "keys" in self.features and self.draw(st.integers(0, 3)) == 0:
            return f"lambda v: {self.expr((*names, 'v'), 1)}"
        return self.choice(self.keys)

    def keyed(self, names: tuple[str, ...], inner: str) -> str:
        """``sorted``, ``min`` or ``max`` with ``key=``.

        Without the "keys" part, the ``sorted`` the old grammar had.
        ``min`` and ``max`` with ``key=`` are left as written.
        """
        if "keys" not in self.features:
            key = self.choice(("", *(f", key={k}" for k in self.keys)))
            rev = self.choice(("", ", reverse=True"))
            return f"sorted({inner}{key}{rev})"
        key = self.key(names)
        form = self.choice(("sorted", "sorted", "reverse", "min", "max", "default"))
        if form == "sorted":
            return f"sorted({inner}, key={key})"
        if form == "reverse":
            return f"sorted({inner}, key={key}, reverse=True)"
        if form == "default":
            name = self.choice(("min", "max"))
            return f"{name}({inner}, key={key}, default=None)"
        return f"{form}({inner}, key={key})"

    def async_top(self, names: tuple[str, ...]) -> str:
        """``await`` inside a comprehension, or ``async for``.

        Only where Python allows them: list, set and dict comprehensions in
        an ``async def``.
        """
        var = self.choice(LOOP_NAMES)
        elt = self.element((*names, var), 1)
        form = self.choice(("list", "set", "dict", "sum", "async_for", "async_if"))
        if form == "list":
            return f"[await atag({elt}) for {var} in {self.pipe(names, 1)}]"
        if form == "set":
            return f"{{await atag({elt}) for {var} in {self.source()}}}"
        if form == "dict":
            return f"{{{var}: await atag({elt}) for {var} in {self.source()}}}"
        if form == "sum":
            return f"sum([await atag({var}) for {var} in {self.source()}])"
        if form == "async_if":
            return (
                f"[{elt} for {var} in {self.source()}"
                f" if await atag({self.cond((*names, var), 1)})]"
            )
        return f"[{elt} async for {var} in agen({self.pipe(names, 1)})]"

    def fold(self) -> str:
        """A reducer right over a source, or over one plain stage.

        Where a reducer keeps a running state (a total, a count of truthy
        elements, a sorted structure), a change read the wrong way shows
        in its result; deep in a body it is often hidden by an element
        that raises or a condition that does not look.
        """
        source = self.source()
        inner = self.choice(
            (
                source,
                source,
                f"map({self.choice(self.helpers)}, {source})",
                # A query called per element: its argument is its key.
                f"map(tag, {source})",
                f"filter({self.choice((*self.predicates, 'None'))}, {source})",
                f"(x * {self.choice(self.literals)} for x in {source})",
                # Built in full before the reducer sees it (D37).
                f"[{self.partial('x', ('x',))} for x in {source}]",
            )
        )
        if "keys" in self.features and self.draw(st.booleans()):
            return self.keyed((), inner)
        reducer = self.choice((*self.reducers, "sorted", "list", "set"))
        if source in ("d", "d.items()") and self.draw(st.booleans()):
            reducer = "dict"
        if reducer == "count":
            return f"sum(1 for _ in {inner})"
        if reducer == "len":
            return f"len({source})"
        if reducer == "set":
            return self.set_of(inner)
        return f"{reducer}({inner})"

    def set_of(self, inner: str) -> str:
        """``set(inner)``; over the bare set through ``iter`` in an async body.

        ``set(s)`` copies a set subclass without calling it, so a query
        that is not rewritten records no read (a documented limit, G21),
        and an async query is never rewritten.
        """
        if self.is_async and inner == "s":
            return "set(iter(s))"
        return f"set({inner})"

    # --- statements ---

    def body(self) -> list[str]:
        names: tuple[str, ...] = ("k",)
        lines: list[str] = []
        others: list[str] = []
        # Mostly each statement catches what it raises, so that one part
        # that raises does not hide what the others give.
        guarded = self.draw(st.integers(0, 3)) > 0

        def add(statement: list[str], name: str, fallback: str) -> None:
            if not guarded:
                lines.extend(statement)
                return
            lines.append("try:")
            lines.extend("    " + line for line in statement)
            lines.append("except Exception as e:")
            lines.append(f"    {name} = {fallback}")

        kinds = ["assign", "assign", "list", "loop", "if"]
        if "defs" in self.features:
            kinds += ["def", "def", "loop_def"]
        if "closures" in self.features:
            kinds += ["late", "captured", "rows"]
        if "effects" in self.features:
            kinds += ["push", "collect"]
        if self.is_async:
            kinds += ["await", "await"]
        for _ in range(self.draw(st.integers(1, 4))):
            kind = self.choice(kinds)
            self.n_locals += 1
            name = f"a{self.n_locals}"
            if kind == "def":
                lines.extend(self.define(f"f{self.n_locals}", names))
                continue
            if kind == "loop_def":
                lines.append(f"{name} = ()")
                add(self.loop_def(name, names), name, name)
                others.append(name)
                continue
            if kind == "rows":
                add(self.rows(name, names), name, "[]")
                others.append(name)
                continue
            if kind == "late":
                add(self.late(name, names), name, "(type(e).__name__, str(e))")
                others.append(name)
                continue
            if kind == "captured":
                local = f"m{self.n_locals}"
                add([f"{local} = {self.reduce(names, 1)}"], local, "0")
                names = (*names, local)
                var = self.choice(LOOP_NAMES)
                body = self.expr((*names, "v"), 1)
                src = self.source()
                use = self.choice(
                    (
                        f"sorted({src}, key=lambda v: {body})",
                        f"list(map(lambda v: {body}, {src}))",
                        f"[(lambda v: {body})({var}) for {var} in {src}]",
                        f"list(filter(lambda v: {body}, {src}))",
                    )
                )
                add([f"{name} = {use}"], name, "(type(e).__name__, str(e))")
                others.append(name)
                continue
            if kind == "push":
                lines.append(f"push({self.choice(names)})")
                continue
            if kind == "collect":
                # A local list changed from an element, not an accumulator:
                # plain Python appends for every element, on every run.
                var = self.choice(LOOP_NAMES)
                elt = self.element((*names, var), 1)
                src = self.pipe(names, 1)
                made = self.choice(
                    (
                        f"[{name}.append({elt}) for {var} in {src}]",
                        f"sum(1 for {var} in {src} if {name}.append({elt}) is None)",
                        f"list(map(lambda v: {name}.append(v) or v, {src}))",
                        f"sorted({src}, key=lambda v: {name}.append(v) or {elt})",
                    )
                )
                lines.append(f"{name} = []")
                add([made], name, name)
                others.append(name)
                continue
            if kind == "await":
                add([f"{name} = await atag({self.expr(names, 1)})"], name, "0")
                names = (*names, name)
                continue
            if kind == "list":
                add([f"{name} = list({self.pipe(names, 1)})"], name, "[]")
                self.lists.append(name)
                continue
            if kind == "loop":
                var = self.choice(LOOP_NAMES)
                lines.append(f"{name} = 0")
                body = f"{name} += {self.expr((*names, var, name), 1)}"
                add([f"for {var} in {self.pipe(names, 1)}:", f"    {body}"], name, name)
            elif kind == "if":
                add(
                    [
                        f"if {self.cond(names, 0)}:",
                        f"    {name} = {self.top(names)}",
                        "else:",
                        f"    {name} = {self.top(names)}",
                    ],
                    name,
                    "(type(e).__name__, str(e))",
                )
                # Of any shape: returned, but not used as a value from here on.
                others.append(name)
                continue
            else:
                add([f"{name} = {self.expr(names, 0)}"], name, "0")
            names = (*names, name)
        # Reducers right over a source in every body, besides the rest.
        tops = [self.fold() for _ in range(self.draw(st.integers(1, 4)))]
        tops += [self.top(names) for _ in range(self.draw(st.integers(0, 2)))]
        if self.is_async:
            tops += [self.async_top(names) for _ in range(self.draw(st.integers(1, 2)))]
        results = []
        for i, expr in enumerate(tops):
            add([f"r{i} = {expr}"], f"r{i}", "(type(e).__name__, str(e))")
            results.append(f"r{i}")
        results += [n for n in names if n != "k"]
        results += others
        if self.draw(st.booleans()):
            results.append("factor()")
        if self.n_walrus:
            # ``:=`` in a comprehension binds in the query (D30); one that
            # never ran leaves its name unbound, in Python as here.
            bound = ", ".join(f"w{i}" for i in range(1, self.n_walrus + 1))
            lines += [
                "try:",
                f"    bound = ({bound},)",
                "except NameError:",
                "    bound = None",
            ]
            results.append("bound")
        lines.append(f"return ({', '.join(results)},)")
        return lines

    def define(self, fn: str, names: tuple[str, ...]) -> list[str]:
        """A function defined in the body, over the names in scope.

        It may take a default from them, and its body may itself hold a
        comprehension over a collection. It is a stage, a filter, a key and
        a function called in elements from here on.
        """
        params = "v"
        inner = (*names, "v")
        if self.draw(st.integers(0, 3)) == 0:
            params = f"v, m={self.choice(names)}"
            inner = (*inner, "m")
        form = self.choice(("expr", "reduce", "cond"))
        if form == "cond":
            body = self.cond(inner, 1)
            self.predicates.append(fn)
        else:
            body = self.reduce(inner, 1) if form == "reduce" else self.expr(inner, 1)
            self.helpers.append(fn)
        self.keys.append(fn)
        self.defs.append(fn)
        return [f"def {fn}({params}):", f"    return {body}"]

    def loop_def(self, name: str, names: tuple[str, ...]) -> list[str]:
        """A function defined in a loop over the loop variable, used there.

        Each pass gives it another value to close over, or to take as a
        default: a pipeline that uses it must see each.

        The loop variable is not one of the comprehensions' names. Where a
        function takes ``x`` from outside, CPython raises
        ``UnboundLocalError`` for ``[x for z in [1 for x in ys]]`` in it;
        rewritten, the inner comprehension is in a helper and ``x`` is the
        outer one.
        """
        var = "i"
        fn = f"g{self.n_locals}"
        params = "v"
        if self.draw(st.booleans()):
            params = f"v, m={var}"
        # Mostly over what changes from pass to pass only, so that it shows.
        local = (var, "v", *(("m",) if "m" in params else ()))
        if self.draw(st.booleans()):
            body = self.expr(local[1:] if "m" in params else local, 1)
            body = f"({body}, {local[-1]})"
        else:
            body = self.expr((*names, *local), 1)
        # Over a collection: a pipeline keeps its state from pass to pass.
        src = self.choice(self.sources)
        use = self.choice(
            (
                f"sum(map({fn}, {src}))",
                f"sorted({src}, key={fn})",
                f"[{fn}(y) for y in {src}]",
                f"list(filter({fn}, {src}))",
                f"max(map({fn}, {src}), default=None)",
            )
        )
        # Often two passes with values that differ, whatever the data.
        if self.draw(st.booleans()):
            loop = self.choice(("(1, 2)", "(0, -1)", "(2, 2.0)"))
        else:
            loop = self.pipe(names, 1)
        return [
            f"for {var} in {loop}:",
            f"    def {fn}({params}):",
            f"        return {body}",
            f"    {name} += ({use},)",
        ]

    def rows(self, name: str, names: tuple[str, ...]) -> list[str]:
        """Values made by a comprehension, changed by the body afterwards.

        A list or dict per element, appended to or updated, or a generator
        per element, advanced: each run must start from new ones.
        """
        var = self.choice(LOOP_NAMES)
        form = self.choice(("list", "dict", "gen"))
        if form == "list":
            made = f"[[{self.element((*names, var), 1)}] for {var} in {self.pipe(names, 1)}]"
            change = "row.append(k)"
        elif form == "dict":
            made = f"[{{'v': {var}}} for {var} in {self.pipe(names, 1)}]"
            change = "row.update(v=(row['v'], k))"
        else:
            made = f"[(k for _ in {self.source()}) for {var} in {self.source()}]"
            change = "next(row, None)"
        return [
            f"{name} = {made}",
            f"for row in {name}:",
            f"    {change}",
            # A generator is not a value to return; what is left of it is.
            f"{name} = [row if type(row) is dict else list(row) for row in {name}]",
        ]

    def late(self, name: str, names: tuple[str, ...]) -> list[str]:
        """Lambdas or generators made in a comprehension's element.

        They close over its loop variable, which is one variable for the
        whole comprehension: called afterwards, all see its last value.
        """
        var = self.choice(LOOP_NAMES)
        src = self.source()
        if self.draw(st.booleans()):
            other = self.choice([n for n in LOOP_NAMES if n != var])
            body = self.expr((*names, var, other), 1)
            made = f"[({body} for {other} in {self.source()}) for {var} in {src}]"
            return [f"{name} = {made}", f"{name} = [list(g) for g in {name}]"]
        body = self.expr((*names, var, "v"), 1)
        arg = self.choice((*names, *self.literals))
        made = f"[lambda v: {body} for {var} in {src}]"
        return [f"{name} = {made}", f"{name} = [f({arg}) for f in {name}]"]


@functools.cache
def _sampler(options: tuple[Any, ...]) -> Any:
    # Drawing is most of the cost of an example; a strategy made once is
    # cheaper to draw from.
    return st.sampled_from(options)


def _some(
    draw: Callable[..., Any], options: tuple[Any, ...], at_least: int = 1
) -> tuple[Any, ...]:
    """Each of ``options`` with even odds, and at least ``at_least`` of them."""
    # One draw for all of them: drawing is most of the cost of an example.
    mask = draw(_masks(len(options), at_least))
    return tuple(o for i, o in enumerate(options) if mask >> i & 1)


@functools.cache
def _masks(n: int, at_least: int) -> Any:
    return st.integers(0, 2**n - 1).filter(lambda m: m.bit_count() >= at_least)


def _program(lines: list[str], is_async: bool = False) -> str:
    body = "\n".join("        " + line for line in lines)
    head = "async def" if is_async else "def"
    return f"def make(k):\n    {head} q():\n{body}\n    return q\n"


# --- the namespaces ---------------------------------------------------------------


def _tag(v: Any) -> Any:
    """Body of the query ``tag`` the bodies call: its argument is its key."""
    return (type(v).__name__, v)


def _plain_tag(v: Any) -> Any:
    hash(v)  # the engine keys the call by its argument, so it raises there too
    return _tag(v)


def _ntag(push: Callable[[Any], None]) -> Callable[[Any], Any]:
    """Body of the query ``ntag``: ``tag`` that also pushes an effect."""

    def ntag(v: Any) -> Any:
        push(("t", v))
        return _tag(v)

    return ntag


def _plain_ntag(push: Callable[[Any], None]) -> Callable[[Any], Any]:
    def ntag(v: Any) -> Any:
        hash(v)
        push(("t", v))
        return _tag(v)

    return ntag


async def _atag(v: Any) -> Any:
    """Body of the async query ``atag`` that async bodies await."""
    await asyncio.sleep(0)
    return v


async def _plain_atag(v: Any) -> Any:
    hash(v)
    await asyncio.sleep(0)
    return v


async def _agen(source: Any) -> Any:
    """An async iterable over ``source``, for ``async for``."""
    for v in source:
        await asyncio.sleep(0)
        yield v


def _helpers(push: Callable[[Any], None]) -> dict[str, Any]:
    def noted(v: Any) -> Any:
        push(v)
        return v

    def noted_odd(v: Any) -> Any:
        # Pushes for some elements only: a pipeline that has never pushed
        # stays incremental until one of these arrives.
        if type(v) is int and v % 2:
            push(("odd", v))
        return v

    def seen(v: Any) -> bool:
        push(("f", v))
        return bool(v)

    def keyed(v: Any) -> Any:
        push(("k", v))
        return v

    return {
        "inc": lambda v: v + 1,
        "neg": lambda v: -v,
        "half": lambda v: v // 2,
        "kind": lambda v: type(v).__name__,
        "is_odd": lambda v: v % 2 == 1,
        "is_pos": lambda v: v > 0,
        "is_int": lambda v: type(v) is int,
        "nan": NAN,
        "inf": INF,
        "push": push,
        "noted": noted,
        "noted_odd": noted_odd,
        "seen": seen,
        "keyed": keyed,
        "agen": _agen,
    }


_counter = Counter[str]()


def _filename() -> str:
    _counter["n"] += 1
    return f"<cascade-generated-{_counter['n']}>"


def _compile(
    source: str, namespace: dict[str, Any], k: Any, filename: str | None = None
) -> Callable[[], Any]:
    """Compile ``source`` so that ``inspect.getsource`` finds it."""
    filename = filename or _filename()
    linecache.cache[filename] = (
        len(source),
        None,
        source.splitlines(keepends=True),
        filename,
    )
    exec(compile(source, filename, "exec"), namespace)  # noqa: S102
    return namespace["make"](k)


# --- exact comparison -----------------------------------------------------------


def _canon(value: Any, signed: bool = False) -> Any:
    """A value equal only for results no code could tell apart.

    Object identity aside, and the sign of a NaN unless ``signed``: CPython
    itself gives ``inf - inf`` and ``nan + nan`` either sign, from one call
    of a function to the next, as its bytecode is specialized (so a loop
    ``a += x`` over ``[nan, inf, -inf]`` does). The engine keeps a NaN's
    sign, in digests and on disk; ``signed`` compares it.
    """
    kind = type(value)
    if kind is float:
        if value != value and not signed:
            return ("float", "nan")
        return ("float", repr(value), math.copysign(1.0, value))
    if kind is list or kind is tuple:
        return (kind.__name__, tuple(_canon(v, signed) for v in value))
    if kind is set or kind is frozenset:
        # Counted: a set can hold several NaN objects.
        items = Counter(_canon(v, signed) for v in value).items()
        return (kind.__name__, frozenset(items))
    if kind is dict:
        items = value.items()
        return ("dict", tuple((_canon(k, signed), _canon(v, signed)) for k, v in items))
    return (kind.__name__, value)


def _outcome(fn: Callable[[], Any], loop: Any = None) -> Any:
    """What ``fn()`` gives; awaited on ``loop`` if it gives an awaitable.

    Also the same with the signs of NaNs: ``(outcome, signed)``.
    """
    try:
        if loop is None:
            value = fn()
        else:
            value = loop.run_until_complete(_awaited(fn))
    except Exception as exc:  # noqa: BLE001
        outcome = ("raises", type(exc).__name__, str(exc))
        return outcome, outcome
    return ("value", _canon(value)), ("value", _canon(value, signed=True))


async def _awaited(fn: Callable[[], Any]) -> Any:
    # Called on the running loop, as an async caller calls a query.
    value = fn()
    if inspect.isawaitable(value):
        value = await value
    return value


# --- the data -------------------------------------------------------------------


@functools.cache
def _changes(family: Family) -> dict[str, Any]:
    """Each kind of change to a collection, by name."""
    values, hashables, keys = family.values, family.hashables, family.keys
    index = st.integers(-4, 4)
    lists = st.sampled_from(("xs", "ys"))

    def on_list(*parts: Any) -> Any:
        return st.tuples(lists, st.tuples(*parts))

    def on(target: str, *parts: Any) -> Any:
        return st.tuples(st.just(target), st.tuples(*parts))

    return {
        "list append": on_list(st.just("append"), values),
        "list insert": on_list(st.just("insert"), index, values),
        # In place: an update, not an insert.
        "list set": on_list(st.just("set"), index, values),
        "list del": on_list(st.just("del"), index),
        "list pop": on_list(st.just("pop")),
        "list remove": on_list(st.just("remove"), values),
        "list extend": on_list(st.just("extend"), st.lists(values, max_size=3)),
        "list reverse": on_list(st.just("reverse")),
        "list sort": on_list(st.just("sort")),
        "list clear": on_list(st.just("clear")),
        "set add": on("s", st.just("add"), hashables),
        "set discard": on("s", st.just("discard"), hashables),
        "set update": on("s", st.just("update"), st.lists(hashables, max_size=3)),
        "set difference": on(
            "s", st.just("difference_update"), st.lists(hashables, max_size=3)
        ),
        # Few keys: mostly one that is there already, so a value replaced.
        "dict set": on("d", st.just("set"), keys, values),
        "dict del": on("d", st.just("del"), keys),
        "dict pop": on("d", st.just("pop"), keys),
        "dict update": on(
            "d", st.just("update"), st.dictionaries(keys, values, max_size=2)
        ),
        "dict clear": on("d", st.just("clear")),
    }


STEP_KINDS = ("factor", "snapshot", "transaction", "restart")


def _steps(draw: Callable[..., Any], family: Family, disk: bool) -> list[Any]:
    """What happens after the first call: some kinds of step only (swarm)."""
    by_target: dict[str, list[str]] = {}
    for name in _changes(family):
        by_target.setdefault(name.split()[0], []).append(name)
    # A value replaced in place (``xs[i] = v``, ``d[k] = v``) is always
    # among them: it is the change a pipeline most easily reads as another.
    changes = tuple(
        name
        for target in _some(draw, tuple(by_target))
        for name in dict.fromkeys(
            (f"{target} set", *_some(draw, tuple(by_target[target])))
        )
        if name in by_target[target]
    )
    kinds = _some(draw, STEP_KINDS[:-1], at_least=0) + (STEP_KINDS[-1:] if disk else ())
    step = _step(family, changes, kinds)
    return draw(st.lists(step, min_size=2, max_size=12), label="steps")


@functools.cache
def _step(family: Family, changes: tuple[str, ...], kinds: tuple[str, ...]) -> Any:
    by_name = _changes(family)
    # A value replaced in place counts twice.
    change = st.one_of(
        *(by_name[name] for name in changes),
        *(by_name[name] for name in changes if name.endswith(" set")),
    )
    steps = {
        # Often a value equal to the one before, of another type.
        "factor": st.tuples(
            st.just("factor"), st.one_of(family.values, st.sampled_from(EQUAL_VALUES))
        ),
        "snapshot": st.tuples(st.just("snapshot")),
        # Changes made in one transaction, kept or rolled back as a whole.
        "transaction": st.tuples(
            st.just("transaction"),
            st.lists(change, min_size=1, max_size=4),
            st.booleans(),
        ),
        "restart": st.tuples(st.just("restart")),
    }
    # Mostly changes: the other kinds of step are there to come between them.
    return st.one_of(
        change,
        change,
        change,
        *(steps[name] for name in kinds),
        *(steps[name] for name in kinds if name == "factor"),
    )


# One example in five runs on a disk backend.
BACKEND = st.sampled_from((*[None] * 8, "mdbx", "sqlite"))


class _Rollback(Exception):
    pass


class _SetModel(dict):
    """The plain side's set: a dict, for its insertion order.

    ``in`` raises what a set raises for an unhashable value; from 3.14 a
    dict's message is another ("as a dict key", not "as a set element").
    """

    def __contains__(self, value: object) -> bool:
        frozenset().__contains__(value)
        return dict.__contains__(self, value)


def _apply(target: str, op: Any, live: dict[str, Any], model: dict[str, Any]) -> None:
    """One change, to the collection and to its plain model."""
    if target in ("xs", "ys"):
        before = list(model[target])
        raised = _apply_list_op(model[target], op)
        assert _apply_list_op(live[target], op) is raised
        if raised is not None:
            # A mutator that raises changes nothing; a list's ``sort`` may
            # leave it part sorted.
            model[target][:] = before
    elif target == "s":
        getattr(live["s"], op[0])(*op[1:])
        _apply_to_ordered_model(model["s"], op)
    else:
        assert _apply_dict_op(live["d"], op) is _apply_dict_op(model["d"], op)


class _Body(NamedTuple):
    source: str
    filename: str
    is_async: bool


class _Live:
    """The engine side of one example: collections, the input and the queries.

    ``initial`` None reopens what an earlier engine left on disk.
    """

    def __init__(
        self,
        bodies: list[_Body],
        k: Any,
        disk: tuple[str, str] | None,
        initial: dict[str, Any] | None,
    ) -> None:
        if disk is None:
            self.engine = Engine()
        else:
            self.engine = Engine(cache_dir=disk[1], cache_backend=disk[0])
        engine = self.engine
        contents = initial or {}
        self.collections: dict[str, Any] = {}
        for name, kind in (
            ("xs", CascadeList),
            ("ys", CascadeList),
            ("s", CascadeSet),
            ("d", CascadeDict),
        ):
            args = [contents[name]] if name in contents else []
            named = {} if disk is None else {"name": name}
            self.collections[name] = kind(engine, *args, **named)

        @engine.input(name="factor")
        def factor() -> Any:
            return 1

        self.factor = factor
        push = engine.accumulator(EFFECTS).push
        shared = {
            "factor": factor,
            "tag": engine.query(_tag, name="tag"),
            "ntag": engine.query(_ntag(push), name="ntag"),
            "atag": engine.query(_atag, name="atag"),
        }
        self.queries = []
        for i, body in enumerate(bodies):
            namespace = _helpers(push) | self.collections | shared
            fn = _compile(body.source, namespace, k, body.filename)
            self.queries.append(engine.query(fn, name=f"q{i}"))
        runs = self.p_runs = [0]
        queries = tuple(zip(self.queries, [body.is_async for body in bodies]))

        if any(body.is_async for body in bodies):

            async def p() -> Any:
                runs[0] += 1
                out = []
                for q, is_async in queries:
                    out.append(await q() if is_async else q())
                return ("p", *out)

        else:

            def p() -> Any:
                runs[0] += 1
                return ("p", *(q() for q, _ in queries))

        self.p = engine.query(p, name="p")


# The accumulator the bodies push to.
EFFECTS = "log"


def _contents(collections: dict[str, Any]) -> Any:
    return _canon(
        (
            list(collections["xs"]),
            list(collections["ys"]),
            list(collections["s"]),
            dict(collections["d"]),
        ),
        signed=True,
    )


def _model_contents(model: dict[str, Any]) -> Any:
    return _canon((model["xs"], model["ys"], list(model["s"]), model["d"]), signed=True)


# Bodies per example: they share the data, the changes and the engine.
BODIES = 2


@settings(
    # CASCADE_PROGRAM_EXAMPLES raises it for a long local run.
    max_examples=int(os.environ.get("CASCADE_PROGRAM_EXAMPLES", "300")),
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)
@given(data=st.data())
def test_generated_query_bodies_equal_plain_python(data: Any) -> None:
    family = data.draw(FAMILY, label="family")
    written = []
    for _ in range(BODIES):
        # One body in four is a coroutine.
        is_async = data.draw(st.integers(0, 3), label="async") == 0
        writer = _Writer(data.draw, family.literals, is_async=is_async)
        source = _program(writer.body(), is_async)
        note(source)
        try:
            compile(source, "<check>", "exec")
        except SyntaxError:
            # ``:=`` where Python does not allow it; such a body is not a query.
            assume(False)
        if writer.walrus_used:
            event("uses :=")
        for feature in writer.features:
            event(f"uses {feature}")
        if is_async:
            event("async")
        written.append((source, is_async))
    backend = data.draw(BACKEND, label="backend")
    if backend is not None:
        family = family.on_disk()
    values = family.values
    k = data.draw(st.one_of(st.integers(-1, 2), values), label="k")
    # Not empty, mostly: a change has something to replace or remove.
    xs0 = data.draw(st.lists(values, min_size=2, max_size=8), label="xs")
    ys0 = data.draw(st.lists(values, max_size=4), label="ys")
    s0 = data.draw(st.lists(family.hashables, min_size=1, max_size=4), label="s")
    d0 = data.draw(
        st.dictionaries(family.keys, values, min_size=2, max_size=4), label="d"
    )
    steps = _steps(data.draw, family, backend is not None)
    event(f"family: {family.name}")
    event(f"backend: {backend or 'memory'}")

    directory = tempfile.mkdtemp(prefix="cascade-programs-") if backend else ""
    disk = (backend, directory) if backend else None
    bodies = [_Body(source, _filename(), is_async) for source, is_async in written]
    initial = {"xs": xs0, "ys": ys0, "s": s0, "d": d0}
    # Async bodies are awaited on a running loop, one per example.
    loop = asyncio.new_event_loop() if any(b.is_async for b in bodies) else None
    opened = [_Live(bodies, k, disk, initial)]
    try:
        _run(bodies, k, disk, opened, initial, steps, loop)
    finally:
        if disk is not None:
            for live in opened:
                live.engine.shutdown()
        if directory:
            shutil.rmtree(directory, ignore_errors=True)
        if loop is not None:
            loop.close()


def _plain_namespace(
    data: dict[str, Any], factor: Callable[[], Any], log: list[Any]
) -> dict[str, Any]:
    """The plain side's names: ``data`` holds the collections' models."""
    return (
        _helpers(log.append)
        | data
        | {
            "factor": factor,
            "tag": _plain_tag,
            "ntag": _plain_ntag(log.append),
            "atag": _plain_atag,
        }
    )


def _run(
    bodies: list[_Body],
    k: Any,
    disk: tuple[str, str] | None,
    opened: list[_Live],
    initial: dict[str, Any],
    steps: list[Any],
    loop: Any,
) -> None:
    live = opened[0]
    sources = [body.source for body in bodies]
    loops = [loop if body.is_async else None for body in bodies]
    model = {
        "xs": list(initial["xs"]),
        "ys": list(initial["ys"]),
        "s": _SetModel.fromkeys(initial["s"]),
        "d": dict(initial["d"]),
        "factor": 1,
    }
    # What the plain functions push; cleared before each call.
    log: list[Any] = []
    plain_ns = _plain_namespace(
        {name: model[name] for name in ("xs", "ys", "s", "d")},
        lambda: model["factor"],
        log,
    )
    plains = [_compile(source, dict(plain_ns), k) for source in sources]
    for body, query in zip(bodies, live.queries):
        rewritten = getattr(query.raw, "__cascade_rewritten__", False)
        event("rewritten" if rewritten else "left as written")
        # The rewriter leaves async queries as written (G22).
        assert not (body.is_async and rewritten), body.source

    # What the plain function pushed on its last call, raising or not, and
    # what the last live call gave, with the signs of NaNs.
    pushed: list[Any] = [None]
    exact: list[Any] = [None]

    def plain_call(plain: Callable[[], Any], on: Any) -> Any:
        """The plain outcome and its effects: none when it raises."""
        log.clear()
        outcome, _ = _outcome(plain, on)
        pushed[0] = list(log)
        return outcome, _canon(list(log) if outcome[0] == "value" else [])

    def live_call(call: Callable[..., Any], on: Any) -> Any:
        effects: dict[str, list[Any]] = {}
        outcome, signed = _outcome(lambda: call(effects=effects), on)
        assert set(effects) <= {EFFECTS}, effects
        exact[0] = (signed, _canon(effects.get(EFFECTS, []), signed=True))
        return outcome, _canon(effects.get(EFFECTS, []))

    last: list[Any] = [None]

    def check(step: Any) -> None:
        wants = []
        seen = []
        for source, plain, query, on in zip(sources, plains, live.queries, loops):
            want = plain_call(plain, on)
            got = live_call(query, on)
            if want[0][0] == "value":
                seen.append(exact[0])
            elif _canon(pushed[0], signed=True) != _canon(pushed[0]):
                # Raised, and pushed a NaN: the live call's effects, which a
                # caller would collect, cannot be seen, nor their signs.
                seen.append(None)
            else:
                seen.append((want[0], _canon(pushed[0])))
            assert got == want, (
                f"after {step!r}:\n{source}\n"
                f"rewritten: {got[0]}\n           effects {got[1]}\n"
                f"plain:     {want[0]}\n           effects {want[1]}"
            )
            # Called again it is a cache hit, which replays the effects.
            again = live_call(query, on)
            assert again == want, (
                f"after {step!r}, a cache hit:\n{source}\n"
                f"rewritten: {again[0]}\n           effects {again[1]}\n"
                f"plain:     {want[0]}\n           effects {want[1]}"
            )
            wants.append(want)
        runs = live.p_runs[0]
        got_p = live_call(live.p, loop)
        raised = [want for want, _ in wants if want[0] == "raises"]
        if raised:
            want_p: Any = (raised[0], _canon([]))
        else:
            value = ("tuple", (("str", "p"), *(w[0][1] for w in wants)))
            effects = ("list", tuple(e for w in wants for e in w[1][1]))
            want_p = (("value", value), effects)
        assert got_p == want_p, (
            f"after {step!r}: the query over the bodies gives\n{got_p}\nnot\n{want_p}"
        )
        # Early cutoff: a caller runs again only when a result it read, or
        # the effects that came with it, changed. (That it does run then is
        # the check just above.) The effects of a call that raises count: a
        # caller that catches the error collects them. A read through a
        # snapshot replaces the cached result with an older one, so the
        # next run at the head compares against that (G4).
        # The live results are compared with their NaN signs: a NaN made by
        # arithmetic may change sign when the query runs again, and so its
        # digest, and the caller runs again for it.
        unknown = last[0] is None or None in last[0] or None in seen
        assert live.p_runs[0] == runs or unknown or last[0] != seen, (
            f"after {step!r}: the query over the bodies ran again, but they "
            f"still give\n{seen}\n" + "\n".join(sources)
        )
        last[0] = seen

    check("start")
    # A snapshot and the plain functions over copies of the data it froze.
    held: tuple[Any, list[Callable[[], Any]]] | None = None
    for step in steps:
        target, *rest = step
        op: Any = rest[0] if rest else None
        if target == "snapshot":
            if held is not None:
                held[0].release()
            value = model["factor"]
            frozen_ns = _plain_namespace(
                {
                    "xs": list(model["xs"]),
                    "ys": list(model["ys"]),
                    "s": _SetModel(model["s"]),
                    "d": dict(model["d"]),
                },
                lambda value=value: value,
                log,
            )
            held = (
                live.engine.snapshot(),
                [_compile(source, dict(frozen_ns), k) for source in sources],
            )
        elif target == "factor":
            live.factor.set(value=op)
            model["factor"] = op
        elif target == "restart":
            assert disk is not None
            if held is not None:
                held[0].release()
                held = None
            live.engine.shutdown()
            live = _Live(bodies, k, disk, None)
            opened.append(live)
            collections = live.collections
            # What comes back from disk is what was stored, down to types.
            reopened = _contents(collections)
            assert reopened == _model_contents(model), (
                f"reopened: {reopened}\nstored:   {_model_contents(model)}"
            )
            # The plain side takes the decoded objects, so that a NaN is one
            # object on both sides again.
            model["xs"][:] = collections["xs"]
            model["ys"][:] = collections["ys"]
            model["s"].clear()
            model["s"].update(dict.fromkeys(collections["s"]))
            model["d"].clear()
            model["d"].update(collections["d"])
            # An input's value is not kept on disk.
            live.factor.set(value=model["factor"])
            last[0] = None
        elif target == "transaction":
            changes, commit = rest
            scratch = {
                "xs": list(model["xs"]),
                "ys": list(model["ys"]),
                "s": dict(model["s"]),
                "d": dict(model["d"]),
            }
            try:
                with live.engine.transaction():
                    for name, change in changes:
                        _apply(name, change, live.collections, scratch)
                    if not commit:
                        raise _Rollback
            except _Rollback:
                pass
            else:
                # The plain namespace holds the model's objects: change them
                # in place.
                model["xs"][:] = scratch["xs"]
                model["ys"][:] = scratch["ys"]
                model["s"].clear()
                model["s"].update(scratch["s"])
                model["d"].clear()
                model["d"].update(scratch["d"])
        else:
            _apply(target, op, live.collections, model)
        assert _contents(live.collections) == _model_contents(model), step
        check(step)
        if held is not None:
            snapshot, frozen = held
            for source, frozen_plain, query, on in zip(
                sources, frozen, live.queries, loops
            ):
                want = plain_call(frozen_plain, on)
                got = live_call(lambda q=query, **kw: q(snapshot=snapshot, **kw), on)
                assert got == want, (
                    f"after {step!r}, through the snapshot:\n{source}\n"
                    f"rewritten: {got[0]}\n           effects {got[1]}\n"
                    f"plain:     {want[0]}\n           effects {want[1]}"
                )
            last[0] = None
    if held is not None:
        held[0].release()


def test_generated_bodies_are_mostly_rewritten() -> None:
    """The grammar must reach the rewriter, or the test above checks little.

    Async bodies are left as written, every one of them (G22).
    """
    seen = Counter[tuple[bool, bool]]()

    @settings(max_examples=200, deadline=None, database=None)
    @given(data=st.data())
    def count(data: Any) -> None:
        family = data.draw(FAMILY)
        is_async = data.draw(st.integers(0, 3)) == 0
        writer = _Writer(data.draw, family.literals, is_async=is_async)
        source = _program(writer.body(), is_async)
        try:
            compile(source, "<check>", "exec")
        except SyntaxError:
            return
        engine = Engine()
        push = engine.accumulator(EFFECTS).push
        namespace = _helpers(push) | {
            "xs": CascadeList(engine, []),
            "ys": CascadeList(engine, []),
            "s": CascadeSet(engine, []),
            "d": CascadeDict(engine, {}),
            "factor": engine.input(name="factor")(lambda: 1),
            "tag": engine.query(_tag, name="tag"),
            "ntag": engine.query(_ntag(push), name="ntag"),
            "atag": engine.query(_atag, name="atag"),
        }
        query = engine.query(_compile(source, namespace, 1), name="q")
        rewritten = bool(getattr(query.raw, "__cascade_rewritten__", False))
        seen[is_async, rewritten] += 1

    count()
    sync = seen[False, True] + seen[False, False]
    assert seen[False, True] >= 0.8 * sync, seen
    assert seen[True, False] > 0 and not seen[True, True], seen


def test_literals_evaluate_to_the_values_they_stand_for() -> None:
    namespace = _helpers([].append)
    for family in FAMILIES:
        for value in family.literals:
            assert _canon(eval(_lit(value), namespace)) == _canon(value)  # noqa: S307


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
