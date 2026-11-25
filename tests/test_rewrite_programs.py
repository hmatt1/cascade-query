"""Generated query bodies against plain Python (G22, test gap 4).

``test_rewrite_differential.py`` checks each reducer in a body written by
hand. Here Hypothesis writes the bodies: several rewritten expressions in one
query, comprehensions inside the elements and conditions of others (also
inside a two-``for`` comprehension that stays as written), ``:=``, lambdas,
closures, plain ``for`` loops and ``if`` statements, locals used as sources,
an input read inside an element, and elements that raise.

Each body is compiled twice from the same source: once in a namespace of
cascade collections and an engine input, registered as a query (and so
rewritten), and once in a namespace of plain lists, an insertion-ordered dict
for the set and a plain dict, called as is. After every change to the
collections and to the input, the query must give what the plain function
gives: the same value down to types, or the same exception with the same
message.
"""

from __future__ import annotations

import linecache
import os
from collections import Counter
from typing import Any, Callable

import pytest
from hypothesis import HealthCheck, assume, event, given, note, settings
from hypothesis import strategies as st

from cascade import CascadeDict, CascadeList, CascadeSet, Engine
from tests.test_rewrite_differential import (
    _apply_dict_op,
    _apply_list_op,
    _apply_to_ordered_model,
    _outcome,
)

# --- the grammar ----------------------------------------------------------------

LOOP_NAMES = ("x", "y", "z")
LIST_SOURCES = ("xs", "ys")
SOURCES = ("xs", "ys", "s", "d", "d.values()", "d.keys()")
HELPERS = ("inc", "neg", "half")
PREDICATES = ("is_odd", "is_pos")
MAX_DEPTH = 2


class _Writer:
    """Draws the source text of one query body.

    ``ints`` are the names in scope that hold an int: loop variables, the
    closure variable ``k`` and locals assigned earlier; ``lists`` are locals
    that hold a plain list of ints and can be a source.
    """

    def __init__(self, draw: Callable[..., Any]) -> None:
        self.draw = draw
        self.n_locals = 0
        self.n_walrus = 0
        self.lists: list[str] = []
        self.walrus_used = False

    def choice(self, options: Any) -> Any:
        return self.draw(st.sampled_from(options))

    # --- expressions ---

    def int_expr(self, ints: tuple[str, ...], depth: int) -> str:
        kinds = ["name", "literal", "binop", "factor", "cond"]
        if depth < MAX_DEPTH:
            kinds += ["reduce", "reduce", "reduce"]
        kind = self.choice(kinds)
        if kind == "name" and ints:
            return self.choice(ints)
        if kind == "binop":
            op = self.choice(("+", "-", "*", "//", "%"))
            left = self.int_expr(ints, depth + 1)
            if op in ("//", "%") and len(ints) > 1 and self.draw(st.booleans()):
                # By the innermost name, often a loop variable: an element
                # that raises for some values only, the case where lazy and
                # eager evaluation differ (D37).
                right = ints[-1]
            else:
                right = self.int_expr(ints, depth + 1)
            return f"({left} {op} {right})"
        if kind == "factor":
            return "factor()"
        if kind == "cond":
            return (
                f"({self.int_expr(ints, depth + 1)} if {self.cond(ints, depth + 1)}"
                f" else {self.int_expr(ints, depth + 1)})"
            )
        if kind == "reduce":
            return self.int_reduce(ints, depth + 1)
        return str(self.draw(st.integers(-2, 3)))

    def cond(self, ints: tuple[str, ...], depth: int) -> str:
        kind = self.choice(("cmp", "pred", "in", "any_all"))
        if kind == "pred" and ints:
            return f"{self.choice(PREDICATES)}({self.choice(ints)})"
        if kind == "in":
            source = self.choice((*SOURCES, *self.lists))
            return f"{self.int_expr(ints, depth + 1)} in {source}"
        if kind == "any_all" and depth < MAX_DEPTH:
            name = self.choice(("any", "all"))
            # A list comprehension here is built in full first (D37).
            inner = self.pipe(ints, depth + 1, generator=self.draw(st.booleans()))
            return f"{name}({inner})"
        op = self.choice(("<", ">", "==", "!="))
        left = self.int_expr(ints, depth + 1)
        return f"{left} {op} {self.int_expr(ints, depth + 1)}"

    def element(self, ints: tuple[str, ...], depth: int) -> str:
        """An element or ``if`` clause body, sometimes with ``:=``."""
        expr = self.int_expr(ints, depth)
        if self.draw(st.integers(0, 9)) == 0:
            self.n_walrus += 1
            self.walrus_used = True
            return f"(w{self.n_walrus} := {expr})"
        return expr

    def source(self) -> str:
        return self.choice((*SOURCES, *self.lists))

    def pipe(
        self, ints: tuple[str, ...], depth: int, *, generator: bool = False
    ) -> str:
        """An iterable of ints: a source, or stages over one."""
        kinds = ["source", "comp", "comp", "map", "filter", "reversed"]
        if depth < MAX_DEPTH:
            kinds += ["nested", "two_for"]
        kind = self.choice(kinds)
        if kind == "source":
            return self.source()
        if kind == "reversed":
            return f"reversed({self.choice((*LIST_SOURCES, *self.lists))})"
        if kind == "map":
            inner = self.pipe(ints, depth + 1)
            if self.draw(st.booleans()):
                return f"map({self.choice(HELPERS)}, {inner})"
            var = self.choice(LOOP_NAMES)
            body = self.int_expr((*ints, var), depth + 1)
            return f"map(lambda {var}: {body}, {inner})"
        if kind == "filter":
            inner = self.pipe(ints, depth + 1)
            fn = self.choice((*PREDICATES, "None"))
            return f"filter({fn}, {inner})"
        var = self.choice(LOOP_NAMES)
        inner_ints = (*ints, var)
        if kind == "two_for":
            # Stays as written. A pipeline in its element that uses its loop
            # variables must stay inside it too (D32).
            other = self.choice([n for n in LOOP_NAMES if n != var])
            both = (*ints, var, other)
            first = self.source()
            second = self.pipe(inner_ints, depth + 1)
            if self.draw(st.booleans()):
                third = next(n for n in LOOP_NAMES if n not in (var, other))
                elt = (
                    f"sum({third} * {var} for {third} in {self.source()}"
                    f" if {third} != {other})"
                )
            else:
                elt = self.element(both, depth + 1)
            return f"[{elt} for {var} in {first} for {other} in {second}]"
        iterable = self.pipe(ints, depth + 1) if kind == "nested" else self.source()
        elt = self.element(inner_ints, depth + 1)
        clause = ""
        if self.draw(st.booleans()):
            clause = f" if {self.element(inner_ints, depth + 1)}"
        if generator or self.draw(st.booleans()):
            return f"({elt} for {var} in {iterable}{clause})"
        return f"[{elt} for {var} in {iterable}{clause}]"

    def int_reduce(self, ints: tuple[str, ...], depth: int) -> str:
        reducer = self.choice(("sum", "len", "min", "max", "any", "all", "count"))
        if reducer == "len":
            var = self.choice(LOOP_NAMES)
            if self.draw(st.booleans()):
                return f"len({self.source()})"
            elt = self.element((*ints, var), depth + 1)
            return f"len([{elt} for {var} in {self.pipe(ints, depth + 1)}])"
        if reducer == "count":
            return f"sum(1 for _ in {self.pipe(ints, depth + 1)})"
        inner = self.pipe(ints, depth + 1, generator=self.draw(st.booleans()))
        if reducer in ("min", "max"):
            return f"{reducer}({inner}, default={self.draw(st.integers(-1, 1))})"
        return f"{reducer}({inner})"

    def any_reduce(self, ints: tuple[str, ...]) -> str:
        """A top-level expression of any type."""
        kind = self.choice(
            ("int", "int", "list", "sorted", "set", "setcomp", "dict", "join")
        )
        if kind == "list":
            return f"list({self.pipe(ints, 1)})"
        if kind == "sorted":
            key = self.choice(("", ", key=neg", ", key=lambda v: v % 3", ""))
            rev = self.choice(("", ", reverse=True"))
            return f"sorted({self.pipe(ints, 1)}{key}{rev})"
        if kind == "set":
            return f"set({self.pipe(ints, 1)})"
        if kind == "setcomp":
            var = self.choice(LOOP_NAMES)
            return f"{{{self.element((*ints, var), 1)} for {var} in {self.source()}}}"
        if kind == "dict":
            # Iterating ``d`` gives its keys; only ``dict(d)`` copies (D36).
            var = self.choice(LOOP_NAMES)
            elt = self.element((*ints, var), 1)
            source = self.choice(("d", "d", "d.keys()", *SOURCES, *self.lists))
            form = self.choice(("comp", "comp", "gen", "map", "items", "copy"))
            if form == "items":
                return "{p: q for p, q in d.items()}"
            if form == "copy":
                return "dict(d)"
            if form == "gen":
                return f"dict(({var}, {elt}) for {var} in {source})"
            if form == "map":
                return f"dict(map(lambda {var}: ({var}, {elt}), {source}))"
            return f"{{{var}: {elt} for {var} in {source}}}"
        if kind == "join":
            return f'",".join(map(str, {self.pipe(ints, 1)}))'
        return self.int_expr(ints, 0)

    # --- statements ---

    def body(self) -> list[str]:
        ints: tuple[str, ...] = ("k",)
        lines: list[str] = []
        others: list[str] = []
        for _ in range(self.draw(st.integers(1, 4))):
            kind = self.choice(("assign", "assign", "list", "loop", "if"))
            self.n_locals += 1
            name = f"a{self.n_locals}"
            if kind == "list":
                lines.append(f"{name} = list({self.pipe(ints, 1)})")
                self.lists.append(name)
                continue
            if kind == "loop":
                var = self.choice(LOOP_NAMES)
                lines.append(f"{name} = 0")
                lines.append(f"for {var} in {self.pipe(ints, 1)}:")
                lines.append(f"    {name} += {self.int_expr((*ints, var, name), 1)}")
            elif kind == "if":
                lines.append(f"if {self.cond(ints, 0)}:")
                lines.append(f"    {name} = {self.any_reduce(ints)}")
                lines.append("else:")
                lines.append(f"    {name} = {self.any_reduce(ints)}")
                # Of any type: returned, but not used as an int from here on.
                others.append(name)
                continue
            else:
                lines.append(f"{name} = {self.int_expr(ints, 0)}")
            ints = (*ints, name)
        results = [self.any_reduce(ints) for _ in range(self.draw(st.integers(1, 3)))]
        results += [n for n in ints if n != "k"]
        results += others
        if self.n_walrus:
            # ``:=`` in a comprehension binds in the query (D30); one that
            # never ran leaves its name unbound, in Python as here.
            names = ", ".join(f"w{i}" for i in range(1, self.n_walrus + 1))
            lines += [
                "try:",
                f"    bound = ({names},)",
                "except NameError:",
                "    bound = None",
            ]
            results.append("bound")
        lines.append(f"return ({', '.join(results)},)")
        return lines


def _program(lines: list[str]) -> str:
    body = "\n".join("        " + line for line in lines)
    return f"def make(k):\n    def q():\n{body}\n    return q\n"


# --- the namespaces ---------------------------------------------------------------


def _helpers() -> dict[str, Any]:
    return {
        "inc": lambda v: v + 1,
        "neg": lambda v: -v,
        "half": lambda v: v // 2,
        "is_odd": lambda v: v % 2 == 1,
        "is_pos": lambda v: v > 0,
    }


_counter = Counter[str]()


def _compile(source: str, namespace: dict[str, Any], k: int) -> Callable[[], Any]:
    """Compile ``source`` so that ``inspect.getsource`` finds it."""
    _counter["n"] += 1
    filename = f"<cascade-generated-{_counter['n']}>"
    linecache.cache[filename] = (
        len(source),
        None,
        source.splitlines(keepends=True),
        filename,
    )
    exec(compile(source, filename, "exec"), namespace)  # noqa: S102
    return namespace["make"](k)


# --- the data -------------------------------------------------------------------

INTS = st.integers(-3, 3)
# Floats take other paths: a float sum is re-added from the stored values,
# and -0.0 == 0 is a tie that only the builtins break the right way.
NUMBERS = st.one_of(INTS, st.sampled_from([0.5, -1.5, 0.0, -0.0, 2.0]))
VALUES = st.shared(st.sampled_from([INTS, NUMBERS]), key="values").flatmap(
    lambda family: family
)
KEYS = st.integers(0, 4)
LIST_OP = st.one_of(
    st.tuples(st.just("append"), VALUES),
    st.tuples(st.just("insert"), st.integers(-6, 6), VALUES),
    st.tuples(st.just("set"), st.integers(-6, 6), VALUES),
    st.tuples(st.just("del"), st.integers(-6, 6)),
    st.tuples(st.just("pop")),
    st.tuples(st.just("remove"), VALUES),
    st.tuples(st.just("extend"), st.lists(VALUES, max_size=3)),
    st.tuples(st.just("reverse")),
    st.tuples(st.just("sort")),
    st.tuples(st.just("clear")),
)
SET_OP = st.one_of(
    st.tuples(st.just("add"), VALUES),
    st.tuples(st.just("discard"), VALUES),
    st.tuples(st.just("update"), st.lists(VALUES, max_size=3)),
    st.tuples(st.just("difference_update"), st.lists(VALUES, max_size=3)),
)
DICT_OP = st.one_of(
    st.tuples(st.just("set"), KEYS, VALUES),
    st.tuples(st.just("del"), KEYS),
    st.tuples(st.just("pop"), KEYS),
    st.tuples(st.just("update"), st.dictionaries(KEYS, VALUES, max_size=2)),
    st.tuples(st.just("clear")),
)
STEP = st.one_of(
    st.tuples(st.sampled_from(("xs", "ys")), LIST_OP),
    st.tuples(st.just("s"), SET_OP),
    st.tuples(st.just("d"), DICT_OP),
    st.tuples(st.just("factor"), st.integers(-2, 2)),
    st.tuples(st.just("snapshot")),
)
# Changes made in one transaction, kept or rolled back as a whole.
TRANSACTION = st.tuples(
    st.just("transaction"),
    st.lists(
        st.one_of(
            st.tuples(st.sampled_from(("xs", "ys")), LIST_OP),
            st.tuples(st.just("s"), SET_OP),
            st.tuples(st.just("d"), DICT_OP),
        ),
        min_size=1,
        max_size=4,
    ),
    st.booleans(),
)


class _Rollback(Exception):
    pass


def _apply(target: str, op: Any, live: dict[str, Any], model: dict[str, Any]) -> None:
    """One change, to the collection and to its plain model."""
    if target in ("xs", "ys"):
        raised = _apply_list_op(model[target], op)
        assert _apply_list_op(live[target], op) is raised
    elif target == "s":
        getattr(live["s"], op[0])(*op[1:])
        _apply_to_ordered_model(model["s"], op)
    else:
        assert _apply_dict_op(live["d"], op) is _apply_dict_op(model["d"], op)


def _check(query: Any, plain: Callable[[], Any], source: str, step: Any) -> None:
    want = _outcome(plain)
    got = _outcome(query)
    assert got == want, (
        f"after {step!r}:\n{source}\nrewritten: {got}\nplain:     {want}"
    )


@settings(
    # CASCADE_PROGRAM_EXAMPLES raises it for a long local run.
    max_examples=int(os.environ.get("CASCADE_PROGRAM_EXAMPLES", "300")),
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)
@given(data=st.data())
def test_generated_query_bodies_equal_plain_python(data: Any) -> None:
    writer = _Writer(data.draw)
    lines = writer.body()
    source = _program(lines)
    note(source)
    try:
        compile(source, "<check>", "exec")
    except SyntaxError:
        # ``:=`` where Python does not allow it; such a body is not a query.
        assume(False)
    k = data.draw(st.integers(-1, 2), label="k")
    xs0 = data.draw(st.lists(VALUES, max_size=5), label="xs")
    ys0 = data.draw(st.lists(VALUES, max_size=4), label="ys")
    s0 = data.draw(st.lists(VALUES, max_size=4), label="s")
    d0 = data.draw(st.dictionaries(KEYS, VALUES, max_size=4), label="d")
    steps = data.draw(st.lists(st.one_of(STEP, TRANSACTION), max_size=8), label="steps")

    engine = Engine()
    xs, ys = CascadeList(engine, xs0), CascadeList(engine, ys0)
    s, d = CascadeSet(engine, s0), CascadeDict(engine, d0)

    @engine.input(name="factor")
    def factor() -> int:
        return 1

    model = {
        "xs": list(xs0),
        "ys": list(ys0),
        "s": dict.fromkeys(s0),
        "d": dict(d0),
        "factor": 1,
    }
    live = _helpers() | {"xs": xs, "ys": ys, "s": s, "d": d, "factor": factor}
    plain_ns = _helpers() | {
        "xs": model["xs"],
        "ys": model["ys"],
        "s": model["s"],
        "d": model["d"],
        "factor": lambda: model["factor"],
    }
    query = engine.query(_compile(source, live, k), name="q")
    plain = _compile(source, plain_ns, k)
    rewritten = getattr(query.raw, "__cascade_rewritten__", False)
    event("rewritten" if rewritten else "left as written")
    if writer.walrus_used:
        event("uses :=")

    _check(query, plain, source, "start")
    event(f"first outcome: {_outcome(plain)[0]}")
    # A snapshot and the plain function over copies of the data it froze.
    held: tuple[Any, Callable[[], Any]] | None = None
    for step in steps:
        target, *rest = step
        op: Any = rest[0] if rest else None
        if target == "snapshot":
            if held is not None:
                held[0].release()
            frozen = {
                "xs": list(model["xs"]),
                "ys": list(model["ys"]),
                "s": dict(model["s"]),
                "d": dict(model["d"]),
            }
            value = model["factor"]
            frozen_ns = _helpers() | frozen | {"factor": lambda value=value: value}
            held = (engine.snapshot(), _compile(source, frozen_ns, k))
        elif target == "factor":
            factor.set(value=op)
            model["factor"] = op
        elif target == "transaction":
            changes, commit = rest
            scratch = {
                "xs": list(model["xs"]),
                "ys": list(model["ys"]),
                "s": dict(model["s"]),
                "d": dict(model["d"]),
            }
            try:
                with engine.transaction():
                    for name, change in changes:
                        _apply(name, change, live, scratch)
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
            for name in ("xs", "ys"):
                assert list(live[name]) == model[name], name
            assert list(s) == list(model["s"])
            assert dict(d) == model["d"]
        else:
            _apply(target, op, live, model)
        _check(query, plain, source, step)
        if held is not None:
            snapshot, frozen_plain = held
            _check(
                lambda snapshot=snapshot: query(snapshot=snapshot),
                frozen_plain,
                source,
                (step, "through the snapshot"),
            )
    if held is not None:
        held[0].release()


def test_generated_bodies_are_mostly_rewritten() -> None:
    """The grammar must reach the rewriter, or the test above checks little."""
    seen = Counter[bool]()

    @settings(max_examples=200, deadline=None, database=None)
    @given(data=st.data())
    def count(data: Any) -> None:
        writer = _Writer(data.draw)
        source = _program(writer.body())
        try:
            compile(source, "<check>", "exec")
        except SyntaxError:
            return
        engine = Engine()
        namespace = _helpers() | {
            "xs": CascadeList(engine, []),
            "ys": CascadeList(engine, []),
            "s": CascadeSet(engine, []),
            "d": CascadeDict(engine, {}),
            "factor": engine.input(name="factor")(lambda: 1),
        }
        query = engine.query(_compile(source, namespace, 1), name="q")
        seen[bool(getattr(query.raw, "__cascade_rewritten__", False))] += 1

    count()
    assert seen[True] >= 0.8 * sum(seen.values()), seen


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
