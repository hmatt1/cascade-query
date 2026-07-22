from __future__ import annotations

import traceback

import pytest

from cascade import CascadeDict, CascadeList, Engine


@pytest.fixture
def engine():
    return Engine()


def rewritten(handle):
    return getattr(handle.raw, "__cascade_rewritten__", False)


# --- requirement 1: opt-out configuration ---


def test_global_default_rewrites(engine):
    xs = CascadeList(engine, [1])

    @engine.query
    def q():
        return sum(x for x in xs)

    assert rewritten(q)


def test_global_opt_out():
    off = Engine(incremental=False)
    xs = CascadeList(off, [1])

    @off.query
    def q():
        return sum(x for x in xs)

    assert not rewritten(q)
    assert q() == 1
    xs.append(2)
    assert q() == 3  # read tracking still invalidates


def test_per_query_opt_out_and_force(engine):
    off = Engine(incremental=False)
    xs = CascadeList(engine, [1])
    ys = CascadeList(off, [2])

    @engine.query(incremental=False)
    def a():
        return sum(x for x in xs)

    @off.query(incremental=True)
    def b():
        return sum(y for y in ys)

    assert not rewritten(a) and rewritten(b)
    assert a() == 1 and b() == 2


# --- requirement 2: transparent interception of standard python ---


def test_metadata_preserved(engine):
    @engine.query
    def documented(a, b=3, *, c=4):
        "docstring survives"
        return sum(x for x in [a, b, c])

    assert rewritten(documented)
    fn = documented.raw
    assert fn.__doc__ == "docstring survives"
    assert fn.__name__ == "documented"
    assert "documented" in fn.__qualname__
    assert fn.__defaults__ == (3,)
    assert fn.__kwdefaults__ == {"c": 4}
    assert documented(1) == 8
    assert fn(1, 2, c=10) == 13


def test_closure_late_binding_preserved(engine):
    xs = CascadeList(engine, [1, 2])
    factor = 10

    @engine.query(memoize=False)
    def scaled():
        return sum(x * factor for x in xs)

    assert rewritten(scaled)
    assert scaled() == 30
    factor = 100
    assert scaled() == 300


def test_method_in_class_rewrites(engine):
    xs = CascadeList(engine, [1, 2, 3])

    class Service:
        def total(self):
            return sum(x for x in xs)

    svc = Service()
    q = engine.query(Service.total)
    assert rewritten(q)
    assert q(svc) == 6


def test_annotations_preserved_as_metadata(engine):
    xs = CascadeList(engine, [1])

    @engine.query
    def typed(scale: int = 2) -> int:
        return sum(x * scale for x in xs)

    assert rewritten(typed)
    # this module uses future annotations, so the originals are strings;
    # the rewrite must carry them over unchanged
    assert typed.raw.__annotations__ == {"scale": "int", "return": "int"}
    assert typed() == 2


def test_comprehensions_in_every_statement_position(engine):
    xs = CascadeList(engine, [1, 2, 3, 4])

    @engine.query
    def q(flag=True):
        counts = [len([x for x in xs if x > i]) for i in [0]]
        if sum(x for x in xs) > 5:
            with_total = sum(x * 2 for x in xs)
        else:
            with_total = 0
        while len([x for x in xs if x > 100]) > 0:
            break
        try:
            inner = min(x for x in xs)
        except ValueError:
            inner = None
        acc = 0
        for _ in range(2):
            acc += max(x for x in xs)
        return (counts, with_total, inner, acc, [x for x in xs if flag])

    assert rewritten(q)
    assert q() == ([4], 20, 1, 8, [1, 2, 3, 4])
    xs.append(10)
    assert q() == ([5], 40, 1, 20, [1, 2, 3, 4, 10])


def test_tuple_targets_and_nested_comprehensions(engine):
    d = CascadeDict(engine, {"a": 1, "bb": 2})

    @engine.query
    def q():
        return (
            [k * v for k, v in d.items()],
            {v: k for k, v in d.items()},
            sum(len([c for c in k]) for k, _ in d.items()),
        )

    assert rewritten(q)
    assert q() == (["a", "bbbb"], {1: "a", 2: "bb"}, 3)
    d["ccc"] = 3
    assert q() == (["a", "bbbb", "ccccccccc"], {1: "a", 2: "bb", 3: "ccc"}, 6)


def test_bare_dictcomp_and_setcomp(engine):
    xs = CascadeList(engine, [1, 2, 2, 3])

    @engine.query
    def q():
        return ({x: x * 10 for x in xs}, {x % 2 for x in xs})

    assert rewritten(q)
    assert q() == ({1: 10, 2: 20, 3: 30}, {0, 1})
    xs.append(4)
    assert q() == ({1: 10, 2: 20, 3: 30, 4: 40}, {0, 1})


# --- requirement 4: stack trace integrity ---


def test_error_in_comprehension_points_at_original_line(engine):
    xs = CascadeList(engine, [1, 0])

    @engine.query(cache_exceptions=False)
    def q():
        return sum(1 // x for x in xs)  # TRACE-MARKER-COMP

    with pytest.raises(ZeroDivisionError):
        q()
    try:
        q()
    except ZeroDivisionError:
        tb = traceback.format_exc()
    assert "test_ast_rewrite.py" in tb
    assert "TRACE-MARKER-COMP" in tb


def test_error_in_plain_code_after_rewrite_points_at_original_line(engine):
    xs = CascadeList(engine, [1])

    @engine.query(cache_exceptions=False)
    def q():
        total = sum(x for x in xs)
        raise RuntimeError(f"plain-{total}")  # TRACE-MARKER-PLAIN

    try:
        q()
    except RuntimeError:
        tb = traceback.format_exc()
    assert "TRACE-MARKER-PLAIN" in tb
    assert "test_ast_rewrite.py" in tb


# --- requirement 5: strict boundaries ---


def test_plain_for_loops_are_never_touched(engine):
    xs = CascadeList(engine, [1, 2, 3])
    runs = []

    @engine.query
    def loop_total():
        runs.append(1)
        acc = 0
        for x in xs:
            acc += x
        return acc

    # no rewrite sites means the function is registered unmodified
    assert not rewritten(loop_total)
    assert loop_total() == 6
    xs.append(4)
    assert loop_total() == 10  # read tracking invalidates the loop
    assert len(runs) == 2


def test_unsupported_call_shapes_stay_native(engine):
    xs = CascadeList(engine, [3, 1, 2])

    @engine.query
    def q():
        return (
            sum((x for x in xs), 0),  # start argument
            min(xs, key=lambda x: -x),  # key kwarg on min
            min(1, 2),  # multiple positionals
            dict(a=1),  # kwargs constructor
            sorted([y for y in xs], [], []) if False else sorted(xs, reverse=True),
            {x for a in [xs] for x in a},  # multiple generators
        )

    assert q() == (6, 3, 1, {"a": 1}, [3, 2, 1], {1, 2, 3})


def test_join_on_non_literal_receiver_untouched(engine):
    class Frame:
        def join(self, other):
            return ("joined", list(other))

    df = Frame()
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def q():
        return df.join([x for x in xs])

    assert q() == ("joined", [1, 2])


def test_shadowed_builtin_disables_rewriting(engine):
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def q():
        sum = lambda vals: 99  # noqa: E731
        return sum(x for x in xs)

    assert not rewritten(q)
    assert q() == 99


def test_lambda_and_async_queries_not_rewritten(engine):
    xs = CascadeList(engine, [1, 2])
    lam = engine.query(lambda: sum(x for x in xs))
    assert not rewritten(lam)
    assert lam() == 3

    @engine.query
    async def aq():
        return sum(x for x in xs)

    assert not rewritten(aq)
    assert aq() == 3  # the engine resolves coroutines itself


def test_exec_defined_function_not_rewritten(engine):
    ns = {"src": CascadeList(engine, [5])}
    exec("def dynamic():\n    return sum(x for x in src)", ns)
    q = engine.query(ns["dynamic"])
    assert not rewritten(q)
    assert q() == 5


def test_comprehension_inside_lambda_body_untouched(engine):
    xs = CascadeList(engine, [2, 1])

    @engine.query
    def q():
        picker = lambda: [x for x in xs]  # noqa: E731
        return (sorted(xs), picker())

    assert rewritten(q)  # the sorted() site rewrites; the lambda body does not
    result = q()
    assert result[0] == [1, 2]
    assert sorted(result[1]) == [1, 2]


# --- requirement 6: enumerate and zip are explicitly excluded ---


def test_enumerate_and_zip_sources_untouched(engine):
    xs = CascadeList(engine, [10, 20])
    runs = []

    @engine.query
    def q():
        runs.append(1)
        pairs = [(i, x) for i, x in enumerate(xs)]
        zipped = [a + b for a, b in zip(xs, xs)]
        return (pairs, zipped, sum(i for i, _ in enumerate(xs)))

    assert not rewritten(q)  # every site aborts, function stays original
    assert q() == ([(0, 10), (1, 20)], [20, 40], 1)
    xs.insert(0, 5)  # index shifts recompute everything, correctly
    assert q() == ([(0, 5), (1, 10), (2, 20)], [10, 20, 40], 3)
    assert len(runs) == 2


def test_enumerate_alongside_rewritable_site(engine):
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def q():
        return (sum(x for x in xs), [i for i, _ in enumerate(xs)])

    assert rewritten(q)  # the sum site rewrites; enumerate site stays native
    assert q() == (3, [0, 1])
    xs.append(3)
    assert q() == (6, [0, 1, 2])


# --- register-time equivalence: rewritten and plain agree ---


def test_rewritten_function_reregisters_cleanly(engine):
    xs = CascadeList(engine, [1, 2])

    def body():
        return sum(x for x in xs)

    q1 = engine.query(body)
    q2 = engine.query(incremental=False)(body)
    assert q1() == q2() == 3
