"""Rewriting keeps Python's scoping (D30, D32).

A comprehension's element and ``if`` clauses are moved into a helper
function. An assignment expression there must bind in the query, so such a
comprehension is not rewritten (D30). A pipeline nested in a comprehension
that stays as written gets its helper hoisted out of that comprehension,
where its loop variables are not visible, so such a pipeline is not
rewritten either (D32). Each body is compared with the same body registered
with ``incremental=False``, after every change to the collections.
"""

from __future__ import annotations

from typing import Any, Callable

import pytest

from cascade import CascadeDict, CascadeList, CascadeSet, Engine


def _bodies(xs: Any, ys: Any, d: Any, s: Any) -> dict[str, Callable[[], Any]]:
    def walrus_element():
        last = None
        out = [(last := x) for x in xs]
        return (last, out)

    def walrus_filter():
        y = None
        out = [x for x in xs if (y := x * 2) > 2]
        return (y, out)

    def walrus_in_reducer():
        t = None
        total = sum((t := x) for x in xs)
        return (t, total)

    def walrus_in_nested_comprehension():
        last = None
        out = [[(last := v) for v in range(x)] for x in xs]
        return (last, out)

    def walrus_in_nested_filter():
        hit = None
        out = [[v for v in range(x) if (hit := v) > 0] for x in xs]
        return (hit, out)

    def walrus_in_set_comprehension():
        k = None
        out = {(k := x) % 3 for x in xs}
        return (k, sorted(out))

    def walrus_in_dict_comprehension():
        k = None
        out = {key: (k := value) for key, value in d.items()}
        return (k, out)

    def walrus_with_a_plain_neighbour():
        last = None
        out = [(last := x) for x in xs]
        doubled = [x * 2 for x in xs]  # still rewritten
        return (last, out, doubled)

    def nested_pipeline_in_a_two_for_comprehension():
        return [sum(y * x for y in ys) for x in xs for _ in (0,)]

    def nested_pipeline_reading_a_shadowed_name():
        x = 100
        return [sum(y * x for y in ys) for x in xs for _ in (0,)], x

    def nested_filter_in_a_two_for_comprehension():
        return [[y for y in ys if y > x] for x in xs for _ in (0,)]

    def nested_pipeline_in_a_walrus_comprehension():
        t = None
        out = [(t := sum(y * x for y in ys)) for x in xs]
        return (t, out)

    def nested_pipeline_in_a_kept_generator():
        return list(max(y + x for y in ys) for x in xs if x)

    def nested_pipeline_in_a_rewritten_comprehension():
        return [sum(y * x for y in ys) for x in xs]

    def nested_pipeline_over_a_set():
        return [len([v for v in s if v > x]) for x in xs for _ in (0,)]

    # Pipelines in a comprehension that stays as written, whose helpers need
    # none of its loop variables: these are rewritten.
    def nested_map_call_in_a_two_for_comprehension():
        return [sum(map(abs, ys)) + x for x in xs for _ in (0,)]

    def nested_pipeline_that_ignores_the_loop_variables():
        return [sum(y * 2 for y in ys) + x for x in xs for _ in (0,)]

    def nested_pipeline_with_a_loop_variable_of_the_same_name():
        return [sum(x * 2 for x in ys) + x for x in xs for _ in (0,)]

    return {
        fn.__name__: fn
        for fn in [
            walrus_element,
            walrus_filter,
            walrus_in_reducer,
            walrus_in_nested_comprehension,
            walrus_in_nested_filter,
            walrus_in_set_comprehension,
            walrus_in_dict_comprehension,
            walrus_with_a_plain_neighbour,
            nested_pipeline_in_a_two_for_comprehension,
            nested_pipeline_reading_a_shadowed_name,
            nested_filter_in_a_two_for_comprehension,
            nested_pipeline_in_a_walrus_comprehension,
            nested_pipeline_in_a_kept_generator,
            nested_pipeline_in_a_rewritten_comprehension,
            nested_pipeline_over_a_set,
            nested_map_call_in_a_two_for_comprehension,
            nested_pipeline_that_ignores_the_loop_variables,
            nested_pipeline_with_a_loop_variable_of_the_same_name,
        ]
    }


BODY_NAMES = sorted(_bodies(None, None, None, None))


def _outcome(handle: Any) -> Any:
    try:
        return ("ok", handle())
    except Exception as exc:  # noqa: BLE001
        return ("raised", type(exc).__name__, str(exc))


@pytest.mark.parametrize("body_name", BODY_NAMES)
def test_rewritten_body_matches_plain_python(body_name: str) -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])
    ys = CascadeList(engine, [10, 20])
    d = CascadeDict(engine, {"a": 1, "b": 2})
    s = CascadeSet(engine, [5])
    rewritten = engine.query(_bodies(xs, ys, d, s)[body_name], name=f"rw:{body_name}")
    plain = engine.query(
        _bodies(xs, ys, d, s)[body_name], name=f"plain:{body_name}", incremental=False
    )
    steps: list[Callable[[], Any]] = [
        lambda: None,
        lambda: xs.append(4),
        lambda: ys.insert(0, 1),
        lambda: xs.remove(2),
        lambda: d.__setitem__("c", 9),
        lambda: s.add(0),
        lambda: xs.clear(),
        lambda: xs.extend([0, 7]),
    ]
    for step in steps:
        step()
        assert _outcome(rewritten) == _outcome(plain)


def test_body_with_walrus_and_plain_comprehension_is_still_rewritten() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2])
    q = engine.query(
        _bodies(xs, None, None, None)["walrus_with_a_plain_neighbour"], name="mixed"
    )
    assert getattr(q.raw, "__cascade_rewritten__", False)
    assert q() == (2, [1, 2], [2, 4])


@pytest.mark.parametrize(
    "body_name",
    [
        "nested_map_call_in_a_two_for_comprehension",
        "nested_pipeline_that_ignores_the_loop_variables",
        "nested_pipeline_with_a_loop_variable_of_the_same_name",
    ],
)
def test_nested_pipeline_that_needs_no_loop_variable_is_still_rewritten(
    body_name: str,
) -> None:
    # Only a helper that would read the comprehension's loop variables keeps
    # a nested pipeline from being rewritten (D32). These read none.
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])
    ys = CascadeList(engine, [10, -20])
    body = _bodies(xs, ys, None, None)[body_name]
    q = engine.query(body, name=f"rw:{body_name}")
    plain = engine.query(body, name=f"plain:{body_name}", incremental=False)
    assert getattr(q.raw, "__cascade_rewritten__", False)
    assert q() == plain()
    assert [p["reducer"] for p in engine.inspect_pipelines()] == ["sum"]
    ys.append(5)
    assert q() == plain()
