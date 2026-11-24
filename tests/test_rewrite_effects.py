"""Accumulator pushes inside a rewritten expression match plain Python (D31).

A pipeline runs its stage, filter and key functions for changed elements
only, in the order the changes arrived. When any of them pushes an effect
(or calls a query whose effects are replayed), the pipeline takes its pushes
back and the expression is evaluated plainly, then and on every later run.
Each test runs one body rewritten and once with ``incremental=False`` and
compares value and effects after every change.
"""

from __future__ import annotations

from typing import Any, Callable

import pytest

from cascade import CascadeDict, CascadeList, Engine

# Stage functions reach the accumulator through a global, so their
# pipelines can be fingerprinted and kept between runs.
_ACC: dict[str, Any] = {}
CALLS: list[Any] = []


def note(x: Any) -> Any:
    _ACC["log"].push(("note", x))
    return x


def keep(x: Any) -> bool:
    _ACC["log"].push(("keep", x))
    return x % 2 == 1


def big(x: Any) -> bool:
    _ACC["log"].push(("big", x))
    return x > 3


def key_of(x: Any) -> Any:
    _ACC["log"].push(("key", x))
    return -x


def note_if_large(x: Any) -> Any:
    if x > 3:
        _ACC["log"].push(("large", x))
    return x


def silent(x: Any) -> Any:
    CALLS.append(x)
    return x * 2


def _bodies(xs: Any) -> dict[str, Callable[[], Any]]:
    log = _ACC["log"]

    def sum_map():
        log.push("before")
        result = sum(note(x) for x in xs)
        log.push("after")
        return result

    def list_filter():
        return [x for x in xs if keep(x)]

    def listcomp_both():
        return [note(x) for x in xs if keep(x)]

    def any_short_circuit():
        return any(big(x) for x in xs)

    def all_short_circuit():
        return all(not big(x) for x in xs)

    def sorted_key():
        return sorted(xs, key=key_of)

    def sorted_map_key():
        return sorted((note(x) for x in xs), key=key_of)

    def max_map():
        return max(note(x) for x in xs)

    def len_listcomp():
        return len([x for x in xs if keep(x)])

    def join_map():
        return ",".join(str(note(x)) for x in xs)

    def data_dependent():
        return sum(note_if_large(x) for x in xs)

    def map_builtin():
        return list(map(note, xs))

    def filter_builtin():
        return sum(filter(keep, xs))

    def raises_after_pushing():
        return sum(note(1 / note(x)) for x in xs)

    return {
        fn.__name__: fn
        for fn in [
            sum_map,
            list_filter,
            listcomp_both,
            any_short_circuit,
            all_short_circuit,
            sorted_key,
            sorted_map_key,
            max_map,
            len_listcomp,
            join_map,
            data_dependent,
            map_builtin,
            filter_builtin,
            raises_after_pushing,
        ]
    }


BODY_NAMES = [
    "sum_map",
    "list_filter",
    "listcomp_both",
    "any_short_circuit",
    "all_short_circuit",
    "sorted_key",
    "sorted_map_key",
    "max_map",
    "len_listcomp",
    "join_map",
    "data_dependent",
    "map_builtin",
    "filter_builtin",
    "raises_after_pushing",
]


def _pair(engine: Engine, xs: Any, body_name: str) -> tuple[Any, Any]:
    rewritten = engine.query(_bodies(xs)[body_name], name=f"rw:{body_name}")
    plain = engine.query(
        _bodies(xs)[body_name], name=f"plain:{body_name}", incremental=False
    )
    assert getattr(rewritten.raw, "__cascade_rewritten__", False)
    assert not getattr(plain.raw, "__cascade_rewritten__", False)
    return rewritten, plain


def _outcome(handle: Any) -> Any:
    effects: dict[str, list[Any]] = {}
    try:
        value = ("ok", handle(effects=effects))
    except Exception as exc:  # noqa: BLE001
        value = ("raised", type(exc).__name__)
    return value, effects.get("log", [])


LIST_STEPS: list[Callable[[Any], None]] = [
    lambda xs: xs.append(4),
    lambda xs: xs.insert(0, 7),
    lambda xs: xs.remove(2),
    lambda xs: xs.__setitem__(1, 5),
    lambda xs: xs.extend([8, 1]),
    lambda xs: xs.pop(0),
    lambda xs: xs.clear(),
    lambda xs: xs.append(6),
]


@pytest.mark.parametrize("body_name", BODY_NAMES)
def test_rewritten_effects_match_plain_python_on_a_list(body_name: str) -> None:
    engine = Engine()
    _ACC["log"] = engine.accumulator("log")
    xs = CascadeList(engine, [3, 2, 1])
    rewritten, plain = _pair(engine, xs, body_name)
    for step in [lambda xs: None, *LIST_STEPS]:
        step(xs)
        expected = _outcome(plain)
        assert _outcome(rewritten) == expected
        # A cache hit replays the same effects.
        assert _outcome(rewritten) == expected


def test_effects_pushed_beside_the_pipeline_are_kept() -> None:
    engine = Engine()
    log = engine.accumulator("log")
    _ACC["log"] = log
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def q() -> int:
        log.push("before")
        total = sum(x * 2 for x in xs)  # pure: stays incremental
        log.push("after")
        return total

    assert _outcome(q) == (("ok", 6), ["before", "after"])
    xs.append(3)
    assert _outcome(q) == (("ok", 12), ["before", "after"])


def test_a_pure_pipeline_stays_incremental() -> None:
    engine = Engine()
    _ACC["log"] = engine.accumulator("log")
    xs = CascadeList(engine, [1, 2, 3])

    @engine.query
    def q() -> int:
        return sum(silent(x) for x in xs)

    CALLS.clear()
    assert q() == 12
    xs.append(4)
    assert q() == 20
    assert CALLS == [1, 2, 3, 4]  # one call for the appended element


def test_stage_calling_a_query_with_effects() -> None:
    engine = Engine()
    log = engine.accumulator("log")
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def tag(x: int) -> int:
        log.push(("tag", x))
        return x * 10

    @engine.query
    def total() -> int:
        return sum(tag(x) for x in xs)

    @engine.query(incremental=False)
    def total_plain() -> int:
        return sum(tag(x) for x in xs)

    for step in [lambda: None, lambda: xs.append(3), lambda: xs.remove(1)]:
        step()
        assert _outcome(total) == _outcome(total_plain)


def test_stage_closing_over_the_accumulator() -> None:
    # Cannot be fingerprinted, so its state was rebuilt each run; with a
    # short-circuiting reducer the rebuild's pushes differed from Python's.
    engine = Engine()
    log = engine.accumulator("log")
    xs = CascadeList(engine, [5, 1, 2])

    def probe(x: int) -> bool:
        log.push(x)
        return x > 3

    @engine.query
    def found() -> bool:
        return any(probe(x) for x in xs)

    assert _outcome(found) == (("ok", True), [5])
    xs.insert(0, 9)
    assert _outcome(found) == (("ok", True), [9])
    xs.pop(0)
    xs.pop(0)
    assert _outcome(found) == (("ok", False), [1, 2])


def test_dict_source_matches_plain_python() -> None:
    engine = Engine()
    _ACC["log"] = engine.accumulator("log")
    source = CascadeDict(engine, {"a": 1, "b": 2})

    def body():
        return {k: note(v) for k, v in source.items()}

    rewritten = engine.query(body, name="dict_rw")
    plain = engine.query(body, name="dict_plain", incremental=False)
    steps: list[Callable[[], Any]] = [
        lambda: None,
        lambda: source.__setitem__("c", 3),
        lambda: source.__setitem__("a", 5),
        lambda: source.pop("b"),
    ]
    for step in steps:
        step()
        assert _outcome(rewritten) == _outcome(plain)


def test_snapshot_read_matches_plain_python() -> None:
    engine = Engine()
    _ACC["log"] = engine.accumulator("log")
    xs = CascadeList(engine, [1, 2])

    def body():
        return any(big(x) for x in xs)

    rewritten = engine.query(body, name="snap_rw")
    plain = engine.query(body, name="snap_plain", incremental=False)
    assert getattr(rewritten.raw, "__cascade_rewritten__", False)
    assert _outcome(rewritten) == _outcome(plain)
    with engine.snapshot() as before:
        xs.insert(0, 9)
        expected_old: dict[str, list[Any]] = {}
        expected_value = plain(snapshot=before, effects=expected_old)
        got: dict[str, list[Any]] = {}
        assert rewritten(snapshot=before, effects=got) == expected_value
        assert got == expected_old
    assert _outcome(rewritten) == _outcome(plain)
