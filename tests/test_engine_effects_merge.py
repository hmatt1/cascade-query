"""What ``effects=`` holds after ``compute_many`` and ``compute_many_stream`` (G12)."""

from __future__ import annotations

import threading
from typing import Any

import pytest

from cascade import Engine

TIMEOUT = 20.0
APIS = ("compute_many", "compute_many_stream")


def _within_timeout(fn: Any) -> Any:
    outcome: list[Any] = []

    def target() -> None:
        try:
            outcome.append(("ok", fn()))
        except BaseException as exc:  # noqa: BLE001
            outcome.append(("raised", exc))

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(TIMEOUT)
    assert not thread.is_alive(), "call did not finish in time"
    kind, value = outcome[0]
    if kind == "raised":
        raise value
    return value


def _fan_out(
    engine: Engine, api: str, calls: list[Any], effects: dict[str, list[Any]]
) -> tuple[list[Any], dict[int, dict[str, list[Any]]] | None]:
    """Results in call order, and what the stream handed out with each."""
    if api == "compute_many":
        return engine.compute_many(calls, effects=effects), None
    results: list[Any] = [None] * len(calls)
    handed_out: dict[int, dict[str, list[Any]]] = {}
    for idx, value, call_effects in engine.compute_many_stream(calls, effects=effects):
        results[idx] = value
        handed_out[idx] = call_effects
    return results, handed_out


@pytest.mark.parametrize("api", APIS)
def test_fan_out_in_a_query_reports_only_what_the_called_queries_pushed(
    api: str,
) -> None:
    engine = Engine()
    notes = engine.accumulator("notes")
    log = engine.accumulator("log")
    inner: dict[str, list[Any]] = {}
    handed_out: list[Any] = []

    @engine.query
    def leaf(i: int) -> int:
        if i:  # leaf(0) pushes nothing at all
            log.push(f"leaf-{i}")
        return i

    @engine.query
    def parent() -> int:
        # The caller's own push, to an accumulator none of the calls uses.
        notes.push("before")
        results, per_call = _fan_out(
            engine, api, [(leaf, (i,)) for i in range(3)], inner
        )
        handed_out.append(per_call)
        return sum(results)

    outer: dict[str, list[Any]] = {}
    assert _within_timeout(lambda: parent(effects=outer)) == 3
    assert inner == {"log": ["leaf-1", "leaf-2"]}
    if api == "compute_many_stream":
        assert handed_out == [{0: {}, 1: {"log": ["leaf-1"]}, 2: {"log": ["leaf-2"]}}]
    assert outer["notes"] == ["before"]
    assert sorted(outer["log"]) == ["leaf-1", "leaf-2"]
    assert set(outer) == {"notes", "log"}


@pytest.mark.parametrize("api", APIS)
def test_accumulator_whose_pushes_were_all_taken_back_is_left_out(api: str) -> None:
    engine = Engine()
    log = engine.accumulator("log")
    kept = engine.accumulator("kept")

    @engine.query(fixed_point=0)
    def grow() -> int:
        previous = grow()
        if previous == 0:
            # Only the first run pushes, and that run is abandoned: its
            # result differs from the guess it was computed from.
            log.push("first run")
        return min(previous + 1, 3)

    @engine.query
    def plain() -> int:
        kept.push("plain")
        return 7

    collected: dict[str, list[Any]] = {}
    results, _ = _within_timeout(
        lambda: _fan_out(engine, api, [(grow, ()), (plain, ())], collected)
    )
    assert results == [3, 7]
    assert collected == {"kept": ["plain"]}


def test_stream_finished_outside_its_query_omits_pushes_taken_back() -> None:
    engine = Engine()
    log = engine.accumulator("log")
    kept = engine.accumulator("kept")
    streams: list[Any] = []
    collected: dict[str, list[Any]] = {}

    # Not memoized, so no worker computes it ahead: it runs where the stream
    # is consumed.
    @engine.query(fixed_point=0, memoize=False)
    def grow() -> int:
        previous = grow()
        if previous == 0:
            log.push("first run")  # in the run that is abandoned, as above
        return min(previous + 1, 3)

    @engine.query
    def leaf() -> int:
        kept.push("leaf")
        return 7

    @engine.query
    def parent() -> int:
        stream = engine.compute_many_stream([(leaf, ()), (grow, ())], effects=collected)
        streams.append(stream)
        return next(stream)[1]  # leaf; the rest is left to the caller

    assert _within_timeout(parent) == 7
    rest = _within_timeout(lambda: list(streams[0]))
    assert [(idx, value) for idx, value, _ in rest] == [(1, 3)]
    assert collected == {"kept": ["leaf"]}
