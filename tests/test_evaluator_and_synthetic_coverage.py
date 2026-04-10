"""Targeted coverage for evaluator and synthetic graph edge paths."""

from __future__ import annotations

from collections import defaultdict

import pytest

from cascade import Engine
from cascade._synthetic_graph import build_fanout_chain_pipeline
from tests.scale_helpers import expected_fanout_chain_total


def test_replay_effects_is_no_op_outside_query_runtime() -> None:
    engine = Engine()
    engine._evaluator.replay_effects({"any": [1]})  # noqa: SLF001


def test_dependency_changed_at_runs_without_active_runtime() -> None:
    engine = Engine()

    @engine.input
    def src() -> int:
        return 7

    @engine.query
    def leaf() -> int:
        return src()

    snap = engine.snapshot()
    key = ("query", leaf.id, ())
    changed_at = engine._internals.dependency_changed_at(key, snap)  # noqa: SLF001
    assert changed_at >= 0
    assert leaf() == 7


def test_push_effect_stages_through_root_effects_dict() -> None:
    engine = Engine()
    acc = engine.accumulator("w")

    @engine.input
    def i() -> int:
        return 0

    @engine.query
    def inner() -> int:
        acc.push("staged")
        return i()

    @engine.query
    def outer() -> int:
        return inner()

    effects: dict[str, list[object]] = {}
    assert outer(effects=effects) == 0
    assert effects["w"] == ["staged"]


def test_push_effect_skips_staging_without_root_effects_dict() -> None:
    engine = Engine()
    acc = engine.accumulator("w")

    @engine.input
    def i() -> int:
        return 0

    @engine.query
    def inner() -> int:
        acc.push("only_frames")
        return i()

    @engine.query
    def outer() -> int:
        return inner()

    assert outer() == 0


def test_fanout_chain_pipeline_rejects_invalid_dimensions() -> None:
    engine = Engine()
    with pytest.raises(ValueError, match="depth must be >= 1"):
        build_fanout_chain_pipeline(engine, depth=0, fanout=1)
    with pytest.raises(ValueError, match="fanout must be >= 1"):
        build_fanout_chain_pipeline(engine, depth=1, fanout=0)


def test_fanout_chain_pipeline_leaf_and_default_counts_branch() -> None:
    engine = Engine()
    leaf, _levels, aggregate = build_fanout_chain_pipeline(engine, depth=1, fanout=2)
    # First aggregate read uses input defaults (executes leaf() -> return 0) before any set().
    assert aggregate() == expected_fanout_chain_total(depth=1, fanout=2, values=[0, 0])
    leaf.set(0, 10)
    leaf.set(1, 20)
    assert aggregate() == expected_fanout_chain_total(depth=1, fanout=2, values=[10, 20])


def test_fanout_chain_pipeline_accepts_explicit_counts_dict() -> None:
    engine = Engine()
    counts: dict[str, int] = defaultdict(int)
    leaf, _levels, aggregate = build_fanout_chain_pipeline(
        engine, depth=1, fanout=1, counts=counts
    )
    leaf.set(0, 5)
    assert aggregate() == expected_fanout_chain_total(depth=1, fanout=1, values=[5])
    assert counts["level_0"] >= 1
