from __future__ import annotations

import ast
from collections import defaultdict
from typing import Any

from cascade import Engine


def build_fanout_chain_pipeline(
    engine: Engine,
    *,
    depth: int,
    fanout: int,
    counts: dict[str, int] | None = None,
) -> tuple[Any, list[Any], Any]:
    """Build a deterministic wide + deep synthetic query graph."""
    if depth < 1:
        raise ValueError("depth must be >= 1")
    if fanout < 1:
        raise ValueError("fanout must be >= 1")

    call_counts = counts if counts is not None else defaultdict(int)

    @engine.input
    def leaf(branch: int) -> int:
        return 0

    levels: list[Any] = []
    prev = None
    for level in range(depth):

        def _make_level(level_index: int, prev_level: Any) -> Any:
            def level_query(branch: int) -> int:
                call_counts[f"level_{level_index}"] += 1
                base = leaf(branch) if prev_level is None else prev_level(branch)
                return base + (level_index + 1)

            level_query.__name__ = f"level_{level_index}"
            level_query.__qualname__ = f"synthetic_level_{level_index}"
            return engine.query(level_query)

        current = _make_level(level, prev)
        levels.append(current)
        prev = current

    top = levels[-1]

    def aggregate_query() -> int:
        call_counts["aggregate"] += 1
        return sum(top(branch) for branch in range(fanout))

    aggregate_query.__name__ = "aggregate_query"
    aggregate_query.__qualname__ = "synthetic_aggregate_query"
    aggregate = engine.query(aggregate_query)
    return leaf, levels, aggregate


def expected_fanout_chain_total(*, depth: int, fanout: int, values: list[int]) -> int:
    increment = depth * (depth + 1) // 2
    if len(values) != fanout:
        raise ValueError("values length must match fanout")
    return sum(v + increment for v in values)


def cached_query_args(graph: dict[str, Any], query_id: str) -> set[tuple[Any, ...]]:
    prefix = f"query:{query_id}"
    args: set[tuple[Any, ...]] = set()
    for node in graph["nodes"]:
        if not node.startswith(prefix):
            continue
        args.add(ast.literal_eval(node[len(prefix) :]))
    return args


def assert_internal_dependents_consistent(engine: Engine) -> None:
    """Internal-only invariant: each dependent edge must match memo deps.

    Keep this helper narrow and centralized so most tests can stay black-box.
    """
    for dep_key, dependents in engine._dependents.items():  # noqa: SLF001
        for dependent in dependents:
            assert dependent in engine._memos  # noqa: SLF001
            memo = engine._memos[dependent]  # noqa: SLF001
            assert any(dep.key == dep_key for dep in memo.deps)
