from __future__ import annotations

import ast
from typing import Any

from cascade import Engine
from cascade._synthetic_graph import build_fanout_chain_pipeline


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
    internals = engine._internals  # noqa: SLF001
    for dep_key, dependents in internals.dependents.items():
        for dependent in dependents:
            assert dependent in internals.memos
            memo = internals.memos[dependent]
            assert any(dep.key == dep_key for dep in memo.deps)
