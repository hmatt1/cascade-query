from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .engine import Engine


def build_fanout_chain_pipeline(
    engine: Engine,
    *,
    depth: int,
    fanout: int,
    counts: dict[str, int] | None = None,
    name_prefix: str = "synthetic",
) -> tuple[Any, list[Any], Any]:
    """Build a deterministic wide + deep synthetic query graph.

    This helper is intentionally shared by both tests and benchmarks so they
    exercise identical fanout/chain behavior.
    """
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
            level_query.__qualname__ = f"{name_prefix}_level_{level_index}"
            return engine.query(level_query)

        current = _make_level(level, prev)
        levels.append(current)
        prev = current

    top = levels[-1]

    def aggregate_query() -> int:
        call_counts["aggregate"] += 1
        return sum(top(branch) for branch in range(fanout))

    aggregate_query.__name__ = "aggregate_query"
    aggregate_query.__qualname__ = f"{name_prefix}_aggregate_query"
    aggregate = engine.query(aggregate_query)
    return leaf, levels, aggregate
