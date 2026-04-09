from __future__ import annotations

import ast
import threading
from typing import Any

from cascade import Engine
from cascade._synthetic_graph import build_fanout_chain_pipeline as _build_fanout_chain_pipeline


def build_fanout_chain_pipeline(*args: Any, **kwargs: Any) -> tuple[Any, list[Any], Any]:
    """Forward to the shared synthetic graph helper.

    Tests import this symbol from `tests.scale_helpers`; keep that call-site
    stable while centralizing implementation in `cascade._synthetic_graph`.
    """
    return _build_fanout_chain_pipeline(*args, **kwargs)


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


def run_prune_with_timeout(
    engine: Engine,
    roots: list[tuple[str, str, tuple[Any, ...]]],
    *,
    timeout: float = 0.5,
) -> None:
    """Run prune in a daemon thread and fail fast on non-termination."""

    finished = threading.Event()
    errors: list[BaseException] = []

    def _run() -> None:
        try:
            engine.prune(roots)
        except BaseException as exc:  # pragma: no cover - defensive
            errors.append(exc)
        finally:
            finished.set()

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    assert finished.wait(timeout), "prune() did not terminate within timeout"
    if errors:
        raise errors[0]
