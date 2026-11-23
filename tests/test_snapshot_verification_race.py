"""Verifying a memo at an old snapshot must not use another revision's row.

The store keeps one memo row per query. Verifying a dependency at an older
snapshot used to re-read that row afterwards; if a concurrent reader at the
newest revision had replaced it in between, its changed_at matched what the
parent recorded at the newest revision, the parent was wrongly accepted, and
the old snapshot returned the newer value.
"""

from __future__ import annotations

import threading
from typing import Any

from cascade import Engine


def test_old_snapshot_read_ignores_rows_replaced_during_verification() -> None:
    engine = Engine()

    @engine.input
    def src() -> int:
        return 0

    @engine.query
    def leaf() -> int:
        return src() * 2

    @engine.query
    def total() -> int:
        return leaf() + 1

    src.set(1)
    assert total() == 3
    snap = engine.snapshot()
    src.set(2)
    assert total() == 5

    evaluator = engine._internals._evaluator  # noqa: SLF001
    store = engine._internals._store  # noqa: SLF001
    original = evaluator._run_in_runtime  # noqa: SLF001
    interleaved: list[int] = []

    def run_then_interleave(runtime: Any, fn: Any) -> Any:
        result = original(runtime, fn)
        # Right after a dependency was verified at the old snapshot, a reader
        # at the newest revision recomputes that dependency on another thread.
        if not runtime.stack and runtime.snapshot.revision < store.revision:
            interleaved.append(runtime.snapshot.revision)
            reader = threading.Thread(target=leaf)
            reader.start()
            reader.join()
        return result

    evaluator._run_in_runtime = run_then_interleave  # noqa: SLF001
    try:
        assert total(snapshot=snap) == 3
        assert total(snapshot=snap) == 3
    finally:
        evaluator._run_in_runtime = original  # noqa: SLF001
        snap.release()
    assert interleaved
    assert total() == 5
