"""A caller inside a pipeline never waits for another thread's run.

A rewritten comprehension holds the engine's pipeline lock while its stages
run, and a stage may call a query. If another thread owns that query's run,
and the run itself reaches a pipeline, the two wait for each other: the
caller for the run, the run for the lock. Found by the state machine's lock
step once it could see a thread blocked on a lock. The caller now runs the
query on its own stack instead, as it does when a wait would never end.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from cascade import CascadeList, Engine

TIMEOUT = 10.0


def test_a_stage_calling_a_query_whose_run_needs_the_pipeline_lock() -> None:
    engine = Engine()
    items = CascadeList(engine, name="items")
    items.extend([1, 2, 3])
    store = engine._internals._store  # noqa: SLF001
    owner_started = threading.Event()
    caller_in_stage = threading.Event()
    caller: dict[str, int] = {}

    @engine.query(name="shifted")
    def shifted(x: int) -> int:
        if threading.current_thread().name == "owner":
            owner_started.set()
            # Go on once the caller waits for this run (before the fix it
            # did) or has finished without waiting (after it).
            assert caller_in_stage.wait(TIMEOUT)
            deadline = time.monotonic() + TIMEOUT
            while (
                caller.get("ident") not in store.waiting_on
                and "done" not in caller
                and time.monotonic() < deadline
            ):
                time.sleep(0.001)
        # A pipeline of its own: it needs the lock the caller holds.
        return x + len([y for y in items])

    @engine.query(name="spread")
    def spread() -> int:
        caller_in_stage.set()
        return sum(shifted(y) for y in items)

    out: dict[str, Any] = {}

    def run_owner() -> None:
        out["owner"] = shifted(1)

    def run_caller() -> None:
        caller["ident"] = threading.get_ident()
        assert owner_started.wait(TIMEOUT)
        out["caller"] = spread()
        caller["done"] = 1

    threads = [
        threading.Thread(target=run_owner, name="owner", daemon=True),
        threading.Thread(target=run_caller, name="caller", daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
    assert not any(t.is_alive() for t in threads), "deadlocked"
    assert out == {"owner": 1 + 3, "caller": (1 + 3) + (2 + 3) + (3 + 3)}
    assert shifted(1) == 4
    assert spread() == 15
