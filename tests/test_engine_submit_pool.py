"""The thread pool ``engine.submit`` uses when it is given no executor.

It is made on the first submit, with a size worked out from the machine's
CPU count. ``os.cpu_count()`` may return None, and a machine may have a
single CPU: submit still works. And submit refuses to run from inside a
query (the future could outlive it), saying so and what to do instead.
"""

from __future__ import annotations

import os
from typing import Any

import pytest

from cascade import Engine


@pytest.mark.parametrize("cpus", [None, 1, 2, 64], ids=lambda c: f"cpus={c}")
def test_submit_runs_whatever_the_cpu_count(
    monkeypatch: pytest.MonkeyPatch, cpus: Any
) -> None:
    monkeypatch.setattr(os, "cpu_count", lambda: cpus)
    engine = Engine()

    @engine.query
    def double(x: int) -> int:
        return 2 * x

    try:
        futures = [engine.submit(double, i) for i in range(5)]
        assert [f.result(timeout=30) for f in futures] == [0, 2, 4, 6, 8]
    finally:
        engine.shutdown()


def test_submit_from_inside_a_query_is_refused_and_says_what_to_do() -> None:
    engine = Engine()

    @engine.query
    def other() -> int:
        return 1

    @engine.query
    def inner() -> int:
        engine.submit(other)
        return 0

    with pytest.raises(
        RuntimeError,
        match=r"^engine\.submit\(\) cannot be called from inside a query: .*"
        r"Call the query directly, or use engine\.compute_many\(\)",
    ):
        inner()
    assert other() == 1
