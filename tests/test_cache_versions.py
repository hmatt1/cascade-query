"""Manual invalidation: ``version=`` per query, ``cache_version=`` per engine (G11, G16).

The code hash sees a query's own bytecode, not the helpers it calls, its
globals, closure values or defaults. ``version=`` and ``cache_version=`` are
how a program says that something it cannot see has changed.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from cascade import Engine

BACKENDS = ("mdbx", "sqlite")

# A helper the queries call. Tests change it between sessions, as an edit
# to a helper function would between two runs of a program.
_HELPER = {"factor": 2}


def _helper(x: int) -> int:
    return x * _HELPER["factor"]


def _session(
    cache: Path,
    backend: str,
    runs: dict[str, int],
    *,
    version: Any = None,
    cache_version: Any = None,
) -> tuple[Engine, Any, Any]:
    engine = Engine(cache_dir=cache, cache_backend=backend, cache_version=cache_version)

    @engine.input(name="versions.base")
    def base() -> int:
        return 10

    @engine.query(name="versions.scaled", version=version)
    def scaled() -> int:
        runs["scaled"] = runs.get("scaled", 0) + 1
        return _helper(base())

    @engine.query(name="versions.parity")
    def parity() -> str:
        runs["parity"] = runs.get("parity", 0) + 1
        return "even" if scaled() % 2 == 0 else "odd"

    return engine, scaled, parity


@pytest.fixture(autouse=True)
def _reset_helper() -> Any:
    _HELPER["factor"] = 2
    yield
    _HELPER["factor"] = 2


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_helper_edit_is_invisible_until_the_version_changes(
    tmp_path: Path, backend: str
) -> None:
    runs: dict[str, int] = {}
    engine, scaled, parity = _session(tmp_path, backend, runs, version=1)
    assert (scaled(), parity()) == (20, "even")
    engine.shutdown()

    # The helper changes; the query's own code does not. Its stored result
    # is served: this is the blind spot version= exists for.
    _HELPER["factor"] = 3
    runs.clear()
    engine, scaled, parity = _session(tmp_path, backend, runs, version=1)
    assert (scaled(), parity()) == (20, "even")
    assert runs == {}
    engine.shutdown()

    # A new version: the stored result is not used. Its value changed, so
    # the dependent runs again too.
    runs.clear()
    engine, scaled, parity = _session(tmp_path, backend, runs, version=2)
    try:
        assert (scaled(), parity()) == (30, "even")
        assert runs == {"scaled": 1, "parity": 1}
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_new_version_with_an_equal_result_keeps_the_dependents(
    tmp_path: Path, backend: str
) -> None:
    runs: dict[str, int] = {}
    engine, scaled, parity = _session(tmp_path, backend, runs, version="a")
    assert parity() == "even"
    engine.shutdown()
    runs.clear()
    engine, scaled, parity = _session(tmp_path, backend, runs, version="b")
    try:
        assert parity() == "even"
        # scaled ran again under the new version, gave 20 again, and the
        # dependent's stored result was reused.
        assert runs == {"scaled": 1}
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_cache_version_salts_every_stored_result(tmp_path: Path, backend: str) -> None:
    runs: dict[str, int] = {}
    engine, scaled, parity = _session(tmp_path, backend, runs, cache_version="v1")
    assert parity() == "even"
    engine.shutdown()

    runs.clear()
    engine, scaled, parity = _session(tmp_path, backend, runs, cache_version="v1")
    assert parity() == "even"
    assert runs == {}
    engine.shutdown()

    _HELPER["factor"] = 3
    runs.clear()
    engine, scaled, parity = _session(tmp_path, backend, runs, cache_version=2)
    assert parity() == "even" and scaled() == 30
    assert runs == {"scaled": 1, "parity": 1}  # nothing stored under "v1" is used
    engine.shutdown()

    # The new results replaced the old ones; the salt is part of their hash.
    runs.clear()
    engine, scaled, parity = _session(tmp_path, backend, runs, cache_version=2)
    try:
        assert scaled() == 30
        assert runs == {}
    finally:
        engine.shutdown()
    # An engine without a salt does not use salted results either.
    runs.clear()
    engine, scaled, parity = _session(tmp_path, backend, runs)
    try:
        assert scaled() == 30
        assert runs == {"scaled": 1}
    finally:
        engine.shutdown()


def test_a_new_version_in_one_process_drops_cached_results() -> None:
    engine = Engine()
    runs = {"n": 0}

    def body() -> int:
        runs["n"] += 1
        return _helper(1)

    @engine.query
    def top() -> int:
        return q() + 1

    q = engine.query(body, name="versions.memory", version=1)
    assert top() == 3
    assert engine.query(body, name="versions.memory", version=1)() == 2
    assert runs["n"] == 1  # the same version keeps the result
    _HELPER["factor"] = 4
    q = engine.query(body, name="versions.memory", version=2)
    assert q() == 4
    assert top() == 5  # the dependent saw the new result
    assert runs["n"] == 2


def test_versions_must_be_str_or_int() -> None:
    engine = Engine()
    for bad in (True, 1.5, [1], None.__class__):
        with pytest.raises(TypeError, match="version= must be a str or an int"):
            engine.query(lambda: 1, name="bad", version=bad)
        with pytest.raises(TypeError, match="cache_version= must be a str or an int"):
            Engine(cache_version=bad)
