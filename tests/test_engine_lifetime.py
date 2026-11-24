"""An engine's lifetime: ``with Engine(...)``, and engines dropped without shutdown (G17, G26).

An engine with ``cache_dir`` runs a writer thread. The thread holds no
reference to the engine, so an engine the program drops is collected like
any object; the writer then writes what is still queued, closes the cache
(releasing the engine's collection locks) and ends.
"""

from __future__ import annotations

import gc
import sqlite3
import warnings
import weakref
from pathlib import Path
from typing import Any

import pytest

from cascade import CascadeList, Engine, PersistentCacheError

BACKENDS = ("mdbx", "sqlite")


def _build(tmp_path: Path, backend: str, runs: dict[str, int]) -> tuple[Engine, Any]:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)

    @engine.query(name="lifetime.square")
    def square(i: int) -> int:
        runs["n"] = runs.get("n", 0) + 1
        return i * i

    return engine, square


@pytest.mark.parametrize("backend", BACKENDS)
def test_dropped_engine_is_collected_and_its_writer_finishes(
    tmp_path: Path, backend: str
) -> None:
    runs: dict[str, int] = {}
    engine, square = _build(tmp_path, backend, runs)
    items = CascadeList(engine, name="lifetime.items")
    items.append("first")  # takes the collection's write lock
    for i in range(300):
        square(i)
    ref = weakref.ref(engine)
    thread = engine._evaluator._disk_thread  # noqa: SLF001
    assert thread is not None and thread.is_alive()
    del engine, square, items
    gc.collect()
    assert ref() is None
    thread.join(30)
    assert not thread.is_alive()

    # Every queued result was written, and the lock was released.
    runs.clear()
    engine, square = _build(tmp_path, backend, runs)
    try:
        assert [square(i) for i in range(300)] == [i * i for i in range(300)]
        assert runs == {}
        items = CascadeList(engine, name="lifetime.items")
        items.append("second")
        assert list(items) == ["first", "second"]
    finally:
        engine.shutdown()


def test_dropped_engines_do_not_accumulate_threads(tmp_path: Path) -> None:
    import threading

    before = threading.active_count()
    for _ in range(5):
        engine, square = _build(tmp_path, "sqlite", {})
        square(1)
        del engine, square
        gc.collect()
    for t in threading.enumerate():
        if t.name == "cascade-disk-writer":
            t.join(30)
    assert threading.active_count() <= before


def test_in_memory_engine_is_collected() -> None:
    engine = Engine()

    @engine.query
    def q() -> int:
        return 1

    q()
    ref = weakref.ref(engine)
    del engine, q
    gc.collect()
    assert ref() is None


@pytest.mark.parametrize("backend", BACKENDS)
def test_engine_as_a_context_manager_shuts_down(tmp_path: Path, backend: str) -> None:
    runs: dict[str, int] = {}
    with Engine(cache_dir=tmp_path, cache_backend=backend) as engine:

        @engine.query(name="lifetime.square")
        def square(i: int) -> int:
            runs["n"] = runs.get("n", 0) + 1
            return i * i

        CascadeList(engine, name="lifetime.items").append(1)
        assert square(4) == 16
        thread = engine._evaluator._disk_thread  # noqa: SLF001
    assert thread is not None and not thread.is_alive()

    # Written and unlocked: a second engine reuses the result and can change
    # the collection.
    runs.clear()
    with Engine(cache_dir=tmp_path, cache_backend=backend) as engine:
        square = engine.query(square.raw, name="lifetime.square")
        assert square(4) == 16
        assert runs == {}
        CascadeList(engine, name="lifetime.items").append(2)

    with Engine() as plain:
        assert plain.revision == 0


def test_context_manager_lets_the_block_error_through(tmp_path: Path) -> None:
    with pytest.raises(KeyError):
        with Engine(cache_dir=tmp_path, cache_backend="sqlite") as engine:
            raise KeyError("boom")
    thread = engine._evaluator._disk_thread  # noqa: SLF001
    assert thread is not None and not thread.is_alive()


class _FailingDisk:
    def __init__(self, engine: Engine) -> None:
        disk = engine._disk  # noqa: SLF001

        def fail(entries: Any) -> None:
            raise sqlite3.OperationalError("database is locked")

        disk.store_entry_many = fail  # type: ignore[method-assign]


def test_context_manager_reports_a_failed_write(tmp_path: Path) -> None:
    with pytest.raises(PersistentCacheError, match="database is locked"):
        with Engine(cache_dir=tmp_path, cache_backend="sqlite") as engine:
            _FailingDisk(engine)
            engine.query(lambda: 1, name="lifetime.one")()
    # With an error from the block, the failed write is a warning instead.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(KeyError):
            with Engine(cache_dir=tmp_path, cache_backend="sqlite") as engine:
                _FailingDisk(engine)
                engine.query(lambda: 2, name="lifetime.two")()
                engine._evaluator._wait_for_writer()  # noqa: SLF001
                raise KeyError("boom")
    assert any("database is locked" in str(w.message) for w in caught)


def test_dropped_engine_with_a_failed_write_warns(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from types import SimpleNamespace

    from cascade import _evaluator

    # The warning is issued on the writer thread; free-threaded builds keep
    # warning filters per context, so it is caught at the source instead.
    issued: list[str] = []
    monkeypatch.setattr(
        _evaluator,
        "warnings",
        SimpleNamespace(warn=lambda message, *a, **k: issued.append(str(message))),
    )
    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
    _FailingDisk(engine)
    engine.query(lambda: 1, name="lifetime.one")()
    engine._evaluator._wait_for_writer()  # noqa: SLF001
    thread = engine._evaluator._disk_thread  # noqa: SLF001
    del engine
    gc.collect()
    assert thread is not None
    thread.join(30)
    assert any(
        "dropped without shutdown()" in message and "database is locked" in message
        for message in issued
    )
