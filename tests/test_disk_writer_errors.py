"""A failing disk write is reported, never a hang (G17, D18).

The writer thread keeps running after an error. ``flush_disk()`` and
``shutdown()`` return promptly; the first one after a failed write raises
``PersistentCacheError`` with the original error as its cause. Results stay
correct in memory. Every call that could hang runs on a daemon thread with a
timeout, so a regression fails instead of freezing the suite.
"""

from __future__ import annotations

import sqlite3
import threading
import warnings
from pathlib import Path
from typing import Any

import pytest

from cascade import Engine, PersistentCacheError

BACKENDS = ("mdbx", "sqlite")
TIMEOUT = 10.0
_count_lock = threading.Lock()


def _call(fn: Any, timeout: float = TIMEOUT) -> tuple[str, Any]:
    """Run ``fn`` on a daemon thread: ("ok", value) or ("raised", exc)."""
    outcome: list[tuple[str, Any]] = []

    def target() -> None:
        try:
            outcome.append(("ok", fn()))
        except BaseException as exc:  # noqa: BLE001
            outcome.append(("raised", exc))

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    assert not thread.is_alive(), f"{fn!r} did not return within {timeout}s"
    return outcome[0]


class _FailingWrites:
    """Replaces ``store_entry_many``; fails while ``failing`` is set."""

    def __init__(self, engine: Engine, exc: BaseException) -> None:
        self.real = engine._disk.store_entry_many  # noqa: SLF001
        self.exc = exc
        self.failing = True
        self.calls = 0
        engine._disk.store_entry_many = self  # type: ignore[union-attr]  # noqa: SLF001

    def __call__(self, entries: Any) -> None:
        self.calls += 1
        if self.failing:
            raise self.exc
        self.real(entries)


def _open(tmp_path: Path, backend: str, runs: dict[str, int]) -> tuple[Engine, Any]:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend, stats=True)

    @engine.query
    def square(i: int) -> int:
        with _count_lock:  # += is not atomic on free-threaded builds
            runs["n"] += 1
        return i * i

    return engine, square


@pytest.mark.parametrize("backend", BACKENDS)
def test_flush_raises_the_write_error_and_the_writer_keeps_going(
    tmp_path: Path, backend: str
) -> None:
    runs = {"n": 0}
    engine, square = _open(tmp_path, backend, runs)
    locked = sqlite3.OperationalError("database is locked")
    writes = _FailingWrites(engine, locked)
    try:
        assert square(2) == 4
        kind, exc = _call(engine.flush_disk)
        assert kind == "raised"
        assert isinstance(exc, PersistentCacheError)
        assert exc.__cause__ is locked
        assert "database is locked" in str(exc)
        assert str(tmp_path) in str(exc) or Path(engine.cache_dir).name in str(exc)

        # In memory nothing changed: a hit, not a re-run.
        assert square(2) == 4
        assert runs["n"] == 1
        # Reported once; the writer is still running and writes again.
        assert engine._evaluator._disk_thread.is_alive()  # noqa: SLF001
        writes.failing = False
        assert square(3) == 9
        assert _call(engine.flush_disk) == ("ok", None)
        assert engine.stats_summary()["disk_stores"] == 1
    finally:
        assert _call(engine.shutdown) == ("ok", None)

    # The result whose write failed is computed again; the later one loads.
    engine, square = _open(tmp_path, backend, runs)
    try:
        assert square(3) == 9
        assert runs["n"] == 2
        assert square(2) == 4
        assert runs["n"] == 3
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_shutdown_raises_an_unreported_write_error_and_closes_the_cache(
    tmp_path: Path, backend: str
) -> None:
    runs = {"n": 0}
    engine, square = _open(tmp_path, backend, runs)
    boom = RuntimeError("disk on fire")
    _FailingWrites(engine, boom)
    assert square(5) == 25
    kind, exc = _call(engine.shutdown)
    assert kind == "raised"
    assert isinstance(exc, PersistentCacheError)
    assert exc.__cause__ is boom
    assert not engine._evaluator._disk_thread.is_alive()  # noqa: SLF001
    # A second shutdown has nothing left to report.
    assert _call(engine.shutdown) == ("ok", None)

    # The cache was closed: a new engine opens the directory normally.
    engine, square = _open(tmp_path, backend, runs)
    try:
        assert square(5) == 25
        assert runs["n"] == 2
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_failed_writes_are_not_counted_as_stored(tmp_path: Path, backend: str) -> None:
    runs = {"n": 0}
    engine, square = _open(tmp_path, backend, runs)
    _FailingWrites(engine, sqlite3.OperationalError("database is locked"))
    try:
        for i in range(5):
            square(i)
        kind, exc = _call(engine.flush_disk)
        assert kind == "raised"
        assert isinstance(exc, PersistentCacheError)
        assert "5 result(s)" in str(exc)
    finally:
        assert _call(engine.shutdown) == ("ok", None)
    assert engine.stats_summary()["disk_stores"] == 0
    events = [t.event for t in engine.traces()]
    assert "disk_store" not in events
    assert events.count("disk_store_failed") == 5


@pytest.mark.parametrize("backend", BACKENDS)
def test_cache_full_error_is_reported_with_its_hint(
    tmp_path: Path, backend: str
) -> None:
    runs = {"n": 0}
    engine, square = _open(tmp_path, backend, runs)
    full = PersistentCacheError(
        "persistent cache is full; pass a larger cache_map_size"
    )
    _FailingWrites(engine, full)
    try:
        square(1)
        kind, exc = _call(engine.flush_disk)
        assert kind == "raised"
        assert isinstance(exc, PersistentCacheError)
        assert exc.__cause__ is full
        assert "cache_map_size" in str(exc)
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_clear_disk_cache_after_a_failed_write_does_not_raise(
    tmp_path: Path, backend: str
) -> None:
    runs = {"n": 0}
    engine, square = _open(tmp_path, backend, runs)
    writes = _FailingWrites(engine, sqlite3.OperationalError("database is locked"))
    try:
        square(1)
        # The failed results are about to be deleted anyway.
        assert _call(engine.clear_disk_cache) == ("ok", None)
        writes.failing = False
        assert _call(engine.flush_disk) == ("ok", None)
    finally:
        assert _call(engine.shutdown) == ("ok", None)


def test_flush_raises_at_once_when_the_writer_was_stopped(tmp_path: Path) -> None:
    runs = {"n": 0}
    engine, square = _open(tmp_path, "sqlite", runs)
    evaluator = engine._evaluator  # noqa: SLF001
    evaluator._disk_queue.put(None)  # noqa: SLF001
    evaluator._disk_thread.join(TIMEOUT)  # noqa: SLF001
    try:
        square(1)
        kind, exc = _call(engine.flush_disk)
        assert kind == "raised"
        assert isinstance(exc, PersistentCacheError)
        assert "not running" in str(exc)
    finally:
        assert _call(engine.shutdown)[0] in ("ok", "raised")


def test_write_error_left_at_exit_is_a_warning(tmp_path: Path) -> None:
    runs = {"n": 0}
    engine, square = _open(tmp_path, "sqlite", runs)
    _FailingWrites(engine, sqlite3.OperationalError("database is locked"))
    try:
        square(1)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            _call(engine._evaluator.flush_disk_at_exit)  # noqa: SLF001
        assert any(
            issubclass(w.category, RuntimeWarning)
            and "database is locked" in str(w.message)
            for w in caught
        )
    finally:
        assert _call(engine.shutdown) == ("ok", None)


def test_writes_from_many_threads_with_intermittent_errors(tmp_path: Path) -> None:
    runs = {"n": 0}
    engine, square = _open(tmp_path, "sqlite", runs)
    writes = _FailingWrites(engine, sqlite3.OperationalError("database is locked"))

    def worker(base: int) -> None:
        for i in range(base, base + 200):
            assert square(i) == i * i
            # Fail and recover now and then while other threads write.
            writes.failing = (i // 37) % 2 == 0

    try:
        threads = [
            threading.Thread(target=worker, args=(n * 1000,), daemon=True)
            for n in range(4)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(TIMEOUT)
            assert not thread.is_alive()
        writes.failing = False
        # Whatever failed is reported once; afterwards flushing is clean.
        first = _call(engine.flush_disk)
        assert first[0] == "ok" or isinstance(first[1], PersistentCacheError)
        assert _call(engine.flush_disk) == ("ok", None)
        assert runs["n"] == 800
    finally:
        assert _call(engine.shutdown) == ("ok", None)


# --- errors MDBX itself raises -------------------------------------------------
#
# The tests above replace ``store_entry_many``. These go through it.

_BLOCK = 256 * 1024
_BLOCKS = 48  # 12 MiB of results


def _open_blocks(tmp_path: Path, map_size: int, runs: dict[str, int]) -> Any:
    engine = Engine(cache_dir=tmp_path, cache_backend="mdbx", cache_map_size=map_size)

    @engine.query(name="block")
    def block(i: int) -> bytes:
        with _count_lock:
            runs["n"] += 1
        return bytes([i]) * _BLOCK

    return engine, block


def test_results_that_do_not_fit_the_map_are_reported_as_a_full_cache(
    tmp_path: Path,
) -> None:
    runs = {"n": 0}
    engine, block = _open_blocks(tmp_path, 1 << 22, runs)  # a map of 4 MiB
    try:
        for i in range(_BLOCKS):
            block(i)
        kind, exc = _call(engine.flush_disk)
        assert kind == "raised"
        assert isinstance(exc, PersistentCacheError)
        assert "is full; pass a larger cache_map_size" in str(exc)
        # Every result is still served from memory.
        assert block(_BLOCKS - 1) == bytes([_BLOCKS - 1]) * _BLOCK
        assert runs["n"] == _BLOCKS
    finally:
        assert _call(engine.shutdown) == ("ok", None)

    # With the larger map the message asks for, the same directory takes
    # them all, and a new engine loads every one.
    engine, block = _open_blocks(tmp_path, 1 << 26, runs)
    try:
        for i in range(_BLOCKS):
            block(i)
        assert _call(engine.flush_disk) == ("ok", None)
    finally:
        assert _call(engine.shutdown) == ("ok", None)
    computed = runs["n"]
    engine, block = _open_blocks(tmp_path, 1 << 26, runs)
    try:
        for i in range(_BLOCKS):
            assert block(i) == bytes([i]) * _BLOCK
        assert runs["n"] == computed
    finally:
        assert _call(engine.shutdown) == ("ok", None)


class _FailingMap:
    """An MDBX map whose writes raise; everything else is the real map's."""

    def __init__(self, real: Any, exc: BaseException) -> None:
        self._real = real
        self._exc = exc

    def put(self, *args: Any, **kwargs: Any) -> None:
        raise self._exc

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def _causes(exc: BaseException | None) -> list[BaseException]:
    chain = []
    while exc is not None:
        chain.append(exc)
        exc = exc.__cause__
    return chain


def test_mdbx_error_other_than_a_full_map_is_reported_as_what_it_is(
    tmp_path: Path,
) -> None:
    """MDBX's own errors are not ``Exception``s: the class is a ``BaseException``.

    The writer thread has to survive one all the same, and ``flush_disk()``
    has to report it, without the advice that belongs to a full cache.
    """
    import mdbx

    runs = {"n": 0}
    engine, square = _open(tmp_path, "mdbx", runs)
    disk = engine._disk  # noqa: SLF001
    corrupted = mdbx.MDBXErrorExc(-30796, "MDBX_CORRUPTED: Database is corrupted")
    assert not isinstance(corrupted, Exception)
    real_meta = disk._meta  # noqa: SLF001
    disk._meta = _FailingMap(real_meta, corrupted)  # noqa: SLF001
    try:
        assert square(2) == 4
        kind, exc = _call(engine.flush_disk)
        assert kind == "raised"
        assert isinstance(exc, PersistentCacheError)
        assert any(cause is corrupted for cause in _causes(exc))
        assert "MDBX_CORRUPTED" in str(exc)
        assert "is full" not in str(exc) and "cache_map_size" not in str(exc)
        # The writer is still running, and writes the next result.
        assert engine._evaluator._disk_thread.is_alive()  # noqa: SLF001
        disk._meta = real_meta  # noqa: SLF001
        assert square(3) == 9
        assert _call(engine.flush_disk) == ("ok", None)
    finally:
        disk._meta = real_meta  # noqa: SLF001
        assert _call(engine.shutdown) == ("ok", None)

    # The write that failed left nothing behind; the later one loads.
    engine, square = _open(tmp_path, "mdbx", runs)
    try:
        assert square(3) == 9
        assert runs["n"] == 2
        assert square(2) == 4
        assert runs["n"] == 3
    finally:
        engine.shutdown()
