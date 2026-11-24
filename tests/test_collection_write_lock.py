"""One engine at a time changes a named collection (G26, D10).

Two engines on one ``cache_dir`` share cached query results and may both
read a named collection. The first engine to change the collection takes an
OS-level lock on it, held until ``shutdown()`` or the end of the process; a
second engine that tries to change it gets ``PersistentCacheError`` and
nothing changes. An engine whose copy is older than what is on disk cannot
change it either. Child processes have timeouts, so a regression fails
instead of hanging the suite.
"""

from __future__ import annotations

import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any

import pytest

import cascade
from cascade import CascadeDict, CascadeList, CascadeSet, Engine, PersistentCacheError

BACKENDS = ("mdbx", "sqlite")
CHILD_TIMEOUT = 60


@pytest.fixture(params=BACKENDS)
def backend(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture
def engines() -> Any:
    opened: list[Engine] = []
    yield opened
    for engine in opened:
        engine.shutdown()


def _open(engines: list[Engine], cache: Path, backend: str) -> Engine:
    engine = Engine(cache_dir=cache, cache_backend=backend)
    engines.append(engine)
    return engine


def _restored(cache: Path, backend: str, name: str = "xs") -> list[Any]:
    engine = Engine(cache_dir=cache, cache_backend=backend)
    try:
        return list(CascadeList(engine, name=name))
    finally:
        engine.shutdown()


def test_second_writer_is_refused_and_changes_nothing(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    first = _open(engines, tmp_path, backend)
    second = _open(engines, tmp_path, backend)
    xs1 = CascadeList(first, [1, 2], name="xs")
    xs2 = CascadeList(second, name="xs")
    assert list(xs2) == [1, 2]  # reading is fine
    xs1.append(3)
    with pytest.raises(PersistentCacheError) as info:
        xs2.append("other")
    message = str(info.value)
    assert "'xs'" in message and "another Engine" in message
    assert repr(str(tmp_path)) in message
    # The refused change was undone in memory and never reached disk.
    assert list(xs2) == [1, 2]
    xs1.append(4)
    for engine in engines:
        engine.shutdown()
    engines.clear()
    assert _restored(tmp_path, backend) == [1, 2, 3, 4]


@pytest.mark.parametrize("kind", ["list", "set", "dict"])
def test_every_kind_of_change_is_refused(
    tmp_path: Path, backend: str, engines: list[Engine], kind: str
) -> None:
    first = _open(engines, tmp_path, backend)
    second = _open(engines, tmp_path, backend)
    if kind == "list":
        CascadeList(first, [1], name="c")
        other = CascadeList(second, name="c")
        change = lambda: other.extend([2, 3])  # noqa: E731
    elif kind == "set":
        CascadeSet(first, {1}, name="c")
        other = CascadeSet(second, name="c")
        change = lambda: other.add(2)  # noqa: E731
    else:
        CascadeDict(first, {"a": 1}, name="c")
        other = CascadeDict(second, name="c")
        change = lambda: other.update(b=2)  # noqa: E731
    before = other.copy()
    with pytest.raises(PersistentCacheError):
        change()
    assert other.copy() == before
    # Compaction rewrites the log on disk, so it is a change too.
    with pytest.raises(PersistentCacheError):
        other.compact()


def test_refused_inside_a_transaction_rolls_back(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    first = _open(engines, tmp_path, backend)
    second = _open(engines, tmp_path, backend)
    CascadeList(first, [1], name="xs")
    xs2 = CascadeList(second, name="xs")
    mine = CascadeList(second, name="mine")
    with pytest.raises(PersistentCacheError):
        with second.transaction():
            mine.append("kept?")
            xs2.append("refused")
    assert list(mine) == []
    assert list(xs2) == [1]


def test_lock_is_released_by_shutdown(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    first = _open(engines, tmp_path, backend)
    CascadeList(first, [1], name="xs").append(2)
    first.shutdown()
    second = _open(engines, tmp_path, backend)
    xs2 = CascadeList(second, name="xs")
    xs2.append(3)
    second.shutdown()
    engines.clear()
    assert _restored(tmp_path, backend) == [1, 2, 3]


def test_an_out_of_date_copy_cannot_be_changed(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    first = _open(engines, tmp_path, backend)
    reader = _open(engines, tmp_path, backend)
    xs1 = CascadeList(first, [1], name="xs")
    stale = CascadeList(reader, name="xs")
    xs1.append(2)
    first.shutdown()  # the lock is free, but the reader's copy is old
    with pytest.raises(PersistentCacheError, match="out of date"):
        stale.append("lost")
    assert list(stale) == [1]
    reader.shutdown()
    engines.clear()
    assert _restored(tmp_path, backend) == [1, 2]
    # A new engine sees the current contents and may change them.
    fresh = _open(engines, tmp_path, backend)
    xs = CascadeList(fresh, name="xs")
    xs.append(3)
    assert list(xs) == [1, 2, 3]


def test_out_of_date_after_another_engine_compacted(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    first = _open(engines, tmp_path, backend)
    reader = _open(engines, tmp_path, backend)
    xs1 = CascadeList(first, [1], name="xs")
    stale = CascadeList(reader, name="xs")
    xs1.extend([2, 3])
    xs1.compact()  # no log entries left on disk, only a snapshot
    first.shutdown()
    with pytest.raises(PersistentCacheError, match="out of date"):
        stale.append("lost")


def test_engines_may_change_different_collections_and_share_results(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    runs = {"n": 0}

    def build(engine: Engine) -> Any:
        @engine.input
        def word() -> str:
            return "abc"

        @engine.query
        def shout() -> str:
            runs["n"] += 1
            return word().upper()

        return shout

    first = _open(engines, tmp_path, backend)
    second = _open(engines, tmp_path, backend)
    a = CascadeList(first, name="a")
    b = CascadeList(second, name="b")
    a.append(1)
    b.append(2)
    assert build(first)() == "ABC"
    first.flush_disk()
    assert build(second)() == "ABC"
    assert runs["n"] == 1  # the second engine loaded the first one's result
    for engine in engines:
        engine.shutdown()
    engines.clear()
    assert _restored(tmp_path, backend, "a") == [1]
    assert _restored(tmp_path, backend, "b") == [2]


def test_queries_over_a_shared_collection_never_mix_contents(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    def build(engine: Engine, xs: CascadeList) -> Any:
        @engine.query
        def total() -> int:
            return sum(x for x in xs)

        return total

    first = _open(engines, tmp_path, backend)
    xs1 = CascadeList(first, [1, 2], name="xs")
    total1 = build(first, xs1)
    reader = _open(engines, tmp_path, backend)
    xs2 = CascadeList(reader, name="xs")
    total2 = build(reader, xs2)
    xs1.append(10)
    assert total1() == 13
    first.flush_disk()
    # The reader still holds the contents it loaded, and its result matches.
    assert total2() == 3
    reader.flush_disk()
    with pytest.raises(PersistentCacheError):
        xs2.append(100)
    assert total2() == 3
    for engine in engines:
        engine.shutdown()
    engines.clear()
    third = _open(engines, tmp_path, backend)
    assert build(third, CascadeList(third, name="xs"))() == 13


def test_threads_of_two_engines_racing_for_one_collection(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    pair = [_open(engines, tmp_path, backend) for _ in range(2)]
    lists = [CascadeList(engine, name="xs") for engine in pair]
    barrier = threading.Barrier(2)
    outcome: dict[int, Any] = {}

    def writer(i: int) -> None:
        barrier.wait(10)
        try:
            for n in range(20):
                lists[i].append((i, n))
            outcome[i] = "ok"
        except PersistentCacheError as exc:
            outcome[i] = exc

    threads = [threading.Thread(target=writer, args=(i,), daemon=True) for i in (0, 1)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(30)
        assert not thread.is_alive()
    winners = [i for i in (0, 1) if outcome[i] == "ok"]
    assert len(winners) == 1, outcome
    for engine in engines:
        engine.shutdown()
    engines.clear()
    assert _restored(tmp_path, backend) == [(winners[0], n) for n in range(20)]


_CHILD = """
import os, sys
from cascade import CascadeList, Engine

mode, cache_dir, backend = sys.argv[1:4]
engine = Engine(cache_dir=cache_dir, cache_backend=backend)
xs = CascadeList(engine, name="xs")
xs.append("child")
print("changed", flush=True)
if mode == "crash":
    os._exit(0)
sys.stdin.readline()
engine.shutdown()
"""


def _child(mode: str, cache: Path, backend: str) -> subprocess.Popen[str]:
    env = dict(os.environ)
    src = str(Path(cascade.__file__).resolve().parent.parent)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (src, env.get("PYTHONPATH", "")) if p
    )
    return subprocess.Popen(
        [sys.executable, "-c", _CHILD, mode, str(cache), backend],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _read_line(proc: subprocess.Popen[str]) -> str:
    line: list[str] = []
    reader = threading.Thread(
        target=lambda: line.append(proc.stdout.readline()),  # type: ignore[union-attr]
        daemon=True,
    )
    reader.start()
    reader.join(CHILD_TIMEOUT)
    assert line, "child process did not report"
    return line[0].strip()


def test_another_process_holding_the_collection_is_refused(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    proc = _child("hold", tmp_path, backend)
    try:
        assert _read_line(proc) == "changed", proc.stderr.read()  # type: ignore[union-attr]
        engine = _open(engines, tmp_path, backend)
        xs = CascadeList(engine, name="xs")
        assert list(xs) == ["child"]
        with pytest.raises(PersistentCacheError, match="another Engine"):
            xs.append("parent")
    finally:
        proc.communicate("\n", timeout=CHILD_TIMEOUT)
    assert proc.returncode == 0


def test_a_crashed_holder_leaves_no_lock_behind(
    tmp_path: Path, backend: str, engines: list[Engine]
) -> None:
    proc = _child("crash", tmp_path, backend)
    out, err = proc.communicate(timeout=CHILD_TIMEOUT)
    assert proc.returncode == 0, err
    assert out.strip() == "changed"
    # The lock file is still there; the lock is not.
    assert list((tmp_path / "locks").iterdir())
    engine = _open(engines, tmp_path, backend)
    xs = CascadeList(engine, name="xs")
    xs.append("parent")
    assert list(xs) == ["child", "parent"]
