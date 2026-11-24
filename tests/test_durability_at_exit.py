"""Results reach disk when a process ends normally without shutdown() (G17).

Each test runs a child interpreter, because the behaviour under test is what
happens at interpreter exit. Every child has a timeout: an exit hook that
waited on a dead writer thread would otherwise hang the suite.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

import cascade

BACKENDS = ("mdbx", "sqlite")
CHILD_TIMEOUT = 120

_CHILD = """
import sys
from cascade import Engine

mode, cache_dir, backend, count = sys.argv[1:5]
count = int(count)
engine = Engine(cache_dir=cache_dir, cache_backend=backend, max_entries=100_000)
runs = [0]

@engine.query
def square(i):
    runs[0] += 1
    return i * i

if mode == "dead-writer":
    # A write error no longer stops the writer (D18), so stop it from outside:
    # the results computed below stay queued with nothing to drain them.
    engine._evaluator._disk_queue.put(None)
    engine._evaluator._disk_thread.join(30)

for i in range(count):
    square(i)

if mode == "read":
    print(runs[0])
    engine.shutdown()
elif mode == "sys-exit":
    sys.exit(3)
elif mode == "raise":
    raise RuntimeError("unhandled")
elif mode == "hard-kill":
    import os
    os._exit(0)
elif mode == "dropped":
    del engine, square
    import gc
    gc.collect()
elif mode == "dead-writer":
    engine._evaluator._disk_thread.join(30)
    print("writer-alive", engine._evaluator._disk_thread.is_alive())
# "write": fall off the end without shutdown() or flush_disk().
"""


def _run_child(
    mode: str, cache_dir: Path, backend: str, count: int
) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    src = str(Path(cascade.__file__).resolve().parent.parent)
    env["PYTHONPATH"] = os.pathsep.join(
        p for p in (src, env.get("PYTHONPATH", "")) if p
    )
    return subprocess.run(
        [sys.executable, "-c", _CHILD, mode, str(cache_dir), backend, str(count)],
        env=env,
        capture_output=True,
        text=True,
        timeout=CHILD_TIMEOUT,
        check=False,
    )


def _bodies_run_on_reread(cache_dir: Path, backend: str, count: int) -> int:
    done = _run_child("read", cache_dir, backend, count)
    assert done.returncode == 0, done.stderr
    return int(done.stdout.strip())


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    ("mode", "returncode"),
    [("write", 0), ("sys-exit", 3), ("raise", 1), ("dropped", 0)],
)
def test_normal_exit_without_shutdown_keeps_every_result(
    tmp_path: Path, backend: str, mode: str, returncode: int
) -> None:
    count = 500
    done = _run_child(mode, tmp_path, backend, count)
    assert done.returncode == returncode, done.stderr
    assert _bodies_run_on_reread(tmp_path, backend, count) == 0


def test_exit_does_not_hang_when_the_writer_thread_is_dead(tmp_path: Path) -> None:
    # The child's writer thread is stopped before any result is queued,
    # leaving the queue non-empty for good. The exit hook must give up
    # rather than wait.
    done = _run_child("dead-writer", tmp_path, "sqlite", 50)
    assert done.returncode == 0, done.stderr
    assert "writer-alive False" in done.stdout


def test_hard_kill_is_not_covered_but_leaves_a_usable_cache(tmp_path: Path) -> None:
    # os._exit skips exit hooks, so queued results may be lost. Whatever is
    # on disk must still open and give correct answers.
    count = 200
    done = _run_child("hard-kill", tmp_path, "mdbx", count)
    assert done.returncode == 0, done.stderr
    assert 0 <= _bodies_run_on_reread(tmp_path, "mdbx", count) <= count


def test_exit_hook_tracks_only_engines_with_a_running_writer(tmp_path: Path) -> None:
    from cascade import Engine, _evaluator

    in_memory = Engine()
    assert in_memory._evaluator not in _evaluator._live_disk_writers  # noqa: SLF001
    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
    evaluator = engine._evaluator  # noqa: SLF001
    assert evaluator in _evaluator._live_disk_writers  # noqa: SLF001
    engine.shutdown()
    assert evaluator not in _evaluator._live_disk_writers  # noqa: SLF001
    # After shutdown the writer is gone; the exit flush must return at once.
    evaluator.flush_disk_at_exit()
