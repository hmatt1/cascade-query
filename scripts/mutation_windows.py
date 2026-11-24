"""Mutation testing on Windows: mutmut's mutants and test selection, without fork.

``mutmut run`` forks one child per mutant, so it does not start on Windows.
This does the same loop with one process per mutant, in a pool, each worker
in its own copy of the mutated tree. Everything else is mutmut's: the mutants
(its generator, covered lines only), the instrumented source (its
trampolines), which tests run for a mutant (those that reached the mutated
function in the stats pass, fastest first, stop at the first failure), and
the result files (``mutants/**/*.meta``), which ``scripts/mutation.py
collect`` reads.

``python scripts/mutation.py run [PATTERN ...]`` calls ``all`` here when it
runs on Windows. The stages can also be run one at a time, from the root of
the repository:

    python scripts/mutation_windows.py prepare    coverage pass, mutants/
    python scripts/mutation_windows.py stats      which tests reach which function
    python scripts/mutation_windows.py run [PATTERN ...]
    python scripts/mutation_windows.py all [PATTERN ...]    the three in a row

``run`` checks the mutants that have no verdict yet, so a run that was
stopped goes on where it was. ``--rerun`` checks them again; so does deleting
``mutants/``. ``--from-file FILE`` checks the mutants named in a file, and
``--out NAME --no-meta`` writes their results to
``mutation-workers/_run/NAME`` and leaves the recorded verdicts alone: that
is how a set of kills is repeated.

Two things differ from mutmut, both because there is no fork:

* The test modules are imported with the mutant active, so a mutant in a
  function called at import time can stop pytest before it runs a test. That
  counts as a kill only if the same tests pass with no mutant and the mutant
  then stops pytest again.
* Each test process may commit ``MEMORY_CAP`` bytes and no more. A mutant
  can make a test allocate without end; two of them at 182 GB each once
  starved every other worker until Windows ran out of commit, and tests
  failed for reasons that were not their mutant.

A full run of ``src/cascade`` is about five hours on 28 processes. Leftovers
of one test run (cache folders a mutant put in the wrong place, the examples
Hypothesis keeps of earlier failures) are removed from a worker's copy before
the next one. Some verdicts still change from run to run: Hypothesis draws
new examples every time.
"""

from __future__ import annotations

import argparse
import collections
import ctypes
import fnmatch
import json
import multiprocessing
import os
import platform
import queue
import shutil
import subprocess
import sys
import threading
import time
import tomllib
import types
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve()
MUTANTS = Path("mutants")
WORKERS = Path("mutation-workers")
SKIP_IN_COPIES = shutil.ignore_patterns(
    "*.meta", "mutmut-stats.json", ".hypothesis", "__pycache__", ".pytest_cache"
)
MEMORY_CAP = 8 * 1024**3  # the suite peaks near 0.7 GB, the state machine near 2 GB
TIMEOUT, NO_TESTS, SUSPICIOUS, SEGFAULT = 36, 33, 35, -11  # mutmut's exit codes
STATUS = {
    0: "survived",
    1: "killed",
    3: "killed",
    5: "no tests",
    NO_TESTS: "no tests",
    TIMEOUT: "timeout",
    SUSPICIOUS: "suspicious",
    SEGFAULT: "segfault",
}

# Put on PYTHONPATH of every test process, and so of whatever a test starts:
# the instrumented source imports mutmut, which refuses to import on Windows.
SHIM = '''\
"""Written by scripts/mutation_windows.py: lets mutmut be imported on Windows."""
import multiprocessing
import platform
import sys
import types

sys.modules.setdefault("resource", types.ModuleType("resource"))
multiprocessing.set_start_method = lambda *_a, **_k: None
_real = platform.system
platform.system = lambda: "Linux"
try:
    import mutmut.__main__  # noqa: F401
except Exception:
    pass
finally:
    platform.system = _real
'''


def import_mutmut() -> Any:
    """mutmut refuses to import on Windows and needs the POSIX-only ``resource``."""
    sys.modules.setdefault("resource", types.ModuleType("resource"))
    multiprocessing.set_start_method = lambda *_a, **_k: None  # type: ignore[assignment]
    real = platform.system
    platform.system = lambda: "Linux"
    try:
        import mutmut.__main__ as mm
    finally:
        platform.system = real
    return mm


def mutmut_config(root: Path = Path(".")) -> dict[str, Any]:
    """``[tool.mutmut]`` of the pyproject.toml in ``root``."""
    with open(root / "pyproject.toml", "rb") as f:
        return tomllib.load(f).get("tool", {}).get("mutmut", {})


# --- Windows job objects: a memory cap per test process ----------------------

if sys.platform == "win32":
    from ctypes import wintypes

    class _IoCounters(ctypes.Structure):
        _fields_ = [
            (name, ctypes.c_ulonglong) for name in ("ro", "wo", "oo", "rt", "wt", "ot")
        ]

    class _BasicLimits(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class _ExtendedLimits(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", _BasicLimits),
            ("IoInfo", _IoCounters),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    _k32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _k32.CreateJobObjectW.restype = wintypes.HANDLE
    _k32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    _k32.SetInformationJobObject.restype = wintypes.BOOL
    _k32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
    ]
    _k32.QueryInformationJobObject.restype = wintypes.BOOL
    _k32.QueryInformationJobObject.argtypes = [
        wintypes.HANDLE,
        ctypes.c_int,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.c_void_p,
    ]
    _k32.AssignProcessToJobObject.restype = wintypes.BOOL
    _k32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _k32.TerminateJobObject.restype = wintypes.BOOL
    _k32.TerminateJobObject.argtypes = [wintypes.HANDLE, wintypes.UINT]
    _k32.CloseHandle.argtypes = [wintypes.HANDLE]
    _EXTENDED_LIMIT_INFORMATION = 9
    _LIMIT_PROCESS_MEMORY, _LIMIT_KILL_ON_JOB_CLOSE = 0x100, 0x2000

    def new_job(memory_cap: int | None) -> int:
        """A job whose processes end with it and, with a cap, may each commit that much."""
        job = _k32.CreateJobObjectW(None, None)
        if not job:
            raise ctypes.WinError(ctypes.get_last_error())
        info = _ExtendedLimits()
        info.BasicLimitInformation.LimitFlags = _LIMIT_KILL_ON_JOB_CLOSE | (
            _LIMIT_PROCESS_MEMORY if memory_cap else 0
        )
        info.ProcessMemoryLimit = memory_cap or 0
        if not _k32.SetInformationJobObject(
            job, _EXTENDED_LIMIT_INFORMATION, ctypes.byref(info), ctypes.sizeof(info)
        ):
            raise ctypes.WinError(ctypes.get_last_error())
        return job

    def job_peak(job: int) -> int:
        """Most memory any one process of the job has committed, in bytes."""
        info = _ExtendedLimits()
        _k32.QueryInformationJobObject(
            job,
            _EXTENDED_LIMIT_INFORMATION,
            ctypes.byref(info),
            ctypes.sizeof(info),
            None,
        )
        return int(info.PeakProcessMemoryUsed)


# --- prepare ------------------------------------------------------------------


def prepare() -> int:
    """mutmut's coverage pass and mutant generation, into ``mutants/``."""
    # See scripts/mutation.py: msgpack must be imported before mutmut unloads
    # modules, and the trampolines need three times the stack.
    import msgpack  # noqa: F401

    sys.setrecursionlimit(3 * sys.getrecursionlimit())
    mm = import_mutmut()
    from mutmut.configuration import Config

    os.environ["MUTANT_UNDER_TEST"] = "mutant_generation"
    Config.ensure_loaded()
    MUTANTS.mkdir(exist_ok=True)
    mm.copy_src_dir()
    mm.copy_also_copy_files()
    mm.setup_source_paths()
    started = time.time()
    mm.store_lines_covered_by_tests()
    print(f"coverage pass: {time.time() - started:.0f}s", flush=True)
    started = time.time()
    for path in mm.walk_source_files():
        result = mm.create_file_mutants(path)
        if result.error:
            raise result.error
    print(f"mutant generation: {time.time() - started:.0f}s", flush=True)
    print(f"{len(load_mutants())} mutants", flush=True)
    return 0


# --- shared -------------------------------------------------------------------


def meta_files() -> list[Path]:
    return sorted(MUTANTS.rglob("*.py.meta"))


def load_mutants() -> dict[str, int | None]:
    mutants: dict[str, int | None] = {}
    for meta in meta_files():
        mutants.update(json.loads(meta.read_text(encoding="utf-8"))["exit_code_by_key"])
    return mutants


def function_of(mutant: str) -> str:
    return mutant.partition("__mutmut_")[0]


def worker_dirs(n: int, fresh: bool = False) -> list[Path]:
    """One copy of the mutated tree per worker, so that test runs cannot collide."""
    if fresh:
        for old in WORKERS.glob("w[0-9]*"):
            shutil.rmtree(old)
    dirs = []
    for i in range(n):
        d = WORKERS / f"w{i:02d}"
        if not d.exists():
            shutil.copytree(MUTANTS, d, ignore=SKIP_IN_COPIES)
        dirs.append(d)
    return dirs


def shim_dir() -> Path:
    directory = (WORKERS / "_shim").resolve()
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "sitecustomize.py"
    if not target.exists() or target.read_text(encoding="utf-8") != SHIM:
        target.write_text(SHIM, encoding="utf-8")
    return directory


def child_env(mutant: str) -> dict[str, str]:
    env = dict(os.environ)
    env["MUTANT_UNDER_TEST"] = mutant
    env["PYTHONUTF8"] = "1"
    paths = [str(shim_dir()), env.get("PYTHONPATH", "")]
    env["PYTHONPATH"] = os.pathsep.join(p for p in paths if p)
    return env


def remove_test_folders(scratch: Path) -> None:
    """Delete what the tests of the last runs wrote (pytest's --basetemp).

    Each worker's folder holds the files of its last test run only, but with
    the caches the tests open that is some 300 MB a worker.
    """
    for folder in scratch.glob("bt_*"):
        shutil.rmtree(folder, ignore_errors=True)


def kill_tree(pid: int) -> None:
    subprocess.run(
        ["taskkill", "/T", "/F", "/PID", str(pid)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def run_child(
    args: list[str],
    cwd: Path,
    mutant: str,
    timeout: float,
    log: Path | None = None,
    memory_cap: int | None = None,
    info: dict | None = None,
) -> int | None:
    """Exit code of ``mutation_windows.py child ...`` run in ``cwd``; None on timeout.

    The child and everything it starts are in one job: they end with it, and
    with ``memory_cap`` none of them can commit more than that. ``info`` gets
    the peak memory of the largest process.
    """
    out = open(log, "w", encoding="utf-8") if log else subprocess.DEVNULL
    job = new_job(memory_cap)
    try:
        proc = subprocess.Popen(
            [sys.executable, "-X", "utf8", str(HERE), "child", *args],
            cwd=cwd,
            env=child_env(mutant),
            stdout=out,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.BELOW_NORMAL_PRIORITY_CLASS
            | subprocess.CREATE_NEW_PROCESS_GROUP,
        )
        _k32.AssignProcessToJobObject(job, int(proc._handle))  # type: ignore[attr-defined]
        try:
            return proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            _k32.TerminateJobObject(job, 1)
            kill_tree(proc.pid)
            proc.wait()
            return None
    finally:
        if info is not None:
            info["peak"] = job_peak(job)
        _k32.TerminateJobObject(job, 1)  # whatever a test left running
        _k32.CloseHandle(job)
        if log:
            out.close()  # type: ignore[union-attr]


# --- child processes ----------------------------------------------------------


def child(mode: str, ids_file: str, basetemp: str, out_file: str | None) -> int:
    """Run in a worker's copy of the mutated tree (the working directory)."""
    sys.setrecursionlimit(3 * sys.getrecursionlimit())
    sys.path.insert(0, str(Path("src").resolve()))
    ids = [i for i in Path(ids_file).read_text(encoding="utf-8").split("\n") if i]
    import pytest

    config = mutmut_config()
    common = [
        "--rootdir=.",
        "-q",
        "-p",
        "no:cacheprovider",
        "--basetemp",
        basetemp,
        *config.get("pytest_add_cli_args", []),
    ]
    if mode == "collect":
        assert out_file is not None
        collected = Path(out_file)

        class Ids:
            def pytest_collection_modifyitems(self, items: list[Any]) -> None:
                collected.write_text(
                    "\n".join(i.nodeid for i in items), encoding="utf-8"
                )

        return int(pytest.main([*common, "--collect-only", *ids], plugins=[Ids()]))
    if mode == "stats":
        assert out_file is not None
        import mutmut.mutation.trampoline as trampoline

        # mutmut's own hook resolves the source paths on every call, which it
        # only needs for max_stack_depth. Recording the name is all it does
        # otherwise, and what makes the stats pass take minutes, not hours.
        hits: set[str] = set()
        trampoline.record_trampoline_hit = hits.add  # type: ignore[assignment]
        by_function: dict[str, set[str]] = collections.defaultdict(set)
        durations: dict[str, float] = {}

        class Stats:
            def pytest_runtest_logstart(self, nodeid: str, location: Any) -> None:
                durations[nodeid] = 0

            def pytest_runtest_teardown(self, item: Any, nextitem: Any) -> None:
                for name in hits:
                    by_function[name].add(item.nodeid)
                hits.clear()

            def pytest_runtest_makereport(self, item: Any, call: Any) -> None:
                durations[item.nodeid] += call.duration

        code = int(pytest.main([*common, "--tb=short", *ids], plugins=[Stats()]))
        Path(out_file).write_text(
            json.dumps(
                {
                    "exit": code,
                    "by_function": {k: sorted(v) for k, v in by_function.items()},
                    "durations": durations,
                }
            ),
            encoding="utf-8",
        )
        return code
    assert mode == "run"
    code = int(pytest.main([*common, "-x", "--tb=no", *ids]))
    sys.stdout.flush()
    os._exit(code)  # a mutant may leave threads behind that never end


# --- stats --------------------------------------------------------------------


def stats(workers: int) -> int:
    """Which tests reach which function, and how long each test takes.

    Makes the workers' copies anew: they must match ``mutants/``.
    """
    dirs = worker_dirs(workers, fresh=True)
    scratch = (WORKERS / "_stats").resolve()
    scratch.mkdir(parents=True, exist_ok=True)
    all_ids = scratch / "all_ids.txt"
    selection = mutmut_config().get("pytest_add_cli_args_test_selection", ["tests/"])
    (scratch / "collect_args.txt").write_text("\n".join(selection), encoding="utf-8")
    code = run_child(
        [
            "collect",
            str(scratch / "collect_args.txt"),
            str(scratch / "bt_collect"),
            str(all_ids),
        ],
        dirs[0],
        "",
        600,
        scratch / "collect.log",
    )
    if code != 0:
        print("collecting tests failed, see", scratch / "collect.log")
        return 1
    ids = all_ids.read_text(encoding="utf-8").split("\n")
    print(f"{len(ids)} tests, {workers} workers", flush=True)
    started = time.time()
    codes: dict[int, int | None] = {}

    def one(i: int) -> None:
        ids_file = scratch / f"ids_{i:02d}.txt"
        ids_file.write_text("\n".join(ids[i::workers]), encoding="utf-8")
        codes[i] = run_child(
            [
                "stats",
                str(ids_file),
                str(scratch / f"bt_{i:02d}"),
                str(scratch / f"out_{i:02d}.json"),
            ],
            dirs[i],
            "stats",
            4 * 3600,
            scratch / f"stats_{i:02d}.log",
        )

    threads = [threading.Thread(target=one, args=(i,)) for i in range(workers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"stats pass: {time.time() - started:.0f}s", flush=True)
    remove_test_folders(scratch)
    by_function: dict[str, set[str]] = collections.defaultdict(set)
    durations: dict[str, float] = {}
    bad = sorted(i for i, c in codes.items() if c != 0)
    for i in range(workers):
        out = scratch / f"out_{i:02d}.json"
        if not out.exists():
            continue
        data = json.loads(out.read_text(encoding="utf-8"))
        for name, tests in data["by_function"].items():
            by_function[name].update(tests)
        durations.update(data["durations"])
    if bad:
        print(
            f"tests failed with no mutant active in buckets {bad}: "
            f"see {scratch}/stats_NN.log"
        )
        return 1
    (MUTANTS / "mutmut-stats.json").write_text(
        json.dumps(
            {
                "tests_by_mangled_function_name": {
                    k: sorted(v) for k, v in by_function.items()
                },
                "duration_by_test": durations,
                "stats_time": time.time() - started,
            }
        ),
        encoding="utf-8",
    )
    mutants = load_mutants()
    untested = sum(1 for m in mutants if not by_function.get(function_of(m)))
    print(
        f"{len(durations)} tests ran, {sum(durations.values()):.0f}s of test time; "
        f"{len(by_function)} functions reached; "
        f"{untested} of {len(mutants)} mutants have no test",
        flush=True,
    )
    return 0


# --- run ----------------------------------------------------------------------


def map_exit(code: int | None) -> int:
    if code is None:
        return TIMEOUT
    if code in (0, 1, 3, 5):
        return code
    if code >= 0xC0000000 or code < 0:
        return SEGFAULT
    return SUSPICIOUS


def flush_meta(
    results: dict[str, tuple[int, float]], estimates: dict[str, float]
) -> None:
    for meta in meta_files():
        data = json.loads(meta.read_text(encoding="utf-8"))
        changed = False
        for mutant in data["exit_code_by_key"]:
            if mutant in results:
                code, seconds = results[mutant]
                if data["exit_code_by_key"][mutant] != code:
                    changed = True
                data["exit_code_by_key"][mutant] = code
                data["durations_by_key"][mutant] = seconds
                data["estimated_durations_by_key"][mutant] = estimates.get(mutant, 0.0)
        if changed:
            meta.write_text(json.dumps(data, indent=4), encoding="utf-8")


def clean_worker(directory: Path, keep: set[str]) -> None:
    """Remove what earlier runs left in a worker's copy.

    A mutant can change where the engine writes (a cache directory of ``None``,
    lock files in the working directory), and Hypothesis keeps the failing
    examples of earlier mutants. Neither may reach the next run.
    """
    for entry in directory.iterdir():
        if entry.name == ".hypothesis":
            shutil.rmtree(entry / "examples", ignore_errors=True)
        elif entry.name in keep:
            continue
        elif entry.is_dir():
            shutil.rmtree(entry, ignore_errors=True)
        else:
            try:
                entry.unlink()
            except OSError:
                pass


def first_failure(log: Path) -> str | None:
    """The test pytest stopped at, from its short summary."""
    try:
        lines = log.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return None
    for line in lines:
        if line.startswith(("FAILED ", "ERROR ")):
            return line.split(" ", 1)[1].split(" - ", 1)[0].strip()
    return None


def run(
    workers: int,
    patterns: list[str],
    limit: int | None = None,
    rerun: bool = False,
    from_file: str | None = None,
    out_name: str = "results.jsonl",
    write_meta: bool = True,
) -> int:
    stats_data = json.loads((MUTANTS / "mutmut-stats.json").read_text(encoding="utf-8"))
    by_function = stats_data["tests_by_mangled_function_name"]
    durations = stats_data["duration_by_test"]
    mutants = load_mutants()
    if from_file:
        wanted = [
            m for m in Path(from_file).read_text(encoding="utf-8").split("\n") if m
        ]
        unknown = [m for m in wanted if m not in mutants]
        if unknown:
            raise SystemExit(
                f"{len(unknown)} unknown mutants in {from_file}, e.g. {unknown[0]}"
            )
        todo = wanted
    else:
        todo = [
            m
            for m, code in mutants.items()
            if (rerun or code is None)
            and (not patterns or any(fnmatch.fnmatch(m, p) for p in patterns))
        ]
    estimates: dict[str, float] = {
        m: sum(durations[t] for t in by_function.get(function_of(m), ())) for m in todo
    }
    todo.sort(key=lambda m: estimates[m])  # as mutmut: cheapest first
    if limit:
        todo = todo[:: max(1, len(todo) // limit)][:limit]
    dirs = worker_dirs(workers)
    also_copied = mutmut_config().get("also_copy", [])
    keep = {"src", "tests", "pyproject.toml", *(Path(p).parts[0] for p in also_copied)}
    scratch = (WORKERS / "_run").resolve()
    (scratch / "odd").mkdir(parents=True, exist_ok=True)
    print(
        f"{len(todo)} mutants to check with {workers} workers, "
        f"{MEMORY_CAP / 1024**3:.0f} GB a process; worst case "
        f"{sum(estimates.values()) / 3600:.1f} h of test time if every one survived",
        flush=True,
    )
    pending: queue.Queue[str] = queue.Queue()
    for m in todo:
        pending.put(m)
    results: dict[str, tuple[int, float]] = {}
    raw_log = open(scratch / out_name, "a", encoding="utf-8")
    lock = threading.Lock()
    started = time.time()

    def worker(i: int) -> None:
        ids_file = scratch / f"ids_{i:02d}.txt"
        out_file = scratch / f"out_{i:02d}.txt"
        child_args = ["run", str(ids_file), str(scratch / f"bt_{i:02d}")]

        def attempt(mutant: str, limit_s: float) -> tuple[int | None, int]:
            clean_worker(dirs[i], keep)
            info: dict = {}
            raw = run_child(
                child_args, dirs[i], mutant, limit_s, out_file, MEMORY_CAP, info
            )
            return raw, info.get("peak", 0)

        while True:
            try:
                mutant = pending.get_nowait()
            except queue.Empty:
                return
            tests = by_function.get(function_of(mutant), [])
            began = time.time()
            raw: int | None = None
            peak, failed, note = 0, None, ""
            if not tests:
                code = NO_TESTS
            else:
                ids_file.write_text(
                    "\n".join(sorted(tests, key=durations.__getitem__)),
                    encoding="utf-8",
                )
                # mutmut allows 15x the tests' time; these times were taken
                # under the same load, and starting Python is on top.
                limit_s = max(180.0, 6 * estimates[mutant] + 90)
                raw, peak = attempt(mutant, limit_s)
                code = map_exit(raw)
                failed = first_failure(out_file)
                if raw in (2, 4):
                    # pytest gave up before running a test: an import or
                    # collection error, or a test it could not find. That can
                    # be the mutant's doing, and then it is a kill. It can
                    # also be the machine's (out of memory once). So the same
                    # tests must pass with no mutant, and the mutant must then
                    # do it again.
                    with lock:
                        serial = len(results)
                    shutil.copyfile(
                        out_file, scratch / "odd" / f"{serial:05d}_first_w{i:02d}.txt"
                    )
                    clean, _ = attempt("", limit_s)
                    if clean != 0:
                        code, note = SUSPICIOUS, f"clean run gave {clean}"
                    else:
                        again, peak = attempt(mutant, limit_s)
                        failed = first_failure(out_file)
                        note = f"first run gave {raw}"
                        if again in (2, 4):
                            code = 1
                        else:
                            raw, code = again, map_exit(again)
                if code not in (0, 1):
                    name = "".join(
                        c if c.isalnum() else "_"
                        for c in mutant.encode("ascii", "replace").decode()
                    )
                    try:
                        tail = out_file.read_text(
                            encoding="utf-8", errors="replace"
                        ).splitlines()[-80:]
                        (scratch / "odd" / f"{name}.txt").write_text(
                            "\n".join(tail), encoding="utf-8"
                        )
                    except OSError:
                        pass
            ended = time.time()
            with lock:
                results[mutant] = (code, ended - began)
                row = {
                    "mutant": mutant,
                    "raw": raw,
                    "code": code,
                    "seconds": round(ended - began, 1),
                    "start": round(began - started, 1),
                    "end": round(ended - started, 1),
                    "peak_mb": round(peak / 1024**2),
                    "failed": failed,
                    "note": note,
                }
                raw_log.write(json.dumps(row) + "\n")
                raw_log.flush()

    threads = [
        threading.Thread(target=worker, args=(i,), daemon=True) for i in range(workers)
    ]
    for t in threads:
        t.start()
    last = 0.0
    while any(t.is_alive() for t in threads):
        time.sleep(2)
        if time.time() - last >= 60:
            last = time.time()
            with lock:
                snapshot = dict(results)
            if write_meta:
                flush_meta(snapshot, estimates)
            counts = collections.Counter(STATUS[c] for c, _ in snapshot.values())
            elapsed = time.time() - started
            # The queue is sorted cheapest first, so pace by test time, not by count.
            done_cost = sum(estimates[m] for m in snapshot)
            total_cost = sum(estimates.values()) or 1.0
            print(
                f"[{elapsed / 60:6.1f} min] {len(snapshot)}/{len(todo)} mutants, "
                f"{100 * done_cost / total_cost:.0f}% of the worst-case test time; "
                f"{dict(counts)}",
                flush=True,
            )
    if write_meta:
        flush_meta(results, estimates)
    raw_log.close()
    remove_test_folders(scratch)
    counts = collections.Counter(STATUS[c] for c, _ in results.values())
    print(f"done in {(time.time() - started) / 60:.1f} min: {dict(counts)}", flush=True)
    return 0


def everything(workers: int, patterns: list[str], rerun: bool) -> int:
    """``prepare``, ``stats`` and ``run`` in a row: what ``mutmut run`` does."""
    # In a process of its own: the coverage pass runs the suite inside it.
    code = subprocess.run(
        [sys.executable, "-X", "utf8", str(HERE), "prepare"]
    ).returncode
    if code != 0:
        return code
    code = stats(workers)
    if code != 0:
        return code
    return run(workers, patterns, rerun=rerun)


def main(argv: list[str]) -> int:
    if sys.platform != "win32":
        print(
            "scripts/mutation_windows.py is for Windows. Elsewhere "
            "`python scripts/mutation.py run` runs mutmut itself.",
            file=sys.stderr,
        )
        return 2
    if argv[:1] == ["child"]:
        mode, ids_file, basetemp, *rest = argv[1:]
        return child(mode, ids_file, basetemp, rest[0] if rest else None)
    default_workers = max(1, (os.cpu_count() or 2) - 4)
    parser = argparse.ArgumentParser(
        description="Mutation testing on Windows: mutmut without fork."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("prepare", help="coverage pass and mutants, into mutants/")
    with_workers = [
        commands.add_parser("stats", help="which tests reach which function"),
        commands.add_parser("run", help="check the mutants that have no verdict"),
        commands.add_parser("all", help="prepare, stats and run in a row"),
    ]
    for p in with_workers:
        p.add_argument(
            "-w",
            "--workers",
            "--max-children",
            type=int,
            default=default_workers,
            help=f"test processes at a time (default: {default_workers})",
        )
    for p in with_workers[1:]:
        p.add_argument("patterns", nargs="*", help="e.g. 'cascade._state.*'")
        p.add_argument("--rerun", action="store_true", help="also checked mutants")
    p = with_workers[1]
    p.add_argument("--limit", type=int, help="an even sample of this many")
    p.add_argument("--from-file", help="exactly the mutants named in this file")
    p.add_argument(
        "--out", default="results.jsonl", help="raw results, in mutation-workers/_run"
    )
    p.add_argument(
        "--no-meta", action="store_true", help="leave mutants/**/*.meta as they are"
    )
    args = parser.parse_args(argv)
    if args.command == "prepare":
        return prepare()
    if args.command == "stats":
        return stats(args.workers)
    if args.command == "all":
        return everything(args.workers, args.patterns, args.rerun)
    return run(
        args.workers,
        args.patterns,
        args.limit,
        args.rerun,
        args.from_file,
        args.out,
        not args.no_meta,
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
