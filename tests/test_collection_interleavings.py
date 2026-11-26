"""Collections under interleavings the test chooses, not the OS.

The stress tests in ``test_collection_concurrency.py`` run threads freely and
check the end state; which interleavings they reach is luck. Here a
scheduler runs one thread at a time and decides, at every pause point, which
thread goes next. A list of picks (the schedule) fixes the interleaving, so
a failure replays exactly, and small scenarios are explored exhaustively.

Pause points:

* every outermost acquisition of a collection's lock (``_CollectionCore.rw``,
  replaced by a ``_SchedLock`` that blocks a thread by taking it out of the
  schedule, not by blocking it in the OS);
* hooks wrapped around the steps of a change: the diff being emitted, the
  head being published, the disk append, the disk snapshot, the disk load,
  the write-lock claim, compaction (on disk and in memory), and each stage
  of a transaction's commit or rollback;
* the start of every reader query body, and between its two reads.

A thread blocked on an instrumented lock leaves the schedule until the lock
is free. The other locks a pause can happen under (the diff log's lock, the
pipeline runtime's lock) are instrumented the same way without being pause
points. A thread that does not reach its next pause point within
``CASCADE_INTERLEAVE_TIMEOUT`` seconds (default 20) fails the run with the
schedule that hung it; managed threads run on daemon threads, so a hung schedule
cannot hang the suite. A deadlock among managed threads fails at once.

Exploration is a depth-first walk over the schedule tree, bounded by the
number of preemptions (switching away from a thread that could go on), as
in CHESS: every schedule with at most that many preemptions is run once.
The default suite uses bound 1 (0 for the largest scenarios); ``-m slow``
runs the same scenarios one bound deeper. Random programs under random
schedules (Hypothesis) add breadth: ``CASCADE_INTERLEAVE_EXAMPLES`` sets
their number (40 by default, 400 with ``CASCADE_INTERLEAVE_FULL=1``).
``CASCADE_INTERLEAVE_REPORT=1`` prints how many schedules each walk ran.

The oracle (``_linearizable``): every writer's change (one call, or a whole
transaction) and every query reader's result is placed in one total order
that respects each thread's program order and real time (an operation that
ended before another began comes first). Applying the changes in that order
to a plain ``list``/``set``/``dict`` must give each call's return value or
exception, each reader's result (``len`` and contents from one query, so
they must agree), the value each snapshot captured (read again after the
run: snapshots never change) and the final contents. A change refused
because another thread's transaction held the collection counts as no
change, and is only allowed while such a transaction was open. After every
run the collection's own invariants are checked (log replay equals the
storage, element ids, the published head), derived queries are compared
with plain Python and must run again exactly when the collection changed,
and a named collection is reloaded from disk and compared.
"""

from __future__ import annotations

import contextvars
import functools
import itertools
import os
import queue
import random
import shutil
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from cascade import (
    CascadeDict,
    CascadeList,
    CascadeSet,
    Engine,
    PersistentCacheError,
)
from cascade import _collections as collections_module
from cascade import _disk_cache as disk_module
from cascade import _store as store_module
from cascade import engine as engine_module

STEP_TIMEOUT = float(os.environ.get("CASCADE_INTERLEAVE_TIMEOUT", "20"))
FULL = os.environ.get("CASCADE_INTERLEAVE_FULL", "") not in ("", "0")
BACKENDS = ("mdbx", "sqlite")

# --------------------------------------------------------------------------
# The scheduler
# --------------------------------------------------------------------------


class _Abort(BaseException):
    """Ends a managed thread whose run has failed elsewhere."""


class _Rollback(Exception):
    """Raised inside a transaction block to roll it back."""


@dataclass(eq=False)
class _Managed:
    name: str
    fn: Callable[[], Any]
    ident: int = 0
    status: str = "new"  # new, ready, blocked, done
    where: str = "start"
    blocked_on: Any = None
    error: BaseException | None = None
    # Waited on for the turn; shares the scheduler's lock, so a handoff
    # wakes only the thread that was picked.
    cv: threading.Condition | None = None


# The scheduler of the run in progress; hooks are no-ops without one.
_ACTIVE: Scheduler | None = None


class _Worker:
    """A daemon thread that runs one managed thread's body per run.

    Starting threads is a good part of a run's cost, so idle workers are
    reused. A worker whose body never returns (a hung schedule) is simply
    never idle again. Each body runs in a fresh context.
    """

    idle: list[_Worker] = []
    idle_lock = threading.Lock()

    def __init__(self) -> None:
        self.jobs: queue.SimpleQueue[Callable[[], None]] = queue.SimpleQueue()
        threading.Thread(target=self._loop, name="interleave", daemon=True).start()

    def _loop(self) -> None:
        while True:
            job = self.jobs.get()
            contextvars.Context().run(job)
            with _Worker.idle_lock:
                _Worker.idle.append(self)

    @classmethod
    def submit(cls, job: Callable[[], None]) -> None:
        with cls.idle_lock:
            worker = cls.idle.pop() if cls.idle else None
        (worker or cls()).jobs.put(job)


def _point(label: str) -> None:
    sched = _ACTIVE
    if sched is not None:
        sched.point(label)


class Scheduler:
    """Runs managed threads one at a time, switching only at pause points.

    ``prefix`` gives the pick at each decision with more than one option
    (modulo the number of options); past it, ``rng`` picks, or else the
    current thread goes on (no preemption). Options are ordered with the
    current thread first, then the rest in creation order, so pick 0 is
    always "no switch".
    """

    def __init__(
        self,
        prefix: Sequence[int] = (),
        *,
        rng: random.Random | None = None,
        expect_widths: Sequence[int] = (),
    ) -> None:
        self.prefix = list(prefix)
        self.expect_widths = list(expect_widths)
        self.rng = rng
        self.lock = threading.Lock()
        # The main thread (and lock waiters outside the schedule) wait here.
        self.cond = threading.Condition(self.lock)
        self.threads: list[_Managed] = []
        self.by_ident: dict[int, _Managed] = {}
        self.turn: _Managed | None = None
        self.picks: list[int] = []
        self.widths: list[int] = []
        self.preemptive: list[bool] = []
        self.clock = 0
        self.progress = 0
        self.failure: str | None = None
        self.trace: list[str] = []

    # -- setup and the main thread's side --------------------------------

    def spawn(self, name: str, fn: Callable[[], Any]) -> _Managed:
        managed = _Managed(name, fn, cv=threading.Condition(self.lock))
        self.threads.append(managed)
        return managed

    def run(self) -> None:
        global _ACTIVE
        assert _ACTIVE is None, "schedulers do not nest"
        _ACTIVE = self
        try:
            for managed in self.threads:
                _Worker.submit(functools.partial(self._main, managed))
            with self.cond:
                deadline = time.monotonic() + STEP_TIMEOUT
                while any(t.status == "new" for t in self.threads):
                    if time.monotonic() > deadline:  # pragma: no cover
                        self._fail("threads did not start")
                        break
                    self.cond.wait(0.05)
                if self.failure is None:
                    self._decide(None)
                seen = self.progress
                deadline = time.monotonic() + STEP_TIMEOUT
                while self.failure is None and not all(
                    t.status == "done" for t in self.threads
                ):
                    self.cond.wait(0.05)
                    if self.progress != seen:
                        seen = self.progress
                        deadline = time.monotonic() + STEP_TIMEOUT
                    elif time.monotonic() > deadline:
                        holder = self.turn
                        self._fail(
                            "schedule hung: "
                            + (
                                f"{holder.name} did not reach its next pause "
                                f"point after {holder.where!r}"
                                if holder is not None
                                else "no thread holds the turn"
                            )
                            + f" within {STEP_TIMEOUT}s"
                        )
                # After a failure, give the others a moment to wind down.
                deadline = time.monotonic() + min(1.0, STEP_TIMEOUT)
                while not all(t.status == "done" for t in self.threads):
                    if time.monotonic() > deadline:
                        break
                    self.cond.wait(0.01)
        finally:
            _ACTIVE = None
        if self.failure is not None:
            raise AssertionError(self.describe(self.failure))

    def describe(self, message: str) -> str:
        return (
            f"{message}\nschedule (picks): {self.picks}\n"
            f"last steps: {' -> '.join(self.trace[-40:])}"
        )

    # -- the managed threads' side ---------------------------------------

    def tick(self) -> int:
        with self.cond:
            self.clock += 1
            return self.clock

    def current(self) -> _Managed | None:
        return self.by_ident.get(threading.get_ident())

    def _main(self, managed: _Managed) -> None:
        with self.cond:
            managed.ident = threading.get_ident()
            self.by_ident[managed.ident] = managed
            managed.status = "ready"
            self.cond.notify_all()
            try:
                self._wait_turn(managed)
            except _Abort:
                managed.status = "done"
                return
        try:
            managed.fn()
        except _Abort:
            pass
        except BaseException as exc:  # noqa: BLE001 - reported by the check
            managed.error = exc
        finally:
            with self.lock:
                managed.status = "done"
                managed.where = "done"
                if self.failure is None and self.turn is managed:
                    try:
                        self._decide(managed)
                    except _Abort:
                        pass

    def point(self, label: str) -> None:
        me = self.current()
        if me is None:
            return
        with self.cond:
            me.where = label
            self._decide(me)
            self._wait_turn(me)

    def block_on(self, me: _Managed, lock: _SchedLock) -> None:
        """Called with ``cond`` held: wait until ``lock`` is free and our turn."""
        me.status = "blocked"
        me.blocked_on = lock
        me.where = f"blocked on {lock.name}"
        self._decide(me)
        self._wait_turn(me)

    def unblock(self, lock: _SchedLock) -> None:
        for t in self.threads:
            if t.status == "blocked" and t.blocked_on is lock:
                t.status = "ready"
                t.blocked_on = None

    def _wait_turn(self, me: _Managed) -> None:
        assert me.cv is not None
        while self.turn is not me:
            if self.failure is not None:
                raise _Abort
            me.cv.wait(0.25)
        if self.failure is not None:
            raise _Abort

    def _decide(self, me: _Managed | None) -> None:
        """Hand the turn to the next thread. Called with ``cond`` held."""
        deadline = time.monotonic() + STEP_TIMEOUT
        while True:
            if self.failure is not None:
                raise _Abort
            ready = [t for t in self.threads if t.status == "ready"]
            if ready:
                if me is not None and me.status == "ready":
                    options = [me] + [t for t in ready if t is not me]
                else:
                    options = ready
                pick = 0
                if len(options) > 1:
                    i = len(self.picks)
                    if i < len(self.prefix):
                        if i < len(self.expect_widths) and (
                            self.expect_widths[i] != len(options)
                        ):
                            self._fail(
                                f"replay diverged at decision {i}: "
                                f"{len(options)} options, {self.expect_widths[i]} "
                                "last time (the run is not deterministic)"
                            )
                            raise _Abort
                        pick = self.prefix[i] % len(options)
                    elif self.rng is not None:
                        pick = self.rng.randrange(len(options))
                    self.picks.append(pick)
                    self.widths.append(len(options))
                    self.preemptive.append(me is not None and me.status == "ready")
                chosen = options[pick]
                self.turn = chosen
                self.progress += 1
                self.trace.append(f"{chosen.name}@{chosen.where}")
                assert chosen.cv is not None
                chosen.cv.notify()
                return
            if all(t.status == "done" for t in self.threads):
                self.turn = None
                self.cond.notify_all()
                return
            blocked = [t for t in self.threads if t.status == "blocked"]
            if any(t.blocked_on.owner not in self.by_ident for t in blocked):
                # Held by a thread outside the schedule: it will let go.
                if time.monotonic() > deadline:  # pragma: no cover
                    self._fail("an unmanaged thread kept a lock")
                    raise _Abort
                self.cond.wait(0.005)
                continue
            self._fail(
                "deadlock: "
                + ", ".join(f"{t.name} waits for {t.blocked_on.name}" for t in blocked)
            )
            raise _Abort

    def _fail(self, message: str) -> None:
        if self.failure is None:
            self.failure = message
        self.cond.notify_all()
        for t in self.threads:
            assert t.cv is not None
            t.cv.notify_all()


class _SchedLock:
    """A reentrant lock whose waiting is done by the scheduler.

    With ``pause`` the outermost acquisition is also a pause point. Threads
    outside the schedule (the main thread before and after a run) wait for
    it the ordinary way.
    """

    def __init__(self, sched: Scheduler, name: str, *, pause: bool) -> None:
        self.sched = sched
        self.name = name
        self.pause = pause
        self.owner: int | None = None
        self.count = 0

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        me_ident = threading.get_ident()
        if self.owner == me_ident:
            self.count += 1
            return True
        sched = self.sched
        me = sched.by_ident.get(me_ident) if _ACTIVE is sched else None
        if me is None:
            with sched.cond:
                deadline = time.monotonic() + STEP_TIMEOUT
                while self.owner is not None:
                    if not blocking or time.monotonic() > deadline:
                        return False
                    sched.cond.wait(0.005)
                self.owner = me_ident
                self.count = 1
            return True
        if self.pause:
            sched.point(f"lock {self.name}")
        with sched.cond:
            while self.owner is not None:
                if not blocking:  # pragma: no cover - not used by cascade
                    return False
                sched.block_on(me, self)
            self.owner = me_ident
            self.count = 1
        return True

    def release(self) -> None:
        assert self.owner == threading.get_ident(), f"{self.name} released by non-owner"
        self.count -= 1
        if self.count:
            return
        with self.sched.cond:
            self.owner = None
            self.sched.unblock(self)

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, *exc: Any) -> None:
        self.release()


# --------------------------------------------------------------------------
# Hooks: pause points inside the library, installed for each test
# --------------------------------------------------------------------------


def _before(label: str, fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        _point(label)
        return fn(*args, **kwargs)

    return wrapper


def _after(label: str, fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        result = fn(*args, **kwargs)
        _point(label)
        return result

    return wrapper


def _emit_hook(fn: Callable[..., Any]) -> Callable[..., Any]:
    @functools.wraps(fn)
    def emit(self: Any, diffs: list[dict[str, Any]]) -> None:
        # The storage has changed; the log has not yet.
        if diffs:
            _point(f"emit {diffs[0]['action']}")
        return fn(self, diffs)

    return emit


@pytest.fixture(autouse=True)
def hooks(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    core = collections_module._CollectionCore  # noqa: SLF001
    patch = monkeypatch.setattr
    patch(core, "emit", _emit_hook(core.emit))
    patch(core, "compact", _before("compact", core.compact))
    patch(core, "compact_memory", _before("compact memory", core.compact_memory))
    patch(core, "_stamp", _before("stamp", core._stamp))  # noqa: SLF001
    patch(core, "_claim_writer", _before("claim", core._claim_writer))  # noqa: SLF001
    patch(core, "write_pending", _before("commit write", core.write_pending))
    patch(core, "after_commit", _before("after commit", core.after_commit))
    patch(core, "release_transaction", _before("release", core.release_transaction))
    patch(core, "roll_back_to", _before("roll back", core.roll_back_to))
    patch(core, "_rollback", _before("reset storage", core._rollback))  # noqa: SLF001
    log = collections_module.DiffLog
    patch(log, "compact_to", _before("fold", log.compact_to))
    handle = engine_module._InputHandle  # noqa: SLF001
    patch(handle, "set", _before("publish", handle.set))
    tx = engine_module.EngineTransaction
    patch(tx, "__exit__", _before("tx exit", tx.__exit__))
    store = store_module.GraphStore
    patch(
        store,
        "apply_prepared_inputs",
        _before("tx publish", store.apply_prepared_inputs),
    )
    for backend in (disk_module.MdbxDiskCache, disk_module.SqliteDiskCache):
        patch(
            backend,
            "collection_append_many",
            _before("disk append", backend.collection_append_many),
        )
        patch(
            backend,
            "collection_snapshot",
            _before("disk snapshot", backend.collection_snapshot),
        )
        patch(
            backend, "collection_load", _after("disk loaded", backend.collection_load)
        )
        if hasattr(backend, "collection_stamp"):
            patch(
                backend,
                "collection_stamp",
                _before("disk stamp", backend.collection_stamp),
            )
        patch(backend, "collection_head", _after("disk head", backend.collection_head))
    yield


def _instrument(sched: Scheduler, engine: Engine, coll: Any) -> None:
    """Put the collection's locks (and the pipeline runtime's) under ``sched``."""
    core = coll._core  # noqa: SLF001
    tag = f"{core.name}@{id(engine) % 1000}"
    core.rw = _SchedLock(sched, f"rw {tag}", pause=True)
    core.log.lock = _SchedLock(sched, f"log {tag}", pause=False)
    rt = engine._incremental_rt  # noqa: SLF001
    if not isinstance(rt._lock, _SchedLock):  # noqa: SLF001
        rt._lock = _SchedLock(sched, "pipelines", pause=False)  # noqa: SLF001


def _uninstrument(engine: Engine, coll: Any) -> None:
    """Plain locks again, for use after the run (reload, fresh queries)."""
    core = coll._core  # noqa: SLF001
    core.rw = threading.RLock()
    core.log.lock = threading.RLock()
    engine._incremental_rt._lock = threading.RLock()  # noqa: SLF001


# --------------------------------------------------------------------------
# Exploration
# --------------------------------------------------------------------------


@dataclass
class Explored:
    runs: int = 0
    complete: bool = False
    longest: int = 0


def _next_prefix(
    picks: list[int], widths: list[int], preemptive: list[bool], bound: int | None
) -> list[int] | None:
    # used[i]: preemptions taken (pick != 0 at a preemptive decision)
    # among the decisions before i.
    used = [0]
    for i in range(len(picks) - 1):
        used.append(used[-1] + (1 if preemptive[i] and picks[i] != 0 else 0))
    for i in reversed(range(len(picks))):
        if picks[i] + 1 >= widths[i]:
            continue
        cost = used[i] + (1 if preemptive[i] else 0)
        if bound is None or cost <= bound:
            return picks[:i] + [picks[i] + 1]
    return None


def explore(
    run_one: Callable[[Scheduler], None],
    *,
    bound: int | None,
    max_runs: int = 100_000,
) -> Explored:
    """Every schedule with at most ``bound`` preemptions (all with None)."""
    stats = Explored()
    prefix: list[int] = []
    widths: list[int] = []
    while stats.runs < max_runs:
        sched = Scheduler(prefix, expect_widths=widths)
        stats.runs += 1
        try:
            run_one(sched)
        except AssertionError as exc:
            raise AssertionError(
                f"schedule {stats.runs} of the walk (bound {bound}): {exc}"
            ) from exc
        stats.longest = max(stats.longest, len(sched.picks))
        nxt = _next_prefix(sched.picks, sched.widths, sched.preemptive, bound)
        if nxt is None:
            stats.complete = True
            break
        prefix = nxt
        widths = sched.widths[: len(nxt)]
    return stats


# --------------------------------------------------------------------------
# Operations and the model
# --------------------------------------------------------------------------


Action = Callable[[Any], Any]


def _outcome(action: Action, target: Any) -> tuple[str, Any]:
    try:
        result = action(target)
    except (IndexError, ValueError, KeyError) as exc:
        return ("err", type(exc).__name__)
    if result is target:
        result = None  # in-place operators return the collection itself
    return ("ok", result)


def _is_refusal(exc: BaseException) -> bool:
    if isinstance(exc, PersistentCacheError):
        return True
    return isinstance(exc, RuntimeError) and "uncommitted changes" in str(exc)


@dataclass(frozen=True)
class Step:
    """One atomic unit a thread performs, as the oracle sees it."""

    kind: str  # write, tx, view, total, mark, snapview, compact
    label: str
    actions: tuple[Action, ...] = ()
    commit: bool = True
    snap: str = ""


def op(label: str, action: Action) -> Step:
    return Step("write", label, (action,))


def tx(label: str, *actions: Action, commit: bool = True) -> Step:
    return Step("tx", label, tuple(actions), commit=commit)


VIEW = Step("view", "view")
TOTAL = Step("total", "sum")
COMPACT = Step("compact", "compact")


def mark(name: str) -> Step:
    return Step("mark", f"snapshot {name}", snap=name)


def snapview(name: str) -> Step:
    return Step("snapview", f"view {name}", snap=name)


@dataclass(eq=False)
class Event:
    thread: str
    step: Step
    start: int
    end: int = 0
    observed: Any = None
    refused: bool = False
    # A transaction's calls: (start, end) of each, in order.
    times: list[tuple[int, int]] = field(default_factory=list)


class Model:
    """Plain builtin standing in for the collection."""

    def __init__(self, kind: str) -> None:
        self.kind = kind

    def thaw(self, state: Any) -> Any:
        if self.kind == "list":
            return list(state)
        if self.kind == "set":
            return set(state)
        return dict(state)

    def freeze(self, value: Any) -> Any:
        if self.kind == "list":
            return tuple(value)
        if self.kind == "set":
            return tuple(sorted(value))
        return tuple(sorted(value.items()))

    def view(self, state: Any) -> Any:
        """What a reader query returns for contents ``state``."""
        return (len(state), state)


@dataclass(frozen=True, eq=False)
class _Atom:
    """One step of the search: a call, a read, or the end of a transaction."""

    kind: str  # write, refused, txop, txend, view, total, mark, snapview, none
    event: Event
    start: int
    end: int
    action: Action | None = None
    observed: Any = None


def _atoms(e: Event) -> list[_Atom]:
    kind = e.step.kind
    if kind == "write":
        if e.refused:
            return [_Atom("refused", e, e.start, e.end)]
        return [_Atom("write", e, e.start, e.end, e.step.actions[0], e.observed[0])]
    if kind == "tx":
        if e.refused:
            return [_Atom("refused", e, e.start, e.end)]
        # The calls in a transaction see each other's changes at once; other
        # threads see them at its end, all together. Until its first change
        # it reserves nothing, so others may change the collection between
        # its calls; from then on they are refused (G25).
        atoms = [
            _Atom("txop", e, began, ended, action, got)
            for (began, ended), action, got in zip(
                e.times, e.step.actions, e.observed, strict=True
            )
        ]
        last = e.times[-1][1] if e.times else e.start
        atoms.append(_Atom("txend", e, last, e.end))
        return atoms
    if kind == "compact":
        return [_Atom("none", e, e.start, e.end)]
    return [_Atom(kind, e, e.start, e.end, observed=e.observed)]


def _linearizable(
    model: Model, init: Any, final: Any, threads: list[list[Event]]
) -> str | None:
    """None if some total order explains every event; else why not."""
    tx_events = [
        e for evs in threads for e in evs if e.step.kind == "tx" and not e.refused
    ]
    for evs in threads:
        for e in evs:
            if e.refused and not any(
                t.thread != e.thread and t.start < e.end and e.start < t.end
                for t in tx_events
            ):
                return f"{e.thread} {e.step.label} was refused with no transaction open"

    atoms = [[atom for e in evs for atom in _atoms(e)] for evs in threads]
    counts = [len(a) for a in atoms]
    seen: set[Any] = set()
    # Per thread: the working contents of its open transaction once that
    # has changed something (it then holds the collection), else None.
    no_tx: tuple[Any, ...] = (None,) * len(atoms)

    def place(
        t: int, a: _Atom, state: Any, txs: tuple[Any, ...], marks: dict[str, Any]
    ) -> tuple[bool, Any, tuple[Any, ...]]:
        kind = a.kind
        holder = next((i for i, x in enumerate(txs) if x is not None and i != t), None)
        if kind == "write":
            if holder is not None:
                return False, state, txs  # it would have been refused
            work = model.thaw(state)
            if _outcome(a.action, work) != a.observed:
                return False, state, txs
            return True, model.freeze(work), txs
        if kind == "txop":
            # Until it changes something, it works on the committed contents.
            working = txs[t] if txs[t] is not None else state
            work = model.thaw(working)
            if _outcome(a.action, work) != a.observed:
                return False, state, txs
            after = model.freeze(work)
            if txs[t] is None and after == working:
                return True, state, txs
            if holder is not None:
                return False, state, txs  # two transactions cannot both hold it
            return True, state, (*txs[:t], after, *txs[t + 1 :])
        if kind == "txend":
            mine = txs[t]
            cleared = (*txs[:t], None, *txs[t + 1 :])
            if a.event.step.commit and mine is not None:
                return True, mine, cleared
            return True, state, cleared
        if kind in ("refused", "none"):
            return True, state, txs
        if kind == "view":
            return a.observed == model.view(state), state, txs
        if kind == "total":
            return a.observed == sum(model.thaw(state)), state, txs
        if kind == "mark":
            marks[a.event.step.snap] = state
            return True, state, txs
        if kind == "snapview":
            return a.observed == model.view(marks[a.event.step.snap]), state, txs
        raise AssertionError(kind)  # pragma: no cover

    def dfs(
        pos: tuple[int, ...], state: Any, txs: tuple[Any, ...], marks: tuple[Any, ...]
    ) -> bool:
        key = (pos, state, txs, marks)
        if key in seen:
            return False
        seen.add(key)
        heads = [atoms[t][pos[t]] for t in range(len(atoms)) if pos[t] < counts[t]]
        if not heads:
            return state == final
        min_end = min(a.end for a in heads)
        for t in range(len(atoms)):
            if pos[t] >= counts[t]:
                continue
            a = atoms[t][pos[t]]
            if a.start > min_end:
                continue  # a step of another thread ended before this one began
            marks_d = dict(marks)
            ok, new_state, new_txs = place(t, a, state, txs, marks_d)
            if not ok:
                continue
            new_pos = (*pos[:t], pos[t] + 1, *pos[t + 1 :])
            if dfs(new_pos, new_state, new_txs, tuple(sorted(marks_d.items()))):
                return True
        return False

    if dfs(tuple(0 for _ in atoms), init, no_tx, ()):
        return None
    lines = ["no serial order explains this run:"]
    for evs in threads:
        for e in evs:
            what = "refused" if e.refused else e.observed
            lines.append(f"  {e.thread} [{e.start}, {e.end}] {e.step.label}: {what}")
    lines.append(f"  final: {final}")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# One engine, one shared collection
# --------------------------------------------------------------------------

_KINDS = {"list": CascadeList, "set": CascadeSet, "dict": CascadeDict}


@dataclass
class Scenario:
    """Threads changing and reading one collection in one engine."""

    kind: str = "list"
    initial: Any = ()
    threads: list[list[Step]] = field(default_factory=list)
    backend: str | None = None
    compact_every: int = 1024
    memory_threshold: int | None = None  # fold old diffs in memory this early


class _Run:
    def __init__(self, sc: Scenario, sched: Scheduler, cache_dir: str | None) -> None:
        self.sc = sc
        self.sched = sched
        self.model = Model(sc.kind)
        self.cache_dir = cache_dir
        if cache_dir is None:
            self.engine = Engine()
            self.coll = _KINDS[sc.kind](self.engine, sc.initial)
        else:
            self.engine = Engine(cache_dir=cache_dir, cache_backend=sc.backend)
            self.name = f"c{next(_names)}"
            self.coll = _KINDS[sc.kind](
                self.engine, name=self.name, compact_every=sc.compact_every
            )
            if sc.initial:
                if sc.kind == "list":
                    self.coll.extend(sc.initial)
                else:
                    self.coll.update(sc.initial)
        self.init = self.model.freeze(self.model.thaw(sc.initial))
        self.events: list[list[Event]] = [[] for _ in sc.threads]
        self.snaps: dict[str, Any] = {}
        self.runs: dict[int, list[int]] = {}  # thread -> heads its view body saw
        self.views: dict[int, Any] = {}
        self.totals: dict[int, Any] = {}
        for t, steps in enumerate(sc.threads):
            self.runs[t] = []
            self.views[t] = self._make_view(t)
            # Rewriting a query costs a parse: only for threads that sum.
            if any(step.kind == "total" for step in steps):
                self.totals[t] = self._make_total(t)

    def _make_view(self, t: int) -> Any:
        coll = self.coll
        heads = self.runs[t]
        kind = self.sc.kind

        def view() -> Any:
            _point("view body")
            heads.append(coll._core.head_for_current_context())  # noqa: SLF001
            n = len(coll)
            _point("between reads")
            if kind == "list":
                contents = tuple(coll)
            elif kind == "set":
                contents = tuple(sorted(coll))
            else:
                contents = tuple(sorted(coll.items()))
            return n, contents

        # Plain reads: each one takes the collection's lock on its own.
        return self.engine.query(view, name=f"view{t}", incremental_per_element=False)

    def _make_total(self, t: int) -> Any:
        coll = self.coll

        def total() -> int:
            _point("total body")
            return sum(v for v in coll)  # rewritten into a pipeline

        return self.engine.query(total, name=f"total{t}")

    # -- inside the managed threads --------------------------------------

    def thread_body(self, t: int) -> Callable[[], None]:
        steps = self.sc.threads[t]
        name = f"T{t}"

        def body() -> None:
            for step in steps:
                ev = Event(name, step, self.sched.tick())
                self._do(t, step, ev)
                ev.end = self.sched.tick()
                self.events[t].append(ev)

        return body

    def _do(self, t: int, step: Step, ev: Event) -> None:
        coll = self.coll
        kind = step.kind
        if kind == "write":
            try:
                ev.observed = [_outcome(step.actions[0], coll)]
            except BaseException as exc:
                if not _is_refusal(exc):
                    raise
                ev.refused = True
        elif kind == "tx":
            got: list[Any] = []
            try:
                with self.engine.transaction():
                    for action in step.actions:
                        began = self.sched.tick()
                        got.append(_outcome(action, coll))
                        ev.times.append((began, self.sched.tick()))
                    if not step.commit:
                        raise _Rollback
            except _Rollback:
                pass
            except BaseException as exc:
                if not _is_refusal(exc):
                    raise
                ev.refused = True
            ev.observed = got
        elif kind == "view":
            ev.observed = self.views[t]()
        elif kind == "total":
            ev.observed = self.totals[t]()
        elif kind == "mark":
            self.snaps[step.snap] = self.engine.snapshot()
        elif kind == "snapview":
            ev.observed = self.views[t](snapshot=self.snaps[step.snap])
        elif kind == "compact":
            coll.compact()
        else:  # pragma: no cover
            raise AssertionError(kind)

    # -- after the run ---------------------------------------------------

    def check(self) -> None:
        for managed in self.sched.threads:
            if managed.error is not None:
                raise AssertionError(
                    self.sched.describe(
                        f"{managed.name} raised {managed.error!r} at {managed.where}"
                    )
                ) from managed.error
        _uninstrument(self.engine, self.coll)
        coll = self.coll
        core = coll._core  # noqa: SLF001
        final = self.model.freeze(coll.copy())
        # Snapshots never change: each is read once more, after everything.
        late: list[Event] = []
        clock = self.sched.clock
        for name, snap in self.snaps.items():
            clock += 1
            ev = Event("main", snapview(name), clock)
            ev.observed = self.views[0](snapshot=snap)
            clock += 1
            ev.end = clock
            late.append(ev)
        threads = [*self.events, late]
        why = _linearizable(self.model, self.init, final, threads)
        if why is not None:
            raise AssertionError(self.sched.describe(why))
        self._check_core(core, final)
        self._check_queries(final)
        for snap in self.snaps.values():
            snap.release()
        self.engine.shutdown()
        if self.cache_dir is not None:
            self._check_reload(final)

    def _check_core(self, core: Any, final: Any) -> None:
        log = core.log
        replay = log.contents_at(log.head)
        if self.sc.kind == "list":
            assert [v for _, v in replay] == list(final), "log replay differs"
            assert [u for u, _ in replay] == self.coll._uids, "element ids differ"  # noqa: SLF001
            assert len(set(self.coll._uids)) == len(self.coll._uids)  # noqa: SLF001
        else:
            assert self.model.freeze(self.model.thaw(replay)) == final
        assert core.tracked_len == len(final)
        assert core.handle() == log.head, "published head is not the log's head"
        assert core._tx is None and core._tx_start is None  # noqa: SLF001

    def _check_queries(self, final: Any) -> None:
        expected = self.model.view(final)
        for t, view in self.views.items():
            if any(s.kind == "snapview" for s in self.sc.threads[t]):
                assert view() == expected
                continue
            heads = self.runs[t]
            before = len(heads)
            assert view() == expected, f"view{t} differs from the collection"
            ran = len(heads) - before
            changed = before == 0 or heads[before - 1] != self.coll._core.handle()  # noqa: SLF001
            assert ran == int(changed), (
                f"view{t} ran {ran} times; the collection "
                f"{'changed' if changed else 'did not change'} since its last run"
            )
            view()
            assert len(heads) == before + ran, f"view{t} ran again with nothing changed"
        if not self.totals:
            return
        assert self.engine.inspect_pipelines(), "sum(...) was not rewritten"
        # A fresh engine with the final contents computes the same.
        fresh = Engine()
        twin = _KINDS[self.sc.kind](fresh, self.model.thaw(final))

        def total() -> int:
            return sum(v for v in twin)

        fresh_total = fresh.query(total, name="total")()
        for t, mine in self.totals.items():
            assert mine() == fresh_total == sum(self.model.thaw(final)), (
                f"total{t} differs"
            )
        fresh.shutdown()

    def _check_reload(self, final: Any) -> None:
        engine = Engine(cache_dir=self.cache_dir, cache_backend=self.sc.backend)
        try:
            again = _KINDS[self.sc.kind](engine, name=self.name)
            assert self.model.freeze(again.copy()) == final, self.sched.describe(
                f"reloaded {self.model.freeze(again.copy())}, expected {final}"
            )
        finally:
            engine.shutdown()


_names = itertools.count()


class Runner:
    """Builds and checks one scenario per schedule; owns the cache directory."""

    def __init__(self, sc: Scenario, monkeypatch: pytest.MonkeyPatch) -> None:
        self.sc = sc
        self.cache_dir = tempfile.mkdtemp() if sc.backend is not None else None
        if sc.memory_threshold is not None:
            monkeypatch.setattr(
                collections_module, "_MEMORY_COMPACT_THRESHOLD", sc.memory_threshold
            )

    def __call__(self, sched: Scheduler) -> None:
        run = _Run(self.sc, sched, self.cache_dir)
        try:
            _instrument(sched, run.engine, run.coll)
            for t in range(len(self.sc.threads)):
                sched.spawn(f"T{t}", run.thread_body(t))
            sched.run()
            run.check()
        except BaseException:
            _uninstrument(run.engine, run.coll)
            try:
                run.engine.shutdown(wait=False)
            except Exception:  # noqa: BLE001, S110 - the run already failed
                pass
            raise

    def close(self) -> None:
        if self.cache_dir is not None:
            shutil.rmtree(self.cache_dir, ignore_errors=True)


@pytest.fixture
def runner(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[[Scenario], Runner]]:
    made: list[Runner] = []

    def make(sc: Scenario) -> Runner:
        r = Runner(sc, monkeypatch)
        made.append(r)
        return r

    yield make
    for r in made:
        r.close()


# --------------------------------------------------------------------------
# Several engines, one cache directory, one named collection
# --------------------------------------------------------------------------


@dataclass
class Shared:
    """Each thread has an engine of its own on one ``cache_dir``.

    Steps: ``CREATE`` (the collection object; ``CREATE_IN_TX`` makes it in
    a transaction that changes nothing), then ``op``/``tx``/``COMPACT``,
    and ``SHUTDOWN``.
    """

    backend: str
    threads: list[list[Step]] = field(default_factory=list)
    compact_every: int = 1024


CREATE = Step("create", "create")
CREATE_IN_TX = Step("create_tx", "create in a transaction")
SHUTDOWN = Step("shutdown", "shutdown")


class _SharedRun:
    def __init__(self, sc: Shared, sched: Scheduler, cache_dir: str, name: str) -> None:
        self.sc = sc
        self.sched = sched
        self.cache_dir = cache_dir
        self.name = name
        self.engines = [
            Engine(cache_dir=cache_dir, cache_backend=sc.backend) for _ in sc.threads
        ]
        self.colls: list[Any] = [None] * len(sc.threads)
        self.loaded: list[Any] = [()] * len(sc.threads)
        # (thread, step, outcomes) of every change that went through, in order
        self.changes: list[tuple[int, Step, Any]] = []
        self.refusals: list[tuple[int, str]] = []

    def _create(self, t: int, in_tx: bool) -> Any:
        engine = self.engines[t]
        if not in_tx:
            return CascadeList(
                engine, name=self.name, compact_every=self.sc.compact_every
            )
        with engine.transaction():
            return CascadeList(
                engine, name=self.name, compact_every=self.sc.compact_every
            )

    def thread_body(self, t: int) -> Callable[[], None]:
        engine = self.engines[t]

        def body() -> None:
            for step in self.sc.threads[t]:
                if step.kind in ("create", "create_tx"):
                    coll = self._create(t, step.kind == "create_tx")
                    _instrument(self.sched, engine, coll)
                    self.colls[t] = coll
                    self.loaded[t] = tuple(coll.copy())
                elif step.kind == "shutdown":
                    engine.shutdown()
                else:
                    self._change(t, step)

        return body

    def _change(self, t: int, step: Step) -> None:
        coll = self.colls[t]
        got: list[Any] = []
        try:
            if step.kind == "compact":
                coll.compact()
                return
            if step.kind == "write":
                got.append(_outcome(step.actions[0], coll))
            else:
                with self.engines[t].transaction():
                    for action in step.actions:
                        got.append(_outcome(action, coll))
                    if not step.commit:
                        raise _Rollback
        except _Rollback:
            return
        except PersistentCacheError as exc:
            self.refusals.append((t, str(exc)))
            return
        self.changes.append((t, step, got))

    def check(self) -> None:
        for managed in self.sched.threads:
            if managed.error is not None:
                raise AssertionError(
                    self.sched.describe(
                        f"{managed.name} raised {managed.error!r} at {managed.where}"
                    )
                ) from managed.error
        for engine, coll in zip(self.engines, self.colls, strict=True):
            if coll is not None:
                _uninstrument(engine, coll)
        model = Model("list")
        describe = self.sched.describe
        # Each engine's copy is what it loaded plus its own changes.
        finals: dict[int, Any] = {}
        for t, coll in enumerate(self.colls):
            if coll is None:
                continue
            state = self.loaded[t]
            for who, step, got in self.changes:
                if who != t:
                    continue
                work = model.thaw(state)
                expect = [_outcome(a, work) for a in step.actions]
                assert expect == got, describe(
                    f"T{t} {step.label} returned {got}, expected {expect}"
                )
                state = model.freeze(work)
            finals[t] = state
            assert tuple(coll.copy()) == state, describe(
                f"T{t} holds {tuple(coll.copy())}, expected {state}"
            )
        # The engines that changed the collection, in the order they did.
        writers: list[int] = []
        for who, _step, _got in self.changes:
            if not writers or writers[-1] != who:
                assert who not in writers, describe(
                    f"T{who} changed the collection again after another engine did"
                )
                writers.append(who)
        if writers:
            assert self.loaded[writers[0]] == (), describe(
                f"the first writer T{writers[0]} loaded {self.loaded[writers[0]]}"
            )
        for before, after in itertools.pairwise(writers):
            # No lost update: each writer started from the last one's contents.
            assert self.loaded[after] == finals[before], describe(
                f"T{after} changed a copy {self.loaded[after]} that is not "
                f"T{before}'s final {finals[before]}"
            )
        for engine in self.engines:
            engine.shutdown()
        expected = finals[writers[-1]] if writers else ()
        engine = Engine(cache_dir=self.cache_dir, cache_backend=self.sc.backend)
        try:
            again = tuple(CascadeList(engine, name=self.name))
        finally:
            engine.shutdown()
        assert again == expected, describe(
            f"reloaded {again}, expected {expected} (writers {writers}, "
            f"refused {self.refusals})"
        )


class SharedRunner:
    def __init__(self, sc: Shared) -> None:
        self.sc = sc
        self.cache_dir = tempfile.mkdtemp()

    def __call__(self, sched: Scheduler) -> None:
        run = _SharedRun(self.sc, sched, self.cache_dir, f"s{next(_names)}")
        try:
            for t in range(len(self.sc.threads)):
                sched.spawn(f"T{t}", run.thread_body(t))
            sched.run()
            run.check()
        except BaseException:
            for engine, coll in zip(run.engines, run.colls, strict=True):
                if coll is not None:
                    _uninstrument(engine, coll)
                try:
                    engine.shutdown(wait=False)
                except Exception:  # noqa: BLE001, S110 - the run already failed
                    pass
            raise

    def close(self) -> None:
        shutil.rmtree(self.cache_dir, ignore_errors=True)


@pytest.fixture
def shared_runner() -> Iterator[Callable[[Shared], SharedRunner]]:
    made: list[SharedRunner] = []

    def make(sc: Shared) -> SharedRunner:
        r = SharedRunner(sc)
        made.append(r)
        return r

    yield make
    for r in made:
        r.close()


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------


def _report(capsys: Any, name: str, stats: Explored) -> None:
    with capsys.disabled():
        if os.environ.get("CASCADE_INTERLEAVE_REPORT"):
            print(f"\n{name}: {stats}")


def _set(i: int, v: Any) -> Action:
    def action(xs: Any) -> None:
        xs[i] = v

    return action


def _del(i: Any) -> Action:
    def action(xs: Any) -> None:
        del xs[i]

    return action


def _append(v: Any) -> Step:
    return op(f"append {v}", lambda xs: xs.append(v))


def _ext(*vs: Any) -> Step:
    return op("extend " + " ".join(map(str, vs)), lambda xs: xs.extend(vs))


def _pop(*i: int) -> Step:
    return op(" ".join(["pop", *map(str, i)]), lambda xs: xs.pop(*i))


# name -> (scenario, preemption bound by default, bound under -m slow).
# Each scenario puts several writers on one collection; most add a reader.
SCENARIOS: dict[str, tuple[Callable[[], Scenario], int, int]] = {
    "list writers and a reader": (
        lambda: Scenario(
            initial=(7,),
            threads=[
                [_append(1)],
                [op("insert 0 2", lambda xs: xs.insert(0, 2))],
                [_pop()],
                [VIEW, TOTAL],
            ],
        ),
        1,
        2,
    ),
    "list writers, two changes each": (
        lambda: Scenario(
            initial=(7,),
            threads=[
                [_append(1), _pop()],
                [
                    op("insert 0 2", lambda xs: xs.insert(0, 2)),
                    op("xs[1] = 5", _set(1, 5)),
                ],
                [_ext(3, 4), op("del xs[0]", _del(0))],
                [VIEW, TOTAL],
            ],
        ),
        0,
        1,
    ),
    "transactions and a plain writer": (
        lambda: Scenario(
            initial=(7,),
            threads=[
                [
                    tx(
                        "tx append 1, pop 0",
                        lambda xs: xs.append(1),
                        lambda xs: xs.pop(0),
                    )
                ],
                [tx("tx append 2, rolled back", lambda xs: xs.append(2), commit=False)],
                [_append(3)],
                [VIEW],
            ],
        ),
        1,
        2,
    ),
    # A transaction holds the collection from its first change only: a
    # writer may come in after a call that changed nothing.
    "transaction whose first call changes nothing": (
        lambda: Scenario(
            threads=[
                [tx("tx pop, append 2", lambda xs: xs.pop(), lambda xs: xs.append(2))],
                [_append(1)],
                [VIEW],
            ],
        ),
        1,
        2,
    ),
    "snapshot reader while old diffs are folded": (
        lambda: Scenario(
            initial=(7,),
            memory_threshold=2,
            threads=[
                [_append(1), _append(2), _append(3)],
                [mark("s"), VIEW, snapview("s")],
                [tx("tx extend 4 5", lambda xs: xs.extend([4, 5]))],
            ],
        ),
        1,
        2,
    ),
    "set writers and a reader": (
        lambda: Scenario(
            kind="set",
            initial=(1,),
            threads=[
                [
                    op("add 2", lambda s: s.add(2)),
                    op("discard 1", lambda s: s.discard(1)),
                ],
                [
                    tx(
                        "tx update 1 3, remove 2",
                        lambda s: s.update({1, 3}),
                        lambda s: s.remove(2),
                    )
                ],
                [VIEW],
            ],
        ),
        1,
        2,
    ),
    "dict writers and a reader": (
        lambda: Scenario(
            kind="dict",
            initial={1: "a"},
            threads=[
                [op("d[2] = b", _set(2, "b")), op("pop 1", lambda d: d.pop(1))],
                [tx("tx d[1] = c, del d[2]", _set(1, "c"), _del(2))],
                [VIEW],
            ],
        ),
        1,
        2,
    ),
}

# Named collections: every change is written to disk before it is applied,
# and compaction (here after every two changes) rewrites the log.
DISK_SCENARIOS: dict[str, tuple[Callable[[str], Scenario], int, int]] = {
    "commit, a writer and compaction": (
        lambda backend: Scenario(
            initial=(7,),
            backend=backend,
            compact_every=2,
            threads=[
                [
                    tx(
                        "tx append 1 2",
                        lambda xs: xs.append(1),
                        lambda xs: xs.append(2),
                    )
                ],
                [_append(3), COMPACT],
            ],
        ),
        1,
        2,
    ),
    "rollback, writers and compaction": (
        lambda backend: Scenario(
            initial=(7,),
            backend=backend,
            compact_every=2,
            threads=[
                [tx("tx append 1, rolled back", lambda xs: xs.append(1), commit=False)],
                [_append(2), _pop(0)],
                [COMPACT, VIEW],
            ],
        ),
        0,
        1,
    ),
}


def _explore_scenario(
    runner: Any, capsys: Any, label: str, sc: Scenario, bound: int
) -> None:
    stats = explore(runner(sc), bound=bound)
    _report(capsys, f"{label} (bound {bound})", stats)
    assert stats.complete


@pytest.mark.parametrize("name", list(SCENARIOS))
def test_interleavings(runner: Any, capsys: Any, name: str) -> None:
    make, bound, _deeper = SCENARIOS[name]
    _explore_scenario(runner, capsys, name, make(), bound)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("name", list(DISK_SCENARIOS))
def test_interleavings_on_disk(
    runner: Any, capsys: Any, name: str, backend: str
) -> None:
    make, bound, _deeper = DISK_SCENARIOS[name]
    _explore_scenario(runner, capsys, f"{name} {backend}", make(backend), bound)


@pytest.mark.slow
@pytest.mark.parametrize("name", list(SCENARIOS))
def test_interleavings_deeper(runner: Any, capsys: Any, name: str) -> None:
    make, _default, bound = SCENARIOS[name]
    _explore_scenario(runner, capsys, name, make(), bound)


@pytest.mark.slow
@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("name", list(DISK_SCENARIOS))
def test_interleavings_on_disk_deeper(
    runner: Any, capsys: Any, name: str, backend: str
) -> None:
    make, _default, bound = DISK_SCENARIOS[name]
    _explore_scenario(runner, capsys, f"{name} {backend}", make(backend), bound)


# -- several engines on one cache directory ---------------------------------


SHARED = {
    # D41: the record a transaction held back is written at commit; another
    # engine may have recorded and compacted the name since.
    "held-back record": [
        [CREATE_IN_TX],
        [CREATE, _ext(1, 2), COMPACT],
    ],
    # The record a new collection stamps at construction, the same way.
    "construction": [
        [CREATE],
        [CREATE, _ext(1, 2), COMPACT],
    ],
    # The write lock, an out-of-date copy, and a lock released by shutdown.
    "two writers": [
        [CREATE, _append(1), SHUTDOWN],
        [CREATE, _append(2), _append(3)],
    ],
    "transaction against a writer": [
        [
            CREATE,
            tx("tx append 1 2", lambda xs: xs.append(1), lambda xs: xs.append(2)),
        ],
        [CREATE, _append(3), COMPACT],
    ],
}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("case", list(SHARED))
def test_engines_sharing_a_named_collection(
    shared_runner: Any, capsys: Any, backend: str, case: str
) -> None:
    sc = Shared(backend=backend, threads=SHARED[case], compact_every=2)
    stats = explore(shared_runner(sc), bound=1)
    _report(capsys, f"shared {case} {backend} (bound 1)", stats)
    assert stats.complete


@pytest.mark.slow
@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("case", list(SHARED))
def test_engines_sharing_a_named_collection_deeper(
    shared_runner: Any, capsys: Any, backend: str, case: str
) -> None:
    sc = Shared(backend=backend, threads=SHARED[case], compact_every=2)
    stats = explore(shared_runner(sc), bound=2)
    _report(capsys, f"shared {case} {backend} (bound 2)", stats)
    assert stats.complete


# -- random programs under random schedules ---------------------------------

_LIST_ACTIONS: list[tuple[str, Action]] = [
    ("append 1", lambda xs: xs.append(1)),
    ("append 2", lambda xs: xs.append(2)),
    ("extend 3 4", lambda xs: xs.extend([3, 4])),
    ("insert 0 5", lambda xs: xs.insert(0, 5)),
    ("insert 1 6", lambda xs: xs.insert(1, 6)),
    ("pop", lambda xs: xs.pop()),
    ("pop 0", lambda xs: xs.pop(0)),
    ("remove 1", lambda xs: xs.remove(1)),
    ("xs[0] = 8", _set(0, 8)),
    ("del xs[-1]", _del(-1)),
    ("del xs[:1]", _del(slice(None, 1))),
    ("clear", lambda xs: xs.clear()),
    ("sort", lambda xs: xs.sort()),
    ("reverse", lambda xs: xs.reverse()),
    ("xs += [9]", lambda xs: xs.__iadd__([9])),
]


@st.composite
def _writer_step(draw: Any, disk: bool) -> Step:
    kinds = ["op", "op", "tx", "rollback"] + (["compact"] if disk else [])
    kind = draw(st.sampled_from(kinds))
    if kind == "compact":
        return COMPACT
    if kind == "op":
        label, action = draw(st.sampled_from(_LIST_ACTIONS))
        return op(label, action)
    chosen = draw(st.lists(st.sampled_from(_LIST_ACTIONS), min_size=1, max_size=2))
    labels = ", ".join(label for label, _ in chosen)
    commit = kind == "tx"
    return tx(
        f"tx {labels}" + ("" if commit else ", rolled back"),
        *(a for _, a in chosen),
        commit=commit,
    )


@st.composite
def _programs(draw: Any, disk: bool) -> Scenario:
    writers = draw(st.integers(2, 3))
    threads = [
        draw(st.lists(_writer_step(disk), min_size=1, max_size=2))
        for _ in range(writers)
    ]
    reader = draw(
        st.sampled_from(
            [[VIEW], [VIEW, TOTAL], [mark("s"), VIEW, snapview("s")], [TOTAL, VIEW]]
        )
    )
    threads.append(reader)
    return Scenario(
        initial=tuple(draw(st.lists(st.integers(0, 3), max_size=3))),
        threads=threads,
        memory_threshold=draw(st.sampled_from([None, 2])),
        compact_every=2,
    )


_RANDOM_EXAMPLES = int(
    os.environ.get("CASCADE_INTERLEAVE_EXAMPLES", "400" if FULL else "40")
)


def _random_settings(examples: int) -> Any:
    return settings(
        max_examples=examples,
        deadline=None,
        suppress_health_check=[
            HealthCheck.function_scoped_fixture,
            HealthCheck.too_slow,
        ],
    )


def _run_random(sc: Scenario, picks: list[int]) -> None:
    with pytest.MonkeyPatch.context() as mp:
        run = Runner(sc, mp)
        try:
            run(Scheduler(picks))
        finally:
            run.close()


@_random_settings(_RANDOM_EXAMPLES)
@given(sc=_programs(disk=False), picks=st.lists(st.integers(0, 3), max_size=60))
def test_random_programs_and_schedules(sc: Scenario, picks: list[int]) -> None:
    _run_random(sc, picks)


@pytest.mark.slow
@pytest.mark.parametrize("backend", BACKENDS)
@_random_settings(max(_RANDOM_EXAMPLES // 2, 20))
@given(data=st.data())
def test_random_programs_and_schedules_on_disk(backend: str, data: Any) -> None:
    sc = data.draw(_programs(disk=True))
    sc.backend = backend
    _run_random(sc, data.draw(st.lists(st.integers(0, 3), max_size=60)))


# --------------------------------------------------------------------------
# Defects this harness found, each pinned by a plain test as well
# --------------------------------------------------------------------------


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_compact_inside_a_transaction_writes_only_committed_contents(
    tmp_path: Any, backend: str, commit: bool
) -> None:
    # compact() wrote the log up to its head, which held the open
    # transaction's changes. Committed, the batch was then appended a second
    # time ([1, 2, 2] on reload); rolled back, it stayed on disk ([1, 2]).
    with Engine(cache_dir=tmp_path, cache_backend=backend) as engine:
        xs = CascadeList(engine, name="xs")
        xs.append(1)
        try:
            with engine.transaction():
                xs.append(2)
                xs.compact()
                if not commit:
                    raise _Rollback
        except _Rollback:
            pass
        expected = [1, 2] if commit else [1]
        assert list(xs) == expected
    with Engine(cache_dir=tmp_path, cache_backend=backend) as reopened:
        assert list(CascadeList(reopened, name="xs")) == expected


@pytest.mark.parametrize("backend", BACKENDS)
def test_compact_from_another_thread_waits_for_a_mutation(
    tmp_path: Any, backend: str
) -> None:
    # compact() ran outside the collection's lock. The compaction a commit
    # runs read the head, a writer appended and compacted at its newer head,
    # and the older snapshot written last deleted the writer's change.
    sc = Scenario(
        initial=(7,),
        backend=backend,
        compact_every=2,
        threads=[
            [tx("tx append 1 2", lambda xs: xs.append(1), lambda xs: xs.append(2))],
            [_append(3)],
        ],
    )
    with pytest.MonkeyPatch.context() as mp:
        run = Runner(sc, mp)
        try:
            assert explore(run, bound=1).complete
        finally:
            run.close()


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("in_tx", [False, True], ids=["plain", "in-transaction"])
def test_stamp_keeps_a_record_another_engine_wrote_after_the_check(
    tmp_path: Any, backend: str, in_tx: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A new named collection that finds no record on disk stamps an empty
    # one, at construction or, made in a transaction, at its commit. Another
    # engine that created, filled and compacted the name after the check had
    # its snapshot replaced by the empty record. D41's fix looked again
    # before the write, in a separate read, which left the same window.
    a = Engine(cache_dir=tmp_path, cache_backend=backend)
    b = Engine(cache_dir=tmp_path, cache_backend=backend)
    disk = a._disk  # noqa: SLF001
    fired: list[bool] = []

    def other_engine_first(write: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not fired:
                fired.append(True)
                theirs = CascadeList(b, name="xs")
                theirs.extend([1, 2])
                theirs.compact()
            return write(*args, **kwargs)

        return wrapper

    # Whichever write the stamp makes, the other engine goes just before it.
    for method in ("collection_stamp", "collection_snapshot"):
        if hasattr(disk, method):
            write = getattr(disk, method)
            monkeypatch.setattr(disk, method, other_engine_first(write))
    try:
        if in_tx:
            with a.transaction():
                mine = CascadeList(a, name="xs")
        else:
            mine = CascadeList(a, name="xs")
        assert fired
        assert list(mine) == []  # it checked before the other engine wrote
    finally:
        a.shutdown()
        b.shutdown()
    with Engine(cache_dir=tmp_path, cache_backend=backend) as reopened:
        assert list(CascadeList(reopened, name="xs")) == [1, 2]


@pytest.mark.parametrize("backend", BACKENDS)
def test_collection_stamp_writes_only_when_there_is_no_record(
    tmp_path: Any, backend: str
) -> None:
    disk = disk_module.DiskCache(tmp_path, map_size=1 << 24, cache_backend=backend)
    try:
        first = {"kind": "list", "base_rev": 0, "next_uid": 0, "items": []}
        assert disk.collection_stamp("xs", first) is None
        assert disk.collection_load("xs") == (first, [])
        held = {"kind": "list", "base_rev": 2, "next_uid": 2, "items": [[0, 1], [1, 2]]}
        disk.collection_snapshot("xs", held, 2)
        assert disk.collection_stamp("xs", first) == held
        assert disk.collection_load("xs") == (held, [])
    finally:
        disk.close()


# --------------------------------------------------------------------------
# The harness itself
# --------------------------------------------------------------------------


def test_harness_reports_a_deadlock() -> None:
    def run_one(sched: Scheduler) -> None:
        a = _SchedLock(sched, "a", pause=True)
        b = _SchedLock(sched, "b", pause=True)

        def first() -> None:
            with a, b:
                pass

        def second() -> None:
            with b, a:
                pass

        sched.spawn("first", first)
        sched.spawn("second", second)
        sched.run()

    with pytest.raises(AssertionError, match="deadlock: .* waits for"):
        explore(run_one, bound=1)


def test_harness_reports_a_hung_schedule(monkeypatch: pytest.MonkeyPatch) -> None:
    # A thread blocked outside the schedule's control fails the run after
    # the timeout, naming where it was; the suite does not hang.
    monkeypatch.setattr(sys.modules[__name__], "STEP_TIMEOUT", 0.3)
    stuck = threading.Event()

    def run_one(sched: Scheduler) -> None:
        sched.spawn("waiter", lambda: (_point("before the wait"), stuck.wait(30)))
        sched.spawn("other", lambda: _point("other"))
        sched.run()

    started = time.monotonic()
    with pytest.raises(AssertionError, match="hung: waiter did not reach"):
        explore(run_one, bound=0)
    stuck.set()
    assert time.monotonic() - started < 10


def test_harness_replays_a_schedule_exactly() -> None:
    def scenario() -> Scenario:
        return SCENARIOS["transactions and a plain writer"][0]()

    first = Scheduler(rng=random.Random(5))
    with pytest.MonkeyPatch.context() as mp:
        run = Runner(scenario(), mp)
        run(first)
        again = Scheduler(first.picks, expect_widths=first.widths)
        run(again)
    assert again.picks == first.picks

    def threads(trace: list[str]) -> list[str]:
        return [step.split("@")[0] for step in trace]

    assert threads(again.trace) == threads(first.trace)


def test_bounded_walk_visits_each_schedule_once() -> None:
    # Two threads, each running four stretches between its start, three
    # pause points and its end: C(8, 4) = 70 schedules, each walked once,
    # which give all C(6, 3) = 20 orders of the six visible steps.
    schedules: list[tuple[int, ...]] = []
    orders: set[tuple[str, ...]] = set()

    def run_one(sched: Scheduler) -> None:
        order: list[str] = []

        def worker(name: str) -> Callable[[], None]:
            def body() -> None:
                for i in range(3):
                    _point(f"{name}{i}")
                    order.append(f"{name}{i}")

            return body

        sched.spawn("a", worker("a"))
        sched.spawn("b", worker("b"))
        sched.run()
        schedules.append(tuple(sched.picks))
        orders.add(tuple(order))

    stats = explore(run_one, bound=None)
    assert stats.complete
    assert len(schedules) == len(set(schedules)) == 70
    assert len(orders) == 20
    schedules.clear()
    bounded = explore(run_one, bound=1)
    assert bounded.runs == len(set(schedules)) < 70
