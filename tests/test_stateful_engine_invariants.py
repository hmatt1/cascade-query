"""Hypothesis state machine: every result equals a fresh plain-Python run.

The model does not look inside the engine. ``World`` holds what a snapshot
freezes (the input values that were set and the collection's contents), and
the ``plain_*`` functions compute each query from a ``World`` with ordinary
Python. Rules change the engine and the model side by side; every read is
compared with the plain function, on the current ``World`` or on the copy a
snapshot was taken with. The revision is modelled as well (G3, G25).

Beside inputs, a dynamic dependency and snapshots, the rules cover what the
2026-11-23 audit found broken and what was fixed since: TTL (G9),
re-registered code (G11), a fixed point read through snapshots (G4, G7),
transactions that commit or roll back (G25), compaction while snapshots are
held (G24), and a new engine on the same cache directory (G15, G16, G26).
Each of those has plain rules, which Hypothesis interleaves with everything
else, and one rule that walks through the scenario that was broken, from
whatever state the machine is in. The machine runs in memory and on each
disk backend.

Beyond one thread, each example draws an LRU bound (``max_entries``) and the
rules add, all compared with the same plain functions:

- errors: ``strict`` raises for a negative value and ``safe`` catches it
  (G8); the outcome of a raising call is compared as ``("raise", value)``;
- accumulator effects: ``noted`` pushes for a negative value, and every read
  compares the effects it collected with the pushes plain Python makes, in
  order, whether the result was computed, loaded or joined (G12);
- eviction: ``prune`` and ``sweep_unaccessed`` at any point, and a bound
  small enough that the LRU drops results while a query runs (G13);
- async queries awaited on a running loop, several tasks at once (G14);
- ``compute_many``, live and through a snapshot;
- threads: readers (live, through a snapshot, async on a loop of their own,
  ``compute_many``) run beside a writer that sets inputs, changes the list
  and evicts. A live read must equal the plain function of one of the states
  the writer passed through, a read through a snapshot the snapshot's state
  exactly; once all have finished, a read on one thread must give the final
  state's value, so a race cannot leave a wrong result behind (G4, G5).
  Results of this rule depend on how the threads interleave, so a failure
  in it may not shrink or repeat. Up to three writers run at once, each on
  inputs and a collection of its own, and a writer's step may be a whole
  transaction that commits or rolls back; a live read must then equal the
  plain function of some combination of the writers' states, never of half
  a transaction. A reader may also ``submit``, whose call may end in
  ``QueryCancelled`` only if a writer changed something (G6);
- two collections: ``items`` (a list) and ``tags`` (a set), with a query
  reading both from one rewritten comprehension, changed directly, in
  transactions and by concurrent writers;
- interleavings chosen by the machine, not by chance: in the lock-step
  rules each thread stops at the start of every query body it runs and goes
  on only when every other thread is stopped too, waiting for a run in the
  engine or finished, and drawn choices pick which stopped thread goes on.
  One of them walks through D35 (a fixed-point member whose result was
  dropped, asked for by several threads at once); another holds a
  submitted run while an input changes, which must cancel it (G6).

Since 2026-11-26:

- writers that share: several threads set the same inputs and change the
  same list and set, a step at a time or in transactions, in the
  concurrent rule (``shared``) and in a lock-step rule that also stops
  between the operations of a transaction. ``_serial`` enumerates every
  order of their steps that agrees with what each writer saw (what each
  ``pop`` got, which changes were refused because another thread's
  transaction had changed that collection, the list inside a transaction);
  the inputs, the list, the set and the revision the engine ends with must
  be those of one such order, and a live read among them must match a
  state one passes through;
- ``submit`` through a held snapshot gives the snapshot's value, alone and
  among threads, and is cancelled like a live one;
- the lock step sees every place a thread can wait for another: a run in
  the engine, the engine's locks (swapped for ``_TrackedLock``), and the
  helper threads of ``submit`` and ``compute_many``, which take part
  themselves. It never needs its patience in a correct run, so a wait that
  long fails the rule, and so does a deadlock (``_Lockstep.watch``);
- hidden cycles (``W_HEADS``): memoize=False fixed-point heads, sync and
  async, whose bodies read state the engine does not track (``Hidden``)
  and a TTL query, with results dropped while they run, on the same
  thread or another. They reach every fallback of the head that runs once
  on its cached members (0.4.2). Members are compared only through their
  head: a memoized member of a memoize=False head keeps the value of the
  head's last run, which hidden state may have made stale;
- a pipeline whose stage calls a query that runs a pipeline of its own
  (``spread``, ``shifted``).

Async in the lock step (2026-11-26, later):

- asyncio tasks are agents of the lock step as threads are. A task stops
  at the start of every async body and at awaits marked inside bodies by
  awaiting a future of its own, which the lock step resolves when the
  schedule picks it; in a sync body it stops as a thread does, holding its
  loop. ``store.waiting_on`` records tasks too, so a task waiting for a
  thread's run, a thread waiting for a task's, and a loop held by one of
  its tasks are all seen. Several tasks run on one loop, one or two loops
  on threads of their own, beside threads (``lockstep_tasks``); a task may
  ``submit`` and await the future, and the sync bodies that an async
  verification runs in the engine's executor run on helper threads that
  take part;
- one of several tasks asking for one query is cancelled at a point the
  machine picks (``cancelled_task_leaves_the_others_alone``): the others
  get the plain value and effects, and a cancelled waiter leaves the run
  alone (one run per key, G5);
- an async fixed point entered by tasks and threads through its head and
  through a member whose result was dropped (D35 with tasks), the G2 check
  on the async path, a sync query calling an async one (``bridge``, which
  on a loop may raise ``EventLoopBlockedError``, G14), and a hidden head
  that returns a value the engine cannot encode, so that its run on the
  cached members has no digest (``Unencodable``);
- a task through a snapshot and a live task asking for one query at once,
  at two revisions (G4, G5), and a TTL result dropped while a hidden cycle
  iterates over it (G7, G13).

The rules with tasks cost more than most, so each does its work on one draw
of its ``turn`` in two or three (``TURN_OF_2``). Event loops are kept for
the session (``_take_loop``).
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import inspect
import itertools
import os
import shutil
import sys
import tempfile
import threading
import time
import traceback
from collections import Counter
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest
from hypothesis import HealthCheck, Phase, settings, strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)

import cascade._collections as collections_module
import cascade._evaluator as evaluator_module
import cascade._scheduler as scheduler_module
from cascade import (
    CascadeList,
    CascadeSet,
    CycleError,
    Engine,
    EventLoopBlockedError,
    QueryCancelled,
    Snapshot,
    SnapshotExpiredError,
    StaleHandleError,
    UnencodableValueError,
)
from tests.scale_helpers import assert_internal_dependents_consistent

NODES = 3
TTL = 10.0
MAX_SNAPSHOTS = 6
QUERIES = (
    "choose",
    "scaled",
    "pair",
    "sensor",
    "reading",
    "reach",
    "total",
    "above_bias",
    "strict",
    "safe",
    "noted",
    "noted_pair",
    "achoose",
    "amix",
    "tag_total",
    "tagged",
    "loop_head",
    "nloop_head",
    "shifted",
    "spread",
    "wloop_head",
    "awloop_head",
    "anoted",
    "aloop_head",
    "bridge",
)
# The heads of the cycles whose bodies read hidden state (``Hidden``).
W_HEADS = ("wloop_head", "awloop_head")
# Their members, read on their own only right after their cycle converged
# (``cycle_follows_a_new_reading``): a memoized member of a memoize=False
# head keeps the value of the head's last run, which hidden state may have
# made stale.
W_MEMBERS = ("wloop_member", "wloop_detour", "awloop_member", "awloop_detour")
# Queries without arguments, and those defined with ``async def``.
NO_ARGS = (
    "sensor",
    "total",
    "above_bias",
    "tag_total",
    "tagged",
    "loop_head",
    "nloop_head",
    "loop_member",
    "spread",
    "aloop_head",
    "aloop_member",
    *W_HEADS,
    *W_MEMBERS,
)
ASYNC_QUERIES = (
    "achoose",
    "amix",
    "awloop_head",
    "anoted",
    "aloop_head",
    "aloop_member",
)
SYNC_QUERIES = tuple(q for q in QUERIES if q not in ASYNC_QUERIES)
# compute_many returns values only; a raising call would end the batch.
MANY_QUERIES = tuple(
    q for q in QUERIES if q not in ("strict", *W_HEADS, *ASYNC_QUERIES)
)
# How long a thread of the concurrent rule may take before it counts as hung.
JOIN_TIMEOUT = 60.0
# How long a lock-step thread may wait for the others to stop. The scheduler
# sees every place a thread can block on another (``_Lockstep._quiet``), so
# in a correct run it never waits this long: when it does, the rule fails
# (``_Lockstep.timeouts``).
LOCKSTEP_PATIENCE = 10.0
# How long every thread of a lock-step rule may be blocked on another, none
# stopped, before the rule fails as deadlocked (``_Lockstep.watch``).
DEADLOCK_PATIENCE = 2.0

INDEXES = st.integers(min_value=0, max_value=4)
NODE = st.integers(min_value=0, max_value=NODES - 1)
SMALL_INTS = st.integers(min_value=-8, max_value=8)
MAYBE_INT = st.one_of(st.none(), SMALL_INTS)
MAYBE_NODE = st.one_of(st.none(), NODE)
VARIANT = st.integers(min_value=0, max_value=2)
PICK = st.integers(min_value=0, max_value=2**16)
READ = st.tuples(st.sampled_from(QUERIES), INDEXES)
READS = st.lists(READ, min_size=1, max_size=6)
SET_INPUT = st.one_of(
    st.tuples(st.just("mode"), st.integers(min_value=0, max_value=1)),
    st.tuples(st.just("left"), INDEXES, MAYBE_INT),
    st.tuples(st.just("right"), INDEXES, MAYBE_INT),
    st.tuples(st.just("bias"), SMALL_INTS),
    st.tuples(st.just("edge"), NODE, MAYBE_NODE),
)
TAG_STEP = st.tuples(st.sampled_from(("tag_add", "tag_discard")), SMALL_INTS)
COLLECTION_STEP = st.one_of(
    st.tuples(st.just("append"), SMALL_INTS),
    st.tuples(st.just("pop")),
    TAG_STEP,
)
TRANSACTION_STEP = st.one_of(
    SET_INPUT,
    COLLECTION_STEP,
    st.tuples(st.just("read"), st.sampled_from(QUERIES), INDEXES),
)
EVICTION = st.one_of(
    st.tuples(st.just("sweep"), st.integers(min_value=0, max_value=30)),
    st.tuples(
        st.just("prune"),
        st.lists(st.tuples(st.sampled_from(QUERIES), INDEXES), max_size=3),
    ),
)
# What a writer of the concurrent rule does while readers read: one change,
# or a transaction of several that commits or rolls back.
WRITER_STEP = st.one_of(
    SET_INPUT,
    COLLECTION_STEP,
    EVICTION,
    st.tuples(
        st.just("txn"),
        st.lists(st.one_of(SET_INPUT, COLLECTION_STEP), min_size=1, max_size=4),
        st.booleans(),
    ),
)
# Concurrent writers own disjoint parts of the world, so that whatever order
# their steps take, they end in one state. With fewer than three writers a
# writer owns several of these domains (``_owner``).
DOMAINS = {
    "mode": 0,
    "bias": 0,
    "append": 0,
    "pop": 0,
    "left": 1,
    "right": 1,
    "tag_add": 1,
    "tag_discard": 1,
    "edge": 2,
    "sweep": 2,
    "prune": 2,
}
READER = st.tuples(
    st.sampled_from(("live", "snapshot", "async", "many", "submit", "submit_snapshot")),
    READS,
)
# Which stopped thread of a lock-step rule goes on next, drawn in turn.
SCHEDULE = st.lists(st.integers(min_value=0, max_value=3), min_size=1, max_size=12)
SYNC_READS = st.lists(
    st.tuples(st.sampled_from(SYNC_QUERIES), INDEXES), min_size=1, max_size=4
)
# Readers of the lock-step rules; ``submit`` and ``many`` hand their calls to
# helper threads, which take part in the lock step as well.
LOCKSTEP_READER = st.tuples(
    st.sampled_from(("live", "snapshot", "submit", "submit_snapshot", "many")),
    SYNC_READS,
)
# Agents of the lock-step rules with tasks: each task of a loop, and each
# thread beside them, reads queries of every kind. A task may read through a
# snapshot and ``submit`` (awaiting the future); a thread may also take the
# sync path of an async query (``plain``).
ANY_READS = st.lists(
    st.tuples(st.sampled_from(QUERIES), INDEXES), min_size=1, max_size=3
)
TASK = st.tuples(
    st.sampled_from(("task", "task_snapshot", "task_submit", "task_submit_snapshot")),
    ANY_READS,
)
LOOP = st.lists(TASK, min_size=1, max_size=3)
THREAD = st.tuples(
    st.sampled_from(("live", "snapshot", "plain", "submit", "submit_snapshot", "many")),
    ANY_READS,
)
# The rules with tasks cost more than most: each does its work on one draw
# in two or three of ``turn`` and nothing on the others, which keeps the file
# within its time budget on the free-threaded build. (A precondition on the
# machine's state would do the same, but makes a replayed failure report
# FlakyStrategyDefinition.)
TURN_OF_2 = st.integers(min_value=0, max_value=1)
TURN_OF_3 = st.integers(min_value=0, max_value=2)
# Results that may be dropped before a lock-step rule.
DROPPABLE = (*QUERIES, "loop_member", "aloop_member")
# Async queries that count their runs and are in no cycle: at most one run
# per key and revision while the revision stands still (G5).
ONCE = ("achoose", "amix", "anoted")
# What tasks ask for when one of them is cancelled.
CANCELLED_QUERIES = (
    "achoose",
    "anoted",
    "amix",
    "aloop_head",
    "awloop_head",
    "choose",
    "noted_pair",
    "bridge",
)
# Writers that share what they change: the same inputs, the same list and
# the same set, a step at a time or in transactions that commit or roll
# back. Few values, so that their writes collide.
SHARED_VALUE = st.integers(min_value=-2, max_value=2)
SHARED_SET = st.one_of(
    st.tuples(st.just("mode"), st.integers(min_value=0, max_value=1)),
    st.tuples(st.just("bias"), SHARED_VALUE),
    st.tuples(st.just("left"), st.integers(min_value=0, max_value=1), SHARED_VALUE),
)
SHARED_CHANGE = st.one_of(
    SHARED_SET,
    st.tuples(st.just("append"), SHARED_VALUE),
    st.tuples(st.just("pop")),
    st.tuples(st.sampled_from(("tag_add", "tag_discard")), SHARED_VALUE),
)
SHARED_STEP = st.one_of(
    SHARED_CHANGE,
    st.tuples(
        st.just("txn"),
        st.lists(SHARED_CHANGE, min_size=1, max_size=3),
        st.booleans(),
    ),
)
# Steps of the concurrent rule's writers when they share (``_serial``
# enumerates their orders, so each writer gets a few).
SHARED_PLAN = 3
# What is dropped while a head of a hidden cycle runs: a member, the head's
# own row, or whatever ``prune`` or ``sweep_unaccessed`` drop.
IN_RUN_EVICTION = st.one_of(
    st.tuples(st.just("drop"), st.sampled_from((*W_MEMBERS, *W_HEADS))),
    EVICTION,
)

# --- the plain-Python model ---------------------------------------------------

DEFAULTS: dict[str, Any] = {
    "mode": lambda: 0,
    "bias": lambda: 0,
    "left": lambda index: index,
    "right": lambda index: -index,
    "edge": lambda node: (node + 1) % NODES,
}

# The bodies ``scaled`` is registered with, one after another (G11).
SCALES: tuple[Any, ...] = (lambda c: c, lambda c: 2 * c, lambda c: -c)
assert len(SCALES) == 3  # VARIANT draws an index into it


@dataclass
class World:
    """What a snapshot freezes: input values and the collections' contents."""

    inputs: dict[tuple[str, tuple[int, ...]], int | None] = field(default_factory=dict)
    items: list[int] = field(default_factory=list)
    tags: set[int] = field(default_factory=set)

    def get(self, name: str, *args: int) -> int | None:
        key = (name, args)
        return self.inputs[key] if key in self.inputs else DEFAULTS[name](*args)

    def copy(self) -> World:
        return World(dict(self.inputs), list(self.items), set(self.tags))


def plain_choose(world: World, index: int) -> int:
    if world.get("mode") == 0:
        active = world.get("left", index)
    else:
        active = world.get("right", index)
    bias = world.get("bias")
    assert bias is not None
    return (0 if active is None else active) + bias


def plain_reach(world: World, node: int) -> tuple[int, ...]:
    seen: list[int] = []
    current: int | None = node
    while current is not None and current not in seen:
        seen.append(current)
        current = world.get("edge", current)
    return tuple(sorted(seen))


def plain_above_bias(world: World) -> int:
    bias = world.get("bias")
    assert bias is not None
    return len([x for x in world.items if x > bias])


def plain_tagged(world: World) -> int:
    return len([x for x in world.items if x in world.tags])


def plain_fixed_point(shift: int) -> int:
    """``head = head // 2 + shift``, iterated from the guess 0 until stable.

    It has two fixed points, ``2 * shift`` and ``2 * shift - 1``; iterating
    from 0 picks one. Those of two shifts never meet, so a stored value that
    one run on the cached members gives again belongs to that shift.
    """
    head = 0
    while (following := head // 2 + shift) != head:
        head = following
    return head


def plain_loop_head(world: World) -> int:
    """``loop_head``: iterated from its ``fixed_point=0`` guess until stable."""
    bias = world.get("bias")
    assert bias is not None
    return plain_fixed_point(bias)


def plain_spread(world: World) -> int:
    """``spread``: a pipeline whose stage calls ``shifted``, a pipeline too."""
    return sum(x % 3 + len(world.tags) for x in world.items)


@dataclass
class Hidden:
    """State the hidden-cycle bodies read that the engine does not track.

    ``drift`` shifts the value, ``detour`` sends the head through another
    member, ``fail`` makes it raise, and ``opaque`` makes it return a value
    the engine cannot encode (``Unencodable``). A change to any of them
    leaves the head's stored row current, so one run on the cached members
    does not give the stored value (or reaches the head again, or raises, or
    gives a value with no digest), and the head must fall back to iterating
    from the guess (0.4.2).
    """

    drift: int = 0
    detour: bool = False
    fail: bool = False
    opaque: bool = False


class Unencodable:
    """A result no codec can encode: ``UnencodableValueError`` (G18).

    A head that returns one raises that error on every engine. Run on its
    cached members, on a disk engine the value has no digest at all
    (``_reused_digest``); in memory it gets an opaque one, never the stored.
    """


def plain_noted(world: World, index: int) -> tuple[int, tuple[Any, ...]]:
    """``noted``: the value and what it pushes to ``notes``."""
    value = plain_choose(world, index)
    return value, (((index, value),) if value < 0 else ())


class _Clock:
    """Stands in for the ``time`` module inside the evaluator (G9)."""

    def __init__(self) -> None:
        self.now = 1000.0

    def time(self) -> float:
        return self.now


@dataclass
class _Held:
    snapshot: Snapshot
    world: World
    released: bool = False


class _Abort(Exception):
    pass


# How far the revision moves for a step with each verdict (``_changes``).
_MOVES = {True: (1,), False: (0,), None: (0, 1)}
_COLLECTION_OF = {
    "append": "items",
    "pop": "items",
    "tag_add": "tags",
    "tag_discard": "tags",
}


def _collections_of(step: tuple[Any, ...]) -> set[str]:
    """The collections a writer's step (or transaction) changes."""
    ops = step[1] if step[0] == "txn" else [step]
    return {_COLLECTION_OF[op[0]] for op in ops if op[0] in _COLLECTION_OF}


# Thread -> the tracked lock it is blocked on (``_TrackedLock``).
_LOCK_WAITS: dict[int, _TrackedLock] = {}


class _TrackedLock:
    """A re-entrant lock that tells the lock step who holds it and who waits.

    The engine's locks are swapped for these when the machine builds an
    engine (``_track_locks``): the store's, the pipelines', and each
    collection's write lock and log lock. A thread blocked on one is still
    while the thread holding it is (``_Lockstep._quiet``).
    """

    __slots__ = ("_count", "_inner", "owner")

    def __init__(self) -> None:
        self._inner = threading.RLock()
        self._count = 0
        self.owner: int | None = None

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        inner = self._inner
        if not inner.acquire(False):
            if not blocking:
                return False
            me = threading.get_ident()
            _LOCK_WAITS[me] = self
            try:
                if not inner.acquire(True, timeout):
                    return False
            finally:
                del _LOCK_WAITS[me]
        self.owner = threading.get_ident()
        self._count += 1
        return True

    __enter__ = acquire

    def release(self) -> None:
        self._count -= 1
        if not self._count:
            self.owner = None
        self._inner.release()

    def __exit__(self, *exc: Any) -> None:
        self.release()

    def _is_owned(self) -> bool:
        return self._inner._is_owned()  # type: ignore[attr-defined]


# Event loops kept for the whole session. A loop may run on any thread, one
# at a time; reusing them saves making a few for every example, which on
# Windows opens a pair of sockets each time, and under load that has hung
# (in ``socket.accept``).
_LOOPS: list[asyncio.AbstractEventLoop] = []
_LOOPS_LOCK = threading.Lock()


def _take_loop() -> asyncio.AbstractEventLoop:
    with _LOOPS_LOCK:
        if _LOOPS:
            return _LOOPS.pop()
    return asyncio.new_event_loop()


def _give_loop(loop: asyncio.AbstractEventLoop) -> None:
    with _LOOPS_LOCK:
        _LOOPS.append(loop)


def _loop_thread(task: asyncio.Task[Any]) -> int | None:
    """The thread running ``task``'s loop; None when it is not running."""
    return getattr(task.get_loop(), "_thread_id", None)


def _wake(future: asyncio.Future[Any]) -> None:
    if not future.done():
        future.set_result(None)


class _Lockstep:
    """Lets one thread or task at a time run, each until it stops.

    The agents are threads and asyncio tasks. A thread taking part stops at
    the start of every query body it runs, and a writer before each of its
    steps (``checkpoint``). A task taking part stops at the start of every
    async query body it runs and at the awaits marked in them
    (``acheckpoint``) by awaiting a future of its own, which leaves its loop
    to the other tasks; in a sync query body it calls, it stops as a thread
    does, holding its whole loop. A stopped agent goes on only when every
    other one is still, and of the stopped agents that can go on the drawn
    ``schedule`` picks which. So the interleaving is chosen by the machine,
    not by the OS or the event loop.

    An agent is still when it is stopped or finished, or blocked on an agent
    that is still itself (``_quiet``): waiting for another agent's run in the
    engine (``store.waiting_on``, which records tasks as well as threads),
    or for one of the engine's locks. A task is also still while its loop is
    held by the code of another task on it that is still, and a thread that
    runs a loop of tasks that take part is still while they all are. Calls
    an agent hands to others (``submit``, ``compute_many``, the sync bodies
    an async verification runs in an executor) run on helper threads that
    take part as well, while the agent that waits for them counts as still
    (``delegating``). A collection another thread's transaction has changed
    is not a place to block: changing it raises.

    No agent should ever wait ``LOCKSTEP_PATIENCE`` for the others; one that
    does goes on, and records why in ``timeouts``, which fails the rule.
    """

    def __init__(self, engine: Engine, schedule: list[int]) -> None:
        self.store = engine._internals._store  # noqa: SLF001
        self.schedule = itertools.cycle(schedule or [0])
        self.cond = threading.Condition()
        # agent (a thread id or an asyncio task) -> running, stopped,
        # delegating or done; a thread running a loop of tasks that take
        # part is "loop" until they are all done
        self.state: dict[Any, str] = {}
        self.stopped: list[Any] = []  # in the order they stopped
        self.turn: Any = None
        # Calls handed to helper threads that have not started yet.
        self.pending = 0
        self.timeouts: list[str] = []
        # Why the others were still when a turn was handed out.
        self.seen: Counter[str] = Counter()
        # A loop thread -> its tasks that take part, and back.
        self.tasks_of: dict[int, list[asyncio.Task[Any]]] = {}
        self.thread_of: dict[asyncio.Task[Any], int] = {}
        # Tasks stopped by awaiting: each one's loop and the future that
        # wakes it.
        self.wakers: dict[asyncio.Task[Any], tuple[Any, asyncio.Future[Any]]] = {}
        # A loop thread -> the task stopped in sync code on it, which holds
        # the loop until it goes on.
        self.holding: dict[int, asyncio.Task[Any]] = {}
        # When the turn was last handed (or the wake-up sent again) to a task.
        self.woken = 0.0

    def _agent(self) -> Any:
        """The current task if it takes part, else the current thread."""
        if asyncio._get_running_loop() is not None:  # noqa: SLF001
            task = asyncio.current_task()
            if task is not None and task in self.state:
                return task
        return threading.get_ident()

    def join(self) -> None:
        with self.cond:
            self.state[threading.get_ident()] = "running"
            self.cond.notify_all()

    def leave(self) -> None:
        with self.cond:
            self.state[threading.get_ident()] = "done"
            self.cond.notify_all()

    @contextlib.contextmanager
    def delegating(self) -> Any:
        """Wait for helper threads, which take part themselves."""
        with self.cond:
            me = self._agent()
            taking_part = self.state.get(me) == "running"
            if taking_part:
                self.state[me] = "delegating"
                self.cond.notify_all()
        try:
            yield
        finally:
            if taking_part:
                with self.cond:
                    self.state[me] = "running"
                    self.cond.notify_all()

    def helper(self, fn: Any, wake: Any = None) -> Any:
        """``fn``, to be run on a helper thread that takes part while it runs.

        ``wake``, an agent delegating to this one call alone, counts as
        running again from the moment the call ends, not from when it gets
        round to noticing.
        """
        with self.cond:
            self.pending += 1

        def run(*args: Any, **kwargs: Any) -> Any:
            me = threading.get_ident()
            with self.cond:
                self.pending -= 1
                self.state[me] = "running"
                self.cond.notify_all()
            try:
                return fn(*args, **kwargs)
            finally:
                with self.cond:
                    self.state[me] = "done"
                    if wake is not None and self.state.get(wake) == "delegating":
                        self.state[wake] = "running"
                    self.cond.notify_all()

        return run

    async def run_tasks(self, programs: list[Any]) -> list[Any]:
        """Run each program as a task on this thread's loop, all taking part.

        A program is called with the list of all the tasks (so that one can
        cancel another) and gives the coroutine to run. Gives each task's
        result, or the exception it ended with.
        """
        me = threading.get_ident()
        loop = asyncio.get_running_loop()
        peers: list[asyncio.Task[Any]] = []
        with self.cond:
            for program in programs:
                task = loop.create_task(program(peers))
                peers.append(task)
                self.state[task] = "running"
                self.thread_of[task] = me
                task.add_done_callback(self._task_done)
            self.tasks_of[me] = peers
            self.state[me] = "loop"
            self.cond.notify_all()
        return await asyncio.gather(*peers, return_exceptions=True)

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        with self.cond:
            self.state[task] = "done"
            thread = self.thread_of[task]
            if all(self.state[t] == "done" for t in self.tasks_of[thread]):
                self.state[thread] = "running"
            self.cond.notify_all()

    def _quiet(self, agent: Any, seen: set[Any] | None = None) -> str | None:
        """Why ``agent`` cannot move until a stopped one does, or None."""
        state = self.state.get(agent)
        if state is not None and state not in ("running", "loop"):
            return state
        # Running, or not taking part: still only while it waits for an
        # agent that is still. A cycle of waits never ends (the deadlock
        # watch reports it).
        seen = set() if seen is None else seen
        if agent in seen:
            return "deadlocked"
        seen.add(agent)
        if isinstance(agent, asyncio.Task):
            if state is None:
                # Not taking part (the task of ``asyncio.run`` on a thread
                # that does): as still as its thread.
                thread = _loop_thread(agent)
                return None if thread is None else self._quiet(thread, seen)
            slot = self.store.waiting_on.get(agent)
            if slot is not None:
                return "run" if self._quiet(slot.owner, seen) else None
            return "loop" if self._held(self.thread_of[agent], seen) else None
        if state == "loop":
            if self._held(agent, seen):
                return "loop"
            tasks = self.tasks_of.get(agent, [])
            return "loop" if all(self._quiet(t, set(seen)) for t in tasks) else None
        return self._held(agent, seen)

    def _held(self, thread: int, seen: set[Any]) -> str | None:
        """Why ``thread``'s code cannot move until a still agent does, or None."""
        if thread in self.holding:
            return "stopped"
        waits = self.store.waiting_on
        slot = waits.get(thread)
        if slot is not None:
            return "run" if self._quiet(slot.owner, seen) else None
        lock = _LOCK_WAITS.get(thread)
        if lock is not None:
            owner = lock.owner
            if owner is not None and owner != thread and self._quiet(owner, seen):
                return "lock"
            return None
        # The task of ``asyncio.run`` on this thread, which does not take
        # part itself, awaiting a run: the thread waits with it.
        for waiter, slot in list(waits.items()):
            if (
                isinstance(waiter, asyncio.Task)
                and waiter not in self.state
                and _loop_thread(waiter) == thread
            ):
                return "run" if self._quiet(slot.owner, seen) else None
        return None

    def _ready(self, agent: Any) -> bool:
        """Whether stopped ``agent`` can go on: its loop is not held."""
        if agent not in self.wakers:
            return True
        return not self._held(self.thread_of[agent], set())

    def _dispatch_locked(self) -> bool:
        """If everyone is still, hand the turn to a drawn stopped agent."""
        if self.turn is not None or self.pending:
            return False
        reasons = [self._quiet(agent) for agent in list(self.state)]
        if not all(reasons):
            return False
        ready = [agent for agent in self.stopped if self._ready(agent)]
        if not ready:
            return False
        self.seen.update(r for r in reasons if r != "stopped")
        self.turn = ready[next(self.schedule) % len(ready)]
        self._wake_turn_locked()
        self.cond.notify_all()
        return True

    def _wake_turn_locked(self) -> None:
        """Wake the task that has the turn, if it stopped by awaiting.

        Sent again by ``watch`` while the task has not gone on: a wake-up
        is a callback handed to another thread's loop, and the waker stays
        on record until the task takes it (``acheckpoint``).
        """
        waker = self.wakers.get(self.turn)
        if waker is not None:
            loop, future = waker
            self.woken = time.monotonic()
            loop.call_soon_threadsafe(_wake, future)

    def _describe(self) -> str:
        waits = self.store.waiting_on
        frames = sys._current_frames()  # noqa: SLF001
        lines = []
        for agent, state in list(self.state.items()):
            run = getattr(waits.get(agent), "owner", None)
            if isinstance(agent, asyncio.Task):
                lines.append(f"{agent.get_name()}: {state}, waits for the run of {run}")
            elif agent in frames:
                lines.append(
                    f"{agent}: {state}, waits for the run of {run}, for the lock "
                    f"of {getattr(_LOCK_WAITS.get(agent), 'owner', None)}, at "
                    + " < ".join(
                        f"{f.name}:{f.lineno}"
                        for f in reversed(traceback.extract_stack(frames[agent])[-6:])
                    )
                )
            else:
                lines.append(f"{agent}: {state}, ended, waits for the run of {run}")
        agents = "\n".join(lines)
        why = [self._quiet(agent) for agent in list(self.state)]
        return (
            f"{agents}\n{self.pending} calls not started, turn {self.turn}, "
            f"stopped {self.stopped}, still {why}, holding {self.holding}, "
            f"ready {[self._ready(agent) for agent in self.stopped]}"
        )

    def _stuck(self) -> bool:
        """Whether no agent can move: all still, and none stopped to go on."""
        with self.cond:
            if self.pending or self.turn is not None:
                return False
            reasons = [self._quiet(agent) for agent in list(self.state)]
            ready = any(self._ready(agent) for agent in self.stopped)
        return all(reasons) and not ready and any(r != "done" for r in reasons)

    def watch(self, threads: list[threading.Thread]) -> str | None:
        """Wait for ``threads``; describe a deadlock if they fall into one.

        Hands out turns as well, for when every agent stopped is a task (a
        stopped thread hands them out itself). Every agent blocked on another
        that is blocked too, with none stopped that could be handed the
        turn, for ``DEADLOCK_PATIENCE`` on end: none of them will ever move
        again.
        """
        deadline = time.monotonic() + JOIN_TIMEOUT
        stuck_since = None
        while time.monotonic() < deadline:
            alive = [t for t in threads if t.is_alive()]
            if not alive:
                return None
            # Only a task that stops waits for this thread to be handed the
            # turn; threads hand it out themselves.
            alive[0].join(0.001 if self.thread_of else 0.01)
            with self.cond:
                if not self._dispatch_locked() and self.turn in self.wakers:
                    if time.monotonic() - self.woken > 0.1:
                        self._wake_turn_locked()
            if not self._stuck():
                stuck_since = None
            elif stuck_since is None:
                stuck_since = time.monotonic()
            elif time.monotonic() - stuck_since > DEADLOCK_PATIENCE:
                return self._describe()
        return None

    def checkpoint(self) -> None:
        with self.cond:
            me = self._agent()
            if self.state.get(me) != "running" or self.timeouts:
                return
            self.state[me] = "stopped"
            self.stopped.append(me)
            thread = threading.get_ident()
            if me != thread:
                # A task stopped in sync code: its whole loop waits with it.
                self.holding[thread] = me
            deadline = time.monotonic() + LOCKSTEP_PATIENCE
            while self.turn != me:
                if self._dispatch_locked():
                    continue
                if time.monotonic() > deadline:
                    self.timeouts.append(self._describe())
                    break
                self.cond.wait(0.001)
            if self.turn == me:
                self.turn = None
            self.stopped.remove(me)
            self.holding.pop(thread, None)
            self.state[me] = "running"
            self.cond.notify_all()

    async def acheckpoint(self) -> None:
        """Stop a task that takes part by awaiting; others stop as threads."""
        task = asyncio.current_task()
        loop = asyncio.get_running_loop()
        with self.cond:
            if task is None or task not in self.state:
                stop = None
            elif self.state[task] != "running" or self.timeouts:
                return
            else:
                stop = loop.create_future()
                self.state[task] = "stopped"
                self.stopped.append(task)
                self.wakers[task] = (loop, stop)
                self._dispatch_locked()
        if stop is None:
            self.checkpoint()
            return
        try:
            done, _ = await asyncio.wait([stop], timeout=LOCKSTEP_PATIENCE)
            if not done:
                with self.cond:
                    self.timeouts.append(self._describe())
        finally:
            # Also when the task is cancelled while it is stopped.
            with self.cond:
                self.wakers.pop(task, None)
                if self.turn is task:
                    self.turn = None
                if task in self.stopped:
                    self.stopped.remove(task)
                self.state[task] = "running"
                self.cond.notify_all()


class _ThreadPerTask(concurrent.futures.Executor):
    """Runs each submitted call on a thread of its own, in the lock step.

    With ``wake_caller`` the agent that submits a call waits for that call
    alone (``_Lockstep.helper``).
    """

    def __init__(self, lockstep: _Lockstep, *, wake_caller: bool = False) -> None:
        self.lockstep = lockstep
        self.wake_caller = wake_caller

    def submit(self, fn: Any, /, *args: Any, **kwargs: Any) -> Any:
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        wake = None
        if self.wake_caller:
            with self.lockstep.cond:
                wake = self.lockstep._agent()  # noqa: SLF001
        task = self.lockstep.helper(fn, wake)

        def run() -> None:
            future.set_running_or_notify_cancel()
            try:
                future.set_result(task(*args, **kwargs))
            except BaseException as exc:  # noqa: BLE001  (handed to the caller)
                future.set_exception(exc)

        threading.Thread(target=run, daemon=True).start()
        return future


def _track_locks(engine: Engine, *collections: Any) -> None:
    """Swap the engine's locks for ``_TrackedLock``s before anything uses them."""
    engine._internals._store.lock = _TrackedLock()  # noqa: SLF001
    engine._incremental_rt._lock = _TrackedLock()  # noqa: SLF001
    for collection in collections:
        core = collection._core  # noqa: SLF001
        core.rw = _TrackedLock()
        core.log.lock = _TrackedLock()


class EngineStateMachine(RuleBasedStateMachine):
    backend: str | None = None

    def __init__(self) -> None:
        super().__init__()
        self.world = World()
        self.revision = 0
        self.variant = 0
        self.snapshots: list[_Held] = []
        self.runs: Counter[str] = Counter()
        # Runs of the queries that count them, per (name, args, revision).
        self.key_runs: Counter[Any] = Counter()
        self.count_lock = threading.Lock()
        self.clock = _Clock()
        self.sensor_value = 0
        # (the time a value stopped being current, the value)
        self.sensor_history: list[tuple[float, int]] = []
        self.cache_dir = None if self.backend is None else tempfile.mkdtemp()
        self.engine: Engine | None = None
        self.max_entries = 10_000
        # Set by the rules that choose how threads interleave; every query
        # body passes through ``_checkpoint`` first.
        self.lockstep: _Lockstep | None = None
        self.hold: Any = None
        # Called at the start of every query body, on any thread
        # (``evict_while_a_head_runs``).
        self.inside: Any = None
        self.hidden = Hidden()
        self.loop = _take_loop()
        # Loops the lock-step rules run their tasks on, each on a thread of
        # its own (``_loop_of``).
        self.task_loops: list[asyncio.AbstractEventLoop] = []
        self._patches = [
            mock.patch.object(evaluator_module, "time", self.clock),
            # Small enough for the machine's handful of changes to reach
            # the folding of old diffs (G24).
            mock.patch.object(collections_module, "_MEMORY_COMPACT_THRESHOLD", 4),
        ]
        for patch in self._patches:
            patch.start()

    @initialize(max_entries=st.sampled_from((10_000, 3, 8)))
    def start(self, max_entries: int) -> None:
        """Build the engine; a small bound makes the LRU drop results (G13)."""
        self.max_entries = max_entries
        self._build_engine()

    # --- the engine under test -----------------------------------------------

    def _build_engine(self) -> None:
        if self.cache_dir is None:
            engine = Engine(max_entries=self.max_entries)
        else:
            assert self.backend is not None
            engine = Engine(
                max_entries=self.max_entries,
                cache_dir=self.cache_dir,
                cache_backend=self.backend,
            )
        # The current handles. A query looks ``scaled`` up here on every
        # run, because registering new code makes the old handle stale.
        h = SimpleNamespace()

        @engine.input(name="mode")
        def mode() -> int:
            return 0

        @engine.input(name="left")
        def left(index: int) -> int | None:
            return index

        @engine.input(name="right")
        def right(index: int) -> int | None:
            return -index

        @engine.input(name="bias")
        def bias() -> int:
            return 0

        @engine.input(name="edge")
        def edge(node: int) -> int | None:
            return (node + 1) % NODES

        checkpoint = self._checkpoint
        acheckpoint = self._acheckpoint

        def count(name: str, *args: Any) -> None:
            """Count a run of ``name(*args)`` at the revision it runs at (G5)."""
            runtime = engine._evaluator.current_runtime()  # noqa: SLF001
            assert runtime is not None
            with self.count_lock:
                self.runs[name] += 1
                self.key_runs[(name, args, runtime.snapshot.revision)] += 1

        @engine.query(name="choose")
        def choose(index: int) -> int:
            checkpoint("choose")
            count("choose", index)
            active = left(index) if mode() == 0 else right(index)
            return (0 if active is None else active) + bias()

        def scaled_once(index: int) -> int:
            return choose(index)

        def scaled_twice(index: int) -> int:
            return 2 * choose(index)

        def scaled_negated(index: int) -> int:
            return -choose(index)

        @engine.query(name="pair")
        def pair(index: int) -> int:
            checkpoint("pair")
            return h.scaled(index) + h.scaled(index + 1)

        @engine.query(name="sensor", ttl=TTL)
        def sensor() -> int:
            checkpoint("sensor")
            return self.sensor_value

        @engine.query(name="reading")
        def reading(index: int) -> int:
            checkpoint("reading")
            return choose(index) + sensor()

        @engine.query(name="reach", fixed_point=())
        def reach(node: int) -> tuple[int, ...]:
            checkpoint("reach")
            successor = edge(node)
            beyond = () if successor is None else reach(successor)
            return tuple(sorted({node, *beyond}))

        items = CascadeList(engine, name="items", compact_every=3)
        tags = CascadeSet(engine, name="tags", compact_every=3)

        @engine.query(name="total")
        def total() -> int:
            checkpoint("total")
            return sum(x for x in items)

        @engine.query(name="above_bias")
        def above_bias() -> int:
            checkpoint("above_bias")
            return len([x for x in items if x > bias()])

        @engine.query(name="tag_total")
        def tag_total() -> int:
            checkpoint("tag_total")
            return sum(t for t in tags)

        @engine.query(name="tagged")
        def tagged() -> int:
            # Each element of one collection looked up in the other.
            checkpoint("tagged")
            return len([x for x in items if x in tags])

        # A cycle with a member apart from its head (D25, D35): the head
        # converges to the plain ``plain_loop_head``.
        @engine.query(name="loop_head", fixed_point=0)
        def loop_head() -> int:
            checkpoint("loop_head")
            return loop_member() + bias()

        @engine.query(name="loop_member")
        def loop_member() -> int:
            checkpoint("loop_member")
            return loop_head() // 2

        # The same cycle with a memoize=False head: every call runs it, on
        # its cached members while nothing changed.
        @engine.query(name="nloop_head", fixed_point=0, memoize=False)
        def nloop_head() -> int:
            checkpoint("nloop_head")
            return nloop_member() + bias()

        @engine.query(name="nloop_member")
        def nloop_member() -> int:
            checkpoint("nloop_member")
            return nloop_head() // 2

        # The same cycle again, whose head also reads state the engine does
        # not track (``Hidden``) and a TTL query. A run on the cached members
        # then need not give the stored value, may reach the head again
        # through ``wloop_detour``, may raise, or may find the row expired:
        # each must fall back to iterating from the guess (0.4.2).
        hidden = self.hidden

        @engine.query(name="wloop_head", fixed_point=0, memoize=False)
        def wloop_head() -> int:
            checkpoint("wloop_head")
            member = wloop_detour() if hidden.detour else wloop_member()
            if hidden.fail:
                raise ValueError("w")
            if hidden.opaque:
                return Unencodable()
            return member + bias() + hidden.drift + sensor()

        @engine.query(name="wloop_member")
        def wloop_member() -> int:
            checkpoint("wloop_member")
            return wloop_head() // 2

        @engine.query(name="wloop_detour")
        def wloop_detour() -> int:
            checkpoint("wloop_detour")
            return wloop_head() // 2

        @engine.query(name="awloop_head", fixed_point=0, memoize=False)
        async def awloop_head() -> int:
            await acheckpoint("awloop_head")
            if hidden.detour:
                member = await awloop_detour()
            else:
                member = await awloop_member()
            if hidden.fail:
                raise ValueError("w")
            if hidden.opaque:
                return Unencodable()
            return member + bias() + hidden.drift + sensor()

        @engine.query(name="awloop_member")
        async def awloop_member() -> int:
            await acheckpoint("awloop_member")
            return (await awloop_head()) // 2

        @engine.query(name="awloop_detour")
        async def awloop_detour() -> int:
            await acheckpoint("awloop_detour")
            return (await awloop_head()) // 2

        # The plain cycle once more, async: tasks and threads enter it (G7).
        @engine.query(name="aloop_head", fixed_point=0)
        async def aloop_head() -> int:
            await acheckpoint("aloop_head")
            return (await aloop_member()) + bias()

        @engine.query(name="aloop_member")
        async def aloop_member() -> int:
            await acheckpoint("aloop_member")
            return (await aloop_head()) // 2

        # A pipeline whose stage calls a query that runs a pipeline of its
        # own: a thread inside ``spread`` holds the pipelines' lock while it
        # waits for ``shifted``.
        @engine.query(name="shifted")
        def shifted(index: int) -> int:
            checkpoint("shifted")
            return index + len([t for t in tags])

        @engine.query(name="spread")
        def spread() -> int:
            checkpoint("spread")
            return sum(shifted(x % 3) for x in items)

        notes = engine.accumulator("notes")

        @engine.query(name="strict")
        def strict(index: int) -> int:
            checkpoint("strict")
            value = choose(index)
            if value < 0:
                raise ValueError(value)
            return value

        @engine.query(name="safe")
        def safe(index: int) -> int:
            checkpoint("safe")
            try:
                return strict(index)
            except ValueError as exc:
                return 100 + exc.args[0]

        @engine.query(name="noted")
        def noted(index: int) -> int:
            checkpoint("noted")
            count("noted", index)
            value = choose(index)
            if value < 0:
                notes.push((index, value))
            return value

        @engine.query(name="noted_pair")
        def noted_pair(index: int) -> int:
            checkpoint("noted_pair")
            return noted(index) + noted(index + 1)

        @engine.query(name="achoose")
        async def achoose(index: int) -> int:
            await acheckpoint("achoose")
            count("achoose", index)
            await asyncio.sleep(0)
            return choose(index)

        @engine.query(name="amix")
        async def amix(index: int) -> int:
            await acheckpoint("amix")
            count("amix", index)
            first = await achoose(index)
            # An await inside the body: others may go on between the two.
            await acheckpoint("amix:between")
            return first - noted(index + 1)

        @engine.query(name="anoted")
        async def anoted(index: int) -> int:
            await acheckpoint("anoted")
            count("anoted", index)
            value = await achoose(index)
            if value < 0:
                notes.push((index, value))
            return value

        # A sync query calling an async one (G14, D21): the sync path. On
        # the thread of a running loop it raises EventLoopBlockedError when
        # ``achoose`` has to run there or is run by a task on that loop.
        @engine.query(name="bridge")
        def bridge(index: int) -> int:
            checkpoint("bridge")
            return achoose(index) + 1

        h.mode, h.left, h.right, h.bias, h.edge = mode, left, right, bias, edge
        h.choose, h.pair, h.sensor, h.reading = choose, pair, sensor, reading
        h.reach, h.total, h.above_bias = reach, total, above_bias
        h.strict, h.safe, h.noted, h.noted_pair = strict, safe, noted, noted_pair
        h.achoose, h.amix, h.anoted, h.bridge = achoose, amix, anoted, bridge
        h.aloop_head, h.aloop_member = aloop_head, aloop_member
        h.tag_total, h.tagged = tag_total, tagged
        h.loop_head, h.loop_member = loop_head, loop_member
        h.nloop_head, h.nloop_member = nloop_head, nloop_member
        h.wloop_head, h.wloop_member, h.wloop_detour = (
            wloop_head,
            wloop_member,
            wloop_detour,
        )
        h.awloop_head, h.awloop_member, h.awloop_detour = (
            awloop_head,
            awloop_member,
            awloop_detour,
        )
        h.shifted, h.spread = shifted, spread
        _track_locks(engine, items, tags)
        self.engine = engine
        self.h = h
        self.items = items
        self.tags = tags
        self.variants = (scaled_once, scaled_twice, scaled_negated)
        self._register_scaled(self.variant)
        # Opening the named collection publishes its head, which is a
        # revision of its own; from here on the revision is modelled.
        self.revision = engine.revision

    def _checkpoint(self, name: str) -> None:
        lockstep = self.lockstep
        if lockstep is not None:
            lockstep.checkpoint()
        hold = self.hold
        if hold is not None:
            hold(name)
        inside = self.inside
        if inside is not None:
            inside(name)

    async def _acheckpoint(self, name: str) -> None:
        """``_checkpoint`` for async bodies: a task stops by awaiting."""
        lockstep = self.lockstep
        if lockstep is not None:
            await lockstep.acheckpoint()
        hold = self.hold
        if hold is not None:
            hold(name)
        inside = self.inside
        if inside is not None:
            inside(name)

    def _register_scaled(self, variant: int) -> None:
        assert self.engine is not None
        self.h.scaled = self.engine.query(self.variants[variant], name="scaled")
        self.variant = variant

    def _close_engine(self) -> None:
        for held in self.snapshots:
            held.snapshot.release()
        self.snapshots = []
        if self.engine is not None:
            engine, self.engine = self.engine, None
            engine.shutdown()

    def teardown(self) -> None:
        try:
            self._close_engine()
        finally:
            for loop in [self.loop, *self.task_loops]:
                _give_loop(loop)
            for patch in self._patches:
                patch.stop()
            if self.cache_dir is not None:
                shutil.rmtree(self.cache_dir, ignore_errors=True)

    # --- what plain Python gives ---------------------------------------------

    def _sensor_values(self) -> set[int]:
        """The sensor readings a TTL result may still hold (G9).

        A result is served until its TTL has elapsed, so beside the current
        value every value that was current within the last ``TTL`` seconds
        is a correct answer, and no other.
        """
        horizon = self.clock.now - TTL
        recent = {value for ended, value in self.sensor_history if ended >= horizon}
        return recent | {self.sensor_value}

    def _expected(self, world: World, query: str, index: int) -> set[Any]:
        scale = SCALES[self.variant]
        if query == "choose":
            return {plain_choose(world, index)}
        if query == "scaled":
            return {scale(plain_choose(world, index))}
        if query == "pair":
            return {
                scale(plain_choose(world, index))
                + scale(plain_choose(world, index + 1))
            }
        if query == "sensor":
            return self._sensor_values()
        if query == "reading":
            return {plain_choose(world, index) + v for v in self._sensor_values()}
        if query == "reach":
            return {plain_reach(world, index % NODES)}
        if query == "total":
            return {sum(world.items)}
        if query == "above_bias":
            return {plain_above_bias(world)}
        if query == "tag_total":
            return {sum(world.tags)}
        if query == "tagged":
            return {plain_tagged(world)}
        if query in ("loop_head", "nloop_head"):
            return {plain_loop_head(world)}
        if query == "shifted":
            return {index + len(world.tags)}
        if query == "spread":
            return {plain_spread(world)}
        if query in ("loop_member", "aloop_member"):
            return {plain_loop_head(world) // 2}
        if query == "aloop_head":
            return {plain_loop_head(world)}
        if query == "bridge":
            return {plain_choose(world, index) + 1}
        return {value for value, _ in self._outcomes(world, query, index)}

    def _outcomes(
        self, world: World, query: str, index: int, on_loop: bool = False
    ) -> set[Any]:
        """Each correct ``(outcome, notes)`` of a call.

        The outcome is the value, or ``("raise", value)`` for the
        ``ValueError`` of ``strict``; ``notes`` is what the call collects in
        ``effects["notes"]``, in order (G12). ``on_loop``: the call is made
        on the thread of a running event loop, where a sync query that has
        to run an async one raises ``EventLoopBlockedError`` (G14).
        """
        if query == "bridge" and on_loop:
            return {(plain_choose(world, index) + 1, ()), (("blocked",), ())}
        if query in ("strict", "safe", "achoose"):
            value = plain_choose(world, index)
            if query == "strict" and value < 0:
                return {(("raise", value), ())}
            if query == "safe" and value < 0:
                return {(100 + value, ())}
            return {(value, ())}
        if query in ("noted", "anoted"):
            return {plain_noted(world, index)}
        if query == "noted_pair":
            (first, a), (second, b) = (
                plain_noted(world, index),
                plain_noted(world, index + 1),
            )
            return {(first + second, a + b)}
        if query == "amix":
            second, notes = plain_noted(world, index + 1)
            return {(plain_choose(world, index) - second, notes)}
        if query in W_HEADS:
            if self.hidden.fail:
                return {(("raise", "w"), ())}
            if self.hidden.opaque:
                return {(("unencodable",), ())}
            bias = world.get("bias")
            assert bias is not None
            shift = bias + self.hidden.drift
            return {(plain_fixed_point(shift + v), ()) for v in self._sensor_values()}
        outcomes = {(value, ()) for value in self._expected(world, query, index)}
        if query in ("loop_member", "aloop_member") and self.max_entries != 10_000:
            # Entered through a member, a cycle whose head has no result
            # may raise (G7); a small LRU bound can drop the head's result
            # while the member runs.
            outcomes.add((("cycle",), ()))
        return outcomes

    def _args(self, query: str, index: int) -> tuple[int, ...]:
        if query in NO_ARGS:
            return ()
        if query == "reach":
            return (index % NODES,)
        return (index,)

    @staticmethod
    def _failure(exc: BaseException) -> Any:
        """The outcome of a call that raised ``exc``; None to re-raise it."""
        if isinstance(exc, UnencodableValueError):
            return ("unencodable",)
        if isinstance(exc, ValueError):
            return ("raise", *exc.args)
        if isinstance(exc, CycleError):
            # Allowed only where the model says so (``loop_member``).
            return ("cycle",)
        if isinstance(exc, EventLoopBlockedError):
            # Allowed only on the thread of a running loop (``bridge``).
            return ("blocked",)
        return None

    @classmethod
    def _outcome(cls, effects: dict[str, list[Any]], call: Any) -> Any:
        """Run ``call``; give its outcome and the notes it collected."""
        try:
            value = call()
        except (ValueError, TypeError, CycleError, EventLoopBlockedError) as exc:
            value = cls._failure(exc)
            if value is None:
                raise
        return value, tuple(effects.get("notes", ()))

    @staticmethod
    def _run_on_a_loop(coroutine: Any) -> Any:
        """Run ``coroutine`` on a loop no other thread is running.

        Like ``asyncio.run``, but on a loop of the pool (``_take_loop``).
        """
        loop = _take_loop()
        try:
            return loop.run_until_complete(coroutine)
        finally:
            _give_loop(loop)

    def _call(self, query: str, index: int, snapshot: Snapshot | None) -> Any:
        if query in ASYNC_QUERIES:
            # Called from plain code an async query takes the sync path; the
            # async one is taken by awaiting on a loop (``_acall``); a reader
            # thread of the concurrent rule runs a loop of its own.
            call = self._acall(query, index, snapshot)
            if threading.current_thread() is threading.main_thread():
                return self.loop.run_until_complete(call)
            return self._run_on_a_loop(call)
        handle = getattr(self.h, query)
        effects: dict[str, list[Any]] = {}
        options: dict[str, Any] = {"effects": effects}
        if snapshot is not None:
            options["snapshot"] = snapshot
        args = self._args(query, index)
        return self._outcome(effects, lambda: handle(*args, **options))

    async def _acall(self, query: str, index: int, snapshot: Snapshot | None) -> Any:
        handle = getattr(self.h, query)
        effects: dict[str, list[Any]] = {}
        options: dict[str, Any] = {"effects": effects}
        if snapshot is not None:
            options["snapshot"] = snapshot
        try:
            result = handle(*self._args(query, index), **options)
            if inspect.isawaitable(result):
                result = await result
        except (ValueError, TypeError, CycleError, EventLoopBlockedError) as exc:
            result = self._failure(exc)
            if result is None:
                raise
        return result, tuple(effects.get("notes", ()))

    def _read(
        self, query: str, index: int, world: World, snapshot: Snapshot | None = None
    ) -> None:
        actual = self._call(query, index, snapshot)
        expected = self._outcomes(world, query, index)
        assert actual in expected, (query, index, actual, expected, snapshot)

    # --- the revision, modelled (G3, G25) ------------------------------------

    @staticmethod
    def _changes(world: World, key: tuple[str, tuple[int, ...]], value: Any) -> Any:
        """Whether setting ``key`` to ``value`` is a change: True, False or None.

        None when the input was never set and gets its default: that is a
        change only if no read has kept the default as its value yet, which
        depends on which queries ran, not on the inputs.
        """
        if key in world.inputs:
            return world.inputs[key] != value
        if DEFAULTS[key[0]](*key[1]) != value:
            return True
        return None

    def _moved(self, before: int, verdicts: list[Any]) -> None:
        """Check the engine's revision against the model and follow it."""
        assert self.engine is not None
        if any(v is True for v in verdicts):
            allowed = {1}
        elif any(v is None for v in verdicts):
            allowed = {0, 1}
        else:
            allowed = {0}
        moved = self.engine.revision - before
        assert moved in allowed, (moved, allowed)
        self.revision += moved

    def _set(self, name: str, args: tuple[int, ...], value: Any) -> None:
        assert self.engine is not None
        before = self.engine.revision
        verdict = self._changes(self.world, (name, args), value)
        getattr(self.h, name).set(*args, value=value)
        self.world.inputs[(name, args)] = value
        self._moved(before, [verdict])

    # --- rules: inputs and reads ---------------------------------------------

    @rule(step=SET_INPUT)
    def set_input(self, step: tuple[Any, ...]) -> None:
        name, *args, value = step
        self._set(name, tuple(args), value)

    @rule(reads=READS)
    def read_live(self, reads: list[tuple[str, int]]) -> None:
        for query, index in reads:
            self._read(query, index, self.world)

    @rule(reads=READS, pick=PICK, through_snapshot=st.booleans())
    def read_async(
        self, reads: list[tuple[str, int]], pick: int, through_snapshot: bool
    ) -> None:
        """Every read as a task on one running loop, all at once."""
        held = None
        if through_snapshot:
            live = [held for held in self.snapshots if not held.released]
            held = live[pick % len(live)] if live else None
        world = self.world if held is None else held.world
        snapshot = None if held is None else held.snapshot

        async def gather() -> list[Any]:
            return await asyncio.gather(
                *(self._acall(query, index, snapshot) for query, index in reads)
            )

        results = self.loop.run_until_complete(gather())
        for (query, index), actual in zip(reads, results, strict=True):
            expected = self._outcomes(world, query, index, on_loop=True)
            assert actual in expected, (query, index, actual, expected, snapshot)

    def _many(
        self, reads: list[tuple[str, int]], snapshot: Snapshot | None
    ) -> list[tuple[tuple[str, int], Any]]:
        assert self.engine is not None
        calls = [(q, i) for q, i in reads if q in MANY_QUERIES]
        if not calls:
            return []
        values = self.engine.compute_many(
            [(getattr(self.h, q), self._args(q, i)) for q, i in calls],
            snapshot=snapshot,
        )
        return list(zip(calls, values, strict=True))

    @rule(reads=READS, pick=PICK, through_snapshot=st.booleans())
    def read_many(
        self, reads: list[tuple[str, int]], pick: int, through_snapshot: bool
    ) -> None:
        """``compute_many`` runs the calls on worker threads."""
        live = [held for held in self.snapshots if not held.released]
        held = live[pick % len(live)] if through_snapshot and live else None
        world = self.world if held is None else held.world
        snapshot = None if held is None else held.snapshot
        for (query, index), value in self._many(reads, snapshot):
            assert value in self._expected(world, query, index), (query, index, value)

    @rule(
        index=INDEXES,
        first=st.integers(min_value=-8, max_value=-1),
        second=st.integers(min_value=-8, max_value=-1),
    )
    def changed_error_reaches_its_catcher(
        self, index: int, first: int, second: int
    ) -> None:
        """G8: an error that changes only in its args is a change.

        ``strict`` keeps raising, with another value; ``safe``, which
        catches it, must follow.
        """
        bias = self.world.get("bias")
        assert bias is not None
        branch = "left" if self.world.get("mode") == 0 else "right"
        self._set(branch, (index,), first - bias)
        self._read("safe", index, self.world)
        self._set(branch, (index,), second - bias)
        self._read("safe", index, self.world)
        self._read("strict", index, self.world)

    # --- rules: eviction (G13) -----------------------------------------------

    def _evict(self, step: tuple[Any, ...]) -> None:
        """Drop results: what a later read returns must not change."""
        assert self.engine is not None
        if step[0] == "drop":
            self._drop(step[1], 0)
            return
        if step[0] == "sweep":
            # Everything not used in roughly the last ``step[1]`` accesses.
            self.engine.sweep_unaccessed(self.engine.access_id - step[1])
            return
        assert step[0] == "prune"
        self.engine.prune(
            [("query", query, self._args(query, index)) for query, index in step[1]]
        )

    @rule(step=EVICTION)
    def evict(self, step: tuple[Any, ...]) -> None:
        self._evict(step)

    @rule(step=EVICTION, reads=READS)
    def evict_between_reads(
        self, step: tuple[Any, ...], reads: list[tuple[str, int]]
    ) -> None:
        """G13: the same reads give the same answers after a drop."""
        for query, index in reads:
            self._read(query, index, self.world)
        self._evict(step)
        for query, index in reads:
            self._read(query, index, self.world)

    # --- rules: threads ------------------------------------------------------

    def _apply(self, step: tuple[Any, ...], world: World) -> Any:
        """Make one change to the engine and to ``world``; give its verdict.

        The verdict is whether the revision moves: True, False, or None
        when that depends on reads (``_changes``).
        """
        kind = step[0]
        if kind == "append":
            self.items.append(step[1])
            world.items.append(step[1])
            return True
        if kind == "pop":
            if not world.items:
                return False
            assert self.items.pop() == world.items.pop()
            return True
        if kind == "tag_add":
            changed = step[1] not in world.tags
            self.tags.add(step[1])
            world.tags.add(step[1])
            return changed
        if kind == "tag_discard":
            changed = step[1] in world.tags
            self.tags.discard(step[1])
            world.tags.discard(step[1])
            return changed
        name, *args, value = step
        verdict = self._changes(world, (name, tuple(args)), value)
        getattr(self.h, name).set(*args, value=value)
        world.inputs[(name, tuple(args))] = value
        return verdict

    def _transact(
        self,
        steps: list[tuple[Any, ...]],
        commit: bool,
        world: World,
        *,
        exclusive: bool = True,
    ) -> tuple[World, list[Any]]:
        """Run ``steps`` in one transaction; give the world after and verdicts.

        A ``("read", query, index)`` step reads inside the block, where
        queries see the engine as it was before the block.
        """
        assert self.engine is not None
        after = world.copy()
        verdicts: list[Any] = []
        try:
            with self.engine.transaction():
                for step in steps:
                    kind = step[0]
                    if kind == "read":
                        self._read(step[1], step[2], world)
                    elif kind in ("append", "pop", "tag_add", "tag_discard"):
                        verdicts.append(self._apply(step, after))
                    else:
                        name, *args, value = step
                        getattr(self.h, name).set(*args, value=value)
                        after.inputs[(name, tuple(args))] = value
                # Collection changes show on the objects at once. (Beside
                # other writers, only a collection this block changes is
                # known here.)
                kinds = {step[0] for step in steps}
                if exclusive or kinds & {"append", "pop"}:
                    assert list(self.items) == after.items
                if exclusive or kinds & {"tag_add", "tag_discard"}:
                    assert set(self.tags) == after.tags
                if not commit:
                    raise _Abort
        except _Abort:
            # Rolled back: nothing changed, and the revision stood still.
            return world, []
        # Committed as one revision; the last value set for an input wins.
        verdicts += [
            self._changes(world, key, value)
            for key, value in after.inputs.items()
            if key not in world.inputs or world.inputs[key] != value
        ]
        return after, verdicts

    @staticmethod
    def _owned(writer: int, writers: int) -> set[str]:
        return {kind for kind, domain in DOMAINS.items() if domain % writers == writer}

    def _plan(
        self, steps: list[tuple[Any, ...]], writer: int, writers: int
    ) -> list[tuple[Any, ...]]:
        """The steps of ``steps`` that touch what ``writer`` owns."""
        owned = self._owned(writer, writers)
        plan = []
        for step in steps:
            if step[0] != "txn":
                if step[0] in owned:
                    plan.append(step)
                continue
            inner = [s for s in step[1] if s[0] in owned]
            if inner:
                plan.append(("txn", inner, step[2]))
        return plan

    def _write(self, step: tuple[Any, ...], world: World) -> tuple[World, Any]:
        """One step of a concurrent writer: the world after it and its verdict."""
        kind = step[0]
        if kind in ("sweep", "prune"):
            self._evict(step)
            return world, False
        if kind == "txn":
            after, verdicts = self._transact(step[1], step[2], world, exclusive=False)
            if any(v is True for v in verdicts):
                return after, True
            return after, (None if any(v is None for v in verdicts) else False)
        after = world.copy()
        return after, self._apply(step, after)

    def _combine(self, base: World, parts: tuple[World, ...]) -> World:
        """The world where writer ``w`` has reached its state ``parts[w]``."""
        world = base.copy()
        for writer, part in enumerate(parts):
            owned = self._owned(writer, len(parts))
            for key, value in part.inputs.items():
                if key[0] in owned:
                    world.inputs[key] = value
            if "append" in owned:
                world.items = list(part.items)
            if "tag_add" in owned:
                world.tags = set(part.tags)
        return world

    def _submit(
        self,
        reads: list[tuple[str, int]],
        snapshot: Snapshot | None = None,
        lockstep: _Lockstep | None = None,
    ) -> list[tuple[tuple[str, int], Any]]:
        """``submit`` every read at once; a cancelled call gives ``cancelled``.

        In a lock-step rule each call runs on a helper thread that takes part
        in the lock step, and the caller is still while it waits.
        """
        assert self.engine is not None
        calls = [(q, i) for q, i in reads if q not in ASYNC_QUERIES]
        executor = None if lockstep is None else _ThreadPerTask(lockstep)
        pending = []
        for query, index in calls:
            effects: dict[str, list[Any]] = {}
            handle = getattr(self.h, query)
            revision = self.engine.revision
            future = self.engine.submit(
                handle,
                *self._args(query, index),
                snapshot=snapshot,
                effects=effects,
                executor=executor,
            )
            pending.append((effects, future, revision))
        out = []
        waiting = (
            contextlib.nullcontext() if lockstep is None else lockstep.delegating()
        )
        with waiting:
            for call, (effects, future, revision) in zip(calls, pending, strict=True):
                try:
                    outcome = self._outcome(
                        effects, lambda f=future: f.result(timeout=JOIN_TIMEOUT)
                    )
                except QueryCancelled:
                    # With the revision before the call: a change must come
                    # after it (G6).
                    outcome = ("cancelled", revision)
                out.append((call, outcome))
        return out

    def _reader(
        self,
        kind: str,
        reads: list[tuple[str, int]],
        snapshot: Snapshot | None,
        lockstep: _Lockstep | None = None,
    ) -> list[tuple[tuple[str, int], Any]]:
        if kind == "async":

            async def gather() -> list[Any]:
                return await asyncio.gather(
                    *(self._acall(q, i, snapshot) for q, i in reads)
                )

            return list(zip(reads, self._run_on_a_loop(gather()), strict=True))
        if kind == "many":
            return list(self._many(reads, snapshot))
        if kind in ("submit", "submit_snapshot"):
            return self._submit(reads, snapshot, lockstep)
        if kind == "plain":
            return [((q, i), self._plain(q, i)) for q, i in reads]
        return [((q, i), self._call(q, i, snapshot)) for q, i in reads]

    def _plain(self, query: str, index: int) -> Any:
        """A call from plain code: an async query takes the sync path (G14)."""
        effects: dict[str, list[Any]] = {}
        handle = getattr(self.h, query)
        args = self._args(query, index)
        return self._outcome(effects, lambda: handle(*args, effects=effects))

    async def _asubmit(
        self,
        query: str,
        index: int,
        snapshot: Snapshot | None,
        lockstep: _Lockstep,
    ) -> Any:
        """``submit`` from a task, which awaits the future meanwhile."""
        assert self.engine is not None
        effects: dict[str, list[Any]] = {}
        revision = self.engine.revision
        with lockstep.delegating():
            future = self.engine.submit(
                getattr(self.h, query),
                *self._args(query, index),
                snapshot=snapshot,
                effects=effects,
                executor=_ThreadPerTask(lockstep, wake_caller=True),
            )
            try:
                result = await asyncio.wrap_future(future)
            except QueryCancelled:
                return ("cancelled", revision)
            except (ValueError, TypeError, CycleError, EventLoopBlockedError) as exc:
                result = self._failure(exc)
                if result is None:
                    raise
        return result, tuple(effects.get("notes", ()))

    def _task_reader(
        self, kind: str, held: _Held | None, reads: list[tuple[str, int]]
    ) -> Any:
        """A task of a loop (``_loop_of``): each read in turn, awaited or submitted.

        The task stops before each read, so that the machine picks which of
        the callers of one query gets to run it.
        """
        snapshot = None if held is None else held.snapshot

        def task(lockstep: _Lockstep) -> Any:
            async def run() -> Any:
                out = []
                for query, index in reads:
                    await lockstep.acheckpoint()
                    if "submit" in kind:
                        outcome = await self._asubmit(query, index, snapshot, lockstep)
                    else:
                        outcome = await self._acall(query, index, snapshot)
                    out.append(((query, index), outcome))
                return (kind, held, out)

            return lambda _peers: run()

        return task

    def _loop_of(self, tasks: list[Any], slot: int = 0) -> Any:
        """A thread running a loop of its own with ``tasks``, all in the lock step.

        Each of ``tasks`` is called with the ``_Lockstep`` and gives a task's
        program (``_Lockstep.run_tasks``). Gives what each task gave, or the
        exception it ended with. Programs that run at once take loops of
        different ``slot``s.
        """
        while len(self.task_loops) <= slot:
            self.task_loops.append(_take_loop())
        loop = self.task_loops[slot]

        def program(lockstep: _Lockstep) -> list[Any]:
            return loop.run_until_complete(
                lockstep.run_tasks([task(lockstep) for task in tasks])
            )

        return program

    # --- writers that share what they change --------------------------------

    def _shared_op(self, op: tuple[Any, ...]) -> Any:
        """One change by a writer that does not know the state; what it saw."""
        kind = op[0]
        if kind == "append":
            self.items.append(op[1])
        elif kind == "pop":
            try:
                return ("popped", self.items.pop())
            except IndexError:
                return ("empty",)
        elif kind == "tag_add":
            self.tags.add(op[1])
        elif kind == "tag_discard":
            self.tags.discard(op[1])
        else:
            name, *args, value = op
            getattr(self.h, name).set(*args, value=value)
        return None

    def _shared_step(self, step: tuple[Any, ...], pause: Any = None) -> Any:
        """One step of a writer beside others that change the same things.

        Gives what ``_serial_step`` needs to know about how it went: whether
        it was refused (a collection with another transaction's uncommitted
        changes raises ``RuntimeError``), what each ``pop`` got, and for a
        transaction whether it committed, whether each operation found its
        collection changed by the transaction (only from then on can nobody
        else change it), and the list as it saw it inside. ``pause`` runs
        between the operations of a transaction, which then stays open while
        other threads go on.
        """
        kind = step[0]
        if kind in ("sweep", "prune"):
            self._evict(step)
            return ("evicted",)
        if kind != "txn":
            try:
                return ("done", self._shared_op(step))
            except RuntimeError as exc:
                assert "uncommitted changes" in str(exc), exc
                return ("refused",)
        assert self.engine is not None
        ops, commit = step[1], step[2]
        seen: list[Any] = []
        inside = None
        cores = {"items": self.items._core, "tags": self.tags._core}  # noqa: SLF001
        try:
            with self.engine.transaction() as tx:
                for position, op in enumerate(ops):
                    if position and pause is not None:
                        pause()
                    got = self._shared_op(op)
                    collection = _COLLECTION_OF.get(op[0])
                    held = collection is not None and cores[collection]._tx is tx  # noqa: SLF001
                    seen.append((got, held))
                # Once it has changed the list, nobody else can until it
                # ends: what it sees now is what it commits.
                if cores["items"]._tx is tx:  # noqa: SLF001
                    inside = list(self.items)
                if pause is not None:
                    pause()
                if not commit:
                    raise _Abort
        except _Abort:
            return ("rolled back",)
        except RuntimeError as exc:
            assert "uncommitted changes" in str(exc), exc
            return ("refused",)
        return ("committed", seen, inside)

    @staticmethod
    def _serial_op(world: World, op: tuple[Any, ...], got: Any) -> Any:
        """Apply ``op`` to ``world`` in place and give its verdict.

        ``Ellipsis`` when what the writer saw (``got``) cannot have happened
        in this state.
        """
        kind = op[0]
        if kind == "append":
            world.items.append(op[1])
            return True
        if kind == "pop":
            if got == ("empty",):
                return ... if world.items else False
            if not world.items or world.items[-1] != got[1]:
                return ...
            world.items.pop()
            return True
        if kind == "tag_add":
            changed = op[1] not in world.tags
            world.tags.add(op[1])
            return changed
        if kind == "tag_discard":
            changed = op[1] in world.tags
            world.tags.discard(op[1])
            return changed
        name, *args, value = op
        key = (name, tuple(args))
        verdict = EngineStateMachine._changes(world, key, value)
        world.inputs[key] = value
        return verdict

    def _serial_step(
        self, world: World, step: tuple[Any, ...], outcome: Any
    ) -> tuple[World, Any] | None:
        """The state after ``step`` taken alone at ``world``, and its verdict.

        None when ``outcome`` (what the writer saw) rules this state out. A
        transaction takes effect as one step when it commits: its inputs
        then, and its collection changes as they were made, since nobody
        else can change a collection it has changed until it ends. An
        operation before the transaction changed that collection changed
        nothing (a ``pop`` of an empty list, adding a tag already there):
        it only read the collection, at a time other writers could still
        change it, so it says nothing about the state at the commit.
        """
        if outcome[0] in ("evicted", "refused", "rolled back"):
            return world, False
        after = world.copy()
        if step[0] != "txn":
            verdict = self._serial_op(after, step, outcome[1])
            return None if verdict is ... else (after, verdict)
        _, seen, inside = outcome
        changed: list[Any] = []
        for op, (got, held) in zip(step[1], seen, strict=True):
            if op[0] in _COLLECTION_OF:
                if not held:
                    continue
                verdict = self._serial_op(after, op, got)
                if verdict is ...:
                    return None
                changed.append(verdict)
            else:
                name, *args, value = op
                after.inputs[(name, tuple(args))] = value
        if inside is not None and after.items != inside:
            return None
        # One revision for the whole commit; the last value set wins.
        changed += [
            self._changes(world, key, value)
            for key, value in after.inputs.items()
            if key not in world.inputs or world.inputs[key] != value
        ]
        if any(v is True for v in changed):
            return after, True
        return after, (None if any(v is None for v in changed) else False)

    @staticmethod
    def _world_key(world: World) -> Any:
        return (
            tuple(sorted(world.inputs.items(), key=lambda kv: kv[0])),
            tuple(world.items),
            tuple(sorted(world.tags)),
        )

    def _serial(
        self, base: World, logs: list[list[tuple[tuple[Any, ...], Any]]]
    ) -> tuple[list[World], dict[Any, World]]:
        """Every order of the writers' steps that agrees with what they saw.

        Each writer's steps keep their order; between writers any order.
        Gives every state some order passes through, and the states at the
        end, each with how far the revision moved to get there (keyed
        ``(state, moved)``).
        """
        frontier: dict[tuple[int, ...], dict[Any, World]] = {
            (0,) * len(logs): {(self._world_key(base), 0): base}
        }
        seen: dict[Any, World] = {self._world_key(base): base}
        for _ in range(sum(len(log) for log in logs)):
            following: dict[tuple[int, ...], dict[Any, World]] = {}
            for position, states in frontier.items():
                for writer, log in enumerate(logs):
                    if position[writer] == len(log):
                        continue
                    step, outcome = log[position[writer]]
                    nxt = list(position)
                    nxt[writer] += 1
                    bucket = following.setdefault(tuple(nxt), {})
                    for (_, moved), world in states.items():
                        taken = self._serial_step(world, step, outcome)
                        if taken is None:
                            continue
                        after, verdict = taken
                        key = self._world_key(after)
                        seen.setdefault(key, after)
                        for step_moved in _MOVES[verdict]:
                            bucket.setdefault((key, moved + step_moved), after)
            frontier = following
        (final,) = frontier.values()
        return list(seen.values()), final

    def _settle(
        self, base: World, logs: list[list[tuple[tuple[Any, ...], Any]]], before: int
    ) -> list[World]:
        """Check the end state against every serial order; adopt it.

        The writers' steps must have taken effect in some order: the inputs,
        the list and the set the engine ends with, and how far its revision
        moved, must be those of one order that agrees with what each writer
        saw. A refusal must have had a cause: another writer's transaction
        that changes the same collection. Gives every state an order passes
        through, which is what a live read may see.
        """
        assert self.engine is not None
        for writer, log in enumerate(logs):
            for step, outcome in log:
                if outcome[0] != "refused":
                    continue
                theirs = {
                    collection
                    for other, other_log in enumerate(logs)
                    if other != writer
                    for other_step, _ in other_log
                    if other_step[0] == "txn"
                    for collection in _collections_of(other_step)
                }
                assert _collections_of(step) & theirs, ("refused", step, logs)
        worlds, finals = self._serial(base, logs)
        moved = self.engine.revision - before
        keys = sorted(
            {
                (op[0], tuple(op[1:-1]))
                for log in logs
                for step, _ in log
                for op in (step[1] if step[0] == "txn" else [step])
                if op[0] not in _COLLECTION_OF and op[0] not in ("sweep", "prune")
            }
        )
        observed = (
            list(self.items),
            set(self.tags),
            [getattr(self.h, name)(*args) for name, args in keys],
        )
        matches = [
            world
            for (_, world_moved), world in finals.items()
            if world_moved == moved
            and (
                world.items,
                world.tags,
                [world.get(name, *args) for name, args in keys],
            )
            == observed
        ]
        assert matches, ("no serial order ends here", observed, moved, logs)
        self.world = matches[0]
        self.revision += moved
        return worlds

    def _check_reads(self, results: list[Any], worlds: list[World], moved: int) -> None:
        """Check each reader's results.

        A live read must match one of ``worlds``, a read through a snapshot
        the snapshot's state; a submitted call may be cancelled only if the
        revision moved (G6).
        """
        assert self.engine is not None
        for kind, held, reads in results:
            candidates = worlds if held is None else [held.world]
            on_loop = kind.startswith("task") or kind == "async"
            for (query, index), actual in reads:
                if kind == "many":  # compute_many: the value alone
                    allowed = {
                        v for w in candidates for v in self._expected(w, query, index)
                    }
                    assert actual in allowed, (query, index, actual, allowed)
                    continue
                if actual[0] == "cancelled":
                    # Only a submitted call, and only after a change.
                    assert "submit" in kind, (query, kind)
                    assert moved > 0 and self.engine.revision > actual[1], (
                        query,
                        kind,
                        actual,
                    )
                    continue
                allowed = {
                    o
                    for w in candidates
                    for o in self._outcomes(w, query, index, on_loop)
                }
                assert actual in allowed, (query, index, actual, allowed, held)

    @rule(
        readers=st.lists(READER, min_size=1, max_size=4),
        writers=st.lists(st.lists(WRITER_STEP, max_size=4), min_size=1, max_size=3),
        pick=PICK,
        shared=st.booleans(),
    )
    def concurrent(
        self,
        readers: list[tuple[str, list[tuple[str, int]]]],
        writers: list[list[tuple[Any, ...]]],
        pick: int,
        shared: bool,
    ) -> None:
        """Readers on threads of their own beside up to three writers.

        Without ``shared`` each writer changes only what it owns
        (``DOMAINS``), so the state at any moment is some combination of the
        states the writers passed through, and a live read runs at one
        revision: it must equal the plain function of one of those
        combinations, never of half a transaction. With ``shared`` the
        writers change the same inputs and collections (a few steps each),
        and the end state and every live read must agree with some order
        of their steps (``_settle``). A read through a snapshot gives the
        snapshot's state exactly. A submitted call may be cancelled, only if
        the revision moved (G6). The engine's own loops are not shared: an
        async reader runs a loop of its own.
        """
        assert self.engine is not None
        before = self.engine.revision
        base = self.world.copy()
        if shared:
            plans = [steps[:SHARED_PLAN] for steps in writers]
        else:
            plans = [
                self._plan(steps, w, len(writers)) for w, steps in enumerate(writers)
            ]
        histories: list[list[World]] = [[base] for _ in plans]
        logs: list[list[Any]] = [[] for _ in plans]
        verdicts: list[Any] = []
        live = [held for held in self.snapshots if not held.released]
        start = threading.Barrier(len(readers) + len(plans))
        results: list[Any] = [None] * len(readers)
        failures: list[BaseException] = []

        def run_reader(slot: int, kind: str, reads: list[tuple[str, int]]) -> None:
            try:
                held = None
                if kind in ("snapshot", "submit_snapshot") and live:
                    held = live[(pick + slot) % len(live)]
                start.wait(timeout=JOIN_TIMEOUT)
                snapshot = None if held is None else held.snapshot
                results[slot] = (kind, held, self._reader(kind, reads, snapshot))
            except BaseException as exc:  # noqa: BLE001  (re-raised below)
                failures.append(exc)

        def run_writer(writer: int) -> None:
            try:
                start.wait(timeout=JOIN_TIMEOUT)
                for step in plans[writer]:
                    if shared:
                        logs[writer].append((step, self._shared_step(step)))
                        continue
                    world, verdict = self._write(step, histories[writer][-1])
                    histories[writer].append(world)
                    verdicts.append(verdict)
            except BaseException as exc:  # noqa: BLE001  (re-raised below)
                failures.append(exc)

        threads = [
            threading.Thread(target=run_reader, args=(slot, kind, reads), daemon=True)
            for slot, (kind, reads) in enumerate(readers)
        ]
        threads += [
            threading.Thread(target=run_writer, args=(w,), daemon=True)
            for w in range(len(plans))
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=JOIN_TIMEOUT)
        assert not any(t.is_alive() for t in threads), "a thread hung"
        if failures:
            raise failures[0]

        moved = self.engine.revision - before
        if shared:
            worlds = self._settle(base, logs, before)
        else:
            worlds = [
                self._combine(base, parts) for parts in itertools.product(*histories)
            ]
            # Each writer's steps follow one another, and every step that
            # changed something moved the revision by one.
            must = sum(1 for v in verdicts if v is True)
            may = sum(1 for v in verdicts if v is None)
            assert must <= moved <= must + may, (moved, verdicts)
            self.revision += moved
            self.world = self._combine(base, tuple(h[-1] for h in histories))
        self._check_reads(results, worlds, moved)
        # Nothing the race left behind may be served from here on.
        for _, reads in readers:
            for query, index in reads:
                self._read(query, index, self.world)

    # --- rules: interleavings the machine chooses ----------------------------

    def _drop(self, query: str, index: int) -> None:
        """Drop one cached result, and nothing else (``prune`` keeps roots)."""
        assert self.engine is not None
        key = self.engine._key_root(("query", query, self._args(query, index)))  # noqa: SLF001
        store = self.engine._internals._store  # noqa: SLF001
        with store.lock:
            if key in store.memos:
                store.drop_memo_locked(key)

    def _lockstep(self, programs: list[Any], schedule: list[int]) -> list[Any]:
        """Run each program on a thread of its own, in lock step.

        A program is called with the ``_Lockstep``. ``compute_many`` runs
        its calls on helper threads that take part too, while its caller
        counts as still. Fails if a thread had to wait ``LOCKSTEP_PATIENCE``
        for the others: the scheduler did not see where one was blocked.
        """
        assert self.engine is not None
        lockstep = _Lockstep(self.engine, schedule)
        ready = threading.Barrier(len(programs))
        results: list[Any] = [None] * len(programs)
        failures: list[BaseException] = []
        executor = scheduler_module.WorkStealingExecutor
        submit_indexed, run_tasks = executor.submit_indexed, executor.run
        evaluator = self.engine._evaluator  # noqa: SLF001
        in_executor = evaluator._run_sync_in_executor  # noqa: SLF001

        async def run_sync_delegating(fn: Any, args: tuple[Any, ...]) -> Any:
            # A sync body that an async verification runs in the engine's
            # executor: on a helper thread that takes part.
            with lockstep.delegating():
                return await in_executor(fn, args)

        def submit_helper(pool: Any, index: int, fn: Any) -> None:
            submit_indexed(pool, index, lockstep.helper(fn))

        def run_delegating(pool: Any, size: int) -> list[Any]:
            with lockstep.delegating():
                return run_tasks(pool, size)

        def run(slot: int, program: Any) -> None:
            lockstep.join()
            try:
                ready.wait(timeout=JOIN_TIMEOUT)
                results[slot] = program(lockstep)
            except BaseException as exc:  # noqa: BLE001  (re-raised below)
                failures.append(exc)
            finally:
                lockstep.leave()

        threads = [
            threading.Thread(target=run, args=(slot, program), daemon=True)
            for slot, program in enumerate(programs)
        ]
        self.lockstep = lockstep
        try:
            with (
                mock.patch.object(executor, "submit_indexed", submit_helper),
                mock.patch.object(executor, "run", run_delegating),
                mock.patch.object(
                    evaluator, "_run_sync_in_executor", run_sync_delegating
                ),
                mock.patch.object(
                    evaluator,
                    "_get_executor",
                    lambda: _ThreadPerTask(lockstep, wake_caller=True),
                ),
            ):
                for thread in threads:
                    thread.start()
                deadlock = lockstep.watch(threads)
        finally:
            self.lockstep = None
        assert deadlock is None, f"deadlocked:\n{deadlock}"
        if any(t.is_alive() for t in threads):
            with lockstep.cond:
                hung = lockstep._describe()  # noqa: SLF001
            raise AssertionError(f"a thread hung:\n{hung}")
        if failures:
            raise failures[0]
        assert not lockstep.timeouts, "\n\n".join(lockstep.timeouts)
        return results

    def _lockstep_reader(
        self,
        kind: str,
        held: _Held | None,
        reads: list[tuple[str, int]],
        *,
        pause: bool = False,
    ) -> Any:
        """A reader thread; with ``pause`` it stops before each read."""
        snapshot = None if held is None else held.snapshot

        def program(lockstep: _Lockstep) -> Any:
            if not pause:
                return (kind, held, self._reader(kind, reads, snapshot, lockstep))
            out = []
            for read in reads:
                lockstep.checkpoint()
                out += self._reader(kind, [read], snapshot, lockstep)
            return (kind, held, out)

        return program

    def _lockstep_readers(
        self, readers: list[tuple[str, list[tuple[str, int]]]], pick: int
    ) -> list[Any]:
        live = [held for held in self.snapshots if not held.released]
        programs = []
        for slot, (kind, reads) in enumerate(readers):
            held = None
            if kind in ("snapshot", "submit_snapshot") and live:
                held = live[(pick + slot) % len(live)]
            programs.append(self._lockstep_reader(kind, held, reads))
        return programs

    def _evictor(self, steps: list[tuple[Any, ...]]) -> Any:
        """A thread that drops results, stopping before each drop."""

        def program(lockstep: _Lockstep) -> None:
            for step in steps:
                lockstep.checkpoint()
                self._evict(step)

        return program

    @rule(
        readers=st.lists(LOCKSTEP_READER, min_size=2, max_size=3),
        drops=st.lists(st.tuples(st.sampled_from(SYNC_QUERIES), INDEXES), max_size=3),
        evictions=st.lists(IN_RUN_EVICTION, max_size=3),
        schedule=SCHEDULE,
        pick=PICK,
    )
    def lockstep_readers(
        self,
        readers: list[tuple[str, list[tuple[str, int]]]],
        drops: list[tuple[str, int]],
        evictions: list[tuple[Any, ...]],
        schedule: list[int],
        pick: int,
    ) -> None:
        """Readers whose interleaving the machine picks, after drops.

        Each read must give the plain value at its revision: the head, or
        the snapshot's (G1, G4, G5). A reader may ``submit`` (live or through
        a snapshot) or use ``compute_many``, whose helper threads take part.
        With ``evictions`` another thread drops results between the readers'
        steps, so a drop can land while a query runs: a hidden cycle's head
        then finds a member gone that it was about to read (0.4.2).
        """
        assert self.engine is not None
        before = self.engine.revision
        for query, index in drops:
            self._drop(query, index)
        programs = self._lockstep_readers(readers, pick)
        if evictions:
            programs.append(self._evictor(evictions))
        results = self._lockstep(programs, schedule)
        assert self.engine.revision == before
        self._check_reads(results[: len(readers)], [self.world], 0)

    @rule(
        on_items=st.booleans(),
        mine=SHARED_VALUE,
        theirs=SHARED_STEP,
        commit=st.booleans(),
    )
    def open_transaction_holds_its_collection(
        self, on_items: bool, mine: int, theirs: tuple[Any, ...], commit: bool
    ) -> None:
        """G25: a collection an open transaction has changed is its own.

        Another thread's change to it, alone or in a transaction of its own,
        is refused until the block ends; one to anything else goes through.
        Then the block commits or rolls back, and the state must be the
        other thread's step followed by the block's change, or the other
        step alone.
        """
        assert self.engine is not None
        before = self.engine.revision
        base = self.world.copy()
        if on_items:
            op: tuple[Any, ...] = ("append", mine)
        else:
            op = ("tag_discard" if mine in base.tags else "tag_add", mine)
        changed = threading.Event()
        outcome: list[Any] = []

        def other() -> None:
            assert changed.wait(JOIN_TIMEOUT)
            outcome.append(self._shared_step(theirs))

        thread = threading.Thread(target=other, daemon=True)
        thread.start()
        inside = None
        try:
            with self.engine.transaction():
                self._shared_op(op)
                changed.set()
                thread.join(JOIN_TIMEOUT)
                if on_items:
                    inside = list(self.items)
                if not commit:
                    raise _Abort
        except _Abort:
            pass
        finally:
            changed.set()
            thread.join(JOIN_TIMEOUT)
        assert not thread.is_alive(), "a thread hung"
        refused = outcome[0] == ("refused",)
        assert refused == bool(_collections_of(theirs) & _collections_of(op)), (
            theirs,
            outcome,
        )
        block = ("txn", [op], commit)
        done = ("committed", [(None, True)], inside) if commit else ("rolled back",)
        self._settle(base, [[(theirs, outcome[0])], [(block, done)]], before)

    @rule(
        writers=st.lists(
            st.lists(SHARED_STEP, min_size=1, max_size=3), min_size=2, max_size=3
        ),
        readers=st.lists(LOCKSTEP_READER, max_size=2),
        loops=st.lists(LOOP, max_size=1),
        schedule=SCHEDULE,
        pick=PICK,
    )
    def shared_writers(
        self,
        writers: list[list[tuple[Any, ...]]],
        readers: list[tuple[str, list[tuple[str, int]]]],
        loops: list[list[tuple[str, list[tuple[str, int]]]]],
        schedule: list[int],
        pick: int,
    ) -> None:
        """Writers that change the same inputs and collections, in lock step.

        Each writer stops before every step and between the operations of a
        transaction, so the machine picks which writer's step comes next,
        and a transaction stays open while the others go on: a change to a
        collection it has changed must be refused. The inputs, the list and
        the set the engine ends with, and how far its revision moved, must
        be those of some serial order of the steps that agrees with what
        each writer saw (``_settle``); a live read of a reader running among
        them must match a state one such order passes through. Readers may be
        tasks on a loop of their own (``loops``), whose awaited and submitted
        reads are held to the same: never half a transaction, and
        ``QueryCancelled`` only for a ``submit`` that a change came after.
        """
        assert self.engine is not None
        before = self.engine.revision
        base = self.world.copy()

        def writer(steps: list[tuple[Any, ...]]) -> Any:
            def program(lockstep: _Lockstep) -> list[Any]:
                log = []
                for step in steps:
                    lockstep.checkpoint()
                    log.append((step, self._shared_step(step, lockstep.checkpoint)))
                return log

            return program

        programs = [writer(steps) for steps in writers]
        programs += self._lockstep_readers(readers, pick)
        slot = itertools.count()
        programs += [
            self._loop_of(
                [
                    self._task_reader(
                        kind, self._held_for(kind, next(slot), pick), reads
                    )
                    for kind, reads in tasks
                ],
                number,
            )
            for number, tasks in enumerate(loops)
        ]
        results = self._lockstep(programs, schedule)
        moved = self.engine.revision - before
        worlds = self._settle(base, results[: len(writers)], before)
        last = len(writers) + len(readers)
        reads = results[len(writers) : last] + self._flatten(results[last:])
        self._check_reads(reads, worlds, moved)
        for query, index in [
            *(read for _, reads in readers for read in reads),
            ("choose", 0),
            ("choose", 1),
            ("total", 0),
            ("tagged", 0),
            ("spread", 0),
        ]:
            self._read(query, index, self.world)

    @rule(
        bias=SMALL_INTS,
        callers=st.lists(
            st.sampled_from(("loop_member", "loop_head")), min_size=1, max_size=2
        ),
        schedule=SCHEDULE,
    )
    def dropped_cycle_member_read_by_threads(
        self, bias: int, callers: list[str], schedule: list[int]
    ) -> None:
        """D35: threads ask for a fixed-point member whose result was dropped.

        The head has a result, so any member can be called (G7), and the
        cycle converges; no caller may get ``CycleError``.
        """
        self._read("loop_head", 0, self.world)
        self._read("loop_member", 0, self.world)
        self._drop("loop_member", 0)
        self._set("bias", (), bias)
        # The first caller asks for the member, as in D35; the others for
        # either.
        callers = ["loop_member", *callers]
        results = self._lockstep(
            [self._lockstep_reader("live", None, [(c, 0)]) for c in callers],
            schedule,
        )
        for caller, (_, _, [(_, actual)]) in zip(callers, results, strict=True):
            expected = self._outcomes(self.world, caller, 0)
            assert actual in expected, (caller, actual, expected)
        self._read("loop_head", 0, self.world)
        self._read("loop_member", 0, self.world)

    @rule(value=SMALL_INTS, first=st.booleans(), schedule=SCHEDULE)
    def pipeline_stage_meets_a_run(
        self, value: int, first: bool, schedule: list[int]
    ) -> None:
        """A pipeline's stage calls a query another thread is running.

        ``spread`` holds the pipelines' lock while its stage calls
        ``shifted``, and ``shifted`` runs a pipeline of its own. With the
        other thread's run of ``shifted`` stopped at its start, the stage
        must not wait for it: that run needs the lock the stage holds.
        """
        self.append_item(value)
        shifted = ("shifted", value % 3)
        for query, index in (("spread", 0), shifted):
            self._drop(query, index)
        readers = [[("spread", 0)], [shifted]]
        if first:
            readers.reverse()
        programs = [self._lockstep_reader("live", None, reads) for reads in readers]
        results = self._lockstep(programs, schedule)
        self._check_reads(results, [self.world], 0)

    @rule(
        query=st.sampled_from(SYNC_QUERIES),
        index=INDEXES,
        through_snapshot=st.booleans(),
        pick=PICK,
    )
    def submitted_run_is_cancelled_by_a_change(
        self, query: str, index: int, through_snapshot: bool, pick: int
    ) -> None:
        """G6: an input changed while a submitted run is held cancels it.

        The run is held at the start of the query's body until the change
        is made. Nothing it computed may be served afterwards. A call
        submitted through a snapshot is cancelled the same way.
        """
        assert self.engine is not None
        held = self._pick_snapshot(pick) if through_snapshot else None
        world = self.world if held is None else held.world
        self._drop(query, index)
        reached = threading.Event()
        release = threading.Event()
        main = threading.get_ident()

        def hold(name: str) -> None:
            if name == query and threading.get_ident() != main and not reached.is_set():
                reached.set()
                assert release.wait(JOIN_TIMEOUT)

        self.hold = hold
        try:
            effects: dict[str, list[Any]] = {}
            future = self.engine.submit(
                getattr(self.h, query),
                *self._args(query, index),
                snapshot=None if held is None else held.snapshot,
                effects=effects,
            )
            deadline = time.monotonic() + JOIN_TIMEOUT
            while not reached.is_set() and not future.done():
                assert time.monotonic() < deadline, "the submitted run never started"
                time.sleep(0.001)
            if reached.is_set():
                bias = self.world.get("bias")
                assert bias is not None
                self._set("bias", (), bias + 1 if bias < 8 else bias - 1)
        finally:
            release.set()
            self.hold = None
        if reached.is_set():
            with pytest.raises(QueryCancelled):
                future.result(timeout=JOIN_TIMEOUT)
        else:
            # Found without running (cached, or loaded from disk).
            outcome = self._outcome(effects, lambda: future.result(JOIN_TIMEOUT))
            assert outcome in self._outcomes(world, query, index)
        self._read(query, index, self.world)

    def _pick_snapshot(self, pick: int) -> _Held:
        """A snapshot still held, or a new one."""
        live = [held for held in self.snapshots if not held.released]
        return live[pick % len(live)] if live else self._take_snapshot()

    @rule(
        reads=st.lists(
            st.tuples(st.sampled_from(SYNC_QUERIES), INDEXES), min_size=1, max_size=3
        ),
        change=st.one_of(st.none(), SET_INPUT, COLLECTION_STEP),
        pick=PICK,
    )
    def submit_through_snapshot(
        self, reads: list[tuple[str, int]], change: Any, pick: int
    ) -> None:
        """``submit`` with a held snapshot gives the snapshot's value (G4).

        After an optional change, so that the head and the snapshot differ;
        with nothing changing while the calls run, none is cancelled.
        """
        assert self.engine is not None
        held = self._pick_snapshot(pick)
        if change is not None:
            before = self.engine.revision
            verdict = self._apply(change, self.world)
            self._moved(before, [verdict])
        for (query, index), actual in self._submit(reads, held.snapshot):
            expected = self._outcomes(held.world, query, index)
            assert actual in expected, (query, index, actual, expected)

    # --- rules: tasks in the lock step (G5, G6, G7, G12, G14) ----------------

    def _held_for(self, kind: str, slot: int, pick: int) -> _Held | None:
        live = [held for held in self.snapshots if not held.released]
        if "snapshot" in kind and live:
            return live[(pick + slot) % len(live)]
        return None

    @staticmethod
    def _flatten(loops: list[Any]) -> list[Any]:
        """What the tasks of each loop gave; an exception one ended with is raised."""
        out = []
        for results in loops:
            for result in results:
                if isinstance(result, BaseException):
                    raise result
                out.append(result)
        return out

    def _runs_beyond(self, before: Counter[Any], limit: int) -> dict[Any, int]:
        """Keys of ``ONCE`` that ran more than ``limit`` times at one revision."""
        return {
            key: n - before[key]
            for key, n in self.key_runs.items()
            if key[0] in ONCE and n - before[key] > limit
        }

    @rule(
        loops=st.lists(LOOP, min_size=1, max_size=2),
        threads=st.lists(THREAD, max_size=2),
        drops=st.lists(st.tuples(st.sampled_from(DROPPABLE), INDEXES), max_size=3),
        evictions=st.lists(IN_RUN_EVICTION, max_size=2),
        schedule=SCHEDULE,
        pick=PICK,
        turn=TURN_OF_2,
    )
    def lockstep_tasks(
        self,
        loops: list[list[tuple[str, list[tuple[str, int]]]]],
        threads: list[tuple[str, list[tuple[str, int]]]],
        drops: list[tuple[str, int]],
        evictions: list[tuple[Any, ...]],
        schedule: list[int],
        pick: int,
        turn: int,
    ) -> None:
        """Tasks on one or two loops, threads beside them, in lock step.

        Several tasks on a loop, each on its own loop thread, read every kind
        of query: async ones awaited, sync ones called from the task, a sync
        query that calls an async one (``bridge``, which on a loop may raise
        ``EventLoopBlockedError``, G14), fixed points sync and async with the
        hidden cycles, through snapshots and through ``submit`` awaited by
        the task. Threads read beside them, also on the sync path of an
        async query, and an evictor may drop results between their steps.
        The machine picks the interleaving at each body's start and at the
        awaits marked in bodies, so a task meets the runs of other tasks and
        threads, for the same keys (G5), at the points it picks. Every read
        must give the plain value and effects of its revision; with nothing
        dropped while they run and only live reads, no async query runs
        twice for one key (G5).
        """
        if turn:
            return  # not its turn (``TURN_OF_2``)
        assert self.engine is not None
        before = self.engine.revision
        for query, index in drops:
            self._drop(query, index)
        slot = itertools.count()
        programs = [
            self._loop_of(
                [
                    self._task_reader(
                        kind, self._held_for(kind, next(slot), pick), reads
                    )
                    for kind, reads in tasks
                ],
                number,
            )
            for number, tasks in enumerate(loops)
        ]
        programs += [
            self._lockstep_reader(
                kind, self._held_for(kind, next(slot), pick), reads, pause=True
            )
            for kind, reads in threads
        ]
        if evictions:
            programs.append(self._evictor(evictions))
        runs = Counter(self.key_runs)
        results = self._lockstep(programs, schedule)
        assert self.engine.revision == before
        reads = self._flatten(results[: len(loops)])
        reads += results[len(loops) : len(loops) + len(threads)]
        self._check_reads(reads, [self.world], 0)
        if (
            self.max_entries == 10_000
            and not evictions
            and all(held is None for _, held, _ in reads)
        ):
            assert not self._runs_beyond(runs, 1)

    def _waits_for(self, task: asyncio.Task[Any], key: Any) -> bool:
        """Whether ``task`` waits for the run of ``key`` (at any revision)."""
        assert self.engine is not None
        store = self.engine._internals._store  # noqa: SLF001
        with store.lock:
            slot = store.waiting_on.get(task)
            return slot is not None and any(
                s is slot and k[0] == key for k, s in store.in_flight.items()
            )

    @rule(
        query=st.sampled_from(CANCELLED_QUERIES),
        index=INDEXES,
        tasks=st.integers(min_value=2, max_value=3),
        thread=st.booleans(),
        victim=st.integers(min_value=0, max_value=2),
        delay=st.integers(min_value=0, max_value=4),
        schedule=SCHEDULE,
        turn=TURN_OF_2,
    )
    def cancelled_task_leaves_the_others_alone(
        self,
        query: str,
        index: int,
        tasks: int,
        thread: bool,
        victim: int,
        delay: int,
        schedule: list[int],
        turn: int,
    ) -> None:
        """G5, G6, D34: of tasks asking for one query, one is cancelled.

        Tasks on one loop, and maybe a thread, ask for the same query, whose
        result (and what it reads) was dropped, so one of them runs it and
        the others wait for that run. Another task stops ``delay`` times and
        then cancels one of them: before it started, while it runs the
        query, while it waits for another's run, or once it is done. That
        one gives ``CancelledError`` or its value; every other caller gives
        the plain value and effects. A cancelled waiter leaves the run alone
        (one run per key, G5); a cancelled owner's run is taken over by the
        others, not handed to them as their own cancellation. Afterwards no
        wait stays on record and the value is cached.
        """
        if turn:
            return  # not its turn (``TURN_OF_2``)
        assert self.engine is not None
        key = self.engine._key_root(("query", query, self._args(query, index)))  # noqa: SLF001
        for name in {query, "achoose", "choose"}:
            self._drop(name, index)
        target = victim % tasks
        seen: dict[str, Any] = {}

        def caller(_lockstep: _Lockstep) -> Any:
            return lambda _peers: self._acall(query, index, None)

        def canceller(lockstep: _Lockstep) -> Any:
            async def run(peers: list[asyncio.Task[Any]]) -> None:
                for _ in range(delay):
                    await lockstep.acheckpoint()
                task = peers[target]
                seen["waiting"] = self._waits_for(task, key)
                if task.done():
                    seen["state"] = "done"
                else:
                    seen["state"] = inspect.getcoroutinestate(task.get_coro())
                task.cancel()

            return run

        programs = [self._loop_of([caller] * tasks + [canceller])]
        if thread:
            programs.append(
                self._lockstep_reader("live", None, [(query, index)], pause=True)
            )
        runs = Counter(self.key_runs)
        results = self._lockstep(programs, schedule)
        expected = self._outcomes(self.world, query, index, on_loop=True)
        for slot, outcome in enumerate(results[0][:tasks]):
            if slot == target and isinstance(outcome, asyncio.CancelledError):
                continue
            if isinstance(outcome, BaseException):
                raise outcome
            assert outcome in expected, (slot, query, index, outcome, expected, seen)
        if thread:
            _, _, [(_, outcome)] = results[1]
            assert outcome in self._outcomes(self.world, query, index), (outcome, seen)
        if self.max_entries == 10_000:
            # Only a cancelled owner leaves its run to be done again.
            alone = seen["waiting"] or seen["state"] in ("done", inspect.CORO_CREATED)
            beyond = self._runs_beyond(runs, 1 if alone else 2)
            assert not beyond, (beyond, seen)
        self._read(query, index, self.world)

    @rule(
        query=st.sampled_from((*ONCE, "choose", "bridge")),
        index=INDEXES,
        bias=SMALL_INTS,
        thread=st.booleans(),
        schedule=SCHEDULE,
        turn=TURN_OF_3,
    )
    def tasks_at_two_revisions_ask_for_one_query(
        self,
        query: str,
        index: int,
        bias: int,
        thread: bool,
        schedule: list[int],
        turn: int,
    ) -> None:
        """G4, G5: a task through a snapshot and a live task ask for one query.

        The query's result was dropped and ``bias``, which it reads, changed
        since the snapshot, so the two run it at different revisions, at
        once; a live thread may ask too. None may take the result of a run
        at the other revision: the task through the snapshot gets the
        snapshot's value, the others the head's.
        """
        if turn:
            return  # not its turn (``TURN_OF_2``)
        assert self.engine is not None
        held = self._take_snapshot()
        before = held.world.get("bias")
        assert before is not None
        self._set("bias", (), bias if bias != before else bias + 1)
        for name in {query, "achoose", "choose"}:
            self._drop(name, index)
        tasks = [
            self._task_reader("task_snapshot", held, [(query, index)]),
            self._task_reader("task", None, [(query, index)]),
        ]
        programs = [self._loop_of(tasks)]
        if thread:
            programs.append(
                self._lockstep_reader("live", None, [(query, index)], pause=True)
            )
        results = self._lockstep(programs, schedule)
        reads = self._flatten(results[:1]) + results[1:]
        self._check_reads(reads, [self.world], 0)

    @rule(
        bias=SMALL_INTS,
        callers=st.lists(
            st.tuples(st.booleans(), st.sampled_from(("aloop_member", "aloop_head"))),
            min_size=1,
            max_size=2,
        ),
        schedule=SCHEDULE,
        turn=TURN_OF_3,
    )
    def dropped_async_cycle_member_read_by_tasks(
        self, bias: int, callers: list[tuple[bool, str]], schedule: list[int], turn: int
    ) -> None:
        """D35 with tasks: an async fixed-point member whose result was dropped.

        Tasks on one loop and threads ask for the member or the head, which
        has a result, so any member can be called (G7); the cycle converges,
        and no caller may get ``CycleError``.
        """
        if turn:
            return  # not its turn (``TURN_OF_2``)
        self._read("aloop_head", 0, self.world)
        self._read("aloop_member", 0, self.world)
        self._drop("aloop_member", 0)
        self._set("bias", (), bias)
        callers = [(True, "aloop_member"), *callers]
        tasks = [self._task_reader("task", None, [(c, 0)]) for on, c in callers if on]
        programs = [self._loop_of(tasks)]
        programs += [
            self._lockstep_reader("live", None, [(c, 0)], pause=True)
            for on, c in callers
            if not on
        ]
        results = self._lockstep(programs, schedule)
        reads = self._flatten(results[:1]) + results[1:]
        self._check_reads(reads, [self.world], 0)
        self._read("aloop_head", 0, self.world)
        self._read("aloop_member", 0, self.world)

    @rule(
        index=INDEXES,
        value=MAYBE_INT,
        turn=TURN_OF_3,
    )
    def inactive_branch_write_does_not_rerun_async(
        self, index: int, value: int | None, turn: int
    ) -> None:
        """G2 on the async path: a write ``choose`` did not read re-runs nothing.

        ``anoted`` awaits ``achoose``, which calls ``choose``; verified on the
        async path, none of the three may run again. Each is read first,
        after one such write, so that each has a result in memory checked
        against the others at a revision of its own: a result loaded from
        disk may have left its dependencies there, and a read through an
        old snapshot replaces a row, which costs a run at the next revision
        (G4). A small LRU bound may still drop one, which then runs again
        (G13).
        """
        if turn:
            return  # not its turn (``TURN_OF_2``)
        names = ("choose", "achoose", "anoted")
        inactive = "right" if self.world.get("mode") == 0 else "left"
        self._set(inactive, (index,), value)
        for name in names:
            self._read(name, index, self.world)
        runs = [self.runs[name] for name in names]
        self._set(inactive, (index,), 8 if value is None else None)
        self._read("anoted", index, self.world)
        if self.max_entries == 10_000:
            assert [self.runs[name] for name in names] == runs

    # --- rules: hidden cycles (0.4.2) ----------------------------------------

    @rule(
        drift=st.integers(min_value=-3, max_value=3),
        detour=st.booleans(),
        fail=st.integers(min_value=0, max_value=3).map(lambda n: n == 0),
        opaque=st.integers(min_value=0, max_value=3).map(lambda n: n == 0),
    )
    def change_hidden(
        self, drift: int, detour: bool, fail: bool, opaque: bool = False
    ) -> None:
        """Change what the hidden cycles read, which the engine does not see."""
        self.hidden.drift = drift
        self.hidden.detour = detour
        self.hidden.fail = fail
        self.hidden.opaque = opaque

    @rule(
        head=st.sampled_from(W_HEADS),
        drift=st.integers(min_value=-3, max_value=3),
        detour=st.booleans(),
        fail=st.booleans(),
        opaque=st.booleans(),
    )
    def hidden_change_falls_back(
        self, head: str, drift: int, detour: bool, fail: bool, opaque: bool
    ) -> None:
        """0.4.2: one run on the cached members disagrees; iterate instead.

        With the head's row in place, a change the engine cannot see makes
        the run on the cached members give another value, reach the head
        again through ``detour``, raise, or give a value that cannot be
        encoded; each time the head must iterate from the guess and give the
        plain value (or raise ``UnencodableValueError``, stored nowhere).
        And back.
        """
        self._read(head, 0, self.world)
        hidden = self.hidden
        before = (hidden.drift, hidden.detour, hidden.fail, hidden.opaque)
        self.change_hidden(drift, detour, fail, opaque)
        self._read(head, 0, self.world)
        self.change_hidden(*before)
        self._read(head, 0, self.world)

    @rule(
        head=st.sampled_from(W_HEADS),
        steps=st.lists(IN_RUN_EVICTION, min_size=1, max_size=3),
        reads=SYNC_READS,
    )
    def evict_while_a_head_runs(
        self, head: str, steps: list[tuple[Any, ...]], reads: list[tuple[str, int]]
    ) -> None:
        """0.4.2: results are dropped while a hidden cycle's head runs.

        The first read leaves a row to run on; in the second, each of the
        head's first runs drops something as it starts (a member, the head's
        own row, or what ``prune`` or ``sweep_unaccessed`` drop). A run on
        the cached members whose member is gone reaches the head again and
        must start over from the guess; every value is the plain one.
        """
        self._read(head, 0, self.world)
        pending = list(steps)
        main = threading.get_ident()

        def inside(name: str) -> None:
            if name == head and pending and threading.get_ident() == main:
                self._evict(pending.pop(0))

        self.inside = inside
        try:
            self._read(head, 0, self.world)
        finally:
            self.inside = None
        for query, index in reads:
            self._read(query, index, self.world)
        self._read(head, 0, self.world)

    @rule(
        head=st.sampled_from(W_HEADS),
        value=SMALL_INTS,
        on_run=st.integers(min_value=2, max_value=3),
    )
    def reading_dropped_while_a_head_iterates(
        self, head: str, value: int, on_run: int
    ) -> None:
        """G7, G13: the TTL result a cycle reads is dropped while it iterates.

        The cycle starts afresh while the old reading is still served; on
        the head's ``on_run``-th run the reading's result is dropped, so it
        runs again and gives the new value. The head must give the fixed
        point of one reading, iterated from the guess: going on from the
        guesses computed with the old one can stop at a fixed point no run
        from the guess reaches.
        """
        prefix = head.removesuffix("_head")
        for query in (head, f"{prefix}_member", f"{prefix}_detour"):
            self._drop(query, 0)
        self.change_sensor(value)
        runs = [0]
        main = threading.get_ident()

        def inside(name: str) -> None:
            if name == head and threading.get_ident() == main:
                runs[0] += 1
                if runs[0] == on_run:
                    self._drop("sensor", 0)

        self.inside = inside
        try:
            self._read(head, 0, self.world)
        finally:
            self.inside = None

    @rule(head=st.sampled_from(W_HEADS), value=SMALL_INTS)
    def expired_cycle_row_is_not_reused(self, head: str, value: int) -> None:
        """0.4.2: a head whose row has expired iterates from the guess.

        The hidden cycle reads the TTL query ``sensor``; once its TTL has
        elapsed only the new reading is right.
        """
        self._read(head, 0, self.world)
        self._read(head, 0, self.world)
        self.change_sensor(value)
        self.clock.now += TTL + 1.0
        assert self._sensor_values() == {value}
        self._read(head, 0, self.world)

    @rule(
        head=st.sampled_from(W_HEADS),
        value=SMALL_INTS,
        expire=st.booleans(),
        member_first=st.booleans(),
    )
    def cycle_follows_a_new_reading(
        self, head: str, value: int, expire: bool, member_first: bool
    ) -> None:
        """A hidden cycle's members follow the TTL reading its head took.

        The members are computed afresh. Then the reading changes and either
        its TTL elapses or its result is dropped (within the TTL it may then
        come back with the new value). A member must expire with its head:
        read on its own it must give half the new fixed point (unless the
        head raises: entered through a member, that cycle may raise
        ``CycleError``, G7). And a head
        whose result is dropped must not leave its members behind: its next
        run would find them cached, never reach itself and add the new
        reading to their old values, one step of the iteration instead of a
        fixed point.
        """
        prefix = head.removesuffix("_head")
        member = f"{prefix}_detour" if self.hidden.detour else f"{prefix}_member"
        for query in (head, f"{prefix}_member", f"{prefix}_detour"):
            self._drop(query, 0)
        self._read(head, 0, self.world)
        self.change_sensor(value)
        if not expire:
            self._drop("sensor", 0)
            self._drop(head, 0)
        elif not member_first or self.hidden.fail or self.hidden.opaque:
            self.clock.now += TTL + 1.0
            self._drop(head, 0)
        else:
            # The head's row stays: the member alone must see its expiry.
            self.clock.now += TTL + 1.0
            actual = self._call(member, 0, None)
            expected = {(v // 2, ()) for v, _ in self._outcomes(self.world, head, 0)}
            assert actual in expected, (member, actual, expected)
        self._read(head, 0, self.world)

    @rule(index=INDEXES, value=MAYBE_INT)
    def inactive_branch_write_does_not_rerun_choose(
        self, index: int, value: int | None
    ) -> None:
        """G2: a write to the branch ``choose`` did not read re-runs nothing."""
        self._read("choose", index, self.world)
        runs = self.runs["choose"]
        inactive = "right" if self.world.get("mode") == 0 else "left"
        self._set(inactive, (index,), value)
        self._read("choose", index, self.world)
        assert self.runs["choose"] == runs

    # --- rules: snapshots (G4, G7, G24) --------------------------------------

    def _take_snapshot(self) -> _Held:
        assert self.engine is not None
        if len(self.snapshots) >= MAX_SNAPSHOTS:
            self.snapshots.pop(0).snapshot.release()
        snapshot = self.engine.snapshot()
        assert snapshot.revision == self.revision
        held = _Held(snapshot, self.world.copy())
        self.snapshots.append(held)
        return held

    def _read_through(self, held: _Held, query: str, index: int) -> None:
        if not held.released:
            self._read(query, index, held.world, held.snapshot)
            return
        # A released snapshot may have lost its history, never its accuracy.
        try:
            self._read(query, index, held.world, held.snapshot)
        except SnapshotExpiredError:
            pass

    @rule()
    def take_snapshot(self) -> None:
        self._take_snapshot()

    @precondition(lambda self: self.snapshots)
    @rule(pick=PICK, reads=READS)
    def read_snapshot(self, pick: int, reads: list[tuple[str, int]]) -> None:
        held = self.snapshots[pick % len(self.snapshots)]
        for query, index in reads:
            self._read_through(held, query, index)

    @precondition(lambda self: any(not held.released for held in self.snapshots))
    @rule(pick=PICK)
    def release_snapshot(self, pick: int) -> None:
        live = [held for held in self.snapshots if not held.released]
        held = live[pick % len(live)]
        held.snapshot.release()
        held.released = True

    # --- rules: TTL (G9) -----------------------------------------------------

    @rule(seconds=st.sampled_from((1.0, 6.0, 11.0, 30.0)))
    def advance_clock(self, seconds: float) -> None:
        self.clock.now += seconds

    @rule(value=SMALL_INTS)
    def change_sensor(self, value: int) -> None:
        if value != self.sensor_value:
            self.sensor_history.append((self.clock.now, self.sensor_value))
            self.sensor_value = value

    @rule(index=INDEXES, value=SMALL_INTS)
    def expired_ttl_reaches_dependents(self, index: int, value: int) -> None:
        """D3: a query above a TTL query follows it once the TTL has elapsed."""
        self._read("reading", index, self.world)
        self.change_sensor(value)
        self.clock.now += TTL + 1.0
        # No older reading is within the TTL now: only ``value`` is right.
        assert self._sensor_values() == {value}
        self._read("reading", index, self.world)

    # --- rules: re-registered code (G11) -------------------------------------

    @rule(variant=VARIANT)
    def reregister_scaled(self, variant: int) -> None:
        old = self.h.scaled
        replaced = variant != self.variant
        self._register_scaled(variant)
        if replaced:
            with pytest.raises(StaleHandleError):
                old(0)
        else:
            assert old(0) in self._expected(self.world, "scaled", 0)

    @rule(variant=VARIANT, index=INDEXES, pick=PICK)
    def reregistered_code_reaches_dependents(
        self, variant: int, index: int, pick: int
    ) -> None:
        """D22: new code reaches what the old code's results went into.

        With no input change in between, at the newest revision and
        through a snapshot.
        """
        held = self.snapshots[pick % len(self.snapshots)] if self.snapshots else None
        self._read("pair", index, self.world)
        if held is not None:
            self._read_through(held, "pair", index)
        self.reregister_scaled(variant)
        self._read("pair", index, self.world)
        if held is not None:
            self._read_through(held, "pair", index)

    # --- rules: the fixed point (G7) -----------------------------------------

    @rule(node=NODE, pick=PICK, successor=MAYBE_NODE)
    def fixed_point_follows_every_member(
        self, node: int, pick: int, successor: int | None
    ) -> None:
        """A cycle follows an input that only another member of it reads."""
        self._read("reach", node, self.world)
        members = plain_reach(self.world, node)
        self._set("edge", (members[pick % len(members)],), successor)
        self._read("reach", node, self.world)

    @rule(node=NODE, changed=NODE, successor=MAYBE_NODE)
    def fixed_point_through_a_snapshot(
        self, node: int, changed: int, successor: int | None
    ) -> None:
        """D2: read through a snapshot, a cycle is iterated on its inputs."""
        held = self._take_snapshot()
        self._set("edge", (changed,), successor)
        self._read_through(held, "reach", node)
        self._read("reach", node, self.world)
        self._read_through(held, "reach", node)

    # --- rules: the collection, transactions, compaction (G24, G25) ----------

    @rule(value=SMALL_INTS)
    def append_item(self, value: int) -> None:
        assert self.engine is not None
        before = self.engine.revision
        self.items.append(value)
        self.world.items.append(value)
        self._moved(before, [True])

    # No precondition on the list: after writers that share it, its
    # contents depend on how their threads interleaved, and Hypothesis needs
    # the same draws to choose the same rules.
    @rule(pick=PICK, value=SMALL_INTS)
    def replace_item(self, pick: int, value: int) -> None:
        assert self.engine is not None
        if not self.world.items:
            return
        before = self.engine.revision
        position = pick % len(self.world.items)
        self.items[position] = value
        self.world.items[position] = value
        self._moved(before, [True])

    @rule()
    def pop_item(self) -> None:
        assert self.engine is not None
        before = self.engine.revision
        verdict = self._apply(("pop",), self.world)
        self._moved(before, [verdict])

    @rule(value=SMALL_INTS, add=st.booleans())
    def change_tags(self, value: int, add: bool) -> None:
        assert self.engine is not None
        before = self.engine.revision
        verdict = self._apply(("tag_add" if add else "tag_discard", value), self.world)
        self._moved(before, [verdict])

    @rule(steps=st.lists(TRANSACTION_STEP, max_size=6), commit=st.booleans())
    def transaction(self, steps: list[tuple[Any, ...]], commit: bool) -> None:
        assert self.engine is not None
        before = self.engine.revision
        self.world, verdicts = self._transact(steps, commit, self.world)
        self._moved(before, verdicts)

    @rule()
    def compact(self) -> None:
        self.items.compact()  # the log on disk, when there is one
        self.items._core.compact_memory()  # noqa: SLF001  (the log in memory)

    @rule(values=st.lists(SMALL_INTS, min_size=4, max_size=8))
    def compaction_keeps_what_snapshots_need(self, values: list[int]) -> None:
        """D12: folding old diffs leaves every held snapshot its contents."""
        held = self._take_snapshot()
        for value in values:
            self.append_item(value)
        self.compact()
        for query in ("total", "above_bias"):
            self._read_through(held, query, 0)
            self._read(query, 0, self.world)
        for older in self.snapshots:
            self._read_through(older, "total", 0)

    # --- rules: a new engine on the same directory (G15, G16, G26) -----------

    @precondition(lambda self: self.backend is not None)
    @rule(reads=READS)
    def reopen(self, reads: list[tuple[str, int]]) -> None:
        """G16: what the old engine stored is used only where it still holds."""
        for query, index in reads:
            self._read(query, index, self.world)
        self._close_engine()
        self._build_engine()
        # Inputs are not persisted, so the new engine starts from their
        # defaults; the named collection is restored with its contents.
        self.world = World(items=self.world.items, tags=self.world.tags)
        for query, index in reads:
            self._read(query, index, self.world)

    # --- invariants ----------------------------------------------------------

    @invariant()
    def revision_matches_model(self) -> None:
        assert self.engine is not None
        assert self.engine.revision == self.revision

    @invariant()
    def collection_matches_model(self) -> None:
        assert list(self.items) == self.world.items
        assert set(self.tags) == self.world.tags

    @invariant()
    def internal_dependent_index_remains_consistent(self) -> None:
        assert self.engine is not None
        assert_internal_dependents_consistent(self.engine)

    @invariant()
    def in_flight_map_is_empty_after_operations(self) -> None:
        assert self.engine is not None
        assert self.engine._internals.in_flight == {}  # noqa: SLF001

    @invariant()
    def no_wait_stays_on_record(self) -> None:
        """Every thread and task that waited for a run has stopped waiting."""
        assert self.engine is not None
        assert self.engine._internals._store.waiting_on == {}  # noqa: SLF001


class MdbxEngineStateMachine(EngineStateMachine):
    backend = "mdbx"


class SqliteEngineStateMachine(EngineStateMachine):
    backend = "sqlite"


# At this size each machine caught, in one run, every fault it was tried
# against: thirteen, seeded into the engine by hand, at least one per group
# of rules. At 40 examples of 30 steps one of them got past two of the three
# machines. A step is cheap, so this is a few seconds per machine.
# ``CASCADE_STATE_EXAMPLES`` raises the count for long runs (flakiness hunts on
# the free-threaded build, seeded faults); ``CASCADE_STATE_SHRINK=0`` reports
# a failure without shrinking it, which with threads can take many minutes.
_SETTINGS = settings(
    max_examples=int(os.environ.get("CASCADE_STATE_EXAMPLES", "100")),
    stateful_step_count=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
    phases=[
        phase
        for phase in Phase
        if phase != Phase.shrink or os.environ.get("CASCADE_STATE_SHRINK") != "0"
    ],
)

TestEngineStateMachine = EngineStateMachine.TestCase
TestEngineStateMachine.settings = _SETTINGS
TestEngineStateMachineMdbx = MdbxEngineStateMachine.TestCase
TestEngineStateMachineMdbx.settings = _SETTINGS
TestEngineStateMachineSqlite = SqliteEngineStateMachine.TestCase
TestEngineStateMachineSqlite.settings = _SETTINGS
