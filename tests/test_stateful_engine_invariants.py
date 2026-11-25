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
  in it may not shrink or repeat.
"""

from __future__ import annotations

import asyncio
import inspect
import shutil
import tempfile
import threading
from collections import Counter
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest
from hypothesis import HealthCheck, settings, strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)

import cascade._collections as collections_module
import cascade._evaluator as evaluator_module
from cascade import (
    CascadeList,
    Engine,
    Snapshot,
    SnapshotExpiredError,
    StaleHandleError,
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
)
# Queries without arguments, and those defined with ``async def``.
NO_ARGS = ("sensor", "total", "above_bias")
ASYNC_QUERIES = ("achoose", "amix")
# compute_many returns values only; a raising call would end the batch.
MANY_QUERIES = tuple(q for q in QUERIES if q not in ("strict", *ASYNC_QUERIES))
# How long a thread of the concurrent rule may take before it counts as hung.
JOIN_TIMEOUT = 60.0

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
TRANSACTION_STEP = st.one_of(
    SET_INPUT,
    st.tuples(st.just("append"), SMALL_INTS),
    st.tuples(st.just("pop")),
    st.tuples(st.just("read"), st.sampled_from(QUERIES), INDEXES),
)
EVICTION = st.one_of(
    st.tuples(st.just("sweep"), st.integers(min_value=0, max_value=30)),
    st.tuples(
        st.just("prune"),
        st.lists(st.tuples(st.sampled_from(QUERIES), INDEXES), max_size=3),
    ),
)
# What the writer of the concurrent rule does while readers read.
WRITER_STEP = st.one_of(
    SET_INPUT,
    st.tuples(st.just("append"), SMALL_INTS),
    st.tuples(st.just("pop")),
    EVICTION,
)
READER = st.tuples(st.sampled_from(("live", "snapshot", "async", "many")), READS)

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
    """What a snapshot freezes: input values and the collection's contents."""

    inputs: dict[tuple[str, tuple[int, ...]], int | None] = field(default_factory=dict)
    items: list[int] = field(default_factory=list)

    def get(self, name: str, *args: int) -> int | None:
        key = (name, args)
        return self.inputs[key] if key in self.inputs else DEFAULTS[name](*args)

    def copy(self) -> World:
        return World(dict(self.inputs), list(self.items))


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


class EngineStateMachine(RuleBasedStateMachine):
    backend: str | None = None

    def __init__(self) -> None:
        super().__init__()
        self.world = World()
        self.revision = 0
        self.variant = 0
        self.snapshots: list[_Held] = []
        self.runs: Counter[str] = Counter()
        self.clock = _Clock()
        self.sensor_value = 0
        # (the time a value stopped being current, the value)
        self.sensor_history: list[tuple[float, int]] = []
        self.cache_dir = None if self.backend is None else tempfile.mkdtemp()
        self.engine: Engine | None = None
        self.max_entries = 10_000
        self.loop = asyncio.new_event_loop()
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

        @engine.query(name="choose")
        def choose(index: int) -> int:
            self.runs["choose"] += 1
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
            return h.scaled(index) + h.scaled(index + 1)

        @engine.query(name="sensor", ttl=TTL)
        def sensor() -> int:
            return self.sensor_value

        @engine.query(name="reading")
        def reading(index: int) -> int:
            return choose(index) + sensor()

        @engine.query(name="reach", fixed_point=())
        def reach(node: int) -> tuple[int, ...]:
            successor = edge(node)
            beyond = () if successor is None else reach(successor)
            return tuple(sorted({node, *beyond}))

        items = CascadeList(engine, name="items", compact_every=3)

        @engine.query(name="total")
        def total() -> int:
            return sum(x for x in items)

        @engine.query(name="above_bias")
        def above_bias() -> int:
            return len([x for x in items if x > bias()])

        notes = engine.accumulator("notes")

        @engine.query(name="strict")
        def strict(index: int) -> int:
            value = choose(index)
            if value < 0:
                raise ValueError(value)
            return value

        @engine.query(name="safe")
        def safe(index: int) -> int:
            try:
                return strict(index)
            except ValueError as exc:
                return 100 + exc.args[0]

        @engine.query(name="noted")
        def noted(index: int) -> int:
            value = choose(index)
            if value < 0:
                notes.push((index, value))
            return value

        @engine.query(name="noted_pair")
        def noted_pair(index: int) -> int:
            return noted(index) + noted(index + 1)

        @engine.query(name="achoose")
        async def achoose(index: int) -> int:
            await asyncio.sleep(0)
            return choose(index)

        @engine.query(name="amix")
        async def amix(index: int) -> int:
            first = await achoose(index)
            return first - noted(index + 1)

        h.mode, h.left, h.right, h.bias, h.edge = mode, left, right, bias, edge
        h.choose, h.pair, h.sensor, h.reading = choose, pair, sensor, reading
        h.reach, h.total, h.above_bias = reach, total, above_bias
        h.strict, h.safe, h.noted, h.noted_pair = strict, safe, noted, noted_pair
        h.achoose, h.amix = achoose, amix
        self.engine = engine
        self.h = h
        self.items = items
        self.variants = (scaled_once, scaled_twice, scaled_negated)
        self._register_scaled(self.variant)
        # Opening the named collection publishes its head, which is a
        # revision of its own; from here on the revision is modelled.
        self.revision = engine.revision

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
            self.loop.close()
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
        return {value for value, _ in self._outcomes(world, query, index)}

    def _outcomes(self, world: World, query: str, index: int) -> set[Any]:
        """Each correct ``(outcome, notes)`` of a call.

        The outcome is the value, or ``("raise", value)`` for the
        ``ValueError`` of ``strict``; ``notes`` is what the call collects in
        ``effects["notes"]``, in order (G12).
        """
        if query in ("strict", "safe", "achoose"):
            value = plain_choose(world, index)
            if query == "strict" and value < 0:
                return {(("raise", value), ())}
            if query == "safe" and value < 0:
                return {(100 + value, ())}
            return {(value, ())}
        if query == "noted":
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
        return {(value, ()) for value in self._expected(world, query, index)}

    def _args(self, query: str, index: int) -> tuple[int, ...]:
        if query in NO_ARGS:
            return ()
        if query == "reach":
            return (index % NODES,)
        return (index,)

    @staticmethod
    def _outcome(effects: dict[str, list[Any]], call: Any) -> Any:
        """Run ``call``; give its outcome and the notes it collected."""
        try:
            value = call()
        except ValueError as exc:
            value = ("raise", *exc.args)
        return value, tuple(effects.get("notes", ()))

    def _call(self, query: str, index: int, snapshot: Snapshot | None) -> Any:
        if query in ASYNC_QUERIES:
            # Called from plain code an async query takes the sync path; the
            # async one is taken by awaiting on a loop (``_acall``); a reader
            # thread of the concurrent rule runs a loop of its own.
            call = self._acall(query, index, snapshot)
            if threading.current_thread() is threading.main_thread():
                return self.loop.run_until_complete(call)
            return asyncio.run(call)
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
        except ValueError as exc:
            result = ("raise", *exc.args)
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
            expected = self._outcomes(world, query, index)
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

    def _write(self, step: tuple[Any, ...], worlds: list[World]) -> Any:
        """One step of the concurrent writer; give its revision verdict."""
        assert self.engine is not None
        world = worlds[-1].copy()
        kind = step[0]
        if kind in ("sweep", "prune"):
            self._evict(step)
            return False
        if kind == "append":
            self.items.append(step[1])
            world.items.append(step[1])
            verdict: Any = True
        elif kind == "pop":
            if not world.items:
                return False
            assert self.items.pop() == world.items.pop()
            verdict = True
        else:
            name, *args, value = step
            verdict = self._changes(world, (name, tuple(args)), value)
            getattr(self.h, name).set(*args, value=value)
            world.inputs[(name, tuple(args))] = value
        worlds.append(world)
        return verdict

    def _reader(
        self, kind: str, reads: list[tuple[str, int]], snapshot: Snapshot | None
    ) -> list[tuple[tuple[str, int], Any]]:
        if kind == "async":

            async def gather() -> list[Any]:
                return await asyncio.gather(
                    *(self._acall(q, i, snapshot) for q, i in reads)
                )

            return list(zip(reads, asyncio.run(gather()), strict=True))
        if kind == "many":
            return [(call, (value,)) for call, value in self._many(reads, snapshot)]
        return [((q, i), self._call(q, i, snapshot)) for q, i in reads]

    @rule(
        readers=st.lists(READER, min_size=1, max_size=4),
        writes=st.lists(WRITER_STEP, max_size=5),
        pick=PICK,
    )
    def concurrent(
        self,
        readers: list[tuple[str, list[tuple[str, int]]]],
        writes: list[tuple[Any, ...]],
        pick: int,
    ) -> None:
        """Readers on threads of their own beside one writer.

        A live read runs at one revision, so it must equal the plain
        function of one of the states the writer passed through; a read
        through a snapshot gives the snapshot's state exactly. The engine's
        own loops are not shared: an async reader runs a loop of its own.
        """
        assert self.engine is not None
        before = self.engine.revision
        worlds = [self.world.copy()]
        verdicts: list[Any] = []
        live = [held for held in self.snapshots if not held.released]
        start = threading.Barrier(len(readers) + 1)
        results: list[Any] = [None] * len(readers)
        failures: list[BaseException] = []

        def run_reader(slot: int, kind: str, reads: list[tuple[str, int]]) -> None:
            try:
                held = None
                if kind == "snapshot" and live:
                    held = live[(pick + slot) % len(live)]
                start.wait(timeout=JOIN_TIMEOUT)
                snapshot = None if held is None else held.snapshot
                results[slot] = (held, self._reader(kind, reads, snapshot))
            except BaseException as exc:  # noqa: BLE001  (re-raised below)
                failures.append(exc)

        def run_writer() -> None:
            try:
                start.wait(timeout=JOIN_TIMEOUT)
                for step in writes:
                    verdicts.append(self._write(step, worlds))
            except BaseException as exc:  # noqa: BLE001  (re-raised below)
                failures.append(exc)

        threads = [
            threading.Thread(target=run_reader, args=(slot, kind, reads), daemon=True)
            for slot, (kind, reads) in enumerate(readers)
        ]
        threads.append(threading.Thread(target=run_writer, daemon=True))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=JOIN_TIMEOUT)
        assert not any(t.is_alive() for t in threads), "a thread hung"
        if failures:
            raise failures[0]

        for held, reads in results:
            for (query, index), actual in reads:
                if held is not None:
                    candidates = [held.world]
                else:
                    candidates = worlds
                if len(actual) == 1:  # compute_many: the value alone
                    allowed = {
                        v for w in candidates for v in self._expected(w, query, index)
                    }
                    assert actual[0] in allowed, (query, index, actual, allowed)
                else:
                    allowed = {
                        o for w in candidates for o in self._outcomes(w, query, index)
                    }
                    assert actual in allowed, (query, index, actual, allowed, held)

        # The writer's steps follow one another, so the revision moved once
        # for each that changed something.
        moved = self.engine.revision - before
        must = sum(1 for v in verdicts if v is True)
        may = sum(1 for v in verdicts if v is None)
        assert must <= moved <= must + may, (moved, verdicts)
        self.revision += moved
        self.world = worlds[-1]
        # Nothing the race left behind may be served from here on.
        for _, reads in readers:
            for query, index in reads:
                self._read(query, index, self.world)

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

    @precondition(lambda self: self.world.items)
    @rule(pick=PICK, value=SMALL_INTS)
    def replace_item(self, pick: int, value: int) -> None:
        assert self.engine is not None
        before = self.engine.revision
        position = pick % len(self.world.items)
        self.items[position] = value
        self.world.items[position] = value
        self._moved(before, [True])

    @precondition(lambda self: self.world.items)
    @rule()
    def pop_item(self) -> None:
        assert self.engine is not None
        before = self.engine.revision
        assert self.items.pop() == self.world.items.pop()
        self._moved(before, [True])

    @rule(steps=st.lists(TRANSACTION_STEP, max_size=6), commit=st.booleans())
    def transaction(self, steps: list[tuple[Any, ...]], commit: bool) -> None:
        assert self.engine is not None
        before = self.engine.revision
        world = self.world
        after = world.copy()
        verdicts: list[Any] = []
        try:
            with self.engine.transaction():
                for step in steps:
                    kind = step[0]
                    if kind == "read":
                        # Queries in the block see the engine as it was
                        # before the block.
                        self._read(step[1], step[2], world)
                    elif kind == "append":
                        self.items.append(step[1])
                        after.items.append(step[1])
                        verdicts.append(True)
                    elif kind == "pop":
                        if after.items:
                            assert self.items.pop() == after.items.pop()
                            verdicts.append(True)
                    else:
                        name, *args, value = step
                        getattr(self.h, name).set(*args, value=value)
                        after.inputs[(name, tuple(args))] = value
                # Collection changes show on the object at once.
                assert list(self.items) == after.items
                if not commit:
                    raise _Abort
        except _Abort:
            # Rolled back: nothing changed, and the revision stood still.
            self._moved(before, [])
            return
        # Committed as one revision; the last value set for an input wins.
        verdicts += [
            self._changes(world, key, value)
            for key, value in after.inputs.items()
            if key not in world.inputs or world.inputs[key] != value
        ]
        self.world = after
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
        self.world = World(items=self.world.items)
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

    @invariant()
    def internal_dependent_index_remains_consistent(self) -> None:
        assert self.engine is not None
        assert_internal_dependents_consistent(self.engine)

    @invariant()
    def in_flight_map_is_empty_after_operations(self) -> None:
        assert self.engine is not None
        assert self.engine._internals.in_flight == {}  # noqa: SLF001


class MdbxEngineStateMachine(EngineStateMachine):
    backend = "mdbx"


class SqliteEngineStateMachine(EngineStateMachine):
    backend = "sqlite"


# At this size each machine caught, in one run, every fault it was tried
# against: thirteen, seeded into the engine by hand, at least one per group
# of rules. At 40 examples of 30 steps one of them got past two of the three
# machines. A step is cheap, so this is a few seconds per machine.
_SETTINGS = settings(
    max_examples=100,
    stateful_step_count=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

TestEngineStateMachine = EngineStateMachine.TestCase
TestEngineStateMachine.settings = _SETTINGS
TestEngineStateMachineMdbx = MdbxEngineStateMachine.TestCase
TestEngineStateMachineMdbx.settings = _SETTINGS
TestEngineStateMachineSqlite = SqliteEngineStateMachine.TestCase
TestEngineStateMachineSqlite.settings = _SETTINGS
