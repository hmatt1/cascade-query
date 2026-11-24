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
"""

from __future__ import annotations

import shutil
import tempfile
from collections import Counter
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest
from hypothesis import HealthCheck, settings, strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
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
)

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
        self._patches = [
            mock.patch.object(evaluator_module, "time", self.clock),
            # Small enough for the machine's handful of changes to reach
            # the folding of old diffs (G24).
            mock.patch.object(collections_module, "_MEMORY_COMPACT_THRESHOLD", 4),
        ]
        for patch in self._patches:
            patch.start()
        try:
            self._build_engine()
        except BaseException:
            self.teardown()
            raise

    # --- the engine under test -----------------------------------------------

    def _build_engine(self) -> None:
        if self.cache_dir is None:
            engine = Engine()
        else:
            assert self.backend is not None
            engine = Engine(cache_dir=self.cache_dir, cache_backend=self.backend)
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

        h.mode, h.left, h.right, h.bias, h.edge = mode, left, right, bias, edge
        h.choose, h.pair, h.sensor, h.reading = choose, pair, sensor, reading
        h.reach, h.total, h.above_bias = reach, total, above_bias
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
        assert query == "above_bias"
        return {plain_above_bias(world)}

    def _call(self, query: str, index: int, snapshot: Snapshot | None) -> Any:
        handle = getattr(self.h, query)
        options = {} if snapshot is None else {"snapshot": snapshot}
        if query in ("sensor", "total", "above_bias"):
            return handle(**options)
        if query == "reach":
            return handle(index % NODES, **options)
        return handle(index, **options)

    def _read(
        self, query: str, index: int, world: World, snapshot: Snapshot | None = None
    ) -> None:
        actual = self._call(query, index, snapshot)
        expected = self._expected(world, query, index)
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
