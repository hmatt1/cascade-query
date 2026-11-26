"""Generated transaction programs, checked against plain Python (G25, G6, G26).

The hand-written transaction tests pin one scenario each. Here Hypothesis
writes the programs, and a model made of plain values says what every read
must return:

1. **One engine.** A program is a list of segments: blocks that commit, raise,
   or fail at commit on a value that cannot be encoded, and plain changes
   between them. A block's steps set inputs, change a list, a set and a dict
   (and a list created in the block), read queries, inputs and ``submit``
   futures, take snapshots, compact, and try to nest a transaction. Inside a
   block the collection objects show the block's changes, while queries,
   input reads and futures see the state before it, the revision stays put
   and nothing reaches disk. After it, the engine holds all of the block or
   none of it: values, revision, the list's element ids and log heads, and
   disk. At the end, every query equals a new engine's built from the model,
   and with a ``cache_dir`` an engine reopened on the directory.
2. **Two engines on one ``cache_dir``**, each on its own thread, on both disk
   backends. Their steps interleave: blocks open on either or both at once,
   changes to two named lists (each engine may try either list; the
   per-collection write lock and the out-of-date check decide), reads, and
   reopening one engine. The model tracks what each engine loaded, which
   engine holds each list's lock, and what is on disk.
3. **``submit`` while blocks are open, committing and rolling back.** Reader
   threads call, submit and snapshot queries while the main thread runs a
   program. The model keeps every committed state in order; each result must
   be a query's value in one of the states committed while the call ran,
   never a rolled-back or partial one. A submitted call is pinned at its
   submit (G6), so a future's value must be the state current then, or
   ``QueryCancelled`` when a change came after it. Futures held at a gate
   until later steps have exact outcomes.
4. **Blocks on asyncio tasks**, on one event loop or two in two threads.
   Actor tasks open, commit, roll back and fail blocks, set inputs, change a
   list, a set and a dict, read, ``submit`` and snapshot queries, try to
   nest a block, spawn tasks (which copy the block's context and are still
   outside it) and are cancelled, in a block and in the middle of a query
   they await there. Reader tasks await async queries whose bodies stop
   between their reads until the test lets them go on, so blocks open, end
   and are cancelled while a read is half done, and two or three readers
   ask for one key at once (G5). The test decides every step, so a program
   runs the same way each time. Every read must be a state committed while
   it ran (an owner's read in its block: the state before the block), a
   snapshot read the snapshot's state; every change refused for another
   task's block is predicted; the revision and the collection objects match
   the model after every step; and two runs of one key at one revision
   never overlap.

``CASCADE_TXN_EXAMPLES`` sets the number of examples of every test here (the
defaults keep the file to a few seconds a test); ``CASCADE_TXN_SHRINK=0``
reports a failure without shrinking it.
"""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import contextvars
import copy
import itertools
import os
import queue
import random
import tempfile
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import pytest
from hypothesis import HealthCheck, Phase, event, given, settings
from hypothesis import strategies as st

from cascade import (
    CascadeDict,
    CascadeList,
    CascadeSet,
    Engine,
    PersistentCacheError,
    QueryCancelled,
    UnencodableValueError,
)
from cascade._collections import DiffLog

TIMEOUT = 30.0
BACKENDS = ("mdbx", "sqlite")


def _settings(default: int) -> settings:
    return settings(
        max_examples=int(os.environ.get("CASCADE_TXN_EXAMPLES", default)),
        deadline=None,
        suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
        phases=[
            phase
            for phase in Phase
            if phase != Phase.shrink or os.environ.get("CASCADE_TXN_SHRINK") != "0"
        ],
    )


class Abort(Exception):
    """Raised at the end of a block that is to roll back."""


class Unencodable:
    """An input value the commit cannot encode (D27)."""

    def __repr__(self) -> str:
        return "Unencodable()"


BAD = Unencodable()


@contextmanager
def _cache_dir(backend: str | None) -> Iterator[str | None]:
    if backend is None:
        yield None
        return
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as path:
        yield path


def _disk_contents(engine: Engine, name: str, kind: str) -> Any:
    """What a new engine would load for collection ``name``, or None."""
    snapshot, tail = engine._disk.collection_load(name)  # noqa: SLF001
    if snapshot is None and not tail:
        return None
    if snapshot is not None:
        log = DiffLog(kind, base_rev=snapshot["base_rev"], base_items=snapshot["items"])
    else:  # pragma: no cover - a stored log always has its snapshot
        log = DiffLog(kind)
    for _rev, diff in tail:
        log.entries.append(diff)
    raw = log.contents_at(log.head)
    if kind == "list":
        return [value for _uid, value in raw]
    if kind == "set":
        return set(raw)
    return dict(raw)


# =============================================================================
# 1. One engine
# =============================================================================


@dataclass
class World:
    """Committed state of the one-engine rig, in plain values."""

    x: Any = 0
    y: Any = 0
    w: list[Any] = field(default_factory=lambda: [0, 0, 0])
    xs: list[int] = field(default_factory=lambda: [1, 2, 3])
    ss: set[int] = field(default_factory=lambda: {1, 4})
    dd: dict[int, int] = field(default_factory=lambda: {0: 5})
    zs: list[int] | None = None  # created by a step, or not yet

    def copy(self) -> World:
        return copy.deepcopy(self)


QUERIES = ("total", "evens", "looped", "tagged", "table", "pick", "everything")


def _expect(world: World, name: str, arg: int = 0) -> Any:
    """What query ``name`` returns over ``world``, in plain Python."""
    xs = world.xs
    if name == "total":
        return sum(xs) + world.x
    if name == "evens":
        return [v for v in xs if v % 2 == 0]
    if name == "looped":
        return list(xs), (xs[0] if xs else None), len(xs)
    if name == "tagged":
        return sorted(v for v in world.ss if v > world.y)
    if name == "table":
        return {k: v + world.w[k % 3] for k, v in world.dd.items()}
    if name == "pick":
        return xs[arg] if arg < len(xs) else None
    if name == "late":
        zs = world.zs or []
        return sum(zs), list(zs)
    if name == "cold":
        return (
            arg,
            list(xs),
            sorted(world.ss),
            dict(world.dd),
            (world.x, world.y, *world.w),
            sum(v * 2 for v in xs),
            len(xs),
        )
    assert name == "everything"
    return (
        _expect(world, "total"),
        _expect(world, "evens"),
        _expect(world, "tagged"),
        _expect(world, "table"),
        _expect(world, "pick", 0),
        world.w[0],
        world.y,
    )


class Rig:
    """An engine with the model's inputs, collections and queries."""

    def __init__(self, engine: Engine, world: World, *, named: bool) -> None:
        self.engine = engine
        self.named = named

        @engine.input
        def x() -> int:
            return 0

        @engine.input
        def y() -> int:
            return 0

        @engine.input
        def w(k: int) -> int:
            return 0

        # compact_every is small so commits also compact the log on disk.
        def name(n: str) -> str | None:
            return n if named else None

        xs = CascadeList(engine, world.xs, name=name("xs"), compact_every=4)
        ss = CascadeSet(engine, world.ss, name=name("ss"), compact_every=4)
        dd = CascadeDict(engine, world.dd, name=name("dd"), compact_every=4)

        @engine.query
        def total() -> int:
            return sum(v for v in xs) + x()

        @engine.query
        def evens() -> list[int]:
            return [v for v in xs if v % 2 == 0]

        @engine.query
        def looped() -> Any:
            out = []
            for v in xs:
                out.append(v)
            return out, (xs[0] if xs else None), len(xs)

        @engine.query
        def tagged() -> list[int]:
            floor = y()
            return sorted(v for v in ss if v > floor)

        @engine.query
        def table() -> dict[int, int]:
            out = {}
            for k, v in dd.items():
                out[k] = v + w(k % 3)
            return out

        @engine.query
        def pick(i: int) -> Any:
            return xs[i] if i < len(xs) else None

        @engine.query
        def everything() -> Any:
            return (total(), evens(), tagged(), table(), pick(0), w(0), y())

        @engine.query
        def cold(salt: int) -> Any:
            # Called with a new salt each time, so never cached: its body
            # runs where it is read (in a block, on a pool thread) and reads
            # the collections there, plainly and through a pipeline.
            out = []
            for v in xs:
                out.append(v)
            return (
                salt,
                out,
                sorted(ss),
                dict(dd),
                (x(), y(), w(0), w(1), w(2)),
                sum(v * 2 for v in xs),
                len(xs),
            )

        self.x, self.y, self.w = x, y, w
        self.xs, self.ss, self.dd = xs, ss, dd
        self.zs: CascadeList | None = None
        self.queries: dict[str, Any] = {
            "total": total,
            "evens": evens,
            "looped": looped,
            "tagged": tagged,
            "table": table,
            "pick": pick,
            "everything": everything,
            "cold": cold,
        }
        if world.zs is not None:
            self.create_late(world.zs)

    def create_late(self, seed: list[int]) -> None:
        zs = CascadeList(
            self.engine, seed, name="zs" if self.named else None, compact_every=4
        )

        @self.engine.query
        def late() -> Any:
            return sum(z for z in zs), list(zs)

        self.zs = zs
        self.queries["late"] = late

    def set_inputs(self, world: World) -> None:
        self.x.set(world.x)
        self.y.set(world.y)
        for k in range(3):
            self.w.set(k, world.w[k])

    def input_value(self, which: str) -> Any:
        if which == "x":
            return self.x()
        if which == "y":
            return self.y()
        return self.w(int(which[1:]))

    def call(self, name: str, arg: int, **kwargs: Any) -> Any:
        query = self.queries[name]
        if name in ("pick", "cold"):
            return query(arg, **kwargs)
        return query(**kwargs)

    def submit(self, name: str, arg: int) -> concurrent.futures.Future[Any]:
        query = self.queries[name]
        if name in ("pick", "cold"):
            return self.engine.submit(query, arg)
        return self.engine.submit(query)

    def contents(self) -> World:
        """The collection objects as plain values (inputs left at zero)."""
        return World(
            xs=list(self.xs),
            ss=set(self.ss),
            dd=dict(self.dd),
            zs=None if self.zs is None else list(self.zs),
        )

    def heads(self) -> dict[str, int]:
        heads = {c: getattr(self, c)._core.log.head for c in ("xs", "ss", "dd")}  # noqa: SLF001
        if self.zs is not None:
            heads["zs"] = self.zs._core.log.head  # noqa: SLF001
        return heads


VALUES = st.integers(min_value=-2, max_value=9)
INDEX = st.integers(min_value=0, max_value=4)
KEYS = st.integers(min_value=0, max_value=3)
ARG = st.integers(min_value=0, max_value=2)

LIST_STEP = st.one_of(
    st.tuples(st.just("append"), VALUES),
    st.tuples(st.just("extend"), st.lists(VALUES, max_size=2)),
    st.tuples(st.just("insert"), INDEX, VALUES),
    st.tuples(st.just("setitem"), INDEX, VALUES),
    st.tuples(st.just("delitem"), INDEX),
    st.tuples(st.just("pop")),
    st.tuples(st.just("remove"), VALUES),
    st.tuples(st.just("clear")),
)
SET_STEP = st.tuples(st.sampled_from(("add", "discard")), VALUES)
DICT_STEP = st.one_of(
    st.tuples(st.just("put"), KEYS, VALUES),
    st.tuples(st.just("delkey"), KEYS),
)
INPUT_STEP = st.one_of(
    st.tuples(st.just("set"), st.sampled_from(("x", "y", "w0", "w1", "w2")), VALUES),
    st.tuples(st.just("set"), st.sampled_from(("x", "y", "w0", "w1", "w2")), VALUES),
    st.tuples(st.just("bad"), st.sampled_from(("x", "w1"))),
)
READ_STEP = st.one_of(
    st.tuples(st.sampled_from(("read", "submit", "snapshot")), st.just("cold"), ARG),
    st.tuples(st.just("read"), st.sampled_from((*QUERIES, "late")), ARG),
    st.tuples(st.just("read_input"), st.sampled_from(("x", "y", "w0", "w1", "w2"))),
    st.tuples(st.just("submit"), st.sampled_from((*QUERIES, "late")), ARG),
    st.tuples(st.just("snapshot"), st.sampled_from((*QUERIES, "late")), ARG),
)
OTHER_STEP = st.one_of(
    st.tuples(st.just("create"), st.lists(VALUES, max_size=3)),
    st.tuples(st.just("zappend"), VALUES),
    st.tuples(st.just("compact"), st.sampled_from(("xs", "ss", "dd", "zs"))),
    st.tuples(st.just("compact_memory"), st.sampled_from(("xs", "dd"))),
    st.tuples(st.just("badval")),
    st.tuples(st.just("nested")),
)
STEP = st.one_of(LIST_STEP, SET_STEP, DICT_STEP, INPUT_STEP, READ_STEP, OTHER_STEP)
SEGMENT = st.one_of(
    st.tuples(st.just("tx"), st.lists(STEP, max_size=8), st.just("commit")),
    st.tuples(st.just("tx"), st.lists(STEP, max_size=8), st.just("raise")),
    st.tuples(st.just("plain"), st.lists(STEP, max_size=3)),
)
PROGRAM = st.lists(SEGMENT, min_size=1, max_size=6)


def _change_list(xs: list[Any], step: tuple[Any, ...]) -> bool | None:
    """Apply a list step to ``xs`` (a plain list or a CascadeList).

    Returns whether it records a change (moves the log), or None when the
    step does not apply to the current contents and was skipped.
    """
    op = step[0]
    n = len(xs)
    if op == "append":
        xs.append(step[1])
        return True
    if op == "extend":
        xs.extend(step[1])
        return bool(step[1])
    if op == "insert":
        xs.insert(step[1], step[2])
        return True
    if op == "setitem":
        if step[1] >= n:
            return None
        xs[step[1]] = step[2]
        return True
    if op == "delitem":
        if step[1] >= n:
            return None
        del xs[step[1]]
        return True
    if op == "pop":
        if not n:
            return None
        xs.pop()
        return True
    if op == "remove":
        if step[1] not in xs:
            return None
        xs.remove(step[1])
        return True
    assert op == "clear"
    xs.clear()
    return bool(n)


LIST_OPS = {
    "append",
    "extend",
    "insert",
    "setitem",
    "delitem",
    "pop",
    "remove",
    "clear",
}


def _change(target: Any, step: tuple[Any, ...]) -> bool | None:
    """Apply a collection step to ``target`` (a Rig's objects or a World)."""
    op = step[0]
    if op in LIST_OPS:
        return _change_list(target.xs, step)
    if op == "add":
        if step[1] in target.ss:
            target.ss.add(step[1])
            return False
        target.ss.add(step[1])
        return True
    if op == "discard":
        present = step[1] in target.ss
        target.ss.discard(step[1])
        return present
    if op == "put":
        target.dd[step[1]] = step[2]
        return True
    if op == "delkey":
        if step[1] not in target.dd:
            return None
        del target.dd[step[1]]
        return True
    assert op == "zappend"
    if target.zs is None:
        return None
    target.zs.append(step[1])
    return True


COLLECTION_OPS = LIST_OPS | {"add", "discard", "put", "delkey", "zappend"}
KINDS = {"xs": "list", "ss": "set", "dd": "dict", "zs": "list"}


def _same_contents(rig: Rig, world: World) -> None:
    assert list(rig.xs) == world.xs
    assert set(rig.ss) == world.ss
    assert list(rig.dd.items()) == list(world.dd.items())
    if world.zs is None:
        assert rig.zs is None
    else:
        assert rig.zs is not None
        assert list(rig.zs) == world.zs


def _input_of(world: World, which: str) -> Any:
    if which in ("x", "y"):
        return getattr(world, which)
    return world.w[int(which[1:])]


def _set_input_of(world: World, which: str, value: Any) -> None:
    if which in ("x", "y"):
        setattr(world, which, value)
    else:
        world.w[int(which[1:])] = value


class OneEngine:
    """Runs a generated program on a Rig and checks it against a World."""

    def __init__(self, rig: Rig, world: World) -> None:
        self.rig = rig
        self.world = world  # committed
        self.revision = rig.engine.revision
        self.salt = 100  # the next argument for ``cold``, a key never read

    # --- checks -------------------------------------------------------------

    def check_reads(self, world: World) -> None:
        rig = self.rig
        for name in QUERIES:
            for arg in (0, 2) if name == "pick" else (0,):
                assert rig.call(name, arg) == _expect(world, name, arg), name
        if rig.zs is not None:
            assert rig.call("late", 0) == _expect(world, "late")
        for which in ("x", "y", "w0", "w1", "w2"):
            assert rig.input_value(which) == _input_of(world, which)

    def check_disk(self, world: World) -> None:
        if not self.rig.named:
            return
        engine = self.rig.engine
        assert _disk_contents(engine, "xs", "list") == world.xs
        assert _disk_contents(engine, "ss", "set") == world.ss
        on_disk = _disk_contents(engine, "dd", "dict")
        assert on_disk == world.dd
        zs = _disk_contents(engine, "zs", "list")
        # A list created in a block that rolled back has no record until
        # its first change; read through a new engine it is empty either way.
        assert (zs or []) == (world.zs or [])

    def check_committed(self) -> None:
        rig = self.rig
        assert rig.engine.revision == self.revision
        _same_contents(rig, self.world)
        self.check_reads(self.world)
        self.check_disk(self.world)

    # --- steps --------------------------------------------------------------

    def step(self, step: tuple[Any, ...], block: Block | None) -> None:
        """Run one step; ``block`` is the open transaction's bookkeeping."""
        rig = self.rig
        op = step[0]
        # What reads see: the committed state, in a block and out of one.
        seen = self.world
        if op in ("read", "submit", "snapshot") and step[1] == "cold":
            self.salt += 1
            step = (op, "cold", self.salt)
        if op in COLLECTION_OPS:
            target = block.staged if block is not None else self.world
            if op == "zappend" and target.zs is None:
                return
            if op in LIST_OPS and _change_list(list(target.xs), step) is None:
                return
            if op == "delkey" and step[1] not in target.dd:
                return
            emitted = _change(rig, step)
            assert _change(target, step) == emitted
            if block is not None:
                block.emitted |= bool(emitted)
            elif emitted:
                self.revision += 1
        elif op == "set" or op == "bad":
            which = step[1]
            value = BAD if op == "bad" else step[2]
            handle_args: tuple[Any, ...] = (
                () if which in ("x", "y") else (int(which[1:]),)
            )
            handle = {"x": rig.x, "y": rig.y}.get(which, rig.w)
            if block is not None:
                assert handle.set(*handle_args, value) is None  # queued
                block.queued[which] = value
            elif value is BAD:
                with pytest.raises(UnencodableValueError):
                    handle.set(*handle_args, value)
            else:
                handle.set(*handle_args, value)
                if _input_of(self.world, which) != value:
                    _set_input_of(self.world, which, value)
                    self.revision += 1
        elif op == "read":
            if step[1] == "late" and rig.zs is None:
                return
            assert rig.call(step[1], step[2]) == _expect(seen, step[1], step[2])
        elif op == "read_input":
            assert rig.input_value(step[1]) == _input_of(seen, step[1])
        elif op == "submit":
            if step[1] == "late" and rig.zs is None:
                return
            future = rig.submit(step[1], step[2])
            assert future.result(TIMEOUT) == _expect(seen, step[1], step[2])
        elif op == "snapshot":
            if step[1] == "late" and rig.zs is None:
                return
            snap = rig.engine.snapshot()
            expected = _expect(seen, step[1], step[2])
            try:
                assert rig.call(step[1], step[2], snapshot=snap) == expected
            except BaseException:
                snap.release()
                raise
            if block is not None:
                # Read again after the block, then released.
                block.snapshots.append((snap, step[1], step[2], expected))
            else:
                snap.release()
        elif op == "create":
            if rig.zs is not None:
                return
            rig.create_late(list(step[1]))
            if block is not None:
                block.staged.zs = list(step[1])
                block.created = True
                block.emitted = True
            else:
                self.world.zs = list(step[1])
                self.revision += 2 if step[1] else 1
        elif op == "compact":
            coll = getattr(rig, step[1])
            if coll is not None:
                coll.compact()
        elif op == "compact_memory":
            getattr(rig, step[1])._core.compact_memory()  # noqa: SLF001
        elif op == "badval":
            if not rig.named:
                return
            with pytest.raises(PersistentCacheError, match="cannot be persisted"):
                rig.xs.append(BAD)
        else:
            assert op == "nested"
            inner = rig.engine.transaction()
            if block is not None:
                with pytest.raises(RuntimeError, match="Nested"):
                    with inner:
                        raise AssertionError("must not run")  # pragma: no cover
            else:
                with inner:
                    pass
        if block is not None:
            # The objects show the block's changes; nothing else moves.
            _same_contents(rig, block.staged)
            assert rig.engine.revision == self.revision
            self.check_disk(self.world)

    def run_block(self, steps: list[tuple[Any, ...]], end: str) -> None:
        rig = self.rig
        block = Block(staged=self.world.copy())
        uids = list(rig.xs._uids)  # noqa: SLF001
        heads = rig.heads()
        # The block fails at commit when an input's last value is bad.
        try:
            with rig.engine.transaction():
                for step in steps:
                    self.step(step, block)
                if end == "raise":
                    raise Abort
            outcome = "commit"
        except Abort:
            outcome = "rollback"
        except UnencodableValueError:
            outcome = "failed"
        bad = any(value is BAD for value in block.queued.values())
        expected = "rollback" if end == "raise" else ("failed" if bad else "commit")
        assert outcome == expected
        event(f"block: {outcome}")
        if outcome == "commit":
            after = block.staged
            changed = block.emitted
            for which, value in block.queued.items():
                if _input_of(after, which) != value:
                    _set_input_of(after, which, value)
                    changed = True
            self.world = after
            self.revision += 1 if changed else 0
        else:
            # Nothing of the block is left: a list created in it is empty.
            if block.created:
                self.world.zs = []
                heads["zs"] = 0
            assert rig.xs._uids == uids  # noqa: SLF001
            assert rig.heads() == heads
        self.check_committed()
        for snap, name, arg, value in block.snapshots:
            with snap:
                assert rig.call(name, arg, snapshot=snap) == value

    def run(self, program: list[tuple[Any, ...]]) -> None:
        self.check_committed()
        for segment in program:
            if segment[0] == "tx":
                self.run_block(segment[1], segment[2])
            else:
                for step in segment[1]:
                    self.step(step, None)
                self.check_committed()


@dataclass
class Block:
    staged: World
    queued: dict[str, Any] = field(default_factory=dict)
    emitted: bool = False
    created: bool = False
    snapshots: list[Any] = field(default_factory=list)


def _fresh_reads(world: World) -> dict[tuple[str, int], Any]:
    """Every query's value on a new in-memory engine built from ``world``."""
    engine = Engine()
    try:
        rig = Rig(engine, world, named=False)
        rig.set_inputs(world)
        out = {}
        for name in rig.queries:
            for arg in (0, 2) if name == "pick" else (0,):
                out[name, arg] = rig.call(name, arg)
        return out
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", [None, *BACKENDS], ids=["memory", *BACKENDS])
@_settings(60)
@given(program=PROGRAM)
def test_one_engine_transaction_programs(backend: str | None, program: Any) -> None:
    with _cache_dir(backend) as path:
        kwargs = {} if path is None else {"cache_dir": path, "cache_backend": backend}
        engine = Engine(**kwargs)
        try:
            runner = OneEngine(Rig(engine, World(), named=path is not None), World())
            runner.run(program)
            world = runner.world
            reads = {
                (name, arg): runner.rig.call(name, arg)
                for name in runner.rig.queries
                for arg in ((0, 2) if name == "pick" else (0,))
            }
        finally:
            engine.shutdown()
        # Results equal those of a new engine built from the model.
        assert reads == _fresh_reads(world)
        if path is None:
            return
        # Reopened on the directory: the collections are what the model
        # committed, and the results (many of them cached on disk) are too.
        reopened = Engine(**kwargs)
        try:
            seed = World(xs=[], ss=set(), dd={}, zs=None if world.zs is None else [])
            rig = Rig(reopened, seed, named=True)
            rig.set_inputs(world)
            _same_contents(rig, world)
            for (name, arg), value in reads.items():
                assert rig.call(name, arg) == value, name
        finally:
            reopened.shutdown()


# Found by test_one_engine_transaction_programs on its first run.


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
@pytest.mark.parametrize("backend", BACKENDS)
def test_compact_in_a_block_writes_only_committed_changes(
    tmp_path: Any, backend: str, commit: bool
) -> None:
    # compact() inside a block wrote a snapshot of the log's head, the
    # block's uncommitted changes included. A rollback left them on disk,
    # and a commit then appended the same diffs again on top of the
    # snapshot: a new engine read [1, 3] as [1, 3, 2] after a rollback and
    # [1, 2, 3] as [1, 2, 2, 3] after a commit.
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    xs = CascadeList(engine, [1], name="xs")
    try:
        with engine.transaction():
            xs.append(2)
            xs.compact()
            assert _disk_contents(engine, "xs", "list") == [1]
            if not commit:
                raise Abort
    except Abort:
        pass
    expected = [1, 2] if commit else [1]
    assert list(xs) == expected
    assert _disk_contents(engine, "xs", "list") == expected
    xs.append(3)
    engine.shutdown()
    reopened = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert list(CascadeList(reopened, name="xs")) == [*expected, 3]
    finally:
        reopened.shutdown()


@pytest.mark.parametrize("backend", BACKENDS)
def test_compact_of_a_collection_created_in_a_rolled_back_block_records_nothing(
    tmp_path: Any, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        with pytest.raises(Abort):
            with engine.transaction():
                made = CascadeList(engine, [1, 2], name="made")
                made.compact()
                raise Abort
        assert _disk_contents(engine, "made", "list") is None
        made.compact()  # still nothing committed: still no record
        assert _disk_contents(engine, "made", "list") is None
    finally:
        engine.shutdown()
    reopened = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        # The name is new: a set may take it, and its seed is used.
        assert set(CascadeSet(reopened, {5}, name="made")) == {5}
    finally:
        reopened.shutdown()


# =============================================================================
# 2. Two engines on one cache_dir
# =============================================================================


class Actor:
    """A thread that runs the calls it is given, one at a time, in order.

    A transaction entered by a call stays open on the thread for the calls
    after it, so two engines can each have a block open at once while the
    test decides the order of every step.
    """

    def __init__(self) -> None:
        self._jobs: queue.Queue[Any] = queue.Queue()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while True:
            job = self._jobs.get()
            if job is None:
                return
            fn, future = job
            try:
                future.set_result(fn())
            except BaseException as exc:  # noqa: BLE001
                future.set_exception(exc)

    def call(self, fn: Callable[[], Any]) -> Any:
        future: concurrent.futures.Future[Any] = concurrent.futures.Future()
        self._jobs.put((fn, future))
        return future.result(TIMEOUT)

    def stop(self) -> None:
        self._jobs.put(None)
        self._thread.join(TIMEOUT)


LISTS = ("la", "lb")
SIDE_QUERIES = ("view", "rows", "both")


class Side:
    """One engine on the shared directory, with two named lists."""

    def __init__(self, path: str, backend: str) -> None:
        engine = Engine(cache_dir=path, cache_backend=backend)

        @engine.input
        def x() -> int:
            return 0

        la = CascadeList(engine, name="la", compact_every=3)
        lb = CascadeList(engine, name="lb", compact_every=3)

        # The same names in both engines: their results share the disk.
        @engine.query
        def view() -> Any:
            return x(), sum(v for v in la), [v for v in lb]

        @engine.query
        def rows() -> Any:
            out = []
            for v in la:
                out.append(v)
            return out, len(lb)

        @engine.query
        def both() -> Any:
            return view(), rows()

        self.engine = engine
        self.x = x
        self.lists = {"la": la, "lb": lb}
        self.queries = {"view": view, "rows": rows, "both": both}
        self.tx: Any = None


def _expect_side(x: Any, lists: dict[str, list[int]], name: str) -> Any:
    view = (x, sum(lists["la"]), list(lists["lb"]))
    rows = (list(lists["la"]), len(lists["lb"]))
    return {"view": view, "rows": rows, "both": (view, rows)}[name]


_UNSET = object()


@dataclass
class SideModel:
    """One engine's own copy of each list, as it loaded or wrote it."""

    revision: int
    lists: dict[str, list[int]]
    seq: dict[str, int]  # how many commits to each list this copy has seen
    x: Any = 0
    writer: dict[str, bool] = field(default_factory=lambda: dict.fromkeys(LISTS, False))
    staged: dict[str, list[int]] | None = None  # an open block's lists
    queued: Any = _UNSET  # an open block's last x
    changed: set[str] = field(default_factory=set)


SIDE = st.integers(min_value=0, max_value=1)
TWO_STEP = st.one_of(
    st.tuples(st.just("begin"), SIDE),
    st.tuples(st.just("begin"), SIDE),
    st.tuples(st.sampled_from(("commit", "raise")), SIDE),
    st.tuples(st.just("set"), SIDE, VALUES),
    st.tuples(st.just("bad"), SIDE),
    st.tuples(st.just("append"), SIDE, st.sampled_from(LISTS), VALUES),
    st.tuples(st.just("append"), SIDE, st.sampled_from(LISTS), VALUES),
    st.tuples(st.just("pop"), SIDE, st.sampled_from(LISTS)),
    st.tuples(st.just("compact"), SIDE, st.sampled_from(LISTS)),
    st.tuples(st.sampled_from(("read", "submit")), SIDE, st.sampled_from(SIDE_QUERIES)),
    st.tuples(st.just("reopen"), SIDE),
    st.tuples(st.just("reopen"), SIDE),
)


class TwoEngines:
    """Runs a two-engine program, each engine on its own Actor thread."""

    def __init__(self, path: str, backend: str) -> None:
        self.path = path
        self.backend = backend
        self.actors = [Actor(), Actor()]
        self.sides: list[Side] = []
        self.models: list[SideModel] = []
        self.disk: dict[str, list[int]] = {name: [] for name in LISTS}
        self.disk_seq = dict.fromkeys(LISTS, 0)
        for i in (0, 1):
            self.sides.append(self.actors[i].call(lambda: Side(path, backend)))
            self.models.append(self._fresh_model(i))

    def _fresh_model(self, i: int) -> SideModel:
        # G3: read x first, so its default is its value. A result reused
        # from disk reads the default without keeping it, and a set to the
        # default after that is a change (a new revision, and G6 cancels).
        assert self.actors[i].call(self.sides[i].x) == 0
        return SideModel(
            revision=self.sides[i].engine.revision,
            lists=copy.deepcopy(self.disk),
            seq=dict(self.disk_seq),
        )

    def close(self) -> None:
        for actor, side in zip(self.actors, self.sides, strict=True):
            try:
                actor.call(side.engine.shutdown)
            finally:
                actor.stop()

    # --- checks -------------------------------------------------------------

    def check(self) -> None:
        engine = self.sides[0].engine
        on_disk = self.actors[0].call(
            lambda: {name: _disk_contents(engine, name, "list") for name in LISTS}
        )
        assert on_disk == self.disk
        for i in (0, 1):
            side, model = self.sides[i], self.models[i]
            shown = model.staged if model.staged is not None else model.lists
            assert {n: list(c) for n, c in side.lists.items()} == shown
            assert side.engine.revision == model.revision
            got = self.actors[i].call(side.queries["both"])
            assert got == _expect_side(model.x, model.lists, "both")

    # --- steps --------------------------------------------------------------

    def may_write(self, i: int, name: str) -> bool:
        """G26: this engine holds the list's lock, or can take it."""
        model, other = self.models[i], self.models[1 - i]
        if model.writer[name]:
            return True
        return not other.writer[name] and self.disk_seq[name] <= model.seq[name]

    def step(self, step: tuple[Any, ...]) -> None:
        op, i = step[0], step[1]
        side, model, actor = self.sides[i], self.models[i], self.actors[i]
        in_block = model.staged is not None
        if op == "begin":
            if in_block:
                return
            side.tx = side.engine.transaction()
            actor.call(side.tx.__enter__)
            model.staged = copy.deepcopy(model.lists)
            model.queued = _UNSET
            model.changed = set()
            if self.models[1 - i].staged is not None:
                event("blocks open on both engines")
        elif op in ("commit", "raise"):
            if not in_block:
                return
            self.end_block(i, op == "commit")
        elif op in ("set", "bad"):
            value = BAD if op == "bad" else step[2]
            if in_block:
                assert actor.call(lambda: side.x.set(value)) is None
                model.queued = value
            elif value is BAD:
                with pytest.raises(UnencodableValueError):
                    actor.call(lambda: side.x.set(value))
            else:
                actor.call(lambda: side.x.set(value))
                if model.x != value:
                    model.x = value
                    model.revision += 1
        elif op in ("append", "pop"):
            name = step[2]
            target = (model.staged if in_block else model.lists)[name]
            if op == "pop" and not target:
                return
            coll = side.lists[name]
            change = (lambda: coll.append(step[3])) if op == "append" else coll.pop
            if not self.may_write(i, name):
                with pytest.raises(PersistentCacheError):
                    actor.call(change)
                event("a change refused by G26")
                return
            actor.call(change)
            model.writer[name] = True
            if op == "append":
                target.append(step[3])
            else:
                target.pop()
            if in_block:
                model.changed.add(name)
            else:
                self.committed(i, name)
                model.revision += 1
        elif op == "compact":
            name = step[2]
            coll = side.lists[name]
            if not self.may_write(i, name):
                with pytest.raises(PersistentCacheError):
                    actor.call(coll.compact)
                return
            actor.call(coll.compact)
            model.writer[name] = True
        elif op in ("read", "submit"):
            query = side.queries[step[2]]
            if op == "read":
                got = actor.call(query)
            else:
                got = actor.call(lambda: side.engine.submit(query)).result(TIMEOUT)
            assert got == _expect_side(model.x, model.lists, step[2])
        else:
            assert op == "reopen"
            if in_block:
                return
            actor.call(side.engine.shutdown)
            event("an engine reopened")
            self.sides[i] = actor.call(lambda: Side(self.path, self.backend))
            self.models[i] = self._fresh_model(i)
        self.check()

    def committed(self, i: int, name: str) -> None:
        """Engine ``i``'s list ``name`` reached disk."""
        model = self.models[i]
        self.disk[name] = list(model.lists[name])
        self.disk_seq[name] += 1
        model.seq[name] = self.disk_seq[name]

    def end_block(self, i: int, commit: bool) -> None:
        side, model, actor = self.sides[i], self.models[i], self.actors[i]
        tx, side.tx = side.tx, None
        if not commit:
            assert actor.call(lambda: tx.__exit__(Abort, Abort(), None)) is None
        elif model.queued is BAD:
            with pytest.raises(UnencodableValueError):
                actor.call(lambda: tx.__exit__(None, None, None))
        else:
            actor.call(lambda: tx.__exit__(None, None, None))
            assert model.staged is not None
            changed = bool(model.changed)
            model.lists = model.staged
            for name in sorted(model.changed):
                self.committed(i, name)
            if model.queued is not _UNSET and model.queued != model.x:
                model.x = model.queued
                changed = True
            model.revision += 1 if changed else 0
        model.staged = None
        model.queued = _UNSET
        model.changed = set()

    def run(self, program: list[tuple[Any, ...]]) -> None:
        self.check()
        for step in program:
            self.step(step)
        for i in (0, 1):
            if self.models[i].staged is not None:
                self.end_block(i, commit=True)
                self.check()


@pytest.mark.parametrize("backend", BACKENDS)
@_settings(40)
@given(program=st.lists(TWO_STEP, min_size=8, max_size=40), x=VALUES)
def test_two_engines_on_one_cache_dir(backend: str, program: Any, x: int) -> None:
    with _cache_dir(backend) as path:
        assert path is not None
        run = TwoEngines(path, backend)
        try:
            run.run(program)
        finally:
            run.close()
        # A new engine restores what the model put on disk, and its results
        # (read from the disk the other two wrote) are those of its inputs.
        third = Side(path, backend)
        try:
            assert {n: list(c) for n, c in third.lists.items()} == run.disk
            for name in SIDE_QUERIES:
                assert third.queries[name]() == _expect_side(0, run.disk, name)
            third.x.set(x)
            for name in SIDE_QUERIES:
                assert third.queries[name]() == _expect_side(x, run.disk, name)
        finally:
            third.engine.shutdown()


# =============================================================================
# 3. submit and reader threads while blocks open, commit and roll back
# =============================================================================


@dataclass(frozen=True)
class Flat:
    """A committed state of the submit rig."""

    x: Any = 0
    y: Any = 0
    xs: tuple[int, ...] = (1, 2)
    ss: frozenset[int] = frozenset({1})


FLAT_QUERIES = ("total", "combo", "slow", "fresh")


def _expect_flat(state: Flat, name: str) -> Any:
    total = sum(state.xs) + state.x
    if name == "total":
        return total
    if name == "combo":
        return state.x, state.y, sorted(state.ss), len(state.xs)
    if name == "slow":
        return state.x, total, sorted(state.ss), state.y
    if name == "fresh":
        return state.x, list(state.xs), sorted(state.ss), state.y, sum(state.xs)
    assert name == "gated"
    return state.x, sum(state.xs), state.y, sorted(state.ss)


class History:
    """Every state the writer has committed or is committing, in order.

    The writer publishes a state before the change that makes it, and
    settles it once the change has returned. A reader that starts at
    ``start()`` and ends at ``end()`` may see any state between the two.
    """

    def __init__(self, first: Flat) -> None:
        self._lock = threading.Lock()
        self.states = [first]
        self._settled = 0

    def start(self) -> int:
        with self._lock:
            return self._settled

    def end(self) -> int:
        with self._lock:
            return len(self.states) - 1

    def publish(self, state: Flat) -> None:
        with self._lock:
            self.states.append(state)

    def settle(self) -> None:
        with self._lock:
            self._settled = len(self.states) - 1

    def values(self, name: str, lo: int, hi: int) -> list[Any]:
        return [_expect_flat(s, name) for s in self.states[lo : hi + 1]]


class SubmitRig:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.gates: dict[int, threading.Event] = {}
        gates = self.gates

        @engine.input
        def x() -> int:
            return 0

        @engine.input
        def y() -> int:
            return 0

        xs = CascadeList(engine, [1, 2])
        ss = CascadeSet(engine, {1})

        @engine.query
        def total() -> int:
            return sum(v for v in xs) + x()

        @engine.query
        def combo() -> Any:
            return x(), y(), sorted(ss), len(xs)

        @engine.query
        def slow() -> Any:
            # Reads spread out in time, so changes land between them.
            a = x()
            time.sleep(0.0003)
            t = total()
            time.sleep(0.0003)
            return a, t, sorted(v for v in ss), y()

        @engine.query
        def gated(token: int) -> Any:
            first = x(), sum(v for v in xs)
            assert gates[token].wait(TIMEOUT)
            return (*first, y(), sorted(ss))

        self.x, self.y, self.xs, self.ss = x, y, xs, ss

        @engine.query
        def fresh(token: int) -> Any:
            # A new token each call: never cached, so the body runs on the
            # caller's thread while a block may be open on another.
            out = []
            for v in xs:
                out.append(v)
            return x(), out, sorted(ss), y(), sum(v for v in xs)

        self.queries = {
            "total": total,
            "combo": combo,
            "slow": slow,
            "fresh": fresh,
            "gated": gated,
        }
        self._tokens = 0
        self._token_lock = threading.Lock()

    def args(self, name: str) -> tuple[Any, ...]:
        """Arguments for a call of ``name``: a new token for ``fresh``."""
        if name != "fresh":
            return ()
        with self._token_lock:
            self._tokens += 1
            return (self._tokens,)


FLAT_STEP = st.one_of(
    st.tuples(st.just("set"), st.sampled_from(("x", "y")), VALUES),
    st.tuples(st.just("bad")),
    st.tuples(st.just("append"), VALUES),
    st.tuples(st.just("pop")),
    st.tuples(st.sampled_from(("add", "discard")), VALUES),
    st.tuples(st.sampled_from(("read", "submit")), st.sampled_from(FLAT_QUERIES)),
    st.tuples(st.just("gate")),
    st.tuples(st.just("release")),
)
FLAT_SEGMENT = st.one_of(
    st.tuples(st.just("tx"), st.lists(FLAT_STEP, max_size=6), st.just("commit")),
    st.tuples(st.just("tx"), st.lists(FLAT_STEP, max_size=6), st.just("raise")),
    st.tuples(st.just("plain"), st.lists(FLAT_STEP, max_size=3)),
)


class Reader:
    """A thread that reads, submits and snapshots until told to stop.

    It records what it saw and the window of states it may have seen; the
    checks run on the main thread afterwards.
    """

    def __init__(self, rig: SubmitRig, history: History, seed: int) -> None:
        self.rig = rig
        self.history = history
        self.rng = random.Random(seed)
        self.stop = threading.Event()
        self.seen: list[tuple[Any, ...]] = []
        self.kept: list[tuple[Any, ...]] = []
        self.error: BaseException | None = None
        self.ops = 0
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def _run(self) -> None:
        try:
            while not self.stop.is_set():
                self._once()
                self.ops += 1
        except BaseException as exc:  # noqa: BLE001
            self.error = exc

    def _once(self) -> None:
        rig, history = self.rig, self.history
        kind = self.rng.choice(("call", "submit", "keep", "snapshot"))
        name = self.rng.choice(FLAT_QUERIES)
        query = rig.queries[name]
        args = rig.args(name)
        lo = history.start()
        if kind == "call":
            got = query(*args)
            self.seen.append((name, lo, history.end(), got))
        elif kind == "snapshot":
            with rig.engine.snapshot() as snap:
                got = query(*args, snapshot=snap)
                time.sleep(0)
                assert query(*args, snapshot=snap) == got
            self.seen.append((name, lo, history.end(), got))
        else:
            future = rig.engine.submit(query, *args)
            hi = history.end()
            if kind == "keep":
                self.kept.append((name, lo, hi, future))
                return
            try:
                got = future.result(TIMEOUT)
            except QueryCancelled:
                got = QueryCancelled
            self.seen.append((name, lo, hi, got, history.end()))


@dataclass
class Gate:
    token: int
    future: concurrent.futures.Future[Any]
    state: int  # the committed state when it was submitted
    changed: bool = False


class SubmitRun:
    """The writer: runs a program on the main thread, beside the readers."""

    def __init__(self, rig: SubmitRig) -> None:
        self.rig = rig
        self.readers: list[Reader] = []
        self.state = Flat()
        self.history = History(self.state)
        # G3: an input's default becomes its value when first read at the
        # newest revision. Until then, setting it to the default is a change.
        assert (rig.x(), rig.y()) == (0, 0)
        self.revision = rig.engine.revision
        self.gates: list[Gate] = []
        self.tokens = 0

    def pace(self) -> None:
        """Let each reader finish a read, so that reads and steps interleave."""
        start = [reader.ops for reader in self.readers]
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and any(
            reader.ops == ops and reader.thread.is_alive()
            for reader, ops in zip(self.readers, start, strict=True)
        ):
            time.sleep(0.0001)

    def publish(self, state: Flat) -> None:
        self.history.publish(state)
        for gate in self.gates:
            gate.changed = True

    def release(self) -> None:
        gate = self.gates.pop(0)
        self.rig.gates[gate.token].set()
        if gate.changed:
            # G6: an input changed after the submit, so it is cancelled.
            with pytest.raises(QueryCancelled):
                gate.future.result(TIMEOUT)
        else:
            want = _expect_flat(self.history.states[gate.state], "gated")
            assert gate.future.result(TIMEOUT) == want

    def change(self, state: Flat, step: tuple[Any, ...]) -> Flat | None:
        """``state`` after a change step, or None when it does not apply."""
        op = step[0]
        if op == "set":
            return Flat(**{**state.__dict__, step[1]: step[2]})
        if op == "append":
            return Flat(state.x, state.y, (*state.xs, step[1]), state.ss)
        if op == "pop":
            if not state.xs:
                return None
            return Flat(state.x, state.y, state.xs[:-1], state.ss)
        if op == "add":
            return Flat(state.x, state.y, state.xs, state.ss | {step[1]})
        assert op == "discard"
        return Flat(state.x, state.y, state.xs, state.ss - {step[1]})

    def apply(self, step: tuple[Any, ...]) -> None:
        rig, op = self.rig, step[0]
        if op == "set":
            getattr(rig, step[1]).set(step[2])
        elif op == "append":
            rig.xs.append(step[1])
        elif op == "pop":
            rig.xs.pop()
        elif op == "add":
            rig.ss.add(step[1])
        else:
            rig.ss.discard(step[1])

    def step(self, step: tuple[Any, ...], block: dict[str, Any] | None) -> None:
        rig, op = self.rig, step[0]
        if op in ("read", "submit"):
            want = _expect_flat(self.state, step[1])
            query = rig.queries[step[1]]
            args = rig.args(step[1])
            if op == "read":
                assert query(*args) == want
            else:
                assert rig.engine.submit(query, *args).result(TIMEOUT) == want
        elif op == "gate":
            if len(self.gates) >= 2:
                return
            self.tokens += 1
            rig.gates[self.tokens] = threading.Event()
            future = rig.engine.submit(rig.queries["gated"], self.tokens)
            self.gates.append(Gate(self.tokens, future, self.history.end()))
        elif op == "release":
            if self.gates:
                self.release()
        elif op == "bad":
            if block is not None:
                assert rig.x.set(BAD) is None
                block["bad"] = True
            else:
                with pytest.raises(UnencodableValueError):
                    rig.x.set(BAD)
        else:
            base = block["staged"] if block is not None else self.state
            after = self.change(base, step)
            if after is None:
                return
            if block is not None:
                self.apply(step)
                if op == "set":
                    block["bad"] = False if step[1] == "x" else block["bad"]
                # A change to a collection moves its log even when it
                # cancels out; an input counts only if it ends changed.
                block["moved"] |= op in ("append", "pop") or (
                    op in ("add", "discard") and after.ss != base.ss
                )
                block["staged"] = after
                return
            if after == self.state and op != "append" and op != "pop":
                self.apply(step)  # a no-op: nothing to publish
            else:
                self.publish(after)
                self.apply(step)
                self.history.settle()
                self.state = after
                self.revision += 1
        assert rig.engine.revision == self.revision
        self.pace()

    def run_block(self, steps: list[tuple[Any, ...]], end: str) -> None:
        block: dict[str, Any] = {"staged": self.state, "moved": False, "bad": False}
        before = self.state
        try:
            with self.rig.engine.transaction():
                for step in steps:
                    self.step(step, block)
                after = block["staged"]
                inputs_moved = (after.x, after.y) != (before.x, before.y)
                commits = end == "commit" and not block["bad"]
                changed = commits and (block["moved"] or inputs_moved)
                if changed:
                    self.publish(after)
                self.pace()  # readers run while the block is open
                if end == "raise":
                    raise Abort
            outcome = "commit"
        except Abort:
            outcome = "rollback"
        except UnencodableValueError:
            outcome = "failed"
        expected = (
            "rollback" if end == "raise" else ("failed" if block["bad"] else "commit")
        )
        assert outcome == expected
        if changed:
            self.history.settle()
            self.state = after
            self.revision += 1
        assert self.rig.engine.revision == self.revision
        assert (list(self.rig.xs), set(self.rig.ss)) == (
            list(self.state.xs),
            set(self.state.ss),
        )

    def run(self, program: list[tuple[Any, ...]]) -> None:
        for segment in program:
            if segment[0] == "tx":
                self.run_block(segment[1], segment[2])
            else:
                for step in segment[1]:
                    self.step(step, None)
            time.sleep(0)
        while self.gates:
            self.release()
        for name in FLAT_QUERIES:
            got = self.rig.queries[name](*self.rig.args(name))
            assert got == _expect_flat(self.state, name)


def _check_reader(history: History, reader: Reader) -> int:
    """Check what one reader saw; returns how many values it saw."""
    if reader.error is not None:
        raise reader.error
    last = history.end()
    for name, lo, hi, *rest in reader.seen:
        got = rest[0]
        if got is QueryCancelled:
            # Only when an input changed after the call was submitted.
            assert rest[1] > lo, (name, lo, hi)
            continue
        assert got in history.values(name, lo, hi), (name, lo, hi, got)
    for name, lo, hi, future in reader.kept:
        try:
            got = future.result(TIMEOUT)
        except QueryCancelled:
            assert last > lo, (name, lo, hi)
            continue
        assert got in history.values(name, lo, hi), (name, lo, hi, got)
    return len(reader.seen) + len(reader.kept)


@pytest.mark.parametrize("backend", [None, "mdbx"], ids=["memory", "mdbx"])
@_settings(60)
@given(
    program=st.lists(FLAT_SEGMENT, min_size=1, max_size=6),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_submit_and_readers_beside_transactions(
    backend: str | None, program: Any, seed: int
) -> None:
    with _cache_dir(backend) as path:
        kwargs = {} if path is None else {"cache_dir": path, "cache_backend": backend}
        engine = Engine(**kwargs)
        rig = SubmitRig(engine)
        run = SubmitRun(rig)
        readers = [Reader(rig, run.history, seed + i) for i in range(2)]
        run.readers = readers
        try:
            run.run(program)
        finally:
            for gate in run.gates:
                rig.gates[gate.token].set()
            for reader in readers:
                reader.stop.set()
            for reader in readers:
                reader.thread.join(TIMEOUT)
        try:
            assert not any(reader.thread.is_alive() for reader in readers)
            seen = sum(_check_reader(run.history, reader) for reader in readers)
            event(f"reader observations: {min(seen // 25 * 25, 200)}+")
            windows = sum(
                1 for r in readers for rec in r.seen + r.kept if rec[2] > rec[1]
            )
            event(f"observations spanning a change: {min(windows // 5 * 5, 50)}+")
            cancelled = sum(
                1 for r in readers for rec in r.seen if rec[3] is QueryCancelled
            )
            event(f"reader submits cancelled: {min(cancelled, 5)}")
        finally:
            engine.shutdown()


# =============================================================================
# 4. Blocks on asyncio tasks, readers on tasks, one or two event loops
# =============================================================================
#
# Every task runs on an event loop thread; the test's own thread (the
# driver) decides each step and waits until it is over, so a program runs
# the same way every time. Actors are long-lived tasks that run the commands
# they are sent, one at a time: a block an actor opens stays open across its
# later commands, owned by that task (G25). Readers are tasks that await an
# async query whose body stops at pause points between its reads until the
# driver lets it go on, so blocks open, commit, roll back and fail while a
# read is half done, and several readers can ask for one key at once (G5).
# A reader that waits for another task's run is seen in ``waiting_on``.


@dataclass(frozen=True)
class AState:
    """A committed state of the async rig; ``dd`` is the dict's items."""

    x: Any = 0
    y: Any = 0
    xs: tuple[int, ...] = (1, 2)
    ss: frozenset[int] = frozenset({1})
    dd: tuple[tuple[int, int], ...] = ((0, 5),)


A_COLLS = ("xs", "ss", "dd")
A_LIST_OPS = ("append", "pop", "insert", "setitem")
A_TARGET = {
    **dict.fromkeys(A_LIST_OPS, "xs"),
    "add": "ss",
    "discard": "ss",
    "put": "dd",
    "delkey": "dd",
}


def _a_expect(state: AState, name: str) -> Any:
    ss = tuple(sorted(state.ss))
    view = (state.x, state.xs, ss, state.dd, state.y)
    if name == "view":
        return view
    if name == "nest":
        return view, len(state.xs), state.y
    if name == "shared":
        return state.x, sum(state.xs), ss, state.dd
    if name == "combo":
        return state.x, state.y, list(state.xs), list(ss), dict(state.dd)
    assert name == "gated"
    return state.x, state.xs, ss, state.y


def _a_change(contents: Any, step: tuple[Any, ...]) -> tuple[Any, bool] | None:
    """``contents`` after a collection step and whether it records a diff.

    None when the step does not apply to these contents (it is skipped).
    """
    op = step[0]
    if op == "append":
        return (*contents, step[1]), True
    if op == "pop":
        return (contents[:-1], True) if contents else None
    if op == "insert":
        i = min(step[1], len(contents))
        return (*contents[:i], step[2], *contents[i:]), True
    if op == "setitem":
        if step[1] >= len(contents):
            return None
        return (*contents[: step[1]], step[2], *contents[step[1] + 1 :]), True
    if op == "add":
        if step[1] in contents:
            return contents, False
        return contents | {step[1]}, True
    if op == "discard":
        if step[1] not in contents:
            return contents, False
        return contents - {step[1]}, True
    items = dict(contents)
    if op == "put":
        items[step[1]] = step[2]
        return tuple(items.items()), True
    assert op == "delkey"
    if step[1] not in items:
        return None
    del items[step[1]]
    return tuple(items.items()), True


def _a_apply(coll: Any, step: tuple[Any, ...]) -> None:
    op = step[0]
    if op == "append":
        coll.append(step[1])
    elif op == "pop":
        coll.pop()
    elif op == "insert":
        coll.insert(step[1], step[2])
    elif op in ("setitem", "put"):
        coll[step[1]] = step[2]
    elif op == "add":
        coll.add(step[1])
    elif op == "discard":
        coll.discard(step[1])
    else:
        del coll[step[1]]


class Paused:
    """A task the driver can hold at a pause point and then let go on."""

    def __init__(self, run: AsyncRun) -> None:
        self.run = run
        self.task: asyncio.Task[Any] | None = None
        self.loop: asyncio.AbstractEventLoop | None = None
        self.at: str | None = None
        self.last = "the start"  # the last pause point, for timeouts
        self._event: asyncio.Event | None = None
        self.keep: Any = None  # a strong reference to the task

    async def hold(self, label: str) -> None:
        event = asyncio.Event()
        with self.run.cond:
            self.at, self.last, self._event = label, label, event
            self.run.cond.notify_all()
        await event.wait()

    def go_on(self) -> None:
        event, self.at, self._event = self._event, None, None
        assert event is not None and self.loop is not None
        self.loop.call_soon_threadsafe(event.set)


class Gates:
    """The pause points of the rig's async query bodies.

    A body stops at one only in a task registered here; everywhere else (an
    actor's own reads, the final checks) it runs straight through.
    """

    def __init__(self) -> None:
        self.held: dict[Any, Paused] = {}

    async def pause(self, label: str) -> None:
        holder = self.held.get(asyncio.current_task())
        if holder is not None:
            await holder.hold(label)


class AsyncRig:
    def __init__(self, engine: Engine, *, named: bool) -> None:
        self.engine = engine
        self.gates = gates = Gates()
        self.tokens = itertools.count(1)
        self.submit_gates: dict[int, threading.Event] = {}
        submit_gates = self.submit_gates
        # Runs of ``shared``: [revision, start, end], end None while running.
        self.shared_runs: list[list[Any]] = []
        self.clock = itertools.count()
        runs, clock = self.shared_runs, self.clock

        @engine.input
        def x() -> int:
            return 0

        @engine.input
        def y() -> int:
            return 0

        def name(n: str) -> str | None:
            return n if named else None

        xs = CascadeList(engine, [1, 2], name=name("axs"), compact_every=4)
        ss = CascadeSet(engine, {1}, name=name("ass"), compact_every=4)
        dd = CascadeDict(engine, {0: 5}, name=name("add"), compact_every=4)

        @engine.query
        async def view(token: int) -> Any:
            a = x()
            await gates.pause("after x")
            items = tuple(v for v in xs)  # async bodies are not rewritten
            await gates.pause("after xs")
            return a, items, tuple(sorted(ss)), tuple(dd.items()), y()

        @engine.query
        async def nest(token: int) -> Any:
            inner = await view(token)
            await gates.pause("after the inner query")
            return inner, len(xs), y()

        @engine.query
        async def shared() -> Any:
            runtime = engine._evaluator.current_runtime()  # noqa: SLF001
            run = [runtime.snapshot.revision, next(clock), None]
            runs.append(run)
            try:
                a = x()
                await gates.pause("after x")
                total = sum(v for v in xs)
                await gates.pause("after xs")
                return a, total, tuple(sorted(ss)), tuple(dd.items())
            finally:
                run[2] = next(clock)

        @engine.query
        def combo() -> Any:
            return x(), y(), list(xs), sorted(ss), dict(dd)

        @engine.query
        def gated(token: int) -> Any:
            first = x(), tuple(v for v in xs)
            assert submit_gates[token].wait(TIMEOUT)
            return (*first, tuple(sorted(ss)), y())

        assert (x(), y()) == (0, 0)  # G3: the defaults are now values
        self.x, self.y = x, y
        self.colls = {"xs": xs, "ss": ss, "dd": dd}
        self.view, self.nest, self.shared = view, nest, shared
        self.combo, self.gated = combo, gated

    def contents(self) -> dict[str, Any]:
        """What the collection objects hold, written as the model writes it."""
        return {
            "xs": tuple(self.colls["xs"]),
            "ss": frozenset(self.colls["ss"]),
            "dd": tuple(self.colls["dd"].items()),
        }


class LoopThread:
    """An event loop running on a daemon thread of its own.

    Kept from one example to the next (``_loop_threads``): a new loop opens
    a localhost socket pair, and on a machine short of ports making one can
    hang, so the loop is made on its thread and waited for with a timeout.
    """

    def __init__(self, index: int) -> None:
        self.index = index
        made: concurrent.futures.Future[asyncio.AbstractEventLoop] = (
            concurrent.futures.Future()
        )

        def run() -> None:
            loop = asyncio.new_event_loop()
            made.set_result(loop)
            loop.run_forever()

        self.thread = threading.Thread(target=run, name=f"loop{index}", daemon=True)
        self.thread.start()
        try:
            self.loop = made.result(TIMEOUT)
        except concurrent.futures.TimeoutError:
            raise AssertionError(
                f"making an event loop took over {TIMEOUT}s (a localhost "
                "socket pair that never connected: is the machine out of ports?)"
            ) from None

    def start(self, coro: Any) -> concurrent.futures.Future[Any]:
        # In an empty context: a task started by code outside any block.
        return contextvars.Context().run(
            asyncio.run_coroutine_threadsafe, coro, self.loop
        )

    def clear(self) -> None:
        """Cancel every task on the loop and wait until they have ended."""

        async def cancel_all() -> None:
            me = asyncio.current_task()
            tasks = [t for t in asyncio.all_tasks() if t is not me]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        self.start(cancel_all()).result(TIMEOUT)

    def close(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(TIMEOUT)
        if not self.thread.is_alive():
            self.loop.close()


_LOOP_THREADS: list[LoopThread] = []


def _loop_threads(count: int) -> list[LoopThread]:
    while len(_LOOP_THREADS) < count:
        _LOOP_THREADS.append(LoopThread(len(_LOOP_THREADS)))
    return _LOOP_THREADS[:count]


@atexit.register
def _close_loop_threads() -> None:
    for lt in _LOOP_THREADS:
        if lt.thread.is_alive():
            lt.close()


class TaskActor(Paused):
    """A task that runs the driver's commands; its blocks belong to it."""

    def __init__(self, run: AsyncRun, ident: int, lt: LoopThread) -> None:
        super().__init__(run)
        self.ident = ident
        self.lt = lt
        self.loop = lt.loop
        self.queue: asyncio.Queue[Any] | None = None
        self.started = threading.Event()
        self.done: concurrent.futures.Future[Any] = concurrent.futures.Future()

    async def main(self) -> None:
        self.task = asyncio.current_task()
        self.queue = asyncio.Queue()
        self.started.set()
        try:
            while True:
                cmd, reply = await self.queue.get()
                if cmd[0] == "stop":
                    reply.set_result(None)
                    break
                if cmd[0] == "begin":
                    await self.block(reply)
                elif cmd[0] in ("commit", "raise"):
                    reply.set_result("no block")
                else:
                    reply.set_result(await self.execute(cmd))
        except BaseException as exc:
            self.done.set_exception(exc)
            raise
        self.done.set_result(None)

    async def block(self, opened: concurrent.futures.Future[Any]) -> None:
        assert self.queue is not None
        ended: concurrent.futures.Future[Any] | None = None
        try:
            with self.run.rig.engine.transaction():
                opened.set_result(None)
                while True:
                    cmd, reply = await self.queue.get()
                    if cmd[0] in ("commit", "raise"):
                        ended = reply
                        if cmd[0] == "raise":
                            raise Abort
                        break
                    reply.set_result(await self.execute(cmd))
        except Abort:
            assert ended is not None
            ended.set_result("rollback")
        except UnencodableValueError:
            if ended is None:
                raise  # not at a commit: the driver reports it
            ended.set_result("failed")
        else:
            assert ended is not None
            ended.set_result("commit")

    async def execute(self, cmd: tuple[Any, ...]) -> tuple[str, Any]:
        rig, engine = self.run.rig, self.run.rig.engine
        op = cmd[0]
        try:
            if op == "set":
                handle = rig.x if cmd[1] == "x" else rig.y
                return "ok", handle.set(cmd[2])
            if op == "mut":
                _a_apply(rig.colls[A_TARGET[cmd[1][0]]], cmd[1])
                return "ok", None
            if op == "aread":
                query = rig.view if cmd[1] == "view" else rig.nest
                return "ok", await query(cmd[2])
            if op == "side":
                return await asyncio.create_task(self.execute(cmd[1]))
            if op == "gread":
                # Held at the body's first pause point, then cancelled.
                me = asyncio.current_task()
                rig.gates.held[me] = self
                try:
                    return "ok", await rig.view(cmd[1])
                finally:
                    del rig.gates.held[me]
            if op == "sread":
                return "ok", rig.combo()
            if op == "asubmit":
                return "ok", await asyncio.wrap_future(engine.submit(rig.combo))
            if op == "hold":
                return "ok", engine.submit(rig.gated, cmd[1])
            if op == "snapshot":
                return "ok", engine.snapshot()
            if op == "nested":
                with engine.transaction():
                    return "ok", None
            # A task made here copies this task's context, open block and
            # all, but is not part of the block (G25).
            assert op in ("spawn", "spawn_reader")
            child = cmd[1]
            child.keep = asyncio.create_task(child.main())
            return "ok", None
        except Exception as exc:  # noqa: BLE001 - the driver checks it
            return "raised", exc


class TaskReader(Paused):
    """A task that awaits one query, held at the body's pause points."""

    def __init__(self, run: AsyncRun, kind: str, snap: Any, snap_state: int) -> None:
        super().__init__(run)
        self.kind = kind
        self.snap, self.snap_state = snap, snap_state
        self.token = next(run.rig.tokens)
        self.lo = run.index()
        self.hi: int | None = None
        self.outcome: tuple[str, Any] | None = None
        self.cancelled = False

    async def main(self) -> None:
        rig = self.run.rig
        self.task = asyncio.current_task()
        self.loop = asyncio.get_running_loop()
        rig.gates.held[self.task] = self
        kwargs = {} if self.snap is None else {"snapshot": self.snap}
        try:
            if self.kind in ("view", "snapview"):
                got = await rig.view(self.token, **kwargs)
            elif self.kind == "nest":
                got = await rig.nest(self.token)
            else:
                got = await rig.shared(**kwargs)
            self.outcome = ("ok", got)
        except BaseException as exc:  # noqa: BLE001 - checked by the driver
            self.outcome = ("raised", exc)
        finally:
            del rig.gates.held[self.task]
            with self.run.cond:
                self.run.cond.notify_all()

    def expected(self, run: AsyncRun) -> list[Any]:
        name = {"snapview": "view", "snapshared": "shared"}.get(self.kind, self.kind)
        if self.snap is not None:
            return [_a_expect(run.states[self.snap_state], name)]
        assert self.hi is not None
        return [_a_expect(s, name) for s in run.states[self.lo : self.hi + 1]]


@dataclass
class ABlock:
    staged: dict[str, Any] = field(default_factory=dict)  # reserved collections
    queued: dict[str, Any] = field(default_factory=dict)
    moved: bool = False


@dataclass
class HeldSubmit:
    future: concurrent.futures.Future[Any]
    token: int
    revision: int
    state: int


ACTORS = 3  # 0-2 started by the driver; 3 is a task an actor spawns
READERS = 3


class AsyncRun:
    """Runs one async program and checks every step against the model."""

    def __init__(self, rig: AsyncRig, loops: int, placement: list[int]) -> None:
        self.rig = rig
        self.cond = threading.Condition()
        self.lts = _loop_threads(loops)
        self.placement = placement
        self.actors: dict[int, TaskActor | None] = dict.fromkeys(range(ACTORS + 1))
        self.readers: dict[int, TaskReader | None] = dict.fromkeys(range(READERS))
        self.finished: list[TaskReader] = []
        self.state = AState()
        self.states = [self.state]
        self.revision = rig.engine.revision
        self.holder: dict[str, int | None] = dict.fromkeys(A_COLLS)
        self.blocks: dict[int, ABlock] = {}
        self.held: list[HeldSubmit] = []
        self.snaps: list[tuple[Any, int]] = []
        for i in range(ACTORS):
            self.start_actor(i, self.lts[placement[i] % loops])

    # --- plumbing -------------------------------------------------------------

    def index(self) -> int:
        return len(self.states) - 1

    def start_actor(self, i: int, lt: LoopThread) -> None:
        actor = TaskActor(self, i, lt)
        actor.keep = lt.start(actor.main())
        assert actor.started.wait(TIMEOUT), f"actor {i} did not start"
        self.actors[i] = actor

    def post(self, actor: TaskActor, cmd: tuple[Any, ...]) -> concurrent.futures.Future:
        reply: concurrent.futures.Future[Any] = concurrent.futures.Future()
        assert actor.queue is not None
        actor.loop.call_soon_threadsafe(actor.queue.put_nowait, (cmd, reply))
        return reply

    def send(self, actor: TaskActor, cmd: tuple[Any, ...]) -> Any:
        try:
            return self.post(actor, cmd).result(TIMEOUT)
        except concurrent.futures.TimeoutError:
            raise AssertionError(
                f"actor {actor.ident} did not finish {cmd[0]!r} within {TIMEOUT}s"
            ) from None

    def until(self, what: str | Callable[[], str], done: Callable[[], bool]) -> None:
        deadline = time.monotonic() + TIMEOUT
        with self.cond:
            while not done():
                if time.monotonic() > deadline:
                    told = what if isinstance(what, str) else what()
                    raise AssertionError(
                        f"timed out after {TIMEOUT}s waiting for {told}"
                    )
                # Woken by pause points and finished readers. A wait for
                # another task's run shows only in ``waiting_on``: polled.
                self.cond.wait(0.002)

    def settle(self) -> None:
        """Wait until every reader is held, waiting for a run, or finished."""
        waiting_on = self.rig.engine._store.waiting_on  # noqa: SLF001

        def still(reader: TaskReader) -> bool:
            if reader.outcome is not None or reader.at is not None:
                return True
            return reader.task is not None and reader.task in waiting_on

        live = [r for r in self.readers.values() if r is not None]
        self.until(
            lambda: "every reader to be held, waiting for a run, or done; "
            + "still going: "
            + ", ".join(f"{r.kind} after {r.last}" for r in live if not still(r)),
            lambda: all(still(r) for r in live),
        )
        if any(r.task in waiting_on for r in live):
            event("a reader waits for another task's run")
        for slot, reader in self.readers.items():
            if reader is not None and reader.outcome is not None:
                reader.hi = self.index()
                self.finished.append(reader)
                self.readers[slot] = None

    # --- the model ------------------------------------------------------------

    def live(self, coll: str) -> Any:
        holder = self.holder[coll]
        if holder is None:
            return getattr(self.state, coll)
        return self.blocks[holder].staged[coll]

    def commit_state(self, **changes: Any) -> None:
        self.state = AState(**{**self.state.__dict__, **changes})
        self.states.append(self.state)
        self.revision += 1

    def check(self) -> None:
        assert self.rig.engine.revision == self.revision
        assert self.rig.contents() == {c: self.live(c) for c in A_COLLS}

    # --- steps ----------------------------------------------------------------

    def step(self, step: tuple[Any, ...]) -> None:
        op = step[0]
        if op in ("advance", "rcancel"):
            self.reader_step(op, step[1])
        elif op == "reader":
            self.start_reader(*step[1:])
        elif op == "readers":
            for slot in range(READERS):
                self.start_reader(slot, *step[1:])
        elif op == "release":
            if self.held:
                self.release()
        else:
            actor = self.actors[step[1]]
            if actor is not None:
                self.actor_step(actor, op, step)
        self.settle()
        self.check()

    def actor_step(
        self, actor: TaskActor, op: str, step: tuple[Any, ...], *, side: bool = False
    ) -> None:
        # ``side``: the actor runs the step in a task it starts and awaits
        # (as ``asyncio.gather`` would), which copies its context but is
        # outside its block (G25): no block of its own, and no collection
        # reserved for it.
        a = -1 if side else actor.ident
        block = None if side else self.blocks.get(a)

        def send(cmd: tuple[Any, ...]) -> Any:
            return self.send(actor, ("side", cmd) if side else cmd)

        if op == "side":
            inner = step[2]
            self.actor_step(actor, inner[0], (inner[0], step[1], *inner[1:]), side=True)
            if actor.ident in self.blocks:
                event(f"a task awaited inside a block: {inner[0]}")
        elif op == "begin":
            if block is None:
                assert self.send(actor, ("begin",)) is None
                self.blocks[a] = ABlock()
                if len(self.blocks) > 1:
                    event("blocks open on two tasks")
        elif op in ("commit", "raise"):
            if block is not None:
                outcome = self.send(actor, (op,))
                self.end_block(a, outcome, commit=op == "commit")
        elif op in ("cancel", "cancel_reading"):
            self.cancel_actor(actor, mid_read=op == "cancel_reading")
        elif op in ("set", "bad"):
            value = BAD if op == "bad" else step[3]
            status, got = send(("set", step[2], value))
            if block is not None:
                assert (status, got) == ("ok", None)  # queued
                block.queued[step[2]] = value
            elif value is BAD:
                assert status == "raised" and isinstance(got, UnencodableValueError)
            else:
                assert status == "ok", got
                if getattr(self.state, step[2]) != value:
                    self.commit_state(**{step[2]: value})
        elif op == "mut":
            self.mutate(a, send, block, step[2])
        elif op in ("aread", "sread", "asubmit"):
            name = "combo" if op in ("sread", "asubmit") else step[2]
            status, got = self.send(actor, (op, name, next(self.rig.tokens)))
            assert status == "ok", got
            # Nothing else runs meanwhile, so exactly the committed state:
            # inside a block too, where queries see the engine as it was
            # before the block (G25).
            assert got == _a_expect(self.state, name), (op, name)
            if block is not None:
                event("an owner reads in its block")
        elif op == "hold":
            if len(self.held) < 2:
                token = next(self.rig.tokens)
                self.rig.submit_gates[token] = threading.Event()
                status, future = self.send(actor, ("hold", token))
                assert status == "ok", future
                self.held.append(HeldSubmit(future, token, self.revision, self.index()))
        elif op == "snapshot":
            if len(self.snaps) < 2:
                status, snap = self.send(actor, ("snapshot",))
                assert status == "ok", snap
                self.snaps.append((snap, self.index()))
        elif op == "nested":
            status, got = self.send(actor, ("nested",))
            if block is not None:
                assert status == "raised" and isinstance(got, RuntimeError), got
                assert "Nested" in str(got)
            else:
                assert status == "ok", got
        else:
            assert op == "spawn"
            if self.actors[ACTORS] is None:
                child = TaskActor(self, ACTORS, actor.lt)
                assert self.send(actor, ("spawn", child)) == ("ok", None)
                assert child.started.wait(TIMEOUT)
                self.actors[ACTORS] = child
                if block is not None:
                    event("a task spawned inside a block")

    def mutate(
        self,
        a: int,
        send: Callable[[tuple[Any, ...]], Any],
        block: ABlock | None,
        change: tuple[Any, ...],
    ) -> None:
        coll = A_TARGET[change[0]]
        after = _a_change(self.live(coll), change)
        if after is None:
            return
        holder = self.holder[coll]
        if holder is not None and holder != a:
            # Reserved by another task's block: refused, nothing changes.
            status, got = send(("mut", change))
            assert status == "raised" and isinstance(got, RuntimeError), got
            assert "another thread or task" in str(got)
            event("a change refused for another task's block")
            return
        assert send(("mut", change)) == ("ok", None)
        contents, emitted = after
        if not emitted:
            return
        if block is not None:
            block.staged[coll] = contents
            block.moved = True
            self.holder[coll] = a
        else:
            self.commit_state(**{coll: contents})

    def end_block(self, a: int, outcome: str, *, commit: bool) -> None:
        block = self.blocks.pop(a)
        bad = any(v is BAD for v in block.queued.values())
        expected = "rollback" if not commit else ("failed" if bad else "commit")
        assert outcome == expected
        event(f"async block: {outcome}")
        for coll in A_COLLS:
            if self.holder[coll] == a:
                self.holder[coll] = None
        if outcome != "commit":
            return
        changes = dict(block.staged)
        changed = block.moved
        for which, value in block.queued.items():
            if getattr(self.state, which) != value:
                changes[which] = value
                changed = True
        if changed:
            self.commit_state(**changes)

    def cancel_actor(self, actor: TaskActor, *, mid_read: bool) -> None:
        a = actor.ident
        if mid_read:
            # Cancelled inside an async query it awaits (in its block, if
            # one is open), held at the query's first pause point.
            self.post(actor, ("gread", next(self.rig.tokens)))
            self.until(f"actor {a} to reach its read", lambda: actor.at is not None)
            actor.at = None
        assert actor.task is not None
        actor.loop.call_soon_threadsafe(actor.task.cancel)
        concurrent.futures.wait([actor.done], TIMEOUT)
        assert actor.done.done(), f"actor {a} did not end when cancelled"
        ended = actor.done.exception()
        assert isinstance(ended, asyncio.CancelledError), (
            f"actor {a} was cancelled and ended with {ended!r}"
        )
        if a in self.blocks:
            # A cancelled task's block rolls back, as for any exception.
            self.end_block(a, "rollback", commit=False)
            event("a block rolled back by cancelling its task")
        self.actors[a] = None
        if a < ACTORS:
            self.start_actor(a, actor.lt)

    def start_reader(
        self, slot: int, kind: str, origin: int | None, which: int
    ) -> None:
        if self.readers[slot] is not None:
            return
        snap, snap_state = None, -1
        if kind in ("snapview", "snapshared"):
            if not self.snaps:
                return
            snap, snap_state = self.snaps[which % len(self.snaps)]
        actor = None if origin is None else self.actors[origin]
        reader = TaskReader(self, kind, snap, snap_state)
        self.readers[slot] = reader
        if actor is None:
            lt = self.lts[self.placement[ACTORS + slot] % len(self.lts)]
            reader.keep = lt.start(reader.main())
        else:
            # Started by an actor's task, it copies that task's context and
            # the block open there, which it is not part of.
            assert self.send(actor, ("spawn_reader", reader)) == ("ok", None)
            if actor.ident in self.blocks:
                event("a reader started inside a block")

    def reader_step(self, op: str, slot: int) -> None:
        reader = self.readers[slot]
        if reader is None:
            return
        if op == "advance":
            if reader.at is not None:
                reader.go_on()
            return
        assert reader.task is not None and reader.loop is not None
        waiting_on = self.rig.engine._store.waiting_on  # noqa: SLF001
        if any(r is not None and r.task in waiting_on for r in self.readers.values()):
            event("a reader cancelled while another waits for a run")
        reader.cancelled = True
        reader.at = None
        reader.loop.call_soon_threadsafe(reader.task.cancel)
        self.until("the cancelled reader to end", lambda: reader.outcome is not None)

    def release(self) -> None:
        held = self.held.pop(0)
        self.rig.submit_gates[held.token].set()
        if held.revision != self.revision:
            # G6: something changed after the submit, so it is cancelled.
            with pytest.raises(QueryCancelled):
                held.future.result(TIMEOUT)
            event("a held submit cancelled")
        else:
            want = _a_expect(self.states[held.state], "gated")
            assert held.future.result(TIMEOUT) == want

    # --- the end --------------------------------------------------------------

    def finish(self) -> None:
        while self.held:
            self.release()
        for _ in range(4 * READERS + 4):
            live = [r for r in self.readers.values() if r is not None]
            if not live:
                break
            for reader in live:
                if reader.at is not None:
                    reader.go_on()
            self.settle()
        assert all(r is None for r in self.readers.values()), "a reader never ended"
        for a, actor in self.actors.items():
            if actor is not None and a in self.blocks:
                self.end_block(a, self.send(actor, ("commit",)), commit=True)
        self.check()
        self.check_readers()
        self.check_shared_runs()
        for actor in self.actors.values():
            if actor is not None:
                assert self.send(actor, ("stop",)) is None
        # Every query, read afresh, gives the final state.
        rig = self.rig
        assert rig.combo() == _a_expect(self.state, "combo")
        future = rig.engine.submit(rig.combo)
        assert future.result(TIMEOUT) == _a_expect(self.state, "combo")

        def awaited(query: Any, *args: Any, **kwargs: Any) -> Any:
            async def call() -> Any:
                return await query(*args, **kwargs)

            return call()

        for name, query in (("view", rig.view), ("nest", rig.nest)):
            got = self.lts[0].start(awaited(query, next(rig.tokens))).result(TIMEOUT)
            assert got == _a_expect(self.state, name)
        got = self.lts[-1].start(awaited(rig.shared)).result(TIMEOUT)
        assert got == _a_expect(self.state, "shared")
        for snap, state in self.snaps:
            read = self.lts[0].start(awaited(rig.view, next(rig.tokens), snapshot=snap))
            assert read.result(TIMEOUT) == _a_expect(self.states[state], "view")

    def check_readers(self) -> None:
        for reader in self.finished:
            assert reader.outcome is not None
            status, got = reader.outcome
            if status == "raised":
                # Only a reader the test cancelled may end without a value.
                assert reader.cancelled, (reader.kind, got)
                assert isinstance(got, asyncio.CancelledError), got
                continue
            event(f"async reader: {reader.kind}")
            if reader.hi is not None and reader.hi > reader.lo:
                event("an async read spanning a change")
            expected = reader.expected(self)
            assert got in expected, (reader.kind, reader.lo, reader.hi, got, expected)

    def check_shared_runs(self) -> None:
        """G5: two runs of ``shared`` at one revision never overlap."""
        by_rev: dict[int, list[tuple[int, int]]] = {}
        for rev, start, end in self.rig.shared_runs:
            assert end is not None
            by_rev.setdefault(rev, []).append((start, end))
        for rev, spans in by_rev.items():
            spans.sort()
            for (_s1, e1), (s2, _e2) in itertools.pairwise(spans):
                assert s2 > e1, f"shared ran twice at once at revision {rev}: {spans}"

    def close(self) -> None:
        for snap, _state in self.snaps:
            snap.release()
        for gate in self.rig.submit_gates.values():
            gate.set()
        for lt in self.lts:
            try:
                lt.clear()
            except BaseException:
                # Not to be used again: a later example gets a new one.
                _LOOP_THREADS.remove(lt)
                lt.close()
                raise


ACTOR = st.integers(min_value=0, max_value=ACTORS)
SLOT = st.integers(min_value=0, max_value=READERS - 1)
READER_KIND = st.sampled_from(
    ("view", "nest", "shared", "shared", "snapview", "snapshared")
)
ORIGIN = st.one_of(st.none(), ACTOR)  # the driver, or an actor's task
WHICH = st.integers(min_value=0, max_value=1)  # which snapshot
A_CHANGE = st.one_of(
    st.tuples(st.just("append"), VALUES),
    st.tuples(st.just("pop")),
    st.tuples(st.just("insert"), INDEX, VALUES),
    st.tuples(st.just("setitem"), INDEX, VALUES),
    st.tuples(st.sampled_from(("add", "discard")), VALUES),
    st.tuples(st.just("put"), KEYS, VALUES),
    st.tuples(st.just("delkey"), KEYS),
)
A_STEP = st.one_of(
    st.tuples(st.just("begin"), ACTOR),
    st.tuples(st.just("begin"), ACTOR),
    st.tuples(st.sampled_from(("commit", "commit", "raise")), ACTOR),
    st.tuples(st.sampled_from(("cancel", "cancel_reading")), ACTOR),
    st.tuples(st.just("set"), ACTOR, st.sampled_from(("x", "y")), VALUES),
    st.tuples(st.just("bad"), ACTOR, st.sampled_from(("x", "y"))),
    st.tuples(st.just("mut"), ACTOR, A_CHANGE),
    st.tuples(st.just("mut"), ACTOR, A_CHANGE),
    st.tuples(
        st.just("side"),
        ACTOR,
        st.one_of(
            st.tuples(st.just("set"), st.sampled_from(("x", "y")), VALUES),
            st.tuples(st.just("mut"), A_CHANGE),
        ),
    ),
    st.tuples(st.just("aread"), ACTOR, st.sampled_from(("view", "nest"))),
    st.tuples(st.sampled_from(("sread", "asubmit", "nested", "spawn")), ACTOR),
    st.tuples(st.just("spawn"), ACTOR),
    st.tuples(st.sampled_from(("hold", "snapshot")), ACTOR),
    st.tuples(st.just("release")),
    st.tuples(st.just("snapshot"), ACTOR),
    st.tuples(st.just("reader"), SLOT, READER_KIND, ORIGIN, WHICH),
    st.tuples(st.just("reader"), SLOT, READER_KIND, ORIGIN, WHICH),
    # Every free slot at once, all asking for the same: joins (G5).
    st.tuples(st.just("readers"), READER_KIND, ORIGIN, WHICH),
    st.tuples(st.just("advance"), SLOT),
    st.tuples(st.just("advance"), SLOT),
    st.tuples(st.just("advance"), SLOT),
    st.tuples(st.just("rcancel"), SLOT),
)


A_PROGRAM = st.lists(A_STEP, min_size=15, max_size=50)
LOOPS = st.integers(min_value=1, max_value=2)
# Which loop each actor and each reader the driver starts runs on.
PLACEMENT = st.lists(
    st.integers(min_value=0, max_value=1),
    min_size=ACTORS + READERS,
    max_size=ACTORS + READERS,
)


def _run_async_program(
    backend: str | None, program: Any, loops: int, placement: list[int]
) -> None:
    with _cache_dir(backend) as path:
        kwargs = {} if path is None else {"cache_dir": path, "cache_backend": backend}
        engine = Engine(**kwargs)
        rig = AsyncRig(engine, named=path is not None)
        run = AsyncRun(rig, loops, placement)
        event(f"event loops: {loops}")
        try:
            run.check()
            for step in program:
                run.step(step)
            run.finish()
            final = run.state
        finally:
            try:
                run.close()
            finally:
                engine.shutdown()
        if path is None:
            return
        # The named collections on disk hold what the model committed.
        reopened = Engine(**kwargs)
        try:
            again = AsyncRig(reopened, named=True)
            assert again.contents() == {c: getattr(final, c) for c in A_COLLS}
        finally:
            reopened.shutdown()


@_settings(200)
@given(program=A_PROGRAM, loops=LOOPS, placement=PLACEMENT)
def test_async_transaction_programs(
    program: Any, loops: int, placement: list[int]
) -> None:
    _run_async_program(None, program, loops, placement)


@_settings(30)
@given(program=A_PROGRAM, loops=LOOPS, placement=PLACEMENT)
def test_async_transaction_programs_on_disk(
    program: Any, loops: int, placement: list[int]
) -> None:
    _run_async_program("mdbx", program, loops, placement)
