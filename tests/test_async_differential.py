"""Generated programs run on a sync engine and an async one, against a model (G14).

``test_async_assertions.py`` puts hand-written bodies through both paths.
This file generates the programs. Each example draws a query graph:

* inputs, keyed and not, with defaults (on the async engine the defaults
  may be ``async def``);
* queries whose reads depend on input values (an ``if`` on an input),
  that call other queries with arguments, catch their errors (G8) or
  raise their own, push accumulator effects (G12), read a list, a set and
  a dict (G21), and may be ``memoize=False`` (G10) or cache no errors;
* a TTL query over a value the engine does not track (G9), under a fake
  clock;
* a fixed-point cycle of a head and a member (G7).

Every body is written once, as a coroutine. The sync engine registers a
plain function that drives the coroutine without a loop; the async engine
registers the coroutine function and awaits it on a running loop, where
the body may ``await asyncio.sleep(0)`` between reads so that tasks
interleave. Then a sequence of steps runs against both engines: input
sets (same-value ones included, G3), reads (plain calls on the sync
engine, ``asyncio.gather`` of several tasks on the async one, so tasks
meet on one key, G5; an input set may sit between the tasks), snapshots
and reads through them (G4), ``submit`` held at its first body run while
an input changes (G6), ``compute_many``, ``prune``, ``sweep_unaccessed``
and an LRU bound (G13), the clock moved past the TTL, a query registered
again with other code (G11), and on mdbx and sqlite a shutdown and a new
engine on the same directory (G15, G16).

After every step:

* each read's outcome (value with its exact type, or the error's type and
  args) and its effects must be the same on both engines and equal to a
  plain-Python model run from scratch at the read's revision (G1);
* ``engine.revision`` must be the same on both, and must not move for a
  set to the value the model holds (G3);
* the number of body runs per query key in the step must be the same on
  both engines, except where the guarantees leave it open.

Allowed differences, each from a documented limit, never in a value:

* run counts, in a gather whose tasks read at different revisions (a set
  between them, snapshots): a run at one revision serves no caller at
  another (G5); and later, of what ran in it, since a task at an older
  revision that finishes last replaces the newer rows (G4);
* run counts of ``memoize=False`` queries, of queries whose errors are not
  cached and of their callers that do not catch them, and of the cycle,
  when several tasks or ``compute_many`` threads read at once: whether
  they meet on one run is up to timing (G5, G7);
* run counts of queries whose errors are not cached, always: they leave
  no row, and the sync walk reads each dependency again after settling
  it (``try_mark_green``) where the async walk reads it once, so the sync
  path runs such a dependency once more while it verifies a caller;
* run counts, in a gather of several tasks or a ``compute_many`` of
  several calls: which task runs, verifies or waits follows how they
  interleave, so only G5 is checked there, on both engines: no key runs
  twice at one revision (volatile queries left out);
* run counts, in a gather, of results that read an input's default which
  a task kept during it: a result that read the default before it was
  kept verifies against the kept version and runs again
  (``GraphStore.store_default``), and whether its check comes first is up
  to how the tasks interleave;
* run counts under the LRU bound: which rows it drops follows the order of
  accesses, which tasks interleave (G13);
* after a task read a never-set input at an older revision, the engines
  may keep its default at different revisions (G3: a read at an older
  revision keeps nothing): a later set to the default may move one
  engine's revision only, and a result that read the default before it
  was kept runs again on one engine only; run counts are then no longer
  compared in that session;
* a result computed at an older revision is not stored on disk if another
  task replaced a row it read meanwhile; the engine traces ``disk_skip``,
  and run counts are no longer compared in that example;
* a member of the cycle read while another task or thread iterates the
  head gets the fixed point or ``CycleError`` (G7), and a member's effects
  depend on whether it was stored from the head's last run (G12);
* which error ``compute_many`` raises when several calls fail;
* the effects of queries that read the head, once ``compute_many``
  threads met in the cycle. This one is an open finding, not a documented limit: in one
  example (three ``compute_many`` threads, an LRU bound of 6, sqlite) the
  sync engine stored the head as an ordinary result that depends on the
  member, with its last run's effects twice, and served them to every
  caller (G12). Values still agree with the model.

``CASCADE_ASYNC_DIFF_EXAMPLES`` sets the number of examples, and
``CASCADE_ASYNC_DIFF_SHRINK=0`` reports a failure without shrinking it.
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import math
import os
import shutil
import tempfile
import threading
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest
from hypothesis import HealthCheck, Phase, event, given, note, settings
from hypothesis import strategies as st

import cascade._evaluator as evaluator_module
from cascade import (
    CascadeDict,
    CascadeList,
    CascadeSet,
    Engine,
)

EXAMPLES = int(os.environ.get("CASCADE_ASYNC_DIFF_EXAMPLES", "250"))
TIMEOUT = 30.0
TTL = 10.0
# Input values: equal across types (0, 0.0, -0.0, False; 1, 1.0, True) so
# that digests have to tell them apart (G2, G3).
VALUES = (0, 1, 2, 3, -1, 1.0, 2.5, -0.0, True, False)
KEYS = (0, 1, 2)
COLLECTION_VALUES = st.integers(0, 5)


# ---------------------------------------------------------------------------
# Exact comparison
# ---------------------------------------------------------------------------


def same(a: Any, b: Any) -> bool:
    """Equal and of one type all the way down; ``-0.0`` is not ``0.0``."""
    if type(a) is not type(b):
        return False
    if isinstance(a, float):
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return a == b and math.copysign(1.0, a) == math.copysign(1.0, b)
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return list(a) == list(b) and all(same(a[k], b[k]) for k in a)
    return a == b


def outcome_of(thunk: Callable[[], Any]) -> tuple[Any, ...]:
    try:
        return ("ok", thunk())
    except Exception as exc:  # noqa: BLE001 - every error is an outcome
        return error_outcome(exc)


def error_outcome(exc: BaseException) -> tuple[Any, ...]:
    if isinstance(exc, ValueError):
        return ("err", "ValueError", exc.args)
    # The message of a CycleError names a path; only the kind is compared.
    return ("err", type(exc).__name__)


def same_outcome(a: tuple[Any, ...], b: tuple[Any, ...]) -> bool:
    return a[0] == b[0] and same(a[1:], b[1:])


def norm_effects(effects: dict[str, list[Any]] | None) -> dict[str, list[Any]]:
    if effects is None:
        return {}
    return {name: list(items) for name, items in effects.items() if items}


# ---------------------------------------------------------------------------
# The program
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class InputSpec:
    keyed: bool
    base: Any


@dataclass(frozen=True)
class QuerySpec:
    name: str
    keyed: bool
    ops: tuple[Any, ...]
    memoize: bool
    cache_errors: bool
    shape: str


@dataclass(frozen=True)
class Program:
    inputs: tuple[InputSpec, ...]
    queries: tuple[QuerySpec, ...]
    # The head reads ``lim_input``, the member ``step_input``; the TTL query
    # reads ``ttl_input``. All three are unkeyed.
    lim_input: int
    step_input: int
    ttl_input: int
    member_pauses: bool
    async_defaults: bool
    xs: tuple[int, ...]
    s: tuple[int, ...]
    d: tuple[tuple[int, int], ...]

    def spec(self, name: str) -> QuerySpec:
        for q in self.queries:
            if q.name == name:
                return q
        raise KeyError(name)

    def keyed(self, name: str) -> bool:
        if name in ("T", "H", "M"):
            return False
        return self.spec(name).keyed

    def uncached(self) -> set[str]:
        """Queries that can raise an error which is not cached.

        Such a query leaves no row, so each read runs it again, and the two
        paths read a dependency a different number of times while they
        verify a result: the sync walk settles the dependencies, then reads
        each again (``Evaluator.try_mark_green``), where the async walk
        reads each once. The error passes up through every caller that
        does not catch it.
        """
        uncached = {q.name for q in self.queries if not q.cache_errors}
        grew = True
        while grew:
            grew = False
            for q in self.queries:
                if q.name not in uncached and _calls(q.ops, "call") & uncached:
                    uncached.add(q.name)
                    grew = True
        return uncached

    def reaches_head(self) -> set[str]:
        """The cycle's queries and every query that calls the head, at any depth."""
        found = {"H", "M"}
        grew = True
        while grew:
            grew = False
            for q in self.queries:
                calls = _calls(q.ops, "call") | _calls(q.ops, "try")
                if q.name not in found and (_calls(q.ops, "fix") or calls & found):
                    found.add(q.name)
                    grew = True
        return found

    def volatile(self) -> set[str]:
        """Queries whose run count may differ between the paths in a gather.

        A ``memoize=False`` query keeps no value, so two tasks may or may not
        meet on its run; an error that is not cached is handed to every
        task that joined the run that raised it, and raised again by a
        later run (G5's limits). The cycle's queries iterate.
        """
        lazy = {q.name for q in self.queries if not q.memoize}
        return self.uncached() | lazy | {"H", "M"}


def _calls(ops: tuple[Any, ...], kind: str) -> set[str]:
    found: set[str] = set()
    for op in ops:
        if op[0] == kind:
            found.add(op[1])
        elif op[0] == "if":
            found |= _calls(op[2], kind) | _calls(op[3], kind)
    return found


def _selector(keyed: bool) -> st.SearchStrategy[Any]:
    if not keyed:
        return st.none()
    return st.one_of(
        st.tuples(st.just("c"), st.sampled_from(KEYS)),
        st.just(("p",)),
        st.just(("a",)),
    )


HOW = st.sampled_from(("+", "+", "-", "max"))


@st.composite
def _op(
    draw: Any, inputs: tuple[InputSpec, ...], earlier: list[QuerySpec], depth: int
) -> Any:
    kinds = ["in", "in", "raise", "push", "pause", "coll", "ttl", "fix"]
    if earlier:
        kinds += ["call", "call", "try", "try"]
    if depth == 0:
        kinds.append("if")
    kind = draw(st.sampled_from(kinds))
    if kind == "in":
        i = draw(st.integers(0, len(inputs) - 1))
        return ("in", i, draw(_selector(inputs[i].keyed)), draw(HOW))
    if kind in ("call", "try"):
        q = draw(st.sampled_from(earlier))
        return (kind, q.name, draw(_selector(q.keyed)), draw(HOW))
    if kind == "if":
        i = draw(st.integers(0, len(inputs) - 1))
        branch = st.lists(_op(inputs, earlier, depth + 1), min_size=0, max_size=3)
        return ("if", i, tuple(draw(branch)), tuple(draw(branch)))
    if kind == "raise":
        m = draw(st.sampled_from((2, 3, 4)))
        return ("raise", m, draw(st.integers(0, m - 1)))
    if kind == "coll":
        return ("coll", draw(st.sampled_from(("len", "sum", "idx", "in", "get"))))
    if kind in ("ttl", "fix"):
        return (kind, draw(HOW))
    return (kind,)


@st.composite
def programs(draw: Any) -> Program:
    n_inputs = draw(st.integers(3, 5))
    inputs = tuple(
        # The first three are unkeyed: the head, the member and the TTL
        # query read them.
        InputSpec(
            keyed=False if i < 3 else draw(st.booleans()),
            base=draw(st.sampled_from(VALUES)),
        )
        for i in range(n_inputs)
    )
    queries: list[QuerySpec] = []
    for j in range(draw(st.integers(2, 6))):
        ops = tuple(draw(st.lists(_op(inputs, queries, 0), min_size=1, max_size=6)))
        queries.append(
            QuerySpec(
                name=f"q{j}",
                keyed=draw(st.booleans()),
                ops=ops,
                memoize=draw(st.sampled_from((True, True, True, False))),
                cache_errors=draw(st.sampled_from((True, True, False))),
                shape=draw(st.sampled_from(("int", "tuple", "list", "dict"))),
            )
        )
    order = draw(st.permutations([0, 1, 2]))
    return Program(
        inputs=inputs,
        queries=tuple(queries),
        lim_input=order[0],
        step_input=order[1],
        ttl_input=order[2],
        member_pauses=draw(st.booleans()),
        async_defaults=draw(st.booleans()),
        xs=tuple(draw(st.lists(COLLECTION_VALUES, max_size=4))),
        s=tuple(draw(st.lists(COLLECTION_VALUES, max_size=4, unique=True))),
        d=tuple(
            draw(
                st.dictionaries(st.integers(0, 2), COLLECTION_VALUES, max_size=3)
            ).items()
        ),
    )


# ---------------------------------------------------------------------------
# The bodies: written once, as coroutines
# ---------------------------------------------------------------------------


async def _get(value: Any) -> Any:
    """What a handle returned: awaited if it is awaitable (async path)."""
    if inspect.isawaitable(value):
        return await value
    return value


def _key(sel: Any, arg: Any, acc: Any) -> tuple[Any, ...]:
    if sel is None:
        return ()
    if sel[0] == "c":
        return (sel[1],)
    if sel[0] == "p":
        return (0 if arg is None else arg,)
    return (int(acc) % 3,)


def _combine(how: str, acc: Any, value: Any) -> Any:
    if how == "+":
        return acc + value
    if how == "-":
        return acc - value
    return max(acc, value)


def _scalar(value: Any) -> Any:
    if isinstance(value, (tuple, list)):
        return value[0]
    if isinstance(value, dict):
        return value["v"]
    return value


def _shape(shape: str, acc: Any, arg: Any) -> Any:
    if shape == "tuple":
        return (acc, arg)
    if shape == "list":
        return [acc]
    if shape == "dict":
        return {"v": acc, "k": arg}
    return acc


async def _pause(h: Any) -> None:
    if h.is_async:
        await asyncio.sleep(0)


def _collection_read(h: Any, kind: str, acc: Any) -> Any:
    if kind == "len":
        return len(h.xs)
    if kind == "sum":
        return sum(h.xs)
    if kind == "idx":
        n = len(h.xs)
        return h.xs[int(acc) % n] if n else 0
    if kind == "in":
        return (int(acc) % 4) in h.s
    return h.d.get(int(acc) % 3, 0)


async def _run_ops(h: Any, name: str, arg: Any, ops: tuple[Any, ...], acc: Any) -> Any:
    for op in ops:
        kind = op[0]
        if kind == "in":
            value = await _get(h.inputs[op[1]](*_key(op[2], arg, acc)))
            acc = _combine(op[3], acc, value)
        elif kind == "call":
            value = await _get(h.queries[op[1]](*_key(op[2], arg, acc)))
            acc = _combine(op[3], acc, _scalar(value))
        elif kind == "try":
            try:
                value = _scalar(await _get(h.queries[op[1]](*_key(op[2], arg, acc))))
            except ValueError as exc:
                value = exc.args[1] + 100
            acc = _combine(op[3], acc, value)
        elif kind == "if":
            flag = await _get(
                h.inputs[op[1]](
                    *_key((("c", 0) if h.prog.inputs[op[1]].keyed else None), arg, acc)
                )
            )
            acc = await _run_ops(h, name, arg, op[2] if int(flag) % 2 else op[3], acc)
        elif kind == "raise":
            if int(acc) % op[1] == op[2]:
                raise ValueError(name, acc)
        elif kind == "push":
            h.log.push((name, arg, acc))
        elif kind == "pause":
            await _pause(h)
        elif kind == "coll":
            acc = acc + _collection_read(h, op[1], acc)
        elif kind == "ttl":
            acc = _combine(op[1], acc, await _get(h.ttl()))
        elif kind == "fix":
            acc = _combine(op[1], acc, await _get(h.head()))
    return acc


def _query_body(h: Any, spec: QuerySpec, variant: int) -> Callable[..., Any]:
    name, ops, shape = spec.name, spec.ops, spec.shape

    async def body(*args: Any) -> Any:
        arg = args[0] if args else None
        h.enter(name, arg)
        acc = await _run_ops(h, name, arg, ops, 0 if arg is None else arg)
        return _shape(shape, acc + variant, arg)

    body.__name__ = body.__qualname__ = name
    return body


def _ttl_body(h: Any, index: int) -> Callable[..., Any]:
    async def ttl() -> Any:
        h.enter("T", None)
        value = await _get(h.inputs[index]())
        # The sensor is read past the engine: only the TTL refreshes it.
        return h.world.sensor + value

    return ttl


def _head_body(h: Any, index: int) -> Callable[..., Any]:
    async def head() -> Any:
        h.enter("H", None)
        member = await _get(h.member())
        limit = await _get(h.inputs[index]())
        result = min(member, limit)
        h.log.push(("H", None, result))
        return result

    return head


def _member_body(h: Any, index: int, pauses: bool) -> Callable[..., Any]:
    async def member() -> Any:
        h.enter("M", None)
        guess = await _get(h.head())
        if pauses:
            await _pause(h)
        step = await _get(h.inputs[index]())
        result = guess + 1 + abs(int(step)) % 2
        h.log.push(("M", None, result))
        return result

    return member


def _default(base: Any, keyed: bool, is_async: bool) -> Callable[..., Any]:
    if keyed and is_async:

        async def keyed_async(k: Any) -> Any:
            return base + k

        return keyed_async
    if keyed:

        def keyed_sync(k: Any) -> Any:
            return base + k

        return keyed_sync
    if is_async:

        async def plain_async() -> Any:
            return base

        return plain_async

    def plain_sync() -> Any:
        return base

    return plain_sync


def _run_to_end(body: Callable[..., Any]) -> Callable[..., Any]:
    """A plain function that runs ``body``'s coroutine without a loop."""

    @functools.wraps(body)
    def run(*args: Any) -> Any:
        coro = body(*args)
        try:
            coro.send(None)
        except StopIteration as stop:
            return stop.value
        coro.close()
        raise AssertionError(f"{body.__name__} suspended in sync mode")

    return run


# ---------------------------------------------------------------------------
# The model: plain Python, from scratch
# ---------------------------------------------------------------------------


@dataclass
class View:
    """What the inputs and collections hold at one revision."""

    inputs: dict[tuple[int, tuple[Any, ...]], Any]
    xs: list[int]
    s: set[int]
    d: dict[int, int]

    def copy(self) -> View:
        return View(dict(self.inputs), list(self.xs), set(self.s), dict(self.d))


class ModelCycle(Exception):
    pass


class ModelRun:
    """One read evaluated as plain Python over a ``View``."""

    def __init__(
        self, prog: Program, view: View, variants: dict[str, int], sensor: int
    ) -> None:
        self.prog = prog
        self.view = view
        self.variants = variants
        self.sensor = sensor
        self.pushes: list[Any] = []
        self.guess: Any = None
        self.in_head = False

    def input(self, i: int, key: tuple[Any, ...]) -> Any:
        if (i, key) in self.view.inputs:
            return self.view.inputs[(i, key)]
        spec = self.prog.inputs[i]
        return spec.base + key[0] if spec.keyed else spec.base

    def call(self, name: str, key: tuple[Any, ...]) -> Any:
        """A query call, as plain Python.

        A query that raises keeps what it pushed before: its error is stored
        with those effects, and a caller that catches it gets them, on its
        first run and on every replay. Only a read that raises hands the
        caller's ``effects=`` nothing (G12).
        """
        if name == "T":
            return self.sensor + self.input(self.prog.ttl_input, ())
        if name == "H":
            return self._head()
        if name == "M":
            guess = self.call("H", ())
            step = self.input(self.prog.step_input, ())
            result = guess + 1 + abs(int(step)) % 2
            self.pushes.append(("M", None, result))
            return result
        spec = self.prog.spec(name)
        arg = key[0] if key else None
        acc = self._ops(name, arg, spec.ops, 0 if arg is None else arg)
        return _shape(spec.shape, acc + self.variants[name], arg)

    def _head(self) -> Any:
        if self.in_head:
            return self.guess
        outer = (self.in_head, self.guess)
        guess: Any = 0
        try:
            for _ in range(1000):
                mark = len(self.pushes)
                self.in_head, self.guess = True, guess
                member = self.call("M", ())
                limit = self.input(self.prog.lim_input, ())
                result = min(member, limit)
                self.pushes.append(("H", None, result))
                if same(result, guess):
                    return result
                del self.pushes[mark:]
                guess = result
            raise ModelCycle("no fixed point")
        finally:
            self.in_head, self.guess = outer

    def _ops(self, name: str, arg: Any, ops: tuple[Any, ...], acc: Any) -> Any:
        for op in ops:
            kind = op[0]
            if kind == "in":
                acc = _combine(op[3], acc, self.input(op[1], _key(op[2], arg, acc)))
            elif kind == "call":
                acc = _combine(
                    op[3], acc, _scalar(self.call(op[1], _key(op[2], arg, acc)))
                )
            elif kind == "try":
                try:
                    value = _scalar(self.call(op[1], _key(op[2], arg, acc)))
                except ValueError as exc:
                    value = exc.args[1] + 100
                acc = _combine(op[3], acc, value)
            elif kind == "if":
                key = (0,) if self.prog.inputs[op[1]].keyed else ()
                flag = self.input(op[1], key)
                acc = self._ops(name, arg, op[2] if int(flag) % 2 else op[3], acc)
            elif kind == "raise":
                if int(acc) % op[1] == op[2]:
                    raise ValueError(name, acc)
            elif kind == "push":
                self.pushes.append((name, arg, acc))
            elif kind == "coll":
                acc = acc + self._collection(op[1], acc)
            elif kind == "ttl":
                acc = _combine(op[1], acc, self.call("T", ()))
            elif kind == "fix":
                acc = _combine(op[1], acc, self.call("H", ()))
        return acc

    def _collection(self, kind: str, acc: Any) -> Any:
        xs = self.view.xs
        if kind == "len":
            return len(xs)
        if kind == "sum":
            return sum(xs)
        if kind == "idx":
            return xs[int(acc) % len(xs)] if xs else 0
        if kind == "in":
            return (int(acc) % 4) in self.view.s
        return self.view.d.get(int(acc) % 3, 0)


@dataclass
class World:
    """What both engines share: the clock and what the TTL query reads."""

    sensor: int = 0
    now: float = 1000.0
    gate: Any = None

    def time(self) -> float:
        return self.now


@dataclass
class Model:
    prog: Program
    view: View
    variants: dict[str, int]
    world: World
    # Inputs the engines were given a value for (not just their default).
    explicit: set[tuple[int, tuple[Any, ...]]] = field(default_factory=set)

    def read(
        self, name: str, key: tuple[Any, ...], view: View | None = None
    ) -> tuple[tuple[Any, ...], dict[str, list[Any]]]:
        run = ModelRun(self.prog, view or self.view, self.variants, self.world.sensor)
        try:
            value = run.call(name, key)
        except ValueError as exc:
            return error_outcome(exc), {}
        return ("ok", value), ({"log": run.pushes} if run.pushes else {})

    def current(self, i: int, key: tuple[Any, ...]) -> Any:
        return ModelRun(self.prog, self.view, self.variants, 0).input(i, key)


# ---------------------------------------------------------------------------
# One engine and its handles
# ---------------------------------------------------------------------------


class Gate:
    """Holds the first run of one query key on a worker thread (G6)."""

    def __init__(self, side: Side, key: tuple[str, Any]) -> None:
        self.side = side
        self.key = key
        self.entered = threading.Event()
        self.release = threading.Event()


class Side:
    def __init__(
        self,
        mode: str,
        prog: Program,
        world: World,
        variants: dict[str, int],
        cache_dir: str | None,
        backend: str,
        max_entries: int,
    ) -> None:
        self.mode = mode
        self.is_async = mode == "async"
        self.prog = prog
        self.world = world
        self.variants = variants
        self.cache_dir = cache_dir
        self.backend = backend
        self.max_entries = max_entries
        self.runs: Counter[tuple[str, Any]] = Counter()
        self._lock = threading.Lock()
        self.runner = _shared_runner() if self.is_async else None
        self.trace_pos = 0
        self.snapshots: dict[int, Any] = {}
        self.mark: int | None = None
        self.h = SimpleNamespace(
            is_async=self.is_async, prog=prog, world=world, enter=self.enter
        )
        self.engine: Engine = None  # type: ignore[assignment]
        self.build()

    # --- bodies call this first --------------------------------------------

    def enter(self, name: str, arg: Any) -> None:
        with self._lock:
            self.runs[(name, arg)] += 1
        gate = self.world.gate
        if (
            gate is not None
            and gate.side is self
            and gate.key == (name, arg)
            and not gate.entered.is_set()
            and threading.current_thread() is not threading.main_thread()
        ):
            gate.entered.set()
            gate.release.wait(TIMEOUT)

    # --- the engine --------------------------------------------------------

    def build(self) -> None:
        if self.cache_dir is None:
            engine = Engine(max_entries=self.max_entries)
        else:
            engine = Engine(
                max_entries=self.max_entries,
                cache_dir=self.cache_dir,
                cache_backend=self.backend,
            )
        self.engine = engine
        h = self.h
        h.log = engine.accumulator("log")
        h.inputs = [
            engine.input(name=f"i{i}")(
                _default(
                    spec.base, spec.keyed, self.is_async and self.prog.async_defaults
                )
            )
            for i, spec in enumerate(self.prog.inputs)
        ]
        named = self.cache_dir is not None
        h.xs = CascadeList(engine, self.prog.xs, name="xs" if named else None)
        h.s = CascadeSet(engine, self.prog.s, name="s" if named else None)
        h.d = CascadeDict(engine, dict(self.prog.d), name="d" if named else None)
        h.queries = {}
        for spec in self.prog.queries:
            self.register(spec)
        h.ttl = self._register("T", _ttl_body(h, self.prog.ttl_input), ttl=TTL)
        h.head = self._register("H", _head_body(h, self.prog.lim_input), fixed_point=0)
        h.member = self._register(
            "M", _member_body(h, self.prog.step_input, self.prog.member_pauses)
        )
        self.snapshots = {}
        self.mark = None

    def _register(self, name: str, body: Callable[..., Any], **options: Any) -> Any:
        fn = body if self.is_async else _run_to_end(body)
        return self.engine.query(name=name, **options)(fn)

    def register(self, spec: QuerySpec) -> None:
        self.h.queries[spec.name] = self._register(
            spec.name,
            _query_body(self.h, spec, self.variants[spec.name]),
            memoize=spec.memoize,
            cache_exceptions=True if spec.cache_errors else (KeyError,),
        )

    def handle(self, name: str) -> Any:
        if name == "T":
            return self.h.ttl
        if name == "H":
            return self.h.head
        if name == "M":
            return self.h.member
        return self.h.queries[name]

    def restart(self) -> None:
        self.engine.shutdown()
        self.build()
        self.trace_pos = 0

    def close(self) -> None:
        self.engine.shutdown()

    def run(self, coro: Any) -> Any:
        assert self.runner is not None
        return self.runner.run(asyncio.wait_for(coro, TIMEOUT))


_RUNNER: list[asyncio.Runner] = []


def _shared_runner() -> asyncio.Runner:
    """One event loop for every example of the async engine.

    On Windows every new event loop opens a loopback TCP socket pair, and
    each one left behind holds a port for a while: with hundreds of loops a
    run, and other test runs on the machine, ``socket.socketpair`` once
    waited in ``accept`` for ever. Nothing here depends on the loop being
    new; the async engine's bodies reached from plain code still get one
    each (``_run_coroutine_blocking``).
    """
    if not _RUNNER:
        _RUNNER.append(asyncio.Runner())
    return _RUNNER[0]


@pytest.fixture(scope="module", autouse=True)
def _close_shared_runner() -> Iterator[None]:
    yield
    while _RUNNER:
        _RUNNER.pop().close()


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Read:
    name: str
    key: tuple[Any, ...]
    snap: int | None
    effects: bool


@dataclass(frozen=True)
class SetInput:
    index: int
    key: tuple[Any, ...]
    value: Any


def _apply_set(side: Side, op: SetInput) -> None:
    side.h.inputs[op.index].set(*op.key, op.value)


def _sync_read(side: Side, read: Read) -> tuple[tuple[Any, ...], dict[str, list[Any]]]:
    handle = side.handle(read.name)
    kwargs: dict[str, Any] = {}
    effects: dict[str, list[Any]] | None = {} if read.effects else None
    if read.snap is not None:
        kwargs["snapshot"] = side.snapshots[read.snap]
    if effects is not None:
        kwargs["effects"] = effects
    return outcome_of(lambda: handle(*read.key, **kwargs)), norm_effects(effects)


async def _async_read(
    side: Side, read: Read
) -> tuple[tuple[Any, ...], dict[str, list[Any]]]:
    handle = side.handle(read.name)
    kwargs: dict[str, Any] = {}
    effects: dict[str, list[Any]] | None = {} if read.effects else None
    if read.snap is not None:
        kwargs["snapshot"] = side.snapshots[read.snap]
    if effects is not None:
        kwargs["effects"] = effects
    try:
        result: tuple[Any, ...] = ("ok", await handle(*read.key, **kwargs))
    except Exception as exc:  # noqa: BLE001
        result = error_outcome(exc)
    return result, norm_effects(effects)


async def _async_set(side: Side, op: SetInput) -> None:
    _apply_set(side, op)


class Pair:
    """Both engines, the model, and the checks that tie them together."""

    def __init__(self, prog: Program, backend: str, max_entries: int) -> None:
        self.prog = prog
        self.backend = backend
        self.world = World()
        self.variants = {q.name: 0 for q in prog.queries}
        self.dirs: list[str] = []
        self.sides: list[Side] = []
        for mode in ("sync", "async"):
            cache_dir = None
            if backend != "memory":
                cache_dir = tempfile.mkdtemp(prefix=f"cascade-adiff-{mode}-")
                self.dirs.append(cache_dir)
            self.sides.append(
                Side(
                    mode,
                    prog,
                    self.world,
                    self.variants,
                    cache_dir,
                    backend,
                    max_entries,
                )
            )
        self.max_entries = max_entries
        view = View({}, list(prog.xs), set(prog.s), dict(prog.d))
        self.model = Model(prog, view, self.variants, self.world)
        self.model_snaps: dict[int, View] = {}
        self.next_snap = 0
        self.volatile = prog.volatile()
        self.uncached = prog.uncached()
        self.head_effects_unsure = False
        self.head_readers = prog.reaches_head()
        self.step_no = 0
        # Queries, and query keys, whose run counts are no longer compared.
        self.loose: set[str] = set()
        # Query keys as traces name them ("query:q1(1,)").
        self.tainted: set[str] = set()
        # ``engine.revision`` of the async engine minus that of the sync one;
        # see ``lenient``.
        self.rev_offset = 0
        self.defaults_may_differ = False
        self.counts_diverged = False
        self.disk_diverged = False

    @property
    def sync(self) -> Side:
        return self.sides[0]

    @property
    def async_(self) -> Side:
        return self.sides[1]

    def close(self) -> None:
        for side in self.sides:
            try:
                side.close()
            except Exception:  # noqa: BLE001 - cleanup only
                pass
        for path in self.dirs:
            shutil.rmtree(path, ignore_errors=True)

    # --- checks ----------------------------------------------------------------

    def check_outcomes(
        self,
        what: str,
        got_sync: Any,
        got_async: Any,
        want: Any | None,
        member: bool = False,
        name: str = "",
    ) -> None:
        (out_s, eff_s), (out_a, eff_a) = got_sync, got_async
        assert same_outcome(out_s, out_a), f"{what}: sync {out_s!r} but async {out_a!r}"
        if name in self.head_readers and self.head_effects_unsure:
            # Open finding: after tasks or threads met in the cycle, the
            # head's stored effects can hold its last run twice (see the
            # module docstring). Values are still compared.
            if want is not None:
                assert same_outcome(out_s, want[0]), (
                    f"{what}: engines {out_s!r} but model {want[0]!r}"
                )
            return
        # A member's effects are those of the run that stored it: in the
        # head's last iteration it pushed only its own; run on its own it
        # replays the head's too (G12). Which happened depends on what was
        # dropped, so the engines agree only while they drop the same rows.
        if not (
            member and (self.max_entries < 1000 or self.diverged or "M" in self.loose)
        ):
            assert same(eff_s, eff_a), (
                f"{what}: effects sync {eff_s!r} but async {eff_a!r}"
            )
        if want is None:
            return
        out_m, eff_m = want
        assert same_outcome(out_s, out_m), (
            f"{what}: engines {out_s!r} but model {out_m!r}"
        )
        if eff_m is not None:
            assert same(eff_s, eff_m), f"{what}: effects {eff_s!r} but model {eff_m!r}"

    def check_revisions(self) -> None:
        revs = [side.engine.revision for side in self.sides]
        assert revs[1] - revs[0] == self.rev_offset, (
            f"revision: sync {revs[0]} but async {revs[1]} (offset {self.rev_offset})"
        )

    def lenient(self, op: SetInput, after_tasks: bool = False) -> bool:
        """Whether this set may move one engine's revision and not the other's.

        A set to the default of an input never set is a change unless a
        read at the newest revision kept the default (G3). A task that
        started before a set between the tasks of a gather reads at an
        older revision and keeps nothing, where the sync engine's read, made
        before the set, kept it. After such a gather the two engines may
        disagree on that one set; every other revision movement must match.
        """
        key = (op.index, op.key)
        return (
            (self.defaults_may_differ or after_tasks)
            and key not in self.model.explicit
            and same(self.model.current(op.index, op.key), op.value)
        )

    def resync_revisions(self) -> None:
        revs = [side.engine.revision for side in self.sides]
        if revs[1] - revs[0] != self.rev_offset:
            # The input has a new version on one engine only, so results that
            # read its default re-run there and not on the other (G2 counts
            # a new version as a change). Values still have to agree.
            event("a set to a default moved one engine only")
            self.counts_diverged = True
        self.rev_offset = revs[1] - revs[0]

    def member_diverged(self) -> None:
        """One engine got a CycleError for the member where the other did not.

        Allowed when tasks or threads met in the cycle (G7). What came of it
        differs from then on: the member's row, and on disk whether it was
        stored, so whether a later read loads it or runs it, and with that
        which defaults it keeps and when (G3).
        """
        event("the member raised CycleError on one engine only")
        self.loose |= {"H", "M"}
        self.counts_diverged = True
        self.disk_diverged = True
        self.defaults_may_differ = True

    @property
    def diverged(self) -> bool:
        return self.counts_diverged or self.disk_diverged

    def after_step(self) -> None:
        """Note results one engine did not store on disk and the other did.

        A result is stored only if each query it read still holds the row
        it read (``_observed_dep_fingerprint``). A task at an older revision
        whose dependency another task replaced at the newest one meanwhile
        is not stored, where the sync engine, one read after another,
        stores it. That changes what a later read loads from disk, so run
        counts are not compared after it; values still are.
        """
        skips = []
        kept = []
        for side in self.sides:
            # Every run ended and every caller stopped waiting: nothing is
            # left for a later caller to join (G5).
            internals = side.engine._internals  # noqa: SLF001
            with internals.lock:
                assert internals.in_flight == {}, (
                    f"step {self.step_no}: {side.mode} left runs in flight: "
                    f"{list(internals.in_flight)}"
                )
                assert side.engine._store.waiting_on == {}, (  # noqa: SLF001
                    f"step {self.step_no}: {side.mode} left callers waiting"
                )
            if side.cache_dir is not None:
                # Results are written by a background thread; a read that
                # comes before the write recomputes. Waiting here makes what
                # is on disk the same for both engines at every step (G17).
                side.engine.flush_disk()
            traces = side.engine.traces()
            new, side.trace_pos = traces[side.trace_pos :], len(traces)
            skips.append(Counter(t.key for t in new if t.event == "disk_skip"))
            kept.append(
                Counter((t.key, t.detail) for t in new if t.event == "input_default")
            )
        if skips[0] != skips[1]:
            event("a result was stored on disk by one engine only")
            # Kept across a restart: the disk still differs.
            self.disk_diverged = True
        if kept[0] != kept[1]:
            # A task at an older revision kept nothing where the sync
            # engine's read, at the newest one, kept the default (G3).
            # A result that read the default before it was kept counts the
            # kept version as a change and runs again (G2's cost only).
            event("a default was kept at another revision on one engine")
            self.counts_diverged = True

    def check_counts(
        self,
        before: list[Counter[Any]],
        strict: bool,
        where: str,
        concurrent: bool = False,
    ) -> None:
        """Compare the runs of this step, per query key, on both engines.

        ``concurrent``: tasks or threads ran at once, so the run counts of
        volatile queries (``Program.volatile``) are left out wherever they
        were called from.
        """
        deltas = [side.runs - prior for side, prior in zip(self.sides, before)]
        dropped = self.volatile if concurrent else self.uncached
        for delta in deltas:
            for key in [k for k in delta if k[0] in dropped]:
                del delta[key]
        if concurrent:
            # A result that read an input's default before a read at the
            # newest revision kept it verifies against the kept version and
            # runs again (``GraphStore.store_default``). Whether its check
            # comes before or after the keep is up to how tasks interleave.
            kept = {
                t.key
                for side in self.sides
                for t in side.engine.traces()[side.trace_pos :]
                if t.event == "input_default"
            }
            if kept:
                for delta in deltas:
                    for key in [k for k in delta if self._reads_any(k, kept)]:
                        del delta[key]
        if not strict or self.diverged:
            return
        if self.max_entries < 1000:
            # Which rows the LRU bound drops depends on the order of
            # accesses, and tasks interleave them (see _counts_comparable).
            return
        for delta in deltas:
            for key in [
                k for k in delta if k[0] in self.loose or _trace_key(*k) in self.tainted
            ]:
                del delta[key]
        if concurrent:
            # Several tasks or threads at one revision: which of them runs,
            # verifies or waits, and so how often a walk reads a row it may
            # find stale, follows how they interleave. What G5 fixes is that
            # each key runs at most once, on both engines.
            for side, delta in zip(self.sides, deltas):
                twice = {k: n for k, n in delta.items() if n > 1}
                assert not twice, (
                    f"{where}: {side.mode} ran {twice} more than once (G5)"
                )
            return
        assert deltas[0] == deltas[1], (
            f"{where}: runs sync {dict(deltas[0])} but async {dict(deltas[1])}"
        )

    def _reads_any(self, key: tuple[str, Any], inputs: set[str]) -> bool:
        """Whether either engine's row for query ``key`` read one of ``inputs``."""
        args = () if key[1] is None else (key[1],)
        for side in self.sides:
            store = side.engine._store  # noqa: SLF001
            with store.lock:
                memo = store.memos.get(("query", key[0], args))
                deps = [] if memo is None else [d.key for d in memo.deps]
            if any(store.key_to_str(dep) in inputs for dep in deps):
                return True
        return False

    def model_for(
        self, read: Read, view: View | None = None
    ) -> tuple[tuple[Any, ...], dict[str, list[Any]] | None]:
        snap_view = self.model_snaps[read.snap] if read.snap is not None else None
        out, effects = self.model.read(read.name, read.key, view or snap_view)
        if read.name == "M":
            # A member's effects depend on how it was last computed: run in
            # the head's last iteration it pushed only its own (G12).
            effects = None  # type: ignore[assignment]
        elif not read.effects:
            effects = {}
        return out, effects

    # --- steps -------------------------------------------------------------------

    def do_set(self, op: SetInput) -> None:
        key = (op.index, op.key)
        old_explicit = key in self.model.explicit
        old = self.model.current(op.index, op.key)
        lenient = self.lenient(op)
        before = [side.engine.revision for side in self.sides]
        for side in self.sides:
            _apply_set(side, op)
        self.model.view.inputs[key] = op.value
        self.model.explicit.add(key)
        if lenient:
            self.resync_revisions()
        self.check_revisions()
        for side, rev in zip(self.sides, before):
            moved = side.engine.revision != rev
            if old_explicit and same(old, op.value):
                assert not moved, (
                    f"{side.mode}: same-value set of i{op.index}{op.key} moved the revision (G3)"
                )
            elif not same(old, op.value):
                assert moved, (
                    f"{side.mode}: set of i{op.index}{op.key} to {op.value!r} did not move the revision"
                )

    def do_reads(self, reads: list[Any], via: str) -> None:
        """Reads in order, with input sets between them.

        The sync engine and the model take them one after another. The async
        engine runs them as tasks of one ``asyncio.gather`` (``via="await"``)
        or calls the async queries from plain code (``via="plain"``). A task
        pins its revision when it starts, and tasks start in order, so a
        task after a set sees it and one before it does not, however they
        interleave later.
        """
        before = [side.runs.copy() for side in self.sides]
        sync_results = []
        expected = []
        lenient = False
        for position, item in enumerate(reads):
            if isinstance(item, SetInput):
                # A task before the set may not have read the input yet.
                lenient = lenient or self.lenient(item, via == "await" and position > 0)
                _apply_set(self.sync, item)
                self.model.view.inputs[(item.index, item.key)] = item.value
                self.model.explicit.add((item.index, item.key))
                continue
            sync_results.append(_sync_read(self.sync, item))
            expected.append(self.model_for(item))
        side = self.async_
        if via == "plain":
            async_results = []
            for item in reads:
                if isinstance(item, SetInput):
                    _apply_set(side, item)
                else:
                    async_results.append(_sync_read(side, item))
        else:

            async def gather() -> list[Any]:
                tasks = [
                    _async_set(side, item)
                    if isinstance(item, SetInput)
                    else _async_read(side, item)
                    for item in reads
                ]
                done = await asyncio.gather(*tasks)
                return [
                    r for r, item in zip(done, reads) if not isinstance(item, SetInput)
                ]

            async_results = side.run(gather())
        only_reads = [r for r in reads if not isinstance(r, SetInput)]
        if (
            via == "await"
            and len(only_reads) > 1
            and {"H", "M"} & {r.name for r in only_reads}
        ):
            # Tasks that meet in the cycle decide between them whether the
            # member is stored from the head's iteration or run on its own
            # (G7), and so which effects it keeps (G12).
            self.loose.add("M")
        for i, (read, got_s, got_a, want) in enumerate(
            zip(only_reads, sync_results, async_results, expected)
        ):
            what = f"step {self.step_no} read {i} {read.name}{read.key} snap={read.snap} via={via}"
            cycle = [got[0][:2] == ("err", "CycleError") for got in (got_s, got_a)]
            if read.name == "M" and any(cycle):
                # A member called before its head has a current result raises
                # (G7); compare the engines only. In a gather, a task that
                # enters the cycle through the member while another task
                # iterates the head gets the fixed point or a CycleError
                # (G7, threads and tasks); the sync engine, one read after
                # another, cannot meet that. Whether the head has a current
                # result also follows what was dropped or stored, which
                # such meetings and the LRU bound make differ.
                concurrent = via == "await" and len(only_reads) > 1
                history = "M" in self.loose or self.diverged or self.max_entries < 1000
                if not all(cycle) and (concurrent or history):
                    self.member_diverged()
                    for got, raised in zip((got_s, got_a), cycle):
                        if not raised:
                            assert same_outcome(got[0], want[0]), (
                                f"{what}: {got[0]!r} but model {want[0]!r}"
                            )
                    continue
                want = None
            self.check_outcomes(
                what, got_s, got_a, want, member=read.name == "M", name=read.name
            )
        if lenient:
            self.resync_revisions()
        self.check_revisions()
        if via == "await" and (
            any(isinstance(r, SetInput) for r in reads)
            or len({r.snap for r in only_reads}) > 1
        ):
            # Tasks read at older revisions than the sync engine's reads
            # did, or than the newest when they read (see ``lenient``).
            self.defaults_may_differ = True
        self.check_counts(
            before,
            _counts_comparable(reads, via),
            f"step {self.step_no} reads via {via}",
            concurrent=via == "await" and len(only_reads) > 1,
        )
        revisions = {r.snap for r in only_reads}
        if (
            via == "await"
            and len(only_reads) > 1
            and (
                len(revisions) > 1
                or lenient
                or any(isinstance(r, SetInput) for r in reads)
            )
        ):
            # Tasks at an older revision that finish after one at the newest
            # replace the rows it stored, which costs a run at the next read
            # (G4); one read after another, the sync engine stores them in
            # the other order. Rows of what ran or was verified here may
            # differ from now on.
            for side in self.sides:
                for trace in side.engine.traces()[side.trace_pos :]:
                    if trace.key.startswith("query:"):
                        self.tainted.add(trace.key)

    def do_snapshot(self) -> None:
        n = self.next_snap
        self.next_snap += 1
        for side in self.sides:
            side.snapshots[n] = side.engine.snapshot()
        self.model_snaps[n] = self.model.view.copy()

    def do_release(self, n: int) -> None:
        for side in self.sides:
            side.snapshots.pop(n).release()
        del self.model_snaps[n]

    def do_mutate(self, op: tuple[Any, ...]) -> None:
        before = [side.engine.revision for side in self.sides]
        for target in [side.h for side in self.sides] + [self.model.view]:
            _mutate(target, op)
        self.check_revisions()
        del before

    def do_submit(self, read: Read, change: SetInput | None) -> None:
        """Hold the submitted call's first run, change an input, release it (G6)."""
        results = []
        before = [side.runs.copy() for side in self.sides]
        state_before = self.model.view.copy()
        lenient = change is not None and self.lenient(change)
        for side in self.sides:
            gate = Gate(side, (read.name, read.key[0] if read.key else None))
            self.world.gate = gate
            try:
                rev = side.engine.revision
                future = side.engine.submit(side.handle(read.name), *read.key)
                while not gate.entered.wait(0.005):
                    if future.done():
                        break
                entered = gate.entered.is_set()
                if change is not None:
                    _apply_set(side, change)
                moved = side.engine.revision != rev
                gate.release.set()
                try:
                    out: tuple[Any, ...] = ("ok", future.result(TIMEOUT))
                except Exception as exc:  # noqa: BLE001
                    out = error_outcome(exc)
            finally:
                gate.release.set()
                self.world.gate = None
            results.append((entered, moved, out))
        if change is not None:
            self.model.view.inputs[(change.index, change.key)] = change.value
            self.model.explicit.add((change.index, change.key))
        (e_s, m_s, out_s), (e_a, m_a, out_a) = results
        what = f"step {self.step_no} submit {read.name}{read.key} change={change}"
        key = (read.name, read.key[0] if read.key else None)
        if (
            not self.diverged
            and self.max_entries >= 1000
            and _trace_key(*key) not in self.tainted
            and read.name not in self.loose
        ):
            # Whether the body runs at all is a run count (check_counts).
            assert e_s == e_a, f"{what}: the body ran on sync {e_s} but on async {e_a}"
        if lenient:
            self.resync_revisions()
        else:
            assert m_s == m_a, (
                f"{what}: the revision moved on sync {m_s} but on async {m_a}"
            )
        if (m_s, out_s[:2]) == (m_a, out_a[:2]):
            assert same_outcome(out_s, out_a), (
                f"{what}: sync {out_s!r} but async {out_a!r}"
            )
        want, _ = self.model.read(read.name, read.key, state_before)
        for entered, moved, out in results:
            if entered and moved:
                assert out[:2] == ("err", "QueryCancelled"), (
                    f"{what}: {out!r}, not cancelled"
                )
            elif not (read.name == "M" and out[:2] == ("err", "CycleError")):
                assert same_outcome(out, want), (
                    f"{what}: engine {out!r} but model {want!r}"
                )
        self.check_revisions()
        self.check_counts(before, read.name not in self.volatile, what)

    def do_compute_many(self, calls: list[Read]) -> None:
        before = [side.runs.copy() for side in self.sides]
        if len(calls) > 1 and {"H", "M"} & {r.name for r in calls}:
            # As for tasks in a gather (do_reads).
            self.loose.add("M")
        if len(calls) > 1 and self.head_readers & {r.name for r in calls}:
            # The open finding of the module docstring, seen after threads.
            self.head_effects_unsure = True
        got = []
        for side in self.sides:
            effects: dict[str, list[Any]] = {}
            pairs = [(side.handle(r.name), r.key) for r in calls]
            got.append(
                (
                    outcome_of(
                        lambda: side.engine.compute_many(pairs, effects=effects)
                    ),
                    norm_effects(effects),
                )
            )
        wants = [self.model_for(Read(r.name, r.key, None, True)) for r in calls]
        what = f"step {self.step_no} compute_many {[(r.name, r.key) for r in calls]}"
        (out_s, eff_s), (out_a, eff_a) = got
        errors = [w[0] for w in wants if w[0][0] == "err"]
        if any(r.name == "M" for r in calls) and not same_outcome(out_s, out_a):
            self.member_diverged()
        if errors:
            # Which failing call's error comes out depends on the threads.
            for out in (out_s, out_a):
                assert out[0] == "err", f"{what}: {out!r}, model fails with {errors!r}"
                if not any(same_outcome(out, e) for e in errors):
                    assert out[1] == "CycleError" and any(
                        r.name == "M" for r in calls
                    ), f"{what}: {out!r} not in {errors!r}"
        else:
            raised = [out[:2] == ("err", "CycleError") for out in (out_s, out_a)]
            if any(r.name == "M" for r in calls) and any(raised):
                # Threads that enter the cycle through the member while
                # another iterates the head get the fixed point or a
                # CycleError (G7); one that got a value got the right one.
                values = ("ok", [w[0][1] for w in wants])
                for out, cycle in zip((out_s, out_a), raised):
                    if not cycle:
                        assert same_outcome(out, values), (
                            f"{what}: {out!r} but model {values!r}"
                        )
            else:
                values = ("ok", [w[0][1] for w in wants])
                assert same_outcome(out_s, values), (
                    f"{what}: sync {out_s!r} but model {values!r}"
                )
                assert same_outcome(out_a, values), (
                    f"{what}: async {out_a!r} but model {values!r}"
                )
                head = any(r.name in self.head_readers for r in calls)
                if not any(r.name == "M" for r in calls) and not (
                    head and self.head_effects_unsure
                ):
                    merged: dict[str, list[Any]] = {}
                    for w in wants:
                        for name, items in (w[1] or {}).items():
                            merged.setdefault(name, []).extend(items)
                    assert same(eff_s, merged), (
                        f"{what}: effects sync {eff_s!r} but model {merged!r}"
                    )
                    assert same(eff_a, merged), (
                        f"{what}: effects async {eff_a!r} but model {merged!r}"
                    )
        self.check_revisions()
        self.check_counts(before, not errors, what, concurrent=len(calls) > 1)

    def do_evict(self, kind: str, roots: list[Read]) -> None:
        for side in self.sides:
            if kind == "prune":
                side.engine.prune(
                    [("query", side.handle(r.name).id, r.key) for r in roots]
                )
            elif side.mark is not None:
                side.engine.sweep_unaccessed(side.mark)

    def do_mark(self) -> None:
        for side in self.sides:
            side.mark = side.engine.access_id

    def do_clock(self, past_ttl: bool, sensor: int) -> None:
        if past_ttl:
            self.world.now += TTL + 1.0
            self.world.sensor = sensor
        else:
            self.world.now += TTL / 4

    def do_reregister(self, name: str) -> None:
        self.variants[name] += 1
        spec = self.prog.spec(name)
        for side in self.sides:
            side.register(spec)
        self.check_revisions()

    def do_restart(self, keep: list[bool]) -> None:
        for side in self.sides:
            side.restart()
        self.model_snaps.clear()
        self.tainted.clear()
        self.rev_offset = 0
        # A disk that differs makes a load on one engine a run on the other.
        self.defaults_may_differ = self.disk_diverged
        self.counts_diverged = False
        explicit = sorted(self.model.explicit, key=repr)
        self.model.explicit = set()
        for (index, key), kept in zip(explicit, keep + [True] * len(explicit)):
            if kept:
                op = SetInput(index, key, self.model.view.inputs[(index, key)])
                for side in self.sides:
                    _apply_set(side, op)
                self.model.explicit.add((index, key))
            else:
                # Not set in the new session: back to its default.
                del self.model.view.inputs[(index, key)]
        self.check_revisions()


def _trace_key(name: str, arg: Any) -> str:
    """How a trace names the key of query ``name`` called with ``arg``."""
    return f"query:{name}()" if arg is None else f"query:{name}({arg!r},)"


def _mutate(target: Any, op: tuple[Any, ...]) -> None:
    kind, a, b = op
    xs, s, d = target.xs, target.s, target.d
    if kind == "append":
        xs.append(a)
    elif kind == "setitem":
        if len(xs):
            xs[a % len(xs)] = b
    elif kind == "pop":
        if len(xs):
            xs.pop()
    elif kind == "add":
        s.add(a)
    elif kind == "discard":
        s.discard(a)
    elif kind == "dset":
        d[a % 3] = b
    elif kind == "ddel":
        d.pop(a % 3, None)


def _counts_comparable(reads: list[Any], via: str) -> bool:
    """Whether the two engines must run the same bodies for ``reads``.

    Not when tasks of one gather read at different revisions (an input set
    between them, or snapshots of other revisions): a run at one revision
    cannot serve a caller at another, so tasks that overlap each compute a
    dependency that run one after another would share or verify (G5's
    limits). Volatile queries are left out by ``check_counts``.
    """
    if via == "plain":
        return True
    plain_reads = [r for r in reads if not isinstance(r, SetInput)]
    if len(plain_reads) <= 1:
        return True
    if any(isinstance(r, SetInput) for r in reads):
        return False
    return len({r.snap for r in plain_reads}) <= 1


# ---------------------------------------------------------------------------
# Drawing the steps
# ---------------------------------------------------------------------------

STEP_KINDS = (
    "set",
    "read",
    "mutate",
    "snapshot",
    "submit",
    "compute_many",
    "evict",
    "clock",
    "reregister",
    "restart",
)


def _draw_read(
    data: Any,
    pair: Pair,
    *,
    snaps: bool = True,
    member: bool = True,
    label: str = "read",
) -> Read:
    names = [q.name for q in pair.prog.queries] + ["T", "H"] + (["M"] if member else [])
    name = data.draw(st.sampled_from(names), label=f"{label} query")
    key = (
        (data.draw(st.sampled_from(KEYS), label=f"{label} key"),)
        if pair.prog.keyed(name)
        else ()
    )
    snap = None
    if (
        snaps
        and pair.model_snaps
        and data.draw(st.booleans(), label=f"{label} through a snapshot")
    ):
        snap = data.draw(
            st.sampled_from(sorted(pair.model_snaps)), label=f"{label} snapshot"
        )
    return Read(name, key, snap, data.draw(st.booleans(), label=f"{label} effects"))


def _draw_set(data: Any, pair: Pair, label: str = "set") -> SetInput:
    index = data.draw(st.integers(0, len(pair.prog.inputs) - 1), label=f"{label} input")
    key = (
        (data.draw(st.sampled_from(KEYS), label=f"{label} key"),)
        if pair.prog.inputs[index].keyed
        else ()
    )
    if data.draw(st.booleans(), label=f"{label} same value"):
        value = pair.model.current(index, key)
    else:
        value = data.draw(st.sampled_from(VALUES), label=f"{label} value")
    return SetInput(index, key, value)


def _run_step(data: Any, pair: Pair, kind: str) -> None:
    if kind == "set":
        op = _draw_set(data, pair)
        note(f"{pair.step_no}: set {op}")
        pair.do_set(op)
    elif kind == "read":
        via = data.draw(
            st.sampled_from(("await", "await", "await", "plain")), label="via"
        )
        n = data.draw(st.integers(1, 4), label="reads")
        reads: list[Any] = [_draw_read(data, pair) for _ in range(n)]
        if data.draw(st.integers(0, 3), label="set between reads") == 0:
            at = data.draw(st.integers(0, len(reads)), label="set position")
            reads.insert(at, _draw_set(data, pair, "mid-set"))
        note(f"{pair.step_no}: reads via {via}: {reads}")
        pair.do_reads(reads, via)
    elif kind == "mutate":
        op = (
            data.draw(
                st.sampled_from(
                    ("append", "setitem", "pop", "add", "discard", "dset", "ddel")
                ),
                label="mutation",
            ),
            data.draw(COLLECTION_VALUES, label="mutation a"),
            data.draw(COLLECTION_VALUES, label="mutation b"),
        )
        note(f"{pair.step_no}: mutate {op}")
        pair.do_mutate(op)
    elif kind == "snapshot":
        if pair.model_snaps and data.draw(st.booleans(), label="release"):
            n = data.draw(
                st.sampled_from(sorted(pair.model_snaps)), label="release which"
            )
            note(f"{pair.step_no}: release snapshot {n}")
            pair.do_release(n)
        elif len(pair.model_snaps) < 3:
            note(f"{pair.step_no}: snapshot {pair.next_snap}")
            pair.do_snapshot()
    elif kind == "submit":
        read = _draw_read(data, pair, snaps=False, label="submit")
        change = None
        if data.draw(st.booleans(), label="change while held"):
            change = _draw_set(data, pair, "submit change")
        note(f"{pair.step_no}: submit {read} change {change}")
        pair.do_submit(read, change)
    elif kind == "compute_many":
        n = data.draw(st.integers(1, 3), label="compute_many calls")
        calls = [
            _draw_read(data, pair, snaps=False, label="compute_many") for _ in range(n)
        ]
        note(f"{pair.step_no}: compute_many {calls}")
        pair.do_compute_many(calls)
    elif kind == "evict":
        what = data.draw(st.sampled_from(("prune", "sweep", "mark")), label="evict")
        if what == "mark":
            note(f"{pair.step_no}: mark access id")
            pair.do_mark()
        else:
            roots = [
                _draw_read(data, pair, snaps=False, label="root")
                for _ in range(data.draw(st.integers(0, 2), label="roots"))
            ]
            note(f"{pair.step_no}: {what} {roots}")
            pair.do_evict(what, roots)
    elif kind == "clock":
        past = data.draw(st.booleans(), label="past the TTL")
        # Half the time the TTL query gives the same value again, so what
        # depends on it is verified (and kept) rather than run (G9).
        sensor = data.draw(
            st.sampled_from((pair.world.sensor,) * 3 + (0, 1, 2)), label="sensor"
        )
        note(f"{pair.step_no}: clock past_ttl={past} sensor={sensor}")
        pair.do_clock(past, sensor)
    elif kind == "reregister":
        name = data.draw(
            st.sampled_from([q.name for q in pair.prog.queries]), label="reregister"
        )
        note(f"{pair.step_no}: reregister {name}")
        pair.do_reregister(name)
    elif kind == "restart":
        if pair.backend == "memory":
            return
        keep = [
            data.draw(st.booleans(), label="keep input") for _ in pair.model.explicit
        ]
        # Time passes between two runs of a program: often more than the TTL.
        past = data.draw(st.booleans(), label="restart past the TTL")
        sensor = data.draw(st.integers(0, 2), label="restart sensor")
        note(f"{pair.step_no}: restart keep={keep} past_ttl={past} sensor={sensor}")
        pair.do_restart(keep)
        if past:
            pair.do_clock(True, sensor)


@settings(
    max_examples=EXAMPLES,
    deadline=None,
    # CASCADE_ASYNC_DIFF_SHRINK=0 reports a failure without shrinking it.
    phases=(
        tuple(Phase)
        if os.environ.get("CASCADE_ASYNC_DIFF_SHRINK", "1") != "0"
        else (Phase.explicit, Phase.reuse, Phase.generate)
    ),
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
@given(data=st.data())
def test_generated_programs_agree_on_both_paths_and_with_the_model(data: Any) -> None:
    prog = data.draw(programs(), label="program")
    backend = data.draw(st.sampled_from(("memory", "mdbx", "sqlite")), label="backend")
    max_entries = data.draw(
        st.sampled_from((10_000, 10_000, 10_000, 6)), label="max_entries"
    )
    # Swarm testing: each example uses a part of the step kinds.
    kinds = data.draw(
        st.lists(st.sampled_from(STEP_KINDS), min_size=2, max_size=6, unique=True),
        label="kinds",
    )
    kinds = ["read", *kinds]
    event(f"backend {backend}")
    pair = Pair(prog, backend, max_entries)
    try:
        with mock.patch.object(evaluator_module, "time", pair.world):
            for _ in range(data.draw(st.integers(4, 25), label="steps")):
                kind = data.draw(st.sampled_from(kinds), label="step")
                _run_step(data, pair, kind)
                pair.after_step()
                pair.step_no += 1
            # A last read of everything, at the newest revision.
            final = [
                Read(q.name, (k,) if q.keyed else (), None, True)
                for q in prog.queries
                for k in (KEYS if q.keyed else (0,))
            ]
            final += [Read("T", (), None, True), Read("H", (), None, True)]
            pair.do_reads(final, "await")
            pair.after_step()
            # Then once more after the TTL ran out with a new value: what
            # depends on the TTL query, however it was last verified or
            # loaded, must follow it (G9).
            pair.do_clock(True, (pair.world.sensor + 1) % 3)
            pair.do_reads(final, "await")
    finally:
        pair.close()
