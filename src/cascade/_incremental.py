"""Incremental map/reduce pipelines over cascade collections.

The AST rewriter turns a supported expression inside a ``@query`` into a call
to :meth:`IncrementalRuntime.run` with a site id, a reducer name, the source
expression's value, and the map/filter/reverse stages. At runtime this module
decides between two paths:

* Source is a cascade collection (or a dict view of one): consume only the
  diffs the collection emitted since this consumer's checkpoint, feed them
  through the fused stage function, and update the reducer's internal state.
* Source is anything else: silently fall back and evaluate the expression
  with plain Python semantics.

Each (site, collection, view, reducer) pair keeps its own state with its own
``last_rev`` checkpoint, so several queries can consume one collection at
independent paces. Adjacent map and filter stages are condensed into a single
per-item function before execution.
"""

from __future__ import annotations

import itertools
import threading
from bisect import bisect_left, bisect_right, insort
from dataclasses import dataclass, field
from typing import Any, Callable

from ._collections import CascadeDict, CascadeList, CascadeSet, _CascadeDictView
from ._serde import hash_bytecode

_VOLATILE = object()
_LOAD = 512


class SortedKeyList:
    """Sorted container of ``(key, ord, uid)`` entries in fixed-size blocks.

    A flat sorted list pays an O(N) memmove per insert; splitting the entries
    into blocks of about ``_LOAD`` items bounds every insert and remove to one
    small block, giving effectively logarithmic updates like the balanced
    tree the spec calls for, without a tree implementation.
    """

    def __init__(self) -> None:
        self._blocks: list[list[tuple[Any, int, Any]]] = []
        self._maxes: list[tuple[Any, int, Any]] = []
        self._len = 0

    def __len__(self) -> int:
        return self._len

    def add(self, entry: tuple[Any, int, Any]) -> None:
        if not self._blocks:
            self._blocks.append([entry])
            self._maxes.append(entry)
            self._len = 1
            return
        i = min(bisect_right(self._maxes, entry), len(self._blocks) - 1)
        block = self._blocks[i]
        insort(block, entry)
        self._maxes[i] = block[-1]
        self._len += 1
        if len(block) > 2 * _LOAD:
            half = block[_LOAD:]
            del block[_LOAD:]
            self._blocks.insert(i + 1, half)
            self._maxes[i] = block[-1]
            self._maxes.insert(i + 1, half[-1])

    def remove(self, entry: tuple[Any, int, Any]) -> None:
        i = bisect_left(self._maxes, entry)
        block = self._blocks[i]
        j = bisect_left(block, entry)
        del block[j]
        self._len -= 1
        if block:
            self._maxes[i] = block[-1]
        else:
            del self._blocks[i]
            del self._maxes[i]

    def first(self) -> tuple[Any, int, Any]:
        return self._blocks[0][0]

    def last(self) -> tuple[Any, int, Any]:
        return self._blocks[-1][-1]

    def __iter__(self):
        return itertools.chain.from_iterable(self._blocks)

    def __reversed__(self):
        return itertools.chain.from_iterable(
            reversed(b) for b in reversed(self._blocks)
        )


@dataclass
class _FinalizeCtx:
    seq: list[Any] | None
    reverse: bool
    args: dict[str, Any]


class _Reducer:
    """Base for incremental reducers.

    ``contrib`` maps a uid to whatever the reducer needs to undo that
    element's contribution when it is removed or replaced. A uid absent from
    ``contrib`` did not pass the filter stages.
    """

    needs_seq = False

    def __init__(self) -> None:
        self.contrib: dict[Any, Any] = {}

    def add(self, uid: Any, value: Any) -> None:
        raise NotImplementedError

    def discard(self, uid: Any) -> None:
        self.contrib.pop(uid, None)

    def result(self, ctx: _FinalizeCtx) -> Any:
        raise NotImplementedError


class _SumReducer(_Reducer):
    def __init__(self) -> None:
        super().__init__()
        self.total: Any = 0

    def add(self, uid: Any, value: Any) -> None:
        self.total = self.total + value
        self.contrib[uid] = value

    def discard(self, uid: Any) -> None:
        if uid in self.contrib:
            self.total = self.total - self.contrib.pop(uid)

    def result(self, ctx: _FinalizeCtx) -> Any:
        return self.total


class _LenReducer(_Reducer):
    def add(self, uid: Any, value: Any) -> None:
        self.contrib[uid] = None

    def result(self, ctx: _FinalizeCtx) -> int:
        return len(self.contrib)


class _AnyAllReducer(_Reducer):
    def __init__(self, mode: str) -> None:
        super().__init__()
        self.mode = mode
        self.true_count = 0

    def add(self, uid: Any, value: Any) -> None:
        truth = bool(value)
        self.contrib[uid] = truth
        self.true_count += truth

    def discard(self, uid: Any) -> None:
        if uid in self.contrib:
            self.true_count -= self.contrib.pop(uid)

    def result(self, ctx: _FinalizeCtx) -> bool:
        if self.mode == "any":
            return self.true_count > 0
        return self.true_count == len(self.contrib)


class _OrderedKeyReducer(_Reducer):
    """Shared machinery for min, max, and sorted: a sorted key structure."""

    def __init__(self, key_fn: Callable[[Any], Any] | None = None) -> None:
        super().__init__()
        self.key_fn = key_fn
        self.tree = SortedKeyList()
        self._next_ord = 0

    def add(self, uid: Any, value: Any) -> None:
        key = value if self.key_fn is None else self.key_fn(value)
        entry = (key, self._next_ord, uid)
        self._next_ord += 1
        self.tree.add(entry)
        self.contrib[uid] = (entry, value)

    def discard(self, uid: Any) -> None:
        item = self.contrib.pop(uid, None)
        if item is not None:
            self.tree.remove(item[0])


class _MinMaxReducer(_OrderedKeyReducer):
    def __init__(self, mode: str) -> None:
        super().__init__()
        self.mode = mode

    def result(self, ctx: _FinalizeCtx) -> Any:
        if not len(self.tree):
            return min(()) if self.mode == "min" else max(())
        entry = self.tree.first() if self.mode == "min" else self.tree.last()
        return self.contrib[entry[2]][1]


class _SortedReducer(_OrderedKeyReducer):
    def result(self, ctx: _FinalizeCtx) -> list[Any]:
        order = reversed(self.tree) if ctx.args.get("sort_reverse") else iter(self.tree)
        return [self.contrib[entry[2]][1] for entry in order]


class _ValueReducer(_Reducer):
    """Stores the mapped value per uid; subclasses shape the final object."""

    def add(self, uid: Any, value: Any) -> None:
        self.contrib[uid] = value

    def _ordered_values(self, ctx: _FinalizeCtx):
        seq = reversed(ctx.seq) if ctx.reverse else ctx.seq
        return (self.contrib[uid] for uid in seq if uid in self.contrib)


class _ListReducer(_ValueReducer):
    needs_seq = True

    def result(self, ctx: _FinalizeCtx) -> list[Any]:
        return list(self._ordered_values(ctx))


class _SetReducer(_ValueReducer):
    def result(self, ctx: _FinalizeCtx) -> set[Any]:
        return set(self.contrib.values())


class _DictReducer(_ValueReducer):
    needs_seq = True

    def result(self, ctx: _FinalizeCtx) -> dict[Any, Any]:
        return dict(self._ordered_values(ctx))


class _JoinReducer(_ValueReducer):
    needs_seq = True

    def result(self, ctx: _FinalizeCtx) -> str:
        return ctx.args["sep"].join(self._ordered_values(ctx))


_REDUCERS: dict[str, Callable[[dict[str, Any]], _Reducer]] = {
    "sum": lambda args: _SumReducer(),
    "len": lambda args: _LenReducer(),
    "any": lambda args: _AnyAllReducer("any"),
    "all": lambda args: _AnyAllReducer("all"),
    "min": lambda args: _MinMaxReducer("min"),
    "max": lambda args: _MinMaxReducer("max"),
    "sorted": lambda args: _SortedReducer(args.get("key")),
    "list": lambda args: _ListReducer(),
    "set": lambda args: _SetReducer(),
    "dict": lambda args: _DictReducer(),
    "join": lambda args: _JoinReducer(),
}


def _fuse(stages: tuple[tuple[Any, ...], ...]) -> Callable[[Any], tuple[bool, Any]]:
    """Condense adjacent map/filter stages into one per-item function.

    A chain of nested ``map``/``filter`` iterators costs one Python frame per
    stage per item; the fused closure walks all stages in a single frame.
    """
    fns = [(kind, fn) for kind, *rest in stages if kind != "reverse" for fn in rest]
    if not fns:
        return lambda value: (True, value)

    def fused(value: Any) -> tuple[bool, Any]:
        for kind, fn in fns:
            if kind == "filter":
                if not fn(value):
                    return False, None
            else:
                value = fn(value)
        return True, value

    return fused


@dataclass
class _PipelineState:
    reducer: _Reducer
    env_fp: Any
    last_rev: int = 0
    seq: list[Any] | None = None
    present: set[Any] = field(default_factory=set)
    impure: bool = False


@dataclass
class _PipelineInfo:
    site: str
    source: str
    view: str | None
    reducer: str
    stages: tuple[str, ...]
    fused_stage_count: int
    consumers: set[str] = field(default_factory=set)
    state: _PipelineState | None = None


class IncrementalRuntime:
    """Owns pipeline states, fusion, checkpoints, and the pipeline graph."""

    def __init__(self, engine: Any) -> None:
        self._engine = engine
        self._lock = threading.RLock()
        self._states: dict[tuple[Any, ...], _PipelineState] = {}
        self._info: dict[tuple[Any, ...], _PipelineInfo] = {}

    # --- entry point called from rewritten query code ---
    def run(
        self,
        site: str,
        reducer_name: str,
        source: Any,
        stages: tuple[tuple[Any, ...], ...],
        **reducer_args: Any,
    ) -> Any:
        resolved = self._resolve_source(source, reducer_name)
        if resolved is None:
            return _fallback(reducer_name, source, stages, reducer_args)
        collection, view = resolved
        reverse = sum(1 for st in stages if st[0] == "reverse") % 2 == 1
        if any(st[0] == "reverse" for st in stages) and isinstance(
            collection, CascadeSet
        ):
            reversed(collection)  # raises TypeError exactly like plain Python

        core = collection._core
        head = core.head_for_current_context()
        # engine.load() restores input versions but not collection logs; if the
        # recorded head outruns the live log, trust the log.
        head = min(head, core.log.head)
        fused = _fuse(stages)
        fingerprint = _env_fingerprint(
            [fn for kind, *rest in stages if kind != "reverse" for fn in rest]
            + ([reducer_args["key"]] if callable(reducer_args.get("key")) else []),
            self._engine._stable_hash,
            self,
        )
        state_key = (site, core.input_qualname, view, reducer_name)
        evaluator = self._engine._evaluator
        with self._lock:
            state = self._states.get(state_key)
            needs_rebuild = (
                state is None
                or state.impure
                or fingerprint is _VOLATILE
                or state.env_fp != fingerprint
                or state.last_rev > head
            )
            dep_base = evaluator.current_frame_dep_count()
            try:
                if needs_rebuild:
                    state = self._build_state(
                        reducer_name, reducer_args, fingerprint, core, view, fused, head
                    )
                    self._states[state_key] = state
                else:
                    self._ingest(
                        state, core, view, fused, core.log.since(state.last_rev, head)
                    )
                    state.last_rev = head
            except BaseException:
                self._states.pop(state_key, None)
                raise
            if dep_base >= 0 and evaluator.current_frame_dep_count() > dep_base:
                # A stage or key function read tracked engine state (another
                # query, an input, another collection). Its per-item results
                # can change without this collection emitting a diff, so the
                # cached state cannot be trusted across runs: rebuild each
                # time. Slower, always correct.
                state.impure = True
            self._record_info(state_key, site, reducer_name, core, view, stages, state)
            ctx = _FinalizeCtx(seq=state.seq, reverse=reverse, args=reducer_args)
            return state.reducer.result(ctx)

    # --- state construction and diff consumption ---
    def _build_state(
        self,
        reducer_name: str,
        reducer_args: dict[str, Any],
        fingerprint: Any,
        core: Any,
        view: str | None,
        fused: Callable[[Any], tuple[bool, Any]],
        head: int,
    ) -> _PipelineState:
        reducer = _REDUCERS[reducer_name](reducer_args)
        state = _PipelineState(reducer=reducer, env_fp=fingerprint)
        if reducer.needs_seq:
            state.seq = []
        contents = core.log.contents_at(head)
        self._ingest(state, core, view, fused, _synthetic_inserts(core.kind, contents))
        state.last_rev = head
        return state

    def _ingest(
        self,
        state: _PipelineState,
        core: Any,
        view: str | None,
        fused: Callable[[Any], tuple[bool, Any]],
        diffs: list[dict[str, Any]],
    ) -> None:
        reducer = state.reducer
        seq = state.seq
        for diff in diffs:
            for op, uid, item in _events(core.kind, view, diff, state):
                if op == "remove":
                    reducer.discard(uid)
                    if seq is not None:
                        seq.remove(uid)
                    continue
                if op == "replace":
                    reducer.discard(uid)
                elif seq is not None:
                    index = diff.get("index")
                    if index is None:
                        seq.append(uid)
                    else:
                        seq.insert(index, uid)
                ok, mapped = fused(item)
                if ok:
                    reducer.add(uid, mapped)

    # --- source resolution and bookkeeping ---
    def _resolve_source(
        self, source: Any, reducer_name: str
    ) -> tuple[Any, str | None] | None:
        if isinstance(source, CascadeList) or isinstance(source, CascadeSet):
            return source, None
        if isinstance(source, CascadeDict):
            return source, "items" if reducer_name == "dict" else "keys"
        if isinstance(source, _CascadeDictView):
            return source.parent, source.which
        return None

    def _record_info(
        self,
        state_key: tuple[Any, ...],
        site: str,
        reducer_name: str,
        core: Any,
        view: str | None,
        stages: tuple[tuple[Any, ...], ...],
        state: _PipelineState,
    ) -> None:
        info = self._info.get(state_key)
        if info is None:
            source = f"collection:{core.kind}:{core.name}"
            if view is not None:
                source += f".{view}"
            logical = tuple(st[0] for st in stages)
            info = _PipelineInfo(
                site=site,
                source=source,
                view=view,
                reducer=reducer_name,
                stages=logical,
                fused_stage_count=1
                if any(k in ("map", "filter") for k in logical)
                else 0,
            )
            self._info[state_key] = info
        info.state = state
        consumer = self._engine._evaluator.current_query_node()
        if consumer is not None:
            info.consumers.add(consumer)

    # --- introspection ---
    def graph_extras(self) -> tuple[list[str], list[tuple[str, str]]]:
        """Pipeline nodes and edges to merge into ``inspect_graph`` output."""
        with self._lock:
            nodes: list[str] = []
            edges: list[tuple[str, str]] = []
            for info in self._info.values():
                chain = [info.source]
                for i, kind in enumerate(k for k in info.stages if k != "reverse"):
                    chain.append(f"{kind}:{info.site}#{i}")
                chain.append(f"reduce:{info.reducer}:{info.site}")
                for node in chain:
                    if node not in nodes:
                        nodes.append(node)
                for parent, dep in zip(chain[1:], chain):
                    edges.append((parent, dep))
                for consumer in sorted(info.consumers):
                    edges.append((consumer, chain[-1]))
            return nodes, edges

    def inspect_pipelines(self) -> list[dict[str, Any]]:
        with self._lock:
            out = []
            for info in self._info.values():
                out.append(
                    {
                        "site": info.site,
                        "source": info.source,
                        "reducer": info.reducer,
                        "stages": list(info.stages),
                        "fused_stage_count": info.fused_stage_count,
                        "last_rev": info.state.last_rev if info.state else 0,
                        "consumers": sorted(info.consumers),
                    }
                )
            return sorted(out, key=lambda p: (p["site"], p["source"], p["reducer"]))


def _synthetic_inserts(kind: str, contents: Any) -> list[dict[str, Any]]:
    if kind == "list":
        return [
            {"action": "insert", "uid": uid, "value": value, "index": i}
            for i, (uid, value) in enumerate(contents)
        ]
    if kind == "set":
        return [{"action": "add", "value": v} for v in contents]
    return [{"action": "upsert", "key": k, "value": v} for k, v in contents.items()]


def _events(kind: str, view: str | None, diff: dict[str, Any], state: _PipelineState):
    """Normalize one diff into (op, uid, item) events for the reducer."""
    action = diff["action"]
    if kind == "list":
        if action == "insert":
            yield "add", diff["uid"], diff["value"]
        elif action == "update":
            yield "replace", diff["uid"], diff["value"]
        else:
            yield "remove", diff["uid"], None
        return
    if kind == "set":
        if action == "add":
            yield "add", diff["value"], diff["value"]
        else:
            yield "remove", diff["value"], None
        return
    key = diff["key"]
    if action == "remove":
        state.present.discard(key)
        yield "remove", key, None
        return
    existed = key in state.present
    state.present.add(key)
    if view == "keys":
        if not existed:
            yield "add", key, key
        return
    item = diff["value"] if view == "values" else (key, diff["value"])
    yield ("replace" if existed else "add"), key, item


def _env_fingerprint(
    fns: list[Callable[..., Any]], stable_hash: Callable[[Any], str], rt: Any
) -> Any:
    """Fingerprint of stage function code plus captured closure and default values.

    A changed fingerprint means the same site now computes something
    different (for example a closed-over factor changed), so the pipeline
    state is rebuilt instead of mixing results from two function versions.
    Function-valued captures (helper functions, nested rewritten sites) are
    fingerprinted recursively by bytecode; the engine runtime itself is a
    constant. A value that cannot be fingerprinted makes the pipeline
    volatile: it rebuilds on every run, which is slower but always correct.
    """
    parts: list[str] = []
    seen: set[int] = set()

    def visit_value(value: Any) -> None:
        if value is rt:
            parts.append("<rt>")
        elif hasattr(value, "__code__"):
            visit_fn(value)
        elif callable(value):
            parts.append(getattr(value, "__qualname__", None) or repr(type(value)))
        else:
            parts.append(stable_hash(value))

    def visit_fn(fn: Callable[..., Any]) -> None:
        if id(fn) in seen:
            parts.append("<cycle>")
            return
        seen.add(id(fn))
        parts.append(hash_bytecode(fn))
        for cell in getattr(fn, "__closure__", None) or ():
            visit_value(cell.cell_contents)
        for default in getattr(fn, "__defaults__", None) or ():
            visit_value(default)

    try:
        for fn in fns:
            visit_value(fn)
    except Exception:
        return _VOLATILE
    return "|".join(parts)


def _fallback(
    reducer_name: str,
    source: Any,
    stages: tuple[tuple[Any, ...], ...],
    reducer_args: dict[str, Any],
) -> Any:
    """Plain-Python execution for non-cascade sources, matching builtin semantics."""
    it: Any = source
    staged = False
    for st in stages:
        if st[0] == "reverse":
            it = reversed(it)
        elif st[0] == "map":
            it = map(st[1], it)
        else:
            it = filter(st[1], it)
        staged = True
    if reducer_name == "len":
        return sum(1 for _ in it) if staged else len(it)
    if reducer_name == "sorted":
        kwargs = {}
        if reducer_args.get("key") is not None:
            kwargs["key"] = reducer_args["key"]
        if reducer_args.get("sort_reverse"):
            kwargs["reverse"] = True
        return sorted(it, **kwargs)
    if reducer_name == "join":
        return reducer_args["sep"].join(it)
    builtin = {
        "sum": sum,
        "any": any,
        "all": all,
        "min": min,
        "max": max,
        "list": list,
        "set": set,
        "dict": dict,
    }[reducer_name]
    return builtin(it)
