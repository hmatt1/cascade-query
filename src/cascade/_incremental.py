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

The result is always what the plain expression gives. A reducer keeps
incremental state only where that state is exact (see each reducer); where
it is not, it applies the builtin to the values it stored. While any
element's stage raises, the expression is evaluated plainly over the
collection's contents instead, because only the lazy, in-order evaluation
knows which exception Python would raise first, or whether it would have
stopped before reaching it.

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

from ._collections import (
    CascadeDict,
    CascadeList,
    ABSENT_HEAD,
    CascadeSet,
    _CascadeDictView,
    _copy_dict,
    _HistoryCompacted,
)
from ._errors import QueryCancelled
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

    @classmethod
    def from_sorted(cls, entries: list[tuple[Any, int, Any]]) -> SortedKeyList:
        """Build from entries that are already in order."""
        out = cls()
        for i in range(0, len(entries), _LOAD):
            block = entries[i : i + _LOAD]
            out._blocks.append(block)
            out._maxes.append(block[-1])
        out._len = len(entries)
        return out

    def has_key(self, key: Any) -> bool:
        """Whether any entry has a key equal to ``key``."""
        # ``(key,)`` sorts just before every entry that starts with ``key``.
        probe = (key,)
        i = bisect_left(self._maxes, probe)
        if i == len(self._blocks):
            return False
        block = self._blocks[i]
        return block[bisect_left(block, probe)][0] == key

    def with_key(self, key: Any) -> list[tuple[Any, int, Any]]:
        """Every entry whose key equals ``key``, in order."""
        probe = (key,)
        blocks = self._blocks
        i = bisect_left(self._maxes, probe)
        out: list[tuple[Any, int, Any]] = []
        first = True
        while i < len(blocks):
            block = blocks[i]
            j = bisect_left(block, probe) if first else 0
            first = False
            while j < len(block) and block[j][0] == key:
                out.append(block[j])
                j += 1
            if j < len(block):
                break
            i += 1
        return out

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
    """What a reducer may ask for when it produces its result.

    ``order`` gives the uids of the collection in the collection's own order
    (a fresh list each call). It costs a pass over the collection, so
    reducers call it only when the result depends on the order.
    """

    order: Callable[[], list[Any]]
    reverse: bool
    args: dict[str, Any]
    # Positions of some uids in the collection, as a dict that holds at
    # least those uids.
    positions: Callable[[list[Any]], dict[Any, int]] | None = None

    def iteration_order(self) -> list[Any]:
        """The uids in the order the plain expression would visit them."""
        uids = self.order()
        if self.reverse:
            uids.reverse()
        return uids


_MISSING = object()


class _Reducer:
    """Base for incremental reducers.

    ``contrib`` maps a uid to whatever the reducer needs to undo that
    element's contribution when it is removed or replaced. A uid absent from
    ``contrib`` did not pass the filter stages.

    A reducer must give exactly what the builtin gives for the mapped values
    in iteration order. Where the incremental state cannot promise that (a
    float sum, keys with no total order, ties whose order is not known), the
    reducer applies the builtin to the stored values instead: the stage
    functions are not run again, only the fold is.
    """

    def __init__(self) -> None:
        self.contrib: dict[Any, Any] = {}
        # Whether ``contrib`` (a dict, so ordered) lists the elements in the
        # collection's order. True while elements are only appended, removed
        # or replaced in place; an insert elsewhere makes it False until
        # ``_values`` puts ``contrib`` back in order. Kept by the reducers
        # that call ``_values``.
        self.in_order = True

    def add(self, uid: Any, value: Any, appended: bool) -> None:
        """Take in an element. ``appended``: it is last in the collection."""
        raise NotImplementedError  # pragma: no cover - every reducer overrides it

    def discard(self, uid: Any) -> Any:
        return self.contrib.pop(uid, None)

    def replace(self, uid: Any, value: Any) -> None:
        """The element keeps its place and has a new value."""
        self.discard(uid)
        self.add(uid, value, False)

    def result(self, ctx: _FinalizeCtx) -> Any:
        raise NotImplementedError  # pragma: no cover - every reducer overrides it

    def _values(self, ctx: _FinalizeCtx) -> list[Any]:
        """The stored values in the order the plain expression yields them."""
        contrib = self.contrib
        if not self.in_order:
            contrib = {uid: contrib[uid] for uid in ctx.order() if uid in contrib}
            self.contrib = contrib
            self.in_order = True
        values = list(contrib.values())
        if ctx.reverse:
            values.reverse()
        return values


class _SumReducer(_Reducer):
    """``sum``: a running total for ints, the builtin's own fold otherwise.

    Int addition is exact and has an inverse, so a total can be kept and
    adjusted. Nothing else is safe to adjust: float addition rounds (and the
    builtin compensates for it in its own way), and other types need not
    have a subtraction at all. With any non-int value present the result is
    the builtin ``sum`` over the stored values in order.
    """

    def __init__(self) -> None:
        super().__init__()
        self.int_total = 0
        self.others = 0

    def add(self, uid: Any, value: Any, appended: bool) -> None:
        if not appended:
            self.in_order = False
        kind = type(value)
        if kind is int or kind is bool:
            self.int_total += value
        else:
            self.others += 1
        self.contrib[uid] = value

    def replace(self, uid: Any, value: Any) -> None:
        old = self.contrib.get(uid, _MISSING)
        if old is _MISSING:
            self.add(uid, value, False)
            return
        self._count(old, -1)
        self._count(value, 1)
        self.contrib[uid] = value  # same key: keeps its place

    def discard(self, uid: Any) -> Any:
        value = self.contrib.pop(uid, _MISSING)
        if value is not _MISSING:
            self._count(value, -1)
        return None

    def _count(self, value: Any, sign: int) -> None:
        kind = type(value)
        if kind is int or kind is bool:
            self.int_total += sign * value
        else:
            self.others += sign

    def result(self, ctx: _FinalizeCtx) -> Any:
        if not self.others:
            return self.int_total
        if self.in_order and not ctx.reverse:
            return sum(self.contrib.values())
        return sum(self._values(ctx))


class _LenReducer(_Reducer):
    def add(self, uid: Any, value: Any, appended: bool) -> None:
        self.contrib[uid] = None

    def result(self, ctx: _FinalizeCtx) -> int:
        return len(self.contrib)


class _AnyAllReducer(_Reducer):
    """``any``/``all`` from a count of truthy elements.

    The builtins stop at the first element that decides the result and never
    evaluate the rest. Here every element is evaluated. With functions that
    do not raise the result is the same; an element that does raise sends
    the whole call to plain evaluation (see ``IncrementalRuntime.run``),
    which stops where the builtin stops.
    """

    def __init__(self, mode: str) -> None:
        super().__init__()
        self.mode = mode
        self.true_count = 0

    def add(self, uid: Any, value: Any, appended: bool) -> None:
        truth = bool(value)
        self.contrib[uid] = truth
        self.true_count += truth

    def discard(self, uid: Any) -> Any:
        truth = self.contrib.pop(uid, None)
        if truth is not None:
            self.true_count -= truth
        return None

    def result(self, ctx: _FinalizeCtx) -> bool:
        if self.mode == "any":
            return self.true_count > 0
        return self.true_count == len(self.contrib)


def _signature(key: Any) -> Any:
    """Shape of ``key`` if keys of that shape are totally ordered, else None.

    Keys with one signature can be kept in a sorted structure: every pair
    compares without error, and the comparisons agree with each other. That
    holds for numbers (``int``, ``bool``, ``float`` except NaN) among
    themselves, for ``str``, for ``bytes``, and for tuples of equal length
    whose positions have equal signatures. It does not hold for NaN (every
    comparison is false), for mixed types (some comparisons raise), or for
    arbitrary objects.
    """
    kind = type(key)
    if kind is int or kind is bool:
        return "n"
    if kind is float:
        return "n" if key == key else None
    if kind is str:
        return "s"
    if kind is bytes:
        return "b"
    if kind is tuple:
        parts = []
        for part in key:
            sig = _signature(part)
            if sig is None:
                return None
            parts.append(sig)
        return tuple(parts)
    return None


class _OrderedKeyReducer(_Reducer):
    """Shared machinery for min, max, and sorted: a sorted key structure.

    ``contrib[uid]`` is ``(entry, value, key, ord)``. ``entry`` is the
    element's ``(key, sign * ord, uid)`` in ``tree``, or None for an element
    kept outside it because its key's signature is not the tree's (see
    ``_signature``). While any such element exists the result is computed by
    the builtin from the stored keys and values in order.

    ``ord`` breaks ties between equal keys, and the builtins break them by
    position: the first of several equal elements wins, and a sort keeps
    them in their original order. So within a group of equal keys the ords
    must rise with the position in the collection.

    Elements appended to the collection get rising ords, and an element
    replaced in place keeps its ord (its key, and so its group, may change).
    Those ords rise with position everywhere, so they are right in whatever
    group the element is in. An element inserted anywhere else gets an ord
    that says nothing about its position: it is ``floating``. While any
    element floats, a key that gains a member is noted in ``unsettled``,
    because the order inside that group can no longer be taken from the
    ords. Before a result that depends on a group's order, ``_settle`` looks
    up where its members are in the collection and hands the group's ords
    out again in that order. That fixes the group, not the ords' meaning
    elsewhere, so its members float from then on; ``_renumber`` gives every
    element the ord of its position and ends all floating.
    """

    def __init__(self, key_fn: Callable[[Any], Any] | None, sign: int) -> None:
        super().__init__()
        self.key_fn = key_fn
        self.sign = sign
        self.tree = SortedKeyList()
        self.sig: Any = None
        self.outside = 0
        self.floating: set[Any] = set()
        self.unsettled: set[Any] = set()
        self._settles = 0
        self._next_ord = 0

    def add(
        self, uid: Any, value: Any, appended: bool, ord_: int | None = None
    ) -> None:
        key = value if self.key_fn is None else self.key_fn(value)
        if ord_ is None:
            ord_ = self._next_ord
            self._next_ord = ord_ + 1
        floating = self.floating
        if not appended:
            floating.add(uid)
        kind = type(key)
        sig = "n" if kind is int else "s" if kind is str else _signature(key)
        entry = None
        if sig is not None:
            tree = self.tree
            if not tree._len:
                self.sig = sig
            if sig == self.sig:
                if floating and tree.has_key(key):
                    self.unsettled.add(key)
                entry = (key, self.sign * ord_, uid)
                tree.add(entry)
        if entry is None:
            self.outside += 1
        self.contrib[uid] = (entry, value, key, ord_)

    def replace(self, uid: Any, value: Any) -> None:
        old = self._drop(uid)
        if old is None:
            # It did not pass the filter before: no ord for its position.
            self.add(uid, value, False)
        else:
            # Same place, same ord; it floats on if it floated.
            self.add(uid, value, True, old[3])

    def discard(self, uid: Any) -> Any:
        if self.floating:
            self.floating.discard(uid)
        return self._drop(uid)

    def _drop(self, uid: Any) -> Any:
        item = self.contrib.pop(uid, None)
        if item is not None:
            if item[0] is None:
                self.outside -= 1
            else:
                self.tree.remove(item[0])
        return item

    def _settle(self, keys: list[Any], ctx: _FinalizeCtx) -> None:
        """Put the ords of each group of equal keys in ``keys`` in position order."""
        tree = self.tree
        self._settles += 1
        if self._settles > 3 or len(keys) * 16 > tree._len:
            # Settling leaves elements floating, so every later tie needs
            # settling too. Now and then, and when most of the tree is in
            # doubt, pay for one pass that puts everything right.
            self._renumber(ctx.order())
            return
        groups = [group for key in keys if len(group := tree.with_key(key)) > 1]
        if not groups:
            return
        contrib = self.contrib
        sign = self.sign
        position = ctx.positions([entry[2] for group in groups for entry in group])
        moved = []
        for group in groups:
            by_position = sorted(group, key=lambda entry: position[entry[2]])
            ords = sorted(contrib[entry[2]][3] for entry in group)
            for entry, ord_ in zip(by_position, ords):
                if contrib[entry[2]][3] != ord_:
                    moved.append((entry, ord_))
        # All out before any goes back in: two entries must never share an ord.
        for entry, _ in moved:
            tree.remove(entry)
        floating = self.floating
        for entry, ord_ in moved:
            key, _, uid = entry
            new = (key, sign * ord_, uid)
            tree.add(new)
            contrib[uid] = (new, contrib[uid][1], key, ord_)
            floating.add(uid)

    def _renumber(self, order: list[Any]) -> None:
        """Give every element the ord of its position and rebuild the tree."""
        contrib = self.contrib
        sign = self.sign
        entries = []
        position = 0
        for uid in order:
            item = contrib.get(uid)
            if item is None:
                continue
            entry, value, key, _ = item
            if entry is not None:  # pragma: no branch - no key is outside the tree here
                entry = (key, sign * position, uid)
                entries.append(entry)
            contrib[uid] = (entry, value, key, position)
            position += 1
        entries.sort()
        self.tree = SortedKeyList.from_sorted(entries)
        self._next_ord = position
        self.floating.clear()
        self.unsettled.clear()
        self._settles = 0


class _MinMaxReducer(_OrderedKeyReducer):
    """``min``/``max``: the first of the smallest (largest) in iteration order."""

    def __init__(self, mode: str, reverse: bool) -> None:
        # ``first()`` is the smallest key with the smallest signed ord and
        # ``last()`` the largest key with the largest: choose the sign that
        # puts the earliest element in iteration order there.
        direction = -1 if reverse else 1
        super().__init__(None, direction if mode == "min" else -direction)
        self.mode = mode

    def result(self, ctx: _FinalizeCtx) -> Any:
        smallest = self.mode == "min"
        if self.outside:
            contrib = self.contrib
            values = [contrib[u][1] for u in ctx.iteration_order() if u in contrib]
            return min(values) if smallest else max(values)
        if not len(self.tree):
            return min(()) if smallest else max(())
        if self.unsettled:
            # Only the order within the winning group can change the result.
            key = (self.tree.first() if smallest else self.tree.last())[0]
            if key in self.unsettled:
                self._settle([key], ctx)
                self.unsettled.discard(key)
        entry = self.tree.first() if smallest else self.tree.last()
        return self.contrib[entry[2]][1]


class _SortedReducer(_OrderedKeyReducer):
    """``sorted``: stable, also with ``reverse=True``."""

    def __init__(
        self, key_fn: Callable[[Any], Any] | None, reverse: bool, descending: bool
    ) -> None:
        # Equal keys keep their iteration order in the output. Ascending
        # output walks the tree forwards, descending output backwards.
        direction = -1 if reverse else 1
        super().__init__(key_fn, -direction if descending else direction)
        self.descending = descending

    def result(self, ctx: _FinalizeCtx) -> list[Any]:
        contrib = self.contrib
        if self.outside:
            # The builtin's own sort over the stored keys: the same
            # comparisons in the same order, so the same result or error.
            uids = [u for u in ctx.iteration_order() if u in contrib]
            uids.sort(key=lambda u: contrib[u][2], reverse=self.descending)
            return [contrib[u][1] for u in uids]
        if self.unsettled:
            self._settle(list(self.unsettled), ctx)
            self.unsettled.clear()
        tree = self.tree
        order = reversed(tree) if self.descending else iter(tree)
        return [contrib[entry[2]][1] for entry in order]


class _ValueReducer(_Reducer):
    """Stores the mapped value per uid; the builtin shapes the final object."""

    def add(self, uid: Any, value: Any, appended: bool) -> None:
        if not appended:
            self.in_order = False
        self.contrib[uid] = value

    def replace(self, uid: Any, value: Any) -> None:
        if uid not in self.contrib:
            self.in_order = False
        self.contrib[uid] = value  # an existing key keeps its place


class _ListReducer(_ValueReducer):
    def result(self, ctx: _FinalizeCtx) -> list[Any]:
        return self._values(ctx)


class _SetReducer(_ValueReducer):
    def result(self, ctx: _FinalizeCtx) -> set[Any]:
        # In order: of two equal values (1 and True) a set keeps the first.
        return set(self._values(ctx))


class _DictReducer(_ValueReducer):
    def result(self, ctx: _FinalizeCtx) -> dict[Any, Any]:
        return dict(self._values(ctx))


class _JoinReducer(_ValueReducer):
    def result(self, ctx: _FinalizeCtx) -> str:
        return ctx.args["sep"].join(self._values(ctx))


_REDUCERS: dict[str, Callable[[dict[str, Any], bool], _Reducer]] = {
    "sum": lambda args, reverse: _SumReducer(),
    "len": lambda args, reverse: _LenReducer(),
    "any": lambda args, reverse: _AnyAllReducer("any"),
    "all": lambda args, reverse: _AnyAllReducer("all"),
    "min": lambda args, reverse: _MinMaxReducer("min", reverse),
    "max": lambda args, reverse: _MinMaxReducer("max", reverse),
    "sorted": lambda args, reverse: _SortedReducer(
        args.get("key"), reverse, bool(args.get("sort_reverse", False))
    ),
    "list": lambda args, reverse: _ListReducer(),
    "set": lambda args, reverse: _SetReducer(),
    "dict": lambda args, reverse: _DictReducer(),
    "join": lambda args, reverse: _JoinReducer(),
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


# Reducers whose result holds the values the stages made.
_HANDS_OUT_VALUES = frozenset({"list", "set", "dict", "sorted", "min", "max"})
_FROZEN_TYPES = (int, float, complex, str, bytes, type(None))


def _frozen(value: Any) -> bool:
    """Whether ``value`` is the same to Python if it is handed out again.

    Not if it can be changed in place, nor if it is a NaN: Python makes a
    new NaN object each time, and a set or dict tells NaN objects apart
    (``{f(x) for _ in (0, 1)}`` holds two). See D46 for a captured one.
    """
    if isinstance(value, (float, complex)):
        return value == value
    if isinstance(value, _FROZEN_TYPES):
        return True
    if isinstance(value, (tuple, frozenset)):
        return all(_frozen(item) for item in value)
    return False


def _watch_frozen(
    fused: Callable[[Any], tuple[bool, Any]], unfrozen: list[bool]
) -> Callable[[Any], tuple[bool, Any]]:
    """``fused`` that notes in ``unfrozen`` a value that is not ``_frozen``."""

    def watched(value: Any) -> tuple[bool, Any]:
        ok, mapped = fused(value)
        if ok and not unfrozen and not _frozen(mapped):
            unfrozen.append(True)
        return ok, mapped

    return watched


@dataclass
class _PipelineState:
    reducer: _Reducer
    env_fp: Any
    last_rev: int = 0
    # Elements the collection holds at ``last_rev``, filtered out or not.
    count: int = 0
    # Uids of elements whose stage, key function or reducer step raised.
    # They are in the collection but not in the reducer. While there are any,
    # the reducer's result is not the expression's: plain evaluation decides
    # whether the call returns (it may stop before it reaches them) or which
    # exception it raises.
    failed: set[Any] = field(default_factory=set)
    # Keys of a dict source, each mapped to the key object the dict holds.
    present: dict[Any, Any] = field(default_factory=dict)
    impure: bool = False
    # A stage, filter or key function pushed accumulator effects. The
    # pipeline calls them for changed elements only, and in the order the
    # changes arrived, so its effects could never be plain Python's; the
    # expression is evaluated plainly from then on (D31).
    effectful: bool = False


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

    def holds_lock(self) -> bool:
        """Whether this thread is inside a pipeline, whose stages may wait."""
        return self._lock._is_owned()  # type: ignore[attr-defined]

    # --- entry point called from rewritten query code ---
    def run(
        self,
        site: str,
        reducer_name: str,
        rstages: tuple[tuple[Any, ...], ...],
        source: Any,
        **reducer_args: Any,
    ) -> Any:
        """Value of ``reducer(stages(source))``, exactly as plain Python gives it.

        ``rstages`` lists the stages from the outermost in: the order in
        which the plain expression evaluates their function expressions,
        all before the source.

        Three paths, all with the same result:

        * the source is not a cascade collection: plain evaluation;
        * the pipeline's state is brought up to date from the collection's
          diffs and the reducer answers from it;
        * if anything raised on the way (a stage, a key function, a
          comparison, or the reducer's own builtin), the expression is
          evaluated plainly over the collection's contents. The incremental
          path evaluates every element, in the order the diffs arrived; the
          plain expression evaluates them lazily, in the collection's order,
          and may stop early. Only plain evaluation knows which exception
          comes first, or that none does: ``any(1 / z for z in [1, 0])`` is
          True. The state remembers which elements raised and stays in use:
          once they are gone or replaced, the reducer answers again.
        """
        stages = rstages[::-1]
        resolved = self._resolve_source(source, reducer_name, stages)
        if resolved is None:
            return _fallback(reducer_name, source, stages, reducer_args)
        collection, view = resolved
        reverse = sum(1 for st in stages if st[0] == "reverse") % 2 == 1
        if any(st[0] == "reverse" for st in stages) and isinstance(
            collection, CascadeSet
        ):
            reversed(collection)  # raises TypeError exactly like plain Python

        core = collection._core
        if core.base_len(collection) != core.tracked_len:
            core.check_storage()  # raises: the storage was changed behind the log
        head = core.head_for_current_context()
        if head == ABSENT_HEAD:
            # Read at a revision from before this collection was created.
            return _fallback(reducer_name, [], stages, reducer_args)
        # If the recorded head outruns the live log, trust the log.
        head = min(head, core.log.head)
        if not _args_supported(reducer_name, reducer_args):
            # Let the builtin reject (or accept) them itself.
            return _fallback(
                reducer_name, _items_at(core, view, head), stages, reducer_args
            )
        fused = _fuse(stages)
        # Values the result hands out that the query body could change.
        unfrozen: list[bool] = []
        if reducer_name in _HANDS_OUT_VALUES and any(st[0] == "map" for st in stages):
            fused = _watch_frozen(fused, unfrozen)
        fingerprint = _env_fingerprint(
            [fn for kind, *rest in stages if kind != "reverse" for fn in rest]
            + ([reducer_args["key"]] if callable(reducer_args.get("key")) else []),
            self._engine._stable_hash,
            self,
        )
        if fingerprint is not _VOLATILE and reducer_args.get("sort_reverse"):
            fingerprint += "|descending"
        state_key = (site, core.input_qualname, view, reducer_name)
        evaluator = self._engine._evaluator
        state = self._states.get(state_key)
        if state is not None and state.effectful:
            return _fallback(
                reducer_name, _items_at(core, view, head), stages, reducer_args
            )
        mark = evaluator.effects_mark()
        with self._lock:
            state = self._states.get(state_key)
            needs_rebuild = (
                state is None
                or state.impure
                or fingerprint is _VOLATILE
                or state.env_fp != fingerprint
                or state.last_rev > head
                or state.last_rev < core.log.base_rev
            )
            read_base = evaluator.current_frame_read_count()
            try:
                if needs_rebuild:
                    state = self._build_state(
                        reducer_name,
                        reducer_args,
                        reverse,
                        fingerprint,
                        core,
                        view,
                        fused,
                        head,
                    )
                    self._states[state_key] = state
                else:
                    try:
                        diffs = core.log.since(state.last_rev, head)
                    except _HistoryCompacted:
                        # Folded into the base since the check above.
                        state = self._build_state(
                            reducer_name,
                            reducer_args,
                            reverse,
                            fingerprint,
                            core,
                            view,
                            fused,
                            head,
                        )
                        self._states[state_key] = state
                    else:
                        self._ingest(state, core, view, fused, diffs)
                        state.last_rev = head
                if read_base >= 0 and evaluator.current_frame_read_count() > read_base:
                    # A stage or key function read tracked engine state
                    # (another query, an input, another collection). Its
                    # per-item results can change without this collection
                    # emitting a diff, so the cached state cannot be trusted
                    # across runs: rebuild each time. Slower, always correct.
                    # Reads are counted, not new dependencies: the query
                    # body may have read the same input or query before the
                    # pipeline did.
                    state.impure = True
                if unfrozen:
                    # A stage made a value that can be changed in place (a
                    # list, a dict, a generator, an object), or a NaN. The
                    # result hands it out, and a body that changes it would
                    # find it changed on the next run, or a set the same
                    # NaN object twice, where Python makes a new one:
                    # rebuild each time (dNN, and a second dNN for NaN).
                    state.impure = True
                self._record_info(
                    state_key, site, reducer_name, core, view, stages, state
                )
                ctx = _FinalizeCtx(
                    order=lambda: _order_at(core, head),
                    reverse=reverse,
                    args=reducer_args,
                    positions=lambda uids: _positions_at(core, head, uids),
                )
                answered = False
                if not state.failed:
                    result = state.reducer.result(ctx)
                    answered = True
            except QueryCancelled:
                self._states.pop(state_key, None)
                raise
            except Exception:
                # The reducer's builtin raised over the stored values (or
                # the contents could not be read). Plain evaluation raises
                # the same; the state may be half updated, so it goes.
                self._states.pop(state_key, None)
                answered = False
            except BaseException:
                self._states.pop(state_key, None)
                raise
            if mark is not None and evaluator.effects_pushed_since(mark):
                # Something the pipeline ran pushed accumulator effects (or
                # replayed a query's). Plain Python pushes them for every
                # element, in the collection's order, and stops where the
                # expression stops; the pipeline ran only the changed
                # elements. Take its pushes back and let plain evaluation
                # push them, now and on every later run (D31).
                evaluator.retract_effects_since(mark)
                self._states[state_key] = _PipelineState(
                    reducer=_Reducer(), env_fp=None, impure=True, effectful=True
                )
            elif answered:
                return result
        return _fallback(
            reducer_name, _items_at(core, view, head), stages, reducer_args
        )

    def min_last_rev(self, input_qualname: str) -> int | None:
        """Oldest log head any pipeline over this collection has ingested up to."""
        with self._lock:
            revs = [
                state.last_rev
                for key, state in self._states.items()
                if key[1] == input_qualname and not state.impure
            ]
        return min(revs) if revs else None

    # --- state construction and diff consumption ---
    def _build_state(
        self,
        reducer_name: str,
        reducer_args: dict[str, Any],
        reverse: bool,
        fingerprint: Any,
        core: Any,
        view: str | None,
        fused: Callable[[Any], tuple[bool, Any]],
        head: int,
    ) -> _PipelineState:
        reducer = _REDUCERS[reducer_name](reducer_args, reverse)
        state = _PipelineState(reducer=reducer, env_fp=fingerprint)
        contents = _raw_at(core, head)
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
        failed = state.failed
        for diff in diffs:
            for op, uid, item in _events(core.kind, view, diff, state):
                if op == "remove":
                    state.count -= 1
                    reducer.discard(uid)
                    if failed:
                        failed.discard(uid)
                    continue
                if op == "replace":
                    if failed:
                        failed.discard(uid)
                    try:
                        ok, mapped = fused(item)
                        if ok:
                            reducer.replace(uid, mapped)
                        else:
                            reducer.discard(uid)
                    except QueryCancelled:
                        raise
                    except Exception:
                        reducer.discard(uid)
                        failed.add(uid)
                    continue
                # Lists say where the element went; sets and dicts append.
                index = diff.get("index")
                appended = index is None or index == state.count
                state.count += 1
                try:
                    ok, mapped = fused(item)
                    if ok:
                        reducer.add(uid, mapped, appended)
                except QueryCancelled:
                    raise
                except Exception:
                    # Every reducer's ``add`` raises before it changes
                    # anything, so the element is simply not in it.
                    failed.add(uid)

    # --- source resolution and bookkeeping ---
    def _resolve_source(
        self, source: Any, reducer_name: str, stages: tuple[tuple[Any, ...], ...]
    ) -> tuple[Any, str | None] | None:
        if isinstance(source, CascadeList) or isinstance(source, CascadeSet):
            return source, None
        if isinstance(source, CascadeDict):
            # ``dict(d)`` copies the mapping; anything that iterates ``d``
            # (a stage, ``{x: ... for x in d}``) sees its keys (D36).
            bare_copy = reducer_name == "dict" and not stages
            return source, "items" if bare_copy else "keys"
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
            logical = tuple(st[0] for st in stages if st[0] != "materialize")
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


def _args_supported(reducer_name: str, reducer_args: dict[str, Any]) -> bool:
    """Whether the reducer's keyword arguments are ones the pipeline handles."""
    if reducer_name != "sorted":
        return True
    key = reducer_args.get("key")
    if key is not None and not callable(key):
        return False
    return type(reducer_args.get("sort_reverse", False)) in (bool, int)


def _raw_at(core: Any, head: int) -> Any:
    """The collection's contents at log head ``head``, in the log's format.

    Lists are ``(uid, value)`` pairs in order, sets a list of values in
    insertion order, dicts a plain dict. Taken from the live storage when
    ``head`` is the newest state (a mutation holds ``core.rw`` until its
    diff is logged, so the two agree), otherwise replayed from the log.
    """
    with core.rw:
        owner = core.owner
        if owner is not None and head == core.log.head:
            if core.kind == "list":
                return list(zip(owner._uids, list.__iter__(owner)))
            if core.kind == "dict":
                return _copy_dict(owner)
            # Insertion order, as the log replays it and iteration gives it.
            return list(owner._order)
        return core.contents_at(head)


def _order_at(core: Any, head: int) -> list[Any]:
    """Uids of the collection at ``head``, in the collection's order."""
    with core.rw:
        owner = core.owner
        if owner is not None and head == core.log.head:
            if core.kind == "list":
                return list(owner._uids)
            if core.kind == "dict":
                return list(dict.keys(owner))
            return list(owner._order)
        contents = core.contents_at(head)
    if core.kind == "list":
        return [uid for uid, _ in contents]
    return list(contents)


def _positions_at(core: Any, head: int, uids: list[Any]) -> dict[Any, int]:
    """Position in the collection at ``head`` of (at least) each of ``uids``."""
    if core.kind == "list" and len(uids) <= 8:
        with core.rw:
            owner = core.owner
            if owner is not None and head == core.log.head:
                index = owner._uids.index
                return {uid: index(uid) for uid in uids}
    order = _order_at(core, head)
    return dict(zip(order, range(len(order))))


def _items_at(core: Any, view: str | None, head: int) -> list[Any]:
    """What iterating the source yields at ``head``, as a plain list."""
    if core.kind == "list":
        with core.rw:
            owner = core.owner
            if owner is not None and head == core.log.head:
                return list(list.__iter__(owner))
        return [value for _, value in core.contents_at(head)]
    raw = _raw_at(core, head)
    if core.kind == "set":
        return raw
    if view == "values":
        return list(raw.values())
    if view == "items":
        return list(raw.items())
    return list(raw)


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
        state.present.pop(key, None)
        yield "remove", key, None
        return
    existed = key in state.present
    if existed:
        # A dict keeps the key it was first given: ``d[1.0] = v`` where
        # ``1`` is a key replaces the value under ``1``.
        key = state.present[key]
    else:
        state.present[key] = key
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
        elif type(value) is float and value != value:
            # Every NaN has one digest, but a set or dict tells NaN objects
            # apart: elements mapped in an earlier run would hold another
            # NaN than the ones mapped now, where Python has one object.
            raise ValueError("a captured NaN is not one value across runs")
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
    """Plain-Python execution: the builtins over the lazily staged source."""
    it: Any = source
    staged = False
    for st in stages:
        if st[0] == "reverse":
            it = reversed(it)
        elif st[0] == "materialize":
            it = list(it)  # where a list comprehension ends (D37)
        elif st[0] == "map":
            it = map(st[1], it)
        else:
            it = filter(st[1], it)
        staged = True
    if reducer_name == "len":
        # Staged ``len`` is only ever ``len([... for ...])``.
        return len(list(it)) if staged else len(it)
    if reducer_name == "sorted":
        kwargs = {}
        if "key" in reducer_args:
            kwargs["key"] = reducer_args["key"]
        if "sort_reverse" in reducer_args:
            kwargs["reverse"] = reducer_args["sort_reverse"]
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
