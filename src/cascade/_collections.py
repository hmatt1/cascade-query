"""Incremental collections that emit diffs into an append-only log.

``CascadeList``, ``CascadeSet``, and ``CascadeDict`` subclass the built-in
types, so every read path behaves exactly like the plain collection (inside a
query running at an older revision, reads return the contents as of that
revision; see ``_CollectionCore.read_view``). Mutating
methods are intercepted: they perform the native mutation first, then append a
diff record to the collection's log and bump a hidden engine input whose value
is the log head. That hidden input is how the rest of the engine sees the
collection: queries that touch it record a dependency on it, invalidation and
snapshots come from the existing input-version machinery, and the persistent
disk cache fingerprints it like any other input.

Diff formats follow the feature spec:

* list: ``{"action": "insert|update|remove", "uid": ID, "value": V}``
  (inserts additionally carry ``"index"``, the position at emit time, so a
  replay of the log reconstructs element order)
* set: ``{"action": "add|remove", "value": V}``
* dict: ``{"action": "upsert|remove", "key": K, "value": V}``

Every diff is tagged with a monotonic ``"rev"`` so downstream consumers can
checkpoint the last revision they processed and request only newer diffs.

When the owning engine has a ``cache_dir`` and the collection has an explicit
``name``, diffs are also written to an on-disk append-only log (event
sourcing). The log auto-compacts into a snapshot once the tail grows past
``compact_every`` diffs, so restarts replay a short tail instead of the full
history.
"""

from __future__ import annotations

import functools
import itertools
import threading
import uuid
from typing import Any, Callable, Iterable, Mapping

from . import _canonical
from ._errors import PersistentCacheError

_KIND_LIST = "list"
_KIND_SET = "set"
_KIND_DICT = "dict"

_auto_ids = itertools.count()

# Entries a collection's in-memory log may hold before it tries to fold the
# oldest ones into its base. The bar rises with the collection's size, so
# compaction (which replays the folded diffs) stays amortized O(1) per diff.
_MEMORY_COMPACT_THRESHOLD = 1024

# Head value the hidden input reports at revisions from before the collection
# was created in this engine. Pipelines treat it as an empty collection.
ABSENT_HEAD = -1

_BASE_LEN = {_KIND_LIST: list.__len__, _KIND_SET: set.__len__, _KIND_DICT: dict.__len__}


def _mutator(method: Callable[..., Any]) -> Callable[..., Any]:
    """Mark a mutating method so the reads it makes internally stay live.

    Mutators are written with ordinary reads (``len(self)``, ``self[i]``,
    ``key in self``). Called inside a query, a second mutation would otherwise
    read through ``read_view`` at the query's revision, which is already
    behind the first one, and emit diffs computed from stale contents.
    """

    @functools.wraps(method)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        local = self._core.mutating
        local.depth = getattr(local, "depth", 0) + 1
        try:
            return method(self, *args, **kwargs)
        finally:
            local.depth -= 1

    return wrapper


class _HistoryCompacted(RuntimeError):
    """Diffs older than the in-memory log's base were folded into it."""


class DiffLog:
    """Append-only, revision-tagged diff log with replay support.

    ``base_items`` is the materialized contents at ``base_rev`` (empty for a
    fresh collection, the loaded snapshot for a restored one). ``entries``
    holds every diff after ``base_rev``, so ``contents_at(rev)`` can rebuild
    the collection at any revision from ``base_rev`` on by replaying from the
    base. ``compact_to`` moves the base forward once older diffs are no
    longer needed.
    """

    def __init__(self, kind: str, *, base_rev: int = 0, base_items: Any = None) -> None:
        self.kind = kind
        self.lock = threading.RLock()
        self.base_rev = base_rev
        self.base_items = base_items if base_items is not None else self._empty()
        self.entries: list[dict[str, Any]] = []

    def _empty(self) -> Any:
        if self.kind == _KIND_LIST:
            return []
        if self.kind == _KIND_SET:
            return []
        return {}

    @property
    def head(self) -> int:
        with self.lock:
            return self.base_rev + len(self.entries)

    def append(self, diff: dict[str, Any]) -> int:
        with self.lock:
            rev = self.base_rev + len(self.entries) + 1
            diff["rev"] = rev
            self.entries.append(diff)
            return rev

    def since(self, after_rev: int, up_to: int) -> list[dict[str, Any]]:
        """All diffs with ``after_rev < rev <= up_to``.

        Raises ``_HistoryCompacted`` if ``after_rev`` predates the in-memory
        base: those diffs were folded into the base (or never loaded).
        """
        with self.lock:
            if after_rev < self.base_rev:
                raise _HistoryCompacted(
                    f"diff history before revision {self.base_rev} is not available"
                )
            lo = after_rev - self.base_rev
            hi = up_to - self.base_rev
            return self.entries[lo:hi]

    def contents_at(self, rev: int) -> Any:
        """Materialized contents at ``rev``, rebuilt by replaying the log.

        For lists the result is a list of ``(uid, value)`` pairs in order; for
        sets a list of values in insertion order; for dicts a plain dict.
        """
        with self.lock:
            if rev < self.base_rev:
                raise _HistoryCompacted(
                    f"contents before revision {self.base_rev} are not available"
                )
            diffs = self.since(self.base_rev, rev)
            if self.kind == _KIND_LIST:
                pairs: list[tuple[Any, Any]] = [tuple(p) for p in self.base_items]
                for d in diffs:
                    action = d["action"]
                    if action == "insert":
                        pairs.insert(d["index"], (d["uid"], d["value"]))
                    elif action == "update":
                        i = _uid_index(pairs, d["uid"])
                        pairs[i] = (d["uid"], d["value"])
                    else:
                        del pairs[_uid_index(pairs, d["uid"])]
                return pairs
            if self.kind == _KIND_SET:
                seen: dict[Any, None] = dict.fromkeys(self.base_items)
                for d in diffs:
                    if d["action"] == "add":
                        seen[d["value"]] = None
                    else:
                        seen.pop(d["value"], None)
                return list(seen)
            data = dict(self.base_items)
            for d in diffs:
                if d["action"] == "upsert":
                    data[d["key"]] = d["value"]
                else:
                    data.pop(d["key"], None)
            return data

    def compact_to(self, target: int) -> bool:
        """Fold every diff up to ``target`` into the base. Returns True if any were."""
        with self.lock:
            target = min(target, self.base_rev + len(self.entries))
            if target <= self.base_rev:
                return False
            base = self.contents_at(target)
            del self.entries[: target - self.base_rev]
            self.base_items = base
            self.base_rev = target
            return True


def _uid_index(pairs: list[tuple[Any, Any]], uid: Any) -> int:
    for i, (u, _) in enumerate(pairs):
        if u == uid:
            return i
    raise KeyError(uid)


class _CollectionCore:
    """Engine binding, diff emission, and disk persistence shared by all kinds."""

    def __init__(
        self,
        engine: Any,
        kind: str,
        name: str | None,
        compact_every: int,
    ) -> None:
        self.engine = engine
        self.kind = kind
        self.compact_every = compact_every
        self.named = name is not None
        if name is not None and "\x00" in name:
            raise ValueError("collection names must not contain NUL characters")
        self.name = name if name is not None else f"{kind}#{next(_auto_ids)}"
        self.disk = engine._disk if self.named else None
        # Unnamed collections in a disk-backed engine still need a unique
        # input id so a stale disk entry from a previous session can never
        # verify against a different collection that happens to share a head.
        suffix = "" if self.named else f":{uuid.uuid4().hex}"
        self.input_qualname = f"cascade_collection.{kind}.{self.name}{suffix}"
        self.log: DiffLog | None = None
        self.next_uid = 0
        self.loaded: Any = None
        self.owner: Any = None
        self._compacted_base = 0
        self._next_memory_check = _MEMORY_COMPACT_THRESHOLD
        # (head, contents) last materialized for a reader at an older revision.
        self._snapshot_cache: tuple[int, Any] | None = None
        # Per-thread depth of mutating methods in progress (see _mutator).
        self.mutating = threading.local()

        if self.disk is not None:
            snapshot, tail = self.disk.collection_load(self.name)
            if snapshot is not None and snapshot["kind"] != kind:
                raise PersistentCacheError(
                    f"persisted collection {self.name!r} is a {snapshot['kind']}, not a {kind}"
                )
            if snapshot is None and not tail:
                # First sighting of this name: stamp an empty snapshot so
                # later sessions can verify the kind before replaying diffs.
                self.disk.collection_snapshot(
                    self.name,
                    {
                        "kind": kind,
                        "base_rev": 0,
                        "next_uid": 0,
                        "items": [] if kind != _KIND_DICT else {},
                    },
                    0,
                )
            else:
                if snapshot is not None:
                    self.log = DiffLog(
                        kind,
                        base_rev=snapshot["base_rev"],
                        base_items=snapshot["items"],
                    )
                    self.next_uid = snapshot["next_uid"]
                    self._compacted_base = snapshot["base_rev"]
                else:
                    self.log = DiffLog(kind)
                for _rev, diff in tail:
                    self.log.entries.append(diff)
                    uid = diff.get("uid")
                    if isinstance(uid, int) and uid >= self.next_uid:
                        self.next_uid = uid + 1
                self.loaded = self.log.contents_at(self.log.head)
        if self.log is None:
            self.log = DiffLog(kind)

        def _head() -> int:
            # Only reached at revisions with no recorded head, which are the
            # ones from before this collection was created (it records its
            # head right below). Reporting the live head there let an older
            # snapshot freeze whatever the collection held when first read.
            return ABSENT_HEAD

        _head.__module__ = "cascade"
        _head.__qualname__ = self.input_qualname
        self.handle = engine.input(_head)
        self.handle.set(self.log.head)

    def read_view(self) -> Any:
        """Contents a direct read must use, or None for the live storage.

        Outside a query this is always None. Inside one it records the
        dependency on this collection and, when the query runs at a revision
        older than the collection's newest change (a snapshot read, or a
        writer that got ahead of a running query), returns the contents as of
        that revision: a plain list, set, or dict that must not be mutated or
        handed out. Without this, ``xs[0]``, ``len(xs)``, ``for x in xs`` and
        every query the AST rewriter leaves alone would read live data.
        """
        if not self.engine._evaluator.in_runtime():
            return None
        head = self.handle()
        if getattr(self.mutating, "depth", 0):
            return None
        log = self.log
        with log.lock:
            if head != ABSENT_HEAD and head >= log.head:
                return None
            cached = self._snapshot_cache
            if cached is not None and cached[0] == head:
                return cached[1]
            if head == ABSENT_HEAD:
                # A revision from before this collection was created.
                raw: Any = [] if self.kind != _KIND_DICT else {}
            else:
                raw = log.contents_at(head)
            if self.kind == _KIND_LIST:
                view: Any = [value for _uid, value in raw]
            elif self.kind == _KIND_SET:
                view = set(raw)
            else:
                view = raw
            self._snapshot_cache = (head, view)
            return view

    def head_for_current_context(self) -> int:
        """Log head as seen by the active runtime (snapshot-aware dependency read)."""
        return self.handle()

    def emit(self, diffs: list[dict[str, Any]]) -> None:
        if not diffs:
            return
        start = self.log.head
        for i, diff in enumerate(diffs):
            diff["rev"] = start + i + 1
        tx = self.engine._active_transaction()
        if self.disk is not None:
            # Persist (or, inside a transaction, check the diffs can be
            # persisted) before committing to the in-memory log, so a failure
            # leaves no partial state: the owner collection is rolled back to
            # the log's truth.
            try:
                if tx is None:
                    self.disk.collection_append_many(
                        self.name, [(diff["rev"], diff) for diff in diffs]
                    )
                else:
                    for diff in diffs:
                        _canonical.encode(diff)
            except PersistentCacheError:
                self._rollback()
                raise
            except Exception as exc:
                self._rollback()
                raise PersistentCacheError(
                    f"collection {self.name!r} received a value that cannot be persisted: {exc}"
                ) from exc
        if tx is not None:
            tx.collections.setdefault(self, start)
            if self.disk is not None:
                tx.pending_disk.setdefault(self, []).extend(
                    (diff["rev"], diff) for diff in diffs
                )
        for diff in diffs:
            self.log.append(diff)
        self.handle.set(self.log.head)
        if tx is None:
            self._maybe_compact_disk()
        if len(self.log.entries) >= self._next_memory_check:
            self.compact_memory()

    def _maybe_compact_disk(self) -> None:
        if (
            self.disk is not None
            and self.log.head - self._compacted_base >= self.compact_every
        ):
            self.compact()

    def write_pending(self, batch: list[tuple[int, dict[str, Any]]]) -> None:
        """Write diffs a transaction held back; called when it commits."""
        try:
            self.disk.collection_append_many(self.name, batch)
        except PersistentCacheError:
            raise
        except Exception as exc:
            raise PersistentCacheError(
                f"collection {self.name!r} could not persist its changes: {exc}"
            ) from exc

    def after_commit(self) -> None:
        """Run the disk compaction a transaction deferred."""
        self._maybe_compact_disk()

    def roll_back_to(self, head: int) -> None:
        """Undo every change after ``head`` (a failed transaction).

        In-memory compaction never folds past the committed head, so the
        diffs being dropped are still in the log.
        """
        log = self.log
        with log.lock:
            if log.head > head:
                del log.entries[head - log.base_rev :]
            # The same head number can now come to mean different contents.
            self._snapshot_cache = None
        self._rollback()

    def _live_len(self) -> int:
        # The owner's own __len__ records a dependency inside a query.
        if self.owner is None:
            return 0
        return _BASE_LEN[self.kind](self.owner)

    def compact_memory(self) -> None:
        """Fold in-memory diffs that no reader can still ask for into the base.

        Readers at a pinned engine revision need the log from the head that
        revision saw (a hard limit). Pipelines need it from their last ingested
        head, but one lagging by more than the collection's size is cheaper to
        rebuild than to replay, so that limit is soft.
        """
        log = self.log
        live_len = self._live_len()
        store = self.engine._store
        with store.lock:
            floor = store.history_floor_locked()
            version = store.input_version_at((self.handle.id, ()), floor)
        head = log.head
        # No version at the floor means the collection did not exist yet at
        # any revision still in use.
        hard = version.value if version is not None else head
        min_last_rev = self.engine._incremental_rt.min_last_rev(self.input_qualname)
        if min_last_rev is None:
            soft = head
        else:
            lag_allowed = max(_MEMORY_COMPACT_THRESHOLD, live_len)
            soft = max(min_last_rev, head - lag_allowed)
        log.compact_to(min(hard, soft))
        self._next_memory_check = max(
            _MEMORY_COMPACT_THRESHOLD, live_len, 2 * len(log.entries)
        )

    def _rollback(self) -> None:
        """Reset the owner's python-level storage to the committed log state."""
        if self.owner is not None:
            self.owner._reset_to(self.log.contents_at(self.log.head))

    def compact(self) -> None:
        """Squash the on-disk event log into a single snapshot at the current head."""
        if self.disk is None:
            return
        head = self.log.head
        items = self.log.contents_at(head)
        if self.kind == _KIND_LIST:
            items = [list(p) for p in items]
        record = {
            "kind": self.kind,
            "base_rev": head,
            "next_uid": self.next_uid,
            "items": items,
        }
        try:
            self.disk.collection_snapshot(self.name, record, head)
        except PersistentCacheError:
            raise
        except Exception as exc:
            raise PersistentCacheError(
                f"collection {self.name!r} holds a value that cannot be persisted: {exc}"
            ) from exc
        self._compacted_base = head


class CascadeList(list):
    """A ``list`` that emits ``insert``/``update``/``remove`` diffs on mutation.

    Every element gets a hidden monotonic unique id. Positional operations
    translate indices to uids through an internal parallel index, so a diff
    always identifies its element stably even as positions shift.
    """

    def __init__(
        self,
        engine: Any,
        iterable: Iterable[Any] = (),
        *,
        name: str | None = None,
        compact_every: int = 1024,
    ) -> None:
        self._core = _CollectionCore(engine, _KIND_LIST, name, compact_every)
        self._core.owner = self
        self._uids: list[int] = []
        if self._core.loaded is not None:
            super().__init__(v for _, v in self._core.loaded)
            self._uids = [u for u, _ in self._core.loaded]
        else:
            super().__init__()
            self.extend(iterable)

    def _reset_to(self, contents: list[tuple[int, Any]]) -> None:
        list.__init__(self, (v for _, v in contents))
        self._uids = [u for u, _ in contents]

    def compact(self) -> None:
        """Squash the on-disk event log into a snapshot at the current head."""
        self._core.compact()

    def _new_uid(self) -> int:
        uid = self._core.next_uid
        self._core.next_uid = uid + 1
        return uid

    def _insert_diff(self, index: int, value: Any) -> dict[str, Any]:
        uid = self._new_uid()
        self._uids.insert(index, uid)
        return {"action": "insert", "uid": uid, "value": value, "index": index}

    # --- mutators ---
    @_mutator
    def append(self, value: Any) -> None:
        list.append(self, value)
        self._core.emit([self._insert_diff(len(self) - 1, value)])

    @_mutator
    def extend(self, iterable: Iterable[Any]) -> None:
        values = list(iterable)
        list.extend(self, values)
        start = len(self) - len(values)
        self._core.emit([self._insert_diff(start + i, v) for i, v in enumerate(values)])

    @_mutator
    def insert(self, index: int, value: Any) -> None:
        eff = _clamped_insert_index(index, len(self))
        list.insert(self, index, value)
        self._core.emit([self._insert_diff(eff, value)])

    @_mutator
    def remove(self, value: Any) -> None:
        index = self.index(value)
        list.__delitem__(self, index)
        uid = self._uids.pop(index)
        self._core.emit([{"action": "remove", "uid": uid, "value": value}])

    @_mutator
    def pop(self, index: int = -1) -> Any:
        value = list.pop(self, index)
        eff = index if index >= 0 else len(self._uids) + index
        uid = self._uids.pop(eff)
        self._core.emit([{"action": "remove", "uid": uid, "value": value}])
        return value

    @_mutator
    def clear(self) -> None:
        diffs = [
            {"action": "remove", "uid": uid, "value": value}
            for uid, value in zip(self._uids, self)
        ]
        list.clear(self)
        self._uids.clear()
        self._core.emit(diffs)

    @_mutator
    def __setitem__(self, index: int | slice, value: Any) -> None:
        if isinstance(index, slice):
            self._set_slice(index, value)
            return
        list.__setitem__(self, index, value)
        eff = index if index >= 0 else len(self) + index
        uid = self._uids[eff]
        self._core.emit([{"action": "update", "uid": uid, "value": value}])

    def _set_slice(self, index: slice, value: Any) -> None:
        values = list(value)
        start, _stop, step = index.indices(len(self))
        positions = list(range(*index.indices(len(self))))
        if step != 1:
            list.__setitem__(self, index, values)
            self._core.emit(
                [
                    {"action": "update", "uid": self._uids[pos], "value": v}
                    for pos, v in zip(positions, values)
                ]
            )
            return
        removed = [
            {"action": "remove", "uid": self._uids[pos], "value": self[pos]}
            for pos in positions
        ]
        list.__setitem__(self, index, values)
        for pos in reversed(positions):
            del self._uids[pos]
        inserted = [self._insert_diff(start + i, v) for i, v in enumerate(values)]
        self._core.emit(removed + inserted)

    @_mutator
    def __delitem__(self, index: int | slice) -> None:
        if isinstance(index, slice):
            positions = list(range(*index.indices(len(self))))
            diffs = [
                {"action": "remove", "uid": self._uids[pos], "value": self[pos]}
                for pos in positions
            ]
            list.__delitem__(self, index)
            for pos in sorted(positions, reverse=True):
                del self._uids[pos]
            self._core.emit(diffs)
            return
        value = self[index]
        list.__delitem__(self, index)
        eff = index if index >= 0 else len(self._uids) + index
        uid = self._uids.pop(eff)
        self._core.emit([{"action": "remove", "uid": uid, "value": value}])

    @_mutator
    def sort(
        self, *, key: Callable[[Any], Any] | None = None, reverse: bool = False
    ) -> None:
        pairs = list(zip(self._uids, self))
        pairs.sort(
            key=(lambda p: p[1]) if key is None else (lambda p: key(p[1])),
            reverse=reverse,
        )
        self._reorder(pairs)

    @_mutator
    def reverse(self) -> None:
        self._reorder(list(zip(self._uids, self))[::-1])

    def _reorder(self, pairs: list[tuple[int, Any]]) -> None:
        removed = [
            {"action": "remove", "uid": uid, "value": value}
            for uid, value in zip(self._uids, self)
        ]
        list.__init__(self, [v for _, v in pairs])
        self._uids = []
        inserted = []
        for i, (uid, v) in enumerate(pairs):
            self._uids.append(uid)
            inserted.append({"action": "insert", "uid": uid, "value": v, "index": i})
        self._core.emit(removed + inserted)

    @_mutator
    def __iadd__(self, other: Iterable[Any]) -> CascadeList:
        self.extend(other)
        return self

    @_mutator
    def __imul__(self, n: int) -> CascadeList:
        if n <= 0:
            self.clear()
        else:
            base = list(self)
            for _ in range(n - 1):
                self.extend(base)
        return self

    # --- tracked reads ---
    # Each read goes through ``read_view``: the live storage normally, the
    # contents as of the reader's revision inside a query that runs behind
    # the newest change (see ``_CollectionCore.read_view``).
    def _view(self) -> list:
        view = self._core.read_view()
        return self if view is None else view

    def __iter__(self):
        return list.__iter__(self._view())

    def __reversed__(self):
        return list.__reversed__(self._view())

    def __len__(self) -> int:
        return list.__len__(self._view())

    def __getitem__(self, index: Any) -> Any:
        return list.__getitem__(self._view(), index)

    def __contains__(self, item: Any) -> bool:
        return list.__contains__(self._view(), item)

    def __eq__(self, other: Any) -> bool:
        return list.__eq__(self._view(), other)

    def __ne__(self, other: Any) -> bool:
        return list.__ne__(self._view(), other)

    __hash__ = None

    def index(self, value: Any, *args: Any) -> int:
        return list.index(self._view(), value, *args)

    def count(self, value: Any) -> int:
        return list.count(self._view(), value)

    def copy(self) -> list:
        return list(list.__iter__(self._view()))


def _clamped_insert_index(index: int, length: int) -> int:
    if index < 0:
        index += length
    return max(0, min(length, index))


class CascadeSet(set):
    """A ``set`` that emits ``add``/``remove`` diffs; the value is its own uid."""

    def __init__(
        self,
        engine: Any,
        iterable: Iterable[Any] = (),
        *,
        name: str | None = None,
        compact_every: int = 1024,
    ) -> None:
        self._core = _CollectionCore(engine, _KIND_SET, name, compact_every)
        self._core.owner = self
        super().__init__()
        if self._core.loaded is not None:
            set.update(self, self._core.loaded)
        else:
            self.update(iterable)

    def _reset_to(self, contents: Iterable[Any]) -> None:
        set.clear(self)
        set.update(self, contents)

    def compact(self) -> None:
        """Squash the on-disk event log into a snapshot at the current head."""
        self._core.compact()

    def _emit_adds(self, values: Iterable[Any]) -> None:
        self._core.emit([{"action": "add", "value": v} for v in values])

    def _emit_removes(self, values: Iterable[Any]) -> None:
        self._core.emit([{"action": "remove", "value": v} for v in values])

    # --- mutators ---
    @_mutator
    def add(self, value: Any) -> None:
        if value in self:
            return
        set.add(self, value)
        self._emit_adds([value])

    @_mutator
    def remove(self, value: Any) -> None:
        set.remove(self, value)
        self._emit_removes([value])

    @_mutator
    def discard(self, value: Any) -> None:
        if value not in self:
            return
        set.discard(self, value)
        self._emit_removes([value])

    @_mutator
    def pop(self) -> Any:
        value = set.pop(self)
        self._emit_removes([value])
        return value

    @_mutator
    def clear(self) -> None:
        values = list(self)
        set.clear(self)
        self._emit_removes(values)

    @_mutator
    def update(self, *others: Iterable[Any]) -> None:
        added = []
        for other in others:
            for v in other:
                if v not in self:
                    set.add(self, v)
                    added.append(v)
        self._emit_adds(added)

    @_mutator
    def difference_update(self, *others: Iterable[Any]) -> None:
        removed = []
        for other in others:
            for v in other:
                if v in self:
                    set.discard(self, v)
                    removed.append(v)
        self._emit_removes(removed)

    @_mutator
    def intersection_update(self, *others: Iterable[Any]) -> None:
        keep = set(self).intersection(*others) if others else set(self)
        removed = [v for v in self if v not in keep]
        set.difference_update(self, removed)
        self._emit_removes(removed)

    @_mutator
    def symmetric_difference_update(self, other: Iterable[Any]) -> None:
        other = set(other)
        removed = [v for v in other if v in self]
        added = [v for v in other if v not in self]
        set.symmetric_difference_update(self, other)
        self._core.emit(
            [{"action": "remove", "value": v} for v in removed]
            + [{"action": "add", "value": v} for v in added]
        )

    @_mutator
    def __ior__(self, other: Any) -> CascadeSet:
        self.update(other)
        return self

    @_mutator
    def __isub__(self, other: Any) -> CascadeSet:
        self.difference_update(other)
        return self

    @_mutator
    def __iand__(self, other: Any) -> CascadeSet:
        self.intersection_update(other)
        return self

    @_mutator
    def __ixor__(self, other: Any) -> CascadeSet:
        self.symmetric_difference_update(other)
        return self

    # --- tracked reads ---
    def _view(self) -> set:
        view = self._core.read_view()
        return self if view is None else view

    def __iter__(self):
        return set.__iter__(self._view())

    def __len__(self) -> int:
        return set.__len__(self._view())

    def __contains__(self, item: Any) -> bool:
        return set.__contains__(self._view(), item)


class _CascadeDictView:
    """Wrapper carrying the parent dict plus which projection to reduce over.

    Iteration, length, and membership delegate to the real dict view, so a
    non-rewritten code path behaves exactly like ``dict.keys()`` and friends.
    """

    def __init__(self, parent: CascadeDict, which: str) -> None:
        self.parent = parent
        self.which = which

    def _raw(self):
        # Records the dependency and honours the reader's revision.
        return getattr(dict, self.which)(self.parent._view())

    def __iter__(self):
        return iter(self._raw())

    def __reversed__(self):
        return reversed(self._raw())

    def __len__(self) -> int:
        return len(self._raw())

    def __contains__(self, item: Any) -> bool:
        return item in self._raw()

    def __repr__(self) -> str:
        live = getattr(dict, self.which)(self.parent)
        return f"cascade_{self.which}({list(live)!r})"


class CascadeDict(dict):
    """A ``dict`` that emits ``upsert``/``remove`` diffs; the key is the uid.

    ``keys()``, ``values()``, and ``items()`` return intercepted views so
    downstream reducers know which projection they consume.
    """

    _MISSING = object()

    def __init__(
        self,
        engine: Any,
        mapping: Mapping[Any, Any] | Iterable[tuple[Any, Any]] = (),
        *,
        name: str | None = None,
        compact_every: int = 1024,
        **kwargs: Any,
    ) -> None:
        self._core = _CollectionCore(engine, _KIND_DICT, name, compact_every)
        self._core.owner = self
        super().__init__()
        if self._core.loaded is not None:
            dict.update(self, self._core.loaded)
        else:
            self.update(mapping, **kwargs)

    def _reset_to(self, contents: dict[Any, Any]) -> None:
        dict.clear(self)
        dict.update(self, contents)

    def compact(self) -> None:
        """Squash the on-disk event log into a snapshot at the current head."""
        self._core.compact()

    # --- mutators ---
    @_mutator
    def __setitem__(self, key: Any, value: Any) -> None:
        dict.__setitem__(self, key, value)
        self._core.emit([{"action": "upsert", "key": key, "value": value}])

    @_mutator
    def __delitem__(self, key: Any) -> None:
        value = dict.__getitem__(self, key)
        dict.__delitem__(self, key)
        self._core.emit([{"action": "remove", "key": key, "value": value}])

    @_mutator
    def update(self, *args: Any, **kwargs: Any) -> None:
        incoming = dict(*args, **kwargs)
        dict.update(self, incoming)
        self._core.emit(
            [{"action": "upsert", "key": k, "value": v} for k, v in incoming.items()]
        )

    @_mutator
    def pop(self, key: Any, default: Any = _MISSING) -> Any:
        if key not in self:
            if default is self._MISSING:
                raise KeyError(key)
            return default
        value = dict.pop(self, key)
        self._core.emit([{"action": "remove", "key": key, "value": value}])
        return value

    @_mutator
    def popitem(self) -> tuple[Any, Any]:
        key, value = dict.popitem(self)
        self._core.emit([{"action": "remove", "key": key, "value": value}])
        return key, value

    @_mutator
    def clear(self) -> None:
        diffs = [{"action": "remove", "key": k, "value": v} for k, v in self.items()]
        dict.clear(self)
        self._core.emit(diffs)

    @_mutator
    def setdefault(self, key: Any, default: Any = None) -> Any:
        if key in self:
            return dict.__getitem__(self, key)
        self[key] = default
        return default

    @_mutator
    def __ior__(self, other: Any) -> CascadeDict:
        self.update(other)
        return self

    # --- intercepted views ---
    def keys(self) -> _CascadeDictView:
        return _CascadeDictView(self, "keys")

    def values(self) -> _CascadeDictView:
        return _CascadeDictView(self, "values")

    def items(self) -> _CascadeDictView:
        return _CascadeDictView(self, "items")

    # --- tracked reads ---
    def _view(self) -> dict:
        view = self._core.read_view()
        return self if view is None else view

    def __iter__(self):
        return dict.__iter__(self._view())

    def __reversed__(self):
        return dict.__reversed__(self._view())

    def __len__(self) -> int:
        return dict.__len__(self._view())

    def __getitem__(self, key: Any) -> Any:
        return dict.__getitem__(self._view(), key)

    def __contains__(self, key: Any) -> bool:
        return dict.__contains__(self._view(), key)

    def get(self, key: Any, default: Any = None) -> Any:
        return dict.get(self._view(), key, default)


CASCADE_COLLECTION_TYPES = (CascadeList, CascadeSet, CascadeDict)
