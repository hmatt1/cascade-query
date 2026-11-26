from __future__ import annotations

import bisect
import dataclasses
import threading
import time
import weakref
from collections import OrderedDict, defaultdict, deque
from collections.abc import Sequence
from typing import Any, Callable, Mapping

from ._code_refs import refs_hash
from ._errors import MutatedValueError, SnapshotExpiredError
from ._keys import display as display_args
from ._serde import hash_bytecode, stable_value_digest, unencodable_error
from ._state import (
    Dependency,
    InFlight,
    InputKey,
    InputVersion,
    MemoEntry,
    QueryKey,
    Snapshot,
    TraceEvent,
)

_TraceRow = tuple[str, "QueryKey", int, str, float]

# Runs a ``fixed_point=`` query gets to converge before the engine gives up
# with ``CycleError``. ``engine.query(max_iterations=...)`` overrides it.
DEFAULT_FIXED_POINT_ITERATIONS = 1000


def _version_revision(version: InputVersion) -> int:
    return version.revision


def _cell_value(cell: Any) -> Any:
    try:
        return cell.cell_contents
    except ValueError:  # an empty cell
        return _cell_value


def _origin(fn: Any) -> Any:
    """The function as written: a rewritten query points back at its source."""
    return getattr(fn, "__wrapped__", fn)


def _same_captures(old: Callable[..., Any], new: Callable[..., Any]) -> bool:
    """True when two functions with equal bytecode also see the same objects.

    Bytecode alone cannot tell apart the functions a factory or a loop makes
    from one body: they differ only in what they captured. Two functions are
    the same query when their globals, closure contents and defaults are the
    very same objects. Identity, not equality: comparing arbitrary captured
    objects could be slow, raise, or call two separate counters "equal".
    """
    if getattr(old, "__globals__", None) is not getattr(new, "__globals__", None):
        return False
    old_cells = getattr(old, "__closure__", None) or ()
    new_cells = getattr(new, "__closure__", None) or ()
    if len(old_cells) != len(new_cells):
        return False
    for a, b in zip(old_cells, new_cells):
        if a is not b and _cell_value(a) is not _cell_value(b):
            return False
    old_defaults = getattr(old, "__defaults__", None) or ()
    new_defaults = getattr(new, "__defaults__", None) or ()
    if len(old_defaults) != len(new_defaults):
        return False
    if any(a is not b for a, b in zip(old_defaults, new_defaults)):
        return False
    old_kw = getattr(old, "__kwdefaults__", None) or {}
    new_kw = getattr(new, "__kwdefaults__", None) or {}
    if old_kw.keys() != new_kw.keys():
        return False
    return all(old_kw[k] is new_kw[k] for k in old_kw)


class _StoredHashes(dict):  # type: ignore[type-arg]
    """Hash each function's persisted results are stored and checked under.

    What is assigned is the hash known at registration: the function's own
    bytecode, ``version=`` and the salts. What is read adds the hash of what
    the function refers to (``_code_refs.refs_hash``: helpers, constants,
    closure values, defaults). That part is worked out on first use and not
    at registration, because a helper defined below a query in its module
    does not exist yet when the query is decorated. It is then kept until
    the function is registered again.
    """

    def __init__(self, functions: dict[str, Callable[..., Any]]) -> None:
        super().__init__()
        self._functions = functions
        self._full: dict[str, str] = {}

    def __setitem__(self, fid: str, base: str) -> None:
        super().__setitem__(fid, base)
        self._full.pop(fid, None)

    def get(self, fid: str, default: Any = None) -> Any:  # type: ignore[override]
        full = self._full.get(fid)
        if full is not None:
            return full
        base = super().get(fid)
        if base is None:
            return default
        fn = self._functions.get(fid)
        refs = refs_hash(fn) if fn is not None else ""
        full = f"{base}+{refs}" if refs else base
        self._full[fid] = full
        return full

    def __getitem__(self, fid: str) -> str:
        full = self.get(fid)
        if full is None:
            raise KeyError(fid)
        return full


class GraphStore:
    def __init__(
        self,
        *,
        max_entries: int,
        trace_limit: int,
        stats: bool = False,
        stats_eviction_recent_cap: int = 32,
        monotonic_seconds: Callable[[], float] | None = None,
        value_digest: Callable[[Any], str] | None = None,
        allow_opaque: bool = False,
        cache_version: str | int | None = None,
        check_mutations: bool = False,
    ) -> None:
        self.lock = threading.RLock()
        # Engine-wide salt of the hashes persisted results are stored under
        # (``_stored_hash_locked``): results stored under another one are
        # not served.
        self._cache_version = cache_version
        self._value_digest: Callable[[Any], str] = value_digest or stable_value_digest
        # Without a disk cache nothing has to be decoded again, so a result
        # or input value the codec cannot encode is kept as it is, under a
        # digest no other value ever has (``digest_or_opaque``).
        self._allow_opaque = allow_opaque
        # Whether a query checks, when its body finishes, that the values it
        # read were not changed in place (``mutated_values``). Off by default:
        # it costs one digest of each value read, per run.
        self.check_mutations_on_run = check_mutations
        self._opaque_lock = threading.Lock()
        self._opaque_count = 0
        self.revision = 0
        self.cancel_epoch = 0
        self.next_access_id = 0
        # Source of ``MemoEntry.changed_at`` stamps; see ``new_change_stamp_locked``.
        self.change_stamp = 0
        # Member of a fixed-point cycle -> the head it was last stored with.
        # Outlives the member's memo: a member whose result was dropped is
        # brought back by entering the cycle through its head, as its cached
        # result would have been (D25). One entry per member ever stored.
        self.cycle_head_of: dict[QueryKey, QueryKey] = {}
        self.max_entries = max_entries
        self.trace_limit = trace_limit
        # Raw rows (event, key, revision, detail, timestamp). ``key`` is the
        # QueryKey tuple, formatted to a string only when traces are read.
        # Building TraceEvent objects eagerly cost ~10% of a cold compute.
        self.trace: deque[_TraceRow] = deque(maxlen=trace_limit)

        self._monotonic_seconds: Callable[[], float] = (
            monotonic_seconds or time.perf_counter
        )
        self._stats_enabled = stats
        self._stats_eviction_recent_cap = max(0, stats_eviction_recent_cap)
        self._stats_by_key: dict[str, float] = {}
        self._stats_evictions_total = 0
        self._stats_evictions_recent: deque[str] = deque(
            maxlen=self._stats_eviction_recent_cap
            if self._stats_eviction_recent_cap > 0
            else None
        )
        self._stats_disk_hits = 0
        self._stats_disk_misses = 0
        self._stats_disk_stores = 0

        self.inputs: dict[InputKey, list[InputVersion]] = {}
        # Input history retention. A revision is pinned while something may
        # still read inputs at it: a snapshot from Engine.snapshot() that has
        # not been released, or a call that is still running. Versions no
        # pinned revision can reach are trimmed, and ``history_floor`` is the
        # oldest revision whose history is still complete.
        self._pins: dict[int, int] = {}
        self.history_floor = 0
        # Unpins that arrived while this thread held ``lock`` (a finalizer or
        # a generator/coroutine closed by GC mid critical section). Applied
        # the next time the registry is touched under the lock.
        self._deferred_unpins: deque[int] = deque()
        # Keys holding more than one version, and appends since the last sweep.
        self._multi_version_keys: set[InputKey] = set()
        self._trim_debt = 0
        self.queries: dict[str, Callable[..., Any]] = {}
        self.input_fns: dict[str, Callable[..., Any]] = {}
        self.input_hashes: dict[str, str] = _StoredHashes(self.input_fns)
        self.query_hashes: dict[str, str] = _StoredHashes(self.queries)
        self.query_memoize: dict[str, bool] = {}
        self.query_fixed_points: dict[str, Any] = {}
        self.query_fixed_point_limits: dict[str, int] = {}
        self.query_cache_exceptions: dict[
            str, bool | tuple[type[BaseException], ...]
        ] = {}
        self.query_ttl: dict[str, float | None] = {}
        # Bytecode hash of the registered function, before any salt (see
        # ``_supersede_locked``). ``query_hashes`` / ``input_hashes`` hold the
        # value stored with, and compared against, persisted results.
        self._code_hashes: dict[tuple[str, str], str] = {}
        self._hash_salts: dict[tuple[str, str], int] = {}
        # Live handles per ("query" | "input", id), told when a different
        # function takes over their name. Weak: handles die with their users.
        self._handles: dict[tuple[str, str], weakref.WeakSet[Any]] = {}
        self.memos: dict[QueryKey, MemoEntry] = {}
        self.dependents: dict[QueryKey, set[QueryKey]] = defaultdict(set)
        # Keyed by (query key, snapshot revision) to keep dedup snapshot-safe.
        self.in_flight: dict[tuple[QueryKey, int], InFlight] = {}
        # The in-flight run each blocked thread or awaiting task waits for,
        # keyed like ``InFlight.owner``. Lets a caller see that waiting
        # would never end (Evaluator._enter_wait_locked). A record goes
        # when the run is released, or when the waiter gives up first.
        self.waiting_on: dict[Any, InFlight] = {}
        # Recency order of memo keys, least recently used first. Mirrors
        # ``memos`` and agrees with ``MemoEntry.last_access`` ordering. Replaces
        # a lazy heap that gained an entry per cache hit and was only drained by
        # eviction, so it grew without bound on hot read paths.
        self._lru: OrderedDict[QueryKey, None] = OrderedDict()
        # Refcount: memo rows participating in an in-flight cache/verify sequence must not
        # be LRU-evicted (nested recompute can otherwise drop the parent before verify ends).
        self._memo_verify_pin_count: dict[QueryKey, int] = {}

    def monotonic_seconds(self) -> float:
        return self._monotonic_seconds()

    def is_stats_enabled(self) -> bool:
        return self._stats_enabled

    def set_stats_enabled(self, enabled: bool) -> None:
        with self.lock:
            self._stats_enabled = enabled

    def set_stats_eviction_recent_cap(self, cap: int) -> None:
        with self.lock:
            self._stats_eviction_recent_cap = max(0, cap)
            new_max = (
                self._stats_eviction_recent_cap
                if self._stats_eviction_recent_cap > 0
                else None
            )
            recent = list(self._stats_evictions_recent)
            if self._stats_eviction_recent_cap > 0:
                recent = recent[-self._stats_eviction_recent_cap :]
            else:
                recent = []
            self._stats_evictions_recent = deque(recent, maxlen=new_max)

    def reset_stats(self) -> None:
        with self.lock:
            self._stats_by_key.clear()
            self._stats_evictions_total = 0
            self._stats_evictions_recent.clear()
            self._stats_disk_hits = 0
            self._stats_disk_misses = 0
            self._stats_disk_stores = 0

    def stats_summary(self) -> dict[str, Any]:
        with self.lock:
            return {
                "by_key": dict(sorted(self._stats_by_key.items())),
                "evictions_total": self._stats_evictions_total,
                "evictions_recent": list(self._stats_evictions_recent),
                "memo_count": len(self.memos),
                "max_entries": self.max_entries,
                "disk_hits": self._stats_disk_hits,
                "disk_misses": self._stats_disk_misses,
                "disk_stores": self._stats_disk_stores,
            }

    def record_disk_hit(self) -> None:
        if not self._stats_enabled:
            return
        with self.lock:
            if self._stats_enabled:
                self._stats_disk_hits += 1

    def record_disk_miss(self) -> None:
        if not self._stats_enabled:
            return
        with self.lock:
            if self._stats_enabled:
                self._stats_disk_misses += 1

    def record_disk_store(self) -> None:
        if not self._stats_enabled:
            return
        with self.lock:
            if self._stats_enabled:
                self._stats_disk_stores += 1

    def record_query_body_time(self, key: QueryKey, seconds: float) -> None:
        if seconds < 0:
            seconds = 0.0
        with self.lock:
            if not self._stats_enabled:
                return
            sk = self.key_to_str(key)
            self._stats_by_key[sk] = self._stats_by_key.get(sk, 0.0) + seconds

    def register_query(
        self,
        query_id: str,
        fn: Callable[..., Any],
        *,
        memoize: bool = True,
        fixed_point: Any = None,
        has_fixed_point: bool = False,
        cache_exceptions: bool | tuple[type[BaseException], ...] = False,
        ttl: float | None = None,
        max_iterations: int = DEFAULT_FIXED_POINT_ITERATIONS,
        version: str | int | None = None,
    ) -> Callable[..., Any]:
        """Register ``fn`` as ``query_id`` and return the function to run.

        That is ``fn``, unless the same query is already registered (see
        ``_replace_locked``): then the registered function is returned, and
        cached results and existing handles stay valid.

        ``version`` is part of the code hash: changing it is a code change,
        in memory and for persisted results.
        """
        fn_hash = hash_bytecode(fn)
        if version is not None:
            fn_hash = f"{fn_hash}|version={version!r}"
        slot = ("query", query_id)
        with self.lock:
            known = query_id in self.queries
            fn, drop = self._replace_locked(
                slot, self.queries.get(query_id), fn, fn_hash
            )
            if known and self.query_ttl.get(query_id) != ttl:
                # Cached results, and everything computed from them, carry
                # an expiry worked out from the old TTL.
                drop = True
            if drop:
                drop_keys = [
                    k for k in self.memos if k[0] == "query" and k[1] == query_id
                ]
                for k in drop_keys:
                    self.drop_memo_locked(k)
                self.reverify_all_locked()
            self.queries[query_id] = fn
            self.query_hashes[query_id] = self._stored_hash_locked(slot)
            self.query_memoize[query_id] = memoize
            self.query_cache_exceptions[query_id] = cache_exceptions
            self.query_ttl[query_id] = ttl
            if has_fixed_point:
                self.query_fixed_points[query_id] = fixed_point
                self.query_fixed_point_limits[query_id] = max_iterations
            return fn

    def _replace_locked(
        self,
        slot: tuple[str, str],
        old: Callable[..., Any] | None,
        new: Callable[..., Any],
        new_hash: str,
    ) -> tuple[Callable[..., Any], bool]:
        """Work out what registering ``new`` in ``slot`` over ``old`` means.

        Returns the function to register and whether cached results (memos,
        or input values) must be dropped. Four cases:

        * Nothing registered, or the same function object: register it,
          dropping results if its ``version=`` changed.
        * The same source function again, for example with other options
          (``incremental_per_element=False`` turns a rewritten function back into the
          original): register the new variant and keep existing handles.
          Results are dropped only if the bytecode differs.
        * Equal bytecode and the same captured objects, as when a module is
          reloaded unchanged: the same query. The old function stays, so
          results and handles remain valid.
        * Anything else is a different function taking over the name: a
          reload with changed code, or a second function made from one body
          by a factory or a loop. Results are dropped and the old handles go
          stale, so nothing can run the old function into the new one's
          cache.

        In the last case, if the two functions capture different objects,
        the slot's salt is bumped. Bytecode cannot tell such functions apart,
        so without the salt the new one would load, from disk, results the
        old one wrote earlier in this process (see ``_stored_hash_locked``).
        """
        old_hash = self._code_hashes.get(slot)
        self._code_hashes[slot] = new_hash
        if old is None:
            return new, False
        if old is new:
            # Only ``version=`` can change the hash of the same function.
            return new, old_hash != new_hash
        same_code = old_hash == new_hash
        if _origin(old) is _origin(new):
            return new, not same_code
        same_captures = _same_captures(old, new)
        if same_code and same_captures:
            return old, False
        handles = self._handles.pop(slot, None)
        if handles is not None:
            for handle in list(handles):
                handle._mark_stale()  # noqa: SLF001
        if not same_captures:
            self._hash_salts[slot] = self._hash_salts.get(slot, 0) + 1
        return new, True

    def _stored_hash_locked(self, slot: tuple[str, str]) -> str:
        """Hash that persisted results of ``slot`` are stored and checked under.

        The bytecode hash, plus the slot's salt once a function with other
        captures has taken over the name in this process. Salted results are
        not reused by a later run; ``name=`` gives such functions their own
        identity, and with it cross-run reuse.
        """
        code_hash = self._code_hashes[slot]
        salt = self._hash_salts.get(slot, 0)
        stored = code_hash if not salt else f"{code_hash}#{salt}"
        if self._cache_version is not None:
            stored = f"{stored}@{self._cache_version!r}"
        return stored

    def track_handle(
        self, kind: str, fid: str, fn: Callable[..., Any], handle: Any
    ) -> None:
        """Remember ``handle`` so it can be marked stale when ``fn`` is replaced."""
        with self.lock:
            registered = (self.queries if kind == "query" else self.input_fns).get(fid)
            if registered is None or _origin(registered) is not _origin(fn):
                # Replaced by another thread between registration and here.
                handle._mark_stale()  # noqa: SLF001
                return
            handles = self._handles.get((kind, fid))
            if handles is None:
                handles = self._handles[(kind, fid)] = weakref.WeakSet()
            handles.add(handle)

    # The per-query config lookups below run several times per query on the
    # hot path. They read a single key from a dict that only register_query
    # writes (under the lock); a lone dict read is atomic both with the GIL and
    # on free-threaded builds, so they skip the RLock round trip.

    def lookup_query_ttl(self, query_id: str) -> float | None:
        return self.query_ttl.get(query_id)

    def lookup_query_memoize(self, query_id: str) -> bool:
        return self.query_memoize.get(query_id, True)

    def lookup_query(self, query_id: str) -> Callable[..., Any]:
        return self.queries[query_id]

    def lookup_query_cache_exceptions(
        self, query_id: str
    ) -> bool | tuple[type[BaseException], ...]:
        return self.query_cache_exceptions.get(query_id, False)

    def lookup_query_fixed_point(self, query_id: str) -> tuple[bool, Any]:
        """Returns (has_fixed_point, fixed_point_value)."""
        with self.lock:
            if query_id in self.query_fixed_points:
                return True, self.query_fixed_points[query_id]
            return False, None

    def lookup_fixed_point_limit(self, query_id: str) -> int:
        return self.query_fixed_point_limits.get(
            query_id, DEFAULT_FIXED_POINT_ITERATIONS
        )

    def register_input(
        self, input_id: str, fn: Callable[..., Any]
    ) -> Callable[..., Any]:
        """Register ``fn`` as the default of ``input_id``; see ``register_query``.

        A different function taking over the name drops the input's values,
        as a change of the default's code always has.
        """
        fn_hash = hash_bytecode(fn)
        slot = ("input", input_id)
        with self.lock:
            fn, drop = self._replace_locked(
                slot, self.input_fns.get(input_id), fn, fn_hash
            )
            if drop:
                drop_keys = [k for k in self.inputs if k[0] == input_id]
                for k in drop_keys:
                    self.inputs.pop(k, None)
                    self._multi_version_keys.discard(k)
                # Everything computed from the old default must be looked at
                # again. Direct readers are dropped outright: one that read
                # the default without storing it (a snapshot read) recorded
                # "no stored version", which is also what it would find now.
                read_keys = [
                    k for k in self.dependents if k[0] == "input" and k[1] == input_id
                ]
                for read_key in read_keys:
                    for reader in list(self.dependents.get(read_key, ())):
                        self.drop_memo_locked(reader)
                self.reverify_all_locked()
            self.input_fns[input_id] = fn
            self.input_hashes[input_id] = self._stored_hash_locked(slot)
            return fn

    def lookup_input(self, input_id: str) -> Callable[..., Any] | None:
        return self.input_fns.get(input_id)

    def snapshot(self) -> Snapshot:
        """An owned snapshot of the current revision (``Engine.snapshot()``).

        Its revision stays pinned until ``release()``, the end of a ``with``
        block, or garbage collection.
        """
        with self.lock:
            rev = self._pin_locked(None)
        snap = Snapshot(rev)
        finalizer = weakref.finalize(snap, self._defer_unpin, rev)
        finalizer.atexit = False
        snap._finalizer = finalizer  # noqa: SLF001
        return snap

    def pin_snapshot(self, snapshot: Snapshot | None) -> Snapshot:
        """Pin ``snapshot`` (or the current revision) for a call about to start.

        Raises ``SnapshotExpiredError`` if the snapshot's history was trimmed.
        The caller must ``unpin`` the returned revision when the call ends.
        """
        # Runs once per top-level call, so the common path is inlined.
        with self.lock:
            if self._deferred_unpins:
                self._drain_unpins_locked()
            if snapshot is None:
                rev = self.revision
            else:
                rev = snapshot.revision
                if rev < self.history_floor:
                    self._raise_expired(rev)
            pins = self._pins
            pins[rev] = pins.get(rev, 0) + 1
        if snapshot is None:
            return Snapshot(rev)
        return snapshot

    def pin_current_locked(self) -> int:
        """Pin the current revision; the caller holds ``lock``."""
        return self._pin_locked(None)

    def _pin_locked(self, snapshot: Snapshot | None) -> int:
        if self._deferred_unpins:
            self._drain_unpins_locked()
        if snapshot is None:
            rev = self.revision
        else:  # pragma: no cover - no caller passes one; pin_snapshot inlines this
            rev = snapshot.revision
            if rev < self.history_floor:
                self._raise_expired(rev)
        pins = self._pins
        pins[rev] = pins.get(rev, 0) + 1
        return rev

    def _raise_expired(self, rev: int) -> None:
        raise SnapshotExpiredError(
            f"input history for revision {rev} was trimmed (history now "
            f"starts at revision {self.history_floor}); keep the snapshot "
            "from Engine.snapshot() unreleased while it is in use"
        )

    def unpin(self, rev: int) -> None:
        """Release one pin on ``rev``. Safe to call from finalizers."""
        if self.lock._is_owned():  # type: ignore[attr-defined]
            self._deferred_unpins.append(rev)
            return
        with self.lock:
            self._unpin_locked(rev)
            if self._deferred_unpins:
                self._drain_unpins_locked()

    def unpin_all(self, revs: list[int]) -> None:
        """Release one pin on each revision in ``revs``."""
        lock = self.lock
        if lock._is_owned():  # type: ignore[attr-defined]
            self._deferred_unpins.extend(revs)
            return
        # Runs once per top-level call, so the common path is inlined.
        with lock:
            pins = self._pins
            for rev in revs:
                count = pins.get(rev, 0)
                if count > 1:
                    pins[rev] = count - 1
                elif count:  # pragma: no branch - unbalanced unpin
                    del pins[rev]
                    if self._multi_version_keys:
                        self._after_last_unpin_locked(rev)
            if self._deferred_unpins:
                self._drain_unpins_locked()

    def _defer_unpin(self, rev: int) -> None:
        # Finalizer callback: may run on any thread at any bytecode boundary,
        # including inside one of this store's critical sections, so it must
        # never take the lock.
        self._deferred_unpins.append(rev)

    def _drain_unpins_locked(self) -> None:
        deferred = self._deferred_unpins
        while deferred:
            self._unpin_locked(deferred.popleft())

    def _unpin_locked(self, rev: int) -> None:
        pins = self._pins
        count = pins.get(rev, 0)
        if count > 1:
            pins[rev] = count - 1
            return
        if count == 0:  # pragma: no cover - unbalanced unpin
            return
        del pins[rev]
        if self._multi_version_keys:
            self._after_last_unpin_locked(rev)

    def _after_last_unpin_locked(self, rev: int) -> None:
        """``rev`` lost its last pin: sweep if that advanced the floor."""
        pins = self._pins
        if pins and rev > min(pins):
            return
        if self._trim_debt >= max(  # pragma: no branch - at least one per such key
            1, len(self._multi_version_keys) // 2
        ):
            self._sweep_locked()

    def history_floor_locked(self) -> int:
        """Oldest revision any reader may still use."""
        pins = self._pins
        return min(pins) if pins else self.revision

    def _sweep_locked(self) -> None:
        floor = self.history_floor_locked()
        if floor > self.history_floor:  # pragma: no branch - only run after it rose
            self.history_floor = floor
        inputs = self.inputs
        remaining = 0
        for key in list(self._multi_version_keys):
            versions = inputs.get(key)
            if versions is None:  # pragma: no cover - dropped inputs leave the set too
                self._multi_version_keys.discard(key)
                continue
            cut = bisect.bisect_right(versions, floor, key=_version_revision) - 1
            if cut > 0:
                versions = versions[cut:]
                inputs[key] = versions
            if len(versions) <= 1:
                self._multi_version_keys.discard(key)
            else:
                remaining += len(versions) - 1
        # Versions an older pin still protected stay owed, so the next floor
        # advance sweeps them even if nothing is set in between.
        self._trim_debt = remaining

    def traces(self) -> list[TraceEvent]:
        with self.lock:
            return self._materialize_traces_locked()

    def _materialize_traces_locked(self) -> list[TraceEvent]:
        return [
            TraceEvent(
                event=event,
                key=self.key_to_str(key),
                revision=revision,
                detail=detail,
                timestamp=timestamp,
            )
            for event, key, revision, detail, timestamp in self.trace
        ]

    def clear_traces(self) -> None:
        with self.lock:
            self.trace.clear()

    def trace_event(self, event: str, key: QueryKey, detail: str = "") -> None:
        if self.trace_limit == 0:
            return
        # Must synchronize with readers/writers of `trace` and with `revision`
        # (recorded on each event). Evaluator paths invoke this from many threads.
        with self.lock:
            self.trace.append((event, key, self.revision, detail, time.time()))

    def key_to_str(self, key: QueryKey) -> str:
        kind, fid, args = key
        return f"{kind}:{fid}{display_args(args)}"

    def _value_changed(self, value: Any, value_hash: str) -> bool:
        """True when ``value`` no longer has the digest it was stored with."""
        if value_hash.startswith("opaque:"):
            return False  # never compared by content
        try:
            return self._value_digest(value) != value_hash
        except TypeError:
            return True  # it was encodable when stored: something was put in

    def mutated_values(
        self, deps: Mapping[QueryKey, int], revision: int
    ) -> list[QueryKey]:
        """The dependencies whose value was changed in place since it was stored.

        Called when a query's body has finished, for what that body read: a
        query result as cached, an input value as of ``revision``. A query
        result changed in place is dropped, so the next read computes it
        again. Costs one digest per dependency per run.
        """
        found: list[QueryKey] = []
        for dep_key in deps:
            kind, fid, args = dep_key
            if kind == "query":
                with self.lock:
                    memo = self.memos.get(dep_key)
                if memo is None or memo.error is not None:
                    continue
                if not self.query_memoize.get(fid, True):
                    continue
                if self._value_changed(memo.value, memo.value_hash):
                    found.append(dep_key)
                    with self.lock:
                        if self.memos.get(dep_key) is memo:  # pragma: no branch
                            self.drop_memo_locked(dep_key)
            else:
                with self.lock:
                    try:
                        version = self.input_version_at((fid, args), revision)
                    except SnapshotExpiredError:  # pragma: no cover
                        # The running call pins this revision's history.
                        continue
                if version is not None and self._value_changed(
                    version.value, version.value_hash
                ):
                    found.append(dep_key)
        return found

    def _replace_changed_value_locked(
        self, versions: list[InputVersion], value: Any
    ) -> None:
        """A same-value ``set``: put ``value`` back if the stored object was changed.

        The digest of the new value equals the one stored, so nothing that
        was computed from the input is affected. But if the stored object
        was changed in place since, it no longer holds that value: swap in
        the object just given, so that the ``set`` repairs it.
        """
        latest = versions[-1]
        if latest.value is not value and self._value_changed(
            latest.value, latest.value_hash
        ):
            versions[-1] = dataclasses.replace(latest, value=value)

    def check_mutations(self) -> list[QueryKey]:
        """Every cached result and current input value changed in place."""
        with self.lock:
            memos = list(self.memos.items())
            inputs = [(k, v[-1]) for k, v in self.inputs.items() if v]
        found: list[QueryKey] = []
        for key, memo in memos:
            if memo.error is not None or not self.query_memoize.get(key[1], True):
                continue
            if self._value_changed(memo.value, memo.value_hash):
                found.append(key)
                with self.lock:
                    if self.memos.get(key) is memo:  # pragma: no branch
                        self.drop_memo_locked(key)
        for (fid, args), version in inputs:
            if self._value_changed(version.value, version.value_hash):
                found.append(("input", fid, args))
        return found

    def mutation_error(
        self, keys: list[QueryKey], reader: QueryKey | None = None
    ) -> MutatedValueError:
        names = ", ".join(self.key_to_str(k) for k in keys)
        where = (
            f" {self.key_to_str(reader)} read it, and its body has finished."
            if reader is not None
            else ""
        )
        parts = [
            f"the value of {names} was changed in place after Cascade stored "
            f"it.{where} Queries and inputs hand out the stored object itself, "
            "not a copy, so changing it changes what every later reader gets."
        ]
        if any(k[0] == "query" for k in keys):
            parts.append(
                "Build a new value instead of changing the returned one (for "
                "example [*rows, x] instead of rows.append(x)); the changed "
                "result has been dropped and is computed again on its next use."
            )
        if any(k[0] == "input" for k in keys):
            parts.append(
                "Give an input a new value with set() instead of changing the "
                "one it returned; until then every query that reads it raises "
                "this error."
            )
        return MutatedValueError(" ".join(parts), tuple(keys))

    def stable_hash(self, value: Any) -> str:
        """Digest of ``value``; ``TypeError`` if the codec cannot encode it."""
        return self._value_digest(value)

    def digest_or_opaque(self, value: Any) -> str:
        """``stable_hash``, or on an engine without ``cache_dir`` an opaque digest.

        An opaque digest is new every time, so a value that cannot be
        encoded never counts as equal to another one, not even to itself:
        a query that returns one always counts as changed, so its dependents
        run again (no early bail-out). That is always safe; only the saving
        is lost. With ``cache_dir`` the value must be encodable and
        ``TypeError`` propagates. Used for query results and cached errors,
        not for input values (``input_digest``) or fixed-point guesses.
        """
        try:
            return self._value_digest(value)
        except TypeError:
            if not self._allow_opaque:
                raise
        with self._opaque_lock:
            self._opaque_count += 1
            return f"opaque:{self._opaque_count}"

    def unencodable_error(
        self, subject: str, root: str, value: Any, exc: Exception
    ) -> TypeError:
        """``UnencodableValueError`` for ``value``, which ``stable_hash`` refused."""
        return unencodable_error(subject, root, value, self._value_digest, exc)

    def input_digest(self, key: InputKey, value: Any) -> str:
        """``stable_hash`` of a value for input ``key``, with a clear error.

        Input values must be encodable on every engine: a same-value ``set``
        is a no-op only when the engine can tell that the value is the same.
        """
        try:
            return self._value_digest(value)
        except TypeError as exc:
            input_id, args = key
            raise self.unencodable_error(
                f"input {input_id}{args!r} was given a value Cascade cannot encode",
                "value",
                value,
                exc,
            ) from exc

    def new_change_stamp_locked(self) -> int:
        """A ``changed_at`` value no memo has carried before.

        Dependents decide whether a query changed by comparing the stamp they
        recorded with the one its memo carries now. The revision cannot serve
        as that stamp: a TTL query that expires, or a query registered again
        with new code, gets a new value while the revision stands still, and
        a dependent that saw the old value at this revision would find the
        same number and keep its result.
        """
        self.change_stamp += 1
        return self.change_stamp

    def touch_memo_locked(self, key: QueryKey) -> None:
        memo = self.memos[key]
        self.next_access_id += 1
        memo.last_access = self.next_access_id
        self._lru[key] = None
        self._lru.move_to_end(key)

    def pin_memo_verification_locked(self, key: QueryKey) -> None:
        self._memo_verify_pin_count[key] = self._memo_verify_pin_count.get(key, 0) + 1

    def unpin_memo_verification_locked(self, key: QueryKey) -> None:
        n = self._memo_verify_pin_count.get(key, 0)
        if n <= 1:
            self._memo_verify_pin_count.pop(key, None)
        else:
            self._memo_verify_pin_count[key] = n - 1

    def push_memo_lru_locked(self, key: QueryKey) -> None:
        if key not in self.memos:
            return
        self._lru[key] = None
        self._lru.move_to_end(key)

    # --- dirty propagation ---------------------------------------------------
    #
    # Without help, verifying a memo after any revision bump means re-walking
    # its whole dependency graph. ``MemoEntry.head_clean`` avoids that: a memo
    # with the flag set is valid at the newest revision as-is.
    #
    # Invariants (all mutations happen under ``self.lock``):
    #   1. A clean memo's query dependencies all have clean memos, so the
    #      ``dependents`` edges from every input it transitively reads are
    #      intact.
    #   2. Hence a dirty memo's dependents are all dirty, and the upward walks
    #      below may stop at the first memo that is already dirty.
    #
    # A memo turns dirty when an input it (transitively) read changes value, or
    # when any memo beneath it is dropped or replaced. It only turns clean
    # again through ``refresh_head_clean_locked`` after being verified or
    # recomputed at the newest revision.

    def mark_dependents_dirty_locked(self, key: QueryKey) -> None:
        """Clear ``head_clean`` on every memo that transitively depends on ``key``."""
        dependents = self.dependents
        memos = self.memos
        stack = [key]
        while stack:
            parents = dependents.get(stack.pop())
            if not parents:
                continue
            for parent in parents:
                memo = memos.get(parent)
                if memo is not None and memo.head_clean:
                    memo.head_clean = False
                    stack.append(parent)

    def reverify_all_locked(self) -> None:
        """Make every memo verify its dependencies again before it is used.

        For a change that no revision records: new code registered under a
        name. A memo verified at a revision is otherwise used at that
        revision without another look (``is_current_locked``), so after such
        a change it would keep serving what the old code produced.

        All memos, not only those ``dependents`` reaches from the replaced
        function: an evicted memo takes its edges with it, so a result two
        levels above the function can be cut off from it. An input change
        gets past such gaps by moving the revision; this does the same for
        every revision, snapshots included, at the cost of one dependency
        walk per memo after a re-registration.
        """
        for memo in self.memos.values():
            memo.verified_at = -1
            memo.head_clean = False

    def is_current_locked(self, memo: MemoEntry, revision: int) -> bool:
        """True when ``memo`` needs no dependency walk to be used at ``revision``."""
        return memo.verified_at == revision or (
            memo.head_clean and revision == self.revision
        )

    def refresh_head_clean_locked(
        self, memo: MemoEntry, revision: int, query_id: str
    ) -> None:
        """Mark a memo just verified/stored at ``revision`` clean if it qualifies.

        Memos with a TTL, memos in a fixed-point cycle, and anything depending
        on a memo that is not itself clean stay dirty and keep being verified
        the slow way.

        This only ever sets the flag. Clearing it here would break invariant 2
        (the memo's dependents would stay clean): a memo that is clean at the
        newest revision stays so when it is also verified at an older pinned
        snapshot, because that walk changes neither its value nor its edges.
        """
        if (
            revision != self.revision
            or memo.cycle_nodes
            or self.query_ttl.get(query_id) is not None
        ):
            return
        memos = self.memos
        for dep in memo.deps:
            if dep.key[0] != "query":
                continue
            dep_memo = memos.get(dep.key)
            if dep_memo is None or not dep_memo.head_clean:
                return
        memo.head_clean = True

    def drop_memo_locked(self, key: QueryKey, *, replacing: bool = False) -> None:
        """Drop the result of ``key``; ``replacing`` when a new one follows."""
        self._memo_verify_pin_count.pop(key, None)
        self._lru.pop(key, None)
        memo = self.memos.pop(key, None)
        if memo is None:
            return
        # Dependents can no longer rely on the edges this memo carried.
        self.mark_dependents_dirty_locked(key)
        for dep in memo.deps:
            dependents = self.dependents.get(dep.key)
            if dependents is None:
                continue
            dependents.discard(key)
            if not dependents:
                self.dependents.pop(dep.key, None)
        cycle = memo.cycle_nodes
        if cycle and cycle[0] == key and not replacing:
            # A fixed-point head: its members were computed from its value.
            # Kept without it, the head's next run would read them from the
            # cache, never reach itself and so never iterate, mixing their
            # old values with whatever it reads anew (a TTL query run again).
            for member in cycle[1:]:
                self.drop_memo_locked(member)

    def _rebuild_lru_locked(self) -> None:
        ordered = sorted(self.memos.items(), key=lambda item: item[1].last_access)
        self._lru = OrderedDict((k, None) for k, _ in ordered)

    def _pop_lru_victim_key_locked(self) -> QueryKey | None:
        stale: list[QueryKey] = []
        victim: QueryKey | None = None
        # Pinned rows are skipped, not removed; there are at most a handful
        # (one per frame of an in-progress verification).
        for key in self._lru:
            if key not in self.memos:
                stale.append(key)
                continue
            if self._memo_verify_pin_count.get(key, 0) > 0:
                continue
            victim = key
            break
        for key in stale:
            del self._lru[key]
        return victim

    def _unpinned_lru_fallback_key_locked(self) -> QueryKey | None:
        best_k: QueryKey | None = None
        best_acc: int | None = None
        for k, memo in self.memos.items():
            if self._memo_verify_pin_count.get(k, 0) > 0:
                continue
            if best_acc is None or memo.last_access < best_acc:
                best_acc = memo.last_access
                best_k = k
        return best_k

    def evict_if_needed_locked(self) -> None:
        while len(self.memos) > self.max_entries:
            victim = self._pop_lru_victim_key_locked()
            if victim is None:
                self._rebuild_lru_locked()
                victim = self._pop_lru_victim_key_locked()
            if victim is None:
                victim = self._unpinned_lru_fallback_key_locked()
            if victim is None:
                break
            self.trace_event("evict", victim, "lru")
            if self._stats_enabled:
                self._stats_evictions_total += 1
                if self._stats_eviction_recent_cap > 0:
                    self._stats_evictions_recent.append(self.key_to_str(victim))
            self.drop_memo_locked(victim)

    def sweep_unaccessed(self, since_access_id: int) -> None:
        with self.lock:
            remove = [
                k
                for k, memo in self.memos.items()
                if memo.last_access <= since_access_id
            ]
            for key in remove:
                self.drop_memo_locked(key)
            self._rebuild_lru_locked()

    def latest_input_version(self, input_key: InputKey) -> InputVersion | None:
        versions = self.inputs.get(input_key)
        if not versions:
            return None
        return versions[-1]

    def first_version_after(
        self, input_key: InputKey, revision: int
    ) -> InputVersion | None:
        """The oldest stored version, if it is newer than ``revision``.

        Only meaningful when ``input_version_at(input_key, revision)`` found
        nothing: then no version was trimmed from below, and this is the
        first value the input was given after ``revision``.
        """
        versions = self.inputs.get(input_key)
        if versions and versions[0].revision > revision:
            return versions[0]
        return None

    def input_version_at(
        self, input_key: InputKey, revision: int
    ) -> InputVersion | None:
        versions = self.inputs.get(input_key)
        if not versions:
            return None
        latest = versions[-1]
        if latest.revision <= revision:
            return latest
        idx = bisect.bisect_right(versions, revision, key=_version_revision) - 1
        if idx < 0:
            # Trimming only removes a prefix, so the answers above stay exact.
            # Below the floor, "no version" may mean "trimmed".
            if revision < self.history_floor:
                raise SnapshotExpiredError(
                    f"input history for revision {revision} was trimmed "
                    f"(history now starts at revision {self.history_floor})"
                )
            return None
        return versions[idx]

    def set_input(
        self,
        input_id: str,
        args: tuple[Any, ...],
        value: Any,
        *,
        bump_cancel_epoch: bool = True,
    ) -> int:
        """Record a new value for an input and return the resulting revision.

        Setting the value an input already holds is a no-op: no version is
        stored, the revision and cancel epoch stay put, and nothing is marked
        dirty.
        """
        key = (input_id, args)
        value_hash = self.input_digest(key, value)
        with self.lock:
            versions = self.inputs.get(key)
            if versions and versions[-1].value_hash == value_hash:
                self._replace_changed_value_locked(versions, value)
                return self.revision
            self.revision += 1
            if bump_cancel_epoch:
                self.cancel_epoch += 1
            self._record_change_locked(key, value, value_hash)
            return self.revision

    def store_default(
        self, key: InputKey, value: Any, revision: int
    ) -> InputVersion | None:
        """Keep the default an input was first read with, at ``revision``.

        Only when nothing is stored for ``key`` and ``revision`` is still the
        newest. The revision does not move and nothing is invalidated: a
        read is not a change. The version's ``changed_at`` is a marker of
        its own (below -2, so no revision and no placeholder), which readers
        of the default from before it was kept do not match: they verify
        against it and run again, in case the default function now returns
        something else. Returns the version, or None if nothing was kept.
        """
        value_hash = self.input_digest(key, value)
        with self.lock:
            if revision != self.revision or self.inputs.get(key):
                return None
            version = InputVersion(
                revision=revision,
                changed_at=-2 - self.new_change_stamp_locked(),
                value_hash=value_hash,
                value=value,
            )
            self._append_version_locked(key, version)
            self.trace_event(
                "input_default",
                ("input", key[0], key[1]),
                detail=f"revision={revision}",
            )
            return version

    def set_inputs(
        self,
        updates: list[tuple[str, tuple[Any, ...], Any]],
        *,
        bump_cancel_epoch: bool = True,
    ) -> int:
        """Apply several input updates as one revision.

        Later updates to the same input win. Inputs whose final value equals
        their current one are skipped, and when nothing changes the revision
        and cancel epoch stay put.
        """
        return self.apply_prepared_inputs(
            self.prepare_inputs(updates), bump_cancel_epoch=bump_cancel_epoch
        )

    def prepare_inputs(
        self, updates: list[tuple[str, tuple[Any, ...], Any]]
    ) -> list[tuple[InputKey, Any, str]]:
        """Resolve last-write-wins and hash every value; changes nothing.

        Everything in ``set_inputs`` that can fail on a bad value (an
        unhashable argument, a value that cannot be encoded) happens here, so
        a transaction can check its updates before it applies anything.
        """
        final: dict[InputKey, Any] = {}
        for input_id, args, value in updates:
            final[(input_id, args)] = value
        return [
            (key, value, self.input_digest(key, value)) for key, value in final.items()
        ]

    def apply_prepared_inputs(
        self,
        hashed: list[tuple[InputKey, Any, str]],
        *,
        bump_cancel_epoch: bool = True,
    ) -> int:
        """Apply updates from ``prepare_inputs`` as one revision."""
        with self.lock:
            changed = []
            for key, value, value_hash in hashed:
                versions = self.inputs.get(key)
                if not versions or versions[-1].value_hash != value_hash:
                    changed.append((key, value, value_hash))
                else:
                    self._replace_changed_value_locked(versions, value)
            if not changed:
                return self.revision
            self.revision += 1
            if bump_cancel_epoch:
                self.cancel_epoch += 1
            for key, value, value_hash in changed:
                self._record_change_locked(key, value, value_hash)
            return self.revision

    def _record_change_locked(self, key: InputKey, value: Any, value_hash: str) -> None:
        """Store a changed value at the current revision and invalidate readers."""
        self._append_version_locked(
            key,
            InputVersion(
                revision=self.revision,
                changed_at=self.revision,
                value_hash=value_hash,
                value=value,
            ),
        )
        trace_key = ("input", key[0], key[1])
        self.mark_dependents_dirty_locked(trace_key)
        self.trace_event("input_set", trace_key, detail=f"changed_at={self.revision}")

    def _append_version_locked(self, key: InputKey, version: InputVersion) -> None:
        """Add ``version`` and drop history no pinned revision can reach.

        Readers bisect without the lock, so trimmed lists are published as new
        lists (copy-on-write), after ``history_floor`` is raised.
        """
        if self._deferred_unpins:
            self._drain_unpins_locked()
        versions = self.inputs.get(key)
        floor = self.history_floor_locked()
        if not versions or floor >= version.revision:
            if versions and version.revision > self.history_floor:
                self.history_floor = version.revision
            self.inputs[key] = [version]
            self._multi_version_keys.discard(key)
            return
        cut = bisect.bisect_right(versions, floor, key=_version_revision) - 1
        if cut > 0:  # pragma: no cover - a sweep ran when the floor last rose
            if floor > self.history_floor:
                self.history_floor = floor
            kept = versions[cut:]
            kept.append(version)
            self.inputs[key] = kept
        else:
            versions.append(version)
            self._trim_debt += 1
        self._multi_version_keys.add(key)

    def inspect_graph(self, *, condense: bool = False) -> dict[str, Any]:
        from ._graph_export import condense_graph

        with self.lock:
            nodes = [self.key_to_str(k) for k in self.memos.keys()]
            edges = []
            for parent, memo in self.memos.items():
                parent_s = self.key_to_str(parent)
                for dep in memo.deps:
                    edges.append((parent_s, self.key_to_str(dep.key)))
            graph = {
                "revision": self.revision,
                "memo_count": len(self.memos),
                "input_count": len(self.inputs),
                "nodes": nodes,
                "edges": edges,
            }
            if condense:
                condensed = condense_graph(graph)
                graph["nodes"] = condensed["nodes"]
                graph["edges"] = condensed["edges"]
            return graph

    def subgraph(
        self, roots: Sequence[QueryKey | str], *, direction: str, condense: bool = False
    ) -> dict[str, Any]:
        if direction not in ("deps", "dependents"):
            raise ValueError("direction must be 'deps' or 'dependents'")
        with self.lock:
            key_str = {self.key_to_str(k): k for k in self.memos}
            nodes_full = list(key_str.keys())
            edges_full: list[tuple[str, str]] = []
            for parent, memo in self.memos.items():
                parent_s = self.key_to_str(parent)
                for dep in memo.deps:
                    edges_full.append((parent_s, self.key_to_str(dep.key)))

            seeds: list[str] = []
            for r in roots:
                if isinstance(r, str):
                    if r in key_str:
                        seeds.append(r)
                else:
                    s = self.key_to_str(r)
                    if r in self.memos:
                        seeds.append(s)

            if not seeds:
                return {
                    "revision": self.revision,
                    "memo_count": 0,
                    "input_count": len(self.inputs),
                    "nodes": [],
                    "edges": [],
                }

            if direction == "deps":
                adj: dict[str, list[str]] = defaultdict(list)
                for parent_s, dep_s in edges_full:
                    adj[parent_s].append(dep_s)
                reachable: set[str] = set()
                queue: deque[str] = deque(seeds)
                while queue:
                    u = queue.popleft()
                    if u in reachable:
                        continue
                    reachable.add(u)
                    for v in adj.get(u, ()):
                        if v not in reachable:
                            queue.append(v)
            else:
                radj: dict[str, list[str]] = defaultdict(list)
                for parent_s, dep_s in edges_full:
                    radj[dep_s].append(parent_s)
                reachable = set()
                queue = deque(seeds)
                while queue:
                    u = queue.popleft()
                    if u in reachable:
                        continue
                    reachable.add(u)
                    for v in radj.get(u, ()):
                        if v not in reachable:
                            queue.append(v)

            nodes_out = [n for n in nodes_full if n in reachable]
            edges_out = [
                (p, d) for p, d in edges_full if p in reachable and d in reachable
            ]
            graph = {
                "revision": self.revision,
                "memo_count": len(nodes_out),
                "input_count": len(self.inputs),
                "nodes": nodes_out,
                "edges": edges_out,
            }
            if condense:
                from ._graph_export import condense_graph

                condensed = condense_graph(graph)
                graph["nodes"] = condensed["nodes"]
                graph["edges"] = condensed["edges"]
            return graph

    def prune(self, roots: list[QueryKey]) -> None:
        with self.lock:
            wanted: set[QueryKey] = set(roots)
            queue = deque(roots)
            while queue:
                node = queue.popleft()
                memo = self.memos.get(node)
                if memo is None:
                    continue
                for dep in memo.deps:
                    if dep.key[0] == "query" and dep.key not in wanted:
                        wanted.add(dep.key)
                        queue.append(dep.key)
            remove = [k for k in self.memos.keys() if k not in wanted]
            for key in remove:
                self.drop_memo_locked(key)

    @staticmethod
    def entry_from_runtime(
        *,
        value: Any,
        value_hash: str,
        changed_at: int,
        verified_at: int,
        deps: dict[QueryKey, int],
        effects: dict[str, tuple[Any, ...]],
        last_access: int,
        computed_at_time: float,
        cycle_nodes: tuple[QueryKey, ...] = (),
        error: BaseException | None = None,
        expires_at: float | None = None,
    ) -> MemoEntry:
        return MemoEntry(
            value=value,
            value_hash=value_hash,
            changed_at=changed_at,
            verified_at=verified_at,
            deps=tuple(
                Dependency(dep_key, observed) for dep_key, observed in deps.items()
            ),
            effects=effects,
            last_access=last_access,
            computed_at_time=computed_at_time,
            cycle_nodes=cycle_nodes,
            error=error,
            expires_at=expires_at,
        )
