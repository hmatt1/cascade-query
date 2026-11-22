from __future__ import annotations

import bisect
import threading
import time
from collections import OrderedDict, defaultdict, deque
from collections.abc import Sequence
from typing import Any, Callable

from ._serde import hash_bytecode, stable_value_digest
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

_TraceRow = tuple[str, "QueryKey | str", int, str, float]


def _version_revision(version: InputVersion) -> int:
    return version.revision


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
    ) -> None:
        self.lock = threading.RLock()
        self._value_digest: Callable[[Any], str] = value_digest or stable_value_digest
        self.revision = 0
        self.cancel_epoch = 0
        self.next_access_id = 0
        self.max_entries = max_entries
        self.trace_limit = trace_limit
        # Raw rows (event, key, revision, detail, timestamp). ``key`` is the
        # QueryKey tuple, formatted to a string only when traces are read, or an
        # already-formatted string for rows restored by ``assign_loaded_state``.
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
        self._stats_persistence_loads = 0
        self._stats_persistence_saves = 0

        self.inputs: dict[InputKey, list[InputVersion]] = {}
        self.input_hashes: dict[str, str] = {}
        self.queries: dict[str, Callable[..., Any]] = {}
        self.query_hashes: dict[str, str] = {}
        self.query_memoize: dict[str, bool] = {}
        self.query_fixed_points: dict[str, Any] = {}
        self.query_cache_exceptions: dict[
            str, bool | tuple[type[BaseException], ...]
        ] = {}
        self.query_ttl: dict[str, float | None] = {}
        self.input_fns: dict[str, Callable[..., Any]] = {}
        self.memos: dict[QueryKey, MemoEntry] = {}
        self.dependents: dict[QueryKey, set[QueryKey]] = defaultdict(set)
        # Keyed by (query key, snapshot revision) to keep dedup snapshot-safe.
        self.in_flight: dict[tuple[QueryKey, int], InFlight] = {}
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
            self._stats_persistence_loads = 0
            self._stats_persistence_saves = 0

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
                "persistence_loads": self._stats_persistence_loads,
                "persistence_saves": self._stats_persistence_saves,
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

    def record_persistence_load(self) -> None:
        if not self._stats_enabled:
            return
        with self.lock:
            if self._stats_enabled:
                self._stats_persistence_loads += 1

    def record_persistence_save(self) -> None:
        if not self._stats_enabled:
            return
        with self.lock:
            if self._stats_enabled:
                self._stats_persistence_saves += 1

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
    ) -> None:
        fn_hash = hash_bytecode(fn)
        with self.lock:
            if query_id in self.query_hashes and self.query_hashes[query_id] != fn_hash:
                drop_keys = [
                    k for k in self.memos if k[0] == "query" and k[1] == query_id
                ]
                for k in drop_keys:
                    self.drop_memo_locked(k)
            self.queries[query_id] = fn
            self.query_hashes[query_id] = fn_hash
            self.query_memoize[query_id] = memoize
            self.query_cache_exceptions[query_id] = cache_exceptions
            self.query_ttl[query_id] = ttl
            if has_fixed_point:
                self.query_fixed_points[query_id] = fixed_point

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

    def register_input(self, input_id: str, fn: Callable[..., Any]) -> None:
        fn_hash = hash_bytecode(fn)
        with self.lock:
            if input_id in self.input_hashes and self.input_hashes[input_id] != fn_hash:
                drop_keys = [k for k in self.inputs if k[0] == input_id]
                for k in drop_keys:
                    self.inputs.pop(k, None)
                    self.mark_dependents_dirty_locked(("input", k[0], k[1]))
            self.input_fns[input_id] = fn
            self.input_hashes[input_id] = fn_hash

    def lookup_input(self, input_id: str) -> Callable[..., Any] | None:
        return self.input_fns.get(input_id)

    def snapshot(self) -> Snapshot:
        with self.lock:
            return Snapshot(revision=self.revision)

    def traces(self) -> list[TraceEvent]:
        with self.lock:
            return self._materialize_traces_locked()

    def _materialize_traces_locked(self) -> list[TraceEvent]:
        return [
            TraceEvent(
                event=event,
                key=key if isinstance(key, str) else self.key_to_str(key),
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
        return f"{kind}:{fid}{args}"

    def stable_hash(self, value: Any) -> str:
        return self._value_digest(value)

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

    def drop_memo_locked(self, key: QueryKey) -> None:
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
        with self.lock:
            self.revision += 1
            if bump_cancel_epoch:
                self.cancel_epoch += 1
            key = (input_id, args)
            versions = self.inputs.setdefault(key, [])
            current = versions[-1] if versions else None
            value_hash = self.stable_hash(value)
            if current is not None and current.value_hash == value_hash:
                changed_at = current.changed_at
            else:
                changed_at = self.revision
            versions.append(
                InputVersion(
                    revision=self.revision,
                    changed_at=changed_at,
                    value_hash=value_hash,
                    value=value,
                )
            )
            trace_key = ("input", input_id, args)
            if changed_at == self.revision:
                self.mark_dependents_dirty_locked(trace_key)
            self.trace_event("input_set", trace_key, detail=f"changed_at={changed_at}")
            return self.revision

    def set_inputs(
        self,
        updates: list[tuple[str, tuple[Any, ...], Any]],
        *,
        bump_cancel_epoch: bool = True,
    ) -> int:
        with self.lock:
            if not updates:
                return self.revision
            self.revision += 1
            if bump_cancel_epoch:
                self.cancel_epoch += 1
            for input_id, args, value in updates:
                key = (input_id, args)
                versions = self.inputs.setdefault(key, [])
                current = versions[-1] if versions else None
                value_hash = self.stable_hash(value)
                if current is not None and current.value_hash == value_hash:
                    changed_at = current.changed_at
                else:
                    changed_at = self.revision
                versions.append(
                    InputVersion(
                        revision=self.revision,
                        changed_at=changed_at,
                        value_hash=value_hash,
                        value=value,
                    )
                )
                trace_key = ("input", input_id, args)
                if changed_at == self.revision:
                    self.mark_dependents_dirty_locked(trace_key)
                self.trace_event(
                    "input_set", trace_key, detail=f"changed_at={changed_at}"
                )
            return self.revision

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

    def assign_loaded_state(self, payload: dict[str, Any]) -> None:
        with self.lock:
            self.revision = payload["revision"]
            self.cancel_epoch = payload["cancel_epoch"]
            self.inputs = payload["inputs"]
            self.memos = payload["memos"]
            # Loaded memos must earn head_clean again through verification.
            for memo in self.memos.values():
                memo.head_clean = False
            deps_in = payload["dependents"]
            self.dependents = defaultdict(set)
            for k, v in deps_in.items():
                self.dependents[k] = set(v)
            self.trace = deque(
                (
                    (ev.event, ev.key, ev.revision, ev.detail, ev.timestamp)
                    for ev in payload["trace"]
                ),
                maxlen=self.trace_limit,
            )
            self.next_access_id = payload["access_id"]
            # In-flight dedup futures are process-local/transient and should never
            # survive a load boundary.
            self.in_flight.clear()
            self._memo_verify_pin_count.clear()
            self._rebuild_lru_locked()

    def make_persistence_payload(self) -> dict[str, Any]:
        with self.lock:
            return {
                "revision": self.revision,
                "cancel_epoch": self.cancel_epoch,
                "inputs": self.inputs,
                "memos": self.memos,
                "dependents": self.dependents,
                "trace": self._materialize_traces_locked(),
                "access_id": self.next_access_id,
            }

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
        )
