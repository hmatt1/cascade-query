from __future__ import annotations

import bisect
import concurrent.futures
import heapq
import threading
import time
from collections import defaultdict, deque
from collections.abc import Sequence
from typing import Any, Callable

from ._serde import stable_value_digest
from ._state import Dependency, InputKey, InputVersion, MemoEntry, QueryKey, Snapshot, TraceEvent


class GraphStore:
    def __init__(
        self,
        *,
        max_entries: int,
        trace_limit: int,
        stats: bool = False,
        stats_eviction_recent_cap: int = 32,
        monotonic_seconds: Callable[[], float] | None = None,
    ) -> None:
        self.lock = threading.RLock()
        self.revision = 0
        self.cancel_epoch = 0
        self.next_access_id = 0
        self.max_entries = max_entries
        self.trace_limit = trace_limit
        self.trace: deque[TraceEvent] = deque(maxlen=trace_limit)

        self._monotonic_seconds: Callable[[], float] = monotonic_seconds or time.perf_counter
        self._stats_enabled = stats
        self._stats_eviction_recent_cap = max(0, stats_eviction_recent_cap)
        self._stats_by_key: dict[str, float] = {}
        self._stats_evictions_total = 0
        self._stats_evictions_recent: deque[str] = deque(
            maxlen=self._stats_eviction_recent_cap if self._stats_eviction_recent_cap > 0 else None
        )

        self.inputs: dict[InputKey, list[InputVersion]] = {}
        self.queries: dict[str, Callable[..., Any]] = {}
        self.memos: dict[QueryKey, MemoEntry] = {}
        self.dependents: dict[QueryKey, set[QueryKey]] = defaultdict(set)
        # Keyed by (query key, snapshot revision) to keep dedup snapshot-safe.
        self.in_flight: dict[tuple[QueryKey, int], concurrent.futures.Future[MemoEntry]] = {}
        # Lazy min-heap (last_access, key); stale entries removed on pop (touches push new tuples).
        self._lru_heap: list[tuple[int, QueryKey]] = []
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
            new_max = self._stats_eviction_recent_cap if self._stats_eviction_recent_cap > 0 else None
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

    def stats_summary(self) -> dict[str, Any]:
        with self.lock:
            return {
                "by_key": dict(sorted(self._stats_by_key.items())),
                "evictions_total": self._stats_evictions_total,
                "evictions_recent": list(self._stats_evictions_recent),
                "memo_count": len(self.memos),
                "max_entries": self.max_entries,
            }

    def record_query_body_time(self, key: QueryKey, seconds: float) -> None:
        if seconds < 0:
            seconds = 0.0
        with self.lock:
            if not self._stats_enabled:
                return
            sk = self.key_to_str(key)
            self._stats_by_key[sk] = self._stats_by_key.get(sk, 0.0) + seconds

    def register_query(self, query_id: str, fn: Callable[..., Any]) -> None:
        with self.lock:
            self.queries[query_id] = fn

    def lookup_query(self, query_id: str) -> Callable[..., Any]:
        with self.lock:
            return self.queries[query_id]

    def snapshot(self) -> Snapshot:
        with self.lock:
            return Snapshot(revision=self.revision)

    def traces(self) -> list[TraceEvent]:
        with self.lock:
            return list(self.trace)

    def clear_traces(self) -> None:
        with self.lock:
            self.trace.clear()

    def trace_event(self, event: str, key: QueryKey, detail: str = "") -> None:
        # Must synchronize with readers/writers of `trace` and with `revision`
        # (recorded on each event). Evaluator paths invoke this from many threads.
        with self.lock:
            self.trace.append(
                TraceEvent(
                    event=event,
                    key=self.key_to_str(key),
                    revision=self.revision,
                    detail=detail,
                    timestamp=time.time(),
                )
            )

    def key_to_str(self, key: QueryKey) -> str:
        kind, fid, args = key
        return f"{kind}:{fid}{args}"

    def stable_hash(self, value: Any) -> str:
        return stable_value_digest(value)

    def touch_memo_locked(self, key: QueryKey) -> None:
        memo = self.memos[key]
        self.next_access_id += 1
        memo.last_access = self.next_access_id
        heapq.heappush(self._lru_heap, (memo.last_access, key))

    def pin_memo_verification_locked(self, key: QueryKey) -> None:
        self._memo_verify_pin_count[key] = self._memo_verify_pin_count.get(key, 0) + 1

    def unpin_memo_verification_locked(self, key: QueryKey) -> None:
        n = self._memo_verify_pin_count.get(key, 0)
        if n <= 1:
            self._memo_verify_pin_count.pop(key, None)
        else:
            self._memo_verify_pin_count[key] = n - 1

    def push_memo_lru_locked(self, key: QueryKey) -> None:
        memo = self.memos.get(key)
        if memo is None:
            return
        heapq.heappush(self._lru_heap, (memo.last_access, key))

    def drop_memo_locked(self, key: QueryKey) -> None:
        self._memo_verify_pin_count.pop(key, None)
        memo = self.memos.pop(key, None)
        if memo is None:
            return
        for dep in memo.deps:
            dependents = self.dependents.get(dep.key)
            if dependents is None:
                continue
            dependents.discard(key)
            if not dependents:
                self.dependents.pop(dep.key, None)

    def _rebuild_lru_heap_locked(self) -> None:
        self._lru_heap = [(memo.last_access, k) for k, memo in self.memos.items()]
        heapq.heapify(self._lru_heap)

    def _pop_lru_victim_key_locked(self) -> QueryKey | None:
        while self._lru_heap:
            last_acc, key = heapq.heappop(self._lru_heap)
            memo = self.memos.get(key)
            if memo is None or memo.last_access != last_acc:
                continue
            if self._memo_verify_pin_count.get(key, 0) > 0:
                continue
            return key
        return None

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
                self._rebuild_lru_heap_locked()
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

    def latest_input_version(self, input_key: InputKey) -> InputVersion | None:
        versions = self.inputs.get(input_key)
        if not versions:
            return None
        return versions[-1]

    def input_version_at(self, input_key: InputKey, revision: int) -> InputVersion | None:
        versions = self.inputs.get(input_key)
        if not versions:
            return None
        revs = [v.revision for v in versions]
        idx = bisect.bisect_right(revs, revision) - 1
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
            self.trace_event("input_set", trace_key, detail=f"changed_at={changed_at}")
            return self.revision

    def inspect_graph(self) -> dict[str, Any]:
        with self.lock:
            nodes = [self.key_to_str(k) for k in self.memos.keys()]
            edges = []
            for parent, memo in self.memos.items():
                parent_s = self.key_to_str(parent)
                for dep in memo.deps:
                    edges.append((parent_s, self.key_to_str(dep.key)))
            return {
                "revision": self.revision,
                "memo_count": len(self.memos),
                "input_count": len(self.inputs),
                "nodes": nodes,
                "edges": edges,
            }

    def subgraph(self, roots: Sequence[QueryKey | str], *, direction: str) -> dict[str, Any]:
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
            edges_out = [(p, d) for p, d in edges_full if p in reachable and d in reachable]
            return {
                "revision": self.revision,
                "memo_count": len(nodes_out),
                "input_count": len(self.inputs),
                "nodes": nodes_out,
                "edges": edges_out,
            }

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
            deps_in = payload["dependents"]
            self.dependents = defaultdict(set)
            for k, v in deps_in.items():
                self.dependents[k] = set(v)
            self.trace = deque(payload["trace"], maxlen=self.trace_limit)
            self.next_access_id = payload["access_id"]
            # In-flight dedup futures are process-local/transient and should never
            # survive a load boundary.
            self.in_flight.clear()
            self._memo_verify_pin_count.clear()
            self._rebuild_lru_heap_locked()

    def make_persistence_payload(self) -> dict[str, Any]:
        with self.lock:
            return {
                "revision": self.revision,
                "cancel_epoch": self.cancel_epoch,
                "inputs": self.inputs,
                "memos": self.memos,
                "dependents": self.dependents,
                "trace": list(self.trace),
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
    ) -> MemoEntry:
        return MemoEntry(
            value=value,
            value_hash=value_hash,
            changed_at=changed_at,
            verified_at=verified_at,
            deps=tuple(Dependency(dep_key, observed) for dep_key, observed in deps.items()),
            effects=effects,
            last_access=last_access,
        )
