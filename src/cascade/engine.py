from __future__ import annotations

import bisect
import concurrent.futures
import contextvars
import hashlib
import pickle
import sqlite3
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping, Sequence


class CycleError(RuntimeError):
    """Raised when a query cycle is detected."""


class CancellationError(RuntimeError):
    """Base cancellation exception for obsolete computations."""


class QueryCancelled(CancellationError):
    """Raised when a running query becomes obsolete."""


@dataclass(frozen=True)
class TraceEvent:
    event: str
    key: str
    revision: int
    detail: str
    timestamp: float


@dataclass(frozen=True)
class Dependency:
    key: tuple[str, str, tuple[Any, ...]]
    observed_changed_at: int


@dataclass
class MemoEntry:
    value: Any
    value_hash: str
    changed_at: int
    verified_at: int
    deps: tuple[Dependency, ...]
    effects: dict[str, tuple[Any, ...]]
    last_access: int


@dataclass(frozen=True)
class InputVersion:
    revision: int
    changed_at: int
    value_hash: str
    value: Any


@dataclass(frozen=True)
class Snapshot:
    revision: int


@dataclass
class _RuntimeFrame:
    key: tuple[str, str, tuple[Any, ...]]
    deps: dict[tuple[str, str, tuple[Any, ...]], int] = field(default_factory=dict)
    effects: dict[str, list[Any]] = field(default_factory=lambda: defaultdict(list))


@dataclass
class _RuntimeState:
    snapshot: Snapshot
    stack: list[_RuntimeFrame]
    root_effects: dict[str, list[Any]] | None
    cancel_epoch: int | None


class _InputHandle:
    def __init__(self, engine: Engine, fn: Callable[..., Any]) -> None:
        self._engine = engine
        self._fn = fn
        self._id = engine._function_id(fn)

    def __call__(self, *args: Any, snapshot: Snapshot | None = None) -> Any:
        return self._engine._read_input(self._id, self._fn, args, snapshot=snapshot)

    def set(self, *args: Any, value: Any = None) -> int:
        if value is None:
            if not args:
                raise TypeError("set() requires input value")
            *input_args, resolved_value = args
            args = tuple(input_args)
            value = resolved_value
        return self._engine._set_input(self._id, tuple(args), value)

    @property
    def id(self) -> str:
        return self._id

    def __repr__(self) -> str:
        return f"<InputHandle {self._id}>"


class _QueryHandle:
    def __init__(self, engine: Engine, fn: Callable[..., Any]) -> None:
        self._engine = engine
        self._fn = fn
        self._id = engine._function_id(fn)

    def __call__(
        self,
        *args: Any,
        snapshot: Snapshot | None = None,
        effects: dict[str, list[Any]] | None = None,
    ) -> Any:
        return self._engine._query_call(self._id, self._fn, tuple(args), snapshot=snapshot, effects=effects)

    @property
    def id(self) -> str:
        return self._id

    @property
    def raw(self) -> Callable[..., Any]:
        return self._fn

    def __repr__(self) -> str:
        return f"<QueryHandle {self._id}>"


class Accumulator:
    def __init__(self, engine: Engine, name: str) -> None:
        self._engine = engine
        self.name = name

    def push(self, item: Any) -> None:
        self._engine._push_effect(self.name, item)

    def __repr__(self) -> str:
        return f"<Accumulator {self.name}>"


class _WorkStealingExecutor:
    def __init__(self, workers: int) -> None:
        self._workers = max(1, workers)
        self._deques = [deque() for _ in range(self._workers)]
        self._lock = threading.RLock()
        self._pending = 0
        self._shutdown = False
        self._wake = threading.Condition(self._lock)

    def submit_indexed(self, index: int, fn: Callable[[], Any]) -> None:
        worker_idx = index % self._workers
        with self._wake:
            self._deques[worker_idx].append((index, fn))
            self._pending += 1
            self._wake.notify_all()

    def run(self, size: int) -> list[Any]:
        results: list[Any] = [None] * size
        errors: list[BaseException | None] = [None] * size
        threads: list[threading.Thread] = []

        def worker_loop(worker_idx: int) -> None:
            while True:
                task = self._take_task(worker_idx)
                if task is None:
                    return
                idx, fn = task
                try:
                    results[idx] = fn()
                except BaseException as exc:  # pragma: no cover - defensive branch
                    errors[idx] = exc
                finally:
                    with self._wake:
                        self._pending -= 1
                        self._wake.notify_all()

        for idx in range(self._workers):
            thread = threading.Thread(target=worker_loop, args=(idx,), daemon=True)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        for error in errors:
            if error is not None:
                raise error
        return results

    def _take_task(self, worker_idx: int) -> tuple[int, Callable[[], Any]] | None:
        with self._wake:
            while True:
                local = self._deques[worker_idx]
                if local:
                    return local.pop()
                for idx, bucket in enumerate(self._deques):
                    if idx == worker_idx:
                        continue
                    if bucket:
                        return bucket.popleft()
                if self._pending == 0 or self._shutdown:
                    return None
                self._wake.wait(timeout=0.01)


class Engine:
    def __init__(self, *, max_entries: int = 10_000, trace_limit: int = 50_000) -> None:
        self._lock = threading.RLock()
        self._revision = 0
        self._cancel_epoch = 0
        self._next_access_id = 0
        self._max_entries = max_entries
        self._trace_limit = trace_limit

        self._runtime_var: contextvars.ContextVar[_RuntimeState | None] = contextvars.ContextVar(
            "cascade_runtime", default=None
        )

        self._inputs: dict[tuple[str, tuple[Any, ...]], list[InputVersion]] = {}
        self._queries: dict[str, Callable[..., Any]] = {}
        self._memos: dict[tuple[str, str, tuple[Any, ...]], MemoEntry] = {}
        self._dependents: dict[tuple[str, str, tuple[Any, ...]], set[tuple[str, str, tuple[Any, ...]]]] = defaultdict(set)
        # Dedup only within the same logical read-view. Sharing in-flight work
        # across snapshot revisions can return stale values.
        self._in_flight: dict[
            tuple[str, str, tuple[Any, ...]],
            dict[int, concurrent.futures.Future[MemoEntry]],
        ] = {}
        self._trace: deque[TraceEvent] = deque(maxlen=trace_limit)

    @property
    def revision(self) -> int:
        return self._revision

    def snapshot(self) -> Snapshot:
        return Snapshot(revision=self._revision)

    def input(self, fn: Callable[..., Any]) -> _InputHandle:
        return _InputHandle(self, fn)

    def query(self, fn: Callable[..., Any]) -> _QueryHandle:
        handle = _QueryHandle(self, fn)
        self._queries[handle.id] = fn
        return handle

    def accumulator(self, name: str) -> Accumulator:
        return Accumulator(self, name=name)

    def submit(
        self,
        query: _QueryHandle,
        *args: Any,
        snapshot: Snapshot | None = None,
        effects: dict[str, list[Any]] | None = None,
        executor: concurrent.futures.Executor | None = None,
    ) -> concurrent.futures.Future[Any]:
        run_snapshot = snapshot or self.snapshot()
        with self._lock:
            cancel_epoch = self._cancel_epoch

        def run() -> Any:
            return self._query_call(query.id, query.raw, tuple(args), snapshot=run_snapshot, effects=effects, cancel_epoch=cancel_epoch)

        if executor is None:
            owned = concurrent.futures.ThreadPoolExecutor(max_workers=1)

            def wrapped() -> Any:
                try:
                    return run()
                finally:
                    owned.shutdown(wait=False, cancel_futures=False)

            return owned.submit(wrapped)
        return executor.submit(run)

    def compute_many(
        self,
        calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]],
        *,
        workers: int | None = None,
        snapshot: Snapshot | None = None,
    ) -> list[Any]:
        if not calls:
            return []
        run_snapshot = snapshot or self.snapshot()
        worker_count = workers or min(32, max(1, len(calls)))
        scheduler = _WorkStealingExecutor(worker_count)
        for idx, (query, args) in enumerate(calls):
            scheduler.submit_indexed(
                idx,
                lambda q=query, a=args: self._query_call(q.id, q.raw, a, snapshot=run_snapshot),
            )
        return scheduler.run(len(calls))

    def traces(self) -> list[TraceEvent]:
        return list(self._trace)

    def clear_traces(self) -> None:
        self._trace.clear()

    def inspect_graph(self) -> dict[str, Any]:
        with self._lock:
            nodes = [self._key_to_str(k) for k in self._memos.keys()]
            edges = []
            for parent, memo in self._memos.items():
                parent_s = self._key_to_str(parent)
                for dep in memo.deps:
                    edges.append((parent_s, self._key_to_str(dep.key)))
            return {
                "revision": self._revision,
                "memo_count": len(self._memos),
                "input_count": len(self._inputs),
                "nodes": nodes,
                "edges": edges,
            }

    def prune(self, roots: Iterable[tuple[str, str, tuple[Any, ...]]]) -> None:
        wanted: set[tuple[str, str, tuple[Any, ...]]] = set(roots)
        queue = deque(roots)
        while queue:
            node = queue.popleft()
            memo = self._memos.get(node)
            if memo is None:
                continue
            for dep in memo.deps:
                if dep.key[0] == "query" and dep.key not in wanted:
                    wanted.add(dep.key)
                    queue.append(dep.key)
        with self._lock:
            remove = [k for k in self._memos.keys() if k not in wanted]
            for key in remove:
                self._drop_memo_locked(key)

    def save(self, path: str) -> None:
        payload = {
            "revision": self._revision,
            "cancel_epoch": self._cancel_epoch,
            "inputs": self._inputs,
            "memos": self._memos,
            "dependents": self._dependents,
            "trace": list(self._trace),
            "access_id": self._next_access_id,
        }
        blob = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
        conn = sqlite3.connect(path)
        try:
            conn.execute("create table if not exists cascade_state (id integer primary key, payload blob not null)")
            conn.execute("delete from cascade_state")
            conn.execute("insert into cascade_state(id, payload) values (1, ?)", (blob,))
            conn.commit()
        finally:
            conn.close()

    def load(self, path: str) -> None:
        conn = sqlite3.connect(path)
        try:
            row = conn.execute("select payload from cascade_state where id = 1").fetchone()
            if row is None:
                return
            payload = pickle.loads(row[0])
        finally:
            conn.close()

        with self._lock:
            self._revision = payload["revision"]
            self._cancel_epoch = payload["cancel_epoch"]
            self._inputs = payload["inputs"]
            self._memos = payload["memos"]
            self._dependents = payload["dependents"]
            self._trace = deque(payload["trace"], maxlen=self._trace_limit)
            self._next_access_id = payload["access_id"]

    # --- internals ---
    def _function_id(self, fn: Callable[..., Any]) -> str:
        return f"{fn.__module__}:{fn.__qualname__}"

    def _trace_event(self, event: str, key: tuple[str, str, tuple[Any, ...]], detail: str = "") -> None:
        self._trace.append(
            TraceEvent(
                event=event,
                key=self._key_to_str(key),
                revision=self._revision,
                detail=detail,
                timestamp=time.time(),
            )
        )

    def _key_to_str(self, key: tuple[str, str, tuple[Any, ...]]) -> str:
        kind, fid, args = key
        return f"{kind}:{fid}{args}"

    def _stable_hash(self, value: Any) -> str:
        digest = hashlib.blake2b(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL), digest_size=20)
        return digest.hexdigest()

    def _touch_memo_locked(self, key: tuple[str, str, tuple[Any, ...]]) -> None:
        memo = self._memos[key]
        self._next_access_id += 1
        memo.last_access = self._next_access_id

    def _drop_memo_locked(self, key: tuple[str, str, tuple[Any, ...]]) -> None:
        memo = self._memos.pop(key, None)
        if memo is None:
            return
        for dep in memo.deps:
            dependents = self._dependents.get(dep.key)
            if dependents is None:
                continue
            dependents.discard(key)
            if not dependents:
                self._dependents.pop(dep.key, None)

    def _evict_if_needed_locked(self) -> None:
        while len(self._memos) > self._max_entries:
            oldest_key = min(self._memos.items(), key=lambda it: it[1].last_access)[0]
            self._trace_event("evict", oldest_key, "lru")
            self._drop_memo_locked(oldest_key)

    def _latest_input_version(self, input_key: tuple[str, tuple[Any, ...]]) -> InputVersion | None:
        versions = self._inputs.get(input_key)
        if not versions:
            return None
        return versions[-1]

    def _input_version_at(self, input_key: tuple[str, tuple[Any, ...]], revision: int) -> InputVersion | None:
        versions = self._inputs.get(input_key)
        if not versions:
            return None
        revs = [v.revision for v in versions]
        idx = bisect.bisect_right(revs, revision) - 1
        if idx < 0:
            return None
        return versions[idx]

    def _set_input(
        self,
        input_id: str,
        args: tuple[Any, ...],
        value: Any,
        *,
        bump_cancel_epoch: bool = True,
    ) -> int:
        with self._lock:
            self._revision += 1
            if bump_cancel_epoch:
                self._cancel_epoch += 1
            key = (input_id, args)
            versions = self._inputs.setdefault(key, [])
            current = versions[-1] if versions else None
            value_hash = self._stable_hash(value)
            if current is not None and current.value_hash == value_hash:
                changed_at = current.changed_at
            else:
                changed_at = self._revision
            versions.append(
                InputVersion(
                    revision=self._revision,
                    changed_at=changed_at,
                    value_hash=value_hash,
                    value=value,
                )
            )
            trace_key = ("input", input_id, args)
            self._trace_event("input_set", trace_key, detail=f"changed_at={changed_at}")
            return self._revision

    def _read_input(
        self,
        input_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
    ) -> Any:
        runtime = self._runtime_var.get()
        use_snapshot = snapshot or (runtime.snapshot if runtime is not None else self.snapshot())
        if runtime is not None:
            self._check_cancelled(runtime.cancel_epoch)
        key = ("input", input_id, args)
        version = self._input_version_at((input_id, args), use_snapshot.revision)
        if version is None:
            default = fn(*args)
            self._set_input(input_id, args, default, bump_cancel_epoch=False)
            version = self._latest_input_version((input_id, args))
            if version is None:  # pragma: no cover - safety belt
                raise RuntimeError("input version missing after initialization")
        self._record_dependency(key, version.changed_at)
        self._trace_event("input_read", key)
        return version.value

    def _check_cancelled(self, runtime_cancel_epoch: int | None) -> None:
        if runtime_cancel_epoch is None:
            return
        with self._lock:
            current = self._cancel_epoch
        if current != runtime_cancel_epoch:
            raise QueryCancelled("query cancelled because inputs changed")

    def _query_call(
        self,
        query_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
        effects: dict[str, list[Any]] | None = None,
        cancel_epoch: int | None = None,
    ) -> Any:
        runtime = self._runtime_var.get()
        if runtime is None:
            token = self._runtime_var.set(
                _RuntimeState(
                    snapshot=snapshot or self.snapshot(),
                    stack=[],
                    root_effects=effects,
                    cancel_epoch=cancel_epoch,
                )
            )
            try:
                return self._query_call(query_id, fn, args, snapshot=snapshot, effects=effects, cancel_epoch=cancel_epoch)
            finally:
                self._runtime_var.reset(token)

        key = ("query", query_id, args)
        if any(frame.key == key for frame in runtime.stack):
            cycle_start = [frame.key for frame in runtime.stack] + [key]
            cycle = " -> ".join(self._key_to_str(k) for k in cycle_start)
            raise CycleError(f"cycle detected: {cycle}")
        self._check_cancelled(runtime.cancel_epoch)
        entry, replay_needed = self._compute_or_get_memo(key, fn, runtime)
        self._record_dependency(key, entry.changed_at)
        if replay_needed:
            self._replay_effects(entry.effects)
        return entry.value

    def _compute_or_get_memo(
        self,
        key: tuple[str, str, tuple[Any, ...]],
        fn: Callable[..., Any],
        runtime: _RuntimeState,
    ) -> tuple[MemoEntry, bool]:
        with self._lock:
            existing = self._memos.get(key)
            if existing is not None:
                self._touch_memo_locked(key)
                if existing.verified_at == runtime.snapshot.revision:
                    self._trace_event("cache_hit", key)
                    return existing, True

                if self._try_mark_green(key, existing, runtime.snapshot):
                    existing.verified_at = runtime.snapshot.revision
                    self._touch_memo_locked(key)
                    self._trace_event("cache_green", key)
                    return existing, True

            in_flight_for_key = self._in_flight.get(key)
            owner_future = None if in_flight_for_key is None else in_flight_for_key.get(runtime.snapshot.revision)
            if owner_future is None:
                owner_future = concurrent.futures.Future()
                if in_flight_for_key is None:
                    in_flight_for_key = {}
                    self._in_flight[key] = in_flight_for_key
                in_flight_for_key[runtime.snapshot.revision] = owner_future
                is_owner = True
            else:
                is_owner = False

        if not is_owner:
            self._trace_event("dedup_wait", key)
            result = owner_future.result()
            return result, True

        try:
            result = self._recompute(key, fn, runtime)
            owner_future.set_result(result)
            return result, False
        except BaseException as exc:
            owner_future.set_exception(exc)
            raise
        finally:
            with self._lock:
                in_flight_for_key = self._in_flight.get(key)
                if in_flight_for_key is not None:
                    in_flight_for_key.pop(runtime.snapshot.revision, None)
                    if not in_flight_for_key:
                        self._in_flight.pop(key, None)

    def _try_mark_green(
        self,
        key: tuple[str, str, tuple[Any, ...]],
        entry: MemoEntry,
        snapshot: Snapshot,
    ) -> bool:
        for dep in entry.deps:
            dep_changed_at = self._dependency_changed_at(dep.key, snapshot)
            if dep_changed_at != dep.observed_changed_at:
                self._trace_event("cache_red", key, detail=self._key_to_str(dep.key))
                return False
        return True

    def _dependency_changed_at(self, key: tuple[str, str, tuple[Any, ...]], snapshot: Snapshot) -> int:
        kind, fid, args = key
        if kind == "input":
            version = self._input_version_at((fid, args), snapshot.revision)
            return -1 if version is None else version.changed_at

        with self._lock:
            memo = self._memos.get(key)
            fn = self._queries[fid]
        if memo is not None and memo.verified_at == snapshot.revision:
            return memo.changed_at

        runtime = self._runtime_var.get()
        if runtime is None:
            token = self._runtime_var.set(_RuntimeState(snapshot=snapshot, stack=[], root_effects=None, cancel_epoch=None))
            try:
                _ = self._query_call(fid, fn, args, snapshot=snapshot, effects=None, cancel_epoch=None)
            finally:
                self._runtime_var.reset(token)
        else:
            # Dependency verification should not add edges to currently executing frame.
            shadow = _RuntimeState(snapshot=snapshot, stack=[], root_effects=None, cancel_epoch=runtime.cancel_epoch)
            token = self._runtime_var.set(shadow)
            try:
                _ = self._query_call(fid, fn, args, snapshot=snapshot, effects=None, cancel_epoch=runtime.cancel_epoch)
            finally:
                self._runtime_var.reset(token)

        with self._lock:
            refreshed = self._memos.get(key)
            if refreshed is None:  # pragma: no cover - safety belt
                raise RuntimeError(f"missing memo for {self._key_to_str(key)}")
            return refreshed.changed_at

    def _recompute(
        self,
        key: tuple[str, str, tuple[Any, ...]],
        fn: Callable[..., Any],
        runtime: _RuntimeState,
    ) -> MemoEntry:
        frame = _RuntimeFrame(key=key)
        runtime.stack.append(frame)
        start = time.perf_counter()
        self._trace_event("recompute_start", key)
        try:
            self._check_cancelled(runtime.cancel_epoch)
            result = fn(*key[2])
            self._check_cancelled(runtime.cancel_epoch)
        finally:
            runtime.stack.pop()
        duration_ms = (time.perf_counter() - start) * 1000.0

        frozen_effects = {name: tuple(items) for name, items in frame.effects.items()}
        value_hash = self._stable_hash(result)
        with self._lock:
            previous = self._memos.get(key)
            if previous is not None and previous.value_hash == value_hash:
                changed_at = previous.changed_at
                self._trace_event("backdate", key)
            else:
                changed_at = runtime.snapshot.revision
            memo = MemoEntry(
                value=result,
                value_hash=value_hash,
                changed_at=changed_at,
                verified_at=runtime.snapshot.revision,
                deps=tuple(Dependency(dep_key, observed) for dep_key, observed in frame.deps.items()),
                effects=frozen_effects,
                last_access=self._next_access_id + 1,
            )
            self._next_access_id += 1
            self._drop_memo_locked(key)
            self._memos[key] = memo
            for dep_key in frame.deps.keys():
                self._dependents[dep_key].add(key)
            self._evict_if_needed_locked()
        self._trace_event("recompute_done", key, detail=f"{duration_ms:.3f}ms")
        return memo

    def _record_dependency(self, dep_key: tuple[str, str, tuple[Any, ...]], observed_changed_at: int) -> None:
        runtime = self._runtime_var.get()
        if runtime is None or not runtime.stack:
            return
        frame = runtime.stack[-1]
        frame.deps[dep_key] = observed_changed_at

    def _replay_effects(self, effects: Mapping[str, Sequence[Any]]) -> None:
        runtime = self._runtime_var.get()
        if runtime is None:
            return
        for name, values in effects.items():
            if runtime.root_effects is not None:
                runtime.root_effects.setdefault(name, []).extend(values)
            # Mirror _push_effect semantics so ancestor memo entries retain
            # transitive effects even when children are served from cache.
            for frame in runtime.stack:
                frame.effects[name].extend(values)

    def _push_effect(self, name: str, item: Any) -> None:
        runtime = self._runtime_var.get()
        if runtime is None:
            raise RuntimeError("accumulator.push() must be called inside a query")
        if runtime.root_effects is not None:
            runtime.root_effects.setdefault(name, []).append(item)
        for frame in runtime.stack:
            frame.effects[name].append(item)

