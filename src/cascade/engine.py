from __future__ import annotations

import asyncio
import contextvars
import inspect
import concurrent.futures
import os
import threading
from typing import Any, Callable, Iterable, Iterator, Literal, Mapping, Sequence


from . import _canonical
from ._ast_rewrite import rewrite_query
from ._disk_cache import DiskCache, DiskCacheProtocol
from ._errors import CancellationError, CycleError, PersistentCacheError, QueryCancelled
from ._evaluator import Evaluator
from ._incremental import IncrementalRuntime
from ._persistence import load_payload, save_payload
from ._runtime import RuntimeState
from ._scheduler import WorkStealingExecutor
from ._state import InputKey, InputVersion, MemoEntry, QueryKey, Snapshot, TraceEvent
from ._store import GraphStore

_get_loop = getattr(asyncio, "_get_running_loop", None)

_UNSET = object()


def _default_submit_pool_workers() -> int:
    return min(32, (os.cpu_count() or 1) + 4)


__all__ = [
    "Accumulator",
    "CancellationError",
    "CycleError",
    "Engine",
    "PersistentCacheError",
    "QueryCancelled",
    "Snapshot",
    "TraceEvent",
]


class EngineTransaction:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.updates: list[tuple[str, tuple[Any, ...], Any]] = []
        self._token: contextvars.Token | None = None

    def __enter__(self) -> EngineTransaction:
        if _active_transaction.get() is not None:
            raise RuntimeError("Nested transactions are not supported")
        self._token = _active_transaction.set(self)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._token is not None:
            _active_transaction.reset(self._token)
        if exc_type is None and self.updates:
            self.engine._set_inputs(self.updates)


_active_transaction: contextvars.ContextVar[EngineTransaction | None] = (
    contextvars.ContextVar("active_transaction", default=None)
)


class _InputHandle:
    def __init__(self, engine: Engine, fn: Callable[..., Any]) -> None:
        self._engine = engine
        self._fn = fn
        self._id = engine._function_id(fn)
        self._is_async_fn = inspect.iscoroutinefunction(fn)

    def __call__(self, *args: Any, snapshot: Snapshot | None = None) -> Any:
        if not self._is_async_fn:
            return self._engine._read_input(self._id, self._fn, args, snapshot=snapshot)

        if _get_loop is not None and _get_loop() is not None:
            return self._engine._read_input_async(
                self._id, self._fn, args, snapshot=snapshot
            )

        if _get_loop is None:
            try:
                asyncio.get_running_loop()
                return self._engine._read_input_async(
                    self._id, self._fn, args, snapshot=snapshot
                )
            except RuntimeError:
                pass

        return self._engine._read_input(self._id, self._fn, args, snapshot=snapshot)

    def set(self, *args: Any, value: Any = _UNSET) -> int | None:
        if value is _UNSET:
            if not args:
                raise TypeError("set() requires input value")
            *input_args, resolved_value = args
            args = tuple(input_args)
            value = resolved_value

        tx = _active_transaction.get()
        if tx is not None and tx.engine is self._engine:
            tx.updates.append((self._id, tuple(args), value))
            return None

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
        self._is_async_fn = inspect.iscoroutinefunction(fn)

    def __call__(
        self,
        *args: Any,
        snapshot: Snapshot | None = None,
        effects: dict[str, list[Any]] | None = None,
    ) -> Any:
        if not self._is_async_fn:
            return self._engine._query_call(
                self._id, self._fn, tuple(args), snapshot=snapshot, effects=effects
            )

        if _get_loop is not None and _get_loop() is not None:
            return self._engine._query_call_async(
                self._id, self._fn, tuple(args), snapshot=snapshot, effects=effects
            )

        if _get_loop is None:
            try:
                asyncio.get_running_loop()
                return self._engine._query_call_async(
                    self._id, self._fn, tuple(args), snapshot=snapshot, effects=effects
                )
            except RuntimeError:
                pass

        return self._engine._query_call(
            self._id, self._fn, tuple(args), snapshot=snapshot, effects=effects
        )

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


class _EngineInternals:
    """Single invariant-oriented probe surface for internal tests.

    This boundary is intentionally narrow. New invariant checks should route
    through this object rather than adding more private attributes on Engine.
    """

    def __init__(self, store: GraphStore, evaluator: Evaluator) -> None:
        self._store = store
        self._evaluator = evaluator

    @property
    def memos(self) -> dict[QueryKey, MemoEntry]:
        return self._store.memos

    @property
    def dependents(self) -> dict[QueryKey, set[QueryKey]]:
        return self._store.dependents

    def latest_input_version(
        self, input_key: tuple[str, tuple[Any, ...]]
    ) -> InputVersion | None:
        return self._store.latest_input_version(input_key)

    def input_version_at(
        self, input_key: tuple[str, tuple[Any, ...]], revision: int
    ) -> InputVersion | None:
        return self._store.input_version_at(input_key, revision)

    def dependency_changed_at(self, key: QueryKey, snapshot: Snapshot) -> int:
        return self._evaluator.dependency_changed_at(key, snapshot)

    @property
    def cancel_epoch(self) -> int:
        return self._store.cancel_epoch

    @property
    def next_access_id(self) -> int:
        return self._store.next_access_id

    @property
    def in_flight(
        self,
    ) -> dict[tuple[QueryKey, int], concurrent.futures.Future[MemoEntry]]:
        return self._store.in_flight

    @property
    def inputs(self) -> dict[InputKey, list[InputVersion]]:
        return self._store.inputs

    @property
    def queries(self) -> dict[str, Callable[..., Any]]:
        return self._store.queries

    @property
    def lock(self) -> threading.RLock:
        return self._store.lock

    @property
    def max_entries(self) -> int:
        return self._store.max_entries


class Engine:
    # Explicit private-policy contract for tests/introspection.
    # Invariant-oriented access should flow through the _internals probe.
    _INTERNAL_TEST_API: tuple[str, ...] = ("_internals",)

    def __init__(
        self,
        *,
        max_entries: int = 10_000,
        trace_limit: int = 50_000,
        stats: bool = False,
        stats_eviction_recent_cap: int = 32,
        stats_clock: Callable[[], float] | None = None,
        cache_dir: str | os.PathLike[str] | None = None,
        cache_map_size: int = 1 << 30,
        cache_backend: str = "mdbx",
        incremental: bool = True,
    ) -> None:
        self._trace_limit = trace_limit
        # Global opt-out for the map/reduce AST interception; per-query
        # incremental= on @engine.query overrides it in either direction.
        self._incremental_default = incremental
        # Passing cache_dir switches on zero-config persistence: MDBX store,
        # deterministic msgpack serialization, and content fingerprints as the
        # cross-session revision markers. Missing libmdbx/msgpack raises here.
        self._disk: DiskCacheProtocol | None = None
        value_digest: Callable[[Any], str] | None = None
        if cache_dir is not None:
            self._disk = DiskCache(cache_dir, map_size=cache_map_size, cache_backend=cache_backend)
            value_digest = _canonical.value_digest
        self._store = GraphStore(
            max_entries=max_entries,
            trace_limit=trace_limit,
            stats=stats,
            stats_eviction_recent_cap=stats_eviction_recent_cap,
            monotonic_seconds=stats_clock,
            value_digest=value_digest,
        )
        self._evaluator = Evaluator(
            self._store, disk=self._disk, get_executor=self._ensure_submit_executor
        )
        self._incremental_rt = IncrementalRuntime(self)
        # Single private probe for invariant-oriented internals.
        self._internals = _EngineInternals(self._store, self._evaluator)
        self._submit_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._submit_executor_lock = threading.Lock()

    @property
    def revision(self) -> int:
        return self._store.revision

    def snapshot(self) -> Snapshot:
        return self._store.snapshot()

    @property
    def cache_dir(self) -> str | None:
        return None if self._disk is None else self._disk.path

    def clear_disk_cache(self) -> None:
        """Delete all entries in the persistent cache; the next run recomputes."""
        if self._disk is None:
            raise PersistentCacheError(
                "engine has no persistent cache; pass cache_dir= to Engine"
            )
        self._disk.clear()

    def transaction(self) -> EngineTransaction:
        return EngineTransaction(self)

    def input(self, fn: Callable[..., Any]) -> _InputHandle:
        handle = _InputHandle(self, fn)
        self._store.register_input(handle.id, fn)
        return handle

    def query(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        memoize: bool = True,
        fixed_point: Any = _UNSET,
        cache_exceptions: bool | tuple[type[BaseException], ...] = True,
        ttl: float | None = None,
        incremental: bool | None = None,
    ) -> Any:
        def decorator(f: Callable[..., Any]) -> _QueryHandle:
            enabled = self._incremental_default if incremental is None else incremental
            run_fn = f
            if enabled:
                rewritten = rewrite_query(f, self._incremental_rt)
                if rewritten is not None:
                    run_fn = rewritten
            handle = _QueryHandle(self, run_fn)
            has_fixed_point = fixed_point is not _UNSET
            self._store.register_query(
                handle.id,
                run_fn,
                memoize=memoize,
                fixed_point=fixed_point,
                has_fixed_point=has_fixed_point,
                cache_exceptions=cache_exceptions,
                ttl=ttl,
            )
            return handle

        if fn is not None:
            return decorator(fn)
        return decorator

    def accumulator(self, name: str) -> Accumulator:
        return Accumulator(self, name=name)

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        """Shut down the lazily created default :meth:`submit` thread pool, if any.

        When ``executor`` is not passed to :meth:`submit`, work runs on a shared
        per-engine pool; call this when discarding the engine if you need prompt
        thread teardown (for example in tests).
        """
        with self._submit_executor_lock:
            pool = self._submit_executor
            self._submit_executor = None
        if pool is not None:
            pool.shutdown(wait=wait, cancel_futures=cancel_futures)
        if self._disk is not None:
            self._disk.close()

    def _ensure_submit_executor(self) -> concurrent.futures.ThreadPoolExecutor:
        with self._submit_executor_lock:
            if self._submit_executor is None:
                self._submit_executor = concurrent.futures.ThreadPoolExecutor(
                    max_workers=_default_submit_pool_workers()
                )
            return self._submit_executor

    def submit(
        self,
        query: _QueryHandle,
        *args: Any,
        snapshot: Snapshot | None = None,
        effects: dict[str, list[Any]] | None = None,
        executor: concurrent.futures.Executor | None = None,
    ) -> concurrent.futures.Future[Any]:
        """Run ``query`` asynchronously.

        If ``executor`` is ``None`` (default), the engine uses a lazily created
        shared :class:`~concurrent.futures.ThreadPoolExecutor` so repeated
        submits do not spawn a new pool each time. Pass a long-lived executor
        when you need isolation, custom limits, or coordinated shutdown with
        other tasks. Call :meth:`shutdown` when dropping the engine if you
        require the default pool to release threads promptly.
        """
        run_snapshot = snapshot or self.snapshot()
        with self._store.lock:
            cancel_epoch = self._store.cancel_epoch

        def run() -> Any:
            return self._query_call(
                query.id,
                query.raw,
                tuple(args),
                snapshot=run_snapshot,
                effects=effects,
                cancel_epoch=cancel_epoch,
            )

        if executor is None:
            return self._ensure_submit_executor().submit(run)
        return executor.submit(run)

    def compute_many(
        self,
        calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]],
        *,
        workers: int | None = None,
        snapshot: Snapshot | None = None,
        effects: dict[str, list[Any]] | None = None,
    ) -> list[Any]:
        if not calls:
            return []
        run_snapshot = snapshot or self.snapshot()
        worker_count = workers or min(32, max(1, len(calls)))
        scheduler = WorkStealingExecutor(worker_count)
        per_call_effects: list[dict[str, list[Any]] | None]
        if effects is None:
            per_call_effects = [None] * len(calls)
        else:
            per_call_effects = [{} for _ in range(len(calls))]
        for idx, (query, args) in enumerate(calls):
            scheduler.submit_indexed(
                idx,
                lambda q=query, a=args, e=per_call_effects[idx]: self._query_call(
                    q.id,
                    q.raw,
                    a,
                    snapshot=run_snapshot,
                    effects=e,
                ),
            )
        results = scheduler.run(len(calls))
        if effects is not None:
            for call_effects in per_call_effects:
                if not call_effects:
                    continue
                for name, items in call_effects.items():
                    if not items:
                        continue
                    effects.setdefault(name, []).extend(items)
        return results

    def compute_many_stream(
        self,
        calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]],
        *,
        workers: int | None = None,
        snapshot: Snapshot | None = None,
        effects: dict[str, list[Any]] | None = None,
    ) -> Iterator[tuple[int, Any, dict[str, list[Any]]]]:
        """Yield completed results as each call finishes.

        Yields ``(index, value, call_effects)`` where ``index`` matches the position in
        ``calls`` and ``call_effects`` contains accumulator output for that call when
        ``effects`` was provided (otherwise an empty dict).

        Unlike :meth:`compute_many`, this API yields items in *completion order*.
        If ``effects`` is provided, it is populated after all calls complete, merged
        deterministically in call order (matching :meth:`compute_many`).
        """
        if not calls:
            return

        run_snapshot = snapshot or self.snapshot()
        worker_count = workers or min(32, max(1, len(calls)))

        per_call_effects: list[dict[str, list[Any]]]
        per_call_effects = [{} for _ in range(len(calls))]

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            future_to_index: dict[concurrent.futures.Future[Any], int] = {}
            for idx, (query, args) in enumerate(calls):
                fut = pool.submit(
                    self._query_call,
                    query.id,
                    query.raw,
                    args,
                    snapshot=run_snapshot,
                    effects=per_call_effects[idx] if effects is not None else None,
                )
                future_to_index[fut] = idx

            for fut in concurrent.futures.as_completed(future_to_index):
                idx = future_to_index[fut]
                value = fut.result()
                call_effects = per_call_effects[idx] if effects is not None else {}
                item = (idx, value, call_effects)
                yield item

        if effects is not None:
            for call_effects in per_call_effects:
                if not call_effects:
                    continue
                for name, items in call_effects.items():
                    if not items:
                        continue
                    effects.setdefault(name, []).extend(items)

    def traces(self) -> list[TraceEvent]:
        return self._store.traces()

    def clear_traces(self) -> None:
        self._store.clear_traces()

    def inspect_graph(self, *, condense: bool = False) -> dict[str, Any]:
        graph = self._store.inspect_graph(condense=condense)
        extra_nodes, extra_edges = self._incremental_rt.graph_extras()
        if extra_nodes:
            known = set(graph["nodes"])
            graph["nodes"] = list(graph["nodes"]) + [
                n for n in extra_nodes if n not in known
            ]
            graph["edges"] = list(graph["edges"]) + extra_edges
        return graph

    def inspect_pipelines(self) -> list[dict[str, Any]]:
        """Logical map/filter/reduce pipelines with fusion and checkpoint info."""
        return self._incremental_rt.inspect_pipelines()

    def subgraph(
        self,
        roots: Sequence[QueryKey | str],
        *,
        direction: Literal["deps", "dependents"] = "deps",
    ) -> dict[str, Any]:
        """Memoized nodes/edges reachable from ``roots`` (default: transitive dependencies).

        Edges follow :meth:`inspect_graph` semantics: ``(parent_key, dep_key)`` means
        *parent depends on dep*. The default ``direction="deps"`` walks from each root
        toward its dependencies (backward along the computation graph). Use
        ``direction="dependents"`` for transitive dependents (forward).

        String roots must match entries in :meth:`inspect_graph` ``nodes``; unknown
        roots are ignored (same policy as :meth:`prune`). ``QueryKey`` roots not
        present in the memo table are ignored. Empty ``roots`` yields empty
        ``nodes`` and ``edges`` (no exception). Thread-safe under the store lock.
        """
        return self._store.subgraph(roots, direction=direction)

    def enable_stats(self, enabled: bool = True) -> None:
        self._store.set_stats_enabled(enabled)

    def stats_summary(self) -> dict[str, Any]:
        return self._store.stats_summary()

    def reset_stats(self) -> None:
        self._store.reset_stats()

    @property
    def access_id(self) -> int:
        """The monotonically increasing sequence number for memo accesses."""
        return self._store.next_access_id

    def sweep_unaccessed(self, since_access_id: int) -> None:
        """Evict all memos that haven't been accessed since the given access ID."""
        self._store.sweep_unaccessed(since_access_id)

    def prune(
        self,
        roots: Iterable[tuple[str, str, tuple[Any, ...]]],
        *,
        vacuum_disk: bool = False,
    ) -> None:
        root_list = list(roots)
        self._store.prune(root_list)
        if vacuum_disk and self._disk is not None:
            self._evaluator.prune_disk_cache(root_list)

    def save(self, path: str) -> None:
        save_payload(path, self._store.make_persistence_payload())

    def load(self, path: str) -> None:
        payload = load_payload(path)
        if payload is None:
            return
        self._store.assign_loaded_state(payload)

    # --- internals ---
    def _function_id(self, fn: Callable[..., Any]) -> str:
        return f"{fn.__module__}:{fn.__qualname__}"

    def _trace_event(self, event: str, key: QueryKey, detail: str = "") -> None:
        self._store.trace_event(event, key, detail=detail)

    def _key_to_str(self, key: QueryKey) -> str:
        return self._store.key_to_str(key)

    def _stable_hash(self, value: Any) -> str:
        return self._store.stable_hash(value)

    def _set_input(
        self,
        input_id: str,
        args: tuple[Any, ...],
        value: Any,
        *,
        bump_cancel_epoch: bool = True,
    ) -> int:
        return self._store.set_input(
            input_id, args, value, bump_cancel_epoch=bump_cancel_epoch
        )

    def _set_inputs(
        self,
        updates: list[tuple[str, tuple[Any, ...], Any]],
        *,
        bump_cancel_epoch: bool = True,
    ) -> int:
        return self._store.set_inputs(updates, bump_cancel_epoch=bump_cancel_epoch)

    def _read_input(
        self,
        input_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
    ) -> Any:
        return self._evaluator.read_input(input_id, fn, args, snapshot=snapshot)

    async def _read_input_async(
        self,
        input_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
    ) -> Any:
        return await self._evaluator.read_input_async(
            input_id, fn, args, snapshot=snapshot
        )

    def _check_cancelled(self, runtime_cancel_epoch: int | None) -> None:
        self._evaluator.check_cancelled(runtime_cancel_epoch)

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
        return self._evaluator.query_call(
            query_id,
            fn,
            args,
            snapshot=snapshot,
            effects=effects,
            cancel_epoch=cancel_epoch,
        )

    async def _query_call_async(
        self,
        query_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
        effects: dict[str, list[Any]] | None = None,
        cancel_epoch: int | None = None,
    ) -> Any:
        return await self._evaluator.query_call_async(
            query_id,
            fn,
            args,
            snapshot=snapshot,
            effects=effects,
            cancel_epoch=cancel_epoch,
        )

    def _compute_or_get_memo(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
    ) -> tuple[MemoEntry, bool]:
        return self._evaluator.compute_or_get_memo(key, fn, runtime)

    def _try_mark_green(
        self, key: QueryKey, entry: MemoEntry, snapshot: Snapshot
    ) -> bool:
        return self._evaluator.try_mark_green(key, entry, snapshot)

    def _recompute(
        self, key: QueryKey, fn: Callable[..., Any], runtime: RuntimeState
    ) -> MemoEntry:
        return self._evaluator.recompute(key, fn, runtime)

    def _record_dependency(self, dep_key: QueryKey, observed_changed_at: int) -> None:
        self._evaluator.record_dependency(dep_key, observed_changed_at)

    def _replay_effects(self, effects: Mapping[str, Sequence[Any]]) -> None:
        self._evaluator.replay_effects(effects)

    def _push_effect(self, name: str, item: Any) -> None:
        self._evaluator.push_effect(name, item)
