from __future__ import annotations

import concurrent.futures
import os
import threading
from typing import Any, Callable, Iterable, Literal, Mapping, Sequence

from ._errors import CancellationError, CycleError, QueryCancelled
from ._evaluator import Evaluator
from ._persistence import load_payload, save_payload
from ._runtime import RuntimeState
from ._scheduler import WorkStealingExecutor
from ._state import InputKey, InputVersion, MemoEntry, QueryKey, Snapshot, TraceEvent
from ._store import GraphStore

_UNSET = object()


def _default_submit_pool_workers() -> int:
    return min(32, (os.cpu_count() or 1) + 4)


__all__ = [
    "Accumulator",
    "CancellationError",
    "CycleError",
    "Engine",
    "QueryCancelled",
    "Snapshot",
    "TraceEvent",
]


class _InputHandle:
    def __init__(self, engine: Engine, fn: Callable[..., Any]) -> None:
        self._engine = engine
        self._fn = fn
        self._id = engine._function_id(fn)

    def __call__(self, *args: Any, snapshot: Snapshot | None = None) -> Any:
        return self._engine._read_input(self._id, self._fn, args, snapshot=snapshot)

    def set(self, *args: Any, value: Any = _UNSET) -> int:
        if value is _UNSET:
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

    def latest_input_version(self, input_key: tuple[str, tuple[Any, ...]]) -> InputVersion | None:
        return self._store.latest_input_version(input_key)

    def input_version_at(self, input_key: tuple[str, tuple[Any, ...]], revision: int) -> InputVersion | None:
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
    def in_flight(self) -> dict[tuple[QueryKey, int], concurrent.futures.Future[MemoEntry]]:
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
    _INTERNAL_TEST_API: tuple[str, ...] = (
        "_internals",
    )

    def __init__(
        self,
        *,
        max_entries: int = 10_000,
        trace_limit: int = 50_000,
        stats: bool = False,
        stats_eviction_recent_cap: int = 32,
        stats_clock: Callable[[], float] | None = None,
    ) -> None:
        self._trace_limit = trace_limit
        self._store = GraphStore(
            max_entries=max_entries,
            trace_limit=trace_limit,
            stats=stats,
            stats_eviction_recent_cap=stats_eviction_recent_cap,
            monotonic_seconds=stats_clock,
        )
        self._evaluator = Evaluator(self._store)
        # Single private probe for invariant-oriented internals.
        self._internals = _EngineInternals(self._store, self._evaluator)
        self._submit_executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._submit_executor_lock = threading.Lock()

    @property
    def revision(self) -> int:
        return self._store.revision

    def snapshot(self) -> Snapshot:
        return self._store.snapshot()

    def input(self, fn: Callable[..., Any]) -> _InputHandle:
        return _InputHandle(self, fn)

    def query(self, fn: Callable[..., Any]) -> _QueryHandle:
        handle = _QueryHandle(self, fn)
        self._store.register_query(handle.id, fn)
        return handle

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
    ) -> list[Any]:
        if not calls:
            return []
        run_snapshot = snapshot or self.snapshot()
        worker_count = workers or min(32, max(1, len(calls)))
        scheduler = WorkStealingExecutor(worker_count)
        for idx, (query, args) in enumerate(calls):
            scheduler.submit_indexed(
                idx,
                lambda q=query, a=args: self._query_call(q.id, q.raw, a, snapshot=run_snapshot),
            )
        return scheduler.run(len(calls))

    def traces(self) -> list[TraceEvent]:
        return self._store.traces()

    def clear_traces(self) -> None:
        self._store.clear_traces()

    def inspect_graph(self) -> dict[str, Any]:
        return self._store.inspect_graph()

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

    def prune(self, roots: Iterable[tuple[str, str, tuple[Any, ...]]]) -> None:
        self._store.prune(list(roots))

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
        return self._store.set_input(input_id, args, value, bump_cancel_epoch=bump_cancel_epoch)

    def _read_input(
        self,
        input_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
    ) -> Any:
        return self._evaluator.read_input(input_id, fn, args, snapshot=snapshot)

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

    def _compute_or_get_memo(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
    ) -> tuple[MemoEntry, bool]:
        return self._evaluator.compute_or_get_memo(key, fn, runtime)

    def _try_mark_green(self, key: QueryKey, entry: MemoEntry, snapshot: Snapshot) -> bool:
        return self._evaluator.try_mark_green(key, entry, snapshot)

    def _recompute(self, key: QueryKey, fn: Callable[..., Any], runtime: RuntimeState) -> MemoEntry:
        return self._evaluator.recompute(key, fn, runtime)

    def _record_dependency(self, dep_key: QueryKey, observed_changed_at: int) -> None:
        self._evaluator.record_dependency(dep_key, observed_changed_at)

    def _replay_effects(self, effects: Mapping[str, Sequence[Any]]) -> None:
        self._evaluator.replay_effects(effects)

    def _push_effect(self, name: str, item: Any) -> None:
        self._evaluator.push_effect(name, item)

