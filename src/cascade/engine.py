from __future__ import annotations

import asyncio
import contextvars
import inspect
import itertools
import concurrent.futures
import os
import threading
from typing import Any, Callable, Iterable, Iterator, Literal, Mapping, Sequence


from . import _canonical
from ._ast_rewrite import rewrite_query
from ._disk_cache import DiskCache, DiskCacheProtocol
from ._errors import (
    CancellationError,
    CycleError,
    EventLoopBlockedError,
    PersistentCacheError,
    QueryCancelled,
    SnapshotExpiredError,
    StaleHandleError,
    UnencodableValueError,
)
from ._evaluator import Evaluator
from ._incremental import IncrementalRuntime
from ._runtime import RuntimeState
from ._scheduler import WorkStealingExecutor
from ._state import (
    InFlight,
    InputKey,
    InputVersion,
    MemoEntry,
    QueryKey,
    Snapshot,
    TraceEvent,
)
from ._store import DEFAULT_FIXED_POINT_ITERATIONS, GraphStore

_get_loop = getattr(asyncio, "_get_running_loop", None)

_UNSET = object()


def _default_submit_pool_workers() -> int:
    return min(32, (os.cpu_count() or 1) + 4)


__all__ = [
    "Accumulator",
    "CancellationError",
    "CycleError",
    "Engine",
    "EventLoopBlockedError",
    "PersistentCacheError",
    "QueryCancelled",
    "Snapshot",
    "SnapshotExpiredError",
    "StaleHandleError",
    "TraceEvent",
    "UnencodableValueError",
]


class EngineTransaction:
    """Batches input updates and collection changes; all apply or none do.

    Input updates are queued and applied as one revision on success.
    Collection changes take effect on the collection object immediately (so
    code inside the block sees them), and are undone if the block raises.
    Named collections hold their disk writes until commit and write them in
    one batch, so a failed transaction never reaches disk.

    A transaction object is single-use: entering it a second time raises
    ``RuntimeError``. Call ``engine.transaction()`` again for a new one.

    A transaction belongs to whoever entered it: the asyncio task, or, when
    it was entered outside any task, the thread. Only work done there, while
    the block is open, is part of it. Anything else is outside it, including
    threads and tasks started inside the block (which may inherit a copy of
    the block's context): their input updates apply at once, as with no
    transaction. The one thing they may not do is change a collection the
    transaction has changed and not yet committed: that raises
    ``RuntimeError``, because the two sets of changes could not be told apart
    on rollback.
    """

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.updates: list[tuple[str, tuple[Any, ...], Any]] = []
        self._token: contextvars.Token | None = None
        self._used = False
        # Owner, valid while ``_open``: see ``_current_transaction``.
        self._open = False
        self._thread = 0
        self._task: Any = None
        # Collections changed in this transaction -> log head before the first
        # change, and disk writes held until commit for named collections.
        self.collections: dict[Any, int] = {}
        self.pending_disk: dict[Any, list[tuple[int, dict[str, Any]]]] = {}

    def __enter__(self) -> EngineTransaction:
        if _current_transaction() is not None:
            raise RuntimeError("Nested transactions are not supported")
        if self._used:
            # The object still holds the first block's updates and rollback
            # points; running them again would replay or undo committed work.
            raise RuntimeError(
                "this transaction has already been used; a transaction object "
                "is single-use, call engine.transaction() again for a new one"
            )
        self._used = True
        self._thread = threading.get_ident()
        self._task = _running_task()
        self._open = True
        self._token = _active_transaction.set(self)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        # Closed first: from here on nothing joins this transaction, while
        # its collections stay reserved until their new heads are published.
        self._open = False
        self._task = None
        if self._token is not None:
            _active_transaction.reset(self._token)
        try:
            if exc_type is not None:
                self._roll_back_collections()
                return
            try:
                # Each collection's batch is one disk transaction. If a later
                # one fails (disk full), earlier ones are already on disk;
                # that rare partial write is the cost of not spanning
                # collections.
                for core, batch in self.pending_disk.items():
                    core.write_pending(batch)
            except BaseException:
                self._roll_back_collections()
                raise
            if self.updates:
                self.engine._set_inputs(self.updates)
            for core in self.collections:
                core.after_commit()
        finally:
            for core in self.collections:
                core.release_transaction(self)

    def _roll_back_collections(self) -> None:
        for core, head in self.collections.items():
            core.roll_back_to(head)


# The transaction entered in this context. A context is copied into every
# asyncio task, into ``asyncio.to_thread`` and ``Context.run`` calls, and on
# free-threaded builds into every new thread, so the marker also shows up in
# places that are not the transaction's owner, and stays there after the
# block has ended. Read it through ``_current_transaction`` only.
_active_transaction: contextvars.ContextVar[EngineTransaction | None] = (
    contextvars.ContextVar("active_transaction", default=None)
)


def _running_task() -> Any:
    """The asyncio task running on this thread, or None."""
    try:
        return asyncio.current_task()
    except RuntimeError:
        return None


def _current_transaction() -> EngineTransaction | None:
    """The open transaction that the caller owns, if any.

    The owner is the task that entered the block. A block entered outside
    any task is owned by its thread: a task can then only run there while
    the block is open if the block itself runs the event loop.
    """
    tx = _active_transaction.get()
    if tx is None:
        return None
    if not tx._open or tx._thread != threading.get_ident():
        return None
    task = tx._task
    if task is not None and task is not _running_task():
        return None
    return tx


def _stale_handle_error(kind: str, handle_id: str) -> StaleHandleError:
    return StaleHandleError(
        f"this handle for {kind} {handle_id!r} is stale: a different function "
        f"was registered under that name after the handle was created. If the "
        f"code was reloaded, use the handle from the new registration (import "
        f"it again). If several {kind} functions are made from one body (a "
        f"factory, a loop, lambdas), give each its own identity with "
        f"engine.{kind}(name=...)."
    )


class _InputHandle:
    def __init__(self, engine: Engine, fn: Callable[..., Any], input_id: str) -> None:
        self._engine = engine
        self._fn = fn
        self._id = input_id
        self._is_async_fn = inspect.iscoroutinefunction(fn)

    def _mark_stale(self) -> None:
        # Swapping the class keeps the check off the path of live handles.
        self.__class__ = _StaleInputHandle

    def __call__(self, *args: Any, snapshot: Snapshot | None = None) -> Any:
        if not self._is_async_fn:
            return self._engine._read_input(self._id, self._fn, args, snapshot=snapshot)

        if _get_loop is not None and _get_loop() is not None:
            if self._engine._evaluator.in_sync_body():
                return self._engine._read_input(
                    self._id, self._fn, args, snapshot=snapshot
                )
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

        if _active_transaction.get() is not None:
            tx = _current_transaction()
            if tx is not None and tx.engine is self._engine:
                tx.updates.append((self._id, tuple(args), value))
                return None

        return self._engine._set_input(self._id, tuple(args), value)

    @property
    def id(self) -> str:
        return self._id

    def __repr__(self) -> str:
        return f"<InputHandle {self._id}>"


class _StaleInputHandle(_InputHandle):
    """An input handle whose function was replaced; every use raises."""

    def __call__(self, *args: Any, snapshot: Snapshot | None = None) -> Any:
        raise _stale_handle_error("input", self._id)

    def set(self, *args: Any, value: Any = _UNSET) -> int | None:
        raise _stale_handle_error("input", self._id)

    def __repr__(self) -> str:
        return f"<InputHandle {self._id} (stale)>"


class _QueryHandle:
    def __init__(self, engine: Engine, fn: Callable[..., Any], query_id: str) -> None:
        self._engine = engine
        self._fn = fn
        self._id = query_id
        self._is_async_fn = inspect.iscoroutinefunction(fn)

    def _mark_stale(self) -> None:
        # Swapping the class keeps the check off the path of live handles.
        self.__class__ = _StaleQueryHandle

    def _live_fn(self) -> Callable[..., Any]:
        """The function to run; raises on a stale handle."""
        return self._fn

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
            if self._engine._evaluator.in_sync_body():
                # A sync query's body on the loop thread cannot await. The
                # sync path serves a cached result and raises
                # EventLoopBlockedError where it would have to block.
                return self._engine._query_call(
                    self._id, self._fn, tuple(args), snapshot=snapshot, effects=effects
                )
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


class _StaleQueryHandle(_QueryHandle):
    """A query handle whose function was replaced; calling it raises."""

    def __call__(
        self,
        *args: Any,
        snapshot: Snapshot | None = None,
        effects: dict[str, list[Any]] | None = None,
    ) -> Any:
        raise _stale_handle_error("query", self._id)

    def _live_fn(self) -> Callable[..., Any]:
        raise _stale_handle_error("query", self._id)

    def __repr__(self) -> str:
        return f"<QueryHandle {self._id} (stale)>"


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
    ) -> dict[tuple[QueryKey, int], InFlight]:
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
        # Passing cache_dir switches on zero-config persistence (MDBX by default,
        # or sqlite), deterministic msgpack serialization, and content
        # fingerprints as the cross-session revision markers. Missing dependencies
        # (e.g. libmdbx/msgpack) raise here.
        self._disk: DiskCacheProtocol | None = None
        value_digest: Callable[[Any], str] | None = None
        if cache_dir is not None:
            self._disk = DiskCache(
                cache_dir, map_size=cache_map_size, cache_backend=cache_backend
            )
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
        """Capture the current revision for consistent reads while inputs change.

        The engine keeps the input history this snapshot needs until it is
        released: call ``release()``, use it as ``with engine.snapshot() as s:``,
        or drop every reference to it. Reading through a released snapshot can
        raise ``SnapshotExpiredError``.
        """
        return self._store.snapshot()

    @property
    def cache_dir(self) -> str | None:
        return None if self._disk is None else self._disk.path

    def clear_disk_cache(self) -> None:
        """Delete every cached query result on disk; the next run recomputes.

        Named collections are not touched. Their logs are data, not cache:
        nothing could rebuild them. To empty a collection, call its
        ``clear()``.
        """
        if self._disk is None:
            raise PersistentCacheError(
                "engine has no persistent cache; pass cache_dir= to Engine"
            )
        # Results whose write failed are being deleted anyway.
        self._evaluator.flush_disk(report=False)
        self._disk.clear()

    def transaction(self) -> EngineTransaction:
        return EngineTransaction(self)

    def _active_transaction(self) -> EngineTransaction | None:
        """The open transaction on this engine that the caller owns, if any."""
        tx = _current_transaction()
        if tx is not None and tx.engine is self:
            return tx
        return None

    def input(
        self, fn: Callable[..., Any] | None = None, *, name: str | None = None
    ) -> Any:
        """Register an input. Use as ``@engine.input`` or ``@engine.input(name=...)``.

        ``name`` replaces the default identity ``module:qualname``. Inputs
        made from one function body (a factory, a loop, lambdas) need
        distinct names; without them each registration replaces the last.
        """

        def decorator(f: Callable[..., Any]) -> _InputHandle:
            input_id = self._resolve_id(f, name)
            registered = self._store.register_input(input_id, f)
            handle = _InputHandle(self, registered, input_id)
            self._store.track_handle("input", input_id, registered, handle)
            return handle

        if fn is not None:
            return decorator(fn)
        return decorator

    def query(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        memoize: bool = True,
        fixed_point: Any = _UNSET,
        cache_exceptions: bool | tuple[type[BaseException], ...] = True,
        ttl: float | None = None,
        incremental: bool | None = None,
        max_iterations: int | None = None,
    ) -> Any:
        if max_iterations is not None:
            if fixed_point is _UNSET:
                raise TypeError("max_iterations= needs fixed_point=")
            if isinstance(max_iterations, bool) or not isinstance(max_iterations, int):
                raise TypeError("max_iterations must be an int")
            if max_iterations < 1:
                raise ValueError("max_iterations must be at least 1")

        def decorator(f: Callable[..., Any]) -> _QueryHandle:
            query_id = self._resolve_id(f, name)
            enabled = self._incremental_default if incremental is None else incremental
            run_fn = f
            if enabled:
                rewritten = rewrite_query(
                    f, self._incremental_rt, site=None if name is None else query_id
                )
                if rewritten is not None:
                    run_fn = rewritten
            has_fixed_point = fixed_point is not _UNSET
            registered = self._store.register_query(
                query_id,
                run_fn,
                memoize=memoize,
                fixed_point=fixed_point,
                has_fixed_point=has_fixed_point,
                cache_exceptions=cache_exceptions,
                ttl=ttl,
                max_iterations=(
                    DEFAULT_FIXED_POINT_ITERATIONS
                    if max_iterations is None
                    else max_iterations
                ),
            )
            handle = _QueryHandle(self, registered, query_id)
            self._store.track_handle("query", query_id, registered, handle)
            return handle

        if fn is not None:
            return decorator(fn)
        return decorator

    def accumulator(self, name: str) -> Accumulator:
        return Accumulator(self, name=name)

    def flush_disk(self) -> None:
        """Wait until every finished result is written to the persistent cache.

        Raises ``PersistentCacheError``, with the original error as its
        cause, if writing any result failed since the last ``flush_disk()``
        or ``shutdown()``. The results stay correct in memory, and the writer
        keeps going with the next ones.
        """
        self._evaluator.flush_disk()

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        """Shut down the lazily created default :meth:`submit` thread pool, if any.

        When ``executor`` is not passed to :meth:`submit`, work runs on a shared
        per-engine pool; call this when discarding the engine if you need prompt
        thread teardown (for example in tests).

        With ``cache_dir`` it also writes the queued results and closes the
        cache. Like :meth:`flush_disk` it raises ``PersistentCacheError`` if
        a result could not be written and that was not reported yet; the
        cache is closed first.
        """
        with self._submit_executor_lock:
            pool = self._submit_executor
            self._submit_executor = None
        if pool is not None:
            pool.shutdown(wait=wait, cancel_futures=cancel_futures)
        try:
            # Raises PersistentCacheError for writes that failed and were not
            # reported by flush_disk(); the cache is closed regardless.
            self._evaluator.shutdown(wait=wait)
        finally:
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

        Raises ``RuntimeError`` when called from inside a running query. The
        returned future can outlive that query and hand its value to code
        the engine cannot see, so the read cannot be recorded as a
        dependency. Inside a query call the other query directly, or use
        :meth:`compute_many` to run several in parallel.
        """
        if self._evaluator.in_runtime():
            raise RuntimeError(
                "engine.submit() cannot be called from inside a query: the "
                "future can outlive the query, so the engine cannot record "
                "what the query read through it. Call the query directly, or "
                "use engine.compute_many() to run several in parallel."
            )
        store = self._store
        run_snapshot = store.pin_snapshot(snapshot)
        rev = run_snapshot.revision
        with store.lock:
            cancel_epoch = store.cancel_epoch

        def run() -> Any:
            return self._query_call(
                query.id,
                query._live_fn(),
                tuple(args),
                snapshot=run_snapshot,
                effects=effects,
                cancel_epoch=cancel_epoch,
            )

        try:
            pool = executor if executor is not None else self._ensure_submit_executor()
            future = pool.submit(run)
        except BaseException:
            store.unpin(rev)
            raise
        future.add_done_callback(lambda _: store.unpin(rev))
        return future

    def compute_many(
        self,
        calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]],
        *,
        workers: int | None = None,
        snapshot: Snapshot | None = None,
        effects: dict[str, list[Any]] | None = None,
    ) -> list[Any]:
        """Run several queries in parallel and return their results in order.

        Called from inside a running query, the requested queries become
        dependencies of that query, exactly as if it had called each of them
        directly: they run at the calling query's revision (``snapshot`` is
        ignored, as it is for a direct nested call), in parallel on worker
        threads, and the calling query then reads each result in call order.
        Queries with ``memoize=False`` keep no value to hand over, so they run
        on the calling thread, one after another.
        """
        if not calls:
            return []
        if self._evaluator.current_runtime() is not None:
            return self._compute_many_in_query(calls, workers, effects)
        run_snapshot = self._store.pin_snapshot(snapshot)
        try:
            return self._compute_many_pinned(calls, workers, run_snapshot, effects)
        finally:
            self._store.unpin(run_snapshot.revision)

    # --- fan-out from inside a query ---
    #
    # Worker threads have no access to the calling query's frame, so a query
    # they run cannot be recorded as its dependency from there. Instead the
    # workers only warm the cache (Evaluator.warm_for_caller) and the calling
    # query then makes an ordinary nested call per requested query. Tracking,
    # effects, errors, cycles and cancellation all come from that call, so
    # they cannot differ from a direct call.

    def _warm_jobs(
        self, calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]]
    ) -> list[tuple[int, Callable[[], None]]]:
        """One cache-warming job per distinct memoized call, with its index."""
        evaluator = self._evaluator
        runtime = evaluator.current_runtime()
        assert runtime is not None
        waiting = evaluator.caller_keys(runtime)
        jobs: list[tuple[int, Callable[[], None]]] = []
        seen: set[Any] = set()
        for idx, (query, args) in enumerate(calls):
            args = tuple(args)
            try:
                if (query.id, args) in seen:
                    continue
                seen.add((query.id, args))
            except TypeError:
                continue  # unhashable arguments: the caller's call reports it
            if not self._store.lookup_query_memoize(query.id):
                continue
            jobs.append(
                (
                    idx,
                    lambda q=query, a=args: evaluator.warm_for_caller(
                        runtime, waiting, q.id, q._live_fn(), a
                    ),
                )
            )
        return jobs

    def _call_in_query(
        self,
        query: _QueryHandle,
        args: tuple[Any, ...],
        call_effects: dict[str, list[Any]] | None,
    ) -> Any:
        """Call ``query`` as the current query would, collecting its effects.

        Effects pushed or replayed by a nested call are appended to the
        calling frame's lists, so this call's share is whatever those lists
        gained while it ran.
        """
        runtime = self._evaluator.current_runtime()
        if runtime is None or not runtime.stack:
            # The stream was handed to code outside the query that made it.
            return self._query_call(
                query.id,
                query._live_fn(),
                tuple(args),
                snapshot=None,
                effects=call_effects,
            )
        frame = runtime.stack[-1]
        before = (
            None
            if call_effects is None
            else {name: len(items) for name, items in frame.effects.items()}
        )
        value = self._query_call(query.id, query._live_fn(), tuple(args), snapshot=None)
        if before is not None and call_effects is not None:
            for name, items in frame.effects.items():
                gained = items[before.get(name, 0) :]
                if gained:
                    call_effects.setdefault(name, []).extend(gained)
        return value

    def _compute_many_in_query(
        self,
        calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]],
        workers: int | None,
        effects: dict[str, list[Any]] | None,
    ) -> list[Any]:
        jobs = self._warm_jobs(calls)
        if len(jobs) > 1:
            scheduler = WorkStealingExecutor(workers or min(32, len(jobs)))
            for slot, (_idx, job) in enumerate(jobs):
                scheduler.submit_indexed(slot, job)
            scheduler.run(len(jobs))
        results = []
        for query, args in calls:
            call_effects: dict[str, list[Any]] | None = None if effects is None else {}
            results.append(self._call_in_query(query, args, call_effects))
            if effects is not None and call_effects:
                for name, items in call_effects.items():
                    effects.setdefault(name, []).extend(items)
        return results

    def _compute_many_pinned(
        self,
        calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]],
        workers: int | None,
        run_snapshot: Snapshot,
        effects: dict[str, list[Any]] | None,
    ) -> list[Any]:
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
                    q._live_fn(),
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

        runtime = self._evaluator.current_runtime()
        if runtime is not None:
            # See compute_many: inside a query each call becomes a dependency.
            # The pin keeps the revision readable for the workers even if the
            # stream is still being consumed after the calling query ended.
            pinned = self._store.pin_snapshot(runtime.snapshot)
            try:
                yield from self._compute_many_stream_in_query(calls, workers, effects)
            finally:
                self._store.unpin(pinned.revision)
            return

        run_snapshot = self._store.pin_snapshot(snapshot)
        try:
            yield from self._compute_many_stream_pinned(
                calls, workers, run_snapshot, effects
            )
        finally:
            self._store.unpin(run_snapshot.revision)

    def _compute_many_stream_in_query(
        self,
        calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]],
        workers: int | None,
        effects: dict[str, list[Any]] | None,
    ) -> Iterator[tuple[int, Any, dict[str, list[Any]]]]:
        jobs = self._warm_jobs(calls)
        warmed = {idx for idx, _job in jobs}
        per_call_effects: list[dict[str, list[Any]]] = [{} for _ in calls]
        worker_count = workers or min(32, max(1, len(jobs)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            future_to_index = {pool.submit(job): idx for idx, job in jobs}
            # Warmed calls in completion order, then the ones that run here
            # (memoize=False, repeated calls).
            order = itertools.chain(
                (
                    future_to_index[fut]
                    for fut in concurrent.futures.as_completed(future_to_index)
                ),
                (idx for idx in range(len(calls)) if idx not in warmed),
            )
            for idx in order:
                query, args = calls[idx]
                value = self._call_in_query(
                    query, args, per_call_effects[idx] if effects is not None else None
                )
                yield (idx, value, per_call_effects[idx] if effects is not None else {})

        if effects is not None:
            for call_effects in per_call_effects:
                for name, items in call_effects.items():
                    if items:
                        effects.setdefault(name, []).extend(items)

    def _compute_many_stream_pinned(
        self,
        calls: Sequence[tuple[_QueryHandle, tuple[Any, ...]]],
        workers: int | None,
        run_snapshot: Snapshot,
        effects: dict[str, list[Any]] | None,
    ) -> Iterator[tuple[int, Any, dict[str, list[Any]]]]:
        worker_count = workers or min(32, max(1, len(calls)))

        per_call_effects: list[dict[str, list[Any]]]
        per_call_effects = [{} for _ in range(len(calls))]

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            future_to_index: dict[concurrent.futures.Future[Any], int] = {}
            for idx, (query, args) in enumerate(calls):
                fut = pool.submit(
                    self._query_call,
                    query.id,
                    query._live_fn(),
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
        """Inspect the current state of the computation graph.

        Args:
            condense: Groups nodes sharing the same base query name (ignoring arguments) into a single summary node when N >= 2.
        """
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
        condense: bool = False,
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

        If ``condense`` is True, groups nodes sharing the same base query name (ignoring arguments) into a single summary node when N >= 2.
        """
        return self._store.subgraph(roots, direction=direction, condense=condense)

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

    # --- internals ---
    def _function_id(self, fn: Callable[..., Any]) -> str:
        return f"{fn.__module__}:{fn.__qualname__}"

    def _resolve_id(self, fn: Callable[..., Any], name: str | None) -> str:
        if name is None:
            return self._function_id(fn)
        if not isinstance(name, str) or not name:
            raise TypeError("name= must be a non-empty string")
        return name

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
