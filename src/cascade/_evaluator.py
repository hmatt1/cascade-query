from __future__ import annotations

import asyncio
import atexit
import queue
import threading
import concurrent.futures
import contextvars
import dataclasses
import functools
import inspect
import time
import types
import warnings
import weakref
from typing import Any, Callable, Mapping, Sequence

from . import _canonical, _disk_cache, _keys
from ._disk_cache import DiskCacheProtocol
from ._errors import (
    CollectionBypassedError,
    CycleError,
    PersistentCacheError,
    QueryCancelled,
    SnapshotExpiredError,
    StaleHandleError,
    UnencodableValueError,
    EventLoopBlockedError,
)
from ._runtime import UNSET, RuntimeFrame, RuntimeState
from ._state import (
    Dependency,
    InFlight,
    InputVersion,
    MemoEntry,
    QueryKey,
    Snapshot,
)
from ._store import GraphStore


# Errors that steer the engine itself. They are never cached and always
# propagate, including out of dependency verification.
_CONTROL_FLOW_ERRORS = (
    QueryCancelled,
    CycleError,
    PersistentCacheError,
    SnapshotExpiredError,
    StaleHandleError,
    EventLoopBlockedError,
)


class _ReenteredQuery(BaseException):
    """A verification reached a query that is waiting for that verification.

    Internal. Raised in a shadow runtime (see ``Evaluator._refuse_reentry``)
    and caught by the verification that owns the running query, which then
    recomputes instead. A ``BaseException`` so that neither query bodies nor
    the engine's own ``except Exception`` clauses swallow it on the way.
    """

    def __init__(self, key: QueryKey) -> None:
        super().__init__(key)
        self.key = key


class _ProvisionalResult(Exception):
    """Handed to callers that joined a run whose result is only provisional.

    Internal. The run belonged to a fixed-point cycle that another thread is
    still iterating; ``heads`` are the queries whose convergence the result
    waits for. Travels through the in-flight future only.
    """

    def __init__(self, heads: frozenset[QueryKey]) -> None:
        super().__init__(heads)
        self.heads = heads


# An unencodable result is the engine refusing a value, not the body
# failing; caching it would make one bad run stick.
class _EnterThroughHead(CycleError):
    """A cycle member reached itself and should have entered through ``head``.

    Internal. Raised where a run of ``member`` would raise ``CycleError``,
    and caught where that run was started, which brings ``head`` up to date
    and looks ``member`` up again (D25). A ``CycleError``, so that anything
    in between treats it as one: never cached, retried by joined callers.
    """

    def __init__(self, message: str, member: QueryKey, head: QueryKey) -> None:
        super().__init__(message)
        self.member = member
        self.head = head


# A bypassed collection is repaired as the error is raised: the next run
# reads it as it is, so the error must not outlive the run.
_NEVER_CACHED_ERRORS = (
    *_CONTROL_FLOW_ERRORS,
    _ReenteredQuery,
    UnencodableValueError,
    CollectionBypassedError,
)

# Stands in for the ``changed_at`` of a query that has no result yet (it is
# running, or its result is provisional). Equal to no real marker, so
# whatever is compared against it counts as changed.
_NEVER_CHANGED_AT = -2


# Evaluators with a disk writer that has not been shut down. A normal
# interpreter exit drains their queues (see ``_flush_all_at_exit``). The set
# holds weak references, and the single module-level hook holds none, so
# registering here never keeps an engine alive.
_running_loop = asyncio._get_running_loop
_get_ident = threading.get_ident


def _run_coroutine_blocking(coro: Any, what: str) -> Any:
    """Run ``coro`` to completion from synchronous code.

    Synchronous code on the thread of a running event loop cannot do that:
    the coroutine needs the loop, which the wait would block. That raises
    ``EventLoopBlockedError`` at once instead of ``asyncio.run``'s
    ``RuntimeError``, which a query would otherwise cache as its result.
    """
    if _running_loop() is not None:
        coro.close()
        raise EventLoopBlockedError(
            f"{what} is async and has to run, but it was reached from "
            "synchronous code on the thread of a running event loop, where "
            "running it would block the loop. Await it from async code (an "
            "async query), or make the synchronous call from another thread, "
            "for example with `await asyncio.to_thread(...)`."
        )
    return asyncio.run(coro)


async def _wait_for_joined_run(shared: concurrent.futures.Future[Any]) -> Any:
    """Wait, in a task, for the run this caller joined, and return its result.

    Every caller that joined the run waits on ``shared``. A task must not
    await it through ``asyncio.wrap_future``: cancelling the task would then
    cancel ``shared`` for all of them, the run's owner would fail where it
    sets the result, and the other waiters would be cancelled with this one
    (D34). The task waits on a future of its own instead, which ``shared``
    settles when the run ends. A task that has stopped waiting by then, or
    whose loop is closed, is left alone.
    """
    loop = asyncio.get_running_loop()
    mine: asyncio.Future[Any] = loop.create_future()

    def settle() -> None:
        if mine.done():
            return
        exc = shared.exception()
        if exc is None:
            mine.set_result(shared.result())
        else:
            mine.set_exception(exc)

    def relay(_done: concurrent.futures.Future[Any]) -> None:
        try:
            loop.call_soon_threadsafe(settle)
        except RuntimeError:
            pass  # the loop is closed: nobody waits on it any more

    shared.add_done_callback(relay)
    return await mine


_live_disk_writers: weakref.WeakSet[Evaluator] = weakref.WeakSet()
_live_disk_writers_lock = threading.Lock()
# Writer threads still running, also those of engines already dropped (they
# drain their queue before they stop). Strong references: a writer leaves
# the set when its thread ends.
_running_writers: set[_DiskWriter] = set()


def _flush_all_at_exit() -> None:
    """Write every queued result before the interpreter exits.

    Runs from ``atexit``, when the daemon writer threads are still alive. A
    process that is killed (``os._exit``, SIGKILL, a crash) never gets here.
    """
    with _live_disk_writers_lock:
        writers = list(_running_writers)
    for writer in writers:
        try:
            writer.flush_at_exit()
        except Exception:  # pragma: no cover - never block or fail the exit
            pass


_STOP = None  # ends the writer thread; the engine closes the cache
_CLOSE = object()  # ends it after closing the cache: the engine was dropped


class _KeyBlobs:
    """Encoded argument blobs and disk entry keys of query keys, both ways.

    A key is otherwise re-encoded when it is hydrated, once per dependent
    that records it, and again by the writer thread. Keys compare by type
    (``_keys``), so 1, 1.0 and True have an entry each, as they have a memo
    each. Both caches are bounded; when one is full it starts over.
    """

    def __init__(self, store: GraphStore) -> None:
        self._store = store
        # QueryKey -> (canonical args blob, disk entry key).
        self.by_key: dict[QueryKey, tuple[bytes, bytes]] = {}
        # Reverse direction for dependency rows read back from disk.
        self.args_by_blob: dict[bytes, tuple[Any, ...]] = {}

    def limit(self) -> int:
        return 2 * self._store.max_entries + 4096

    def blobs_for(self, key: QueryKey) -> tuple[bytes, bytes]:
        """``(args_blob, entry_key)`` for ``key``; TypeError if not encodable."""
        cached = self.by_key.get(key)
        if cached is None:
            kind, fid, args = key
            args_blob = _canonical.encode(_keys.plain_args(args))
            cached = (args_blob, _disk_cache.entry_key(kind, fid, args_blob))
            if len(self.by_key) >= self.limit():
                self.by_key.clear()
            self.by_key[key] = cached
        return cached

    def args_from_blob(self, args_blob: bytes) -> Any:
        args = self.args_by_blob.get(args_blob)
        if args is None:
            args = _canonical.decode(args_blob)
            if isinstance(args, tuple):
                args = _keys.key_from_plain(args)
                if len(self.args_by_blob) >= self.limit():
                    self.args_by_blob.clear()
                self.args_by_blob[args_blob] = args
        return args


class _DiskWriter:
    """The thread that writes finished results to the disk cache in batches.

    It holds the cache, the store (for traces and counters) and the key
    blobs, but not the ``Evaluator`` or the ``Engine``: an engine dropped
    without ``shutdown()`` can be collected. A finalizer on the evaluator
    then queues ``_CLOSE``, and the thread writes what is still queued,
    closes the cache (releasing the engine's collection locks) and ends.
    """

    def __init__(
        self, disk: DiskCacheProtocol, store: GraphStore, keys: _KeyBlobs
    ) -> None:
        self.disk = disk
        self.store = store
        self.keys = keys
        self.queue: queue.Queue[Any] = queue.Queue()
        # The first error the writer met since one was last reported, and
        # how many results were lost to errors in that time.
        self.failure: BaseException | None = None
        self.failures_lost = 0
        self.failure_lock = threading.Lock()
        self.thread = threading.Thread(
            target=self._run, name="cascade-disk-writer", daemon=True
        )
        with _live_disk_writers_lock:
            _running_writers.add(self)
        self.thread.start()

    def _run(self) -> None:
        """Write queued results in batches until ``_STOP`` or ``_CLOSE`` arrives.

        The thread never dies on an error. A batch that fails (the database
        is locked by another process, the map is full, a value no longer
        encodes) is dropped, counted, and reported by the next
        ``flush_disk()`` or ``shutdown()``; the next batch is tried as usual,
        so a transient failure costs only the results it hit. The results
        stay correct in memory either way.
        """
        pending = self.queue
        try:
            while True:
                item = pending.get()
                if item is _STOP or item is _CLOSE:
                    if item is _CLOSE:
                        self._close_dropped()
                    pending.task_done()
                    break
                batch = [item]
                try:
                    for _ in range(999):
                        next_item = pending.get_nowait()
                        if next_item is _STOP or next_item is _CLOSE:
                            pending.put(next_item)
                            break
                        batch.append(next_item)
                except queue.Empty:
                    pass
                try:
                    self.write_batch(batch)
                except BaseException as exc:  # noqa: BLE001 - reported by flush_disk
                    self.note_failure(exc, len(batch))
                    for entry in batch:
                        self.store.trace_event(
                            "disk_store_failed", entry[0], detail=type(exc).__name__
                        )
                finally:
                    for _ in batch:
                        pending.task_done()
        finally:
            with _live_disk_writers_lock:
                _running_writers.discard(self)

    def _close_dropped(self) -> None:
        """The engine was dropped: close the cache, report a lost write."""
        try:
            self.disk.close()
        except Exception:  # noqa: BLE001  # pragma: no cover - best effort
            pass
        failure = self.take_failure()
        if failure is not None:
            exc, lost = failure
            warnings.warn(
                f"{lost} result(s) of an Engine dropped without shutdown() "
                f"could not be written to the persistent cache: "
                f"{type(exc).__name__}: {exc}",
                RuntimeWarning,
                stacklevel=1,
            )

    def release(self) -> None:
        """Finalizer of the evaluator: stop the writer once the queue is drained."""
        if self.thread.is_alive():
            self.queue.put(_CLOSE)
            return
        # The writer was stopped already (shutdown() closed the cache too,
        # or it was stopped by hand). Closing twice is harmless; closing
        # here could wait for a lock this thread holds, so use another.
        try:
            threading.Thread(target=self.disk.close, daemon=True).start()
        except RuntimeError:  # pragma: no cover - interpreter shutting down
            pass

    def write_batch(self, batch: list[Any]) -> None:
        entries = []
        store = self.store
        for (
            key,
            deps_records,
            memo_value,
            memo_error,
            memo_value_hash,
            fn_hash,
            computed_at_time,
            effects_node,
        ) in batch:
            kind, fid, args = key
            try:
                entry_key = self.keys.blobs_for(key)[1]
            except TypeError:  # pragma: no cover
                store.trace_event("disk_unkeyed", key)
                continue
            try:
                if memo_error is not None:
                    value_blob = _canonical.encode(memo_error)
                else:
                    value_blob = _canonical.encode(memo_value)
            except TypeError:  # pragma: no cover
                continue

            if _canonical.digest_bytes(value_blob) != memo_value_hash:
                store.trace_event(
                    "disk_skip", key, detail="value mutated before persist"
                )
                continue

            record = {
                "kind": kind,
                "id": fid,
                "fn_hash": fn_hash,
                "value_hash": memo_value_hash,
                "deps": deps_records,
                "effects": effects_node,
                "is_error": memo_error is not None,
                "computed_at_time": computed_at_time,
            }
            stored_blob: bytes | None = value_blob
            if len(value_blob) <= _disk_cache.INLINE_VALUE_MAX_BYTES:
                record["value"] = value_blob
                stored_blob = None

            try:
                record_blob = _canonical.encode(record)
            except TypeError:  # pragma: no cover
                continue

            entries.append(
                (key, (entry_key, record_blob, memo_value_hash, stored_blob))
            )

        if entries:
            self.disk.store_entry_many([entry for _, entry in entries])
            # Counted only once the write has committed.
            for key, _ in entries:
                store.trace_event("disk_store", key)
                store.record_disk_store()

    def note_failure(self, exc: BaseException, lost: int) -> None:
        with self.failure_lock:
            if self.failure is None:
                self.failure = exc
            self.failures_lost += lost

    def take_failure(self) -> tuple[BaseException, int] | None:
        with self.failure_lock:
            exc = self.failure
            if exc is None:
                return None
            lost = self.failures_lost
            self.failure = None
            self.failures_lost = 0
            return exc, lost

    def flush_at_exit(self) -> None:
        """Wait for queued writes, giving up if the thread is gone.

        At interpreter exit nothing may raise or wait for ever, so this
        returns once nothing is left to drain the queue. A write failure
        nobody was told about is printed as a warning.
        """
        pending = self.queue
        thread = self.thread
        with pending.all_tasks_done:
            while pending.unfinished_tasks:
                if not thread.is_alive():
                    return
                pending.all_tasks_done.wait(0.05)
        failure = self.take_failure()
        if failure is not None:
            exc, lost = failure
            path = _disk_path(self.disk)
            warnings.warn(
                f"{lost} result(s) could not be written to the persistent cache "
                f"at {path!r}: {type(exc).__name__}: {exc}.",
                RuntimeWarning,
                stacklevel=1,
            )


def _disk_path(disk: Any) -> str:
    try:
        return str(disk.path)
    except Exception:  # noqa: BLE001  # pragma: no cover - closed handle
        return "<closed>"


atexit.register(_flush_all_at_exit)


class Evaluator:
    def __init__(
        self,
        store: GraphStore,
        *,
        disk: DiskCacheProtocol | None = None,
        get_executor: Callable[[], concurrent.futures.Executor] | None = None,
    ) -> None:
        self._store = store
        self._disk = disk
        self._get_executor = get_executor
        self._runtime_var: contextvars.ContextVar[RuntimeState | None] = (
            contextvars.ContextVar("cascade_runtime", default=None)
        )
        # Keys currently being verified against the disk cache in this context.
        # Guards against unbounded recursion if a corrupted store ever encodes
        # a dependency cycle; real cycles are still caught by the frame stack.
        # Queries whose results this context is bringing up to date for a
        # verification (``_dependency_entry``). Stored results can depend on
        # each other in a cycle (results of a fixed-point cycle read at
        # different revisions by different threads); a walk that reaches
        # one of these again counts it as changed instead of recursing.
        self._verifying_var: contextvars.ContextVar[frozenset[QueryKey]] = (
            contextvars.ContextVar("cascade_verifying", default=frozenset())
        )
        self._hydrating_var: contextvars.ContextVar[frozenset[QueryKey]] = (
            contextvars.ContextVar("cascade_hydrating", default=frozenset())
        )
        # Queries that are running on other threads and waiting for this
        # context to finish (set by ``warm_for_caller``). Waiting for one of
        # them would never end.
        self._waiting_callers_var: contextvars.ContextVar[frozenset[QueryKey]] = (
            contextvars.ContextVar("cascade_waiting_callers", default=frozenset())
        )
        self._keys = _KeyBlobs(store)
        # Aliases kept for introspection; the dicts are cleared in place.
        self._key_blobs = self._keys.by_key
        self._args_by_blob = self._keys.args_by_blob
        self._writer: _DiskWriter | None = None
        if self._disk is not None:
            self._writer = _DiskWriter(self._disk, store, self._keys)
            with _live_disk_writers_lock:
                _live_disk_writers.add(self)
            # The writer holds no reference to this evaluator, so an engine
            # dropped without shutdown() is collected; this then stops the
            # writer once it has written what is queued, and closes the cache.
            finalizer = weakref.finalize(self, self._writer.release)
            finalizer.atexit = False

    @property
    def _disk_queue(self) -> queue.Queue[Any] | None:
        return None if self._writer is None else self._writer.queue

    @property
    def _disk_thread(self) -> threading.Thread | None:
        return None if self._writer is None else self._writer.thread

    def _run_in_runtime(self, runtime: RuntimeState, fn: Callable[[], Any]) -> Any:
        token = self._runtime_var.set(runtime)
        try:
            result = fn()
            if runtime.root_effects is not None:
                for name, values in runtime.staged_root_effects.items():
                    runtime.root_effects.setdefault(name, []).extend(values)
            return result
        finally:
            self._runtime_var.reset(token)
            pins = runtime.pins
            if pins:
                runtime.pins = None
                self._store.unpin_all(pins)

    async def _run_in_runtime_async(
        self, runtime: RuntimeState, fn: Callable[[], Any]
    ) -> Any:
        token = self._runtime_var.set(runtime)
        try:
            result = await fn()
            if runtime.root_effects is not None:
                for name, values in runtime.staged_root_effects.items():
                    runtime.root_effects.setdefault(name, []).extend(values)
            return result
        finally:
            self._runtime_var.reset(token)
            pins = runtime.pins
            if pins:
                runtime.pins = None
                self._store.unpin_all(pins)

    async def _run_sync_in_executor(
        self, fn: Callable[..., Any], args: tuple[Any, ...]
    ) -> Any:
        """Run a sync query body or input default off the event loop.

        ``run_in_executor`` does not carry context variables to the worker
        thread, so without ``copy_context`` the body would run outside the
        current runtime: its reads would record no dependencies (leaving the
        memo permanently stale) and would not see the runtime's snapshot.
        """
        loop = asyncio.get_running_loop()
        executor = self._get_executor() if self._get_executor else None
        ctx = contextvars.copy_context()
        positional, keywords = _keys.call_parts(args)
        if keywords:
            return await loop.run_in_executor(
                executor, functools.partial(ctx.run, fn, *positional, **keywords)
            )
        return await loop.run_in_executor(executor, ctx.run, fn, *positional)

    def check_cancelled(self, runtime_cancel_epoch: int | None) -> None:
        if runtime_cancel_epoch is None:
            return
        # Lock-free: a single int attribute read is atomic, and cancel_epoch
        # only ever moves forward. Called 2-3 times per recompute.
        if self._store.cancel_epoch != runtime_cancel_epoch:
            raise QueryCancelled("query cancelled because inputs changed")

    def current_runtime(self) -> RuntimeState | None:
        """The runtime of the query executing in this context, if any."""
        return self._runtime_var.get()

    def caller_keys(self, runtime: RuntimeState) -> frozenset[QueryKey]:
        """Every query that is waiting for the code running in ``runtime``."""
        return self._waiting_callers_var.get().union(f.key for f in runtime.stack)

    def warm_for_caller(
        self,
        caller: RuntimeState,
        waiting: frozenset[QueryKey],
        query_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
    ) -> None:
        """Compute a query on this thread for a query running on another.

        Fan-out inside a query (``Engine.compute_many``) works in two steps:
        worker threads call this to compute the requested queries in parallel
        at the caller's revision, then the calling query calls each of them
        itself. That second call is an ordinary nested call, usually a cache
        hit, and is what records the dependency, replays effects and raises
        errors. So nothing here needs to reach the caller, and every failure
        is dropped: the caller's own call meets it again.

        ``caller`` must stay pinned by its owner until this returns.
        """
        runtime = RuntimeState(
            snapshot=caller.snapshot,
            stack=[],
            root_effects=None,
            staged_root_effects={},
            cancel_epoch=caller.cancel_epoch,
            snapshot_explicit=caller.snapshot_explicit,
        )
        token = self._waiting_callers_var.set(waiting)
        try:
            self._run_in_runtime(
                runtime, lambda: self.query_call(query_id, fn, args, snapshot=None)
            )
        except BaseException:  # noqa: BLE001
            pass
        finally:
            self._waiting_callers_var.reset(token)

    def _refuse_wait_on_caller(self, key: QueryKey) -> None:
        """Raise instead of waiting for a query that is waiting for us.

        Only reachable from ``warm_for_caller``: the query that fanned out is
        still running on its own thread, blocked until this one finishes, so
        joining its run would deadlock. On one thread the frame stack catches
        this as a cycle; the caller's own call still does.
        """
        waiting = self._waiting_callers_var.get()
        if (
            waiting and key in waiting
        ):  # pragma: no branch - only called when _enter_wait_locked returned None
            raise CycleError(
                f"cycle detected: {self._store.key_to_str(key)} fans out to a "
                "query that depends on it"
            )

    def in_sync_body(self) -> bool:
        """True while the innermost running query is a synchronous one.

        Its body cannot await, so on an event loop's thread the handles it
        calls take the synchronous path (see ``_QueryHandle.__call__``).
        """
        runtime = self._runtime_var.get()
        if runtime is None or not runtime.stack:
            return False
        return not runtime.stack[-1].is_async_body

    def in_runtime(self) -> bool:
        """True when the current context is executing inside a query."""
        return self._runtime_var.get() is not None

    def current_query_node(self) -> str | None:
        """Node string of the innermost executing query, if any."""
        runtime = self._runtime_var.get()
        if runtime is None or not runtime.stack:
            return None
        return self._store.key_to_str(runtime.stack[-1].key)

    def current_frame_read_count(self) -> int:
        """Tracked reads made so far by the innermost frame, or -1 outside a query.

        Counts every read, also one of a dependency the frame already has, so
        a caller can tell whether code it ran in between read anything.
        """
        runtime = self._runtime_var.get()
        if runtime is None or not runtime.stack:
            return -1
        return runtime.stack[-1].reads

    def push_effect(self, name: str, item: Any) -> None:
        runtime = self._runtime_var.get()
        if runtime is None:
            raise RuntimeError("accumulator.push() must be called inside a query")
        if runtime.root_effects is not None:
            runtime.staged_root_effects.setdefault(name, []).append(item)
        for frame in runtime.stack:
            frame.effects[name].append(item)

    def replay_effects(self, effects: Mapping[str, Sequence[Any]]) -> None:
        runtime = self._runtime_var.get()
        if runtime is None:
            return
        for name, values in effects.items():
            if runtime.root_effects is not None:
                runtime.staged_root_effects.setdefault(name, []).extend(values)
            # Mirror push semantics so ancestors retain transitive effects even
            # when children are served from cache.
            for frame in runtime.stack:
                frame.effects[name].extend(values)

    def record_dependency(self, dep_key: QueryKey, observed_changed_at: int) -> None:
        runtime = self._runtime_var.get()
        if runtime is None or not runtime.stack:
            return
        frame = runtime.stack[-1]
        frame.reads += 1
        existing = frame.deps.get(dep_key)
        if existing is None or observed_changed_at > existing:
            frame.deps[dep_key] = observed_changed_at

    @staticmethod
    def _note_expiry(runtime: RuntimeState, expires_at: float) -> None:
        """The running query read a result that stops being valid at ``expires_at``."""
        if runtime.stack:
            frame = runtime.stack[-1]
            if frame.expires_at is None or expires_at < frame.expires_at:
                frame.expires_at = expires_at

    def _usable_locked(self, memo: MemoEntry, revision: int) -> bool:
        """True when ``memo`` can be used at ``revision`` without a dependency walk.

        A memo verified at this revision is still not usable once a TTL
        result beneath it has expired: only the walk finds and refreshes it.
        """
        expires_at = memo.expires_at
        if expires_at is not None and time.time() > expires_at:
            return False
        return self._store.is_current_locked(memo, revision)

    def _default_version(
        self, input_key: tuple[str, tuple[Any, ...]], revision: int, default: Any
    ) -> InputVersion:
        """The version to report for an input read by default at ``revision``.

        Normally a placeholder that says "no stored version": the read is at
        an older revision than the newest, where nothing is kept. But the
        default may have been kept since, at a newer revision, by a read
        there. A read that finds that stored version holding this same value
        reports it, so its results are still valid at the revision the
        default was kept at.
        """
        value_hash = self._store.input_digest(input_key, default)
        stored = self._store.first_version_after(input_key, revision)
        if stored is not None and stored.value_hash == value_hash:
            return stored
        return InputVersion(
            revision=revision,
            changed_at=-1,
            value_hash=value_hash,
            value=default,
        )

    def _keep_default(
        self,
        input_key: tuple[str, tuple[Any, ...]],
        revision: int,
        default: Any,
    ) -> InputVersion:
        """The version to report for a read of a never-set input at ``revision``.

        A read at the newest revision, through a snapshot of it or not,
        keeps the default as the input's value (``GraphStore.store_default``),
        so every later read at that revision sees the same value and the
        default function is not called again. That does not move
        ``engine.revision``: a read is not a change. A read at an older
        revision keeps nothing.
        """
        version = self._store.store_default(input_key, default, revision)
        if version is not None:
            return version
        # Another thread may have kept its default at this revision in the
        # meantime: report that one, as every later read will.
        version = self._store.input_version_at(input_key, revision)
        if version is not None:
            return version
        return self._default_version(input_key, revision, default)

    def read_input(
        self,
        input_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
    ) -> Any:
        runtime = self._runtime_var.get()
        if snapshot is not None:
            revision = snapshot.revision
        elif runtime is not None:
            revision = runtime.snapshot.revision
        else:
            revision = -1
        if runtime is not None:
            self.check_cancelled(runtime.cancel_epoch)
        key = ("input", input_id, args)
        if revision < 0:
            # No runtime and no snapshot: read the newest version. Doing it
            # under the lock keeps the revision and the version consistent
            # without pinning.
            with self._store.lock:
                revision = self._store.revision
                version = self._store.latest_input_version((input_id, args))
        else:
            version = self._store.input_version_at((input_id, args), revision)
        if version is None:
            default = _keys.call(fn, args)
            if isinstance(default, types.CoroutineType):
                default = _run_coroutine_blocking(
                    default, self._store.key_to_str(("input", input_id, args))
                )
            version = self._keep_default((input_id, args), revision, default)
        self.record_dependency(key, version.changed_at)
        self._store.trace_event("input_read", key)
        return version.value

    async def read_input_async(
        self,
        input_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
    ) -> Any:
        runtime = self._runtime_var.get()
        if snapshot is not None:
            revision = snapshot.revision
        elif runtime is not None:
            revision = runtime.snapshot.revision
        else:
            revision = -1
        if runtime is not None:
            self.check_cancelled(runtime.cancel_epoch)
        key = ("input", input_id, args)
        if revision < 0:
            # No runtime and no snapshot: read the newest version. Doing it
            # under the lock keeps the revision and the version consistent
            # without pinning.
            with self._store.lock:
                revision = self._store.revision
                version = self._store.latest_input_version((input_id, args))
        else:
            version = self._store.input_version_at((input_id, args), revision)
        if version is None:
            if inspect.iscoroutinefunction(fn):
                default = await _keys.call(fn, args)
            else:
                default = await self._run_sync_in_executor(fn, args)
            version = self._keep_default((input_id, args), revision, default)
        self.record_dependency(key, version.changed_at)
        self._store.trace_event("input_read", key)
        return version.value

    def query_call(
        self,
        query_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
        effects: dict[str, list[Any]] | None = None,
        cancel_epoch: int | None = None,
        need_value: bool = True,
    ) -> Any:
        runtime = self._runtime_var.get()
        if runtime is None:
            pinned = self._store.pin_snapshot(snapshot)
            root_runtime = RuntimeState(
                snapshot=pinned,
                stack=[],
                root_effects=effects,
                staged_root_effects={},
                cancel_epoch=cancel_epoch,
                snapshot_explicit=snapshot is not None,
                pins=[pinned.revision],
            )
            return self._run_in_runtime(
                root_runtime,
                lambda: self.query_call(
                    query_id,
                    fn,
                    args,
                    snapshot=snapshot,
                    effects=effects,
                    cancel_epoch=cancel_epoch,
                    need_value=need_value,
                ),
            )

        key: QueryKey = ("query", query_id, args)
        for i, frame in enumerate(runtime.stack):
            if frame.key == key:
                return self._read_cycle_guess(runtime, i, frame, key)
        self.check_cancelled(runtime.cancel_epoch)
        entry, replay_needed = self.compute_or_get_memo(
            key, fn, runtime, need_value=need_value
        )
        self.record_dependency(key, entry.changed_at)
        if entry.expires_at is not None:
            self._note_expiry(runtime, entry.expires_at)
        if entry.cycle_heads is not None:
            self._note_cycle_heads(runtime, entry.cycle_heads)
        if replay_needed:
            self.replay_effects(entry.effects)
        if entry.error is not None:
            raise entry.error
        return entry.value

    async def query_call_async(
        self,
        query_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
        effects: dict[str, list[Any]] | None = None,
        cancel_epoch: int | None = None,
        need_value: bool = True,
    ) -> Any:
        runtime = self._runtime_var.get()
        if runtime is None:
            pinned = self._store.pin_snapshot(snapshot)
            root_runtime = RuntimeState(
                snapshot=pinned,
                stack=[],
                root_effects=effects,
                staged_root_effects={},
                cancel_epoch=cancel_epoch,
                snapshot_explicit=snapshot is not None,
                is_async=True,
                pins=[pinned.revision],
            )
            return await self._run_in_runtime_async(
                root_runtime,
                lambda: self.query_call_async(
                    query_id,
                    fn,
                    args,
                    snapshot=snapshot,
                    effects=effects,
                    cancel_epoch=cancel_epoch,
                    need_value=need_value,
                ),
            )

        key: QueryKey = ("query", query_id, args)
        for i, frame in enumerate(runtime.stack):
            if frame.key == key:
                return self._read_cycle_guess(runtime, i, frame, key)
        self.check_cancelled(runtime.cancel_epoch)
        entry, replay_needed = await self.compute_or_get_memo_async(
            key, fn, runtime, need_value=need_value
        )
        self.record_dependency(key, entry.changed_at)
        if entry.expires_at is not None:
            self._note_expiry(runtime, entry.expires_at)
        if entry.cycle_heads is not None:
            self._note_cycle_heads(runtime, entry.cycle_heads)
        if replay_needed:
            self.replay_effects(entry.effects)
        if entry.error is not None:
            raise entry.error
        return entry.value

    def compute_or_get_memo(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
        *,
        need_value: bool = True,
        _routed: bool = False,
    ) -> tuple[MemoEntry, bool]:
        # Both are None outside fixed-point cycles and verification.
        provisional = runtime.provisional
        if provisional is not None:
            pending = provisional.get(key)
            if pending is not None:
                return pending, True
        if runtime.outer is not None:
            self._refuse_reentry(runtime, key)
        with self._store.lock:
            existing = self._store.memos.get(key)
            is_memoized = self._store.lookup_query_memoize(key[1])
            if existing is not None:
                self._store.touch_memo_locked(key)

                # Only memos with a TTL at or beneath them carry an expiry,
                # so everything else pays one attribute read here.
                expires_at = existing.expires_at
                if (
                    expires_at is None or time.time() <= expires_at
                ) and self._store.is_current_locked(
                    existing, runtime.snapshot.revision
                ):
                    existing.verified_at = runtime.snapshot.revision
                    self._store.trace_event("cache_hit", key)
                    if is_memoized or not need_value:
                        return existing, True

                self._store.pin_memo_verification_locked(key)

        if existing is not None:
            # The dependency walk runs WITHOUT the store lock. It can reach a
            # dependency another thread is computing and block on that
            # thread's result; the owner needs the lock to finish, so waiting
            # with it held deadlocks. The pin keeps the row from LRU eviction.
            try:
                green = self.try_mark_green(key, existing, runtime.snapshot)
            finally:
                with self._store.lock:
                    self._store.unpin_memo_verification_locked(key)
            if green:
                self._commit_green(key, existing, runtime.snapshot.revision)
                self._store.trace_event("cache_green", key)
                if is_memoized or not need_value:
                    return existing, True

        agent = _get_ident()
        with self._store.lock:
            in_flight_key = (key, runtime.snapshot.revision)
            finished = self._finished_meanwhile_locked(
                in_flight_key, existing, is_memoized or not need_value
            )
            if finished is None:
                slot, waiter_future = self._claim_in_flight_locked(
                    in_flight_key, agent, agent
                )
                if waiter_future is not None:
                    waits = self._enter_wait_locked(key, slot, agent, agent, True)

        if finished is not None:
            self._store.trace_event("cache_hit", key)
            return finished, True
        if waiter_future is not None:
            if waits is None:
                self._refuse_wait_on_caller(key)
            if not waits:
                return self._run_instead_of_waiting(key, fn, runtime, slot), False
            self._store.trace_event("dedup_wait", key)
            # The error objects come from another thread's run and are
            # shared by every caller that joined it, so the retries below
            # run outside the except clauses: an error raised there must not
            # get one of them as its context.
            pending_heads = failed = None
            try:
                result = waiter_future.result()
            except _ProvisionalResult as pending_cycle:
                pending_heads = pending_cycle.heads
            except (QueryCancelled, CycleError, _ReenteredQuery) as exc:
                if not self._retry_after(exc, runtime):
                    raise
                failed = exc
            finally:
                self._leave_wait(agent, slot)
            if pending_heads is not None:
                if self._owns_any(runtime, pending_heads):  # pragma: no cover
                    # Computed from another run's guess for a head this
                    # caller is iterating itself: run it here, on our guess.
                    # Two callers iterate one head at one revision only when
                    # the owner of the head's run gave up (a cancelled task,
                    # a wait cut short by an interrupt) while another ran it
                    # on its own stack. No test brings a thread here.
                    return self._run_instead_of_waiting(key, fn, runtime, slot), False
                self._await_cycle_heads(pending_heads, runtime)
            elif failed is not None:
                self._joined_run_failed(key, runtime, failed)
                failed = None
            else:
                return result, True
            return self.compute_or_get_memo(key, fn, runtime, need_value=need_value)

        redirect: QueryKey | None = None
        try:
            hydrated: MemoEntry | None = None
            if self._disk is not None and existing is None and is_memoized:
                hydrated = self._try_hydrate_from_disk(key, runtime)
            if hydrated is not None:
                self._release_in_flight(in_flight_key, slot, result=hydrated)
                return hydrated, True
            result = self.recompute(key, fn, runtime, routed=_routed)
        except _EnterThroughHead as exc:
            self._release_in_flight(in_flight_key, slot, exc=exc)
            if exc.member != key:
                raise
            redirect = exc.head
        except BaseException as exc:
            self._release_in_flight(in_flight_key, slot, exc=exc)
            raise
        if redirect is not None:
            # Outside the except clause, so that what this raises does not
            # carry the redirect as its context.
            self._enter_cycle_through_head(redirect, runtime)
            return self.compute_or_get_memo(
                key, fn, runtime, need_value=need_value, _routed=True
            )
        if result.cycle_heads is not None:
            # Provisional: good for this runtime only. Callers on other
            # threads that joined this run must wait for the cycle instead.
            self._release_in_flight(
                in_flight_key, slot, exc=_ProvisionalResult(result.cycle_heads)
            )
            return result, False
        self._release_in_flight(in_flight_key, slot, result=result)
        return result, False

    async def compute_or_get_memo_async(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
        *,
        need_value: bool = True,
        _routed: bool = False,
    ) -> tuple[MemoEntry, bool]:
        # Both are None outside fixed-point cycles and verification.
        provisional = runtime.provisional
        if provisional is not None:
            pending = provisional.get(key)
            if pending is not None:
                return pending, True
        if runtime.outer is not None:
            self._refuse_reentry(runtime, key)
        with self._store.lock:
            existing = self._store.memos.get(key)
            is_memoized = self._store.lookup_query_memoize(key[1])
            if existing is not None:
                self._store.touch_memo_locked(key)

                # Only memos with a TTL at or beneath them carry an expiry,
                # so everything else pays one attribute read here.
                expires_at = existing.expires_at
                if (
                    expires_at is None or time.time() <= expires_at
                ) and self._store.is_current_locked(
                    existing, runtime.snapshot.revision
                ):
                    existing.verified_at = runtime.snapshot.revision
                    self._store.trace_event("cache_hit", key)
                    if is_memoized or not need_value:
                        return existing, True
                self._store.pin_memo_verification_locked(key)

        if existing is not None:
            try:
                green = await self.try_mark_green_async(key, existing, runtime.snapshot)
            finally:
                with self._store.lock:
                    self._store.unpin_memo_verification_locked(key)
            if green:
                self._commit_green(key, existing, runtime.snapshot.revision)
                self._store.trace_event("cache_green", key)
                if is_memoized or not need_value:
                    return existing, True

        thread = threading.get_ident()
        task = asyncio.current_task()
        # A task waits by awaiting, which leaves its thread free.
        agent: Any = thread if task is None else task
        with self._store.lock:
            in_flight_key = (key, runtime.snapshot.revision)
            finished = self._finished_meanwhile_locked(
                in_flight_key, existing, is_memoized or not need_value
            )
            if finished is None:
                slot, waiter_future = self._claim_in_flight_locked(
                    in_flight_key, agent, thread
                )
                if waiter_future is not None:
                    waits = self._enter_wait_locked(
                        key, slot, agent, thread, task is None
                    )

        if finished is not None:
            self._store.trace_event("cache_hit", key)
            return finished, True
        if waiter_future is not None:
            if waits is None:
                self._refuse_wait_on_caller(key)
            if not waits:
                return await self._run_instead_of_waiting_async(key, fn, runtime), False
            self._store.trace_event("dedup_wait", key)
            # See compute_or_get_memo: retries run outside the except clauses.
            pending_heads = failed = None
            try:
                result = await _wait_for_joined_run(waiter_future)
            except _ProvisionalResult as pending_cycle:
                pending_heads = pending_cycle.heads
            except (QueryCancelled, CycleError, _ReenteredQuery) as exc:
                if not self._retry_after(exc, runtime):
                    raise
                failed = exc
            finally:
                self._leave_wait(agent, slot)
            if pending_heads is not None:
                if self._owns_any(runtime, pending_heads):
                    return (
                        await self._run_instead_of_waiting_async(key, fn, runtime),
                        False,
                    )
                await self._await_cycle_heads_async(pending_heads, runtime)
            elif failed is not None:
                self._joined_run_failed(key, runtime, failed)
                failed = None
            else:
                return result, True
            return await self.compute_or_get_memo_async(
                key, fn, runtime, need_value=need_value
            )

        redirect: QueryKey | None = None
        try:
            hydrated: MemoEntry | None = None
            if self._disk is not None and existing is None and is_memoized:
                hydrated = await self._try_hydrate_from_disk_async(key, runtime)
            if hydrated is not None:
                self._release_in_flight(in_flight_key, slot, result=hydrated)
                return hydrated, True
            result = await self.recompute_async(key, fn, runtime, routed=_routed)
        except _EnterThroughHead as exc:
            self._release_in_flight(in_flight_key, slot, exc=exc)
            if exc.member != key:
                raise
            redirect = exc.head
        except BaseException as exc:
            self._release_in_flight(in_flight_key, slot, exc=exc)
            raise
        if redirect is not None:
            await self._enter_cycle_through_head_async(redirect, runtime)
            return await self.compute_or_get_memo_async(
                key, fn, runtime, need_value=need_value, _routed=True
            )
        if result.cycle_heads is not None:
            self._release_in_flight(
                in_flight_key, slot, exc=_ProvisionalResult(result.cycle_heads)
            )
            return result, False
        self._release_in_flight(in_flight_key, slot, result=result)
        return result, False

    @staticmethod
    def _retry_after(exc: BaseException, runtime: RuntimeState) -> bool:
        """Whether a caller whose joined run raised ``exc`` runs the query itself.

        Not for a ``CycleError`` met while verifying a dependency: there it
        propagates as before, the verification counts the dependency as
        changed, and the query it verifies runs for real, where a joined
        ``CycleError`` is retried.
        """
        return not (runtime.verifying and isinstance(exc, CycleError))

    def _joined_run_failed(
        self, key: QueryKey, runtime: RuntimeState, exc: BaseException
    ) -> None:
        """The run this caller joined ended in an error that is not the caller's.

        ``QueryCancelled``: only ``submit`` cancels, and only the submitted
        call itself; the error says that call's inputs changed, not this
        caller's. A caller that cannot be cancelled (a direct call,
        ``compute_many``), or a submitted one that started after the change,
        claims the query again and runs it at its own revision. A caller
        that is cancelled itself raises its own ``QueryCancelled`` here.

        ``CycleError``: whether a cycle is an error depends on where it was
        entered (a fixed-point cycle entered through a member raises, entered
        through its head converges), so the owner's error says nothing about
        this caller, which runs the query itself and meets the cycle from its
        own stack. Never cached, so the retry cannot loop on a stored error.

        ``_ReenteredQuery``: the owner abandoned a verification that reached
        a query running in its own runtime; that says nothing about this
        caller either.
        """
        if isinstance(exc, QueryCancelled):
            self.check_cancelled(runtime.cancel_epoch)
        self._store.trace_event(
            "dedup_retry", key, detail=f"joined run raised {type(exc).__name__}"
        )

    def _enter_wait_locked(
        self,
        key: QueryKey,
        slot: InFlight,
        agent: Any,
        thread: int,
        blocking: bool,
    ) -> bool | None:
        """Register ``agent`` as waiting for ``slot``, unless that never ends.

        ``agent`` is a thread id for a synchronous caller (``blocking``) and
        an asyncio task for an async one; ``thread`` is the thread it runs
        on. Follows the chain owner -> run that owner waits for -> its owner
        and so on. Waiting never ends when the chain comes back to
        ``agent``: two threads or tasks that entered one dependency cycle
        through different queries, each running one query and waiting for
        the other's. It also never ends when a blocking caller would wait
        for a task on its own thread: that task needs the thread back to
        finish. A task that is not waiting for a run can still be stuck
        because its thread is blocked; the chain goes on through what that
        thread waits for.

        Returns True when registered (wait, then call ``_leave_wait``),
        False when waiting would never end (run the query on this stack
        instead: the cycle is then visible to it), and None when the caller
        must refuse (``_refuse_wait_on_caller``). Caller holds ``store.lock``.
        """
        waiting = self._waiting_callers_var.get()
        if waiting and key in waiting:
            return None
        waiting_on = self._store.waiting_on
        seen: set[Any] = set()
        current: InFlight | None = slot
        while current is not None:
            owner = current.owner
            if owner == agent or (blocking and current.owner_thread == thread):
                self._store.trace_event("dedup_cycle", key)
                return False
            if owner in seen:
                break  # a cycle of waits that does not include this caller
            seen.add(owner)
            nxt = waiting_on.get(owner)
            if nxt is None and owner != current.owner_thread:
                # An async owner that waits for nothing may still be stuck
                # behind a synchronous wait on its thread.
                nxt = waiting_on.get(current.owner_thread)
            current = nxt
        waiting_on[agent] = slot
        return True

    def _leave_wait(self, agent: Any, slot: InFlight) -> None:
        """Forget that ``agent`` waits for ``slot``, if it is still on record.

        A run that is released drops the records of its waiters itself
        (``_release_in_flight``). This is for a waiter that stops before
        that: a cancelled task, an interrupted thread.
        """
        with self._store.lock:
            waiting_on = self._store.waiting_on
            if waiting_on.get(agent) is slot:
                del waiting_on[agent]

    def _run_instead_of_waiting(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
        slot: InFlight,
    ) -> MemoEntry:
        """Run ``key`` on this stack because waiting for its owner never ends.

        The owner keeps its in-flight slot and finishes its own run later.
        Run here, the query reaches what is running on this stack again, so
        the cycle is handled as on one thread: the guess of a fixed-point
        head, or ``CycleError``. A synchronous query run by a task on this
        thread's event loop is simply run here too. An async one cannot be.
        """
        if slot.owner_thread == threading.get_ident() and inspect.iscoroutinefunction(
            fn
        ):
            raise EventLoopBlockedError(
                f"{self._store.key_to_str(key)} is running as a task on the "
                "event loop of this thread, and synchronous code here would "
                "wait for it while blocking that loop. Await it from async "
                "code (an async query), or make the synchronous call from "
                "another thread, for example with `await asyncio.to_thread(...)`."
            )
        return self.recompute(key, fn, runtime)

    async def _run_instead_of_waiting_async(
        self, key: QueryKey, fn: Callable[..., Any], runtime: RuntimeState
    ) -> MemoEntry:
        return await self.recompute_async(key, fn, runtime)

    def _commit_green(self, key: QueryKey, memo: MemoEntry, revision: int) -> None:
        """Record that ``memo`` was verified unchanged at ``revision``."""
        with self._store.lock:
            memo.verified_at = revision
            # The walk ran unlocked, so the row may have been dropped or
            # replaced meanwhile. ``memo`` is still a correct answer at
            # ``revision`` for this caller, but only the stored row is
            # touched or marked clean.
            if self._store.memos.get(key) is memo:
                self._store.refresh_head_clean_locked(memo, revision, key[1])
                self._store.touch_memo_locked(key)

    def _finished_meanwhile_locked(
        self,
        in_flight_key: tuple[QueryKey, int],
        seen: MemoEntry | None,
        usable: bool,
    ) -> MemoEntry | None:
        """Result another thread stored since this caller looked, if any.

        The caller read ``memos`` (finding ``seen``) and then dropped the
        lock. An owner that stored its result and released its in-flight
        slot in that gap leaves nothing to wait on, so without this check
        the caller would claim a fresh slot and run the query a second time.
        Caller must hold ``store.lock``.
        """
        key, revision = in_flight_key
        if not usable or in_flight_key in self._store.in_flight:
            return None
        memo = self._store.memos.get(key)
        if memo is None or memo is seen or memo.verified_at != revision:
            return None
        self._store.touch_memo_locked(key)
        return memo

    def _claim_in_flight_locked(
        self,
        in_flight_key: tuple[QueryKey, int],
        owner: Any = None,
        owner_thread: int | None = None,
    ) -> tuple[InFlight, concurrent.futures.Future[MemoEntry] | None]:
        """Register as owner of ``in_flight_key`` or join the existing owner.

        Returns ``(slot, None)`` for the owner and ``(slot, future)`` for a
        waiter. The Future (and the Condition/RLock it allocates) is created
        only once a second caller shows up, so uncontended computes never pay
        for it. ``owner`` (a thread id, or the asyncio task) and its thread
        are recorded for ``_enter_wait_locked``. Caller must hold
        ``store.lock``.
        """
        slot = self._store.in_flight.get(in_flight_key)
        if slot is None:
            slot = InFlight(owner, owner_thread)
            self._store.in_flight[in_flight_key] = slot
            return slot, None
        if slot.future is None:
            slot.future = concurrent.futures.Future()
        return slot, slot.future

    def _release_in_flight(
        self,
        in_flight_key: tuple[QueryKey, int],
        slot: InFlight,
        *,
        result: MemoEntry | None = None,
        exc: BaseException | None = None,
    ) -> None:
        # Unregister and read the waiter future in one critical section: a
        # waiter either attached its future before this point, or finds no slot
        # and becomes the owner of a fresh computation.
        with self._store.lock:
            if self._store.in_flight.get(in_flight_key) is slot:
                del self._store.in_flight[in_flight_key]
            future = slot.future
            if future is not None:
                # Whoever joined this run stops waiting here, not when it
                # gets round to saying so. Until then its record would tell
                # the owner of this run, asking for a query the waiter
                # runs, that the waiter still waits for it: a cycle of
                # waits that is not there, and a second run of that query
                # (D33).
                waiting_on = self._store.waiting_on
                for waiter in [a for a, s in waiting_on.items() if s is slot]:
                    del waiting_on[waiter]
        if future is None:
            return
        if exc is not None:
            future.set_exception(exc)
        else:
            future.set_result(result)  # type: ignore[arg-type]

    def try_mark_green(
        self, key: QueryKey, entry: MemoEntry, snapshot: Snapshot
    ) -> bool:
        ttl = self._store.lookup_query_ttl(key[1])
        if ttl is not None and time.time() - entry.computed_at_time > ttl:
            self._store.trace_event("cache_red_ttl", key)
            return False

        # The memo is good until its own TTL runs out or, if sooner, until
        # the first TTL result beneath it does. Dependencies are re-read
        # below, so this picks up the new expiry of one that was refreshed
        # with an equal value.
        expires_at = None if ttl is None else entry.computed_at_time + ttl
        try:
            self._settle_dependencies(key, entry, snapshot)
            for dep in entry.deps:
                dep_key = dep.key
                if dep_key[0] == "input":
                    dep_changed_at = self.dependency_changed_at(dep_key, snapshot)
                else:
                    dep_entry = self._dependency_entry(dep_key, snapshot)
                    if dep_entry is None:
                        dep_changed_at = _NEVER_CHANGED_AT
                    else:
                        dep_changed_at = dep_entry.changed_at
                        dep_expires_at = dep_entry.expires_at
                        if dep_expires_at is not None and (
                            expires_at is None or dep_expires_at < expires_at
                        ):
                            expires_at = dep_expires_at
                if dep_changed_at != dep.observed_changed_at:
                    self._store.trace_event(
                        "cache_red", key, detail=self._store.key_to_str(dep_key)
                    )
                    return False
        except CycleError:
            # Bringing a dependency up to date ran into a cycle. The walk
            # takes dependencies in recorded order, each from an empty
            # stack, so it can enter a fixed-point cycle through a member
            # where this query's own code enters it through the head (the
            # member's result was dropped, or its cycle failed last time).
            # Only a run of this query shows whether the cycle is an error.
            self._store.trace_event("cache_red", key, detail="dependency cycle")
            return False
        except _CONTROL_FLOW_ERRORS:
            raise
        except _ReenteredQuery as reentry:
            if not self._reentry_is_ours(reentry):
                raise
            # The walk reached a query that is running in this runtime, or
            # whose result here is provisional. Only a real run can tell.
            self._store.trace_event("cache_red", key, detail="dependency is running")
            return False
        except Exception:
            # A dependency now raises an error that is not cached. That is a
            # change, and whether it propagates is up to this query's own
            # code (it may catch it), so re-run the query rather than raise
            # from the check.
            self._store.trace_event("cache_red", key, detail="dependency raised")
            return False
        entry.expires_at = expires_at
        return True

    def _settle_dependencies(
        self, root: QueryKey, root_entry: MemoEntry, snapshot: Snapshot
    ) -> None:
        """Verify ``root``'s unverified query dependencies deepest-first.

        ``dependency_changed_at`` verifies a stale dependency by recursing into
        it, which costs several Python frames per level, so a long chain of
        stale memos (pinned snapshots, TTL or cycle memos below, memos loaded by
        ``load()``) overflowed the recursion limit. This explicit-stack pass
        settles each dependency only once its own dependencies are settled, so
        the nested call it makes finds everything beneath already verified and
        returns at constant depth.

        It visits exactly what the recursive walk would, in the same order: a
        memo's dependencies are examined in recorded order, and the scan stops
        at the first one whose ``changed_at`` differs, because the memo will be
        recomputed and its later dependencies may no longer be read. Anything
        it cannot handle (missing memos, fixed-point cycles, frames already
        executing) is left to the normal path in ``try_mark_green``.
        """
        store = self._store
        runtime = self._runtime_var.get()
        executing = (
            {frame.key for frame in runtime.stack} if runtime is not None else set()
        )
        # Provisional results cannot be verified against either. Usually None.
        overlay = runtime.provisional if runtime is not None else None
        revision = snapshot.revision
        # Frames are [key, memo, index of the next dependency to examine].
        stack: list[list[Any]] = [[root, root_entry, 0]]
        visiting = {root}
        settled: set[QueryKey] = set()
        while stack:
            frame = stack[-1]
            deps = frame[1].deps
            index = frame[2]
            pushed = False
            while index < len(deps):
                dep = deps[index]
                dep_key = dep.key
                if dep_key[0] == "input":
                    version = store.input_version_at((dep_key[1], dep_key[2]), revision)
                    changed_at = -1 if version is None else version.changed_at
                    if changed_at != dep.observed_changed_at:
                        break
                    index += 1
                    continue
                if overlay and dep_key in overlay:
                    break
                with store.lock:
                    dep_memo = store.memos.get(dep_key)
                    if dep_memo is None:
                        current = False
                    else:
                        # Inlined ``_usable_locked``: this loop is hot.
                        dep_expires_at = dep_memo.expires_at
                        current = (
                            dep_expires_at is None or time.time() <= dep_expires_at
                        ) and store.is_current_locked(dep_memo, revision)
                if current:
                    if dep_memo.changed_at != dep.observed_changed_at:  # type: ignore[union-attr]
                        break
                    index += 1
                    continue
                if dep_key in settled or dep_key in visiting or dep_key in executing:
                    break
                if dep_memo is None:
                    # Nothing to walk; computing it is the only way forward.
                    self._dependency_entry(dep_key, snapshot)
                    settled.add(dep_key)
                    continue
                frame[2] = index
                stack.append([dep_key, dep_memo, 0])
                visiting.add(dep_key)
                pushed = True
                break
            if pushed:
                continue
            stack.pop()
            visiting.discard(frame[0])
            if stack:
                # All of this memo's examined dependencies are now verified,
                # so verifying the memo itself does not recurse further.
                self._dependency_entry(frame[0], snapshot)
                settled.add(frame[0])

    async def try_mark_green_async(
        self, key: QueryKey, entry: MemoEntry, snapshot: Snapshot
    ) -> bool:
        ttl = self._store.lookup_query_ttl(key[1])
        if ttl is not None and time.time() - entry.computed_at_time > ttl:
            self._store.trace_event("cache_red_ttl", key)
            return False

        # See try_mark_green.
        expires_at = None if ttl is None else entry.computed_at_time + ttl
        try:
            for dep in entry.deps:
                dep_key = dep.key
                if dep_key[0] == "input":
                    dep_changed_at = self.dependency_changed_at(dep_key, snapshot)
                else:
                    dep_entry = await self._dependency_entry_async(dep_key, snapshot)
                    if dep_entry is None:
                        dep_changed_at = _NEVER_CHANGED_AT
                    else:
                        dep_changed_at = dep_entry.changed_at
                        dep_expires_at = dep_entry.expires_at
                        if dep_expires_at is not None and (
                            expires_at is None or dep_expires_at < expires_at
                        ):
                            expires_at = dep_expires_at
                if dep_changed_at != dep.observed_changed_at:
                    self._store.trace_event(
                        "cache_red", key, detail=self._store.key_to_str(dep_key)
                    )
                    return False
        except CycleError:
            self._store.trace_event("cache_red", key, detail="dependency cycle")
            return False
        except _CONTROL_FLOW_ERRORS:
            raise
        except _ReenteredQuery as reentry:
            if not self._reentry_is_ours(reentry):
                raise
            self._store.trace_event("cache_red", key, detail="dependency is running")
            return False
        except Exception:
            # See try_mark_green.
            self._store.trace_event("cache_red", key, detail="dependency raised")
            return False
        entry.expires_at = expires_at
        return True

    def _shadow_entry(
        self, key: QueryKey, fn: Callable[..., Any], *, need_value: bool
    ) -> MemoEntry:
        """Bring ``key`` up to date in the current shadow runtime; return its entry.

        Equivalent to ``query_call`` from a runtime with an empty stack, except
        that it returns the entry verified or computed at the runtime's
        snapshot, including one holding a cached error. Callers must use that entry instead of re-reading
        ``memos[key]`` afterwards: the store keeps one row per key, and a
        concurrent reader at another revision may already have replaced it.
        """
        runtime = self._runtime_var.get()
        assert runtime is not None
        self.check_cancelled(runtime.cancel_epoch)
        entry, _ = self.compute_or_get_memo(key, fn, runtime, need_value=need_value)
        # A cached error is returned, not raised: callers only compare the
        # entry's changed_at / value_hash. Raising here would throw the
        # dependency's error out of a parent that is merely being verified,
        # even when the parent's own code catches it.
        return entry

    async def _shadow_entry_async(
        self, key: QueryKey, fn: Callable[..., Any], *, need_value: bool
    ) -> MemoEntry:
        runtime = self._runtime_var.get()
        assert runtime is not None
        self.check_cancelled(runtime.cancel_epoch)
        entry, _ = await self.compute_or_get_memo_async(
            key, fn, runtime, need_value=need_value
        )
        # See _shadow_entry: cached errors are returned, not raised.
        return entry

    def dependency_changed_at(self, key: QueryKey, snapshot: Snapshot) -> int:
        kind, fid, args = key
        if kind == "input":
            version = self._store.input_version_at((fid, args), snapshot.revision)
            return -1 if version is None else version.changed_at
        entry = self._dependency_entry(key, snapshot)
        return _NEVER_CHANGED_AT if entry is None else entry.changed_at

    def _dependency_entry(
        self, key: QueryKey, snapshot: Snapshot, *, as_verification: bool = True
    ) -> MemoEntry | None:
        """The memo of query ``key`` brought up to date at ``snapshot``.

        ``as_verification`` says what the run is for. A verification gives
        up on a joined run that ended in a ``CycleError`` (``_retry_after``):
        the dependency counts as changed and the query being verified runs
        for real. ``_enter_cycle_through_head`` passes False: nothing runs
        for real after it, so the head has to get through here.

        None when the query has no settled result to compare against: it is
        running further up this context's stack, or its result here is
        provisional (part of a fixed-point cycle still being iterated).
        """
        fid = key[1]
        runtime = self._runtime_var.get()
        if runtime is not None:
            overlay = runtime.provisional
            if overlay is not None and key in overlay:
                return None
        with self._store.lock:
            memo = self._store.memos.get(key)
            if memo is not None:
                # Inlined ``_usable_locked``: called once per dependency.
                expires_at = memo.expires_at
                if (
                    expires_at is None or time.time() <= expires_at
                ) and self._store.is_current_locked(memo, snapshot.revision):
                    return memo
        fn = self._store.lookup_query(fid)
        if runtime is not None:
            for frame in runtime.stack:
                if frame.key == key:
                    return None
        verifying = self._verifying_var.get()
        if key in verifying:
            return None
        token = self._verifying_var.set(verifying | {key})
        try:
            return self._dependency_entry_shadowed(
                key, fn, snapshot, runtime, as_verification
            )
        finally:
            self._verifying_var.reset(token)

    def _dependency_entry_shadowed(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        snapshot: Snapshot,
        runtime: RuntimeState | None,
        as_verification: bool = True,
    ) -> MemoEntry:
        if runtime is None:
            self._store.pin_snapshot(snapshot)
            entry = self._run_in_runtime(
                RuntimeState(
                    snapshot=snapshot,
                    stack=[],
                    root_effects=None,
                    staged_root_effects={},
                    cancel_epoch=None,
                    snapshot_explicit=True,
                    pins=[snapshot.revision],
                    verifying=as_verification,
                ),
                lambda: self._shadow_entry(key, fn, need_value=False),
            )
        else:
            # Dependency verification should not add edges to currently executing frame.
            shadow = RuntimeState(
                snapshot=snapshot,
                stack=[],
                root_effects=None,
                staged_root_effects={},
                cancel_epoch=runtime.cancel_epoch,
                snapshot_explicit=True,
                # Inlined ``_shadow_outer``.
                outer=(
                    runtime if runtime.stack or runtime.provisional else runtime.outer
                ),
                verifying=as_verification,
            )
            entry = self._run_in_runtime(
                shadow,
                lambda: self._shadow_entry(key, fn, need_value=False),
            )

        # The entry verified at ``snapshot``, not ``memos[key]``: a reader at
        # another revision may already have replaced that row, and its
        # changed_at says nothing about this revision.
        return entry

    async def dependency_changed_at_async(
        self, key: QueryKey, snapshot: Snapshot
    ) -> int:
        kind, fid, args = key
        if kind == "input":
            version = self._store.input_version_at((fid, args), snapshot.revision)
            return -1 if version is None else version.changed_at
        entry = await self._dependency_entry_async(key, snapshot)
        return _NEVER_CHANGED_AT if entry is None else entry.changed_at

    async def _dependency_entry_async(
        self, key: QueryKey, snapshot: Snapshot, *, as_verification: bool = True
    ) -> MemoEntry | None:
        fid = key[1]
        runtime = self._runtime_var.get()
        if runtime is not None:
            overlay = runtime.provisional
            if overlay is not None and key in overlay:
                return None
        with self._store.lock:
            memo = self._store.memos.get(key)
            if memo is not None and self._usable_locked(memo, snapshot.revision):
                return memo
        fn = self._store.lookup_query(fid)
        if runtime is not None:
            for frame in runtime.stack:
                if frame.key == key:
                    return None
        verifying = self._verifying_var.get()
        if key in verifying:
            return None
        token = self._verifying_var.set(verifying | {key})
        try:
            return await self._dependency_entry_shadowed_async(
                key, fn, snapshot, runtime, as_verification
            )
        finally:
            self._verifying_var.reset(token)

    async def _dependency_entry_shadowed_async(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        snapshot: Snapshot,
        runtime: RuntimeState | None,
        as_verification: bool = True,
    ) -> MemoEntry:
        if runtime is None:
            self._store.pin_snapshot(snapshot)
            entry = await self._run_in_runtime_async(
                RuntimeState(
                    snapshot=snapshot,
                    stack=[],
                    root_effects=None,
                    staged_root_effects={},
                    cancel_epoch=None,
                    snapshot_explicit=True,
                    is_async=True,
                    pins=[snapshot.revision],
                    verifying=as_verification,
                ),
                lambda: self._shadow_entry_async(key, fn, need_value=False),
            )
        else:
            shadow = RuntimeState(
                snapshot=snapshot,
                stack=[],
                root_effects=None,
                staged_root_effects={},
                cancel_epoch=runtime.cancel_epoch,
                snapshot_explicit=True,
                is_async=True,
                outer=self._shadow_outer(runtime),
                verifying=as_verification,
            )
            entry = await self._run_in_runtime_async(
                shadow,
                lambda: self._shadow_entry_async(key, fn, need_value=False),
            )

        return entry

    # --- fixed points ---------------------------------------------------------
    #
    # A query registered with ``fixed_point=`` may reach itself. The call that
    # closes the cycle returns the current guess; the query re-runs until its
    # result equals the guess it was computed from. All of that happens at
    # the caller's revision: nothing here moves ``store.revision``.
    #
    # Results computed from a guess are *provisional*. They are kept in an
    # overlay on the runtime (``RuntimeState.provisional``), never in the
    # store, so no other reader can pick up a value that only held for an
    # intermediate guess. Each provisional entry lists the cycle heads it
    # depends on (``MemoEntry.cycle_heads``). When a head re-runs with a new
    # guess, its entries are thrown away; when it converges they move to the
    # store together with the head's own result.

    def _read_cycle_guess(
        self, runtime: RuntimeState, index: int, frame: RuntimeFrame, key: QueryKey
    ) -> Any:
        """``key`` is running at ``runtime.stack[index]`` and was called again."""
        stack = runtime.stack
        if frame.cycle_guess is UNSET:
            has_fp, fp_val = self._store.lookup_query_fixed_point(key[1])
            if not has_fp:
                cycle_start = [f.key for f in stack[index:]] + [key]
                cycle = " -> ".join(self._store.key_to_str(k) for k in cycle_start)
                message = f"cycle detected: {cycle}"
                if not frame.routed:
                    head = self._head_to_enter_through(stack, index, key)
                    if head is not None:
                        raise _EnterThroughHead(message, key, head)
                raise CycleError(message)
            frame.cycle_guess = fp_val
        frame.is_cycle_root = True
        overlay = runtime.provisional
        if overlay is None:
            overlay = {}
        # Every query between the head and this call will hold a provisional
        # result; make sure each of them can see the overlay.
        for running in stack[index:]:
            owner = running.runtime
            if owner is not None and owner.provisional is None:
                owner.provisional = overlay
        if (
            runtime.provisional is None
        ):  # pragma: no cover - stack[index:] always includes runtime's frames
            runtime.provisional = overlay
        top = stack[-1]
        # The marker is replaced by the head's real one when the cycle
        # converges; until then it must match nothing.
        top.deps[key] = _NEVER_CHANGED_AT
        top.reads += 1
        if top.cycle_heads is None:
            top.cycle_heads = {key}
        else:
            top.cycle_heads.add(key)
        return frame.cycle_guess

    def _head_to_enter_through(
        self, stack: list[RuntimeFrame], index: int, key: QueryKey
    ) -> QueryKey | None:
        """The head a run of ``key`` should have entered its cycle through.

        ``key`` runs at ``stack[index]``, has no ``fixed_point=`` and was
        reached again. That is the documented ``CycleError`` for a cycle
        entered cold through a member. But if the path back to ``key``
        passes a fixed-point head that already has a result (in memory or
        on disk), or ``key`` was last stored as a member of that head's
        cycle, a cached result of ``key`` would have been brought up to date
        through the head (its dependency on the head is verified first).
        ``key`` runs here only because its result is missing: dropped, or
        never stored in this session (a head loaded from disk stores no
        members). Entering through the head gives what the cached result
        would have (D25). Only reached on the way to a ``CycleError``.
        """
        store = self._store
        known_head = store.cycle_head_of.get(key)
        for running in stack[index + 1 :]:
            head = running.key
            if not store.lookup_query_fixed_point(head[1])[0]:
                continue
            if head == known_head or head in store.memos or self._stored_on_disk(head):
                return head
        return None

    def _stored_on_disk(self, key: QueryKey) -> bool:
        """Whether the disk cache holds a result of ``key`` from its current code."""
        disk = self._disk
        if disk is None:
            return False
        try:
            record = disk.load_entry(self._key_blobs_for(key)[1])
        except Exception:  # noqa: BLE001 - unreadable counts as absent
            return False
        if record is None:
            return False
        fn_hash = record.get("fn_hash")
        with self._store.lock:
            current = self._store.query_hashes.get(key[1])
        return fn_hash is None or current is None or fn_hash == current

    def _enter_cycle_through_head(self, head: QueryKey, runtime: RuntimeState) -> bool:
        """Bring the fixed-point head ``head`` up to date before a member runs.

        Called for a cycle member whose run reached itself through ``head``
        (see ``_head_to_enter_through``). Bringing ``head`` up to date from
        here enters the cycle through it and stores its members; the caller
        then looks the member up once more, and runs it if it is not there.

        Returns False when ``head`` runs in this runtime: the member is then
        part of that run. Errors are left for the member's own run to meet.

        The head does not run as a verification here. Its run asks for the
        member, which another caller may be running; if that run ends in a
        ``CycleError`` (it reached itself, as this caller's did), the head
        has to run the member itself. Giving up there left the head as it
        was, and this caller with a ``CycleError`` for a cycle that
        converges (D35).
        """
        if self._owns(runtime, head):
            return False
        try:
            self._dependency_entry(head, runtime.snapshot, as_verification=False)
        except CycleError:
            pass
        except _CONTROL_FLOW_ERRORS:
            raise
        except Exception:  # noqa: BLE001 - the member's own run meets it again
            pass
        return True

    async def _enter_cycle_through_head_async(
        self, head: QueryKey, runtime: RuntimeState
    ) -> bool:
        if self._owns(runtime, head):
            return False
        try:
            await self._dependency_entry_async(
                head, runtime.snapshot, as_verification=False
            )
        except CycleError:
            pass
        except _CONTROL_FLOW_ERRORS:
            raise
        except Exception:  # noqa: BLE001
            pass
        return True

    @staticmethod
    def _retract_effects(
        runtime: RuntimeState,
        ancestors: list[RuntimeFrame],
        effects: Mapping[str, list[Any]],
    ) -> None:
        """Take back what an abandoned run pushed into its callers.

        Every push or replay during a run is appended to each frame on the
        stack and to the root's staged effects, so what the run added is
        the tail of each of those lists, as long as the run's own list.
        """
        Evaluator._retract_counts(
            runtime, ancestors, {name: len(items) for name, items in effects.items()}
        )

    @staticmethod
    def _retract_counts(
        runtime: RuntimeState, frames: list[RuntimeFrame], counts: Mapping[str, int]
    ) -> None:
        """Drop the last ``counts[name]`` items of each list ``name`` was pushed to."""
        staged = (
            runtime.staged_root_effects if runtime.root_effects is not None else None
        )
        for name, count in counts.items():
            if count <= 0:
                continue
            for frame in frames:
                del frame.effects[name][-count:]
            if staged is not None:
                del staged[name][-count:]

    # --- effects pushed by code the query runs on its behalf ---------------
    #
    # A rewritten pipeline calls stage functions for some elements only; if
    # they push effects, the query's effects would depend on which elements
    # changed (D31). The pipeline brackets its work with these to notice a
    # push, take it back and evaluate the expression plainly instead.

    def effects_mark(self) -> Any:
        """Where the running query's effects stand now, or None outside a query."""
        runtime = self._runtime_var.get()
        if runtime is None or not runtime.stack:
            return None
        frame = runtime.stack[-1]
        effects = frame.effects
        lengths = (
            {name: len(items) for name, items in effects.items()} if effects else None
        )
        return runtime, frame, lengths

    @staticmethod
    def effects_pushed_since(mark: Any) -> bool:
        """Whether anything was pushed or replayed into the query since ``mark``."""
        _, frame, lengths = mark
        effects = frame.effects
        if not effects:
            return False
        if lengths is None:
            return any(effects.values())
        return any(
            len(items) != lengths.get(name, 0) for name, items in effects.items()
        )

    def retract_effects_since(self, mark: Any) -> None:
        """Take back everything pushed or replayed into the query since ``mark``."""
        runtime, frame, lengths = mark
        lengths = lengths or {}
        counts = {
            name: len(items) - lengths.get(name, 0)
            for name, items in frame.effects.items()
        }
        self._retract_counts(runtime, runtime.stack, counts)

    @staticmethod
    def _note_cycle_heads(runtime: RuntimeState, heads: frozenset[QueryKey]) -> None:
        """The running query read a provisional result that depends on ``heads``."""
        if (
            runtime.stack
        ):  # pragma: no branch - provisional results only exist inside a running query
            frame = runtime.stack[-1]
            if frame.cycle_heads is None:
                frame.cycle_heads = set(heads)
            else:
                frame.cycle_heads.update(heads)

    @staticmethod
    def _discard_provisional(runtime: RuntimeState, head: QueryKey) -> None:
        """Forget every provisional result computed from ``head``'s guess."""
        overlay = runtime.provisional
        if overlay:
            for key in [k for k, e in overlay.items() if head in e.cycle_heads]:
                del overlay[key]

    def _next_iteration(
        self,
        runtime: RuntimeState,
        frame: RuntimeFrame,
        key: QueryKey,
        result: Any,
        value_hash: str,
        error: BaseException | None,
        iterations: int,
    ) -> bool:
        """Decide whether a cycle head must run again; prepare the frame if so.

        The head is done when the run did not read its guess, raised an error
        that is cached, or produced a result equal to the guess.
        """
        if not frame.is_cycle_root or error is not None:
            return False
        if frame.cycle_guess_hash is None:
            guess = frame.cycle_guess
            try:
                frame.cycle_guess_hash = self._store.stable_hash(guess)
            except TypeError as exc:
                raise self._store.unencodable_error(
                    f"the fixed_point= guess of {self._store.key_to_str(key)} "
                    "cannot be encoded",
                    "guess",
                    guess,
                    exc,
                ) from exc
        if value_hash == frame.cycle_guess_hash:
            return False
        self._discard_provisional(runtime, key)
        limit = self._store.lookup_fixed_point_limit(key[1])
        if iterations >= limit:
            raise CycleError(
                f"fixed point of {self._store.key_to_str(key)} did not converge "
                f"after {limit} iterations: each run still returned a result "
                "different from the one before. Make the query converge, or "
                "raise the limit with max_iterations= if it only needs longer"
            )
        frame.cycle_guess = result
        frame.cycle_guess_hash = value_hash
        frame.is_cycle_root = False
        frame.deps.clear()
        # The run is abandoned: what it pushed must not reach the callers
        # above the head either, only the run that converges counts (D26).
        stack = runtime.stack
        pos = len(stack) - 1  # the head's frame is on top; compare by identity
        while (
            pos >= 0 and stack[pos] is not frame
        ):  # pragma: no branch - the head's frame is on top
            pos -= 1  # pragma: no cover
        self._retract_effects(runtime, stack[: max(pos, 0)], frame.effects)
        frame.effects.clear()
        frame.expires_at = None
        frame.cycle_heads = None
        return True

    def _refuse_reentry(self, runtime: RuntimeState, key: QueryKey) -> None:
        """Raise if ``key`` belongs to a query run that is waiting for this check.

        ``runtime`` is a shadow runtime: it brings a dependency up to date
        for a verification started by ``runtime.outer``. If the walk reaches
        a query that is running there, or one whose result there is still
        provisional, there is nothing to verify against yet. Running it
        again from here would not see the frames above, so a cycle would go
        undetected (or wait for itself). The verification is abandoned
        instead, and the memo being verified is recomputed where the cycle
        is visible.
        """
        outer = runtime.outer
        while outer is not None:
            if self._owns(outer, key):
                raise _ReenteredQuery(key)
            outer = outer.outer

    @staticmethod
    def _shadow_outer(runtime: RuntimeState) -> RuntimeState | None:
        """What a shadow runtime started from ``runtime`` records as its outer.

        ``runtime`` itself, unless it runs nothing and holds nothing
        provisional: then it can own no query, and leaving it out keeps the
        chain ``_refuse_reentry`` walks as short as the nesting of real runs.
        Nothing is added to ``runtime`` while the shadow is alive.
        """
        if runtime.stack or runtime.provisional:
            return runtime
        return runtime.outer

    @staticmethod
    def _owns(runtime: RuntimeState, key: QueryKey) -> bool:
        overlay = runtime.provisional
        if overlay is not None and key in overlay:
            return True
        for frame in runtime.stack:
            if frame.key == key:
                return True
        return False

    @classmethod
    def _owns_any(cls, runtime: RuntimeState, keys: frozenset[QueryKey]) -> bool:
        return any(cls._owns(runtime, key) for key in keys)

    def _reentry_is_ours(self, reentry: _ReenteredQuery) -> bool:
        runtime = self._runtime_var.get()
        return runtime is not None and self._owns(runtime, reentry.key)

    def _await_cycle_heads(
        self, heads: frozenset[QueryKey], runtime: RuntimeState
    ) -> None:
        """Wait for the fixed points another thread is still iterating.

        The run this caller joined produced a provisional result. Its final
        value exists once the cycle heads it depends on have converged, so
        ask for those: that joins their runs, or finds them finished.
        """
        for head in heads:
            try:
                fn = self._store.lookup_query(head[1])
                self.compute_or_get_memo(head, fn, runtime, need_value=False)
            except _CONTROL_FLOW_ERRORS:
                raise
            except Exception:  # noqa: BLE001 - the caller's own run meets it again
                pass

    async def _await_cycle_heads_async(
        self, heads: frozenset[QueryKey], runtime: RuntimeState
    ) -> None:
        for head in heads:
            try:
                fn = self._store.lookup_query(head[1])
                await self.compute_or_get_memo_async(
                    head, fn, runtime, need_value=False
                )
            except _CONTROL_FLOW_ERRORS:
                raise
            except Exception:  # noqa: BLE001
                pass

    @staticmethod
    def _inherit_failed_deps(
        stack: list[RuntimeFrame], frame: RuntimeFrame, exc: BaseException
    ) -> None:
        """Hand a failed query's dependencies to its caller.

        A query that raises an error which is not cached leaves no memo, so
        the caller cannot depend on it. If the caller catches the error and
        returns a fallback, that fallback still depends on everything the
        failed query read: without these edges it would never be recomputed
        when those inputs change.
        """
        if not stack or isinstance(exc, QueryCancelled):
            return
        executing = {f.key for f in stack}
        caller = stack[-1]
        caller_deps = caller.deps
        caller.reads += 1
        for dep_key, observed in frame.deps.items():
            if dep_key not in executing:
                caller_deps.setdefault(dep_key, observed)
        if frame.expires_at is not None and (
            caller.expires_at is None or frame.expires_at < caller.expires_at
        ):
            caller.expires_at = frame.expires_at
        if frame.cycle_heads:
            # What the failed query read still depends on the guesses of the
            # cycle heads running beneath the caller. Its own cycle, if it
            # was a head, ended with it.
            heads = frame.cycle_heads
            heads.discard(frame.key)
            if heads:
                if caller.cycle_heads is None:
                    caller.cycle_heads = set(heads)
                else:
                    caller.cycle_heads.update(heads)

    def _result_digest(self, key: QueryKey, result: Any) -> str:
        try:
            return self._store.stable_hash(result)
        except TypeError as exc:
            raise self._unencodable_result(key, result, exc) from exc

    def _fail_unencodable(
        self,
        key: QueryKey,
        frame: RuntimeFrame,
        runtime: RuntimeState,
        result: Any,
        exc: TypeError,
    ) -> TypeError:
        """A finished run's result cannot be encoded; its frame is off the stack.

        The run fails like one that raised an error that is not cached:
        nothing is stored, and a caller that catches the error still depends
        on what the run read. Returns the error to raise.
        """
        if frame.cycle_guess is not UNSET:
            self._discard_provisional(runtime, key)
        self._inherit_failed_deps(runtime.stack, frame, exc)
        return self._unencodable_result(key, result, exc)

    def _unencodable_result(
        self, key: QueryKey, result: Any, exc: TypeError
    ) -> TypeError:
        return self._store.unencodable_error(
            f"{self._store.key_to_str(key)} returned a value Cascade cannot encode",
            "result",
            result,
            exc,
        )

    def _cacheable_error_digest(self, key: QueryKey, exc: BaseException) -> str | None:
        """Digest of ``exc`` if it is to be cached as the result of ``key``.

        None when it is not cached: by the error-caching rules, or because
        its ``args`` cannot be encoded. Such an error is raised as it is,
        uncached, so the caller always sees the exception the query raised.
        """
        if not self._should_cache_error(key, exc):
            return None
        try:
            return self._store.digest_or_opaque(exc)
        except TypeError:
            self._store.trace_event(
                "error_not_cached", key, detail="exception args cannot be encoded"
            )
            return None

    def _should_cache_error(self, key: QueryKey, exc: BaseException) -> bool:
        if isinstance(exc, _NEVER_CACHED_ERRORS):
            return False
        cache_ex = self._store.lookup_query_cache_exceptions(key[1])
        if cache_ex is True:
            return isinstance(exc, Exception)
        if isinstance(cache_ex, tuple):
            return isinstance(exc, cache_ex)
        return False

    def _drop_previous_cycle_locked(self, key: QueryKey, old_memo: MemoEntry) -> None:
        """Drop what the last fixed point of ``key`` left behind.

        Those results were computed from the old fixed point and are about
        to be computed again; dropping them saves verifying each one against
        a head that is running.
        """
        for k in old_memo.cycle_nodes:
            if k != key:
                self._store.drop_memo_locked(k)

    def recompute(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
        *,
        routed: bool = True,
    ) -> MemoEntry:
        frame = RuntimeFrame(key=key, runtime=runtime, routed=routed)
        runtime.stack.append(frame)
        start = self._store.monotonic_seconds()
        self._store.trace_event("recompute_start", key)

        with self._store.lock:
            old_memo = self._store.memos.get(key)
            if old_memo is not None and old_memo.cycle_nodes:
                self._drop_previous_cycle_locked(key, old_memo)

        error: BaseException | None = None
        # Digest of the result, or of a cached error. Computed after the run
        # (see _finish_run): the common path pays for it once, outside the
        # body's exception handling.
        value_hash: str | None = None
        iterations = 1
        while True:
            try:
                self.check_cancelled(runtime.cancel_epoch)
                result = _keys.call(fn, key[2])
                if isinstance(result, types.CoroutineType):
                    # The body is a coroutine: what it calls may await.
                    frame.is_async_body = True
                    result = _run_coroutine_blocking(
                        result, self._store.key_to_str(key)
                    )
                self.check_cancelled(runtime.cancel_epoch)
                error = None
                value_hash = None
            except BaseException as exc:
                error_hash = self._cacheable_error_digest(key, exc)
                if error_hash is not None:
                    error = exc
                    result = None
                    value_hash = error_hash
                else:
                    runtime.stack.pop()
                    if frame.cycle_guess is not UNSET:
                        self._discard_provisional(runtime, key)
                    if isinstance(exc, _EnterThroughHead) and exc.member == key:
                        # This run is abandoned and the query looked up again.
                        self._retract_effects(runtime, runtime.stack, frame.effects)
                    self._inherit_failed_deps(runtime.stack, frame, exc)
                    raise

            if frame.is_cycle_root:
                try:
                    if value_hash is None:
                        value_hash = self._result_digest(key, result)
                    again = self._next_iteration(
                        runtime, frame, key, result, value_hash, error, iterations
                    )
                except (CycleError, UnencodableValueError) as exc:
                    runtime.stack.pop()
                    if isinstance(exc, UnencodableValueError):
                        self._discard_provisional(runtime, key)
                        self._inherit_failed_deps(runtime.stack, frame, exc)
                    raise
                if again:
                    iterations += 1
                    continue

            runtime.stack.pop()
            break

        return self._finish_recompute(
            key, frame, runtime, result, error, start, value_hash
        )

    async def recompute_async(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
        *,
        routed: bool = True,
    ) -> MemoEntry:
        frame = RuntimeFrame(key=key, is_async_body=True, routed=routed)
        new_runtime = RuntimeState(
            snapshot=runtime.snapshot,
            stack=runtime.stack + [frame],
            root_effects=runtime.root_effects,
            staged_root_effects=runtime.staged_root_effects,
            cancel_epoch=runtime.cancel_epoch,
            snapshot_explicit=runtime.snapshot_explicit,
            is_async=True,
            pins=runtime.pins,
            provisional=runtime.provisional,
            outer=runtime.outer,
            verifying=runtime.verifying,
        )
        frame.runtime = new_runtime
        token = self._runtime_var.set(new_runtime)
        try:
            start = self._store.monotonic_seconds()
            self._store.trace_event("recompute_start", key)

            with self._store.lock:
                old_memo = self._store.memos.get(key)
                if old_memo is not None and old_memo.cycle_nodes:
                    self._drop_previous_cycle_locked(key, old_memo)

            error: BaseException | None = None
            value_hash: str | None = None
            iterations = 1
            while True:
                try:
                    self.check_cancelled(new_runtime.cancel_epoch)
                    if inspect.iscoroutinefunction(fn):
                        result = await _keys.call(fn, key[2])
                    else:
                        result = await self._run_sync_in_executor(fn, key[2])
                    self.check_cancelled(new_runtime.cancel_epoch)
                    error = None
                    value_hash = None
                except BaseException as exc:
                    error_hash = self._cacheable_error_digest(key, exc)
                    if error_hash is not None:
                        error = exc
                        result = None
                        value_hash = error_hash
                    else:
                        if frame.cycle_guess is not UNSET:
                            self._discard_provisional(new_runtime, key)
                        if isinstance(exc, _EnterThroughHead) and exc.member == key:
                            self._retract_effects(
                                new_runtime, runtime.stack, frame.effects
                            )
                        self._inherit_failed_deps(runtime.stack, frame, exc)
                        raise

                if frame.is_cycle_root:
                    try:
                        if value_hash is None:
                            value_hash = self._result_digest(key, result)
                        again = self._next_iteration(
                            new_runtime,
                            frame,
                            key,
                            result,
                            value_hash,
                            error,
                            iterations,
                        )
                    except UnencodableValueError as exc:
                        self._discard_provisional(new_runtime, key)
                        self._inherit_failed_deps(runtime.stack, frame, exc)
                        raise
                    if again:
                        iterations += 1
                        continue

                break

            return self._finish_recompute(
                key, frame, new_runtime, result, error, start, value_hash
            )
        finally:
            self._runtime_var.reset(token)
            frame.runtime = None  # break the frame <-> runtime reference cycle

    def _finish_recompute(
        self,
        key: QueryKey,
        frame: RuntimeFrame,
        runtime: RuntimeState,
        result: Any,
        error: BaseException | None,
        start: float,
        value_hash: str | None,
    ) -> MemoEntry:
        """Turn a finished run into a memo: stored, or provisional in a cycle.

        ``value_hash`` is the digest of ``error`` if set, else of ``result``,
        or None when it is still to be computed (the common case).
        """
        store = self._store
        elapsed = store.monotonic_seconds() - start
        revision = runtime.snapshot.revision
        if value_hash is None:
            try:
                value_hash = store.digest_or_opaque(result)
            except TypeError as exc:
                raise self._fail_unencodable(key, frame, runtime, result, exc) from exc

        frozen_effects = {name: tuple(items) for name, items in frame.effects.items()}

        is_memoized = store.lookup_query_memoize(key[1])
        computed_at_time = time.time()
        expires_at = frame.expires_at
        ttl = store.lookup_query_ttl(key[1])
        if ttl is not None and (
            expires_at is None or computed_at_time + ttl < expires_at
        ):
            expires_at = computed_at_time + ttl

        deps = frame.deps
        heads = frame.cycle_heads
        members: list[tuple[QueryKey, MemoEntry]] = []
        cycle_nodes: tuple[QueryKey, ...] = ()
        if frame.cycle_guess is not UNSET:
            # This query was the head of a fixed-point cycle. Its result
            # depends on everything the cycle read from outside, so it takes
            # over the outside dependencies of every member, and drops the
            # members themselves: they depend on the head, not the reverse.
            deps.pop(key, None)
            if heads:
                heads.discard(key)
            overlay = runtime.provisional
            if overlay:
                members = [(k, e) for k, e in overlay.items() if key in e.cycle_heads]
            member_keys = {k for k, _ in members}
            for k in member_keys:
                deps.pop(k, None)
            for _, entry in members:
                for dep in entry.deps:
                    dep_key = dep.key
                    if dep_key == key or dep_key in member_keys:
                        continue
                    seen = deps.get(dep_key)
                    if seen is None or dep.observed_changed_at > seen:
                        deps[dep_key] = dep.observed_changed_at
                if (
                    entry.expires_at is not None
                    and (  # pragma: no branch - _note_expiry already propagated it
                        expires_at is None or entry.expires_at < expires_at
                    )
                ):
                    expires_at = entry.expires_at  # pragma: no cover
            cycle_nodes = (key, *member_keys)
            if error is not None:
                # A cached error is final but it is not the guess the
                # members were computed from: they cannot be kept.
                for k in member_keys:
                    del overlay[k]  # type: ignore[union-attr]
                members = []
        provisional = bool(heads)

        with store.lock:
            previous = None if provisional else store.memos.get(key)
            if previous is not None and previous.value_hash == value_hash:
                changed_at = previous.changed_at
                store.trace_event("backdate", key)
            else:
                # Inlined ``new_change_stamp_locked``.
                changed_at = store.change_stamp = store.change_stamp + 1
            store.next_access_id += 1
            full_memo = store.entry_from_runtime(
                value=result,
                value_hash=value_hash,
                changed_at=changed_at,
                verified_at=revision,
                deps=deps,
                effects=frozen_effects,
                last_access=store.next_access_id,
                computed_at_time=computed_at_time,
                cycle_nodes=cycle_nodes,
                error=error,
                expires_at=expires_at,
            )
            # Members recorded a placeholder for the head; give them the
            # marker its result really has.
            for _, entry in members:
                entry.deps = tuple(
                    Dependency(key, changed_at) if dep.key == key else dep
                    for dep in entry.deps
                )
            if provisional:
                # Depends on the guess of a cycle head further up the stack:
                # visible to this runtime only, until that head converges.
                outer_heads = frozenset(heads)  # type: ignore[arg-type]
                full_memo.cycle_heads = outer_heads
                overlay = runtime.provisional
                if (
                    overlay is None
                ):  # pragma: no cover - _handle_cycle already created the overlay
                    overlay = runtime.provisional = {}
                overlay[key] = full_memo
                for _, entry in members:
                    entry.cycle_heads = (entry.cycle_heads - {key}) | outer_heads
            else:
                if is_memoized:
                    stored_memo = full_memo
                else:
                    stored_memo = store.entry_from_runtime(
                        value=None,
                        value_hash=value_hash,
                        changed_at=changed_at,
                        verified_at=revision,
                        deps=deps,
                        effects=frozen_effects,
                        last_access=store.next_access_id,
                        computed_at_time=computed_at_time,
                        cycle_nodes=cycle_nodes,
                        error=error,
                        expires_at=expires_at,
                    )
                store.drop_memo_locked(key)
                store.memos[key] = stored_memo
                store.push_memo_lru_locked(key)
                dependents = store.dependents
                for dep_key in deps:
                    dependents[dep_key].add(key)
                store.refresh_head_clean_locked(stored_memo, revision, key[1])
                cycle_head_of = store.cycle_head_of
                for member_key, entry in members:
                    del runtime.provisional[member_key]  # type: ignore[index]
                    cycle_head_of[member_key] = key
                    entry.cycle_heads = None
                    entry.verified_at = revision
                    if not store.lookup_query_memoize(member_key[1]):
                        entry = dataclasses.replace(entry, value=None)
                    self._store_memo_locked(member_key, entry, revision)
                store.evict_if_needed_locked()
        if not provisional and self._disk is not None and is_memoized:
            self._persist_entry(key, full_memo)
        store.trace_event("recompute_done", key, detail=f"{elapsed * 1000.0:.3f}ms")
        if store.is_stats_enabled():
            store.record_query_body_time(key, elapsed)
        return full_memo

    def _store_memo_locked(self, key: QueryKey, memo: MemoEntry, revision: int) -> None:
        store = self._store
        store.drop_memo_locked(key)
        store.memos[key] = memo
        store.push_memo_lru_locked(key)
        dependents = store.dependents
        for dep in memo.deps:
            dependents[dep.key].add(key)
        store.refresh_head_clean_locked(memo, revision, key[1])

    # --- persistent disk cache ---

    def _key_cache_limit(self) -> int:
        return self._keys.limit()

    def _key_blobs_for(self, key: QueryKey) -> tuple[bytes, bytes]:
        """``(args_blob, entry_key)`` for ``key``; TypeError if not encodable."""
        return self._keys.blobs_for(key)

    def _args_from_blob(self, args_blob: bytes) -> Any:
        return self._keys.args_from_blob(args_blob)

    def _try_hydrate_from_disk(
        self, key: QueryKey, runtime: RuntimeState
    ) -> MemoEntry | None:
        disk = self._disk
        if disk is None:
            return None  # pragma: no cover
        hydrating = self._hydrating_var.get()
        if key in hydrating:
            return None  # pragma: no cover
        kind, fid, args = key
        try:
            entry_key = self._key_blobs_for(key)[1]
        except TypeError:  # pragma: no cover
            self._store.trace_event("disk_unkeyed", key)
            return None  # pragma: no cover
        record = disk.load_entry(entry_key)
        if record is None:
            self._store.trace_event("disk_miss", key)
            self._store.record_disk_miss()
            return None  # pragma: no cover

        fn_hash = record.get("fn_hash")
        with self._store.lock:
            current_fn_hash = (
                self._store.query_hashes.get(fid)
                if kind == "query"
                else self._store.input_hashes.get(fid)
            )
        if (
            fn_hash is not None
            and current_fn_hash is not None
            and fn_hash != current_fn_hash
        ):
            self._store.trace_event("disk_red", key, detail="function hash changed")
            return None  # pragma: no cover

        ttl = self._store.lookup_query_ttl(fid)
        computed_at_time = record.get("computed_at_time", 0.0)
        if ttl is not None and time.time() - computed_at_time > ttl:
            self._store.trace_event("disk_red_ttl", key)
            return None

        token = self._hydrating_var.set(hydrating | {key})
        try:
            verified = self._verify_disk_deps(key, record, runtime)
        finally:
            self._hydrating_var.reset(token)
        if verified is None:
            return None  # pragma: no cover
        observed, expires_at = verified
        if ttl is not None and (
            expires_at is None or computed_at_time + ttl < expires_at
        ):
            expires_at = computed_at_time + ttl
        value_hash = record.get("value_hash")
        if not isinstance(value_hash, str):
            return None  # pragma: no cover
        # Small values travel inside the record itself (one read per
        # hit); only larger ones live in the content-addressed blob store.
        blob = record.get("value")
        if not isinstance(blob, bytes):
            blob = disk.load_blob(value_hash)
        if blob is None or _canonical.digest_bytes(blob) != value_hash:
            self._store.trace_event(
                "disk_red", key, detail="value blob missing or corrupt"
            )
            return None  # pragma: no cover
        try:
            value = _canonical.decode(blob)
            effects_raw = record.get("effects", {})
            effects = {str(name): tuple(items) for name, items in effects_raw.items()}
            is_error = record.get("is_error", False)
        except Exception:  # pragma: no cover
            self._store.trace_event("disk_red", key, detail="record decode failed")
            return None  # pragma: no cover

        error = None
        if is_error:
            error = value
            value = None

        with self._store.lock:
            self._store.next_access_id += 1
            memo = self._store.entry_from_runtime(
                value=value,
                value_hash=value_hash,
                changed_at=self._store.new_change_stamp_locked(),
                verified_at=runtime.snapshot.revision,
                deps=observed,
                effects=effects,
                last_access=self._store.next_access_id,
                computed_at_time=computed_at_time,
                error=error,
                expires_at=expires_at,
            )
            self._store.drop_memo_locked(key)
            self._store.memos[key] = memo
            self._store.push_memo_lru_locked(key)
            for dep_key in observed:
                self._store.dependents[dep_key].add(key)
            self._store.refresh_head_clean_locked(
                memo, runtime.snapshot.revision, key[1]
            )
            self._store.evict_if_needed_locked()
        self._store.trace_event("disk_hit", key)
        self._store.record_disk_hit()
        return memo

    def _verify_disk_deps(
        self,
        key: QueryKey,
        record: dict[str, Any],
        runtime: RuntimeState,
    ) -> tuple[dict[QueryKey, int], float | None] | None:
        """Check a stored record's dependencies against the current state.

        Returns what a run would have recorded, and the earliest expiry among
        the TTL-bound results it depends on; None if any dependency differs.
        """
        deps_raw = record.get("deps")
        if not isinstance(deps_raw, list):
            return None  # pragma: no cover
        observed: dict[QueryKey, int] = {}
        expires_at: float | None = None
        for row in deps_raw:
            if not isinstance(row, list) or len(row) != 4:
                return None  # pragma: no cover
            dep_kind, dep_fid, dep_args_blob, fingerprint = row
            if not (
                isinstance(dep_kind, str)
                and isinstance(dep_fid, str)
                and isinstance(dep_args_blob, bytes)
            ):
                return None  # pragma: no cover
            try:
                dep_args = self._args_from_blob(dep_args_blob)
            except Exception:  # pragma: no cover
                return None  # pragma: no cover
            if not isinstance(dep_args, tuple):
                return None  # pragma: no cover
            dep_key: QueryKey = (dep_kind, dep_fid, dep_args)
            if dep_kind == "input":
                version = self._current_input_version(dep_fid, dep_args, runtime)
                if version is None or version.value_hash != fingerprint:
                    self._store.trace_event(
                        "disk_red", key, detail=self._store.key_to_str(dep_key)
                    )
                    return None  # pragma: no cover
                observed[dep_key] = version.changed_at
            elif dep_kind == "query":
                try:
                    state = self._current_query_state(dep_fid, dep_args, runtime)
                except CycleError:
                    # As in try_mark_green: only a real run can tell.
                    state = None
                except _CONTROL_FLOW_ERRORS:
                    raise
                except _ReenteredQuery as reentry:
                    if not self._owns(runtime, reentry.key):
                        raise
                    # The dependency is running in this runtime (a cycle):
                    # nothing to compare with, so compute instead of loading.
                    state = None
                except Exception:
                    # The dependency raises an uncached error: not a match.
                    state = None
                if state is None or state[0] != fingerprint:
                    self._store.trace_event(
                        "disk_red", key, detail=self._store.key_to_str(dep_key)
                    )
                    return None  # pragma: no cover
                observed[dep_key] = state[1]
                if state[2] is not None and (
                    expires_at is None or state[2] < expires_at
                ):
                    expires_at = state[2]
            else:
                return None  # pragma: no cover
        return observed, expires_at

    def _current_input_version(
        self,
        input_id: str,
        args: tuple[Any, ...],
        runtime: RuntimeState,
    ) -> InputVersion | None:
        input_key = (input_id, args)
        version = self._store.input_version_at(input_key, runtime.snapshot.revision)
        if version is not None:
            return version
        fn = self._store.lookup_input(input_id)
        if fn is None:
            return None
        # Pull the input to fingerprint its current content; for file-backed
        # inputs this is the re-read-and-hash step that decides freshness.
        # Verification is deliberately read-only: materializing here would
        # advance the store revision past the runtime snapshot and desync the
        # rest of the evaluation. changed_at=-1 matches the engine's existing
        # convention for never-set default inputs, so green checks stay stable.
        value = _keys.call(fn, args)
        if isinstance(value, types.CoroutineType):
            value = _run_coroutine_blocking(
                value, self._store.key_to_str(("input", input_id, args))
            )
        return InputVersion(
            revision=runtime.snapshot.revision,
            changed_at=-1,
            value_hash=self._store.stable_hash(value),
            value=value,
        )

    def _current_query_state(
        self,
        query_id: str,
        args: tuple[Any, ...],
        runtime: RuntimeState,
    ) -> tuple[str, int, float | None] | None:
        """``(value_hash, changed_at, expires_at)`` of a query brought up to date."""
        dep_key: QueryKey = ("query", query_id, args)
        try:
            fn = self._store.lookup_query(query_id)
        except KeyError:
            return None
        # Same shadow-runtime pattern as dependency_changed_at: bringing the
        # dependency up to date must not add edges to any executing frame.
        shadow = RuntimeState(
            snapshot=runtime.snapshot,
            stack=[],
            root_effects=None,
            staged_root_effects={},
            cancel_epoch=runtime.cancel_epoch,
            snapshot_explicit=runtime.snapshot_explicit,
            # Inlined ``_shadow_outer``.
            outer=(runtime if runtime.stack or runtime.provisional else runtime.outer),
            verifying=True,
        )
        entry = self._run_in_runtime(
            shadow,
            lambda: self._shadow_entry(dep_key, fn, need_value=True),
        )
        # As in dependency_changed_at: use the entry valid at this revision,
        # not whatever row is stored by now.
        return entry.value_hash, entry.changed_at, entry.expires_at

    async def _try_hydrate_from_disk_async(
        self, key: QueryKey, runtime: RuntimeState
    ) -> MemoEntry | None:
        disk = self._disk
        if disk is None:
            return None  # pragma: no cover
        hydrating = self._hydrating_var.get()
        if key in hydrating:
            return None  # pragma: no cover
        kind, fid, args = key
        try:
            entry_key = self._key_blobs_for(key)[1]
        except TypeError:  # pragma: no cover
            self._store.trace_event("disk_unkeyed", key)  # pragma: no cover
            return None  # pragma: no cover
        record = disk.load_entry(entry_key)
        if record is None:
            self._store.trace_event("disk_miss", key)  # pragma: no cover
            self._store.record_disk_miss()
            return None  # pragma: no cover

        fn_hash = record.get("fn_hash")
        with self._store.lock:
            current_fn_hash = (
                self._store.query_hashes.get(fid)
                if kind == "query"
                else self._store.input_hashes.get(fid)
            )
        if (
            fn_hash is not None
            and current_fn_hash is not None
            and fn_hash != current_fn_hash
        ):
            self._store.trace_event(
                "disk_red", key, detail="function hash changed"
            )  # pragma: no cover
            return None  # pragma: no cover

        ttl = self._store.lookup_query_ttl(fid)
        computed_at_time = record.get("computed_at_time", 0.0)
        if ttl is not None and time.time() - computed_at_time > ttl:
            self._store.trace_event("disk_red_ttl", key)
            return None

        token = self._hydrating_var.set(hydrating | {key})
        try:
            verified = await self._verify_disk_deps_async(key, record, runtime)
        finally:
            self._hydrating_var.reset(token)
        if verified is None:
            return None  # pragma: no cover
        observed, expires_at = verified
        if ttl is not None and (
            expires_at is None or computed_at_time + ttl < expires_at
        ):
            expires_at = computed_at_time + ttl
        value_hash = record.get("value_hash")
        if not isinstance(value_hash, str):  # pragma: no cover
            return None  # pragma: no cover
        # Small values travel inside the record itself (one read per
        # hit); only larger ones live in the content-addressed blob store.
        blob = record.get("value")
        if not isinstance(blob, bytes):
            blob = disk.load_blob(value_hash)
        if blob is None or _canonical.digest_bytes(blob) != value_hash:
            self._store.trace_event(
                "disk_red", key, detail="value blob missing or corrupt"
            )  # pragma: no cover
            return None  # pragma: no cover
        try:
            value = _canonical.decode(blob)
            effects_raw = record.get("effects", {})
            effects = {str(name): tuple(items) for name, items in effects_raw.items()}
            is_error = record.get("is_error", False)
        except Exception:  # pragma: no cover
            self._store.trace_event(
                "disk_red", key, detail="record decode failed"
            )  # pragma: no cover
            return None  # pragma: no cover

        error = None
        if is_error:
            error = value
            value = None

        with self._store.lock:
            self._store.next_access_id += 1
            memo = self._store.entry_from_runtime(
                value=value,
                value_hash=value_hash,
                changed_at=self._store.new_change_stamp_locked(),
                verified_at=runtime.snapshot.revision,
                deps=observed,
                effects=effects,
                last_access=self._store.next_access_id,
                computed_at_time=computed_at_time,
                error=error,
                expires_at=expires_at,
            )
            self._store.drop_memo_locked(key)
            self._store.memos[key] = memo
            self._store.push_memo_lru_locked(key)
            for dep_key in observed:
                self._store.dependents[dep_key].add(key)
            self._store.refresh_head_clean_locked(
                memo, runtime.snapshot.revision, key[1]
            )
            self._store.evict_if_needed_locked()
        self._store.trace_event("disk_hit", key)
        self._store.record_disk_hit()
        return memo

    async def _verify_disk_deps_async(
        self,
        key: QueryKey,
        record: dict[str, Any],
        runtime: RuntimeState,
    ) -> tuple[dict[QueryKey, int], float | None] | None:
        deps_raw = record.get("deps")
        if not isinstance(deps_raw, list):  # pragma: no cover
            return None  # pragma: no cover
        observed: dict[QueryKey, int] = {}
        expires_at: float | None = None
        for row in deps_raw:
            if not isinstance(row, list) or len(row) != 4:  # pragma: no cover
                return None  # pragma: no cover
            dep_kind, dep_fid, dep_args_blob, fingerprint = row
            if not (
                isinstance(dep_kind, str)
                and isinstance(dep_fid, str)
                and isinstance(dep_args_blob, bytes)
            ):
                return None  # pragma: no cover
            try:
                dep_args = self._args_from_blob(dep_args_blob)
            except Exception:  # pragma: no cover
                return None  # pragma: no cover
            if not isinstance(dep_args, tuple):  # pragma: no cover
                return None  # pragma: no cover
            dep_key: QueryKey = (dep_kind, dep_fid, dep_args)
            if dep_kind == "input":
                version = await self._current_input_version_async(
                    dep_fid, dep_args, runtime
                )
                if version is None or version.value_hash != fingerprint:
                    self._store.trace_event(
                        "disk_red", key, detail=self._store.key_to_str(dep_key)
                    )  # pragma: no cover
                    return None  # pragma: no cover
                observed[dep_key] = version.changed_at
            elif dep_kind == "query":
                try:
                    state = await self._current_query_state_async(
                        dep_fid, dep_args, runtime
                    )
                except CycleError:
                    state = None
                except _CONTROL_FLOW_ERRORS:
                    raise
                except _ReenteredQuery as reentry:
                    if not self._owns(runtime, reentry.key):
                        raise
                    state = None
                except Exception:
                    state = None
                if state is None or state[0] != fingerprint:
                    self._store.trace_event(
                        "disk_red", key, detail=self._store.key_to_str(dep_key)
                    )  # pragma: no cover
                    return None  # pragma: no cover
                observed[dep_key] = state[1]
                if state[2] is not None and (
                    expires_at is None or state[2] < expires_at
                ):
                    expires_at = state[2]
            else:
                return None  # pragma: no cover
        return observed, expires_at

    async def _current_input_version_async(
        self,
        input_id: str,
        args: tuple[Any, ...],
        runtime: RuntimeState,
    ) -> InputVersion | None:
        input_key = (input_id, args)
        version = self._store.input_version_at(input_key, runtime.snapshot.revision)
        if version is not None:
            return version
        fn = self._store.lookup_input(input_id)
        if fn is None:
            return None  # pragma: no cover

        import inspect

        if inspect.iscoroutinefunction(fn):
            value = await _keys.call(fn, args)
        else:
            value = await self._run_sync_in_executor(fn, args)

        return InputVersion(
            revision=runtime.snapshot.revision,
            changed_at=-1,
            value_hash=self._store.stable_hash(value),
            value=value,
        )

    async def _current_query_state_async(
        self,
        query_id: str,
        args: tuple[Any, ...],
        runtime: RuntimeState,
    ) -> tuple[str, int, float | None] | None:
        dep_key: QueryKey = ("query", query_id, args)
        try:
            fn = self._store.lookup_query(query_id)
        except KeyError:
            return None  # pragma: no cover

        shadow = RuntimeState(
            snapshot=runtime.snapshot,
            stack=[],
            root_effects=None,
            staged_root_effects={},
            cancel_epoch=runtime.cancel_epoch,
            snapshot_explicit=runtime.snapshot_explicit,
            is_async=True,
            outer=self._shadow_outer(runtime),
            verifying=True,
        )
        entry = await self._run_in_runtime_async(
            shadow,
            lambda: self._shadow_entry_async(dep_key, fn, need_value=True),
        )
        return entry.value_hash, entry.changed_at, entry.expires_at

    def _note_write_failure(
        self, exc: BaseException, lost: int
    ) -> None:  # pragma: no cover - unused helper
        if self._writer is not None:
            self._writer.note_failure(exc, lost)

    def _take_write_failure(self) -> tuple[BaseException, int] | None:
        return None if self._writer is None else self._writer.take_failure()

    def _disk_path(self) -> str:
        return _disk_path(self._disk)

    def _raise_write_failure(self) -> None:
        """Raise ``PersistentCacheError`` for writes that failed since the last report."""
        failure = self._take_write_failure()
        if failure is None:
            return
        exc, lost = failure
        raise PersistentCacheError(
            f"{lost} result(s) could not be written to the persistent cache at "
            f"{self._disk_path()!r}: {type(exc).__name__}: {exc}. They are "
            "still correct in memory and will be computed again by the next "
            "engine on this directory. The disk writer keeps running and "
            "tries again with the next results."
        ) from exc

    def _wait_for_writer(self) -> None:
        """Wait until the writer thread has handled every queued result.

        Raises ``PersistentCacheError`` instead of waiting for ever when the
        writer thread is not running (it does not stop on its own before
        ``shutdown``, but it can be stopped from outside).
        """
        pending = self._disk_queue
        if pending is None:
            return
        thread = self._disk_thread
        with pending.all_tasks_done:
            while pending.unfinished_tasks:
                if thread is None or not thread.is_alive():
                    raise PersistentCacheError(
                        "the disk writer of the persistent cache at "
                        f"{self._disk_path()!r} is not running; "
                        f"{pending.unfinished_tasks} queued result(s) were "
                        "not written"
                    )
                pending.all_tasks_done.wait(0.05)

    def flush_disk(self, *, report: bool = True) -> None:
        """Wait for queued results; raise ``PersistentCacheError`` if a write failed.

        With ``report=False`` write failures are forgotten instead (the
        caller is about to delete the cached results anyway).
        """
        self._wait_for_writer()
        if report:
            self._raise_write_failure()
        else:
            self._take_write_failure()

    def flush_disk_at_exit(self) -> None:
        """Wait for queued writes, giving up if the writer thread is gone.

        ``flush_disk`` raises when the writer is gone; at interpreter exit
        nothing may raise or wait for ever, so this variant returns once
        nothing is left to drain the queue. A write failure nobody was told
        about is printed as a warning.
        """
        if self._writer is not None:
            self._writer.flush_at_exit()

    def shutdown(self, wait: bool = True) -> None:
        """Stop the writer thread; raise for failed writes not reported yet."""
        if self._disk_queue is not None:
            thread = self._disk_thread
            self._disk_queue.put(_STOP)
            if wait and thread is not None:
                thread.join()
                # Joined, so nothing is left to flush at exit. Without the
                # join the queue may still hold results; stay registered.
                with _live_disk_writers_lock:
                    _live_disk_writers.discard(self)
                self._raise_write_failure()

    def _persist_entry(self, key: QueryKey, memo: MemoEntry) -> None:
        if (
            self._disk_queue is None
        ):  # pragma: no cover - only called when self._disk is not None
            return

        deps_records = self._dep_fingerprint_rows(key, memo)
        if deps_records is None:
            return

        effects_node = {name: list(items) for name, items in memo.effects.items()}
        kind, fid, _ = key
        with self._store.lock:
            fn_hash = (
                self._store.query_hashes.get(fid)
                if kind == "query"
                else self._store.input_hashes.get(fid)
            )

        self._disk_queue.put(
            (
                key,
                deps_records,
                memo.value,
                memo.error,
                memo.value_hash,
                fn_hash,
                memo.computed_at_time,
                effects_node,
            )
        )

    def _dep_fingerprint_rows(
        self, key: QueryKey, memo: MemoEntry
    ) -> list[list[Any]] | None:
        rows: list[list[Any]] = []
        for dep in memo.deps:
            dep_kind, dep_fid, dep_args = dep.key
            try:
                dep_args_blob = self._key_blobs_for(dep.key)[0]
            except TypeError:  # pragma: no cover
                self._store.trace_event(
                    "disk_unkeyed", key, detail=self._store.key_to_str(dep.key)
                )  # pragma: no cover
                return None  # pragma: no cover
            fingerprint = self._observed_dep_fingerprint(
                dep.key, dep.observed_changed_at, memo.verified_at
            )
            if fingerprint is None:
                self._store.trace_event(
                    "disk_skip", key, detail=self._store.key_to_str(dep.key)
                )
                return None  # pragma: no cover
            rows.append([dep_kind, dep_fid, dep_args_blob, fingerprint])
        return rows

    def _observed_dep_fingerprint(
        self,
        dep_key: QueryKey,
        observed_changed_at: int,
        verified_at: int,
    ) -> str | None:
        dep_kind, dep_fid, dep_args = dep_key
        if dep_kind == "query":
            with self._store.lock:
                dep_memo = self._store.memos.get(dep_key)
            if dep_memo is None or dep_memo.changed_at != observed_changed_at:
                return None  # pragma: no cover
            return dep_memo.value_hash
        input_key = (dep_fid, dep_args)
        with self._store.lock:
            try:
                version = self._store.input_version_at(input_key, verified_at)
            except SnapshotExpiredError:
                version = None
            if version is None:
                # Materialized mid-run at a revision above verified_at, or never
                # materialized at all (pull-based default read).
                version = self._store.latest_input_version(input_key)
        if version is not None:
            if version.changed_at != observed_changed_at:
                return None  # pragma: no cover
            return version.value_hash
        if observed_changed_at != -1:
            return None  # pragma: no cover
        fn = self._store.lookup_input(dep_fid)
        if fn is None:
            return None  # pragma: no cover
        try:
            return self._store.stable_hash(_keys.call(fn, dep_args))
        except TypeError:  # pragma: no cover
            return None  # pragma: no cover

    def prune_disk_cache(self, roots: list[QueryKey]) -> None:
        disk = self._disk
        if disk is None:
            return

        import collections

        wanted_entries = set()
        wanted_blobs = set()
        queue = collections.deque()

        for key in roots:
            kind, fid, args = key
            try:
                entry_key = self._key_blobs_for(key)[1]
            except TypeError:  # pragma: no cover
                continue
            ekey = entry_key
            queue.append(ekey)
            wanted_entries.add(ekey)

        while queue:
            ekey = queue.popleft()
            record = disk.load_entry(ekey)
            if record is None:
                continue

            value_hash = record.get("value_hash")
            if value_hash:
                wanted_blobs.add(value_hash)

            for dep in record.get("deps", []):
                dep_kind, dep_fid, dep_args_blob, _ = dep
                if dep_kind != "query":
                    continue
                dep_ekey = _disk_cache.entry_key(dep_kind, dep_fid, dep_args_blob)
                if dep_ekey not in wanted_entries:
                    wanted_entries.add(dep_ekey)
                    queue.append(dep_ekey)

        disk.retain(wanted_entries, wanted_blobs)
