from __future__ import annotations

import asyncio
import atexit
import queue
import threading
import concurrent.futures
import contextvars
import inspect
import time
import types
import weakref
from typing import Any, Callable, Mapping, Sequence

from . import _canonical, _disk_cache
from ._disk_cache import DiskCacheProtocol
from ._errors import (
    CycleError,
    PersistentCacheError,
    QueryCancelled,
    SnapshotExpiredError,
    StaleHandleError,
)
from ._runtime import UNSET, RuntimeFrame, RuntimeState
from ._state import InFlight, InputVersion, MemoEntry, QueryKey, Snapshot
from ._store import GraphStore


# Errors that steer the engine itself. They are never cached and always
# propagate, including out of dependency verification.
_CONTROL_FLOW_ERRORS = (
    QueryCancelled,
    CycleError,
    PersistentCacheError,
    SnapshotExpiredError,
    StaleHandleError,
)


# Evaluators with a disk writer that has not been shut down. A normal
# interpreter exit drains their queues (see ``_flush_all_at_exit``). The set
# holds weak references, and the single module-level hook holds none, so
# registering here never keeps an engine alive.
_live_disk_writers: weakref.WeakSet[Evaluator] = weakref.WeakSet()
_live_disk_writers_lock = threading.Lock()


def _flush_all_at_exit() -> None:
    """Write every queued result before the interpreter exits.

    Runs from ``atexit``, when the daemon writer threads are still alive. A
    process that is killed (``os._exit``, SIGKILL, a crash) never gets here.
    """
    with _live_disk_writers_lock:
        evaluators = list(_live_disk_writers)
    for evaluator in evaluators:
        try:
            evaluator.flush_disk_at_exit()
        except Exception:  # pragma: no cover - never block or fail the exit
            pass


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
        self._hydrating_var: contextvars.ContextVar[frozenset[QueryKey]] = (
            contextvars.ContextVar("cascade_hydrating", default=frozenset())
        )
        # Queries that are running on other threads and waiting for this
        # context to finish (set by ``warm_for_caller``). Waiting for one of
        # them would never end.
        self._waiting_callers_var: contextvars.ContextVar[frozenset[QueryKey]] = (
            contextvars.ContextVar("cascade_waiting_callers", default=frozenset())
        )
        self._disk_queue: (
            queue.Queue[
                tuple[
                    QueryKey,
                    list[list[Any]],
                    Any,
                    Any,
                    str,
                    str | None,
                    int,
                    dict[str, list[Any]],
                ]
                | None
            ]
            | None
        ) = None
        self._disk_thread: threading.Thread | None = None
        # QueryKey -> (canonical args blob, disk entry key). A key is otherwise
        # re-encoded when it is hydrated, once per dependent that records it,
        # and again by the writer thread. Keys that compare equal share an
        # entry (1, 1.0 and True are one memo key in memory as well).
        self._key_blobs: dict[QueryKey, tuple[bytes, bytes]] = {}
        # Reverse direction for dependency rows read back from disk.
        self._args_by_blob: dict[bytes, tuple[Any, ...]] = {}
        if self._disk is not None:
            self._disk_queue = queue.Queue()
            self._disk_thread = threading.Thread(target=self._disk_worker, daemon=True)
            self._disk_thread.start()
            with _live_disk_writers_lock:
                _live_disk_writers.add(self)

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
        return await loop.run_in_executor(executor, ctx.run, fn, *args)

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
        if waiting and key in waiting:
            raise CycleError(
                f"cycle detected: {self._store.key_to_str(key)} fans out to a "
                "query that depends on it"
            )

    def in_runtime(self) -> bool:
        """True when the current context is executing inside a query."""
        return self._runtime_var.get() is not None

    def current_query_node(self) -> str | None:
        """Node string of the innermost executing query, if any."""
        runtime = self._runtime_var.get()
        if runtime is None or not runtime.stack:
            return None
        return self._store.key_to_str(runtime.stack[-1].key)

    def current_frame_dep_count(self) -> int:
        """Dependency count of the innermost frame, or -1 outside a query."""
        runtime = self._runtime_var.get()
        if runtime is None or not runtime.stack:
            return -1
        return len(runtime.stack[-1].deps)

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
            snapshot_explicit = True
        elif runtime is not None:
            revision = runtime.snapshot.revision
            snapshot_explicit = runtime.snapshot_explicit
        else:
            revision = -1
            snapshot_explicit = False
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
            default = fn(*args)
            if isinstance(default, types.CoroutineType):
                default = asyncio.run(default)
            should_materialize = False
            if not snapshot_explicit:
                with self._store.lock:
                    latest = self._store.latest_input_version((input_id, args))
                    should_materialize = (
                        latest is None and revision == self._store.revision
                    )
            if should_materialize:
                self._store.set_input(input_id, args, default, bump_cancel_epoch=False)
                version = self._store.latest_input_version((input_id, args))
                if version is None:  # pragma: no cover - safety belt
                    raise RuntimeError("input version missing after initialization")
            else:
                version = InputVersion(
                    revision=revision,
                    changed_at=-1,
                    value_hash=self._store.stable_hash(default),
                    value=default,
                )
        self.record_dependency(key, version.changed_at)
        self._store.trace_event("input_read", key)
        return version.value

    async def read_input_async(  # pragma: no cover
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
            snapshot_explicit = True
        elif runtime is not None:
            revision = runtime.snapshot.revision
            snapshot_explicit = runtime.snapshot_explicit
        else:
            revision = -1
            snapshot_explicit = False
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
                default = await fn(*args)
            else:
                default = await self._run_sync_in_executor(fn, args)
            should_materialize = False
            if not snapshot_explicit:
                with self._store.lock:
                    latest = self._store.latest_input_version((input_id, args))
                    should_materialize = (
                        latest is None and revision == self._store.revision
                    )
            if should_materialize:
                self._store.set_input(input_id, args, default, bump_cancel_epoch=False)
                version = self._store.latest_input_version((input_id, args))
                if version is None:  # pragma: no cover
                    raise RuntimeError("input version missing after initialization")
            else:
                version = InputVersion(
                    revision=revision,
                    changed_at=-1,
                    value_hash=self._store.stable_hash(default),
                    value=default,
                )
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
                if frame.cycle_guess is UNSET:
                    has_fp, fp_val = self._store.lookup_query_fixed_point(query_id)
                    if not has_fp:
                        cycle_start = [f.key for f in runtime.stack[i:]] + [key]
                        cycle = " -> ".join(
                            self._store.key_to_str(k) for k in cycle_start
                        )
                        raise CycleError(f"cycle detected: {cycle}")
                    frame.cycle_guess = fp_val

                frame.is_cycle_root = True
                for f in runtime.stack[i:]:
                    frame.cycle_nodes.add(f.key)

                # Record the back-edge dependency so the calling node is properly invalidated later
                if len(runtime.stack) > 0:
                    runtime.stack[-1].deps[key] = runtime.snapshot.revision
                return frame.cycle_guess
        self.check_cancelled(runtime.cancel_epoch)
        entry, replay_needed = self.compute_or_get_memo(
            key, fn, runtime, need_value=need_value
        )
        self.record_dependency(key, entry.changed_at)
        if entry.expires_at is not None:
            self._note_expiry(runtime, entry.expires_at)
        if replay_needed:
            self.replay_effects(entry.effects)
        if entry.error is not None:
            raise entry.error
        return entry.value

    async def query_call_async(  # pragma: no cover
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
                if frame.cycle_guess is UNSET:
                    has_fp, fp_val = self._store.lookup_query_fixed_point(query_id)
                    if not has_fp:
                        cycle_start = [f.key for f in runtime.stack[i:]] + [key]
                        cycle = " -> ".join(
                            self._store.key_to_str(k) for k in cycle_start
                        )
                        raise CycleError(f"cycle detected: {cycle}")
                    frame.cycle_guess = fp_val

                frame.is_cycle_root = True
                for f in runtime.stack[i:]:
                    frame.cycle_nodes.add(f.key)

                if len(runtime.stack) > 0:
                    runtime.stack[-1].deps[key] = runtime.snapshot.revision
                return frame.cycle_guess
        self.check_cancelled(runtime.cancel_epoch)
        entry, replay_needed = await self.compute_or_get_memo_async(
            key, fn, runtime, need_value=need_value
        )
        self.record_dependency(key, entry.changed_at)
        if entry.expires_at is not None:
            self._note_expiry(runtime, entry.expires_at)
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
    ) -> tuple[MemoEntry, bool]:
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

        with self._store.lock:
            in_flight_key = (key, runtime.snapshot.revision)
            finished = self._finished_meanwhile_locked(
                in_flight_key, existing, is_memoized or not need_value
            )
            if finished is None:
                slot, waiter_future = self._claim_in_flight_locked(in_flight_key)

        if finished is not None:
            self._store.trace_event("cache_hit", key)
            return finished, True
        if waiter_future is not None:
            self._refuse_wait_on_caller(key)
            self._store.trace_event("dedup_wait", key)
            result = waiter_future.result()
            return result, True

        try:
            hydrated: MemoEntry | None = None
            if self._disk is not None and existing is None and is_memoized:
                hydrated = self._try_hydrate_from_disk(key, runtime)
            if hydrated is not None:
                self._release_in_flight(in_flight_key, slot, result=hydrated)
                return hydrated, True
            result = self.recompute(key, fn, runtime)
        except BaseException as exc:
            self._release_in_flight(in_flight_key, slot, exc=exc)
            raise
        self._release_in_flight(in_flight_key, slot, result=result)
        return result, False

    async def compute_or_get_memo_async(  # pragma: no cover
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
        *,
        need_value: bool = True,
    ) -> tuple[MemoEntry, bool]:
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

        with self._store.lock:
            in_flight_key = (key, runtime.snapshot.revision)
            finished = self._finished_meanwhile_locked(
                in_flight_key, existing, is_memoized or not need_value
            )
            if finished is None:
                slot, waiter_future = self._claim_in_flight_locked(in_flight_key)

        if finished is not None:
            self._store.trace_event("cache_hit", key)
            return finished, True
        if waiter_future is not None:
            self._refuse_wait_on_caller(key)
            self._store.trace_event("dedup_wait", key)
            result = await asyncio.wrap_future(waiter_future)
            return result, True

        try:
            hydrated: MemoEntry | None = None
            if self._disk is not None and existing is None and is_memoized:
                hydrated = await self._try_hydrate_from_disk_async(key, runtime)
            if hydrated is not None:
                self._release_in_flight(in_flight_key, slot, result=hydrated)
                return hydrated, True
            result = await self.recompute_async(key, fn, runtime)
        except BaseException as exc:
            self._release_in_flight(in_flight_key, slot, exc=exc)
            raise
        self._release_in_flight(in_flight_key, slot, result=result)
        return result, False

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
        self, in_flight_key: tuple[QueryKey, int]
    ) -> tuple[InFlight, concurrent.futures.Future[MemoEntry] | None]:
        """Register as owner of ``in_flight_key`` or join the existing owner.

        Returns ``(slot, None)`` for the owner and ``(slot, future)`` for a
        waiter. The Future (and the Condition/RLock it allocates) is created
        only once a second caller shows up, so uncontended computes never pay
        for it. Caller must hold ``store.lock``.
        """
        slot = self._store.in_flight.get(in_flight_key)
        if slot is None:
            slot = InFlight()
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
                        dep_changed_at = snapshot.revision
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
        except _CONTROL_FLOW_ERRORS:
            raise
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
                with store.lock:
                    dep_memo = store.memos.get(dep_key)
                    current = dep_memo is not None and self._usable_locked(
                        dep_memo, revision
                    )
                if current:
                    if dep_memo.changed_at != dep.observed_changed_at:  # type: ignore[union-attr]
                        break
                    index += 1
                    continue
                if dep_key in settled or dep_key in visiting or dep_key in executing:
                    break
                if dep_memo is None:
                    # Nothing to walk; computing it is the only way forward.
                    self.dependency_changed_at(dep_key, snapshot)
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
                self.dependency_changed_at(frame[0], snapshot)
                settled.add(frame[0])

    async def try_mark_green_async(
        self, key: QueryKey, entry: MemoEntry, snapshot: Snapshot
    ) -> bool:  # pragma: no cover
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
                        dep_changed_at = snapshot.revision
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
        except _CONTROL_FLOW_ERRORS:
            raise
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
    ) -> MemoEntry:  # pragma: no cover
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
        return snapshot.revision if entry is None else entry.changed_at

    def _dependency_entry(self, key: QueryKey, snapshot: Snapshot) -> MemoEntry | None:
        """The memo of query ``key`` brought up to date at ``snapshot``.

        None when the query is running further up this context's stack: it
        has no result yet.
        """
        fid = key[1]
        with self._store.lock:
            memo = self._store.memos.get(key)
            if memo is not None and self._usable_locked(memo, snapshot.revision):
                return memo
        fn = self._store.lookup_query(fid)

        runtime = self._runtime_var.get()
        if runtime is not None:
            for frame in runtime.stack:
                if frame.key == key:
                    return None

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
    ) -> int:  # pragma: no cover
        kind, fid, args = key
        if kind == "input":
            version = self._store.input_version_at((fid, args), snapshot.revision)
            return -1 if version is None else version.changed_at
        entry = await self._dependency_entry_async(key, snapshot)
        return snapshot.revision if entry is None else entry.changed_at

    async def _dependency_entry_async(
        self, key: QueryKey, snapshot: Snapshot
    ) -> MemoEntry | None:  # pragma: no cover
        fid = key[1]
        with self._store.lock:
            memo = self._store.memos.get(key)
            if memo is not None and self._usable_locked(memo, snapshot.revision):
                return memo
        fn = self._store.lookup_query(fid)

        runtime = self._runtime_var.get()
        if runtime is not None:
            for frame in runtime.stack:
                if frame.key == key:
                    return None

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
            )
            entry = await self._run_in_runtime_async(
                shadow,
                lambda: self._shadow_entry_async(key, fn, need_value=False),
            )

        return entry

    def _restart_cycle(
        self, runtime: RuntimeState, frame: RuntimeFrame, key: QueryKey
    ) -> None:
        """Move a fixed-point cycle to a fresh revision for its next iteration."""
        store = self._store
        with store.lock:
            store.revision += 1
            rev = store.pin_current_locked()
            if runtime.pins is None:
                runtime.pins = []
            runtime.pins.append(rev)
            runtime.snapshot = Snapshot(rev)
            for k in frame.cycle_nodes:
                if k != key:
                    store.drop_memo_locked(k)

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
        for dep_key, observed in frame.deps.items():
            if dep_key not in executing:
                caller_deps.setdefault(dep_key, observed)
        if frame.expires_at is not None and (
            caller.expires_at is None or frame.expires_at < caller.expires_at
        ):
            caller.expires_at = frame.expires_at

    def recompute(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
    ) -> MemoEntry:
        frame = RuntimeFrame(key=key)
        runtime.stack.append(frame)
        start = self._store.monotonic_seconds()
        self._store.trace_event("recompute_start", key)

        with self._store.lock:
            old_memo = self._store.memos.get(key)
            if old_memo is not None and old_memo.cycle_nodes:
                for k in old_memo.cycle_nodes:
                    if k != key:
                        self._store.drop_memo_locked(k)

        error: BaseException | None = None
        while True:
            try:
                self.check_cancelled(runtime.cancel_epoch)
                result = fn(*key[2])
                if isinstance(result, types.CoroutineType):
                    result = asyncio.run(result)
                self.check_cancelled(runtime.cancel_epoch)
                error = None
            except BaseException as exc:
                cache_ex = self._store.lookup_query_cache_exceptions(key[1])
                should_cache = False
                if isinstance(
                    exc,
                    (
                        QueryCancelled,
                        CycleError,
                        PersistentCacheError,
                        SnapshotExpiredError,
                        StaleHandleError,
                    ),
                ):
                    pass
                elif cache_ex is True:
                    should_cache = isinstance(exc, Exception)
                elif isinstance(cache_ex, tuple):
                    should_cache = isinstance(exc, cache_ex)

                if should_cache:
                    error = exc
                    result = None
                else:
                    runtime.stack.pop()
                    self._inherit_failed_deps(runtime.stack, frame, exc)
                    raise

            if frame.is_cycle_root:
                if error is None and self._store.stable_hash(
                    result
                ) != self._store.stable_hash(frame.cycle_guess):
                    frame.cycle_guess = result
                    frame.is_cycle_root = False
                    self._restart_cycle(runtime, frame, key)
                    frame.deps.clear()
                    frame.effects.clear()
                    frame.expires_at = None
                    continue

            runtime.stack.pop()
            break

        elapsed = self._store.monotonic_seconds() - start
        duration_ms = elapsed * 1000.0

        frozen_effects = {name: tuple(items) for name, items in frame.effects.items()}
        if error is not None:
            value_hash = self._store.stable_hash(error)
        else:
            value_hash = self._store.stable_hash(result)

        if frame.cycle_nodes:
            for k in frame.cycle_nodes:
                frame.deps.pop(k, None)

        is_memoized = self._store.lookup_query_memoize(key[1])
        computed_at_time = time.time()
        expires_at = frame.expires_at
        ttl = self._store.lookup_query_ttl(key[1])
        if ttl is not None and (
            expires_at is None or computed_at_time + ttl < expires_at
        ):
            expires_at = computed_at_time + ttl
        with self._store.lock:
            previous = self._store.memos.get(key)
            if previous is not None and previous.value_hash == value_hash:
                changed_at = previous.changed_at
                self._store.trace_event("backdate", key)
            else:
                changed_at = self._store.new_change_stamp_locked()
            self._store.next_access_id += 1
            full_memo = self._store.entry_from_runtime(
                value=result,
                value_hash=value_hash,
                changed_at=changed_at,
                verified_at=runtime.snapshot.revision,
                deps=frame.deps,
                effects=frozen_effects,
                last_access=self._store.next_access_id,
                computed_at_time=computed_at_time,
                cycle_nodes=tuple(frame.cycle_nodes),
                error=error,
                expires_at=expires_at,
            )
            if is_memoized:
                stored_memo = full_memo
            else:
                stored_memo = self._store.entry_from_runtime(
                    value=None,
                    value_hash=value_hash,
                    changed_at=changed_at,
                    verified_at=runtime.snapshot.revision,
                    deps=frame.deps,
                    effects=frozen_effects,
                    last_access=self._store.next_access_id,
                    computed_at_time=computed_at_time,
                    cycle_nodes=tuple(frame.cycle_nodes),
                    error=error,
                    expires_at=expires_at,
                )
            self._store.drop_memo_locked(key)
            self._store.memos[key] = stored_memo
            self._store.push_memo_lru_locked(key)
            for dep_key in frame.deps.keys():
                self._store.dependents[dep_key].add(key)
            self._store.refresh_head_clean_locked(
                stored_memo, runtime.snapshot.revision, key[1]
            )
            self._store.evict_if_needed_locked()
        if self._disk is not None and is_memoized:
            self._persist_entry(key, full_memo)
        self._store.trace_event("recompute_done", key, detail=f"{duration_ms:.3f}ms")
        if self._store.is_stats_enabled():
            self._store.record_query_body_time(key, elapsed)
        return full_memo

    async def recompute_async(  # pragma: no cover
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
    ) -> MemoEntry:
        frame = RuntimeFrame(key=key)
        new_runtime = RuntimeState(
            snapshot=runtime.snapshot,
            stack=runtime.stack + [frame],
            root_effects=runtime.root_effects,
            staged_root_effects=runtime.staged_root_effects,
            cancel_epoch=runtime.cancel_epoch,
            snapshot_explicit=runtime.snapshot_explicit,
            is_async=True,
            pins=runtime.pins,
        )
        token = self._runtime_var.set(new_runtime)
        try:
            start = self._store.monotonic_seconds()
            self._store.trace_event("recompute_start", key)

            with self._store.lock:
                old_memo = self._store.memos.get(key)
                if old_memo is not None and old_memo.cycle_nodes:
                    for k in old_memo.cycle_nodes:
                        if k != key:
                            self._store.drop_memo_locked(k)

            error: BaseException | None = None
            while True:
                try:
                    self.check_cancelled(new_runtime.cancel_epoch)
                    if inspect.iscoroutinefunction(fn):
                        result = await fn(*key[2])
                    else:
                        result = await self._run_sync_in_executor(fn, key[2])
                    self.check_cancelled(new_runtime.cancel_epoch)
                    error = None
                except BaseException as exc:
                    cache_ex = self._store.lookup_query_cache_exceptions(key[1])
                    should_cache = False
                    if isinstance(
                        exc,
                        (
                            QueryCancelled,
                            CycleError,
                            PersistentCacheError,
                            SnapshotExpiredError,
                            StaleHandleError,
                        ),
                    ):
                        pass
                    elif cache_ex is True:
                        should_cache = isinstance(exc, Exception)
                    elif isinstance(cache_ex, tuple):
                        should_cache = isinstance(exc, cache_ex)

                    if should_cache:
                        error = exc
                        result = None
                    else:
                        self._inherit_failed_deps(runtime.stack, frame, exc)
                        raise

                if frame.is_cycle_root:
                    if error is None and self._store.stable_hash(
                        result
                    ) != self._store.stable_hash(frame.cycle_guess):
                        frame.cycle_guess = result
                        frame.is_cycle_root = False
                        self._restart_cycle(new_runtime, frame, key)
                        frame.deps.clear()
                        frame.effects.clear()
                        frame.expires_at = None
                        continue

                break

            elapsed = self._store.monotonic_seconds() - start
            duration_ms = elapsed * 1000.0

            frozen_effects = {
                name: tuple(items) for name, items in frame.effects.items()
            }
            if error is not None:
                value_hash = self._store.stable_hash(error)
            else:
                value_hash = self._store.stable_hash(result)

            if frame.cycle_nodes:
                for k in frame.cycle_nodes:
                    frame.deps.pop(k, None)

            is_memoized = self._store.lookup_query_memoize(key[1])
            computed_at_time = time.time()
            expires_at = frame.expires_at
            ttl = self._store.lookup_query_ttl(key[1])
            if ttl is not None and (
                expires_at is None or computed_at_time + ttl < expires_at
            ):
                expires_at = computed_at_time + ttl
            with self._store.lock:
                previous = self._store.memos.get(key)
                if previous is not None and previous.value_hash == value_hash:
                    changed_at = previous.changed_at
                    self._store.trace_event("backdate", key)
                else:
                    changed_at = self._store.new_change_stamp_locked()
                self._store.next_access_id += 1
                full_memo = self._store.entry_from_runtime(
                    value=result,
                    value_hash=value_hash,
                    changed_at=changed_at,
                    verified_at=new_runtime.snapshot.revision,
                    deps=frame.deps,
                    effects=frozen_effects,
                    last_access=self._store.next_access_id,
                    computed_at_time=computed_at_time,
                    cycle_nodes=tuple(frame.cycle_nodes),
                    error=error,
                    expires_at=expires_at,
                )
                if is_memoized:
                    stored_memo = full_memo
                else:
                    stored_memo = self._store.entry_from_runtime(
                        value=None,
                        value_hash=value_hash,
                        changed_at=changed_at,
                        verified_at=new_runtime.snapshot.revision,
                        deps=frame.deps,
                        effects=frozen_effects,
                        last_access=self._store.next_access_id,
                        computed_at_time=computed_at_time,
                        cycle_nodes=tuple(frame.cycle_nodes),
                        error=error,
                        expires_at=expires_at,
                    )
                self._store.drop_memo_locked(key)
                self._store.memos[key] = stored_memo
                self._store.push_memo_lru_locked(key)
                for dep_key in frame.deps.keys():
                    self._store.dependents[dep_key].add(key)
                self._store.refresh_head_clean_locked(
                    stored_memo, new_runtime.snapshot.revision, key[1]
                )
                self._store.evict_if_needed_locked()
            if self._disk is not None and is_memoized:
                self._persist_entry(key, full_memo)
            self._store.trace_event(
                "recompute_done", key, detail=f"{duration_ms:.3f}ms"
            )
            if self._store.is_stats_enabled():
                self._store.record_query_body_time(key, elapsed)
            return full_memo
        finally:
            runtime.snapshot = new_runtime.snapshot
            runtime.pins = new_runtime.pins
            self._runtime_var.reset(token)

    # --- persistent disk cache ---

    def _key_cache_limit(self) -> int:
        return 2 * self._store.max_entries + 4096

    def _key_blobs_for(self, key: QueryKey) -> tuple[bytes, bytes]:
        """``(args_blob, entry_key)`` for ``key``; TypeError if not encodable."""
        cached = self._key_blobs.get(key)
        if cached is None:
            kind, fid, args = key
            args_blob = _canonical.encode(args)
            cached = (args_blob, _disk_cache.entry_key(kind, fid, args_blob))
            if len(self._key_blobs) >= self._key_cache_limit():
                self._key_blobs.clear()
            self._key_blobs[key] = cached
        return cached

    def _args_from_blob(self, args_blob: bytes) -> Any:
        args = self._args_by_blob.get(args_blob)
        if args is None:
            args = _canonical.decode(args_blob)
            if isinstance(args, tuple):
                if len(self._args_by_blob) >= self._key_cache_limit():
                    self._args_by_blob.clear()
                self._args_by_blob[args_blob] = args
        return args

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
                except _CONTROL_FLOW_ERRORS:
                    raise
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
        value = fn(*args)
        import types

        if isinstance(value, types.CoroutineType):
            import asyncio

            value = asyncio.run(value)
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
    ) -> MemoEntry | None:  # pragma: no cover
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

    async def _verify_disk_deps_async(  # pragma: no cover
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
                except _CONTROL_FLOW_ERRORS:
                    raise
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

    async def _current_input_version_async(  # pragma: no cover
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
            value = await fn(*args)
        else:
            value = await self._run_sync_in_executor(fn, args)

        return InputVersion(
            revision=runtime.snapshot.revision,
            changed_at=-1,
            value_hash=self._store.stable_hash(value),
            value=value,
        )

    async def _current_query_state_async(  # pragma: no cover
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
        )
        entry = await self._run_in_runtime_async(
            shadow,
            lambda: self._shadow_entry_async(dep_key, fn, need_value=True),
        )
        return entry.value_hash, entry.changed_at, entry.expires_at

    def _disk_worker(self) -> None:
        if self._disk is None or self._disk_queue is None:
            return  # pragma: no cover
        while True:
            item = self._disk_queue.get()
            if item is None:
                self._disk_queue.task_done()
                break
            batch = [item]
            try:
                for _ in range(999):
                    next_item = self._disk_queue.get_nowait()
                    if next_item is None:
                        self._disk_queue.put(None)
                        break
                    batch.append(next_item)
            except queue.Empty:
                pass

            entries = []
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
                    entry_key = self._key_blobs_for(key)[1]
                except TypeError:  # pragma: no cover
                    self._store.trace_event("disk_unkeyed", key)
                    continue
                try:
                    if memo_error is not None:
                        value_blob = _canonical.encode(memo_error)
                    else:
                        value_blob = _canonical.encode(memo_value)
                except TypeError:  # pragma: no cover
                    continue

                if _canonical.digest_bytes(value_blob) != memo_value_hash:
                    self._store.trace_event(
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

                entries.append((entry_key, record_blob, memo_value_hash, stored_blob))

            if entries:
                try:
                    self._disk.store_entry_many(entries)
                except PersistentCacheError:
                    pass
                for entry in batch:
                    self._store.trace_event("disk_store", entry[0])
                    self._store.record_disk_store()
            for _ in batch:
                self._disk_queue.task_done()

    def flush_disk(self) -> None:
        if self._disk_queue is not None:
            self._disk_queue.join()

    def flush_disk_at_exit(self) -> None:
        """Wait for queued writes, giving up if the writer thread is gone.

        ``flush_disk`` waits for the queue unconditionally. At interpreter
        exit that would turn a dead writer into a process that never ends,
        so this variant also returns once nothing is left to drain the queue.
        """
        pending = self._disk_queue
        thread = self._disk_thread
        if pending is None:
            return
        with pending.all_tasks_done:
            while pending.unfinished_tasks:
                if thread is None or not thread.is_alive():
                    return
                pending.all_tasks_done.wait(0.05)

    def shutdown(self, wait: bool = True) -> None:
        if self._disk_queue is not None:
            self._disk_queue.put(None)
            if wait and self._disk_thread is not None:
                self._disk_thread.join()
                # Joined, so nothing is left to flush at exit. Without the
                # join the queue may still hold results; stay registered.
                with _live_disk_writers_lock:
                    _live_disk_writers.discard(self)

    def _persist_entry(self, key: QueryKey, memo: MemoEntry) -> None:
        if self._disk_queue is None:
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
            return self._store.stable_hash(fn(*dep_args))
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
