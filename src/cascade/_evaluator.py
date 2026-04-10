from __future__ import annotations

import concurrent.futures
import contextvars
import time
from typing import Any, Callable, Mapping, Sequence

from ._errors import CycleError, QueryCancelled
from ._runtime import RuntimeFrame, RuntimeState
from ._state import InputVersion, MemoEntry, QueryKey, Snapshot
from ._store import GraphStore


class Evaluator:
    def __init__(self, store: GraphStore) -> None:
        self._store = store
        self._runtime_var: contextvars.ContextVar[RuntimeState | None] = contextvars.ContextVar(
            "cascade_runtime", default=None
        )

    def _run_in_runtime(self, runtime: RuntimeState, fn: Callable[[], Any]) -> Any:
        token = self._runtime_var.set(runtime)
        try:
            result = fn()
            # Root effects become externally visible only after successful query
            # completion, preventing failed executions from leaking partial output.
            if runtime.root_effects is not None:
                for name, values in runtime.staged_root_effects.items():
                    runtime.root_effects.setdefault(name, []).extend(values)
            return result
        finally:
            self._runtime_var.reset(token)

    def check_cancelled(self, runtime_cancel_epoch: int | None) -> None:
        if runtime_cancel_epoch is None:
            return
        with self._store.lock:
            current = self._store.cancel_epoch
        if current != runtime_cancel_epoch:
            raise QueryCancelled("query cancelled because inputs changed")

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
        frame.deps[dep_key] = observed_changed_at

    def read_input(
        self,
        input_id: str,
        fn: Callable[..., Any],
        args: tuple[Any, ...],
        *,
        snapshot: Snapshot | None,
    ) -> Any:
        runtime = self._runtime_var.get()
        use_snapshot = snapshot or (runtime.snapshot if runtime is not None else self._store.snapshot())
        snapshot_pinned = snapshot is not None or (runtime.snapshot_pinned if runtime is not None else False)
        if runtime is not None:
            self.check_cancelled(runtime.cancel_epoch)
        key = ("input", input_id, args)
        version = self._store.input_version_at((input_id, args), use_snapshot.revision)
        if version is None:
            default = fn(*args)
            should_materialize = False
            if not snapshot_pinned:
                with self._store.lock:
                    latest = self._store.latest_input_version((input_id, args))
                    should_materialize = latest is None and use_snapshot.revision == self._store.revision
            if should_materialize:
                self._store.set_input(input_id, args, default, bump_cancel_epoch=False)
                version = self._store.latest_input_version((input_id, args))
                if version is None:  # pragma: no cover - safety belt
                    raise RuntimeError("input version missing after initialization")
            else:
                version = InputVersion(
                    revision=use_snapshot.revision,
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
    ) -> Any:
        runtime = self._runtime_var.get()
        if runtime is None:
            root_runtime = RuntimeState(
                snapshot=snapshot or self._store.snapshot(),
                stack=[],
                root_effects=effects,
                staged_root_effects={},
                cancel_epoch=cancel_epoch,
                snapshot_pinned=snapshot is not None,
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
                ),
            )

        key: QueryKey = ("query", query_id, args)
        if any(frame.key == key for frame in runtime.stack):
            cycle_start = [frame.key for frame in runtime.stack] + [key]
            cycle = " -> ".join(self._store.key_to_str(k) for k in cycle_start)
            raise CycleError(f"cycle detected: {cycle}")
        self.check_cancelled(runtime.cancel_epoch)
        entry, replay_needed = self.compute_or_get_memo(key, fn, runtime)
        self.record_dependency(key, entry.changed_at)
        if replay_needed:
            self.replay_effects(entry.effects)
        return entry.value

    def compute_or_get_memo(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
    ) -> tuple[MemoEntry, bool]:
        with self._store.lock:
            existing = self._store.memos.get(key)
            if existing is not None:
                self._store.touch_memo_locked(key)
                if existing.verified_at == runtime.snapshot.revision:
                    self._store.trace_event("cache_hit", key)
                    return existing, True

                if self.try_mark_green(key, existing, runtime.snapshot):
                    existing.verified_at = runtime.snapshot.revision
                    self._store.touch_memo_locked(key)
                    self._store.trace_event("cache_green", key)
                    return existing, True

            in_flight_key = (key, runtime.snapshot.revision)
            owner_future = self._store.in_flight.get(in_flight_key)
            if owner_future is None:
                owner_future = concurrent.futures.Future()
                self._store.in_flight[in_flight_key] = owner_future
                is_owner = True
            else:
                is_owner = False

        if not is_owner:
            self._store.trace_event("dedup_wait", key)
            result = owner_future.result()
            return result, True

        try:
            result = self.recompute(key, fn, runtime)
            owner_future.set_result(result)
            return result, False
        except BaseException as exc:
            owner_future.set_exception(exc)
            raise
        finally:
            with self._store.lock:
                self._store.in_flight.pop((key, runtime.snapshot.revision), None)

    def try_mark_green(self, key: QueryKey, entry: MemoEntry, snapshot: Snapshot) -> bool:
        for dep in entry.deps:
            dep_changed_at = self.dependency_changed_at(dep.key, snapshot)
            if dep_changed_at != dep.observed_changed_at:
                self._store.trace_event("cache_red", key, detail=self._store.key_to_str(dep.key))
                return False
        return True

    def dependency_changed_at(self, key: QueryKey, snapshot: Snapshot) -> int:
        kind, fid, args = key
        if kind == "input":
            version = self._store.input_version_at((fid, args), snapshot.revision)
            return -1 if version is None else version.changed_at

        with self._store.lock:
            memo = self._store.memos.get(key)
        fn = self._store.lookup_query(fid)
        if memo is not None and memo.verified_at == snapshot.revision:
            return memo.changed_at

        runtime = self._runtime_var.get()
        if runtime is None:
            self._run_in_runtime(
                RuntimeState(
                    snapshot=snapshot,
                    stack=[],
                    root_effects=None,
                    staged_root_effects={},
                    cancel_epoch=None,
                    snapshot_pinned=True,
                ),
                lambda: self.query_call(fid, fn, args, snapshot=snapshot, effects=None, cancel_epoch=None),
            )
        else:
            # Dependency verification should not add edges to currently executing frame.
            shadow = RuntimeState(
                snapshot=snapshot,
                stack=[],
                root_effects=None,
                staged_root_effects={},
                cancel_epoch=runtime.cancel_epoch,
                snapshot_pinned=True,
            )
            self._run_in_runtime(
                shadow,
                lambda: self.query_call(
                    fid, fn, args, snapshot=snapshot, effects=None, cancel_epoch=runtime.cancel_epoch
                ),
            )

        with self._store.lock:
            refreshed = self._store.memos.get(key)
            if refreshed is None:  # pragma: no cover - safety belt
                raise RuntimeError(f"missing memo for {self._store.key_to_str(key)}")
            return refreshed.changed_at

    def recompute(
        self,
        key: QueryKey,
        fn: Callable[..., Any],
        runtime: RuntimeState,
    ) -> MemoEntry:
        frame = RuntimeFrame(key=key)
        runtime.stack.append(frame)
        start = time.perf_counter()
        self._store.trace_event("recompute_start", key)
        try:
            self.check_cancelled(runtime.cancel_epoch)
            result = fn(*key[2])
            self.check_cancelled(runtime.cancel_epoch)
        finally:
            runtime.stack.pop()
        duration_ms = (time.perf_counter() - start) * 1000.0

        frozen_effects = {name: tuple(items) for name, items in frame.effects.items()}
        value_hash = self._store.stable_hash(result)
        with self._store.lock:
            previous = self._store.memos.get(key)
            if previous is not None and previous.value_hash == value_hash:
                changed_at = previous.changed_at
                self._store.trace_event("backdate", key)
            else:
                changed_at = runtime.snapshot.revision
            self._store.next_access_id += 1
            memo = self._store.entry_from_runtime(
                value=result,
                value_hash=value_hash,
                changed_at=changed_at,
                verified_at=runtime.snapshot.revision,
                deps=frame.deps,
                effects=frozen_effects,
                last_access=self._store.next_access_id,
            )
            self._store.drop_memo_locked(key)
            self._store.memos[key] = memo
            self._store.push_memo_lru_locked(key)
            for dep_key in frame.deps.keys():
                self._store.dependents[dep_key].add(key)
            self._store.evict_if_needed_locked()
        self._store.trace_event("recompute_done", key, detail=f"{duration_ms:.3f}ms")
        return memo
