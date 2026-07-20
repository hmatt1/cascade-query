from __future__ import annotations

import concurrent.futures
import contextvars
from typing import Any, Callable, Mapping, Sequence

from . import _canonical, _disk_cache
from ._disk_cache import DiskCache
from ._errors import CycleError, PersistentCacheError, QueryCancelled
from ._runtime import RuntimeFrame, RuntimeState
from ._state import InputVersion, MemoEntry, QueryKey, Snapshot
from ._store import GraphStore


class Evaluator:
    def __init__(self, store: GraphStore, *, disk: DiskCache | None = None) -> None:
        self._store = store
        self._disk = disk
        self._runtime_var: contextvars.ContextVar[RuntimeState | None] = contextvars.ContextVar(
            "cascade_runtime", default=None
        )
        # Keys currently being verified against the disk cache in this context.
        # Guards against unbounded recursion if a corrupted store ever encodes
        # a dependency cycle; real cycles are still caught by the frame stack.
        self._hydrating_var: contextvars.ContextVar[frozenset[QueryKey]] = contextvars.ContextVar(
            "cascade_hydrating", default=frozenset()
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

                self._store.pin_memo_verification_locked(key)
                try:
                    green = self.try_mark_green(key, existing, runtime.snapshot)
                finally:
                    self._store.unpin_memo_verification_locked(key)

                if green:
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
            hydrated: MemoEntry | None = None
            if self._disk is not None and existing is None:
                hydrated = self._try_hydrate_from_disk(key, runtime)
            if hydrated is not None:
                owner_future.set_result(hydrated)
                return hydrated, True
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
        start = self._store.monotonic_seconds()
        self._store.trace_event("recompute_start", key)
        try:
            self.check_cancelled(runtime.cancel_epoch)
            result = fn(*key[2])
            self.check_cancelled(runtime.cancel_epoch)
        finally:
            runtime.stack.pop()
        elapsed = self._store.monotonic_seconds() - start
        duration_ms = elapsed * 1000.0

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
        if self._disk is not None:
            self._persist_entry(key, memo)
        self._store.trace_event("recompute_done", key, detail=f"{duration_ms:.3f}ms")
        if self._store.is_stats_enabled():
            self._store.record_query_body_time(key, elapsed)
        return memo

    # --- persistent disk cache ---

    def _try_hydrate_from_disk(self, key: QueryKey, runtime: RuntimeState) -> MemoEntry | None:
        disk = self._disk
        if disk is None:
            return None
        hydrating = self._hydrating_var.get()
        if key in hydrating:
            return None
        kind, fid, args = key
        try:
            args_blob = _canonical.encode(args)
        except TypeError:
            self._store.trace_event("disk_unkeyed", key)
            return None
        record = disk.load_entry(_disk_cache.entry_key(kind, fid, args_blob))
        if record is None:
            self._store.trace_event("disk_miss", key)
            return None
        token = self._hydrating_var.set(hydrating | {key})
        try:
            observed = self._verify_disk_deps(key, record, runtime)
        finally:
            self._hydrating_var.reset(token)
        if observed is None:
            return None
        value_hash = record.get("value_hash")
        if not isinstance(value_hash, str):
            return None
        blob = disk.load_blob(value_hash)
        if blob is None or _canonical.digest_bytes(blob) != value_hash:
            self._store.trace_event("disk_red", key, detail="value blob missing or corrupt")
            return None
        try:
            value = _canonical.decode(blob)
            effects_raw = record.get("effects", {})
            effects = {str(name): tuple(items) for name, items in effects_raw.items()}
        except Exception:
            self._store.trace_event("disk_red", key, detail="record decode failed")
            return None
        with self._store.lock:
            self._store.next_access_id += 1
            memo = self._store.entry_from_runtime(
                value=value,
                value_hash=value_hash,
                changed_at=runtime.snapshot.revision,
                verified_at=runtime.snapshot.revision,
                deps=observed,
                effects=effects,
                last_access=self._store.next_access_id,
            )
            self._store.drop_memo_locked(key)
            self._store.memos[key] = memo
            self._store.push_memo_lru_locked(key)
            for dep_key in observed:
                self._store.dependents[dep_key].add(key)
            self._store.evict_if_needed_locked()
        self._store.trace_event("disk_hit", key)
        return memo

    def _verify_disk_deps(
        self,
        key: QueryKey,
        record: dict[str, Any],
        runtime: RuntimeState,
    ) -> dict[QueryKey, int] | None:
        deps_raw = record.get("deps")
        if not isinstance(deps_raw, list):
            return None
        observed: dict[QueryKey, int] = {}
        for row in deps_raw:
            if not isinstance(row, list) or len(row) != 4:
                return None
            dep_kind, dep_fid, dep_args_blob, fingerprint = row
            if not (isinstance(dep_kind, str) and isinstance(dep_fid, str) and isinstance(dep_args_blob, bytes)):
                return None
            try:
                dep_args = _canonical.decode(dep_args_blob)
            except Exception:
                return None
            if not isinstance(dep_args, tuple):
                return None
            dep_key: QueryKey = (dep_kind, dep_fid, dep_args)
            if dep_kind == "input":
                version = self._current_input_version(dep_fid, dep_args, runtime)
                if version is None or version.value_hash != fingerprint:
                    self._store.trace_event("disk_red", key, detail=self._store.key_to_str(dep_key))
                    return None
                observed[dep_key] = version.changed_at
            elif dep_kind == "query":
                state = self._current_query_state(dep_fid, dep_args, runtime)
                if state is None or state[0] != fingerprint:
                    self._store.trace_event("disk_red", key, detail=self._store.key_to_str(dep_key))
                    return None
                observed[dep_key] = state[1]
            else:
                return None
        return observed

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
    ) -> tuple[str, int] | None:
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
            snapshot_pinned=runtime.snapshot_pinned,
        )
        self._run_in_runtime(
            shadow,
            lambda: self.query_call(
                query_id, fn, args, snapshot=runtime.snapshot, effects=None, cancel_epoch=runtime.cancel_epoch
            ),
        )
        with self._store.lock:
            memo = self._store.memos.get(dep_key)
            if memo is None:
                return None
            return memo.value_hash, memo.changed_at

    def _persist_entry(self, key: QueryKey, memo: MemoEntry) -> None:
        disk = self._disk
        if disk is None:
            return
        kind, fid, args = key
        try:
            args_blob = _canonical.encode(args)
        except TypeError:
            self._store.trace_event("disk_unkeyed", key)
            return
        deps_records = self._dep_fingerprint_rows(key, memo)
        if deps_records is None:
            return
        try:
            value_blob = _canonical.encode(memo.value)
        except TypeError as exc:
            raise PersistentCacheError(
                f"query {self._store.key_to_str(key)!r} returned a value the persistent cache "
                f"cannot serialize: {exc}"
            ) from exc
        if _canonical.digest_bytes(value_blob) != memo.value_hash:
            # The result object mutated between hashing and persistence; storing
            # it would poison future runs, so skip and recompute next session.
            self._store.trace_event("disk_skip", key, detail="value mutated before persist")
            return
        try:
            effects_node = {name: list(items) for name, items in memo.effects.items()}
            record = {
                "kind": kind,
                "id": fid,
                "value_hash": memo.value_hash,
                "deps": deps_records,
                "effects": effects_node,
            }
            disk.store_entry(_disk_cache.entry_key(kind, fid, args_blob), record, memo.value_hash, value_blob)
        except PersistentCacheError:
            raise
        except TypeError as exc:
            raise PersistentCacheError(
                f"accumulator effects recorded by {self._store.key_to_str(key)!r} cannot be "
                f"serialized for the persistent cache: {exc}"
            ) from exc
        self._store.trace_event("disk_store", key)

    def _dep_fingerprint_rows(self, key: QueryKey, memo: MemoEntry) -> list[list[Any]] | None:
        rows: list[list[Any]] = []
        for dep in memo.deps:
            dep_kind, dep_fid, dep_args = dep.key
            try:
                dep_args_blob = _canonical.encode(dep_args)
            except TypeError:
                self._store.trace_event("disk_unkeyed", key, detail=self._store.key_to_str(dep.key))
                return None
            fingerprint = self._observed_dep_fingerprint(dep.key, dep.observed_changed_at, memo.verified_at)
            if fingerprint is None:
                self._store.trace_event("disk_skip", key, detail=self._store.key_to_str(dep.key))
                return None
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
                return None
            return dep_memo.value_hash
        input_key = (dep_fid, dep_args)
        with self._store.lock:
            version = self._store.input_version_at(input_key, verified_at)
            if version is None:
                # Materialized mid-run at a revision above verified_at, or never
                # materialized at all (pull-based default read).
                version = self._store.latest_input_version(input_key)
        if version is not None:
            if version.changed_at != observed_changed_at:
                return None
            return version.value_hash
        if observed_changed_at != -1:
            return None
        fn = self._store.lookup_input(dep_fid)
        if fn is None:
            return None
        try:
            return self._store.stable_hash(fn(*dep_args))
        except TypeError:
            return None
