def patch():
    path = r"C:\Users\Matt\Projects\cascade-query\src\cascade\_evaluator.py"
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    async_methods = """
    async def _try_hydrate_from_disk_async(self, key: QueryKey, runtime: RuntimeState) -> MemoEntry | None:
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
            
        fn_hash = record.get("fn_hash")
        with self._store.lock:
            current_fn_hash = self._store.query_hashes.get(fid) if kind == "query" else self._store.input_hashes.get(fid)
        if fn_hash is not None and current_fn_hash is not None and fn_hash != current_fn_hash:
            self._store.trace_event("disk_red", key, detail="function hash changed")
            return None
            
        token = self._hydrating_var.set(hydrating | {key})
        try:
            observed = await self._verify_disk_deps_async(key, record, runtime)
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
            is_error = record.get("is_error", False)
        except Exception:
            self._store.trace_event("disk_red", key, detail="record decode failed")
            return None
            
        error = None
        if is_error:
            error = value
            value = None
            
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
                error=error,
            )
            self._store.drop_memo_locked(key)
            self._store.memos[key] = memo
            self._store.push_memo_lru_locked(key)
            for dep_key in observed:
                self._store.dependents[dep_key].add(key)
            self._store.evict_if_needed_locked()
        self._store.trace_event("disk_hit", key)
        return memo

    async def _verify_disk_deps_async(
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
                version = await self._current_input_version_async(dep_fid, dep_args, runtime)
                if version is None or version.value_hash != fingerprint:
                    self._store.trace_event("disk_red", key, detail=self._store.key_to_str(dep_key))
                    return None
                observed[dep_key] = version.changed_at
            elif dep_kind == "query":
                state = await self._current_query_state_async(dep_fid, dep_args, runtime)
                if state is None or state[0] != fingerprint:
                    self._store.trace_event("disk_red", key, detail=self._store.key_to_str(dep_key))
                    return None
                observed[dep_key] = state[1]
            else:
                return None
        return observed

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
            return None
            
        import inspect
        if inspect.iscoroutinefunction(fn):
            value = await fn(*args)
        else:
            import asyncio
            loop = asyncio.get_running_loop()
            executor = self._get_executor() if self._get_executor else None
            value = await loop.run_in_executor(executor, fn, *args)
            
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
    ) -> tuple[str, int] | None:
        dep_key: QueryKey = ("query", query_id, args)
        try:
            fn = self._store.lookup_query(query_id)
        except KeyError:
            return None
            
        shadow = RuntimeState(
            snapshot=runtime.snapshot,
            stack=[],
            root_effects=None,
            staged_root_effects={},
            cancel_epoch=runtime.cancel_epoch,
            snapshot_pinned=runtime.snapshot_pinned,
            is_async=True,
        )
        await self._run_in_runtime_async(
            shadow,
            lambda: self.query_call_async(
                query_id, fn, args, snapshot=runtime.snapshot, effects=None, cancel_epoch=runtime.cancel_epoch
            ),
        )
        with self._store.lock:
            memo = self._store.memos.get(dep_key)
            if memo is None:
                return None
            return memo.value_hash, memo.changed_at

"""

    # Add the methods after _persist_entry
    idx = content.find("    def _persist_entry(")
    if idx == -1:
        print("Could not find _persist_entry")
        return

    content = content[:idx] + async_methods + content[idx:]

    # Also fix compute_or_get_memo_async to call the async version
    content = content.replace(
        "hydrated = self._try_hydrate_from_disk(key, runtime)",
        "hydrated = await self._try_hydrate_from_disk_async(key, runtime)",
    )

    # Also fix synchronous `_current_input_version` to handle coroutines properly using `asyncio.run`
    # Just in case it gets called synchronously.
    sync_input_fix = """        value = fn(*args)
        import types
        if isinstance(value, types.CoroutineType):
            import asyncio
            value = asyncio.run(value)
        return InputVersion("""
    content = content.replace(
        """        value = fn(*args)
        return InputVersion(""",
        sync_input_fix,
    )

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


if __name__ == "__main__":
    patch()
