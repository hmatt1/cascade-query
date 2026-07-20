"""LMDB-backed persistent store for query metadata and value blobs.

Layout: one LMDB environment per ``cache_dir`` with three named databases.
``meta`` maps a deterministic entry key to a packed record describing a query
result (value fingerprint, dependency fingerprints, accumulator effects).
``blobs`` is content-addressed: value fingerprint -> canonical value bytes.
``sys`` holds the on-disk format stamp; a mismatched stamp wipes the cache,
which is always safe because everything here can be recomputed.
"""

from __future__ import annotations

import hashlib
import os
import threading
from typing import Any

from . import _canonical
from ._errors import PersistentCacheError

try:
    import lmdb
except ImportError:  # pragma: no cover - exercised via monkeypatch in tests
    lmdb = None  # type: ignore[assignment]

LMDB_INSTALL_HINT = (
    "cascade persistent caching requires the 'lmdb' package for the on-disk store; "
    "there is no fallback. Install it with: pip install lmdb"
)

DISK_FORMAT = 1
_FORMAT_KEY = b"format"


class _SharedEnv:
    def __init__(self, env: Any) -> None:
        self.env = env
        self.refs = 1


# LMDB permits exactly one environment handle per path per process; concurrent
# access from other processes is handled by LMDB's own file locks. Multiple
# DiskCache instances in one process therefore share a refcounted environment,
# and the first opener's map_size wins for the life of the process.
_ENV_REGISTRY: dict[str, _SharedEnv] = {}
_ENV_LOCK = threading.Lock()


def _acquire_env(path: str, map_size: int) -> tuple[str, Any]:
    registry_key = os.path.realpath(path)
    with _ENV_LOCK:
        shared = _ENV_REGISTRY.get(registry_key)
        if shared is not None:
            shared.refs += 1
            return registry_key, shared.env
        try:
            # sync=False trades durability of the last few writes for speed.
            # A crash can lose recent entries, never corrupt the store, and a
            # lost entry only costs one recompute.
            env = lmdb.open(
                path,
                map_size=map_size,
                max_dbs=3,
                subdir=True,
                sync=False,
                metasync=False,
            )
        except lmdb.Error as exc:
            raise PersistentCacheError(f"failed to open persistent cache at {path!r}: {exc}") from exc
        _ENV_REGISTRY[registry_key] = _SharedEnv(env)
        return registry_key, env


def _release_env(registry_key: str) -> None:
    with _ENV_LOCK:
        shared = _ENV_REGISTRY.get(registry_key)
        if shared is None:
            return
        shared.refs -= 1
        if shared.refs <= 0:
            del _ENV_REGISTRY[registry_key]
            shared.env.sync(True)
            shared.env.close()


def entry_key(kind: str, function_id: str, args_blob: bytes) -> bytes:
    """Deterministic cache address: Hash(kind + name + Hash(args))."""
    args_hash = hashlib.blake2b(args_blob, digest_size=20).digest()
    h = hashlib.blake2b(digest_size=20)
    h.update(kind.encode("utf-8"))
    h.update(b"\x00")
    h.update(function_id.encode("utf-8"))
    h.update(b"\x00")
    h.update(args_hash)
    return h.digest()


class DiskCache:
    def __init__(self, cache_dir: str | os.PathLike[str], *, map_size: int) -> None:
        if lmdb is None:
            raise PersistentCacheError(LMDB_INSTALL_HINT)
        _canonical.require_msgpack()
        path = os.fspath(cache_dir)
        os.makedirs(path, exist_ok=True)
        self._registry_key, self._env = _acquire_env(path, map_size)
        self._meta = self._env.open_db(b"meta")
        self._blobs = self._env.open_db(b"blobs")
        self._sys = self._env.open_db(b"sys")
        self._closed = False
        self._ensure_format()

    @property
    def path(self) -> str:
        return self._env.path()

    def _ensure_format(self) -> None:
        stamp = DISK_FORMAT.to_bytes(4, "big")
        with self._env.begin(write=True) as txn:
            current = txn.get(_FORMAT_KEY, db=self._sys)
            if current == stamp:
                return
            if current is not None:
                txn.drop(self._meta, delete=False)
                txn.drop(self._blobs, delete=False)
            txn.put(_FORMAT_KEY, stamp, db=self._sys)

    def load_entry(self, key: bytes) -> dict[str, Any] | None:
        with self._env.begin() as txn:
            raw = txn.get(key, db=self._meta)
        if raw is None:
            return None
        try:
            record = _canonical.decode(bytes(raw))
        except Exception:
            return None
        if not isinstance(record, dict):
            return None
        return record

    def load_blob(self, value_hash: str) -> bytes | None:
        with self._env.begin() as txn:
            raw = txn.get(value_hash.encode("ascii"), db=self._blobs)
        return None if raw is None else bytes(raw)

    def store_entry(self, key: bytes, record: dict[str, Any], value_hash: str, value_blob: bytes) -> None:
        record_blob = _canonical.encode(record)
        try:
            with self._env.begin(write=True) as txn:
                txn.put(value_hash.encode("ascii"), value_blob, db=self._blobs)
                txn.put(key, record_blob, db=self._meta)
        except lmdb.MapFullError as exc:
            raise PersistentCacheError(
                f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                "to Engine, or clear the cache with engine.clear_disk_cache()."
            ) from exc

    def clear(self) -> None:
        with self._env.begin(write=True) as txn:
            txn.drop(self._meta, delete=False)
            txn.drop(self._blobs, delete=False)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            _release_env(self._registry_key)
