"""MDBX-backed persistent store for query metadata and value blobs.

Layout: one MDBX environment per ``cache_dir`` with three named databases.
``meta`` maps a deterministic entry key to a packed record describing a query
result (value fingerprint, dependency fingerprints, accumulator effects).
``blobs`` is content-addressed: value fingerprint -> canonical value bytes.
``sys`` holds the on-disk format stamp; a mismatched stamp wipes the cache,
which is always safe because everything here can be recomputed.
"""

from __future__ import annotations

import hashlib
import os
import sys
import threading
from typing import Any, Protocol

from . import _canonical
from ._errors import PersistentCacheError


try:
    import lmdb
except ImportError:  # pragma: no cover
    lmdb = None  # type: ignore[assignment]

LMDB_INSTALL_HINT = (
    "cascade persistent caching requires the 'lmdb' package for the on-disk store; "
    "there is no fallback. Install it with: pip install lmdb"
)

try:
    import mdbx
except ImportError:  # pragma: no cover - exercised via monkeypatch in tests
    mdbx = None  # type: ignore[assignment]

MDBX_INSTALL_HINT = (
    "cascade persistent caching requires the 'libmdbx' package for the on-disk store; "
    "there is no fallback. Install it with: pip install libmdbx"
)

_FREE_THREADED = False
if hasattr(sys, "_is_gil_enabled"):
    _FREE_THREADED = not sys._is_gil_enabled()

_FT_LOCK = threading.RLock()


class TxnContext:
    def __init__(self, env: Any, write: bool, kwargs: dict[str, Any]) -> None:
        self.env = env
        self.write = write
        self.kwargs = kwargs
        self.txn: Any = None

    def __enter__(self) -> Any:
        if _FREE_THREADED:
            _FT_LOCK.acquire()  # pragma: no cover
        flags = 0 if self.write else mdbx.MDBXTXNFlags.MDBX_TXN_RDONLY
        self.txn = self.env.start_transaction(flags=flags, **self.kwargs)
        return self.txn.__enter__()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        try:
            if exc_type is None and self.write:
                self.txn.commit()
            return self.txn.__exit__(exc_type, exc_val, exc_tb)
        finally:
            if _FREE_THREADED:
                _FT_LOCK.release()  # pragma: no cover


DISK_FORMAT = 1
_FORMAT_KEY = b"format"


class _SharedEnv:
    def __init__(self, env: Any) -> None:
        self.env = env
        self.refs = 1


# MDBX permits exactly one environment handle per path per process; concurrent
# access from other processes is handled by MDBX's own file locks. Multiple
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
            if _FREE_THREADED:  # pragma: no cover
                _FT_LOCK.acquire()
            try:
                env = mdbx.Env(
                    path,
                    flags=mdbx.MDBXEnvFlags.MDBX_SAFE_NOSYNC | mdbx.MDBXEnvFlags.MDBX_NOMETASYNC,
                    maxdbs=4,
                    geometry=mdbx.Geometry(size_upper=map_size)
                )
            finally:
                if _FREE_THREADED:  # pragma: no cover
                    _FT_LOCK.release()

        except mdbx.MDBXErrorExc as exc:
            raise PersistentCacheError(
                f"failed to open persistent cache at {path!r}: {exc}"
            ) from exc
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
            if _FREE_THREADED:  # pragma: no cover
                _FT_LOCK.acquire()
            try:
                shared.env.sync(True)
                shared.env.close()
            finally:
                if _FREE_THREADED:  # pragma: no cover
                    _FT_LOCK.release()


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


class MdbxDiskCache:
    def __init__(self, cache_dir: str | os.PathLike[str], *, map_size: int) -> None:
        if mdbx is None:
            raise PersistentCacheError(MDBX_INSTALL_HINT)
        _canonical.require_msgpack()
        path = os.fspath(cache_dir)
        os.makedirs(path, exist_ok=True)
        self._registry_key, self._env = _acquire_env(path, map_size)
        if _FREE_THREADED:  # pragma: no cover
            _FT_LOCK.acquire()
        try:
            with self._env.start_transaction() as txn:
                self._meta = txn.create_map(b"meta")
                self._blobs = txn.create_map(b"blobs")
                self._sys = txn.create_map(b"sys")
                self._collections = txn.create_map(b"collections")
                txn.commit()
        finally:
            if _FREE_THREADED:  # pragma: no cover
                _FT_LOCK.release()
        self._closed = False
        self._ensure_format()

    @property
    def path(self) -> str:
        return self._env.get_path()

    def _begin(self, write: bool = False, **kwargs: Any) -> Any:
        return TxnContext(self._env, write, kwargs)

    def _ensure_format(self) -> None:
        stamp = DISK_FORMAT.to_bytes(4, "big")
        with self._begin(write=True) as txn:
            current = self._sys.get(txn, _FORMAT_KEY)
            if current == stamp:
                return
            if current is not None:
                self._meta.drop(txn, delete=False)
                self._blobs.drop(txn, delete=False)
            self._sys.put(txn, _FORMAT_KEY, stamp)

    def load_entry(self, key: bytes) -> dict[str, Any] | None:
        with self._begin() as txn:
            raw = self._meta.get(txn, key)
        if raw is None:
            return None  # pragma: no cover
        try:
            record = _canonical.decode(bytes(raw))
        except Exception:  # pragma: no cover  # pragma: no cover
            return None  # pragma: no cover
        if not isinstance(record, dict):
            return None  # pragma: no cover
        return record

    def load_blob(self, value_hash: str) -> bytes | None:
        with self._begin() as txn:
            raw = self._blobs.get(txn, value_hash.encode("ascii"))
        return None if raw is None else bytes(raw)  # pragma: no cover

    def store_entry(
        self, key: bytes, record: dict[str, Any], value_hash: str, value_blob: bytes
    ) -> None:
        record_blob = _canonical.encode(record)
        try:
            with self._begin(write=True) as txn:
                self._blobs.put(txn, value_hash.encode("ascii"), value_blob)
                self._meta.put(txn, key, record_blob)
        except mdbx.MDBXErrorExc as exc:
            if "MDBX_MAP_FULL" in str(exc):
                raise PersistentCacheError(
                    f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                    "to Engine, or clear the cache with engine.clear_disk_cache()."
                ) from exc
            raise

    def store_entry_many(self, entries: list[tuple[bytes, bytes, str, bytes]]) -> None:
        if not entries:
            return
        try:
            with self._begin(write=True) as txn:
                for key, record_blob, value_hash, value_blob in entries:
                    self._blobs.put(txn, value_hash.encode("ascii"), value_blob)
                    self._meta.put(txn, key, record_blob)
        except mdbx.MDBXErrorExc as exc:
            if "MDBX_MAP_FULL" in str(exc):
                raise PersistentCacheError(
                    f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                    "to Engine, or clear the cache with engine.clear_disk_cache()."
                ) from exc
            raise

    def clear(self) -> None:
        with self._begin(write=True) as txn:
            self._meta.drop(txn, delete=False)
            self._blobs.drop(txn, delete=False)
            self._collections.drop(txn, delete=False)

    # --- collection event sourcing ---
    # ``s\x00{name}`` holds the compacted snapshot record; ``l\x00{name}\x00{rev}``
    # holds one diff per revision after the snapshot base, with the revision as
    # 8 big-endian bytes so a cursor range scan replays the tail in order.

    @staticmethod
    def _snap_key(name: str) -> bytes:
        return b"s\x00" + name.encode("utf-8")

    @staticmethod
    def _log_prefix(name: str) -> bytes:
        return b"l\x00" + name.encode("utf-8") + b"\x00"

    def _log_key(self, name: str, rev: int) -> bytes:
        return self._log_prefix(name) + rev.to_bytes(8, "big")

    def collection_load(
        self, name: str
    ) -> tuple[dict[str, Any] | None, list[tuple[int, dict[str, Any]]]]:
        """Snapshot record (or None) plus the ordered tail of diffs after it."""
        prefix = self._log_prefix(name)
        with self._begin() as txn:
            raw = self._collections.get(txn, self._snap_key(name))
            snapshot = _canonical.decode(bytes(raw)) if raw is not None else None
            tail: list[tuple[int, dict[str, Any]]] = []
            with mdbx.Cursor(self._collections, txn) as curs:
                for key, value in curs.iter(start_key=prefix):
                    if not bytes(key).startswith(prefix):
                        break
                    rev = int.from_bytes(bytes(key)[len(prefix) :], "big")
                    tail.append((rev, _canonical.decode(bytes(value))))
        return snapshot, tail

    def collection_append_many(
        self, name: str, batch: list[tuple[int, dict[str, Any]]]
    ) -> None:
        blobs = [
            (self._log_key(name, rev), _canonical.encode(diff)) for rev, diff in batch
        ]
        try:
            with self._begin(write=True) as txn:
                for key, blob in blobs:
                    self._collections.put(txn, key, blob)
        except mdbx.MDBXErrorExc as exc:
            if "MDBX_MAP_FULL" in str(exc):
                raise PersistentCacheError(
                    f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                    "to Engine, or clear the cache with engine.clear_disk_cache()."
                ) from exc
            raise

    def collection_snapshot(
        self, name: str, record: dict[str, Any], upto_rev: int
    ) -> None:
        """Write a compacted snapshot and delete every log entry at or below it."""
        blob = _canonical.encode(record)
        prefix = self._log_prefix(name)
        try:
            with self._begin(write=True) as txn:
                self._collections.put(txn, self._snap_key(name), blob)
                drop: list[bytes] = []
                with mdbx.Cursor(self._collections, txn) as curs:
                    for key, _ in curs.iter(start_key=prefix):
                        kb = bytes(key)
                        if not kb.startswith(prefix):
                            break
                        if int.from_bytes(kb[len(prefix) :], "big") <= upto_rev:
                            drop.append(kb)
                        else:
                            break
                for key in drop:
                    self._collections.delete(txn, key)
        except mdbx.MDBXErrorExc as exc:
            if "MDBX_MAP_FULL" in str(exc):
                raise PersistentCacheError(
                    f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                    "to Engine, or clear the cache with engine.clear_disk_cache()."
                ) from exc
            raise

    def retain(self, wanted_entries: set[bytes], wanted_blobs: set[str]) -> None:
        """Drop all meta entries and blobs not in the provided sets."""
        with self._begin(write=True) as txn:
            drop_meta = []
            with mdbx.Cursor(self._meta, txn) as curs:
                for key, _ in curs.iter():
                    if key not in wanted_entries:
                        drop_meta.append(key)
            for k in drop_meta:
                self._meta.delete(txn, k)

            drop_blobs = []
            with mdbx.Cursor(self._blobs, txn) as curs:
                for key, _ in curs.iter():
                    if key.decode("ascii") not in wanted_blobs:
                        drop_blobs.append(key)
            for k in drop_blobs:
                self._blobs.delete(txn, k)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            _release_env(self._registry_key)


class LmdbTxnContext:  # pragma: no cover
    def __init__(self, env: Any, write: bool, kwargs: dict[str, Any], lock: threading.RLock) -> None:
        self.env = env
        self.write = write
        self.kwargs = kwargs
        self.lock = lock
        self.txn: Any = None

    def __enter__(self) -> Any:
        if _FREE_THREADED:
            _FT_LOCK.acquire()
        elif self.write:
            self.lock.acquire()
        try:
            self.txn = self.env.begin(write=self.write, **self.kwargs)
        except Exception:  # pragma: no cover
            if _FREE_THREADED:
                _FT_LOCK.release()
            elif self.write:
                self.lock.release()
            raise
        return self.txn

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        try:
            if exc_type is None and self.write:
                self.txn.commit()
            elif exc_type is not None and self.write:
                self.txn.abort()
        finally:
            if _FREE_THREADED:
                _FT_LOCK.release()
            elif self.write:
                self.lock.release()

def _acquire_lmdb_env(path: str, map_size: int) -> tuple[str, Any]:  # pragma: no cover
    registry_key = "lmdb:" + os.path.realpath(path)
    with _ENV_LOCK:
        shared = _ENV_REGISTRY.get(registry_key)
        if shared is not None:
            shared.refs += 1
            return registry_key, shared.env
        try:
            if _FREE_THREADED:  # pragma: no cover
                _FT_LOCK.acquire()
            try:
                env = lmdb.open(
                    path,
                    map_size=map_size,
                    max_dbs=4,
                    sync=False,
                    metasync=False
                )
            finally:
                if _FREE_THREADED:  # pragma: no cover
                    _FT_LOCK.release()

        except lmdb.Error as exc:  # pragma: no cover
            raise PersistentCacheError(
                f"failed to open persistent cache at {path!r}: {exc}"
            ) from exc
        _ENV_REGISTRY[registry_key] = _SharedEnv(env)
        return registry_key, env

class LmdbDiskCache:  # pragma: no cover
    def __init__(self, cache_dir: str | os.PathLike[str], *, map_size: int) -> None:
        if lmdb is None:
            raise PersistentCacheError(LMDB_INSTALL_HINT)
        _canonical.require_msgpack()
        path = os.fspath(cache_dir)
        os.makedirs(path, exist_ok=True)
        self._registry_key, self._env = _acquire_lmdb_env(path, map_size)
        self._lock = threading.RLock()
        
        with self._begin(write=True) as txn:
            self._meta = self._env.open_db(b"meta", txn=txn)
            self._blobs = self._env.open_db(b"blobs", txn=txn)
            self._sys = self._env.open_db(b"sys", txn=txn)
            self._collections = self._env.open_db(b"collections", txn=txn)

        self._closed = False
        self._ensure_format()

    @property
    def path(self) -> str:
        return self._env.path()

    def _begin(self, write: bool = False, **kwargs: Any) -> Any:
        return LmdbTxnContext(self._env, write, kwargs, self._lock)

    def _ensure_format(self) -> None:
        stamp = DISK_FORMAT.to_bytes(4, "big")
        with self._begin(write=True) as txn:
            current = txn.get(_FORMAT_KEY, db=self._sys)
            if current == stamp:
                return
            if current is not None:
                txn.drop(self._meta, delete=False)
                txn.drop(self._blobs, delete=False)
            txn.put(_FORMAT_KEY, stamp, db=self._sys)

    def load_entry(self, key: bytes) -> dict[str, Any] | None:
        with self._begin() as txn:
            raw = txn.get(key, db=self._meta)
        if raw is None:
            return None  # pragma: no cover
        try:
            record = _canonical.decode(bytes(raw))
        except Exception:  # pragma: no cover  # pragma: no cover
            return None  # pragma: no cover
        if not isinstance(record, dict):
            return None  # pragma: no cover
        return record

    def load_blob(self, value_hash: str) -> bytes | None:
        with self._begin() as txn:
            raw = txn.get(value_hash.encode("ascii"), db=self._blobs)
        return None if raw is None else bytes(raw)  # pragma: no cover

    def store_entry(
        self, key: bytes, record: dict[str, Any], value_hash: str, value_blob: bytes
    ) -> None:
        record_blob = _canonical.encode(record)
        try:
            with self._begin(write=True) as txn:
                txn.put(value_hash.encode("ascii"), value_blob, db=self._blobs)
                txn.put(key, record_blob, db=self._meta)
        except lmdb.MapFullError as exc:  # pragma: no cover
            raise PersistentCacheError(
                f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                "to Engine, or clear the cache with engine.clear_disk_cache()."
            ) from exc

    def store_entry_many(self, entries: list[tuple[bytes, bytes, str, bytes]]) -> None:  # pragma: no cover
        if not entries:
            return
        try:
            with self._begin(write=True) as txn:
                for key, record_blob, value_hash, value_blob in entries:
                    txn.put(value_hash.encode("ascii"), value_blob, db=self._blobs)
                    txn.put(key, record_blob, db=self._meta)
        except lmdb.MapFullError as exc:  # pragma: no cover
            raise PersistentCacheError(
                f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                "to Engine, or clear the cache with engine.clear_disk_cache()."
            ) from exc

    def clear(self) -> None:
        with self._begin(write=True) as txn:
            txn.drop(self._meta, delete=False)
            txn.drop(self._blobs, delete=False)
            txn.drop(self._collections, delete=False)

    @staticmethod
    def _snap_key(name: str) -> bytes:
        return b"s\x00" + name.encode("utf-8")

    @staticmethod
    def _log_prefix(name: str) -> bytes:
        return b"l\x00" + name.encode("utf-8") + b"\x00"

    def _log_key(self, name: str, rev: int) -> bytes:
        return self._log_prefix(name) + rev.to_bytes(8, "big")

    def collection_load(
        self, name: str
    ) -> tuple[dict[str, Any] | None, list[tuple[int, dict[str, Any]]]]:
        prefix = self._log_prefix(name)
        with self._begin() as txn:
            raw = txn.get(self._snap_key(name), db=self._collections)
            snapshot = _canonical.decode(bytes(raw)) if raw is not None else None
            tail: list[tuple[int, dict[str, Any]]] = []
            with txn.cursor(db=self._collections) as curs:
                if curs.set_range(prefix):
                    for key, value in curs:
                        if not bytes(key).startswith(prefix):
                            break
                        rev = int.from_bytes(bytes(key)[len(prefix) :], "big")
                        tail.append((rev, _canonical.decode(bytes(value))))
        return snapshot, tail

    def collection_append_many(
        self, name: str, batch: list[tuple[int, dict[str, Any]]]
    ) -> None:
        blobs = [
            (self._log_key(name, rev), _canonical.encode(diff)) for rev, diff in batch
        ]
        try:
            with self._begin(write=True) as txn:
                for key, blob in blobs:
                    txn.put(key, blob, db=self._collections)
        except lmdb.MapFullError as exc:  # pragma: no cover
            raise PersistentCacheError(
                f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                "to Engine, or clear the cache with engine.clear_disk_cache()."
            ) from exc

    def collection_snapshot(
        self, name: str, record: dict[str, Any], upto_rev: int
    ) -> None:
        blob = _canonical.encode(record)
        prefix = self._log_prefix(name)
        try:
            with self._begin(write=True) as txn:
                txn.put(self._snap_key(name), blob, db=self._collections)
                drop: list[bytes] = []
                with txn.cursor(db=self._collections) as curs:
                    if curs.set_range(prefix):
                        for key, _ in curs:
                            kb = bytes(key)
                            if not kb.startswith(prefix):
                                break
                            if int.from_bytes(kb[len(prefix) :], "big") <= upto_rev:
                                drop.append(kb)
                            else:
                                break
                for key in drop:
                    txn.delete(key, db=self._collections)
        except lmdb.MapFullError as exc:  # pragma: no cover
            raise PersistentCacheError(
                f"persistent cache at {self.path!r} is full; pass a larger cache_map_size "
                "to Engine, or clear the cache with engine.clear_disk_cache()."
            ) from exc

    def retain(self, wanted_entries: set[bytes], wanted_blobs: set[str]) -> None:
        with self._begin(write=True) as txn:
            drop_meta = []
            with txn.cursor(db=self._meta) as curs:
                for key, _ in curs:
                    if key not in wanted_entries:
                        drop_meta.append(key)
            for k in drop_meta:
                txn.delete(k, db=self._meta)

            drop_blobs = []
            with txn.cursor(db=self._blobs) as curs:
                for key, _ in curs:
                    if key.decode("ascii") not in wanted_blobs:
                        drop_blobs.append(key)
            for k in drop_blobs:
                txn.delete(k, db=self._blobs)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            _release_env(self._registry_key)


class SqliteDiskCache:
    def __init__(self, cache_dir: str | os.PathLike[str], *, map_size: int) -> None:
        _canonical.require_msgpack()
        import sqlite3
        
        path_dir = os.fspath(cache_dir)
        os.makedirs(path_dir, exist_ok=True)
        self._path = os.path.join(path_dir, "cache.db")
        
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        self._closed = False
        
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            
        self._ensure_format()

    @property
    def path(self) -> str:
        return self._path

    def _ensure_format(self) -> None:
        stamp = DISK_FORMAT.to_bytes(4, "big")
        with self._lock:
            self._conn.execute("begin")
            try:
                self._conn.execute("create table if not exists sys (k blob primary key, v blob)")
                self._conn.execute("create table if not exists meta (k blob primary key, v blob)")
                self._conn.execute("create table if not exists blobs (k blob primary key, v blob)")
                self._conn.execute("create table if not exists collections (k blob primary key, v blob)")
                
                row = self._conn.execute("select v from sys where k = ?", (_FORMAT_KEY,)).fetchone()
                current = row[0] if row else None
                
                if current == stamp:
                    self._conn.execute("commit")
                    return
                    
                if current is not None:
                    self._conn.execute("delete from meta")
                    self._conn.execute("delete from blobs")
                    self._conn.execute("delete from collections")
                    
                self._conn.execute("insert or replace into sys (k, v) values (?, ?)", (_FORMAT_KEY, stamp))
                self._conn.execute("commit")
            except Exception:
                self._conn.execute("rollback")
                raise

    def load_entry(self, key: bytes) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute("select v from meta where k = ?", (key,)).fetchone()
            if row is None:
                return None
            try:
                record = _canonical.decode(bytes(row[0]))
            except Exception:
                return None
            if not isinstance(record, dict):
                return None
            return record

    def load_blob(self, value_hash: str) -> bytes | None:
        with self._lock:
            row = self._conn.execute("select v from blobs where k = ?", (value_hash.encode("ascii"),)).fetchone()
            return None if row is None else bytes(row[0])

    def store_entry(
        self, key: bytes, record: dict[str, Any], value_hash: str, value_blob: bytes
    ) -> None:
        record_blob = _canonical.encode(record)
        with self._lock:
            self._conn.execute("begin")
            try:
                self._conn.execute("insert or replace into blobs (k, v) values (?, ?)", (value_hash.encode("ascii"), value_blob))
                self._conn.execute("insert or replace into meta (k, v) values (?, ?)", (key, record_blob))
                self._conn.execute("commit")
            except Exception:
                self._conn.execute("rollback")
                raise

    def store_entry_many(self, entries: list[tuple[bytes, bytes, str, bytes]]) -> None:
        if not entries:
            return
        with self._lock:
            self._conn.execute("begin")
            try:
                for key, record_blob, value_hash, value_blob in entries:
                    self._conn.execute("insert or replace into blobs (k, v) values (?, ?)", (value_hash.encode("ascii"), value_blob))
                    self._conn.execute("insert or replace into meta (k, v) values (?, ?)", (key, record_blob))
                self._conn.execute("commit")
            except Exception:
                self._conn.execute("rollback")
                raise

    def clear(self) -> None:
        with self._lock:
            self._conn.execute("begin")
            try:
                self._conn.execute("delete from meta")
                self._conn.execute("delete from blobs")
                self._conn.execute("delete from collections")
                self._conn.execute("commit")
            except Exception:
                self._conn.execute("rollback")
                raise

    @staticmethod
    def _snap_key(name: str) -> bytes:
        return b"s\x00" + name.encode("utf-8")

    @staticmethod
    def _log_prefix(name: str) -> bytes:
        return b"l\x00" + name.encode("utf-8") + b"\x00"

    def _log_key(self, name: str, rev: int) -> bytes:
        return self._log_prefix(name) + rev.to_bytes(8, "big")

    def collection_load(
        self, name: str
    ) -> tuple[dict[str, Any] | None, list[tuple[int, dict[str, Any]]]]:
        prefix = self._log_prefix(name)
        with self._lock:
            row = self._conn.execute("select v from collections where k = ?", (self._snap_key(name),)).fetchone()
            snapshot = _canonical.decode(bytes(row[0])) if row is not None else None
            
            tail: list[tuple[int, dict[str, Any]]] = []
            
            # Since SQLite orders blobs byte-by-byte (memcmp), we can just do >= and < bounds
            # prefix ends with \x00, so prefix + ÿ is the upper bound
            upper_bound = prefix[:-1] + b"\xff"
            
            cursor = self._conn.execute("select k, v from collections where k >= ? and k < ? order by k asc", (prefix, upper_bound))
            for k, v in cursor:
                if not bytes(k).startswith(prefix):
                    break
                rev = int.from_bytes(bytes(k)[len(prefix):], "big")
                tail.append((rev, _canonical.decode(bytes(v))))
                
        return snapshot, tail

    def collection_append_many(
        self, name: str, batch: list[tuple[int, dict[str, Any]]]
    ) -> None:
        blobs = [
            (self._log_key(name, rev), _canonical.encode(diff)) for rev, diff in batch
        ]
        with self._lock:
            self._conn.execute("begin")
            try:
                for k, v in blobs:
                    self._conn.execute("insert or replace into collections (k, v) values (?, ?)", (k, v))
                self._conn.execute("commit")
            except Exception:
                self._conn.execute("rollback")
                raise

    def collection_snapshot(
        self, name: str, record: dict[str, Any], upto_rev: int
    ) -> None:
        snap_blob = _canonical.encode(record)
        prefix = self._log_prefix(name)
        
        with self._lock:
            self._conn.execute("begin")
            try:
                self._conn.execute("insert or replace into collections (k, v) values (?, ?)", (self._snap_key(name), snap_blob))
                
                # Delete older log entries
                upper_bound = prefix + (upto_rev + 1).to_bytes(8, "big")
                self._conn.execute("delete from collections where k >= ? and k < ?", (prefix, upper_bound))
                
                self._conn.execute("commit")
            except Exception:
                self._conn.execute("rollback")
                raise

    def retain(self, wanted_entries: set[bytes], wanted_blobs: set[str]) -> None:
        with self._lock:
            self._conn.execute("begin")
            try:
                # Meta
                rows = self._conn.execute("select k from meta").fetchall()
                for row in rows:
                    if row[0] not in wanted_entries:
                        self._conn.execute("delete from meta where k = ?", (row[0],))
                        
                # Blobs
                wanted_blobs_bytes = {b.encode("ascii") for b in wanted_blobs}
                rows = self._conn.execute("select k from blobs").fetchall()
                for row in rows:
                    if row[0] not in wanted_blobs_bytes:
                        self._conn.execute("delete from blobs where k = ?", (row[0],))
                        
                self._conn.execute("commit")
            except Exception:
                self._conn.execute("rollback")
                raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            with self._lock:
                self._conn.close()


class DiskCacheProtocol(Protocol):
    @property
    def path(self) -> str: ...
    def load_entry(self, key: bytes) -> dict[str, Any] | None: ...
    def load_blob(self, value_hash: str) -> bytes | None: ...
    def store_entry(self, key: bytes, record: dict[str, Any], value_hash: str, value_blob: bytes) -> None: ...
    def store_entry_many(self, entries: list[tuple[bytes, bytes, str, bytes]]) -> None: ...
    def clear(self) -> None: ...
    def collection_load(self, name: str) -> tuple[dict[str, Any] | None, list[tuple[int, dict[str, Any]]]]: ...
    def collection_append_many(self, name: str, batch: list[tuple[int, dict[str, Any]]]) -> None: ...
    def collection_snapshot(self, name: str, record: dict[str, Any], upto_rev: int) -> None: ...
    def retain(self, wanted_entries: set[bytes], wanted_blobs: set[str]) -> None: ...
    def close(self) -> None: ...

def DiskCache(cache_dir: str | os.PathLike[str], *, map_size: int, cache_backend: str = "mdbx") -> DiskCacheProtocol:
    if cache_backend == "lmdb":
        return LmdbDiskCache(cache_dir, map_size=map_size)
    if cache_backend == "sqlite":
        return SqliteDiskCache(cache_dir, map_size=map_size)
    return MdbxDiskCache(cache_dir, map_size=map_size)

