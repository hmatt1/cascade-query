"""MDBX-backed persistent store for query metadata and value blobs.

Layout: one MDBX environment per ``cache_dir`` with four named databases.
``meta`` maps a deterministic entry key to a packed record describing a query
result (value fingerprint, dependency fingerprints, accumulator effects).
``blobs`` is content-addressed: value fingerprint -> canonical value bytes.
``collections`` holds the logs of named collections.
``sys`` holds two format stamps, because the two kinds of data age
differently:

* ``DISK_FORMAT`` covers ``meta`` and ``blobs``. They are a cache: a
  mismatched stamp empties them, which is always safe because every result
  can be recomputed.
* ``COLLECTION_FORMAT`` covers ``collections``. A collection's log is the
  only copy of its contents, so it is never emptied. A directory whose
  collection data this version cannot read is refused with
  ``PersistentCacheError`` and left as it is.

The sqlite backend keeps the same four tables in one database file.
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


# 2: single-pass canonical encoding (see _canonical) and inline small values.
# 3: dicts are encoded in insertion order, and value fingerprints depend on
#    it. A format 2 result holds its dicts in sorted key order, which is not
#    the order the query returned, so format 2 results are not served.
# 4: new value types (Enum, Decimal, paths, ...; see _canonical), and a
#    bytearray is stored as itself: format 3 stored it as bytes, so a result
#    that returned one came back as bytes.
DISK_FORMAT = 4

# Values whose canonical encoding is at most this many bytes are stored
# inside their meta record instead of the content-addressed blob store, so
# a cache hit needs one read instead of two. Larger values keep using
# blobs, where identical results are deduplicated.
INLINE_VALUE_MAX_BYTES = 1024
_FORMAT_KEY = b"format"

# Format of named collections' logs, versioned apart from the result cache
# so that a ``DISK_FORMAT`` bump keeps them. Bump it only together with code
# that migrates the older formats: collection data must never be dropped.
# 1: snapshot record and one diff per revision, each a canonical msgpack map.
# 2: the same, with the value types of result format 4 (Enum, Decimal, ...,
#    a bytearray as itself). Format 1 logs are read unchanged, so a directory
#    in format 1 is only restamped; a release that reads only format 1
#    refuses it instead of failing on a value it does not know.
COLLECTION_FORMAT = 2
# Collection formats this version reads.
_READABLE_COLLECTION_FORMATS = frozenset({1, 2})
_COLLECTION_FORMAT_KEY = b"collections_format"
# Directories written before collections had a stamp of their own carry only
# the result-cache stamp. This maps that stamp to the collection format the
# same code wrote. (Format 2 sorted the keys of the maps in a collection
# log; the format 3 decoder reads those unchanged, so both are collection
# format 1.)
_UNSTAMPED_COLLECTION_FORMATS = {2: 1}


def _check_collection_format(
    path: str,
    result_stamp: bytes | None,
    collection_stamp: bytes | None,
    has_collection_data: Any,
) -> None:
    """Raise unless the stored collection data is in ``COLLECTION_FORMAT``.

    Called before anything in the directory is changed.
    """
    if collection_stamp is not None:
        stored: int | None = int.from_bytes(bytes(collection_stamp), "big")
    elif result_stamp is None:
        return  # a new directory
    else:
        written_by = int.from_bytes(bytes(result_stamp), "big")
        stored = _UNSTAMPED_COLLECTION_FORMATS.get(written_by)
        if stored is None:
            if not has_collection_data():
                return
            raise PersistentCacheError(
                f"the cache at {path!r} holds named collections written in "
                f"disk format {written_by}, which this version of cascade "
                "cannot read. Nothing was changed. Collection data cannot be "
                "recomputed: open the directory with the version that wrote "
                "it, or delete the directory if the data is not needed."
            )
    if stored not in _READABLE_COLLECTION_FORMATS:
        raise PersistentCacheError(
            f"the cache at {path!r} holds named collections in collection "
            f"format {stored}; this version of cascade reads formats "
            f"{sorted(_READABLE_COLLECTION_FORMATS)}. Nothing was changed. Collection data "
            "cannot be recomputed: open the directory with a version of "
            "cascade that reads it, or delete the directory if the data is "
            "not needed."
        )


class _FileLock:
    """Exclusive, non-blocking OS-level lock on one file.

    ``fcntl.flock`` on POSIX, ``msvcrt.locking`` on Windows. The lock
    belongs to the open file: it conflicts with another holder in the same
    process as well as in another process, and the operating system drops
    it when the holder closes the file, exits or crashes. A lock file left
    behind therefore never blocks anyone.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self._fd: int | None = None

    def try_acquire(self) -> bool:
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            self._lock(fd)
        except OSError:
            os.close(fd)
            return False
        except BaseException:  # pragma: no cover
            os.close(fd)
            raise
        self._fd = fd
        return True

    if sys.platform == "win32":

        @staticmethod
        def _lock(fd: int) -> None:
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)

        @staticmethod
        def _unlock(fd: int) -> None:
            import msvcrt

            os.lseek(fd, 0, os.SEEK_SET)
            msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)

    else:

        @staticmethod
        def _lock(fd: int) -> None:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

        @staticmethod
        def _unlock(fd: int) -> None:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_UN)

    def release(self) -> None:
        fd = self._fd
        if fd is None:
            return
        self._fd = None
        try:
            self._unlock(fd)
        except OSError:  # pragma: no cover - closing releases it anyway
            pass
        finally:
            os.close(fd)


class _CollectionWriteLocks:
    """The named collections one engine may change, each under a file lock.

    Two engines that both change one named collection number their log
    entries from their own copy of its log, so they overwrite each other's
    entries on disk, and a result computed from one engine's contents
    verifies against the other's (D10). An engine therefore takes an
    exclusive lock on a collection the first time it changes it, and holds
    it until the cache is closed (``Engine.shutdown()``, or the process
    ending). Other engines can still open and read the collection, and share
    every cached query result. Lock files live in ``locks/`` inside the
    cache directory, one per collection name.
    """

    def __init__(self, cache_dir: str) -> None:
        self._dir = os.path.join(cache_dir, "locks")
        self._cache_dir = cache_dir
        self._held: dict[str, _FileLock] = {}
        self._mutex = threading.Lock()

    def acquire(self, name: str) -> bool:
        """Take the lock for ``name``; False if this engine held it already.

        Raises ``PersistentCacheError`` if another engine holds it.
        """
        with self._mutex:
            if name in self._held:
                return False
            os.makedirs(self._dir, exist_ok=True)
            digest = hashlib.blake2b(name.encode("utf-8"), digest_size=16).hexdigest()
            lock = _FileLock(os.path.join(self._dir, f"collection-{digest}.lock"))
            if not lock.try_acquire():
                raise PersistentCacheError(
                    f"named collection {name!r} in the cache directory "
                    f"{self._cache_dir!r} is being changed by another Engine, "
                    "in this process or another one. One engine at a time may "
                    "change a named collection (any number may read it): call "
                    "shutdown() on the other engine first, or give this one a "
                    "cache_dir of its own."
                )
            self._held[name] = lock
            return True

    def release(self, name: str) -> None:
        with self._mutex:
            lock = self._held.pop(name, None)
        if lock is not None:
            lock.release()

    def release_all(self) -> None:
        with self._mutex:
            held = list(self._held.values())
            self._held.clear()
        for lock in held:
            lock.release()


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
                    flags=mdbx.MDBXEnvFlags.MDBX_SAFE_NOSYNC
                    | mdbx.MDBXEnvFlags.MDBX_NOMETASYNC,
                    maxdbs=4,
                    geometry=mdbx.Geometry(size_upper=map_size),
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
        self._collection_locks = _CollectionWriteLocks(path)
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
        try:
            self._ensure_format()
        except BaseException:
            self.close()
            raise

    @property
    def path(self) -> str:
        return self._env.get_path()

    def _begin(self, write: bool = False, **kwargs: Any) -> Any:
        return TxnContext(self._env, write, kwargs)

    def _ensure_format(self) -> None:
        stamp = DISK_FORMAT.to_bytes(4, "big")
        collection_stamp = COLLECTION_FORMAT.to_bytes(4, "big")
        with self._begin(write=True) as txn:
            current = self._sys.get(txn, _FORMAT_KEY)
            current_collections = self._sys.get(txn, _COLLECTION_FORMAT_KEY)
            if current == stamp and current_collections == collection_stamp:
                return

            def has_collection_data() -> bool:
                with mdbx.Cursor(self._collections, txn) as curs:
                    for _ in curs.iter():
                        return True
                return False

            _check_collection_format(
                self.path, current, current_collections, has_collection_data
            )
            if current is not None and current != stamp:
                # Results written by another format are not decodable. They
                # can be recomputed, so drop them. Collection logs stay.
                self._meta.drop(txn, delete=False)
                self._blobs.drop(txn, delete=False)
            self._sys.put(txn, _FORMAT_KEY, stamp)
            self._sys.put(txn, _COLLECTION_FORMAT_KEY, collection_stamp)

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
    ) -> None:  # pragma: no cover
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

    def store_entry_many(
        self, entries: list[tuple[bytes, bytes, str, bytes | None]]
    ) -> None:
        if not entries:  # pragma: no cover
            return
        try:
            with self._begin(write=True) as txn:
                for key, record_blob, value_hash, value_blob in entries:
                    if value_blob is not None:
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
        """Delete every cached query result. Collection logs are kept.

        Results can be recomputed; a named collection's log is the only copy
        of its contents. Dropping the log under a live collection also left
        its later diffs on disk with nothing before them, so the next run
        restored a fragment.
        """
        with self._begin(write=True) as txn:
            self._meta.drop(txn, delete=False)
            self._blobs.drop(txn, delete=False)

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

    def collection_head(self, name: str) -> int:
        """Revision of the newest change to ``name`` on disk (0 if none)."""
        prefix = self._log_prefix(name)
        last = None
        with self._begin() as txn:
            with mdbx.Cursor(self._collections, txn) as curs:
                for key, _ in curs.iter(start_key=prefix):
                    kb = bytes(key)
                    if not kb.startswith(prefix):
                        break
                    last = kb
            if last is not None:
                return int.from_bytes(last[len(prefix) :], "big")
            raw = self._collections.get(txn, self._snap_key(name))
        if raw is None:
            return 0
        return int(_canonical.decode(bytes(raw)).get("base_rev", 0))

    def lock_collection(self, name: str) -> bool:
        return self._collection_locks.acquire(name)

    def unlock_collection(self, name: str) -> None:
        self._collection_locks.release(name)

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
            try:
                _release_env(self._registry_key)
            finally:
                self._collection_locks.release_all()


class SqliteDiskCache:
    def __init__(self, cache_dir: str | os.PathLike[str], *, map_size: int) -> None:
        _canonical.require_msgpack()
        import sqlite3

        path_dir = os.fspath(cache_dir)
        os.makedirs(path_dir, exist_ok=True)
        self._path = os.path.join(path_dir, "cache.db")

        self._lock = threading.RLock()
        self._collection_locks = _CollectionWriteLocks(path_dir)
        self._conn = sqlite3.connect(
            self._path, check_same_thread=False, isolation_level=None
        )
        self._closed = False

        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")

        try:
            self._ensure_format()
        except BaseException:
            self.close()
            raise

    @property
    def path(self) -> str:
        return self._path

    def _ensure_format(self) -> None:
        stamp = DISK_FORMAT.to_bytes(4, "big")
        collection_stamp = COLLECTION_FORMAT.to_bytes(4, "big")
        with self._lock:
            self._conn.execute("begin")
            try:
                self._conn.execute(
                    "create table if not exists sys (k blob primary key, v blob)"
                )
                self._conn.execute(
                    "create table if not exists meta (k blob primary key, v blob)"
                )
                self._conn.execute(
                    "create table if not exists blobs (k blob primary key, v blob)"
                )
                self._conn.execute(
                    "create table if not exists collections (k blob primary key, v blob)"
                )

                row = self._conn.execute(
                    "select v from sys where k = ?", (_FORMAT_KEY,)
                ).fetchone()
                current = row[0] if row else None
                row = self._conn.execute(
                    "select v from sys where k = ?", (_COLLECTION_FORMAT_KEY,)
                ).fetchone()
                current_collections = row[0] if row else None

                if current == stamp and current_collections == collection_stamp:
                    self._conn.execute("commit")
                    return

                def has_collection_data() -> bool:
                    return (
                        self._conn.execute(
                            "select 1 from collections limit 1"
                        ).fetchone()
                        is not None
                    )

                _check_collection_format(
                    self._path, current, current_collections, has_collection_data
                )
                if current is not None and current != stamp:
                    # Results written by another format are not decodable.
                    # They can be recomputed, so drop them. Collection logs
                    # stay.
                    self._conn.execute("delete from meta")
                    self._conn.execute("delete from blobs")

                self._conn.executemany(
                    "insert or replace into sys (k, v) values (?, ?)",
                    [
                        (_FORMAT_KEY, stamp),
                        (_COLLECTION_FORMAT_KEY, collection_stamp),
                    ],
                )
                self._conn.execute("commit")
            except BaseException:
                self._conn.execute("rollback")
                raise

    def load_entry(self, key: bytes) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "select v from meta where k = ?", (key,)
            ).fetchone()
            if row is None:
                return None
            try:
                record = _canonical.decode(bytes(row[0]))
            except Exception:  # pragma: no cover
                return None
            if not isinstance(record, dict):  # pragma: no cover
                return None
            return record

    def load_blob(self, value_hash: str) -> bytes | None:
        with self._lock:
            row = self._conn.execute(
                "select v from blobs where k = ?", (value_hash.encode("ascii"),)
            ).fetchone()
            return None if row is None else bytes(row[0])

    def store_entry(
        self, key: bytes, record: dict[str, Any], value_hash: str, value_blob: bytes
    ) -> None:  # pragma: no cover
        record_blob = _canonical.encode(record)
        with self._lock:
            self._conn.execute("begin")
            try:
                self._conn.execute(
                    "insert or replace into blobs (k, v) values (?, ?)",
                    (value_hash.encode("ascii"), value_blob),
                )
                self._conn.execute(
                    "insert or replace into meta (k, v) values (?, ?)",
                    (key, record_blob),
                )
                self._conn.execute("commit")
            except Exception:  # pragma: no cover
                self._conn.execute("rollback")
                raise

    def store_entry_many(
        self, entries: list[tuple[bytes, bytes, str, bytes | None]]
    ) -> None:
        if not entries:  # pragma: no cover
            return
        with self._lock:
            self._conn.execute("begin")
            try:
                for key, record_blob, value_hash, value_blob in entries:
                    if value_blob is not None:
                        self._conn.execute(
                            "insert or replace into blobs (k, v) values (?, ?)",
                            (value_hash.encode("ascii"), value_blob),
                        )
                    self._conn.execute(
                        "insert or replace into meta (k, v) values (?, ?)",
                        (key, record_blob),
                    )
                self._conn.execute("commit")
            except Exception:  # pragma: no cover
                self._conn.execute("rollback")
                raise

    def clear(self) -> None:
        """Delete every cached query result. Collection logs are kept."""
        with self._lock:
            self._conn.execute("begin")
            try:
                self._conn.execute("delete from meta")
                self._conn.execute("delete from blobs")
                self._conn.execute("commit")
            except Exception:  # pragma: no cover
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
            row = self._conn.execute(
                "select v from collections where k = ?", (self._snap_key(name),)
            ).fetchone()
            snapshot = _canonical.decode(bytes(row[0])) if row is not None else None

            tail: list[tuple[int, dict[str, Any]]] = []

            # Since SQLite orders blobs byte-by-byte (memcmp), we can just do >= and < bounds
            # prefix ends with \x00, so prefix + ÿ is the upper bound
            upper_bound = prefix[:-1] + b"\xff"

            cursor = self._conn.execute(
                "select k, v from collections where k >= ? and k < ? order by k asc",
                (prefix, upper_bound),
            )
            for k, v in cursor:
                if not bytes(k).startswith(prefix):  # pragma: no cover
                    break
                rev = int.from_bytes(bytes(k)[len(prefix) :], "big")
                tail.append((rev, _canonical.decode(bytes(v))))

        return snapshot, tail

    def collection_head(self, name: str) -> int:
        """Revision of the newest change to ``name`` on disk (0 if none)."""
        prefix = self._log_prefix(name)
        with self._lock:
            row = self._conn.execute(
                "select max(k) from collections where k >= ? and k < ?",
                (prefix, prefix[:-1] + b"\xff"),
            ).fetchone()
            if row is not None and row[0] is not None:
                return int.from_bytes(bytes(row[0])[len(prefix) :], "big")
            row = self._conn.execute(
                "select v from collections where k = ?", (self._snap_key(name),)
            ).fetchone()
        if row is None:
            return 0
        return int(_canonical.decode(bytes(row[0])).get("base_rev", 0))

    def lock_collection(self, name: str) -> bool:
        return self._collection_locks.acquire(name)

    def unlock_collection(self, name: str) -> None:
        self._collection_locks.release(name)

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
                    self._conn.execute(
                        "insert or replace into collections (k, v) values (?, ?)",
                        (k, v),
                    )
                self._conn.execute("commit")
            except Exception:  # pragma: no cover
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
                self._conn.execute(
                    "insert or replace into collections (k, v) values (?, ?)",
                    (self._snap_key(name), snap_blob),
                )

                # Delete older log entries
                upper_bound = prefix + (upto_rev + 1).to_bytes(8, "big")
                self._conn.execute(
                    "delete from collections where k >= ? and k < ?",
                    (prefix, upper_bound),
                )

                self._conn.execute("commit")
            except Exception:  # pragma: no cover
                self._conn.execute("rollback")
                raise

    def retain(self, wanted_entries: set[bytes], wanted_blobs: set[str]) -> None:
        with self._lock:
            self._conn.execute("begin")
            try:
                # Meta
                rows = self._conn.execute("select k from meta").fetchall()
                for row in rows:
                    if row[0] not in wanted_entries:  # pragma: no cover
                        self._conn.execute("delete from meta where k = ?", (row[0],))

                # Blobs
                wanted_blobs_bytes = {b.encode("ascii") for b in wanted_blobs}
                rows = self._conn.execute("select k from blobs").fetchall()
                for row in rows:
                    if row[0] not in wanted_blobs_bytes:  # pragma: no cover
                        self._conn.execute("delete from blobs where k = ?", (row[0],))

                self._conn.execute("commit")
            except Exception:  # pragma: no cover
                self._conn.execute("rollback")
                raise

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                with self._lock:
                    self._conn.close()
            finally:
                self._collection_locks.release_all()


class DiskCacheProtocol(Protocol):
    @property
    def path(self) -> str: ...
    def load_entry(self, key: bytes) -> dict[str, Any] | None: ...
    def load_blob(self, value_hash: str) -> bytes | None: ...
    def store_entry(
        self, key: bytes, record: dict[str, Any], value_hash: str, value_blob: bytes
    ) -> None: ...
    def store_entry_many(
        self, entries: list[tuple[bytes, bytes, str, bytes | None]]
    ) -> None: ...
    def clear(self) -> None: ...
    def collection_load(
        self, name: str
    ) -> tuple[dict[str, Any] | None, list[tuple[int, dict[str, Any]]]]: ...
    def collection_append_many(
        self, name: str, batch: list[tuple[int, dict[str, Any]]]
    ) -> None: ...
    def collection_head(self, name: str) -> int: ...
    def lock_collection(self, name: str) -> bool: ...
    def unlock_collection(self, name: str) -> None: ...
    def collection_snapshot(
        self, name: str, record: dict[str, Any], upto_rev: int
    ) -> None: ...
    def retain(self, wanted_entries: set[bytes], wanted_blobs: set[str]) -> None: ...
    def close(self) -> None: ...


def DiskCache(
    cache_dir: str | os.PathLike[str], *, map_size: int, cache_backend: str = "mdbx"
) -> DiskCacheProtocol:
    if cache_backend == "sqlite":
        return SqliteDiskCache(cache_dir, map_size=map_size)
    if cache_backend == "mdbx":
        return MdbxDiskCache(cache_dir, map_size=map_size)
    raise ValueError(
        f"unknown cache_backend {cache_backend!r}; expected 'mdbx' or 'sqlite'"
    )
