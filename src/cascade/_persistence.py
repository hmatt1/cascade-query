from __future__ import annotations

import sqlite3
import os
from typing import Any, Literal

from ._serde import dumps_payload, loads_payload
from ._errors import PersistentCacheError


def save_payload(
    path: str, payload: dict[str, Any], backend: Literal["sqlite", "mdbx"] = "sqlite"
) -> None:
    blob = dumps_payload(payload)

    if backend == "sqlite":
        conn = sqlite3.connect(path)
        try:
            conn.execute(
                "create table if not exists cascade_state (id integer primary key, payload blob not null)"
            )
            conn.execute("delete from cascade_state")
            conn.execute(
                "insert into cascade_state(id, payload) values (1, ?)", (blob,)
            )
            conn.commit()
        finally:
            conn.close()

    elif backend == "mdbx":
        try:
            import mdbx
        except ImportError:  # pragma: no cover
            raise PersistentCacheError("libmdbx is required for mdbx backend")
        os.makedirs(path, exist_ok=True)
        env = mdbx.Env(path, maxdbs=1, flags=mdbx.MDBXEnvFlags.MDBX_SAFE_NOSYNC)
        try:
            with env.start_transaction() as txn:
                db = txn.create_map(b"snapshot")
                db.put(txn, b"state", blob)
                txn.commit()
        finally:
            env.close()

    else:  # pragma: no cover
        raise ValueError(f"Unknown backend: {backend}")


def load_payload(
    path: str, backend: Literal["sqlite", "mdbx"] = "sqlite"
) -> dict[str, Any] | None:
    if not os.path.exists(path):  # pragma: no cover
        return None

    blob: bytes | None = None

    if backend == "sqlite":
        if os.path.isdir(path):  # pragma: no cover
            return None
        conn = sqlite3.connect(path)
        try:
            row = conn.execute(
                "select payload from cascade_state where id = 1"
            ).fetchone()
            if row is None:
                return None
            blob = row[0]
        finally:
            conn.close()

    elif backend == "mdbx":
        if not os.path.isdir(path):  # pragma: no cover
            return None
        try:
            import mdbx
        except ImportError:  # pragma: no cover
            raise PersistentCacheError("libmdbx is required for mdbx backend")

        try:
            env = mdbx.Env(path, maxdbs=1, flags=mdbx.MDBXEnvFlags.MDBX_RDONLY)
        except Exception:  # pragma: no cover
            return None

        try:
            with env.start_transaction(flags=mdbx.MDBXTXNFlags.MDBX_TXN_RDONLY) as txn:
                try:
                    db = txn.open_map(b"snapshot")
                    raw = db.get(txn, b"state")
                    blob = bytes(raw) if raw is not None else None
                except Exception:  # pragma: no cover
                    return None
        finally:
            env.close()

    else:  # pragma: no cover
        raise ValueError(f"Unknown backend: {backend}")

    if blob is None:  # pragma: no cover
        return None
    return loads_payload(blob)
