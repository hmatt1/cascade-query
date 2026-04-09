from __future__ import annotations

import pickle
import sqlite3
from typing import Any


def save_payload(path: str, payload: dict[str, Any]) -> None:
    blob = pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL)
    conn = sqlite3.connect(path)
    try:
        conn.execute("create table if not exists cascade_state (id integer primary key, payload blob not null)")
        conn.execute("delete from cascade_state")
        conn.execute("insert into cascade_state(id, payload) values (1, ?)", (blob,))
        conn.commit()
    finally:
        conn.close()


def load_payload(path: str) -> dict[str, Any] | None:
    conn = sqlite3.connect(path)
    try:
        row = conn.execute("select payload from cascade_state where id = 1").fetchone()
        if row is None:
            return None
        return pickle.loads(row[0])
    finally:
        conn.close()
