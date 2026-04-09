"""Canonical JSON-compatible encoding for persistence and stable value hashing.

Avoids pickle for safer load paths and deterministic, inspectable on-disk state.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import importlib
import json
import math
import types
from typing import Any

from ._state import Dependency, InputVersion, MemoEntry, TraceEvent

PERSISTENCE_FORMAT = 1

# Used for canonical key ordering; kept separate so tests can monkeypatch `json.dumps`
# on this module without breaking recursive sorting inside `_to_jsonable`.
_json_dumps = json.dumps


def _resolve_type(module_name: str, qualname: str) -> type[Any]:
    mod = importlib.import_module(module_name)
    cur: Any = mod
    for part in qualname.split("."):
        cur = getattr(cur, part)
    if not isinstance(cur, type):
        raise TypeError(f"{module_name}.{qualname!r} is not a type")
    return cur


def stable_value_digest(value: Any) -> str:
    """Blake2b hex digest of the canonical JSON form of ``value``."""
    blob = json.dumps(
        _to_jsonable(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.blake2b(blob, digest_size=20).hexdigest()


def dumps_payload(payload: dict[str, Any]) -> bytes:
    """Serialize a graph-store persistence dict to UTF-8 JSON bytes."""
    envelope = {"format": PERSISTENCE_FORMAT, "payload": _to_jsonable(payload)}
    return json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")


def loads_payload(blob: bytes) -> dict[str, Any]:
    """Decode ``dumps_payload`` output back to a persistence dict."""
    text = blob.decode("utf-8")
    envelope = json.loads(text)
    if not isinstance(envelope, dict):
        raise ValueError("persistence envelope must be a JSON object")
    fmt = envelope.get("format")
    if fmt != PERSISTENCE_FORMAT:
        raise ValueError(f"unsupported persistence format: {fmt!r}")
    body = envelope.get("payload")
    if body is None:
        raise ValueError("missing payload field")
    raw = _from_jsonable(body)
    if not isinstance(raw, dict):
        raise ValueError("payload must decode to a dict")
    return raw  # type: ignore[return-value]


def _sort_key(obj: Any) -> str:
    return _json_dumps(
        _to_jsonable(obj),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _encode_float(f: float) -> dict[str, Any]:
    if math.isnan(f):
        return {"__float__": "nan", "sign": math.copysign(1.0, f)}
    if math.isinf(f):
        return {"__float__": "inf", "sign": 1.0 if f > 0 else -1.0}
    return {"__float__": "h", "v": float.hex(f)}


def _decode_float(d: dict[str, Any]) -> float:
    tag = d["__float__"]
    if tag == "nan":
        return float(math.copysign(float("nan"), d["sign"]))
    if tag == "inf":
        s = float(d["sign"])
        return float("inf") if s > 0 else float("-inf")
    assert tag == "h"
    return float.fromhex(d["v"])


def _to_jsonable(obj: Any) -> Any:
    if obj is None or obj is True or obj is False:
        return obj
    if type(obj) is int:
        return obj
    if type(obj) is float:
        return _encode_float(obj)
    if type(obj) is str:
        return obj
    if type(obj) is bytes:
        return {"__bytes__": base64.standard_b64encode(obj).decode("ascii")}
    if type(obj) is bytearray:
        return {"__bytes__": base64.standard_b64encode(bytes(obj)).decode("ascii")}

    if isinstance(obj, TraceEvent):
        return {
            "__TraceEvent__": {
                "event": obj.event,
                "key": obj.key,
                "revision": obj.revision,
                "detail": obj.detail,
                "timestamp": _to_jsonable(obj.timestamp),
            }
        }
    if isinstance(obj, Dependency):
        return {"__Dependency__": {"key": _to_jsonable(obj.key), "observed_changed_at": obj.observed_changed_at}}
    if isinstance(obj, InputVersion):
        return {
            "__InputVersion__": {
                "revision": obj.revision,
                "changed_at": obj.changed_at,
                "value_hash": obj.value_hash,
                "value": _to_jsonable(obj.value),
            }
        }
    if isinstance(obj, MemoEntry):
        return {
            "__MemoEntry__": {
                "value": _to_jsonable(obj.value),
                "value_hash": obj.value_hash,
                "changed_at": obj.changed_at,
                "verified_at": obj.verified_at,
                "deps": _to_jsonable(list(obj.deps)),
                "effects": _to_jsonable(obj.effects),
                "last_access": obj.last_access,
            }
        }

    if isinstance(obj, tuple) and hasattr(obj, "_fields"):
        return {
            "__namedtuple__": {
                "m": type(obj).__module__,
                "q": type(obj).__qualname__,
                "f": [_to_jsonable(getattr(obj, f)) for f in obj._fields],
            }
        }
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        names = tuple(sorted(obj.__dataclass_fields__.keys()))
        return {
            "__dataclass__": {
                "m": type(obj).__module__,
                "q": type(obj).__qualname__,
                "f": [[n, _to_jsonable(getattr(obj, n))] for n in names],
            }
        }

    if isinstance(obj, tuple):
        return {"__tuple__": [_to_jsonable(x) for x in obj]}
    if isinstance(obj, list):
        return {"__list__": [_to_jsonable(x) for x in obj]}
    if isinstance(obj, frozenset):
        items = sorted((_sort_key(x), _to_jsonable(x)) for x in obj)
        return {"__frozenset__": [v for _, v in items]}
    if isinstance(obj, set):
        items = sorted((_sort_key(x), _to_jsonable(x)) for x in obj)
        return {"__set__": [v for _, v in items]}
    if isinstance(obj, dict):
        pairs: list[list[Any]] = []
        for k in sorted(obj.keys(), key=_sort_key):
            pairs.append([_to_jsonable(k), _to_jsonable(obj[k])])
        return {"__map__": pairs}

    raise TypeError(
        f"cascade serde: unsupported type {type(obj).__module__}.{type(obj).__qualname__!r}; "
        "use primitives, bytes, collections, @dataclass / typing.NamedTuple instances, or "
        "cascade state types (MemoEntry, InputVersion, …)."
    )


def _from_jsonable(obj: Any) -> Any:
    if obj is None or obj is True or obj is False:
        return obj
    if type(obj) is int:
        return obj
    if type(obj) is float:
        return obj
    if type(obj) is str:
        return obj

    if not isinstance(obj, dict):
        raise TypeError(f"cascade serde: expected dict, list, or scalar at this position, got {type(obj)!r}")

    if "__float__" in obj:
        return _decode_float(obj)

    if len(obj) == 1:
        sole_key = next(iter(obj))
        sole_val = obj[sole_key]
        if sole_key == "__bytes__":
            return base64.standard_b64decode(str(sole_val))
        if sole_key == "__tuple__":
            return tuple(_from_jsonable(x) for x in sole_val)
        if sole_key == "__list__":
            return [_from_jsonable(x) for x in sole_val]
        if sole_key == "__frozenset__":
            return frozenset(_from_jsonable(x) for x in sole_val)
        if sole_key == "__set__":
            return {_from_jsonable(x) for x in sole_val}
        if sole_key == "__map__":
            out: dict[Any, Any] = {}
            for pair in sole_val:
                if not isinstance(pair, list) or len(pair) != 2:
                    raise ValueError("invalid __map__ entry")
                out[_from_jsonable(pair[0])] = _from_jsonable(pair[1])
            return out
        if sole_key == "__TraceEvent__":
            d = sole_val
            return TraceEvent(
                event=d["event"],
                key=d["key"],
                revision=d["revision"],
                detail=d["detail"],
                timestamp=_from_jsonable(d["timestamp"]),
            )
        if sole_key == "__Dependency__":
            d = sole_val
            return Dependency(key=_from_jsonable(d["key"]), observed_changed_at=d["observed_changed_at"])
        if sole_key == "__InputVersion__":
            d = sole_val
            return InputVersion(
                revision=d["revision"],
                changed_at=d["changed_at"],
                value_hash=d["value_hash"],
                value=_from_jsonable(d["value"]),
            )
        if sole_key == "__MemoEntry__":
            d = sole_val
            deps_raw = _from_jsonable(d["deps"])
            if not isinstance(deps_raw, list):
                raise ValueError("MemoEntry.deps must decode to a list")
            return MemoEntry(
                value=_from_jsonable(d["value"]),
                value_hash=d["value_hash"],
                changed_at=d["changed_at"],
                verified_at=d["verified_at"],
                deps=tuple(deps_raw),
                effects=_from_jsonable(d["effects"]),
                last_access=d["last_access"],
            )
        if sole_key == "__dataclass__":
            d = sole_val
            fields = {pair[0]: _from_jsonable(pair[1]) for pair in d["f"]}
            try:
                cls = _resolve_type(d["m"], d["q"])
                if dataclasses.is_dataclass(cls) and isinstance(cls, type):
                    return cls(**fields)
            except Exception:
                pass
            return types.SimpleNamespace(**fields)
        if sole_key == "__namedtuple__":
            d = sole_val
            vals = [_from_jsonable(x) for x in d["f"]]
            try:
                cls = _resolve_type(d["m"], d["q"])
                if isinstance(cls, type) and issubclass(cls, tuple) and hasattr(cls, "_make"):
                    return cls(*vals)
            except Exception:
                pass
            return tuple(vals)

    raise TypeError(f"cascade serde: unrecognized object shape: {obj!r}")
