"""Canonical JSON encoding used for stable value hashing, plus bytecode hashing.

The digest tells apart everything a query could observe in a value: types,
items, and the insertion order of dicts. Sets have no order and are hashed
in a canonical one. ``_canonical`` follows the same rule for the disk cache.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime
import hashlib
import json
import math
from typing import Any

# What json.dumps(..., ensure_ascii=False) emits for a str.
_encode_json_str = json.encoder.encode_basestring


def hash_bytecode(fn: Any) -> str:
    """Recursively compute a stable hash of a function's bytecode logic."""
    if not hasattr(fn, "__code__"):
        return ""

    def _hash_code(c: Any) -> bytes:
        h = hashlib.sha256()
        h.update(c.co_code)
        for const in c.co_consts:
            if hasattr(const, "co_code"):
                h.update(_hash_code(const))
            else:
                h.update(type(const).__name__.encode("utf-8"))
                h.update(repr(const).encode("utf-8"))
        for name in c.co_names:
            h.update(name.encode("utf-8"))
        for varname in c.co_varnames:
            h.update(varname.encode("utf-8"))
        return h.digest()

    return _hash_code(fn.__code__).hex()


_JSON_SCALAR_TEXT: dict[Any, str] = {None: "null", True: "true", False: "false"}


def _scalar_json_text(value: Any) -> str | None:
    """Canonical JSON text for common scalars, identical to the general path."""
    t = type(value)
    if t is int:
        return int.__repr__(value)
    if t is str:
        return _encode_json_str(value)
    if value is None or value is True or value is False:
        return _JSON_SCALAR_TEXT[value]
    return None


def stable_value_digest(value: Any) -> str:
    """Blake2b hex digest of the canonical JSON form of ``value``."""
    text = _scalar_json_text(value)
    if text is not None:
        return hashlib.blake2b(text.encode("utf-8"), digest_size=20).hexdigest()
    # No sort_keys: the dicts _to_jsonable builds have a fixed key order,
    # and a user's dict is a ``__map__`` list of pairs in insertion order.
    blob = json.dumps(
        _to_jsonable(value),
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.blake2b(blob, digest_size=20).hexdigest()


def _sort_key(obj: Any) -> str:
    return json.dumps(
        _to_jsonable(obj),
        separators=(",", ":"),
        ensure_ascii=False,
    )


def _encode_float(f: float) -> dict[str, Any]:
    if math.isnan(f):
        return {"__float__": "nan", "sign": math.copysign(1.0, f)}
    if math.isinf(f):
        return {"__float__": "inf", "sign": 1.0 if f > 0 else -1.0}
    return {"__float__": "h", "v": float.hex(f)}


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
    if isinstance(obj, datetime.datetime):
        return {"__datetime__": obj.isoformat()}
    if isinstance(obj, datetime.date):
        return {"__date__": obj.isoformat()}
    if isinstance(obj, datetime.time):
        return {"__time__": obj.isoformat()}
    if isinstance(obj, datetime.timedelta):
        return {"__timedelta__": [obj.days, obj.seconds, obj.microseconds]}
    if isinstance(obj, datetime.timezone):
        return {"__timezone__": [_to_jsonable(obj.utcoffset(None)), obj.tzname(None)]}

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
        # In insertion order: a dependent that iterates the dict sees it.
        return {"__map__": [[_to_jsonable(k), _to_jsonable(v)] for k, v in obj.items()]}

    if isinstance(obj, BaseException):
        return {
            "__exception__": {
                "m": type(obj).__module__,
                "q": type(obj).__qualname__,
                "a": [_to_jsonable(x) for x in obj.args],
            }
        }

    raise TypeError(
        f"cascade serde: unsupported type {type(obj).__module__}.{type(obj).__qualname__!r}; "
        "use primitives, bytes, collections, datetime, @dataclass / typing.NamedTuple instances, "
        "or BaseException."
    )
