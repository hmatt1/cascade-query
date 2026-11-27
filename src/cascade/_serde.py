"""Canonical JSON encoding used for stable value hashing, plus bytecode hashing.

The digest tells apart everything a query could observe in a value: types,
items, and the insertion order of dicts. Sets have no order and are hashed
in a canonical one. ``_canonical`` follows the same rule for the disk cache.
"""

from __future__ import annotations

import base64
import collections
import dataclasses
import datetime
import decimal
import enum
import fractions
import hashlib
import json
import math
import reprlib
import uuid
from typing import Any, Callable, Iterator

from . import _canonical, _types
from ._errors import UnencodableValueError

# What json.dumps(..., ensure_ascii=False) emits for a str.
_encode_json_str = json.encoder.encode_basestring


def _hash_const(const: Any, hash_code: Callable[[Any], bytes]) -> bytes:
    """Deterministic digest for one entry in ``code.co_consts``.

    ``repr`` on a ``frozenset`` (or a ``tuple`` holding one) iterates in hash
    order, which varies with ``PYTHONHASHSEED`` across processes. CPython puts
    ``frozenset`` constants into ``co_consts`` for constant membership tests
    (``x in ("a", "b")``) and for every ``@dataclass(frozen=True)``'s generated
    ``__setattr__`` / ``__delattr__``.
    """
    if hasattr(const, "co_code"):
        return hashlib.sha256(b"code:" + hash_code(const)).digest()
    t = type(const)
    if t is tuple:
        h = hashlib.sha256(b"tuple:")
        for item in const:
            h.update(_hash_const(item, hash_code))
        return h.digest()
    if t is frozenset or t is set:
        tag = b"frozenset:" if t is frozenset else b"set:"
        h = hashlib.sha256(tag)
        for digest in sorted(_hash_const(item, hash_code) for item in const):
            h.update(digest)
        return h.digest()
    return hashlib.sha256(
        t.__name__.encode("utf-8") + b":" + repr(const).encode("utf-8")
    ).digest()


def hash_bytecode(fn: Any) -> str:
    """Recursively compute a stable hash of a function's bytecode logic."""
    if not hasattr(fn, "__code__"):
        return ""

    def _hash_code(c: Any) -> bytes:
        h = hashlib.sha256()
        h.update(c.co_code)
        for const in c.co_consts:
            h.update(_hash_const(const, _hash_code))
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
    reg = _types.registration_for(type(obj))
    if reg is not None:
        return {
            "__registered__": [
                reg.name,
                _to_jsonable(_types.encode_registered(reg, obj)),
            ]
        }
    if type(obj) is bytearray:
        return {"__bytearray__": base64.standard_b64encode(bytes(obj)).decode("ascii")}
    if isinstance(obj, datetime.datetime):
        if obj.fold:
            return {"__datetime__": obj.isoformat(), "fold": 1}
        return {"__datetime__": obj.isoformat()}
    if isinstance(obj, datetime.date):
        return {"__date__": obj.isoformat()}
    if isinstance(obj, datetime.time):
        if obj.fold:
            return {"__time__": obj.isoformat(), "fold": 1}
        return {"__time__": obj.isoformat()}
    if isinstance(obj, datetime.timedelta):
        return {"__timedelta__": [obj.days, obj.seconds, obj.microseconds]}
    if isinstance(obj, datetime.timezone):
        return {"__timezone__": [_to_jsonable(obj.utcoffset(None)), obj.tzname(None)]}
    t = type(obj)
    if isinstance(obj, enum.Enum):
        by_name, payload = _canonical.enum_payload(obj)
        return {
            "__enum__": {
                "m": t.__module__,
                "q": t.__qualname__,
                "n" if by_name else "v": payload if by_name else _to_jsonable(payload),
            }
        }
    if t is decimal.Decimal:
        return {"__decimal__": str(obj)}
    if t is fractions.Fraction:
        return {"__fraction__": [obj.numerator, obj.denominator]}
    if t is complex:
        return {"__complex__": [_encode_float(obj.real), _encode_float(obj.imag)]}
    if t is uuid.UUID:
        return {"__uuid__": obj.hex}
    if t in _canonical.PATH_CLASSES:
        return {"__path__": [_canonical.PATH_CLASSES.index(t), str(obj)]}
    if t is range:
        return {"__range__": [obj.start, obj.stop, obj.step]}
    if t is collections.deque:
        return {"__deque__": [obj.maxlen, [_to_jsonable(x) for x in obj]]}
    if t.__module__ == "numpy":
        array = _canonical.numpy_array_parts(obj)
        if array is not None:
            dtype, shape, data = array
            return {"__ndarray__": [dtype, shape, _b64(data)]}
        scalar = _canonical.numpy_scalar_parts(obj)
        if scalar is not None:
            return {"__npscalar__": [scalar[0], _b64(scalar[1])]}

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
        "register the class with cascade.register_type() to make it encodable"
    )


def _b64(data: bytes) -> str:
    return base64.standard_b64encode(data).decode("ascii")


SUPPORTED_TYPES = (
    "None, bool, int, float, complex, str, bytes, bytearray, list, tuple, "
    "set, frozenset, dict, deque, range, Decimal, Fraction, UUID, Enum "
    "members, pathlib paths, the datetime types, dataclasses, NamedTuples, "
    "exceptions, numpy arrays and scalars (when numpy is installed), and "
    "classes registered with cascade.register_type()"
)


def _children(obj: Any) -> Iterator[tuple[str, Any]]:
    """The parts of ``obj`` the encoders visit, each with its path suffix."""
    reg = _types.registration_for(type(obj))
    if reg is not None:
        try:
            encoded = reg.encode(obj)
        except Exception:  # noqa: BLE001 - reported as the object itself
            return
        yield " (as encoded)", encoded
    elif isinstance(obj, enum.Enum):
        by_name, payload = _canonical.enum_payload(obj)
        if not by_name:
            yield ".value", payload
    elif isinstance(obj, collections.deque):
        for i, item in enumerate(obj):
            yield f"[{i}]", item
    elif isinstance(obj, tuple) and hasattr(obj, "_fields"):
        for name in obj._fields:
            yield f".{name}", getattr(obj, name)
    elif dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        for name in obj.__dataclass_fields__:
            yield f".{name}", getattr(obj, name)
    elif isinstance(obj, (tuple, list)):
        for i, item in enumerate(obj):
            yield f"[{i}]", item
    elif isinstance(obj, (set, frozenset)):
        for item in obj:
            yield f" element {reprlib.repr(item)}", item
    elif isinstance(obj, dict):
        for k, v in obj.items():
            yield f" key {reprlib.repr(k)}", k
            yield f"[{reprlib.repr(k)}]", v
    elif isinstance(obj, BaseException):
        for i, item in enumerate(obj.args):
            yield f".args[{i}]", item


def locate_unencodable(
    value: Any, digest: Callable[[Any], str], root: str
) -> tuple[str, type] | None:
    """Path and type of the innermost part of ``value`` that ``digest`` refuses.

    Only called after ``digest(value)`` raised, so its cost does not matter.
    """

    def refused(v: Any) -> bool:
        try:
            digest(v)
        except TypeError:
            return True
        except Exception:  # noqa: BLE001 - not an encoding question
            return False
        return False

    if not refused(value):
        return None
    path, obj = root, value
    while True:
        for suffix, child in _children(obj):
            if refused(child):
                path += suffix
                obj = child
                break
        else:
            return path, type(obj)


def unencodable_error(
    subject: str, root: str, value: Any, digest: Callable[[Any], str], exc: Exception
) -> UnencodableValueError:
    """The error for a ``value`` of ``subject`` that ``digest`` refused with ``exc``."""
    found = locate_unencodable(value, digest, root)
    if found is None:  # pragma: no cover - digest(value) did not fail again
        where = f"{root} cannot be encoded"
    else:
        path, bad_type = found
        where = (
            f"{path} is a {bad_type.__module__}.{bad_type.__qualname__}, "
            "which Cascade cannot encode"
        )
    return UnencodableValueError(
        f"{subject}: {where}. Results and input values are encoded to tell "
        "whether they changed and to store them; supported types are "
        f"{SUPPORTED_TYPES}. (Codec error: {exc})"
    )
