"""Deterministic msgpack encoding and hashing for the persistent disk cache.

Two values produce identical byte streams exactly when no Python code could
tell them apart by their contents: same types, same items, and for dicts the
same insertion order. Set elements have no order to observe and are emitted
in a canonical one. A dict's insertion order is part of its value: a query
that iterates the dict sees it, so ``{"a": 1, "b": 2}`` and
``{"b": 2, "a": 1}``, though ``==``, are different results with different
fingerprints, and a decoded dict has the order it was encoded with.

Fingerprints are blake2b digests of those byte streams. msgpack is imported
lazily so the in-memory engine keeps working without it; enabling
``cache_dir`` requires it.

Wire format (disk format 3; format 2 differed only in sorting dict keys)
------------------------------------------------------------------------
Values msgpack cannot represent natively (tuples, sets, dataclasses, ...) are
written as an ordinary msgpack array whose first element is a zero-length
extension marker carrying the type code::

    (1, "a")      ->  [ext(TUPLE, b""), 1, "a"]
    {3, 1}        ->  [ext(SET, b""), 1, 3]

A plain ``list`` can never start with such a marker, so the form is
unambiguous. Because tagged values are ordinary arrays, a whole value is
packed with a single ``packb`` call and unpacked with a single ``unpackb``
(the unpacker invokes ``_list_hook`` as each array closes).
"""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import importlib
from typing import Any

from ._errors import PersistentCacheError

try:
    import msgpack
except ImportError:  # pragma: no cover - exercised via monkeypatch in tests
    msgpack = None  # type: ignore[assignment]

MSGPACK_INSTALL_HINT = (
    "cascade persistent caching requires the 'msgpack' package for deterministic "
    "serialization; there is no fallback. Install it with: pip install msgpack"
)

_EXT_TUPLE = 1
_EXT_SET = 2
_EXT_FROZENSET = 3
_EXT_MAP = 4  # dict with at least one non-str key
_EXT_BIGINT = 5  # int outside the native msgpack 64-bit range
_EXT_DATACLASS = 6
_EXT_NAMEDTUPLE = 7
_EXT_EXCEPTION = 8
_EXT_DATETIME = 9
_EXT_DATE = 10
_EXT_TIME = 11
_EXT_TIMEDELTA = 12
_EXT_TIMEZONE = 13

_INT64_MIN = -(2**63)
_UINT64_MAX = 2**64 - 1

_PASSTHROUGH_SCALARS = (str, bytes, float, bool, type(None))


class _Tag:
    """Decoded form of a zero-length extension marker."""

    __slots__ = ("code",)

    def __init__(self, code: int) -> None:
        self.code = code


_TAGS: dict[int, _Tag] = {code: _Tag(code) for code in range(1, 14)}
# Encode-side markers, built on first use because msgpack is optional.
_MARKERS: dict[int, Any] = {}


def require_msgpack() -> Any:
    if msgpack is None:
        raise PersistentCacheError(MSGPACK_INSTALL_HINT)
    return msgpack


def _marker(code: int) -> Any:
    marker = _MARKERS.get(code)
    if marker is None:
        marker = _MARKERS[code] = msgpack.ExtType(code, b"")
    return marker


def encode(value: Any) -> bytes:
    """Canonical msgpack bytes for ``value``; see the module docstring."""
    mp = require_msgpack()
    return mp.packb(_encode_node(value), use_bin_type=True)


def decode(blob: bytes) -> Any:
    """Inverse of :func:`encode`."""
    mp = require_msgpack()
    return mp.unpackb(
        blob,
        raw=False,
        use_list=True,
        strict_map_key=False,
        ext_hook=_ext_hook,
        list_hook=_list_hook,
    )


def digest_bytes(blob: bytes) -> str:
    return hashlib.blake2b(blob, digest_size=20).hexdigest()


def value_digest(value: Any) -> str:
    """Blake2b hex fingerprint of the canonical byte stream of ``value``."""
    t = type(value)
    if t in _PASSTHROUGH_SCALARS or (t is int and _INT64_MIN <= value <= _UINT64_MAX):
        # Same bytes encode() would produce: _encode_node returns these as-is.
        return digest_bytes(require_msgpack().packb(value, use_bin_type=True))
    return digest_bytes(encode(value))


def _packed(node: Any) -> bytes:
    return msgpack.packb(node, use_bin_type=True)


def _encode_node(obj: Any) -> Any:
    t = type(obj)
    # Exact-type fast paths first: these cover nearly every node in practice.
    if t in _PASSTHROUGH_SCALARS:
        return obj
    if t is int:
        if _INT64_MIN <= obj <= _UINT64_MAX:
            return obj
        magnitude = abs(obj)
        return [
            _marker(_EXT_BIGINT),
            obj < 0,
            magnitude.to_bytes((magnitude.bit_length() + 7) // 8 or 1, "big"),
        ]
    if t is tuple:
        return [_marker(_EXT_TUPLE), *[_encode_node(x) for x in obj]]
    if t is list:
        return [_encode_node(x) for x in obj]
    if t is dict:
        return _encode_dict(obj)

    if t is bytearray:
        return bytes(obj)
    if isinstance(obj, datetime.datetime):
        return [_marker(_EXT_DATETIME), obj.isoformat()]
    if isinstance(obj, datetime.date):
        return [_marker(_EXT_DATE), obj.isoformat()]
    if isinstance(obj, datetime.time):
        return [_marker(_EXT_TIME), obj.isoformat()]
    if isinstance(obj, datetime.timedelta):
        return [_marker(_EXT_TIMEDELTA), obj.days, obj.seconds, obj.microseconds]
    if isinstance(obj, datetime.timezone):
        td = obj.utcoffset(None)
        return [
            _marker(_EXT_TIMEZONE),
            td.days,
            td.seconds,
            td.microseconds,
            obj.tzname(None),
        ]

    if isinstance(obj, tuple) and hasattr(obj, "_fields"):
        return [
            _marker(_EXT_NAMEDTUPLE),
            t.__module__,
            t.__qualname__,
            *[_encode_node(getattr(obj, f)) for f in obj._fields],
        ]
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        node: list[Any] = [_marker(_EXT_DATACLASS), t.__module__, t.__qualname__]
        for name in sorted(obj.__dataclass_fields__.keys()):
            node.append(name)
            node.append(_encode_node(getattr(obj, name)))
        return node

    if isinstance(obj, tuple):
        return [_marker(_EXT_TUPLE), *[_encode_node(x) for x in obj]]
    if isinstance(obj, list):
        return [_encode_node(x) for x in obj]
    if isinstance(obj, frozenset):
        return [_marker(_EXT_FROZENSET), *_sorted_nodes(obj)]
    if isinstance(obj, set):
        return [_marker(_EXT_SET), *_sorted_nodes(obj)]
    if isinstance(obj, dict):
        return _encode_dict(obj)

    if isinstance(obj, BaseException):
        return [
            _marker(_EXT_EXCEPTION),
            t.__module__,
            t.__qualname__,
            *[_encode_node(x) for x in obj.args],
        ]

    raise TypeError(
        f"cascade persistent cache: cannot serialize {t.__module__}.{t.__qualname__!r}; "
        "supported types are primitives, bytes, list/tuple/set/frozenset/dict, datetime, "
        "@dataclass instances, typing.NamedTuple instances, and BaseException."
    )


def _sorted_nodes(items: Any) -> list[Any]:
    encoded = [_encode_node(x) for x in items]
    # Homogeneous str or int sets (the common case) have a natural total
    # order; anything else is ordered by its own packed bytes.
    kinds = {type(x) for x in encoded}
    if kinds == {str} or kinds == {int}:
        encoded.sort()
        return encoded
    return sorted(encoded, key=_packed)


def _encode_dict(obj: dict[Any, Any]) -> Any:
    # Insertion order is kept on purpose (module docstring). msgpack writes a
    # map in the dict's own order and reads it back in stream order.
    if all(type(k) is str for k in obj):
        return {k: _encode_node(v) for k, v in obj.items()}
    node: list[Any] = [_marker(_EXT_MAP)]
    for k, v in obj.items():
        node.append(_encode_node(k))
        node.append(_encode_node(v))
    return node


def _resolve_type(module_name: str, qualname: str) -> type[Any]:
    mod = importlib.import_module(module_name)
    cur: Any = mod
    for part in qualname.split("."):
        cur = getattr(cur, part)
    if not isinstance(cur, type):
        raise TypeError(f"{module_name}.{qualname!r} is not a type")  # pragma: no cover
    return cur


def _ext_hook(code: int, data: bytes) -> Any:
    tag = _TAGS.get(code)
    if tag is None or data:
        raise ValueError(
            f"cascade persistent cache: unexpected extension (code {code}, "
            f"{len(data)} payload bytes); the blob is not disk format 2"
        )
    return tag


def _list_hook(items: list[Any]) -> Any:
    # Children are already decoded: the unpacker calls this bottom-up.
    if items and type(items[0]) is _Tag:
        return _untag(items[0].code, items)
    return items


def _make_dataclass(module_name: str, qualname: str, fields: dict[str, Any]) -> Any:
    cls = _resolve_type(module_name, qualname)
    if not (dataclasses.is_dataclass(cls) and isinstance(cls, type)):
        raise TypeError(
            f"{module_name}.{qualname!r} is not a dataclass"
        )  # pragma: no cover
    return cls(**fields)


def _make_namedtuple(module_name: str, qualname: str, values: list[Any]) -> Any:
    cls = _resolve_type(module_name, qualname)
    if not (isinstance(cls, type) and issubclass(cls, tuple) and hasattr(cls, "_make")):
        raise TypeError(
            f"{module_name}.{qualname!r} is not a NamedTuple"
        )  # pragma: no cover
    return cls(*values)


def _make_exception(module_name: str, qualname: str, args: list[Any]) -> Any:
    cls = _resolve_type(module_name, qualname)
    if not (isinstance(cls, type) and issubclass(cls, BaseException)):
        raise TypeError(
            f"{module_name}.{qualname!r} is not an Exception"
        )  # pragma: no cover
    return cls(*args)


def _make_bigint(negative: bool, magnitude: bytes) -> int:
    value = int.from_bytes(magnitude, "big")
    return -value if negative else value


def _make_timezone(
    days: int, seconds: int, microseconds: int, name: str
) -> datetime.timezone:
    return datetime.timezone(
        datetime.timedelta(days=days, seconds=seconds, microseconds=microseconds),
        name,
    )


def _untag(code: int, items: list[Any]) -> Any:
    """Rebuild a tagged value from ``[marker, *already-decoded parts]``."""
    if code == _EXT_TUPLE:
        return tuple(items[1:])
    if code == _EXT_SET:
        return set(items[1:])
    if code == _EXT_FROZENSET:
        return frozenset(items[1:])
    if code == _EXT_MAP:
        return {items[i]: items[i + 1] for i in range(1, len(items), 2)}
    if code == _EXT_BIGINT:
        return _make_bigint(items[1], items[2])
    if code == _EXT_DATACLASS:
        fields = {items[i]: items[i + 1] for i in range(3, len(items), 2)}
        return _make_dataclass(items[1], items[2], fields)
    if code == _EXT_NAMEDTUPLE:
        return _make_namedtuple(items[1], items[2], items[3:])
    if code == _EXT_EXCEPTION:
        return _make_exception(items[1], items[2], items[3:])
    if code == _EXT_DATETIME:
        return datetime.datetime.fromisoformat(items[1])
    if code == _EXT_DATE:
        return datetime.date.fromisoformat(items[1])
    if code == _EXT_TIME:
        return datetime.time.fromisoformat(items[1])
    if code == _EXT_TIMEDELTA:
        return datetime.timedelta(
            days=items[1], seconds=items[2], microseconds=items[3]
        )
    return _make_timezone(items[1], items[2], items[3], items[4])
