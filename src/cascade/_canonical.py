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

Wire format (disk format 4)
---------------------------
Format 2 sorted dict keys; format 3 kept their order; format 4 added the
codes from ``_EXT_ENUM`` on and stopped writing a ``bytearray`` as ``bytes``.

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

import collections
import dataclasses
import datetime
import decimal
import enum
import fractions
import hashlib
import importlib
import pathlib
import sys
import uuid
from typing import Any

from . import _types
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
_EXT_ENUM = 14
_EXT_DECIMAL = 15
_EXT_FRACTION = 16
_EXT_COMPLEX = 17
_EXT_UUID = 18
_EXT_PATH = 19
_EXT_BYTEARRAY = 20
_EXT_RANGE = 21
_EXT_DEQUE = 22
_EXT_NDARRAY = 23
_EXT_NPSCALAR = 24
_EXT_REGISTERED = 25
_LAST_EXT = _EXT_REGISTERED

# The concrete path classes, by the number they are stored under. A path
# keeps its class: a query can tell a POSIX path from a Windows one.
PATH_CLASSES: tuple[type, ...] = (
    pathlib.PurePosixPath,
    pathlib.PureWindowsPath,
    pathlib.PosixPath,
    pathlib.WindowsPath,
)
_PATH_CODES = {cls: i for i, cls in enumerate(PATH_CLASSES)}

_INT64_MIN = -(2**63)
_UINT64_MAX = 2**64 - 1

_PASSTHROUGH_SCALARS = (str, bytes, float, bool, type(None))


class _Tag:
    """Decoded form of a zero-length extension marker."""

    __slots__ = ("code",)

    def __init__(self, code: int) -> None:
        self.code = code


_TAGS: dict[int, _Tag] = {code: _Tag(code) for code in range(1, _LAST_EXT + 1)}
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

    reg = _types.registration_for(t)
    if reg is not None:
        return [
            _marker(_EXT_REGISTERED),
            reg.name,
            _encode_node(_types.encode_registered(reg, obj)),
        ]
    if t is bytearray:
        return [_marker(_EXT_BYTEARRAY), bytes(obj)]
    if isinstance(obj, datetime.datetime):
        # ``fold`` is not part of the ISO text; it follows when set.
        if obj.fold:
            return [_marker(_EXT_DATETIME), obj.isoformat(), 1]
        return [_marker(_EXT_DATETIME), obj.isoformat()]
    if isinstance(obj, datetime.date):
        return [_marker(_EXT_DATE), obj.isoformat()]
    if isinstance(obj, datetime.time):
        if obj.fold:
            return [_marker(_EXT_TIME), obj.isoformat(), 1]
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
    if isinstance(obj, enum.Enum):
        by_name, payload = enum_payload(obj)
        return [
            _marker(_EXT_ENUM),
            t.__module__,
            t.__qualname__,
            by_name,
            payload if by_name else _encode_node(payload),
        ]
    if t is decimal.Decimal:
        return [_marker(_EXT_DECIMAL), str(obj)]
    if t is fractions.Fraction:
        return [
            _marker(_EXT_FRACTION),
            _encode_node(obj.numerator),
            _encode_node(obj.denominator),
        ]
    if t is complex:
        return [_marker(_EXT_COMPLEX), obj.real, obj.imag]
    if t is uuid.UUID:
        return [_marker(_EXT_UUID), obj.bytes]
    path_code = _PATH_CODES.get(t)
    if path_code is not None:
        return [_marker(_EXT_PATH), path_code, str(obj)]
    if t is range:
        return [
            _marker(_EXT_RANGE),
            _encode_node(obj.start),
            _encode_node(obj.stop),
            _encode_node(obj.step),
        ]
    if t is collections.deque:
        return [_marker(_EXT_DEQUE), obj.maxlen, *[_encode_node(x) for x in obj]]
    if t.__module__ == "numpy":
        numpy_node = _encode_numpy(obj)
        if numpy_node is not None:
            return numpy_node

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
        f"cascade persistent cache: cannot serialize {t.__module__}.{t.__qualname__!r}; register the "
        "class with cascade.register_type() to make it encodable"
    )


def enum_payload(obj: enum.Enum) -> tuple[bool, Any]:
    """``(True, name)`` for a named member, else ``(False, value)``.

    Composite and pseudo members of a ``Flag`` have no name of their own to
    look up, so they are stored by value.
    """
    name = obj._name_
    if name is not None and type(obj)._member_map_.get(name) is obj:
        return True, name
    return False, obj._value_


def _plain_numpy_dtype(dtype: Any) -> bool:
    # Arrays of Python objects and structured dtypes are not supported:
    # their bytes are not their value.
    return not (dtype.hasobject or dtype.fields is not None or dtype.subdtype)


def numpy_array_parts(obj: Any) -> tuple[str, list[int], bytes] | None:
    """``(dtype, shape, C-order bytes)`` of a plain ndarray; None otherwise.

    numpy is never imported here: a value can only be a numpy array if the
    program imported numpy itself.
    """
    np = sys.modules.get("numpy")
    if np is None or type(obj) is not np.ndarray:
        return None
    if not _plain_numpy_dtype(obj.dtype):
        return None
    return obj.dtype.str, list(obj.shape), obj.tobytes(order="C")


def numpy_scalar_parts(obj: Any) -> tuple[str, bytes] | None:
    """``(dtype, bytes)`` of a numpy scalar such as ``numpy.float32(1.5)``."""
    np = sys.modules.get("numpy")
    if np is None or not isinstance(obj, np.generic):
        return None
    if not _plain_numpy_dtype(obj.dtype):
        return None
    return obj.dtype.str, obj.tobytes()


def _encode_numpy(obj: Any) -> Any:
    parts = numpy_array_parts(obj)
    if parts is not None:
        return [_marker(_EXT_NDARRAY), *parts]
    scalar = numpy_scalar_parts(obj)
    if scalar is not None:
        return [_marker(_EXT_NPSCALAR), *scalar]
    return None


def make_numpy_array(dtype: str, shape: list[int], data: bytes) -> Any:
    np = importlib.import_module("numpy")
    dt = np.dtype(dtype)
    if dt.itemsize == 0:
        return np.empty(tuple(shape), dtype=dt)
    return np.frombuffer(data, dtype=dt).reshape(tuple(shape)).copy()


def make_numpy_scalar(dtype: str, data: bytes) -> Any:
    np = importlib.import_module("numpy")
    return np.frombuffer(data, dtype=np.dtype(dtype))[0]


def make_enum(module_name: str, qualname: str, by_name: bool, payload: Any) -> Any:
    cls = _resolve_type(module_name, qualname)
    if not issubclass(cls, enum.Enum):
        raise TypeError(f"{module_name}.{qualname!r} is not an Enum")
    return cls[payload] if by_name else cls(payload)


def make_path(code: int, text: str) -> Any:
    return PATH_CLASSES[code](text)


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
            f"{len(data)} payload bytes); the blob is not disk format 2 or later"
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
        moment = datetime.datetime.fromisoformat(items[1])
        return moment.replace(fold=1) if len(items) > 2 else moment
    if code == _EXT_DATE:
        return datetime.date.fromisoformat(items[1])
    if code == _EXT_TIME:
        clock = datetime.time.fromisoformat(items[1])
        return clock.replace(fold=1) if len(items) > 2 else clock
    if code == _EXT_TIMEDELTA:
        return datetime.timedelta(
            days=items[1], seconds=items[2], microseconds=items[3]
        )
    if code == _EXT_TIMEZONE:
        return _make_timezone(items[1], items[2], items[3], items[4])
    if code == _EXT_ENUM:
        return make_enum(items[1], items[2], items[3], items[4])
    if code == _EXT_DECIMAL:
        return decimal.Decimal(items[1])
    if code == _EXT_FRACTION:
        return fractions.Fraction(items[1], items[2])
    if code == _EXT_COMPLEX:
        return complex(items[1], items[2])
    if code == _EXT_UUID:
        return uuid.UUID(bytes=items[1])
    if code == _EXT_PATH:
        return make_path(items[1], items[2])
    if code == _EXT_BYTEARRAY:
        return bytearray(items[1])
    if code == _EXT_RANGE:
        return range(items[1], items[2], items[3])
    if code == _EXT_DEQUE:
        return collections.deque(items[2:], items[1])
    if code == _EXT_NDARRAY:
        return make_numpy_array(items[1], items[2], items[3])
    if code == _EXT_NPSCALAR:
        return make_numpy_scalar(items[1], items[2])
    return _types.decode_registered(items[1], items[2])
