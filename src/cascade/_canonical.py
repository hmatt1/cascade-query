"""Deterministic msgpack encoding and hashing for the persistent disk cache.

Equal values must always produce identical byte streams, so dict keys and set
elements are ordered by their own encoded bytes before packing. Fingerprints
are blake2b digests of those byte streams. msgpack is imported lazily so the
in-memory engine keeps working without it; enabling ``cache_dir`` requires it.
"""

from __future__ import annotations

import dataclasses
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

_INT64_MIN = -(2**63)
_UINT64_MAX = 2**64 - 1


def require_msgpack() -> Any:
    if msgpack is None:
        raise PersistentCacheError(MSGPACK_INSTALL_HINT)
    return msgpack


def encode(value: Any) -> bytes:
    """Canonical msgpack bytes for ``value``. Equal values yield equal bytes."""
    mp = require_msgpack()
    return mp.packb(_encode_node(value), use_bin_type=True)


def decode(blob: bytes) -> Any:
    """Inverse of :func:`encode`."""
    mp = require_msgpack()
    node = mp.unpackb(blob, raw=False, use_list=True, strict_map_key=False)
    return _decode_node(node)


def digest_bytes(blob: bytes) -> str:
    return hashlib.blake2b(blob, digest_size=20).hexdigest()


def value_digest(value: Any) -> str:
    """Blake2b hex fingerprint of the canonical byte stream of ``value``."""
    return digest_bytes(encode(value))


def _packed(node: Any) -> bytes:
    return msgpack.packb(node, use_bin_type=True)


def _ext(code: int, payload_node: Any) -> Any:
    return msgpack.ExtType(code, _packed(payload_node))


def _bigint_payload(value: int) -> list[Any]:
    magnitude = abs(value)
    return [value < 0, magnitude.to_bytes((magnitude.bit_length() + 7) // 8 or 1, "big")]


def _encode_node(obj: Any) -> Any:
    if obj is None or obj is True or obj is False:
        return obj
    t = type(obj)
    if t is int:
        if _INT64_MIN <= obj <= _UINT64_MAX:
            return obj
        return _ext(_EXT_BIGINT, _bigint_payload(obj))
    if t is float or t is str or t is bytes:
        return obj
    if t is bytearray:
        return bytes(obj)

    if isinstance(obj, tuple) and hasattr(obj, "_fields"):
        return _ext(
            _EXT_NAMEDTUPLE,
            [type(obj).__module__, type(obj).__qualname__, [_encode_node(getattr(obj, f)) for f in obj._fields]],
        )
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        names = sorted(obj.__dataclass_fields__.keys())
        return _ext(
            _EXT_DATACLASS,
            [type(obj).__module__, type(obj).__qualname__, [[n, _encode_node(getattr(obj, n))] for n in names]],
        )

    if isinstance(obj, tuple):
        return _ext(_EXT_TUPLE, [_encode_node(x) for x in obj])
    if isinstance(obj, list):
        return [_encode_node(x) for x in obj]
    if isinstance(obj, frozenset):
        return _ext(_EXT_FROZENSET, _sorted_nodes(obj))
    if isinstance(obj, set):
        return _ext(_EXT_SET, _sorted_nodes(obj))
    if isinstance(obj, dict):
        return _encode_dict(obj)

    if isinstance(obj, BaseException):
        return _ext(
            8,  # _EXT_EXCEPTION
            [type(obj).__module__, type(obj).__qualname__, [_encode_node(x) for x in obj.args]]
        )

    raise TypeError(
        f"cascade persistent cache: cannot serialize {type(obj).__module__}.{type(obj).__qualname__!r}; "
        "supported types are primitives, bytes, list/tuple/set/frozenset/dict, "
        "@dataclass instances, typing.NamedTuple instances, and BaseException."
    )


def _sorted_nodes(items: Any) -> list[Any]:
    encoded = [_encode_node(x) for x in items]
    return sorted(encoded, key=_packed)


def _encode_dict(obj: dict[Any, Any]) -> Any:
    pairs = [(_encode_node(k), _encode_node(v)) for k, v in obj.items()]
    pairs.sort(key=lambda kv: _packed(kv[0]))
    if all(type(k) is str for k in obj.keys()):
        return {k: v for k, v in pairs}
    return _ext(_EXT_MAP, [[k, v] for k, v in pairs])


def _resolve_type(module_name: str, qualname: str) -> type[Any]:
    mod = importlib.import_module(module_name)
    cur: Any = mod
    for part in qualname.split("."):
        cur = getattr(cur, part)
    if not isinstance(cur, type):
        raise TypeError(f"{module_name}.{qualname!r} is not a type")
    return cur


def _decode_node(node: Any) -> Any:
    if isinstance(node, msgpack.ExtType):
        return _decode_ext(node)
    if isinstance(node, list):
        return [_decode_node(x) for x in node]
    if isinstance(node, dict):
        return {k: _decode_node(v) for k, v in node.items()}
    return node


def _decode_ext(ext: Any) -> Any:
    payload = msgpack.unpackb(ext.data, raw=False, use_list=True, strict_map_key=False)
    if ext.code == _EXT_TUPLE:
        return tuple(_decode_node(x) for x in payload)
    if ext.code == _EXT_SET:
        return {_decode_node(x) for x in payload}
    if ext.code == _EXT_FROZENSET:
        return frozenset(_decode_node(x) for x in payload)
    if ext.code == _EXT_MAP:
        return {_decode_node(pair[0]): _decode_node(pair[1]) for pair in payload}
    if ext.code == _EXT_BIGINT:
        negative, magnitude = payload
        value = int.from_bytes(magnitude, "big")
        return -value if negative else value
    if ext.code == _EXT_DATACLASS:
        module_name, qualname, field_pairs = payload
        fields = {pair[0]: _decode_node(pair[1]) for pair in field_pairs}
        cls = _resolve_type(module_name, qualname)
        if not (dataclasses.is_dataclass(cls) and isinstance(cls, type)):
            raise TypeError(f"{module_name}.{qualname!r} is not a dataclass")
        return cls(**fields)
    if ext.code == _EXT_NAMEDTUPLE:
        module_name, qualname, values = payload
        vals = [_decode_node(x) for x in values]
        cls = _resolve_type(module_name, qualname)
        if not (isinstance(cls, type) and issubclass(cls, tuple) and hasattr(cls, "_make")):
            raise TypeError(f"{module_name}.{qualname!r} is not a NamedTuple")
        return cls(*vals)
    if ext.code == 8:  # _EXT_EXCEPTION
        module_name, qualname, values = payload
        vals = [_decode_node(x) for x in values]
        cls = _resolve_type(module_name, qualname)
        if not (isinstance(cls, type) and issubclass(cls, BaseException)):
            raise TypeError(f"{module_name}.{qualname!r} is not an Exception")
        return cls(*vals)
    raise ValueError(f"cascade persistent cache: unknown extension code {ext.code}")
