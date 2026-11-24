"""User-registered value types: ``cascade.register_type``.

Cascade encodes results and input values to tell whether they changed and
to store them on disk. It knows the standard value types (see ``_canonical``
and ``_serde``); any other class is registered here with a pair of functions
that turn an instance into an encodable value and back. Nothing is pickled:
the decode function decides what an encoded value may become, which keeps
the cache directory's trust boundary where it was (``docs/guarantees.md``).

The registry is process-wide, like the classes it describes, and shared by
every engine. A class is registered once; registering it again with other
functions is refused, because values already encoded under the first pair
would then disagree with new ones.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import fractions
import pathlib
import threading
import uuid
from typing import Any, Callable

_lock = threading.Lock()


@dataclasses.dataclass(frozen=True)
class Registration:
    cls: type
    name: str
    encode: Callable[[Any], Any]
    decode: Callable[[Any], Any]


# Written under ``_lock``; read without it (a lone dict read is atomic, also
# on free-threaded builds).
_BY_TYPE: dict[type, Registration] = {}
_BY_NAME: dict[str, Registration] = {}

# Classes the codecs encode themselves. Registering one would make two
# encodings for one value, so it is refused.
_NATIVE: frozenset[type] = frozenset(
    {
        type(None),
        bool,
        int,
        float,
        complex,
        str,
        bytes,
        bytearray,
        list,
        tuple,
        set,
        frozenset,
        dict,
        range,
        collections.deque,
        decimal.Decimal,
        fractions.Fraction,
        uuid.UUID,
        datetime.datetime,
        datetime.date,
        datetime.time,
        datetime.timedelta,
        datetime.timezone,
        pathlib.PurePosixPath,
        pathlib.PureWindowsPath,
        pathlib.PosixPath,
        pathlib.WindowsPath,
    }
)


def register_type(
    cls: type,
    *,
    encode: Callable[[Any], Any],
    decode: Callable[[Any], Any],
    name: str | None = None,
) -> None:
    """Teach Cascade to encode instances of ``cls``.

    ``encode(obj)`` returns a value Cascade can already encode (a tuple of
    fields, a dict, a str, another registered type, ...), and
    ``decode(encoded)`` rebuilds an equal instance from it. Two instances
    that ``encode`` maps to equal values are treated as the same value:
    early bail-out, same-value ``set`` and reuse from disk rely on it, so
    ``encode`` must capture everything a query could observe, and be
    deterministic.

    ``name`` identifies the type on disk; it defaults to
    ``"module:qualname"``. Change it (``"Point/2"``) when the encoding
    changes, so that values stored under the old one are recomputed rather
    than decoded wrongly. Only instances of exactly ``cls`` use the
    registration, not subclasses.

    Register a class before an engine encodes or loads its instances, for
    example at import time next to the class. A value stored under a name
    that is not registered is not decoded: a cached result is recomputed,
    and restoring a named collection raises.
    """
    if not isinstance(cls, type):
        raise TypeError(f"register_type() needs a class, not {cls!r}")
    if cls in _NATIVE:
        raise ValueError(
            f"{cls.__module__}.{cls.__qualname__} is encoded natively by Cascade "
            "and cannot be registered"
        )
    if not callable(encode) or not callable(decode):
        raise TypeError("register_type() needs callable encode= and decode=")
    if name is None:
        name = f"{cls.__module__}:{cls.__qualname__}"
    if not isinstance(name, str) or not name:
        raise TypeError("name= must be a non-empty string")
    reg = Registration(cls=cls, name=name, encode=encode, decode=decode)
    with _lock:
        old = _BY_TYPE.get(cls)
        if old is not None:
            if old == reg:
                return
            raise ValueError(
                f"{cls.__module__}.{cls.__qualname__} is already registered "
                f"(as {old.name!r}) with other functions. A type's encoding "
                "cannot change while values encoded by it may be cached."
            )
        taken = _BY_NAME.get(name)
        if taken is not None:
            raise ValueError(
                f"the name {name!r} is already registered for "
                f"{taken.cls.__module__}.{taken.cls.__qualname__}"
            )
        _BY_TYPE[cls] = reg
        _BY_NAME[name] = reg


def registration_for(cls: type) -> Registration | None:
    return _BY_TYPE.get(cls)


def registration_named(name: str) -> Registration | None:
    return _BY_NAME.get(name)


def encode_registered(reg: Registration, obj: Any) -> Any:
    """``reg.encode(obj)``; a failure is reported as a ``TypeError``."""
    try:
        return reg.encode(obj)
    except Exception as exc:
        raise TypeError(
            f"the encode function registered for {reg.name!r} raised "
            f"{type(exc).__name__}: {exc}"
        ) from exc


def decode_registered(name: str, payload: Any) -> Any:
    reg = _BY_NAME.get(name)
    if reg is None:
        raise ValueError(
            f"the value was stored as registered type {name!r}, which is not "
            "registered in this process; call cascade.register_type() for it "
            "before the engine reads it"
        )
    return reg.decode(payload)
