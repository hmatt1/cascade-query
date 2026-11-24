"""Cache keys for query and input arguments.

A call is cached under its arguments. Python's ``==`` and ``hash`` are not
enough to tell calls apart: ``1``, ``1.0`` and ``True`` are equal and hash
alike, yet a body can tell them apart (``type(x)``, ``repr(x)``, ``x / 2``).
So an argument whose type ``==`` does not pin down is wrapped: a ``bool`` as
``(bool, value)``, a ``float`` as ``(float, value)`` (``(NEGATIVE_ZERO,
-0.0)`` for minus zero), anything else in a ``TypedArg`` that also compares a
*shape*: its type, and for containers the shapes of their items. Arguments of
the types where ``==`` is exact (``int``, ``str``, ``bytes``, ``None``) are
left as they are, so the common key is the plain tuple of arguments and costs
one type check per argument. The two tagged tuples cannot be confused with an
argument: a tuple argument holding a type or a float is itself wrapped in a
``TypedArg``, so a bare tuple in a key that starts with ``bool``, ``float``
or ``NEGATIVE_ZERO`` is always a tag. They compare and hash in C, which keeps
a cache hit with a ``float`` or ``bool`` argument as cheap as one with an
``int``.

Keyword arguments are bound to the function's parameters, with defaults
filled in, so ``q(1)``, ``q(x=1)`` and ``q(1, y=2)`` (where ``y=2`` is the
default) are one key. Keyword-only parameters and ``**kwargs`` end the key in
a ``KwArgs``.

Three forms of the same arguments:

* the *plain* form, ``(1, 2.0, {"k": 3})``: positional values, then a dict of
  keyword values if there are any. It is what the disk cache encodes, and
  what ``key_args`` turns into a key.
* the *key* form, ``(1, TypedArg(2.0), KwArgs(k=3))``: hashable, compared by
  type as well as value. It is the ``args`` of a ``QueryKey``.
* the *call* form: ``fn(*positional, **keywords)``; see ``call``.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import inspect
import math
import pathlib
from typing import Any, Callable

# Types whose ``==`` already tells apart everything a body can observe
# (identity aside): equal values of these types are one key as they are.
EXACT_TYPES: frozenset[type] = frozenset({int, str, bytes, type(None)})

# ``==`` hides the representation of these: Decimal("1.0") == Decimal("1.00"),
# a datetime's time zone and fold, a Windows path's case, the start of an
# empty range. Their ``repr`` shows it, and is used as their shape.
_REPR_SHAPED: tuple[type, ...] = (
    range,
    decimal.Decimal,
    datetime.datetime,
    datetime.time,
    datetime.timezone,
    pathlib.PurePath,
)

_NEGATIVE_ZERO = "-0.0"


class _NegativeZero:
    """Tag of a ``-0.0`` argument; ``0.0 == -0.0``, but a body can tell."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "NEGATIVE_ZERO"


NEGATIVE_ZERO = _NegativeZero()
_SCALAR_TAGS = (float, bool, NEGATIVE_ZERO)


def shape(value: Any) -> Any:
    """What ``==`` does not compare about ``value``, as a hashable; None if nothing.

    Two values are one key when they are ``==`` and their shapes are equal.
    The shape is the type, recursively through tuples, frozensets, sets,
    lists, dicts and dataclass fields, plus the sign of a zero float and the
    ``repr`` of the types in ``_REPR_SHAPED``. Dict order is not part of it:
    two equal dicts in one argument share a key whatever their order.
    """
    t = type(value)
    if t in EXACT_TYPES:
        return None
    if t is float:
        if value == 0.0 and math.copysign(1.0, value) < 0:
            return _NEGATIVE_ZERO
        return float
    if t is bool:
        return bool
    if t is tuple:
        shapes = tuple([shape(x) for x in value])
        for s in shapes:
            if s is not None:
                return (tuple, shapes)
        return None
    if t is frozenset or t is set:
        for x in value:
            if type(x) not in EXACT_TYPES:
                return (t, frozenset([(shape(x), x) for x in value]))
        return None if t is frozenset else set
    if t is complex:
        return (
            complex,
            math.copysign(1.0, value.real),
            math.copysign(1.0, value.imag),
        )
    if isinstance(value, tuple):  # NamedTuple and other tuple subclasses
        return (t, tuple([shape(x) for x in value]))
    if isinstance(value, _REPR_SHAPED):
        return (t, repr(value))
    if t is list:
        return (list, tuple([shape(x) for x in value]))
    if t is dict:
        return (
            dict,
            frozenset([(shape(k), k, shape(v)) for k, v in value.items()]),
        )
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return (
            t,
            tuple(
                [
                    shape(getattr(value, f.name))
                    for f in dataclasses.fields(value)
                    if f.compare
                ]
            ),
        )
    return t


class TypedArg:
    """An argument compared by shape as well as by ``==``."""

    __slots__ = ("value", "shape", "_hash")

    def __init__(self, value: Any, value_shape: Any) -> None:
        self.value = value
        self.shape = value_shape
        # Raises TypeError for an unhashable value, as a plain key would.
        self._hash = hash((value_shape, value))

    def __eq__(self, other: object) -> bool:
        if type(other) is not TypedArg:
            return NotImplemented
        if self.shape != other.shape:
            return False
        value = self.value
        return value is other.value or bool(value == other.value)

    def __hash__(self) -> int:
        return self._hash

    def __repr__(self) -> str:
        return repr(self.value)


class KwArgs:
    """Keyword-only and ``**kwargs`` values at the end of a key, by name."""

    __slots__ = ("items", "_hash")

    def __init__(self, values: dict[str, Any]) -> None:
        self.items = tuple(sorted((name, _key_value(v)) for name, v in values.items()))
        self._hash = hash(self.items)

    def __eq__(self, other: object) -> bool:
        if type(other) is not KwArgs:
            return NotImplemented
        return self.items == other.items

    def __hash__(self) -> int:
        return self._hash

    def plain(self) -> dict[str, Any]:
        return {name: _plain_value(v) for name, v in self.items}

    def __repr__(self) -> str:
        return "**" + repr(self.plain())


def _key_value(value: Any) -> Any:
    t = type(value)
    if t in EXACT_TYPES:
        return value
    if t is float:
        if value == 0.0 and math.copysign(1.0, value) < 0:
            return (NEGATIVE_ZERO, value)
        return (float, value)
    if t is bool:
        return (bool, value)
    value_shape = shape(value)
    if value_shape is None:
        hash(value)  # an unhashable value fails here, as a plain key would
        return value
    return TypedArg(value, value_shape)


def key_args(args: tuple[Any, ...]) -> tuple[Any, ...]:
    """The key form of positional arguments (see the module docstring).

    Raises ``TypeError`` for an unhashable argument.
    """
    for a in args:
        if type(a) not in EXACT_TYPES:
            break
    else:
        return args
    out = []
    for a in args:
        t = type(a)
        if t in EXACT_TYPES:
            out.append(a)
        elif t is float and a != 0.0:  # inlined _key_value: the common case
            out.append((float, a))
        else:
            out.append(_key_value(a))
    return tuple(out)


def key_from_plain(plain: tuple[Any, ...]) -> tuple[Any, ...]:
    """The key form of arguments in the plain form, as the disk cache stores them.

    A trailing dict holds keyword values: a dict cannot be a positional
    argument (it is not hashable), so it never ends the plain form otherwise.
    """
    if plain and type(plain[-1]) is dict:
        return key_args(plain[:-1]) + (KwArgs(plain[-1]),)
    return key_args(plain)


def _is_wrapped(a: Any) -> bool:
    t = type(a)
    return (
        t is TypedArg
        or t is KwArgs
        or (t is tuple and len(a) == 2 and a[0] in _SCALAR_TAGS)
    )


def _plain_value(a: Any) -> Any:
    t = type(a)
    if t is TypedArg:
        return a.value
    if t is tuple and len(a) == 2 and a[0] in _SCALAR_TAGS:
        return a[1]
    return a


def plain_args(key: tuple[Any, ...]) -> tuple[Any, ...]:
    """The plain form of key arguments; ``key_from_plain`` turns it back."""
    for a in key:
        if type(a) not in EXACT_TYPES and _is_wrapped(a):
            break
    else:
        return key
    out = []
    for a in key:
        if type(a) is KwArgs:
            out.append(a.plain())
        else:
            out.append(_plain_value(a))
    return tuple(out)


def is_key_form(args: tuple[Any, ...]) -> bool:
    """True if ``args`` holds a wrapped argument, so is a key already."""
    for a in args:
        if type(a) not in EXACT_TYPES and _is_wrapped(a):
            return True
    return False


def display(key: tuple[Any, ...]) -> str:
    """``key`` as the call's arguments: ``(1.5, True, **{'k': 2})``."""
    if not is_key_form(key):
        return repr(key)
    parts = [repr(a) if type(a) is KwArgs else repr(_plain_value(a)) for a in key]
    if len(parts) == 1 and type(key[0]) is not KwArgs:
        return f"({parts[0]},)"
    return "(" + ", ".join(parts) + ")"


def call_parts(key: tuple[Any, ...]) -> tuple[tuple[Any, ...], dict[str, Any]]:
    """``(positional, keywords)`` to call a function with key arguments."""
    if key and type(key[-1]) is KwArgs:
        return plain_args(key[:-1]), key[-1].plain()
    return plain_args(key), {}


def call(fn: Callable[..., Any], key: tuple[Any, ...]) -> Any:
    """Call ``fn`` with the arguments ``key`` stands for."""
    for a in key:
        if type(a) not in EXACT_TYPES and _is_wrapped(a):
            break
    else:
        return fn(*key)
    args, kwargs = call_parts(key)
    return fn(*args, **kwargs)


# --- binding calls to parameters ---------------------------------------------

_MISSING = object()
_OMITTED = object()
_EMPTY = inspect.Parameter.empty
_POSITIONAL = (
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
)


def _usable_default(default: Any) -> bool:
    """Whether a default can be part of a key: hashable and encodable.

    A default that is neither (a ``[]``, a sentinel ``object()``) is left out
    of the key when the call does not pass another value, as before keyword
    support: putting it in would make every such call unhashable or keep its
    results off the disk.
    """
    from ._serde import stable_value_digest

    try:
        hash(default)
        stable_value_digest(default)
    except Exception:  # noqa: BLE001 - any failure means "keep it out"
        return False
    return True


class ArgSpec:
    """How the calls of one function map to keys; built once per registration."""

    def __init__(self, fn: Callable[..., Any], label: str) -> None:
        self.label = label
        try:
            sig: inspect.Signature | None = inspect.signature(fn)
        except (TypeError, ValueError):
            sig = None
        self.sig = sig
        names: list[str] = []
        defaults: list[Any] = []
        usable: list[bool] = []
        self.nposonly = 0
        self.var_pos = False
        self.var_kw = False
        self.kwonly: list[tuple[str, Any, bool]] = []
        params = () if sig is None else tuple(sig.parameters.values())
        for p in params:
            if p.kind in _POSITIONAL:
                if p.kind is inspect.Parameter.POSITIONAL_ONLY:
                    self.nposonly += 1
                names.append(p.name)
                defaults.append(p.default)
                usable.append(p.default is _EMPTY or _usable_default(p.default))
            elif p.kind is inspect.Parameter.VAR_POSITIONAL:
                self.var_pos = True
            elif p.kind is inspect.Parameter.KEYWORD_ONLY:
                self.kwonly.append(
                    (
                        p.name,
                        p.default,
                        p.default is _EMPTY or _usable_default(p.default),
                    )
                )
            else:
                self.var_kw = True
        self.names = names
        self.npos = len(names)
        self.defaults = defaults
        self.usable = usable
        self.index = {name: i for i, name in enumerate(names) if i >= self.nposonly}
        self.kwonly_names = frozenset(name for name, _, _ in self.kwonly)
        required = sum(1 for d in defaults if d is _EMPTY)
        simple = not (self.var_pos or self.kwonly or self.var_kw)
        all_usable = all(usable)
        # Calls that need no binding: every positional parameter passed,
        # no keywords. Defaults left out of keys need binding to be left out.
        self.fast_len = self.npos if simple and all_usable and sig is not None else -1
        # Positional calls that only need trailing defaults appended.
        trailing_defaults_only = (
            simple and all_usable and all(d is _EMPTY for d in defaults[:required])
        )
        self.fill_from = required if trailing_defaults_only else self.npos + 1
        self.default_tail = tuple(defaults[required:]) if trailing_defaults_only else ()

    def key(self, args: tuple[Any, ...], kwargs: dict[str, Any] | None) -> tuple:
        """The key form of a call ``fn(*args, **kwargs)``."""
        n = len(args)
        if not kwargs:
            if n == self.fast_len:
                return key_args(args)
            if self.fill_from <= n < self.npos:
                return key_args(args + self.default_tail[n - self.fill_from :])
        positional, keywords = self.bind(args, kwargs or {})
        if keywords:
            return key_args(positional) + (KwArgs(keywords),)
        return key_args(positional)

    def _fail(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> None:
        assert self.sig is not None
        try:
            self.sig.bind(*args, **kwargs)
        except TypeError as exc:
            raise TypeError(f"{self.label}: {exc}") from None
        raise TypeError(  # pragma: no cover - bind accepted what we refused
            f"{self.label}: cannot bind these arguments"
        )

    def bind(
        self, args: tuple[Any, ...], kwargs: dict[str, Any]
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """``(positional, keywords)`` of a call: parameters in order, defaults filled in.

        ``keywords`` holds keyword-only and ``**kwargs`` values, and values
        after a default that cannot be part of a key (``_close_gaps``).
        """
        if self.sig is None:
            if kwargs:
                raise TypeError(
                    f"{self.label}: keyword arguments are not supported, the "
                    "function's signature cannot be inspected"
                )
            return args, {}
        npos = self.npos
        n = len(args)
        if n > npos and not self.var_pos:
            self._fail(args, kwargs)
        slots = list(args[:npos])
        if n < npos:
            slots.extend([_MISSING] * (npos - n))
        tail: dict[str, Any] = {}
        index = self.index
        for name, value in kwargs.items():
            i = index.get(name)
            if i is not None:
                if slots[i] is not _MISSING:
                    self._fail(args, kwargs)
                slots[i] = value
            elif name in self.kwonly_names or self.var_kw:
                tail[name] = value
            else:
                self._fail(args, kwargs)
        defaults = self.defaults
        usable = self.usable
        omitted = False
        for i in range(npos):
            value = slots[i]
            if value is _MISSING:
                default = defaults[i]
                if default is _EMPTY:
                    self._fail(args, kwargs)
                if usable[i]:
                    slots[i] = default
                else:
                    slots[i] = _OMITTED
                    omitted = True
            elif not usable[i] and value is defaults[i]:
                slots[i] = _OMITTED
                omitted = True
        for name, default, ok in self.kwonly:
            if name not in tail:
                if default is _EMPTY:
                    self._fail(args, kwargs)
                if ok:
                    tail[name] = default
            elif not ok and tail[name] is default:
                del tail[name]
        if omitted:
            if n > npos:
                # Extra positional arguments follow: no gap can be closed.
                slots = [
                    defaults[i] if v is _OMITTED else v for i, v in enumerate(slots)
                ]
            else:
                slots = self._close_gaps(slots, tail)
        positional = tuple(slots)
        if n > npos:
            positional += args[npos:]
        return positional, tail

    def _close_gaps(self, slots: list[Any], tail: dict[str, Any]) -> list[Any]:
        """Leave defaults that cannot be keys out of the positional part.

        Trailing ones are dropped (the call falls back on them). A value
        after a gap moves to the keywords, unless its parameter is
        positional-only: then the gaps before it are filled with their
        defaults again, as a plain positional call would pass them.
        """
        last_posonly = -1
        for i in range(self.nposonly):
            if slots[i] is not _OMITTED:
                last_posonly = i
        for i in range(last_posonly):
            if slots[i] is _OMITTED:
                slots[i] = self.defaults[i]
        first_gap = next((i for i, v in enumerate(slots) if v is _OMITTED), None)
        if first_gap is None:
            return slots
        for i in range(first_gap, len(slots)):
            if slots[i] is not _OMITTED:
                tail[self.names[i]] = slots[i]
        return slots[:first_gap]

    def reserved_keyword_clash(self, reserved: tuple[str, ...]) -> str | None:
        """A keyword-only parameter whose name the handle keeps for itself."""
        for name, _, _ in self.kwonly:
            if name in reserved:
                return name
        return None
