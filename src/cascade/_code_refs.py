"""Hash of the code and constants a function uses but does not contain.

``hash_bytecode`` covers a query's own body. A stored result also depends on
what the body reaches by name: helper functions, classes, module constants,
closure values and default arguments. ``refs_hash`` follows those, so that
editing one of them between runs changes the hash a result is stored under
and the result is recomputed, with no ``version=`` to bump.

What is followed, starting from the function:

* global names its code loads, and ``module.attr`` where the global is a
  module;
* its closure cells, defaults and keyword defaults.

What each of those contributes:

* a Python function or a class from the program's own code: its bytecode,
  and whatever it refers to in turn (a class: its methods and those of its
  base classes);
* nothing for a name the function itself assigns (a ``nonlocal`` or
  ``global`` counter): that is not a constant;
* an immutable value (a number, a string, a tuple or frozenset of them, an
  enum member, a frozen dataclass, a date, a path, ...): its digest;
* anything else nothing: mutable containers and other objects, whose
  contents can differ from run to run without the code changing; queries,
  inputs and collections, which the engine tracks itself; the standard
  library and installed packages, which are too large to follow and change
  with a version (``Engine(cache_version=...)`` covers that).

Method calls on objects (``obj.method()``) are not followed: the class is
only known at run time.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import dis
import enum
import fractions
import hashlib
import pathlib
import sys
import types
import uuid
from typing import Any, Callable, Iterator

from ._serde import hash_bytecode, stable_value_digest

_MISSING = object()

_IMMUTABLE_TYPES: tuple[type, ...] = (
    type(None),
    bool,
    int,
    float,
    complex,
    str,
    bytes,
    range,
    decimal.Decimal,
    fractions.Fraction,
    datetime.date,
    datetime.time,
    datetime.timedelta,
    datetime.tzinfo,
    uuid.UUID,
    enum.Enum,
    pathlib.PurePath,
)

_LOAD_NAME_OPS = frozenset({"LOAD_GLOBAL", "LOAD_NAME"})
_LOAD_ATTR_OPS = frozenset({"LOAD_ATTR", "LOAD_METHOD"})
_STORE_OPS = frozenset({"STORE_GLOBAL", "DELETE_GLOBAL", "STORE_DEREF", "DELETE_DEREF"})


def _immutable(value: Any, depth: int = 0) -> bool:
    """True for a value whose content cannot change after it is made."""
    if depth > 20:
        return False
    if isinstance(value, _IMMUTABLE_TYPES):
        return True
    if isinstance(value, (tuple, frozenset)):
        return all(_immutable(x, depth + 1) for x in value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        params = getattr(type(value), "__dataclass_params__", None)
        if params is None or not params.frozen:
            return False
        return all(
            _immutable(getattr(value, f.name, None), depth + 1)
            for f in dataclasses.fields(value)
        )
    return False


def _own_code(obj: Any) -> bool:
    """True for a function or class of the program, not of a library."""
    module = getattr(obj, "__module__", None) or ""
    top = module.split(".", 1)[0]
    if top == "cascade" or top in sys.stdlib_module_names:
        return False
    code = getattr(obj, "__code__", None)
    if code is not None:
        filename = code.co_filename.replace("\\", "/")
        if "/site-packages/" in filename or "/dist-packages/" in filename:
            return False
    return True


def _own_module(module: types.ModuleType) -> bool:
    """True for a module of the program, not of a library."""
    top = (getattr(module, "__name__", None) or "").split(".", 1)[0]
    if top == "cascade" or top in sys.stdlib_module_names:
        return False
    filename = (getattr(module, "__file__", None) or "").replace("\\", "/")
    return "/site-packages/" not in filename and "/dist-packages/" not in filename


def _names_loaded(code: types.CodeType) -> Iterator[tuple[str, str | None]]:
    """``(global name, attribute or None)`` for each global the code loads.

    Nested functions and comprehensions are included. The attribute is the
    one read straight off the global, which matters when it is a module.
    """
    previous: str | None = None
    for ins in dis.get_instructions(code):
        if previous is not None:
            yield previous, ins.argval if ins.opname in _LOAD_ATTR_OPS else None
            previous = None
        if ins.opname in _LOAD_NAME_OPS:
            previous = ins.argval
    if previous is not None:  # pragma: no cover - code ends in a return
        yield previous, None
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            yield from _names_loaded(const)


def _names_assigned(code: types.CodeType) -> set[str]:
    """Global and closure names the code assigns or deletes, nested code included.

    A name the function writes is not a constant of it: a ``nonlocal`` or
    ``global`` counter holds another value on every run.
    """
    names = {
        ins.argval for ins in dis.get_instructions(code) if ins.opname in _STORE_OPS
    }
    for const in code.co_consts:
        if isinstance(const, types.CodeType):
            names |= _names_assigned(const)
    return names


def refs_hash(fn: Callable[..., Any]) -> str:
    """Digest of what ``fn`` refers to outside its own body; ``""`` if nothing."""
    fn = getattr(fn, "__wrapped__", fn)
    parts: list[str] = []
    seen: set[int] = set()

    def visit_value(label: str, value: Any) -> None:
        if isinstance(value, (staticmethod, classmethod)):
            value = value.__func__
        elif isinstance(value, property):
            for accessor in (value.fget, value.fset, value.fdel):
                if accessor is not None:
                    visit_value(label, accessor)
            return
        if isinstance(value, types.FunctionType):
            if not _own_code(value) or id(value) in seen:
                return
            seen.add(id(value))
            parts.append(f"{label}=fn:{hash_bytecode(value)}")
            visit_function(value)
        elif isinstance(value, type):
            if not _own_code(value) or id(value) in seen:
                return
            seen.add(id(value))
            parts.append(f"{label}=class:{value.__qualname__}")
            for cls in value.__mro__:
                if cls is object or not _own_code(cls):
                    continue
                for name, member in sorted(vars(cls).items(), key=lambda item: item[0]):
                    if isinstance(
                        member,
                        (types.FunctionType, staticmethod, classmethod, property),
                    ):
                        visit_value(f"{label}.{name}", member)
        elif _immutable(value):
            try:
                parts.append(f"{label}=value:{stable_value_digest(value)}")
            except Exception:  # noqa: BLE001 - a value the codec does not know
                return

    def visit_function(f: types.FunctionType) -> None:
        namespace = f.__globals__
        assigned = _names_assigned(f.__code__)
        for name, attr in _names_loaded(f.__code__):
            value = namespace.get(name, _MISSING)
            if value is _MISSING or name in assigned:
                continue  # a builtin, not defined yet, or not a constant
            if isinstance(value, types.ModuleType):
                if attr is not None and _own_module(value):
                    member = getattr(value, attr, _MISSING)
                    if member is not _MISSING:
                        visit_value(f"{name}.{attr}", member)
                continue
            visit_value(name, value)
        for name, cell in zip(f.__code__.co_freevars, f.__closure__ or ()):
            if name in assigned:
                continue
            try:
                visit_value(f"closure:{name}", cell.cell_contents)
            except ValueError:  # an empty cell: assigned later
                continue
        for i, default in enumerate(f.__defaults__ or ()):
            visit_value(f"default:{i}", default)
        for name, default in sorted((f.__kwdefaults__ or {}).items()):
            visit_value(f"default:{name}", default)

    if not isinstance(fn, types.FunctionType):
        return ""
    seen.add(id(fn))
    try:
        visit_function(fn)
    except Exception:  # noqa: BLE001 - never fail a query over its hash
        return "unhashable-refs"
    if not parts:
        return ""
    return hashlib.blake2b("\n".join(parts).encode("utf-8"), digest_size=12).hexdigest()
