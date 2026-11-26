"""Stored results follow the code and constants a query uses (G11, G16).

A query's own bytecode is not all its result depends on: it calls helper
functions and reads module constants, closure values and defaults. The hash
a result is stored under covers those too, so editing one between runs
recomputes the result with no ``version=`` to bump. Each test runs a small
program twice on one ``cache_dir``, as two runs of a script would, with one
thing edited in between.
"""

from __future__ import annotations

import dataclasses
import sys
import textwrap
import types
from pathlib import Path
from typing import Any

import pytest

from cascade import Engine
from cascade._code_refs import refs_hash

BACKENDS = ("mdbx", "sqlite")

PROGRAM = """
import json

runs = []

@engine.input
def n():
    return 2

@engine.query
def q():
    runs.append("q")
    return normalize(n()) + LIMIT

@engine.query
def above():
    runs.append("above")
    return q() % 2

# Defined below the query that calls it, as helpers often are.
def normalize(x):
    return inner(x) {op} 1

def inner(x):
    return x * {factor}

LIMIT = {limit}
"""


def _run(cache: Path, backend: str, source: str, call: str = "q") -> tuple[Any, list]:
    """One run of ``source`` as a script; returns the result and what ran."""
    engine = Engine(cache_dir=cache, cache_backend=backend)
    namespace: dict[str, Any] = {"engine": engine, "__name__": "program"}
    try:
        exec(textwrap.dedent(source), namespace)
        return namespace[call](), list(namespace.get("runs", []))
    finally:
        engine.shutdown()


def _program(op: str = "+", factor: int = 1, limit: int = 0) -> str:
    return PROGRAM.format(op=op, factor=factor, limit=limit)


@pytest.mark.parametrize("backend", BACKENDS)
def test_unchanged_program_is_served_from_disk(tmp_path: Path, backend: str) -> None:
    assert _run(tmp_path, backend, _program()) == (3, ["q"])
    assert _run(tmp_path, backend, _program()) == (3, [])


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    ("edit", "expected"),
    [
        ({"op": "*"}, 2),  # the helper the query calls
        ({"factor": 10}, 21),  # a helper of that helper
        ({"limit": 1000}, 1003),  # a module constant
    ],
    ids=["helper", "helper of a helper", "constant"],
)
def test_an_edit_outside_the_query_body_recomputes_its_result(
    tmp_path: Path, backend: str, edit: dict[str, Any], expected: int
) -> None:
    assert _run(tmp_path, backend, _program()) == (3, ["q"])
    assert _run(tmp_path, backend, _program(**edit)) == (expected, ["q"])
    # And the new result is stored under the new code.
    assert _run(tmp_path, backend, _program(**edit)) == (expected, [])


@pytest.mark.parametrize("backend", BACKENDS)
def test_dependents_follow_the_new_result_and_stop_at_an_equal_one(
    tmp_path: Path, backend: str
) -> None:
    assert _run(tmp_path, backend, _program(), "above") == (1, ["above", "q"])
    # 3 -> 5: q runs again, and its dependent sees an equal result above it.
    value, ran = _run(tmp_path, backend, _program(limit=2), "above")
    assert value == 1
    assert "q" in ran
    # 3 -> 4: the dependent's own result changes.
    assert _run(tmp_path, backend, _program(limit=1), "above")[0] == 0


FACTORY = """
runs = []

def make(k, default):
    @engine.query(name="scaled")
    def scaled(x, bonus=default):
        runs.append(x)
        return k * x + bonus
    return scaled

q = lambda: make({k}, {default})(3)
"""


@pytest.mark.parametrize("backend", BACKENDS)
def test_closure_values_and_defaults_are_part_of_the_stored_hash(
    tmp_path: Path, backend: str
) -> None:
    def run(k: int, default: int) -> tuple[Any, list]:
        return _run(tmp_path, backend, FACTORY.format(k=k, default=default))

    assert run(2, 0) == (6, [3])
    assert run(2, 0) == (6, [])
    assert run(5, 0) == (15, [3])  # the captured k
    assert run(5, 1) == (16, [3])  # the default


CLASSES = """
runs = []

class Base:
    def rate(self):
        return {rate}

class Pricer(Base):
    @staticmethod
    def round(x):
        return x {rounding} 1

@engine.query
def q():
    runs.append("q")
    return Pricer.round(Pricer().rate())
"""


@pytest.mark.parametrize("backend", BACKENDS)
def test_methods_of_a_class_the_query_names_are_followed(
    tmp_path: Path, backend: str
) -> None:
    def run(rate: int, rounding: str) -> tuple[Any, list]:
        source = CLASSES.format(rate=rate, rounding=rounding)
        return _run(tmp_path, backend, source)

    assert run(10, "+") == (11, ["q"])
    assert run(10, "+") == (11, [])
    assert run(10, "-") == (9, ["q"])  # a static method of the class
    assert run(20, "-") == (19, ["q"])  # a method of its base class


@pytest.mark.parametrize("backend", BACKENDS)
def test_a_function_reached_through_a_module_is_followed(
    tmp_path: Path, backend: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = """
    import program_helpers
    runs = []

    @engine.query
    def q():
        runs.append("q")
        return program_helpers.double(4)
    """

    def run(body: str) -> tuple[Any, list]:
        module = types.ModuleType("program_helpers")
        exec(f"def double(x):\n    return {body}\n", module.__dict__)
        monkeypatch.setitem(sys.modules, "program_helpers", module)
        return _run(tmp_path, backend, source)

    assert run("x * 2") == (8, ["q"])
    assert run("x * 2") == (8, [])
    assert run("x + x + 1") == (9, ["q"])


COUNTERS = """
import json

runs = []
total_runs = 0
seen = {{}}

def make():
    calls = 0

    @engine.query(name="counted")
    def counted():
        nonlocal calls
        global total_runs
        calls += 1
        total_runs += 1
        seen[len(seen)] = True
        runs.append("counted")
        # The lambda is code nested in the query: the names the query
        # assigns must still be known as such when it has some.
        return json.dumps(sorted([1, {value}], key=lambda v: v))

    return counted

q = make()
"""


@pytest.mark.parametrize("backend", BACKENDS)
def test_counters_and_mutable_objects_do_not_disturb_the_stored_hash(
    tmp_path: Path, backend: str
) -> None:
    """Names the query assigns, and mutable containers, are not constants."""

    def run(value: int, warm_up: str = "") -> tuple[Any, list]:
        source = COUNTERS.format(value=value) + warm_up
        return _run(tmp_path, backend, source)

    assert run(1) == ("[1, 1]", ["counted"])
    # Another run, in which the containers hold something else by the time
    # the query is first called.
    assert run(1, "seen['x'] = 1\ntotal_runs = 7\n") == ("[1, 1]", [])
    assert run(2) == ("[1, 2]", ["counted"])


def test_version_still_forces_a_recompute(tmp_path: Path) -> None:
    source = """
    runs = []

    @engine.query(version={version})
    def q():
        runs.append("q")
        return 1
    """
    assert _run(tmp_path, "sqlite", source.format(version=1)) == (1, ["q"])
    assert _run(tmp_path, "sqlite", source.format(version=1)) == (1, [])
    assert _run(tmp_path, "sqlite", source.format(version=2)) == (1, ["q"])


def test_input_defaults_follow_their_helpers(tmp_path: Path) -> None:
    source = """
    runs = []

    def start():
        return {start}

    @engine.input
    def n():
        return start()

    @engine.query
    def q():
        runs.append("q")
        return n() + 1
    """
    assert _run(tmp_path, "sqlite", source.format(start=1)) == (2, ["q"])
    assert _run(tmp_path, "sqlite", source.format(start=1)) == (2, [])
    assert _run(tmp_path, "sqlite", source.format(start=5)) == (6, ["q"])


def test_a_default_of_a_helper_is_part_of_the_stored_hash(tmp_path: Path) -> None:
    """The query's own defaults are in the call's key; a helper's are not."""
    source = """
    runs = []

    def scale(x, factor={factor}):
        return x * factor

    @engine.query
    def q():
        runs.append("q")
        return scale(3)
    """
    assert _run(tmp_path, "sqlite", source.format(factor=2)) == (6, ["q"])
    assert _run(tmp_path, "sqlite", source.format(factor=2)) == (6, [])
    assert _run(tmp_path, "sqlite", source.format(factor=5)) == (15, ["q"])


def test_an_edit_to_a_query_under_a_wrapping_decorator_recomputes_it(
    tmp_path: Path,
) -> None:
    # D40: the stored hash covered the wrapper's bytecode, and refs_hash
    # followed __wrapped__ to the body's references but not to the body.
    source = """
    import functools

    runs = []

    def logged(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        return wrapper

    @engine.query
    @logged
    def q():
        runs.append("q")
        return {value}
    """
    assert _run(tmp_path, "sqlite", source.format(value=1)) == (1, ["q"])
    assert _run(tmp_path, "sqlite", source.format(value=1)) == (1, [])
    assert _run(tmp_path, "sqlite", source.format(value=2)) == (2, ["q"])


def test_an_edit_under_two_decorators_or_in_the_decorator_recomputes(
    tmp_path: Path,
) -> None:
    source = """
    import functools

    runs = []

    def scale():
        return {scale}

    def logged(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return scale() * fn(*args, **kwargs)
        return wrapper

    def traced(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        return wrapper

    @engine.query
    @traced
    @logged
    def q():
        runs.append("q")
        return {value}
    """
    assert _run(tmp_path, "sqlite", source.format(scale=1, value=1)) == (1, ["q"])
    assert _run(tmp_path, "sqlite", source.format(scale=1, value=1)) == (1, [])
    # The body, two wrappers down.
    assert _run(tmp_path, "sqlite", source.format(scale=1, value=2)) == (2, ["q"])
    # A helper the inner wrapper calls.
    assert _run(tmp_path, "sqlite", source.format(scale=3, value=2)) == (6, ["q"])
    assert _run(tmp_path, "sqlite", source.format(scale=3, value=2)) == (6, [])


def test_the_fix_for_wrapped_bodies_leaves_rewritten_queries_hashes_alone() -> None:
    # A query Cascade rewrote wraps its source function; its hash is that of
    # the source's references only, as before D40's fix, so results stored
    # by 0.4.0 stay valid.
    from cascade import Engine

    engine = Engine()
    handle = engine.query(_rewritable)
    registered = engine._store.queries[handle.id]  # noqa: SLF001
    assert getattr(registered, "__cascade_rewritten__", False)
    assert getattr(registered, "__wrapped__") is _rewritable
    assert refs_hash(registered) == refs_hash(_rewritable) != ""
    assert handle((1, 2, 5)) == 3


_REWRITE_LIMIT = 3


def _rewritable(xs: Any) -> int:
    return sum(x for x in xs if x < _REWRITE_LIMIT)


# -- refs_hash itself -------------------------------------------------------


def _namespace(source: str) -> dict[str, Any]:
    namespace: dict[str, Any] = {"__name__": "program"}
    exec(textwrap.dedent(source), namespace)
    return namespace


def test_refs_hash_is_empty_for_a_function_that_refers_to_nothing() -> None:
    ns = _namespace("def f(x):\n    return len(str(x)) + 1\n")
    assert refs_hash(ns["f"]) == ""


def test_refs_hash_ignores_the_standard_library_and_mutable_objects() -> None:
    source = """
    import json, os
    STATE = {"k": 1}
    ITEMS = [1, 2]

    def f(x):
        ITEMS.append(x)
        return json.dumps(STATE) + os.sep
    """
    assert refs_hash(_namespace(source)["f"]) == ""


def test_refs_hash_tells_values_of_different_types_apart() -> None:
    def hashed(literal: str) -> str:
        ns = _namespace(f"K = {literal}\ndef f():\n    return K\n")
        return refs_hash(ns["f"])

    hashes = [hashed(v) for v in ("1", "1.0", "True", "'1'", "(1,)", "None")]
    assert len(set(hashes)) == len(hashes)
    assert hashed("1") == hashed("1")
    assert hashed("[1]") == ""  # a list is not a constant


def test_refs_hash_handles_recursion_and_names_not_defined_yet() -> None:
    source = """
    def even(n):
        return n == 0 or odd(n - 1)

    def odd(n):
        return n != 0 and even(n - 1)

    def f(n):
        return even(n) and later(n)
    """
    ns = _namespace(source)
    before = refs_hash(ns["f"])
    assert before
    ns["later"] = lambda n: True
    assert refs_hash(ns["f"]) != before


def test_refs_hash_of_something_that_is_not_a_function_is_empty() -> None:
    assert refs_hash(len) == ""
    assert refs_hash(object()) == ""  # type: ignore[arg-type]


def test_refs_hash_follows_properties_and_frozen_dataclasses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # dataclasses looks the class's module up by name.
    monkeypatch.setitem(sys.modules, "program", types.ModuleType("program"))

    def hashed(area: str, unit: str) -> str:
        source = f"""
        import dataclasses

        @dataclasses.dataclass(frozen=True)
        class Unit:
            name: str

        class Shape:
            @property
            def area(self):
                return {area}

        UNIT = Unit({unit!r})

        def f(shape):
            return Shape.area.fget(shape), UNIT
        """
        return refs_hash(_namespace(source)["f"])

    base = hashed("1", "m")
    assert hashed("1", "m") == base
    assert hashed("2", "m") != base
    assert hashed("1", "cm") != base


def test_refs_hash_edge_cases_of_what_is_followed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each kind of reference that contributes nothing, or something."""
    import datetime

    class OwnZone(datetime.tzinfo):  # immutable by type, unknown to the codec
        def utcoffset(self, dt: Any) -> datetime.timedelta:
            return datetime.timedelta(0)

    library = compile(
        "def from_a_library(x):\n    return x\n",
        "/venv/lib/site-packages/somelib/core.py",
        "exec",
    )
    library_ns: dict[str, Any] = {"__name__": "somelib.core"}
    exec(library, library_ns)

    @dataclasses_mutable
    class Box:
        value: int

    own_module = types.ModuleType("program_tools")
    own_module.TOOL = 5  # type: ignore[attr-defined]

    def hashed(**names: Any) -> str:
        source = """
        def f(x, *, scale=SCALE):
            def inner():
                nonlocal x
                x += 1
                return lambda: HELPER
            return (DEEP, BOX, ZONE, LIB(x), dumps, TOOLS.TOOL, TOOLS.MISSING,
                    TOOLS, inner, scale)
        """
        ns = _namespace("import json\nfrom json import dumps\n")
        ns.update(names)
        exec(textwrap.dedent(source), ns)
        return refs_hash(ns["f"])

    deep: Any = 1
    for _ in range(30):
        deep = (deep,)
    base = dict(
        SCALE=1,
        HELPER=lambda: 1,
        DEEP=deep,
        BOX=Box(1),
        ZONE=OwnZone(),
        LIB=library_ns["from_a_library"],
        TOOLS=own_module,
    )
    first = hashed(**base)
    assert first and hashed(**base) == first
    # Not followed: a value nested too deep, a mutable dataclass, a tzinfo
    # the codec cannot encode, a library function, a module itself.
    assert (
        hashed(**{**base, "DEEP": (deep,), "BOX": Box(2), "ZONE": OwnZone()}) == first
    )
    # Followed: a keyword-only default, a module's constant, a helper used
    # only inside a lambda nested in a nested function.
    assert hashed(**{**base, "SCALE": 2}) != first
    own_module.TOOL = 6  # type: ignore[attr-defined]
    assert hashed(**base) != first
    own_module.TOOL = 5  # type: ignore[attr-defined]
    assert hashed(**{**base, "HELPER": lambda: 2}) != first


def dataclasses_mutable(cls: type) -> type:
    import dataclasses

    return dataclasses.dataclass(cls)


def test_refs_hash_with_a_closure_cell_not_assigned_yet() -> None:
    def make() -> Any:
        def f() -> Any:
            return later  # noqa: F821 - assigned below, after the hash

        before = refs_hash(f)
        later = 3  # noqa: F841
        return before, refs_hash(f)

    before, after = make()
    assert before == ""
    assert after != ""


def test_refs_hash_never_fails_the_query() -> None:
    class Hostile:
        @property
        def __class__(self) -> Any:  # type: ignore[override]
            raise RuntimeError("no")

    ns = _namespace("def f():\n    return THING\n")
    ns["THING"] = Hostile()
    assert refs_hash(ns["f"]) == "unhashable-refs"


# -- refs_hash in detail ----------------------------------------------------


def _hashed(source: str, **names: Any) -> str:
    """``refs_hash`` of the ``f`` that ``source`` defines, ``names`` as globals."""
    namespace: dict[str, Any] = {"__name__": "program", **names}
    exec(textwrap.dedent(source), namespace)
    return refs_hash(namespace["f"])


def _function_at(filename: str, module: str) -> Any:
    """A function as if imported from ``filename``, in module ``module``."""
    namespace: dict[str, Any] = {"__name__": module}
    exec(compile("def lib(x):\n    return x + 1\n", filename, "exec"), namespace)
    return namespace["lib"]


def _module_at(name: str, filename: str | None) -> types.ModuleType:
    module = types.ModuleType(name)
    if filename is not None:
        module.__file__ = filename
    module.CONST = 1  # type: ignore[attr-defined]
    return module


SITE_PACKAGES = "/venv/lib/python3.12/site-packages/somelib/"
WINDOWS_SITE_PACKAGES = "C:\\venv\\Lib\\site-packages\\somelib\\"
DIST_PACKAGES = "/usr/lib/python3/dist-packages/somelib/"


def test_refs_hash_does_not_follow_the_standard_library_cascade_or_packages() -> None:
    """Functions, classes and module constants of each, at any package depth."""
    source = """
    import xml.etree.ElementTree as ET
    import cascade._keys as KEYS
    from json import dumps
    from json.decoder import JSONDecoder
    from email.utils import formatdate
    from xml.etree.ElementTree import tostring
    from cascade import Engine
    from cascade._keys import shape

    def f(x):
        return (
            dumps, JSONDecoder, formatdate, tostring, Engine, shape,
            ET.VERSION, KEYS._NEGATIVE_ZERO,
            SITE(x), WINDOWS_SITE(x), DIST(x),
            SITE_MOD.CONST, WINDOWS_SITE_MOD.CONST, DIST_MOD.CONST,
        )
    """
    packages = {
        "SITE": _function_at(SITE_PACKAGES + "core.py", "somelib.core"),
        "WINDOWS_SITE": _function_at(WINDOWS_SITE_PACKAGES + "core.py", "somelib.core"),
        "DIST": _function_at(DIST_PACKAGES + "core.py", "somelib.core"),
        "SITE_MOD": _module_at("somelib", SITE_PACKAGES + "__init__.py"),
        "WINDOWS_SITE_MOD": _module_at(
            "somelib", WINDOWS_SITE_PACKAGES + "__init__.py"
        ),
        "DIST_MOD": _module_at("somelib", DIST_PACKAGES + "__init__.py"),
    }
    assert _hashed(source, **packages) == ""


def test_refs_hash_follows_the_program_wherever_its_files_are() -> None:
    for folder in ("/srv/app/", "C:\\Users\\me\\app\\"):
        fn = _function_at(folder + "tools.py", "tools")
        assert _hashed("def f(x):\n    return FN(x)\n", FN=fn) != ""
        module = _module_at("tools", folder + "tools.py")
        assert _hashed("def f():\n    return MOD.CONST\n", MOD=module) != ""
    module = _module_at("tools", None)  # made at run time, from no file
    assert _hashed("def f():\n    return MOD.CONST\n", MOD=module) != ""


@pytest.mark.parametrize(
    "source",
    [
        "A, B = {a}, {b}\ndef f():\n    return A, B\n",
        "import types\n"
        "M = types.ModuleType('program_m')\n"
        "M.A, M.B = {a}, {b}\n"
        "def f():\n    return M.A, M.B\n",
        "def make(a, b):\n"
        "    def f():\n        return a, b\n"
        "    return f\n"
        "f = make({a}, {b})\n",
        "def f(a={a}, b={b}):\n    return a, b\n",
        "def f(*, a={a}, b={b}):\n    return a, b\n",
    ],
    ids=["globals", "module attributes", "closure", "defaults", "keyword defaults"],
)
def test_refs_hash_says_which_name_holds_a_value(source: str) -> None:
    """A value moving to another name changes the hash.

    Even when the name it left holds something that is not followed (a list
    here): the two functions return different things.
    """
    first = _hashed(source.format(a="[1]", b="1"))
    second = _hashed(source.format(a="1", b="[1]"))
    assert first and second and first != second


CLASS_MEMBERS = """
from json import dumps

def helper():
    return 1

class C:
    a = {kind}({a})
    b = {kind}({b})

def f():
    return C
"""


@pytest.mark.parametrize("kind", ["staticmethod", "property"])
def test_refs_hash_says_which_member_of_a_class_holds_a_function(kind: str) -> None:
    first = _hashed(CLASS_MEMBERS.format(kind=kind, a="helper", b="dumps"))
    second = _hashed(CLASS_MEMBERS.format(kind=kind, a="dumps", b="helper"))
    assert first and second and first != second


def test_refs_hash_follows_a_base_class_listed_after_a_library_one() -> None:
    source = """
    import json

    class Rates:
        def rate(self):
            return {rate}

    class Pricer(json.JSONEncoder, Rates):
        pass

    def f():
        return Pricer
    """
    assert _hashed(source.format(rate=1)) != _hashed(source.format(rate=2))


def test_refs_hash_tells_a_missing_module_attribute_from_one_set_to_none() -> None:
    settings = types.ModuleType("program_settings")
    source = "def f():\n    return SETTINGS.LIMIT\n"
    missing = _hashed(source, SETTINGS=settings)
    settings.LIMIT = None  # type: ignore[attr-defined]
    assert _hashed(source, SETTINGS=settings) != missing


def test_refs_hash_goes_on_past_a_name_it_skips() -> None:
    """A builtin, a name the function assigns, or an empty cell is skipped."""
    source = """
    counter = 0

    def f(x):
        global counter
        counter += 1
        return len(x) + K
    """
    assert _hashed(source, K=1) != _hashed(source, K=2)

    def counting(k: int) -> Any:
        a_count = 0

        def f() -> int:
            nonlocal a_count
            a_count += 1
            return a_count + k

        return f

    assert refs_hash(counting(1)) != refs_hash(counting(2))

    def hashed_before_assignment(k: int) -> str:
        def f() -> Any:
            return a_later, k

        hashed = refs_hash(f)
        a_later = 0
        return hashed

    assert hashed_before_assignment(1) != hashed_before_assignment(2)


def test_refs_hash_counts_a_class_or_the_function_itself_once() -> None:
    classes = """
    class C:
        def m(self):
            return C

    def f():
        return {body}
    """
    assert _hashed(classes.format(body="C")) == _hashed(classes.format(body="C, C"))
    # Its own body is the bytecode hash's part, not the references'.
    assert _hashed("def f(n):\n    return 1 if n < 2 else n * f(n - 1)\n") == ""


@dataclasses.dataclass(frozen=True)
class _Node:
    child: Any


@dataclasses.dataclass(frozen=True)
class _Box:
    items: Any


@dataclasses.dataclass(frozen=True)
class _Unset:
    value: int = dataclasses.field(init=False)


def test_refs_hash_of_nested_and_odd_frozen_values() -> None:
    def nested(make: Any, leaf: int) -> Any:
        value: Any = leaf
        for _ in range(15):
            value = make(value)
        return value

    cycle = _Node(None)
    object.__setattr__(cycle, "child", cycle)
    source = "def f():\n    return K, TUPLE, CHAIN, CYCLE, UNSET, BOX\n"
    base = dict(
        K=1,
        TUPLE=nested(lambda v: (v,), 1),
        CHAIN=nested(_Node, 1),
        CYCLE=cycle,  # too deep to be a constant, wherever it is cut
        UNSET=_Unset(),  # a field never set: the codec cannot take it
        BOX=_Box([1]),
    )
    first = _hashed(source, **base)
    assert first not in ("", "unhashable-refs")
    # Neither of the odd values stops the others from being followed.
    assert _hashed(source, **{**base, "K": 2}) != first
    # Fifteen levels deep is followed, through tuples and through dataclasses.
    assert _hashed(source, **{**base, "TUPLE": nested(lambda v: (v,), 2)}) != first
    assert _hashed(source, **{**base, "CHAIN": nested(_Node, 2)}) != first
    # A frozen dataclass holding a list is not a constant.
    assert _hashed(source, **{**base, "BOX": _Box([2])}) == first


def test_the_stored_hash_is_worked_out_once_per_registration() -> None:
    """Base hash plus refs hash, kept until the function is registered again."""
    from cascade._store import _StoredHashes

    ns = _namespace("K = 1\ndef f():\n    return K\n")
    hashes = _StoredHashes({"f": ns["f"]})
    hashes["f"] = "base"
    first = hashes["f"]
    assert first == "base+" + refs_hash(ns["f"])
    ns["K"] = 2
    assert hashes["f"] == hashes.get("f") == first
    hashes["f"] = "base"
    assert hashes["f"] == "base+" + refs_hash(ns["f"]) != first
    hashes["no function"] = "base"
    assert hashes["no function"] == "base"
    assert hashes.get("unknown", "default") == "default"
    with pytest.raises(KeyError, match="unknown"):
        hashes["unknown"]


def test_the_layers_of_a_function_end_at_the_innermost_or_a_loop() -> None:
    from cascade._code_refs import _layers

    def plain() -> None:
        pass

    def inner() -> None:
        pass

    def outer() -> None:
        pass

    outer.__wrapped__ = inner  # type: ignore[attr-defined]
    looped = lambda: None  # noqa: E731
    looped.__wrapped__ = looped  # type: ignore[attr-defined]
    assert _layers(plain) == [plain]
    assert _layers(outer) == [outer, inner]
    assert _layers(looped) == [looped]


def test_a_long_wrapped_chain_stops_and_a_callable_layer_is_passed_over() -> None:
    from cascade._code_refs import _layers

    chain: list[Any] = []
    for _ in range(100):
        f = lambda: None  # noqa: E731
        if chain:
            f.__wrapped__ = chain[-1]  # type: ignore[attr-defined]
        chain.append(f)
    assert len(_layers(chain[-1])) == 64

    namespace = _namespace(
        """
        def body():
            return {value}

        class Middle:  # a callable object between two functions
            def __init__(self, fn):
                self.__wrapped__ = fn
            def __call__(self):
                return self.__wrapped__()

        middle = Middle(body)

        def outer():
            return middle()

        outer.__wrapped__ = middle
        """.replace("{value}", "1")
    )
    edited = _namespace(
        """
        def body():
            return 2

        class Middle:
            def __init__(self, fn):
                self.__wrapped__ = fn
            def __call__(self):
                return self.__wrapped__()

        middle = Middle(body)

        def outer():
            return middle()

        outer.__wrapped__ = middle
        """
    )
    # The body two layers down, past the object, is part of the hash.
    assert refs_hash(namespace["outer"]) != refs_hash(edited["outer"])
