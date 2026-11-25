"""Stored results follow the code and constants a query uses (G11, G16).

A query's own bytecode is not all its result depends on: it calls helper
functions and reads module constants, closure values and defaults. The hash
a result is stored under covers those too, so editing one between runs
recomputes the result with no ``version=`` to bump. Each test runs a small
program twice on one ``cache_dir``, as two runs of a script would, with one
thing edited in between.
"""

from __future__ import annotations

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
        return json.dumps([1, {value}])

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
