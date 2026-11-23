"""Query and input identity: name collisions, stale handles and ``name=`` (D9)."""

from __future__ import annotations

import gc
from pathlib import Path
from typing import Any

import pytest

from cascade import CascadeList, Engine, StaleHandleError


def _define(source: str, namespace: dict[str, Any], name: str) -> Any:
    """Define ``name`` by executing ``source`` in ``namespace``, as a reload does."""
    exec(source, namespace)  # noqa: S102
    fn = namespace[name]
    fn.__module__ = "reloaded_module"
    return fn


def test_second_function_from_a_factory_replaces_the_first() -> None:
    engine = Engine()

    def make(k: int) -> Any:
        @engine.query
        def q() -> int:
            return k

        return q

    q1 = make(1)
    assert q1() == 1
    q2 = make(2)
    assert q2.id == q1.id
    # The new function does not see the old one's cached result ...
    assert q2() == 2
    # ... and the old handle cannot run the old function into q2's cache.
    with pytest.raises(StaleHandleError, match=r"name=\.\.\."):
        q1()
    assert q2() == 2
    assert "stale" in repr(q1)


def test_name_gives_factory_queries_separate_identities() -> None:
    engine = Engine()
    runs: dict[int, int] = {}

    def make(k: int) -> Any:
        @engine.query(name=f"scaled:{k}")
        def q(x: int) -> int:
            runs[k] = runs.get(k, 0) + 1
            return k * x

        return q

    q2, q3 = make(2), make(3)
    assert (q2.id, q3.id) == ("scaled:2", "scaled:3")
    assert [q2(5), q3(5), q2(5), q3(5)] == [10, 15, 10, 15]
    assert runs == {2: 1, 3: 1}


def test_name_gives_lambdas_separate_identities() -> None:
    engine = Engine()
    double = engine.query(lambda x: x * 2, name="double")
    triple = engine.query(lambda x: x * 3, name="triple")
    assert (double(4), triple(4), double(4)) == (8, 12, 8)


def test_named_factory_queries_keep_separate_pipelines() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])

    def make(k: int) -> Any:
        @engine.query(name=f"total-times-{k}")
        def q() -> int:
            return sum(x * k for x in xs)

        return q

    q2, q3 = make(2), make(3)
    assert (q2(), q3()) == (12, 18)
    xs.append(4)
    assert (q2(), q3(), q2()) == (20, 30, 20)


@pytest.mark.parametrize("name", ["", 3, b"q"])
def test_name_must_be_a_non_empty_string(name: Any) -> None:
    engine = Engine()
    with pytest.raises(TypeError, match="name="):
        engine.query(lambda: 1, name=name)
    with pytest.raises(TypeError, match="name="):
        engine.input(lambda: 1, name=name)


def test_reload_with_changed_code_makes_old_handles_stale() -> None:
    engine = Engine()
    namespace: dict[str, Any] = {}
    old = engine.query(_define("def q(x):\n    return x + 1\n", namespace, "q"))
    assert old(1) == 2
    new = engine.query(_define("def q(x):\n    return x + 100\n", namespace, "q"))
    assert new.id == old.id
    assert new(1) == 101
    with pytest.raises(StaleHandleError, match="reloaded_module:q"):
        old(1)
    # Every way of running a handle refuses a stale one.
    with pytest.raises(StaleHandleError):
        engine.compute_many([(old, (1,)), (new, (2,))])
    with pytest.raises(StaleHandleError):
        list(engine.compute_many_stream([(old, (1,))]))
    try:
        with pytest.raises(StaleHandleError):
            engine.submit(old, 1).result(timeout=20)
    finally:
        engine.shutdown()
    assert new(1) == 101


def test_reload_with_unchanged_code_keeps_results_and_handles() -> None:
    engine = Engine()
    namespace: dict[str, Any] = {"runs": []}
    source = "def q(x):\n    runs.append(x)\n    return x + 1\n"
    old = engine.query(_define(source, namespace, "q"))
    assert old(1) == 2
    new = engine.query(_define(source, namespace, "q"))
    # Same code, same globals, nothing captured: the same query.
    assert new(1) == 2
    assert old(1) == 2
    assert namespace["runs"] == [1]


def test_registering_one_function_twice_keeps_both_handles() -> None:
    engine = Engine()
    runs = [0]

    def body(x: int) -> int:
        runs[0] += 1
        return x * 2

    first = engine.query(body)
    second = engine.query(body, ttl=60.0)
    assert (first(2), second(2), first(2)) == (4, 4, 4)
    assert runs == [1]


def test_stale_handle_error_is_not_cached_in_the_calling_query() -> None:
    engine = Engine()
    namespace: dict[str, Any] = {}
    handles = {"dep": engine.query(_define("def q():\n    return 1\n", namespace, "q"))}

    @engine.input
    def tick() -> int:
        return 0

    @engine.query
    def parent() -> int:
        return handles["dep"]() + 1 + tick()

    assert parent() == 2
    replacement = engine.query(_define("def q():\n    return 10\n", namespace, "q"))
    tick.set(1)
    with pytest.raises(StaleHandleError):
        parent()
    # Pointing the caller at the new handle is enough; no error is stuck.
    handles["dep"] = replacement
    assert parent() == 12


def test_second_input_from_a_factory_replaces_the_first() -> None:
    engine = Engine()

    def make(default: int) -> Any:
        @engine.input
        def setting() -> int:
            return default

        return setting

    first = make(1)
    assert first() == 1
    first.set(5)
    second = make(2)
    # The first input's values are not served for the second one.
    assert second() == 2
    with pytest.raises(StaleHandleError, match=r"engine\.input\(name="):
        first()
    with pytest.raises(StaleHandleError):
        first.set(7)
    assert second() == 2


def test_name_gives_factory_inputs_separate_identities() -> None:
    engine = Engine()

    def make(key: str, default: int) -> Any:
        @engine.input(name=f"setting:{key}")
        def setting() -> int:
            return default

        return setting

    a, b = make("a", 1), make("b", 2)

    @engine.query
    def total() -> int:
        return a() + b()

    assert total() == 3
    a.set(10)
    assert (a(), b(), total()) == (10, 2, 12)


def test_registering_a_collections_hidden_input_again_keeps_it() -> None:
    # Two collection objects with one name register the same hidden input
    # function twice. That is the same input, not a replacement.
    engine = Engine()
    first = CascadeList(engine, [1])
    core_input = first._core.handle  # noqa: SLF001
    again = engine.input(core_input._fn)  # noqa: SLF001
    assert again() == core_input()


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_replacing_function_does_not_load_the_old_ones_results_from_disk(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:

        def make(k: int) -> Any:
            @engine.query
            def q(x: int) -> int:
                return k * x

            return q

        assert make(2)(5) == 10
        engine.flush_disk()
        second = make(3)
        assert second(5) == 15
        engine.flush_disk()
        third = make(2)
        assert third(5) == 10
    finally:
        engine.shutdown()


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_named_factory_queries_are_reused_across_sessions(
    tmp_path: Path, backend: str
) -> None:
    def session() -> tuple[list[int], int]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend)
        runs = [0]
        try:

            def make(k: int) -> Any:
                @engine.query(name=f"times:{k}")
                def q(x: int) -> int:
                    runs[0] += 1
                    return k * x

                return q

            return [make(2)(5), make(3)(5)], runs[0]
        finally:
            engine.shutdown()

    assert session() == ([10, 15], 2)
    assert session() == ([10, 15], 0)


def test_handles_are_tracked_weakly() -> None:
    engine = Engine()

    def body() -> int:
        return 1

    handle = engine.query(body)
    slot = ("query", handle.id)
    assert len(engine._store._handles[slot]) == 1  # noqa: SLF001
    del handle
    gc.collect()
    assert len(engine._store._handles[slot]) == 0  # noqa: SLF001
