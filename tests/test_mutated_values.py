"""Cached values changed in place are reported, not served (G1).

Queries hand out their cached object and inputs the value they were given.
Changing one in place changes what every later reader gets, and Cascade
cannot see it happen. ``Engine.check_mutations()`` checks everything the
engine holds. With ``Engine(check_mutations=True)`` a query also checks,
when its body finishes, that each value it read still has the digest it was
stored with; a changed one raises ``MutatedValueError``. Off by default, it
costs nothing then.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from cascade import Engine, MutatedValueError


def _graph(engine: Engine) -> dict[str, Any]:
    runs: dict[str, int] = {}

    @engine.input
    def n() -> int:
        return 2

    @engine.input
    def cfg() -> dict[str, int]:
        return {"factor": 1}

    @engine.query
    def rows() -> list[int]:
        runs["rows"] = runs.get("rows", 0) + 1
        return list(range(n()))

    @engine.query
    def total() -> int:
        runs["total"] = runs.get("total", 0) + 1
        return sum(rows()) * cfg()["factor"]

    @engine.input
    def salt() -> int:
        return 0

    @engine.query
    def report() -> str:
        return f"{total()}:{salt()}"

    return {"n": n, "cfg": cfg, "rows": rows, "total": total, "salt": salt,
            "report": report, "runs": runs}  # fmt: skip


def test_a_query_that_changes_another_querys_result_is_stopped() -> None:
    engine = Engine(check_mutations=True)

    @engine.query
    def base() -> list[int]:
        return [1, 2]

    @engine.query
    def with_total() -> list[int]:
        values = base()
        values.append(sum(values))  # meant to build a new list
        return values

    with pytest.raises(MutatedValueError, match="base") as info:
        with_total()
    message = str(info.value)
    assert "with_total" in message
    assert "changed in place" in message
    assert info.value.keys == (("query", base.id, ()),)
    # The changed result was dropped: the next read computes it again.
    assert base() == [1, 2]
    # Never cached: the query runs, and is stopped, again.
    with pytest.raises(MutatedValueError):
        with_total()


def test_a_caller_that_changed_a_result_is_caught_by_the_next_reader_that_runs() -> (
    None
):
    engine = Engine(check_mutations=True)
    q = _graph(engine)
    assert q["report"]() == "1:0"
    q["rows"]().append(99)  # outside any query
    # Nothing runs, so nothing is checked: the cached report is served.
    assert q["report"]() == "1:0"
    # Something makes a reader run: it reads the changed list and stops.
    q["cfg"].set({"factor": 2})
    with pytest.raises(MutatedValueError, match="rows"):
        q["total"]()
    # The changed result was dropped, so the retry is right.
    assert q["total"]() == 2
    assert q["report"]() == "2:0"


def test_an_input_value_changed_without_set_raises_until_it_is_set() -> None:
    engine = Engine(check_mutations=True)
    q = _graph(engine)
    assert q["total"]() == 1
    q["cfg"]()["factor"] = 10  # instead of cfg.set(...)
    q["n"].set(3)  # total runs again and reads the changed value
    for _ in range(2):
        with pytest.raises(MutatedValueError, match="set\\(\\)") as info:
            q["total"]()
        assert info.value.keys == (("input", q["cfg"].id, ()),)
    q["cfg"].set({"factor": 10})
    assert q["total"]() == 30


def test_check_mutations_finds_what_no_query_has_read_since() -> None:
    engine = Engine(check_mutations=True)
    q = _graph(engine)
    engine.check_mutations()  # empty engine
    assert q["report"]() == "1:0"
    engine.check_mutations()  # nothing changed
    q["rows"]().append(99)
    q["cfg"]()["factor"] = 5
    with pytest.raises(MutatedValueError) as info:
        engine.check_mutations()
    assert set(info.value.keys) == {
        ("query", q["rows"].id, ()),
        ("input", q["cfg"].id, ()),
    }
    # The result was dropped; the input stays changed until it is set.
    with pytest.raises(MutatedValueError, match="cfg"):
        engine.check_mutations()
    q["cfg"].set({"factor": 1})
    engine.check_mutations()
    assert q["rows"]() == [0, 1]


def test_a_query_that_catches_the_error_for_a_changed_result_recovers() -> None:
    engine = Engine(check_mutations=True)
    q = _graph(engine)

    @engine.query
    def guarded() -> Any:
        try:
            return q["total"]()
        except MutatedValueError:
            return "changed"

    assert guarded() == 1
    q["rows"]().append(99)
    q["cfg"].set({"factor": 2})  # total runs again; rows does not
    # Bringing guarded up to date runs total, whose check finds the changed
    # list and drops it; guarded's own run then gets total from a fresh
    # rows. Its fallback is not needed.
    assert guarded() == 2
    assert q["rows"]() == [0, 1]
    q["n"].set(4)
    assert guarded() == 12


def test_a_changed_input_cannot_be_caught_away() -> None:
    """Whatever read it, directly or through a caught error, is stopped."""
    engine = Engine(check_mutations=True)
    q = _graph(engine)

    @engine.query
    def guarded() -> Any:
        try:
            return q["total"]()
        except MutatedValueError:
            return "changed"

    assert guarded() == 1
    q["cfg"]()["factor"] = 7
    q["n"].set(3)
    with pytest.raises(MutatedValueError, match="cfg"):
        guarded()
    q["cfg"].set({"factor": 7})
    assert guarded() == 21


def test_setting_the_original_value_repairs_a_changed_input() -> None:
    """The digest is unchanged, so nothing reruns, but the object is replaced."""
    engine = Engine(check_mutations=True)
    q = _graph(engine)
    assert q["total"]() == 1
    revision = engine.revision
    q["cfg"]()["factor"] = 9
    q["cfg"].set({"factor": 1})
    assert engine.revision == revision
    assert q["cfg"]() == {"factor": 1}
    engine.check_mutations()
    with engine.transaction():
        q["cfg"]()["factor"] = 9
        q["cfg"].set({"factor": 1})
    assert q["cfg"]() == {"factor": 1}
    assert engine.revision == revision


def test_values_that_are_never_compared_by_content_are_not_checked() -> None:
    engine = Engine(check_mutations=True)

    class Node:  # cannot be encoded: compared by identity, kept as it is
        def __init__(self) -> None:
            self.children: list[int] = []

    @engine.input
    def n() -> int:
        return 1

    @engine.query
    def tree() -> Node:
        return Node()

    @engine.query(memoize=False)
    def scratch() -> list[int]:
        return [n()]

    @engine.query
    def reader() -> int:
        tree().children.append(1)
        scratch().append(2)
        return n()

    assert reader() == 1
    n.set(2)
    assert reader() == 2
    engine.check_mutations()


def test_a_snapshot_read_checks_the_value_as_of_the_snapshot() -> None:
    engine = Engine(check_mutations=True)

    @engine.input
    def items() -> list[int]:
        return [1]

    @engine.query
    def count() -> int:
        return len(items())

    old = items()
    with engine.snapshot() as before:
        items.set([1, 2])
        old.append(5)  # the value as of the snapshot
        assert count() == 2  # the current value is fine
        with pytest.raises(MutatedValueError, match="items"):
            count(snapshot=before)


def test_async_queries_check_what_they_read() -> None:
    engine = Engine(check_mutations=True)

    @engine.query
    async def base() -> list[int]:
        return [1, 2]

    @engine.query
    async def with_total() -> list[int]:
        values = await base()
        values.append(sum(values))
        return values

    async def main() -> None:
        with pytest.raises(MutatedValueError, match="base"):
            await with_total()
        assert await base() == [1, 2]

    asyncio.run(main())


@pytest.mark.parametrize("backend", ["mdbx", "sqlite"])
def test_a_result_loaded_from_disk_is_checked_too(tmp_path: Any, backend: str) -> None:
    def session() -> tuple[Engine, dict[str, Any]]:
        engine = Engine(cache_dir=tmp_path, cache_backend=backend, check_mutations=True)
        return engine, _graph(engine)

    engine, q = session()
    assert q["total"]() == 1
    engine.shutdown()
    engine, q = session()
    try:
        assert q["rows"]() == [0, 1]  # loaded
        q["rows"]().append(5)
        q["cfg"].set({"factor": 3})
        with pytest.raises(MutatedValueError, match="rows"):
            q["total"]()
        assert q["total"]() == 3
    finally:
        engine.shutdown()


def test_an_input_set_to_an_object_that_is_later_changed_is_caught() -> None:
    engine = Engine(check_mutations=True)
    data = [1, 2]

    @engine.input
    def source() -> list[int]:
        return []

    @engine.query
    def size() -> int:
        return len(source())

    source.set(data)
    assert size() == 2
    data.append(3)  # the caller kept a reference to what it set
    with pytest.raises(MutatedValueError, match="source"):
        engine.check_mutations()


def test_something_that_cannot_be_encoded_put_into_a_result_counts_as_a_change() -> (
    None
):
    engine = Engine(check_mutations=True)
    q = _graph(engine)
    assert q["total"]() == 1
    q["rows"]().append(object())
    with pytest.raises(MutatedValueError, match="rows"):
        engine.check_mutations()


def test_stored_hashes_of_unknown_names() -> None:
    engine = Engine(check_mutations=True)
    q = _graph(engine)
    hashes = engine._store.query_hashes
    assert hashes.get("no:such-query") is None
    assert hashes.get("no:such-query", "x") == "x"
    with pytest.raises(KeyError):
        hashes["no:such-query"]
    assert hashes[q["rows"].id] == hashes.get(q["rows"].id)


def test_by_default_queries_do_not_check_but_check_mutations_does() -> None:
    engine = Engine()
    with pytest.raises(TypeError, match="must be a bool"):
        Engine(check_mutations=1)  # type: ignore[arg-type]

    @engine.query
    def base() -> list[int]:
        return [1, 2]

    @engine.query
    def with_total() -> list[int]:
        values = base()
        values.append(sum(values))
        return values

    assert with_total() == [1, 2, 3]  # not checked: served as computed
    assert base() == [1, 2, 3]
    with pytest.raises(MutatedValueError, match="base"):
        engine.check_mutations()
    assert base() == [1, 2]  # dropped and computed again


def test_values_read_after_ones_that_are_not_checked_are_still_checked() -> None:
    """A cached error or a ``memoize=False`` result read first skips only itself."""
    engine = Engine(check_mutations=True)

    @engine.query(cache_exceptions=True)
    def failing() -> int:
        raise ValueError("cached")

    @engine.query(memoize=False)
    def scratch() -> list[int]:
        return [0]

    @engine.query
    def base() -> list[int]:
        return [1, 2]

    @engine.query
    def after_error() -> int:
        try:
            failing()
        except ValueError:
            pass
        values = base()
        values.append(3)  # meant to build a new list
        return len(values)

    @engine.query
    def after_unmemoized() -> int:
        scratch()
        values = base()
        values.append(3)
        return len(values)

    for reader in (after_error, after_unmemoized):
        with pytest.raises(MutatedValueError) as info:
            reader()
        assert info.value.keys == (("query", base.id, ()),)
        assert base() == [1, 2]  # dropped and computed again


def test_check_mutations_goes_past_values_it_does_not_check() -> None:
    engine = Engine()

    @engine.query(cache_exceptions=True)
    def failing() -> int:
        raise ValueError("cached")

    @engine.query(memoize=False)
    def scratch() -> list[int]:
        return [0]

    @engine.query
    def base() -> list[int]:
        return [1, 2]

    # Stored in this order: the changed result comes after the other two.
    with pytest.raises(ValueError):
        failing()
    assert scratch() == [0]
    base().append(3)
    with pytest.raises(MutatedValueError) as info:
        engine.check_mutations()
    assert info.value.keys == (("query", base.id, ()),)
    assert base() == [1, 2]


def test_setting_an_equal_value_keeps_the_object_the_input_holds() -> None:
    """Nothing changed, so nothing is replaced: only a changed object is."""
    engine = Engine(check_mutations=True)
    first = [1, 2]

    @engine.input
    def source() -> list[int]:
        return []

    source.set(first)
    revision = engine.revision
    source.set([1, 2])
    assert engine.revision == revision
    assert source() is first
    engine.check_mutations()


def test_the_error_names_every_changed_value_and_says_what_to_do() -> None:
    engine = Engine()

    @engine.input
    def cfg() -> dict[str, int]:
        return {"a": 1}

    @engine.query
    def rows() -> list[int]:
        return [1]

    rows_name = f"query:{rows.id}()"
    cfg_name = f"input:{cfg.id}()"

    def message() -> str:
        with pytest.raises(MutatedValueError) as info:
            engine.check_mutations()
        return str(info.value)

    rows().append(2)
    cfg()["a"] = 2
    both = message()
    assert both.startswith(
        f"the value of {rows_name}, {cfg_name} was changed in place after "
        "Cascade stored it. Queries and inputs hand out the stored object"
    )
    assert (
        "every later reader gets. Build a new value instead of changing the "
        "returned one (for example [*rows, x] instead of rows.append(x))"
    ) in both
    assert (
        "computed again on its next use. Give an input a new value with set() "
        "instead of changing the one it returned"
    ) in both

    # Only the advice for what was changed: a query result, or an input.
    cfg.set({"a": 1})
    rows().append(3)
    only_result = message()
    assert "Build a new value" in only_result
    assert "Give an input" not in only_result
    cfg()["a"] = 5
    only_input = message()
    assert "Give an input" in only_input
    assert "Build a new value" not in only_input


def test_a_dependency_that_raised_has_no_value_to_check() -> None:
    engine = Engine(check_mutations=True)

    @engine.input
    def n() -> int:
        return 0

    @engine.query
    def failing() -> int:
        raise ValueError("cached")

    @engine.query
    def reader() -> str:
        try:
            failing()
        except ValueError:
            pass
        return f"ran at {n()}"

    assert reader() == "ran at 0"
    n.set(1)
    assert reader() == "ran at 1"
