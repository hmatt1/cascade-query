from __future__ import annotations

from cascade import Engine
from tests.scale_helpers import assert_internal_dependents_consistent


def test_internal_api_policy_lists_remain_explicit() -> None:
    # Keep policy lists explicit and stable so private coupling is deliberate.
    assert Engine._INTERNAL_TEST_API == ("_internals",)
    assert Engine._LEGACY_PRIVATE_SHIMS == (
        "_latest_input_version",
        "_input_version_at",
        "_dependency_changed_at",
        "_memos",
        "_dependents",
        "_revision",
        "_cancel_epoch",
        "_next_access_id",
        "_max_entries",
        "_inputs",
        "_queries",
        "_in_flight",
        "_lock",
    )


def test_internal_dependency_helpers_and_indexes_are_consistent() -> None:
    engine = Engine()

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        return tuple(source(name).splitlines())

    @engine.query
    def symbol_count(name: str) -> int:
        return len(parse(name))

    # Invariant probes flow through the dedicated _internals object.
    internals = engine._internals  # noqa: SLF001
    assert internals.latest_input_version((source.id, ("missing",))) is None
    assert internals.input_version_at((source.id, ("main",)), revision=0) is None

    source.set("main", "a\nb")
    latest = internals.latest_input_version((source.id, ("main",)))
    assert latest is not None
    assert latest.value == "a\nb"
    assert internals.input_version_at((source.id, ("main",)), revision=engine.revision) is latest
    assert engine._input_version_at((source.id, ("main",)), revision=engine.revision) is latest  # noqa: SLF001
    snap = engine.snapshot()
    assert symbol_count("main", snapshot=snap) == 2

    dep_key = ("query", parse.id, ("main",))
    assert internals.dependency_changed_at(dep_key, snap) == snap.revision
    # Legacy aliases remain available during migration.
    assert engine._latest_input_version((source.id, ("main",))) is not None  # noqa: SLF001
    assert engine._input_version_at((source.id, ("main",)), revision=0) is None  # noqa: SLF001
    assert engine._dependency_changed_at(dep_key, snap) == snap.revision  # noqa: SLF001
    assert_internal_dependents_consistent(engine)


def test_private_set_input_default_still_bumps_cancel_epoch() -> None:
    engine = Engine()

    before = engine._cancel_epoch  # noqa: SLF001
    engine._set_input("tests:manual_input", (), 1)  # noqa: SLF001
    assert engine._cancel_epoch == before + 1  # noqa: SLF001


def test_cache_hit_updates_access_order_monotonically() -> None:
    engine = Engine()

    @engine.input
    def source() -> int:
        return 0

    @engine.query
    def value() -> int:
        return source() + 1

    source.set(10)
    assert value() == 11
    memo_key = ("query", value.id, ())
    first_access = engine._memos[memo_key].last_access  # noqa: SLF001
    access_id_before_hit = engine._next_access_id  # noqa: SLF001

    # Cache-hit path should advance access ordering by exactly one step.
    assert value() == 11
    assert engine._next_access_id == access_id_before_hit + 1  # noqa: SLF001
    second_access = engine._memos[memo_key].last_access  # noqa: SLF001
    assert second_access == engine._next_access_id  # noqa: SLF001
    assert second_access > first_access
