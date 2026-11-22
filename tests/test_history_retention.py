"""Input history retention: no-op sets, revision pinning, trimming, log compaction."""

from __future__ import annotations

from typing import Any

from cascade import CascadeList, Engine


def _store(engine: Engine) -> Any:
    return engine._internals._store  # noqa: SLF001


def _versions(engine: Engine, handle: Any, *args: Any) -> list[Any]:
    return list(_store(engine).inputs.get((handle.id, args), []))


def _counter_pipeline() -> tuple[Engine, Any, Any, dict[str, int]]:
    engine = Engine()
    runs = {"double": 0}

    @engine.input
    def source(name: str) -> int:
        return 0

    @engine.query
    def double(name: str) -> int:
        runs["double"] += 1
        return source(name) * 2

    return engine, source, double, runs


def test_same_value_set_is_a_full_noop() -> None:
    engine, source, double, runs = _counter_pipeline()
    source.set("a", 5)
    assert double("a") == 10
    store = _store(engine)
    revision, epoch = engine.revision, store.cancel_epoch
    traces = len(engine.traces())

    assert source.set("a", 5) == revision
    assert engine.revision == revision
    assert store.cancel_epoch == epoch
    assert len(engine.traces()) == traces
    assert len(_versions(engine, source, "a")) == 1
    assert store.memos[("query", double.id, ("a",))].head_clean
    assert double("a") == 10
    assert runs["double"] == 1


def test_transaction_with_only_unchanged_values_does_not_bump() -> None:
    engine, source, _, _ = _counter_pipeline()
    source.set("a", 1)
    source.set("b", 2)
    revision = engine.revision
    with engine.transaction():
        source.set("a", 1)
        source.set("b", 2)
    assert engine.revision == revision


def test_transaction_bumps_once_and_skips_unchanged_inputs() -> None:
    engine, source, _, _ = _counter_pipeline()
    source.set("a", 1)
    source.set("b", 2)
    revision = engine.revision
    with engine.transaction():
        source.set("a", 1)
        source.set("b", 3)
    assert engine.revision == revision + 1
    assert [v.value for v in _versions(engine, source, "a")][-1] == 1
    assert _versions(engine, source, "a")[-1].revision < engine.revision
    assert _versions(engine, source, "b")[-1].value == 3


def test_transaction_that_returns_to_the_original_value_is_a_noop() -> None:
    engine, source, double, runs = _counter_pipeline()
    source.set("a", 1)
    assert double("a") == 2
    revision = engine.revision
    with engine.transaction():
        source.set("a", 7)
        source.set("a", 1)
    assert engine.revision == revision
    assert double("a") == 2
    assert runs["double"] == 1


def test_collection_mutations_in_one_transaction_store_one_version() -> None:
    engine = Engine()
    xs = CascadeList(engine, [])
    handle = xs._core.handle  # noqa: SLF001
    before = len(_versions(engine, handle))
    with engine.transaction():
        for i in range(10):
            xs.append(i)
    assert len(_versions(engine, handle)) <= before + 1
    assert _versions(engine, handle)[-1].value == xs._core.log.head  # noqa: SLF001
