"""In-memory log compaction against snapshots and transactions (G24, G25).

Compaction folds old diffs into the log's base. It must keep every diff a
live snapshot or an open transaction can still ask for, also when an older
pinned revision predates the collection (D12).
"""

from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

import pytest

from cascade import (
    CascadeDict,
    CascadeList,
    CascadeSet,
    Engine,
    Snapshot,
    SnapshotExpiredError,
)

BACKENDS = ("mdbx", "sqlite")
CHURN = 3_000  # enough appends for several in-memory compaction checks


def _log(collection: Any) -> Any:
    return collection._core.log  # noqa: SLF001


def test_every_snapshot_after_creation_keeps_its_contents_under_an_older_pin() -> None:
    engine = Engine()
    before_collection = engine.snapshot()
    xs = CascadeList(engine, [0])

    @engine.query
    def direct() -> list[int]:
        return [xs[i] for i in range(len(xs))]

    @engine.query
    def rewritten() -> int:
        return sum(v for v in xs)

    snapshots: list[tuple[Snapshot, int]] = []
    for i in range(1, CHURN + 1):
        xs.append(i)
        if i % 500 == 0:
            snapshots.append((engine.snapshot(), i))

    for snapshot, n in snapshots:
        assert direct(snapshot=snapshot) == list(range(n + 1))
        assert rewritten(snapshot=snapshot) == n * (n + 1) // 2
    assert direct(snapshot=before_collection) == []
    assert rewritten(snapshot=before_collection) == 0
    assert rewritten() == CHURN * (CHURN + 1) // 2

    # Releasing the pins lets compaction go on.
    before_collection.release()
    for snapshot, _ in snapshots:
        snapshot.release()
    for i in range(CHURN):
        xs.append(i)
    assert len(_log(xs).entries) <= 2 * (2 * CHURN + 1)
    assert _log(xs).base_rev > 0


@pytest.mark.parametrize("kind", ["set", "dict"])
def test_set_and_dict_snapshots_survive_compaction_with_an_older_pin(kind: str) -> None:
    engine = Engine()
    before_collection = engine.snapshot()
    if kind == "set":
        coll: Any = CascadeSet(engine, [-1])

        def churn(i: int) -> None:
            coll.add(i)
    else:
        coll = CascadeDict(engine, {-1: -1})

        def churn(i: int) -> None:
            coll[i] = i

    @engine.query
    def keys() -> list[int]:
        return sorted(coll)

    snapshot = engine.snapshot()
    for i in range(CHURN):
        churn(i)
    assert keys(snapshot=snapshot) == [-1]
    assert keys(snapshot=before_collection) == []
    assert keys() == [-1, *range(CHURN)]
    before_collection.release()
    snapshot.release()


def test_unpinned_reader_of_a_compacted_log_gets_the_public_error() -> None:
    engine = Engine()
    held = engine.snapshot()  # keeps input history, so only the log is short
    xs = CascadeList(engine, [1])

    @engine.query
    def n() -> int:
        return len(xs)

    @engine.query
    def total() -> int:
        return sum(v for v in xs)

    hand_built = Snapshot(engine.revision)
    for i in range(10):
        xs.append(i)
    # Fold the log past the revision the hand-built snapshot names. Nothing
    # pins that revision, so compaction owes it nothing.
    log = _log(xs)
    assert log.compact_to(log.head)
    with pytest.raises(SnapshotExpiredError, match="keep the snapshot"):
        n(snapshot=hand_built)
    with pytest.raises(SnapshotExpiredError):
        total(snapshot=hand_built)
    # The error is not cached, and head reads are untouched.
    with pytest.raises(SnapshotExpiredError):
        n(snapshot=hand_built)
    assert n() == 11
    assert total() == 46
    held.release()


@pytest.mark.parametrize("kind", ["list", "set", "dict"])
def test_rollback_of_a_collection_created_in_the_block(kind: str) -> None:
    engine = Engine()
    made: list[Any] = []
    with pytest.raises(RuntimeError, match="abort"):
        with engine.transaction():
            if kind == "list":
                coll: Any = CascadeList(engine, [])
                made.append(coll)
                for i in range(CHURN):
                    coll.append(i)
            elif kind == "set":
                coll = CascadeSet(engine)
                made.append(coll)
                for i in range(CHURN):
                    coll.add(i)
            else:
                coll = CascadeDict(engine)
                made.append(coll)
                for i in range(CHURN):
                    coll[i] = i
            raise RuntimeError("abort")
    assert len(made[0]) == 0
    assert _log(made[0]).head == 0


def test_rollback_restores_an_existing_collection_under_an_older_pin() -> None:
    engine = Engine()
    before_collection = engine.snapshot()
    xs = CascadeList(engine, ["a", "b"])

    @engine.query
    def joined() -> str:
        return "".join(xs)

    assert joined() == "ab"
    with pytest.raises(RuntimeError, match="abort"):
        with engine.transaction():
            for i in range(CHURN):
                xs.append(str(i))
            del xs[0]
            raise RuntimeError("abort")
    assert list(xs) == ["a", "b"]
    assert joined() == "ab"
    xs.append("c")
    assert joined() == "abc"
    assert joined(snapshot=before_collection) == ""
    before_collection.release()


def test_commit_after_compaction_checks_keeps_snapshots_and_contents() -> None:
    engine = Engine()
    before_collection = engine.snapshot()
    xs = CascadeList(engine, [1])

    @engine.query
    def n() -> int:
        return len(xs)

    snapshot = engine.snapshot()
    with engine.transaction():
        for i in range(CHURN):
            xs.append(i)
        assert n() == 1  # queries in the block see the engine as it was
    assert n() == CHURN + 1
    assert n(snapshot=snapshot) == 1
    snapshot.release()
    before_collection.release()
    # The transaction no longer holds the log back.
    for i in range(CHURN):
        xs.append(i)
    assert _log(xs).base_rev > 0
    assert n() == 2 * CHURN + 1


@pytest.mark.parametrize("backend", BACKENDS)
def test_rolled_back_named_collection_is_unchanged_in_memory_and_on_disk(
    tmp_path: Path, backend: str
) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        before_collection = engine.snapshot()
        xs = CascadeList(engine, [1, 2, 3], name="xs")
        with pytest.raises(RuntimeError, match="abort"):
            with engine.transaction():
                for i in range(CHURN):
                    xs.append(i)
                raise RuntimeError("abort")
        assert list(xs) == [1, 2, 3]
        with pytest.raises(RuntimeError, match="abort"):
            with engine.transaction():
                ys = CascadeList(engine, [], name="ys")
                for i in range(CHURN):
                    ys.append(i)
                raise RuntimeError("abort")
        assert list(ys) == []
        before_collection.release()
    finally:
        engine.shutdown()

    reopened = Engine(cache_dir=tmp_path, cache_backend=backend)
    try:
        assert list(CascadeList(reopened, name="xs")) == [1, 2, 3]
        assert list(CascadeList(reopened, name="ys")) == []
    finally:
        reopened.shutdown()


def test_snapshot_readers_and_a_writer_under_an_older_pin() -> None:
    """Threads: every snapshot read returns the length its snapshot saw."""
    engine = Engine()
    before_collection = engine.snapshot()
    xs = CascadeList(engine, [0])

    @engine.query
    def n() -> int:
        return len(xs)

    errors: list[BaseException] = []
    done = threading.Event()

    def writer() -> None:
        try:
            for i in range(2 * CHURN):
                xs.append(i)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            done.set()

    def reader() -> None:
        try:
            while not done.is_set():
                with engine.snapshot() as snapshot:
                    seen = n(snapshot=snapshot)
                    # Read again once the writer has moved on.
                    again = n(snapshot=snapshot)
                    if seen != again:
                        raise AssertionError(f"snapshot moved: {seen} -> {again}")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=writer, daemon=True)] + [
        threading.Thread(target=reader, daemon=True) for _ in range(3)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not any(t.is_alive() for t in threads), "a thread hung"
    assert errors == []
    assert n() == 2 * CHURN + 1
    assert n(snapshot=before_collection) == 0
    before_collection.release()


# --- reads at an older revision once the log no longer starts at zero ---
#
# The tests above hold a pin from before the collection exists, so its log
# keeps its base at revision zero for as long as they read. A reader at an
# older revision is counted from the base, and that is only visible when the
# base has moved: after a compaction, or after reopening a compacted
# collection.


def _collection(kind: str, engine: Engine, name: str | None = None) -> Any:
    """A collection of ints and a function that adds one to it."""
    if kind == "list":
        xs = CascadeList(engine, name=name)
        return xs, xs.append
    if kind == "set":
        tags = CascadeSet(engine, name=name)
        return tags, tags.add
    table = CascadeDict(engine, name=name)
    return table, lambda i: table.__setitem__(i, i)


@pytest.mark.parametrize("kind", ["list", "set", "dict"])
def test_snapshot_taken_after_a_compaction_keeps_its_contents(kind: str) -> None:
    engine = Engine()
    coll, add = _collection(kind, engine)

    @engine.query
    def contents() -> list[int]:
        return sorted(coll)

    for i in range(CHURN):
        add(i)
    assert _log(coll).base_rev > 0

    snapshot = engine.snapshot()
    for i in range(CHURN, CHURN + 5):
        add(i)
    assert contents(snapshot=snapshot) == list(range(CHURN))
    assert contents() == list(range(CHURN + 5))

    # The pin holds while compaction goes on behind it.
    later = engine.snapshot()
    for i in range(CHURN + 5, 2 * CHURN):
        add(i)
    assert contents(snapshot=later) == list(range(CHURN + 5))
    assert contents() == list(range(2 * CHURN))
    snapshot.release()
    later.release()


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("kind", ["list", "set", "dict"])
def test_snapshot_of_a_collection_reopened_from_a_compacted_state_keeps_its_contents(
    tmp_path: Path, kind: str, backend: str
) -> None:
    with Engine(cache_dir=tmp_path, cache_backend=backend) as engine:
        coll, add = _collection(kind, engine, name="c")
        for i in range(3):
            add(i)
        coll.compact()

    with Engine(cache_dir=tmp_path, cache_backend=backend) as engine:
        coll, add = _collection(kind, engine, name="c")
        # Loaded from the stored snapshot: the log starts at its revision.
        assert _log(coll).base_rev > 0

        @engine.query
        def contents() -> list[int]:
            return sorted(coll)

        snapshot = engine.snapshot()
        add(3)
        add(4)
        assert contents(snapshot=snapshot) == [0, 1, 2]
        assert contents() == [0, 1, 2, 3, 4]
        snapshot.release()
