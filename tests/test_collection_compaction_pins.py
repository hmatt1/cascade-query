"""In-memory log compaction against snapshots and transactions (G24, G25).

Compaction folds old diffs into the log's base. It must keep every diff a
live snapshot or an open transaction can still ask for, also when an older
pinned revision predates the collection (D12).
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
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
TIMEOUT = 10.0  # for threads that must not wait for each other


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


# --- a compaction that falls due inside a mutation ---
#
# In-memory compaction asks the pipelines how far they have read the log,
# which takes the pipeline runtime's lock. A pipeline holds that lock while it
# reads the collection, and a read takes the collection's lock; a mutation
# holds the collection's lock until its change is recorded. So a compaction
# that falls due inside a mutation must wait until the mutation has let go of
# the collection, or a reader and a writer end up waiting for each other.


def test_compaction_due_inside_a_mutation_does_not_block_a_pipeline_reading() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2])
    reader_in_pipeline = threading.Event()
    writer_in_mutation = threading.Event()

    def plus_length(x: int) -> int:
        if not reader_in_pipeline.is_set():
            # The first element: the pipeline holds its lock here. Go on only
            # once the writer is inside its mutation, so that reading the
            # collection below has to wait for the mutation to end.
            reader_in_pipeline.set()
            assert writer_in_mutation.wait(TIMEOUT)
        return x + len(xs)

    @engine.query
    def shifted() -> list[int]:
        return [plus_length(x) for x in xs]

    def many() -> Iterator[int]:
        # extend() consumes this inside the mutation.
        writer_in_mutation.set()
        yield from range(CHURN)

    outcomes: dict[str, Any] = {}

    def reader() -> None:
        try:
            outcomes["reader"] = shifted()
        except BaseException as exc:  # noqa: BLE001
            outcomes["reader"] = exc

    def writer() -> None:
        try:
            assert reader_in_pipeline.wait(TIMEOUT)
            # Enough changes at once for a compaction check to fall due.
            xs.extend(many())
            outcomes["writer"] = "done"
        except BaseException as exc:  # noqa: BLE001
            outcomes["writer"] = exc

    threads = [
        threading.Thread(target=reader, daemon=True),
        threading.Thread(target=writer, daemon=True),
    ]
    for t in threads:
        t.start()
    deadline = time.monotonic() + TIMEOUT
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.monotonic()))
    assert not any(t.is_alive() for t in threads), "reader and writer deadlocked"

    # The scenario is the one described: the query is a pipeline, and the
    # reader ran at the revision from before the mutation.
    assert [p["reducer"] for p in engine.inspect_pipelines()] == ["list"]
    assert outcomes == {"reader": [3, 4], "writer": "done"}
    everything = [1, 2, *range(CHURN)]
    assert shifted() == [x + len(everything) for x in everything]


# --- a compaction that passes a pipeline while it runs ---
#
# A pipeline that lags far behind is not waited for: compaction folds the
# diffs it has not read, and the pipeline starts again from the contents. A
# pipeline checks for that before it reads the log, but the compaction asked
# for the pipelines' checkpoints before this one took the runtime's lock, and
# can fold the diffs between the pipeline's check and its read.


class _Seen:
    """A stage function that records what it was called with."""

    def __init__(self) -> None:
        self.values: list[int] = []

    def __call__(self, value: int) -> int:
        self.values.append(value)
        return value


def test_compaction_between_a_pipelines_check_and_its_read_makes_it_start_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2])
    seen = _Seen()

    @engine.query
    def total() -> int:
        return sum(seen(x) for x in xs)

    assert total() == 3
    seen.values.clear()
    log = _log(xs)
    checkpoint = log.head  # how far the pipeline has read

    writer_about_to_fold = threading.Event()
    reader_checked = threading.Event()
    folded = threading.Event()
    base_when_checked: list[int] = []
    reader_thread: list[int] = []

    # Hold point in the writer: the first compaction that folds diffs the
    # pipeline has not read waits until the pipeline has made its check.
    fold = log.compact_to

    def held_compact_to(target: int) -> bool:
        if target <= checkpoint or writer_about_to_fold.is_set():
            return fold(target)
        writer_about_to_fold.set()
        assert reader_checked.wait(TIMEOUT)
        try:
            return fold(target)
        finally:
            folded.set()

    monkeypatch.setattr(log, "compact_to", held_compact_to)

    # Hold point in the reader: the pipeline counts the query's reads right
    # after its check, with nothing else in between. It goes on to read the
    # log only when the writer has folded.
    evaluator = engine._evaluator  # noqa: SLF001
    read_count = evaluator.current_frame_read_count

    def held_read_count() -> int:
        if threading.get_ident() in reader_thread and not reader_checked.is_set():
            base_when_checked.append(log.base_rev)
            reader_checked.set()
            assert folded.wait(TIMEOUT)
        return read_count()

    monkeypatch.setattr(evaluator, "current_frame_read_count", held_read_count)

    outcomes: dict[str, Any] = {}

    def writer() -> None:
        try:
            # Changes that leave the list short, so that the pipeline is
            # soon further behind than the list is long. Far more of them
            # than it takes; the loop ends at the hold point.
            for value in range(10, 10 + 10 * CHURN):
                if writer_about_to_fold.is_set():
                    break
                xs[0] = value
            outcomes["writer"] = "done"
        except BaseException as exc:  # noqa: BLE001
            outcomes["writer"] = exc

    def reader() -> None:
        try:
            assert writer_about_to_fold.wait(TIMEOUT)
            reader_thread.append(threading.get_ident())
            outcomes["reader"] = total()
        except BaseException as exc:  # noqa: BLE001
            outcomes["reader"] = exc

    threads = [
        threading.Thread(target=writer, daemon=True),
        threading.Thread(target=reader, daemon=True),
    ]
    for t in threads:
        t.start()
    deadline = time.monotonic() + TIMEOUT
    for t in threads:
        t.join(timeout=max(0.0, deadline - time.monotonic()))
    assert not any(t.is_alive() for t in threads), "reader or writer hung"

    # The scenario is the one described: when the pipeline checked, the log
    # still held everything after its checkpoint; when it read, it did not.
    assert base_when_checked == [checkpoint]
    assert log.base_rev > checkpoint
    final = list(xs)
    assert outcomes == {"writer": "done", "reader": sum(final)}
    # It started again from the contents: every element, not the changes.
    assert seen.values == final
    # And what it left behind is a whole state: one change, one call.
    seen.values.clear()
    xs.append(5)
    assert total() == sum(final) + 5
    assert seen.values == [5]
