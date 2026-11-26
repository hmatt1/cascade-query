"""Queries, ``submit`` and a second engine inside a transaction block (G25).

Inside ``with engine.transaction():`` a collection's own changes show on the
object at once, while queries run in the block see the engine as it was
before the block. These tests pin what that means for:

* a query reading a collection changed earlier in the block, with a plain
  Python body and with a rewritten comprehension or reducer, inside the
  block, after a commit and after a rollback, and whether a result computed
  in the block is served afterwards;
* a collection created in the block, unnamed and named;
* ``engine.submit`` from inside the block (G6);
* two engines: one changing its state while the other has a block open,
  transactions on both at once from two threads or two tasks of one thread,
  and two engines on one ``cache_dir`` with named collections (G26).

After each step every read is compared with plain Python run on the
collection's contents at that point.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from cascade import (
    CascadeDict,
    CascadeList,
    CascadeSet,
    Engine,
    PersistentCacheError,
    QueryCancelled,
)

BACKENDS = ("mdbx", "sqlite")
KINDS = ("list", "set", "dict")
TIMEOUT = 30.0


class Abort(Exception):
    pass


@pytest.fixture
def engines() -> Iterator[Callable[..., Engine]]:
    """Make engines that are shut down when the test ends."""
    made: list[Engine] = []

    def make(**kwargs: Any) -> Engine:
        engine = Engine(**kwargs)
        made.append(engine)
        return engine

    yield make
    for engine in made:
        engine.shutdown()


def _run_block(engine: Engine, body: Callable[[], None], *, commit: bool) -> None:
    """Run ``body`` in a transaction that commits, or raises at its end."""
    if commit:
        with engine.transaction():
            body()
        return
    with pytest.raises(Abort):
        with engine.transaction():
            body()
            raise Abort


# --- one collection of each kind, its queries and plain Python ---------------
#
# ``rewritten`` is written with comprehensions and reducers the engine turns
# into incremental pipelines; ``looped`` reads the same collection with a
# ``for`` loop and direct reads, which are tracked but never rewritten.
# ``plain`` computes what both must return from a plain copy of the contents.


class _Case:
    def __init__(self, engine: Engine, kind: str, *, name: str | None = None) -> None:
        self.kind = kind
        if kind == "list":
            xs = CascadeList(engine, [3, 1, 4, 1, 5], name=name)
            self.coll: Any = xs

            @engine.query
            def rewritten() -> Any:
                return (
                    sum(x for x in xs),
                    [x * 2 for x in xs if x % 2],
                    sorted(xs),
                    len(xs),
                    max(x for x in xs) if xs else None,
                )

            @engine.query
            def looped() -> Any:
                out = []
                for x in xs:
                    out.append(x)
                return out, (xs[0] if xs else None), 4 in xs

        elif kind == "set":
            s = CascadeSet(engine, [1, 2, 3], name=name)
            self.coll = s

            @engine.query
            def rewritten() -> Any:
                return (
                    sum(x for x in s),
                    sorted(x for x in s if x > 1),
                    len(s),
                    {x * 10 for x in s},
                )

            @engine.query
            def looped() -> Any:
                out = []
                for x in s:
                    out.append(x)
                return sorted(out), 2 in s

        else:
            d = CascadeDict(engine, {"a": 1, "b": 2}, name=name)
            self.coll = d

            @engine.query
            def rewritten() -> Any:
                return (
                    sum(v for v in d.values()),
                    {k: v + 1 for k, v in d.items()},
                    sorted(d),
                    len(d),
                )

            @engine.query
            def looped() -> Any:
                out = []
                for k in d:
                    out.append((k, d[k]))
                return out, d.get("b"), "c" in d

        self.rewritten = rewritten
        self.looped = looped

    def contents(self) -> Any:
        """A plain copy of the collection, read outside any query."""
        if self.kind == "list":
            return list(self.coll)
        if self.kind == "set":
            return set(self.coll)
        return dict(self.coll)

    def plain(self, contents: Any) -> tuple[Any, Any]:
        """What ``rewritten`` and ``looped`` return over ``contents``."""
        if self.kind == "list":
            xs = list(contents)
            return (
                (
                    sum(xs),
                    [x * 2 for x in xs if x % 2],
                    sorted(xs),
                    len(xs),
                    max(xs) if xs else None,
                ),
                (list(xs), xs[0] if xs else None, 4 in xs),
            )
        if self.kind == "set":
            s = set(contents)
            return (
                (sum(s), sorted(x for x in s if x > 1), len(s), {x * 10 for x in s}),
                (sorted(s), 2 in s),
            )
        d = dict(contents)
        return (
            (sum(d.values()), {k: v + 1 for k, v in d.items()}, sorted(d), len(d)),
            (list(d.items()), d.get("b"), "c" in d),
        )

    def reads(self) -> tuple[Any, Any]:
        return self.rewritten(), self.looped()

    def first_changes(self) -> None:
        """Append/add, remove/discard and item assignment."""
        c = self.coll
        if self.kind == "list":
            c.append(9)
            c.remove(1)
            c[0] = 7
        elif self.kind == "set":
            c.add(10)
            c.discard(1)
        else:
            c["a"] = 10
            c["c"] = 3

    def more_changes(self) -> None:
        c = self.coll
        if self.kind == "list":
            c.insert(1, 2)
            del c[-1]
        elif self.kind == "set":
            c.add(4)
            c.remove(2)
        else:
            del c["b"]
            c["d"] = 4

    def change_outside(self) -> None:
        c = self.coll
        if self.kind == "list":
            c.append(11)
        elif self.kind == "set":
            c.add(11)
        else:
            c["z"] = 11


# --- 1. a query reading a collection changed earlier in the block ------------


@pytest.mark.parametrize("warm", [True, False], ids=["warm", "cold"])
@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
@pytest.mark.parametrize("kind", KINDS)
def test_query_in_the_block_reads_the_collection_as_before_the_block(
    engines: Callable[..., Engine], kind: str, commit: bool, warm: bool
) -> None:
    # ``warm``: the queries ran before the block, so their pipelines and
    # results exist; ``cold``: they run for the first time inside it, and
    # build their pipelines from the contents as of before the block.
    engine = engines()
    case = _Case(engine, kind)
    before = case.contents()
    if warm:
        assert case.reads() == case.plain(before)
    in_block: list[Any] = []

    def body() -> None:
        case.first_changes()
        changed = case.contents()
        assert changed != before  # the object shows the change at once
        assert case.reads() == case.plain(before)
        case.more_changes()
        # A second round of changes, and the queries called again: the
        # results computed above are the right ones for this revision.
        assert case.reads() == case.plain(before)
        in_block.append(case.contents())

    _run_block(engine, body, commit=commit)
    after = in_block[0] if commit else before
    assert case.contents() == after
    assert case.reads() == case.plain(after)
    assert case.reads() == case.plain(after)  # and again, from the cache
    case.change_outside()
    assert case.reads() == case.plain(case.contents())


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_rolled_back_list_and_dict_keep_their_order(
    engines: Callable[..., Engine], commit: bool
) -> None:
    # Positions matter for a list and iteration order for a dict: the rows a
    # ``for`` loop sees after the block are exactly plain Python's.
    engine = engines()
    xs = CascadeList(engine, ["a", "b", "c"])
    d = CascadeDict(engine, {"x": 1, "y": 2, "z": 3})

    @engine.query
    def rows() -> Any:
        return [x for x in xs], [(k, v) for k, v in d.items()], list(reversed(xs))

    assert rows() == (["a", "b", "c"], [("x", 1), ("y", 2), ("z", 3)], ["c", "b", "a"])

    def body() -> None:
        del xs[0]
        xs.insert(0, "new")
        xs.append("a")
        del d["x"]
        d["x"] = 9
        d["w"] = 0
        assert rows() == (
            ["a", "b", "c"],
            [("x", 1), ("y", 2), ("z", 3)],
            ["c", "b", "a"],
        )

    _run_block(engine, body, commit=commit)
    plain_xs = ["new", "b", "c", "a"] if commit else ["a", "b", "c"]
    plain_d = {"y": 2, "z": 3, "x": 9, "w": 0} if commit else {"x": 1, "y": 2, "z": 3}
    assert list(xs) == plain_xs
    assert list(d.items()) == list(plain_d.items())
    assert rows() == (plain_xs, list(plain_d.items()), plain_xs[::-1])


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_collection_and_input_read_together_are_one_consistent_state(
    engines: Callable[..., Engine], commit: bool
) -> None:
    engine = engines()
    xs = CascadeList(engine, [1, 2, 3])

    @engine.input
    def factor() -> int:
        return 1

    @engine.query
    def scaled() -> list[int]:
        f = factor()
        return [x * f for x in xs]

    assert scaled() == [1, 2, 3]

    def body() -> None:
        factor.set(10)
        xs.append(4)
        # Neither the queued input nor the collection's change: both as
        # before the block, never one without the other.
        assert scaled() == [1, 2, 3]
        assert factor() == 1

    _run_block(engine, body, commit=commit)
    assert scaled() == ([10, 20, 30, 40] if commit else [1, 2, 3])


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_snapshot_taken_in_the_block_holds_the_state_before_it(
    engines: Callable[..., Engine], commit: bool
) -> None:
    # G24 and G25 together: a snapshot taken inside the block pins the
    # revision before it, and keeps reading that after the block.
    engine = engines()
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def total() -> int:
        return sum(x for x in xs)

    assert total() == 3
    held: list[Any] = []

    def body() -> None:
        xs.append(10)
        held.append(engine.snapshot())
        assert total(snapshot=held[0]) == 3

    _run_block(engine, body, commit=commit)
    with held[0] as snap:
        assert total(snapshot=snap) == 3
        assert total() == (13 if commit else 3)
        xs.append(100)
        assert total(snapshot=snap) == 3
        assert total() == (113 if commit else 103)


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_compute_many_and_an_async_query_in_the_block_see_the_state_before_it(
    engines: Callable[..., Engine], commit: bool
) -> None:
    # Worker threads of compute_many are outside the block (G25), and read at
    # the revision of the call: the one before the block. An event loop run
    # by the block itself is inside it.
    engine = engines()
    xs = CascadeList(engine, [1, 2, 3])

    @engine.query
    def total() -> int:
        return sum(x for x in xs)

    @engine.query
    def evens() -> list[int]:
        return [x for x in xs if x % 2 == 0]

    @engine.query
    async def doubled() -> list[int]:
        await asyncio.sleep(0)
        return [x * 2 for x in xs]

    async def awaited() -> list[int]:
        return await doubled()

    def body() -> None:
        xs.append(4)
        xs.remove(1)
        assert engine.compute_many([(total, ()), (evens, ())], workers=2) == [6, [2]]
        assert asyncio.run(awaited()) == [2, 4, 6]

    _run_block(engine, body, commit=commit)
    final = [2, 3, 4] if commit else [1, 2, 3]
    assert engine.compute_many([(total, ()), (evens, ())], workers=2) == [
        sum(final),
        [x for x in final if x % 2 == 0],
    ]
    assert asyncio.run(awaited()) == [x * 2 for x in final]


def test_changes_that_cancel_out_still_commit_one_revision(
    engines: Callable[..., Engine],
) -> None:
    # Decided, not specified: the collection's log moved (two diffs), so the
    # block commits a revision even though the contents are as before. The
    # results are plain Python's either way.
    engine = engines()
    xs = CascadeList(engine, [1, 2])
    d = CascadeDict(engine, {"k": 1})

    @engine.query
    def view() -> Any:
        return sum(x for x in xs), dict(d)

    assert view() == (3, {"k": 1})
    revision = engine.revision
    with engine.transaction():
        xs.append(5)
        xs.pop()
        d["k"] = 2
        d["k"] = 1
        assert view() == (3, {"k": 1})
    assert engine.revision == revision + 1
    assert view() == (3, {"k": 1})
    xs.append(7)
    assert view() == (10, {"k": 1})


# --- 2. a collection created in the block ------------------------------------


def _make(engine: Engine, kind: str, name: str | None = None) -> Any:
    if kind == "list":
        return CascadeList(engine, [1, 2, 3], name=name)
    if kind == "set":
        return CascadeSet(engine, {1, 2, 3}, name=name)
    return CascadeDict(engine, {1: 1, 2: 2, 3: 3}, name=name)


def _plain_copy(coll: Any) -> Any:
    if isinstance(coll, CascadeList):
        return list(coll)
    if isinstance(coll, CascadeSet):
        return set(coll)
    return dict(coll)


def _empty_like(coll: Any) -> Any:
    return type(_plain_copy(coll))()


def _add_one(coll: Any, value: int) -> None:
    if isinstance(coll, CascadeList):
        coll.append(value)
    elif isinstance(coll, CascadeSet):
        coll.add(value)
    else:
        coll[value] = value


def _register_readers(engine: Engine, coll: Any) -> tuple[Any, Any]:
    @engine.query
    def total() -> int:
        return sum(x for x in coll)

    @engine.query
    def contents() -> Any:
        if isinstance(coll, CascadeDict):
            return dict(coll)
        if isinstance(coll, CascadeSet):
            return set(coll)
        return list(coll)

    return total, contents


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
@pytest.mark.parametrize("kind", KINDS)
def test_collection_created_in_the_block(
    engines: Callable[..., Engine], kind: str, commit: bool
) -> None:
    # Specified (README, G25): queries in the block read a collection created
    # there as empty, and a rollback undoes its contents. Decided: after a
    # rollback the object stays usable as an empty collection.
    engine = engines()
    made: list[Any] = []
    readers: list[Any] = []

    def body() -> None:
        coll = _make(engine, kind)
        made.append(coll)
        readers.extend(_register_readers(engine, coll))
        total, contents = readers
        assert _plain_copy(coll) == _make_plain(kind)
        assert total() == 0
        assert contents() == _empty_like(coll)
        _add_one(coll, 10)
        assert total() == 0

    _run_block(engine, body, commit=commit)
    coll = made[0]
    total, contents = readers
    expected = _make_plain(kind)
    if commit:
        _add_plain(expected, 10)
    else:
        expected = type(expected)()
    assert _plain_copy(coll) == expected
    assert contents() == expected
    assert total() == sum(expected)
    _add_one(coll, 100)
    _add_plain(expected, 100)
    assert _plain_copy(coll) == expected
    assert contents() == expected
    assert total() == sum(expected)


def _make_plain(kind: str) -> Any:
    if kind == "list":
        return [1, 2, 3]
    if kind == "set":
        return {1, 2, 3}
    return {1: 1, 2: 2, 3: 3}


def _add_plain(plain: Any, value: int) -> None:
    if isinstance(plain, list):
        plain.append(value)
    elif isinstance(plain, set):
        plain.add(value)
    else:
        plain[value] = value


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_collection_created_in_the_block_reads_empty_through_an_older_snapshot(
    engines: Callable[..., Engine], commit: bool
) -> None:
    # G24: a collection created after the snapshot reads as empty through it.
    engine = engines()
    made: list[Any] = []
    with engine.snapshot() as before:

        def body() -> None:
            made.append(CascadeList(engine, [1, 2]))

        _run_block(engine, body, commit=commit)
        xs = made[0]

        @engine.query
        def total() -> int:
            return sum(x for x in xs)

        assert total(snapshot=before) == 0
        assert total() == (3 if commit else 0)
        xs.append(5)
        assert total(snapshot=before) == 0
        assert total() == (8 if commit else 5)


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("kind", KINDS)
def test_named_collection_created_in_the_block(
    engines: Callable[..., Engine],
    tmp_path: Path,
    kind: str,
    backend: str,
    commit: bool,
) -> None:
    engine = engines(cache_dir=tmp_path, cache_backend=backend)
    made: list[Any] = []
    readers: list[Any] = []

    def body() -> None:
        coll = _make(engine, kind, name="made")
        made.append(coll)
        readers.extend(_register_readers(engine, coll))
        assert readers[0]() == 0
        assert readers[1]() == _empty_like(coll)
        _add_one(coll, 10)

    _run_block(engine, body, commit=commit)
    coll = made[0]
    total, contents = readers
    expected = _make_plain(kind)
    if commit:
        _add_plain(expected, 10)
    else:
        expected = type(expected)()
    assert contents() == expected
    assert total() == sum(expected)
    # The object stays usable, and its next change reaches disk.
    _add_one(coll, 100)
    _add_plain(expected, 100)
    assert contents() == expected
    engine.shutdown()

    reopened = engines(cache_dir=tmp_path, cache_backend=backend)
    again = {"list": CascadeList, "set": CascadeSet, "dict": CascadeDict}[kind](
        reopened, name="made"
    )
    assert _plain_copy(again) == expected
    total, contents = _register_readers(reopened, again)
    assert contents() == expected
    assert total() == sum(expected)


@pytest.mark.parametrize("backend", BACKENDS)
def test_rolled_back_creation_of_a_named_collection_leaves_no_trace(
    engines: Callable[..., Engine], tmp_path: Path, backend: str
) -> None:
    engine = engines(cache_dir=tmp_path, cache_backend=backend)
    with pytest.raises(Abort):
        with engine.transaction():
            CascadeList(engine, [1, 2, 3], name="seeded")
            raise Abort
    engine.shutdown()

    reopened = engines(cache_dir=tmp_path, cache_backend=backend)
    # As if the block had never run: the name is new, so the seed is used.
    assert list(CascadeList(reopened, [1, 2, 3], name="seeded")) == [1, 2, 3]


def test_rolled_back_creation_does_not_fix_the_kind_of_a_name(
    engines: Callable[..., Engine], tmp_path: Path
) -> None:
    engine = engines(cache_dir=tmp_path)
    with pytest.raises(Abort):
        with engine.transaction():
            CascadeList(engine, [1], name="either")
            raise Abort
    engine.shutdown()
    reopened = engines(cache_dir=tmp_path)
    assert set(CascadeSet(reopened, {1, 2}, name="either")) == {1, 2}


@pytest.mark.parametrize("backend", BACKENDS)
def test_committed_creation_of_a_named_collection_fixes_its_kind(
    engines: Callable[..., Engine], tmp_path: Path, backend: str
) -> None:
    # Created in a block that commits with nothing in it, the collection is
    # recorded at the commit, so the name keeps its kind.
    engine = engines(cache_dir=tmp_path, cache_backend=backend)
    with engine.transaction():
        CascadeList(engine, name="kept")
    engine.shutdown()
    reopened = engines(cache_dir=tmp_path, cache_backend=backend)
    with pytest.raises(PersistentCacheError, match="is a list, not a set"):
        CascadeSet(reopened, name="kept")


@pytest.mark.parametrize("backend", BACKENDS)
def test_rolled_back_creation_is_recorded_by_its_first_later_change(
    engines: Callable[..., Engine], tmp_path: Path, backend: str
) -> None:
    engine = engines(cache_dir=tmp_path, cache_backend=backend)
    with pytest.raises(Abort):
        with engine.transaction():
            xs = CascadeList(engine, [1, 2], name="later")
            raise Abort
    assert list(xs) == []
    xs.append(7)
    engine.shutdown()
    reopened = engines(cache_dir=tmp_path, cache_backend=backend)
    assert list(CascadeList(reopened, [0], name="later")) == [7]


# --- 3. engine.submit inside the block (G6) ---------------------------------


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_submit_finishing_in_the_block_sees_the_state_before_it(
    engines: Callable[..., Engine], commit: bool
) -> None:
    # Allowed from the block (it is not inside a query). The pool thread is
    # outside the block, and the call is pinned at the revision before it.
    engine = engines()
    xs = CascadeList(engine, [1, 2, 3])
    tags = CascadeSet(engine, {"a"})

    @engine.input
    def bias() -> int:
        return 0

    @engine.query
    def total() -> int:
        return sum(x for x in xs) + bias()

    @engine.query
    def loop_total() -> int:
        out = 0
        for x in xs:
            out += x
        return out + len(tags)

    def body() -> None:
        xs.append(10)
        tags.add("b")
        bias.set(100)
        assert engine.submit(total).result(TIMEOUT) == 6
        assert engine.submit(loop_total).result(TIMEOUT) == 7
        assert total() == 6  # the stored result is the one for this revision

    _run_block(engine, body, commit=commit)
    if commit:
        assert (total(), loop_total()) == (116, 18)
        assert engine.submit(total).result(TIMEOUT) == 116
    else:
        assert (total(), loop_total()) == (6, 7)
        assert engine.submit(loop_total).result(TIMEOUT) == 7


def test_submit_running_when_the_block_commits_is_cancelled(
    engines: Callable[..., Engine],
) -> None:
    # G6: the commit changes inputs (the collection's head among them), so a
    # submitted call still running raises QueryCancelled and stores nothing.
    engine = engines()
    xs = CascadeList(engine, [1, 2, 3])
    started = threading.Event()
    gate = threading.Event()
    runs: list[int] = []

    @engine.query
    def total() -> int:
        runs.append(1)
        seen = sum(x for x in xs)
        started.set()
        assert gate.wait(TIMEOUT)
        return seen

    with engine.transaction():
        xs.append(10)
        future = engine.submit(total)
        assert started.wait(TIMEOUT)
    gate.set()
    with pytest.raises(QueryCancelled):
        future.result(TIMEOUT)
    assert total() == 16
    assert len(runs) == 2
    assert total() == 16
    assert len(runs) == 2


def test_cancelled_submit_stores_nothing_for_a_query_the_block_did_not_touch(
    engines: Callable[..., Engine],
) -> None:
    # Only the cancel epoch is compared (any input change cancels, as G6
    # words it), so a query over an untouched collection is cancelled too.
    # Nothing is stored: the direct call after it runs the body again.
    engine = engines()
    xs = CascadeList(engine, [1])
    ys = CascadeList(engine, [5, 6])
    started = threading.Event()
    gate = threading.Event()
    runs: list[int] = []

    @engine.query
    def other() -> int:
        runs.append(1)
        started.set()
        assert gate.wait(TIMEOUT)
        return sum(y for y in ys)

    with engine.transaction():
        xs.append(2)
        future = engine.submit(other)
        assert started.wait(TIMEOUT)
    gate.set()
    with pytest.raises(QueryCancelled):
        future.result(TIMEOUT)
    assert other() == 11
    assert len(runs) == 2


def test_submit_running_when_the_block_rolls_back_finishes_with_the_right_value(
    engines: Callable[..., Engine],
) -> None:
    # Decided: a rollback changes no input, so nothing is cancelled. The call
    # reads after the rollback, at the revision before the block, and what
    # it stores is the right value from then on (no second run).
    engine = engines()
    xs = CascadeList(engine, [1, 2, 3])
    started = threading.Event()
    gate = threading.Event()
    runs: list[int] = []

    @engine.query
    def total() -> int:
        runs.append(1)
        started.set()
        assert gate.wait(TIMEOUT)
        return sum(x for x in xs)

    with pytest.raises(Abort):
        with engine.transaction():
            xs.append(10)
            xs[0] = 100
            future = engine.submit(total)
            assert started.wait(TIMEOUT)
            raise Abort
    gate.set()
    assert future.result(TIMEOUT) == 6
    assert list(xs) == [1, 2, 3]
    assert total() == 6
    assert len(runs) == 1
    xs.append(4)
    assert total() == 10


def test_submit_reading_a_collection_mid_block_then_rolled_back(
    engines: Callable[..., Engine],
) -> None:
    # The submitted call reads the collection while the block's changes are
    # in its log, then the block rolls back and reuses those head numbers
    # for other contents. Nothing read in the block leaks into later reads.
    engine = engines()
    xs = CascadeList(engine, [1, 2, 3])
    read_done = threading.Event()

    @engine.query
    def total() -> int:
        value = sum(x for x in xs)
        read_done.set()
        return value

    @engine.query
    def evens() -> list[int]:
        return [x for x in xs if x % 2 == 0]

    with pytest.raises(Abort):
        with engine.transaction():
            xs.extend([4, 6])
            assert engine.submit(total).result(TIMEOUT) == 6
            assert engine.submit(evens).result(TIMEOUT) == [2]
            raise Abort
    assert read_done.is_set()
    xs.extend([7, 9])  # the same head numbers as the rolled-back changes
    assert list(xs) == [1, 2, 3, 7, 9]
    assert total() == 22
    assert evens() == [2]


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_submit_of_a_query_over_a_collection_created_in_the_block(
    engines: Callable[..., Engine], commit: bool
) -> None:
    engine = engines()
    made: list[Any] = []
    readers: list[Any] = []

    def body() -> None:
        xs = CascadeList(engine, [1, 2])
        made.append(xs)

        @engine.query
        def total() -> int:
            return sum(x for x in xs)

        readers.append(total)
        assert engine.submit(total).result(TIMEOUT) == 0

    _run_block(engine, body, commit=commit)
    total = readers[0]
    assert engine.submit(total).result(TIMEOUT) == (3 if commit else 0)
    made[0].append(10)
    assert total() == (13 if commit else 10)


def test_submit_with_a_snapshot_from_the_block_is_cancelled_by_the_commit(
    engines: Callable[..., Engine],
) -> None:
    # Decided: a submitted call pinned to a snapshot is cancelled by a commit
    # like any other (G6 cancels on any input change). The snapshot still
    # reads the state before the block afterwards.
    engine = engines()
    xs = CascadeList(engine, [1, 2])
    started = threading.Event()
    gate = threading.Event()

    @engine.query
    def total() -> int:
        started.set()
        assert gate.wait(TIMEOUT)
        return sum(x for x in xs)

    with engine.transaction():
        xs.append(10)
        snap = engine.snapshot()
        future = engine.submit(total, snapshot=snap)
        assert started.wait(TIMEOUT)
    gate.set()
    with snap:
        with pytest.raises(QueryCancelled):
            future.result(TIMEOUT)
        assert total(snapshot=snap) == 3
        assert total() == 13


# --- 4. two engines ----------------------------------------------------------


def _engine_with_state(make: Callable[..., Engine], **kwargs: Any) -> Any:
    engine = make(**kwargs)
    xs = CascadeList(engine, ["base"])

    @engine.input
    def flag() -> int:
        return 0

    @engine.query
    def view() -> Any:
        return [x for x in xs], flag()

    return engine, xs, flag, view


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_other_engine_changes_apply_at_once_during_a_block(
    engines: Callable[..., Engine], commit: bool
) -> None:
    # Decided: a transaction covers its own engine only. From the same
    # thread, inside A's block, engine B's input updates and collection
    # changes apply at once, its queries see them, A's rollback leaves them,
    # and they do not reserve B's collection.
    a, xa, flag_a, view_a = _engine_with_state(engines)
    b, xb, flag_b, view_b = _engine_with_state(engines)
    assert view_a() == view_b() == (["base"], 0)
    revision_b = b.revision

    def body() -> None:
        xa.append("a")
        flag_a.set(1)
        xb.append("b")
        flag_b.set(2)
        assert b.revision == revision_b + 2
        assert view_b() == (["base", "b"], 2)
        assert view_a() == (["base"], 0)
        # B's collection is not reserved: another thread may change it.
        other = threading.Thread(target=xb.append, args=("thread",))
        other.start()
        other.join(TIMEOUT)
        assert not other.is_alive()
        assert view_b() == (["base", "b", "thread"], 2)

    _run_block(a, body, commit=commit)
    assert view_a() == ((["base", "a"], 1) if commit else (["base"], 0))
    assert view_b() == (["base", "b", "thread"], 2)


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
def test_nested_transaction_on_another_engine_is_refused_and_changes_nothing(
    engines: Callable[..., Engine], commit: bool
) -> None:
    # Specified: a second transaction inside an open one raises, even for a
    # different engine. The outer block goes on as before, and B is free to
    # open its own transaction after it.
    a, xa, flag_a, view_a = _engine_with_state(engines)
    b, xb, flag_b, view_b = _engine_with_state(engines)

    def body() -> None:
        xa.append("a")
        inner = b.transaction()
        with pytest.raises(RuntimeError, match="Nested"):
            with inner:
                raise AssertionError("the inner block must not run")  # pragma: no cover
        flag_a.set(1)
        assert view_a() == (["base"], 0)
        assert view_b() == (["base"], 0)

    _run_block(a, body, commit=commit)
    assert view_a() == ((["base", "a"], 1) if commit else (["base"], 0))
    assert view_b() == (["base"], 0)
    with b.transaction():
        xb.append("b")
        flag_b.set(5)
        assert view_b() == (["base"], 0)
    assert view_b() == (["base", "b"], 5)


@pytest.mark.parametrize("a_commits", [True, False], ids=["a-commits", "a-rolls-back"])
def test_transactions_on_two_engines_in_two_tasks_of_one_thread(
    engines: Callable[..., Engine], a_commits: bool
) -> None:
    # Two blocks open at once on one thread, one per asyncio task: neither is
    # nested in the other (each task owns its own). Each block's queries see
    # its engine as before the block; B's rollback leaves A's block alone;
    # B's task may not change a collection A's block has changed.
    a, xa, flag_a, view_a = _engine_with_state(engines)
    b, xb, flag_b, view_b = _engine_with_state(engines)

    async def main() -> None:
        a_open = asyncio.Event()
        b_done = asyncio.Event()

        async def on_a() -> None:
            try:
                with a.transaction():
                    xa.append("a")
                    flag_a.set(1)
                    a_open.set()
                    await asyncio.wait_for(b_done.wait(), TIMEOUT)
                    assert list(xa) == ["base", "a"]
                    assert view_a() == (["base"], 0)
                    assert view_b() == (["base"], 0)  # B rolled back
                    xa.append("a2")
                    if not a_commits:
                        raise Abort
            except Abort:
                pass

        async def on_b() -> None:
            await asyncio.wait_for(a_open.wait(), TIMEOUT)
            with pytest.raises(Abort):
                with b.transaction():
                    xb.append("b")
                    flag_b.set(2)
                    with pytest.raises(RuntimeError, match="another thread or task"):
                        xa.append("from b")
                    assert view_b() == (["base"], 0)
                    assert view_a() == (["base"], 0)
                    raise Abort
            b_done.set()

        await asyncio.wait_for(asyncio.gather(on_a(), on_b()), TIMEOUT)

    asyncio.run(main())
    assert view_a() == ((["base", "a", "a2"], 1) if a_commits else (["base"], 0))
    assert view_b() == (["base"], 0)
    assert list(xb) == ["base"]


def test_transactions_on_two_engines_in_two_threads_at_once(
    engines: Callable[..., Engine],
) -> None:
    # A commits and B rolls back while both blocks are open: each outcome is
    # its own, and each block read its own engine as before it.
    a, xa, flag_a, view_a = _engine_with_state(engines)
    b, xb, flag_b, view_b = _engine_with_state(engines)
    both_open = threading.Barrier(2, timeout=TIMEOUT)
    b_ended = threading.Event()
    errors: list[BaseException] = []

    def on_a() -> None:
        try:
            with a.transaction():
                xa.append("a")
                flag_a.set(1)
                both_open.wait()
                assert b_ended.wait(TIMEOUT)
                assert view_a() == (["base"], 0)
                assert view_b() == (["base"], 0)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def on_b() -> None:
        try:
            with pytest.raises(Abort):
                with b.transaction():
                    xb.append("b")
                    flag_b.set(1)
                    both_open.wait()
                    assert view_b() == (["base"], 0)
                    raise Abort
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            b_ended.set()

    threads = [threading.Thread(target=f, daemon=True) for f in (on_a, on_b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(TIMEOUT)
    assert not any(t.is_alive() for t in threads)
    assert errors == []
    assert view_a() == (["base", "a"], 1)
    assert view_b() == (["base"], 0)


@pytest.mark.parametrize("commit", [True, False], ids=["commit", "rollback"])
@pytest.mark.parametrize("backend", BACKENDS)
def test_two_engines_on_one_cache_dir_with_a_named_collection_in_a_block(
    engines: Callable[..., Engine], tmp_path: Path, backend: str, commit: bool
) -> None:
    # G26 inside a transaction: A's first change in the block takes the
    # collection's lock, so B is refused (and changes nothing) inside and
    # after the block while A is alive, also after a rollback. B's own named
    # collection is not part of A's block: written at once, kept on rollback.
    a = engines(cache_dir=tmp_path, cache_backend=backend)
    b = engines(cache_dir=tmp_path, cache_backend=backend)
    xs_a = CascadeList(a, [1], name="xs")
    xs_b = CascadeList(b, name="xs")
    ys_b = CascadeList(b, ["y"], name="ys")
    assert list(xs_b) == [1]

    @a.query
    def total_a() -> int:
        return sum(x for x in xs_a)

    @b.query
    def total_b() -> int:
        return sum(x for x in xs_b)

    def body() -> None:
        xs_a.append(2)
        with pytest.raises(PersistentCacheError):
            xs_b.append(99)
        assert list(xs_b) == [1]
        ys_b.append("y2")
        assert total_a() == 1
        assert total_b() == 1

    _run_block(a, body, commit=commit)
    assert list(xs_a) == ([1, 2] if commit else [1])
    assert total_a() == (3 if commit else 1)
    with pytest.raises(PersistentCacheError):
        xs_b.append(99)
    assert list(xs_b) == [1]
    assert total_b() == 1
    a.shutdown()

    if commit:
        # B's copy is older than what A wrote: still refused, as out of date.
        with pytest.raises(PersistentCacheError, match="out of date"):
            xs_b.append(100)
        assert list(xs_b) == [1]
        expected = [1, 2]
    else:
        # Nothing reached disk, so B's copy is current and B may write now.
        xs_b.append(100)
        assert total_b() == 101
        expected = [1, 100]
    b.shutdown()

    c = engines(cache_dir=tmp_path, cache_backend=backend)
    assert list(CascadeList(c, name="xs")) == expected
    assert list(CascadeList(c, name="ys")) == ["y", "y2"]


@pytest.mark.parametrize("backend", BACKENDS)
def test_second_engine_block_refused_on_a_named_collection_rolls_back_whole(
    engines: Callable[..., Engine], tmp_path: Path, backend: str
) -> None:
    # Both engines in a block at once, on two threads, changing the same
    # named collection: the second is refused at its change, its block
    # raises and rolls back everything in it; the first commits.
    a = engines(cache_dir=tmp_path, cache_backend=backend)
    b = engines(cache_dir=tmp_path, cache_backend=backend)
    xs_a = CascadeList(a, ["base"], name="xs")
    xs_b = CascadeList(b, name="xs")
    own_b = CascadeList(b, name="own")

    @b.input
    def flag_b() -> int:
        return 0

    @b.query
    def view_b() -> Any:
        return list(xs_b), list(own_b), flag_b()

    a_changed = threading.Event()
    b_ended = threading.Event()
    errors: list[BaseException] = []

    def on_a() -> None:
        try:
            with a.transaction():
                xs_a.append("a")
                a_changed.set()
                assert b_ended.wait(TIMEOUT)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def on_b() -> None:
        try:
            assert a_changed.wait(TIMEOUT)
            with pytest.raises(PersistentCacheError):
                with b.transaction():
                    own_b.append("own")
                    flag_b.set(1)
                    xs_b.append("b")
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)
        finally:
            b_ended.set()

    threads = [threading.Thread(target=f, daemon=True) for f in (on_a, on_b)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(TIMEOUT)
    assert not any(t.is_alive() for t in threads)
    assert errors == []
    assert view_b() == (["base"], [], 0)
    assert list(xs_a) == ["base", "a"]
    a.shutdown()
    b.shutdown()

    c = engines(cache_dir=tmp_path, cache_backend=backend)
    assert list(CascadeList(c, name="xs")) == ["base", "a"]
    assert list(CascadeList(c, name="own")) == []
