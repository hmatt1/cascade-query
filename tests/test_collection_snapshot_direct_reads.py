"""Direct collection reads inside a query honour the query's revision.

Only reducer calls the AST rewriter recognises used to read a collection as
of the snapshot. Everything else (``xs[0]``, ``len(xs)``, a plain ``for``
loop, and every read in a query whose source could not be rewritten) recorded
the dependency but returned the live contents.
"""

from __future__ import annotations

import pytest

from cascade import CascadeDict, CascadeList, CascadeSet, Engine


def test_list_reads_use_the_snapshot() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])

    @engine.query
    def view() -> tuple:
        looped = []
        for value in xs:
            looped.append(value)
        return (
            xs[0],
            xs[-1],
            xs[1:],
            len(xs),
            looped,
            list(reversed(xs)),
            2 in xs,
            99 in xs,
            xs == [1, 2, 3],
            xs != [1, 2, 3],
            xs.index(3),
            xs.count(1),
            xs.copy(),
        )

    with engine.snapshot() as snap:
        before = view(snapshot=snap)
        xs[0] = 99
        xs.append(1)
        del xs[1]
        assert list(xs) == [99, 3, 1]

        assert view(snapshot=snap) == before
        assert before == (
            1,
            3,
            [2, 3],
            3,
            [1, 2, 3],
            [3, 2, 1],
            True,
            False,
            True,
            False,
            2,
            1,
            [1, 2, 3],
        )
    assert view() == (
        99,
        1,
        [3, 1],
        3,
        [99, 3, 1],
        [1, 3, 99],
        False,
        True,
        False,
        True,
        1,
        1,
        [99, 3, 1],
    )


def test_set_reads_use_the_snapshot() -> None:
    engine = Engine()
    tags = CascadeSet(engine, {"a", "b"})

    @engine.query
    def view() -> tuple:
        members = []
        for tag in tags:
            members.append(tag)
        return len(tags), sorted(members), "a" in tags, "c" in tags

    with engine.snapshot() as snap:
        tags.discard("a")
        tags.add("c")
        tags.add("d")
        assert view(snapshot=snap) == (2, ["a", "b"], True, False)
    assert view() == (3, ["b", "c", "d"], False, True)


def test_dict_reads_use_the_snapshot() -> None:
    engine = Engine()
    meta = CascadeDict(engine, {"a": 1, "b": 2})

    @engine.query
    def view() -> tuple:
        keys = []
        for key in meta:
            keys.append(key)
        pairs = []
        for key, value in meta.items():
            pairs.append((key, value))
        return (
            meta["a"],
            meta.get("b"),
            meta.get("c", "missing"),
            len(meta),
            keys,
            pairs,
            "b" in meta,
            len(meta.values()),
            2 in meta.values(),
            ("a", 1) in meta.items(),
            list(reversed(meta)),
            list(meta.keys()),
        )

    with engine.snapshot() as snap:
        meta["a"] = 10
        del meta["b"]
        meta["c"] = 3
        assert view(snapshot=snap) == (
            1,
            2,
            "missing",
            2,
            ["a", "b"],
            [("a", 1), ("b", 2)],
            True,
            2,
            True,
            True,
            ["b", "a"],
            ["a", "b"],
        )

        @engine.query
        def missing() -> int:
            return meta["c"]

        with pytest.raises(KeyError):
            missing(snapshot=snap)
        assert missing() == 3


def test_snapshot_before_creation_reads_empty() -> None:
    engine = Engine()

    @engine.input
    def tick() -> int:
        return 0

    tick.set(1)
    with engine.snapshot() as before:
        xs = CascadeList(engine, [1])
        tags = CascadeSet(engine, {1})
        meta = CascadeDict(engine, {"a": 1})

        @engine.query
        def sizes() -> tuple:
            return len(xs), len(tags), len(meta), meta.get("a"), 1 in tags

        assert sizes() == (1, 1, 1, 1, True)
        assert sizes(snapshot=before) == (0, 0, 0, None, False)


def test_query_without_readable_source_uses_the_snapshot() -> None:
    """exec/REPL/stdin queries are not rewritten; their reads go direct."""
    engine = Engine()
    xs = CascadeList(engine, [1, 2, 3])
    meta = CascadeDict(engine, {"a": 1})
    namespace = {"engine": engine, "xs": xs, "meta": meta}
    exec(
        "@engine.query\n"
        "def total():\n"
        "    return sum(v for v in xs), sorted(meta.items()), len(xs)\n",
        namespace,
    )
    total = namespace["total"]

    with engine.snapshot() as snap:
        assert total() == (6, [("a", 1)], 3)
        xs.append(4)
        meta["b"] = 2
        assert total() == (10, [("a", 1), ("b", 2)], 4)
        assert total(snapshot=snap) == (6, [("a", 1)], 3)


def test_snapshot_read_tracks_later_snapshots_separately() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1])

    @engine.query
    def first() -> int:
        return xs[0]

    with engine.snapshot() as one:
        xs[0] = 2
        with engine.snapshot() as two:
            xs[0] = 3
            assert first(snapshot=one) == 1
            assert first(snapshot=two) == 2
            assert first() == 3
            assert first(snapshot=one) == 1


def test_mutations_inside_a_query_still_apply_in_order() -> None:
    """Mutators read live state even though the query's revision is behind."""
    engine = Engine()
    xs = CascadeList(engine, [1, 2])
    meta = CascadeDict(engine, {"a": 1})
    tags = CascadeSet(engine, {1})

    @engine.query
    def mutate() -> int:
        xs.append(3)
        xs.append(4)
        xs.insert(1, 9)
        xs.pop()
        del xs[0]
        meta.setdefault("b", 2)
        meta.setdefault("b", 3)
        meta.pop("a")
        tags.add(2)
        tags.discard(1)
        return 1

    mutate()
    assert list(xs) == [9, 2, 3]
    assert dict(meta) == {"b": 2}
    assert set(tags) == {2}
    # The diff logs agree with the live contents.
    assert [v for _, v in xs._core.log.contents_at(xs._core.log.head)] == [9, 2, 3]
    assert meta._core.log.contents_at(meta._core.log.head) == {"b": 2}
    assert tags._core.log.contents_at(tags._core.log.head) == [2]


def test_reads_outside_queries_are_live() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1])
    with engine.snapshot():
        xs.append(2)
        assert list(xs) == [1, 2]
        assert len(xs) == 2
        assert xs[1] == 2
