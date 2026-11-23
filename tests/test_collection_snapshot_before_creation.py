"""A snapshot from before a collection existed sees it as empty.

The collection's hidden head input has no version at such revisions, so its
default used to report the live head: the old snapshot showed whatever the
collection held at read time and followed later changes.
"""

from __future__ import annotations

from cascade import CascadeDict, CascadeList, CascadeSet, Engine


def test_snapshot_before_creation_sees_empty_collections() -> None:
    engine = Engine()

    @engine.input
    def factor() -> int:
        return 1

    factor.set(1)
    with engine.snapshot() as before:
        xs = CascadeList(engine, [10, 20])
        tags = CascadeSet(engine, {1, 2})
        meta = CascadeDict(engine, {"a": 1})

        @engine.query
        def total() -> int:
            return sum(v * factor() for v in xs)

        @engine.query
        def summary() -> tuple:
            return (
                sum(1 for _ in tags),
                sorted(meta.items()),
                sorted(v for v in tags),
                [v for v in xs if v > 0],
            )

        assert total() == 30
        assert summary() == (2, [("a", 1)], [1, 2], [10, 20])
        assert total(snapshot=before) == 0
        assert summary(snapshot=before) == (0, [], [], [])

        xs.append(30)
        tags.add(3)
        meta["b"] = 2
        assert total() == 60
        assert total(snapshot=before) == 0
        assert summary(snapshot=before) == (0, [], [], [])


def test_snapshot_after_creation_sees_the_contents_at_that_time() -> None:
    engine = Engine()
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def total() -> int:
        return sum(v for v in xs)

    with engine.snapshot() as after_creation:
        xs.append(3)
        assert total() == 6
        assert total(snapshot=after_creation) == 3
