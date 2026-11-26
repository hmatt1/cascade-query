"""Results Cascade cannot encode, on an engine without ``cache_dir`` (G1, G2, G8).

Nothing has to be written or decoded there, so such a result (a syntax tree,
an object of the program's own class) is cached as it is. It cannot be
compared by content, so it never counts as equal to an earlier result: a
query that returns one and runs again always re-runs its dependents. That
loses the early bail-out, never correctness. Input values and fixed-point
guesses must still be encodable (``test_unencodable_values.py``).
"""

from __future__ import annotations

import ast
import asyncio
from typing import Any

import pytest

from cascade import Engine, UnencodableValueError


class Node:
    def __init__(self, label: str) -> None:
        self.label = label


def test_unencodable_result_is_cached_as_the_same_object() -> None:
    engine = Engine()
    runs = {"n": 0}

    @engine.query
    def parse(source: str) -> ast.AST:
        runs["n"] += 1
        return ast.parse(source)

    tree = parse("x = 1")
    assert isinstance(tree, ast.Module)
    assert parse("x = 1") is tree
    assert runs["n"] == 1
    assert parse("y = 2") is not tree
    assert runs["n"] == 2


def test_dependents_rerun_when_an_unencodable_result_is_recomputed() -> None:
    engine = Engine()
    runs = {"node": 0, "label": 0, "top": 0}

    @engine.input
    def source() -> str:
        return "a"

    @engine.query
    def node() -> Node:
        runs["node"] += 1
        return Node(source().upper())

    @engine.query
    def label() -> str:
        runs["label"] += 1
        return node().label

    @engine.query
    def top() -> str:
        runs["top"] += 1
        return label() + "!"

    assert top() == "A!"
    source.set("b")
    assert top() == "B!"
    # node() cannot be compared, so label() ran again; label()'s own result
    # is a str and changed, so top() ran again too.
    assert runs == {"node": 2, "label": 2, "top": 2}
    source.set("B")  # node() is "B" again, but cannot be shown to be equal
    assert top() == "B!"
    assert runs == {"node": 3, "label": 3, "top": 2}  # label() bails out


def test_exception_with_unencodable_args_is_cached_in_memory() -> None:
    engine = Engine()
    runs = {"n": 0}
    raised: list[BaseException] = []

    @engine.query
    def boom() -> Any:
        runs["n"] += 1
        exc = KeyError(Node("k"), "context")
        raised.append(exc)
        raise exc

    for _ in range(3):
        with pytest.raises(KeyError) as info:
            boom()
        assert info.value is raised[0]
    assert runs["n"] == 1


def test_unencodable_results_in_memoize_false_async_snapshot_and_compute_many() -> None:
    engine = Engine()

    @engine.input
    def n() -> int:
        return 1

    @engine.query(memoize=False)
    def passthrough() -> Node:
        return Node(str(n()))

    @engine.query
    async def remote() -> Node:
        await asyncio.sleep(0)
        return Node("r" + str(n()))

    @engine.query
    def item(i: int) -> Node:
        return Node(f"{i}:{n()}")

    @engine.query
    def total() -> str:
        return passthrough().label

    assert total() == "1"
    with engine.snapshot() as before:
        n.set(2)
        assert total() == "2"
        assert total(snapshot=before) == "1"

    async def main() -> Node:
        return await remote()

    assert asyncio.run(main()).label == "r2"
    nodes = engine.compute_many([(item, (i,)) for i in range(3)])
    assert [x.label for x in nodes] == ["0:2", "1:2", "2:2"]


def test_fixed_point_and_input_values_still_need_an_encoding() -> None:
    engine = Engine()

    @engine.input
    def setting() -> Any:
        return 0

    with pytest.raises(UnencodableValueError, match=setting.id):
        setting.set(Node("x"))

    @engine.query(fixed_point=0)
    def head() -> Any:
        head()
        return Node("never equal to the guess")

    with pytest.raises(UnencodableValueError, match=head.id):
        head()


def test_with_cache_dir_an_unencodable_result_is_an_error(tmp_path: Any) -> None:
    engine = Engine(cache_dir=tmp_path, cache_backend="sqlite")
    try:

        @engine.query
        def make() -> Any:
            return Node("x")

        with pytest.raises(UnencodableValueError, match=make.id):
            make()
    finally:
        engine.shutdown()


@pytest.mark.parametrize(
    "make",
    [
        lambda: (x for x in range(3)),
        lambda: map(str, [1, 2]),
        lambda: zip("ab", "cd"),
        lambda: iter([1, 2]),
    ],
    ids=["generator", "map", "zip", "iter"],
)
def test_one_shot_iterator_result_is_refused(make: Any) -> None:
    """A cached iterator would be used up by its first caller."""
    engine = Engine()
    runs = {"n": 0}

    @engine.query
    def items() -> Any:
        runs["n"] += 1
        return make()

    with pytest.raises(UnencodableValueError, match="can be read only once") as info:
        items()
    assert "items" in str(info.value)
    # It names what was returned: "returned a generator", "a map", ...
    assert f"returned a {type(make()).__name__}," in str(info.value)
    # Not cached: the next call runs the body and raises again.
    with pytest.raises(UnencodableValueError):
        items()
    assert runs["n"] == 2


def test_caller_that_catches_a_refused_iterator_still_tracks_its_inputs() -> None:
    engine = Engine()

    @engine.input
    def n() -> int:
        return 2

    @engine.query
    def items() -> Any:
        return (x for x in range(n())) if n() < 5 else list(range(n()))

    @engine.query
    def safe() -> Any:
        try:
            return items()
        except UnencodableValueError:
            return "refused"

    assert safe() == "refused"
    n.set(6)
    assert safe() == [0, 1, 2, 3, 4, 5]


def test_one_shot_iterator_from_an_async_query_is_refused() -> None:
    engine = Engine()

    @engine.query
    async def items() -> Any:
        return (x for x in range(3))

    async def main() -> Any:
        return await items()

    with pytest.raises(UnencodableValueError, match="can be read only once"):
        asyncio.run(main())


def test_values_that_can_be_read_again_and_unmemoized_iterators_are_fine() -> None:
    import warnings

    engine = Engine()

    @engine.query
    def tree() -> ast.AST:
        return ast.parse("x = 1")

    @engine.query
    def numbers() -> list[int]:
        return [1, 2, 3]

    @engine.query(memoize=False)
    def stream() -> Any:
        return (x for x in range(3))

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        tree()
        numbers()
        assert list(stream()) == [0, 1, 2]
        assert list(stream()) == [0, 1, 2]
