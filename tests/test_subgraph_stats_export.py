from __future__ import annotations

import pytest

from cascade import Engine, condense_graph, export_dot, export_mermaid


def test_subgraph_deps_linear_chain_matches_inspect_graph() -> None:
    engine = Engine()

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        return tuple(row.strip() for row in source(name).splitlines() if row.strip())

    @engine.query
    def symbol_count(name: str) -> int:
        return len(parse(name))

    source.set("main", "a\nb")
    assert symbol_count("main") == 2
    full = engine.inspect_graph()
    top_key = next(k for k in full["nodes"] if "symbol_count" in k)
    mid_key = next(k for k in full["nodes"] if "parse" in k)

    sub = engine.subgraph([top_key], direction="deps")
    assert set(sub["nodes"]) == {top_key, mid_key}
    assert set(sub["edges"]) == set(full["edges"])

    sub_mid = engine.subgraph([mid_key], direction="deps")
    assert set(sub_mid["nodes"]) == {mid_key}
    assert len(sub_mid["edges"]) == 1


def test_subgraph_dependents_walks_upward() -> None:
    engine = Engine()

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def parse(name: str) -> tuple[str, ...]:
        return tuple(row.strip() for row in source(name).splitlines() if row.strip())

    @engine.query
    def symbol_count(name: str) -> int:
        return len(parse(name))

    source.set("main", "x")
    assert symbol_count("main") == 1
    full = engine.inspect_graph()
    top_key = next(k for k in full["nodes"] if "symbol_count" in k)
    mid_key = next(k for k in full["nodes"] if "parse" in k)

    sub = engine.subgraph([mid_key], direction="dependents")
    assert set(sub["nodes"]) == {mid_key, top_key}
    assert set(sub["edges"]) == {(top_key, mid_key)}


def test_subgraph_accepts_query_key_roots() -> None:
    engine = Engine()

    @engine.input
    def i() -> int:
        return 0

    @engine.query
    def q() -> int:
        return i()

    i.set(1)
    assert q() == 1
    key: tuple[str, str, tuple[int, ...]] = ("query", q.id, ())
    sub = engine.subgraph([key], direction="deps")
    assert set(sub["nodes"]) == set(engine.inspect_graph()["nodes"])


def test_subgraph_empty_roots_and_unknown_roots() -> None:
    engine = Engine()

    @engine.input
    def i() -> int:
        return 0

    @engine.query
    def q() -> int:
        return i()

    i.set(1)
    assert q() == 1
    empty = engine.subgraph([], direction="deps")
    assert empty["nodes"] == []
    assert empty["edges"] == []
    assert empty["memo_count"] == 0

    g = engine.inspect_graph()
    real = next(iter(g["nodes"]))
    ignored = engine.subgraph(["no.such:node", real], direction="deps")
    same = engine.subgraph([real], direction="deps")
    assert ignored == same


def test_subgraph_invalid_direction() -> None:
    engine = Engine()
    with pytest.raises(ValueError, match="direction"):
        engine.subgraph([], direction="both")  # type: ignore[arg-type]


def test_stats_by_key_uses_injected_clock() -> None:
    seq = iter([0.0, 5.0, 5.0, 5.5])

    def clock() -> float:
        return next(seq)

    engine = Engine(stats=True, stats_clock=clock)

    @engine.input
    def i() -> int:
        return 0

    @engine.query
    def slow() -> int:
        return i() + 1

    @engine.query
    def fast() -> int:
        return i() + 2

    i.set(1)
    slow()
    fast()
    summary = engine.stats_summary()
    by = summary["by_key"]
    slow_k = next(k for k in by if "slow" in k)
    fast_k = next(k for k in by if "fast" in k)
    assert by[slow_k] == 5.0
    assert by[fast_k] == 0.5


def test_stats_eviction_increments_and_memo_stays_bounded() -> None:
    engine = Engine(max_entries=2, stats=True, stats_eviction_recent_cap=10)

    @engine.input
    def s() -> int:
        return 0

    @engine.query
    def a() -> int:
        return s()

    @engine.query
    def b() -> int:
        return s() + 1

    @engine.query
    def c() -> int:
        return s() + 2

    s.set(1)
    a()
    assert engine.stats_summary()["evictions_total"] == 0
    assert engine.inspect_graph()["memo_count"] == 1
    b()
    assert engine.inspect_graph()["memo_count"] == 2
    c()
    summary = engine.stats_summary()
    assert summary["evictions_total"] >= 1
    assert summary["memo_count"] == 2
    assert len(summary["evictions_recent"]) >= 1


def test_reset_stats_clears_counters() -> None:
    engine = Engine(stats=True)

    @engine.input
    def i() -> int:
        return 0

    @engine.query
    def q() -> int:
        return i()

    i.set(1)
    q()
    assert engine.stats_summary()["by_key"]
    engine.reset_stats()
    assert engine.stats_summary()["by_key"] == {}


def test_export_dot_and_mermaid_round_trip_edges() -> None:
    graph = {
        "revision": 1,
        "memo_count": 2,
        "input_count": 0,
        "nodes": ['query:mod:a("x", 1)', "input:mod:i('y')"],
        "edges": [
            ('query:mod:a("x", 1)', "input:mod:i('y')"),
            ('query:mod:a("x", 1)', 'query:mod:a("x", 1)'),
        ],
    }
    dot = export_dot(graph)
    assert 'label="query:mod:a(\\"x\\", 1)"' in dot
    assert "n1 -> n0" in dot
    assert "n1 -> n1" in dot
    mmd = export_mermaid(graph)
    assert "flowchart TD" in mmd
    assert "-->" in mmd
    assert mmd.count("-->") == 2


def test_export_mermaid_newlines_sanitized() -> None:
    graph = {
        "nodes": ["a\nb"],
        "edges": [],
    }
    mmd = export_mermaid(graph)
    assert '["a b"]' in mmd


def test_enable_stats_runtime_toggle() -> None:
    engine = Engine(stats=False)

    @engine.input
    def i() -> int:
        return 0

    @engine.query
    def q() -> int:
        return i()

    i.set(1)
    q()
    assert engine.stats_summary()["by_key"] == {}
    engine.enable_stats(True)
    i.set(2)
    q()
    assert engine.stats_summary()["by_key"]


# --- condensed graphs, and graphs that are not well formed ---


def _fan_out(engine: Engine) -> None:
    """``total`` over three calls of ``leaf``, each reading one input."""

    @engine.input
    def base() -> int:
        return 1

    @engine.query
    def leaf(i: int) -> int:
        return base() + i

    @engine.query
    def total() -> int:
        return leaf(0) + leaf(1) + leaf(2)

    assert total() == 6


def test_condense_groups_the_calls_of_one_query() -> None:
    engine = Engine()
    _fan_out(engine)
    full = engine.inspect_graph()
    top = next(n for n in full["nodes"] if "total" in n)
    assert len([n for n in full["nodes"] if "leaf(" in n]) == 3

    for graph in (
        engine.inspect_graph(condense=True),
        engine.subgraph([top], condense=True),
    ):
        group = next(n for n in graph["nodes"] if n.endswith("leaf (3 nodes)"))
        assert not [n for n in graph["nodes"] if "leaf(" in n]
        # Three edges into the group and three out of it are one each now.
        assert len(graph["edges"]) == 2
        assert (top, group) in graph["edges"]
        assert [d for p, d in graph["edges"] if p == group][0].endswith("base()")


def test_export_condenses_on_request() -> None:
    engine = Engine()
    _fan_out(engine)
    graph = engine.inspect_graph()

    assert export_dot(graph).count("leaf(") == 3
    dot = export_dot(graph, condense=True)
    assert dot.count("leaf (3 nodes)") == 1
    assert "leaf(" not in dot
    assert dot.count("->") == 2

    assert export_mermaid(graph).count("leaf(") == 3
    mermaid = export_mermaid(graph, condense=True)
    assert mermaid.count("leaf (3 nodes)") == 1
    assert "leaf(" not in mermaid
    assert mermaid.count("-->") == 2


def test_condense_leaves_single_calls_and_bare_names_alone() -> None:
    graph = {
        "nodes": ["bare", "f(1)", "f(2)", "g(1)"],
        # The last edge has one end only and is skipped.
        "edges": [("bare", "f(1)"), ("bare", "f(2)"), ("g(1)", "bare"), ("bare",)],
    }
    assert condense_graph(graph) == {
        "nodes": ["bare", "f (2 nodes)", "g(1)"],
        "edges": [("bare", "f (2 nodes)"), ("g(1)", "bare")],
    }
    # A group has to reach the size asked for.
    assert condense_graph(graph, min_group_size=3)["nodes"] == graph["nodes"]


def test_export_skips_edges_without_two_ends() -> None:
    graph = {"nodes": ["a"], "edges": [("a",), ("a", "b")]}
    dot = export_dot(graph)
    assert dot.count("->") == 1
    assert 'label="b"' in dot  # an end that is no node of its own still shows
    mermaid = export_mermaid(graph)
    assert mermaid.count("-->") == 1
    assert '["b"]' in mermaid


def test_subgraph_follows_a_shared_dependency_once() -> None:
    engine = Engine()

    @engine.query
    def shared() -> int:
        return 1

    @engine.query
    def left() -> int:
        return shared() + 1

    @engine.query
    def right() -> int:
        return shared() + 2

    @engine.query
    def top() -> int:
        return left() + right()

    assert top() == 5
    nodes = engine.inspect_graph()["nodes"]
    name = {
        short: next(n for n in nodes if f".{short}()" in n)
        for short in ("shared", "left", "right", "top")
    }
    diamond = {
        (name["top"], name["left"]),
        (name["top"], name["right"]),
        (name["left"], name["shared"]),
        (name["right"], name["shared"]),
    }
    down = engine.subgraph([name["top"]])
    assert sorted(down["nodes"]) == sorted(nodes)
    assert len(down["edges"]) == 4 and set(down["edges"]) == diamond
    up = engine.subgraph([name["shared"]], direction="dependents")
    assert sorted(up["nodes"]) == sorted(nodes)
    assert len(up["edges"]) == 4 and set(up["edges"]) == diamond
    # From one side only the other side is left out.
    side = engine.subgraph([name["left"]])
    assert sorted(side["nodes"]) == sorted([name["left"], name["shared"]])


def _chain(engine: Engine) -> dict[str, str]:
    """``top`` reads ``leaf`` and then ``middle``, which reads ``leaf`` too."""

    @engine.query
    def leaf() -> int:
        return 1

    @engine.query
    def middle() -> int:
        return leaf() + 1

    @engine.query
    def top() -> int:
        return leaf() + middle()

    assert top() == 3
    nodes = engine.inspect_graph()["nodes"]
    return {
        short: next(n for n in nodes if f".{short}()" in n)
        for short in ("leaf", "middle", "top")
    }


@pytest.mark.parametrize("direction", ["deps", "dependents"])
def test_subgraph_lists_a_root_that_another_root_reaches_once(direction: str) -> None:
    engine = Engine()
    name = _chain(engine)
    every_edge = {
        (name["top"], name["leaf"]),
        (name["top"], name["middle"]),
        (name["middle"], name["leaf"]),
    }
    # Whichever way the walk goes, it meets one of the roots again.
    for roots in ([name["leaf"], name["top"]], [name["top"], name["leaf"]]):
        sub = engine.subgraph(roots, direction=direction)  # type: ignore[arg-type]
        assert sorted(sub["nodes"]) == sorted(name.values())
        assert len(sub["edges"]) == 3 and set(sub["edges"]) == every_edge
        assert sub["memo_count"] == 3


def test_subgraph_ignores_a_key_root_that_has_no_result() -> None:
    engine = Engine()

    @engine.query
    def square(x: int) -> int:
        return x * x

    assert square(3) == 9
    known = ("query", square.id, (3,))
    never_called = ("query", square.id, (4,))
    assert engine.subgraph([never_called]) == engine.subgraph([])
    assert engine.subgraph([never_called, known]) == engine.subgraph([known])
    assert len(engine.subgraph([known])["nodes"]) == 1


def test_evictions_are_counted_but_not_listed_when_the_recent_cap_is_zero() -> None:
    engine = Engine(max_entries=1, stats=True, stats_eviction_recent_cap=0)

    @engine.query
    def value(x: int) -> int:
        return x

    for x in range(4):
        value(x)
    summary = engine.stats_summary()
    assert summary["evictions_total"] == 3
    assert summary["evictions_recent"] == []
    assert summary["memo_count"] == 1
