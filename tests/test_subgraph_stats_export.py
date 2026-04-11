from __future__ import annotations

import pytest

from cascade import Engine, export_dot, export_mermaid


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
