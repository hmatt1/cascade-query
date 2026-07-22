from __future__ import annotations

import pytest

from cascade import CascadeDict, CascadeList, Engine, export_mermaid


@pytest.fixture
def engine():
    return Engine()


def test_pipeline_nodes_and_edge_chain_in_graph(engine):
    xs = CascadeList(engine, [1, 2, 3], name="orders")

    @engine.query
    def total():
        return sum(x * 2 for x in xs if x > 1)

    assert total() == 10
    graph = engine.inspect_graph()
    nodes = set(graph["nodes"])
    edges = set(graph["edges"])

    source = "collection:list:orders"
    pipes = engine.inspect_pipelines()
    assert len(pipes) == 1
    pipe = pipes[0]
    site = pipe["site"]
    filter_node = f"filter:{site}#0"
    map_node = f"map:{site}#1"
    reduce_node = f"reduce:sum:{site}"

    assert {source, filter_node, map_node, reduce_node} <= nodes
    assert (filter_node, source) in edges
    assert (map_node, filter_node) in edges
    assert (reduce_node, map_node) in edges

    consumers = pipe["consumers"]
    assert len(consumers) == 1
    consumer = consumers[0]
    assert consumer in nodes
    assert (consumer, reduce_node) in edges


def test_dict_view_source_labeled_with_projection(engine):
    d = CascadeDict(engine, {"a": 1}, name="cfg")

    @engine.query
    def vals():
        return sum(v for v in d.values())

    assert vals() == 1
    sources = {p["source"] for p in engine.inspect_pipelines()}
    assert sources == {"collection:dict:cfg.values"}
    assert "collection:dict:cfg.values" in engine.inspect_graph()["nodes"]


def test_mermaid_export_includes_pipeline_nodes(engine):
    xs = CascadeList(engine, [1], name="xs")

    @engine.query
    def q():
        return sum(x for x in xs)

    q()
    mermaid = export_mermaid(engine.inspect_graph())
    assert "collection:list:xs" in mermaid
    assert "reduce:sum:" in mermaid
    assert "-->" in mermaid


def test_graph_without_pipelines_matches_store_output(engine):
    @engine.query
    def plain():
        return 42

    plain()
    graph = engine.inspect_graph()
    store_graph = engine._store.inspect_graph()
    assert graph["nodes"] == store_graph["nodes"]
    assert graph["edges"] == store_graph["edges"]


def test_repeated_runs_do_not_duplicate_nodes_or_edges(engine):
    xs = CascadeList(engine, [1])

    @engine.query
    def q():
        return sum(x for x in xs)

    q()
    xs.append(2)
    q()
    graph = engine.inspect_graph()
    assert len(graph["nodes"]) == len(set(graph["nodes"]))
    assert len(graph["edges"]) == len(set(graph["edges"]))


def test_two_consumers_share_one_pipeline_per_site(engine):
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def shared(tag):
        return sum(x for x in xs)

    assert shared("a") == shared("b") == 3
    pipes = engine.inspect_pipelines()
    assert len(pipes) == 1
    assert len(pipes[0]["consumers"]) == 2


def test_inspect_pipelines_shape_and_ordering(engine):
    xs = CascadeList(engine, [3, 1], name="nums")

    @engine.query
    def a():
        return sorted(x for x in xs)

    @engine.query
    def b():
        return len(xs)

    a(), b()
    pipes = engine.inspect_pipelines()
    assert [set(p) for p in pipes] == [
        {
            "site",
            "source",
            "reducer",
            "stages",
            "fused_stage_count",
            "last_rev",
            "consumers",
        }
    ] * 2
    assert pipes == sorted(pipes, key=lambda p: (p["site"], p["source"], p["reducer"]))
    reducers = {p["reducer"] for p in pipes}
    assert reducers == {"sorted", "len"}
    assert all(p["last_rev"] == 2 for p in pipes)


def test_fused_stage_count_zero_for_bare_reduction(engine):
    xs = CascadeList(engine, [1, 2])

    @engine.query
    def q():
        return len(xs)

    q()
    pipe = engine.inspect_pipelines()[0]
    assert pipe["stages"] == []
    assert pipe["fused_stage_count"] == 0
