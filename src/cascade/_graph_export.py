from __future__ import annotations

from typing import Any, Mapping
import re


def _dot_escape_label(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _mermaid_escape_label(s: str) -> str:
    return (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", " ")
        .replace("\r", " ")
    )


def _vertices_from_graph(graph: Mapping[str, Any]) -> list[str]:
    nodes = graph.get("nodes", ())
    edges = graph.get("edges", ())
    out: set[str] = set(nodes)
    for e in edges:
        if len(e) >= 2:
            out.add(e[0])
            out.add(e[1])
    return sorted(out)


def condense_graph(graph: Mapping[str, Any], min_group_size: int = 2) -> dict[str, Any]:
    """
    Condenses a graph dictionary by grouping nodes that share the same base name 
    (ignoring arguments) if there are at least `min_group_size` of them.
    """
    vertices = _vertices_from_graph(graph)
    
    base_name_groups = {}
    for v in vertices:
        match = re.match(r"^(.*?)\(.*\)$", v)
        if match:
            base_name = match.group(1)
            base_name_groups.setdefault(base_name, []).append(v)
        else:
            base_name_groups.setdefault(v, []).append(v)
            
    node_mapping = {}
    condensed_nodes = []
    
    for base_name, group in base_name_groups.items():
        if len(group) >= min_group_size and base_name != group[0]:
            condensed_name = f"{base_name} ({len(group)} nodes)"
            condensed_nodes.append(condensed_name)
            for v in group:
                node_mapping[v] = condensed_name
        else:
            for v in group:
                node_mapping[v] = v
                condensed_nodes.append(v)
                
    condensed_edges = []
    seen_edges = set()
    
    for e in graph.get("edges", ()):
        if len(e) < 2:
            continue
        p, d = e[0], e[1]
        
        new_p = node_mapping.get(p, p)
        new_d = node_mapping.get(d, d)
        
        edge_tuple = (new_p, new_d)
        if edge_tuple not in seen_edges:
            seen_edges.add(edge_tuple)
            condensed_edges.append(edge_tuple)
            
    return {
        "nodes": condensed_nodes,
        "edges": condensed_edges
    }


def export_dot(graph: Mapping[str, Any], *, directed: bool = True, condense: bool = False) -> str:
    """Render ``graph`` (``inspect_graph`` / :meth:`~cascade.engine.Engine.subgraph` shape) as Graphviz DOT.

    Node names are internal ids ``n0``, ``n1``, … with full keys in ``label`` attributes
    so keys may contain parentheses, quotes, and other characters safely.
    """
    if condense:
        graph = condense_graph(graph)
    vertices = _vertices_from_graph(graph)
    vid = {v: f"n{i}" for i, v in enumerate(vertices)}
    opener = "digraph G" if directed else "graph G"
    sep = " -> " if directed else " -- "
    lines = [f"{opener} {{", "  node [shape=box];"]
    for v in vertices:
        lines.append(f'  {vid[v]} [label="{_dot_escape_label(v)}"];')
    for e in graph.get("edges", ()):
        if len(e) < 2:
            continue
        p, d = e[0], e[1]
        lines.append(f"  {vid[p]}{sep}{vid[d]};")
    lines.append("}")
    return "\n".join(lines)


def export_mermaid(graph: Mapping[str, Any], *, condense: bool = False) -> str:
    """Render ``graph`` as a Mermaid ``flowchart TD`` (directed).

    Labels are embedded in ``["..."]`` node syntax. Characters that Mermaid treats
    specially inside labels (for example raw newlines) are replaced with spaces;
    double quotes and backslashes are escaped for the quoted label string.
    """
    if condense:
        graph = condense_graph(graph)
    vertices = _vertices_from_graph(graph)
    vid = {v: f"n{i}" for i, v in enumerate(vertices)}
    lines = ["flowchart TD"]
    for v in vertices:
        lines.append(f'    {vid[v]}["{_mermaid_escape_label(v)}"]')
    for e in graph.get("edges", ()):
        if len(e) < 2:
            continue
        p, d = e[0], e[1]
        lines.append(f"    {vid[p]} --> {vid[d]}")
    return "\n".join(lines)
