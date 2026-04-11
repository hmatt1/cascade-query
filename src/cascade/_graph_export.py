from __future__ import annotations

from typing import Mapping


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


def export_dot(graph: Mapping[str, Any], *, directed: bool = True) -> str:
    """Render ``graph`` (``inspect_graph`` / :meth:`~cascade.engine.Engine.subgraph` shape) as Graphviz DOT.

    Node names are internal ids ``n0``, ``n1``, … with full keys in ``label`` attributes
    so keys may contain parentheses, quotes, and other characters safely.
    """
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


def export_mermaid(graph: Mapping[str, Any]) -> str:
    """Render ``graph`` as a Mermaid ``flowchart TD`` (directed).

    Labels are embedded in ``["..."]`` node syntax. Characters that Mermaid treats
    specially inside labels (for example raw newlines) are replaced with spaces;
    double quotes and backslashes are escaped for the quoted label string.
    """
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
