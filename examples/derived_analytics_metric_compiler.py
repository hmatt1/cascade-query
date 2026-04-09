from __future__ import annotations

from dataclasses import dataclass

from cascade import Engine


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    expression: str


def _extract_metric_refs(expression: str) -> tuple[str, ...]:
    refs: list[str] = []
    for token in expression.replace("+", " ").replace("-", " ").replace("/", " ").split():
        if token.startswith("m:"):
            refs.append(token.removeprefix("m:"))
    return tuple(refs)


def run_demo() -> None:
    print("=== Derived analytics metric compiler example ===")
    print("Incrementally compile metric DAGs and dependency expansions.")

    engine = Engine()
    calls = {"compile_metric": 0, "expand_dependencies": 0}

    @engine.input
    def metric_def(name: str) -> MetricDefinition:
        return MetricDefinition(name=name, expression="0")

    @engine.query
    def compile_metric(name: str) -> str:
        calls["compile_metric"] += 1
        definition = metric_def(name)
        refs = _extract_metric_refs(definition.expression)
        if not refs:
            return f"{definition.name} := {definition.expression}"
        compiled_refs = ", ".join(expand_dependencies(ref) for ref in refs)
        return f"{definition.name} := eval({definition.expression}) deps=[{compiled_refs}]"

    @engine.query
    def expand_dependencies(name: str) -> str:
        calls["expand_dependencies"] += 1
        definition = metric_def(name)
        refs = _extract_metric_refs(definition.expression)
        if not refs:
            return name
        return f"{name}({','.join(expand_dependencies(ref) for ref in refs)})"

    print("Step 1: Seed a small metric graph.")
    metric_def.set("revenue", MetricDefinition("revenue", "orders_total"))
    metric_def.set("orders", MetricDefinition("orders", "orders_count"))
    metric_def.set("aov", MetricDefinition("aov", "m:revenue / m:orders"))
    metric_def.set("north_star", MetricDefinition("north_star", "m:aov + m:orders"))
    print("Compiled north_star:", compile_metric("north_star"))
    print("Counters:", calls)

    print("Step 2: Re-run north_star without changes (cache hit).")
    print("Compiled north_star:", compile_metric("north_star"))
    print("Counters:", calls)

    print("Step 3: Change orders metric only.")
    metric_def.set("orders", MetricDefinition("orders", "orders_count * 2"))
    print("Compiled north_star:", compile_metric("north_star"))
    print("Compiled revenue (should stay cached):", compile_metric("revenue"))
    print("Counters:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
