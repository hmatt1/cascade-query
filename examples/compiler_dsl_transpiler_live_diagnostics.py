from __future__ import annotations

from dataclasses import dataclass

from cascade import Engine


@dataclass(frozen=True)
class Program:
    assignments: tuple[tuple[str, str], ...]


def run_demo() -> None:
    print("=== Compiler/DSL transpiler with live diagnostics example ===")
    print("Stages: parse -> lower -> typecheck, with warning replay on cache hits.")

    engine = Engine()
    warnings = engine.accumulator("warnings")
    calls = {"parse": 0, "lower": 0, "typecheck": 0}

    @engine.input
    def source(module: str) -> str:
        return ""

    @engine.query
    def parse(module: str) -> Program:
        calls["parse"] += 1
        rows: list[tuple[str, str]] = []
        for line in source(module).splitlines():
            text = line.strip()
            if not text:
                continue
            lhs, rhs = text.split("=", 1)
            rows.append((lhs.strip(), rhs.strip()))
        return Program(assignments=tuple(rows))

    @engine.query
    def lower(module: str) -> tuple[tuple[str, str], ...]:
        calls["lower"] += 1
        return tuple((name, expr.upper()) for name, expr in parse(module).assignments)

    @engine.query
    def typecheck(module: str) -> tuple[str, ...]:
        calls["typecheck"] += 1
        names: set[str] = set()
        lowered = lower(module)
        for name, expr in lowered:
            if name in names:
                warnings.push(f"{module}: duplicate binding `{name}`")
            names.add(name)
            if "TODO" in expr:
                warnings.push(f"{module}: unresolved expression in `{name}`")
        return tuple(name for name, _ in lowered)

    print("Step 1: First compile with one unresolved expression.")
    source.set("math.dsl", "sum = a + b\ntemp = todo()")
    effects_first: dict[str, list[str]] = {}
    print("Typechecked names:", typecheck("math.dsl", effects=effects_first))
    print("Warnings:", effects_first.get("warnings", []))
    print("Counters:", calls)

    print("Step 2: Re-run with no changes (warnings replayed from cache).")
    effects_second: dict[str, list[str]] = {}
    print("Typechecked names:", typecheck("math.dsl", effects=effects_second))
    print("Warnings:", effects_second.get("warnings", []))
    print("Counters:", calls)

    print("Step 3: Fix source and compile again.")
    source.set("math.dsl", "sum = a + b\ntemp = c + d")
    effects_third: dict[str, list[str]] = {}
    print("Typechecked names:", typecheck("math.dsl", effects=effects_third))
    print("Warnings:", effects_third.get("warnings", []))
    print("Counters:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
