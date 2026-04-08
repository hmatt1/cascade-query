from __future__ import annotations

from dataclasses import dataclass

from cascade import Engine


@dataclass(frozen=True)
class Program:
    assignments: tuple[tuple[str, int], ...]


def make_compiler() -> tuple[Engine, object, object, object, dict[str, int]]:
    engine = Engine()
    warnings = engine.accumulator("warnings")
    calls = {"parse": 0, "symbol_names": 0, "typecheck": 0}

    @engine.input
    def source(file_id: str) -> str:
        return ""

    @engine.query
    def parse(file_id: str) -> Program:
        calls["parse"] += 1
        text = source(file_id)
        rows = []
        for line in text.splitlines():
            if not line.strip():
                continue
            lhs, rhs = line.split("=")
            rows.append((lhs.strip(), int(rhs.strip())))
        return Program(assignments=tuple(rows))

    @engine.query
    def symbol_names(file_id: str) -> tuple[str, ...]:
        calls["symbol_names"] += 1
        program = parse(file_id)
        return tuple(name for name, _ in program.assignments)

    @engine.query
    def typecheck(file_id: str) -> tuple[str, ...]:
        calls["typecheck"] += 1
        names = symbol_names(file_id)
        seen: set[str] = set()
        for name in names:
            if name in seen:
                warnings.push(f"duplicate symbol: {name}")
            seen.add(name)
        return names

    return engine, source, parse, typecheck, calls


def run_demo() -> None:
    print("=== Compiler pipeline example ===")
    print("Step 1: Build an engine with source, parse, and typecheck queries.")
    engine, source, parse, typecheck, calls = make_compiler()

    print("Step 2: Seed a source file and run the pipeline once.")
    source.set("main", "a = 1\nb = 2")
    print("Parsed program:", parse("main"))
    effects: dict[str, list[str]] = {}
    print("Typecheck names:", typecheck("main", effects=effects))
    print("Warnings:", effects.get("warnings", []))
    print("Call counters after first run:", calls)

    print("Step 3: Re-run without input changes (cache hits, no recomputation).")
    effects_second: dict[str, list[str]] = {}
    print("Typecheck names:", typecheck("main", effects=effects_second))
    print("Warnings replayed from cache:", effects_second.get("warnings", []))
    print("Call counters after cache-hit run:", calls)

    print("Step 4: Introduce a duplicate symbol and inspect side-effect output.")
    source.set("main", "a = 1\na = 2")
    effects_third: dict[str, list[str]] = {}
    print("Typecheck names:", typecheck("main", effects=effects_third))
    print("Warnings after mutation:", effects_third.get("warnings", []))
    graph = engine.inspect_graph()
    print("Graph summary:", {"memo_count": graph["memo_count"], "input_count": graph["input_count"]})
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
