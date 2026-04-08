from __future__ import annotations

from dataclasses import dataclass

from cascade import Engine


@dataclass(frozen=True)
class Program:
    assignments: tuple[tuple[str, int], ...]


def make_compiler() -> tuple[Engine, object, object, object]:
    engine = Engine()
    warnings = engine.accumulator("warnings")

    @engine.input
    def source(file_id: str) -> str:
        return ""

    @engine.query
    def parse(file_id: str) -> Program:
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
        program = parse(file_id)
        return tuple(name for name, _ in program.assignments)

    @engine.query
    def typecheck(file_id: str) -> tuple[str, ...]:
        names = symbol_names(file_id)
        seen: set[str] = set()
        for name in names:
            if name in seen:
                warnings.push(f"duplicate symbol: {name}")
            seen.add(name)
        return names

    return engine, source, parse, typecheck


def run_demo() -> None:
    engine, source, parse, typecheck = make_compiler()
    source.set("main", "a = 1\nb = 2")
    print(parse("main"))
    effects: dict[str, list[str]] = {}
    print(typecheck("main", effects=effects))
    print("warnings:", effects.get("warnings", []))
    print("graph:", engine.inspect_graph())


if __name__ == "__main__":
    run_demo()
