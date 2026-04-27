from __future__ import annotations

from cascade import Engine


def run_dynamic_demo() -> None:
    print("=== Dynamic macro expansion example ===")
    print("Runtime-generated dependencies and selective recomputation.")
    engine = Engine()
    counters = {"expand": 0, "lowered": 0}

    @engine.input
    def source(module: str) -> str:
        return ""

    @engine.query
    def expand(module: str) -> tuple[str, ...]:
        counters["expand"] += 1
        text = source(module)
        generated = []
        for line in text.splitlines():
            if line.startswith("macro make_fn "):
                name = line.split()[-1]
                generated.append(f"fn {name}()")
            else:
                generated.append(line)
        return tuple(generated)

    @engine.query
    def lowered(module: str) -> tuple[str, ...]:
        counters["lowered"] += 1
        # lowered depends on results generated at runtime by expand.
        return tuple(line.lower() for line in expand(module))

    print("Step 1: Set source and run expansion.")
    source.set("core", "macro make_fn Add\nmacro make_fn Sub")
    print("Lowered output:", lowered("core"))
    print("Counters:", counters)

    print("Step 2: Re-run with unchanged inputs (results retrieved from cache).")
    print("Lowered output:", lowered("core"))
    print("Counters:", counters)

    print("Step 3: Update input and re-run.")
    source.set("core", "macro make_fn Add\nmacro make_fn Mul")
    print("Lowered output:", lowered("core"))
    print("Counters:", counters)

    graph = engine.inspect_graph()
    print("Graph summary:", {"memo_count": graph["memo_count"], "input_count": graph["input_count"]})
    print("Example complete.")


if __name__ == "__main__":
    run_dynamic_demo()
