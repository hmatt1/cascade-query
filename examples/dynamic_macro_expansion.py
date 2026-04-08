from __future__ import annotations

from cascade import Engine


def run_dynamic_demo() -> None:
    engine = Engine()

    @engine.input
    def source(module: str) -> str:
        return ""

    @engine.query
    def expand(module: str) -> tuple[str, ...]:
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
        # Dynamic graph expansion: lowered depends on runtime-generated output.
        return tuple(line.lower() for line in expand(module))

    source.set("core", "macro make_fn Add\nmacro make_fn Sub")
    print(lowered("core"))
    source.set("core", "macro make_fn Add\nmacro make_fn Mul")
    print(lowered("core"))
    print(engine.inspect_graph())


if __name__ == "__main__":
    run_dynamic_demo()
