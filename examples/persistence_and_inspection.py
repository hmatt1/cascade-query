from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from cascade import Engine


def build_line_counter() -> tuple[Engine, object, object]:
    engine = Engine()

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def non_empty_lines(name: str) -> int:
        return len([line for line in source(name).splitlines() if line.strip()])

    return engine, source, non_empty_lines


def run_persistence_demo() -> None:
    print("=== Persistence and graph inspection example ===")
    print("This example demonstrates save/load and inspecting graph structure.")

    engine_a, source_a, non_empty_lines_a = build_line_counter()
    source_a.set("main", "alpha\n\nbeta\n")
    source_a.set("lib", "gamma\n")
    print("Step 1: Compute values in engine A.")
    print("main line count:", non_empty_lines_a("main"))
    print("lib line count:", non_empty_lines_a("lib"))
    graph_before = engine_a.inspect_graph()
    print(
        "Graph summary before save:",
        {"memo_count": graph_before["memo_count"], "input_count": graph_before["input_count"]},
    )

    with TemporaryDirectory() as tmp_dir:
        db_path = Path(tmp_dir) / "cascade_state.db"
        engine_a.save(str(db_path))
        print("Step 2: Saved state to:", db_path)

        counters = {"line_count_runs": 0}
        engine_b = Engine()

        @engine_b.input
        def source(name: str) -> str:
            return ""

        @engine_b.query
        def non_empty_lines(name: str) -> int:
            counters["line_count_runs"] += 1
            return len([line for line in source(name).splitlines() if line.strip()])

        print("Step 3: Load state into engine B and query values.")
        engine_b.load(str(db_path))
        print("main line count:", non_empty_lines("main"))
        print("lib line count:", non_empty_lines("lib"))
        print("Query recomputations after load:", counters["line_count_runs"])

        graph_after = engine_b.inspect_graph()
        print(
            "Graph summary after load:",
            {"memo_count": graph_after["memo_count"], "input_count": graph_after["input_count"]},
        )
    print("Example complete.")


if __name__ == "__main__":
    run_persistence_demo()
