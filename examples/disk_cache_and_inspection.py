from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from cascade import Engine


def build_line_counter(
    cache_dir: Path, recompute_counter: dict[str, int]
) -> tuple[Engine, object, object]:
    engine = Engine(cache_dir=cache_dir, cache_backend="sqlite")

    @engine.input
    def source(name: str) -> str:
        return ""

    @engine.query
    def non_empty_lines(name: str) -> int:
        recompute_counter["line_count_runs"] = (
            recompute_counter.get("line_count_runs", 0) + 1
        )
        return len([line for line in source(name).splitlines() if line.strip()])

    return engine, source, non_empty_lines


def run_disk_cache_demo() -> None:
    print("=== Disk cache and graph inspection example ===")
    print("Reuse results across engine sessions and inspect graph structure.")

    files = {"main": "alpha\n\nbeta\n", "lib": "gamma\n"}

    with TemporaryDirectory() as tmp_dir:
        cache_dir = Path(tmp_dir) / "cascade_cache"

        counts_a: dict[str, int] = {}
        engine_a, source_a, non_empty_lines_a = build_line_counter(cache_dir, counts_a)
        for name, text in files.items():
            source_a.set(name, text)
        print(
            "Step 1: Compute initial values in engine A (results go to the disk cache)."
        )
        print("main line count:", non_empty_lines_a("main"))
        print("lib line count:", non_empty_lines_a("lib"))
        print("Query computations in engine A:", counts_a.get("line_count_runs", 0))
        graph = engine_a.inspect_graph()
        print(
            "Graph summary:",
            {"memo_count": graph["memo_count"], "input_count": graph["input_count"]},
        )

        main_key = ("query", non_empty_lines_a.id, ("main",))
        deps = engine_a.subgraph([main_key], direction="deps")
        print(f"Step 2: Dependencies for 'main': {len(deps['nodes'])} nodes found.")

        lib_key = ("query", non_empty_lines_a.id, ("lib",))
        engine_a.prune([lib_key])
        print(
            "Step 3: Pruned everything except 'lib'; nodes left in memory:",
            engine_a.inspect_graph()["memo_count"],
        )
        engine_a.flush_disk()
        engine_a.shutdown()

        # A new engine, as in a new process: set the same inputs again and the
        # results come from the disk cache instead of being recomputed.
        counts_b: dict[str, int] = {}
        engine_b, source_b, non_empty_lines_b = build_line_counter(cache_dir, counts_b)
        for name, text in files.items():
            source_b.set(name, text)
        print("Step 4: New engine B with the same cache_dir and inputs.")
        print("main line count:", non_empty_lines_b("main"))
        print("lib line count:", non_empty_lines_b("lib"))
        print(
            "Query computations in engine B (0 means served from disk):",
            counts_b.get("line_count_runs", 0),
        )

        source_b.set("lib", "gamma\ndelta\n")
        print("Step 5: Change 'lib'; only that query recomputes.")
        print("lib line count:", non_empty_lines_b("lib"))
        print("Query computations in engine B:", counts_b.get("line_count_runs", 0))
        engine_b.shutdown()

    print("Example complete.")


if __name__ == "__main__":
    run_disk_cache_demo()
