from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

from cascade import Engine


def build_pipeline(cache_dir: Path, runs: dict[str, int]) -> tuple[Engine, Any]:
    engine = Engine(cache_dir=cache_dir)
    warnings = engine.accumulator("warnings")

    @engine.input
    def source_text(path: str) -> str:
        return Path(path).read_text()

    @engine.query
    def normalized(path: str) -> str:
        runs["normalized"] = runs.get("normalized", 0) + 1
        text = source_text(path)
        if "\t" in text:
            warnings.push(f"{path} contains tabs")
        return " ".join(text.split())

    @engine.query
    def word_count(path: str) -> int:
        runs["word_count"] = runs.get("word_count", 0) + 1
        return len(normalized(path).split())

    return engine, word_count


def run_disk_cache_demo() -> None:
    print("=== Zero-config persistent disk cache example ===")
    print("Cache query results across engine sessions with cache_dir.")

    with TemporaryDirectory() as tmp_dir:
        cache_dir = Path(tmp_dir) / "cascade_cache"
        source = Path(tmp_dir) / "module.txt"
        source.write_text("def main():\treturn 42")

        runs: dict[str, int] = {}
        print("Step 1: Session A computes and persists to disk.")
        engine_a, word_count_a = build_pipeline(cache_dir, runs)
        effects: dict[str, list[Any]] = {}
        print("word count:", word_count_a(str(source), effects=effects))
        print("query executions:", runs, "warnings:", effects.get("warnings"))
        engine_a.shutdown()

        runs.clear()
        print("Step 2: Session B (a fresh engine) hits the disk cache.")
        engine_b, word_count_b = build_pipeline(cache_dir, runs)
        effects = {}
        print("word count:", word_count_b(str(source), effects=effects))
        print("query executions:", runs, "warnings replayed:", effects.get("warnings"))
        engine_b.shutdown()

        source.write_text("def main():\treturn 42  # noted")

        runs.clear()
        print("Step 3: Session C sees the file edit and recomputes only what changed.")
        engine_c, word_count_c = build_pipeline(cache_dir, runs)
        print("word count:", word_count_c(str(source)))
        print("query executions:", runs)
        engine_c.shutdown()

    print("Example complete.")


if __name__ == "__main__":
    run_disk_cache_demo()
