from __future__ import annotations

from cascade import Engine


def run_snapshot_demo() -> None:
    print("=== Snapshot isolation example ===")
    print("Goal: prove snapshot reads stay stable while live state changes.")
    engine = Engine()

    @engine.input
    def source(file_id: str) -> str:
        return ""

    @engine.query
    def parse(file_id: str) -> tuple[str, ...]:
        return tuple(line.strip() for line in source(file_id).splitlines() if line.strip())

    print("Step 1: Set initial source and take a snapshot.")
    source.set("main", "alpha\nbeta")
    frozen = engine.snapshot()
    print("Snapshot revision:", frozen.revision)
    print("Live parse before mutation:", parse("main"))

    print("Step 2: Mutate live source after snapshot.")
    source.set("main", "alpha\ngamma\ndelta")
    print("Live parse after mutation:", parse("main"))

    print("Step 3: Read through the old snapshot.")
    print("Snapshot parse (frozen view):", parse("main", snapshot=frozen))
    print("Example complete.")


if __name__ == "__main__":
    run_snapshot_demo()
