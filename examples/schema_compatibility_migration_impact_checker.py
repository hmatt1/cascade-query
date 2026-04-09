from __future__ import annotations

from cascade import Engine


def run_demo() -> None:
    print("=== Schema compatibility + migration impact checker example ===")
    engine = Engine()
    calls: dict[str, int] = {"parse_schema": 0, "direct_breaks": 0, "consumer_breaks": 0}

    @engine.input
    def schema(module: str) -> str:
        return ""

    @engine.input
    def consumers(module: str) -> tuple[str, ...]:
        return ()

    @engine.query
    def parse_schema(module: str) -> tuple[str, ...]:
        calls["parse_schema"] += 1
        return tuple(sorted({field.strip() for field in schema(module).split(",") if field.strip()}))

    @engine.query
    def direct_breaks(module: str) -> tuple[str, ...]:
        calls["direct_breaks"] += 1
        current = set(parse_schema(module))
        impacted: list[str] = []
        for consumer in consumers(module):
            required = set(parse_schema(consumer))
            missing = tuple(sorted(required - current))
            if missing:
                impacted.append(f"{consumer} missing {missing}")
        return tuple(impacted)

    @engine.query
    def consumer_breaks(module: str) -> tuple[str, ...]:
        calls["consumer_breaks"] += 1
        results: list[str] = list(direct_breaks(module))
        for consumer in consumers(module):
            results.extend(consumer_breaks(consumer))
        return tuple(results)

    print("Step 1: Define producer + downstream contracts.")
    schema.set("Order", "id,total,status")
    schema.set("BillingExport", "id,total")
    schema.set("FraudFeed", "id,status")
    consumers.set("Order", ("BillingExport", "FraudFeed"))
    consumers.set("BillingExport", ())
    consumers.set("FraudFeed", ())
    print("Impacts:", consumer_breaks("Order"))
    print("Counters:", calls)

    print("Step 2: Evolve Order schema and recompute impacts.")
    schema.set("Order", "id,total")
    print("Impacts:", consumer_breaks("Order"))
    print("Counters after schema evolution:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
