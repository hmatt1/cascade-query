import time
from cascade.engine import Engine

engine = Engine()

@engine.input
def config(key: str) -> str:
    return ""

@engine.query
def render_ui() -> str:
    print("  [EXEC] render_ui() is running...")
    theme = config("theme")
    layout = config("layout")
    return f"UI(theme={theme}, layout={layout})"

if __name__ == "__main__":
    print("=== First Run ===")
    config.set("theme", "light")
    config.set("layout", "grid")
    print("Result:", render_ui())

    print("\n=== Without Transactions (Flapping) ===")
    print("If we set theme and layout separately, an intermediate read might observe inconsistent state.")
    config.set("theme", "dark")
    # Imagine a concurrent read happens here! It sees theme="dark", layout="grid"
    config.set("layout", "list")
    print("Result:", render_ui())

    print("\n=== With Transactions ===")
    print("Setting both inputs in a transaction ensures they are committed atomically.")
    with engine.transaction():
        config.set("theme", "blue")
        config.set("layout", "sidebar")
    
    @engine.query
    def read_all() -> int:
        # A query that depends on 100 inputs
        return sum(len(config(f"key_{i}")) for i in range(100))

    # Evaluate once to prime the cache
    read_all()

    print("\n=== Efficiency: Without Transactions (Reactive System) ===")
    start_time = time.time()
    for i in range(100):
        config.set(f"key_{i}", "value")
        # In a reactive system, each update might trigger a re-eval
        read_all()
    no_tx_time = time.time() - start_time
    print(f"100 separate sets + 100 reads took {no_tx_time:.4f} seconds.")

    print("\n=== Efficiency: With Transactions (Reactive System) ===")
    start_time = time.time()
    with engine.transaction():
        for i in range(100):
            config.set(f"key_{i}", "value_new")
    # Only re-evaluate once after the transaction commits
    read_all()
    tx_time = time.time() - start_time
    print(f"100 batched sets + 1 read took {tx_time:.4f} seconds.")
    if tx_time > 0:
        print(f"Transactions were {no_tx_time/tx_time:.1f}x faster in this scenario.")
    
    print("\nStep 1")
    print("Example complete.")
