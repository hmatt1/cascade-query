from cascade.engine import Engine

engine = Engine()


# We use an engine input to simulate changing upstream data
@engine.input
def get_data() -> str:
    return "foo1,bar1"


# 1. The Multi-Output Query
@engine.query
def process_data() -> tuple[str, str]:
    """
    Returns multiple independent outputs (a tuple of two strings).
    """
    print("  [EXEC] process_data() is running...")
    parts = get_data().split(",")
    return parts[0], parts[1]


# 2. The Projection Queries
# These isolate the outputs so downstream consumers only depend on what they need.
@engine.query
def get_foo() -> str:
    print("  [EXEC] get_foo() projection is running...")
    return process_data()[0]


@engine.query
def get_bar() -> str:
    print("  [EXEC] get_bar() projection is running...")
    return process_data()[1]


# 3. Downstream Consumers
@engine.query
def downstream_foo_user() -> str:
    """A downstream query that ONLY cares about 'foo'."""
    print("  [EXEC] downstream_foo_user() is running...")
    foo_value = get_foo()
    return f"Result: {foo_value}"


if __name__ == "__main__":
    print("=== SCENARIO 1: First Run ===")
    print(
        "Expected: Everything should run (process_data, get_foo, downstream_foo_user)."
    )
    downstream_foo_user()

    print("\n=== SCENARIO 2: Second Run (No changes) ===")
    print("Expected: NOTHING should run (all cache hits).")
    downstream_foo_user()

    print("\n=== SCENARIO 3: Changing Input (Only affects 'bar') ===")
    print("Expected: 'process_data' and 'get_foo' will run.")
    print(
        "          But 'downstream_foo_user' will NOT run because 'foo' didn't change!"
    )
    # We change the data so 'foo' stays the same, but 'bar' changes.
    get_data.set("foo1,bar2")
    downstream_foo_user()

    print("\n=== SCENARIO 4: Changing Input (Affects 'foo') ===")
    print("Expected: Everything should run because 'foo' actually changed.")
    # Now we change the data in a way that affects 'foo'
    get_data.set("foo2,bar2")
    downstream_foo_user()
    print("\nStep 1")
    print("Example complete.")
