"""
Example demonstrating Fixed-Point Cycle Solving for Type Inference.

When compiling modern languages (like TypeScript), the compiler often has to infer
the return types of functions. If two functions call each other (mutual recursion),
the compiler encounters a circular dependency.

Using a fixed-point solver, the compiler starts by assuming the return type is "Unknown"
(an empty set) and continuously runs the type-checker until the union of all discovered
types stabilizes!
"""

from cascade import Engine

engine = Engine()

# Scenario: We are inferring the return types of two mutually recursive functions:
#
# function ping(n) {
#     if (n === 0) return "done";   // Returns a STRING
#     return pong(n - 1);           // Returns whatever pong() returns
# }
#
# function pong(n) {
#     if (n === 0) return 42;       // Returns a NUMBER
#     return ping(n - 1);           // Returns whatever ping() returns
# }
#
# Because they call each other, their return types form a cycle:
# type(ping) = "string" UNION type(pong)
# type(pong) = "number" UNION type(ping)


@engine.query(fixed_point=frozenset())
def infer_ping_type() -> frozenset[str]:
    # ping() returns a "string", plus whatever pong() returns
    types = set(["string"])
    types.update(infer_pong_type())
    return frozenset(types)


@engine.query
def infer_pong_type() -> frozenset[str]:
    # pong() returns a "number", plus whatever ping() returns
    types = set(["number"])
    types.update(infer_ping_type())
    return frozenset(types)


def main() -> None:
    print("Step 1: Running Type Inference Engine...")

    # We expect the engine to gracefully resolve the circular type constraint:
    #
    # Iteration 1:
    #   ping_guess = empty set ()
    #   pong evaluates to: "number" UNION () = ("number")
    #   ping evaluates to: "string" UNION ("number") = ("string", "number")
    #   Mismatch! (guess was empty, result is "string", "number"). Loops!
    #
    # Iteration 2:
    #   ping_guess = ("string", "number")
    #   pong evaluates to: "number" UNION ("string", "number") = ("string", "number")
    #   ping evaluates to: "string" UNION ("string", "number") = ("string", "number")
    #   Matches guess! Converged.

    ping_type = infer_ping_type()
    pong_type = infer_pong_type()

    # Sort for deterministic printing
    print(f"  Inferred return type for ping(): {' | '.join(sorted(ping_type))}")
    print(f"  Inferred return type for pong(): {' | '.join(sorted(pong_type))}")

    print("\nExample complete.")


if __name__ == "__main__":
    main()
