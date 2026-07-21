"""
Example demonstrating how to use Fixed-Point Cycle Solving in cascade-query.

In normal DAG-based evaluation, circular dependencies raise a CycleError.
By providing `fixed_point=...` to `@engine.query`, cascade-query intercepts the cycle
and iterates the sub-graph until the values converge to a steady state (a fixed point).

This is very useful for iterative spreadsheet calculations, UI layout constraints,
or type inference algorithms.
"""

from cascade import Engine

engine = Engine()


@engine.input
def base_profit() -> float:
    return 100000.0


# In this scenario, Employee Bonus is 10% of the Net Profit.
# However, the Net Profit is calculated AFTER deducting the Bonus!
# This creates a circular dependency: Bonus -> Net Profit -> Bonus -> Net Profit...

@engine.query(fixed_point=0.0)
def bonus_pool() -> float:
    # Bonus is 10% of net profit (rounded to nearest cent to ensure fast convergence)
    return round(net_profit() * 0.10, 2)


@engine.query
def net_profit() -> float:
    # Net profit is base profit minus the bonus
    return base_profit() - bonus_pool()


def main() -> None:
    print("Step 1: Evaluating spreadsheet with base profit of $100,000")
    
    # We expect:
    # Iteration 1: bonus guess=0.0 -> net_profit=100,000 -> bonus=10,000
    # Iteration 2: bonus guess=10,000 -> net_profit=90,000 -> bonus=9,000
    # Iteration 3: bonus guess=9,000 -> net_profit=91,000 -> bonus=9,100
    # ... converges rapidly!
    
    final_bonus = bonus_pool()
    final_profit = net_profit()
    print(f"  Converged Bonus Pool: ${final_bonus:,.2f}")
    print(f"  Converged Net Profit: ${final_profit:,.2f}")
    print(f"  Total: ${final_bonus + final_profit:,.2f} (should equal $100,000.00)")
    
    print("\nStep 2: Modifying base profit to $150,000 (incremental update)")
    base_profit.set(150000.0)
    
    # The cycle will reactively re-evaluate using the new base profit.
    final_bonus_2 = bonus_pool()
    final_profit_2 = net_profit()
    print(f"  Converged Bonus Pool: ${final_bonus_2:,.2f}")
    print(f"  Converged Net Profit: ${final_profit_2:,.2f}")
    
    print("\nExample complete.")


if __name__ == "__main__":
    main()
