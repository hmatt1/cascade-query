"""Incremental collections: standard Python comprehensions over live data.

CascadeList, CascadeSet, and CascadeDict look and act like their builtin
counterparts, but every mutation emits a diff into an event log. Queries
written with ordinary comprehensions and reducers (sum, len, min, max,
sorted, any, all, list, set, dict, str.join) are rewritten at registration
into incremental pipelines that consume only those diffs. Appending one item
to a list of a million reruns your mapping function once, for that item.
"""

from cascade import CascadeDict, CascadeList, Engine

engine = Engine()

orders = CascadeList(engine, [120, 45, 300, 80])
prices = CascadeDict(engine, {"basic": 10, "pro": 25})

score_calls = []


def score(order):
    score_calls.append(order)
    return order * 2


@engine.query
def big_order_total():
    return sum(score(o) for o in orders if o >= 100)


@engine.query
def price_report():
    return ", ".join(f"{name}={cost}" for name, cost in sorted(prices.items()))


def example_walkthrough():
    print("Step 1: First computation walks the whole collection")
    print(f"  total = {big_order_total()}")
    print(
        f"  score() ran {len(score_calls)} times (once per element passing the filter)"
    )

    print("Step 2: Cached while nothing changes")
    score_calls.clear()
    big_order_total()
    print(f"  score() ran {len(score_calls)} times")

    print("Step 3: One append means one unit of work")
    orders.append(500)
    print(f"  total = {big_order_total()}")
    print(f"  score() ran {len(score_calls)} times, on {score_calls}")

    print("Step 4: Updates and removals adjust the running state")
    orders[0] = 20  # falls below the filter now
    orders.remove(300)
    print(f"  total = {big_order_total()}")

    print("Step 5: Dict views work the same way")
    print(f"  report: {price_report()}")
    prices["enterprise"] = 90
    del prices["basic"]
    print(f"  report: {price_report()}")

    print("Step 6: The pipelines are visible on the graph")
    for pipe in engine.inspect_pipelines():
        stages = " -> ".join(pipe["stages"]) or "(none)"
        print(
            f"  {pipe['source']} [{stages}] -> {pipe['reducer']} (checkpoint rev {pipe['last_rev']})"
        )

    print("Example complete.")


if __name__ == "__main__":
    example_walkthrough()
