import time
from cascade import Engine

# Incremental data pipeline.
# Re-evaluates only the metrics affected by changed inputs.

engine = Engine()

@engine.input
def raw_sales_data() -> list[int]:
    return [100, 200, 300]

@engine.input
def tax_rate() -> float:
    return 0.1

@engine.query
def total_revenue() -> int:
    print("⏳ Summing up raw sales...")
    time.sleep(1)
    return sum(raw_sales_data())

@engine.query
def net_profit() -> float:
    print("⏳ Calculating net profit...")
    time.sleep(1)
    revenue = total_revenue()
    return revenue * (1 - tax_rate())

# --- Demo ---

print("Step 1: Initial Run")
print(f"Net Profit: {net_profit()}")

print("\nStep 2: Change Tax Rate (Revenue remains cached)")
tax_rate.set(0.15)
print(f"Net Profit: {net_profit()}")

print("\nStep 3: Change Sales Data (All queries refresh)")
raw_sales_data.set([100, 200, 300, 400])
print(f"Net Profit: {net_profit()}")

print("Example complete.")
