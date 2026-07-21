import time
from cascade import Engine

engine = Engine()

def example_walkthrough():
    print("Step 1: First Run")
    
    # Define an initial query function
    @engine.query
    def calculate_tax(amount: float) -> float:
        print(f"  [Compute] Calculating tax for {amount}...")
        time.sleep(0.5)
        return amount * 0.05
        
    start = time.perf_counter()
    result = calculate_tax(100.0)
    elapsed = time.perf_counter() - start
    print(f"Result: {result}")
    print(f"Took {elapsed:.2f}s\n")
    
    print("Step 2: Second Run (Cache Hit)")
    start = time.perf_counter()
    result = calculate_tax(100.0)
    elapsed = time.perf_counter() - start
    print(f"Result: {result}")
    print(f"Took {elapsed:.2f}s (Cache Hit!)\n")
    
    print("Step 3: Editing Function Logic")
    # Simulate a developer changing the code and the hot-reloader evaluating it.
    # The new function has the same module/qualname (so the query ID is identical),
    # but the logic (and therefore the bytecode hash) is different!
    @engine.query
    def calculate_tax_v2(amount: float) -> float:
        print(f"  [Compute] Calculating tax (v2) for {amount}...")
        time.sleep(0.5)
        return amount * 0.10 # Tax is now 10%
        
    calculate_tax_v2.raw.__module__ = calculate_tax.raw.__module__
    calculate_tax_v2.raw.__qualname__ = calculate_tax.raw.__qualname__
    
    # We re-assign the query handle to the name 'calculate_tax'
    # By simply re-registering it under the same name, Cascade automatically detects
    # the bytecode change and invalidates the previous cached values!
    calculate_tax = calculate_tax_v2
    
    print("Step 4: Third Run (Cache Miss Due To Code Change)")
    start = time.perf_counter()
    result = calculate_tax(100.0)
    elapsed = time.perf_counter() - start
    print(f"Result: {result}")
    print(f"Took {elapsed:.2f}s (Cache Miss! Code changed)\n")
    
    print("Example complete.")

if __name__ == "__main__":
    example_walkthrough()
