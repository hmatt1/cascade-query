from cascade import Engine

# We will patch monotonic_seconds to simulate time advancing without
# actually sleeping in this example.
engine = Engine()

simulated_time = 0.0
engine._store.monotonic_seconds = lambda: simulated_time


@engine.input
def db_url():
    return "postgres://localhost:5432"


@engine.query(ttl=5.0)
def fetch_users(url: str):
    """
    This query fetches users. It caches the result for 5 seconds.
    If called again within 5 seconds, it returns the cached result.
    After 5 seconds, it recomputes and 'fetches' anew.
    """
    print(f"  [Network] Fetching users from {url} at time {simulated_time}...")
    return ["alice", "bob", "charlie"]


@engine.query
def process_users():
    """
    This query processes the users. It only recomputes if fetch_users()
    returns a different result (which in this mock, it doesn't, but the
    fetch query still runs to check).
    """
    users = fetch_users(db_url())
    print("  [CPU] Processing users...")
    return [u.upper() for u in users]


print("Step 1: --- Initial Call (Time 0.0) ---")
print(process_users())

print("\nStep 2: --- Immediate Call (Time 1.0) ---")
simulated_time = 1.0
print("Notice how no network or CPU logs appear because it's a pure cache hit:")
print(process_users())

print("\nStep 3: --- Call after TTL expires (Time 6.0) ---")
simulated_time = 6.0
print("Notice how the network log appears (TTL expired) but the CPU log does NOT,")
print("because the fetched users were the same, so cascade bails out early!")
print(process_users())

print("\nStep 4: --- Changing the input ---")
db_url.set("postgres://remote:5432")
print("Notice how both network and CPU logs appear, because the input changed:")
print(process_users())
print("\nExample complete.")
