import asyncio
import time
from cascade import Engine

engine = Engine()


# We can define input sources that return data (or coroutines!)
@engine.input
def user_id():
    return "user_1"


# Queries can be asynchronous to perform IO-bound operations
@engine.query
async def fetch_user_data(uid: str):
    print(f"  [Network] Fetching data for {uid}...")
    await asyncio.sleep(0.5)  # Simulate network IO
    return {"id": uid, "name": "Alice", "role": "admin"}


@engine.query
async def fetch_permissions(role: str):
    print(f"  [Network] Fetching permissions for role: {role}...")
    await asyncio.sleep(0.5)
    if role == "admin":
        return ["read", "write", "delete"]
    return ["read"]


# Async queries can call other async queries
@engine.query
async def user_profile():
    uid = user_id()
    data = await fetch_user_data(uid)
    perms = await fetch_permissions(data["role"])

    return {"user": data, "permissions": perms}


async def main():
    print("--- First Run ---")
    print("Step 1: Fetching initial data")
    start = time.perf_counter()
    # Execute the graph
    profile = await user_profile()
    elapsed = time.perf_counter() - start
    print(f"Result: {profile}")
    print(f"Took {elapsed:.2f}s (Expected ~1.0s)\n")

    print("--- Second Run (Cache Hit) ---")
    start = time.perf_counter()
    # Cache hit! No network calls will be made.
    profile = await user_profile()
    elapsed = time.perf_counter() - start
    print(f"Result: {profile}")
    print(f"Took {elapsed:.2f}s (Expected ~0.0s)\n")

    print("--- Input Change ---")
    # Change the input, triggering invalidation
    user_id.set("user_2")

    print("--- Third Run (Cache Miss) ---")
    start = time.perf_counter()
    # Cache miss for user data, but wait! Does the role change?
    # If the new user also has the 'admin' role, fetch_permissions won't re-run
    # because of Early Bail-out!
    profile = await user_profile()
    elapsed = time.perf_counter() - start
    print(f"Result: {profile}")
    print(f"Took {elapsed:.2f}s\n")
    print("Example complete.")


if __name__ == "__main__":
    asyncio.run(main())
