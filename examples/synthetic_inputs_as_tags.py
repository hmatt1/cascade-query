"""
Synthetic Inputs as "Tags" for Bulk Invalidation
=================================================

A common pattern in caching systems is wanting to "flush" or "invalidate" a group
of queries by a tag (e.g., "flush all queries tagged with 'db'"). 

In Cascade, explicitly flushing an intermediate query is an anti-pattern. If you
evict a node without its inputs changing, it receives a brand new `changed_at` epoch
when it recomputes, which destroys early bail-out for all downstream dependencies!

The "Cascade Way" is to use a synthetic `@engine.input` as your tag.
Queries read this input. When you want to "flush", you simply bump the input's epoch.
Because the previous cache entry is preserved, if the re-computation yields the exact
same data, downstream nodes will gracefully bail out, saving you a massive amount of work.
"""

import time
from cascade import Engine

engine = Engine()

# 1. Create a synthetic input to act as our "db" tag.
@engine.input
def db_epoch():
    return 0

# A simulated external database
_DB = {
    "user_1": {"name": "Alice", "role": "admin"},
    "user_2": {"name": "Bob", "role": "user"}
}

@engine.query
def fetch_user(uid: str):
    # 2. Read the synthetic input. This tags `fetch_user` as dependent on `db_epoch`.
    db_epoch()
    
    print(f"  [DB] Running SELECT * FROM users WHERE id = '{uid}'")
    time.sleep(0.5) # Simulate DB latency
    return _DB.get(uid)

@engine.query
def render_profile(uid: str):
    print(f"  [UI] Rendering profile for {uid}...")
    user = fetch_user(uid)
    return f"Profile Card: {user['name']} ({user['role']})"

def main():
    print("Step 1: --- FIRST RUN (Cold Cache) ---")
    print(render_profile("user_1"))
    
    print("\nStep 2: --- CHANGING A GLOBAL FLAG ---")
    # This will hit the cache instantly without hitting the DB or re-rendering
    print(render_profile("user_1"))
    
    print("\n--- BUMPING DB EPOCH (Flushing 'db' tag) ---")
    # This invalidates all queries that read `db_epoch()`
    db_epoch.set(db_epoch() + 1)
    
    print("\n--- THIRD RUN (Early Bail-out) ---")
    # fetch_user will re-run because db_epoch changed.
    # However, the DB returns the EXACT SAME data for user_1.
    # Cascade's red/green system sees the value hasn't changed, and
    # EARLY BAIL-OUTS render_profile, skipping the UI rendering step!
    print(render_profile("user_1"))
    
    print("\n--- BUMPING DB EPOCH (Real Data Change) ---")
    _DB["user_1"]["role"] = "super_admin"
    db_epoch.set(db_epoch() + 1)
    
    print("\n--- FOURTH RUN (Full Recompute) ---")
    # fetch_user will re-run.
    # The DB returns DIFFERENT data.
    # Cascade propagates the change, forcing render_profile to re-run.
    print(render_profile("user_1"))
    print("\nExample complete.")

if __name__ == "__main__":
    main()
