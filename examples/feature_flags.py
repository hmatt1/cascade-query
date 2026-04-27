import time
from cascade import Engine

# Incremental feature flag resolution.
# This example demonstrates caching of permission checks.

engine = Engine()

@engine.input
def user_plan(user_id: str) -> str:
    return "free"

@engine.input
def global_feature_toggle(feature: str) -> bool:
    return True

@engine.query
def check_permission(user_id: str, feature: str) -> bool:
    print(f"⏳ Calculating permission for {user_id} on {feature}...")
    time.sleep(1) 
    
    if not global_feature_toggle(feature):
        return False
        
    plan = user_plan(user_id)
    if feature == "pro_stats":
        return plan in ["pro", "enterprise"]
    return True

# --- Scenario ---

print("Step 1: First check (Slow)")
print(f"Permission: {check_permission('alice', 'pro_stats')}")

print("\nStep 2: Second check (Instant - Cached)")
print(f"Permission: {check_permission('alice', 'pro_stats')}")

print("\nStep 3: Change plan for 'alice'")
user_plan.set("alice", value="pro")

print("\nStep 4: Third check (Slow again - Cache invalidated)")
print(f"Permission: {check_permission('alice', 'pro_stats')}")

print("\nStep 5: Change UNRELATED flag ('export')")
global_feature_toggle.set("export", value=False)

print("\nStep 6: Fourth check (Instant - 'pro_stats' is unaffected)")
print(f"Permission: {check_permission('alice', 'pro_stats')}")

print("Example complete.")
