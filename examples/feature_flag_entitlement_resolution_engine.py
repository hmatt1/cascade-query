from __future__ import annotations

from cascade import Engine


def run_demo() -> None:
    print("=== Feature-flag / entitlement resolution engine example ===")
    print("Compute effective access from plan, flags, and overrides incrementally.")

    engine = Engine()
    calls = {"effective_access": 0}

    @engine.input
    def account_plan(account_id: str) -> str:
        return "free"

    @engine.input
    def global_flag(feature: str) -> bool:
        return False

    @engine.input
    def org_override(account_id: str, feature: str) -> bool | None:
        return None

    @engine.input
    def user_override(user_id: str, feature: str) -> bool | None:
        return None

    @engine.query
    def base_plan_access(account_id: str, feature: str) -> bool:
        plan = account_plan(account_id)
        if feature == "export":
            return plan in {"pro", "enterprise"}
        if feature == "audit-log":
            return plan == "enterprise"
        return False

    @engine.query
    def effective_access(user_id: str, account_id: str, feature: str) -> bool:
        calls["effective_access"] += 1
        user = user_override(user_id, feature)
        if user is not None:
            return user
        org = org_override(account_id, feature)
        if org is not None:
            return org
        return base_plan_access(account_id, feature) and global_flag(feature)

    print("Step 1: Configure account, flags, and evaluate features.")
    account_plan.set("acme", "pro")
    global_flag.set("export", True)
    global_flag.set("audit-log", True)
    print("u1 export:", effective_access("u1", "acme", "export"))
    print("u1 audit-log:", effective_access("u1", "acme", "audit-log"))
    print("Recompute counter:", calls)

    print("Step 2: Add user override; only affected key recomputes.")
    user_override.set("u1", "audit-log", True)
    print("u1 export:", effective_access("u1", "acme", "export"))
    print("u1 audit-log:", effective_access("u1", "acme", "audit-log"))
    print("Recompute counter:", calls)

    print("Step 3: Disable global export flag and observe targeted invalidation.")
    global_flag.set("export", False)
    print("u1 export:", effective_access("u1", "acme", "export"))
    print("u1 audit-log:", effective_access("u1", "acme", "audit-log"))
    print("Recompute counter:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
