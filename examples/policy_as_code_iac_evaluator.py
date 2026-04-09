from __future__ import annotations

from cascade import Engine


def run_demo() -> None:
    print("=== Policy-as-code evaluator for IaC changes example ===")
    print("Only impacted resources/rules are reevaluated after edits.")

    engine = Engine()
    violations = engine.accumulator("violations")
    calls = {"resource": {}, "violations_for_resource": {}, "policy_summary": 0}

    def bump(stage: str, key: str) -> None:
        bucket = calls[stage]
        bucket[key] = bucket.get(key, 0) + 1

    @engine.input
    def resource_spec(resource_id: str) -> tuple[tuple[str, str], ...]:
        return ()

    @engine.input
    def enabled_policies() -> tuple[str, ...]:
        return ()

    @engine.input
    def resources_in_scope() -> tuple[str, ...]:
        return ()

    @engine.query
    def resource(resource_id: str) -> dict[str, str]:
        bump("resource", resource_id)
        return dict(resource_spec(resource_id))

    @engine.query
    def violations_for_resource(resource_id: str) -> tuple[str, ...]:
        bump("violations_for_resource", resource_id)
        spec = resource(resource_id)
        policy_set = set(enabled_policies())
        issues: list[str] = []
        if "no_public_ip" in policy_set and spec.get("public_ip") == "true":
            issues.append(f"{resource_id}: public IP is forbidden")
        if "require_owner" in policy_set and not spec.get("owner"):
            issues.append(f"{resource_id}: owner tag is required")
        for issue in issues:
            violations.push(issue)
        return tuple(issues)

    @engine.query
    def policy_summary() -> tuple[tuple[str, tuple[str, ...]], ...]:
        calls["policy_summary"] += 1
        return tuple((rid, violations_for_resource(rid)) for rid in resources_in_scope())

    print("Step 1: Seed IaC resources and policy set.")
    resources_in_scope.set(("vm.api", "vm.worker", "bucket.logs"))
    enabled_policies.set(("no_public_ip", "require_owner"))
    resource_spec.set("vm.api", (("public_ip", "true"), ("owner", "platform")))
    resource_spec.set("vm.worker", (("public_ip", "false"), ("owner", "")))
    resource_spec.set("bucket.logs", (("public_ip", "false"), ("owner", "security")))
    effects_first: dict[str, list[str]] = {}
    print("summary:", policy_summary(effects=effects_first))
    print("violations:", effects_first.get("violations", []))
    print("Counters after first run:", calls)

    print("Step 2: Fix only vm.worker ownership and recompute summary.")
    resource_spec.set("vm.worker", (("public_ip", "false"), ("owner", "data")))
    effects_second: dict[str, list[str]] = {}
    print("summary:", policy_summary(effects=effects_second))
    print("violations:", effects_second.get("violations", []))
    print("Counters after targeted recompute:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
