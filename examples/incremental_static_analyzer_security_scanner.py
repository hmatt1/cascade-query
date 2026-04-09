from __future__ import annotations

from cascade import Engine


def run_demo() -> None:
    print("=== Incremental static analyzer / security scanner example ===")
    print("Recompute findings only for changed files and impacted dependents.")

    engine = Engine()
    findings = engine.accumulator("findings")
    calls: dict[str, int] = {"imports": 0, "vuln_scan": 0, "module_risk": 0, "repo_scan": 0}

    @engine.input
    def source(module: str) -> str:
        return ""

    @engine.input
    def imports_of(module: str) -> tuple[str, ...]:
        return ()

    @engine.input
    def modules() -> tuple[str, ...]:
        return ()

    @engine.query
    def import_index(module: str) -> tuple[str, ...]:
        calls["imports"] += 1
        return imports_of(module)

    @engine.query
    def vuln_scan(module: str) -> tuple[str, ...]:
        calls["vuln_scan"] += 1
        issues: list[str] = []
        text = source(module)
        if "eval(" in text:
            msg = f"{module}: dangerous eval()"
            findings.push(msg)
            issues.append(msg)
        if "subprocess.run(" in text and "shell=True" in text:
            msg = f"{module}: shell=True command execution"
            findings.push(msg)
            issues.append(msg)
        return tuple(issues)

    @engine.query
    def module_risk(module: str) -> tuple[str, ...]:
        calls["module_risk"] += 1
        local = list(vuln_scan(module))
        for dep in import_index(module):
            if vuln_scan(dep):
                msg = f"{module}: transitively imports risky module {dep}"
                findings.push(msg)
                local.append(msg)
        return tuple(local)

    @engine.query
    def repo_scan() -> tuple[str, ...]:
        calls["repo_scan"] += 1
        all_issues: list[str] = []
        for module in modules():
            all_issues.extend(module_risk(module))
        return tuple(all_issues)

    print("Step 1: Seed graph and run full scan.")
    modules.set(("app", "utils", "legacy"))
    imports_of.set("app", ("utils",))
    imports_of.set("utils", ())
    imports_of.set("legacy", ())
    source.set("app", "def handler(data):\n    return sanitize(data)")
    source.set("utils", "def sanitize(data):\n    return data.strip()")
    source.set("legacy", "def run(cmd):\n    return eval(cmd)")
    effects_a: dict[str, list[str]] = {}
    print("repo findings:", repo_scan(effects=effects_a))
    print("side-effect findings:", effects_a.get("findings", []))
    print("Counters:", calls)

    print("Step 2: Remove eval() from legacy and rescan.")
    source.set("legacy", "def run(cmd):\n    return cmd.upper()")
    effects_b: dict[str, list[str]] = {}
    print("repo findings:", repo_scan(effects=effects_b))
    print("side-effect findings:", effects_b.get("findings", []))
    print("Counters after targeted recompute:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
