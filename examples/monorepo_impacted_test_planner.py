from __future__ import annotations

from cascade import Engine


def run_demo() -> None:
    print("=== Monorepo impacted-test planner example ===")
    print("Goal: run only tests affected by a PR diff.")
    engine = Engine()
    calls: dict[str, dict[str, int]] = {"module_deps": {}, "module_tests": {}, "impacted_tests": {}}

    def bump(stage: str, key: str) -> None:
        per_stage = calls[stage]
        per_stage[key] = per_stage.get(key, 0) + 1

    @engine.input
    def module_manifest(module: str) -> str:
        return ""

    @engine.query
    def module_deps(module: str) -> tuple[str, ...]:
        bump("module_deps", module)
        deps: list[str] = []
        for row in module_manifest(module).splitlines():
            row = row.strip()
            if row.startswith("deps:"):
                deps = [part.strip() for part in row.removeprefix("deps:").split(",") if part.strip()]
        return tuple(deps)

    @engine.query
    def module_tests(module: str) -> tuple[str, ...]:
        bump("module_tests", module)
        tests: list[str] = []
        for row in module_manifest(module).splitlines():
            row = row.strip()
            if row.startswith("tests:"):
                tests = [part.strip() for part in row.removeprefix("tests:").split(",") if part.strip()]
        return tuple(tests)

    @engine.input
    def all_modules() -> tuple[str, ...]:
        return ()

    @engine.query
    def downstream_modules(module: str) -> tuple[str, ...]:
        impacted: set[str] = {module}
        changed = True
        while changed:
            changed = False
            for candidate in all_modules():
                if candidate in impacted:
                    continue
                if any(dep in impacted for dep in module_deps(candidate)):
                    impacted.add(candidate)
                    changed = True
        return tuple(sorted(impacted))

    @engine.query
    def impacted_tests(changed_modules: tuple[str, ...]) -> tuple[str, ...]:
        bump("impacted_tests", ",".join(changed_modules))
        impacted_modules: set[str] = set()
        for module in changed_modules:
            impacted_modules.update(downstream_modules(module))
        tests: set[str] = set()
        for module in sorted(impacted_modules):
            tests.update(module_tests(module))
        return tuple(sorted(tests))

    print("Step 1: Register module manifests and compute tests for a diff.")
    all_modules.set(("core", "api", "web", "cli"))
    module_manifest.set(
        "core",
        "deps:\ntests: tests/core/test_parser.py, tests/core/test_runtime.py",
    )
    module_manifest.set("api", "deps: core\ntests: tests/api/test_handlers.py")
    module_manifest.set("web", "deps: api\ntests: tests/web/test_ui.py")
    module_manifest.set("cli", "deps: core\ntests: tests/cli/test_commands.py")
    print("Changed modules: ('core',)")
    print("Impacted tests:", impacted_tests(("core",)))
    print("Counters:", calls)

    print("Step 2: Update only web module tests and recompute a web-only diff.")
    module_manifest.set("web", "deps: api\ntests: tests/web/test_ui.py, tests/web/test_routing.py")
    print("Changed modules: ('web',)")
    print("Impacted tests:", impacted_tests(("web",)))
    print("Counters after targeted update:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
