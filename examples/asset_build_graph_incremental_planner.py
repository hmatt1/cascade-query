from __future__ import annotations

from cascade import Engine


def run_demo() -> None:
    print("=== Asset/build graph incremental planner example ===")
    print("Rebuild only outputs impacted by source/config/toolchain changes.")

    engine = Engine()
    calls = {"compile_module": 0, "bundle_app": 0, "emit_css": 0}

    @engine.input
    def file_text(path: str) -> str:
        return ""

    @engine.input
    def toolchain_version() -> str:
        return "v1"

    @engine.query
    def module_deps(path: str) -> tuple[str, ...]:
        deps: list[str] = []
        for line in file_text(path).splitlines():
            if line.startswith("import "):
                deps.append(line.removeprefix("import ").strip())
        return tuple(deps)

    @engine.query
    def compile_module(path: str) -> str:
        calls["compile_module"] += 1
        deps = module_deps(path)
        dep_signature = "|".join(sorted(deps))
        return f"obj({path})::{toolchain_version()}::deps={dep_signature}"

    @engine.query
    def bundle_app(entrypoint: str) -> str:
        calls["bundle_app"] += 1
        object_parts = [compile_module(entrypoint)]
        for dep in module_deps(entrypoint):
            object_parts.append(compile_module(dep))
        return f"bundle<{'+'.join(object_parts)}>"

    @engine.query
    def emit_css(path: str) -> str:
        calls["emit_css"] += 1
        return f"css({path})::{hash(file_text(path)) & 0xFFFF}"

    print("Step 1: Seed files and build JS bundle + CSS output.")
    file_text.set("src/main.ts", "import src/math.ts\nconsole.log(1)")
    file_text.set("src/math.ts", "export const add = (a, b) => a + b")
    file_text.set("src/styles.css", "body { color: black; }")
    print("bundle:", bundle_app("src/main.ts"))
    print("css:", emit_css("src/styles.css"))
    print("Counters:", calls)

    print("Step 2: Edit only CSS; JS bundle should stay cached.")
    file_text.set("src/styles.css", "body { color: navy; }")
    print("bundle:", bundle_app("src/main.ts"))
    print("css:", emit_css("src/styles.css"))
    print("Counters after CSS-only edit:", calls)

    print("Step 3: Upgrade toolchain; only compiled artifacts should refresh.")
    toolchain_version.set("v2")
    print("bundle:", bundle_app("src/main.ts"))
    print("css:", emit_css("src/styles.css"))
    print("Counters after toolchain bump:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
