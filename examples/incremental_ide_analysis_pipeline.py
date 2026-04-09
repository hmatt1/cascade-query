from __future__ import annotations

from dataclasses import dataclass

from cascade import Engine


@dataclass(frozen=True)
class ParsedFile:
    lines: tuple[str, ...]


def run_demo() -> None:
    print("=== Incremental IDE analysis pipeline example ===")
    print("Pipeline: parse -> symbol index -> type diagnostics -> code actions")

    engine = Engine()
    diagnostics = engine.accumulator("diagnostics")
    calls: dict[str, dict[str, int]] = {
        "parse": {},
        "symbol_index": {},
        "type_diagnostics": {},
        "code_actions": {},
    }

    def bump(stage: str, path: str) -> None:
        per_stage = calls[stage]
        per_stage[path] = per_stage.get(path, 0) + 1

    @engine.input
    def source(path: str) -> str:
        return ""

    @engine.query
    def parse(path: str) -> ParsedFile:
        bump("parse", path)
        rows = tuple(line.strip() for line in source(path).splitlines() if line.strip())
        return ParsedFile(lines=rows)

    @engine.query
    def symbol_index(path: str) -> tuple[str, ...]:
        bump("symbol_index", path)
        names: list[str] = []
        for line in parse(path).lines:
            if line.startswith("let "):
                names.append(line.removeprefix("let ").split("=", 1)[0].strip())
        return tuple(names)

    @engine.query
    def type_diagnostics(path: str) -> tuple[str, ...]:
        bump("type_diagnostics", path)
        defined = set(symbol_index(path))
        issues: list[str] = []
        for line_no, line in enumerate(parse(path).lines, start=1):
            if line.startswith("use "):
                symbol = line.removeprefix("use ").strip()
                if symbol not in defined:
                    message = f"{path}:{line_no}: unknown symbol '{symbol}'"
                    diagnostics.push(message)
                    issues.append(message)
        return tuple(issues)

    @engine.query
    def code_actions(path: str) -> tuple[str, ...]:
        bump("code_actions", path)
        fixes = []
        for issue in type_diagnostics(path):
            symbol = issue.split("'")[-2]
            fixes.append(f"{path}: add `let {symbol} = ...`")
        return tuple(fixes)

    print("Step 1: Analyze two files.")
    source.set("main.qc", "let count = 1\nuse count\nuse missing")
    source.set("utils.qc", "let helper = 1\nuse helper")

    effects_main: dict[str, list[str]] = {}
    effects_utils: dict[str, list[str]] = {}
    print("main actions:", code_actions("main.qc", effects=effects_main))
    print("main diagnostics:", effects_main.get("diagnostics", []))
    print("utils actions:", code_actions("utils.qc", effects=effects_utils))
    print("utils diagnostics:", effects_utils.get("diagnostics", []))
    print("Counters after first analysis:", calls)

    print("Step 2: Edit only main.qc and re-run both files.")
    source.set("main.qc", "let count = 1\nlet missing = 2\nuse count\nuse missing")
    effects_main_second: dict[str, list[str]] = {}
    effects_utils_second: dict[str, list[str]] = {}
    print("main actions:", code_actions("main.qc", effects=effects_main_second))
    print("main diagnostics:", effects_main_second.get("diagnostics", []))
    print("utils actions:", code_actions("utils.qc", effects=effects_utils_second))
    print("utils diagnostics:", effects_utils_second.get("diagnostics", []))
    print("Counters after targeted recompute:", calls)
    print("Example complete.")


if __name__ == "__main__":
    run_demo()
