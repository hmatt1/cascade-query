"""Mutation testing with mutmut: run it, and turn its results into a report.

    python scripts/mutation.py run [mutmut run args] run mutmut
    python scripts/mutation.py collect               mutants/ -> mutation-results/
    python scripts/mutation.py summary               the report, as Markdown
    python scripts/mutation.py shards [NAME ...]     the CI matrix, as JSON

On one machine, the first three in that order are a whole run. mutmut needs
``fork``, which Windows does not have: there ``run`` hands over to
``scripts/mutation_windows.py``, which checks the same mutants against the
same tests with one process per mutant. ``.github/workflows/mutation.yml``
(started by hand) does the same split over several runners: one ``run`` +
``collect`` per shard, one ``summary`` over all of them. The survival rate
is the share of mutants the tests ran against and did not notice: code that
is covered but whose behaviour no assertion pins down.
"""

from __future__ import annotations

import argparse
import contextlib
import difflib
import fnmatch
import io
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

SOURCE = Path("src/cascade")
MUTANTS = Path("mutants")
RESULTS = Path("mutation-results")

# Which modules each CI job mutates. A full run is hours of test runs, so it
# is split by module; every module must be listed (``shards`` checks).
SHARDS: dict[str, list[str]] = {
    "evaluator": ["_evaluator"],
    "engine": ["engine"],
    "collections": ["_collections"],
    "store": ["_store"],
    "incremental": ["_incremental"],
    "disk": ["_disk_cache", "_serde", "_canonical"],
    "rewrite-keys": ["_ast_rewrite", "_keys", "_types"],
    "small": [
        "_errors",
        "_graph_export",
        "_runtime",
        "_scheduler",
        "_state",
        "_synthetic_graph",
    ],
}

# mutmut's exit codes (mutmut.__main__.status_by_exit_code); anything else
# it calls suspicious.
STATUS_BY_EXIT_CODE: dict[int | None, str] = {
    None: "not checked",
    0: "survived",
    1: "killed",
    3: "killed",
    2: "interrupted",
    5: "no tests",
    33: "no tests",
    34: "skipped",
    35: "suspicious",
    36: "timeout",
    37: "caught by type check",
    24: "timeout",
    -24: "timeout",
    152: "timeout",
    255: "timeout",
    -11: "segfault",
    -9: "segfault",
}
COLUMNS = ("killed", "survived", "timeout", "no tests", "other", "not checked")


# --- shards -------------------------------------------------------------------


def shards(only: list[str]) -> int:
    modules = sorted(p.stem for p in SOURCE.glob("*.py") if p.stem != "__init__")
    listed = [m for names in SHARDS.values() for m in names]
    missing = sorted(set(modules) - set(listed))
    stale = sorted(set(listed) - set(modules))
    twice = sorted(m for m, n in Counter(listed).items() if n > 1)
    unknown = sorted(set(only) - set(SHARDS))
    for problem, names in (
        ("in no shard", missing),
        ("listed but not in src/cascade", stale),
        ("in more than one shard", twice),
        ("not a shard", unknown),
    ):
        if names:
            print(f"scripts/mutation.py SHARDS: {problem}: {names}", file=sys.stderr)
    if missing or stale or twice or unknown:
        return 1
    matrix = [
        {"shard": name, "mutants": " ".join(f"cascade.{m}.*" for m in members)}
        for name, members in SHARDS.items()
        if not only or name in only
    ]
    print(json.dumps(matrix))
    return 0


# --- run ----------------------------------------------------------------------


def run(mutmut_args: list[str]) -> int:
    """``mutmut run``, set up so that this project's test suite passes under it.

    Both steps were found by running the suite the way mutmut does; without
    them mutmut stops with "failed to collect stats".
    """
    if sys.platform == "win32":
        # No fork: the same mutants and tests, one process per mutant. It
        # takes mutant patterns and --max-children as mutmut does.
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import mutation_windows

        return mutation_windows.main(["all", *mutmut_args])

    # mutmut runs the suite several times in one process and, after its
    # coverage pass, drops every module that pass imported so that the
    # mutated copies get imported next. A msgpack imported a second time no
    # longer round-trips tuples (they come back as lists), which fails the
    # codec tests. Imported here, it is loaded before mutmut starts counting
    # and is never dropped.
    import msgpack  # noqa: F401

    # A call of a mutated function passes through two trampoline frames, so
    # the suite's deep dependency chains need three times the stack.
    sys.setrecursionlimit(3 * sys.getrecursionlimit())

    from mutmut.__main__ import cli

    cli(["run", *mutmut_args], prog_name="mutmut")
    return 0


# --- collect ------------------------------------------------------------------


def _module_of(mutant: str) -> str:
    return mutant.rpartition(".")[0]


def _function_of(mutant: str) -> str:
    """``cascade._store.xǁGraphStoreǁset_input__mutmut_3`` -> ``GraphStore.set_input``."""
    name = mutant.rpartition(".")[2].rpartition("__mutmut_")[0]
    if name.startswith("xǁ"):
        return name[2:].replace("ǁ", ".")
    return name.removeprefix("x_")


def _read_meta(mutants_dir: Path) -> dict[Path, dict[str, str]]:
    """Source path -> {mutant: status}, from mutmut's ``.meta`` files."""
    by_path: dict[Path, dict[str, str]] = {}
    for meta in sorted(mutants_dir.rglob("*.py.meta")):
        exit_codes = json.loads(meta.read_text(encoding="utf-8"))["exit_code_by_key"]
        source = meta.relative_to(mutants_dir).with_suffix("")  # drops ".meta"
        by_path[source] = {
            mutant: STATUS_BY_EXIT_CODE.get(code, "suspicious")
            for mutant, code in exit_codes.items()
        }
    return by_path


def _survivor_diffs(survivors: dict[Path, list[str]], limit: int) -> str:
    """Unified diffs of surviving mutants, read from mutmut's mutated sources."""
    said = io.StringIO()
    try:
        # mutmut refuses to import on Windows, with a message on stdout.
        with contextlib.redirect_stdout(said):
            if sys.platform == "win32":
                sys.path.insert(0, str(Path(__file__).resolve().parent))
                import mutation_windows

                mutation_windows.import_mutmut()
            import libcst as cst
            from mutmut.__main__ import (
                read_mutant_function,
                read_mutants_module,
                read_original_function,
            )
    except (Exception, SystemExit) as exc:
        reason = said.getvalue().strip() or repr(exc)
        return f"# no diffs: mutmut could not be imported here ({reason})\n"
    out: list[str] = []
    written = 0
    total = sum(len(names) for names in survivors.values())
    for path, names in survivors.items():
        if written >= limit:
            break
        module = read_mutants_module(path)
        for name in names[: limit - written]:
            try:
                before = cst.Module([read_original_function(module, name)]).code
                after = cst.Module([read_mutant_function(module, name)]).code
            except Exception as exc:
                out.append(f"# {name}: no diff ({exc})\n")
                continue
            label = f"{path.as_posix()}  {name}"
            diff = difflib.unified_diff(
                before.strip().splitlines(),
                after.strip().splitlines(),
                fromfile=label,
                tofile=label,
                n=2,
                lineterm="",
            )
            out.append("\n".join(diff) + "\n\n")
            written += 1
    if written < total:
        out.append(f"# {total - written} more surviving mutants not shown\n")
    return "".join(out)


def collect(shard: str, out: Path, mutants_dir: Path, diff_limit: int) -> int:
    patterns = [f"cascade.{m}.*" for m in SHARDS.get(shard, [])]
    by_path = _read_meta(mutants_dir)
    checked: dict[str, str] = {}
    totals: Counter[str] = Counter()
    survivors: dict[Path, list[str]] = {}
    for path, statuses in by_path.items():
        for mutant, status in statuses.items():
            totals[_module_of(mutant)] += 1
            if status == "not checked":
                continue
            checked[mutant] = status
            if status == "survived":
                survivors.setdefault(path, []).append(mutant)
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(
        json.dumps({"shard": shard, "totals": totals, "checked": checked}, indent=1),
        encoding="utf-8",
    )
    (out / "survivors.diff").write_text(
        _survivor_diffs(survivors, diff_limit), encoding="utf-8"
    )
    ours = {
        module: total
        for module, total in totals.items()
        if not patterns or any(fnmatch.fnmatch(f"{module}.x", p) for p in patterns)
    }
    print(f"### Mutation testing: {shard}\n")
    print(_table(ours, checked))
    if not checked:
        print("\nNo mutant was checked: mutmut stopped before it got to them.")
    return 0


# --- summary ------------------------------------------------------------------


def _count(statuses: list[str]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for status in statuses:
        counts[status if status in COLUMNS else "other"] += 1
    return counts


def _rate(counts: Counter[str]) -> float | None:
    decided = counts["killed"] + counts["survived"] + counts["timeout"]
    return counts["survived"] / decided if decided else None


def _row(label: str, total: int, counts: Counter[str]) -> str:
    rate = _rate(counts)
    cells = [label, str(total), *(str(counts[c]) for c in COLUMNS)]
    cells.append("n/a" if rate is None else f"{rate:.1%}")
    return "| " + " | ".join(cells) + " |"


def _table(totals: dict[str, int], checked: dict[str, str]) -> str:
    """A Markdown table: one row per module, then the total."""
    by_module: dict[str, list[str]] = {module: [] for module in totals}
    for mutant, status in checked.items():
        by_module.setdefault(_module_of(mutant), []).append(status)
    header = ["Module", "Mutants", *(c.capitalize() for c in COLUMNS), "Survival rate"]
    lines = [
        "| " + " | ".join(header) + " |",
        "|---|" + "---:|" * (len(header) - 1),
    ]
    overall: Counter[str] = Counter()
    for module in sorted(by_module):
        counts = _count(by_module[module])
        total = max(totals.get(module, 0), len(by_module[module]))
        counts["not checked"] = total - len(by_module[module])
        overall += counts
        lines.append(_row(f"`{module}`", total, counts))
    lines.append(_row("**All**", sum(overall.values()), overall))
    return "\n".join(lines)


def summary(results_dir: Path, json_out: Path | None) -> int:
    totals: dict[str, int] = {}
    checked: dict[str, str] = {}
    files = sorted(results_dir.rglob("results.json"))
    for file in files:
        data = json.loads(file.read_text(encoding="utf-8"))
        for module, total in data["totals"].items():
            totals[module] = max(totals.get(module, 0), total)
        checked.update(data["checked"])
    print("## Mutation testing\n")
    if not files:
        print("No results: no shard got as far as collecting any.")
        return 1
    print(_table(totals, checked))
    print(
        "\nSurvival rate = survived / (killed + survived + timeout): the share "
        "of mutants the tests ran against and did not notice. Only covered "
        "lines are mutated. The diffs are in each shard's `survivors.diff`."
    )
    by_function = Counter(
        f"{_module_of(m)}: {_function_of(m)}"
        for m, status in checked.items()
        if status == "survived"
    )
    if by_function:
        print("\n### Functions with the most surviving mutants\n")
        print("| Function | Survived |\n|---|---:|")
        for function, n in by_function.most_common(25):
            print(f"| `{function}` | {n} |")
    counts = _count(list(checked.values()))
    if json_out is not None:
        payload: dict[str, Any] = {
            "mutants": sum(totals.values()),
            "checked": len(checked),
            **{c: counts[c] for c in COLUMNS if c != "not checked"},
            "survival_rate": _rate(counts),
        }
        json_out.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    # A module none of whose mutants was checked means a shard died early.
    seen = {_module_of(m) for m in checked}
    unchecked = sorted(m for m, total in totals.items() if total and m not in seen)
    if unchecked:
        print(f"\nNot checked at all: {', '.join(f'`{m}`' for m in unchecked)}.")
        return 1
    return 0


# --- command line -------------------------------------------------------------


def main(argv: list[str]) -> int:
    if argv[:1] == ["run"]:  # everything after it belongs to mutmut
        return run(argv[1:])
    parser = argparse.ArgumentParser(
        description="Mutation testing with mutmut: run it, and report its results."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="run mutmut; the arguments go to `mutmut run`")
    p = commands.add_parser("shards", help="print the CI matrix as JSON")
    p.add_argument("only", nargs="*", help="limit the matrix to these shards")
    p = commands.add_parser("collect", help="summarise mutants/ into results")
    p.add_argument("--shard", default="all", help="a CI shard; default: all modules")
    p.add_argument("--out", type=Path, help=f"default: {RESULTS}/<shard>")
    p.add_argument("--mutants-dir", type=Path, default=MUTANTS)
    p.add_argument("--diff-limit", type=int, default=300)
    p = commands.add_parser("summary", help="merge collected results into a report")
    p.add_argument("results_dir", type=Path, nargs="?", default=RESULTS)
    p.add_argument("--json", type=Path, default=None)
    args = parser.parse_args(argv)
    if args.command == "shards":
        return shards(args.only)
    if args.command == "collect":
        out = args.out or RESULTS / args.shard
        return collect(args.shard, out, args.mutants_dir, args.diff_limit)
    return summary(args.results_dir, args.json)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
