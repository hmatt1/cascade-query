# AGENTS.md

## Cursor Cloud specific instructions

This is a pure Python library (`query-cascade`) with no external services. Python **3.14+ free-threaded** (`python3.14t`) is required. CI uses `3.14t` with `PYTHON_GIL=0`.

### Key commands

All commands must run under `python3.14t` with `PYTHON_GIL=0` exported.

| Action | Command |
|---|---|
| Lint | `python3.14t -m ruff check .` |
| Test (default, excludes slow) | `python3.14t -m pytest -q` |
| Test like CI main step (excludes slow + ignores perf module) | `python3.14t -m pytest -q --ignore=tests/test_performance.py` |
| Test with coverage | `python3.14t -m pytest -q --cov=src/cascade --cov-branch --cov-report=term-missing` |
| Test slow/scale only | `python3.14t -m pytest -q -m slow` |
| Test all including slow | `python3.14t -m pytest -q -m "slow or not slow"` |
| Build package | `python3.14t -m build` |
| Run an example | `python3.14t examples/compiler_pipeline.py` |

### Notes

- The interpreter is `python3.14t` (free-threaded build from the `deadsnakes/ppa` PPA). Always set `export PYTHON_GIL=0` before running.
- pip-installed scripts (`pytest`, `ruff`) land in `~/.local/bin`; ensure this is on `PATH`.
- The default `pytest` invocation skips tests marked `@pytest.mark.slow` (configured in `pyproject.toml` via `addopts = ["-m", "not slow"]`).
- CI enforces 95% overall coverage and 90% branch coverage — keep these thresholds in mind when adding new code.
- `tests/test_performance.py::test_compute_many_parallel_speedup_scenario` is the most VM-sensitive performance gate (parallel speedup). CI runs it in a dedicated step. Locally, set `CASCADE_QUERY_PARALLEL_PERF_RETRIES=3` to re-run before failing, or `CASCADE_QUERY_SKIP_PARALLEL_PERF=1` to skip. `test_performance_regressions` excludes that scenario from its bundled threshold pass; it can still fail on very weak hosts but is usually stable.
- There are no external services, databases, or Docker dependencies. The entire dev loop is `pip install -e .` + `pytest`.
