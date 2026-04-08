# AGENTS.md

## Cursor Cloud specific instructions

This is a pure Python library (`query-cascade`) with no external services. Python 3.10+ is required (CI uses 3.12).

### Key commands

| Action | Command |
|---|---|
| Lint | `python3 -m ruff check .` |
| Test (default, excludes slow) | `pytest -q` |
| Test with coverage | `pytest -q --cov=src/cascade --cov-branch --cov-report=term-missing` |
| Test slow/scale only | `pytest -q -m slow` |
| Test all including slow | `pytest -q -m "slow or not slow"` |
| Build package | `python3 -m build` |
| Run an example | `python3 examples/compiler_pipeline.py` |

### Notes

- The default `pytest` invocation skips tests marked `@pytest.mark.slow` (configured in `pyproject.toml` via `addopts = ["-m", "not slow"]`).
- CI enforces 95% overall coverage and 90% branch coverage — keep these thresholds in mind when adding new code.
- There are no external services, databases, or Docker dependencies. The entire dev loop is `pip install -e .` + `pytest`.
