# Agent Notes and Guidelines

This file contains important context and instructions for AI agents working on this repository.

## Linting and Formatting
- **Always run `ruff` before committing:** The CI pipeline enforces strict linting and formatting rules via `ruff`. 
- Before committing and pushing any code changes, always run:
  ```bash
  python -m ruff check --fix .
  python -m ruff format .
  ```
  This ensures that unused imports, missing or unused `noqa` directives, and formatting inconsistencies are automatically resolved without breaking the build.

## Storage Backend
- **Use `libmdbx`:** The project's persistent disk cache relies on `libmdbx` (using the `mdbx-py` python package, imported as `mdbx`). 
- **Do not use `lmdb`:** We migrated away from `lmdb` because its python bindings do not support free-threading Python. Always use `mdbx.Env` instead of `lmdb.open`, and ensure proper transaction management (e.g. manually committing `txn.commit()` when not using auto-committing features).
