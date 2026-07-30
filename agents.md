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
- **Cache Backends:** The project supports multiple persistent cache backends via the `cache_backend` argument (`"mdbx"`, `"sqlite"`, `"lmdb"`).
- **Default is `libmdbx`:** The default and recommended persistent disk cache relies on `libmdbx` (using the `mdbx-py` python package, imported as `mdbx`). 
- **LMDB Caveat:** We historically migrated away from `lmdb` because its python bindings do not support free-threading Python cleanly. It is supported as an alternative backend, but `mdbx` or `sqlite` are preferred for 3.14t multi-threading.
