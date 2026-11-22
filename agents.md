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
- **Cache Backends:** The project supports multiple persistent cache backends via the `cache_backend` argument (`"mdbx"`, `"sqlite"`).
- **Default is `libmdbx`:** The default and recommended persistent disk cache relies on `libmdbx` (using the `mdbx-py` python package, imported as `mdbx`). 
- **LMDB was removed:** the `lmdb` backend segfaulted under parallel load on free-threaded Python (3.14t) and has been deleted. Do not reintroduce it; use `mdbx` or `sqlite`.
