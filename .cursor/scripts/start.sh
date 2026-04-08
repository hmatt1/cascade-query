#!/usr/bin/env bash
set -euo pipefail

# Keep startup idempotent and ensure required tools remain importable.
python3 -m pip --version >/dev/null
python3 -c "import pytest" >/dev/null
