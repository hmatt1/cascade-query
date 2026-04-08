#!/usr/bin/env bash
set -euo pipefail

export PYTHON_GIL=0
export PATH="$HOME/.local/bin:$PATH"

# Keep startup idempotent and ensure required tools remain importable.
python3.14t -m pip --version >/dev/null
python3.14t -c "import pytest, sys, sysconfig; assert sysconfig.get_config_var('Py_GIL_DISABLED') == 1 and not sys._is_gil_enabled()" >/dev/null
