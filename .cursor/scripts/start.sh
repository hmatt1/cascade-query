#!/usr/bin/env bash
set -euo pipefail

# Keep startup idempotent and ensure required tools remain importable.
python -m pip --version >/dev/null
python -c "import pytest, sys, sysconfig; assert sysconfig.get_config_var('Py_GIL_DISABLED') == 1 and not sys._is_gil_enabled()" >/dev/null
