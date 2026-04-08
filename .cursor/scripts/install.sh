#!/usr/bin/env bash
set -euo pipefail

# Preinstall project and Python test tooling for cloud agents.
python -c "import sysconfig; assert sysconfig.get_config_var('Py_GIL_DISABLED') == 1, 'Expected a free-threaded Python build (Py_GIL_DISABLED=1)'"
python -c "import sys; assert not sys._is_gil_enabled(), 'Expected GIL disabled at runtime for this project baseline'"
python -m pip install --upgrade pip
python -m pip install -e . pytest pytest-cov
