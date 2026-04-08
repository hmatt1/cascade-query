#!/usr/bin/env bash
set -euo pipefail

export PYTHON_GIL=0

# Install free-threaded Python 3.14t from deadsnakes if not already present.
if ! command -v python3.14t &>/dev/null; then
  sudo add-apt-repository -y ppa:deadsnakes/ppa
  sudo apt-get update -qq
  sudo apt-get install -y -qq python3.14-nogil python3.14-venv python3.14-dev
fi

# Bootstrap pip if missing.
if ! python3.14t -m pip --version &>/dev/null; then
  python3.14t -m ensurepip --upgrade
fi

# Verify free-threaded runtime.
python3.14t -c "import sysconfig; assert sysconfig.get_config_var('Py_GIL_DISABLED') == 1, 'Expected a free-threaded Python build (Py_GIL_DISABLED=1)'"
python3.14t -c "import sys; assert not sys._is_gil_enabled(), 'Expected GIL disabled at runtime for this project baseline'"

# Ensure ~/.local/bin is on PATH for pip-installed scripts.
export PATH="$HOME/.local/bin:$PATH"

# Install project in editable mode with dev dependencies.
python3.14t -m pip install --upgrade pip
python3.14t -m pip install -e . pytest pytest-cov ruff build
