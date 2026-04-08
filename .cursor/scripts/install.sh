#!/usr/bin/env bash
set -euo pipefail

# Preinstall project and Python test tooling for cloud agents.
python3 -m pip install --upgrade pip
python3 -m pip install -e . pytest pytest-cov
