#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"
export PYTHON_GIL="${PYTHON_GIL:-0}"

MAX_CHILDREN="${MUTMUT_MAX_CHILDREN:-2}"

echo "Running mutmut with max-children=${MAX_CHILDREN}"
# Through mutation.py: plain `mutmut run` stops at "failed to collect stats"
# on this suite (see the comments in its run()).
python "$(dirname "$0")/mutation.py" run --max-children "${MAX_CHILDREN}" "$@"
echo
echo "Mutation status summary:"
mutmut results
