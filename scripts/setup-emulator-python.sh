#!/usr/bin/env bash

set -euo pipefail

fail() {
    echo "setup-emulator-python: $*" >&2
    exit 1
}

if [[ "${1:-}" == -h || "${1:-}" == --help ]]; then
    echo "Usage: ./scripts/setup-emulator-python.sh"
    exit 0
fi
[[ $# -eq 0 ]] || fail "unexpected argument: $1"
((EUID != 0)) || fail "run this script as your normal user, not with sudo"
command -v python3 >/dev/null 2>&1 || fail "python3 is not installed"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
venv_dir="$repo_root/emulator/venv"

python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 10) else "Python 3.10+ is required")'
if [[ ! -x "$venv_dir/bin/python" ]]; then
    python3 -m venv "$venv_dir"
fi

env -u PYTHONPATH "$venv_dir/bin/python" -m pip install "$repo_root/emulator"
env -u PYTHONPATH "$venv_dir/bin/python" -B "$repo_root/emulator/run_experiment.py" --dry-run >/dev/null

echo "Python environment is ready: $venv_dir"
