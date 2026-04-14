#! /bin/bash
set -eu

cd "$(readlink -f "$(dirname "$0")")"
exec python3 src/analyze.py "$@"
