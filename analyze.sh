#! /bin/bash
set -eu

cd "$(readlink -f "$(dirname "$0")")"
exec python src/analyze.py "$@"
