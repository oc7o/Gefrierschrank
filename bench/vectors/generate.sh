#!/usr/bin/env bash
# Builds the vector datasets (see generate.py) into <out>/repo with a throwaway ES 9.3 container.
# usage: $SLOT docker 1 bench/vectors/generate.sh <out-dir> [docs-per-index]
set -euo pipefail
if [ $# -lt 1 ]; then
  sed -n '2,3p' "$0"; exit 2
fi
exec python3 "$(cd "$(dirname "$0")" && pwd)/generate.py" "$@"
