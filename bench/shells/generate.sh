#!/bin/bash
# Builds the 16-index ILM repository for --mounted-shells (8 originals x 30,000 documents and their
# 8 mounted partial-* shells, one shard each) on a throwaway Elasticsearch 9.3 container.
#
# usage: SLOT=.../slot.sh; $SLOT docker 1 bench/shells/generate.sh <out-dir> [--retention]
#   <out-dir>/ilm-repo           the repository after the frozen phase (16 names)
#   <out-dir>/ilm-repo-retained  with --retention: the shells re-mounted, slm-2026.09.23 taken and
#                                slm-2026.09.21/22 deleted by retention
#   <out-dir>/expected.json      snapshots and per-index document counts
# Container gs-shells-es9 on host port 9341 (image gefrierschrank-es9), removed on exit. The
# output directory must not exist yet.
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "usage: $0 <out-dir> [--retention]" >&2
  exit 2
fi
out=$1
shift
[ -e "$out" ] && { echo "$out exists; use a new output directory" >&2; exit 2; }
here=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$out/repos"
out=$(cd "$out" && pwd)
chmod 777 "$out/repos"

name=gs-shells-es9
docker rm -f "$name" >/dev/null 2>&1 || true
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
docker run -d --name "$name" -p 9341:9200 -e discovery.type=single-node -e xpack.security.enabled=false \
  -e "ES_JAVA_OPTS=-Xms2g -Xmx2g" -e path.repo=/repo -e xpack.license.self_generated.type=trial \
  -e cluster.routing.allocation.disk.threshold_enabled=false -v "$out/repos:/repo" gefrierschrank-es9:latest >/dev/null
python3 -I "$here/generate.py" http://localhost:9341 "$out" "$@"
