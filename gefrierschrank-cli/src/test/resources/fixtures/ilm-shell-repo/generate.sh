#!/bin/bash
# Rebuilds an ilm-shell-repo-shaped repository on a throwaway Elasticsearch 9.3 container
# (image gefrierschrank-es9, container gs-shells-es9, host port 9341, removed on exit).
# Written after the fact so the fixture can be recreated or extended: the committed bytes in this
# directory were generated once and are never overwritten, and a regeneration has other UUIDs
# and slightly different segment bytes (ShellGroupSurveyTest and MountedShellsExportTest pin the
# committed ones); names, snapshots, documents and survey groups are the same.
#
# usage: SLOT=.../slot.sh; $SLOT docker 1 bash generate.sh <out-dir>   (out-dir must not exist)
set -euo pipefail
[ $# -eq 1 ] || { echo "usage: $0 <out-dir>" >&2; exit 2; }
[ -e "$1" ] && { echo "$1 exists; use a new output directory" >&2; exit 2; }
D=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$1/repos"
OUT=$(cd "$1" && pwd)
chmod 777 "$OUT/repos"
docker rm -f gs-shells-es9 >/dev/null 2>&1 || true
trap 'docker rm -f gs-shells-es9 >/dev/null 2>&1 || true' EXIT
docker run -d --name gs-shells-es9 -p 9341:9200 -e discovery.type=single-node -e xpack.security.enabled=false \
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" -e path.repo=/repo -e xpack.license.self_generated.type=trial \
  -e cluster.routing.allocation.disk.threshold_enabled=false -v "$OUT/repos:/repo" gefrierschrank-es9:latest >/dev/null
python3 -I "$D/generate.py" http://localhost:9341 "$OUT/repos" "$OUT/ilm-shell-repo"
