#!/bin/bash
# Regenerates before/ and after/ (ES 9.3 image gefrierschrank-es9, port 9320, container gs-retired-shell-gen).
# before/: slm-1 still retained. after/: slm-2 taken and slm-1 deleted by retention; the shell
# partial-logs-a still references the unchanged backing snapshot ilm-backing of logs-a.
set -e
D=$(cd "$(dirname "$0")" && pwd)
W=$(mktemp -d); mkdir -p $W/repos $W/out; chmod 777 $W/repos
docker rm -f gs-retired-shell-gen >/dev/null 2>&1 || true
docker run -d --name gs-retired-shell-gen -p 9320:9200 -e discovery.type=single-node -e xpack.security.enabled=false \
  -e "ES_JAVA_OPTS=-Xms1g -Xmx1g" -e path.repo=/repo -e xpack.license.self_generated.type=trial \
  -e cluster.routing.allocation.disk.threshold_enabled=false -v $W/repos:/repo gefrierschrank-es9:latest >/dev/null
trap 'docker rm -f gs-retired-shell-gen >/dev/null' EXIT
python3 $D/generate.py http://localhost:9320 $W/repos $W/out
rm -rf $D/before $D/after && cp -R $W/out/before $W/out/after $D/
