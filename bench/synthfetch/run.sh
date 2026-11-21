#!/usr/bin/env bash
# Timed runs of the selective-fetch measurement over the datasets of bench/synthfetch/generate.py.
# Run inside one bench hold, after building the topic jar; SLOT names the semaphore script:
#   $SLOT bench 1 bench/synthfetch/run.sh <baseline-cli-jar> <topic-cli-jar> [repeats, default 3]
# Holds the docker slot only while the harness's MinIO runs (measure.py), then digests the first
# repeat's tables: baseline vs topic flag off, topic flag off vs flag on.
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "usage: $0 <baseline-cli-jar> <topic-cli-jar> [repeats]" >&2
  exit 2
fi
[ -n "${SLOT:-}" ] || { echo "set SLOT to the semaphore script" >&2; exit 2; }
root=$(cd "$(dirname "$0")/../.." && pwd)
base_jar=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
topic_jar=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
repeats=${3:-3}
[ -d "$root/target/synthfetch/minio-data" ] || { echo "no datasets: run bench/synthfetch/generate.py first" >&2; exit 2; }
out=$root/target/synthfetch/results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$out"
echo "load at start: $(sysctl -n vm.loadavg)" | tee "$out/load.txt"
shasum -a 256 "$base_jar" "$topic_jar" | tee "$out/jars.txt"

"$SLOT" docker 1 uv run --offline --frozen --project "$root/bench" python "$root/bench/synthfetch/measure.py" \
  "$base_jar" "$topic_jar" "$repeats" "$out"
echo "load at end: $(sysctl -n vm.loadavg)" | tee -a "$out/load.txt"

for label in base off on; do
  uv run --offline --frozen --project "$root/bench" gsbench digest "$out/tables/$label-r1" --out "$out/digests-$label.jsonl"
done
echo "== baseline vs flag off"
uv run --offline --frozen --project "$root/bench" gsbench compare-digests --strict "$out/digests-base.jsonl" "$out/digests-off.jsonl" \
  | tee "$out/compare-base-off.txt" || true
echo "== flag off vs flag on"
uv run --offline --frozen --project "$root/bench" gsbench compare-digests --strict "$out/digests-off.jsonl" "$out/digests-on.jsonl" \
  | tee "$out/compare-off-on.txt" || true
echo "results: $out"
