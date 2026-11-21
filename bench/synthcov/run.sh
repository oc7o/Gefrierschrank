#!/usr/bin/env bash
# Timed runs of the synthetic-coverage measurements over the datasets of bench/synthcov/generate.py
# (target/synthcov/repo). Run inside one bench hold, after building the topic jar:
#   $SLOT bench 1 bench/synthcov/run.sh <baseline-cli-jar> <topic-cli-jar> [repeats]
# Writes target/synthcov/results/<timestamp>/runs.tsv (one line per run: measurement, jar label,
# index, shard threads, repeat, wall seconds, peak RSS bytes, per-shard rows and milliseconds from
# the run report) and summary.json (medians), and verifies one table per jar and index against
# Elasticsearch's own output (generate.py verify).
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "usage: $0 <baseline-cli-jar> <topic-cli-jar> [repeats]" >&2
  exit 2
fi
root=$(cd "$(dirname "$0")/../.." && pwd)
base_jar=$1
topic_jar=$2
repeats=${3:-3}
repo=$root/target/synthcov/repo
[ -d "$repo" ] || { echo "no datasets: run bench/synthcov/generate.py generate first" >&2; exit 2; }
out=$root/target/synthcov/results/$(date +%Y%m%d-%H%M%S)
mkdir -p "$out/tables" "$out/reports" "$out/logs"
printf 'measurement\tjar\tindex\tthreads\trepeat\twall_s\tmax_rss_bytes\tshards\n' > "$out/runs.tsv"
echo "load at start: $(sysctl -n vm.loadavg)" | tee "$out/load.txt"

# One export: dump-to-delta of one index into a fresh table, timed with /usr/bin/time -l.
run() {
  local measurement=$1 label=$2 jar=$3 index=$4 threads=$5 repeat=$6
  local name="$measurement-$label-$index-k$threads-r$repeat"
  local start end
  start=$(python3 -c 'import time; print(time.time())')
  /usr/bin/time -l java -Xmx4g -jar "$jar" dump-to-delta "$repo" --index "$index" -o "$out/tables/$name" \
    --report "$out/reports/$name.json" --shard-threads "$threads" --parallelism 1 > "$out/logs/$name.log" 2> "$out/logs/$name.err" || true
  end=$(python3 -c 'import time; print(time.time())')
  local rss shards
  rss=$(awk '/maximum resident set size/ {print $1}' "$out/logs/$name.err")
  shards=$(python3 - "$out/reports/$name.json" <<'PY'
import json, sys
try:
    report = json.load(open(sys.argv[1]))
except (OSError, ValueError):
    print("no-report"); sys.exit()
print(";".join(f"{s['shardId']}:{s['outcome']}:{s['rowsWritten']}:{s['elapsedMillis']}" for s in report["shards"]))
PY
)
  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$measurement" "$label" "$index" "$threads" "$repeat" \
    "$(python3 -c "print(round($end - $start, 3))")" "$rss" "$shards" >> "$out/runs.tsv"
  echo "$name: $(tail -1 "$out/runs.tsv" | cut -f6-)"
}

for r in $(seq 1 "$repeats"); do
  # (1) nested vs flat on the topic jar, one thread and auto.
  for threads in 1 auto; do
    run nested topic "$topic_jar" nested-synth "$threads" "$r"
    run nested topic "$topic_jar" flat-synth "$threads" "$r"
  done
  # (2) documents rebuilt through the rewrite (srcoff shard 1) vs the same documents native.
  run rewrite topic "$topic_jar" srcoff 1 "$r"
  run rewrite topic "$topic_jar" srcoff-twin-synth 1 "$r"
  # (3)/(4) flat synthetic, baseline vs topic back to back.
  run flat base "$base_jar" flat-synth auto "$r"
  run flat topic "$topic_jar" flat-synth auto "$r"
done
echo "load at end: $(sysctl -n vm.loadavg)" | tee -a "$out/load.txt"

verify() {
  uv run --offline --frozen --project "$root/bench" python "$root/bench/synthcov/generate.py" verify "$out/tables/$1" "$2" \
    | tee -a "$out/verify.jsonl" || true
}
verify nested-topic-nested-synth-kauto-r1 nested-synth
verify nested-topic-flat-synth-kauto-r1 flat-synth
verify rewrite-topic-srcoff-k1-r1 srcoff
verify rewrite-topic-srcoff-twin-synth-k1-r1 srcoff-twin-synth
verify flat-base-flat-synth-kauto-r1 flat-synth
uv run --offline --frozen --project "$root/bench" gsbench digest "$out/tables" \
  --tables 'flat-base-flat-synth-kauto-r1,flat-topic-flat-synth-kauto-r1' --out "$out/flat-digests.jsonl" || true

python3 - "$out" <<'PY'
import csv, json, statistics, sys
out = sys.argv[1]
rows = list(csv.DictReader(open(f"{out}/runs.tsv"), delimiter="\t"))
summary = {}
for row in rows:
    key = f"{row['measurement']}/{row['jar']}/{row['index']}/k{row['threads']}"
    entry = summary.setdefault(key, {"wall_s": [], "max_rss_bytes": [], "shards": []})
    entry["wall_s"].append(float(row["wall_s"]))
    entry["max_rss_bytes"].append(int(row["max_rss_bytes"] or 0))
    entry["shards"].append(row["shards"])
for entry in summary.values():
    entry["median_wall_s"] = statistics.median(entry["wall_s"])
    entry["median_max_rss_bytes"] = statistics.median(entry["max_rss_bytes"])
    shard_ms = {}
    for shards in entry["shards"]:
        for shard in shards.split(";"):
            parts = shard.split(":")
            if len(parts) == 4:
                shard_ms.setdefault(parts[0], []).append((int(parts[2]), int(parts[3])))
    entry["median_rows_per_s_by_shard"] = {
        shard: round(statistics.median(rows / ms * 1000 for rows, ms in values if ms), 1) for shard, values in shard_ms.items()}
json.dump(summary, open(f"{out}/summary.json", "w"), indent=1)
for key, entry in summary.items():
    print(key, entry["median_wall_s"], "s", entry["median_max_rss_bytes"] // (1 << 20), "MiB", entry["median_rows_per_s_by_shard"])
PY
echo "results: $out"
