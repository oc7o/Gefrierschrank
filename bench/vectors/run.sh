#!/usr/bin/env bash
# Throughput and heap of vector rehydration, like for like: the baseline jar (1.1.0) exports the
# <name>-included twin (vectors in the stored _source), the topic jar the excluded <name> (vectors
# put back), same documents, one after the other, per --shard-threads value.
#
# usage: bench/vectors/run.sh throughput <baseline-jar> <topic-jar> <data-dir> <out-dir> [repeats] [datasets] [threads]
#        bench/vectors/run.sh heap <baseline-jar> <topic-jar> <data-dir> <out-dir> [dataset] [seconds-into-run]
#   data-dir: generate.sh's output (repo/ and manifest.json); datasets: comma list (default: all four);
#   threads: comma list of --shard-threads values (default 1,auto). Every run uses -Xmx4g, one shard
#   at a time (--parallelism 1) and a fresh table. throughput writes runs.tsv (one line per run:
#   dataset jar threads docs wall_s cpu_s max_rss_bytes table_bytes) and summary.md (medians);
#   heap writes heap.tsv (largest live heap of GC.class_histogram samples, one per second from
#   [seconds-into-run] on, at --shard-threads 1 and 4) and
#   one JFR recording per run with old-object samples.
# GSBENCH_VECTORS_JVM_ARGS adds JVM options to every export, e.g.
# -Dgefrierschrank.shard.threads.minRangeLiveDocs=1000 to split these 100k-document shards into
# ranges (auto keeps a shard below 250k live documents in one range).
# With GSBENCH_SLOT set, the whole run holds the machine-wide bench slot; it needs no container.
set -euo pipefail

if [ $# -lt 5 ]; then
  sed -n '2,18p' "$0"; exit 2
fi
if [ -n "${GSBENCH_SLOT:-}" ] && [ -z "${GSBENCH_VECTORS_SLOT_HELD:-}" ]; then
  GSBENCH_VECTORS_SLOT_HELD=1 exec "$GSBENCH_SLOT" bench 1 "$0" "$@"
fi

mode=$1; base=$(cd "$(dirname "$2")" && pwd)/$(basename "$2"); topic=$(cd "$(dirname "$3")" && pwd)/$(basename "$3")
data=$(cd "$4" && pwd); mkdir -p "$5"; out=$(cd "$5" && pwd)
repo=$data/repo
work=$out/work

table_bytes() {
  find "$1" -name '*.parquet' -not -path '*/_export_metadata/*' -print0 | xargs -0 stat -f %z | awk '{s+=$1} END {print s+0}'
}

throughput() {
  local repeats=${1:-3} datasets=${2:-vec384,vec384-bbq,vec-nested,vec-sparse} threads=${3:-1,auto}
  local tsv=$out/runs.tsv
  printf 'dataset\tjar\tthreads\tdocs\twall_s\tcpu_s\tmax_rss_bytes\ttable_bytes\n' > "$tsv"
  echo "load at start: $(uptime)" > "$out/load.txt"
  for dataset in ${datasets//,/ }; do
    for k in ${threads//,/ }; do
      for i in $(seq 1 "$repeats"); do
        for side in base topic; do
          if [ $side = base ]; then jar=$base; index=$dataset-included; else jar=$topic; index=$dataset; fi
          table=$work/$side-$dataset-$k; report=$work/$side-$dataset-$k.json; timing=$work/time.txt
          mkdir -p "$work"
          start=$(python3 -c 'import time; print(time.monotonic())')
          rm -rf "$table"
          /usr/bin/time -l -o "$timing" java -Xmx4g ${GSBENCH_VECTORS_JVM_ARGS:-} -jar "$jar" dump-to-delta "$repo" --index "$index" -o "$table" \
            --shard-threads "$k" --parallelism 1 --report "$report" >/dev/null 2>"$work/stderr.txt"
          end=$(python3 -c 'import time; print(time.monotonic())')
          python3 - "$tsv" "$dataset" "$side" "$k" "$report" "$timing" "$start" "$end" "$(table_bytes "$table")" <<'PY'
import json, re, sys
tsv, dataset, side, k, report, timing, start, end, table = sys.argv[1:]
r = json.load(open(report))
if r.get("committedCount", 0) < 1 or r.get("quarantinedCount", 0) or r["committedCount"] != len(r.get("shards", [])):
    sys.exit(f"{side} {dataset}: not every shard committed")
t = open(timing).read()
user = float(re.search(r"([\d.]+) user", t).group(1)); sys_ = float(re.search(r"([\d.]+) sys", t).group(1))
rss = int(re.search(r"(\d+)\s+maximum resident set size", t).group(1))
docs = r["rowsWrittenThisRun"]
with open(tsv, "a") as f:
    f.write(f"{dataset}\t{side}\t{k}\t{docs}\t{float(end) - float(start):.2f}\t{user + sys_:.2f}\t{rss}\t{table}\n")
PY
        done
      done
    done
  done
  echo "load at end: $(uptime)" >> "$out/load.txt"
  rm -rf "$work"
  python3 - "$tsv" "$out/summary.md" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1]), delimiter="\t"))
groups = {}
for r in rows:
    groups.setdefault((r["dataset"], r["threads"], r["jar"]), []).append(r)
lines = ["| dataset | threads | jar | docs | docs/s | output MB/s | CPU s | max RSS MiB | runs |",
         "|---|---|---|---|---|---|---|---|---|"]
for (dataset, k, jar), rs in groups.items():
    wall = statistics.median(float(r["wall_s"]) for r in rs)
    docs = int(rs[0]["docs"])
    mb = statistics.median(int(r["table_bytes"]) for r in rs) / 1e6
    cpu = statistics.median(float(r["cpu_s"]) for r in rs)
    rss = statistics.median(int(r["max_rss_bytes"]) for r in rs) / 2**20
    lines.append(f"| {dataset} | {k} | {jar} | {docs} | {docs / wall:,.0f} | {mb / wall:.1f} | {cpu:.1f} | {rss:,.0f} | {len(rs)} |")
open(sys.argv[2], "w").write("\n".join(lines) + "\n")
print("\n".join(lines))
PY
}

heap() {
  local dataset=${1:-vec384} after=${2:-20}
  local tsv=$out/heap.tsv
  printf 'dataset\tjar\tthreads\tlive_heap_bytes\texit\n' > "$tsv"
  mkdir -p "$work"
  for k in 1 4; do
    for side in base topic; do
      if [ $side = base ]; then jar=$base; index=$dataset-included; else jar=$topic; index=$dataset; fi
      rm -rf "$work/$side-$k"
      java -Xmx4g ${GSBENCH_VECTORS_JVM_ARGS:-} -XX:StartFlightRecording=settings=profile,filename="$out/$side-$k.jfr" -jar "$jar" dump-to-delta "$repo" \
        --index "$index" -o "$work/$side-$k" --shard-threads "$k" --parallelism 1 >"$out/$side-$k.out" 2>"$out/$side-$k.err" &
      pid=$!
      sleep "$after"
      # The largest live heap of samples one second apart while the export runs (each forces a full GC).
      live=0
      while kill -0 "$pid" 2>/dev/null; do
        sample=$(jcmd "$pid" GC.class_histogram 2>/dev/null | awk '/^Total/ {print $3}') || sample=""
        [ -n "$sample" ] && [ "$sample" -gt "$live" ] && live=$sample
        sleep 1
      done
      status=0; wait "$pid" || status=$?
      printf '%s\t%s\t%s\t%s\t%s\n' "$dataset" "$side" "$k" "$live" "$status" >> "$tsv"
    done
  done
  rm -rf "$work"
  cat "$tsv"
}

case $mode in
  throughput) shift 5; throughput "$@" ;;
  heap) shift 5; heap "$@" ;;
  *) echo "unknown mode $mode" >&2; exit 2 ;;
esac
