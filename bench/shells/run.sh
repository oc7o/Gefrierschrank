#!/bin/bash
# Timed A/B/C runs and acceptance checks of --mounted-shells on the repository generate.sh builds.
#
#   A = baseline jar, default mode   B = topic jar, default mode
#   C = topic jar, --mounted-shells skip-if-original
#
# usage (inside one bench hold; no container is needed):
#   SLOT=.../slot.sh; $SLOT bench 1 bench/shells/run.sh <baseline-jar> <topic-jar> <ilm-repo> <out-dir> [repetitions]
#   bench/shells/run.sh --retention <topic-jar> <ilm-repo> <ilm-repo-retained> <out-dir>
#
# Measurement: every repetition runs A, B and C at --parallelism 1 and 4 (order rotated per
# repetition), -Xmx4g, local source and target, each into a fresh root. Recorded per run: exit
# code, wall/user/sys/max RSS (/usr/bin/time -l), rows and source bytes (sums of the per-index
# reports' rowsWrittenThisRun/bytesProcessedThisRun), output size (du -sk), C's planning time and
# the load average before and after. results.tsv holds every run, summary.md the medians.
# Acceptance (repetition 1, both parallelisms): A, B, C exit 0; A and B have byte-identical
# manifests, equal per-index reports and stdout once timestamps, elapsed times, paths and the
# report keys bytesFetched/fetch (1.2.0 writes them on every shard) are masked, and equal table
# digests; C writes exactly the originals' tables, no partial-* directory, records every shell as
# ALIAS_OF its original (aliasCount, complete, nothing pending), and its tables' digests equal A's.
# --retention checks the interplay with SLM retention.
# JAVA (default java) selects the JVM. out-dir must not exist yet.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
bench=$(cd "$here/.." && pwd)
JAVA=${JAVA:-java}

die() { echo "$*" >&2; exit 2; }
abspath() { (cd "$(dirname "$1")" && echo "$(pwd)/$(basename "$1")"); }
gsbench() { uv run --offline --frozen --project "$bench" gsbench "$@"; }

# run <jar> <repo> <root> <reports> <log-prefix> [dump-to-delta args...]: exit code in $code.
run() {
  local jar=$1 repo=$2 root=$3 reports=$4 log=$5
  shift 5
  set +e
  /usr/bin/time -l -o "$log.time" "$JAVA" -Xmx4g -jar "$jar" dump-to-delta "$repo" --all-indices \
    -o "$root" --report "$reports" "$@" > "$log.out" 2> "$log.err"
  code=$?
  set -e
}

# Exits 1 with the failed checks listed when any acceptance check fails.
check() {
  python3 -I - "$@" <<'PY'
import json, os, re, subprocess, sys

mode, out = sys.argv[1], sys.argv[2]
failures = []

def expect(condition, message):
    print(("PASS " if condition else "FAIL ") + message)
    if not condition:
        failures.append(message)

def load(path):
    with open(path) as f:
        return json.load(f)

def masked_report(path, root):
    report = load(path)
    for key in ("startedAt", "asOf", "elapsedMillis"):
        report.pop(key, None)
    for shard in report.get("shards", []):
        for key in ("finishedAt", "elapsedMillis", "bytesFetched", "fetch"):
            shard.pop(key, None)
    return json.dumps(report, sort_keys=True).replace(root, "<root>")

def masked_stdout(path, root):
    with open(path) as f:
        lines = f.read().splitlines()
    lines = [l for l in lines if not l.startswith("gefrierschrank ")]  # build line: version and build time
    text = "\n".join(lines).replace(root, "<root>")
    text = re.sub(r"temp-disk budget \d+ MiB", "temp-disk budget <free> MiB", text)  # 80% of the free space at start
    return re.sub(r" in \d+ms$", " in <t>ms", text, flags=re.M)

def digests(path):
    with open(path) as f:
        return {row["table"]: row for row in map(json.loads, f)}

if mode == "abc":
    parallelism, runs = sys.argv[3], sys.argv[4:]
    a, b, c = runs
    for name, run in (("A", a), ("B", b), ("C", c)):
        expect(open(run + "/exit").read().strip() == "0", f"{name} exits 0 ({run})")
    with open(a + "/reports/manifest.json", "rb") as fa, open(b + "/reports/manifest.json", "rb") as fb:
        expect(fa.read() == fb.read(), "A and B manifests are byte-identical")
    manifest_a = load(a + "/reports/manifest.json")
    names = [e["indexName"] for e in manifest_a["indices"]]
    for name in names:
        expect(masked_report(f"{a}/reports/{name}.json", a + "/root") == masked_report(f"{b}/reports/{name}.json", b + "/root"),
               f"A and B report of {name} are equal (masked)")
    stdout_a, stdout_b = masked_stdout(a + "/run.out", a + "/root"), masked_stdout(b + "/run.out", b + "/root")
    if parallelism == "1":
        expect(stdout_a == stdout_b, "A and B stdout are equal (masked)")
    else:
        # Concurrent indices print their "Exporting" lines in whatever order they start.
        expect(sorted(stdout_a.splitlines()) == sorted(stdout_b.splitlines()), "A and B stdout hold the same lines (masked)")
    da, db, dc = (digests(r + "/digests.jsonl") for r in runs)
    expect(da == db, f"A and B table digests are equal ({len(da)} tables)")
    originals = sorted(n for n in names if not n.startswith("partial-"))
    shells = sorted(n for n in names if n.startswith("partial-"))
    expect(sorted(os.listdir(c + "/root")) == originals, f"C writes exactly the {len(originals)} originals' tables")
    expect(not any(n.startswith("partial-") for n in os.listdir(c + "/root")), "C writes no partial-* directory")
    for table in dc:
        expect(dc[table] == da.get(table), f"C digest of {table} equals A's")
    expect(sorted({t.split("/")[0] for t in dc}) == originals, "C has a digest for every original and nothing else")
    manifest_c = load(c + "/reports/manifest.json")
    entries = {e["indexName"]: e for e in manifest_c["indices"]}
    expect([e["indexName"] for e in manifest_c["indices"]] == names, f"C's manifest lists all {len(names)} names in listing order")
    expect(all(entries[s]["outcome"] == "ALIAS_OF" and entries[s].get("aliasOf") == s[len("partial-"):] for s in shells),
           f"C records all {len(shells)} shells as ALIAS_OF their original")
    expect(all(entries[o]["outcome"] == "COMPLETED" for o in originals), "C completes every original")
    expect(manifest_c.get("aliasCount") == len(shells), f"C aliasCount is {len(shells)}")
    expect(manifest_c["complete"] is True and manifest_c["pendingIndices"] == [], "C is complete with nothing pending")
    rows_a = {n: load(f"{a}/reports/{n}.json")["rowsWrittenThisRun"] for n in names}
    rows_c = sum(load(f"{c}/reports/{n}.json")["rowsWrittenThisRun"] for n in originals)
    expect(rows_c == sum(rows_a[o] for o in originals), f"C writes the originals' rows of A ({rows_c} of {sum(rows_a.values())})")
    expect(all(rows_a[s] == rows_a[s[len("partial-"):]] for s in shells), "A wrote every shell's rows twice, once per name")
    expect(sum(v["rows"] for v in dc.values() if "_export_metadata" not in v["table"]) == rows_c,
           f"C's tables hold {rows_c} rows")
elif mode == "retention":
    first, second, fresh, over = sys.argv[3:]
    for name, run in (("default run on ilm-repo", first), ("default run on ilm-repo-retained over it", second),
                      ("alias run on ilm-repo-retained, fresh root", fresh), ("alias run on ilm-repo-retained over the default root", over)):
        expect(open(run + "/exit").read().strip() == "0", f"{name} exits 0")
    m = load(second + "/reports/manifest.json")
    shells = [e["indexName"] for e in m["indices"] if e["indexName"].startswith("partial-")]
    expect(m["complete"] and m["failedCount"] == 0 and m["behindLatestCount"] == len(shells),
           f"the default rerun after retention is complete with behindLatestCount {len(shells)}")
    m = load(fresh + "/reports/manifest.json")
    expect(m.get("aliasCount") == len(shells) and m["completedCount"] == len(m["indices"]) - len(shells) and m["complete"],
           f"the alias run on a fresh root has {len(shells)} ALIAS_OF entries and the originals completed")
    expect(sorted(os.listdir(fresh + "/root")) == sorted(e["indexName"] for e in m["indices"] if e["outcome"] == "COMPLETED"),
           "the alias run on a fresh root writes only the originals' tables")
    m = load(over + "/reports/manifest.json")
    expect(m.get("aliasCount") == 0 and m["completedCount"] == len(m["indices"]) and m["complete"],
           "the alias run over the default root completes every name, no alias")
    skipped = all(r["skippedCount"] == r["totalShards"] for r in
                  (load(f"{over}/reports/{e['indexName']}.json") for e in m["indices"]))
    expect(skipped, "every shard of that run is skipped as already committed")
    with open(over + "/run.out") as f:
        expect(f.read().count("already has a table; exporting it") == len(shells), "it names every shell's existing table")
elif mode == "summary":
    import statistics
    rows = []
    with open(out + "/results.tsv") as f:
        header = f.readline().rstrip("\n").split("\t")
        for line in f:
            rows.append(dict(zip(header, line.rstrip("\n").split("\t"))))
    lines = ["| config | --parallelism | runs | wall s | user+sys s | max RSS MiB | rows | source bytes | output MiB | planning ms |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    medians = {}
    for config in ("A", "B", "C"):
        for p in ("1", "4"):
            sel = [r for r in rows if r["config"] == config and r["parallelism"] == p]
            if not sel:
                continue
            med = lambda key: statistics.median(float(r[key]) for r in sel)
            medians[(config, p)] = med("wall")
            planning = [float(r["planning_ms"]) for r in sel if r["planning_ms"] != "-"]
            lines.append(f"| {config} | {p} | {len(sel)} | {med('wall'):.2f} | {med('cpu'):.2f} | {med('max_rss_kb') / 1024:.0f} "
                         f"| {int(med('rows')):,} | {int(med('source_bytes')):,} | {med('output_kb') / 1024:.0f} "
                         f"| {statistics.median(planning) if planning else '-'} |")
    lines.append("")
    for p in ("1", "4"):
        if ("B", p) in medians and ("C", p) in medians:
            lines.append(f"- p{p}: C/B wall {medians[('C', p)] / medians[('B', p)]:.2f}x, "
                         f"B/A wall {medians[('B', p)] / medians[('A', p)]:.2f}x")
    with open(out + "/summary.md", "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))
sys.exit(1 if failures else 0)
PY
}

# Facts of one finished run, appended to results.tsv.
record() {
  local config=$1 p=$2 rep=$3 dir=$4 load_before=$5 load_after=$6
  python3 -I - "$config" "$p" "$rep" "$dir" "$load_before" "$load_after" "$out/results.tsv" <<'PY'
import json, os, re, subprocess, sys
config, p, rep, d, before, after, results = sys.argv[1:]
time = open(d + "/run.time").read()
wall, user, system = (float(x) for x in re.search(r"([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys", time).groups())
rss = int(re.search(r"(\d+)\s+maximum resident set size", time).group(1)) // 1024
manifest = json.load(open(d + "/reports/manifest.json"))
rows = source = 0
for entry in manifest["indices"]:
    if entry.get("reportFile"):
        report = json.load(open(d + "/reports/" + entry["reportFile"]))
        rows += report["rowsWrittenThisRun"]
        source += report["bytesProcessedThisRun"]
du = int(subprocess.run(["du", "-sk", d + "/root"], capture_output=True, text=True).stdout.split()[0]) if os.path.isdir(d + "/root") else 0
planning = re.search(r"\(planned in (\d+) ms\)", open(d + "/run.out").read())
fields = [config, p, rep, open(d + "/exit").read().strip(), f"{wall:.2f}", f"{user + system:.2f}", str(rss), str(rows),
          str(source), str(du), planning.group(1) if planning else "-", before, after]
with open(results, "a") as f:
    f.write("\t".join(fields) + "\n")
print("\t".join(fields))
PY
}

loadavg() { sysctl -n vm.loadavg | awk '{print $2}'; }

if [ "${1:-}" = "--retention" ]; then
  [ $# -eq 5 ] || die "usage: $0 --retention <topic-jar> <ilm-repo> <ilm-repo-retained> <out-dir>"
  jar=$(abspath "$2"); repo=$(abspath "$3"); retained=$(abspath "$4"); out=$5
  [ -e "$out" ] && die "$out exists; use a new output directory"
  mkdir -p "$out"; out=$(cd "$out" && pwd)
  echo "load at start: $(uptime)"
  shared=$out/default-root
  for step in first second fresh over; do mkdir -p "$out/$step"; done
  run "$jar" "$repo" "$shared" "$out/first/reports" "$out/first/run"; echo $code > "$out/first/exit"
  run "$jar" "$retained" "$shared" "$out/second/reports" "$out/second/run" --parallelism 4; echo $code > "$out/second/exit"
  run "$jar" "$retained" "$out/fresh/root" "$out/fresh/reports" "$out/fresh/run" --mounted-shells skip-if-original
  echo $code > "$out/fresh/exit"
  run "$jar" "$retained" "$shared" "$out/over/reports" "$out/over/run" --mounted-shells skip-if-original
  echo $code > "$out/over/exit"
  echo "load at end: $(uptime)"
  check retention "$out" "$out/first" "$out/second" "$out/fresh" "$out/over" | tee "$out/acceptance.txt"
  exit "${PIPESTATUS[0]}"
fi

[ $# -ge 4 ] || die "usage: $0 <baseline-jar> <topic-jar> <ilm-repo> <out-dir> [repetitions]"
base=$(abspath "$1"); topic=$(abspath "$2"); repo=$(abspath "$3"); out=$4; reps=${5:-5}
[ -f "$base" ] && [ -f "$topic" ] && [ -d "$repo" ] || die "missing jar or repository"
[ -e "$out" ] && die "$out exists; use a new output directory"
mkdir -p "$out"; out=$(cd "$out" && pwd)
printf 'config\tparallelism\trep\texit\twall\tcpu\tmax_rss_kb\trows\tsource_bytes\toutput_kb\tplanning_ms\tload_before\tload_after\n' \
  > "$out/results.tsv"
{
  echo "baseline: $base ($(shasum -a 256 "$base" | cut -c1-16))"
  echo "topic:    $topic"
  echo "repo:     $repo"
  echo "java:     $("$JAVA" -version 2>&1 | head -1)"
  echo "load at start: $(uptime)"
} | tee "$out/environment.txt"

status=0
for rep in $(seq 1 "$reps"); do
  case $((rep % 3)) in 1) order="A B C" ;; 2) order="B C A" ;; 0) order="C A B" ;; esac
  for p in 1 4; do
    for config in $order; do
      dir=$out/runs/$config-p$p-r$rep
      mkdir -p "$dir"
      args=(--parallelism "$p")
      jar=$topic
      [ "$config" = A ] && jar=$base
      [ "$config" = C ] && args+=(--mounted-shells skip-if-original)
      before=$(loadavg)
      run "$jar" "$repo" "$dir/root" "$dir/reports" "$dir/run" "${args[@]}"
      echo $code > "$dir/exit"
      record "$config" "$p" "$rep" "$dir" "$before" "$(loadavg)"
    done
    if [ "$rep" = 1 ]; then
      for config in A B C; do
        gsbench digest "$out/runs/$config-p$p-r1/root" --out "$out/runs/$config-p$p-r1/digests.jsonl" > /dev/null
      done
      echo "--- acceptance, --parallelism $p ---" | tee -a "$out/acceptance.txt"
      check abc "$out" "$p" "$out/runs/A-p$p-r1" "$out/runs/B-p$p-r1" "$out/runs/C-p$p-r1" | tee -a "$out/acceptance.txt" \
        || status=1
      gsbench compare-digests "$out/runs/A-p$p-r1/digests.jsonl" "$out/runs/B-p$p-r1/digests.jsonl" --strict \
        | tee -a "$out/acceptance.txt" || status=1
    else
      for config in A B C; do rm -rf "$out/runs/$config-p$p-r$rep/root"; done
    fi
  done
done
echo "load at end: $(uptime)" | tee -a "$out/environment.txt"
check summary "$out"
exit $status
