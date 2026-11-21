#!/usr/bin/env bash
# Exports every committed fixture repository with one CLI jar (dump-to-delta --all-indices) and digests
# every resulting Delta table, so two jars (or one jar with and without a flag) can be compared with
# `gsbench compare-digests <out-a> <out-b>`.
#
# usage: bench/fixture-digests.sh <cli-jar> <out-dir> [-- extra dump-to-delta args]
#   JAVA (default java) and JAVA_OPTS select and tune the JVM.
# out-dir must not exist yet: exporting into existing tables would skip committed shards.
# Writes <out-dir>/tables/<fixture>/<index>, reports/<fixture>/, logs/<fixture>.log,
# exit-codes.tsv (fixture, dump-to-delta exit code), digests.jsonl and fetch-modes.jsonl (each run
# report's shard fetch modes and byte sums, then the total over every report at any depth).
# A fixture whose export fails is recorded with its exit code and digested as far as it got; the
# script itself fails only when the harness does.
set -euo pipefail

if [ $# -lt 2 ]; then
  echo "usage: $0 <cli-jar> <out-dir> [-- extra dump-to-delta args]" >&2
  exit 2
fi
jar=$1
out=$2
shift 2
if [ $# -gt 0 ]; then
  if [ "$1" != "--" ]; then
    echo "extra dump-to-delta args go after --" >&2
    exit 2
  fi
  shift
fi
[ -f "$jar" ] || { echo "no such jar: $jar" >&2; exit 2; }
[ -e "$out" ] && { echo "$out exists; use a new output directory" >&2; exit 2; }

root=$(cd "$(dirname "$0")/.." && pwd)
jar=$(cd "$(dirname "$jar")" && pwd)/$(basename "$jar")
mkdir -p "$out/tables" "$out/reports" "$out/logs"
out=$(cd "$out" && pwd)

# A repository root is a directory holding index.latest; only committed fixtures count, so a
# half-generated fixture in a worktree never changes what two runs compare.
repos=$(git -C "$root" ls-files -- '*/src/test/resources/fixtures/*index.latest' | sed 's|/index.latest$||' | sort)
[ -n "$repos" ] || { echo "no committed fixture repositories under $root" >&2; exit 1; }

printf 'fixture\texit\n' > "$out/exit-codes.tsv"
while IFS= read -r repo; do
  name=$(printf '%s' "$repo" | sed -e 's|^gefrierschrank-||' -e 's|/src/test/resources/fixtures/|/|')
  mkdir -p "$out/tables/$(dirname "$name")" "$out/reports/$(dirname "$name")" "$out/logs/$(dirname "$name")"
  set +e
  # shellcheck disable=SC2086 # JAVA_OPTS is a list of JVM options
  "${JAVA:-java}" ${JAVA_OPTS:-} -jar "$jar" dump-to-delta "$root/$repo" --all-indices \
    -o "$out/tables/$name" --report "$out/reports/$name" "$@" > "$out/logs/$name.log" 2>&1
  code=$?
  set -e
  printf '%s\t%s\n' "$name" "$code" >> "$out/exit-codes.tsv"
  echo "$name: exit $code"
done <<< "$repos"

uv run --offline --frozen --project "$root/bench" gsbench digest "$out/tables" --out "$out/digests.jsonl"
uv run --offline --frozen --project "$root/bench" gsbench fetch-modes "$out/reports" --per-report > "$out/fetch-modes.jsonl"
