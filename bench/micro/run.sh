#!/usr/bin/env bash
# Single-thread CPU micro benchmark of the stored-source read path on local Lucene segments
# (no S3, no download): production reader vs. sequential merge-instance reader, with and
# without the archive/Delta write path, for LZ4, DEFLATE and two ZSTD stored-fields presets.
#
# usage: micro/run.sh <cli-fat-jar> [work-dir] [docs-per-index] [seconds-per-measurement] [variants]
#   variants: comma list of lz4-80k,deflate-480k,zstd-14k,zstd-240k,smile-lz4,cbor-lz4,yaml-lz4 (default: all;
#   the last three, _source stored as SMILE/CBOR/YAML, only on jars that convert stored formats)
# Segments are built once per work dir and reused, so two jars are measured on identical input.
# With GSBENCH_SLOT set, the run holds the machine-wide bench slot.
set -euo pipefail

if [ $# -lt 1 ]; then
  sed -n '2,10p' "$0"; exit 2
fi
if [ -n "${GSBENCH_SLOT:-}" ] && [ -z "${GSBENCH_MICRO_SLOT_HELD:-}" ]; then
  GSBENCH_MICRO_SLOT_HELD=1 exec "$GSBENCH_SLOT" bench 1 "$0" "$@"
fi

here=$(cd "$(dirname "$0")" && pwd)
jar=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
work=${2:-$here/target/work}
docs=${3:-300000}
seconds=${4:-20}
variants=${5:-}
classes=$here/target/classes-$$

mkdir -p "$work"
trap 'rm -rf "$classes"' EXIT
javac -nowarn -encoding UTF-8 -cp "$jar" -d "$classes" $(find "$here/src" -name '*.java')
cp -R "$here/resources/." "$classes/"
java -Xmx4g --enable-native-access=ALL-UNNAMED -cp "$classes:$jar" \
  de.sloow.gefrierschrank.delta.Bench "$work" "$docs" "$seconds" ${variants:+"$variants"}
