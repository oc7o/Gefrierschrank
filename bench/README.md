# gsbench: end-to-end benchmark and correctness gate

`gsbench` measures a Gefrierschrank CLI jar against the tuned alternative on the same
Elasticsearch snapshot data, and reports numbers only for indices whose export passes a
per-document correctness gate.

| route | what runs |
|---|---|
| **gs** | `java -jar <jar> dump-to-delta s3://gs-bench-snapshots/<base_path> --all-indices -o <dir> --report <dir>` against a dedicated MinIO, once per snapshot (ES 9, 8, 7) |
| **es** (baseline) | restore the snapshot on an ES node (8.x/9.x snapshots on 9.3, 7.x on 8.17; recovery throttles lifted, 0 replicas, refresh off), one PIT per index with sliced `search_after` (slices = max(shards, cores), sort `_shard_doc`, size 10000, `track_total_hits: false`, one process per slice, gzip) to NDJSON, then delta-rs `write_deltalake` (ZSTD, 256 MiB target files). Restore, export and transform are timed separately |

Both routes produce a Delta table per index with `_id`, `_routing` and `_source` as a string
column, and both go through the same gate.

## Requirements

- Docker with the cached images `gefrierschrank-es7`, `gefrierschrank-es8`, `gefrierschrank-es9`
  (7.17.9 / 8.17.0 / 9.3.0 with repository-s3, built from the repository root with
  `docker compose --profile esN build`) and `minio/minio`. Nothing is pulled.
- [uv](https://docs.astral.sh/uv/). The first `uv sync` needs network once (Python 3.12,
  deltalake, pyarrow, psutil, requests, pyyaml; versions pinned in `uv.lock`).
- A JDK (`java` on the PATH, or `GSBENCH_JAVA_HOME`) and a built CLI fat jar.

Everything the harness creates is prefixed `gs-bench-` and uses host ports 9100-9119 only:
MinIO on 9100/9101 (`MINIO_PROMETHEUS_AUTH_TYPE=public`, own volume), ES 7/8/9 on 9110/9111/9112,
at most one ES node at a time with a 2 GB heap (`GSBENCH_ES_HEAP` to change). The shared test
MinIO on :9000 is never touched.

## Usage

```sh
# benchmark a jar (datasets are built on first use and cached)
uv run --project bench gsbench run --label main --scale smoke \
  --jar gefrierschrank-cli/target/gefrierschrank-cli-1.2.0.jar

# compare two runs: ratios B/A; exit 1 on a gate failure in B that expected-failures.yaml does not list,
# an invariant violation in B, or a document-table schema that differs from A's
uv run --project bench gsbench compare bench/results/<main-run>/result.json bench/results/<new-run>/result.json

uv run --project bench gsbench prepare --scale smoke   # only build/refresh the datasets
uv run --project bench gsbench gate --self-test        # the gate must catch every kind of corruption it targets
uv run --project bench gsbench down                    # remove containers, volumes, buckets and the dataset cache
```

Builds up to 1.1.0 ship a Lucene 9 worker jar (removed in 1.2.0). Without `--worker-jar`, the
worker jar beside the CLI jar or in the same Maven reactor build (`gefrierschrank-lucene9/target/`)
is used if present; `--no-worker-jar` runs without one (1.0.0 then quarantines every shard, which
`expected-failures.yaml` lists).

`run` options: `--repeat N` (default 3; medians and spread are reported), `--routes gs|es|gs,es`,
`--suite default|frozen` (see [Datasets](#datasets)), `--majors 9,8,7` (default: every major of the suite), `--jvm-args '...'`, `--gs-args '...'` (flags that older jars lack, e.g.
`--parallelism 4`, are only ever passed here, so any jar works), `--target s3a` (write the Delta
output to the bench MinIO to include uploads), `--slices N`, `--keep-output`, `--rebuild`.
A value that starts with a dash and has no space, such as `--parallelism=4`, must be attached with
`=` (`--gs-args=--parallelism=4`), or argparse takes it for an option of its own; with a space
(`--gs-args '--parallelism 4'`) either form works. The Gefrierschrank JVM gets no `-Xmx` unless
`--jvm-args` sets one, so its peak RSS follows the JVM's default heap sizing and varies by
±20-40% between repeats; pin a heap (e.g. `--jvm-args=-Xmx4g`) when RSS is the comparison.

Label convention: run the base build with `--label main`. Builds before the 1.1.0 version bump
still reported version 1.0.0, so the 1.0.0 entries in `expected-failures.yaml` also key on the label.

Results go to `bench/results/<timestamp>-<label>/` (`result.json`, `result.md`, per-run logs and
the exact command lines; gitignored; a run that fails midway leaves `result.partial.json` with
what it measured). The jars are copied into the run directory for the run and removed after it.
`bench/reference/main-1.0.0-smoke.json` is the 1.0.0 smoke reference run (3 repeats), usable as
`A` in `gsbench compare`; running the base jar back to back with the new one is more precise on a
shared machine. `GSBENCH_CACHE` and `GSBENCH_RESULTS` relocate the dataset cache and the results, e.g. to reuse
the datasets another worktree already built (the snapshots themselves live in the bench MinIO).

### Shared machines

Set `GSBENCH_SLOT=/path/to/slot.sh` (a `slot.sh <pool> <n> <cmd...>` semaphore) and the harness
holds the `docker` slot for every container action and while an ES node runs, and the `bench` slot
for the whole of `run` (dataset build and both routes), `prepare` and `down`, so no other gsbench
instance on the shared bench MinIO can rebuild or remove snapshots, or add S3 traffic, while a run
measures (`bench` is always taken before `docker`). Do not additionally wrap the whole command in
the same pools; a caller that already holds a pool sets `GSBENCH_HELD_SLOTS=bench` (comma list).
Without `GSBENCH_SLOT` no locking happens.

## Datasets

Seeded and deterministic (`--seed`, default 42): the same seed produces the same documents,
ids and oracle hashes. `--scale smoke|standard|large` is about 255k / 2.5M / 10M documents in
total (`tiny` exists only to exercise the harness). Documents are ECS-like, about 1.1 KB, with
keyword, `match_only_text`, text+keyword multi-fields, ip, longs beyond 2^53, decimals with more
digits than a double holds, unicode/emoji, escaped quotes, custom `_routing` on a subset, and
numeric, base64 and UTF-8 `_id`s. About 2% of sources are pretty-printed or oddly spaced and 2%
ASCII-escaped: sources are sent to ES verbatim (multi-line ones through the single-document
API), so the sha256 of the exact generated bytes is the byte-exact truth.

| snapshot | index | shape |
|---|---|---|
| ES 9.3 | `logs-lz4` | default codec (LZ4), 2 shards, force-merged |
| | `logs-zstd` | `best_compression` (ZSTD), multi-segment, 5% updates, 3% deletes, soft deletes retained |
| | `docs-nested` | nested field, custom routing on every doc, deletes |
| | `logs-logsdb` | `index.mode: logsdb` (synthetic source, trial license), wildcard `url.path`, keyword over `ignore_above` |
| | `tsdb-hist` | `time_series` with a histogram metric, ES-generated `_id` |
| | `vec-default` | `dense_vector` created on 9.3, so vectors are excluded from the stored `_source` |
| ES 8.17 | `logs-zstd8` | `best_compression` (ZSTD), 2 shards, force-merged |
| | `logs8-lz4` | default codec, multi-segment, updates + deletes (not `logs-lz4-8`: `logs-*-*` names are claimed by the built-in data stream template) |
| | `synth8` | synthetic source |
| ES 7.17 | `logs-default7` | default codec, 2 shards |
| | `logs-deflate7` | `best_compression` (DEFLATE), nested docs, updates + deletes |

`--suite frozen` builds a repository shaped like an ILM frozen tier instead: every generation is
one shard force-merged to one segment, and nothing is updated or deleted.

| snapshot | indices | shape |
|---|---|---|
| ES 9.3 | `ilm-big-000001` | one very large generation, `best_compression` (ZSTD): 3M documents at `--scale standard` |
| | `ilm-logsdb-000001` .. `000008` | logsdb (synthetic source), 80,000 documents each at standard |
| ES 8.17 | `ilm-app8-000001` .. `000020` | `best_compression` (ZSTD), 20,000 to 60,000 documents each at standard (785,000 in total) |

At `--scale standard` that is 4.4M documents (about 5 GB of source JSON). The snapshots take about
2.5 GB in the bench MinIO; a 1.0.0 run writes about 4 GB of uncompressed Parquet, and the ES
route's NDJSON of the large index alone is about 3.5 GB, so check the free disk first.

Before each snapshot the build captures the ES oracle per index through a PIT: live root
count, the `(_id, _routing)` set, per-document raw and canonical `_source` hashes, and
`_disk_usage`. The build refuses a dataset whose oracle disagrees with the generator (count,
ids, routing, stored bytes), whose synthetic index still stores `_source`, or whose vector
index does not exclude vectors, and one whose updates and deletes left no deleted document in the
snapshot's segments (the resurrection checks would then test nothing). Each major is snapshotted
to its own `base_path`; datasets are cached under `bench/cache/` by a hash of the dataset spec,
the ES version and the code that decides documents, snapshot and oracle hashes (`generate`,
`datasets`, `snapshot`, `pit`, `canonical`), and reused while the snapshot still exists in MinIO.

## Gate

Per index, against the oracle:

- row count equals the live root count;
- `(_id, _routing)` set equality (missing, extra, resurrected deleted ids);
- no duplicate `_id`;
- stored source: sha256 of the exported `_source` UTF-8 bytes equals the generator's raw hash;
- every index: canonical hash (sorted keys, compact, numbers compared as exact decimals, never
  floats) equals ES's, which covers synthetic source and re-inserted vectors;
- `_source_kind` as expected (`stored` / `synthetic`) where the table has the column.

The baseline output goes through the same gate. `expected-failures.yaml` maps
(label and/or BuildInfo version, dataset) to a finding id and an expected outcome: `xfail`
tolerates a failure (only of the checks listed in `tolerates`, if given), `loud-failure` requires
the index to end FAILED or QUARANTINED with every committed row correct: only missing rows are
excused, so a COMMITTED export with stripped content, or a partly quarantined index whose
committed shards carry wrong, duplicated or extra rows, is a gate failure. Every matching entry
is tried in file order. Statuses: `pass`, `fail`, `xfail`, `loud-failure`, `xpass` (an expected
failure that no longer happens: narrow the entry).

`gsbench gate --self-test` corrupts copies of a table by dropping a row, flipping a byte,
re-spacing a source (stored source only), duplicating a row and resurrecting a deleted id, and
fails unless each copy fails exactly the checks aimed at it (the re-spaced one only `raw_hash`).
The table is generated, or with `--table <delta table> --index <name>` a copy of an exported table
with all its columns, gated against that index's oracle.

Besides the gate, every run checks three product invariants and exits 2 when one fails: the
Gefrierschrank route sends no write request (and no bytes) to the snapshot bucket, no Parquet
file in an output table lacks an AddFile, and no report or manifest contains a document `_id`
(non-numeric ones) or a document-only value of the generator (such as `shop.example.com`).
`result.json` records each table's column schema; `compare` fails when B's differs from A's.

## Metrics

Per route and phase: wall time; CPU user+sys (`wait4` rusage of the process tree for the jar,
the exporter and the transform, cgroup `usage_usec` for the ES container); peak RSS (200 ms
process-tree sampling plus `ru_maxrss`; `docker stats` for ES); peak disk (tmpdir plus output
for Gefrierschrank, sampled every 200 ms; ES data volume plus NDJSON plus Delta for the
baseline); MinIO per-bucket requests per API and bytes; output bytes, Parquet data-file count
and AddFile count; rows, exit code, quarantined shards and the jar's BuildInfo line. The MinIO
image id and version go into `result.json`.

The baseline exporter runs one process per slice: in one process, response parsing and NDJSON
encoding hold the GIL and the client, not ES, bounded the export. Workers are spawned and warmed
up (imports, HTTP connection) before the PIT opens. Export wall covers PIT open to close, export
CPU is the slice workers' CPU in that window plus the ES cgroup CPU over the exporter's run (ES
idles while workers start); the workers' start-up is reported beside them. The
busiest slice's CPU seconds per wall second are reported per index; above 0.8 the report flags
the export as client-bound. The ES node stays up across the repeats of a snapshot, so from the
second repeat on its JIT is warm, while every Gefrierschrank run is a fresh JVM; node start-up
is reported but not in the totals. Both favour the baseline at small scales.

Route totals include indices that fail the gate (1.0.0 quarantines the logsdb and TSDB indices
in seconds), so `result.md` and `compare` also give the ratio over only the indices both routes
(or both builds) export correctly.

Caveats: MinIO refreshes its bucket metrics at most every 10 s, so the harness waits 11 s
before and after each measured S3 phase. Before measuring, a run reads one object of the snapshot
bucket and fails unless the GET and sent-bytes series show it (metric names change between MinIO
versions); every Gefrierschrank run and restore must show GET traffic as well. Sampling can miss
RSS and disk peaks shorter than 200 ms.
Other processes on the machine add noise: use repeats, read the spread, and compare runs taken
under similar load (the load average is recorded).

## Micro benchmark

`bench/micro/run.sh <cli-fat-jar> [work-dir] [docs] [seconds] [variants]` compiles the
single-thread read-path benchmark against any CLI jar and runs it on local Lucene segments
(LZ4, DEFLATE, ZSTD 14 KB and ZSTD 240 KB stored fields): production reader vs. sequential
merge-instance reader, with and without the archive/Delta write path. Segments are built once
per work dir, so two jars read identical input. If a jar's `ArchiveContext` API differs, the
read measurements still run and the end-to-end ones are skipped with the reason.

## Feature harnesses

Each 1.2.0 feature has its own harness beside gsbench, with its own datasets and README:

- [bench/shells](shells/README.md): `--mounted-shells skip-if-original` on a generated 16-index
  ILM repository (8 originals, 8 mounted shells): the baseline jar (A), the jar under test by
  default (B) and with the option (C), compared by manifests, masked reports and table digests,
  plus checks against SLM retention.
- [bench/vectors](vectors/README.md): throughput and heap of vector rehydration, like for like
  against the 1.1.0 jar exporting the `-included` twins. Correctness of rehydrated rows is
  `gsbench shadow`'s job: it restores a snapshot read-only on an ES node of the bench and compares
  the Delta tables with its fetch (`exclude_vectors: false`), counts only, exit 1 on any mismatch.
- [bench/synthcov](synthcov/README.md): cost of nested and source-disabled reconstruction, and
  that neither changes the plain synthetic path.
- [bench/synthfetch](synthfetch/README.md): `--selective-fetch` on non-compound ES 9.3 logsdb,
  TSDB and nested indices and their compound twins, with its own Elasticsearch and MinIO
  containers (ports 9252-9254) and gsbench's measurement code.

## Table digests

`gsbench digest` and `gsbench compare-digests` (`gsbench/tabledigest.py`) compare two exports
table by table without any oracle, e.g. two jars, or one jar with and without a flag:

```sh
# one JSON line per Delta table below the root (an index table's _export_metadata included),
# sorted by relative path; --tables a,b,... limits it to those relative paths or glob patterns
uv run --offline --frozen --project bench gsbench digest <root-or-table> --out a.jsonl
uv run --offline --frozen --project bench gsbench compare-digests a.jsonl b.jsonl   # [--rows-only] [--strict]

# export every committed fixture repository with one jar and digest the results
bench/fixture-digests.sh <cli-jar> <out-dir> [-- extra dump-to-delta args]
uv run --offline --frozen --project bench gsbench compare-digests target/dig/base target/dig/new
```

A line is `{"table", "rows", "rowDigest", "schema", "tags", "txns", "files"}`; each digest is the
first 16 hex of a sha256 and independent of file names, file order and commit timing:

| field | sha256 over |
|---|---|
| `rowDigest` | the sorted per-row sha256 of every row as JSON (all columns, raw values, keys sorted, `_source` as its raw string) |
| `schema` | the Delta schema string of the latest `metaData` action |
| `tags` | the sorted multiset of the active AddFile tag maps (shard keys; paths excluded) |
| `txns` | the sorted SetTransaction app ids |

`files` counts active AddFiles. Schema, tags and transactions come from replaying the raw Delta log
(newest checkpoint plus later JSON commits); rows are read through delta-rs, which must see the same
files. Digests contain no document content.

`compare-digests` prints every common table whose fields differ (`MISMATCH`) and every table present
on one side only (`ONLY-A`/`ONLY-B`) and exits 1 on a mismatch; `--strict` also fails on one-sided
tables, `--rows-only` compares only `rows` and `rowDigest`. A directory argument stands for its
`digests.jsonl`.

`fixture-digests.sh` finds every committed repository (a directory with `index.latest`) under
`*/src/test/resources/fixtures/`, runs `dump-to-delta --all-indices` on each into a new `<out-dir>`
(it refuses an existing one: exporting into existing tables would skip committed shards) and writes
`tables/<fixture>/<index>`, `reports/<fixture>/`, `logs/<fixture>.log`, `exit-codes.tsv`,
`digests.jsonl` and `fetch-modes.jsonl`, with fixtures named like `core/demo-repo`. A failing export is recorded with its exit
code and digested as far as it got; compare `exit-codes.tsv` and the reports for the quarantine sets.
`JAVA` and `JAVA_OPTS` select and tune the JVM.

`gsbench fetch-modes <root> [--per-report]` counts the `fetch` mode of every shard of every run report
(a JSON object with `indexName` and `shards`) found at any depth under `<root>` (a fixture-digests
output, its `reports/`, or a gsbench result directory, whose reports sit under `gs/rep<n>/es<major>/report/`)
and sums `bytesFetched` and `bytesProcessed`: one line per report with `--per-report`, then the total.
`fixture-digests.sh` writes it as `fetch-modes.jsonl`.

## Tests

`uv run --project bench pytest bench/tests` covers the canonical form, generator determinism,
the PIT response walker, the process-per-slice exporter (against a stub ES), the gate checks and
self-test, expected-failure resolution, the invariant checks, the measurement code and `compare`.
