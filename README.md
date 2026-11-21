# Gefrierschrank

*(German for "freezer" — reads what's frozen.)*

A Java CLI and library that reads **Elasticsearch frozen-tier snapshot
repositories directly from S3 (or local disk)** — no Elasticsearch cluster
required — and can dump the documents to JSONL or straight into a **Delta
Lake table**.

Built for the case where you have terabytes of old data sitting in an ES
snapshot repository and want the documents out (for archival, migration, or
loading into a lakehouse) without paying to spin up and restore into a real
cluster first.

## What it does

- Parses the snapshot repository's own on-disk/on-S3 format directly:
  `index-N` / `index.latest` metadata, per-shard `snap-*.dat` blobs, and the
  Elasticsearch-proprietary Lucene codecs (`ES87BloomFilter`, `ES812Postings`,
  etc.) needed to read the actual segment files.
- Supports **Elasticsearch 7, 8, and 9** snapshots out of the box — these
  write Lucene 8, 9, and 10 segments respectively, and Gefrierschrank reads
  all of them in-process with Lucene 10 (see [Architecture](#architecture)).
- Exports every live root document of **one retained snapshot per index**: the latest,
  or, when a table is resumed, the snapshot it was started from while that snapshot is
  still retained. A mounted index follows its exact backing snapshot reference, including
  references into separately registered repositories. Historical versions are not combined.
- Preserves complete `_source` JSON in a Delta string column. Stored JSON source is
  retained exactly; synthetic source, and documents of ES 8+ indices that no longer store
  `_source`, are reconstructed with Elasticsearch mappers. Stored SMILE, CBOR and YAML sources
  are converted to JSON, and vectors ES 9.2+ keeps out of `_source` are put back (see
  [compatibility notes](#compatibility-notes)).
- Can dump to plain JSONL, or directly into a **Delta Lake table** (local or
  S3) via [delta-standalone](https://github.com/delta-io/delta), with:
  - **Resumable, exactly-once writes**: a crashed or interrupted dump can be
    re-run and will neither duplicate nor lose rows — see
    [Resumability](#resumability--error-quarantine).
  - **Per-shard error quarantine**: a corrupt or unreadable shard is logged
    and skipped rather than aborting the whole run.
  - **A structured JSON run report**: shard-level outcomes, counts, byte
    sizes, and timings — useful when you can't tail logs on a long-running
    or unattended job.
  - A stable archive schema that preserves arrays, nested objects, large numbers,
    unusual field names, and mapping changes inside `_source` without coercion.
  - Stored `_id` and `_routing`, snapshot provenance, and a companion Delta table
    holding mappings/settings and mounted-reference lineage.

## What it does not do (yet)

- No Delta partitioning or automatic conversion of source fields to typed analytic
  columns. Query or derive those columns from `_source` after extraction.
- No recovery of values that Elasticsearch itself did not retain (pruned source, values
  ignored at index time in documents rebuilt without stored `_source`, original formatting/order
  lost by synthetic source). Unsupported codecs or synthetic mappers, including
  `semantic_text`, fail explicitly instead of dropping
  fields. Stored source with such fields remains exportable. Stored IDs/routing are
  preserved when present; generated IDs absent from stored fields are null, with
  `_snapshot_uuid`, `_shard`, and `_doc` retained as a snapshot-local row identity.
- No rehydration of `rank_vectors` or `semantic_text` values that ES 9.2+ keeps out of
  the stored `_source`: such indices fail instead of exporting rows without them. Dense
  and sparse vectors are put back (see [compatibility notes](#compatibility-notes)).
- Without `--selective-fetch`, every file of a shard is downloaded and checksum-verified before
  the shard is read, including postings, points and vectors that a stored-source export never
  reads. The option (default off) skips them for stored-source indices and, for synthetic-source
  shards, keeps only the structures Elasticsearch's loaders read; compound segments still download
  whole.
- A second writer on the same table is refused only for local tables; on
  `s3a://` targets it is not detected, so keep to one process per table (see
  [parallelism](#parallelism-and-temp-disk)).
- Only tested against MinIO as the S3-compatible target/source in this
  project's own test suite; should work against real AWS S3 (standard
  `S3AFileSystem`/AWS SDK v2 configuration) but hasn't been exercised there.

## Requirements

- **JDK 21 or newer**, one JVM for everything (Lucene 10's class files
  require 21+; an earlier Hadoop version used to force a JDK ≤21 ceiling —
  that constraint no longer applies after upgrading to Hadoop 3.4.3).
- Maven 3.9+.
- Docker (only for the local dev/test MinIO instance — not needed to just
  build the project).

## Quick start

From a fresh clone to a Delta table, using the small snapshot repository that ships with the
tests (three indices of 500 documents, written by Elasticsearch 7.17, 8.17 and 9.3):

```sh
git clone https://github.com/oc7o/Gefrierschrank.git && cd Gefrierschrank
mvn package -DskipTests        # about a minute; the first build downloads the dependencies
alias gs='java --enable-native-access=ALL-UNNAMED -jar gefrierschrank-cli/target/gefrierschrank-cli-1.2.0.jar'
REPO=gefrierschrank-core/src/test/resources/fixtures/demo-repo

gs --version
gs info "$REPO"      # snapshots and indices in the repository
gs survey "$REPO"    # what dump-to-delta would do with each index (reads metadata only)

# One index into a local Delta table, with a per-shard run report
gs dump-to-delta "$REPO" --index test_data_9_3_0 -o /tmp/gs-quickstart/test_data_9_3_0 \
  --report /tmp/gs-quickstart/report.json

# Run it again: the committed shard is skipped, nothing is written twice
gs dump-to-delta "$REPO" --index test_data_9_3_0 -o /tmp/gs-quickstart/test_data_9_3_0

# Every index of the repository, one table per index under the output root
gs dump-to-delta "$REPO" --all-indices -o /tmp/gs-quickstart/all
```

Read a table with any Delta Lake reader, for example Python's `deltalake`:

```sh
uv run --no-project --with 'deltalake[pyarrow]' --with pandas python -c "
from deltalake import DeltaTable
df = DeltaTable('/tmp/gs-quickstart/test_data_9_3_0').to_pandas()
print(len(df), df[['_id', '_source_kind', '_source']].head())"
```

For your own data, point the same commands at a filesystem repository path or at
`s3://bucket/prefix` with `--source-endpoint` and credentials, and write to a local path or an
`s3a://` table ([Usage](#usage)). Run `survey` first on a large repository: it lists the indices
that would fail and the bytes each would read. `--enable-native-access=ALL-UNNAMED` only silences
the JDK's warnings about zstd's native library.

## Building

```sh
mvn install
```

This builds all five modules (see [Architecture](#architecture)) and
produces a runnable fat jar at
`gefrierschrank-cli/target/gefrierschrank-cli-1.2.0.jar`, which is all a deployment
needs.

## Running the tests

```sh
./run-tests.sh
# equivalent to: mvn package -DskipTests && mvn test
```

Tests across all 5 modules run under a single JDK. The S3 tests
(`S3BlobStoreTest`, `S3RangedReadTest`, `S3BodyResumeMinioTest`,
`StsCredentialsS3Test`, `InspectCommandsS3Test`, `DeltaS3WriterTest`) need a
local MinIO instance on port 9000 — start one with:

```sh
docker compose up -d minio
```

They're regular JUnit tests, not integration tests behind a profile flag —
run `docker compose up -d minio` first if you're running the full suite.

End-to-end benchmarks against the Elasticsearch restore-and-export alternative, with a per-document correctness gate, live in [bench/](bench/README.md).

## Usage

All commands go through the fat jar:

```sh
java -jar gefrierschrank-cli/target/gefrierschrank-cli-1.2.0.jar <command> ...
```

### Inspect a repository

```sh
# Overview: snapshots (state, version, index_version), indices, shard generations
java -jar gefrierschrank-cli-*.jar info /path/to/snapshot-repo

# Shards of one index, with file counts and sizes; a mounted index names its backing snapshot
java -jar gefrierschrank-cli-*.jar shards /path/to/snapshot-repo my_index

# Files of one shard: blob name, physical name, size, Lucene version
java -jar gefrierschrank-cli-*.jar files /path/to/snapshot-repo my_index 0

# All three also read s3:// repositories, with the same options as survey
java -jar gefrierschrank-cli-*.jar info s3://my-bucket/prefix \
  --source-endpoint http://localhost:9000 --source-credentials env

# What dump-to-delta will do with every index, and the shard bytes it would read (metadata only)
java -jar gefrierschrank-cli-*.jar survey s3://my-bucket/prefix --source-endpoint http://localhost:9000
```

### Dump to JSONL

```sh
java -jar gefrierschrank-cli-*.jar dump /path/to/snapshot-repo \
  --index my_index -o /tmp/my_index.jsonl

# or from S3 / a MinIO-compatible endpoint:
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
java -jar gefrierschrank-cli-*.jar dump s3://my-bucket/prefix \
  --index my_index -o /tmp/my_index.jsonl \
  --source-endpoint http://localhost:9000
```

`dump` writes one line per live document: its `_source`, without `_id`,
routing or snapshot columns. It plans the index the way `dump-to-delta`
does:

- It reads the latest snapshot of the name. A mounted searchable snapshot
  (`partial-*`, `restored-*`) exports the documents it mounts, found through
  the same `--repository NAME=URI`, `--repository-endpoint` and
  `--repository-env` options; the original index name exports its own
  latest snapshot.
- Stored `_source` is written byte-exact, except that raw CR/LF in
  pretty-printed JSON become spaces so each document stays on one line.
  Synthetic-source indices (logsdb, TSDB) get reconstructed rows, the same
  text `dump-to-delta` writes: not byte-exact, and not marked as
  reconstructed in the JSONL. `_source` stored as SMILE, CBOR or YAML is
  written as the JSON text Elasticsearch returns for it (not byte-exact).
  Documents whose excluded vectors were put back are not byte-exact either
  (see the compatibility notes).
- An index that `dump-to-delta` refuses before any download fails the same
  way, before any shard is read and with a fixed `error:` line: created by
  Elasticsearch 6.x or older, `_source` provably neither stored nor
  reconstructable, or stored `_source` that excludes `rank_vectors` or
  `semantic_text` values. See [Compatibility notes](#compatibility-notes).
- A file output is written to a partial file of its own beside it,
  `<output>.<random>.partial`, and moved into place only when every shard is
  written, so concurrent runs never share one. A symbolic link keeps
  pointing at its target, which is the file replaced, and a replaced file
  keeps its permission bits. A failure deletes the partial file and leaves
  an existing output untouched; an exception prints one `error:` line and
  exits 1, while a JVM error such as `OutOfMemoryError` keeps its stack
  trace. SIGTERM and Ctrl-C delete the partial file as well; only `kill -9`
  or a crash leaves it behind, to be deleted by hand. A read-only output, a
  missing or read-only output directory and a dangling symbolic link are
  refused before the repository is read.
- `/dev/stdout`, `/dev/null`, a named pipe or a process substitution such as
  `-o >(gzip > my_index.jsonl.gz)` cannot be renamed over, so `dump` writes
  into them directly and prints its status lines to stderr instead of
  stdout. A failure then leaves what was already written in the stream:
  check the exit code.
- `--source-credentials env|ambient` and `--download-concurrency` work as
  for `dump-to-delta`; `--endpoint` is an alias of `--source-endpoint`.
- `-Dgefrierschrank.lucene8.runtime=worker` fails with exit code 2 and
  `the Lucene 9 worker was removed in 1.2.0` before the repository is read, as
  in `dump-to-delta` (see [Architecture](#architecture)).

### Dump to a Delta Lake table

```sh
export AWS_ACCESS_KEY_ID=...        AWS_SECRET_ACCESS_KEY=...        # source repo
export TARGET_AWS_ACCESS_KEY_ID=... TARGET_AWS_SECRET_ACCESS_KEY=... # Delta table target
                                                                       # (falls back to the
                                                                       # AWS_* vars above if
                                                                       # source and target are
                                                                       # the same account)

java -jar gefrierschrank-cli-*.jar dump-to-delta s3://source-bucket/prefix \
  --index my_index \
  -o s3a://target-bucket/delta/my_index \
  --source-endpoint http://source-s3:9000 \
  --target-endpoint http://target-s3:9000 \
  --report /tmp/my_index-run-report.json
```

Source and target S3 endpoints/credentials are configured independently —
the snapshot repository can be on one provider and your lakehouse on
another.

`-o`/`--output` also accepts a plain local path instead of `s3a://` for a
local Delta table.

Options for long runs, each described below:

- `--parallelism N` and `--temp-budget SIZE`: shards processed concurrently across the
  run (with `--all-indices`, up to N indices at once), and the temp disk they may occupy
  ([parallelism](#parallelism-and-temp-disk));
- `--shard-threads N|auto`: threads that read one shard as contiguous document ranges, for
  large single-shard indices ([shard threads](#shard-threads));
- `--snapshot auto|latest|<name or UUID>`: which retained snapshot a table is
  exported or resumed from ([snapshot selection](#snapshot-selection));
- `--parquet-codec zstd|snappy|uncompressed` and the file size target
  ([Parquet output](#parquet-output));
- `--download-concurrency N`, `--selective-fetch`, `--source-credentials env|ambient` and
  `--target-credentials env|ambient` ([source download, retries and
  credentials](#source-download-retries-and-credentials));
- `--mounted-shells export|skip-if-original` (with `--all-indices`, below) and
  `--source-disabled-accept unmapped-values` ([compatibility notes](#compatibility-notes)).

To dump every index in the repository with one unattended command, replace
`--index` with `--all-indices`. `-o`/`--output` then becomes a table
**root** (each index lands at `<root>/<indexName>`) and `--report` becomes a
**directory** (each index writes `<dir>/<indexName>.json`, plus one
aggregate `<dir>/manifest.json` summarizing the whole run):

```sh
java -jar gefrierschrank-cli-*.jar dump-to-delta s3://source-bucket/prefix \
  --all-indices \
  -o s3a://target-bucket/delta-tables \
  --source-endpoint http://source-s3:9000 \
  --target-endpoint http://target-s3:9000 \
  --report /tmp/all-indices-run
```

For example, an index named `logs-2024-01` lands at
`s3a://target-bucket/delta-tables/logs-2024-01` with its report at
`/tmp/all-indices-run/logs-2024-01.json`. One index failing outright (e.g.
its shard catalog can't be resolved at all) is recorded in the manifest and
does not stop the rest of the indices from being processed, unless the target
itself cannot store data ([exit codes](#exit-codes)) — `--index` and
`--all-indices` are mutually exclusive; exactly one must be given.

`dump-to-delta` exports the latest retained state of **every index name in the source
repository** (see [snapshot selection](#snapshot-selection) for resumed tables), not just
the indices in the newest repository-wide snapshot. Older
indices that still exist only in earlier snapshots are therefore included. If both
an original index and its mounted name appear in the catalog, each gets its own
table by default; do not union those tables assuming their documents are disjoint.
With `--mounted-shells skip-if-original` (only with `--all-indices`, else exit 2), a
mounted shell whose original is exported in the same run from exactly the same backing
index, snapshot and shard count (the identity `survey` groups by, taken on the snapshot
the export selects) gets no table: the manifest records it as `ALIAS_OF` the original,
whose table `<root>/<original>` holds its documents (with `_index` naming the original;
rows naming the shell do not exist). A shell whose original is not in the run, failed
outright or exported another snapshot is exported as usual, and so is a shell that already
has a table from an earlier run, including a table directory an interrupted run left without
a commit (it is resumed, not aliased; delete it to drop it).

If a mounted index points to a different repository, register its location using
the repository name in its metadata (the failure message supplies that name):

```sh
java -jar gefrierschrank-cli-*.jar dump-to-delta s3://source-bucket/prefix \
  --all-indices -o s3a://target-bucket/complete-export \
  --source-endpoint http://source-s3:9000 \
  --target-endpoint http://target-s3:9000 \
  --repository frozen-repo=s3://backing-bucket/backing-prefix \
  --repository-endpoint frozen-repo=http://backing-s3:9000 \
  --report /tmp/complete-export-report
```

Repeat `--repository NAME=URI` for multiple backing repositories. Locations can be
local paths or S3 URIs. Additional S3 repositories use source credentials and default
to `--source-endpoint`; the endpoint override is optional. For a different account,
use `--repository-env frozen-repo=FROZEN` and set `FROZEN_AWS_ACCESS_KEY_ID`,
`FROZEN_AWS_SECRET_ACCESS_KEY`, and optionally `FROZEN_AWS_SESSION_TOKEN` and
`FROZEN_AWS_REGION`. Secrets stay out of command arguments. The CLI reads only the
referenced backing snapshot, even if that index has a newer snapshot elsewhere.
Missing or ambiguous references, cycles, and missing Lucene commits are errors.

The aggregate manifest records the full planned index count, `pendingIndices`, and
`complete`. A killed run leaves pending indices visible; `complete` becomes true only
after all planned indices finish without quarantine or failure. With
`--mounted-shells skip-if-original`, a shell's entry can have the outcome `ALIAS_OF` with
`aliasOf` (the original's index name; no report file or snapshot fields, the original's
entry carries them), and the manifest gains `aliasCount`. A shell is decided only after its
original's entry is recorded (an original with quarantined shards still aliases its shells,
and the run exits `3` from the original's entry); until then it is pending. Without the
option neither key nor the outcome appears and the manifest is unchanged, so automation
that switches on `outcome` must know `ALIAS_OF` before the option is turned on.

The document table contains `_source` (a JSON **string**), `_id`, `_routing`,
`_source_kind` (`stored`, `synthetic`, `stored-smile`, `stored-cbor` or `stored-yaml`, each optionally
followed by `+vectors` or `+vectors-reserialized`; only `stored` is the stored bytes unchanged, and
`stored-smile|cbor|yaml` rows hold the JSON rendering of a non-JSON stored `_source`, see
[Compatibility notes](#compatibility-notes)), `_index`, `_snapshot`, `_snapshot_uuid`,
`_repository_index_id`, `_source_index`, `_source_snapshot_uuid`, `_shard`, and `_doc`.
`_unmapped_fields` is reserved by the common writer and normally null in this format.
Original document fields with any of these names remain intact inside `_source`.
`<table>/_export_metadata` is a separate Delta table with original and resolved index
metadata, plus the reference chain; treat it as sensitive source metadata.

For example, in Spark SQL:

```sql
SELECT _id, get_json_object(_source, '$.value') AS value
FROM delta.`s3a://target-bucket/complete-export/logs-2024-01`;
```

**Use a fresh output root when upgrading from the old typed-column export.** The CLI
rejects legacy tables rather than treating their shard tags as proof of complete-source
coverage.

#### Snapshot selection

A table only ever holds one snapshot of its index; appending another would mix old and
new states. `--snapshot` decides which one a run exports:

- `auto` (default): a table that already holds committed shards, or only its
  `_export_metadata` row, of one snapshot that the repository still retains is completed
  from **that** snapshot, even if a newer one was taken since. An interrupted or partly
  quarantined export therefore resumes after the next SLM snapshot instead of starting
  over at a new root. A new table, or one whose snapshot has been deleted by retention,
  uses the latest retained snapshot; for the latter the run is refused as below, except
  for a mounted index whose backing data is unchanged (see below).
- `latest`: always the latest retained snapshot. A table started from any other snapshot
  is refused (`ArchiveTargetConflictException`).
- a snapshot name or UUID (only with `--index`): exactly that snapshot. `auto` and
  `latest` are keywords, so a snapshot that is literally named `auto` or `latest` is
  selected by its UUID.

The refusal happens before anything is written. To export that index from the selected
snapshot instead, delete its table directory (`<root>/<index>`, which includes its
`_export_metadata`) and run again; with `--all-indices` only that index is exported again,
the other, complete tables are skipped. A table whose first run quarantined every shard
holds no documents yet, so deleting it loses nothing. A new output root works too.

A table completed from an older snapshot is as of that snapshot, not the index's latest
state. This is visible everywhere: stdout prints `Completing <index> from snapshot <X>;
newer snapshot <Y> available; this table is as of <X>`, the run report carries
`snapshot.newerSnapshotAvailable`, and the `--all-indices` manifest records each index's
`snapshotUuid` and `newerSnapshotAvailable` plus a `behindLatestCount`. Such a table still
counts as complete (exit code `0`). To export the newer state, delete the table directory
as above or use a new output root.

A mounted (searchable snapshot) index is in every SLM snapshot, but its data is the one
immutable backing snapshot it was mounted from. Under `auto`, its table therefore survives
retention deleting the shell snapshot it was started from: when the latest snapshot still
mounts the same backing index and snapshot, the table is completed as of the deleted
snapshot, and a complete one is skipped. Every condition is checked against the table's
own keys and its single `_export_metadata` row; anything else (an index with its own data,
another backing snapshot, mixed keys) is refused as above. Rows written after the deletion
keep naming the deleted snapshot in `_snapshot_uuid` and `_snapshot`, and `_export_metadata`
keeps that snapshot's index metadata; the rows themselves are the same. stdout prints
`Completing <index> as of snapshot <X>, which retention has deleted; its backing snapshot
<uuid> of <backing index> is unchanged`, and reports and the manifest mark the table
`newerSnapshotAvailable`. `latest` and an explicit snapshot still refuse such a table.

#### Parallelism and temp disk

`--parallelism N` (1-256, default 1) is the run-wide bound on shards processed at the same
time. With `--all-indices`, up to N indices are exported at once, started in listing order,
and all their shards share that one bound, admitted first come, first served. A repository
of single-shard indices, the frozen tier's usual shape after an ILM shrink, thus gains from
N > 1 too: one index downloads while another decodes and a third commits. Every shard is
downloaded to the process's temporary run directory under `java.io.tmpdir` (completely, unless
`--selective-fetch` leaves out what the export never reads) and checksum-verified before it is
read, so concurrent shards need up to N shard sizes of temp disk
(see also [temporary files](#temporary-files-throughput-and-crash-cleanup)). `--temp-budget SIZE`
(e.g. `500G`, `2T`; binary units, a bare number means MiB) caps what concurrently processed
shards may occupy; a shard larger than the budget runs alone. Without it, the budget is 80%
of the temp directory's free space at start. Within an index, shards are admitted in order,
and every table and final report is the same as a serial run's; only intermediate reports
list shards in completion order. With several indices in flight, their stdout lines
interleave (each line stays whole), each index's summary is printed when it finishes, and
the manifest lists indices in listing order with those still running as pending; after a
kill, several tables may be partly written, and each resumes on its own. Concurrent shards download,
verify, decode and write their Parquet files in parallel; only the short Delta commits
into one table are serialized. Each concurrent shard may hold a large document plus its
current Parquet row group in heap: plan roughly 1 GiB of `-Xmx` per concurrent shard (a
warning is printed otherwise); documents of 1 MiB and more are admitted through a heap budget
(see [shard threads](#shard-threads)), so an oversized one quarantines its shard instead of
ending the run. Synthetic-source shards share one isolated Elasticsearch
classloader.

Only one process may write to a given table at a time. For local tables this is enforced:
an export locks `<table>/_gefrierschrank.lock` before it reads the table and holds it until
the index ends, and a second writer, in another process or the same one, fails at once with
`TableLockedException` (exit 1; `FAILED` under `--all-indices`) before it downloads anything.
Delta readers ignore the file; an export that wrote nothing into a table removes it again.
On `s3a://` targets a second writer is not detected (one warning per run), so keep to one
process per table there. Two overlapping `--all-indices` processes on one local root split the
tables between them: each exports the tables it locked first, fails the others with
`TableLockedException` and exits 4; a rerun completes whatever is left. A local table that two
overlapping processes wrote before this lock
existed can fail every rerun with a `ChecksumException` on `_delta_log/<N>.json`: both
committed version N, and the hidden `_delta_log/.<N>.json.crc` belongs to the other commit.
Deleting that `.crc` file makes the table readable again; the next run re-exports the shard
whose commit was overwritten, and its Parquet files stay behind unreferenced.

#### Shard threads

`--shard-threads N` (1-64, or `auto`; default `auto`) reads one shard with up to N threads: the
shard is split into N contiguous document-id ranges with equal numbers of live documents,
each range is decoded and written to its own Parquet files on its own thread from the same
downloaded shard, and the shard is still committed once, exactly like a shard read by one
thread. This is what speeds up a large single-shard index, which `--parallelism` cannot.
`auto` uses the CPU cores divided by `--parallelism`, at most 8. The effective number is
lower when the shard has fewer than 250,000 live documents per range, or when the heap is
too small (see below). A failure in any range fails the whole shard, which is quarantined as
before, with no file of any range left behind.

- Heap: each range writes its own Parquet file and holds its own row group (32 MiB of
  compressed pages) plus buffers, about 40 MiB per range (38.7-40.1 MiB measured). The number of
  ranges per shard is capped at (max heap / `--parallelism` - 512 MiB) / 40 MiB, and a
  warning is printed at startup when that cap applies. Synthetic-source ranges each open
  their own reconstruction reader, which reconstructs documents ahead of the range in batches
  of up to 1024 documents or 8 MiB and holds a batch twice while converting it, so a
  synthetic-source range counts 56 MiB, in this cap and in the large-document budget below.
- Large documents (this applies at every setting, including one thread): a document of 1 MiB
  or more first reserves about 6 times its size from one heap budget shared by every shard
  and range (the maximum heap minus 40 MiB per writing range, 56 MiB per synthetic-source
  one). The budget also counts what an open Parquet file keeps of the large documents
  already written to it until the file is closed (up to about four times the largest of
  them, plus twice each further one), and, for every range that is reading, room to read one
  more document as large as the largest seen so far (2.5 times its size: a reader holds the
  decompressed block and the document before the budget learns its size). A range starts
  reading only when that room is there. A document that does not fit has its range's open
  Parquet file closed, and if it still does not fit it waits in a temporary file in the
  shard's temp directory (outside `--temp-budget`), not in the heap, first come, first
  served. On a small heap, large documents are therefore written one after another, and a
  shard of them as more, smaller files. A document that can never fit fails its shard with
  `DocumentTooLargeException`: a quarantine that the next run retries, instead of an
  `OutOfMemoryError` that ends every run. A shard read with several threads is first read
  again as one range, which has more of the budget to itself. The estimate is deliberately
  on the safe side, so near the limit a shard can be quarantined that would just have fit.
  `-Dgefrierschrank.document.reserveMultiplier=<factor>` (a number above 0) changes the
  factor. Not covered: documents larger than any seen before that several ranges or shards
  read at the same moment, before the first of them reaches the budget, each take about 2.5
  times their size of heap. Keep `--parallelism` x `--shard-threads` x 2.5 x the largest
  document well below the maximum heap; a startup warning names the settings under which
  that does not hold for documents of up to 100 MiB (Elasticsearch's default request size
  limit).
- On an `s3a://` target, each range's open file buffers its upload blocks in
  `fs.s3a.buffer.dir` like any open data file, outside `--temp-budget`, so
  `--parallelism` x `--shard-threads` files buffer at once.
- The rows and every column are the same at every setting. The Parquet files are not: each
  range writes its own files, so a shard has up to N-1 more (partial) files than with one
  thread. Within a file rows are ordered by `_doc`, and a shard's files follow each other
  in `_doc` order.
- Speed-up needs disk bandwidth: N ranges read the downloaded shard from the temp directory
  at up to 170-360 MB/s at N = 8. A cloud volume with a low throughput limit (a default gp3
  volume allows 125 MB/s) caps the gain unless the shard is still in the page cache; see
  [temporary files, throughput and crash cleanup](#temporary-files-throughput-and-crash-cleanup).

#### Exit codes

`0` = every shard committed (every index, with `--all-indices`); `3` = the run finished but one or more shards were
quarantined (check `--report`); `4` = (`--all-indices` only) at least one
index failed outright before its own shards could even be attempted — worse
than a quarantined shard, since that index produced no report at all; `1` =
a run-level failure that isn't index-specific (repository not found,
unreachable, etc.), or with `--index` that index failing outright (for example a
refused table); `2` = an invalid command line, rejected before anything is
read or written (for example `--snapshot <name>` with `--all-indices`, or a
`--parquet-codec` whose native library cannot load). An `ALIAS_OF` manifest entry
(`--mounted-shells skip-if-original`) adds nothing to the exit code: its original's own
entry carries any `3` or `4`.

A target that cannot store data (disk full, file too large, quota or I/O error) ends the
whole run with `1` and one `error: TargetStorageException: target storage failure (<reason>)`
line, with `--index` and `--all-indices` alike, since every later shard would be downloaded
only to fail the same way; it is never a quarantine. With `--all-indices` that index
is `FAILED` with `errorClass` `TargetStorageException`, no further index starts, indices
already running finish and are recorded, and the rest stay in `pendingIndices`.

A JVM error such as `OutOfMemoryError` ends the whole run with `1` and its stack trace, also
when it strikes a download, shard, range or index thread at `--parallelism` above 1: the run
does not wait for shards still in flight, and the temp run directory is deleted. Do not add
`-XX:+ExitOnOutOfMemoryError`: the JVM then exits with `3`, the code for quarantined shards,
and leaves the temp run directory to the next start's sweep.

#### Parquet output

Data files are ZSTD-compressed Parquet, typically about 5x smaller than uncompressed
Parquet for log-like documents. Readers need ZSTD support: parquet-mr >= 1.10 (Spark >= 3.2), Trino, DuckDB
and delta-rs all read it. For older readers, pass `--parquet-codec snappy` or
`--parquet-codec uncompressed`. The codec applies per file, so a resumed run may use a
different codec than the run that started the table.

ZSTD and Snappy compress through native libraries that zstd-jni and snappy-java unpack
into `java.io.tmpdir`. Where that fails (a `noexec` temp directory on hardened hosts,
Docker `--tmpfs /tmp`, read-only images), the default falls back to uncompressed Parquet,
as 1.0.0 wrote it, and prints one `warning: Parquet ZSTD compression unavailable (...)`
line at startup; `-DZstdTempFolder=<exec-capable dir>` or `-Djava.io.tmpdir=<dir>` brings
ZSTD back. An explicit `--parquet-codec zstd` or `--parquet-codec snappy` whose library
cannot load stops the command before exporting anything (exit 2); Snappy's directory is
set with `-Dorg.xerial.snappy.tempdir=<dir>`. Only `--parquet-codec uncompressed` needs
no native library. Reading ZSTD-compressed Elasticsearch segments has its own, separate
fallback (see [compatibility notes](#compatibility-notes)).

Each shard's data is rolled into files of about 256 MiB of written (compressed) Parquet,
plus one smaller tail file per shard. The target can be changed with
`-Dgefrierschrank.parquet.target-file-bytes=512m` on the `java` command line. Files larger
than one upload part (16 MiB unless `fs.s3a.multipart.size` is configured, see below) are sent
to S3 as multipart uploads. A killed process (`kill -9`, OOM
kill) can leave an incomplete multipart upload behind, which is invisible but billed, so
give the target bucket a lifecycle rule with `AbortIncompleteMultipartUpload` (for
example after 7 days). Do not set `fs.s3a.multipart.purge` while any writer is active
on the bucket: it aborts other writers' in-progress uploads.

Parquet row groups are capped at 32 MiB, so a file being written holds at most about 32 MiB
of compressed pages in heap (earlier builds up to 128 MiB; 1.0.0 started a new file every
10,000 rows, about 11 MB). Allow about 40 MiB of heap per file written at the same time, the
row group plus buffers. Readers are unaffected: a 256 MiB file simply has several row groups.

On an `s3a://` target each open data file buffers up to
(`fs.s3a.fast.upload.active.blocks` + 1) × `fs.s3a.multipart.size` of upload blocks on local
disk. Unless you configured these keys yourself, Gefrierschrank uses 16 MiB parts and 8
active blocks: at most 144 MiB per open file, where Hadoop's defaults (64 MiB parts, 4 active
blocks) allow 320 MiB and peaked at 201 MB. With 32 MiB row groups the measured peak is about
47 MB. More of a file is already uploaded when it is closed, which shortens the serial upload
at the end of each shard. The blocks go to the process's
[temporary run directory](#temporary-files-throughput-and-crash-cleanup), on the same
filesystem as the shard downloads, instead of `/tmp/hadoop-$USER/s3a`. A
`fs.s3a.multipart.size`, `fs.s3a.fast.upload.active.blocks` or `fs.s3a.buffer.dir` from your
Hadoop configuration is kept, and so is Hadoop's buffer location when you chose it through
`-Dhadoop.tmp.dir`, a configured `hadoop.tmp.dir` or the `LOCAL_DIRS` environment variable
(set by YARN and some container platforms).
Smaller parts mean more upload requests; with S3's limit of 10,000 parts an object can still
reach 156 GiB.

## Resumability & error quarantine

Two guarantees for `dump-to-delta`, both load-bearing for long/unattended
runs against large repositories:

- **Resumable, exactly-once**: re-running the same command against the same
  source index and target table skips shards already committed and only
  processes the rest — no missing rows, no duplicate rows — as long as the
  snapshot the table was started from is retained, or for a mounted index its
  backing snapshot (see [snapshot selection](#snapshot-selection)). This works
  without a separate checkpoint file: the Delta transaction log itself
  records which shard produced each committed file, so "what's done" is
  always consistent with what's actually in the table, even across a hard
  crash.
- **Per-shard quarantine, not whole-run failure**: if a single shard fails
  (corrupt segment, unsupported codec, transient error), it's recorded in
  the run report and skipped — the run continues with the remaining shards.
  A quarantined shard is never marked committed, so it's automatically
  retried on the next invocation, exactly like an unattempted shard.
  Two failures are deliberately not quarantined: a target that cannot store
  data (disk full, quota, I/O error) ends the whole run, and a document too
  large for the heap budget quarantines its shard with
  `DocumentTooLargeException` rather than ending the run with an
  `OutOfMemoryError` (see [exit codes](#exit-codes) and
  [shard threads](#shard-threads)).

Mounted searchable snapshots are resolved before shard extraction. Their documents
are exported into the mounted index's table. `SKIPPED_NO_DATA` is a legacy report
value and is no longer emitted by extraction. A no-data shell is never counted as
successful coverage. Only shards already committed to the same archive can be skipped.

Reporting never interferes with extraction: if the run report or manifest
can't be written (unwritable path, full disk), a warning is printed and the
dump continues. The data is the valuable part; the report is not.

The `--report` JSON is safe to share without exposing indexed document
content — it holds shard identity, row/byte counts, timings, and (for
quarantined shards) the failing exception's class name and up to twelve stack locations, never raw
exception messages or `_source` content. Stack locations retain the caller of
`Objects.requireNonNull`, which is needed to diagnose message-less failures.
Index names, snapshot names, paths, and timing may still be sensitive; redact
identifiers before sharing a report.

Byte counts describe the total source shard file sizes, including reused files;
they are not Parquet output sizes. Empty mapped shards commit an empty Parquet
file so they can be skipped on retry. Deleted and soft-deleted document versions
are excluded from export.

Every source file the export reads is checked against the repository's checksum
before Lucene opens it. The archive
keeps source values as JSON rather than forcing them into numeric columns. A mapping
with ordinary stored source (ES 8+ indices whose `_source` was disabled later or from creation)
rebuilds a document without stored `_source` through Elasticsearch's mappers when every field
holding data has doc values or a stored field (see [compatibility notes](#compatibility-notes));
otherwise such a live document quarantines its shard. Empty Lucene shards are valid and committed.

### Source download, retries and credentials

Each shard is downloaded to a temporary directory before it is read. Every file
the export reads is checked against the repository's length and CRC32 checksum
**while its bytes arrive**, so no second read pass over the local copy is needed.
Verification still finishes before Lucene opens the directory. Local-disk
corruption between the write and the open is not checked again. Without
`--selective-fetch`, every file of the shard is downloaded and checked.

- `--download-concurrency N` (`dump-to-delta` and `dump`, 1-32, default 8) sets
  the number of download streams per shard. Files download in parallel, and the
  parts of a chunked file do too. A part blob larger than 64 MiB is fetched as
  16 MiB byte ranges, all pinned to the blob version (S3 ETag via `If-Match`)
  that the first range came from. If the blob changes mid-download, the shard
  fails; ranges of different versions are never combined. A store that
  ignores `Range` or sends no strong ETag gets such parts read whole instead.
  `1` downloads file after file on one stream. The equivalent JVM property is
  `-Dgefrierschrank.download.concurrency=N` (held to 1-32).
- `--selective-fetch` (`dump-to-delta` only, default off) downloads only the
  shard files a stored-source export reads: segment and field infos, stored
  fields, live documents and doc values. The postings, points, norms, term
  vectors and vector files of non-compound segments stay in the repository;
  they are never opened, so they are not verified either. Compound segments
  (`.cfs`; Elasticsearch writes every segment below `index.compound_format`,
  1 GB by default, as one) are downloaded whole, so small shards save little. The commit files come first,
  then the rest of the plan. Indices whose vectors are kept out of `_source`
  and segments that mark nested documents without `_primary_term` take the full
  download. A shard whose documents are rebuilt from doc values and stored
  fields (synthetic source, `_source` disabled later or from creation) first
  downloads what every plan keeps, then only the structures Elasticsearch's own
  loaders read for it: they are found by reconstructing the first and last 64
  live documents of each segment with the missing structures recorded (nested
  objects need postings, `dense_vector` the vector files, `sparse_vector`
  postings and term vectors, a source-less document of a text field with a
  keyword sub-field norms and postings). Such a shard is probed only when a
  non-compound segment holds at least 16 MiB of those structures
  (`-Dgefrierschrank.download.selective.syntheticMinBytes`), is admitted under
  `--temp-budget` by its full size, and reports why it was fetched in full in a
  stderr note. A document outside the sample that reads a structure left out
  fails the read, which completes the shard as below. A selectively
  fetched shard that fails to open or to export downloads and verifies the
  rest and is read once more in the same run (report `fetch: completed`); only
  a second failure quarantines it. A shard that cannot open with the full
  download only because Lucene cannot load its postings or vector format may
  export with the option. Every shard report carries `bytesFetched` and
  `fetch` (`full`, `selective`, `completed`, `preflight` or `none`), with or
  without the option. Under `--temp-budget` a selective shard is admitted by
  its selected size, so a shard that completes its download can exceed the
  budget by its skipped bytes. The equivalent JVM property is
  `-Dgefrierschrank.download.selective=true` (`true` or `false`).
- `-Dgefrierschrank.s3.maxConnections=N` (default 128) sizes the HTTP pool of
  each source S3 client.
- Source S3 requests retry transient failures: I/O errors, 5xx, throttling and
  clock skew. A request gets up to 10 attempts, with full-jitter exponential
  backoff from 0.5 s (1 s when throttled) up to 20 s and no circuit breaker.
  A server's `Retry-After` is capped at 20 s. The jittered waits add up to at
  most about 92 s per request, but only about 46 s on average. Measured and
  simulated, about 9 in 10 outages are absorbed up to about 30 s of refused or
  reset connections or of 5xx errors other than throttling (500, 502, 504,
  503 ServiceUnavailable; backoff from 0.5 s), and up to about 40 s of
  throttling (429, 503 SlowDown; backoff from 1 s). Longer outages are
  absorbed only sometimes, 60 s rarely. 403, 404 and expired credentials fail
  at once. S3A target retries keep the Hadoop defaults.
- A connection that breaks in the middle of a response body (a reset, a
  premature close, or no data for the 30 s socket timeout) is resumed: the
  rest of the body is requested again from the first byte not yet received,
  pinned to the blob's ETag with `If-Match`. Only what was in flight on the
  broken connection is downloaded again (a few percent of a shard at most in
  fault tests), no byte is verified twice, and bytes of two versions of a
  blob are never combined: a blob that changed in between fails the shard.
  A body is resumed at most 5 times, after pauses of 0.2 s doubling to 5 s,
  and each reopen gets the full retry budget above. One stderr line per
  resume names the blob and the exception class. This covers shard files and
  repository metadata alike. A store that sends no strong ETag cannot pin a
  continuation, so a broken body there still quarantines the shard, which is
  retried on the next run.
- `-Dgefrierschrank.s3.maxAttempts=N` (default 10) sets the attempts per
  source request. About 14 absorb a 60 s outage in most cases, 16 almost
  always. Above 10, the overall per-request limit grows from 10 min to 1 min
  per attempt. `AWS_RETRY_MODE`, `AWS_MAX_ATTEMPTS` and a profile's
  `retry_mode` do not apply to source clients.
- `--source-credentials env|ambient` (`dump-to-delta`, `dump`, `survey`, `field-types`,
  `orphans`, `info`, `shards`, `files`) and `--target-credentials env|ambient`
  (`dump-to-delta`):
  - `env` (the default) reads static keys and an optional session token.
    Source: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` /
    `AWS_SESSION_TOKEN`. Target: `TARGET_AWS_*`, falling back to `AWS_*`. A
    token is only paired with keys from its own namespace, and
    `TARGET_AWS_SESSION_TOKEN` without target keys is an error.
  - **Static environment credentials cannot refresh inside a running
    process.** A run that outlives the token (for example 1 h for STS
    AssumeRole) fails every request after expiry. Resume makes a re-run safe.
  - `ambient` uses refreshing sources and never environment keys. On the
    source side that is `AWS_PROFILE` (`role_arn`, `credential_process`,
    SSO), web identity (IRSA), the container endpoint, then the instance
    profile. On the target side it is S3A's profile provider when
    `TARGET_AWS_PROFILE` is set, then web identity, then the container or
    instance role. `ambient` combined with that side's explicit keys is
    rejected.
  - `credential_process` is also the hook for non-AWS STS services such as
    MinIO's.
  - A backing repository with `--repository-env NAME=PREFIX` can name a profile
    with `PREFIX_AWS_PROFILE` instead of `PREFIX_AWS_*` keys.
  - Source credentials are resolved once at startup, so a misconfiguration
    fails before any shard work. Credential values are never printed.
- In both modes the target's S3A credential provider is pinned. S3A's default
  chain therefore can never pick up environment keys on its own; the only
  `AWS_*` fallback is the documented one in `env` mode.
- Gefrierschrank never writes to the source repository. Give the source role
  read-only access, particularly in `ambient` mode, where an instance or pod
  role may grant more.

### Temporary files, throughput and crash cleanup

Each `gefrierschrank` process keeps its temporary files in one owner-only directory,
`<java.io.tmpdir>/gefrierschrank-run-<random>`: the downloaded shards
(`gefrierschrank-shard-*`, for `dump` too, with any large document waiting for heap inside its
shard's directory), and for `s3a://` targets the S3A upload blocks (`s3a/`). Only `dump`'s
partial output file lives elsewhere, beside the output it replaces, and the native libraries
of zstd-jni and snappy-java sit directly in `java.io.tmpdir` while they load. While the process runs it holds a lock on the
directory's `run.lock`. The directory is deleted when the process exits, including on SIGTERM,
SIGINT and after a fatal error. Shards still in flight at that moment cannot recreate it: from
then until the JVM halts, an empty file of the same name holds its place, and the JVM deletes
that file last. A process killed with SIGKILL (`kill -9`, the OOM killer)
cannot clean up. The next `gefrierschrank` start therefore removes run directories whose lock
is free and that are at least 10 minutes old, and prints how much it reclaimed. It removes
only real directories named `gefrierschrank-run-*` owned by the current user, and empty files of
that name left by a process killed while it was exiting. It never follows a symbolic link and
never touches the directory of a process that is still running. The sweep
runs before the default `--temp-budget` is computed, so reclaimed space counts.

- Temporary files of earlier versions (top-level `gefrierschrank-shard-*`,
  `gefrierschrank-worker-*.jsonl`, `gefrierschrank-index-metadata-*.json`) carry no owner
  marker. They are reported at start, never deleted; remove them by hand once no older
  process is running.
- The lock is an operating-system file lock. On NFS and other network filesystems such locks
  may be advisory or unsupported, so do not share one network temp directory between hosts;
  keep `java.io.tmpdir` on local disk.
- `-Djava.io.tmpdir=<dir>` moves all of it, the S3A blocks included, unless
  `fs.s3a.buffer.dir`, `hadoop.tmp.dir` (also as `-Dhadoop.tmp.dir`) or the `LOCAL_DIRS`
  environment variable chooses their place.

**Throughput.** Every downloaded shard byte is written once to the temp directory, and the
stored-fields file is read back while its documents are decoded. The temp volume should
sustain at least the aggregate decode rate: about 40 MB/s per concurrently decoded shard,
170-360 MB/s per shard with `--shard-threads`, and up to twice that when shards are larger
than the page cache, because the read-back then comes from disk. On EC2 use instance-store
NVMe or gp3 with provisioned throughput (500 MiB/s or more). A baseline gp3 volume
(125 MiB/s) caps an export at about 0.45 TB per hour. These figures are modelled from
measured decode rates, not measured on EBS.

**Orphaned Parquet files.** All files of a shard are committed in one Delta transaction at the
end of the shard. A crash or a stop (`kill -9`, OOM kill, SIGTERM, SIGINT, or a fatal error while
other shards are in flight) therefore leaves every data file the in-flight shards had staged, up
to a whole shard's Parquet output per shard in flight. The Delta log never references them, so they
never affect results, and a resumed run writes new files. Remove them with Delta's vacuum:
Spark `VACUUM <table> RETAIN 168 HOURS`, or delta-rs vacuum in full mode with its default
retention (its default lite mode only removes files the log once referenced). Never set the
retention below the longest time a shard can take from its first staged file to its commit,
and never disable the retention check while a writer may be active: vacuum would then delete
a running export's staged files. Gefrierschrank has no vacuum command of its own. Incomplete
multipart uploads need the lifecycle rule described under [Parquet output](#parquet-output).

## Compatibility notes

- **ZSTD segments on hardened hosts.** ES ≥ 8.14 `best_compression`, logsdb and TSDB indices store
  fields (and, on 9.3, binary doc values) with ZSTD. Gefrierschrank decodes them with zstd-jni, which
  extracts its native library into `java.io.tmpdir`. Where that directory is mounted `noexec` (hardened
  hosts, Docker `--tmpfs /tmp`, read-only images) or JNI is unavailable, it falls back to a pure-Java
  decoder automatically, prints one `zstd-jni native library unavailable (...)` line to stderr and keeps
  going; ZSTD decoding is then roughly 1.7x slower per block. To restore native speed pass
  `-DZstdTempFolder=<exec-capable dir>` (or `-DZstdNativePath=<path to libzstd-jni>`);
  `-Dgefrierschrank.zstd.decoder=java` forces the pure-Java decoder. Both decoders produce identical
  bytes; the repository CRC32 check still guards against corruption.
- **`_source` stored as SMILE, CBOR or YAML.** Elasticsearch stores `_source` in the content type of
  the indexing request. Each document's format is detected as Elasticsearch 9.3 detects it, and a
  SMILE, CBOR or YAML document is exported as the compact JSON text Elasticsearch returns for it
  (`GET <index>/_source/<id>` with a JSON `Accept` header): key order kept, binary numbers as their
  shortest decimal text, binary values as base64, NaN and infinities as the strings `"NaN"`,
  `"Infinity"` and `"-Infinity"`. Such rows are marked `_source_kind` `stored-smile`, `stored-cbor` or
  `stored-yaml` and are not byte-exact; queries that use `_source_kind = 'stored'` to mean "not
  reconstructed" should use `LIKE 'stored%'`. Per shard, `dump-to-delta` prints one
  `note: shard <n> of <index>: <a> SMILE, <b> CBOR, <c> YAML documents converted to JSON` line with
  counts only, and the run report's per-shard field `convertedSources` holds their total (present
  only when it is above 0). Where Elasticsearch itself cannot render a document, the export still
  writes one: duplicate keys are kept, a CBOR decimal fraction is written as its decimal text, and
  YAML `.nan`/`.inf` become `"NaN"`/`"Infinity"`. YAML stored without a leading `---` cannot be
  detected (Elasticsearch's GET fails as well) and fails its shard as in 1.1.0, like any other
  content that is not a JSON object; so does a document that cannot be parsed in its format
  (`SourceConversionException`). 1.1.0 quarantined every shard holding such a document; rerunning
  those shards commits them.
- **ES 9.3 logsdb/TSDB binary doc values.** Segments written or merged by ES 9.3 compress BINARY doc
  values (wildcard, histogram, binary, range types, counted_keyword, keyword values over
  `ignore_above`) with ZSTD. Synthetic-source reconstruction reads them; 1.0.0 quarantined every such
  shard with a `NullPointerException`, so rerunning 1.0.0-quarantined shards now commits them.
- **Vectors excluded from `_source` (ES 9.2+).** Indices created on ES 9.2 or later keep indexed
  `dense_vector` and stored `sparse_vector` values out of the stored `_source` by default
  (`index.mapping.exclude_source_vectors`). `dump-to-delta` and `dump` put them back per shard
  through Elasticsearch's own loaders, as Elasticsearch's fetch with
  `"_source": {"exclude_vectors": false}` does: dense values as Elasticsearch renders them (cosine
  vectors come back renormalized, a few float ulps from what the client sent; byte and bit vectors
  as integers), sparse weights as Elasticsearch decodes them. A document without vectors keeps its
  stored bytes and `_source_kind` `stored`; root-level vectors are appended inside the stored
  object after every stored byte (`stored+vectors`); vectors under an object or nested field give
  Elasticsearch's rendering of the whole document (`stored+vectors-reserialized`). Neither is
  byte-exact, and `dump` writes the same text. `rank_vectors` and `semantic_text` kept out of
  `_source`, or an exclusion Elasticsearch's mapper code cannot evaluate, still fail with
  `ExcludedSourceVectorsException` before any table, metadata row or download (`--index`: exit 1;
  `--all-indices`: `FAILED`, exit 4), as does every affected index with
  `-Dgefrierschrank.vectors.rehydrate=false`, which restores 1.1.0's refusal. Each file of a
  rehydrating index is tagged `gefrierschrank.sourceVectors=rehydrated-v1`; a table that 1.0.0
  wrote for such an index (rows without their vectors, files without the tag) is refused with
  `VectorlessTableException` instead of being completed: the run prints `Exporting ...`, may add
  the snapshot's missing `_export_metadata` row, writes no document row and fails the index.
  Export it to a new output root. Indices with the setting `false`, synthetic source, `index: false`
  dense vectors or `store: false` sparse vectors keep their vectors in `_source` and export
  byte-exact as before. `survey` shows rehydrated indices as `exportable` with a note
  naming how many vector fields come back; only indices that keep a stored `_source` become
  exportable through rehydration (one whose `_source` is disabled keeps `warn_source_disabled_es8`:
  its documents without stored `_source` have nothing to put vectors into).
  `not_exportable(excluded_vectors)` is left for the refused ones.
- **Unexportable indices fail before download.** An index created by Elasticsearch 6.x or older
  (`index.version.created` below 7000000), one with `_source.enabled: false` in a snapshot written
  by ES 7 or older (which cannot flip that setting), and one created before 8.0 with
  `index.mapping.source.mode: disabled` fail with `IndexCreatedTooOldException` or
  `SourceDisabledException` before any table, metadata row or download (`--index`: exit 1;
  `--all-indices`: `FAILED`, exit 4); so does an ES 8+ index created with
  `index.mapping.source.mode: disabled` whose mapping the reconstruction coverage rule refuses
  (`UnreconstructableSourceException` with the mapping's type tokens, e.g. `object:enabled-false`).
  Earlier releases downloaded and quarantined every shard on every run (exit 3), and completed an
  empty such index with 0 rows; a table an earlier release completed with 0 rows is left
  untouched, but its index now reports `FAILED` on every run, so an `--all-indices` run that used
  to exit 0 or 3 exits 4. Every other ES 8+ index created with `index.mapping.source.mode: disabled`
  exports all its documents as reconstructed `synthetic` rows (a shard whose segments hold a field
  without doc values or stored field quarantines after its download), and ES 8+ accepts
  `_source.enabled: false` on an existing index, whose documents written since are reconstructed
  too (see *Nested objects and `_source` disabled later*). With `--selective-fetch`, a shard of
  such an index that reconstruction would refuse is quarantined after its commit files only
  (`fetch: preflight` in the run report), with the same error class and message as after a full
  download; a keyword sub-field added to a text field later (see below) is the exception, found
  only by the read after the download.
- **`survey` verdicts and CSV columns.** `survey` now classifies with the export's own checks:
  `exportable`, `exportable_synthetic` (reconstructed, not byte-exact), `mounted` (sized and
  classified by its backing snapshot; `other_repository` when that lives elsewhere and needs
  `--repository`), `not_exportable` with a reason (`too_old`, `source_disabled`,
  `excluded_vectors`, `semantic_text_synthetic`, `unsupported_synthetic_type`, or
  `backing_missing` for a mount whose backing snapshot was deleted from this repository),
  `warn_source_disabled_es8` and `unknown`. The CSV header is now
  `index,verdict,reason,shards,bytes,backing,group,note` (was `index,verdict,shards,bytes,note`);
  `bytes` is empty with `--no-sizes`, and `group` names the backing identity shared by an
  original and its mounted shells, which `dump-to-delta --all-indices` exports once per name
  unless `--mounted-shells skip-if-original` is given.
  Indices with vector fields are checked with Elasticsearch's own mapper code, loaded once per run
  (about 1.5 s and up to about 1.3 GB resident memory on a repository of 2,000 vector indices).
  `field-types` gains a `SYNTHETIC` column. `info` prints state names and each snapshot's
  `index_version` (ES records `version` 8.11.0 for every snapshot since 8.11).
- **Nested objects and `_source` disabled later.** Synthetic-source roots export with their nested
  objects exactly as Elasticsearch's GET renders them (one object stays an object, several become
  an array in indexed order, an empty or null array is omitted); nested children never become rows,
  and `survey` reports such indices `exportable_synthetic` (1.1.0 quarantined their shards with a
  `NullPointerException`). Documents without stored `_source` of an ES 8+ index whose `_source`
  was disabled later, or that was created with `index.mapping.source.mode: disabled`, are rebuilt
  through Elasticsearch's own synthetic loaders and exported as `synthetic` rows (not byte-exact;
  arrays sorted and de-duplicated as in any synthetic export; stored documents stay byte-exact)
  when every field holding data has doc values or a stored field the mapping stores. Otherwise
  the shard quarantines with `UnreconstructableSourceException`, whose message names
  Elasticsearch type tokens, never field names: `<type>:no-native-source` (e.g. text without
  keyword sub-field or `store`), `<type>:fallback` (e.g. `geo_shape`, keyword without doc values),
  `<type>:copy_to`, `<type>:not-indexed` (e.g. binary without doc values), `object:enabled-false`,
  `dynamic:false`, `dynamic:runtime`, `runtime:shadowing`, `flattened:ignore_above`,
  `nested:keep-all`, `<type>:diverted`, `unmapped`. A segment that mixes stored and source-less
  documents after a merge is judged by all of its fields. A text or `match_only_text` field whose
  keyword sub-field was added to the mapping after some documents were written refuses with
  `<type>:no-native-source` when a live document of a segment holds a text value that the
  sub-field does not hold (its value would otherwise be missing uncounted; a stored-only
  sub-field, without doc values or index, cannot be checked per document and is trusted); with
  `--selective-fetch` this refusal comes from the read, after the download, never from the
  commit-point preflight (`fetch: preflight`), because the commit files cannot show it. A value
  without tokens (`""`, `"-"`) of such a field without norms (`match_only_text`, `norms: false`)
  is found through `_field_names`, which indices created by 8.0 or later (the only ones
  reconstructed) cannot disable; a document without stored `_source` of an index created by 7.x
  quarantines its shard with `SourceNotSyntheticException`. A field whose every value was deleted
  and merged away does not refuse. A mapping with an index-time `script` or a `normalizer` (even
  the built-in `lowercase`) cannot be built by the isolated MapperService, which has no script
  compiler and no analysis modules: the shards of such an index that hold documents without
  stored `_source` quarantine (one created with `index.mapping.source.mode: disabled` fails before
  download), as synthetic-source indices with such mappings already did in 1.1.0. Values
  Elasticsearch ignored at index time (`ignore_malformed`, `ignore_above`, the field limit) were
  kept only in the disabled `_source` and are missing from reconstructed rows; a text field whose
  keyword sub-field ignored
  a value (the default dynamic string mapping does so above 256 characters) is missing as a whole.
  Such documents are counted per shard (report field `ignoredFieldDocuments`, a content-free
  stderr note). A `constant_keyword` with a value appears in every reconstructed row, so such rows
  may gain the field although the document never sent it. `dump-to-delta --source-disabled-accept
  unmapped-values` also exports shards whose only refusal is `dynamic:false`/`dynamic:runtime`:
  values sent under such objects are then lost uncounted, announced by one warning per index and
  flagged per shard (`unrecoverableUnmappedValues: true`).
  `-Dgefrierschrank.synthetic.reconstructDisabledSource=false` restores 1.1.0's refusals. `survey`
  keeps `warn_source_disabled_es8` for both kinds of disabled source, with a new note.

## Architecture

Five Maven modules:

| Module | Contents |
|---|---|
| `gefrierschrank-core` | Lucene-free: repository/snapshot format decoding (SMILE + the ES `ChecksumBlobStoreFormat` envelope), stored `_source` format detection and SMILE/CBOR/YAML to JSON conversion, `BlobStore` abstraction (local filesystem + S3), index mapping parsing |
| `gefrierschrank-lucene10` | In-process reader for every supported shard: ES7 (Lucene 8, opened read-only with an index-created floor of Lucene 8), ES8 (Lucene 9) and ES9 (Lucene 10, native); one sequential stored-field visit per document |
| `gefrierschrank-synthetic-source` | Reconstructs `_source` for documents that don't store it at all (synthetic source — see below). **Apache-2.0 does not apply to this module** — see [License](#license) |
| `gefrierschrank-delta` | Delta Lake writer: schema mapping, Parquet encoding via parquet-mr, resumability/quarantine/report logic |
| `gefrierschrank-cli` | picocli commands; reads every shard in-process on Lucene 10 |

**Why one Lucene runtime reads every supported Elasticsearch version:**
Elasticsearch versions write different Lucene major versions (7.x → Lucene
8, 8.x → Lucene 9, 9.x → Lucene 10 — Elasticsearch 9 does write native
Lucene 10 segments). Lucene 10 opens indices created up to two majors back,
read-only, so every shard is read in-process against Lucene 10: the reader
opens the latest commit with an index-created floor of Lucene 8, which also
covers ES7-created indices whose segments were all rewritten under ES8.
Indices created by ES6 (Lucene 7) fail before any download with
`IndexCreatedTooOldException` (see [compatibility notes](#compatibility-notes)).

Up to 1.1.0, a separate Lucene 9 worker JVM (module `gefrierschrank-lucene9`) could
still read shards with Lucene 8 files behind the deprecated
`-Dgefrierschrank.lucene8.runtime=worker` opt-out. The worker was removed in 1.2.0.
The property is still recognised so that a deployment that sets it learns of the
removal: with `worker`, `dump-to-delta` and `dump` exit with code 2 and
`the Lucene 9 worker was removed in 1.2.0` before the repository is read; any value
other than `in-process` is refused the same way.

**Why Elasticsearch's proprietary postings/doc-values formats needed stub
codecs:** Elasticsearch registers its own Lucene `PostingsFormat` and
`DocValuesFormat` implementations (e.g. `ES87BloomFilter`, `ES812Postings`,
`ES819TSDB`) for the `_id` field, time-series metrics, and similar, which
aren't present in stock Lucene. Gefrierschrank ships minimal read-only SPI
stub implementations that delegate to the underlying standard Lucene
format, just enough to read documents back out — it does not reimplement
ES's bloom-filter optimization, TSDB compression, or anything write-side.

**Synthetic source — why one module needs the real Elasticsearch, not a
stub:** some indices (commonly TSDB/time-series data streams) use
"synthetic source": `_source` is never stored at all, and Elasticsearch
reconstructs it on the fly from doc values at query time. A read-only stub
can't do this — reconstruction genuinely requires Elasticsearch's own field
mapper logic (which field types get which doc-values encoding, and how
each one is turned back into JSON). `gefrierschrank-synthetic-source`
depends directly on `org.elasticsearch:elasticsearch` and builds a
standalone `MapperService` (no running ES node, no cluster) to do exactly
that reconstruction. Because Elasticsearch's own jar also registers Lucene
SPI providers under the *same* names as this project's read-only stubs
(`ES819TSDB` etc.) — and a JVM's `ServiceLoader` resolution for a given SPI
name is decided once and cached — this module loads Elasticsearch's classes
through a private, isolated classloader rather than the ordinary classpath,
so the real ES819TSDB decoder is used only for this one reconstruction path
while every other shard in the same process keeps resolving to the safe,
lightweight stubs it already depended on. That isolated classloader is created
once per process and shared by every synthetic shard, including concurrent ones,
and by the excluded-vectors check and vector rehydration; each shard builds only its own
`MapperService` and reconstructs segment by segment in document order, in
batches of consecutive documents (up to 1024 or 8 MiB) so that doc values are
loaded once per run of documents, as in Elasticsearch's fetch phase. The text
is the same as 1.0.0 wrote: Elasticsearch's own rendering is passed through
only where it is provably identical.
Vector rehydration opens one session per shard in the same isolated world (the
pruned vector mapping and a plain reader of the shard), and each range of the
shard reads its documents' vectors through a cursor of its own, on its own thread.

Synthetic-source shards are reconstructed from memory-mapped files, the way Elasticsearch reads
them. Each open synthetic-source shard (and each range of one read with `--shard-threads`)
reserves virtual address space of about its own size (not resident memory); where mapping fails,
for example under a `ulimit -v`, the shard is read through ordinary file reads instead, and
`-Dgefrierschrank.reconstruction.directory=niofs` does that for every shard. Any other value
than `mmap` or `niofs` is rejected before the repository is read.

## Development notes

- The project follows a functional-core/imperative-shell style:
  `BlobStore` is the only I/O seam for repository access, so decoding logic
  is testable without any S3/filesystem in the loop.
- `gefrierschrank-core/src/test/resources/fixtures/demo-repo` holds a small,
  real snapshot repository (ES 7.17.9, 8.17.0, and 9.3.0, ~650KB total,
  generated via `scripts/generate_test_snapshots.py` against real ES
  containers) used by most tests instead of hand-built mock data.
- TDD throughout: most non-trivial logic was built test-first against this
  real fixture data or against a live MinIO instance, not mocks.

## Running this against a large repository

If you have terabytes in a frozen tier and want to know what is in there, what is extractable and
what it would take to move it into a lakehouse, I run inventories and exports as a service —
on your infrastructure, with read-only credentials, nothing leaves your account.
Details: https://sloow.de/gefrierschrank/#service · elias@sloow.de

## License

Apache License 2.0 — see [LICENSE](LICENSE) — applies to every module **except
`gefrierschrank-synthetic-source`**.

That module depends directly on `org.elasticsearch:elasticsearch` 9.3.0. Elastic
makes Elasticsearch available under a choice of AGPL-3.0, SSPL-1.0, or the Elastic
License 2.0 — not Apache-2.0. The module declares **AGPL-3.0** (the OSI-approved
option among those three) — see
[synthetic-source LICENSE](gefrierschrank-synthetic-source/LICENSE).

**What this means plainly, if you distribute or run Gefrierschrank as a
network service with that module included:** AGPL-3.0's source-availability
obligation (including its network-use clause, §13) attaches to the combined
work. If you only use the other four modules (`core`/`lucene10`/`delta`/`cli`),
none of that applies; those stay plain Apache-2.0. If your use is purely internal
(you don't distribute the binary or offer it as a network service to anyone
outside your own use), AGPL's obligations don't trigger regardless of which
modules you include.

**A second, separate, MORE restrictive license also applies to part of
`gefrierschrank-synthetic-source`.** That module additionally vendors real
Elasticsearch x-pack source (the `aggregate_metric_double`, `histogram`,
`constant_keyword`, `wildcard`, `version`, `counted_keyword`, `unsigned_long`,
`shape`, `geo_shape` and `point` field-type mappers, under
`src/main/java/org/elasticsearch/xpack/`; 1.1.0 shipped the last six without
naming them in this notice, although their files carried the Elastic License 2.0
header) that Elastic licenses under the
**Elastic License 2.0 ONLY** — no AGPL-3.0/SSPL-1.0 alternative, unlike the
tri-licensed `mapper-extras` vendoring (`scaled_float` and friends) that the
rest of that module's AGPL-3.0 declaration covers. EL2.0 is source-available,
not OSI-approved open source, and specifically forbids offering the software
to third parties as a hosted/managed service — a restriction AGPL-3.0 does not
itself impose. See
[LICENSE-ELASTIC-2.0](gefrierschrank-synthetic-source/LICENSE-ELASTIC-2.0) —
a separate file from that module's `LICENSE`, deliberately not merged into
it — for the full text and exactly which files this covers.
