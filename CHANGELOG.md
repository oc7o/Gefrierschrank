# Changelog

## 1.2.0 (2026-10-10)

The coverage release: five topics (selective fetch, mounted shells, non-JSON stored `_source`,
vector rehydration, synthetic coverage) and the removal of the Lucene 9 worker. Indices and
shards that 1.1.0 refused or quarantined now export where Elasticsearch itself can still render
their documents.

### Performance

Measured against 1.1.0 on the same machine in one sitting ([BENCHMARK.md](BENCHMARK.md), medians
of 3, local MinIO, `-Xmx4g`, the ES route measured in the same sitting):

| dataset | 1.1.0 | 1.2.0 | 1.2.0 `--selective-fetch` | ES restore + PIT export |
|---|---|---|---|---|
| standard, 2.52M documents, 11 indices (ES 9.3 / 8.17 / 7.17), default `--parallelism 1` | 36.0 s | 36.2 s | 36.3 s | 50.6 s |
| same, `--parallelism 4` | 20.7 s | 20.9 s | 20.5 s | |
| frozen tier at smoke scale, 442,500 documents, 29 single-shard indices, `--parallelism 1` | 15.9 s | 14.9 s | 14.8 s | 11.4 s |
| same, `--parallelism 4` | 10.8 s | 10.5 s | 10.7 s | |

- Data that 1.1.0 already exported runs at the same speed (summed per-index time of the 10
  indices both builds export on the standard dataset: 0.98x at `--parallelism 1`, 1.00x at 4;
  frozen-tier differences are within the noise). The 1.2.0 totals include `vec-default`, which
  1.1.0 refuses: 50,000 documents with rehydrated vectors in about 1 s, +52 GETs and +46 MB read.
- `--selective-fetch` reads 4.8% fewer bytes and 14% fewer GETs on the standard dataset, all on its
  ES 7.17 indices (-20% and -24%); every ES 8.17/9.3 shard of the benchmark datasets is one compound
  segment and is downloaded whole (no saving, no cost). On non-compound ES 9.3 synthetic-source
  segments (bench/synthfetch) it reads 37% (logsdb), 31% (TSDB) and 18% (nested) fewer bytes, with
  identical tables. On the local MinIO wall time and CPU do not change; the saving is bytes,
  requests and temp disk.
- New paths cost: rehydrating vectors 1.7-1.9x the CPU of exporting the same documents with
  stored vectors (2.4-2.5x for `sparse_vector`) and 0.5-1.0 GiB more peak RSS; converting SMILE
  and CBOR `_source` 0.57-0.61x of the JSON end-to-end rate (about 4-5 us per document), YAML
  0.12x (about 45 us); `--mounted-shells skip-if-original` 0.65x the wall time and 0.82x the CPU
  of the default on a 16-index ILM repository (0.80x and 0.80x at `--parallelism 4`).

### Features

- New `dump-to-delta --selective-fetch` (default off; `-Dgefrierschrank.download.selective=true`,
  `true` or `false`, anything else exit 2): for stored-source indices, only the files the export
  reads are downloaded (segment and field infos, stored fields, live documents, doc values); the
  postings, points, norms, term vectors and vector files of non-compound segments stay in the
  repository and are neither downloaded nor verified. Compound segments, i.e. every Elasticsearch
  8/9 segment below 1 GB, are downloaded whole. Every shard first downloads its commit files
  (`segments_N`, `.si`, `.fnm`, compound files), which adds one round trip per shard. A shard whose
  documents are rebuilt from doc values and stored fields (synthetic source, `_source` disabled
  later or from creation) then downloads what every plan keeps and only the index structures
  Elasticsearch's own loaders read for it, found by reconstructing the first and last 64 live
  documents of every segment with the missing structures recorded (nested objects and
  `_doc_count`: postings; `dense_vector`: vector files; `sparse_vector`: postings and term vectors;
  documents rebuilt without stored `_source` that have a text field with a keyword sub-field:
  postings and norms); it is probed only when a non-compound segment holds at least 16 MiB of
  such structures (`-Dgefrierschrank.download.selective.syntheticMinBytes`, an invalid value is
  exit 2), and the reason a candidate was fetched in full is printed in a content-free note
  (`vectors`, `below-threshold`, `legacy-nested`, `probe-failed`). Indices whose vectors are read
  from the vector files and segments that mark nested documents without `_primary_term` take the
  full download. A selectively fetched shard that fails to open or to export, or reads a structure
  left out, downloads and verifies the rest and is read once more in the same run
  (`fetch: completed`); only a second failure quarantines it, and rows are never affected. A shard
  whose full open fails only because Lucene cannot load one of its postings or vector formats may
  export, since the pruned open never resolves those formats. Under `--temp-budget` a stored-source
  shard is admitted by its selected size (a completion can exceed the lease by its skipped bytes),
  a probed candidate by its full size. `dump` has no such option and keeps the full download. The
  checksum guarantee now reads: every file the export reads is verified against the repository's
  length and CRC32 before Lucene opens it; without `--selective-fetch` that is still every file
  of the shard.
- `dump-to-delta --all-indices --mounted-shells skip-if-original` writes one table per
  exact-identity group: a mounted searchable-snapshot shell whose original index is exported in
  the same run from exactly the same backing index, snapshot and shard count (the groups `survey`
  reports, taken on the snapshot the export selects) gets no table, and the manifest records it
  with the outcome `ALIAS_OF` and `aliasOf` naming the original, whose table holds its documents
  (with `_index` naming the original). The manifest gains `aliasCount`. A shell is exported as
  before when its original is not in the run, fails outright or exports another snapshot, or
  when the shell already has a table. On an ILM frozen tier where every original is still
  retained this halves the bytes read, the rows and the output (16-index ILM repository: 0.65x
  wall and 0.82x CPU at `--parallelism 1`, 0.80x and 0.80x at 4). The default, `--mounted-shells export`, is unchanged; the
  option requires `--all-indices` (exit 2 otherwise). `ALIAS_OF` is neutral for the exit code
  (the original's own entry carries any 3 or 4); automation that switches on the manifest's
  `outcome` must know it before turning the option on.
- **Vector rehydration.** Indices created on Elasticsearch 9.2+ that keep `dense_vector` (float,
  byte, bit) or `sparse_vector` values out of the stored `_source`
  (`index.mapping.exclude_source_vectors`, the default) are now exported with their vectors put
  back by Elasticsearch's own loaders, as its fetch with `"_source": {"exclude_vectors": false}`
  returns them; 1.1.0 failed such indices with `ExcludedSourceVectorsException`. Rows say how:
  `_source_kind` `stored` (no vectors in that document, byte-exact), `stored+vectors` (root-level
  vectors appended inside the stored object after every stored byte) or
  `stored+vectors-reserialized` (vectors below the root: Elasticsearch's rendering of the whole
  document). Rehydrated values are Elasticsearch's: cosine vectors come back renormalized (a few
  float ulps from what the client sent), byte and bit vectors as integers, sparse weights as
  Elasticsearch decodes them. Several vector fields of one object are appended in path order
  (Elasticsearch's own order is randomized per node). `dump` writes the same text. The column
  schema is unchanged. Such indices take the full download. Cost, measured against 1.1.0
  exporting the same documents with their vectors stored: 1.7-1.9x the CPU (2.4-2.5x for
  `sparse_vector`), and 0.5-1.0 GiB more peak RSS per process (about 3.3x at one range per shard,
  1.7-2.2x at four), for the isolated Elasticsearch classes and the memory-mapped vector files.
  `-Dgefrierschrank.vectors.rehydrate=false` restores 1.1.0's refusal of every affected index.
- `dump-to-delta --source-disabled-accept unmapped-values` (or
  `-Dgefrierschrank.synthetic.sourceDisabledAccept=unmapped-values`, which `dump` honours too):
  shards whose only refusal is `dynamic:false`/`dynamic:runtime` export; values sent under such
  objects are lost uncounted, announced by one content-free warning per index before its
  download and flagged per shard (`unrecoverableUnmappedValues: true`). Default off; any other
  value is a usage error (exit 2).

### Behaviour changes

Default runs, without any new option, change only in these ways:

- Synthetic-source indices with nested objects export: roots carry their nested objects exactly as
  Elasticsearch's GET renders them, nested children never become rows (1.1.0 quarantined every
  such shard with `NullPointerException`). `survey` reports them `exportable_synthetic`; the
  `nested_synthetic` reason is gone; `field-types` marks `nested` exportable. Tables 1.1.0 left
  with quarantined nested shards complete on resume. There is no switch.
- Documents without stored `_source` of ES 8+ indices whose `_source` was disabled after creation
  are reconstructed by default through Elasticsearch's synthetic loaders and exported as
  `synthetic` rows (not byte-exact: arrays sorted and de-duplicated, nulls and empty values
  omitted) when every field holding data has doc values or a stored field the mapping stores;
  stored documents stay byte-exact. A shard that cannot be reconstructed quarantines with
  `UnreconstructableSourceException` naming Elasticsearch type tokens (never field names) instead
  of 1.1.0's `SourceNotSyntheticException`. A segment that mixes stored and source-less documents
  is judged by all of its fields. A text or `match_only_text` field whose keyword sub-field was
  added after some documents were written refuses with `<type>:no-native-source` when a live
  document holds a text value the sub-field lacks (with `--selective-fetch` at read time, after
  the download). Values without tokens of such fields without norms are found through
  `_field_names`, which indices created by 8.0+ (the only ones reconstructed) cannot disable;
  documents without stored `_source` of 7.x-created indices still quarantine with
  `SourceNotSyntheticException`. Mappings with an index-time `script` or a `normalizer` cannot be
  built by the isolated MapperService, so such shards still quarantine, as synthetic-source
  indices with such mappings did in 1.1.0.
- ES 8+ indices created with `index.mapping.source.mode: disabled` are reconstructed the same way
  (every row `synthetic`) instead of failing before download with `SourceDisabledException`; they
  fail before download only when their mapping is refused (`UnreconstructableSourceException` with
  mapping-level tokens, `--index` exit 1, `--all-indices` FAILED, exit 4); a refusal that depends
  on a segment quarantines that shard after its download on every rerun (with `--selective-fetch`,
  after its commit files). `SourceDisabledException` (new fixed text) remains for
  `_source.enabled: false` in snapshots of ES 7 or older and for the setting on indices created
  before 8.0; indices created by ES 6.x or older still fail with `IndexCreatedTooOldException`.
  This reverses that part of 1.1.0's refusal before download. `survey` classifies these indices
  `warn_source_disabled_es8` (was `not_exportable(source_disabled)`), with a new note.
- Values Elasticsearch ignored at index time (`ignore_malformed`, `ignore_above`, the field limit)
  were kept only in the disabled `_source` and are missing from reconstructed rows; a text field
  whose keyword sub-field ignored its value (the default dynamic string mapping does so above 256
  characters) is missing as a whole. Such documents are counted per shard (report field
  `ignoredFieldDocuments`, present only when > 0) and noted on stderr without content, once per
  committed shard.
- A `constant_keyword` with a value appears in every reconstructed row, so such rows may gain the
  field although the document never sent it; no shard refuses for it.
- Values under `dynamic: false`/`dynamic: runtime` objects cannot be recovered or counted, so
  source-less shards of such mappings refuse (`dynamic:false`, `dynamic:runtime`) unless
  `--source-disabled-accept unmapped-values` is given.
- `-Dgefrierschrank.synthetic.reconstructDisabledSource=false` restores 1.1.0's refusals for
  source-disabled indices (`SourceNotSyntheticException` per shard, `SourceDisabledException`
  before download for `index.mapping.source.mode: disabled`); nested reconstruction has no switch.
- The residual commit-point preflight, which fails a shard that reconstruction would refuse after
  its commit files only (`fetch: preflight`, same error class and message as after a full
  download), runs only with `--selective-fetch`. Without it, such a shard is downloaded in full
  and quarantined on every rerun (exit 3).
- Run reports: every shard entry gains `bytesFetched` (bytes downloaded for the shard in this
  run, embedded files excluded) and `fetch` (`full`, `selective`, `completed`, `preflight` or
  `none`), always, with or without `--selective-fetch`; a full-fetch run reports its own download
  volume. Consumers that compare reports byte for byte must accept (or mask) the two new keys.
- Run reports: `convertedSources` (SMILE, CBOR and YAML documents converted in a committed shard),
  `ignoredFieldDocuments` (documents rebuilt without stored `_source` whose `_ignored` lists a
  field) and `unrecoverableUnmappedValues: true` (with `--source-disabled-accept unmapped-values`,
  a shard that rebuilt documents whose unmapped values are lost) per shard, each present only when
  set, so reports of JSON-only stored-source data carry only the two keys above. dump-to-delta
  prints the `_ignored` note once per shard after the shard committed, never for an attempt that
  failed or was retried, and not when another writer had already committed the shard; `dump`
  prints it when the shard's reader closes.
- `_source_kind` gains `stored-smile`, `stored-cbor` and `stored-yaml`, and any value can be
  followed by `+vectors` or `+vectors-reserialized`. Only `stored` rows are byte-exact; queries
  that use `_source_kind = 'stored'` to mean "not reconstructed" should use `LIKE 'stored%'`.
- `survey`: rehydrated indices that keep their stored `_source` are `exportable` with the note
  `<n> vector field(s) rehydrated from the vector files (index.mapping.exclude_source_vectors)`;
  one whose `_source` was disabled after creation keeps `warn_source_disabled_es8` (its
  source-less documents have no stored source to put vectors into);
  `not_exportable(excluded_vectors)` remains for the refused ones.
- Every Parquet file of a rehydrating index carries the AddFile tag
  `gefrierschrank.sourceVectors=rehydrated-v1`. A table 1.0.0 wrote for such an index (rows
  without their vectors, files without the tag) is refused with `VectorlessTableException`
  (`--index` exit 1, `--all-indices` FAILED, exit 4) after the `Exporting ...` line and the
  `_export_metadata` step, before any shard is read; export the index to a new output root.
  Tables of every other index keep exactly 1.1.0's tags.
- errorClass changes in reports and manifests: nested-under-synthetic shards
  `NullPointerException` -> committed; shards of ES 8+ indices with `_source` disabled later
  `SourceNotSyntheticException` -> committed or `UnreconstructableSourceException` (a mixed
  stored/disabled shard that refuses, too); ES 8+ indices created with
  `index.mapping.source.mode: disabled` `SourceDisabledException` (FAILED) -> committed, FAILED or
  QUARANTINED with `UnreconstructableSourceException`; SMILE/CBOR/YAML shards
  `JsonParseException` -> committed, or `SourceConversionException` for a document that cannot be
  parsed in its format; excluded-vector indices `ExcludedSourceVectorsException` (FAILED) ->
  committed, with rehydration failures quarantining the shard as `VectorRehydrationException` or
  `SourceVectorsMismatchException` (fixed, content-free messages; the next run retries). With
  `--selective-fetch` only: `PrunedFormatAccessException` when a shard read a structure that was
  not downloaded and its in-run retry failed too. Every new exception has a fixed, content-free
  message.

### Removed

- The Lucene 9 worker module (`gefrierschrank-lucene9`), deprecated in 1.1.0, is removed; every
  shard is read in-process, as by default since 1.1.0, and the CLI jar is all a deployment needs.
  `-Dgefrierschrank.lucene8.runtime=worker` now fails `dump-to-delta` and `dump` before the
  repository is read with exit 2 and `the Lucene 9 worker was removed in 1.2.0`; any other value
  than `in-process` is the same usage error (1.1.0: exit 1 with `InvalidRuntimeSettingException`).
  There is no downgrade path: nothing promises that an earlier release can read or resume a table
  1.2.0 wrote.

### Fixes

- Stored `_source` written as SMILE, CBOR or YAML no longer quarantines its shard. Each document's
  format is detected as Elasticsearch 9.3 detects it, and the row holds the compact JSON text
  Elasticsearch returns for that document (byte-equal to its `GET <index>/_source/<id>` on the
  fixture), with `_source_kind` `stored-smile`, `stored-cbor` or `stored-yaml`. These rows are not
  byte-exact; `stored` rows stay the stored bytes. Per shard, `dump-to-delta` prints one
  `note: shard <n> of <index>: <a> SMILE, <b> CBOR, <c> YAML documents converted to JSON` line
  (counts only), and the run report's per-shard field `convertedSources` holds the total (present
  only when above 0). `dump` writes the same JSON. Shards that 1.1.0 quarantined for such documents
  commit when rerun. YAML stored without a leading `---`, which Elasticsearch cannot detect either,
  and any other content that is not a JSON object still fail their shard; a document that cannot be
  parsed in its format fails it with `SourceConversionException`.
- `--version` printed `gefrierschrank 1.0.0` in every release so far; it now prints the build's own
  version and build time, the line `dump-to-delta` logs at start.
- The license notice of `gefrierschrank-synthetic-source` was incomplete in 1.1.0: besides the
  `aggregate_metric_double`, `histogram`, `constant_keyword` and `wildcard` mappers, the module
  has also vendored the `version`, `counted_keyword`, `unsigned_long`, `shape`, `geo_shape` and
  `point` mappers since 1.1.0. Their files always carried Elastic License 2.0 headers, but the
  README, the module's `LICENSE`, `LICENSE-ELASTIC-2.0` and its pom named only the first four.
  All four now list every vendored Elastic-License-2.0-only package. Nothing was relicensed; if
  you offer the tool to third parties as a hosted or managed service, the Elastic License 2.0
  restriction applies to these mappers as it always did.
- README: a quick start from a fresh clone to a Delta table, using the bundled demo repository.

### Dependencies

- `jackson-dataformat-cbor` and `jackson-dataformat-yaml` 2.17.1 (with snakeyaml 2.2) are added to
  the CLI jar for the conversion.

### Benchmark harness

- `gsbench digest` / `compare-digests` and `bench/fixture-digests.sh` compare two exports table by
  table without an oracle; `gsbench fetch-modes` sums the `fetch` modes and `bytesFetched` of run
  reports; `gsbench shadow` compares Delta tables with a read-only restore's fetch (counts only).
- New harnesses: `bench/shells` (mounted shells), `bench/vectors` (rehydration throughput and
  heap), `bench/synthcov` (nested and source-disabled reconstruction), `bench/synthfetch`
  (selective fetch of synthetic-source shards).
- `bench/expected-failures.yaml` expects `vec-default` to fail loudly only for the 1.1.0 baseline.

### Known limitations

- `--selective-fetch` is off by default: without it every file of a shard is downloaded and
  verified. With it, compound segments (every Elasticsearch 8/9 segment below 1 GB) still download
  whole.
- `rank_vectors` and `semantic_text` kept out of `_source`, and exclusions Elasticsearch's mapper
  code cannot evaluate, still fail with `ExcludedSourceVectorsException` before any table or
  download; its message now says so.
- Values Elasticsearch did not keep cannot be recovered in reconstructed documents: ignored values
  (counted), values under `dynamic: false`/`dynamic: runtime` objects (refused, or lost uncounted
  with the opt-in), and original formatting and array order.
- A keyword sub-field added to a text field later is found only by the read after the full
  download, even with `--selective-fetch`.
- Only MinIO was exercised as the S3 source and target; real S3, EBS and Linux were not measured.

## 1.1.0 (2026-10-09)

The performance release: measured work on top of 1.0.0. The end-to-end measurement is in
[BENCHMARK.md](BENCHMARK.md).

### Performance

Measured against 1.0.0 on the same machine (`BENCHMARK.md`, medians, local MinIO, `-Xmx4g`):

| dataset | 1.0.0 | 1.1.0, `--parallelism 4` | ES restore + PIT export |
|---|---|---|---|
| standard, 2.52M documents, 11 indices (ES 9.3 / 8.17 / 7.17) | 367 s | 24 s | 58 s |
| frozen tier, 4.43M documents, 29 single-shard indices | 1171 s | 22 s (with `--shard-threads auto`) | 71 s |

- Stored fields are read once per document through Lucene's sequential (merge) reader instead of
  twice through the random-access reader; ZSTD `best_compression` shards gain the most (48x per
  document on one core).
- The archive row path hands the reader's bytes to Parquet unchanged when a strict validator
  accepts them; only non-canonical documents take the old per-document path.
- `--parallelism N` bounds shards in flight across the whole run, so several indices run at once;
  `--shard-threads N|auto` reads one large shard as contiguous document ranges on several threads.
- Shard files download on 8 concurrent streams (ranged 16 MiB reads above 64 MiB, pinned by ETag).
- Synthetic-source reconstruction runs in batches of consecutive documents (up to 1024 or 8 MiB)
  from memory-mapped files, with Elasticsearch's own rendering passed through where it is provably
  identical to the 1.0.0 text.
- Output is ZSTD-compressed Parquet in 256 MiB files with 32 MiB row groups (1.0.0: uncompressed,
  10,000 rows per file). S3A uploads use 16 MiB parts with 8 active blocks, buffered in the run's
  temp root, where the user configured nothing else.

### Behaviour changes

- `--shard-threads` defaults to `auto` (cores divided by `--parallelism`, at most 8); a shard is
  split only when every range holds at least 250,000 live documents. Keep
  `--parallelism x --shard-threads x 2.5 x the largest document` below the heap, or pass
  `--shard-threads 1`.
- A resumed table is completed from the snapshot it was started from while that snapshot is still
  retained (`--snapshot auto`, the default); the report sets `newerSnapshotAvailable` and the
  manifest counts `behindLatestCount`. `--snapshot latest` restores the 1.0.0 refusal. A mounted
  index's table whose shell snapshot retention deleted is completed from the unchanged backing
  snapshot.
- Indices that can never export fail before any download: created by Elasticsearch 6.x or older,
  `_source` disabled, or vectors excluded from the stored `_source` (ES 9.2+). `--index` exits 1,
  `--all-indices` marks the index FAILED and exits 4, where 1.0.0 downloaded and quarantined every
  shard (exit 3) or silently exported rows without their vectors.
- A target that cannot store data (disk full, quota, I/O error) ends the run with exit 1 and one
  `TargetStorageException` line instead of quarantining shard after shard.
- Documents too large for the heap wait on disk and, when they can never fit, quarantine their
  shard with `DocumentTooLargeException` instead of ending the run with an `OutOfMemoryError`.
- One writer per local table: a second process fails at once with `TableLockedException`
  (`<table>/_gefrierschrank.lock`). On `s3a://` targets a second writer is not detected; keep to
  one process per table.
- Temporary files live in `<java.io.tmpdir>/gefrierschrank-run-<random>`, deleted on exit, signal
  and fatal error; dead roots of crashed runs are removed at the next start after 10 minutes.
  Leftovers of earlier versions are reported, never deleted.
- `survey` classifies with the export's own checks; verdicts are `exportable`,
  `exportable_synthetic`, `mounted(...)`, `not_exportable(<reason>)`, `warn_source_disabled_es8`
  and `unknown`, and the CSV header is `index,verdict,reason,shards,bytes,backing,group,note`
  (was `index,verdict,shards,bytes,note`). Scripts that parse the CSV positionally need updating.
- `dump` plans like `dump-to-delta`: a mount name exports the mounted snapshot's documents,
  synthetic indices export reconstructed rows, the export preflight runs first, and a file output
  appears only on success.
- Source requests that break mid-body resume from the first undelivered byte (at most 5 times per
  stream); the retry budget stays at 10 attempts with 0.5-20 s jitter
  (`-Dgefrierschrank.s3.maxAttempts` raises it).

### Fixes

- ES 9.3 logsdb and TSDB shards with ZSTD-compressed binary doc values reconstruct instead of
  quarantining with a `NullPointerException`.
- ES 7 (Lucene 8) shards are read in-process and byte-exact; 1.0.0 needed the separate Lucene 9
  worker, which reserialized `_source`.
- Where zstd-jni cannot load its native library (`noexec` temp directories, read-only images), a
  pure-Java decoder takes over with one warning instead of a JVM death.
- Nine more Elastic-licensed x-pack mappers are vendored for synthetic-source reconstruction
  (`version`, `counted_keyword`, `unsigned_long`, `shape`, `geo_shape` among them).

### Deprecated

- The Lucene 9 worker (`-Dgefrierschrank.lucene8.runtime=worker`) prints a warning at start, runs
  with a scrubbed environment, and is ignored by `dump`. The module is removed in the next release.

### Known limitations

- No rehydration of vectors that ES 9.2+ keeps out of the stored `_source`; such indices fail
  loudly.
- Every file of a shard is still downloaded, including postings, points and vectors the export
  never reads.
- A stored `_source` written in SMILE, CBOR or YAML is not converted and quarantines its shard.
- Only MinIO was exercised as the S3 source and target; real S3, EBS and Linux were not measured.

## 1.0.0

First release.
