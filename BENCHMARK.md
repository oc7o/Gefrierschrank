# Benchmark: 1.0.0, 1.1.0 and 1.2.0 against the Elasticsearch restore + PIT export

The authoritative end-to-end measurement of the performance work. It compares the released 1.0.0
build, an interim build and the final build with each other and with the
tuned Elasticsearch alternative (restore the snapshot on an export cluster, PIT + sliced
`search_after`, NDJSON, Parquet/Delta), on three datasets, with the per-document correctness gate on
every run. Taken on 2026-09-29 with the `gsbench` harness in [bench/](bench/README.md) and the micro
benchmark in [bench/micro](bench/README.md#micro-benchmark).

The final build of that sitting was released as 1.1.0. The 1.2.0 release was
measured against it on 2026-10-10 in the next section; everything after it is the
2026-09-29 sitting.

## 1.2.0 against 1.1.0 (2026-10-10)

The final measurement of the 1.2.0 work: the 1.2.0 build (CLI jar sha256
`a0cd2c3f0db0`) against the released 1.1.0 jar (sha256 `b5f4f19a70ba`) and against the
Elasticsearch route, measured again in the same sitting. 1.2.0 adds exportability (non-JSON stored
`_source`, rehydrated vectors, nested objects under synthetic source, documents whose `_source` was
disabled later), `--mounted-shells skip-if-original` and the default-off `--selective-fetch`; none
of it sets out to make a shard that 1.1.0 already exported faster or slower. The sections after this one are the 1.1.0 sitting of 2026-09-29 and stay as measured
then.

### Method of the 1.2.0 sitting

- Machine as in [Machine and load](#machine-and-load): Apple M1 Pro, 16 GB, macOS 27.0, OpenJDK
  24.0.1, OrbStack VM with 8 CPUs and 8.4 GB for the bench MinIO and one Elasticsearch node, 2 GB
  heap. No other job ran during the sitting. Before every `gsbench run` the driver waited
  for a 1-minute load average below 3.0: 2.4-3.0 at the start of all 14 runs, 2.8-6.9 at their
  end (the gates run between repeats), 14.6 at the end of the standard ES route. Free disk 14-19
  GiB.
- Timeline: standard 14:07-14:49 (one `slot.sh bench 1` hold), frozen tier at smoke scale
  14:49-15:19 (one hold), bench/shells, bench/vectors and bench/micro 15:19-15:35 (one hold),
  bench/synthfetch 15:36-15:57 (one hold).
- Builds and configurations: every Gefrierschrank run `-Xmx4g` and `--no-worker-jar` (1.1.0
  reads ES 7 shards in-process by default; 1.2.0 has no worker). Both builds at their defaults
  (`--parallelism 1`, `--shard-threads auto`; no shard of these datasets reaches the 2 x 250,000
  live documents `auto` needs to split, so every shard is read on one thread) and at
  `--parallelism 4`; 1.2.0 once more with `--selective-fetch` (default 16 MiB synthetic
  threshold). Labels `main` (1.1.0), `1.2.0` and `1.2.0-sf` (`--selective-fetch`), `-p4` for
  `--parallelism 4`, 3 repeats each, back to back in this order, then the ES route (3 repeats):

```sh
G="uv run --offline --frozen --project bench gsbench"; X=--jvm-args=-Xmx4g
# S = "--suite default --scale standard" | "--suite frozen --scale smoke"; P = "" | "--parallelism 4"
$G run --label main[-p4]     $S --repeat 3 --routes gs --jar <1.1.0 jar> --no-worker-jar $X [--gs-args "$P"]
$G run --label 1.2.0[-p4]    $S --repeat 3 --routes gs --jar <1.2.0 jar> --no-worker-jar $X [--gs-args "$P"]
$G run --label 1.2.0-sf[-p4] $S --repeat 3 --routes gs --jar <1.2.0 jar> --no-worker-jar $X --gs-args "--selective-fetch $P"
$G run --label es            $S --repeat 3 --routes es --jar <1.2.0 jar>
```

- The ES route ran in this sitting with the method of [Routes and metrics](#routes-and-metrics);
  its figures are not reused from 1.1.0's sitting. Node start-up is not counted: 17 s (9.3.0) +
  10 s (8.17.0) for the default suite, 19 s for the frozen suite (whose ES 8 snapshot restores on
  9.3.0).
- The frozen tier ran at smoke scale (442,500 documents: `ilm-big-000001` 300,000, eight logsdb
  generations of 8,000, twenty `ilm-app8` generations of 2,000-6,000); its standard scale (4.43M
  documents) was not rerun for lack of disk. Within this sitting the repeat spread ((max - min) / median)
  of the Gefrierschrank route's wall time was 1.0-5.7% at standard scale and 0.3-4.3% on the
  frozen tier, except 1.1.0 at p1 there (11.7%, one slow repeat); as in [Noise](#noise),
  differences under about 5% are not meaningful.

### Results: standard scale (2.52M documents, 11 indices)

Medians of 3 repeats, wall (min-max).

| metric | 1.1.0 | 1.2.0 | 1.2.0 `--selective-fetch` | 1.1.0 p4 | 1.2.0 p4 | 1.2.0 p4 `--selective-fetch` | ES route |
|---|---|---|---|---|---|---|---|
| wall s | 36.0 (35.6-37.7) | 36.2 (36.1-36.8) | 36.3 (36.1-36.4) | 20.7 (20.6-20.8) | 20.9 (20.9-21.2) | **20.5** (20.4-20.7) | 50.6 (49.5-52.2) |
| wall / 1.1.0 at the same parallelism | 1 | 1.00x | 1.01x | 1 | 1.01x | 0.99x | - |
| wall / ES route | 0.71x | 0.72x | 0.72x | 0.41x | 0.41x | **0.41x** | 1 |
| CPU s | 58.9 | 60.9 | 60.7 | 67.5 | 70.0 | 68.9 | 266.2 |
| peak RSS | 1.71 GB | 1.69 GB | 1.72 GB | 2.28 GB | 2.10 GB | 2.18 GB | 5.0 GB |
| peak disk | 415 MB | 433 MB | 433 MB | 756 MB | 772 MB | 743 MB | 1.5 GB |
| S3 GET requests / bytes read | 669 / 1.69 GB | 721 / 1.74 GB | **621 / 1.66 GB** | 669 / 1.69 GB | 721 / 1.74 GB | **621 / 1.66 GB** | 699 / 1.74 GB (restore) |
| output | 569 MB, 14 files | 587 MB, 15 files | 587 MB, 15 files | 569 MB, 14 files | 587 MB, 15 files | 587 MB, 15 files | 617 MB, 11 files |
| per snapshot wall s (ES 9 / 8 / 7) | 17.8 / 11.7 / 6.6 | 18.1 / 11.7 / 6.8 | 18.3 / 11.6 / 6.5 | 8.7 / 7.4 / 4.6 | 9.0 / 7.3 / 4.7 | 8.8 / 7.3 / 4.5 | - |
| per snapshot CPU s | 26.5 / 20.3 / 12.2 | 28.6 / 20.3 / 12.1 | 28.7 / 20.3 / 11.7 | 31.5 / 22.8 / 12.9 | 34.2 / 23.3 / 12.6 | 33.3 / 23.3 / 12.1 | - |
| 10 indices both builds export, summed per-index elapsed s | 29.8 | 29.2 (0.98x) | 29.0 (0.97x) | 30.7 | 30.8 (1.00x) | 29.9 (0.98x) | - |
| indices passing the gate | 10 of 11 | 11 of 11 | 11 of 11 | 10 of 11 | 11 of 11 | 11 of 11 | 11 of 11 |

- **1.2.0 against 1.1.0**: no change on the data both export. Wall totals are within 1% at both
  parallelisms, and the summed per-index times of the 10 indices both builds export are 0.98x
  (p1) and 1.00x (p4). The 1.2.0 totals carry one more index: `vec-default` (50,000 documents,
  vectors excluded from the stored `_source`), which 1.1.0 fails before its download and 1.2.0
  exports with the vectors rehydrated in 0.96 s at p1. That index is the whole difference in
  bytes read (+52 GETs, +46 MB), output (+18 MB, one file) and most of the CPU (+2.0 s at p1, all on
  the ES 9 snapshot; ES 8 and ES 7 are equal within 0.1 s).
- **`--selective-fetch`**: 11 of 15 shards take the selective plan; `logs-logsdb`, `tsdb-hist`
  and `synth8` are fetched in full (`below-threshold`: compound synthetic segments) and
  `vec-default` in full (rehydrated vectors are read from the vector files). Bytes read fall from
  1.74 to 1.66 GB (-4.8%) and GETs from 721 to 621 (-14%), all of it on the two ES 7.17 indices:
  `logs-default7` 192.6 -> 153.7 MiB (-20.2%), `logs-deflate7` 173.2 -> 131.7 MiB (-24.0%). Every
  ES 8.17 and 9.3 shard saves nothing (see below). Wall and CPU do not move (0.99-1.00x; the bench
  MinIO serves bytes at memory speed); peak disk is unchanged at p1, where it is set by one ES 9
  shard, and 0.96x at p4.
- **Against the ES route**, measured in this sitting at 50.6 s (57.8 s on 2026-09-29; its export
  phase took 36.3 s against 42.7 s): 1.2.0 takes 0.72x the wall time at its default and 0.41x at
  p4, 0.23-0.26x the CPU, 0.34-0.42x the peak memory and 0.29-0.51x the peak disk. ES still wins
  the synthetic-source indices that are too small to split (`logs-logsdb` 2.6 s against 6.1 s,
  `synth8` 2.0 s against 4.9 s, `tsdb-hist` a tie) for the reasons in [Where Gefrierschrank beats
  the ES route](#where-gefrierschrank-beats-the-es-route-where-it-trails-and-why); `vec-default`
  is a tie (1.03 s against 0.96 s).

Per index, Gefrierschrank's elapsed seconds (median of 3, including download and verification; at
p4 including the wait for the shared pool) and the ES route's export + transform:

| index | ES | kind | docs | 1.1.0 | 1.2.0 | 1.2.0 sf | 1.1.0 p4 | 1.2.0 p4 | 1.2.0 p4 sf | ES route |
|---|---|---|---|---|---|---|---|---|---|---|
| logs-lz4 | 9.3 | stored | 400,000 | 3.43 | 3.17 | 3.21 | 2.29 | 2.11 | 1.98 | 5.00 |
| logs-zstd | 9.3 | stored | 388,000 | 3.31 | 2.99 | 3.09 | 3.83 | 3.97 | 3.89 | 14.85 |
| docs-nested | 9.3 | stored | 144,000 | 1.59 | 1.50 | 1.57 | 1.94 | 1.95 | 2.02 | 2.31 |
| logs-logsdb | 9.3 | synthetic | 200,000 | 6.04 | 6.14 | 6.07 | 6.23 | 6.43 | 6.38 | 2.57 |
| tsdb-hist | 9.3 | synthetic | 100,000 | 0.55 | 0.56 | 0.56 | 1.07 | 0.91 | 0.76 | 0.59 |
| vec-default | 9.3 | vectors excluded from `_source` | 50,000 | FAILED (expected) | 0.96 | 0.96 | FAILED | 1.40 | 1.30 | 1.03 |
| logs-zstd8 | 8.17 | stored | 300,000 | 2.22 | 2.35 | 2.19 | 2.01 | 2.04 | 1.96 | 3.40 |
| logs8-lz4 | 8.17 | stored | 294,000 | 2.69 | 2.55 | 2.58 | 3.08 | 3.14 | 3.07 | 3.60 |
| synth8 | 8.17 | synthetic | 150,000 | 5.03 | 4.90 | 4.98 | 5.61 | 5.55 | 5.52 | 2.02 |
| logs-default7 | 7.17 | stored | 250,000 | 2.24 | 2.26 | 2.16 | 1.74 | 1.76 | 1.62 | 3.87 |
| logs-deflate7 | 7.17 | stored | 242,500 | 2.67 | 2.74 | 2.60 | 2.87 | 2.94 | 2.75 | 8.44 |

### Results: frozen tier at smoke scale (442,500 documents, 29 single-shard indices)

Medians of 3 repeats, wall (min-max).

| metric | 1.1.0 | 1.2.0 | 1.2.0 `--selective-fetch` | 1.1.0 p4 | 1.2.0 p4 | 1.2.0 p4 `--selective-fetch` | ES route |
|---|---|---|---|---|---|---|---|
| wall s | 15.9 (15.5-17.3) | 14.9 (14.9-15.1) | 14.8 (14.8-15.3) | 10.8 (10.7-10.8) | **10.5** (10.3-10.7) | 10.7 (10.7-10.7) | 11.4 (11.3-12.9) |
| wall / 1.1.0 at the same parallelism | 1 | 0.94x | 0.93x | 1 | 0.97x | 0.99x | - |
| wall / ES route | 1.39x | 1.31x | 1.30x | 0.95x | **0.92x** | 0.94x | 1 |
| CPU s | 26.9 | 26.2 | 25.9 | 32.9 | 32.9 | 32.8 | 34.6 |
| peak RSS | 1.52 GB | 1.45 GB | 1.40 GB | 1.78 GB | 1.86 GB | 1.71 GB | 4.4 GB |
| peak disk | 241 MB | 241 MB | 246 MB | 245 MB | 245 MB | 250 MB | 673 MB |
| S3 GET requests / bytes read | 159 / 240 MB | 159 / 240 MB | 159 / 240 MB | 159 / 240 MB | 159 / 240 MB | 159 / 240 MB | 149 / 240 MB (restore) |
| output | 105 MB, 29 files | 105 MB, 29 files | 105 MB, 29 files | 105 MB, 29 files | 105 MB, 29 files | 105 MB, 29 files | 110 MB, 29 files |
| per snapshot wall s (ES 9 / 8) | 9.8 / 6.2 | 9.1 / 5.8 | 9.0 / 5.8 | 7.5 / 3.3 | 7.3 / 3.3 | 7.4 / 3.3 | - |
| indices passing the gate | 29 of 29 | 29 of 29 | 29 of 29 | 29 of 29 | 29 of 29 | 29 of 29 | 29 of 29 |

| indices | docs | 1.1.0 | 1.2.0 | 1.2.0 sf | 1.1.0 p4 | 1.2.0 p4 | 1.2.0 p4 sf | ES route |
|---|---|---|---|---|---|---|---|---|
| ilm-big-000001 (ZSTD, 1 shard) | 300,000 | 2.11 | 2.07 | 2.05 | 2.25 | 2.25 | 2.22 | 3.50 |
| ilm-logsdb-000001..8 (logsdb), summed | 64,000 | 4.66 | 4.30 | 4.33 | 12.60 | 12.50 | 12.64 | 2.39 |
| ilm-app8-000001..20 (ZSTD), summed | 78,500 | 2.69 | 2.52 | 2.48 | 4.05 | 4.08 | 4.08 | 4.49 |

- 1.2.0 measured 0.94x of 1.1.0 at p1 and 0.97x at p4 with equal CPU (0.97x, 1.00x). Nothing in
  1.2.0 changes these read paths, and 1.1.0's p1 spread (15.5-17.3 s) covers the difference, so
  this is noise, not a gain.
- `--selective-fetch`: the 21 stored-source shards take the selective plan and save nothing, the 8
  logsdb shards are fetched in full (`below-threshold`); bytes read are identical (240.1 MB),
  because every shard of this suite is one compound segment.
- Against the ES route: 1.31x at p1 and 0.92x at p4, before ES's 19 s node start-up. At this
  scale each Gefrierschrank JVM's fixed cost (about 2 s, plus about 2 s for the first synthetic
  shard's isolated class loader: `ilm-logsdb-000008` 2.35 s at p1 against 0.26-0.32 s for its
  siblings) is a large share; the standard-scale frozen tier of 1.1.0's sitting (0.31x of the ES
  route) is the representative figure.

### What selective fetch can and cannot show here

- Every ES 8.17 and 9.3 shard of both gsbench suites is a single compound segment
  (`.cfs`/`.cfe`): Elasticsearch writes every segment below `index.compound_format` (1 GB by
  default) as a compound file, and selective fetch downloads compound segments whole. So on ES 8/9
  data these datasets can show only that the flag costs nothing (equal tables, equal bytes, no
  extra GET, wall and CPU within noise); they cannot show its saving. ES 7.17 writes non-compound
  segments at its default 0.1 ratio, which is where the -20% and -24% above come from.
- Every synthetic-source shard of gsbench is compound too, so `--selective-fetch` fetches it in
  full without a probe (`below-threshold`, even at a 0 threshold).
- bench/synthfetch builds ES 9.3 indices with `index.compound_format: false`, one force-merged
  segment each, like a segment above 1 GB, and their compound twins:

  `SLOT=$SLOT $SLOT bench 1 bench/synthfetch/run.sh <1.1.0 jar> <1.2.0 jar> 3` (15:36-15:57, load
  3.4 -> 4.6), one shard per index, `-Xmx4g`, medians of 3; 1.5M logsdb documents (117 MiB), 5M
  TSDB documents (88 MiB), 1M logsdb roots with a nested field (133 MiB):

  | index | build | S3 MiB read | GETs | peak temp MiB | wall s | CPU s | fetch |
  |---|---|---|---|---|---|---|---|
  | logsdb-nc | 1.1.0 / 1.2.0 | 116.9 / 116.9 | 20 / 20 | 116.9 / 116.9 | 7.66 / 7.87 | 33.2 / 34.5 | - / full |
  | | 1.2.0 `--selective-fetch` | **73.3 (-37.3%)** | 13 | 73.3 | 6.80 | 28.3 | selective, kept no index structure (1 probe call, 137-150 ms) |
  | tsdb-nc | 1.1.0 / 1.2.0 | 88.0 / 88.0 | 19 / 19 | 88.0 / 88.0 | 7.79 / 9.28 | 37.1 / 47.5 | - / full |
  | | 1.2.0 `--selective-fetch` | **60.7 (-31.0%)** | 13 | 60.7 | 7.92 | 37.0 | selective, kept no index structure |
  | nested-nc | 1.1.0 / 1.2.0 | 132.8 / 132.8 | 20 / 20 | 132.8 / 132.8 | 3.70 (exit 3) / 7.50 | 8.3 / 26.6 | - / full |
  | | 1.2.0 `--selective-fetch` | **109.3 (-17.7%)** | 17 | 109.3 | 7.04 | 23.9 | selective, kept postings (2 probe calls) |
  | logsdb-c, tsdb-c, nested-c (compound) | all three | 117.1, 88.4, 132.5 | 14, 12, 15 | same | 6.9-9.2 | - | flag on: full (below-threshold) |

  The `-nc` indices download 18-37% fewer bytes and 15-35% fewer GETs, and their peak temp disk
  falls by the same bytes; tables are identical (`compare-digests --strict`, 12 of 12 flag off
  against flag on; 1.1.0 against 1.2.0 10 of 10 identical, `nested-*` only in 1.2.0, which 1.1.0
  cannot export). Wall and CPU stay within this harness's run-to-run variation (the TSDB runs of
  every label alternate between about 7.7 and 9.3 s); `logsdb-nc` with the flag was faster in all
  three repeats (6.5-7.0 s against 7.9 s, 0.82x the CPU), which this sitting cannot attribute.
  The probe costs 1-2 sampled reconstructions per segment (130-260 ms). The compound twins behave as
  without the flag.

- Not measured here: stored-source shards with non-compound ES 8/9 segments (in an earlier check,
  generated non-compound ES 8.17/9.3 snapshots read 28-29% fewer bytes with identical tables; not
  rerun) and real S3, where bytes and requests cost time; on the local MinIO the download is
  nearly free, so the flag's effect here is on bytes, GETs and temp disk only.

### Measured costs of the new features

Vector rehydration (bench/vectors, `run.sh throughput ... 3 vec384,vec384-bbq,vec-nested,vec-sparse
1,4` with `-Dgefrierschrank.shard.threads.minRangeLiveDocs=1000`, load 5.1 -> 6.6): 1.1.0 exports
the `-included` twin (vectors stored in `_source`), 1.2.0 the excluded index with the vectors put
back; 200,000 documents per index (vec-nested 66,666 roots with 3 chunks), 2 shards, medians of 3.

| dataset | ranges per shard | docs/s 1.1.0 / 1.2.0 | wall 1.2.0 / 1.1.0 | CPU s 1.1.0 / 1.2.0 | CPU ratio | max RSS MiB 1.1.0 / 1.2.0 |
|---|---|---|---|---|---|---|
| vec384 (`int8_hnsw`) | 1 | 24,600 / 13,201 | 1.86x | 11.6 / 21.0 | 1.81x | 431 / 1,441 |
| vec384 | 4 | 48,426 / 28,249 | 1.71x | 13.7 / 23.3 | 1.70x | 691 / 1,447 |
| vec384-bbq | 1 | 24,631 / 13,423 | 1.83x | 11.6 / 20.6 | 1.78x | 433 / 1,468 |
| vec384-bbq | 4 | 49,628 / 28,986 | 1.71x | 14.0 / 23.4 | 1.67x | 691 / 1,174 |
| vec-nested | 1 | 8,439 / 4,511 | 1.87x | 11.0 / 20.8 | 1.89x | 440 / 1,430 |
| vec-nested | 4 | 16,584 / 9,510 | 1.74x | 13.6 / 23.1 | 1.70x | 658 / 1,461 |
| vec-sparse | 1 | 42,827 / 15,823 | 2.71x | 7.8 / 19.5 | 2.50x | 427 / 1,445 |
| vec-sparse | 4 | 68,027 / 29,762 | 2.29x | 9.8 / 23.2 | 2.37x | 688 / 1,477 |

Rehydration costs 1.7-1.9x the CPU of exporting the same documents with their vectors stored
(2.4-2.5x for `sparse_vector`) and 0.47-1.0 GiB more peak RSS per process (3.3-3.4x at one range
per shard, 1.7-2.2x at four), for the isolated Elasticsearch classes and the memory-mapped vector
files.

Conversion of SMILE, CBOR and YAML `_source` (bench/micro on the 1.2.0 jar, 300,000 documents of
about 1.08 KB as JSON, LZ4, one segment, 3 rounds, load 2.9-6.3, documents/s, medians):

| stored as | read + conversion | byte e2e, 1 thread | byte e2e, 4 shard threads | e2e / JSON | added per document |
|---|---|---|---|---|---|
| JSON | 739,748 (read only) | 166,028 | 445,057 | 1 | - |
| SMILE | 197,063 | 101,885 | 306,814 | 0.61x | about 3.8 us |
| CBOR | 171,809 | 94,301 | 285,220 | 0.57x | about 4.6 us |
| YAML | 21,828 | 19,789 | 73,214 | 0.12x | about 45 us |

1.1.0 quarantined such shards, so there is no 1.1.0 figure; JSON documents keep the 1.1.0 path
(an in-JVM A/B put it within 1%, and the gsbench rows above agree).

Mounted shells (bench/shells, `run.sh` A = 1.1.0, B = 1.2.0, C = 1.2.0 `--mounted-shells
skip-if-original`, 16-index ILM repository of 8 originals and their 8 shells, 5 repetitions,
medians, load 3.5 -> 5.1):

| config | --parallelism | wall s | user+sys s | max RSS MiB | rows | output MiB |
|---|---|---|---|---|---|---|
| A 1.1.0 | 1 | 6.36 | 9.51 | 418 | 475,040 | 71 |
| B 1.2.0 | 1 | 6.34 | 9.55 | 418 | 475,040 | 71 |
| C 1.2.0 skip-if-original | 1 | **4.14** | 7.82 | 416 | 237,520 | 36 |
| A 1.1.0 | 4 | 3.34 | 13.24 | 779 | 475,040 | 71 |
| B 1.2.0 | 4 | 3.30 | 13.14 | 675 | 475,040 | 71 |
| C 1.2.0 skip-if-original | 4 | **2.64** | 10.50 | 659 | 237,520 | 36 |

C against B: 0.65x the wall time and 0.82x the CPU at p1, 0.80x and 0.80x at p4, with exactly half
the rows, source bytes and output; B against A 1.00x and 0.99x. The acceptance checks of `run.sh`
(98) all passed: A and B manifests byte-identical, reports equal after the masks, table digests
equal; C 8 tables, 8 `ALIAS_OF`, digests equal to A's tables of the same names.

Nested and source-disabled reconstruction (bench/synthcov) was not rerun: the release notes quote
none of its figures.

### Correctness of the 1.2.0 runs

- gsbench: every repeat of every 1.2.0 configuration passed the gate on every index, `vec-default`
  included (row count, `(_id, _routing)` set, stored-source sha256, canonical hash against ES,
  `_source_kind`); 1.1.0 passed every index except `vec-default` (loud failure, the 1.1.0 entry of
  `bench/expected-failures.yaml`). No invariant violation (no write to the snapshot bucket, no
  Parquet file without an AddFile, no document content in any report or manifest), and `gsbench
  compare` exits 0 with the document-table schema equal to 1.1.0's for every pair (1.1.0 -> 1.2.0,
  1.2.0 -> `--selective-fetch`, p1 -> p4, ES route -> 1.2.0) on both datasets.
- Fixture repositories (`bench/fixture-digests.sh`, 18 repositories, not timed): 1.1.0 against
  1.2.0 without the flag, 114 common tables identical except the three tables of indices whose
  `_source` was disabled later (1.1.0 quarantined their source-less shard), and 30 tables only in
  1.2.0, all of them indices 1.1.0 refuses or quarantines (SMILE/CBOR/YAML, rehydrated vectors,
  nested under synthetic source, `index.mapping.source.mode: disabled`); 1.2.0 without against
  with `--selective-fetch`: 144 tables identical, identical exit codes and quarantine sets.

### Status of the known gaps after 1.2.0

| gap | status after 1.2.0 |
|---|---|
| selective fetch | built behind `--selective-fetch` (default off): stored-source shards skip postings, points, norms, term vectors and vector files of non-compound segments; synthetic-source shards are probed for the structures their loaders read. Saving shown on ES 7 (-20 to -24%) and on non-compound ES 9.3 segments (bench/synthfetch); compound ES 8/9 segments, i.e. every segment below 1 GB, download whole |
| failures found only after download | ES 8+ indices whose `_source` was disabled later and nested objects under synthetic source now export; what stays unexportable fails before download (mapping) or, with `--selective-fetch`, after the commit files (`fetch: preflight`) |
| an original and its mounted shells exported twice | avoided with `--mounted-shells skip-if-original` (default `export`) |
| vector rehydration | done: vectors put back by Elasticsearch's loaders; `rank_vectors`/`semantic_text` still fail loudly |
| worker stdout noise | moot: the Lucene 9 worker is removed in 1.2.0 |


## Summary of the 1.1.0 sitting (2026-09-29)

Wall time in seconds (median over the repeats), local MinIO, `-Xmx4g`. "p4" is `--parallelism 4`, "K
auto" is `--shard-threads auto`; final p1 was the shipped default when measured (`--shard-threads` defaults to `auto` since 2026-10-09). The ES route is restore + export +
transform on a warm node; its node start-up (18.6 s for 9.3.0, 10.5 s for 8.17.0) is not counted.

| dataset | 1.0.0 | interim p4 | final p1 | final p4 | final p4 K auto | ES route |
|---|---|---|---|---|---|---|
| standard: 2.52M documents, 11 indices on ES 9.3 / 8.17 / 7.17 | 367.2 | 59.0 | 38.9 | **23.6** | 23.6 | 57.8 |
| frozen tier: 4.43M documents, 29 single-shard indices, one of 3M | 1170.8 | 107.2 | - | 32.7 | **22.2** | 71.2 |
| smoke: 252k documents, 11 indices | 52.8 | 19.0 | 16.5 | 12.3 | 12.4 | **9.0** |
| CPU s, standard / frozen tier | 408.5 / 1207.6 | 96.5 / 120.1 | 64.8 / - | 72.6 / 77.9 | 72.9 / 79.3 | 299.8 / 314.0 |
| peak RSS, standard / frozen tier | 1.79 / 2.55 GB | 2.01 / 1.35 GB | 1.54 GB / - | 2.21 / 1.80 GB | 2.17 / 1.80 GB | 4.92 / 7.12 GB |
| peak disk, standard / frozen tier | 1.41 / 5.22 GB | 0.43 / 2.36 GB | 0.48 GB / - | 0.72 / 2.51 GB | 0.74 / 2.49 GB | 1.54 / 6.71 GB |
| indices passing the gate, standard / frozen tier | 8 / 21 | 10 / 29 | 10 / - | 10 / 29 | 10 / 29 | 11 / 29 |

- **Against 1.0.0**: the final build takes 0.106x the wall time at its defaults and 0.064x at p4 on
  the standard dataset, and 0.019x on the frozen tier with K auto (22 s instead of 19.5 minutes),
  with 0.16-0.18x and 0.066x of the CPU. It writes 0.21-0.23x the bytes (ZSTD Parquet instead of
  uncompressed) in 6-8% of the files.
- **Against the interim build** at the same `--parallelism 4`: 0.40x on the standard dataset and
  0.31x on the frozen tier (0.21x with K auto). The per-document path is 2.45-2.66x faster on one
  core (byte row path, micro benchmark), indices now run concurrently, and a large shard can be
  read as ranges.
- **Against the ES route**: 0.41x the wall time and 0.24x the CPU on the standard dataset (final
  p4), 0.31x and 0.25x on the frozen tier (final p4 K auto), with 0.25-0.45x the peak memory and
  0.37-0.47x the peak disk, before counting ES's node start-up. Even at its sequential default the
  final build takes 0.67x of the ES route's time at standard scale. The ES route is still faster on
  the logsdb and synthetic-source indices of 80,000-200,000 documents, 2.3-2.8x per index (the small
  TSDB index ties), because it reads one shard with 8 slices where Gefrierschrank does not split
  shards that small, and on the smoke dataset, where Gefrierschrank's per-JVM fixed costs dominate
  (12.3 s against 9.0 s, or about 38 s with ES's start-up).
- **Micro benchmark** (one core, local segments, 300k documents): the final build's archive path
  runs at 138-173k documents/s (149-187 MB/s of source), 2.45-2.66x the interim build and 5.2x
  (LZ4) to 48x (ZSTD 240 KB) 1.0.0; 4 shard threads reach 380-431k documents/s on one shard.
- **Correctness**: every repeat of every configuration passed the per-document gate on every index
  except the documented expected failures (1.0.0's logsdb and TSDB quarantines and silent vector
  loss; the interim and final builds' loud failure on excluded vectors, the index FAILED before
  download). No regression, no invariant violation, the document-table schema unchanged from 1.0.0.

## Method

### Machine and load

- Apple M1 Pro (6 performance and 2 efficiency cores), 16 GB RAM, internal APFS SSD, macOS 27.0,
  OpenJDK 24.0.1; Python 3.12 (uv) for the harness and for the ES route's exporter and transform.
- Docker (OrbStack, 29.4.0) VM with 8 CPUs and 8.4 GB for MinIO (`minio/minio` 2025-09-07T16:13:09Z,
  image `14cea493d9a3`) and one Elasticsearch node at a time: 9.3.0 for the 9.x and 8.x snapshots,
  8.17.0 for the 7.x snapshots, 2 GB heap. MinIO runs on the same host, so an S3 request costs well
  under a millisecond and bytes move at memory speed.
- Every timed block ran inside one `slot.sh bench 1` hold per phase; no other job ran.
  Before every `gsbench run` the driver waited until the 1-minute load average was below 5.0. The
  machine's idle load is about 4: `Messages.app` spun one core at 100% throughout (a stuck process
  outside this work, left alone), plus desktop apps and macOS background daemons. Load at the start
  of the runs: 4.2-5.0 (smoke), 4.6-4.9 (standard), 3.9-5.0 (frozen tier); gates run between repeats
  outside the timed windows and raise it to 6-10, hence the waits.
- Free disk on the data volume was 11-27 GiB of 460 GB, which sized the frozen-tier dataset; a
  watchdog would have stopped any run below 3 GiB.
- Timeline: smoke 11:48-12:14, standard 12:33-13:32, frozen tier 14:17-15:07 (1.0.0, interim,
  final). At 15:07 the host went to clamshell sleep during the frozen tier's ES route, right after
  its first repeat's last export; OrbStack pauses its VM whenever macOS drops to DarkWake, so that
  run was discarded. The ES route ran again when the host was fully awake (17:10), followed at once
  by the final build's frozen-tier routes again, so the frozen-tier comparison with the ES route is
  within one sitting. The micro benchmark ran last. The macOS power log shows no sleep or DarkWake
  transition between 11:30 and 15:07, and the driver checked it after every later run.

### Builds

Three CLI fat jars, each built with `mvn -o -q package -DskipTests` in a detached worktree created
for this benchmark and removed afterwards:

| label | build | CLI jar sha256 (first 12) | Lucene 9 worker jar |
|---|---|---|---|
| `main` | 1.0.0 (release) | `b72d2b8b06aa` | 1.0.0's `gefrierschrank-lucene9-1.0.0.jar` (`--worker-jar`), which 1.0.0 needs for ES 7 shards |
| `interim-*` | interim (development build) | `12399d58e42c` | none (`--no-worker-jar`: ES 7 shards are read in-process) |
| `final-*` | final (development build, released as 1.1.0) | `92ac9044369e` | none |

All three report `gefrierschrank 1.0.0` as their version. Every run passes `-Xmx4g` (`--jvm-args`),
so peak RSS is comparable between builds.

### Configurations

| label | build | options | meaning |
|---|---|---|---|
| `main` | 1.0.0 | none | shards one after another, one thread each |
| `interim-p4` | interim | `--parallelism 4` | up to 4 shards of one index at once, indices one after another |
| `final-p1` | final | none (the defaults at measurement time: `--parallelism 1`, `--shard-threads 1`) | as 1.0.0's scheduling, with every per-shard improvement |
| `final-p4` | final | `--parallelism 4` | up to 4 shards in flight across the whole run, several indices at once |
| `final-p4-kauto` | final | `--parallelism 4 --shard-threads auto` | as `final-p4`, and a shard of at least 2 x 250,000 live documents is read as ranges on clamp(8 cores / 4, 1, 8) = 2 threads |
| `es` | - | 8 slices | restore + PIT export + transform, below |

`--shard-threads auto` splits a shard only when every range gets at least 250,000 live documents
(`MIN_RANGE_LIVE_DOCS`). No shard of the default datasets reaches 500,000 documents (the largest has
388,000), so there `final-p4-kauto` runs exactly like `final-p4`; the frozen tier's 3M-document
shard is split into 2 ranges.

### Datasets

gsbench's seeded datasets (seed 42), shapes in [bench/README.md](bench/README.md#datasets). The
suite became part of the dataset key when the frozen suite was added, so the default datasets were
rebuilt for this sitting with the same documents; ES's flush and merge timing gave some indices a
different segment layout than in earlier sittings (see Noise).

| suite / scale | documents | source JSON | stored fields | snapshots (dataset keys) |
|---|---|---|---|---|
| default / smoke | 251,850 in 11 indices | 282 MB | 91 MB | es9-9bfd020e8845, es8-4d8949d0c525, es7-7b53c2a4969f |
| default / standard | 2,518,500 in 11 indices | 2.82 GB | 906 MB | es9-146143e24c07, es8-d38b6afd45c2, es7-87044dac166b |
| frozen / standard | 4,425,000 in 29 indices | 5.04 GB | 1.02 GB | es9-4ed5721dfaf1, es8-2360864a763d |

- **Default suite**: 11 indices on ES 9.3, 8.17 and 7.17 of one or two shards and at most 400,000
  documents each: LZ4, ZSTD and DEFLATE stored fields, multi-segment indices with updates and
  deletes, nested documents with custom routing, logsdb and TSDB (synthetic source), synthetic
  source on 8.17, and a 9.3 `dense_vector` index whose vectors are excluded from the stored
  `_source`.
- **Frozen suite** (`--suite frozen`, added for this benchmark): the shape of an ILM
  frozen tier, every generation one shard force-merged to one segment, nothing deleted. ES 9.3:
  `ilm-big-000001`, 3,000,000 documents of `best_compression` (ZSTD 240 KB blocks; 1.67 GB shard,
  3.43 GB of source JSON), and eight logsdb generations `ilm-logsdb-000001..8` of 80,000 documents.
  ES 8.17: twenty `best_compression` generations `ilm-app8-000001..20` of 20,000-60,000 documents
  (785,000 in total). A larger index of at least 5M documents did not fit:
  5M documents would have needed about 6 GB of ES-route NDJSON and 7 GB of 1.0.0's uncompressed
  Parquet on top of the snapshots, the ES data volume and the cache, with 13-15 GiB free when the
  dataset was built.

### Routes and metrics

Defined in [bench/README.md](bench/README.md#metrics). Gefrierschrank runs one `dump-to-delta
--all-indices` process per snapshot (three JVMs per repeat for the default suite, two for the frozen
suite) against the bench MinIO, writing to local disk; wall and CPU cover the whole process tree,
peak RSS is the largest process tree, peak disk counts temp directory plus output. Per-index times
are the run report's `elapsedMillis`: at `--parallelism 4` indices overlap and an index waiting for
the shared shard pool shows a longer elapsed time than at p1, so route wall and CPU are the figures
to compare there.

The ES route starts a fresh node per ES version, restores each snapshot on it, exports each index
with one PIT and sliced `search_after` sorted on `_shard_doc` (8 slices, one Python process each) to
gzip NDJSON, and writes Delta with delta-rs. Its totals are restore + export + transform; node
start-up (18.6 s for 9.3.0, 10.5 s for 8.17.0) is reported but not counted, and the node stays up
across repeats, so its JIT is warm from repeat 2 on while every Gefrierschrank run is a fresh JVM.
Its CPU includes the ES container's cgroup CPU, and its peak RSS the ES node.

### Commands

From the final build's `bench/`, with `GSBENCH_SLOT=<slot.sh> GSBENCH_HELD_SLOTS=bench` and
`GSBENCH_CACHE`/`GSBENCH_RESULTS` in a local directory:

```sh
G="uv run --offline --frozen --project bench gsbench"; X=--jvm-args=-Xmx4g
# S = "--scale smoke" | "--scale standard" | "--suite frozen --scale standard"; R = 3 (2 for the frozen tier, 1 for its 1.0.0 run)
$G run --label main           $S --repeat $R --routes gs --jar <1.0.0 cli jar> --worker-jar <1.0.0 lucene9 jar> $X
$G run --label interim-p4     $S --repeat $R --routes gs --jar <interim jar> --no-worker-jar $X --gs-args '--parallelism 4'
$G run --label final-p1       $S --repeat $R --routes gs --jar <final jar> --no-worker-jar $X            # not run on the frozen tier
$G run --label final-p4       $S --repeat $R --routes gs --jar <final jar> --no-worker-jar $X --gs-args '--parallelism 4'
$G run --label final-p4-kauto $S --repeat $R --routes gs --jar <final jar> --no-worker-jar $X --gs-args '--parallelism 4 --shard-threads auto'
$G run --label es             $S --repeat $R --routes es --jar <final jar> --no-worker-jar
$G compare <result A> <result B>
bench/micro/run.sh <cli jar> <one work dir> 300000 10 lz4-80k,deflate-480k,zstd-240k   # 3 rounds, alternating 1.0.0, interim, final
```

The routes of a scale ran back to back in that order; the frozen tier in two sittings (see Machine
and load).

### Noise

- Repeat spread ((max - min) / median) of the Gefrierschrank route's wall time: 0.5-3.7% at standard
  scale, 0.5-5.6% at smoke, 0.9-8.0% on the frozen tier (2 repeats). Per index it is at most 6.2% at
  p1 and up to 19% at p4, where indices compete for the pool. The ES route spreads 8.7% at standard,
  11.6% on the frozen tier and 32% at smoke, its first repeat being cold.
- Across sittings: the final build's frozen-tier runs repeated within 2.5% three hours apart (p4
  32.2 -> 32.7 s, p4 K auto 22.7 -> 22.2 s). A single p4 K auto repeat taken three minutes after the
  host woke, with a desktop app still busy (1-minute load 4.4, 5-minute 7.1 at its start), took 30.2
  s; it is reported as noise and not used. 1.0.0 at standard scale took 367.2 s here against 358.8 s
  and 362.5 s in two earlier sittings, interim p4 59.0 s against 59.3 s, and the micro benchmark
  reproduces the earlier sitting within 5%.
- The rebuilt default datasets have the same documents but not the same segments as in earlier
  sittings. `logs-logsdb` has 39 segments instead of 9, and the final build now needs 7.2 s for it
  at p1 where an earlier development build measured 4.9 s on the old dataset. An A/B on
  the rebuilt ES 9 snapshot, alternating that build and the final build in 2 x 2 repeats,
  gives route wall medians of 18.5 s and 18.1 s and `logs-logsdb` 7.1 s and 7.0 s: the dataset
  moved, not the build; the changes between the two builds cost nothing measurable.
- Differences under about 5% within a sitting and under about 15% across sittings are not
  meaningful.

## Results: standard scale (2.52M documents)

### Route totals

Medians of 3 repeats, wall (min-max).

| metric | 1.0.0 | interim p4 | final p1 | final p4 | final p4 K auto | ES route |
|---|---|---|---|---|---|---|
| wall s | 367.2 (363.2-367.2) | 59.0 (58.3-59.3) | 38.9 (38.8-39.0) | **23.6** (23.0-23.8) | 23.6 (23.4-23.7) | 57.8 (56.6-61.6) |
| wall / 1.0.0 | 1 | 0.161x | 0.106x | **0.064x** | 0.064x | 0.157x |
| wall / ES route | 6.35x | 1.02x | 0.67x | **0.41x** | 0.41x | 1 |
| CPU s | 408.5 | 96.5 | 64.8 | 72.6 | 72.9 | 299.8 |
| CPU / ES route | 1.36x | 0.32x | 0.22x | 0.24x | 0.24x | 1 |
| peak RSS | 1.79 GB | 2.01 GB | 1.54 GB | 2.21 GB | 2.17 GB | 4.92 GB |
| peak disk | 1.41 GB | 434 MB | 481 MB | 724 MB | 739 MB | 1.54 GB |
| S3 GET requests / bytes read | 678 / 1.74 GB | 652 / 1.69 GB | 652 / 1.69 GB | 652 / 1.69 GB | 652 / 1.69 GB | 682 / 1.74 GB (restore) |
| output | 2.65 GB, 227 files | 568 MB, 14 files | 569 MB, 14 files | 569 MB, 14 files | 569 MB, 14 files | 617 MB, 11 files |
| per snapshot wall s (ES 9 / 8 / 7) | 149.6 / 131.2 / 85.8 | 28.7 / 20.0 / 10.2 | 18.7 / 12.9 / 7.3 | 10.2 / 8.3 / 5.1 | 10.0 / 8.3 / 5.1 | 32.7 / 11.5 / 13.8 |
| per snapshot CPU s | 162.5 / 144.5 / 101.5 | 45.2 / 32.3 / 18.9 | 29.4 / 22.2 / 13.3 | 33.7 / 24.9 / 14.0 | 33.5 / 25.4 / 14.2 | - |
| indices passing the gate | 8 of 11 | 10 of 11 | 10 of 11 | 10 of 11 | 10 of 11 | 11 of 11 |

- The final build at its defaults (p1) takes 0.66x the wall time and 0.67x the CPU of the interim
  build at `--parallelism 4`, with a quarter of the concurrency: the byte row path removes the JSON
  round trips per row that capped the interim build at about 60k docs/s per core, and synthetic
  reconstruction is about 1.7x faster.
- `--parallelism 4` now runs indices concurrently: 0.61x of p1, 0.40x of the interim build at the
  same setting (the interim build ran only the shards of one index at once, so its single-shard
  indices gained nothing). It costs 12% more CPU than p1 and 1.5x the peak disk: up to four shards
  of different indices are downloaded at once.
- `final-p4-kauto` equals `final-p4`: no shard here reaches 2 x 250,000 live documents.
- S3: every build after 1.0.0 skips `vec-default`'s shard (the preflight refuses it before the
  download). Otherwise every build downloads and verifies every file of every shard, 1.69
  GB for 906 MB of stored fields; selective fetch came in 1.2.0.
- Output: 1.0.0 writes uncompressed 10,000-row files (4.7x the bytes); the interim build and later
  write ZSTD Parquet, one file per shard at this size (files roll at 256 MiB).
- ES route phases: restore 2.8 s, export 42.7 s (275.7 s of CPU for the ES node and the 8 slice
  processes, 4.92 GB peak RSS), transform 12.1 s (15.5 s CPU). Not counted: node start-up 18.6 s
  (9.3.0) + 10.5 s (8.17.0).

### Per index

Gefrierschrank per-index elapsed seconds (median of 3), including download and verification, next to
the ES route's export + transform (its restore is per snapshot, above). At p4 the per-index times
include waiting for the shared pool.

| index | ES | shape | docs | 1.0.0 | interim p4 | final p1 | final p4 | final p4 K auto | ES route |
|---|---|---|---|---|---|---|---|---|---|
| logs-lz4 | 9.3 | LZ4, 2 shards, force-merged | 400,000 | 16.13 | 3.94 | 3.38 | 2.72 | 2.61 | 5.70 |
| logs-zstd | 9.3 | ZSTD, 1 shard, 34 segments, updates + deletes | 388,000 | 116.76 | 6.50 | 2.88 | 3.84 | 3.81 | 18.04 |
| docs-nested | 9.3 | nested, custom routing, 2 shards, deletes | 144,000 | 7.23 | 2.34 | 1.72 | 2.22 | 2.10 | 2.57 |
| logs-logsdb | 9.3 | logsdb (synthetic), 1 shard, 39 segments | 200,000 | quarantined | 11.86 | 7.18 | 7.38 | 7.36 | 3.05 |
| tsdb-hist | 9.3 | TSDB histogram (synthetic), 1 shard | 100,000 | quarantined | 1.18 | 0.64 | 3.12 | 3.16 | 0.64 |
| vec-default | 9.3 | vectors excluded from `_source` | 50,000 | committed without vectors | FAILED | FAILED | FAILED | FAILED | 1.08 |
| logs-zstd8 | 8.17 | ZSTD, 2 shards, force-merged | 300,000 | 92.50 | 2.79 | 2.37 | 2.45 | 2.41 | 4.00 |
| logs8-lz4 | 8.17 | LZ4, 1 shard, 36 segments, updates + deletes | 294,000 | 12.24 | 5.53 | 2.77 | 3.66 | 3.61 | 4.28 |
| synth8 | 8.17 | synthetic, 1 shard | 150,000 | 24.92 | 9.54 | 5.61 | 6.33 | 6.36 | 2.28 |
| logs-default7 | 7.17 | LZ4, 2 shards | 250,000 | 15.44 | 2.65 | 2.47 | 2.01 | 1.98 | 3.58 |
| logs-deflate7 | 7.17 | DEFLATE, nested, 1 shard, updates + deletes | 242,500 | 68.40 | 5.39 | 2.92 | 3.24 | 3.20 | 9.44 |
| **8 indices every build exports** | | | 2,168,500 | 353.6 | 38.7 | 24.1 | (overlapping) | (overlapping) | 49.9 |

- Final p1 against 1.0.0: 0.025-0.043x on ZSTD and DEFLATE (1.0.0 decompressed a whole 240 or 480 KB
  stored-fields block twice per document), 0.16-0.24x on LZ4, nested and synthetic source.
- Final p1 exports stored source at 83-135k root documents/s per single-threaded shard including
  download and verification (`logs-zstd` 388k in 2.88 s; `logs-lz4` 400k in 3.38 s over two shards
  one after another), 0.44-0.61x of the interim build's time on the single-shard indices.
- Against the ES route, final p1 is faster on every stored-source index (0.16x on `logs-zstd`, 0.31x
  on `logs-deflate7`, 0.59-0.69x on the rest) and slower on the synthetic-source indices:
  `logs-logsdb` 2.35x and `synth8` 2.46x of ES's time, `tsdb-hist` a tie. On the 8 indices every
  build exports, final p1 needs 24.1 s against the ES route's 49.9 s of export + transform.

## Results: frozen tier (4.43M documents, 29 single-shard indices)

Two sittings. The first (14:17-15:03) ran 1.0.0 once (15 minutes for the ES 9 snapshot alone), then
the interim build, final p4 and final p4 K auto twice each; the second (17:10-17:35) ran the ES
route twice and then the two final configurations twice again. The final build's numbers agree
between the sittings within 2.5%, so the tables use the second sitting for the final build and the
ES route and the first for 1.0.0 and the interim build. Wall (min-max) over the repeats.

| metric | 1.0.0 | interim p4 | final p4 | final p4 K auto | ES route |
|---|---|---|---|---|---|
| wall s | 1170.8 (1 repeat) | 107.2 (104.9-109.4) | 32.7 (31.4-34.0) | **22.2** (21.6-22.9) | 71.2 (67.0-75.3) |
| wall / 1.0.0 | 1 | 0.092x | 0.028x | **0.019x** | 0.061x |
| wall / ES route | 16.4x | 1.51x | 0.46x | **0.31x** | 1 |
| first sitting | - | - | 32.2 (31.6-32.8) | 22.7 (22.6-22.8) | (interrupted) |
| CPU s | 1207.6 | 120.1 | 77.9 | 79.3 | 314.0 |
| peak RSS | 2.55 GB | 1.35 GB | 1.80 GB | 1.80 GB | 7.12 GB |
| peak disk | 5.22 GB | 2.36 GB | 2.51 GB | 2.49 GB | 6.71 GB |
| S3 GET requests / bytes read | 172 / 2.36 GB | 267 / 2.36 GB | 267 / 2.36 GB | 267 / 2.36 GB | 172 / 2.36 GB (restore) |
| output | 4.48 GB, 382 files | 1.04 GB, 31 files | 1.04 GB, 31 files | 1.04 GB, 32 files | 1.10 GB, 31 files |
| per snapshot wall s (ES 9 / 8) | 924.1 / 246.7 | 87.2 / 20.0 | 26.5 / 6.1 | 16.5 / 5.7 | 57.0 / 14.2 |
| indices passing the gate | 21 of 29 | 29 of 29 | 29 of 29 | 29 of 29 | 29 of 29 |

| indices | docs | 1.0.0 | interim p4 | final p4 | final p4 K auto | ES route (export + transform) |
|---|---|---|---|---|---|---|
| ilm-big-000001 (ZSTD, 1 shard, 1.67 GB) | 3,000,000 | 906.7 | 51.0 | 24.0 | **14.1** | 39.2 |
| ilm-logsdb-000001..8 (logsdb), summed | 640,000 | quarantined | 32.6 | 30.7 | 31.1 | **11.2** |
| ilm-app8-000001..20 (ZSTD), summed | 785,000 | 242.5 | 16.0 | 11.8 | 11.2 | 12.9 |

- On this shape the gains compound: 1.0.0 reads ZSTD 240 KB blocks at about 3,300 documents/s (the
  whole block decompressed twice per document), the interim build fixes the read path (51 s for the
  large index) but runs single-shard indices one after another even at `--parallelism 4`; the final
  build runs up to four indices at once and reads the 3M-document shard as 2 ranges: 14.1 s, 212k
  documents/s including the 1.67 GB download, 0.59x of the same build without ranges.
- Against the ES route the final build takes 0.31x the wall time and 0.25x the CPU, 0.25x the peak
  memory and 0.37x the peak disk. The ES route's restore alone (7.9 s) is a third of
  Gefrierschrank's whole run; its export of the large index takes 30.9 s with 8 slices (98 s of
  client CPU and 92 s of ES CPU), plus 8.3 s of transform.
- The ES route wins the logsdb generations (1.4 s each against about 3.9 s each at p4, where they
  share the pool with the large index), for the reason given in "Where Gefrierschrank beats the ES
  route, where it trails, and why". The 20 small ZSTD generations are about even per index (11.2 s
  against 12.9 s summed) and far apart per snapshot (5.7 s against 14.2 s), because Gefrierschrank
  overlaps them four at a time.
- 1.0.0 wrote 4.48 GB of uncompressed Parquet and needed 5.2 GB of disk; ranged downloads (16 MiB
  parts above 64 MiB) make the interim and final builds send 267 GETs instead of 172 for the same
  bytes.

## Results: smoke scale (252k documents)

Medians of 3 repeats, wall (min-max).

| metric | 1.0.0 | interim p4 | final p1 | final p4 | final p4 K auto | ES route |
|---|---|---|---|---|---|---|
| wall s | 52.8 (52.5-53.9) | 19.0 (18.9-19.5) | 16.5 (16.4-16.7) | 12.3 (12.2-12.9) | 12.4 (12.4-12.4) | **9.0** (8.8-11.7) |
| wall / ES route | 5.88x | 2.12x | 1.84x | 1.37x | 1.38x | 1 |
| CPU s | 81.0 | 44.0 | 35.6 | 40.8 | 40.7 | 37.6 |
| peak RSS | 2.21 GB | 1.70 GB | 1.47 GB | 1.59 GB | 1.58 GB | 3.61 GB |
| peak disk | 133 MB | 45 MB | 45 MB | 63 MB | 63 MB | 161 MB |
| S3 GET requests / bytes read | 463 / 0.18 GB | 455 / 0.18 GB | 455 / 0.18 GB | 455 / 0.18 GB | 455 / 0.18 GB | 467 / 0.18 GB |
| output | 265 MB, 28 files | 57.1 MB, 14 | 57.2 MB, 14 | 57.2 MB, 14 | 57.2 MB, 14 | 62.1 MB, 11 |
| per snapshot wall s (ES 9 / 8 / 7) | 22.2 / 18.3 / 12.3 | 8.6 / 7.0 / 3.6 | 7.3 / 6.0 / 3.2 | 4.5 / 5.1 / 2.7 | 4.5 / 5.1 / 2.7 | 4.8 / 2.0 / 2.2 |

| index | docs | 1.0.0 | interim p4 | final p1 | final p4 | final p4 K auto | ES route |
|---|---|---|---|---|---|---|---|
| logs-lz4 | 40,000 | 1.85 | 0.59 | 0.57 | 0.51 | 0.53 | 0.73 |
| logs-zstd | 38,800 | 12.21 | 0.84 | 0.47 | 0.59 | 0.61 | 2.06 |
| docs-nested | 14,400 | 1.44 | 0.55 | 0.53 | 0.73 | 0.72 | 0.46 |
| logs-logsdb | 20,000 | quarantined | 3.87 | 3.12 | 2.38 | 2.37 | 0.49 |
| tsdb-hist | 10,000 | quarantined | 0.32 | 0.21 | 1.94 | 1.93 | 0.23 |
| vec-default | 5,000 | committed without vectors | FAILED | FAILED | FAILED | FAILED | 0.29 |
| logs-zstd8 | 30,000 | 10.01 | 0.91 | 0.78 | 0.98 | 0.90 | 0.59 |
| logs8-lz4 | 29,400 | 1.64 | 0.76 | 0.44 | 0.96 | 0.92 | 0.63 |
| synth8 | 15,000 | 4.66 | 3.27 | 2.68 | 3.08 | 3.10 | 0.41 |
| logs-default7 | 25,000 | 2.69 | 0.56 | 0.55 | 0.78 | 0.80 | 0.62 |
| logs-deflate7 | 24,250 | 7.72 | 1.03 | 0.70 | 0.81 | 0.82 | 1.23 |

At smoke scale fixed costs dominate the Gefrierschrank route: about 2 s per JVM outside the index
timers (JVM start, repository catalog, three JVMs per repeat), and about 2 s for the first synthetic
shard of a JVM, which builds the isolated Elasticsearch class loader (`logs-logsdb` and `synth8`
above). The ES route's fixed cost is its node start-up, 29 s here and not counted; with it the route
would take about 38 s. The ES route's first repeat is cold (11.7 s against 8.8-9.0 s).

## Micro benchmark: read path and archive path

`bench/micro`: 300,000 ECS-like documents of about 1.08 KB (324 MB of source JSON) in one
force-merged segment per stored-fields codec, local files, one thread, `-Xmx4g`. One work dir, so
every jar reads identical segments. Each measurement runs 10 s after a warm-up; an e2e pass ends
once all 300k documents are written. Medians of 3 rounds, each round running 1.0.0, the interim
build and the final build one after another (17:35-18:05, 1-minute load 3.9-4.9 at each start), in
documents/s.

- **read**: the production reader (`Lucene10ShardReader`: source and metadata of every live
  document); **seq read**: a sequential merge-instance reader, the ceiling for reading.
- **e2e (production path)**: each build's own archive path into a local Delta table. 1.0.0 and the
  interim build: read -> `ArchiveContext` JSON row -> `DeltaTableWriter`; final: read -> byte row
  path -> `DeltaTableWriter.writeArchive`. The last column is the final build with the shard read as
  4 ranges on 4 threads.

| stored fields | build | read | seq read | e2e (production path) | e2e, 4 shard threads | e2e per round |
|---|---|---|---|---|---|---|
| LZ4 80 KB (ES default codec) | 1.0.0 | 61,653 | 711,970 | 28,949 | - | 28.9k / 30.1k / 28.8k |
| | interim | 701,888 | 711,046 | 60,248 | - | 60.2k / 62.9k / 55.8k |
| | final | 704,515 | 707,599 | **150,269** | 380,277 | 156.5k / 150.3k / 142.4k |
| DEFLATE 480 KB (`best_compression` before 8.14) | 1.0.0 | 4,916 | 443,551 | 4,391 | - | 4.4k / 4.5k / 4.4k |
| | interim | 427,750 | 437,180 | 56,527 | - | 55.5k / 61.0k / 56.5k |
| | final | 416,921 | 427,203 | **138,263** | 384,703 | 140.4k / 134.4k / 138.3k |
| ZSTD 240 KB/2048 (`best_compression` since 8.14) | 1.0.0 | 3,918 | 1,226,523 | 3,622 | - | 3.6k / 3.7k / 3.6k |
| | interim | 1,167,123 | 1,220,317 | 65,175 | - | 65.2k / 67.1k / 64.0k |
| | final | 1,192,382 | 1,185,817 | **173,334** | 430,752 | 173.5k / 152.5k / 173.3k |

| stored fields | e2e final / interim | e2e final / 1.0.0 | 4 shard threads / 1 |
|---|---|---|---|
| LZ4 80 KB | 2.49x | 5.2x | 2.53x |
| DEFLATE 480 KB | 2.45x | 31.5x | 2.78x |
| ZSTD 240 KB | 2.66x | 47.9x | 2.49x |

- The read path has been at the sequential-reader ceiling since the interim build (read = seq read
  for every codec); 1.0.0 read ZSTD 240 KB at 0.3% and DEFLATE at 1.1% of it.
- The byte row path lifts the single-core end-to-end rate from 56-65k to 138-173k documents/s,
  149-187 MB/s of source JSON; four ranges on one shard reach 380-431k. The gsbench runs agree:
  stored-source shards ran at 83-135k documents/s per thread at p1, below the micro figure because
  they include download, verification, commit and, for multi-segment indices, deleted documents.
- Output of the 300k rows: 1.0.0 29-30 uncompressed files of about 11 MB (325 MB); the interim
  and final builds one 61.4 MB ZSTD file (the final build with 32 MiB row groups, 2 in this file).
- ZSTD 14 KB (the stored-fields default only in ES snapshot builds) was not run this time; the
  previous sitting measured it between LZ4 and ZSTD 240 KB (final 165k documents/s).

## Gate results

The same in every repeat of every run. No gate failure outside `bench/expected-failures.yaml`, so no
regression.

| dataset | index | 1.0.0 | interim and every final configuration | ES route |
|---|---|---|---|---|
| default | logs-lz4, logs-zstd, docs-nested, logs-zstd8, logs8-lz4, synth8, logs-default7, logs-deflate7 | pass | pass | pass |
| default | logs-logsdb, tsdb-hist | loud-failure (QUARANTINED with a NullPointerException, 0 rows) | pass | pass |
| default | vec-default | xfail (COMMITTED, every row lacks its vectors) | loud-failure (FAILED before its shard is downloaded, no table) | pass |
| frozen | ilm-big-000001, ilm-app8-000001..20 | pass | pass | pass |
| frozen | ilm-logsdb-000001..8 | loud-failure (as above) | pass | pass |

- "pass": the row count equals ES's live root count; the `(_id, _routing)` set is equal (no missing,
  extra or resurrected deleted id) with no duplicate `_id`; the sha256 of every stored `_source`
  equals the generator's bytes; the canonical hash equals ES's; `_source_kind` is as expected. At p4
  and with shard ranges the tables hold the same rows as at p1.
- The two expected failures are documented product behaviour: the logsdb and TSDB quarantine is
  fixed since the interim build, and an index whose vectors are excluded from `_source` fails loudly
  since the interim build instead of silently dropping the vectors; rehydrating them is open.
- The product invariants held in every run: no write to the snapshot bucket, no Parquet file without
  an AddFile, no document id or content in any report or manifest.
- `gsbench compare` exits 0 (no unexpected gate failure, no invariant violation, the document-table
  schema equal to 1.0.0's) for main -> interim-p4, main -> final-p1, main -> final-p4, interim-p4 ->
  final-p4, final-p1 -> final-p4 and final-p1 -> final-p4-kauto at smoke and standard, and main ->
  final-p4, main -> final-p4-kauto, interim-p4 -> final-p4, interim-p4 -> final-p4-kauto and
  final-p4 -> final-p4-kauto on the frozen tier.

## Where Gefrierschrank beats the ES route, where it trails, and why

Gefrierschrank is ahead:

- **On every stored-source index, and by the most on multi-segment `best_compression` ones.** The
  final build reads each segment's stored fields front to back with the sequential merge-instance
  reader and hands the stored `_source` bytes to Parquet unchanged (the byte row path):
  83-135k documents/s per thread at standard scale, download and checksums included. The ES route
  pays for a fetch phase that serializes every hit to JSON, 8 client processes that parse it and
  re-encode it as gzip NDJSON, and a second pass that parses the NDJSON again into Parquet: about 4x
  the route CPU (299.8 s against 64.8-72.9 s at standard scale, 314 s against 78 s on the frozen
  tier). On `logs-zstd` (ZSTD, 34 segments, deletes) and `logs-deflate7` (DEFLATE on 8.17) ES itself
  spent 98 s and 45 s of CPU (Gefrierschrank needs 0.16x and 0.31x of its time there), against 6-10
  s on the LZ4 and force-merged ZSTD indices of similar size; that is consistent with its sliced
  fetch decompressing a whole block per hit where it cannot use the sequential reader.
- **On large single shards.** With `--shard-threads auto` the 3M-document shard is read as 2 ranges:
  14.1 s for 3M documents including the download, against the ES route's 39.2 s of export and
  transform after a 6.6 s restore. Without ranges the final build needs 24.0 s, still 0.61x of ES's
  export and transform.
- **On resources.** Peak RSS 1.5-2.2 GB against 4.9 GB (standard) and 7.1 GB (frozen tier), peak
  disk 0.5-0.7 GB against 1.5 GB and 2.5 GB against 6.7 GB: the ES route holds the restored index on
  its data volume plus the NDJSON staging plus the output, Gefrierschrank at most the shards in
  flight plus the output.
- **Without a cluster.** The ES route's node start-up (29 s here, not counted) and, outside a
  benchmark, provisioning an export cluster and registering the repository do not exist on this
  route.

The ES route is ahead:

- **On synthetic-source shards too small to be split.** `logs-logsdb` 3.05 s against 7.18 s at final
  p1, `synth8` 2.28 s against 5.61 s, and the frozen tier's 80,000-document logsdb generations (1.4
  s each against about 3.9 s at p4). The per-document cost is similar (ES spent 7.8 s of CPU on
  `logs-logsdb`; Gefrierschrank's reconstruction plus writer costs about 30 us per document), but
  ES reads one shard with 8 slices while Gefrierschrank reads it with one thread: `--shard-threads`
  splits a shard only into ranges of at least 250,000 live documents (`MIN_RANGE_LIVE_DOCS`), so
  these shards are never split. In the previous sitting, `--shard-threads 4` with a 50,000-document
  minimum brought `logs-logsdb` (then 9 segments) to 2.04 s against ES's 2.98 s. Lowering the
  minimum for synthetic shards is the lever; it is not the shipped default.
- **On small repositories, if its start-up is not counted.** At smoke scale fixed per-JVM costs (one
  JVM per snapshot, about 2 s each, plus about 2 s to build the isolated Elasticsearch class loader
  for the first synthetic shard) dominate: 12.3 s against 9.0 s, or against about 38 s with ES's
  node start-up.

## Data for three defaults

What this benchmark shows about three shipped defaults.

- **`--parallelism` default (shipped 1).** At standard scale p4 takes 0.61x the wall
  time of p1 for 12% more CPU, 1.43x the peak RSS (2.21 against 1.54 GB at `-Xmx4g`) and 1.5x the
  peak disk (724 against 481 MB). p1 was not run on the frozen tier; on a repository of 16
  single-shard indices, an earlier measurement put p4 at 0.46x of p1.
- **`--shard-threads auto` default (shipped 1 when measured, `auto` since 2026-10-09).** On the one
  shard large enough to be split (3M documents, 2 ranges at p4) it takes 0.59x the time (14.1
  against 24.0 s) for 2% more route CPU, the same peak RSS and one more Parquet file, with the same
  rows (gate). It never splits the synthetic-source shards where the ES route is ahead, because of
  the 250,000-document minimum per range. A check of `PER_RANGE_HEAP_BYTES` on wide documents is
  still to be run.
- **Selective fetch (not in these builds).** Every build downloads 1.69 GB at standard scale for 906
  MB of stored fields, and 2.36 GB on the frozen tier for 1.02 GB of stored fields (synthetic
  indices also need doc values and terms). On local MinIO the download is too cheap to separate from
  the wall time; on S3 it is the part selective fetch removes.

## What the numbers do not show

- **Real S3 and cloud disks.** MinIO runs on the same host with sub-millisecond latency and no
  bandwidth limit, so the download is nearly free here and the numbers mostly measure CPU. On S3 the
  download share grows; the 8 streams per shard with ranged 16 MiB reads, body resume and the S3A
  upload defaults matter there. These paths were exercised only against MinIO. `--target s3a` was
  not run here.
- **Selective fetch is not done.** Every build downloads and checksum-verifies every file of every
  shard (postings, points, norms, doc values), 1.69 GB at standard scale for 906 MB of stored
  fields. Downloading only what a stored-source export reads was left for the next release; it would
  cut S3 bytes and temp disk, and it is where the ES route's restore (which also copies every file)
  could be beaten on bytes, not only on time.
- **macOS/APFS, one machine.** The mmap reconstruction gain and all I/O figures come from macOS
  on an APFS SSD; Linux (cheaper `pread`), EBS and network file systems were not measured. The ES
  route's single 2 GB node shares the same 8 cores with its exporter; a dedicated export cluster
  can be larger, and its provisioning, node start-up and snapshot-repository setup are not counted.
- **Larger scales.** The largest shard here has 3M documents and the largest repository 4.4M; the
  `--scale large` profile (10M), a 5M+ single shard and multi-TB repositories were not run. The
  memory gate, the heap bound of shard threads and the temp budget are exercised by the test suite,
  not here; every run had 4 GiB of heap.
- **Other settings.** `--shard-threads` above `auto` at p4 (2 per shard), `--parallelism` above 4,
  `--shard-threads auto` at p1 (8 threads per shard) and the ES route with more or fewer slices were
  not measured in this sitting.
- **Recovery paths.** Crash, resume, quarantine and retry are covered by the suite, not by gsbench;
  the gate does not check the lineage columns (`_shard`, `_doc`, `_snapshot_uuid`).

## Status of the known gaps after 1.1.0

| gap | status after 1.1.0 |
|---|---|
| JSON round trips per row | done: the byte row path hands the reader's bytes to Parquet; a strict validator sends only non-canonical sources through the old per-document path |
| intra-shard parallelism | done: `--shard-threads`; default `auto` since 2026-10-09 |
| `DocumentTooLargeException` guard | done: the document memory gate |
| thread-safe repository inspector, index-level parallelism | done: memoized immutable catalog; `--parallelism` spans indices |
| an original and its mounted shells exported twice | reported, not avoided: `survey` lists exact-identity groups and their bytes; an alias mode was left for the next release |
| failures found only after download | partly: ES 6.x-created and provably source-less indices, and excluded vectors, fail before any download; ES 8+ indices whose `_source` was later disabled and nested objects under synthetic source still download first |
| no in-run retry of quarantined shards | open by choice: broken bodies are resumed and the SDK retries each request, but a quarantined shard waits for the next run |
| resume key tied to the snapshot UUID | resolved for mounted shells whose snapshot SLM retention deleted: such runs complete under `auto`; content digests and incremental re-export are not built |
| worker stdout noise | mitigated: the deprecated worker's environment is scrubbed and its stdout line count checked; the worker is removed next release |
| selective fetch | not built; first item for the next release |
| S3A upload inside `ParquetWriter.close()` | reduced by 32 MiB row groups and 16 MiB parts (serial share 9.2% -> 2.8% on big files at 50 MB/s); re-measure on real S3 |
| vector rehydration | open; such indices fail loudly |
