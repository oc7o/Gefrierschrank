# Vector rehydration benchmark

Throughput and heap of exporting indices whose stored `_source` excludes vectors (ES 9.2+
default), measured like for like: the 1.1.0 jar cannot export such an index, so it exports the
`<name>-included` twin (same documents, vectors kept in `_source`), and the 1.2.0 jar exports the
excluded `<name>` with the vectors put back by Elasticsearch's loaders. The output rows are the same
up to float text.

Correctness is not measured here: the vector fixture tests (`SourceVectorsExportFixtureTest`,
`Es93VectorsRestoredFetchTest`) and `gsbench shadow` compare the rows with Elasticsearch's own
fetch.

## Datasets

`generate.sh <out> [docs]` (default 200,000 documents per index; `vec-nested` gets a third of
that, with three chunks each) starts its own container `gs-vectors-bench-es9` (ES 9.3.0, port
9242), indexes each pair with identical, seeded documents, force-merges every index to one segment
per shard (two shards each) and snapshots all of them into the filesystem repository `<out>/repo`.
The container is removed on exit. Hold the docker slot, outside any bench hold:

```sh
SLOT=/path/to/slot.sh   # semaphore `slot.sh <pool> <n> <cmd...>` (bench/README.md); without one, drop the `$SLOT <pool> <n>` prefixes
$SLOT docker 1 bench/vectors/generate.sh target/vectors-data
```

| index | vectors |
|---|---|
| `vec384` | `dense_vector` float, 384 dims, cosine, `int8_hnsw` (the ES default) |
| `vec384-bbq` | the same with `bbq_hnsw` |
| `vec-nested` | nested `chunks`, 3 per document, each with a 384-dim float vector |
| `vec-sparse` | `sparse_vector`, about 100 tokens per document |

## Runs

`run.sh throughput <baseline-jar> <topic-jar> <data> <out> [repeats] [datasets] [threads]` runs
the two jars back to back per dataset and `--shard-threads` value (default `1,auto`), `-Xmx4g`,
one shard at a time, and writes `runs.tsv` and `summary.md` (medians: docs/s, output MB/s of
Parquet, CPU seconds, max RSS). `run.sh heap ...` samples the live heap once a second while
each export runs (`jcmd GC.class_histogram`, keeping the largest) at `--shard-threads` 1 and 4 for both jars and keeps a JFR recording
with old-object samples per run; the difference between the jars is the heap rehydration holds,
which sets `SourceVectorsShardReader.RANGE_HEAP_BYTES`.

`GSBENCH_VECTORS_JVM_ARGS` adds JVM options to every export; with 100k-document shards `auto`
keeps one range per shard (a range needs 250k live documents), so range scaling and the heap per
range are measured with `-Dgefrierschrank.shard.threads.minRangeLiveDocs=1000` and threads 4.

Both modes take the machine-wide bench slot themselves when `GSBENCH_SLOT` is set and need no
container.
