# bench/synthfetch: selective fetch of synthetic-source shards

Measures what `dump-to-delta --selective-fetch` saves on synthetic-source shards: bytes downloaded
from S3 and peak temp disk, with the baseline jar, the topic jar without the flag and the topic jar
with it, on the same snapshot. gsbench's datasets cannot show it: every synthetic index there is one
compound segment, which selective fetch downloads whole.

## Datasets (once)

```sh
SLOT=/path/to/slot.sh   # semaphore `slot.sh <pool> <n> <cmd...>` (bench/README.md); without one, drop the `$SLOT <pool> <n>` prefixes
$SLOT docker 1 python3 bench/synthfetch/generate.py            # --scale small for a quick check
```

Starts MinIO `gs-synthfetch-minio` (ports 9253/9254, data in `target/synthfetch/minio-data`) and
Elasticsearch `gs-synthfetch-es93` (image `gefrierschrank-es9`, 9.3.0, port 9252, trial license),
indexes, force-merges each index to one segment, snapshots into `s3://synthfetch/repo` and removes
both containers. Refuses to run when `target/synthfetch` exists; `datasets.json` records sizes.

| index | documents (standard) | content |
|---|---|---|
| `logsdb-nc` / `logsdb-c` | 1,500,000 | logsdb, ECS-like (keyword, ip, long, match_only_text, wildcard, flattened) |
| `tsdb-nc` / `tsdb-c` | 5,000,000 | time_series: dimension, gauge, counter, histogram |
| `nested-nc` / `nested-c` | 1,000,000 roots | logsdb with a nested `items` field |

`-nc` indices set `index.compound_format: false`, so their segment is not compound, like a
segment of 1 GB and more; the `-c` twins keep the default and are compound (control:
fetched in full, no probe).

## Runs (inside one bench hold)

```sh
SLOT=$SLOT $SLOT bench 1 bench/synthfetch/run.sh <baseline-cli-jar> <topic-cli-jar> [repeats, default 3]
```

Takes the docker slot only while its MinIO runs (`measure.py`). Per repeat, the baseline jar, the
topic jar and the topic jar with `--selective-fetch` export every index back to back
(`java -Xmx4g -jar <jar> dump-to-delta s3://synthfetch/repo --index <index>`), each into a fresh
table. Per run: wall, CPU, peak RSS, peak temp disk (200 ms samples of the run's
`java.io.tmpdir`), S3 GETs and bytes sent (MinIO bucket metrics, settled scrapes), and per shard
`bytesFetched`, `bytesProcessed`, `fetch` and the content-free selective-fetch note (structures
kept, probe calls and milliseconds, or why the shard was fetched in full). The first repeat's tables
are digested: baseline vs topic flag off, topic flag off vs flag on (`gsbench compare-digests
--strict`). The baseline jar (1.1.0) fails `nested-*` (nested objects under synthetic source became
exportable in 1.2.0), so those tables exist only for the topic jar.

Results: `target/synthfetch/results/<timestamp>/` (`runs.jsonl`, `summary.json`, `digests-*.jsonl`,
`compare-*.txt`, `load.txt`, `jars.txt`). `SYNTHFETCH_JVM_ARGS` adds JVM options to every run, for
example `-Dgefrierschrank.download.selective.syntheticMinBytes=0` to exercise the probe on the small
datasets; never set it for a measurement.
