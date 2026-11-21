# bench/synthcov: cost of nested and source-disabled reconstruction

Measures what this reconstruction costs: nested objects under synthetic source, documents rebuilt
through the rewritten mapping after `_source` was disabled, and that neither changes the plain
synthetic path.

## Datasets (once)

```sh
SLOT=/path/to/slot.sh   # semaphore `slot.sh <pool> <n> <cmd...>` (bench/README.md); without one, drop the `$SLOT <pool> <n>` prefixes
$SLOT docker 1 python3 bench/synthcov/generate.py generate
```

Starts `gs-synthcov-bench-es93` (image `gefrierschrank-es9`, ES 9.3.0, host port 9262, 2 GB heap),
indexes, force-merges to one segment per shard, snapshots into `target/synthcov/repo` and removes
the container. Refuses to run when `target/synthcov` exists.

| index | shards | content |
|---|---|---|
| `nested-synth` | 1 | 150k logs-like roots with 0-5 `items` as `nested`, synthetic source |
| `flat-synth` | 1 | the same documents, `items` a plain object array, synthetic source |
| `srcoff` | 2 | 150k stored documents routed to shard 0, then `_source.enabled: false`, then 150k more routed to shard 1 |
| `srcoff-twin-synth` | 1 | srcoff's second half, native synthetic source |

`target/synthcov/expected/<index>.jsonl.gz` holds Elasticsearch's own search output of every
document (for srcoff: the exact text sent for the stored half, the twin's rendering for the rest).

## Runs (inside one bench hold)

```sh
$SLOT bench 1 bench/synthcov/run.sh <baseline-cli-jar> <topic-cli-jar> [repeats, default 3]
```

Every run is `java -Xmx4g -jar <jar> dump-to-delta target/synthcov/repo --index <index>
--shard-threads <k> --parallelism 1` into a fresh table, timed with `/usr/bin/time -l` (wall
seconds, peak RSS) and read back from its run report (rows and milliseconds per shard):

1. `nested-synth` vs `flat-synth` on the topic jar at `--shard-threads 1` and `auto`;
2. `srcoff` shard 1 (rebuilt through the rewrite) vs `srcoff-twin-synth` (native) at one thread;
3. `flat-synth` on the baseline and the topic jar, back to back, at `auto` (table digests compared);
4. peak RSS of (1) at `auto` against the baseline's flat run.

One table per jar and index is checked against the expected rows (`generate.py verify`, needs the
bench uv environment: value equality per `_id`, stored rows byte-exact, nothing missing or extra).
Results: `target/synthcov/results/<timestamp>/` (`runs.tsv`, `summary.json`, `verify.jsonl`,
`flat-digests.jsonl`, `load.txt`). No container runs during the timed part.
