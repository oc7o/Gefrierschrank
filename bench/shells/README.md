# Mounted shells harness

Measures and accepts `dump-to-delta --all-indices --mounted-shells skip-if-original` on an
ILM-shaped repository: 8 single-shard originals `applogs-1..8` of 30,000 ECS-like documents
(1 in 97 deleted, 1 in 7 routed) and their 8 mounted shells `partial-applogs-N`, each mounted
from the original's own ILM snapshot. Each original's latest snapshot is exactly what its shell
mounts, so `survey` reports 8 exact-identity groups and a default export reads every shard twice.

## Generate the repository (docker slot, about 2 minutes)

```sh
SLOT=/path/to/slot.sh   # semaphore `slot.sh <pool> <n> <cmd...>` (bench/README.md); without one, drop the `$SLOT <pool> <n>` prefixes
W=/path/to/Gefrierschrank   # any checkout
$SLOT docker 1 bash $W/bench/shells/generate.sh $W/target/shells --retention
```

Container `gs-shells-es9` (image `gefrierschrank-es9`, Elasticsearch 9.3) on host port 9341,
removed on exit. Output (untracked, under `target/`):

- `ilm-repo`: the 16 names after the frozen phase (snapshots `slm-2026.09.20`, one
  `2026.09.21-applogs-N-frozen-policy` per original, `slm-2026.09.21`, `slm-2026.09.22`);
- `ilm-repo-retained` (with `--retention`): the shells re-mounted, `slm-2026.09.23` taken and
  `slm-2026.09.21/22` deleted by retention;
- `expected.json`: snapshot names and per-original document counts (237,520 rows in total).

The documents are seeded; the repository's UUIDs differ per generation, so compare runs of one
generation with each other, never digests across generations.

## Measure and accept (bench slot)

```sh
BASE=/path/to/gefrierschrank-cli-1.1.0.jar   # the baseline jar (a v1.1.0 build)
NEW=$W/gefrierschrank-cli/target/gefrierschrank-cli-1.2.0.jar   # the jar under test
$SLOT bench 1 bash $W/bench/shells/run.sh $BASE $NEW $W/target/shells/ilm-repo $W/target/shells-run 5
bash $W/bench/shells/run.sh --retention $NEW $W/target/shells/ilm-repo $W/target/shells/ilm-repo-retained $W/target/shells-retention
```

`run.sh` runs A (baseline jar, default), B (jar under test, default) and C (jar under test,
`--mounted-shells skip-if-original`) at `--parallelism 1` and `4`, `-Xmx4g`, each into a fresh root,
the order rotated per repetition. It needs no container. Per run it records exit code, wall,
user+sys and max RSS (`/usr/bin/time -l`), the rows and source bytes of the per-index reports,
the output size, C's planning time (from its `planned in N ms` line) and the load average before
and after (`results.tsv`, medians in `summary.md`).

On the first repetition of each parallelism it checks (`acceptance.txt`, exit 1 on any failure):

- A, B and C exit 0;
- A and B: byte-identical manifests; per-index reports equal once `startedAt`, `asOf`,
  `elapsedMillis`, `finishedAt` and the 1.2.0 report keys `bytesFetched`/`fetch` are masked;
  stdout equal once the build line, table paths, `in N ms` and the temp-disk budget are masked
  (as a set of lines at `--parallelism 4`, where indices start concurrently); equal table digests
  (`gsbench digest`, `gsbench compare-digests --strict`);
- C: exactly the originals' tables and no `partial-*` directory, every shell `ALIAS_OF` its
  original, `aliasCount` 8, `complete` true, nothing pending, its tables' digests equal to A's
  tables of the same names.

`--retention` checks the interplay with SLM retention: a default run on `ilm-repo`, then a
default run on `ilm-repo-retained` into the same root (exit 0, `behindLatestCount` 8); an alias
run on `ilm-repo-retained` into a fresh root (8 tables, 8 `ALIAS_OF`); an alias run on
`ilm-repo-retained` over the default root (16 completed, no alias, every shard skipped).
