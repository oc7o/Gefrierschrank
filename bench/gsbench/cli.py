"""gsbench: prepare | run | gate | compare | digest | compare-digests | fetch-modes | report | down | shadow."""

import argparse
import json
import shlex
import sys

from . import config
from .datasets import suite_majors


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="gsbench", description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)

    def dataset_opts(sp):
        sp.add_argument("--scale", default="smoke", choices=["tiny", "smoke", "standard", "large"])
        sp.add_argument("--seed", type=int, default=42)
        sp.add_argument("--suite", default="default", choices=["default", "frozen"],
                        help="dataset suite: default (11 indices of every shape) or frozen (an ILM frozen tier: "
                             "many single-shard force-merged generations and one very large one)")
        sp.add_argument("--majors", help="ES majors whose snapshots are used (default: all of the suite, "
                                         "9,8,7 or frozen's 9,8)")

    sp = sub.add_parser("prepare", help="build (or reuse) the seeded snapshot datasets and their oracles")
    dataset_opts(sp)
    sp.add_argument("--rebuild", action="store_true", help="ignore cached datasets")

    sp = sub.add_parser("run", help="benchmark one CLI jar against the baseline and gate both")
    dataset_opts(sp)
    sp.add_argument("--jar", required=True, help="Gefrierschrank CLI fat jar")
    sp.add_argument("--worker-jar", help="gefrierschrank-lucene9 worker jar; default: the one beside the CLI jar "
                                         "or in the same Maven reactor build, if present")
    sp.add_argument("--no-worker-jar", action="store_true", help="run without any worker jar")
    sp.add_argument("--label", required=True, help="name of this build, e.g. main or candidate")
    sp.add_argument("--repeat", type=int, default=3)
    sp.add_argument("--routes", default="gs,es", help="gs, es or gs,es")
    sp.add_argument("--jvm-args", default="", type=shlex.split, help="extra JVM options for the CLI jar")
    sp.add_argument("--gs-args", default="", type=shlex.split,
                    help="extra dump-to-delta options (flags older jars lack go only here)")
    sp.add_argument("--target", default="local", choices=["local", "s3a"], help="Delta output location")
    sp.add_argument("--slices", type=int, default=0, help="baseline PIT slices (default max(shards, cores))")
    sp.add_argument("--keep-output", action="store_true", help="keep Delta/NDJSON outputs after gating")
    sp.add_argument("--rebuild", action="store_true")
    sp.add_argument("--results", help=f"results root (default {config.RESULTS_DIR})")

    sp = sub.add_parser("gate", help="gate a Delta table against a cached oracle, or --self-test")
    dataset_opts(sp)
    sp.add_argument("--self-test", action="store_true",
                    help="corrupt copies of a table (generated, or --table) in every way the gate targets; each "
                         "must fail exactly its checks")
    sp.add_argument("--table", help="Delta table to check (with --self-test: to corrupt copies of)")
    sp.add_argument("--index", help="index name of the table")

    sp = sub.add_parser("compare", help="markdown ratios of two result JSONs; exit 1 on unexpected failures in B")
    sp.add_argument("a")
    sp.add_argument("b")
    sp.add_argument("-o", "--output", help="also write the markdown here")

    sp = sub.add_parser("digest", help="order-independent digests of every Delta table under a root, one JSON line each")
    sp.add_argument("root", help="table root (every directory with a _delta_log below it) or a single table")
    sp.add_argument("--tables", help="comma-separated relative table paths or glob patterns to digest (default: all)")
    sp.add_argument("--out", required=True, help="JSONL file to write")

    sp = sub.add_parser("compare-digests",
                        help="compare two digest files; exit 1 on a mismatching common table")
    sp.add_argument("a", help="digest JSONL (or a fixture-digests.sh output directory)")
    sp.add_argument("b", help="digest JSONL (or a fixture-digests.sh output directory)")
    sp.add_argument("--rows-only", action="store_true", help="compare only rows and rowDigest")
    sp.add_argument("--strict", action="store_true", help="also exit 1 on tables present on one side only")

    sp = sub.add_parser("fetch-modes",
                        help="count the fetch mode of every shard in every run report under a root, at any depth")
    sp.add_argument("root", help="report root, e.g. a fixture-digests.sh output directory or a gsbench result directory")
    sp.add_argument("--per-report", action="store_true", help="also print one line per run report")

    sp = sub.add_parser("report", help="re-render the markdown of a result JSON")
    sp.add_argument("result")

    sp = sub.add_parser("down", help="remove every gs-bench container, volume and bucket, and the dataset cache")
    sp.add_argument("--keep-cache", action="store_true")

    sp = sub.add_parser("shadow", help="compare Delta tables with a read-only restore's fetch "
                                       "(exclude_vectors false); counts only, exit 1 on any mismatch")
    where = sp.add_mutually_exclusive_group(required=True)
    where.add_argument("--repo", help="filesystem snapshot repository (copied to a scratch bench MinIO prefix)")
    where.add_argument("--s3-base-path", help=f"repository base path in the bench MinIO bucket {config.SNAPSHOT_BUCKET}")
    sp.add_argument("--snapshot", help="snapshot name (default: the latest holding each index)")
    sp.add_argument("--tables", required=True, help="Delta root holding one table per index name")
    sp.add_argument("--indices", required=True, help="comma-separated index names")
    sp.add_argument("--major", type=int, default=9, choices=sorted(config.ES_NODES), help="ES node to restore on")
    sp.add_argument("--out", help="also write the JSON result here")

    args = p.parse_args(argv)
    if args.command == "prepare":
        from .infra import slot
        from .snapshot import prepare
        with slot("bench"):  # see runner.run: builds must not overlap another instance's measurements
            datasets = prepare(args.scale, args.seed, suite_majors(args.suite, args.majors), rebuild=args.rebuild,
                               suite=args.suite)
        for ds in datasets.values():
            print(json.dumps({"dataset": ds.dir.name, "base_path": ds.base_path, "snapshot": ds.snapshot,
                              "indices": {k: {"count": v["count"], "oracle": v["oracle_digest"][:16]}
                                          for k, v in ds.indices.items()}}))
        return 0
    if args.command == "run":
        from .runner import run
        return run(args)
    if args.command == "gate":
        from . import gate
        if args.self_test and not args.table:
            ok = gate.self_test()
            print("self-test " + ("passed" if ok else "FAILED"))
            return 0 if ok else 1
        if not (args.table and args.index):
            p.error("gate needs --self-test and/or --table and --index")
        from .snapshot import load_cached
        for major in suite_majors(args.suite, args.majors):
            ds = load_cached(major, args.scale, args.seed, args.suite)
            if ds and args.index in ds.indices:
                oracle, truth = str(ds.oracle_path(args.index)), str(ds.truth_path(args.index))
                kind = ds.indices[args.index]["kind"]
                if args.self_test:
                    ok = gate.self_test(table=args.table, oracle_path=oracle, truth_path=truth, kind=kind)
                    print("self-test " + ("passed" if ok else "FAILED"))
                    return 0 if ok else 1
                res = gate.gate_table(args.table, oracle, truth, kind)
                print(gate.dumps(res))
                return 0 if res["passed"] else 1
        p.error(f"no cached dataset holds index {args.index} (run gsbench prepare first)")
    if args.command == "compare":
        from .compare import main as compare_main
        return compare_main(args.a, args.b, args.output)
    if args.command == "digest":
        from .tabledigest import digest
        return digest(args.root, args.out, args.tables)
    if args.command == "compare-digests":
        from .tabledigest import compare_digests
        return compare_digests(args.a, args.b, rows_only=args.rows_only, strict=args.strict)
    if args.command == "fetch-modes":
        from .tabledigest import fetch_modes
        return fetch_modes(args.root, per_report=args.per_report)
    if args.command == "report":
        from pathlib import Path

        from .report import render
        print(render(json.loads(Path(args.result).read_text())))
        return 0
    if args.command == "down":
        from .infra import down
        down(keep_cache=args.keep_cache)
        return 0
    if args.command == "shadow":
        from .shadow import run as shadow_run
        return shadow_run(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
