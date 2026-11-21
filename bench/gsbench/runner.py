"""`gsbench run`: datasets, both routes, gate, result JSON + markdown."""

import json
import shutil
import time
from pathlib import Path

from . import config, gate, invariants, metrics, report
from .infra import log, slot
from .datasets import suite_majors
from .routes import es_baseline, gs
from .snapshot import prepare

GS_SUMMARY_KEYS = ("wall_s", "cpu_s", "cpu_user_s", "cpu_sys_s", "peak_rss_bytes", "peak_disk_bytes")


def _gate_jobs(datasets: dict, tables: dict[str, str], storage_options=None) -> tuple[list[str], list[dict]]:
    names, jobs = [], []
    for ds in datasets.values():
        for index, meta in ds.indices.items():
            names.append(index)
            jobs.append({"table": tables.get(index), "oracle": str(ds.oracle_path(index)),
                         "truth": str(ds.truth_path(index)), "kind": meta["kind"], "storage_options": storage_options})
    return names, jobs


def _gs_route(args, jar: Path, worker: Path | None, datasets: dict, run_dir: Path, run_id: str, result: dict,
              gate_results: dict) -> None:
    installed, installed_worker = gs.install(jar, worker, run_dir / "install")
    reps: list[dict] = []
    result.setdefault("routes", {})["gs"] = {"repeats": reps}  # a partial result keeps finished repeats
    try:
        for rep in range(1, args.repeat + 1):
            entry = gs.run_repeat(installed, installed_worker, datasets, run_dir, rep, args.jvm_args,
                                  args.gs_args, args.target, run_id)
            tables = {n: i["table"] for m in entry["majors"].values() for n, i in m["indices"].items()}
            so = gs.s3_storage_options() if args.target == "s3a" else None
            names, jobs = _gate_jobs(datasets, tables, so)
            for name, res in zip(names, gate.gate_many(jobs)):
                gate_results["gs"].setdefault(name, []).append(res)
                log(f"gate gs rep {rep} {name}: {'PASS' if res['passed'] else 'FAIL'} {gate.summary_line(res)}")
            if not args.keep_output:
                gs.cleanup(run_dir, rep, args.target, run_id)
            reps.append(entry)
    finally:
        # without APFS clones every run would keep a full copy of the jars (~0.9 GB)
        shutil.rmtree(run_dir / "install", ignore_errors=True)
    leaks = {}
    for major, ds in datasets.items():
        leaks.update(invariants.report_leaks(sorted(run_dir.glob(f"gs/rep*/es{major}/report")),
                                             [ds.oracle_path(i) for i in ds.indices]))
    result["report_leaks"] = leaks


def run(args) -> int:
    run_id = time.strftime("%Y%m%d-%H%M%S") + f"-{args.label}"
    run_dir = (Path(args.results) if args.results else config.RESULTS_DIR) / run_id
    run_dir.mkdir(parents=True)
    log(f"run {run_id} -> {run_dir}")
    jar = Path(args.jar).resolve()
    if args.no_worker_jar:
        worker = None
    elif args.worker_jar:
        worker = Path(args.worker_jar).resolve()
    else:
        worker = gs.find_worker_jar(jar)
        if worker:
            log(f"using worker jar found next to the CLI jar's build: {worker}")
    majors = suite_majors(args.suite, args.majors)
    routes = set(args.routes.split(","))
    result = {
        "schema": 1, "run_id": run_id, "created": time.strftime("%Y-%m-%dT%H:%M:%S%z"), "label": args.label,
        "suite": args.suite, "scale": args.scale, "seed": args.seed, "repeat": args.repeat, "target": args.target,
        "jvm_args": args.jvm_args, "gs_args": args.gs_args, "slices": args.slices or "max(shards, cores)",
        "machine": metrics.machine_info(), "load_start": metrics.load(),
        "jar": gs.jar_info(jar), "worker_jar": gs.jar_info(worker) if worker else None,
    }
    started = time.monotonic()
    expected = gate.load_expected()
    gate_results: dict[str, dict[str, list]] = {"gs": {}, "es": {}}

    try:
        # The bench slot covers the dataset build too: no other gsbench instance on the shared bench
        # MinIO may rebuild or remove these snapshots, or add S3 traffic, while this run measures.
        with slot("bench"):
            result["bench_slot_wait_seconds"] = round(time.monotonic() - started, 1)
            prepared = time.monotonic()
            datasets = prepare(args.scale, args.seed, majors, rebuild=args.rebuild, suite=args.suite)
            result["prepare_seconds"] = round(time.monotonic() - prepared, 1)
            result["datasets"] = {f"es{m}": {k: v for k, v in ds.meta.items()} for m, ds in datasets.items()}
            metrics.minio_check(f"{next(iter(datasets.values())).base_path}/index.latest")
            result["minio"] = metrics.minio_info()
            if "gs" in routes:
                _gs_route(args, jar, worker, datasets, run_dir, run_id, result, gate_results)
            if "es" in routes:
                def es_done(rep, entry):
                    tables = {n: i["table"] for n, i in entry["indices"].items()}
                    names, jobs = _gate_jobs(datasets, tables)
                    for name, res in zip(names, gate.gate_many(jobs)):
                        gate_results["es"].setdefault(name, []).append(res)
                        log(f"gate es rep {rep} {name}: {'PASS' if res['passed'] else 'FAIL'} {gate.summary_line(res)}")
                    if not args.keep_output:
                        es_baseline.cleanup(run_dir, rep)

                reps = es_baseline.run(datasets, run_dir, args.repeat, args.slices, es_done)
                result.setdefault("routes", {})["es"] = {"repeats": reps}
    except BaseException as e:
        result["error"] = f"{type(e).__name__}: {e}"
        result["gate_results"] = gate_results
        result["seconds"] = round(time.monotonic() - started, 1)
        (run_dir / "result.partial.json").write_text(json.dumps(result, indent=2, default=str))
        log(f"run failed; what was measured so far: {run_dir / 'result.partial.json'}")
        raise

    result["load_end"] = metrics.load()
    result["gate"] = resolve_gate(result, datasets, gate_results, expected, worker is not None)
    result["invariants"] = invariants.violations(result)
    result["summary"] = summarize(result, datasets)
    result["seconds"] = round(time.monotonic() - started, 1)
    (run_dir / "result.json").write_text(json.dumps(result, indent=2, default=str))
    md = report.render(result)
    (run_dir / "result.md").write_text(md)
    print(md)
    log(f"results: {run_dir / 'result.json'}")
    failures = [f"{route}/{i}" for route, ix in result["gate"].items() for i, g in ix.items() if g["status"] == "fail"]
    if failures:
        log(f"UNEXPECTED GATE FAILURES: {', '.join(failures)}")
    for v in result["invariants"]:
        log(f"INVARIANT VIOLATED: {v}")
    return 2 if failures or result["invariants"] else 0


def resolve_gate(result: dict, datasets: dict, gate_results: dict, expected: list, has_worker: bool) -> dict:
    out = {}
    gs_reps = result.get("routes", {}).get("gs", {}).get("repeats", [])
    build_info = next((m["build_info"] for r in gs_reps for m in r["majors"].values() if m.get("build_info")), None)
    version = gs.BUILD_INFO.match(build_info).group(1) if build_info else None
    result["jar"]["build_info"] = build_info
    result["jar"]["version"] = version
    for route, per_index in gate_results.items():
        if not per_index:
            continue
        out[route] = {}
        for ds in datasets.values():
            for index in ds.indices:
                runs = per_index.get(index, [])
                entries = []
                outcomes = []
                if route == "gs":
                    entries = gate.matching_expected(expected, result["label"], version, index, has_worker)
                    outcomes = [r["majors"][f"es{ds.major}"]["indices"][index]["outcome"] for r in gs_reps]
                resolved = [gate.resolve_any(res, outcomes[k] if outcomes else None, entries)
                            for k, res in enumerate(runs)]
                statuses = [s for s, _ in resolved]
                status = gate.worst(statuses)
                entry = next((e for s, e in resolved if s == status), None)
                failing = next((res for res in runs if not res["passed"]), runs[-1] if runs else None)
                out[route][index] = {
                    "status": status, "per_repeat": statuses, "outcomes": outcomes,
                    "expected": entry, "checks": failing["checks"] if failing else None,
                    "counts": failing["counts"] if failing else None,
                    "table_error": failing.get("table_error") if failing else None,
                }
    return out


def summarize(result: dict, datasets: dict) -> dict:
    s = {}
    routes = result.get("routes", {})
    passing = {route: {i for i, g in ix.items() if g["status"] in ("pass", "xpass")}
               for route, ix in result["gate"].items()}
    # Route totals include indices a route got wrong (e.g. quarantined in seconds); the headline
    # ratio compares only indices both routes exported correctly.
    both = sorted(passing.get("gs", set()) & passing.get("es", set()))
    s["both_passing"] = both
    if "gs" in routes:
        reps = routes["gs"]["repeats"]
        g = {k: metrics.summarize([r["total"][k] for r in reps]) for k in GS_SUMMARY_KEYS}
        g["s3_requests"] = metrics.summarize([r["total"]["s3"].get(config.SNAPSHOT_BUCKET, {}).get("requests_total", 0)
                                              for r in reps])
        g["s3_get"] = metrics.summarize([r["total"]["s3"].get(config.SNAPSHOT_BUCKET, {}).get("requests", {})
                                         .get("getobject", 0) for r in reps])
        g["s3_tx_bytes"] = metrics.summarize([r["total"]["s3"].get(config.SNAPSHOT_BUCKET, {}).get("tx_bytes", 0)
                                              for r in reps])
        for k in ("bytes", "data_bytes", "add_files", "parquet_files"):
            g[f"output_{k}"] = metrics.summarize([r["total"]["output"][k] for r in reps])
        if result["target"] == "s3a":
            out_bucket = [r["total"]["s3"].get(config.OUTPUT_BUCKET, {}) for r in reps]
            g["s3_output_requests"] = metrics.summarize([b.get("requests_total", 0) for b in out_bucket])
            g["s3_output_rx_bytes"] = metrics.summarize([b.get("rx_bytes", 0) for b in out_bucket])
        g["passing_elapsed_s"] = metrics.summarize([
            sum((i["elapsed_ms"] or 0) / 1000 for m in r["majors"].values() for n, i in m["indices"].items()
                if n in passing.get("gs", set())) for r in reps])
        g["both_passing_elapsed_s"] = metrics.summarize([
            sum((i["elapsed_ms"] or 0) / 1000 for m in r["majors"].values() for n, i in m["indices"].items()
                if n in both) for r in reps]) if both else None
        g["per_major"] = {mj: {k: metrics.summarize([r["majors"][mj][k] for r in reps]) for k in GS_SUMMARY_KEYS}
                          for mj in reps[0]["majors"]}
        s["gs"] = g
    if "es" in routes:
        reps = routes["es"]["repeats"]
        e = {}
        for phase in ("restore", "export", "transform"):
            e[phase] = {k: metrics.summarize([r["phases"][phase][k] for r in reps]) for k in ("wall_s", "cpu_s", "peak_rss_bytes")}
        e["total"] = {k: metrics.summarize([r["total"][k] for r in reps]) for k in ("wall_s", "cpu_s", "peak_rss_bytes", "peak_disk_bytes")}
        e["s3_requests"] = metrics.summarize([r["total"]["s3"].get(config.SNAPSHOT_BUCKET, {}).get("requests_total", 0) for r in reps])
        e["s3_get"] = metrics.summarize([r["total"]["s3"].get(config.SNAPSHOT_BUCKET, {}).get("requests", {}).get("getobject", 0) for r in reps])
        e["s3_tx_bytes"] = metrics.summarize([r["total"]["s3"].get(config.SNAPSHOT_BUCKET, {}).get("tx_bytes", 0) for r in reps])
        for k in ("bytes", "data_bytes", "add_files", "parquet_files"):
            e[f"output_{k}"] = metrics.summarize([r["total"]["output"][k] for r in reps])
        e["node_startup_s"] = {g["restore_node"]: g["node_startup_s"] for g in reps[0]["groups"].values()}
        e["passing_export_transform_s"] = metrics.summarize([
            sum(i["export"]["wall_s"] + i["transform"]["wall_s"] for n, i in r["indices"].items()
                if n in passing.get("es", set())) for r in reps])
        e["both_passing_export_transform_s"] = metrics.summarize([
            sum(i["export"]["wall_s"] + i["transform"]["wall_s"] for n, i in r["indices"].items() if n in both)
            for r in reps]) if both else None
        e["client_startup_s"] = metrics.summarize([r["phases"]["export"]["client_startup_s"] for r in reps])
        e["max_slice_cpu_per_wall"] = max((i["export"].get("max_slice_cpu_per_wall") or 0)
                                          for r in reps for i in r["indices"].values())
        s["es"] = e
    per_index = {}
    for ds in datasets.values():
        for index, meta in ds.indices.items():
            row = {"major": ds.major, "kind": meta["kind"], "docs": meta["count"], "source_bytes": meta["source_bytes"],
                   "segments": meta["segments"]}
            if "gs" in routes:
                ok = index in passing.get("gs", set())
                reps = [r["majors"][f"es{ds.major}"]["indices"][index] for r in routes["gs"]["repeats"]]
                row["gs"] = {"status": result["gate"]["gs"][index]["status"], "outcome": reps[-1]["outcome"],
                             "quarantine_errors": reps[-1]["quarantine_errors"],
                             "rows": reps[-1]["rows"],
                             "elapsed_s": metrics.summarize([(i["elapsed_ms"] or 0) / 1000 for i in reps]) if ok else None,
                             "output_bytes": reps[-1]["output"].get("total_bytes") if ok else None,
                             "add_files": reps[-1]["output"].get("add_files") if ok else None,
                             "parquet_files": reps[-1]["output"].get("parquet_files_on_disk") if ok else None}
            if "es" in routes:
                ok = index in passing.get("es", set())
                reps = [r["indices"][index] for r in routes["es"]["repeats"]]
                row["es"] = {"status": result["gate"]["es"][index]["status"], "rows": reps[-1]["rows"],
                             "export_s": metrics.summarize([i["export"]["wall_s"] for i in reps]) if ok else None,
                             "export_es_cpu_s": metrics.summarize([i["export"]["es_cpu_s"] for i in reps]) if ok else None,
                             "export_client_cpu_s": metrics.summarize([i["export"]["client_cpu_s"] for i in reps])
                             if ok else None,
                             "max_slice_cpu_per_wall": max(i["export"]["max_slice_cpu_per_wall"] or 0 for i in reps),
                             "transform_s": metrics.summarize([i["transform"]["wall_s"] for i in reps]) if ok else None,
                             "output_bytes": reps[-1]["output"].get("total_bytes") if ok else None,
                             "add_files": reps[-1]["output"].get("add_files") if ok else None}
            per_index[index] = row
    s["per_index"] = per_index
    return s
