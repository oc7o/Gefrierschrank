"""Tuned alternative: restore on an ES node, PIT + sliced search_after to NDJSON, delta-rs to Delta.

8.x and 9.x snapshots restore on ES 9.3; 7.x snapshots on ES 8.17 (9.x cannot read 7.x indices).
Recovery throttles are lifted, replicas are 0 and refresh is off, so restore runs at the speed
of the node and the object store. Restore, export and transform are timed separately; the
exporter (one process per slice, see es_export) and the transform run in their own processes so
their CPU/RSS are measured exactly.
"""

import json
import os
import shutil
import sys
import time
from pathlib import Path

from .. import config, metrics
from ..es import Es, Node
from ..infra import log

TUNING = {
    "indices.recovery.max_bytes_per_sec": "10gb",
    "indices.recovery.max_concurrent_file_chunks": 8,
    "cluster.routing.allocation.node_concurrent_recoveries": 16,
    "cluster.routing.allocation.node_initial_primaries_recoveries": 16,
}


def groups(datasets: dict) -> dict[int, list]:
    out: dict[int, list] = {}
    for major, ds in datasets.items():
        out.setdefault(config.RESTORE_NODE_FOR_MAJOR[major], []).append(ds)
    return out


def _es_measure(es: Es, fn):
    """Wall + container CPU + container peak memory around fn(); fn returns a dict merged in."""
    sampler = metrics.ContainerMemSampler(es.node.container)
    sampler.start()
    cpu0 = es.cpu_usec()
    t0 = time.monotonic()
    extra = fn()
    wall = time.monotonic() - t0
    cpu1 = es.cpu_usec()
    peak = sampler.stop()
    return {"wall_s": round(wall, 3), "es_cpu_s": round((cpu1 - cpu0) / 1e6, 3), "es_peak_mem_bytes": peak, **extra}


def restore(es: Es, ds, rep: int) -> dict:
    repo = f"bench_es{ds.major}"
    indices = list(ds.indices)

    before = metrics.minio_settled_scrape()

    def go():
        es.post(f"/_snapshot/{repo}/{ds.snapshot}/_restore", {
            "indices": ",".join(indices), "include_global_state": False,
            "index_settings": {"index.number_of_replicas": 0, "index.refresh_interval": "-1"},
        }, params={"wait_for_completion": "true"}, timeout=7200)
        es.get(f"/_cluster/health/{','.join(indices)}", params={"wait_for_status": "green", "timeout": "30m"},
               timeout=1900)
        return {}

    m = _es_measure(es, go)
    m["s3"] = metrics.minio_delta(before, metrics.minio_settled_scrape())
    metrics.check_s3_read(m["s3"], f"restore of {ds.base_path}")
    m["cpu_s"] = m["es_cpu_s"]
    m["peak_rss_bytes"] = m["es_peak_mem_bytes"]
    m["es_data_bytes"] = es.data_bytes()
    log(f"es rep {rep}: restored {ds.base_path} in {m['wall_s']:.1f}s ({m['es_data_bytes'] / 1e6:.0f} MB)")
    return m


def export_index(es: Es, ds, index: str, out_dir: Path, slices: int) -> dict:
    """wall_s and cpu_s cover the export window (PIT open to close, warm workers); the exporter
    process as a whole, start-up included, is in process_wall_s and client["cpu_s"]."""
    meta = ds.indices[index]
    cmd = [sys.executable, "-m", "gsbench.routes.es_export", "--url", es.url, "--index", index,
           "--out-dir", str(out_dir)]
    if slices:
        cmd += ["--slices", str(slices)]
    if meta.get("source_param") is not None:
        cmd += ["--source-param", json.dumps(meta["source_param"])]

    def go():
        client = metrics.run_measured(cmd, env=dict(os.environ), log_path=out_dir.parent / f"{index}.export.log",
                                      stdout_capture=True)
        if client["exit_code"] != 0:
            raise RuntimeError(f"export of {index} failed, see {out_dir.parent / f'{index}.export.log'}")
        stats = json.loads(client.pop("stdout"))
        return {"client": client, "window": stats}

    m = _es_measure(es, go)
    w = m.pop("window")
    m.update({"docs": w["docs"], "slices": w["slices"], "search_requests": w["search_requests"],
              "process_wall_s": m["wall_s"], "wall_s": round(w["seconds"], 3),
              "client_startup_s": round(w["startup_seconds"], 3),
              "client_cpu_s": round(w["client_cpu_s"], 3),
              "slice_wall_s": w["slice_wall_s"], "slice_cpu_s": w["slice_cpu_s"],
              "max_slice_cpu_per_wall": w["max_slice_cpu_per_wall"]})
    m["cpu_s"] = round(m["es_cpu_s"] + m["client_cpu_s"], 3)
    m["peak_rss_bytes"] = m["es_peak_mem_bytes"] + m["client"]["peak_rss_bytes"]
    m["ndjson_bytes"] = metrics.dir_size(out_dir)
    return m


def transform_index(ndjson_dir: Path, table: Path, log_path: Path) -> dict:
    cmd = [sys.executable, "-m", "gsbench.transform", "--in-dir", str(ndjson_dir), "--table", str(table)]
    m = metrics.run_measured(cmd, env=dict(os.environ), log_path=log_path, stdout_capture=True)
    if m["exit_code"] != 0:
        raise RuntimeError(f"transform into {table} failed, see {log_path}")
    m["rows"] = json.loads(m.pop("stdout"))["rows"]
    m["output"] = metrics.delta_output_stats(str(table))
    return m


def run(datasets: dict, run_dir: Path, repeats: int, slices: int, on_repeat_done) -> list[dict]:
    """Returns one entry per repeat; on_repeat_done(rep, entry) gates and cleans up its outputs."""
    reps = [{"rep": r, "groups": {}, "indices": {}} for r in range(1, repeats + 1)]
    for node_major, dss in groups(datasets).items():
        with Node(node_major) as es:
            if node_major >= 8:
                es.start_trial()
            es.put("/_cluster/settings", {"persistent": TUNING})
            for ds in dss:
                es.register_repo(f"bench_es{ds.major}", ds.base_path, readonly=True,
                                 extra={"max_restore_bytes_per_sec": "10gb", "max_snapshot_bytes_per_sec": "10gb"})
            for entry in reps:
                rep = entry["rep"]
                for ds in dss:
                    work = run_dir / "es" / f"rep{rep}" / f"es{ds.major}"
                    if work.exists():
                        shutil.rmtree(work)
                    work.mkdir(parents=True)
                    load_before = metrics.load()
                    # node provisioning is reported but kept out of the route totals
                    group = {"restore_node": es.node.version, "node_startup_s": es.startup_s,
                             "restore": restore(es, ds, rep), "indices": {}}
                    for index in ds.indices:
                        ndjson = work / "ndjson" / index
                        ndjson.mkdir(parents=True)
                        ex = export_index(es, ds, index, ndjson, slices)
                        tr = transform_index(ndjson, work / "delta" / index, work / f"{index}.transform.log")
                        shutil.rmtree(ndjson)  # a tuned pipeline drops the NDJSON once it is loaded
                        entry["indices"][index] = group["indices"][index] = {
                            "major": ds.major, "export": ex, "transform": tr, "rows": tr["rows"],
                            "table": str(work / "delta" / index), "output": tr["output"]}
                        log(f"es rep {rep}: {index}: export {ex['wall_s']:.1f}s ({ex['docs']} docs, "
                            f"{ex['slices']} slices, ES CPU {ex['es_cpu_s']:.1f}s, client CPU "
                            f"{ex['client_cpu_s']:.1f}s, busiest slice {ex['max_slice_cpu_per_wall']} core) "
                            f"transform {tr['wall_s']:.1f}s")
                    es.delete("/" + ",".join(ds.indices))
                    _finish_group(group, load_before)
                    entry["groups"][f"es{ds.major}"] = group
    for entry in reps:
        _finish_repeat(entry)
        on_repeat_done(entry["rep"], entry)
    return reps


def _finish_group(group: dict, load_before) -> None:
    ix = group["indices"].values()
    r = group["restore"]
    group["load_before"], group["load_after"] = load_before, metrics.load()
    # Peak disk: restored data plus, per index in order, its NDJSON next to all Delta output so far.
    delta_so_far = 0
    peak = r["es_data_bytes"]
    for i in ix:
        delta_bytes = i["output"].get("total_bytes", 0)
        peak = max(peak, r["es_data_bytes"] + delta_so_far + i["export"]["ndjson_bytes"] + delta_bytes)
        delta_so_far += delta_bytes
    group["peak_disk_bytes"] = peak


def _finish_repeat(entry: dict) -> None:
    groups_ = entry["groups"].values()
    ix = entry["indices"].values()
    restore_ = {"wall_s": sum(g["restore"]["wall_s"] for g in groups_),
                "cpu_s": sum(g["restore"]["cpu_s"] for g in groups_),
                "peak_rss_bytes": max(g["restore"]["peak_rss_bytes"] for g in groups_),
                "s3": _merge_s3([g["restore"]["s3"] for g in groups_])}
    export = {"wall_s": sum(i["export"]["wall_s"] for i in ix), "cpu_s": sum(i["export"]["cpu_s"] for i in ix),
              "es_cpu_s": sum(i["export"]["es_cpu_s"] for i in ix),
              "client_cpu_s": sum(i["export"]["client_cpu_s"] for i in ix),
              "client_process_cpu_s": sum(i["export"]["client"]["cpu_s"] for i in ix),
              "client_startup_s": sum(i["export"]["client_startup_s"] for i in ix),
              "peak_rss_bytes": max(i["export"]["peak_rss_bytes"] for i in ix),
              "ndjson_bytes": sum(i["export"]["ndjson_bytes"] for i in ix)}
    transform = {"wall_s": sum(i["transform"]["wall_s"] for i in ix), "cpu_s": sum(i["transform"]["cpu_s"] for i in ix),
                 "peak_rss_bytes": max(i["transform"]["peak_rss_bytes"] for i in ix)}
    output = {"bytes": sum(i["output"].get("total_bytes", 0) for i in ix),
              "data_bytes": sum(i["output"].get("data_bytes", 0) for i in ix),
              "add_files": sum(i["output"].get("add_files", 0) for i in ix),
              "parquet_files": sum(i["output"].get("parquet_files_on_disk", 0) for i in ix)}
    entry["phases"] = {"restore": restore_, "export": export, "transform": transform}
    entry["total"] = {
        "wall_s": restore_["wall_s"] + export["wall_s"] + transform["wall_s"],
        "cpu_s": restore_["cpu_s"] + export["cpu_s"] + transform["cpu_s"],
        "peak_rss_bytes": max(restore_["peak_rss_bytes"], export["peak_rss_bytes"], transform["peak_rss_bytes"]),
        "peak_disk_bytes": max(g["peak_disk_bytes"] for g in groups_),
        "s3": restore_["s3"], "output": output}


def _merge_s3(parts: list[dict]) -> dict:
    from .gs import _sum_s3
    return _sum_s3(parts)


def cleanup(run_dir: Path, rep: int) -> None:
    shutil.rmtree(run_dir / "es" / f"rep{rep}", ignore_errors=True)
