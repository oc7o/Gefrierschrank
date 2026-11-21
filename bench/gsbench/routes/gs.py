"""Gefrierschrank route: `dump-to-delta --all-indices` straight from the snapshot repository.

Any CLI jar works: only flags every released jar understands are passed by default; newer flags
go through --gs-args.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

from .. import config, metrics
from ..infra import log, minio_env, s3_delete_prefix

BUILD_INFO = re.compile(r"^gefrierschrank (\S+) \(built ([^)]*)\)")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def jar_info(path: Path) -> dict:
    st = path.stat()
    return {"path": str(path), "name": path.name, "size": st.st_size, "mtime": int(st.st_mtime), "sha256": sha256(path)}


def install(jar: Path, worker_jar: Path | None, install_dir: Path) -> tuple[Path, Path | None]:
    """Copy the jars side by side: 1.0.0 finds its worker next to the CLI jar."""
    install_dir.mkdir(parents=True, exist_ok=True)

    def place(src: Path) -> Path:
        dst = install_dir / src.name
        if dst.exists():
            dst.unlink()
        if sys.platform == "darwin" and subprocess.run(["cp", "-c", str(src), str(dst)]).returncode == 0:
            return dst  # APFS clone: instant, and immune to later rebuilds of the source jar
        shutil.copy2(src, dst)
        return dst

    return place(jar), (place(worker_jar) if worker_jar else None)


def find_worker_jar(cli_jar: Path) -> Path | None:
    """The Lucene 9 worker jar a release ships beside the CLI jar, or the one of the same Maven
    reactor build (gefrierschrank-cli/target -> gefrierschrank-lucene9/target): the one with the
    CLI jar's version if present, else the newest."""
    m = re.fullmatch(r"gefrierschrank-cli-(.+)\.jar", cli_jar.name)
    same_version = f"gefrierschrank-lucene9-{m.group(1)}.jar" if m else None
    for directory in (cli_jar.parent, cli_jar.parent.parent.parent / "gefrierschrank-lucene9" / "target"):
        found = [p for p in directory.glob("gefrierschrank-lucene9-*.jar") if not p.name.startswith("original-")]
        if found:
            return next((p for p in found if p.name == same_version), None) or max(found, key=lambda p: p.stat().st_mtime)
    return None


def java_binary() -> str:
    home = os.environ.get("GSBENCH_JAVA_HOME") or os.environ.get("JAVA_HOME")
    return str(Path(home) / "bin" / "java") if home else "java"


def run_major(jar: Path, worker: Path | None, dataset, work: Path, jvm_args: list[str], gs_args: list[str],
              target: str, run_id: str, before: dict) -> dict:
    """before: a settled MinIO scrape; the settled scrape after this run is returned in m["_after"]."""
    tmp, report = work / "tmp", work / "report"
    for d in (tmp, report):
        d.mkdir(parents=True, exist_ok=True)
    storage_options = None
    if target == "s3a":
        out_prefix = f"{config.OUTPUT_BUCKET}/{run_id}/{work.parent.name}/{work.name}"
        s3_delete_prefix(out_prefix)
        out = f"s3a://{out_prefix}"
        storage_options = s3_storage_options()
        disk_paths = [tmp]
    else:
        out = str(work / "out")
        disk_paths = [tmp, work / "out"]
    cmd = [java_binary(), f"-Djava.io.tmpdir={tmp}"]
    if worker:
        cmd.append(f"-Dgefrierschrank.lucene9.workerJar={worker}")
    cmd += [*jvm_args, "-jar", str(jar), "dump-to-delta", f"s3://{config.SNAPSHOT_BUCKET}/{dataset.base_path}",
            "--source-endpoint", config.MINIO_URL, "--all-indices", "-o", out, "--report", str(report)]
    if target == "s3a":
        cmd += ["--target-endpoint", config.MINIO_URL]
    cmd += gs_args
    (work / "command.txt").write_text(" ".join(cmd) + "\n")
    load_before = metrics.load()
    m = metrics.run_measured(cmd, env=minio_env(), cwd=str(work), log_path=work / "output.log", disk_paths=disk_paths)
    after = metrics.minio_settled_scrape()
    s3 = metrics.minio_delta(before, after)
    m["load_before"], m["load_after"] = load_before, metrics.load()
    m["s3"] = s3
    m["source_writes"] = metrics.source_writes(s3)
    m["command"] = cmd
    text = (work / "output.log").read_text(errors="replace")
    m["build_info"] = next((line for line in text.splitlines() if BUILD_INFO.match(line)), None)
    manifest = _json(report / "manifest.json")
    m["manifest"] = {k: manifest.get(k) for k in ("totalIndices", "completedCount", "completedWithQuarantineCount",
                                                   "failedCount", "complete")} if manifest else None
    if manifest and manifest.get("totalIndices"):
        metrics.check_s3_read(s3, f"dump-to-delta of {dataset.base_path}")
    outcomes = {e["indexName"]: e for e in (manifest or {}).get("indices", [])}
    indices = {}
    for name in dataset.indices:
        entry = outcomes.get(name)
        rep = _json(report / f"{name}.json") if entry and entry.get("reportFile") else None
        if entry is None:
            outcome = "MISSING"
        elif entry["outcome"] == "FAILED":
            outcome = "FAILED"
        elif rep and rep.get("quarantinedCount", 0) > 0:
            outcome = "QUARANTINED"
        else:
            outcome = "COMMITTED"
        table = f"{out}/{name}" if target == "s3a" else str(Path(out) / name)
        delta_uri = table.replace("s3a://", "s3://")
        indices[name] = {
            "outcome": outcome,
            "error_class": entry.get("errorClass") if entry else None,
            "rows": rep.get("rowsWrittenThisRun") if rep else 0,
            "elapsed_ms": rep.get("elapsedMillis") if rep else None,
            "total_shards": rep.get("totalShards") if rep else None,
            "committed_shards": rep.get("committedCount") if rep else None,
            "quarantined_shards": rep.get("quarantinedCount") if rep else None,
            "quarantine_errors": sorted({s.get("errorClass") for s in (rep or {}).get("shards", [])
                                         if s.get("outcome") == "QUARANTINED"} - {None}),
            "source_shard_bytes": rep.get("bytesProcessedThisRun") if rep else None,
            "table": delta_uri,
            "output": metrics.delta_output_stats(delta_uri, storage_options),
        }
    m["indices"] = indices
    m["output"] = {
        "bytes": sum(i["output"].get("total_bytes") or i["output"].get("data_bytes", 0) for i in indices.values()),
        "data_bytes": sum(i["output"].get("data_bytes", 0) for i in indices.values()),
        "add_files": sum(i["output"].get("add_files", 0) for i in indices.values()),
        "parquet_files": sum(i["output"].get("parquet_files_on_disk", 0) for i in indices.values()),
    }
    m["_after"] = after
    return m


def s3_storage_options() -> dict:
    return {"AWS_ENDPOINT_URL": config.MINIO_URL, "AWS_ACCESS_KEY_ID": config.MINIO_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": config.MINIO_SECRET_KEY, "AWS_REGION": "us-east-1", "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"}


def _json(path: Path):
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, ValueError):
        return None


def run_repeat(jar: Path, worker: Path | None, datasets: dict, run_dir: Path, rep: int, jvm_args, gs_args,
               target: str, run_id: str) -> dict:
    majors = {}
    before = metrics.minio_settled_scrape()
    for major, ds in datasets.items():
        work = run_dir / "gs" / f"rep{rep}" / f"es{major}"
        if work.exists():
            shutil.rmtree(work)
        work.mkdir(parents=True)
        log(f"gs rep {rep}: es{major} snapshot {ds.base_path}")
        majors[f"es{major}"] = run_major(jar, worker, ds, work, jvm_args, gs_args, target, run_id, before)
        r = majors[f"es{major}"]
        # reading the output tables back (gate, stats) is local unless the target is S3
        before = r.pop("_after") if target != "s3a" else metrics.minio_settled_scrape()
        log(f"gs rep {rep}: es{major} exit {r['exit_code']} wall {r['wall_s']:.1f}s cpu {r['cpu_s']:.1f}s "
            f"rss {r['peak_rss_bytes'] / 1e6:.0f} MB " + " ".join(f"{n}={i['outcome']}" for n, i in r["indices"].items()))
    total = {k: sum(m[k] for m in majors.values()) for k in ("wall_s", "cpu_s", "cpu_user_s", "cpu_sys_s")}
    total["peak_rss_bytes"] = max(m["peak_rss_bytes"] for m in majors.values())
    total["peak_disk_bytes"] = max(m.get("peak_disk_bytes", 0) for m in majors.values())
    total["s3"] = _sum_s3([m["s3"] for m in majors.values()])
    total["output"] = {k: sum(m["output"][k] for m in majors.values()) for k in next(iter(majors.values()))["output"]}
    return {"rep": rep, "majors": majors, "total": total}


def _sum_s3(parts: list[dict]) -> dict:
    out: dict = {}
    for part in parts:
        for bucket, e in part.items():
            t = out.setdefault(bucket, {"requests": {}, "requests_total": 0, "tx_bytes": 0, "rx_bytes": 0})
            for api, n in e["requests"].items():
                t["requests"][api] = t["requests"].get(api, 0) + n
            for k in ("requests_total", "tx_bytes", "rx_bytes"):
                t[k] += e[k]
    return out


def cleanup(run_dir: Path, rep: int, target: str, run_id: str) -> None:
    base = run_dir / "gs" / f"rep{rep}"
    for major_dir in base.glob("es*"):
        for sub in ("out", "tmp"):
            shutil.rmtree(major_dir / sub, ignore_errors=True)
    if target == "s3a":
        s3_delete_prefix(f"{config.OUTPUT_BUCKET}/{run_id}/rep{rep}")
