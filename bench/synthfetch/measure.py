"""Timed runs of the selective-fetch measurement (bench/synthfetch/README.md), started by run.sh
inside the bench hold and the docker slot. Starts the harness's MinIO over target/synthfetch/minio-data,
exports every dataset index with the baseline jar, the topic jar and the topic jar with
--selective-fetch, back to back per repeat, and removes the container on exit.

  uv run --offline --frozen --project bench python bench/synthfetch/measure.py <baseline-jar> <topic-jar> <repeats> <out>

SYNTHFETCH_JVM_ARGS adds JVM options to every run (e.g. -Dgefrierschrank.download.selective.syntheticMinBytes=0
to exercise the probe on the small datasets; never set for a measurement).

Per run: wall, CPU, peak RSS and peak temp disk (200 ms samples of the run's java.io.tmpdir), S3 GETs
and bytes sent from MinIO's bucket metrics, and from the run report per shard bytesFetched,
bytesProcessed and fetch; the content-free selective-fetch note (probe calls and milliseconds, or
why the shard was fetched in full) from stderr. Writes runs.jsonl and summary.json into <out>, the
tables into <out>/tables/<label>-r<repeat>/<index>.
"""
import json
import os
import pathlib
import re
import statistics
import subprocess
import sys
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bench"))
from gsbench import config, metrics  # noqa: E402

DATA = ROOT / "target/synthfetch/minio-data"
MINIO = "gs-synthfetch-minio"
MINIO_URL = "http://localhost:9253"
BUCKET = "synthfetch"
INDICES = ["logsdb-nc", "logsdb-c", "tsdb-nc", "tsdb-c", "nested-nc", "nested-c"]
NOTE = re.compile(r"^note: shard (\d+) of [^:]+: (.*)$")


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def start_minio():
    if MINIO in docker("ps", "-a", "--format", "{{.Names}}").stdout.split():
        raise RuntimeError(f"container {MINIO} already exists; remove it first")
    docker("run", "-d", "--name", MINIO, "-p", "9253:9000", "-p", "9254:9001",
           "-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin",
           "-e", "MINIO_PROMETHEUS_AUTH_TYPE=public", "-v", f"{DATA}:/data",
           "minio/minio:latest", "server", "/data", "--console-address", ":9001")
    for _ in range(60):
        try:
            with urllib.request.urlopen(f"{MINIO_URL}/minio/health/live", timeout=2):
                return
        except OSError:
            time.sleep(1)
    raise RuntimeError("MinIO did not start")


def minio_info():
    image = docker("inspect", MINIO, "--format", "{{.Image}}", check=False).stdout.strip()
    with urllib.request.urlopen(f"{MINIO_URL}/minio/v2/metrics/cluster", timeout=30) as response:
        text = response.read().decode()
    version = re.search(r'minio_software_version_info\{[^}]*version="([^"]+)"', text)
    return {"image_id": image, "version": version.group(1) if version else None}


def run_one(jar, extra, index, work, table, env, before):
    tmp = work / "tmp"
    tmp.mkdir(parents=True)
    cmd = ["java", "-Xmx4g", f"-Djava.io.tmpdir={tmp}", *os.environ.get("SYNTHFETCH_JVM_ARGS", "").split(),
           "-jar", str(jar), "dump-to-delta",
           f"s3://{BUCKET}/repo", "--source-endpoint", MINIO_URL, "--index", index,
           "-o", str(table), "--report", str(work / "report.json"), *extra]
    (work / "command.txt").write_text(" ".join(cmd) + "\n")
    load_before = metrics.load()
    m = metrics.run_measured(cmd, env=env, cwd=str(work), log_path=work / "output.log", disk_paths=[tmp])
    after = metrics.minio_settled_scrape()
    s3 = metrics.minio_delta(before, after, buckets=(BUCKET,)).get(BUCKET, {})
    m["load_before"], m["load_after"] = load_before, metrics.load()
    m["s3_get"] = s3.get("requests", {}).get("getobject", 0)
    m["s3_requests"] = s3.get("requests_total", sum(s3.get("requests", {}).values()))
    m["s3_tx_bytes"] = s3.get("tx_bytes", 0)
    m["s3_writes"] = metrics.source_writes({config.SNAPSHOT_BUCKET: s3})
    try:
        report = json.loads((work / "report.json").read_text())
    except (OSError, ValueError):
        report = {"shards": []}
    m["shards"] = [{k: shard.get(k) for k in ("shardId", "outcome", "rowsWritten", "bytesProcessed", "bytesFetched", "fetch",
                                                "errorClass")} for shard in report.get("shards", [])]
    notes = {}
    for line in (work / "output.log").read_text(errors="replace").splitlines():
        match = NOTE.match(line)
        if match and ("selective fetch kept" in match.group(2) or "fetched in full" in match.group(2)):
            notes[match.group(1)] = match.group(2)
    m["notes"] = notes
    return m, after


def median(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def main():
    base_jar, topic_jar, repeats, out = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), int(sys.argv[3]), pathlib.Path(sys.argv[4])
    labels = [("base", base_jar, []), ("off", topic_jar, []), ("on", topic_jar, ["--selective-fetch"])]
    config.MINIO_URL = MINIO_URL
    env = dict(os.environ, AWS_ACCESS_KEY_ID="minioadmin", AWS_SECRET_ACCESS_KEY="minioadmin", AWS_REGION="us-east-1")
    env.pop("AWS_SESSION_TOKEN", None)
    env.pop("AWS_PROFILE", None)
    start_minio()
    runs = []
    try:
        header = {"machine": metrics.machine_info(), "load_start": metrics.load(), "minio": minio_info(),
                  "jars": {"base": str(base_jar), "topic": str(topic_jar)}, "repeats": repeats}
        before = metrics.minio_settled_scrape()
        with open(out / "runs.jsonl", "w") as log:
            for repeat in range(1, repeats + 1):
                for label, jar, extra in labels:
                    for index in INDICES:
                        work = out / "runs" / f"{label}-{index}-r{repeat}"
                        table = out / "tables" / f"{label}-r{repeat}" / index
                        m, before = run_one(jar, extra, index, work, table, env, before)
                        m.update(label=label, index=index, repeat=repeat)
                        runs.append(m)
                        log.write(json.dumps(m) + "\n")
                        log.flush()
                        print(f"{label} {index} r{repeat}: {m['wall_s']} s, exit {m['exit_code']}, "
                              f"S3 {m['s3_tx_bytes']} B / {m['s3_get']} GETs, temp peak {m.get('peak_disk_bytes')}, "
                              f"fetch {[s['fetch'] for s in m['shards']]} {list(m['notes'].values())}", flush=True)
        header["load_end"] = metrics.load()
    finally:
        docker("rm", "-f", MINIO, check=False)
    summary = {"header": header, "indices": {}}
    for index in INDICES:
        for label, _, _ in labels:
            mine = [m for m in runs if m["index"] == index and m["label"] == label]
            if not mine:
                continue
            summary["indices"].setdefault(index, {})[label] = {
                "wall_s": median([m["wall_s"] for m in mine]),
                "cpu_s": median([m["cpu_s"] for m in mine]),
                "peak_rss_bytes": median([m["peak_rss_bytes"] for m in mine]),
                "peak_temp_bytes": median([m.get("peak_disk_bytes") for m in mine]),
                "s3_tx_bytes": median([m["s3_tx_bytes"] for m in mine]),
                "s3_get": median([m["s3_get"] for m in mine]),
                "bytes_fetched": median([sum(s["bytesFetched"] or 0 for s in m["shards"]) for m in mine]),
                "bytes_processed": median([sum(s["bytesProcessed"] or 0 for s in m["shards"]) for m in mine]),
                "fetch": sorted({s["fetch"] for m in mine for s in m["shards"]} - {None}),
                "outcomes": sorted({s["outcome"] for m in mine for s in m["shards"]} - {None}),
                "exit_codes": sorted({m["exit_code"] for m in mine}),
                "notes": sorted({note for m in mine for note in m["notes"].values()}),
                "source_writes": [m["s3_writes"] for m in mine if m["s3_writes"]],
            }
    (out / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    print(json.dumps(summary["indices"], indent=1))


if __name__ == "__main__":
    sys.exit(main())
