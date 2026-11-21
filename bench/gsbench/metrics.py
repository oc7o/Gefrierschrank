"""Measurement: process-tree CPU/RSS, disk peaks, container CPU/memory, MinIO S3 counters."""

import json
import os
import re
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from urllib.parse import unquote

import psutil
import requests

from . import config


def dir_size(path: Path) -> int:
    total = 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                try:
                    if entry.is_dir(follow_symlinks=False):
                        total += dir_size(Path(entry.path))
                    else:
                        total += entry.stat(follow_symlinks=False).st_size
                except FileNotFoundError:
                    pass
    except (FileNotFoundError, NotADirectoryError):
        pass
    return total


class TreeSampler(threading.Thread):
    """Samples the RSS sum of a process tree and the size of some directories every 200 ms.
    Sampling can miss shorter peaks (e.g. a short-lived per-shard temp dir); ru_maxrss from wait4
    complements the RSS side."""

    def __init__(self, pid: int, disk_paths=(), interval: float = 0.2, disk_interval: float = 0.2):
        super().__init__(daemon=True)
        self.pid = pid
        self.disk_paths = [Path(p) for p in disk_paths]
        self.interval = interval
        self.disk_interval = disk_interval
        self.peak_rss = 0
        self.peak_disk = 0
        self.samples = 0
        self._halt = threading.Event()

    def sample_disk(self) -> None:
        self.peak_disk = max(self.peak_disk, sum(dir_size(p) for p in self.disk_paths))

    def run(self) -> None:
        try:
            root = psutil.Process(self.pid)
        except psutil.NoSuchProcess:
            return
        next_disk = 0.0
        while not self._halt.is_set():
            rss = 0
            try:
                procs = [root] + root.children(recursive=True)
            except psutil.NoSuchProcess:
                break
            for p in procs:
                try:
                    rss += p.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
            self.peak_rss = max(self.peak_rss, rss)
            self.samples += 1
            now = time.monotonic()
            if self.disk_paths and now >= next_disk:
                self.sample_disk()
                next_disk = now + self.disk_interval
            self._halt.wait(self.interval)

    def stop(self) -> None:
        self._halt.set()
        self.join(timeout=10)
        if self.disk_paths:
            self.sample_disk()


def run_measured(cmd: list[str], env=None, cwd=None, log_path: Path | None = None, disk_paths=(),
                 stdout_capture: bool = False) -> dict:
    """Run cmd to completion. CPU comes from wait4, which covers every descendant the process
    reaped itself (e.g. Lucene 9 worker JVMs), so nothing is lost between samples."""
    out = open(log_path, "wb") if log_path else subprocess.DEVNULL
    started = time.monotonic()
    proc = subprocess.Popen(cmd, env=env, cwd=cwd, stdout=subprocess.PIPE if stdout_capture else out,
                            stderr=out if log_path else subprocess.DEVNULL)
    sampler = TreeSampler(proc.pid, disk_paths)
    sampler.start()
    captured = b""
    if stdout_capture:
        captured = proc.stdout.read()
    _, status, usage = os.wait4(proc.pid, 0)
    wall = time.monotonic() - started
    proc.returncode = os.waitstatus_to_exitcode(status)
    sampler.stop()
    if log_path:
        out.close()
    maxrss = usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024
    result = {
        "wall_s": round(wall, 3),
        "cpu_user_s": round(usage.ru_utime, 3),
        "cpu_sys_s": round(usage.ru_stime, 3),
        "cpu_s": round(usage.ru_utime + usage.ru_stime, 3),
        "peak_rss_bytes": max(sampler.peak_rss, maxrss),
        "peak_rss_sampled_bytes": sampler.peak_rss,
        "max_single_process_rss_bytes": maxrss,
        "exit_code": proc.returncode,
    }
    if disk_paths:
        result["peak_disk_bytes"] = sampler.peak_disk
    if stdout_capture:
        result["stdout"] = captured.decode(errors="replace")
    return result


class ContainerMemSampler(threading.Thread):
    """Peak memory of a container as `docker stats` reports it (usage without page cache)."""

    _UNITS = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4,
              "kB": 1000, "KB": 1000, "MB": 1000**2, "GB": 1000**3}

    def __init__(self, container: str):
        super().__init__(daemon=True)
        self.container = container
        self.peak = 0
        self.proc = None

    @classmethod
    def parse(cls, text: str) -> int:
        m = re.match(r"\s*([\d.]+)\s*([A-Za-z]+)", text)
        return int(float(m.group(1)) * cls._UNITS.get(m.group(2), 1)) if m else 0

    def run(self) -> None:
        self.proc = subprocess.Popen(["docker", "stats", "--format", "{{json .}}", self.container],
                                     stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        for line in self.proc.stdout:
            line = line.strip().lstrip("\x1b[2J\x1b[H").strip()
            start = line.find("{")
            if start < 0:
                continue
            try:
                usage = json.loads(line[start:])["MemUsage"].split("/")[0]
            except (ValueError, KeyError):
                continue
            self.peak = max(self.peak, self.parse(usage))

    def stop(self) -> int:
        # docker stats emits about once per second; a one-shot reading covers shorter phases
        once = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", self.container],
                              capture_output=True, text=True)
        if once.returncode == 0:
            self.peak = max(self.peak, self.parse(once.stdout.split("/")[0]))
        if self.proc:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        self.join(timeout=10)
        return self.peak


_METRIC = re.compile(r'^(minio_bucket_(?:requests_total|traffic_received_bytes|traffic_sent_bytes))\{([^}]*)\}\s+(\S+)$')


# MinIO serves bucket metrics from a cache refreshed at most every 10 s (measured: counters of a
# burst appear only 8-12 s later). A scrape taken after this settle time with no S3 activity in
# between reflects every request made before the wait began.
MINIO_METRICS_SETTLE_S = 11.0


def minio_settled_scrape() -> dict:
    time.sleep(MINIO_METRICS_SETTLE_S)
    return minio_scrape()


def minio_scrape() -> dict:
    """{bucket: {"requests": {api: n}, "rx_bytes": n, "tx_bytes": n}} from MinIO's bucket metrics."""
    r = requests.get(f"{config.MINIO_URL}/minio/v2/metrics/bucket", timeout=30)
    r.raise_for_status()
    out: dict = {}
    for line in r.text.splitlines():
        m = _METRIC.match(line)
        if not m:
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2)))
        bucket = out.setdefault(labels.get("bucket", "?"), {"requests": {}, "rx_bytes": 0, "tx_bytes": 0})
        value = int(float(m.group(3)))
        if m.group(1) == "minio_bucket_requests_total":
            bucket["requests"][labels.get("api", "?")] = bucket["requests"].get(labels.get("api", "?"), 0) + value
        elif m.group(1) == "minio_bucket_traffic_received_bytes":
            bucket["rx_bytes"] += value
        else:
            bucket["tx_bytes"] += value
    return out


def minio_check(probe_key: str) -> None:
    """Fail unless MinIO's bucket metrics carry the series every S3 number is computed from.
    probe_key: a small object in the snapshot bucket, read once so the GET and sent-bytes series
    of that bucket must show up (metric names differ between MinIO versions)."""
    from .infra import s3fs
    before = minio_scrape()
    with s3fs().open_input_stream(f"{config.SNAPSHOT_BUCKET}/{probe_key}") as f:
        f.read()
    delta = minio_delta(before, minio_settled_scrape(), buckets=(config.SNAPSHOT_BUCKET,))
    got = delta.get(config.SNAPSHOT_BUCKET, {})
    if not got.get("requests", {}).get("getobject") or not got.get("tx_bytes"):
        raise RuntimeError("MinIO bucket metrics did not record a GET of the snapshot bucket "
                           f"(minio_bucket_requests_total / minio_bucket_traffic_sent_bytes): {got or 'nothing'}")


def check_s3_read(delta: dict, what: str) -> None:
    """A phase that read the snapshot repository must show GETs and sent bytes."""
    got = delta.get(config.SNAPSHOT_BUCKET, {})
    if not got.get("requests", {}).get("getobject") or not got.get("tx_bytes"):
        raise RuntimeError(f"{what} read the snapshot repository but MinIO recorded no GET traffic: {got or 'nothing'}")


# Any request that could modify an object; the metric's api label is lower case.
_WRITE_API_PREFIXES = ("put", "delete", "copy", "post", "newmultipart", "complete", "abort", "restore",
                       "uploadpart")


def source_writes(delta: dict) -> dict:
    """Write requests and received bytes on the snapshot bucket (must stay empty for the GS route)."""
    got = delta.get(config.SNAPSHOT_BUCKET, {})
    writes = {api: n for api, n in got.get("requests", {}).items() if api.startswith(_WRITE_API_PREFIXES)}
    if got.get("rx_bytes"):
        writes["rx_bytes"] = got["rx_bytes"]
    return writes


def minio_info() -> dict:
    info = {}
    try:
        image = subprocess.run(["docker", "inspect", "gs-bench-minio", "--format", "{{.Image}}"],
                               capture_output=True, text=True, timeout=30).stdout.strip()
        info["image_id"] = image
        digests = subprocess.run(["docker", "image", "inspect", image, "--format", "{{json .RepoDigests}}"],
                                 capture_output=True, text=True, timeout=30).stdout.strip()
        info["repo_digests"] = json.loads(digests) if digests else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        pass
    try:
        text = requests.get(f"{config.MINIO_URL}/minio/v2/metrics/cluster", timeout=30).text
        m = re.search(r'minio_software_version_info\{[^}]*version="([^"]+)"', text)
        info["version"] = m.group(1) if m else None
    except requests.RequestException:
        pass
    return info


def minio_delta(before: dict, after: dict, buckets=config.BUCKETS) -> dict:
    """S3 traffic between two scrapes. tx = bytes MinIO sent (downloads), rx = received (uploads)."""
    out = {}
    for b in buckets:
        a, z = before.get(b, {"requests": {}, "rx_bytes": 0, "tx_bytes": 0}), after.get(b, {"requests": {}, "rx_bytes": 0, "tx_bytes": 0})
        reqs = {api: z["requests"].get(api, 0) - a["requests"].get(api, 0)
                for api in set(a["requests"]) | set(z["requests"])}
        reqs = {k: v for k, v in sorted(reqs.items()) if v}
        entry = {"requests": reqs, "requests_total": sum(reqs.values()),
                 "tx_bytes": z["tx_bytes"] - a["tx_bytes"], "rx_bytes": z["rx_bytes"] - a["rx_bytes"]}
        if entry["requests_total"] or entry["tx_bytes"] or entry["rx_bytes"]:
            out[b] = entry
    return out


def _data_files_on_disk(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return [p for p in root.rglob("*.parquet") if "_delta_log" not in p.parts and "_export_metadata" not in p.parts]


def delta_output_stats(table: str, storage_options: dict | None = None) -> dict:
    """Active data files (AddFile count), their bytes, the column schema, and the bytes/files on
    disk; for local tables also the Parquet files no AddFile references (orphans)."""
    from deltalake import DeltaTable
    stats = {"exists": False}
    try:
        dt = DeltaTable(table, storage_options=storage_options)
    except Exception:  # noqa: BLE001 - absent or unreadable table: reported, gate fails it
        if "://" not in table:
            # a quarantined first shard leaves no table, but must not leave its data files either
            stats["orphan_files"] = len(_data_files_on_disk(Path(table)))
        return stats
    adds = dt.get_add_actions(flatten=True)
    sizes = adds.column("size_bytes").to_pylist() if adds.num_rows else []
    rows = adds.column("num_records").to_pylist() if adds.num_rows and "num_records" in adds.column_names else []
    known = [r for r in rows if r is not None]
    stats.update({"exists": True, "add_files": adds.num_rows, "data_bytes": int(sum(sizes)),
                  "rows_from_stats": int(sum(known)) if known else None, "version": dt.version(),
                  "schema": [[f["name"], f["type"], f["nullable"]] for f in json.loads(dt.schema().to_json())["fields"]]})
    if "://" not in table:
        root = Path(table)
        parquet = _data_files_on_disk(root)
        referenced = {os.path.realpath(unquote(u.removeprefix("file://"))) for u in dt.file_uris()}
        stats["parquet_files_on_disk"] = len(parquet)
        stats["orphan_files"] = sum(1 for p in parquet if os.path.realpath(p) not in referenced)
        stats["total_bytes"] = dir_size(root)
    elif table.startswith("s3://"):
        import pyarrow.fs as pafs

        from .infra import s3fs
        infos = [i for i in s3fs().get_file_info(pafs.FileSelector(table[len("s3://"):], recursive=True))
                 if i.type == pafs.FileType.File]
        stats["parquet_files_on_disk"] = sum(1 for i in infos if i.path.endswith(".parquet")
                                             and "/_delta_log/" not in i.path and "/_export_metadata/" not in i.path)
        stats["total_bytes"] = sum(i.size for i in infos)
    return stats


def summarize(values: list[float]) -> dict | None:
    values = [v for v in values if v is not None]
    if not values:
        return None
    med = statistics.median(values)
    return {"median": med, "min": min(values), "max": max(values), "n": len(values),
            "spread_pct": round(100 * (max(values) - min(values)) / med, 1) if med else 0.0}


def machine_info() -> dict:
    info = {"platform": sys.platform, "cores": os.cpu_count(), "memory_bytes": psutil.virtual_memory().total,
            "python": sys.version.split()[0]}
    try:
        info["cpu"] = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True,
                                     text=True).stdout.strip()
    except OSError:
        pass
    try:
        d = json.loads(subprocess.run(["docker", "info", "--format", "{{json .}}"], capture_output=True,
                                      text=True, timeout=30).stdout)
        info["docker"] = {"cpus": d.get("NCPU"), "memory_bytes": d.get("MemTotal"),
                          "server_version": d.get("ServerVersion")}
    except (OSError, ValueError, subprocess.TimeoutExpired):
        pass
    try:
        info["java"] = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr.splitlines()[0]
    except (OSError, IndexError):
        pass
    return info


def load() -> list[float]:
    return [round(x, 2) for x in os.getloadavg()]
