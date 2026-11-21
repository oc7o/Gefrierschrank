"""Datasets of the selective-fetch measurement (bench/synthfetch/README.md): ES 9.3 synthetic-source
indices with one force-merged segment each, non-compound and as compound twins, snapshotted into
the harness's own MinIO.

  logsdb-nc / logsdb-c    logsdb, ECS-like documents (default 1,500,000)
  tsdb-nc / tsdb-c        time_series metrics with a dimension, gauge, counter, histogram (default 5,000,000)
  nested-nc / nested-c    logsdb with a nested `items` field (default 1,000,000 roots)

The -nc indices set `index.compound_format: false` (their force-merged segment is not compound, as
a segment of 1 GB and more is not by default); the -c twins keep Elasticsearch's default, so their
segment is compound and selective fetch fetches them in full (the control). One shard each.

Containers (removed on exit): MinIO gs-synthfetch-minio (ports 9253 S3, 9254 console; data in
target/synthfetch/minio-data, bucket `synthfetch`, repository base path `repo`) and Elasticsearch
gs-synthfetch-es93 (image gefrierschrank-es9, port 9252, 2 GB heap, trial license), both on the
docker network gs-synthfetch-bench. Stdlib only; refuses to run when target/synthfetch exists. Hold
the docker slot:
  $SLOT docker 1 python3 bench/synthfetch/generate.py [--scale small|standard]
"""
import datetime
import json
import pathlib
import random
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / "target/synthfetch"
NETWORK = "gs-synthfetch-bench"
MINIO = "gs-synthfetch-minio"
ES_CONTAINER = "gs-synthfetch-es93"
ES_PORT = 9252
ES = f"http://localhost:{ES_PORT}"
BUCKET = "synthfetch"
BASE_PATH = "repo"
SCALES = {"small": {"logsdb": 50_000, "tsdb": 20_000, "nested": 10_000},
          "standard": {"logsdb": 1_500_000, "tsdb": 5_000_000, "nested": 1_000_000}}
HOSTS = [f"host-{i:02d}.example.internal" for i in range(40)]
SERVICES = ["checkout", "search", "auth", "billing", "catalog", "gateway"]
WORDS = "request served backend upstream cache miss retry timeout payload shard segment snapshot".split()


def es(method, path, body=None, timeout=1800):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(ES + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def start():
    for name in (MINIO, ES_CONTAINER):
        if name in docker("ps", "-a", "--format", "{{.Names}}").stdout.split():
            raise RuntimeError(f"container {name} already exists; remove it first")
    docker("network", "create", NETWORK, check=False)
    data = OUT / "minio-data"
    data.mkdir(parents=True)
    docker("run", "-d", "--name", MINIO, "--network", NETWORK, "-p", "9253:9000", "-p", "9254:9001",
           "-e", "MINIO_ROOT_USER=minioadmin", "-e", "MINIO_ROOT_PASSWORD=minioadmin",
           "-v", f"{data}:/data", "minio/minio:latest", "server", "/data", "--console-address", ":9001")
    for _ in range(60):
        result = docker("run", "--rm", "--network", NETWORK, "--entrypoint", "sh", "minio/mc:latest", "-c",
                        f"mc alias set m http://{MINIO}:9000 minioadmin minioadmin >/dev/null && mc mb -p m/{BUCKET}",
                        check=False)
        if result.returncode == 0:
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"cannot create bucket {BUCKET}: {result.stderr[-300:]}")
    docker("run", "-d", "--name", ES_CONTAINER, "--network", NETWORK, "-p", f"{ES_PORT}:9200",
           "-e", "discovery.type=single-node", "-e", "xpack.security.enabled=false",
           "-e", "cluster.routing.allocation.disk.threshold_enabled=false",
           "-e", "ES_JAVA_OPTS=-Xms2g -Xmx2g", "gefrierschrank-es9:latest")
    deadline = time.time() + 240
    while time.time() < deadline:
        try:
            if es("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")["status"] in ("yellow", "green"):
                break
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(2)
    else:
        raise RuntimeError("Elasticsearch did not start")
    for _ in range(60):
        try:
            license_info = es("GET", "/_license")["license"]
            break
        except urllib.error.HTTPError:
            time.sleep(1)
    else:
        raise RuntimeError("no license information")
    if license_info["type"] not in ("trial", "platinum", "enterprise"):
        es("POST", "/_license/start_trial?acknowledge=true")


def bulk(index, docs, op="index"):
    lines = []
    for doc in docs:
        lines.append(json.dumps({op: {"_index": index}}))
        lines.append(json.dumps(doc, separators=(",", ":")))
    request = urllib.request.Request(f"{ES}/_bulk", data=("\n".join(lines) + "\n").encode(), method="POST",
                                     headers={"Content-Type": "application/x-ndjson"})
    with urllib.request.urlopen(request, timeout=600) as response:
        result = json.load(response)
    if result.get("errors"):
        failed = [item for item in result["items"] if "error" in next(iter(item.values()))]
        raise RuntimeError(f"bulk errors in {index}: {failed[:2]}")


def logs_mapping(nested=False):
    properties = {
        "@timestamp": {"type": "date"},
        "host": {"properties": {"name": {"type": "keyword"}}},
        "service": {"properties": {"name": {"type": "keyword"}}},
        "http": {"properties": {"status": {"type": "integer"}, "bytes": {"type": "long"}}},
        "client": {"properties": {"ip": {"type": "ip"}}},
        "event": {"properties": {"duration": {"type": "long"}}},
        "message": {"type": "match_only_text"},
        "url": {"properties": {"path": {"type": "wildcard"}}},
        "user_agent": {"properties": {"original": {"type": "keyword", "ignore_above": 1024}}},
        "labels": {"type": "flattened"},
        "tags": {"type": "keyword"},
    }
    if nested:
        properties["items"] = {"type": "nested", "properties": {
            "name": {"type": "keyword"}, "qty": {"type": "integer"}, "price": {"type": "double"}}}
    return {"properties": properties}


def logs_doc(rng, i, nested=False):
    doc = {
        "@timestamp": f"2026-07-{1 + i % 28:02d}T{i % 24:02d}:{(i * 7) % 60:02d}:{(i * 13) % 60:02d}.{i % 1000:03d}Z",
        "host": {"name": HOSTS[i % len(HOSTS)]},
        "service": {"name": SERVICES[i % len(SERVICES)]},
        "http": {"status": [200, 200, 200, 201, 404, 500][i % 6], "bytes": rng.randint(100, 5_000_000)},
        "client": {"ip": f"10.{i % 256}.{(i // 256) % 256}.{(i * 7) % 256}"},
        "event": {"duration": rng.randint(1_000, 900_000_000)},
        "message": " ".join(rng.choice(WORDS) for _ in range(8 + i % 20)),
        "url": {"path": f"/api/v{1 + i % 3}/{SERVICES[i % len(SERVICES)]}/{rng.randint(0, 99_999)}"},
        "user_agent": {"original": f"Mozilla/5.0 (X11; Linux x86_64) client/{i % 50}"},
        "labels": {"env": ["prod", "staging", "dev"][i % 3], "zone": f"z{i % 4}"},
        "tags": sorted({rng.choice(["a", "b", "c", "d"]) for _ in range(1 + i % 3)}),
    }
    if nested and i % 5:
        doc["items"] = [{"name": f"item-{rng.randint(0, 999)}", "qty": rng.randint(1, 9), "price": round(rng.uniform(1, 99), 2)}
                        for _ in range(i % 5)]
    return doc


TSDB_START = datetime.datetime(2026, 7, 1, tzinfo=datetime.timezone.utc)


def tsdb_doc(rng, i):
    # Strictly increasing timestamps: a time series document's id derives from its dimensions and timestamp.
    at = TSDB_START + datetime.timedelta(milliseconds=100 * i)
    return {"@timestamp": at.strftime("%Y-%m-%dT%H:%M:%S.") + f"{at.microsecond // 1000:03d}Z",
            "host": HOSTS[i % len(HOSTS)], "cpu": round(rng.random() * 100, 3), "requests": i,
            "latency": {"values": [0.1, 0.5, 1.0, 5.0], "counts": [rng.randint(0, 50) for _ in range(4)]}}


TSDB_MAPPING = {"properties": {
    "@timestamp": {"type": "date"},
    "host": {"type": "keyword", "time_series_dimension": True},
    "cpu": {"type": "double", "time_series_metric": "gauge"},
    "requests": {"type": "long", "time_series_metric": "counter"},
    "latency": {"type": "histogram"},
}}


def build(index, settings, mapping, count, document, op="index"):
    body = {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"} | settings
    es("PUT", "/" + index, {"settings": body, "mappings": mapping})
    rng = random.Random(index.split("-")[0])
    started = time.time()
    batch = []
    for i in range(count):
        batch.append(document(rng, i))
        if len(batch) == 5000:
            bulk(index, batch, op)
            batch = []
    if batch:
        bulk(index, batch, op)
    es("POST", f"/{index}/_refresh")
    es("POST", f"/{index}/_forcemerge?max_num_segments=1")
    es("POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")
    segments = [segment for copies in es("GET", f"/{index}/_segments")["indices"][index]["shards"].values()
                for copy in copies for segment in copy["segments"].values()]
    stats = es("GET", f"/{index}/_stats/store")["indices"][index]["primaries"]["store"]["size_in_bytes"]
    mode = es("GET", f"/{index}/_settings?include_defaults=true&flat_settings=true")[index]
    source_mode = mode["settings"].get("index.mapping.source.mode") or mode["defaults"].get("index.mapping.source.mode")
    if str(source_mode).lower() != "synthetic":
        raise RuntimeError(f"{index} is not synthetic: {source_mode}")
    info = {"docs": count, "segments": len(segments), "compound": [s["compound"] for s in segments],
            "store_bytes": stats, "seconds": round(time.time() - started, 1)}
    print(index, json.dumps(info), flush=True)
    return info


def main():
    scale = sys.argv[sys.argv.index("--scale") + 1] if "--scale" in sys.argv else "standard"
    counts = SCALES[scale]
    if OUT.exists():
        raise RuntimeError(f"{OUT} exists; remove it to regenerate")
    OUT.mkdir(parents=True)
    report = {"scale": scale, "indices": {}}
    try:
        start()
        report["es_version"] = es("GET", "/")["version"]["number"]
        nc = {"index.compound_format": "false"}
        tsdb = {"index.mode": "time_series", "index.routing_path": ["host"],
                "index.time_series.start_time": "2026-07-01T00:00:00Z", "index.time_series.end_time": "2026-08-01T00:00:00Z"}
        for suffix, extra in (("nc", nc), ("c", {})):
            report["indices"][f"logsdb-{suffix}"] = build(f"logsdb-{suffix}", {"index.mode": "logsdb"} | extra, logs_mapping(),
                                                          counts["logsdb"], logs_doc)
            report["indices"][f"tsdb-{suffix}"] = build(f"tsdb-{suffix}", tsdb | extra, TSDB_MAPPING, counts["tsdb"], tsdb_doc,
                                                        op="create")
            report["indices"][f"nested-{suffix}"] = build(f"nested-{suffix}", {"index.mode": "logsdb"} | extra,
                                                          logs_mapping(nested=True), counts["nested"],
                                                          lambda rng, i: logs_doc(rng, i, nested=True))
        for index, info in report["indices"].items():
            if info["compound"] != [index.endswith("-c")]:
                raise RuntimeError(f"{index}: expected one {'compound' if index.endswith('-c') else 'non-compound'} segment, "
                                   f"got {info['compound']}")
        es("PUT", "/_snapshot/synthfetch", {"type": "s3", "settings": {
            "bucket": BUCKET, "base_path": BASE_PATH, "endpoint": f"{MINIO}:9000", "protocol": "http",
            "path_style_access": True}})
        snapshot = es("PUT", "/_snapshot/synthfetch/synthfetch?wait_for_completion=true",
                      {"indices": ",".join(report["indices"]), "include_global_state": False})["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot}")
        (OUT / "datasets.json").write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
        print(f"wrote {OUT} ({report['es_version']}, scale {scale})")
    finally:
        docker("rm", "-f", ES_CONTAINER, MINIO, check=False)
        docker("network", "rm", NETWORK, check=False)


if __name__ == "__main__":
    sys.exit(main())
