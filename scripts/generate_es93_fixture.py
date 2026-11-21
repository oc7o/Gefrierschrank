"""Generate a small REAL ES 9.3.0 snapshot-repository fixture for ES 9.x compatibility tests.

Indices:
  test_es93_logsdb_binary    logsdb + best_compression (synthetic source, ZSTD stored fields) with
                             BINARY doc values that ES 9.3 writes through ES819TSDBDocValuesFormat
                             with ZSTD-compressed blocks: sparse multi-valued wildcard url.path with
                             a few long values, histogram, binary doc values, ip_range,
                             counted_keyword, keyword over ignore_above, text. Two flushed segments,
                             one with more than 1024 docs.
  test_es93_tsdb_hist        time_series (default codec) with a histogram metric and a wildcard.
  test_es93_vectors_default  stored source with dense_vector dims 8: created on 9.2+, so ES
                             excludes the vectors from the stored _source by default.
  test_es93_vectors_included the same with index.mapping.exclude_source_vectors=false.

Writes gefrierschrank-synthetic-source/src/test/resources/fixtures/es93-repo plus one
es93-<index>-expected.json per index: {_id: _source} exactly as ES's _search returns it (synthetic
source for logsdb/TSDB, "_source": {"exclude_vectors": false} for the vector indices).

Runs its own throwaway container gs-compat-es93 (cached image gefrierschrank-es9, ES 9.3.0) on
port 9240 with a bind-mounted shared-filesystem repository, starts a trial license (logsdb synthetic
source needs it) and removes the container afterwards. Hold the docker slot while running:
  $SLOT docker 1 python3 scripts/generate_es93_fixture.py
"""
import base64
import json
import pathlib
import random
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "gefrierschrank-synthetic-source/src/test/resources/fixtures"
OUTPUT = FIXTURES / "es93-repo"
WORK = ROOT / "target/es93-fixture-work"
CONTAINER = "gs-compat-es93"
PORT = 9240
IMAGE = "gefrierschrank-es9"
ES = f"http://localhost:{PORT}"

LOGSDB = "test_es93_logsdb_binary"
TSDB = "test_es93_tsdb_hist"
VECTORS_DEFAULT = "test_es93_vectors_default"
VECTORS_INCLUDED = "test_es93_vectors_included"

HOSTS = ["web-01.example.internal", "web-02.example.internal", "db-07.example.internal"]
PATHS = ["/api/v1/orders", "/api/v2/users/search", "/static/app.js", "/health", "/api/v1/items/*?q=a"]


def es(method, path, body=None, timeout=120):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(ES + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def start_container():
    if docker("ps", "-a", "--format", "{{.Names}}").stdout.split().count(CONTAINER):
        raise RuntimeError(f"container {CONTAINER} already exists; remove it first")
    snapshots = WORK / "snapshots"
    snapshots.mkdir(parents=True)
    docker("run", "-d", "--name", CONTAINER, "-p", f"{PORT}:9200",
           "-e", "discovery.type=single-node", "-e", "xpack.security.enabled=false",
           "-e", "ES_JAVA_OPTS=-Xms1g -Xmx1g", "-e", "path.repo=/snapshots",
           "-v", f"{snapshots}:/snapshots", IMAGE)
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            if es("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")["status"] in ("yellow", "green"):
                return snapshots
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(2)
    raise RuntimeError("Elasticsearch did not start")


def bulk(index, docs, op="index"):
    lines = []
    for doc_id, doc in docs:
        action = {"_index": index} if doc_id is None else {"_index": index, "_id": doc_id}
        lines.append(json.dumps({op: action}))
        lines.append(json.dumps(doc))
    request = urllib.request.Request(f"{ES}/_bulk?refresh=true", data=("\n".join(lines) + "\n").encode(),
                                     method="POST", headers={"Content-Type": "application/x-ndjson"})
    with urllib.request.urlopen(request, timeout=300) as response:
        result = json.load(response)
    if result.get("errors"):
        failed = [item for item in result["items"] if "error" in next(iter(item.values()))]
        raise RuntimeError(f"bulk errors in {index}: {failed[:2]}")


def create(index, settings, mappings, batches, op="index"):
    es("PUT", "/" + index, {"settings": dict({"number_of_shards": 1, "number_of_replicas": 0}, **settings),
                            "mappings": mappings})
    for batch in batches:
        bulk(index, batch, op)
        es("POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")


def long_path(rng, length):
    return "/archive/" + "".join(rng.choice("abcdefghijklmnopqrstuvwxyz0123456789/-_") for _ in range(length))


def logsdb_doc(rng, i):
    doc = {
        "@timestamp": f"2026-09-{1 + i % 28:02d}T{i % 24:02d}:{(i * 7) % 60:02d}:{(i * 13) % 60:02d}.{i % 1000:03d}Z",
        "host": {"name": HOSTS[i % len(HOSTS)]},
        "message": f"request {i} served by {HOSTS[i % len(HOSTS)]} in {rng.randint(1, 5000)} ms",
        "status": "ok" if i % 3 else ("error-" + "x" * (i % 30)),
    }
    if i % 5 != 2:
        paths = [f"{rng.choice(PATHS)}/{rng.randint(0, 99999)}" for _ in range(1 + i % 3)]
        if i % 250 == 7:
            paths.append(long_path(rng, 3000 + i))
        doc["url"] = {"path": paths[0] if len(paths) == 1 else paths}
    if i % 2 == 0:
        doc["latency_histogram"] = {"values": [0.1, 0.5, 1.0, 5.0],
                                    "counts": [rng.randint(0, 50) for _ in range(4)]}
    if i % 3 == 1:
        doc["payload"] = base64.b64encode(rng.randbytes(16 + i % 48)).decode()
    if i % 5 < 2:
        doc["client_range"] = ({"gte": f"10.0.{i % 256}.0", "lte": f"10.0.{i % 256}.255"} if i % 2
                               else f"10.1.{i % 256}.0/24")
    if i % 4 == 0:
        doc["labels"] = [rng.choice(["red", "green", "blue"]) for _ in range(1 + i % 4)]
    return doc


def tsdb_doc(rng, i):
    host = HOSTS[i % len(HOSTS)]
    return {
        "@timestamp": f"2026-09-10T{(i // 60) % 24:02d}:{i % 60:02d}:{(i * 7) % 60:02d}.{i % 1000:03d}Z",
        "host": host,
        "cpu": round(rng.uniform(0, 100), 2),
        "latency": {"values": [1.0, 10.0, 100.0], "counts": [rng.randint(0, 9) for _ in range(3)]},
        "url": f"{rng.choice(PATHS)}/{rng.randint(0, 999)}",
    }


def vector_doc(rng, i):
    return {"title": f"document {i:02d}", "embedding": [round(rng.uniform(-1, 1), 3) or 0.5 for _ in range(8)]}


def expected_sources(index, exclude_vectors=None):
    body = {"size": 10000, "query": {"match_all": {}}}
    if exclude_vectors is not None:
        body["_source"] = {"exclude_vectors": exclude_vectors}
    hits = es("POST", f"/{index}/_search", body)["hits"]["hits"]
    return {hit["_id"]: hit["_source"] for hit in sorted(hits, key=lambda h: h["_id"])}


def write_expected(path, sources):
    """One document per line keeps the files small and diffable."""
    lines = [f" {json.dumps(doc_id)}: {json.dumps(source, separators=(',', ':'))}" for doc_id, source in sources.items()]
    path.write_text("{\n" + ",\n".join(lines) + "\n}\n")


def segments(index):
    shards = es("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"]
    return {name: {k: seg.get(k) for k in ("num_docs", "deleted_docs", "size_in_bytes", "compound", "attributes")}
            for copy in shards for name, seg in copy["segments"].items()}


def main():
    if OUTPUT.exists():
        raise RuntimeError(f"{OUTPUT} already exists; move it away first")
    if WORK.exists():
        shutil.rmtree(WORK)
    snapshots = start_container()
    try:
        license_type = es("GET", "/_license")["license"]
        if license_type["status"] != "active" or license_type["type"] not in ("trial", "platinum", "enterprise"):
            es("POST", "/_license/start_trial?acknowledge=true")
        rng = random.Random(9300)

        logs = [(f"log-{i:04d}", logsdb_doc(rng, i)) for i in range(1300)]
        create(LOGSDB, {"index.mode": "logsdb", "index.codec": "best_compression"}, {"properties": {
            "@timestamp": {"type": "date"},
            "host": {"properties": {"name": {"type": "keyword"}}},
            "message": {"type": "text"},
            "status": {"type": "keyword", "ignore_above": 16},
            "url": {"properties": {"path": {"type": "wildcard"}}},
            "latency_histogram": {"type": "histogram"},
            "payload": {"type": "binary", "doc_values": True},
            "client_range": {"type": "ip_range"},
            "labels": {"type": "counted_keyword"},
        }}, [logs[:1100], logs[1100:]])

        metrics = [(None, tsdb_doc(rng, i)) for i in range(400)]
        create(TSDB, {"index.mode": "time_series", "index.routing_path": ["host"],
                      "index.time_series.start_time": "2026-09-01T00:00:00Z",
                      "index.time_series.end_time": "2026-10-01T00:00:00Z"}, {"properties": {
            "@timestamp": {"type": "date"},
            "host": {"type": "keyword", "time_series_dimension": True},
            "cpu": {"type": "double", "time_series_metric": "gauge"},
            "latency": {"type": "histogram"},
            "url": {"type": "wildcard"},
        }}, [metrics[:300], metrics[300:]], op="create")

        vector_mappings = {"properties": {"title": {"type": "keyword"},
                                          "embedding": {"type": "dense_vector", "dims": 8, "similarity": "cosine"}}}
        create(VECTORS_DEFAULT, {}, vector_mappings, [[(f"vec-{i:02d}", vector_doc(rng, i)) for i in range(20)]])
        create(VECTORS_INCLUDED, {"index.mapping.exclude_source_vectors": False}, vector_mappings,
               [[(f"vec-{i:02d}", vector_doc(rng, i)) for i in range(20)]])

        indices = [LOGSDB, TSDB, VECTORS_DEFAULT, VECTORS_INCLUDED]
        report = {"elasticsearch_version": es("GET", "/")["version"]["number"], "indices": {}}
        for index in indices:
            effective = es("GET", f"/{index}/_settings?include_defaults=true&flat_settings=true")[index]
            report["indices"][index] = {
                "doc_count": es("GET", f"/{index}/_count")["count"], "segments": segments(index),
                "settings": {k: v for part in (effective["settings"], effective["defaults"]) for k, v in part.items()
                             if k.startswith(("index.codec", "index.mode", "index.mapping.source",
                                              "index.mapping.exclude_source_vectors", "index.version.created"))}}
        if len(report["indices"][LOGSDB]["segments"]) != 2 or len(report["indices"][TSDB]["segments"]) != 2:
            raise RuntimeError(f"expected two segments per synthetic index: {json.dumps(report, indent=2)}")
        expected = {LOGSDB: expected_sources(LOGSDB), TSDB: expected_sources(TSDB),
                    VECTORS_DEFAULT: expected_sources(VECTORS_DEFAULT, exclude_vectors=False),
                    VECTORS_INCLUDED: expected_sources(VECTORS_INCLUDED, exclude_vectors=False)}

        es("PUT", "/_snapshot/es93", {"type": "fs", "settings": {"location": "/snapshots/es93"}})
        snapshot = es("PUT", "/_snapshot/es93/es93-fixture?wait_for_completion=true",
                      {"indices": ",".join(indices), "include_global_state": False}, timeout=300)["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot}")
        shutil.copytree(snapshots / "es93", OUTPUT)
        for index in indices:
            name = index.removeprefix("test_es93_").replace("_", "-")
            write_expected(FIXTURES / f"es93-{name}-expected.json", expected[index])
        print(json.dumps(report, indent=2))
        size = sum(p.stat().st_size for p in OUTPUT.rglob("*") if p.is_file())
        print(f"Wrote {OUTPUT} ({size} bytes)")
    finally:
        docker("rm", "-f", CONTAINER, check=False)
        shutil.rmtree(WORK, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
