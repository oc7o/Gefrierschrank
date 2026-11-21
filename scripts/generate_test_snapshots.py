"""
Generate test snapshots for ES 7, 8, and 9 against a MinIO snapshot repository.

For each ES version:
  1. Start the ES container (docker compose --profile esN up -d)
  2. Wait for health
  3. Install repository-s3 plugin if not present, then restart
  4. Register MinIO as a snapshot repository
  5. Create an index with diverse field types and bulk-index dummy documents
  6. Take a snapshot
  7. Tear down the ES container

MinIO must already be running (docker compose up -d minio).

Usage:
  cd scripts && uv run generate_test_snapshots.py

The ES 7-family fixtures for the in-process Lucene 8 read path have their own subcommands
(es7-family, es7-best-compression, es7-goldens), and so has the stored-format fixture
(source-formats); see the sections near the end of this file.
"""

import base64
import hashlib
import json
import pathlib
import random
import shutil
import string
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

import requests

# ── Configuration ────────────────────────────────────────────────────────────

PROJECT_ROOT = subprocess.run(
    ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
).stdout.strip() or "."

ES_VERSIONS = [
    {"profile": "es7", "tag": "7.17.9", "label": "ES 7.17.9 (Lucene 8.x)"},
    {"profile": "es8", "tag": "8.17.0", "label": "ES 8.17.0 (Lucene 9.x)"},
    {"profile": "es9", "tag": "9.3.0", "label": "ES 9.3.0 (Lucene 10.x)"},
]

MINIO_ENDPOINT = "http://localhost:9000"      # host-side endpoint (mc, browser)
MINIO_DOCKER_ENDPOINT = "http://minio:9000"   # endpoint as seen from ES container
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
SNAPSHOT_BUCKET = "snapshots"
SNAPSHOT_REPO_NAME = "minio_repo"

ES_URL = "http://localhost:9200"
NUM_DOCS = 500
INDEX_NAME_PREFIX = "test_data"

# TSDB / synthetic-source fixtures. These exercise a fundamentally different read path:
# with synthetic source, _source is NOT stored - Elasticsearch reconstructs it from doc values
# at query time, using its proprietary ES87TSDB/ES819TSDB doc-values formats. Only ES 9.x
# writes ES819TSDB, so the es9 profile is the one that matters for that format.
TSDB_INDEX_NAME_PREFIX = "test_tsdb"
SYNTHETIC_INDEX_NAME_PREFIX = "test_synthetic"
NUM_TSDB_DOCS = 200


# ── Docker helpers ───────────────────────────────────────────────────────────

def run(cmd: list[str], check: bool = True, timeout: int = 120) -> subprocess.CompletedProcess:
    print(f"  $ {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=True, text=True, check=check, timeout=timeout)


def docker_compose(profile: str, *args: str) -> subprocess.CompletedProcess:
    return run(
        ["docker", "compose", "--profile", profile, *args],
        check=True,
    )


def wait_for_es(timeout: int = 120) -> None:
    print("  Waiting for ES to become healthy...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{ES_URL}/_cluster/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") in ("green", "yellow"):
                print(f"  ES is up: {r.json()['status']}")
                return
        except requests.ConnectionError:
            pass
        time.sleep(2)
    raise TimeoutError(f"ES did not become healthy within {timeout}s")


# ── MinIO bucket setup ──────────────────────────────────────────────────────

def ensure_minio_bucket():
    """Create the snapshot bucket in MinIO if it doesn't exist."""
    print("Setting up MinIO bucket...")
    # Find the compose network MinIO is attached to, so the mc container can reach it
    container_id = run(["docker", "compose", "ps", "-q", "minio"]).stdout.strip()
    network = run([
        "docker", "inspect", "-f",
        "{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}",
        container_id,
    ]).stdout.strip()
    print(f"  Using network '{network}'")
    run([
        "docker", "run", "--rm", "--network", network, "--entrypoint", "/bin/sh",
        "minio/mc", "-c",
        f"mc alias set local http://minio:9000 {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} && "
        f"mc mb --ignore-existing local/{SNAPSHOT_BUCKET}"
    ], check=True, timeout=60)
    print(f"  Bucket '{SNAPSHOT_BUCKET}' ready")


# ── Snapshot repository setup ───────────────────────────────────────────────

def register_snapshot_repo():
    print("  Registering snapshot repository...")
    r = requests.put(
        f"{ES_URL}/_snapshot/{SNAPSHOT_REPO_NAME}",
        headers={"Content-Type": "application/json"},
        json={
            "type": "s3",
            "settings": {
                "bucket": SNAPSHOT_BUCKET,
                "endpoint": MINIO_DOCKER_ENDPOINT.replace("http://", ""),
                "protocol": "http",
                "path_style_access": True,
                "compress": True,
            },
        },
        timeout=30,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Failed to register repo: {r.status_code} {r.text}")
    print(f"  Repository '{SNAPSHOT_REPO_NAME}' registered")


# ── Index creation and data generation ───────────────────────────────────────

INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "id":              {"type": "integer"},
            "title":           {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "description":     {"type": "text"},
            "category":        {"type": "keyword"},
            "tags":            {"type": "keyword"},
            "price":           {"type": "float"},
            "quantity":        {"type": "integer"},
            "in_stock":        {"type": "boolean"},
            "created_at":      {"type": "date"},
            "metadata":        {"type": "object", "enabled": False},
            "location":        {"type": "geo_point"},
            "rating":          {"type": "float"},
            "sku":             {"type": "keyword"},
            "weight_kg":       {"type": "float"},
            "views":           {"type": "long"},
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
}

CATEGORIES = ["electronics", "books", "clothing", "food", "tools", "toys", "sports"]
TAGS = ["sale", "new", "featured", "limited", "popular", "clearance", "exclusive"]


def generate_doc(doc_id: int) -> dict:
    return {
        "id": doc_id,
        "title": f"Product {doc_id} - {''.join(random.choices(string.ascii_uppercase, k=5))}",
        "description": " ".join(random.choices(
            ["lorem", "ipsum", "dolor", "sit", "amet", "consectetur", "adipiscing",
             "elit", "sed", "do", "eiusmod", "tempor", "incididunt"],
            k=random.randint(10, 30),
        )),
        "category": random.choice(CATEGORIES),
        "tags": random.sample(TAGS, k=random.randint(1, 3)),
        "price": round(random.uniform(0.99, 9999.99), 2),
        "quantity": random.randint(0, 10000),
        "in_stock": random.random() > 0.2,
        "created_at": datetime(
            random.randint(2020, 2025), random.randint(1, 12),
            random.randint(1, 28), random.randint(0, 23),
            random.randint(0, 59), random.randint(0, 59),
            tzinfo=timezone.utc,
        ).isoformat(),
        "metadata": {
            "color": random.choice(["red", "blue", "green", "black", "white"]),
            "size": random.choice(["S", "M", "L", "XL"]),
            "brand": f"Brand-{random.randint(1, 20)}",
        },
        "location": {
            "lat": round(random.uniform(-90, 90), 6),
            "lon": round(random.uniform(-180, 180), 6),
        },
        "rating": round(random.uniform(0, 5), 1),
        "sku": f"SKU-{''.join(random.choices(string.ascii_uppercase + string.digits, k=8))}",
        "weight_kg": round(random.uniform(0.01, 50.0), 3),
        "views": random.randint(0, 1_000_000),
    }


def bulk_index(es_version_tag: str):
    index_name = f"{INDEX_NAME_PREFIX}_{es_version_tag.replace('.', '_')}"
    print(f"  Creating index '{index_name}'...")

    # Delete if exists from a previous run
    requests.delete(f"{ES_URL}/{index_name}", timeout=10)

    r = requests.put(
        f"{ES_URL}/{index_name}",
        headers={"Content-Type": "application/json"},
        json=INDEX_MAPPING,
        timeout=30,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Failed to create index: {r.status_code} {r.text}")

    print(f"  Bulk-indexing {NUM_DOCS} documents...")
    batch_size = 100
    for batch_start in range(0, NUM_DOCS, batch_size):
        lines = []
        for i in range(batch_start, min(batch_start + batch_size, NUM_DOCS)):
            lines.append(json.dumps({"index": {"_index": index_name, "_id": i + 1}}))
            lines.append(json.dumps(generate_doc(i + 1)))
        body = "\n".join(lines) + "\n"
        r = requests.post(
            f"{ES_URL}/_bulk",
            headers={"Content-Type": "application/x-ndjson"},
            data=body,
            timeout=30,
        )
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Bulk index failed: {r.status_code} {r.text}")
        if r.json().get("errors"):
            errors = [item for item in r.json()["items"] if "error" in item.get("index", {})]
            raise RuntimeError(f"Bulk index had errors: {errors[:3]}")

    # Force a flush so segments are committed to disk
    requests.post(f"{ES_URL}/{index_name}/_flush", timeout=30)
    print(f"  Indexed {NUM_DOCS} documents into '{index_name}'")


# ── TSDB / synthetic-source index creation ───────────────────────────────────

# A time-series index: routing_path + time_series mode make ES use its TSDB codec
# (ES819TSDBDocValuesFormat on 9.x) and synthetic source, so _source is never stored.
TSDB_INDEX_BODY = {
    "mappings": {
        "properties": {
            "@timestamp":   {"type": "date"},
            "host":         {"type": "keyword", "time_series_dimension": True},
            "region":       {"type": "keyword", "time_series_dimension": True},
            "cpu_pct":      {"type": "double", "time_series_metric": "gauge"},
            "mem_bytes":    {"type": "long", "time_series_metric": "gauge"},
            "requests":     {"type": "long", "time_series_metric": "counter"},
            "status":       {"type": "keyword"},
        }
    },
    "settings": {
        "index.mode": "time_series",
        "index.routing_path": ["host", "region"],
        "index.time_series.start_time": "2026-01-01T00:00:00Z",
        "index.time_series.end_time": "2026-12-31T23:59:59Z",
        "number_of_shards": 1,
        "number_of_replicas": 0,
    },
}

# A plain (non-TSDB) index that merely opts into synthetic source. This isolates the two
# variables: synthetic source WITHOUT the TSDB doc-values format, so a reader can be tested
# against stock Lucene doc values first before tackling ES's proprietary encoding.
SYNTHETIC_INDEX_BODY = {
    "mappings": {
        "_source": {"mode": "synthetic"},
        "properties": {
            "id":        {"type": "integer"},
            "name":      {"type": "keyword"},
            "score":     {"type": "double"},
            "count":     {"type": "long"},
            "active":    {"type": "boolean"},
            "created":   {"type": "date"},
            # scaled_float lives in ES's mapper-extras module (bundled into a real node, not
            # part of org.elasticsearch:elasticsearch core) - included here specifically so the
            # fixture this generates exercises gefrierschrank-synthetic-source's vendored
            # MapperExtrasPlugin wiring (see that module's ScaledFloatFieldMapper vendoring notes).
            "price":     {"type": "scaled_float", "scaling_factor": 100},
            # These four live in separate ES x-pack plugin modules, EL2.0-only licensed (a
            # materially different, more restrictive license than mapper-extras's tri-license -
            # see gefrierschrank-synthetic-source's org.elasticsearch.xpack.*.package-info.java
            # and LICENSE-ELASTIC-2.0). Included here to exercise the vendored x-pack mappers for
            # aggregate_metric_double/histogram/constant_keyword/wildcard: without them, an index
            # using one fails reconstruction with "No mapper found for type [x]".
            "agg_latency":  {"type": "aggregate_metric_double", "metrics": ["min", "max", "sum", "value_count"], "default_metric": "max"},
            "resp_sizes":   {"type": "histogram"},
            "service_kind": {"type": "constant_keyword", "value": "web"},
            "search_tags":  {"type": "wildcard"},
            # These five exercise the vendored x-pack mappers for version, counted_keyword,
            # unsigned_long and shape/geo_shape, also EL2.0-only: without them, an index using one
            # fails with the same "No mapper found for type [x]" (see
            # gefrierschrank-synthetic-source's org.elasticsearch.xpack.{versionfield,
            # countedkeyword,unsignedlong,spatial}.*.package-info.java). semantic_text
            # (ML-inference-backed) is intentionally out of scope.
            "app_version":  {"type": "version"},
            "tag_counts":   {"type": "counted_keyword"},
            "session_id":   {"type": "unsigned_long"},
            "bounds":       {"type": "shape"},
            "location":     {"type": "geo_shape"},
        },
    },
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
}

HOSTS = [f"host-{i:02d}" for i in range(1, 11)]
REGIONS = ["eu-central-1", "us-east-1", "ap-south-1"]


def create_tsdb_index(es_version_tag: str) -> str | None:
    """Create a time-series index (TSDB codec + synthetic source). Returns the name, or None."""
    index_name = f"{TSDB_INDEX_NAME_PREFIX}_{es_version_tag.replace('.', '_')}"
    print(f"  Creating TSDB index '{index_name}'...")
    requests.delete(f"{ES_URL}/{index_name}", timeout=10)
    r = requests.put(
        f"{ES_URL}/{index_name}",
        headers={"Content-Type": "application/json"},
        json=TSDB_INDEX_BODY,
        timeout=30,
    )
    if r.status_code not in (200, 201):
        print(f"  ! TSDB index not supported on this version: {r.status_code} {r.text[:200]}")
        return None

    print(f"  Bulk-indexing {NUM_TSDB_DOCS} time-series documents...")
    lines = []
    base = datetime(2026, 6, 1, tzinfo=timezone.utc)
    for i in range(NUM_TSDB_DOCS):
        ts = base.replace(minute=i % 60, second=i % 60)
        doc = {
            "@timestamp": ts.isoformat(),
            "host": HOSTS[i % len(HOSTS)],
            "region": REGIONS[i % len(REGIONS)],
            "cpu_pct": round(random.uniform(0, 100), 3),
            "mem_bytes": random.randint(1 << 20, 1 << 34),
            "requests": i * 7,
            "status": random.choice(["ok", "warn", "error"]),
        }
        lines.append(json.dumps({"create": {"_index": index_name}}))
        lines.append(json.dumps(doc))
    r = requests.post(
        f"{ES_URL}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data="\n".join(lines) + "\n",
        timeout=60,
    )
    if r.status_code not in (200, 201) or r.json().get("errors"):
        errs = [i for i in r.json().get("items", []) if "error" in i.get("create", {})][:2]
        print(f"  ! TSDB bulk index failed: {r.status_code} {errs}")
        return None
    requests.post(f"{ES_URL}/{index_name}/_forcemerge?max_num_segments=1", timeout=120)
    requests.post(f"{ES_URL}/{index_name}/_flush", timeout=30)
    print(f"  Indexed {NUM_TSDB_DOCS} documents into '{index_name}'")
    return index_name


def create_synthetic_source_index(es_version_tag: str) -> str | None:
    """Create a non-TSDB index with synthetic source. Returns the name, or None if unsupported."""
    index_name = f"{SYNTHETIC_INDEX_NAME_PREFIX}_{es_version_tag.replace('.', '_')}"
    print(f"  Creating synthetic-source index '{index_name}'...")
    requests.delete(f"{ES_URL}/{index_name}", timeout=10)
    r = requests.put(
        f"{ES_URL}/{index_name}",
        headers={"Content-Type": "application/json"},
        json=SYNTHETIC_INDEX_BODY,
        timeout=30,
    )
    if r.status_code not in (200, 201):
        print(f"  ! synthetic source not supported here: {r.status_code} {r.text[:200]}")
        return None

    lines = []
    for i in range(NUM_TSDB_DOCS):
        min_val = round(random.uniform(1, 50), 2)
        max_val = round(min_val + random.uniform(1, 200), 2)
        value_count = random.randint(1, 500)
        sum_val = round(random.uniform(min_val, max_val) * value_count, 2)
        doc = {
            "id": i,
            "name": f"name-{i:04d}",
            "score": round(random.uniform(0, 100), 4),
            "count": random.randint(0, 1 << 40),
            "active": i % 3 != 0,
            "created": datetime(2026, 1, 1, tzinfo=timezone.utc).isoformat(),
            "price": round(random.uniform(0, 500), 2),
            # See SYNTHETIC_INDEX_BODY's mapping comment for why these four are EL2.0-only.
            "agg_latency": {"min": min_val, "max": max_val, "sum": sum_val, "value_count": value_count},
            "resp_sizes": {
                "values": [64.0, 128.0, 256.0, 512.0, 1024.0],
                "counts": [random.randint(1, 20) for _ in range(5)],
            },
            "service_kind": "web",
            "search_tags": f"tag-{i:04d}-*-search*wildcard*{'abc' if i % 2 == 0 else 'xyz'}",
            # See SYNTHETIC_INDEX_BODY's mapping comment for these five (version/
            # counted_keyword/unsigned_long/shape/geo_shape). Deterministic (not random) so the
            # test suite can assert exact expected values.
            "app_version": [
                "1.0.0", "1.2.3", "2.0.0-rc1", "2.1.0", "2.4.1-beta",
                "0.9.5", "3.0.0", "10.2.1", "2.1.0-alpha.1", "1.0.0+build.5",
            ][i % 10],
            "tag_counts": [
                tag
                for j, tag in enumerate(
                    ["red", "green", "blue", "alpha", "beta"][(i + k) % 5] for k in range(1 + (i % 3))
                )
                for _ in range(1 + ((i + j) % 3))
            ],
            # Deliberately spans values both below and above Long.MAX_VALUE (9223372036854775807)
            # - the whole point of unsigned_long is the latter would be negative if misread as a
            # signed long. Sent as a numeric STRING (ES's unsigned_long field accepts this) to
            # avoid JSON double-precision loss for the full unsigned 64-bit range.
            "session_id": str(
                (9223372036854775807 + 1 + i * 1_000_000_000_000) if i % 2 == 0 else (i * 12345)
            ),
            "bounds": (
                f"POLYGON (({i} {i}, {i + 10} {i}, {i + 10} {i + 10}, {i} {i + 10}, {i} {i}))"
                if i % 2 == 0
                else f"POINT ({i * 1.5} {i * 2.5})"
            ),
            "location": (
                f"POINT ({-170 + (i * 6) % 340} {-80 + (i * 3) % 160})"
                if i % 3 == 0
                else (
                    f"POLYGON (({-170 + (i * 6) % 340} {-80 + (i * 3) % 160}, "
                    f"{-170 + (i * 6) % 340 + 1} {-80 + (i * 3) % 160}, "
                    f"{-170 + (i * 6) % 340 + 1} {-80 + (i * 3) % 160 + 1}, "
                    f"{-170 + (i * 6) % 340} {-80 + (i * 3) % 160 + 1}, "
                    f"{-170 + (i * 6) % 340} {-80 + (i * 3) % 160}))"
                )
            ),
        }
        lines.append(json.dumps({"index": {"_index": index_name, "_id": i}}))
        lines.append(json.dumps(doc))
    r = requests.post(
        f"{ES_URL}/_bulk",
        headers={"Content-Type": "application/x-ndjson"},
        data="\n".join(lines) + "\n",
        timeout=60,
    )
    if r.status_code not in (200, 201) or r.json().get("errors"):
        errs = [i for i in r.json().get("items", []) if "error" in i.get("index", {})][:2]
        print(f"  ! synthetic bulk index failed: {r.status_code} {errs}")
        return None
    requests.post(f"{ES_URL}/{index_name}/_forcemerge?max_num_segments=1", timeout=120)
    requests.post(f"{ES_URL}/{index_name}/_flush", timeout=30)
    print(f"  Indexed {NUM_TSDB_DOCS} documents into '{index_name}'")
    return index_name


# ── Snapshot creation ────────────────────────────────────────────────────────

def take_snapshot(es_version_tag: str, extra_indices: list[str] | None = None):
    snapshot_name = f"snap_{es_version_tag.replace('.', '_')}"
    print(f"  Taking snapshot '{snapshot_name}'...")

    indices = [f"{INDEX_NAME_PREFIX}_{es_version_tag.replace('.', '_')}"]
    indices.extend(extra_indices or [])

    r = requests.put(
        f"{ES_URL}/_snapshot/{SNAPSHOT_REPO_NAME}/{snapshot_name}?wait_for_completion=true",
        headers={"Content-Type": "application/json"},
        json={
            "indices": ",".join(indices),
            "include_global_state": False,
        },
        timeout=120,
    )
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Snapshot failed: {r.status_code} {r.text}")

    snapshot_info = r.json().get("snapshot", {})
    if snapshot_info.get("state") != "SUCCESS":
        raise RuntimeError(f"Snapshot did not succeed: {snapshot_info}")

    print(f"  Snapshot '{snapshot_name}' created successfully")


# ── Main loop ────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  Gefrierschrank — Test Snapshot Generator")
    print("=" * 70)

    # Ensure MinIO is running and bucket exists
    print("\n[0/4] Starting MinIO...")
    run(["docker", "compose", "up", "-d", "minio"], check=True, timeout=60)
    time.sleep(3)
    ensure_minio_bucket()

    for i, es in enumerate(ES_VERSIONS, 1):
        profile = es["profile"]
        tag = es["tag"]
        label = es["label"]
        print(f"\n[{i}/{len(ES_VERSIONS)}] {label}")
        print("-" * 50)
        print("  Starting ES container...")
        docker_compose(profile, "up", "-d", profile)
        wait_for_es()

        # Register snapshot repository
        register_snapshot_repo()

        # Create index and bulk-index documents
        bulk_index(tag)

        # TSDB + synthetic-source fixtures: these have no stored _source at all, and (on 9.x)
        # exercise ES's proprietary ES819TSDB doc-values format. Best-effort - older versions
        # reject some of these settings, which is why each returns None instead of raising.
        extra = [name for name in (create_tsdb_index(tag), create_synthetic_source_index(tag)) if name]

        # Take snapshot
        take_snapshot(tag, extra)

        # Tear down ES container
        print("  Stopping ES container...")
        docker_compose(profile, "stop", profile)
        docker_compose(profile, "rm", "-f", profile)

        print(f"  Done with {label}")

    print("\n" + "=" * 70)
    print("  All snapshots generated successfully!")
    print(f"  Check MinIO console at http://localhost:9001")
    print(f"  Bucket: {SNAPSHOT_BUCKET}")
    print("=" * 70)


# ── ES 7-family fixtures (in-process Lucene 8 reads) ─────────────────────────
#
# Small committed repositories for the in-process ES 7 read path (Lucene 10 opens indices
# created by Lucene 8 read-only). Unlike main() these use filesystem repositories bind-mounted
# into throwaway containers, so the generated directory IS the fixture and MinIO is not touched.
#
#   es7-features-repo  ES 7.17.9: soft deletes (updates, deletes, tombstones), hard deletes
#                      (soft_deletes disabled), nested docs, custom routing, numeric / base64 /
#                      UTF-8 / auto-generated ids, a CR/LF pretty-printed source, a completion
#                      field, and an index without _source.
#   es7-upgraded-repo  the 7.17.9 index es7_upgraded restored twice into ES 8.17.0 with extra docs
#                      and deletes: es7_mixed keeps its Lucene 8 segments next to new Lucene 9 ones;
#                      es7_merged is force-merged, so every file is written by Lucene 9 while the
#                      index-created Lucene major stays 8.
#
#   es7-best-compression-repo  ES 7.17.9 index.codec best_compression (Lucene87 DEFLATE with preset
#                      dictionaries, 480 KB chunks): a segment spanning several chunks, a ~1 MB
#                      document stored in slices, soft updates and deletes.
#
# Search results of every index are saved next to the golden rows for cross-checking. The golden
# rows themselves come from the 1.0.0 Lucene 9 worker path (es7-goldens below).
# Usage: cd scripts && uv run generate_test_snapshots.py es7-family
#        cd scripts && uv run generate_test_snapshots.py es7-best-compression
#        cd scripts && uv run generate_test_snapshots.py es7-goldens <1.0.0 cli jar> <1.0.0 lucene9 jar>

ES7_FAMILY_FIXTURES = pathlib.Path(PROJECT_ROOT) / "gefrierschrank-core/src/test/resources/fixtures"
ES7_FAMILY_CONTAINERS = {"es7": ("gs-lucene10-es7", "gefrierschrank-es7:latest", 9220),
                         "es8": ("gs-lucene10-es8", "gefrierschrank-es8:latest", 9221)}


def es7_family_request(port: int, method: str, path: str, body=None, ok=(200, 201)) -> dict:
    r = requests.request(method, f"http://localhost:{port}{path}", json=body, timeout=180,
                         headers={"Content-Type": "application/json"})
    if r.status_code not in ok:
        raise RuntimeError(f"{method} {path} failed: {r.status_code} {r.text[:500]}")
    return r.json() if r.text else {}


def es7_family_start(kind: str, mounts: dict[str, pathlib.Path], readonly: set[str]) -> int:
    name, image, port = ES7_FAMILY_CONTAINERS[kind]
    run(["docker", "rm", "-f", name], check=False)
    command = ["docker", "run", "-d", "--name", name, "-p", f"{port}:9200",
               "-e", "discovery.type=single-node", "-e", "xpack.security.enabled=false",
               "-e", "ES_JAVA_OPTS=-Xms512m -Xmx512m", "-e", "ingest.geoip.downloader.enabled=false",
               "-e", "path.repo=" + ",".join(mounts)]
    for target, source in mounts.items():
        command += ["-v", f"{source}:{target}" + (":ro" if target in readonly else "")]
    run(command + [image])
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            health = requests.get(f"http://localhost:{port}/_cluster/health", timeout=5)
            if health.status_code == 200 and health.json().get("status") in ("green", "yellow"):
                return port
        except requests.ConnectionError:
            pass
        time.sleep(2)
    raise TimeoutError(f"{name} did not become healthy")


def es7_family_stop(kind: str) -> None:
    run(["docker", "rm", "-f", ES7_FAMILY_CONTAINERS[kind][0]], check=False)


def es7_family_bulk(port: int, index: str, actions: list[tuple[dict, dict | None]]) -> None:
    lines = []
    for action, doc in actions:
        lines.append(json.dumps(action))
        if doc is not None:
            lines.append(json.dumps(doc))
    r = requests.post(f"http://localhost:{port}/{index}/_bulk?refresh=true", data="\n".join(lines) + "\n",
                      headers={"Content-Type": "application/x-ndjson"}, timeout=180)
    if r.status_code != 200 or r.json().get("errors"):
        raise RuntimeError(f"bulk into {index} failed: {r.status_code} {r.text[:500]}")


def es7_family_flush(port: int, index: str) -> None:
    es7_family_request(port, "POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")


def es7_family_doc(rng: random.Random, n: int, nested: bool) -> dict:
    doc = {"n": n, "title": f"Item {n} " + " ".join(rng.choice(["alpha", "beta", "gamma", "delta", "omega"])
                                                     for _ in range(rng.randint(3, 8))),
           "tenant": f"tenant-{n % 3}", "price": round(rng.uniform(1, 500), 2)}
    if nested:
        doc["comments"] = [{"author": f"user-{n}-{c}", "text": f"comment {c} on {n}"} for c in range(2)]
    return doc


# numeric, leading-zero (base64 alphabet), base64 whose decoded first byte needs the 0xfd escape,
# and UTF-8 ids; see Elasticsearch's Uid.encodeId.
ES7_FAMILY_IDS = ["1", "42", "9007199254740993", "0017", "dGVzdC1pZA", "_w", "_-8", "AAAAAAAA",
                  "user@example.com", "\u00fcmlaut-\u00df", "id with spaces", "\u6f22\u5b57", "a", "-"]


def es7_family_features(port: int, rng: random.Random) -> list[str]:
    features = "es7_features"
    es7_family_request(port, "PUT", "/" + features, {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"properties": {
            "n": {"type": "integer"}, "title": {"type": "text"}, "tenant": {"type": "keyword"},
            "price": {"type": "double"},
            "comments": {"type": "nested", "properties": {"author": {"type": "keyword"}, "text": {"type": "text"}}}}}})
    ids = list(ES7_FAMILY_IDS)
    for batch in range(3):
        actions = []
        for i in range(20):
            n = batch * 20 + i
            doc_id = ids[n] if n < len(ids) else f"doc-{n}"
            action = {"index": {"_id": doc_id}}
            if n % 4 == 1:
                action["index"]["routing"] = f"route-{n % 3}"
            actions.append((action, es7_family_doc(rng, n, nested=n % 5 == 0)))
        if batch == 2:
            actions += [({"index": {}}, es7_family_doc(rng, 1000 + i, nested=False)) for i in range(4)]
        es7_family_bulk(port, features, actions)
        es7_family_flush(port, features)
    # A pretty-printed source with raw CR/LF and a number beyond double precision: stored byte-exact.
    pretty = '{\r\n  "n" : 500,\r\n  "title" : "pretty printed",\n  "price" : 1.10000000000000000001\n}'
    r = requests.put(f"http://localhost:{port}/{features}/_doc/pretty?refresh=true", data=pretty.encode(),
                     headers={"Content-Type": "application/json"}, timeout=60)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"pretty doc failed: {r.status_code} {r.text[:300]}")
    updates = [({"index": {"_id": ids[n]}}, es7_family_doc(rng, 100 + n, nested=n % 5 == 0)) for n in (2, 5, 10)]
    updates += [({"index": {"_id": "doc-21", "routing": "route-0"}}, es7_family_doc(rng, 121, nested=False))]
    deletes = [({"delete": {"_id": ids[n]}}, None) for n in (3, 7)]
    deletes += [({"delete": {"_id": "doc-25"}}, None), ({"delete": {"_id": "doc-33", "routing": "route-0"}}, None)]
    es7_family_bulk(port, features, updates + deletes)
    es7_family_flush(port, features)

    hard = "es7_hard_deletes"
    es7_family_request(port, "PUT", "/" + hard, {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0, "index.soft_deletes.enabled": False},
        "mappings": {"properties": {"n": {"type": "integer"}, "title": {"type": "text"}}}})
    for batch in range(2):
        es7_family_bulk(port, hard, [({"index": {"_id": str(batch * 15 + i)}}, {"n": batch * 15 + i, "title": f"hard {i}"})
                                     for i in range(15)])
        es7_family_flush(port, hard)
    es7_family_bulk(port, hard, [({"delete": {"_id": str(n)}}, None) for n in (1, 16, 29)]
                    + [({"index": {"_id": str(n)}}, {"n": n, "title": "updated"}) for n in (4, 20)])
    es7_family_flush(port, hard)

    completion = "es7_completion"
    es7_family_request(port, "PUT", "/" + completion, {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"properties": {"name": {"type": "keyword"}, "suggest": {"type": "completion"}}}})
    es7_family_bulk(port, completion, [({"index": {"_id": f"c{i}"}}, {"name": f"name {i}", "suggest": [f"sugg{i}", f"alt {i}"]})
                                       for i in range(12)])
    es7_family_flush(port, completion)

    no_source = "es7_no_source"
    es7_family_request(port, "PUT", "/" + no_source, {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {"_source": {"enabled": False}, "properties": {"name": {"type": "keyword"}}}})
    es7_family_bulk(port, no_source, [({"index": {"_id": f"n{i}"}}, {"name": f"name {i}"}) for i in range(6)])
    es7_family_flush(port, no_source)
    return [features, hard, completion, no_source]


def es7_family_search(port: int, indices: list[str]) -> dict:
    result = {}
    for index in indices:
        hits = es7_family_request(port, "POST", f"/{index}/_search", {"size": 1000, "sort": ["_doc"]})["hits"]["hits"]
        result[index] = [{"_id": h["_id"], "_routing": h.get("_routing"), "_source": h.get("_source")} for h in hits]
    return result


def es7_family_snapshot(port: int, repository: str, location: str, snapshot: str, indices: list[str],
                        readonly: bool = False) -> None:
    es7_family_request(port, "PUT", f"/_snapshot/{repository}",
                       {"type": "fs", "settings": {"location": location, "readonly": readonly, "compress": True}})
    if snapshot:
        info = es7_family_request(port, "PUT", f"/_snapshot/{repository}/{snapshot}?wait_for_completion=true",
                                  {"indices": ",".join(indices), "include_global_state": False})["snapshot"]
        if info.get("state") != "SUCCESS":
            raise RuntimeError(f"snapshot {snapshot} failed: {info}")


def es7_family() -> None:
    features_out = ES7_FAMILY_FIXTURES / "es7-features-repo"
    upgraded_out = ES7_FAMILY_FIXTURES / "es7-upgraded-repo"
    goldens_out = ES7_FAMILY_FIXTURES / "es7-golden-rows"
    for existing in (features_out, upgraded_out):
        if existing.exists():
            raise RuntimeError(f"{existing} exists; move it away before regenerating")
    rng = random.Random(7179)
    with tempfile.TemporaryDirectory(prefix="gs-lucene10-es7-family-") as work:
        work = pathlib.Path(work)
        repos = {name: work / name for name in ("features", "upgrade-source", "upgraded")}
        for path in repos.values():
            path.mkdir()
            path.chmod(0o777)
        try:
            port = es7_family_start("es7", {"/repo": repos["features"], "/repo-up": repos["upgrade-source"]}, set())
            indices = es7_family_features(port, rng)
            es7_family_request(port, "PUT", "/es7_upgraded", {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0},
                "mappings": {"properties": {"n": {"type": "integer"}, "title": {"type": "text"},
                                            "tenant": {"type": "keyword"}, "price": {"type": "double"}}}})
            for batch in range(2):
                es7_family_bulk(port, "es7_upgraded", [({"index": {"_id": f"up-{batch * 20 + i}"}},
                                                         es7_family_doc(rng, batch * 20 + i, nested=False))
                                                        for i in range(20)])
                es7_family_flush(port, "es7_upgraded")
            features_search = es7_family_search(port, indices)
            es7_family_snapshot(port, "features", "/repo", "es7_features_snapshot", indices)
            es7_family_snapshot(port, "upgrade-source", "/repo-up", "es7_upgrade_source", ["es7_upgraded"])
        finally:
            es7_family_stop("es7")
        try:
            port = es7_family_start("es8", {"/repo-up": repos["upgrade-source"], "/repo": repos["upgraded"]},
                                    {"/repo-up"})
            es7_family_snapshot(port, "upgrade-source", "/repo-up", "", [], readonly=True)
            for target in ("es7_mixed", "es7_merged"):
                es7_family_request(port, "POST", "/_snapshot/upgrade-source/es7_upgrade_source/_restore?wait_for_completion=true",
                                   {"indices": "es7_upgraded", "rename_pattern": "es7_upgraded",
                                    "rename_replacement": target, "include_global_state": False})
                es7_family_bulk(port, target, [({"index": {"_id": f"new-{i}"}}, es7_family_doc(rng, 200 + i, nested=False))
                                               for i in range(15)]
                                + [({"delete": {"_id": f"up-{n}"}}, None) for n in (3, 27)]
                                + [({"index": {"_id": "up-8"}}, es7_family_doc(rng, 308, nested=False))])
                es7_family_flush(port, target)
                if target == "es7_merged":
                    es7_family_request(port, "POST", f"/{target}/_forcemerge?max_num_segments=1")
                    es7_family_flush(port, target)
            upgraded_search = es7_family_search(port, ["es7_mixed", "es7_merged"])
            es7_family_snapshot(port, "upgraded", "/repo", "es7_upgraded_snapshot", ["es7_mixed", "es7_merged"])
        finally:
            es7_family_stop("es8")
        shutil.copytree(repos["features"], features_out)
        shutil.copytree(repos["upgraded"], upgraded_out)
    goldens_out.mkdir(exist_ok=True)
    (goldens_out / "es7-features-search.json").write_text(json.dumps(features_search, indent=1, ensure_ascii=False) + "\n")
    (goldens_out / "es7-upgraded-search.json").write_text(json.dumps(upgraded_search, indent=1, ensure_ascii=False) + "\n")
    print(f"Wrote {features_out}, {upgraded_out} and search results under {goldens_out}")


ES7_BEST_COMPRESSION_CHUNK = 491520  # Lucene87StoredFieldsFormat BEST_COMPRESSION block length


def es7_best_compression_source(rng: random.Random, n: int) -> bytes:
    templates = ["GET /api/v{a}/items/{b} 200 in {c} ms", "cache miss for key user:{b} on shard {a}",
                 "retrying upstream call {b} after timeout {c}", "flushed segment {b} with {c} docs"]
    message = " | ".join(rng.choice(templates).format(a=rng.randint(1, 3), b=rng.randint(1, 8), c=rng.randint(1, 9))
                         for _ in range(rng.randint(64, 76)))
    doc = {"n": n, "host": f"web-{n % 17:02d}", "message": message, "bytes": rng.randint(100, 900_000),
           "label": "\u00fcber-\u6f22-" + str(n)}
    return json.dumps(doc, separators=(",", ":"), ensure_ascii=False).encode()


def es7_best_compression_huge_source() -> bytes:
    parts = [f'{{"n":9999,"blob":"']
    size = len(parts[0])
    i = 0
    while size < 2 * ES7_BEST_COMPRESSION_CHUNK + 60_000:
        part = f"line {i:07d} lorem ipsum dolor sit amet; "
        parts.append(part)
        size += len(part)
        i += 1
    parts.append('"}')
    return "".join(parts).encode()


def es7_best_compression_bulk(port: int, index: str, docs: list[tuple[str, str | None, bytes | None]]) -> None:
    lines = []
    for doc_id, routing, source in docs:
        meta = {"_id": doc_id} | ({"routing": routing} if routing else {})
        lines.append(json.dumps({"index" if source is not None else "delete": meta}).encode())
        if source is not None:
            lines.append(source)
    r = requests.post(f"http://localhost:{port}/{index}/_bulk?refresh=true", data=b"\n".join(lines) + b"\n",
                      headers={"Content-Type": "application/x-ndjson"}, timeout=180)
    if r.status_code != 200 or r.json().get("errors"):
        raise RuntimeError(f"bulk into {index} failed: {r.status_code} {r.text[:500]}")


def es7_best_compression() -> None:
    out = ES7_FAMILY_FIXTURES / "es7-best-compression-repo"
    goldens_out = ES7_FAMILY_FIXTURES / "es7-golden-rows"
    if out.exists():
        raise RuntimeError(f"{out} exists; move it away before regenerating")
    index = "es7_best_compression"
    rng = random.Random(7_1709)
    live: dict[str, tuple[str | None, bytes]] = {}

    def index_docs(docs: list[tuple[str, str | None, bytes | None]]) -> None:
        es7_best_compression_bulk(port, index, docs)
        for doc_id, routing, source in docs:
            if source is None:
                live.pop(doc_id)
            else:
                live[doc_id] = (routing, source)

    with tempfile.TemporaryDirectory(prefix="gs-lucene10-es7-bc-") as work:
        repo = pathlib.Path(work) / "repo"
        repo.mkdir()
        repo.chmod(0o777)
        try:
            port = es7_family_start("es7", {"/repo": repo}, set())
            es7_family_request(port, "PUT", "/" + index, {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0, "index.codec": "best_compression"},
                "mappings": {"dynamic": False, "properties": {"n": {"type": "integer"}}}})
            # One segment of ~600 KB stored fields: two chunks, each of 48 KB DEFLATE sub-blocks.
            index_docs([(f"bc-{n}", f"r{n % 4}" if n % 5 == 0 else None, es7_best_compression_source(rng, n))
                        for n in range(200)])
            es7_family_flush(port, index)
            # A ~1 MB document between normal ones: its chunk exceeds twice the chunk size and is sliced.
            index_docs([(f"bc-{n}", None, es7_best_compression_source(rng, n)) for n in range(200, 230)]
                       + [("huge", None, es7_best_compression_huge_source())]
                       + [(f"bc-{n}", None, es7_best_compression_source(rng, n)) for n in range(230, 260)])
            es7_family_flush(port, index)
            index_docs([(f"bc-{n}", f"r{n % 4}" if n % 5 == 0 else None, es7_best_compression_source(rng, 1000 + n))
                        for n in (0, 3, 150, 199, 210, 255)]
                       + [(f"bc-{n}", f"r{n % 4}" if n % 5 == 0 else None, None) for n in (5, 77, 220, 240)])
            es7_family_flush(port, index)
            hits = es7_family_request(port, "POST", f"/{index}/_search",
                                      {"size": 10000, "sort": ["_doc"]})["hits"]["hits"]
            if {h["_id"] for h in hits} != set(live):
                raise RuntimeError("search ids differ from the indexed ids")
            for hit in hits:
                routing, source = live[hit["_id"]]
                if hit.get("_routing") != routing or hit["_source"] != json.loads(source):
                    raise RuntimeError(f"search hit {hit['_id']} differs from the indexed document")
            es7_family_snapshot(port, "best-compression", "/repo", "es7_best_compression_snapshot", [index])
        finally:
            es7_family_stop("es7")
        shutil.copytree(repo, out)
    # The exact bytes sent are what ES stores as _source, so goldens are checked byte for byte.
    search = {index: [{"_id": doc_id, "_routing": routing, "sha256": hashlib.sha256(source).hexdigest()}
                      for doc_id, (routing, source) in sorted(live.items())]}
    goldens_out.mkdir(exist_ok=True)
    (goldens_out / "es7-best-compression-search.json").write_text(json.dumps(search, indent=1) + "\n")
    print(f"Wrote {out} and {goldens_out / 'es7-best-compression-search.json'}")


def es7_goldens(cli_jar: str, worker_jar: str) -> None:
    """Captures the golden rows of every ES 7-family index through the 1.0.0 worker path."""
    goldens_out = ES7_FAMILY_FIXTURES / "es7-golden-rows"
    with tempfile.TemporaryDirectory(prefix="gs-lucene10-goldens-") as work:
        work = pathlib.Path(work)
        source = work / "CaptureEs7Goldens.java"
        source.write_text(CAPTURE_ES7_GOLDENS_JAVA)
        classes = work / "classes"
        run(["javac", "-nowarn", "-cp", cli_jar, "-d", str(classes), str(source)])
        # Same class loader as the jar: the tool reaches the package-private WorkerLineReader.
        result = subprocess.run(["java", "-cp", f"{classes}:{cli_jar}", "de.sloow.gefrierschrank.cli.CaptureEs7Goldens",
                                 str(ES7_FAMILY_FIXTURES), worker_jar, str(goldens_out)],
                                capture_output=True, text=True, timeout=1800)
        print(result.stdout)
        if result.returncode != 0:
            raise RuntimeError(f"golden capture failed:\n{result.stderr[-4000:]}")


# Reads each ES 7-family shard exactly as 1.0.0 dump-to-delta did (WorkerLineReader with envelope and
# index metadata) and records (_id, _routing, _source_kind, _doc, sha256 of the source) per document,
# cross-checked against the _search dumps written by the generators above. Run against the 1.0.0 jars.
CAPTURE_ES7_GOLDENS_JAVA = r"""
package de.sloow.gefrierschrank.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import de.sloow.gefrierschrank.core.repo.FileSystemBlobStore;
import de.sloow.gefrierschrank.core.repo.RepositoryInspector;
import de.sloow.gefrierschrank.core.repo.ShardMaterializer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class CaptureEs7Goldens {
    static final ObjectMapper JSON = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static void main(String[] args) throws Exception {
        Path fixtures = Path.of(args[0]);
        Path workerJar = Path.of(args[1]);
        Path out = Path.of(args[2]);
        var plan = new LinkedHashMap<String, List<String>>();
        plan.put("demo-repo", List.of("test_data_7_17_9"));
        plan.put("es7-features-repo", List.of("es7_features", "es7_hard_deletes", "es7_completion", "es7_no_source"));
        plan.put("es7-upgraded-repo", List.of("es7_mixed", "es7_merged"));
        plan.put("es7-best-compression-repo", List.of("es7_best_compression"));
        var searches = Map.of(
            "es7-features-repo", JSON.readTree(out.resolve("es7-features-search.json").toFile()),
            "es7-upgraded-repo", JSON.readTree(out.resolve("es7-upgraded-search.json").toFile()),
            "es7-best-compression-repo", JSON.readTree(out.resolve("es7-best-compression-search.json").toFile()));
        for (var entry : plan.entrySet()) {
            Path repo = fixtures.resolve(entry.getKey());
            var blobs = new FileSystemBlobStore(repo);
            var inspector = new RepositoryInspector(blobs);
            ObjectNode file = JSON.createObjectNode();
            file.put("_comment", "Captured with the 1.0.0 lucene9 worker path (WorkerLineReader envelope, index metadata"
                + " passed) from gefrierschrank-cli-1.0.0.jar/gefrierschrank-lucene9-1.0.0.jar.");
            for (String index : entry.getValue()) {
                ObjectNode node = file.putObject(index);
                var shard = inspector.shardSnapshot(index, 0);
                node.put("snapshot", inspector.latestSnapshotUuid(index));
                node.put("shard", 0);
                node.put("runtime_1_0_0", LuceneRuntimeSelector.runtimeFor(shard));
                var writtenBy = new java.util.TreeSet<String>();
                shard.files().forEach(f -> { if (f.writtenBy() != null) writtenBy.add(f.writtenBy()); });
                node.putPOJO("written_by", writtenBy);
                Path dir = Files.createTempDirectory("gs-golden-");
                new ShardMaterializer(blobs).materialize(shard, dir);
                JsonNode metadata = inspector.indexMetadata(index);
                var rows = node.putArray("rows");
                var sources = new LinkedHashMap<String, byte[]>();
                try (var reader = new WorkerLineReader(new Lucene9Worker(workerJar), dir, metadata, true)) {
                    for (int i = 0; i < reader.docCount(); i++) {
                        var md = reader.documentMetadata(i);
                        byte[][] holder = new byte[1][];
                        reader.readSource(i, (bytes, length) -> holder[0] = java.util.Arrays.copyOf(bytes, length));
                        ObjectNode row = rows.addObject();
                        row.put("_id", md.id());
                        row.put("_routing", md.routing());
                        row.put("_source_kind", md.sourceKind());
                        row.put("_doc", md.slot());
                        row.put("sha256", sha256(holder[0]));
                        if (sources.put(md.id(), holder[0]) != null) throw new IllegalStateException("duplicate id " + md.id());
                    }
                } catch (Exception failure) {
                    node.remove("rows");
                    node.put("failure", failure.getClass().getName());
                    System.out.println(index + ": 1.0.0 worker failed with " + failure);
                }
                if (node.has("rows")) crossCheck(entry.getKey(), index, sources, node, searches.get(entry.getKey()));
                try (var walk = Files.walk(dir)) {
                    walk.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
                }
            }
            Files.writeString(out.resolve(entry.getKey() + ".json"), JSON.writeValueAsString(file) + "\n");
        }
    }

    static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    static void crossCheck(String repo, String index, Map<String, byte[]> sources, ObjectNode node, JsonNode search)
        throws Exception {
        if (search == null) {
            if (sources.size() != 500) throw new IllegalStateException(index + " rows " + sources.size());
            for (byte[] source : sources.values())
                if (!JSON.readTree(source).path("title").asText().startsWith("Product")) throw new IllegalStateException(index);
            System.out.println(repo + "/" + index + ": " + sources.size() + " rows, titles verified");
            return;
        }
        var hits = new LinkedHashMap<String, JsonNode>();
        for (JsonNode hit : search.path(index)) hits.put(hit.path("_id").asText(), hit);
        if (!hits.keySet().equals(sources.keySet()))
            throw new IllegalStateException(index + " ids differ: search " + hits.keySet() + " worker " + sources.keySet());
        for (JsonNode row : node.path("rows")) {
            String id = row.path("_id").asText();
            JsonNode hit = hits.get(id);
            boolean sameSource = hit.has("sha256")
                ? hit.path("sha256").asText().equals(row.path("sha256").asText())
                : JSON.readTree(new String(sources.get(id), StandardCharsets.UTF_8)).equals(hit.path("_source"));
            if (!sameSource) throw new IllegalStateException(index + " source differs for " + id);
            if (!java.util.Objects.equals(row.path("_routing").asText(null), hit.path("_routing").asText(null)))
                throw new IllegalStateException(index + " routing differs for " + id);
        }
        System.out.println(repo + "/" + index + ": " + sources.size() + " rows equal to Elasticsearch"
            + (search.path(index).path(0).has("sha256") ? " (indexed bytes)" : " _search"));
    }
}
"""


# ── Source formats fixture (stored _source in SMILE, CBOR and YAML) ─────────
#
# ES 9.3.0 stores a document's _source in the request's content type. source-formats-repo holds the
# same logical documents indexed as JSON (sf_json, the twin), SMILE (sf_smile), CBOR (sf_cbor, three
# encodings: indefinite map, definite map, self-describe tag) and YAML (sf_yaml), plus
# sf_nonfinite (NaN and infinities, CBOR half floats and bignums under an enabled:false object),
# sf_yaml_text (hand-written YAML: comments, anchors, block scalars, ...), sf_yaml_nonfinite
# (.nan/.inf) and sf_yaml_nomarker (YAML without the leading '---', which ES cannot detect).
# source-formats-expected.json records every request body's format, the JSON twin's bodies and,
# for every live document, what GET /<index>/_source/<id> with Accept: application/json returned.
# Python has no SMILE encoder: the bodies are encoded by the Jackson factories of a built CLI jar.
# Usage: $SLOT docker 1 uv run --project scripts scripts/generate_test_snapshots.py source-formats \
#            gefrierschrank-cli/target/gefrierschrank-cli-<version>.jar

SOURCE_FORMATS_CONTAINER = ("gs-source-formats-es9", "gefrierschrank-es9:latest", 9222)
SOURCE_FORMATS_REPO = ES7_FAMILY_FIXTURES / "source-formats-repo"
SOURCE_FORMATS_EXPECTED = ES7_FAMILY_FIXTURES / "source-formats-expected.json"
SOURCE_FORMATS_CONTENT_TYPES = {"json": "application/json", "smile": "application/smile",
                                "cbor": "application/cbor", "yaml": "application/yaml"}
SOURCE_FORMATS_MAPPING = {"properties": {
    "n": {"type": "integer"}, "title": {"type": "text"}, "price": {"type": "double"},
    "count": {"type": "long"}, "tags": {"type": "keyword"}, "flag": {"type": "boolean"},
    "obj": {"properties": {"a": {"type": "integer"}, "b": {"type": "keyword"}}},
    "bin": {"type": "binary"}, "raw": {"type": "object", "enabled": False}}}
SOURCE_FORMATS_TWINS = ["sf_json", "sf_smile", "sf_cbor", "sf_yaml"]


def source_formats_doc(rng: random.Random, n: int) -> dict:
    """One logical document; "__f32__<x>" marks a 32-bit float and "bin" a binary value (base64 here)."""
    titles = ["plain title", "Über 漢字 \U0001F600", "tab\there\nnew line \"quoted\" \\ slash /", "\u0001 control"]
    doc = {"n": n, "title": f"{titles[n % len(titles)]} {n}", "price": round(rng.uniform(1, 500), 2),
           "count": 2 ** 40 + n, "tags": [f"t{n % 3}", f"t{n % 5}"], "flag": n % 2 == 0, "nothing": None,
           "obj": {"a": n, "b": [1, 2.5, "x"]},
           "raw": {"big": 123456789012345678901234567890 + n, "neg": -n, "f32": "__f32__1.1", "half": 0.5,
                   "deep": {"list": [[], {}, [n, "s"]], "e": 1e-7}}}
    if n % 3 == 0:
        doc["bin"] = base64.b64encode(bytes(rng.randrange(256) for _ in range(5 + n % 7))).decode()
    return doc


def source_formats_twin(value):
    """The JSON twin of a logical document: markers replaced by their plain values."""
    if isinstance(value, dict):
        return {k: source_formats_twin(v) for k, v in value.items()}
    if isinstance(value, list):
        return [source_formats_twin(v) for v in value]
    if isinstance(value, str) and value.startswith("__f32__"):
        return float(value[len("__f32__"):])
    return value


def source_formats_cbor_head(major: int, value: int) -> bytes:
    if value < 24:
        return bytes([major << 5 | value])
    for info, size in ((24, 1), (25, 2), (26, 4), (27, 8)):
        if value < 1 << (8 * size):
            return bytes([major << 5 | info]) + value.to_bytes(size, "big")
    raise ValueError(value)


def source_formats_cbor_text(text: str) -> bytes:
    data = text.encode()
    return source_formats_cbor_head(3, len(data)) + data


def source_formats_handmade_cbor() -> dict[str, bytes]:
    """CBOR the Jackson generator never writes: half-precision floats and bignum tags."""
    def doc(n: int, raw: list[tuple[str, bytes]]) -> bytes:
        body = source_formats_cbor_head(5, len(raw)) + b"".join(source_formats_cbor_text(k) + v for k, v in raw)
        return (source_formats_cbor_head(5, 2) + source_formats_cbor_text("n") + source_formats_cbor_head(0, n)
                + source_formats_cbor_text("raw") + body)
    two64 = bytes([0xC2, 0x49, 0x01]) + bytes(8)
    neg_two64 = bytes([0xC3, 0x49, 0x01]) + bytes(8)
    return {"nf-cbor-half": doc(3, [("h", bytes([0xF9, 0x3E, 0x00])), ("hnan", bytes([0xF9, 0x7E, 0x00])),
                                    ("hinf", bytes([0xF9, 0x7C, 0x00])), ("hninf", bytes([0xF9, 0xFC, 0x00]))]),
            "nf-cbor-bignum": doc(4, [("p", two64), ("m", neg_two64)])}


SOURCE_FORMATS_YAML_TEXT = {
    "yt-comments": "---\n# leading comment\nn: 1 # trailing comment\nraw:\n  plain: hello world\n"
                   "  quoted: \"12\"\n  single: 'it''s'\n  hex: 0x1F\n  octal: 0o17\n  float: 1.10\n  exp: 1e3\n",
    "yt-anchors": "---\nn: 2\nraw:\n  anchor: &a anchored value\n  alias: *a\n  list: &l [1, 2]\n",
    "yt-scalars": "---\nn: 3\nraw:\n  literal: |\n    line one\n    line two\n  folded: >\n    one\n    two\n"
                  "  keep: |+\n    kept\n\n  strip: |-\n    stripped\n",
    "yt-nulls": "---\nn: 4\nraw:\n  tilde: ~\n  word: null\n  empty:\n  empty_quoted: \"\"\n  flow: {x: [1, ~, two]}\n",
    "yt-words": "---\nn: 5\nraw:\n  yes_word: yes\n  no_word: no\n  on_word: on\n  true_word: true\n"
                "  date: 2024-01-02\n  stamp: 2024-01-02T03:04:05Z\n  unicode: \"\\u00fc \\U0001F600\"\n",
}
SOURCE_FORMATS_YAML_NONFINITE = {
    "yn-1": "---\nn: 1\nraw:\n  nan: .nan\n  inf: .inf\n  ninf: -.inf\n",
    "yn-2": "---\nn: 2\nraw:\n  overflow: 1.0e+400\n  negoverflow: -1.0e+400\n",
}
SOURCE_FORMATS_YAML_NOMARKER = {
    "ym-1": "n: 1\nraw:\n  a: plain\n",
    "ym-2": "n: 2\nraw: {b: [1, 2]}\n",
}

SOURCE_FORMATS_ENCODER_JAVA = r"""
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.cbor.CBORFactory;
import com.fasterxml.jackson.dataformat.cbor.CBORGenerator;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Map;

/** Encodes logical documents (one JSON object per line: out, encoding, doc) into request bodies. */
public final class EncodeSourceFormats {
    static final Map<String, JsonFactory> FACTORIES = Map.of(
        // Elasticsearch's SMILE defaults: header and shared names on, shared values off, raw binary.
        "smile", SmileFactory.builder().disable(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT).build(),
        "smile-shared", SmileFactory.builder().disable(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT)
            .enable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES).build(),
        "cbor-indefinite", CBORFactory.builder().build(),
        "cbor-definite", CBORFactory.builder().build(),
        "cbor-typed", CBORFactory.builder().enable(CBORGenerator.Feature.WRITE_TYPE_HEADER).build(),
        "yaml", YAMLFactory.builder().build());

    public static void main(String[] args) throws Exception {
        var json = new ObjectMapper();
        for (String line : Files.readAllLines(Path.of(args[0]))) {
            JsonNode job = json.readTree(line);
            String encoding = job.get("encoding").asText();
            var out = new ByteArrayOutputStream();
            try (JsonGenerator generator = FACTORIES.get(encoding).createGenerator(out)) {
                write(generator, null, job.get("doc"), encoding.equals("cbor-definite"));
            }
            Files.write(Path.of(job.get("out").asText()), out.toByteArray());
        }
    }

    static void write(JsonGenerator g, String name, JsonNode node, boolean definite) throws Exception {
        if (node.isObject()) {
            if (definite) g.writeStartObject(null, node.size()); else g.writeStartObject();
            var fields = node.fields();
            while (fields.hasNext()) {
                var field = fields.next();
                g.writeFieldName(field.getKey());
                write(g, field.getKey(), field.getValue(), definite);
            }
            g.writeEndObject();
        } else if (node.isArray()) {
            if (definite) g.writeStartArray(null, node.size()); else g.writeStartArray();
            for (JsonNode element : node) write(g, null, element, definite);
            g.writeEndArray();
        } else if (node.isTextual() && "bin".equals(name)) {
            g.writeBinary(Base64.getDecoder().decode(node.asText()));
        } else if (node.isTextual() && node.asText().startsWith("__")) {
            String marker = node.asText();
            switch (marker) {
                case "__dNaN__" -> g.writeNumber(Double.NaN);
                case "__fNaN__" -> g.writeNumber(Float.NaN);
                case "__dInf__" -> g.writeNumber(Double.POSITIVE_INFINITY);
                case "__d-Inf__" -> g.writeNumber(Double.NEGATIVE_INFINITY);
                case "__fInf__" -> g.writeNumber(Float.POSITIVE_INFINITY);
                case "__f-Inf__" -> g.writeNumber(Float.NEGATIVE_INFINITY);
                default -> {
                    if (!marker.startsWith("__f32__")) throw new IllegalArgumentException(marker);
                    g.writeNumber(Float.parseFloat(marker.substring("__f32__".length())));
                }
            }
        } else if (node.isTextual()) {
            g.writeString(node.asText());
        } else if (node.isBigInteger()) {
            g.writeNumber(node.bigIntegerValue());
        } else if (node.isLong()) {
            g.writeNumber(node.longValue());
        } else if (node.isInt()) {
            g.writeNumber(node.intValue());
        } else if (node.isDouble()) {
            g.writeNumber(node.doubleValue());
        } else if (node.isBoolean()) {
            g.writeBoolean(node.booleanValue());
        } else if (node.isNull()) {
            g.writeNull();
        } else {
            throw new IllegalArgumentException(node.getNodeType().toString());
        }
    }
}
"""


def source_formats_request(method: str, path: str, body: bytes | None = None, content_type: str = "application/json",
                           ok=(200, 201)) -> requests.Response:
    port = SOURCE_FORMATS_CONTAINER[2]
    r = requests.request(method, f"http://localhost:{port}{path}", data=body, timeout=180,
                         headers={"Content-Type": content_type, "Accept": "application/json"})
    if ok and r.status_code not in ok:
        raise RuntimeError(f"{method} {path} failed: {r.status_code} {r.text[:500]}")
    return r


def source_formats_json(method: str, path: str, body=None) -> dict:
    data = None if body is None else json.dumps(body).encode()
    r = source_formats_request(method, path, data)
    return r.json() if r.text else {}


def source_formats_start(repo: pathlib.Path) -> None:
    name, image, port = SOURCE_FORMATS_CONTAINER
    run(["docker", "rm", "-f", name], check=False)
    run(["docker", "run", "-d", "--name", name, "-p", f"{port}:9200",
         "-e", "discovery.type=single-node", "-e", "xpack.security.enabled=false",
         "-e", "ES_JAVA_OPTS=-Xms512m -Xmx512m", "-e", "ingest.geoip.downloader.enabled=false",
         "-e", "path.repo=/repo", "-v", f"{repo}:/repo", image])
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            health = requests.get(f"http://localhost:{port}/_cluster/health", timeout=5)
            if health.status_code == 200 and health.json().get("status") in ("green", "yellow"):
                return
        except requests.ConnectionError:
            pass
        time.sleep(2)
    raise TimeoutError(f"{name} did not become healthy")


def source_formats_encode(cli_jar: str, work: pathlib.Path, jobs: list[tuple[str, str, dict]]) -> dict[str, bytes]:
    """Encodes (key, encoding, doc) jobs with the jar's Jackson factories; returns key -> body."""
    source = work / "EncodeSourceFormats.java"
    source.write_text(SOURCE_FORMATS_ENCODER_JAVA)
    classes = work / "encoder-classes"
    run(["javac", "-nowarn", "-cp", cli_jar, "-d", str(classes), str(source)])
    bodies_dir = work / "bodies"
    bodies_dir.mkdir()
    lines = []
    for i, (key, encoding, doc) in enumerate(jobs):
        lines.append(json.dumps({"out": str(bodies_dir / f"{i}.bin"), "encoding": encoding, "doc": doc}))
    (work / "jobs.jsonl").write_text("\n".join(lines) + "\n")
    run(["java", "-cp", f"{classes}:{cli_jar}", "EncodeSourceFormats", str(work / "jobs.jsonl")], timeout=300)
    return {key: (bodies_dir / f"{i}.bin").read_bytes() for i, (key, _, _) in enumerate(jobs)}


def source_formats(cli_jar: str) -> None:
    for existing in (SOURCE_FORMATS_REPO, SOURCE_FORMATS_EXPECTED):
        if existing.exists():
            raise RuntimeError(f"{existing} exists; move it away before regenerating")
    rng = random.Random(9_3_0)
    ids = [f"sf-{n:02d}" for n in range(40)]
    routing = {doc_id: f"r{n % 3}" for n, doc_id in enumerate(ids) if n % 7 == 3}
    logical = {doc_id: source_formats_doc(rng, n) for n, doc_id in enumerate(ids)}
    updated = {"sf-05": source_formats_doc(rng, 105)}
    deleted = ["sf-07"]
    cbor_encodings = ["cbor-indefinite", "cbor-definite", "cbor-typed"]
    jobs = []
    for doc_id, doc in list(logical.items()) + [(k + "#update", v) for k, v in updated.items()]:
        n = int(doc_id[3:5])
        jobs.append((f"sf_smile/{doc_id}", "smile-shared" if n % 4 == 0 else "smile", doc))
        jobs.append((f"sf_cbor/{doc_id}", cbor_encodings[n % 3], doc))
        jobs.append((f"sf_yaml/{doc_id}", "yaml", doc))
    nonfinite = {"n": 1, "raw": {"dnan": "__dNaN__", "fnan": "__fNaN__", "dinf": "__dInf__", "dninf": "__d-Inf__",
                                 "finf": "__fInf__", "fninf": "__f-Inf__", "ok": 1.25}}
    jobs.append(("sf_nonfinite/nf-smile", "smile", nonfinite))
    jobs.append(("sf_nonfinite/nf-cbor", "cbor-indefinite", dict(nonfinite, n=2)))

    expected = {"_comment": "Generated by scripts/generate_test_snapshots.py source-formats against "
                            "gefrierschrank-es9:latest. format/encoding: how each request body was encoded; "
                            "body: the JSON twin's request body (sf_json only); get_status/get_source: "
                            "GET /<index>/_source/<id> with Accept: application/json, raw.",
                "indices": {}}
    with tempfile.TemporaryDirectory(prefix="gs-source-formats-") as work:
        work = pathlib.Path(work)
        bodies = source_formats_encode(cli_jar, work, jobs)
        bodies.update({f"sf_nonfinite/{k}": v for k, v in source_formats_handmade_cbor().items()})
        repo = work / "repo"
        repo.mkdir()
        repo.chmod(0o777)
        try:
            source_formats_start(repo)
            version = requests.get(f"http://localhost:{SOURCE_FORMATS_CONTAINER[2]}/", timeout=10).json()["version"]["number"]
            expected["elasticsearch"] = version
            indices = SOURCE_FORMATS_TWINS + ["sf_nonfinite", "sf_yaml_text", "sf_yaml_nonfinite", "sf_yaml_nomarker"]
            for index in indices:
                mapping = SOURCE_FORMATS_MAPPING if index in SOURCE_FORMATS_TWINS else {
                    "properties": {"n": {"type": "integer"}, "raw": {"type": "object", "enabled": False}}}
                source_formats_json("PUT", "/" + index, {
                    "settings": {"number_of_shards": 1, "number_of_replicas": 0}, "mappings": mapping})
                expected["indices"][index] = {}

            def put(index: str, doc_id: str, fmt: str, body: bytes, encoding: str) -> None:
                query = f"?routing={routing[doc_id]}" if doc_id in routing else ""
                source_formats_request("PUT", f"/{index}/_doc/{requests.utils.quote(doc_id)}{query}", body,
                                       SOURCE_FORMATS_CONTENT_TYPES[fmt])
                entry = {"routing": routing.get(doc_id), "format": fmt, "encoding": encoding}
                if fmt == "json":
                    entry["body"] = body.decode()
                expected["indices"][index][doc_id] = entry

            def twin_body(doc: dict) -> bytes:
                return json.dumps(source_formats_twin(doc), ensure_ascii=False).encode()

            for batch in (ids[:20], ids[20:]):
                for doc_id in batch:
                    n = int(doc_id[3:5])
                    put("sf_json", doc_id, "json", twin_body(logical[doc_id]), "json")
                    put("sf_smile", doc_id, "smile", bodies[f"sf_smile/{doc_id}"], "smile-shared" if n % 4 == 0 else "smile")
                    put("sf_cbor", doc_id, "cbor", bodies[f"sf_cbor/{doc_id}"], cbor_encodings[n % 3])
                    put("sf_yaml", doc_id, "yaml", bodies[f"sf_yaml/{doc_id}"], "yaml")
                for index in SOURCE_FORMATS_TWINS:
                    source_formats_request("POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")
            for doc_id, doc in updated.items():
                n = int(doc_id[3:5])
                put("sf_json", doc_id, "json", twin_body(doc), "json")
                put("sf_smile", doc_id, "smile", bodies[f"sf_smile/{doc_id}#update"], "smile-shared" if n % 4 == 0 else "smile")
                put("sf_cbor", doc_id, "cbor", bodies[f"sf_cbor/{doc_id}#update"], cbor_encodings[n % 3])
                put("sf_yaml", doc_id, "yaml", bodies[f"sf_yaml/{doc_id}#update"], "yaml")
            for doc_id in deleted:
                query = f"?routing={routing[doc_id]}" if doc_id in routing else ""
                for index in SOURCE_FORMATS_TWINS:
                    source_formats_request("DELETE", f"/{index}/_doc/{doc_id}{query}")
                    del expected["indices"][index][doc_id]
            for doc_id in ("nf-smile", "nf-cbor", "nf-cbor-half", "nf-cbor-bignum"):
                encoding = {"nf-smile": "smile", "nf-cbor": "cbor-indefinite"}.get(doc_id, "handmade")
                put("sf_nonfinite", doc_id, "smile" if doc_id == "nf-smile" else "cbor", bodies[f"sf_nonfinite/{doc_id}"],
                    encoding)
            for index, texts in (("sf_yaml_text", SOURCE_FORMATS_YAML_TEXT), ("sf_yaml_nonfinite", SOURCE_FORMATS_YAML_NONFINITE),
                                 ("sf_yaml_nomarker", SOURCE_FORMATS_YAML_NOMARKER)):
                for doc_id, text in texts.items():
                    put(index, doc_id, "yaml", text.encode(), "text")
                    expected["indices"][index][doc_id]["body"] = text
            for index in indices:
                source_formats_request("POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")
                source_formats_request("POST", f"/{index}/_refresh")
                for doc_id, entry in expected["indices"][index].items():
                    query = f"?routing={entry['routing']}" if entry["routing"] else ""
                    r = source_formats_request("GET", f"/{index}/_source/{requests.utils.quote(doc_id)}{query}", ok=None)
                    entry["get_status"] = r.status_code
                    if r.status_code == 200:
                        entry["get_source"] = r.content.decode("utf-8")
                    else:
                        entry["get_error_type"] = r.json().get("error", {}).get("type") if r.text else None
                count = source_formats_json("GET", f"/{index}/_count")["count"]
                if count != len(expected["indices"][index]):
                    raise RuntimeError(f"{index}: {count} documents, expected {len(expected['indices'][index])}")
            source_formats_json("PUT", "/_snapshot/sf", {"type": "fs", "settings": {"location": "/repo", "compress": True}})
            info = source_formats_json("PUT", "/_snapshot/sf/sf_snapshot?wait_for_completion=true",
                                       {"indices": ",".join(indices), "include_global_state": False})["snapshot"]
            if info.get("state") != "SUCCESS":
                raise RuntimeError(f"snapshot failed: {info}")
        finally:
            run(["docker", "rm", "-f", SOURCE_FORMATS_CONTAINER[0]], check=False)
        shutil.copytree(repo, SOURCE_FORMATS_REPO)
    SOURCE_FORMATS_EXPECTED.write_text(json.dumps(expected, indent=1, ensure_ascii=False, sort_keys=True) + "\n")
    for index, docs in expected["indices"].items():
        statuses = sorted({entry["get_status"] for entry in docs.values()})
        print(f"{index}: {len(docs)} live documents, GET statuses {statuses}")
    print(f"Wrote {SOURCE_FORMATS_REPO} and {SOURCE_FORMATS_EXPECTED}")


if __name__ == "__main__":
    if sys.argv[1:] == ["es7-family"]:
        es7_family()
        sys.exit(0)
    if sys.argv[1:] == ["es7-best-compression"]:
        es7_best_compression()
        sys.exit(0)
    if sys.argv[1:2] == ["es7-goldens"] and len(sys.argv) == 4:
        es7_goldens(sys.argv[2], sys.argv[3])
        sys.exit(0)
    if sys.argv[1:2] == ["source-formats"] and len(sys.argv) == 3:
        source_formats(sys.argv[2])
        sys.exit(0)
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        print("  ES containers may still be running. Clean up with:")
        print("  docker compose --profile es7 --profile es8 --profile es9 down")
        sys.exit(1)
