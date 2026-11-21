"""Generate small REAL snapshot-repository fixtures with synthetic _source.

Three fixtures, selected by the first argument:

zstd (default)
    zstd-synth-repo: shards using ES's zstd stored fields (Zstd814StoredFieldsFormat,
    BEST_COMPRESSION) together with synthetic _source. Regression fixture for the in-process
    NullPointerException in org.elasticsearch.index.codec.zstd.ZstdCompressionMode$ZstdDecompressor
    .decompress (ES's native zstd binding is unavailable outside a real node). Generated with ES 8.17.

deletes
    synth-deletes-repo: synthetic-source indices in LZ4 (codec default) and ZSTD (best_compression)
    variants, each with several unmerged segments of more than 10 docs, about 10% of the docs deleted
    by id and some updated (the soft-deleted older versions stay in their segments), a keyword over
    ignore_above in some docs, a sparse object, non-BMP characters and one empty document. A third
    index adds a nested field. Generated with ES 9.3; ES's own _search output is the expected JSON.

large
    A larger variant of the deletes corpus for timing (logsdb/ZSTD and time_series/LZ4). Written to
    --output (for example under a module's target/ directory), never committed.

Uses a local ES and the local MinIO service only: registers a uniquely named repository under a
unique base_path in the "snapshots" bucket, creates only this script's own indices (refusing to run
if they already exist), and removes the indices, snapshot and repository registration afterwards.
Requires an active trial license (started automatically).

ES is reached at $ES_URL (default http://localhost:9200, the docker compose es8/es9 profiles). With
--container IMAGE the script instead starts IMAGE as a throwaway single-node container named
gs-synth-<suffix> on --port (default 9230) in the compose network, so ES can reach minio:9000, and
removes it afterwards. Examples:

    python3 scripts/generate_zstd_synth_fixture.py
    python3 scripts/generate_zstd_synth_fixture.py deletes --container gefrierschrank-es9
    python3 scripts/generate_zstd_synth_fixture.py large --container gefrierschrank-es9 \\
        --output gefrierschrank-synthetic-source/target/synth-large-repo --docs 20000
"""
import argparse
import json
import os
import pathlib
import random
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from generate_frozen_fixture import s3_get  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "gefrierschrank-synthetic-source/src/test/resources/fixtures"
OUTPUT = FIXTURES / "zstd-synth-repo"
DELETES_OUTPUT = FIXTURES / "synth-deletes-repo"
DELETES_EXPECTED = FIXTURES / "synth-deletes-expected.json"
BUCKET = "snapshots"
SYNTH_INDEX = "test_zstd_synth"
LOGSDB_INDEX = "test_zstd_logsdb"
ZSTD_ATTRIBUTE = "Zstd814StoredFieldsFormat.mode"
COMPOSE_NETWORK = "gefrierschrank_default"

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")

WORDS = ("request served upstream latency cache miss backend timeout retry client connection pool "
         "gateway handshake payload checksum shard replica segment merge flush refresh compress "
         "decompress block stored field codec elastic lucene snapshot repository frozen tier").split()
HOSTS = [f"web-{i:02d}.example.internal" for i in range(1, 6)]
METHODS = ["GET", "POST", "PUT", "DELETE", "HEAD"]

SYNTH_MAPPINGS = {"properties": {
    "host": {"type": "keyword"},
    "bytes": {"type": "long"},
    "@timestamp": {"type": "date"},
    # No keyword sub-field: synthetic source must keep the original text in stored fields.
    "message": {"type": "text"},
    "price": {"type": "scaled_float", "scaling_factor": 100},
    "http": {"properties": {"method": {"type": "keyword"}}},
}}
SYNTH_SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0,
                  "index.codec": "best_compression"}

LOGSDB_MAPPINGS = {"properties": {
    "@timestamp": {"type": "date"},
    "host": {"properties": {"name": {"type": "keyword"}}},
    "message": {"type": "text"},
    "status": {"type": "long"},
}}
LOGSDB_SETTINGS = {"number_of_shards": 1, "number_of_replicas": 0, "index.mode": "logsdb",
                   "index.codec": "best_compression"}

DELETES_MAPPINGS = {"properties": {
    "@timestamp": {"type": "date"},
    "host": {"type": "keyword"},
    "status": {"type": "long"},
    "bytes": {"type": "long"},
    # Values longer than ignore_above are not indexed; synthetic source reads them back from
    # stored fields instead of doc values.
    "tag": {"type": "keyword", "ignore_above": 16},
    "tags": {"type": "keyword"},
    "note": {"type": "keyword"},
    "message": {"type": "text"},
    "labels": {"properties": {"env": {"type": "keyword"}, "zone": {"type": "keyword"},
                              "rack": {"type": "long"}}},
}}
NESTED_MAPPINGS = {"properties": {
    "@timestamp": {"type": "date"},
    "host": {"type": "keyword"},
    "status": {"type": "long"},
    "items": {"type": "nested", "properties": {"name": {"type": "keyword"}, "qty": {"type": "long"}}},
}}
# Keeps the merge policy away from these few small segments, so every flushed batch stays its own
# segment and the soft-deleted older versions of updated documents stay where they were written.
NO_MERGES = {"index.merge.policy.segments_per_tier": 100, "index.merge.policy.deletes_pct_allowed": 50}
DELETES_SETTINGS = dict(NO_MERGES, number_of_shards=1, number_of_replicas=0,
                        **{"index.mapping.source.mode": "synthetic"})
DELETES_INDICES = {
    "test_deletes_lz4": (dict(DELETES_SETTINGS, **{"index.codec": "default"}), DELETES_MAPPINGS),
    "test_deletes_zstd": (dict(DELETES_SETTINGS, **{"index.codec": "best_compression"}), DELETES_MAPPINGS),
    "test_deletes_nested": (dict(DELETES_SETTINGS, **{"index.codec": "default"}), NESTED_MAPPINGS),
}
NOTES = ["café crème", "\U0001F600 grinning", "日本語", "rocket \U0001F680\U0001F680",
         "plain"]


def es(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(ES_URL + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=300) as response:
        return json.load(response)


def es_try(method, path, body=None):
    """Like es() but returns (status, json) instead of raising on HTTP errors."""
    try:
        return 200, es(method, path, body)
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read() or b"{}")


def message(rng, i, words=(45, 70)):
    return f"event {i:03d}: " + " ".join(rng.choice(WORDS) for _ in range(rng.randint(*words))) + "."


def synth_doc(rng, i):
    return {
        "host": HOSTS[i % len(HOSTS)],
        "bytes": rng.randint(0, 5_000_000) * (1 + i % 3),
        "@timestamp": f"2026-03-{1 + i % 28:02d}T{i % 24:02d}:{(i * 7) % 60:02d}:{(i * 13) % 60:02d}.{i:03d}Z",
        "message": message(rng, i),
        "price": round(rng.uniform(0, 1000), 2),
        "http": {"method": METHODS[i % len(METHODS)]},
    }


def logsdb_doc(rng, i):
    return {
        "@timestamp": f"2026-04-{1 + i % 28:02d}T{i % 24:02d}:{(i * 11) % 60:02d}:00Z",
        "host": {"name": HOSTS[i % 3]},
        "message": message(rng, i),
        "status": [200, 201, 404, 500, 503][i % 5],
    }


def deletes_doc(rng, i, version=0, words=(8, 24)):
    if i == 17:
        return {}
    doc = {
        "@timestamp": f"2026-05-{1 + i % 28:02d}T{i % 24:02d}:{(i * 7) % 60:02d}:{(i * 13) % 60:02d}Z",
        "host": HOSTS[i % len(HOSTS)],
        "status": [200, 201, 404, 500, 503][(i + version) % 5],
        "bytes": rng.randint(0, 5_000_000),
        "message": message(rng, i, words),
    }
    if i % 3 == 0:
        doc["tag"] = f"t{i}"
    elif i % 3 == 1:
        doc["tag"] = f"\U0001F680 over-the-limit-tag-{i:04d}"
    if i % 5 == 0:
        doc["tags"] = [METHODS[i % 5], METHODS[(i + 2) % 5]]
    if i % 4 == 0:
        doc["labels"] = {"env": ["prod", "stage"][i % 2], "zone": f"z{i % 3}", "rack": i % 7}
    elif i % 11 == 0:
        doc["labels"] = {"zone": f"z{i % 3}"}
    if i % 7 == 0:
        doc["note"] = NOTES[(i // 7) % len(NOTES)]
    if version:
        doc["labels"] = dict(doc.get("labels", {}), env="updated")
    return doc


def nested_doc(rng, i):
    return {
        "@timestamp": f"2026-06-{1 + i % 28:02d}T{i % 24:02d}:00:00Z",
        "host": HOSTS[i % len(HOSTS)],
        "status": [200, 404, 500][i % 3],
        "items": [{"name": f"item-{i}-{j}", "qty": rng.randint(1, 9)} for j in range(1 + i % 3)],
    }


def bulk(index, docs):
    lines = []
    for doc_id, doc in docs:
        lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}))
        lines.append(json.dumps(doc))
    data = ("\n".join(lines) + "\n").encode()
    request = urllib.request.Request(ES_URL + "/_bulk?refresh=true", data=data, method="POST",
                                     headers={"Content-Type": "application/x-ndjson"})
    with urllib.request.urlopen(request, timeout=300) as response:
        result = json.load(response)
    if result.get("errors"):
        raise RuntimeError(f"bulk errors: {[i for i in result['items'] if 'error' in i['index']][:2]}")


def flush(index):
    es("POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")


def create_index(index, settings, mappings, batches):
    """Index each batch then flush, so every batch becomes its own committed segment."""
    status, body = es_try("PUT", "/" + index, {"settings": settings, "mappings": mappings})
    if status != 200:
        raise RuntimeError(f"create {index} failed: {status} {body}")
    for batch in batches:
        bulk(index, batch)
        flush(index)


def create_synth_index(rng):
    """Prefer the index.mapping.source.mode setting; fall back to the _source.mode mapping."""
    docs = [(f"doc-{i:03d}", synth_doc(rng, i)) for i in range(1, 41)]
    batches = [docs[:20], docs[20:]]
    try:
        create_index(SYNTH_INDEX, dict(SYNTH_SETTINGS, **{"index.mapping.source.mode": "synthetic"}),
                     SYNTH_MAPPINGS, batches)
        return "index.mapping.source.mode setting"
    except RuntimeError as error:
        print(f"  index.mapping.source.mode rejected: {str(error)[:300]}")
        es_try("DELETE", "/" + SYNTH_INDEX)
    create_index(SYNTH_INDEX, SYNTH_SETTINGS, dict(SYNTH_MAPPINGS, _source={"mode": "synthetic"}), batches)
    return "_source.mode mapping"


def segments(index):
    # ES 8.x removed ?verbose=true (400 unrecognized parameter); attributes are always returned.
    shards = es("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"]
    result = {}
    for copy in shards:
        for name, seg in copy["segments"].items():
            result[name] = {k: seg.get(k) for k in ("generation", "num_docs", "deleted_docs", "size_in_bytes",
                                                     "committed", "search", "compound", "version", "attributes")}
    return result


def expected_sources(index):
    hits = es("GET", f"/{index}/_search?size=10000")["hits"]["hits"]
    return {hit["_id"]: hit["_source"] for hit in sorted(hits, key=lambda h: h["_id"])}


def index_report(index):
    effective = es("GET", f"/{index}/_settings?include_defaults=true&flat_settings=true")[index]
    return {
        "doc_count": es("GET", f"/{index}/_count")["count"], "segments": segments(index),
        "settings": {k: v for part in (effective["settings"], effective["defaults"])
                     for k, v in part.items()
                     if k.startswith(("index.codec", "index.mode", "index.mapping.source",
                                      "index.sort", "index.number_of", "index.merge.policy"))},
        "mapping": es("GET", f"/{index}/_mapping")[index]["mappings"]}


def download(prefix, output):
    ns = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
    total = 0
    token = None
    while True:
        params = {"list-type": "2", "prefix": prefix + "/"}
        if token:
            params["continuation-token"] = token
        query = urllib.parse.urlencode(sorted(params.items()), quote_via=urllib.parse.quote)
        listing = ET.fromstring(s3_get("/" + BUCKET, query))
        for item in listing.findall("s:Contents", ns):
            key = item.findtext("s:Key", namespaces=ns)
            target = output / key.removeprefix(prefix + "/")
            target.parent.mkdir(parents=True, exist_ok=True)
            blob = s3_get(f"/{BUCKET}/" + urllib.parse.quote(key, safe="/"))
            target.write_bytes(blob)
            total += len(blob)
        if listing.findtext("s:IsTruncated", namespaces=ns) != "true":
            return total
        token = listing.findtext("s:NextContinuationToken", namespaces=ns)


def ensure_trial_license():
    # A fresh node answers 404 until it has generated its self-issued basic license.
    for _ in range(60):
        status, body = es_try("GET", "/_license")
        if status == 200:
            break
        time.sleep(1)
    license_type = body["license"]
    if license_type["status"] != "active" or license_type["type"] not in ("trial", "platinum", "enterprise"):
        es("POST", "/_license/start_trial?acknowledge=true")


def refuse_existing(indices):
    for index in indices:
        if es_try("GET", "/" + index)[0] == 200:
            raise RuntimeError(f"index {index} already exists; refusing to touch it")


def snapshot_and_download(indices, name, output):
    """Snapshots `indices` into a fresh repository, downloads the repository and cleans up ES."""
    suffix = uuid.uuid4().hex[:8]
    repository = f"{name}-fixture-{suffix}"
    prefix = f"{name}-fixture/{suffix}"
    es("PUT", "/_snapshot/" + repository, {"type": "s3", "settings": {
        "bucket": BUCKET, "base_path": prefix, "endpoint": "http://minio:9000",
        "protocol": "http", "path_style_access": True}})
    try:
        snapshot = es("PUT", f"/_snapshot/{repository}/{name}?wait_for_completion=true",
                      {"indices": ",".join(indices), "include_global_state": False})["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot}")
        size = download(prefix, output)
        print(f"Wrote {output} ({size} bytes, repository {repository}, base_path {prefix})")
        es_try("DELETE", f"/_snapshot/{repository}/{name}")
    finally:
        es_try("DELETE", "/_snapshot/" + repository)


def generate_zstd():
    if OUTPUT.exists():
        raise RuntimeError(f"{OUTPUT} already exists; move it away first")
    refuse_existing((SYNTH_INDEX, LOGSDB_INDEX))
    ensure_trial_license()

    suffix = uuid.uuid4().hex[:8]
    repository = "zstd-synth-fixture-" + suffix
    prefix = "zstd-synth-fixture/" + suffix
    rng = random.Random(8170)
    created = []
    try:
        es("PUT", "/_snapshot/" + repository, {"type": "s3", "settings": {
            "bucket": BUCKET, "base_path": prefix, "endpoint": "http://minio:9000",
            "protocol": "http", "path_style_access": True}})
        created.append(SYNTH_INDEX)
        source_mode = create_synth_index(rng)
        indices = [SYNTH_INDEX]
        try:
            docs = [(f"log-{i:03d}", logsdb_doc(rng, i)) for i in range(1, 21)]
            created.append(LOGSDB_INDEX)
            create_index(LOGSDB_INDEX, LOGSDB_SETTINGS, LOGSDB_MAPPINGS, [docs[:10], docs[10:]])
            indices.append(LOGSDB_INDEX)
        except Exception as error:  # optional index: skip, don't fail the whole fixture
            print(f"  ! skipping {LOGSDB_INDEX}: {error}")
            es_try("DELETE", "/" + LOGSDB_INDEX)
            created.remove(LOGSDB_INDEX)

        report = {"elasticsearch_version": es("GET", "/")["version"]["number"],
                  "synthetic_source_via": source_mode, "indices": {}}
        for index in indices:
            segs = segments(index)
            modes = {name: (seg["attributes"] or {}).get(ZSTD_ATTRIBUTE) for name, seg in segs.items()}
            if not all(mode == "BEST_COMPRESSION" for mode in modes.values()):
                raise RuntimeError(f"{index} segments are not zstd BEST_COMPRESSION: {segs}")
            report["indices"][index] = index_report(index)
        expected = {index: expected_sources(index) for index in indices}

        snapshot = es("PUT", f"/_snapshot/{repository}/zstd-synth?wait_for_completion=true",
                      {"indices": ",".join(indices), "include_global_state": False})["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot}")
        size = download(prefix, OUTPUT)
        (FIXTURES / "zstd-synth-expected.json").write_text(json.dumps(expected[SYNTH_INDEX], indent=2) + "\n")
        if LOGSDB_INDEX in expected:
            (FIXTURES / "zstd-logsdb-expected.json").write_text(
                json.dumps(expected[LOGSDB_INDEX], indent=2) + "\n")
        print(json.dumps(report, indent=2))
        print(f"Wrote {OUTPUT} ({size} bytes, repository {repository}, base_path {prefix})")
        es_try("DELETE", f"/_snapshot/{repository}/zstd-synth")
    finally:
        for index in created:
            es_try("DELETE", "/" + index)
        es_try("DELETE", "/_snapshot/" + repository)


def build_deletes_index(index, settings, mappings, doc_count, batches, rng, doc_fn, words=(8, 24)):
    """Batches of fresh docs (one segment each), then ~10% deletes and some updates (one more segment)."""
    ids = [f"d-{i:05d}" for i in range(doc_count)]
    docs = [(ids[i], doc_fn(rng, i) if doc_fn is nested_doc else doc_fn(rng, i, 0, words))
            for i in range(doc_count)]
    size = -(-doc_count // batches)
    create_index(index, settings, mappings, [docs[start:start + size] for start in range(0, doc_count, size)])
    deleted = [ids[i] for i in range(3, doc_count, 10)]
    updated = [ids[i] for i in range(6, doc_count, 15) if ids[i] not in deleted]
    if doc_fn is nested_doc:
        bulk(index, [(doc_id, nested_doc(rng, doc_count + n)) for n, doc_id in enumerate(updated)])
    else:
        bulk(index, [(doc_id, deletes_doc(rng, int(doc_id[2:]), 1, words)) for doc_id in updated])
    for doc_id in deleted:
        es("DELETE", f"/{index}/_doc/{doc_id}")
    flush(index)
    # Flushing commits the deletes but does not refresh the searcher _search and _segments read.
    es("POST", f"/{index}/_refresh")
    return deleted, updated


def check_layout(index, min_segments, zstd):
    segs = segments(index)
    big = [s for s in segs.values() if s["num_docs"] + s["deleted_docs"] > 10]
    if len(big) < min_segments or not any(s["deleted_docs"] for s in segs.values()):
        raise RuntimeError(f"{index}: expected >= {min_segments} segments of > 10 docs with deletes: {segs}")
    modes = {(s["attributes"] or {}).get(ZSTD_ATTRIBUTE) for s in segs.values()}
    if zstd and modes != {"BEST_COMPRESSION"}:
        raise RuntimeError(f"{index} segments are not zstd BEST_COMPRESSION: {segs}")
    if not zstd and modes != {None}:
        raise RuntimeError(f"{index} segments unexpectedly use zstd: {segs}")


def generate_deletes():
    if DELETES_OUTPUT.exists():
        raise RuntimeError(f"{DELETES_OUTPUT} already exists; move it away first")
    refuse_existing(DELETES_INDICES)
    ensure_trial_license()
    created = []
    try:
        expected = {}
        report = {"elasticsearch_version": es("GET", "/")["version"]["number"], "indices": {}}
        for index, (settings, mappings) in DELETES_INDICES.items():
            created.append(index)
            nested = "nested" in index
            rng = random.Random(9300)
            deleted, updated = build_deletes_index(index, settings, mappings, 60 if nested else 120,
                                                   3 if nested else 4, rng,
                                                   nested_doc if nested else deletes_doc)
            check_layout(index, 3, settings["index.codec"] == "best_compression")
            report["indices"][index] = index_report(index)
            expected[index] = {"deleted_ids": deleted, "updated_ids": updated,
                               "sources": expected_sources(index)}
        snapshot_and_download(list(DELETES_INDICES), "synth-deletes", DELETES_OUTPUT)
        DELETES_EXPECTED.write_text(json.dumps(expected, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        for index in created:
            es_try("DELETE", "/" + index)


def generate_large(output, doc_count):
    output = pathlib.Path(output).resolve()
    if output.exists():
        raise RuntimeError(f"{output} already exists; move it away first")
    # No ignore_above here: logsdb/time_series keep over-long values in zstd-compressed binary doc
    # values, which the isolated reconstruction cannot decompress yet (ES native access).
    properties = dict(DELETES_MAPPINGS["properties"], tag={"type": "keyword"})
    indices = {
        "test_large_logsdb": ({"number_of_shards": 1, "number_of_replicas": 0, "index.mode": "logsdb",
                               "index.codec": "best_compression", **NO_MERGES},
                              {"properties": properties}),
        "test_large_tsdb": ({"number_of_shards": 1, "number_of_replicas": 0, "index.mode": "time_series",
                             "index.routing_path": ["host"], "index.codec": "default",
                             "index.time_series.start_time": "2026-05-01T00:00:00Z",
                             "index.time_series.end_time": "2026-06-01T00:00:00Z",
                             **NO_MERGES},
                            {"properties": dict(properties,
                                                host={"type": "keyword", "time_series_dimension": True})}),
    }
    refuse_existing(indices)
    ensure_trial_license()
    created = []
    try:
        for index, (settings, mappings) in indices.items():
            created.append(index)
            rng = random.Random(9301)
            if index == "test_large_tsdb":
                # TSDB rejects deletes by id and needs unique (dimension, @timestamp) pairs.
                docs = []
                for i in range(doc_count):
                    doc = deletes_doc(rng, i, 0, (8, 24)) or {"status": 0}
                    doc["host"] = HOSTS[i % len(HOSTS)]
                    doc["@timestamp"] = f"2026-05-{1 + (i // 86400) % 28:02d}T{(i // 3600) % 24:02d}:{(i // 60) % 60:02d}:{i % 60:02d}Z"
                    docs.append((None, doc))
                status, body = es_try("PUT", "/" + index, {"settings": settings, "mappings": mappings})
                if status != 200:
                    raise RuntimeError(f"create {index} failed: {status} {body}")
                for start in range(0, doc_count, 2000):
                    lines = []
                    for _, doc in docs[start:start + 2000]:
                        lines.append(json.dumps({"create": {"_index": index}}))
                        lines.append(json.dumps(doc))
                    request = urllib.request.Request(ES_URL + "/_bulk", data=("\n".join(lines) + "\n").encode(),
                                                     method="POST", headers={"Content-Type": "application/x-ndjson"})
                    with urllib.request.urlopen(request, timeout=300) as response:
                        if json.load(response).get("errors"):
                            raise RuntimeError(f"bulk errors in {index}")
                    if (start // 2000) % 5 == 4:
                        flush(index)
                flush(index)
            else:
                # logsdb requires @timestamp, so the empty document gets one here.
                build_deletes_index(index, settings, mappings, doc_count, max(3, doc_count // 5000), rng,
                                    lambda r, i, v, w: deletes_doc(r, i, v, w) or {"@timestamp": "2026-05-01T00:00:00Z"},
                                    (45, 70))
            print(index, json.dumps({k: v["num_docs"] for k, v in segments(index).items()}))
        snapshot_and_download(list(indices), "synth-large", output)
    finally:
        for index in created:
            es_try("DELETE", "/" + index)


def wait_for_es(timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if es("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")["status"] in ("yellow", "green"):
                return
        except (urllib.error.URLError, ConnectionError, TimeoutError, json.JSONDecodeError):
            pass
        time.sleep(2)
    raise RuntimeError(f"Elasticsearch at {ES_URL} did not become ready")


def main():
    global ES_URL
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("fixture", nargs="?", default="zstd", choices=("zstd", "deletes", "large"))
    parser.add_argument("--container", help="start this ES image as a throwaway gs-synth-* container")
    parser.add_argument("--port", type=int, default=9230)
    parser.add_argument("--output", help="output repository directory for 'large'")
    parser.add_argument("--docs", type=int, default=20000, help="documents per index for 'large'")
    args = parser.parse_args()
    if args.fixture == "large" and not args.output:
        parser.error("large needs --output")

    container = None
    if args.container:
        container = "gs-synth-" + uuid.uuid4().hex[:8]
        subprocess.run(["docker", "run", "-d", "--rm", "--name", container, "--network", COMPOSE_NETWORK,
                        "-p", f"{args.port}:9200", "-e", "discovery.type=single-node",
                        "-e", "ES_JAVA_OPTS=-Xms1g -Xmx1g", "-e", "xpack.security.enabled=false",
                        args.container], check=True, stdout=subprocess.DEVNULL)
        ES_URL = f"http://localhost:{args.port}"
    try:
        wait_for_es()
        if args.fixture == "zstd":
            generate_zstd()
        elif args.fixture == "deletes":
            generate_deletes()
        else:
            generate_large(args.output, args.docs)
    finally:
        if container:
            subprocess.run(["docker", "rm", "-f", container], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
