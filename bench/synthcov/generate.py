"""Datasets and correctness check of the synthetic-coverage measurements (bench/synthcov/README.md).

  generate  builds, once, into target/synthcov/: an ES 9.3 snapshot repository (repo/) with
            nested-synth   150k roots, 0-5 nested items, synthetic source, one shard
            flat-synth     the same documents, items as a plain object array, synthetic, one shard
            srcoff         150k stored documents on shard 0, then _source.enabled:false, then
                           150k more on shard 1 (rebuilt through the rewritten mapping)
            srcoff-twin-synth  srcoff's second half as native synthetic source, one shard
            and expected/<index>.jsonl.gz: {"_id", "_source"} as Elasticsearch's search returns them
            (srcoff: the exact text sent for its stored half, the twin's rendering for the rest).
            Container gs-synthcov-bench-es93 on port 9262; hold the docker slot:
              $SLOT docker 1 python3 bench/synthcov/generate.py generate
  verify    compares a Delta table with the expected rows: value equality per _id (stored rows
            byte-exact), no missing or extra _id. Needs deltalake:
              uv run --offline --frozen --project bench python bench/synthcov/generate.py verify <table> <index>
"""
import gzip
import json
import pathlib
import random
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
OUT = ROOT / "target/synthcov"
CONTAINER = "gs-synthcov-bench-es93"
PORT = 9262
IMAGE = "gefrierschrank-es9"
ES = f"http://localhost:{PORT}"
ROOTS = 150_000
SERVICES = ["checkout", "search", "auth", "billing", "catalog", "gateway"]
WORDS = "request served backend upstream cache miss retry timeout payload shard segment snapshot".split()


def es(method, path, body=None, timeout=600):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(ES + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def mapping(items_type):
    return {"properties": {
        "@timestamp": {"type": "date"},
        "host": {"properties": {"name": {"type": "keyword"}}},
        "service": {"type": "keyword"},
        "status": {"type": "integer"},
        "bytes": {"type": "long"},
        "latency": {"type": "double"},
        "client": {"type": "ip"},
        "message": {"type": "text", "fields": {"raw": {"type": "keyword", "ignore_above": 8191}}},
        "tags": {"type": "keyword"},
        "items": {"type": items_type, "properties": {
            "name": {"type": "keyword"}, "qty": {"type": "integer"}, "price": {"type": "double"}}},
    }}


def document(rng, i):
    doc = {
        "@timestamp": f"2026-07-{1 + i % 28:02d}T{i % 24:02d}:{(i * 7) % 60:02d}:{(i * 13) % 60:02d}.{i % 1000:03d}Z",
        "host": {"name": f"host-{i % 97:02d}.example.internal"},
        "service": SERVICES[i % len(SERVICES)],
        "status": [200, 200, 200, 201, 404, 500][i % 6],
        "bytes": rng.randint(100, 5_000_000),
        "latency": round(rng.uniform(0.1, 900.0), 3),
        "client": f"10.{i % 256}.{(i // 256) % 256}.{(i * 7) % 256}",
        "message": " ".join(rng.choice(WORDS) for _ in range(8 + i % 20)),
        "tags": sorted({rng.choice(["a", "b", "c", "d"]) for _ in range(1 + i % 3)}),
    }
    count = i % 6
    if count:
        doc["items"] = [{"name": f"item-{rng.randint(0, 999)}", "qty": rng.randint(1, 9), "price": round(rng.uniform(1, 99), 2)}
                        for _ in range(count)]
    return doc


def bulk(index, docs):
    lines = []
    for doc_id, routing, text in docs:
        action = {"_index": index, "_id": doc_id}
        if routing:
            action["routing"] = routing
        lines += [json.dumps({"index": action}), text]
    request = urllib.request.Request(f"{ES}/_bulk", data=("\n".join(lines) + "\n").encode(), method="POST",
                                     headers={"Content-Type": "application/x-ndjson"})
    with urllib.request.urlopen(request, timeout=600) as response:
        result = json.load(response)
    if result.get("errors"):
        raise RuntimeError(f"bulk errors in {index}: {[i for i in result['items'] if 'error' in next(iter(i.values()))][:2]}")


def load(index, docs):
    for start in range(0, len(docs), 2000):
        bulk(index, docs[start:start + 2000])
    es("POST", f"/{index}/_refresh")


def create(index, shards, settings, body):
    es("PUT", "/" + index, {"settings": dict({"number_of_shards": shards, "number_of_replicas": 0,
                                              "refresh_interval": "-1"}, **settings), "mappings": body})


def search_all(index):
    hits = {}
    page = es("POST", f"/{index}/_search?scroll=5m", {"size": 10000, "sort": ["_doc"]})
    while page["hits"]["hits"]:
        for hit in page["hits"]["hits"]:
            hits[hit["_id"]] = hit["_source"]
        page = es("POST", "/_search/scroll", {"scroll": "5m", "scroll_id": page["_scroll_id"]})
    return hits


def write_expected(index, rows):
    path = OUT / "expected" / f"{index}.jsonl.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as out:
        for doc_id in sorted(rows):
            out.write(json.dumps({"_id": doc_id, "_source": rows[doc_id]}, separators=(",", ":")) + "\n")


def generate():
    if OUT.exists():
        raise RuntimeError(f"{OUT} exists; datasets are generated once (remove it to regenerate)")
    if CONTAINER in docker("ps", "-a", "--format", "{{.Names}}").stdout.split():
        raise RuntimeError(f"container {CONTAINER} already exists; remove it first")
    snapshots = OUT / "snapshots"
    snapshots.mkdir(parents=True)
    docker("run", "-d", "--name", CONTAINER, "-p", f"{PORT}:9200", "-e", "discovery.type=single-node",
           "-e", "xpack.security.enabled=false", "-e", "ES_JAVA_OPTS=-Xms2g -Xmx2g", "-e", "path.repo=/snapshots",
           "-v", f"{snapshots}:/snapshots", IMAGE)
    try:
        deadline = time.time() + 240
        while True:
            try:
                if es("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")["status"] in ("yellow", "green"):
                    break
            except (urllib.error.URLError, ConnectionError, OSError):
                pass
            if time.time() > deadline:
                raise RuntimeError("Elasticsearch did not start")
            time.sleep(2)
        # Synthetic source needs an enterprise-level license; without it Elasticsearch silently stores _source.
        for _ in range(60):
            try:
                license_info = es("GET", "/_license")["license"]
                break
            except urllib.error.HTTPError as missing:
                if missing.code != 404:
                    raise
                time.sleep(2)
        else:
            license_info = {"status": "missing", "type": "none"}
        if license_info["status"] != "active" or license_info["type"] not in ("trial", "platinum", "enterprise"):
            es("POST", "/_license/start_trial?acknowledge=true")
        rng = random.Random(150_000)
        texts = [(f"doc-{i:06d}", json.dumps(document(rng, i), separators=(",", ":"))) for i in range(2 * ROOTS)]
        synthetic = {"index.mapping.source.mode": "synthetic"}

        create("nested-synth", 1, synthetic, mapping("nested"))
        load("nested-synth", [(doc_id, None, text) for doc_id, text in texts[:ROOTS]])
        create("flat-synth", 1, synthetic, mapping("object"))
        load("flat-synth", [(doc_id, None, text) for doc_id, text in texts[:ROOTS]])

        create("srcoff", 2, {}, mapping("object"))
        shard_of = {}
        for n in range(200):
            shard_of.setdefault(es("GET", f"/srcoff/_search_shards?routing=r{n}")["shards"][0][0]["shard"], f"r{n}")
            if len(shard_of) == 2:
                break
        load("srcoff", [(doc_id, shard_of[0], text) for doc_id, text in texts[:ROOTS]])
        es("POST", "/srcoff/_flush?force=true")
        es("PUT", "/srcoff/_mapping", {"_source": {"enabled": False}})
        load("srcoff", [(doc_id, shard_of[1], text) for doc_id, text in texts[ROOTS:]])
        create("srcoff-twin-synth", 1, synthetic, mapping("object"))
        load("srcoff-twin-synth", [(doc_id, None, text) for doc_id, text in texts[ROOTS:]])

        indices = ["nested-synth", "flat-synth", "srcoff", "srcoff-twin-synth"]
        for index in ("nested-synth", "flat-synth", "srcoff-twin-synth"):
            mode = es("GET", f"/{index}/_settings?include_defaults=true&flat_settings=true")[index]
            effective = {**mode["defaults"], **mode["settings"]}.get("index.mapping.source.mode", "").lower()
            if effective != "synthetic":
                raise RuntimeError(f"{index} does not use synthetic source ({effective})")
        for index in indices:
            es("POST", f"/{index}/_forcemerge?max_num_segments=1", timeout=1800)
            es("POST", f"/{index}/_flush?force=true")
        write_expected("nested-synth", search_all("nested-synth"))
        write_expected("flat-synth", search_all("flat-synth"))
        twin = search_all("srcoff-twin-synth")
        write_expected("srcoff-twin-synth", twin)
        stored = {doc_id: text for doc_id, text in texts[:ROOTS]}
        write_expected("srcoff", stored | twin)
        es("PUT", "/_snapshot/bench", {"type": "fs", "settings": {"location": "/snapshots/bench"}})
        snapshot = es("PUT", "/_snapshot/bench/synthcov?wait_for_completion=true",
                      {"indices": ",".join(indices), "include_global_state": False}, timeout=1800)["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot}")
        shutil.copytree(snapshots / "bench", OUT / "repo")
        counts = {index: es("GET", f"/{index}/_count")["count"] for index in indices}
        (OUT / "datasets.json").write_text(json.dumps({"elasticsearch": es("GET", "/")["version"]["number"], "counts": counts,
                                                       "routing": shard_of}, indent=1) + "\n")
        print(json.dumps(counts))
    finally:
        docker("rm", "-f", CONTAINER, check=False)
        shutil.rmtree(snapshots, ignore_errors=True)


def verify(table, index):
    from deltalake import DeltaTable
    expected = {}
    with gzip.open(OUT / "expected" / f"{index}.jsonl.gz", "rt", encoding="utf-8") as lines:
        for line in lines:
            row = json.loads(line)
            expected[row["_id"]] = row["_source"]
    actual = DeltaTable(table).to_pyarrow_table(columns=["_id", "_source", "_source_kind"]).to_pylist()
    problems = []
    seen = set()
    for row in actual:
        doc_id = row["_id"]
        if doc_id in seen:
            problems.append(f"duplicate {doc_id}")
        seen.add(doc_id)
        want = expected.get(doc_id)
        if want is None:
            problems.append(f"unexpected {doc_id}")
        elif isinstance(want, str):
            if row["_source_kind"] != "stored" or row["_source"] != want:
                problems.append(f"stored row differs: {doc_id}")
        elif row["_source_kind"] != "synthetic" or json.loads(row["_source"]) != want:
            problems.append(f"reconstructed row differs: {doc_id}")
    missing = set(expected) - seen
    if missing:
        problems.append(f"{len(missing)} missing, e.g. {sorted(missing)[:3]}")
    print(json.dumps({"index": index, "rows": len(actual), "expected": len(expected), "problems": len(problems),
                      "first": problems[:5]}))
    return 1 if problems else 0


if __name__ == "__main__":
    if sys.argv[1:2] == ["generate"]:
        generate()
    elif sys.argv[1:2] == ["verify"] and len(sys.argv) == 4:
        sys.exit(verify(sys.argv[2], sys.argv[3]))
    else:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
