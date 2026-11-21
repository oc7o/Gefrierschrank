"""Vector datasets for the vector-rehydration throughput and heap measurements.

Each dataset is an index pair with identical documents: <name> keeps the vectors out of the
stored _source (ES 9.3 default, rehydrated by 1.2.0) and <name>-included keeps them in
(index.mapping.exclude_source_vectors false, what 1.1.0 can export). Like for like: 1.1.0 exporting
the included twin against 1.2.0 exporting the excluded index.

  vec384       dense_vector float 384 dims cosine, int8_hnsw (ES default), 2 shards
  vec384-bbq   the same with bbq_hnsw
  vec-nested   nested chunks, 3 per document, each a 384-dim float vector and a short text
  vec-sparse   sparse_vector with about 100 tokens per document (ELSER-like)

Every index is force-merged to one segment per shard and snapshotted into an fs repository
(<out>/repo). Container gs-vectors-bench-es9 (image gefrierschrank-es9, ES 9.3.0), port 9242,
removed on exit. Python stdlib only. Run under the docker slot, outside any bench hold:
  $SLOT docker 1 bench/vectors/generate.sh <out> [docs]
"""
import json
import pathlib
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

CONTAINER = "gs-vectors-bench-es9"
PORT = 9242
IMAGE = "gefrierschrank-es9"
ES = f"http://localhost:{PORT}"
SEED = 384
BATCH = 500
WORDS = [f"{a}{b}" for a in ("ka", "lo", "mi", "nu", "pe", "ro", "su", "ti", "va", "ze")
         for b in ("bar", "dex", "fon", "gul", "hir", "jam", "kor", "lun", "mor", "nix", "pol", "qua", "rin", "sol",
                   "tar", "ulm", "vok", "wex", "yar", "zon")]


def es(method, path, body=None, timeout=1800, ndjson=False):
    data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    request = urllib.request.Request(ES + path, data=data, method=method, headers={
        "Content-Type": "application/x-ndjson" if ndjson else "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def vector(rng, dims):
    return [round(rng.uniform(-1, 1), 4) or 0.5 for _ in range(dims)]


def text(rng, words):
    return " ".join(rng.choice(WORDS) for _ in range(words))


def documents(name, docs):
    """(id, document) pairs, deterministic per dataset and document number."""
    for i in range(docs):
        rng = random.Random(f"{SEED}-{name}-{i}")
        doc = {"title": f"{name} {i:07d}", "body": text(rng, 30), "rank": rng.randint(0, 1_000_000)}
        if name == "vec-nested":
            doc["chunks"] = [{"text": text(rng, 12), "vector": vector(rng, 384)} for _ in range(3)]
        elif name == "vec-sparse":
            doc["tokens"] = {w: round(rng.uniform(0.05, 3.0), 3) for w in rng.sample(WORDS, 100)}
        else:
            doc["embedding"] = vector(rng, 384)
        yield f"{i:07d}", doc


def mapping(name):
    properties = {"title": {"type": "keyword"}, "body": {"type": "text"}, "rank": {"type": "long"}}
    if name == "vec384":
        properties["embedding"] = {"type": "dense_vector", "dims": 384, "similarity": "cosine",
                                   "index_options": {"type": "int8_hnsw"}}
    elif name == "vec384-bbq":
        properties["embedding"] = {"type": "dense_vector", "dims": 384, "similarity": "cosine",
                                   "index_options": {"type": "bbq_hnsw"}}
    elif name == "vec-nested":
        properties["chunks"] = {"type": "nested", "properties": {
            "text": {"type": "text"},
            "vector": {"type": "dense_vector", "dims": 384, "similarity": "cosine", "index_options": {"type": "int8_hnsw"}}}}
    elif name == "vec-sparse":
        properties["tokens"] = {"type": "sparse_vector"}
    return {"properties": properties}


def bulk_all(indices, pairs):
    """Sends every batch to each index of the pair, from four threads."""
    batches = []
    batch = []
    for doc_id, doc in pairs:
        batch.append((doc_id, json.dumps(doc, separators=(",", ":"))))
        if len(batch) == BATCH:
            batches.append(batch)
            batch = []
    if batch:
        batches.append(batch)
    pending = list(enumerate(batches))
    lock = threading.Lock()
    failures = []

    def worker():
        while True:
            with lock:
                if not pending or failures:
                    return
                _, chunk = pending.pop()
            for index in indices:
                lines = []
                for doc_id, source in chunk:
                    lines.append(json.dumps({"index": {"_index": index, "_id": doc_id}}))
                    lines.append(source)
                try:
                    result = es("POST", "/_bulk", ("\n".join(lines) + "\n").encode(), ndjson=True)
                    if result.get("errors"):
                        failures.append(f"bulk errors in {index}")
                except (urllib.error.URLError, OSError) as e:
                    failures.append(f"{index}: {type(e).__name__}")

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if failures:
        raise RuntimeError(failures[0])


def main():
    out = pathlib.Path(sys.argv[1]).resolve()
    scale = int(sys.argv[2]) if len(sys.argv) > 2 else 200_000
    datasets = {"vec384": scale, "vec384-bbq": scale, "vec-nested": max(1, scale // 3), "vec-sparse": scale}
    repo = out / "repo"
    if repo.exists():
        raise RuntimeError(f"{repo} already exists; remove it first")
    if docker("ps", "-a", "--format", "{{.Names}}").stdout.split().count(CONTAINER):
        raise RuntimeError(f"container {CONTAINER} already exists; remove it first")
    repo.mkdir(parents=True)
    docker("run", "-d", "--name", CONTAINER, "-p", f"{PORT}:9200", "-e", "discovery.type=single-node",
           "-e", "xpack.security.enabled=false", "-e", "ES_JAVA_OPTS=-Xms3g -Xmx3g", "-e", "path.repo=/snapshots",
           "-e", "cluster.routing.allocation.disk.threshold_enabled=false", "-v", f"{repo}:/snapshots/vectors", IMAGE)
    manifest = {"seed": SEED, "datasets": {}}
    try:
        deadline = time.time() + 240
        while True:
            try:
                if es("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")["status"] in ("yellow", "green"):
                    break
            except (urllib.error.URLError, OSError, ValueError):
                pass
            if time.time() > deadline:
                raise RuntimeError("Elasticsearch did not start")
            time.sleep(2)
        indices = []
        for name, docs in datasets.items():
            pair = [name, name + "-included"]
            for index, exclude in zip(pair, (True, False)):
                es("PUT", "/" + index, {"settings": {"number_of_shards": 2, "number_of_replicas": 0,
                                                     "refresh_interval": "-1",
                                                     "index.mapping.exclude_source_vectors": exclude},
                                        "mappings": mapping(name)})
            started = time.time()
            bulk_all(pair, documents(name, docs))
            for index in pair:
                es("POST", f"/{index}/_refresh")
                es("POST", f"/{index}/_forcemerge?max_num_segments=1", timeout=7200)
                count = es("GET", f"/{index}/_count")["count"]
                if count != docs:
                    raise RuntimeError(f"{index}: {count} documents, expected {docs}")
                stats = es("GET", f"/{index}/_stats/store")["indices"][index]["primaries"]["store"]["size_in_bytes"]
                manifest["datasets"][index] = {"docs": docs, "store_bytes": stats, "excluded": index == name}
            print(f"{name}: {docs} documents per index in {time.time() - started:.0f}s", flush=True)
            indices += pair
        es("PUT", "/_snapshot/vectors", {"type": "fs", "settings": {"location": "/snapshots/vectors"}})
        snapshot = es("PUT", "/_snapshot/vectors/vectors?wait_for_completion=true",
                      {"indices": ",".join(indices), "include_global_state": False})["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot['state']}")
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps(manifest, indent=2))
    finally:
        docker("rm", "-f", CONTAINER, check=False)


if __name__ == "__main__":
    main()
