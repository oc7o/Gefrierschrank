"""Generate the ES 9.3.0 vector-rehydration fixture.

Default step: writes gefrierschrank-synthetic-source/src/test/resources/fixtures/vec-repo plus, per
index, vec-<index>-expected.json (Elasticsearch's own _search fetch with
"_source": {"exclude_vectors": false}, one document per line, each _source as the raw bytes ES
returned) and vec-<index>-input.json (the documents as sent, after updates and deletes).

Indices (one shard each, two flushed segments):
  vec_root           embedding float cosine dims 8, emb_bbq float dims 64 bbq_hnsw, keyword title;
                     a quarter of the documents without vectors, two updates (the old version is
                     soft-deleted in segment 1), two deletes
  vec_root_included  the same documents with index.mapping.exclude_source_vectors=false (twin)
  vec_object         meta.embedding float l2_norm; half the documents have other meta fields, the
                     rest only the vector, and a few send the literal dotted key "meta.embedding"
  vec_nested         nested chunks with chunks.vector (float) and chunks.text; roots with 0, 1 and
                     3 chunks; one root deleted
  vec_byte_bit       b element_type byte l2_norm dims 8; bits element_type bit dims 64, half sent
                     as hex strings
  vec_sparse         tokens sparse_vector with unicode feature names
  vec_noindex        dense_vector index: false: kept in the stored _source (ES 9.3 only hides it
                     from the default fetch), so nothing is rehydrated

Step --restored-es93: restores the existing es93-repo snapshot read-only (bind mount :ro and
"readonly": true) and writes vec-es93-restored-expected.json, Elasticsearch's fetch of every
document of test_es93_vectors_default with the vectors included.

Container gs-vec-es93 (image gefrierschrank-es9, ES 9.3.0) on port 9241, removed on exit. Python
stdlib only. Refuses to overwrite existing output. Hold the docker slot while running:
  $SLOT docker 1 python3 scripts/generate_vector_fixture.py [--restored-es93]
"""
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
OUTPUT = FIXTURES / "vec-repo"
ES93_REPO = FIXTURES / "es93-repo"
RESTORED_EXPECTED = FIXTURES / "vec-es93-restored-expected.json"
WORK = ROOT / "target/vec-fixture-work"
CONTAINER = "gs-vec-es93"
PORT = 9241
IMAGE = "gefrierschrank-es9"
ES = f"http://localhost:{PORT}"
FIRST_9_2_INDEX_VERSION = 9_039_001

ROOT_INDEX = "vec_root"
ROOT_INCLUDED = "vec_root_included"
OBJECT = "vec_object"
NESTED = "vec_nested"
BYTE_BIT = "vec_byte_bit"
SPARSE = "vec_sparse"
NOINDEX = "vec_noindex"
INDICES = [ROOT_INDEX, ROOT_INCLUDED, OBJECT, NESTED, BYTE_BIT, SPARSE, NOINDEX]
ES93_VECTORS_DEFAULT = "test_es93_vectors_default"


def es_raw(method, path, body=None, timeout=120):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(ES + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8")


def es(method, path, body=None, timeout=120):
    return json.loads(es_raw(method, path, body, timeout))


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def start_container(*mounts):
    if docker("ps", "-a", "--format", "{{.Names}}").stdout.split().count(CONTAINER):
        raise RuntimeError(f"container {CONTAINER} already exists; remove it first")
    volumes = []
    for mount in mounts:
        volumes += ["-v", mount]
    docker("run", "-d", "--name", CONTAINER, "-p", f"{PORT}:9200",
           "-e", "discovery.type=single-node", "-e", "xpack.security.enabled=false",
           "-e", "ES_JAVA_OPTS=-Xms1g -Xmx1g", "-e", "path.repo=/snapshots", *volumes, IMAGE)
    deadline = time.time() + 180
    while time.time() < deadline:
        try:
            if es("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")["status"] in ("yellow", "green"):
                return
        except (urllib.error.URLError, ConnectionError, OSError, ValueError):
            pass
        time.sleep(2)
    raise RuntimeError("Elasticsearch did not start")


def bulk(index, operations):
    """operations: (op, id, doc) with op index or delete."""
    lines = []
    for op, doc_id, doc in operations:
        lines.append(json.dumps({op: {"_index": index, "_id": doc_id}}))
        if op != "delete":
            lines.append(json.dumps(doc))
    request = urllib.request.Request(f"{ES}/_bulk?refresh=true", data=("\n".join(lines) + "\n").encode(),
                                     method="POST", headers={"Content-Type": "application/x-ndjson"})
    with urllib.request.urlopen(request, timeout=300) as response:
        result = json.load(response)
    if result.get("errors"):
        failed = [item for item in result["items"] if "error" in next(iter(item.values()))]
        raise RuntimeError(f"bulk errors in {index}: {failed[:2]}")


def create(index, settings, mappings, batches):
    """Each batch becomes its own flushed segment. Returns the live documents as sent."""
    es("PUT", "/" + index, {"settings": dict({"number_of_shards": 1, "number_of_replicas": 0}, **settings),
                            "mappings": mappings})
    live = {}
    for batch in batches:
        bulk(index, batch)
        es("POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")
        for op, doc_id, doc in batch:
            if op == "delete":
                live.pop(doc_id, None)
            else:
                live[doc_id] = doc
    return dict(sorted(live.items()))


def floats(rng, dims):
    return [round(rng.uniform(-1, 1), 3) or 0.5 for _ in range(dims)]


def root_batches(rng):
    docs = {}
    for i in range(32):
        doc = {"title": f"root {i:02d}"}
        if i % 4 != 3:
            doc["embedding"] = floats(rng, 8)
            doc["emb_bbq"] = floats(rng, 64)
        docs[f"r-{i:02d}"] = doc
    first = [("index", doc_id, doc) for doc_id, doc in list(docs.items())[:20]]
    second = [("index", doc_id, doc) for doc_id, doc in list(docs.items())[20:]]
    for doc_id in ("r-01", "r-05"):
        second.append(("index", doc_id, {"title": f"updated {doc_id}", "embedding": floats(rng, 8),
                                         "emb_bbq": floats(rng, 64)}))
    second += [("delete", "r-02", None), ("delete", "r-24", None)]
    return [first, second]


def object_batches(rng):
    first, second = [], []
    for i in range(30):
        vector = floats(rng, 8)
        if i % 10 == 9:
            doc = {"name": f"object {i:02d}", "meta.embedding": vector}
        elif i % 2 == 0:
            doc = {"name": f"object {i:02d}", "meta": {"source": f"src-{i % 3}", "embedding": vector, "rank": i}}
        else:
            doc = {"name": f"object {i:02d}", "meta": {"embedding": vector}}
        (first if i < 18 else second).append(("index", f"o-{i:02d}", doc))
    return [first, second]


def nested_batches(rng):
    first, second = [], []
    for i in range(30):
        chunks = [{"text": f"chunk {i}-{c}", "vector": floats(rng, 8)} for c in range([0, 1, 3][i % 3])]
        doc = {"title": f"nested {i:02d}", "chunks": chunks} if chunks or i % 2 else {"title": f"nested {i:02d}"}
        (first if i < 16 else second).append(("index", f"n-{i:02d}", doc))
    second.append(("delete", "n-04", None))
    return [first, second]


def byte_bit_batches(rng):
    first, second = [], []
    for i in range(30):
        b = [rng.randint(-128, 127) for _ in range(8)]
        bits = rng.randbytes(8)
        doc = {"name": f"bytes {i:02d}", "b": b,
               "bits": bits.hex() if i % 2 else [x - 256 if x > 127 else x for x in bits]}
        (first if i < 15 else second).append(("index", f"b-{i:02d}", doc))
    return [first, second]


def sparse_batches(rng):
    words = ["alpha", "beta", "gamma", "delta", "épée", "größe", "日本", "naïve", "zeta", "ω"]
    first, second = [], []
    for i in range(30):
        tokens = {w: round(rng.uniform(0.01, 3.0), 3) for w in rng.sample(words, 3 + i % 5)}
        (first if i < 15 else second).append(("index", f"s-{i:02d}", {"name": f"sparse {i:02d}", "tokens": tokens}))
    return [first, second]


def noindex_batches(rng):
    docs = [("index", f"x-{i:02d}", {"name": f"plain {i:02d}", "v": floats(rng, 8)}) for i in range(24)]
    return [docs[:12], docs[12:]]


def span(text, i):
    """End index of the JSON value starting at text[i] (no whitespace before it)."""
    c = text[i]
    if c == '"':
        i += 1
        while text[i] != '"':
            i += 2 if text[i] == "\\" else 1
        return i + 1
    if c in "{[":
        close = "}" if c == "{" else "]"
        i += 1
        while True:
            i = skip_ws(text, i)
            if text[i] == close:
                return i + 1
            if text[i] == ",":
                i += 1
                continue
            if c == "{":
                i = skip_ws(text, span(text, i))
                assert text[i] == ":"
                i = skip_ws(text, i + 1)
            i = span(text, i)
    while i < len(text) and text[i] not in ",]} \t\r\n":
        i += 1
    return i


def skip_ws(text, i):
    while text[i] in " \t\r\n":
        i += 1
    return i


def members(text, start):
    """(key, value_start, value_end) of the object starting at text[start]."""
    out = []
    i = start + 1
    while True:
        i = skip_ws(text, i)
        if text[i] == "}":
            return out
        if text[i] == ",":
            i += 1
            continue
        key_end = span(text, i)
        key = json.loads(text[i:key_end])
        i = skip_ws(text, key_end)
        i = skip_ws(text, i + 1)
        end = span(text, i)
        out.append((key, i, end))
        i = end


def member(text, start, name):
    for key, value_start, value_end in members(text, start):
        if key == name:
            return value_start, value_end
    raise KeyError(name)


def fetched_sources(index, exclude_vectors):
    """{_id: raw _source text} exactly as ES's _search wrote the bytes."""
    body = {"size": 10000, "query": {"match_all": {}}, "sort": ["_doc"]}
    if exclude_vectors is not None:
        body["_source"] = {"exclude_vectors": exclude_vectors}
    text = es_raw("POST", f"/{index}/_search", body)
    hits_start, _ = member(text, 0, "hits")
    array_start, array_end = member(text, hits_start, "hits")
    sources = {}
    i = array_start + 1
    while True:
        i = skip_ws(text, i)
        if text[i] == "]":
            break
        if text[i] == ",":
            i += 1
            continue
        hit_end = span(text, i)
        id_start, id_end = member(text, i, "_id")
        source_start, source_end = member(text, i, "_source")
        sources[json.loads(text[id_start:id_end])] = text[source_start:source_end]
        i = hit_end
    return dict(sorted(sources.items()))


def write_raw_expected(path, sources):
    """One document per line, the raw ES bytes after the key: tests read the text line by line."""
    if path.exists():
        raise RuntimeError(f"{path} already exists; move it away first")
    for raw in sources.values():
        if "\n" in raw:
            raise RuntimeError("a fetched _source spans lines")
    lines = [f" {json.dumps(doc_id)}: {raw}" for doc_id, raw in sources.items()]
    path.write_text("{\n" + ",\n".join(lines) + "\n}\n", encoding="utf-8")


def write_input(path, docs):
    if path.exists():
        raise RuntimeError(f"{path} already exists; move it away first")
    lines = [f" {json.dumps(doc_id)}: {json.dumps(doc, separators=(',', ':'), ensure_ascii=False)}"
             for doc_id, doc in docs.items()]
    path.write_text("{\n" + ",\n".join(lines) + "\n}\n", encoding="utf-8")


def segments(index):
    shards = es("GET", f"/{index}/_segments")["indices"][index]["shards"]["0"]
    return {name: {k: seg.get(k) for k in ("num_docs", "deleted_docs", "compound")}
            for copy in shards for name, seg in copy["segments"].items()}


def without(source, keys):
    return {k: v for k, v in source.items() if k not in keys}


def generate():
    outputs = [OUTPUT] + [FIXTURES / f"vec-{index}-{kind}.json" for index in INDICES for kind in ("expected", "input")]
    for path in outputs:
        if path.exists():
            raise RuntimeError(f"{path} already exists; move it away first")
    if WORK.exists():
        shutil.rmtree(WORK)
    snapshots = WORK / "snapshots"
    snapshots.mkdir(parents=True)
    start_container(f"{snapshots}:/snapshots")
    try:
        rng = random.Random(9241)
        vector = lambda dims, **extra: dict({"type": "dense_vector", "dims": dims}, **extra)
        root_mapping = {"properties": {"title": {"type": "keyword"},
                                       "embedding": vector(8, similarity="cosine"),
                                       "emb_bbq": vector(64, similarity="cosine", index_options={"type": "bbq_hnsw"})}}
        batches = root_batches(rng)
        inputs = {ROOT_INDEX: create(ROOT_INDEX, {}, root_mapping, batches),
                  ROOT_INCLUDED: create(ROOT_INCLUDED, {"index.mapping.exclude_source_vectors": False},
                                        root_mapping, batches)}
        inputs[OBJECT] = create(OBJECT, {}, {"properties": {
            "name": {"type": "keyword"},
            "meta": {"properties": {"source": {"type": "keyword"}, "rank": {"type": "integer"},
                                    "embedding": vector(8, similarity="l2_norm")}}}}, object_batches(rng))
        inputs[NESTED] = create(NESTED, {}, {"properties": {
            "title": {"type": "keyword"},
            "chunks": {"type": "nested", "properties": {"text": {"type": "keyword"},
                                                         "vector": vector(8, similarity="l2_norm")}}}},
            nested_batches(rng))
        inputs[BYTE_BIT] = create(BYTE_BIT, {}, {"properties": {
            "name": {"type": "keyword"},
            "b": vector(8, element_type="byte", similarity="l2_norm"),
            "bits": vector(64, element_type="bit")}}, byte_bit_batches(rng))
        inputs[SPARSE] = create(SPARSE, {}, {"properties": {
            "name": {"type": "keyword"}, "tokens": {"type": "sparse_vector"}}}, sparse_batches(rng))
        inputs[NOINDEX] = create(NOINDEX, {}, {"properties": {
            "name": {"type": "keyword"}, "v": vector(8, index=False)}}, noindex_batches(rng))

        report = {"elasticsearch_version": es("GET", "/")["version"]["number"], "indices": {}}
        expected = {}
        for index in INDICES:
            effective = es("GET", f"/{index}/_settings?include_defaults=true&flat_settings=true")[index]
            settings = {k: v for part in (effective["settings"], effective["defaults"]) for k, v in part.items()
                        if k.startswith(("index.mapping.exclude_source_vectors", "index.version.created"))}
            count = es("GET", f"/{index}/_count")["count"]
            report["indices"][index] = {"doc_count": count, "segments": segments(index), "settings": settings}
            if int(settings["index.version.created"]) < FIRST_9_2_INDEX_VERSION:
                raise RuntimeError(f"{index} was not created on 9.2+")
            if count != len(inputs[index]):
                raise RuntimeError(f"{index}: _count {count} != {len(inputs[index])} live documents sent")
            if len(report["indices"][index]["segments"]) != 2:
                raise RuntimeError(f"{index}: expected two segments: {report['indices'][index]}")
            expected[index] = fetched_sources(index, exclude_vectors=False)
            if sorted(expected[index]) != sorted(inputs[index]):
                raise RuntimeError(f"{index}: fetched ids differ from the live documents sent")

        default_root = fetched_sources(ROOT_INDEX, exclude_vectors=None)
        for doc_id, raw in expected[ROOT_INDEX].items():
            full = json.loads(raw)
            if without(full, {"embedding", "emb_bbq"}) != json.loads(default_root[doc_id]):
                raise RuntimeError(f"{ROOT_INDEX}/{doc_id}: default and vector fetch differ beyond the vectors")
        for index in (ROOT_INDEX, OBJECT, NESTED, BYTE_BIT, SPARSE, NOINDEX):
            if fetched_sources(index, exclude_vectors=None) == expected[index]:
                raise RuntimeError(f"{index}: Elasticsearch's default fetch shows the vectors")
        if fetched_sources(ROOT_INCLUDED, exclude_vectors=None) != expected[ROOT_INCLUDED]:
            raise RuntimeError(f"{ROOT_INCLUDED}: the twin must keep its vectors in the stored _source")

        es("PUT", "/_snapshot/vec", {"type": "fs", "settings": {"location": "/snapshots/vec"}})
        snapshot = es("PUT", "/_snapshot/vec/vec-fixture?wait_for_completion=true",
                      {"indices": ",".join(INDICES), "include_global_state": False}, timeout=300)["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot}")
        shutil.copytree(snapshots / "vec", OUTPUT)
        for index in INDICES:
            write_raw_expected(FIXTURES / f"vec-{index}-expected.json", expected[index])
            write_input(FIXTURES / f"vec-{index}-input.json", inputs[index])
        print(json.dumps(report, indent=2))
        size = sum(p.stat().st_size for p in OUTPUT.rglob("*") if p.is_file())
        print(f"Wrote {OUTPUT} ({size} bytes)")
    finally:
        docker("rm", "-f", CONTAINER, check=False)
        shutil.rmtree(WORK, ignore_errors=True)


def restored_es93():
    """Elasticsearch's fetch of the committed es93-repo snapshot's vector index, restored read-only."""
    if RESTORED_EXPECTED.exists():
        raise RuntimeError(f"{RESTORED_EXPECTED} already exists; move it away first")
    before = sorted((p.relative_to(ES93_REPO).as_posix(), p.stat().st_size, p.stat().st_mtime_ns)
                    for p in ES93_REPO.rglob("*") if p.is_file())
    start_container(f"{ES93_REPO}:/snapshots/es93:ro")
    try:
        es("PUT", "/_snapshot/es93", {"type": "fs", "settings": {"location": "/snapshots/es93", "readonly": True}})
        snapshots = es("GET", "/_snapshot/es93/_all")["snapshots"]
        if len(snapshots) != 1:
            raise RuntimeError(f"expected one snapshot in es93-repo, found {len(snapshots)}")
        restore = es("POST", f"/_snapshot/es93/{snapshots[0]['snapshot']}/_restore?wait_for_completion=true",
                     {"indices": ES93_VECTORS_DEFAULT, "include_global_state": False}, timeout=300)
        if restore["snapshot"]["shards"]["failed"] != 0:
            raise RuntimeError(f"restore failed: {restore}")
        es("GET", f"/_cluster/health/{ES93_VECTORS_DEFAULT}?wait_for_status=green&timeout=60s")
        sources = fetched_sources(ES93_VECTORS_DEFAULT, exclude_vectors=False)
        if len(sources) != 20:
            raise RuntimeError(f"expected 20 documents, fetched {len(sources)}")
        write_raw_expected(RESTORED_EXPECTED, sources)
        print(f"Wrote {RESTORED_EXPECTED} ({len(sources)} documents)")
    finally:
        docker("rm", "-f", CONTAINER, check=False)
    after = sorted((p.relative_to(ES93_REPO).as_posix(), p.stat().st_size, p.stat().st_mtime_ns)
                   for p in ES93_REPO.rglob("*") if p.is_file())
    if before != after:
        raise RuntimeError("es93-repo changed during the restore")


if __name__ == "__main__":
    if sys.argv[1:] == ["--restored-es93"]:
        restored_es93()
    elif sys.argv[1:]:
        sys.exit("usage: generate_vector_fixture.py [--restored-es93]")
    else:
        generate()
