"""Generate the synthetic-coverage fixtures: real ES 9.3 (and, with --es817, ES 8.17) snapshots of
nested objects under synthetic source and of indices whose _source was disabled after creation or
from creation (index.mapping.source.mode: disabled), with Elasticsearch's own output as reference.

ES 9.3 indices (synthcov-repo, container gs-synthcov-es93 on port 9260):
  cov_nested_synth     synthetic source, 2 shards, `items` nested (with nested `parts`), roots with
                       0-3 items, `items: []`, `items: null`, missing sub-fields; updates and
                       deletes across three flushes; ~1,200 roots.
  cov_nested_logsdb    the same documents under index.mode logsdb (skipped, with the reason
                       recorded, if Elasticsearch rejects nested there).
  cov_srcoff_native    stored source, 2 shards: batch 1, flush, `_source.enabled: false`, batch 2,
                       flush. Shard 0 holds batch 1 only, shard 1 both. Native types only, a
                       constant_keyword with a value (some batch-2 documents send it), keyword and
                       text sub-field ignore_above, a malformed long (counted in _ignored).
  cov_srcoff_merged    cov_srcoff_native force-merged to one mixed segment per shard.
  cov_srcoff_text      plus a text field without keyword sub-field and a keyword without doc values
                       or store holding data in batch 2 (shard 1 refuses).
  cov_srcoff_mapping   dynamic:false with unmapped values, an enabled:false object, a binary field,
                       a flattened field with ignore_above and a shadowing runtime field.
  cov_srcoff_copyto    a keyword copied into a text catch-all.
  cov_srcoff_dynamic   one dynamic:false and one dynamic:runtime object receiving unmapped values in
                       batch 2, nothing else that refuses (the --source-disabled-accept opt-in).
  cov_srcmode_native   created with index.mapping.source.mode: disabled, native types and a
                       constant_keyword with a value; every shard reconstructs.
  cov_srcmode_mapping  the same setting with an enabled:false object: refused before download.
  cov_srcmode_text     the same setting, a text field without keyword sub-field holding data in
                       shard 1 only.
ES 8.17 (synthcov8-repo with --es817, container gs-synthcov-es817 on port 9261): cov8_nested_synth
(`_source.mode: synthetic` mapping), cov8_srcoff_native, cov8_srcmode_native (when 8.17 accepts the
setting).

Oracles: reconstructed documents are compared with a native-synthetic twin index (not part of the
snapshot) that received the same documents with the same ids: fields listed in the twin document's
_ignored removed (a listed keyword sub-field removes its text field), constant_keyword values added
where the document lacks them, and for the opt-in oracle the values under dynamic:false/runtime
objects removed.

Writes per index synthcov-<index>-expected.json: {"sources": {_id: expected row source},
"input": {_id: exact JSON text sent}, "shards": {_id: shard}, "reconstructed": [_ids rebuilt without
stored _source], "ignored": [reconstructed _ids whose _ignored lists a field], "deleted_ids": [...],
"updated_ids": [...], optionally "sources_optin"} and synthcov-report.json (ES version, rejections,
segments). Stdlib only. Hold the docker slot; refuses to overwrite:
  $SLOT docker 1 python3 scripts/generate_synthetic_coverage_fixture.py [--es817]
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

ES817 = "--es817" in sys.argv[1:]
if ES817:
    OUTPUT, REPO, CONTAINER, PORT, IMAGE, PREFIX = FIXTURES / "synthcov8-repo", "synthcov8", "gs-synthcov-es817", 9261, "gefrierschrank-es8", "cov8_"
else:
    OUTPUT, REPO, CONTAINER, PORT, IMAGE, PREFIX = FIXTURES / "synthcov-repo", "synthcov", "gs-synthcov-es93", 9260, "gefrierschrank-es9", "cov_"
WORK = ROOT / f"target/{REPO}-fixture-work"
ES = f"http://localhost:{PORT}"


class EsError(RuntimeError):
    def __init__(self, status, body):
        super().__init__(f"HTTP {status}: {body[:500]}")
        self.status = status
        self.body = body


def es(method, path, body=None, timeout=300):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(ES + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as e:
        raise EsError(e.code, e.read().decode(errors="replace")) from None


def docker(*args, check=True):
    return subprocess.run(["docker", *args], check=check, capture_output=True, text=True)


def start_container():
    if CONTAINER in docker("ps", "-a", "--format", "{{.Names}}").stdout.split():
        raise RuntimeError(f"container {CONTAINER} already exists; remove it first")
    snapshots = WORK / "snapshots"
    snapshots.mkdir(parents=True)
    docker("run", "-d", "--name", CONTAINER, "-p", f"{PORT}:9200",
           "-e", "discovery.type=single-node", "-e", "xpack.security.enabled=false",
           "-e", "ES_JAVA_OPTS=-Xms1g -Xmx1g", "-e", "path.repo=/snapshots",
           "-v", f"{snapshots}:/snapshots", IMAGE)
    deadline = time.time() + 240
    while time.time() < deadline:
        try:
            if es("GET", "/_cluster/health?wait_for_status=yellow&timeout=5s")["status"] in ("yellow", "green"):
                return snapshots
        except (urllib.error.URLError, ConnectionError, OSError, EsError):
            pass
        time.sleep(2)
    raise RuntimeError("Elasticsearch did not start")


def compact(doc):
    return json.dumps(doc, separators=(",", ":"))


def bulk(index, docs, op="index"):
    """docs: (id, routing or None, exact JSON text or None for a delete)."""
    lines = []
    for doc_id, routing, text in docs:
        action = {"_index": index, "_id": doc_id}
        if routing is not None:
            action["routing"] = routing
        lines.append(json.dumps({op if text is not None else "delete": action}))
        if text is not None:
            lines.append(text)
    request = urllib.request.Request(f"{ES}/_bulk?refresh=true", data=("\n".join(lines) + "\n").encode(),
                                     method="POST", headers={"Content-Type": "application/x-ndjson"})
    with urllib.request.urlopen(request, timeout=300) as response:
        result = json.load(response)
    if result.get("errors"):
        failed = [item for item in result["items"] if "error" in next(iter(item.values()))]
        raise RuntimeError(f"bulk errors in {index}: {failed[:2]}")


def flush(index):
    es("POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")


def create(index, settings, mappings):
    es("PUT", "/" + index, {"settings": dict({"number_of_shards": 2, "number_of_replicas": 0}, **settings),
                            "mappings": mappings})


def routing_per_shard(index):
    """One routing value per shard of a two-shard index."""
    found = {}
    for n in range(200):
        value = f"r{n}"
        shard = es("GET", f"/{index}/_search_shards?routing={value}")["shards"][0][0]["shard"]
        found.setdefault(shard, value)
        if len(found) == 2:
            return found[0], found[1]
    raise RuntimeError(f"no routing for both shards of {index}")


def search_all(index, shard=None):
    path = f"/{index}/_search" + (f"?preference=_shards:{shard}" if shard is not None else "")
    hits = es("POST", path, {"size": 10000, "query": {"match_all": {}}, "sort": ["_doc"]})["hits"]["hits"]
    return {hit["_id"]: hit for hit in hits}


def shard_of_each(index):
    return {doc_id: shard for shard in (0, 1) for doc_id in search_all(index, shard)}


def segments(index):
    shards = es("GET", f"/{index}/_segments")["indices"][index]["shards"]
    return {shard: [{k: seg.get(k) for k in ("num_docs", "deleted_docs", "compound")} | {"name": name}
                    for copy in copies for name, seg in copy["segments"].items()]
            for shard, copies in shards.items()}


def write_expected(index, expected):
    path = FIXTURES / f"synthcov-{index}-expected.json"
    parts = []
    for key, value in expected.items():
        if isinstance(value, dict):
            body = ",\n".join(f"  {json.dumps(k)}: {compact(v)}" for k, v in sorted(value.items()))
            parts.append(f" {json.dumps(key)}: {{\n{body}\n }}")
        else:
            parts.append(f" {json.dumps(key)}: {compact(value)}")
    path.write_text("{\n" + ",\n".join(parts) + "\n}\n")


# ---------------------------------------------------------------- nested under synthetic source

def nested_doc(rng, i):
    doc = {"@timestamp": f"2026-06-{1 + i % 28:02d}T{i % 24:02d}:{i % 60:02d}:00.000Z",
           "server": f"web-{i % 7:02d}.example.internal", "status": [200, 201, 404, 500][i % 4]}
    shape = i % 9
    if shape == 6:
        doc["items"] = []
    elif shape == 7:
        doc["items"] = None
    elif shape != 8:
        items = []
        for k in range(shape % 4):
            item = {"name": f"item-{i}-{k}", "qty": rng.randint(1, 9)}
            if (i + k) % 5 == 0:
                del item["qty"]
            if (i + k) % 4 == 1:
                item["parts"] = [{"sku": f"sku-{i}-{k}-{p}"} for p in range(1 + (i + k) % 3)]
            items.append(item)
        if len(items) == 1 and i % 2:
            doc["items"] = items[0]
        elif items:
            doc["items"] = items
    return doc


NESTED_MAPPING = {"properties": {
    "@timestamp": {"type": "date"}, "server": {"type": "keyword"}, "status": {"type": "integer"},
    "items": {"type": "nested", "properties": {
        "name": {"type": "keyword"}, "qty": {"type": "integer"},
        "parts": {"type": "nested", "properties": {"sku": {"type": "keyword"}}}}}}}


def build_nested(index, settings, mapping):
    rng = random.Random(4711)
    create(index, settings, mapping)
    ids = [f"n-{i:05d}" for i in range(1200)]
    sent = {}
    batches = [range(0, 600), range(600, 1000), range(1000, 1200)]
    deleted, updated = [], []
    for b, batch in enumerate(batches):
        docs = []
        for i in batch:
            text = compact(nested_doc(rng, i))
            sent[ids[i]] = text
            docs.append((ids[i], None, text))
        if b == 1:
            for i in range(3, 600, 37):
                text = compact(nested_doc(rng, 5000 + i))
                sent[ids[i]] = text
                updated.append(ids[i])
                docs.append((ids[i], None, text))
        bulk(index, docs)
        if b == 2:
            deletes = [ids[i] for i in range(10, 1100, 53) if ids[i] not in updated]
            bulk(index, [(doc_id, None, None) for doc_id in deletes])
            deleted.extend(deletes)
            for doc_id in deletes:
                sent.pop(doc_id)
        flush(index)
    hits = search_all(index)
    return {"sources": {doc_id: hit["_source"] for doc_id, hit in hits.items()}, "input": sent,
            "shards": shard_of_each(index), "deleted_ids": sorted(deleted), "updated_ids": sorted(updated)}


# ---------------------------------------------------------------- _source disabled later / from creation

def native_properties(extra=None):
    properties = {
        "kw": {"type": "keyword"},
        "kw_short": {"type": "keyword", "ignore_above": 16},
        "count": {"type": "long", "ignore_malformed": True},
        "ratio": {"type": "double"},
        "when": {"type": "date"},
        "flag": {"type": "boolean"},
        "addr": {"type": "ip"},
        "where": {"type": "geo_point"},
        "title": {"type": "text", "fields": {"raw": {"type": "keyword", "ignore_above": 32}}},
        "note": {"type": "text", "store": True},
        "code": {"type": "keyword", "doc_values": False, "store": True},
        "dataset": {"type": "constant_keyword", "value": "coverage"},
        "items": {"type": "nested", "properties": {"name": {"type": "keyword"}, "qty": {"type": "integer"}}},
    }
    properties.update(extra or {})
    return properties


def native_doc(rng, i, batch):
    doc = {"kw": f"value-{i % 13}", "kw_short": f"short-{i}" if i % 6 else f"a-value-longer-than-sixteen-{i}",
           "count": i * 3 if i % 11 else "not-a-number", "ratio": round(rng.uniform(0, 10), 3),
           "when": f"2026-05-{1 + i % 28:02d}T{i % 24:02d}:00:00.000Z", "flag": i % 2 == 0,
           "addr": f"10.0.{i % 256}.{(i * 7) % 256}", "where": {"lat": round(rng.uniform(-80, 80), 4),
                                                                "lon": round(rng.uniform(-170, 170), 4)},
           "title": f"title {i}" if i % 5 else f"a long title {i} that goes well beyond thirty-two characters",
           "note": f"note number {i}", "code": f"C{i:04d}"}
    if i % 3 == 0:
        doc["items"] = [{"name": f"it-{i}-{k}", "qty": k + 1} for k in range(1 + i % 3)]
    if batch == 2 and i % 4 == 0:
        doc["dataset"] = "coverage"
    elif batch == 1 and i % 2 == 0:
        doc["dataset"] = "coverage"
    return doc


def prune(value):
    if isinstance(value, dict):
        out = {k: prune(v) for k, v in value.items()}
        return {k: v for k, v in out.items() if not (isinstance(v, (dict, list)) and not v)}
    if isinstance(value, list):
        out = [prune(v) for v in value]
        return [v for v in out if not (isinstance(v, (dict, list)) and not v)]
    return value


def remove_path(source, path, properties):
    """Removes a field (dotted path) from a rendered source; a listed multi-field removes its parent field."""
    parts = path.split(".")
    node, props = source, properties
    for depth, part in enumerate(parts):
        if isinstance(node, list):
            for element in node:
                remove_path(element, ".".join(parts[depth:]), props)
            return
        if not isinstance(node, dict) or part not in node:
            return
        definition = props.get(part, {})
        if depth == len(parts) - 1 or "fields" in definition:
            del node[part]
            return
        node, props = node[part], definition.get("properties", {})


def oracle(twin_hit, properties, constants, dropped=()):
    source = json.loads(json.dumps(twin_hit["_source"]))
    for field in twin_hit.get("_ignored", []):
        remove_path(source, field, properties)
    for path in dropped:
        remove_path(source, path, properties)
    source = prune(source)
    for name, value in constants.items():
        if name not in source:
            # Where native rendering puts a sent value: before the first field ordered after it.
            keys = list(source)
            at = next((n for n, key in enumerate(keys) if key > name), len(keys))
            items = list(source.items())
            source = dict(items[:at] + [(name, value)] + items[at:])
    return source


def build_disabled_later(index, properties, constants, batch1, batch2, merge=False, extra_mapping=None, dropped=()):
    """batch1/batch2: lists of (id, doc). Batch 1 goes to both shards, batch 2 to shard 1 only."""
    mapping = dict({"properties": properties}, **(extra_mapping or {}))
    create(index, {}, mapping)
    r0, r1 = routing_per_shard(index)
    sent, routing = {}, {}
    docs = []
    for n, (doc_id, doc) in enumerate(batch1):
        sent[doc_id] = compact(doc)
        routing[doc_id] = r0 if n % 2 == 0 else r1
        docs.append((doc_id, routing[doc_id], sent[doc_id]))
    bulk(index, docs)
    flush(index)
    es("PUT", f"/{index}/_mapping", {"_source": {"enabled": False}})
    docs = []
    for doc_id, doc in batch2:
        sent[doc_id] = compact(doc)
        routing[doc_id] = r1
        docs.append((doc_id, r1, sent[doc_id]))
    bulk(index, docs)
    flush(index)
    if merge:
        es("POST", f"/{index}/_forcemerge?max_num_segments=1")
    twin = index + "_twin"
    create(twin, {"index.mapping.source.mode": "synthetic"}, mapping)
    bulk(twin, [(doc_id, r1, sent[doc_id]) for doc_id, _ in batch2])
    twin_hits = search_all(twin)
    hits = search_all(index)
    reconstructed = [doc_id for doc_id, _ in batch2]
    expected = {"sources": {}, "input": sent, "shards": shard_of_each(index), "routing": routing,
                "reconstructed": sorted(reconstructed),
                "ignored": sorted(d for d in reconstructed if hits[d].get("_ignored"))}
    for doc_id, _ in batch1:
        expected["sources"][doc_id] = json.loads(sent[doc_id])
    for doc_id in reconstructed:
        expected["sources"][doc_id] = oracle(twin_hits[doc_id], properties, constants)
    if dropped:
        expected["sources_optin"] = dict(expected["sources"])
        for doc_id in reconstructed:
            expected["sources_optin"][doc_id] = oracle(twin_hits[doc_id], properties, constants, dropped)
    es("DELETE", "/" + twin)
    return expected


def build_disabled_from_creation(index, properties, constants, docs, report, shard1_only=None):
    """docs: (id, doc); documents of shard1_only (ids) go to shard 1 only, the rest alternate."""
    mapping = {"properties": properties}
    try:
        create(index, {"index.mapping.source.mode": "disabled"}, mapping)
    except EsError as rejected:
        report["rejections"][index] = {"status": rejected.status,
                                       "type": json.loads(rejected.body).get("error", {}).get("type")}
        return None
    r0, r1 = routing_per_shard(index)
    sent, routing = {}, {}
    for n, (doc_id, doc) in enumerate(docs):
        sent[doc_id] = compact(doc)
        routing[doc_id] = r1 if (shard1_only and doc_id in shard1_only) or n % 2 else r0
    bulk(index, [(doc_id, routing[doc_id], sent[doc_id]) for doc_id, _ in docs])
    flush(index)
    twin = index + "_twin_synth"
    create(twin, {"index.mapping.source.mode": "synthetic"}, mapping)
    bulk(twin, [(doc_id, routing[doc_id], sent[doc_id]) for doc_id, _ in docs])
    twin_hits = search_all(twin)
    hits = search_all(index)
    expected = {"sources": {d: oracle(twin_hits[d], properties, constants) for d, _ in docs}, "input": sent,
                "shards": shard_of_each(index), "routing": routing, "reconstructed": sorted(d for d, _ in docs),
                "ignored": sorted(d for d, _ in docs if hits[d].get("_ignored"))}
    es("DELETE", "/" + twin)
    return expected


def main():
    if OUTPUT.exists():
        raise RuntimeError(f"{OUTPUT} already exists; move it away first")
    if WORK.exists():
        shutil.rmtree(WORK)
    snapshots = start_container()
    try:
        version = es("GET", "/")["version"]["number"]
        for _ in range(60):
            try:
                license_info = es("GET", "/_license")["license"]
                break
            except EsError as missing:
                if missing.status != 404:
                    raise
                time.sleep(2)
        else:
            license_info = {"status": "missing", "type": "none"}
        if license_info["status"] != "active" or license_info["type"] not in ("trial", "platinum", "enterprise"):
            es("POST", "/_license/start_trial?acknowledge=true")
        report = {"elasticsearch_version": version, "rejections": {}, "indices": {}}
        expected = {}
        constants = {"dataset": "coverage"}
        rng = random.Random(817 if ES817 else 93)

        nested = [(PREFIX + "nested_synth", {}, dict(NESTED_MAPPING, _source={"mode": "synthetic"}))] if ES817 else [
            (PREFIX + "nested_synth", {"index.mapping.source.mode": "synthetic"}, NESTED_MAPPING),
            (PREFIX + "nested_logsdb", {"index.mode": "logsdb"}, NESTED_MAPPING)]
        for index, settings, mapping in nested:
            try:
                expected[index] = build_nested(index, settings, mapping)
            except EsError as rejected:
                report["rejections"][index] = {"status": rejected.status,
                                               "type": json.loads(rejected.body).get("error", {}).get("type")}

        def batches(prefix, n1, n2, doc=native_doc):
            return ([(f"{prefix}-a{i:03d}", doc(rng, i, 1)) for i in range(n1)],
                    [(f"{prefix}-b{i:03d}", doc(rng, 1000 + i, 2)) for i in range(n2)])

        b1, b2 = batches("nat", 60, 60)
        expected[PREFIX + "srcoff_native"] = build_disabled_later(PREFIX + "srcoff_native", native_properties(), constants, b1, b2)

        if not ES817:
            b1, b2 = batches("mrg", 40, 40)
            expected[PREFIX + "srcoff_merged"] = build_disabled_later(PREFIX + "srcoff_merged", native_properties(), constants,
                                                                      b1, b2, merge=True)

            def text_doc(r, i, batch):
                doc = native_doc(r, i, batch)
                if batch == 2:
                    doc["message"] = f"free text message {i}"
                    doc["tag"] = f"tag-{i % 5}"
                return doc
            b1, b2 = batches("txt", 30, 30, text_doc)
            expected[PREFIX + "srcoff_text"] = build_disabled_later(
                PREFIX + "srcoff_text", native_properties({"message": {"type": "text"},
                                                           "tag": {"type": "keyword", "doc_values": False}}),
                constants, b1, b2)

            def mapping_doc(r, i, batch):
                doc = {"kw": f"value-{i}", "count": i}
                if batch == 2:
                    doc.update({"unmapped_field": f"lost-{i}", "blob": {"anything": i}, "payload": "AAEC",
                                "labels": {"short": "ok", "long": f"value-longer-than-ten-{i}"}, "shadowed": f"rt-{i}"})
                return doc
            b1, b2 = batches("map", 20, 20, mapping_doc)
            expected[PREFIX + "srcoff_mapping"] = build_disabled_later(
                PREFIX + "srcoff_mapping",
                {"kw": {"type": "keyword"}, "count": {"type": "long"}, "blob": {"type": "object", "enabled": False},
                 "payload": {"type": "binary"}, "labels": {"type": "flattened", "ignore_above": 10}},
                {}, b1, b2, extra_mapping={"dynamic": False, "runtime": {"shadowed": {"type": "keyword"}}})

            def copyto_doc(r, i, batch):
                return {"kw": f"value-{i}", "count": i, "label": f"label {i}"}
            b1, b2 = batches("cpy", 20, 20, copyto_doc)
            expected[PREFIX + "srcoff_copyto"] = build_disabled_later(
                PREFIX + "srcoff_copyto", {"kw": {"type": "keyword"}, "count": {"type": "long"},
                                          "label": {"type": "keyword", "copy_to": "all_text"}, "all_text": {"type": "text"}},
                {}, b1, b2)

            def dynamic_doc(r, i, batch):
                doc = {"kw": f"value-{i}", "count": i, "fixed": {"known": f"k-{i}"}, "rt": {"known": f"r-{i}"}}
                if batch == 2:
                    doc["fixed"]["extra"] = f"lost-{i}"
                    doc["rt"]["extra"] = f"shadowed-{i}"
                return doc
            b1, b2 = batches("dyn", 20, 20, dynamic_doc)
            expected[PREFIX + "srcoff_dynamic"] = build_disabled_later(
                PREFIX + "srcoff_dynamic",
                {"kw": {"type": "keyword"}, "count": {"type": "long"},
                 "fixed": {"type": "object", "dynamic": False, "properties": {"known": {"type": "keyword"}}},
                 "rt": {"type": "object", "dynamic": "runtime", "properties": {"known": {"type": "keyword"}}}},
                {}, b1, b2, dropped=("fixed.extra", "rt.extra"))

        srcmode = [(f"mode-{i:03d}", native_doc(rng, 2000 + i, 2)) for i in range(60)]
        result = build_disabled_from_creation(PREFIX + "srcmode_native", native_properties(), constants, srcmode, report)
        if result is not None:
            expected[PREFIX + "srcmode_native"] = result
        if not ES817:
            properties = {"kw": {"type": "keyword"}, "count": {"type": "long"}, "blob": {"type": "object", "enabled": False}}
            result = build_disabled_from_creation(PREFIX + "srcmode_mapping", properties, {},
                                                  [(f"mm-{i:03d}", {"kw": f"v-{i}", "count": i}) for i in range(10)], report)
            if result is not None:
                expected[PREFIX + "srcmode_mapping"] = result
            text_docs = [(f"mt-{i:03d}", {"kw": f"v-{i}", "count": i} | ({"message": f"text {i}"} if i >= 10 else {}))
                         for i in range(20)]
            result = build_disabled_from_creation(PREFIX + "srcmode_text", {"kw": {"type": "keyword"}, "count": {"type": "long"},
                                                                            "message": {"type": "text"}},
                                                  {}, text_docs, report, shard1_only={d for d, doc in text_docs if "message" in doc})
            if result is not None:
                expected[PREFIX + "srcmode_text"] = result

        indices = sorted(expected)
        for index in indices:
            effective = es("GET", f"/{index}/_settings?include_defaults=true&flat_settings=true")[index]
            report["indices"][index] = {
                "doc_count": es("GET", f"/{index}/_count")["count"], "segments": segments(index),
                "settings": {k: v for part in (effective["settings"], effective["defaults"]) for k, v in part.items()
                             if k.startswith(("index.mode", "index.mapping.source", "index.version.created", "index.codec"))},
                "mapping_source": es("GET", f"/{index}/_mapping")[index]["mappings"].get("_source")}
        es("PUT", f"/_snapshot/{REPO}", {"type": "fs", "settings": {"location": f"/snapshots/{REPO}"}})
        snapshot = es("PUT", f"/_snapshot/{REPO}/{REPO}-fixture?wait_for_completion=true",
                      {"indices": ",".join(indices), "include_global_state": False}, timeout=600)["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot}")
        shutil.copytree(snapshots / REPO, OUTPUT)
        for index in indices:
            write_expected(index, expected[index])
        (FIXTURES / f"{REPO}-report.json").write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
        print(json.dumps({"version": version, "rejections": report["rejections"],
                          "indices": {i: report["indices"][i]["doc_count"] for i in indices}}, indent=1))
        size = sum(p.stat().st_size for p in OUTPUT.rglob("*") if p.is_file())
        print(f"Wrote {OUTPUT} ({size} bytes)")
    finally:
        docker("rm", "-f", CONTAINER, check=False)
        shutil.rmtree(WORK, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
