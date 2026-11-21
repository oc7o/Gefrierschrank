"""Generate the selective-fetch fixture: a real ES 9.3 snapshot of synthetic-source indices whose
segments are NOT compound, so selective fetch can leave index structures out of them (every other
synthetic fixture is compound: Elasticsearch writes segments below 1 GB as compound files).

Every index has one shard and `index.compound_format: false`; unless noted it is written in two
bulk+flush rounds and force-merged to one segment (Elasticsearch flushes compound segments whatever
that setting says; the forced merge rewrites them under it). The script asserts that no segment
is compound but m3_compound's and the second one of m3_logsdb_multi.

  m3_logsdb        logsdb, ECS-like: keyword (one over ignore_above), ip, date, long, text with a
                   keyword sub-field, match_only_text, wildcard, flattened, scaled_float, geo_point
  m3_logsdb_multi  the first round merged (non-compound), a second compound round updating documents
                   of the first, then deletes: a commit mixing a prunable and a compound segment
  m3_tsdb          time_series: a dimension, gauge and counter metrics, histogram,
                   aggregate_metric_double
  m3_types         synthetic, one field per type Elasticsearch accepts under synthetic source in
                   this registry (a type it rejects is recorded in m3-report.json, never dropped
                   silently)
  m3_types_analyzed  the same for the types whose mapping names an analyzer (token_count,
                   completion), apart so that a reconstruction registry without that analyzer
                   fails only this index
  m3_doc_count     synthetic, documents carrying `_doc_count`
  m3_nested        synthetic, a nested field with a nested field inside
  m3_dense         synthetic, dense_vector int8_hnsw/cosine
  m3_sparse        synthetic, sparse_vector values only on documents 200 and later
  m3_src_off       stored source, `_source.enabled: false` after round 1, merged into one segment
                   mixing stored and source-less documents
  m3_compound      logsdb with the default compound setting (the control: compound, fetched in full)

Writes gefrierschrank-synthetic-source/src/test/resources/fixtures/m3-repo and m3-report.json
beside it (ES version, rejected types, per index the effective source mode, codec and segments).
Container gs-m3-es93 (image gefrierschrank-es9) on port 9251, trial license (synthetic source needs
it), removed on exit. Stdlib only; refuses to overwrite. Hold the docker slot:
  $SLOT docker 1 python3 scripts/generate_m3_fixture.py
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
OUTPUT = FIXTURES / "m3-repo"
REPORT = FIXTURES / "m3-report.json"
WORK = ROOT / "target/m3-fixture-work"
CONTAINER = "gs-m3-es93"
PORT = 9251
IMAGE = "gefrierschrank-es9"
ES = f"http://localhost:{PORT}"
REPO = "m3"


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


def bulk(index, docs, op="index"):
    """docs: (id, document or None for a delete)."""
    lines = []
    for doc_id, doc in docs:
        action = {"_index": index, "_id": doc_id} if doc_id is not None else {"_index": index}
        if doc is None:
            lines.append(json.dumps({"delete": action}))
        else:
            lines.append(json.dumps({op: action}))
            lines.append(json.dumps(doc, separators=(",", ":")))
    request = urllib.request.Request(f"{ES}/_bulk?refresh=true", data=("\n".join(lines) + "\n").encode(),
                                     method="POST", headers={"Content-Type": "application/x-ndjson"})
    with urllib.request.urlopen(request, timeout=300) as response:
        result = json.load(response)
    if result.get("errors"):
        failed = [item for item in result["items"] if "error" in next(iter(item.values()))]
        raise RuntimeError(f"bulk errors in {index}: {failed[:2]}")


def flush(index):
    es("POST", f"/{index}/_flush?force=true&wait_if_ongoing=true")


def create(index, settings, mappings, compound=False):
    base = {"number_of_shards": 1, "number_of_replicas": 0}
    if not compound:
        base["index.compound_format"] = "false"
    es("PUT", "/" + index, {"settings": base | settings, "mappings": mappings})


def rounds(index, batches, op="index", merge=True):
    for batch in batches:
        bulk(index, batch, op)
        flush(index)
    if merge:
        es("POST", f"/{index}/_forcemerge?max_num_segments=1", timeout=600)
        flush(index)


def segments(index):
    shards = es("GET", f"/{index}/_segments")["indices"][index]["shards"]
    return [{"name": name, "num_docs": seg.get("num_docs"), "deleted_docs": seg.get("deleted_docs"),
             "compound": seg.get("compound")}
            for copies in shards.values() for copy in copies for name, seg in copy["segments"].items()]


def effective(index):
    settings = es("GET", f"/{index}/_settings?include_defaults=true&flat_settings=true")[index]
    return {k: v for part in (settings["settings"], settings["defaults"]) for k, v in part.items()
            if k.startswith(("index.mode", "index.mapping.source", "index.version.created", "index.codec",
                             "index.compound_format"))}


HOSTS = ["web-01.example.internal", "web-02.example.internal", "db-07.example.internal", "cache-3.example.internal"]
PATHS = ["/api/v1/orders", "/api/v2/users/search", "/static/app.js", "/health", "/api/v1/items/*?q=a"]


def logs_mapping():
    return {"properties": {
        "@timestamp": {"type": "date"},
        "host": {"properties": {"name": {"type": "keyword"}}},
        "client": {"properties": {"ip": {"type": "ip"}}},
        "http": {"properties": {"status": {"type": "long"}, "bytes": {"type": "long"}}},
        "event": {"properties": {"original": {"type": "text", "fields": {"raw": {"type": "keyword"}}}}},
        "message": {"type": "match_only_text"},
        "url": {"properties": {"path": {"type": "wildcard"}}},
        "labels": {"type": "flattened"},
        "ratio": {"type": "scaled_float", "scaling_factor": 100},
        "location": {"type": "geo_point"},
        "tag": {"type": "keyword", "ignore_above": 16},
    }}


def logs_doc(rng, i):
    doc = {"@timestamp": f"2026-09-{1 + i % 28:02d}T{i % 24:02d}:{i % 60:02d}:{i % 59:02d}.{i % 1000:03d}Z",
           "host": {"name": HOSTS[i % len(HOSTS)]},
           "client": {"ip": f"10.{i % 7}.{i % 251}.{1 + i % 250}"},
           "http": {"status": [200, 201, 304, 404, 500][i % 5], "bytes": rng.randint(100, 100_000)},
           "event": {"original": f"GET {PATHS[i % len(PATHS)]} took {rng.randint(1, 900)} ms"},
           "message": f"request {i} from {HOSTS[i % len(HOSTS)]} served",
           "url": {"path": PATHS[i % len(PATHS)] + ("/" + "x" * (i % 40) if i % 6 == 0 else "")},
           "labels": {"env": ["prod", "staging"][i % 2], "team": f"t{i % 5}"},
           "ratio": round(rng.random(), 2),
           "location": {"lat": round(rng.uniform(-80, 80), 4), "lon": round(rng.uniform(-170, 170), 4)},
           "tag": "short" if i % 4 else "a-tag-longer-than-sixteen-characters"}
    if i % 9 == 0:
        del doc["ratio"]
    return doc


def tsdb_doc(rng, i):
    return {"@timestamp": f"2026-09-{1 + i % 28:02d}T{i % 24:02d}:{(i * 7) % 60:02d}:{i % 60:02d}Z",
            "host": HOSTS[i % len(HOSTS)],
            "cpu": round(rng.random() * 100, 3),
            "requests": 1000 + i,
            "latency": {"values": [0.1, 0.5, 1.0, 5.0], "counts": [rng.randint(0, 50) for _ in range(4)]},
            "load": {"min": 0.5, "max": 1.5 + i % 3, "sum": 10.0 + i, "value_count": 7}}


# Types whose mapping names an analyzer; kept apart so that a registry without that analyzer fails
# only their own index.
ANALYZED_TYPES = {
    "f_token_count": ({"type": "token_count", "analyzer": "standard"}, lambda i: f"one two {i}"),
    "f_completion": ({"type": "completion"}, lambda i: f"suggest {i % 11}"),
}

# One field per type; fields of a type Elasticsearch rejects under synthetic source are dropped and recorded.
TYPES = {
    "f_keyword": ({"type": "keyword"}, lambda i: f"k-{i % 13}"),
    "f_long": ({"type": "long"}, lambda i: i * 1_000_003),
    "f_integer": ({"type": "integer"}, lambda i: i - 50),
    "f_short": ({"type": "short"}, lambda i: i % 300),
    "f_byte": ({"type": "byte"}, lambda i: i % 100),
    "f_double": ({"type": "double"}, lambda i: i / 7),
    "f_float": ({"type": "float"}, lambda i: i / 4),
    "f_half_float": ({"type": "half_float"}, lambda i: i / 2),
    "f_scaled_float": ({"type": "scaled_float", "scaling_factor": 10}, lambda i: i / 10),
    "f_unsigned_long": ({"type": "unsigned_long"}, lambda i: 18_000_000_000_000_000_000 - i),
    "f_date": ({"type": "date"}, lambda i: f"2026-09-{1 + i % 28:02d}T10:00:00Z"),
    "f_date_nanos": ({"type": "date_nanos"}, lambda i: f"2026-09-{1 + i % 28:02d}T10:00:00.123456789Z"),
    "f_boolean": ({"type": "boolean"}, lambda i: i % 2 == 0),
    "f_ip": ({"type": "ip"}, lambda i: f"192.168.{i % 256}.{1 + i % 250}"),
    "f_version": ({"type": "version"}, lambda i: f"1.{i % 10}.{i % 3}"),
    "f_wildcard": ({"type": "wildcard"}, lambda i: f"/path/{i}/file"),
    "f_flattened": ({"type": "flattened"}, lambda i: {"a": f"v{i % 3}", "b": {"c": str(i)}}),
    "f_match_only_text": ({"type": "match_only_text"}, lambda i: f"words of document {i}"),
    "f_text": ({"type": "text", "fields": {"kw": {"type": "keyword"}}}, lambda i: f"text value {i % 17}"),
    "f_text_stored": ({"type": "text", "store": True}, lambda i: f"stored text {i}"),
    "f_constant": ({"type": "constant_keyword", "value": "always"}, lambda i: "always"),
    "f_binary": ({"type": "binary", "doc_values": True}, lambda i: "AAECAwQ="),
    "f_geo_point": ({"type": "geo_point"}, lambda i: {"lat": 10 + i % 50, "lon": 20 + i % 60}),
    "f_geo_shape": ({"type": "geo_shape"}, lambda i: {"type": "point", "coordinates": [i % 90, i % 45]}),
    "f_shape": ({"type": "shape"}, lambda i: {"type": "point", "coordinates": [i, i + 1]}),
    "f_ip_range": ({"type": "ip_range"}, lambda i: {"gte": "10.0.0.0", "lte": f"10.0.{i % 200}.255"}),
    "f_long_range": ({"type": "long_range"}, lambda i: {"gte": i, "lte": i + 10}),
    "f_integer_range": ({"type": "integer_range"}, lambda i: {"gte": i, "lte": i + 5}),
    "f_float_range": ({"type": "float_range"}, lambda i: {"gte": i / 2, "lte": i}),
    "f_double_range": ({"type": "double_range"}, lambda i: {"gte": i / 3, "lte": i}),
    "f_date_range": ({"type": "date_range"}, lambda i: {"gte": "2026-01-01", "lte": f"2026-02-{1 + i % 28:02d}"}),
    "f_counted_keyword": ({"type": "counted_keyword"}, lambda i: ["a", "b", "a"][: 1 + i % 3]),
    "f_rank_feature": ({"type": "rank_feature"}, lambda i: 1 + i % 9),
    "f_rank_features": ({"type": "rank_features"}, lambda i: {"x": 1 + i % 5, "y": 2.5}),
    "f_search_as_you_type": ({"type": "search_as_you_type"}, lambda i: f"quick brown fox {i}"),
    "f_histogram": ({"type": "histogram"}, lambda i: {"values": [0.1, 0.2], "counts": [i % 5, 1]}),
    "f_aggregate": ({"type": "aggregate_metric_double", "metrics": ["min", "max"], "default_metric": "max"},
                    lambda i: {"min": i, "max": i + 1}),
}


def accepted_types(report, types):
    """The `types` Elasticsearch accepts in a synthetic-source index, each tried on its own."""
    kept = {}
    for name, (mapping, value) in types.items():
        probe = "m3_probe_type"
        try:
            create(probe, {"index.mapping.source.mode": "synthetic"}, {"properties": {name: mapping}})
            bulk(probe, [("p", {name: value(1)})])
            kept[name] = (mapping, value)
        except EsError as e:
            report["rejected_types"][mapping["type"]] = e.body[:200]
        finally:
            try:
                es("DELETE", "/" + probe)
            except EsError:
                pass
    return kept


def nested_doc(rng, i):
    doc = {"@timestamp": f"2026-09-{1 + i % 28:02d}T{i % 24:02d}:00:00Z", "server": HOSTS[i % len(HOSTS)]}
    items = []
    for k in range(i % 4):
        item = {"name": f"item-{i}-{k}", "qty": rng.randint(1, 9)}
        if (i + k) % 3 == 1:
            item["parts"] = [{"sku": f"sku-{i}-{k}-{p}"} for p in range(1 + (i + k) % 3)]
        items.append(item)
    if items:
        doc["items"] = items
    return doc


def main():
    if OUTPUT.exists() or REPORT.exists():
        raise RuntimeError(f"{OUTPUT} or {REPORT} already exists; move it away first")
    if WORK.exists():
        shutil.rmtree(WORK)
    snapshots = start_container()
    report = {"rejected_types": {}, "indices": {}}
    try:
        version = es("GET", "/")["version"]["number"]
        report["version"] = version
        license_info = None
        for _ in range(60):
            try:
                license_info = es("GET", "/_license")["license"]
                break
            except EsError:
                time.sleep(1)  # the license service answers 404 for a moment after startup
        if license_info is None:
            raise RuntimeError("no license information")
        if license_info["status"] != "active" or license_info["type"] not in ("trial", "platinum", "enterprise"):
            es("POST", "/_license/start_trial?acknowledge=true")
        rng = random.Random(9251)
        synthetic = {"index.mapping.source.mode": "synthetic"}
        merged = []

        logs = [(f"log-{i:04d}", logs_doc(rng, i)) for i in range(400)]
        create("m3_logsdb", {"index.mode": "logsdb"}, logs_mapping())
        rounds("m3_logsdb", [logs[:200], logs[200:]])
        merged.append("m3_logsdb")

        # Flushed segments are compound whatever index.compound_format says; a forced merge rewrites
        # them under the merge policy. So: a merged non-compound segment of the first round, a flushed
        # compound one of the second round that updates documents of the first (soft deletes), and
        # deletes on top as .liv generations. A commit mixing a prunable and a compound segment.
        create("m3_logsdb_multi", {"index.mode": "logsdb"}, logs_mapping())
        updates = [(f"log-{i:04d}", logs_doc(rng, 1000 + i)) for i in range(0, 200, 7)]
        deletes = [(f"log-{i:04d}", None) for i in range(3, 400, 11)]
        rounds("m3_logsdb_multi", [logs[:200]])
        rounds("m3_logsdb_multi", [logs[200:] + updates, deletes], merge=False)

        create("m3_compound", {"index.mode": "logsdb"}, logs_mapping(), compound=True)
        rounds("m3_compound", [logs[:200], logs[200:]], merge=False)

        create("m3_tsdb", {"index.mode": "time_series", "index.routing_path": ["host"],
                           "index.time_series.start_time": "2026-09-01T00:00:00Z",
                           "index.time_series.end_time": "2026-10-01T00:00:00Z"}, {"properties": {
            "@timestamp": {"type": "date"},
            "host": {"type": "keyword", "time_series_dimension": True},
            "cpu": {"type": "double", "time_series_metric": "gauge"},
            "requests": {"type": "long", "time_series_metric": "counter"},
            "latency": {"type": "histogram"},
            "load": {"type": "aggregate_metric_double", "metrics": ["min", "max", "sum", "value_count"],
                     "default_metric": "max"},
        }})
        metrics = [(None, tsdb_doc(rng, i)) for i in range(400)]
        rounds("m3_tsdb", [metrics[:200], metrics[200:]], op="create")
        merged.append("m3_tsdb")

        for index, candidates in (("m3_types", TYPES), ("m3_types_analyzed", ANALYZED_TYPES)):
            types = accepted_types(report, candidates)
            create(index, synthetic, {"properties": {name: mapping for name, (mapping, _) in types.items()}})
            typed = [(f"t-{i:03d}", {name: value(i) for name, (_, value) in types.items()}) for i in range(300)]
            rounds(index, [typed[:150], typed[150:]])
            merged.append(index)

        create("m3_doc_count", synthetic, {"properties": {"host": {"type": "keyword"}, "hits": {"type": "long"}}})
        counted = [(f"c-{i:03d}", {"host": HOSTS[i % len(HOSTS)], "hits": i, "_doc_count": 1 + i % 5}) for i in range(300)]
        rounds("m3_doc_count", [counted[:150], counted[150:]])
        merged.append("m3_doc_count")

        create("m3_nested", synthetic, {"properties": {
            "@timestamp": {"type": "date"}, "server": {"type": "keyword"},
            "items": {"type": "nested", "properties": {
                "name": {"type": "keyword"}, "qty": {"type": "long"},
                "parts": {"type": "nested", "properties": {"sku": {"type": "keyword"}}}}}}})
        nested = [(f"n-{i:03d}", nested_doc(rng, i)) for i in range(300)]
        rounds("m3_nested", [nested[:150], nested[150:]])
        merged.append("m3_nested")

        create("m3_dense", synthetic, {"properties": {
            "title": {"type": "keyword"},
            "embedding": {"type": "dense_vector", "dims": 8, "index": True, "similarity": "cosine",
                          "index_options": {"type": "int8_hnsw"}}}})
        dense = [(f"d-{i:03d}", {"title": f"doc {i}", "embedding": [round(rng.uniform(-1, 1), 4) for _ in range(8)]})
                 for i in range(300)]
        rounds("m3_dense", [dense[:150], dense[150:]])
        merged.append("m3_dense")

        create("m3_sparse", synthetic, {"properties": {"title": {"type": "keyword"}, "tokens": {"type": "sparse_vector"}}})
        sparse = [(f"s-{i:03d}", {"title": f"doc {i}"} | ({"tokens": {f"t{i % 7}": 1.5, f"u{i % 3}": 0.25}} if i >= 200 else {}))
                  for i in range(300)]
        rounds("m3_sparse", [sparse[:150], sparse[150:]])
        merged.append("m3_sparse")

        create("m3_src_off", {}, {"properties": {"kw": {"type": "keyword"}, "count": {"type": "long"},
                                                 "at": {"type": "date"}, "ip": {"type": "ip"}}})
        source_docs = [(f"o-{i:03d}", {"kw": f"v-{i % 11}", "count": i, "at": f"2026-09-{1 + i % 28:02d}",
                                       "ip": f"10.1.{i % 200}.{1 + i % 250}"}) for i in range(200)]
        bulk("m3_src_off", source_docs[:100])
        flush("m3_src_off")
        es("PUT", "/m3_src_off/_mapping", {"_source": {"enabled": False}})
        bulk("m3_src_off", source_docs[100:])
        flush("m3_src_off")
        es("POST", "/m3_src_off/_forcemerge?max_num_segments=1", timeout=600)
        flush("m3_src_off")

        indices = sorted(merged + ["m3_logsdb_multi", "m3_compound", "m3_src_off"])
        for index in indices:
            report["indices"][index] = {"doc_count": es("GET", f"/{index}/_count")["count"],
                                        "settings": effective(index), "segments": segments(index)}
        for index in merged + ["m3_src_off"]:
            if any(segment["compound"] for segment in report["indices"][index]["segments"]):
                raise RuntimeError(f"{index} has a compound segment")
        for index in merged:
            if str(report["indices"][index]["settings"].get("index.mapping.source.mode")).lower() != "synthetic":
                raise RuntimeError(f"{index} is not synthetic: {report['indices'][index]['settings']}")
        multi = [segment["compound"] for segment in report["indices"]["m3_logsdb_multi"]["segments"]]
        if not (False in multi and True in multi):
            raise RuntimeError(f"m3_logsdb_multi does not mix compound and non-compound segments: {multi}")
        if not all(segment["compound"] for segment in report["indices"]["m3_compound"]["segments"]):
            raise RuntimeError("m3_compound is not compound")

        es("PUT", f"/_snapshot/{REPO}", {"type": "fs", "settings": {"location": f"/snapshots/{REPO}"}})
        snapshot = es("PUT", f"/_snapshot/{REPO}/{REPO}-fixture?wait_for_completion=true",
                      {"indices": ",".join(indices), "include_global_state": False}, timeout=600)["snapshot"]
        if snapshot["state"] != "SUCCESS":
            raise RuntimeError(f"snapshot failed: {snapshot}")
        shutil.copytree(snapshots / REPO, OUTPUT)
        REPORT.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n")
        size = sum(p.stat().st_size for p in OUTPUT.rglob("*") if p.is_file())
        print(json.dumps({"version": version, "rejected_types": sorted(report["rejected_types"]),
                          "indices": {i: report["indices"][i]["doc_count"] for i in indices}}, indent=1))
        print(f"Wrote {OUTPUT} ({size} bytes)")
    finally:
        docker("rm", "-f", CONTAINER, check=False)
        shutil.rmtree(WORK, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
