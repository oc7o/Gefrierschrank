"""Generates the 16-index ILM repository of bench/shells on an Elasticsearch 9.3 node.

The shape of the repository: 8 single-shard indices applogs-1..8 of 30,000 ECS-like documents,
snapshotted hot by SLM (slm-2026.09.20), then each snapshotted by ILM
(2026.09.21-applogs-N-frozen-policy), mounted from that snapshot as partial-applogs-N
(full_copy) and deleted, and the shells snapshotted by SLM (slm-2026.09.21, slm-2026.09.22).
Each original's latest snapshot is exactly what its shell mounts, so survey reports 8
exact-identity groups. With --retention the shells are re-mounted as Elasticsearch does after a
restart of the frozen tier, SLM takes slm-2026.09.23 and retention deletes slm-2026.09.21/22.

Seeded, so a rerun builds the same documents (the repository's UUIDs differ).
usage: generate.py <es-url> <out-dir> [--retention]
"""
import json
import random
import shutil
import sys
import time
import urllib.request

BASE = sys.argv[1]
OUT = sys.argv[2]
RETENTION = "--retention" in sys.argv[3:]
INDICES = 8
DOCS = 30_000
BATCH = 3_000
REPO = "r"


def es(method, path, body=None, timeout=900, ndjson=None):
    data = ndjson.encode() if ndjson is not None else (None if body is None else json.dumps(body).encode())
    content_type = "application/x-ndjson" if ndjson is not None else "application/json"
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Content-Type": content_type})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def wait_for_node():
    for _ in range(150):
        try:
            es("GET", "/")
            return
        except Exception:
            time.sleep(2)
    raise SystemExit("Elasticsearch did not start")


WORDS = ("alpha beta gamma delta epsilon zeta eta theta iota kappa lambda omicron sigma tau upsilon "
         "request response timeout retry cache session token upstream downstream payload").split()
LEVELS = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR"]
SERVICES = ["checkout", "search", "auth", "billing", "inventory", "gateway", "mailer"]


def document(rng, index_no, n):
    message = " ".join(rng.choice(WORDS) for _ in range(rng.randint(30, 60)))
    doc = {
        "@timestamp": f"2026-09-{1 + n % 19:02d}T{n % 24:02d}:{n % 60:02d}:{(n * 7) % 60:02d}.{n % 1000:03d}Z",
        "message": message,
        "log": {"level": rng.choice(LEVELS), "logger": f"app.{rng.choice(SERVICES)}.handler"},
        "service": {"name": rng.choice(SERVICES), "version": f"1.{rng.randint(0, 9)}.{rng.randint(0, 30)}"},
        "host": {"name": f"host-{rng.randint(1, 64):03d}", "ip": f"10.{index_no}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"},
        "http": {"request": {"method": rng.choice(["GET", "POST", "PUT"]), "bytes": rng.randint(100, 90_000)},
                 "response": {"status_code": rng.choice([200, 200, 200, 201, 404, 500]), "time_ms": round(rng.random() * 900, 3)}},
        "user": {"id": f"u{rng.randint(1, 50_000)}"},
        "trace": {"id": "%032x" % rng.getrandbits(128)},
        "labels": {"env": rng.choice(["prod", "staging"]), "region": rng.choice(["eu-1", "us-2", "ap-3"])},
        "tags": rng.sample(WORDS, 3),
    }
    if n % 11 == 0:
        doc["error"] = {"type": "TimeoutException", "stack_trace": " ".join(rng.choice(WORDS) for _ in range(40))}
    return doc


def build_index(index_no):
    name = f"applogs-{index_no}"
    es("PUT", f"/{name}", {"settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1"},
                           "mappings": {"properties": {"@timestamp": {"type": "date"}, "message": {"type": "text"}}}})
    rng = random.Random(1000 + index_no)
    for start in range(0, DOCS, BATCH):
        lines = []
        for n in range(start, min(DOCS, start + BATCH)):
            action = {"_id": f"{name}-{n}"}
            if n % 7 == 0:
                action["routing"] = f"tenant-{n % 5}"
            lines.append(json.dumps({"index": action}))
            lines.append(json.dumps(document(rng, index_no, n), separators=(",", ":")))
        result = es("POST", f"/{name}/_bulk", ndjson="\n".join(lines) + "\n")
        if result.get("errors"):
            raise SystemExit(f"bulk errors in {name}")
    deleted = 0
    for n in range(3, DOCS, 97):
        routing = f"?routing=tenant-{n % 5}" if n % 7 == 0 else ""
        es("DELETE", f"/{name}/_doc/{name}-{n}{routing}")
        deleted += 1
    es("POST", f"/{name}/_flush")
    es("PUT", f"/{name}/_settings", {"index.blocks.write": True})
    es("POST", f"/{name}/_forcemerge?max_num_segments=1")
    es("GET", f"/_cluster/health/{name}?wait_for_status=green&timeout=120s")
    return name, DOCS - deleted


def snapshot(name, indices):
    es("PUT", f"/_snapshot/{REPO}/{name}?wait_for_completion=true",
       {"indices": ",".join(indices), "include_global_state": False})
    time.sleep(1.1)  # distinct start times, so "latest" is unambiguous


def mount(snapshot_name, index, shell):
    es("POST", f"/_snapshot/{REPO}/{snapshot_name}/_mount?wait_for_completion=true&storage=full_copy",
       {"index": index, "renamed_index": shell, "index_settings": {"index.number_of_replicas": 0}})
    es("GET", f"/_cluster/health/{shell}?wait_for_status=green&timeout=300s", timeout=400)


def main():
    wait_for_node()
    es("PUT", f"/_snapshot/{REPO}", {"type": "fs", "settings": {"location": f"/repo/{REPO}"}})
    counts = dict(build_index(i) for i in range(1, INDICES + 1))
    originals = sorted(counts, key=lambda n: int(n.split("-")[1]))
    snapshot("slm-2026.09.20", originals)
    ilm = {}
    for name in originals:
        ilm[name] = f"2026.09.21-{name}-frozen-policy"
        snapshot(ilm[name], [name])
        mount(ilm[name], name, f"partial-{name}")
        es("DELETE", f"/{name}")
    shells = [f"partial-{name}" for name in originals]
    snapshot("slm-2026.09.21", shells)
    snapshot("slm-2026.09.22", shells)
    shutil.copytree(f"{OUT}/repos/{REPO}", f"{OUT}/ilm-repo")
    expected = {"documents": counts, "rows": sum(counts.values()),
                "ilm-repo": [s["snapshot"] for s in es("GET", f"/_snapshot/{REPO}/_all")["snapshots"]]}
    if RETENTION:
        for name in originals:
            es("DELETE", f"/partial-{name}")
            mount(ilm[name], name, f"partial-{name}")
        snapshot("slm-2026.09.23", shells)
        es("DELETE", f"/_snapshot/{REPO}/slm-2026.09.21,slm-2026.09.22")
        shutil.copytree(f"{OUT}/repos/{REPO}", f"{OUT}/ilm-repo-retained")
        expected["ilm-repo-retained"] = [s["snapshot"] for s in es("GET", f"/_snapshot/{REPO}/_all")["snapshots"]]
    with open(f"{OUT}/expected.json", "w") as f:
        json.dump(expected, f, indent=1)
    print(json.dumps(expected, indent=1))


main()
