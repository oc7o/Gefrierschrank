"""Builds an ILM-shaped repository: SLM and ILM searchable_snapshot share one repository.

applogs-a, applogs-b and plain-c (one shard, three documents each) are snapshotted hot by SLM
(slm-2026.09.20). applogs-a/-b then each get an ILM snapshot (2026.09.21-<name>-frozen-policy),
are mounted from it as partial-<name> (full_copy) and deleted. A second SLM snapshot
(slm-2026.09.22) holds the shells and plain-c. Each original's latest snapshot is exactly what
its shell mounts.

usage: generate.py <es-url> <repository-dir> <out-dir>
"""
import json, shutil, sys, time, urllib.request

BASE, REPO_DIR, OUT = sys.argv[1:4]


def es(method, path, body=None, timeout=600):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def snapshot(name, indices):
    es("PUT", f"/_snapshot/r/{name}?wait_for_completion=true", {"indices": ",".join(indices), "include_global_state": False})
    time.sleep(1.1)


for _ in range(150):
    try:
        es("GET", "/"); break
    except Exception:
        time.sleep(2)
es("PUT", "/_snapshot/r", {"type": "fs", "settings": {"location": "/repo/r"}})
names = ["applogs-a", "applogs-b", "plain-c"]
for name in names:
    es("PUT", f"/{name}", {"settings": {"number_of_shards": 1, "number_of_replicas": 0}})
    for n in range(3):
        es("PUT", f"/{name}/_doc/{name}-{n}",
           {"@timestamp": f"2026-09-{20 + n}T00:00:00Z", "message": f"{name} doc {n}", "n": n})
    es("POST", f"/{name}/_flush")
    es("GET", f"/_cluster/health/{name}?wait_for_status=green&timeout=120s")
snapshot("slm-2026.09.20", names)
for name in ["applogs-a", "applogs-b"]:
    ilm = f"2026.09.21-{name}-frozen-policy"
    es("PUT", f"/{name}/_settings", {"index.blocks.write": True})
    snapshot(ilm, [name])
    es("POST", f"/_snapshot/r/{ilm}/_mount?wait_for_completion=true&storage=full_copy",
       {"index": name, "renamed_index": f"partial-{name}", "index_settings": {"index.number_of_replicas": 0}})
    es("GET", f"/_cluster/health/partial-{name}?wait_for_status=green&timeout=300s", timeout=400)
    es("DELETE", f"/{name}")
snapshot("slm-2026.09.22", ["partial-applogs-a", "partial-applogs-b", "plain-c"])
shutil.copytree(REPO_DIR + "/r", OUT)
print("snapshots:", [(s["snapshot"], s["indices"]) for s in es("GET", "/_snapshot/r/_all")["snapshots"]])
