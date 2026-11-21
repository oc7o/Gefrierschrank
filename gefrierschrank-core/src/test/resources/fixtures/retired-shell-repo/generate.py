"""Builds a small retired-shell repository on a throwaway ES 9.3 container.

logs-a (2 shards) is snapshotted as ilm-backing, mounted as partial-logs-a (full_copy) and
deleted, as ILM's frozen phase does. SLM snapshots the shell as slm-1 (state 'before'), then as
slm-2, and retention deletes slm-1 (state 'after').
"""
import json, shutil, sys, time, urllib.request

BASE = sys.argv[1]
REPO_DIR = sys.argv[2]
OUT = sys.argv[3]

def es(method, path, body=None, timeout=600):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)

for _ in range(150):
    try:
        es("GET", "/"); break
    except Exception:
        time.sleep(2)
es("PUT", "/_snapshot/r", {"type": "fs", "settings": {"location": "/repo/r"}})
es("PUT", "/logs-a", {"settings": {"number_of_shards": 2, "number_of_replicas": 0},
                      "mappings": {"properties": {"message": {"type": "text"}, "n": {"type": "long"}}}})
docs = [
    ("a1", None, {"message": "alpha", "n": 1, "tags": ["x", "y"]}),
    ("a2", None, {"message": "beta", "n": 2.50, "nested": {"k": "v"}}),
    ("a3", "tenant", {"message": "routed", "n": 3}),
    ("a4", None, {"message": "gamma  spaced", "n": 4, "price": 1.50}),
    ("a5", None, {"message": "to delete", "n": 5}),
    ("a6", None, {"message": "unicode äöü 😀", "n": 6}),
    ("a7", None, {"n": 7, "message": "key order kept"}),
]
for doc_id, routing, source in docs:
    path = f"/logs-a/_doc/{doc_id}" + (f"?routing={routing}" if routing else "")
    es("PUT", path, source)
es("DELETE", "/logs-a/_doc/a5")
es("POST", "/logs-a/_flush")
es("PUT", "/logs-a/_settings", {"index.blocks.write": True})
es("GET", "/_cluster/health/logs-a?wait_for_status=green&timeout=120s")
es("PUT", "/_snapshot/r/ilm-backing?wait_for_completion=true", {"indices": "logs-a", "include_global_state": False})
es("POST", "/_snapshot/r/ilm-backing/_mount?wait_for_completion=true&storage=full_copy",
   {"index": "logs-a", "renamed_index": "partial-logs-a", "index_settings": {"index.number_of_replicas": 0}})
es("GET", "/_cluster/health/partial-logs-a?wait_for_status=green&timeout=300s", timeout=400)
es("DELETE", "/logs-a")
es("PUT", "/_snapshot/r/slm-1?wait_for_completion=true", {"indices": "partial-logs-a", "include_global_state": False})
shutil.copytree(REPO_DIR + "/r", OUT + "/before")
time.sleep(1.5)
es("PUT", "/_snapshot/r/slm-2?wait_for_completion=true", {"indices": "partial-logs-a", "include_global_state": False})
es("DELETE", "/_snapshot/r/slm-1")
shutil.copytree(REPO_DIR + "/r", OUT + "/after")
print("snapshots:", [(s["snapshot"], s["uuid"], s["indices"]) for s in es("GET", "/_snapshot/r/_all")["snapshots"]])
