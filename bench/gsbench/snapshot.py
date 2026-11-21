"""Dataset build: index generated documents on a real ES node, capture the oracle, snapshot.

One snapshot repository base path per ES major and dataset key. A cached dataset (oracle files
plus a marker) is reused as long as its snapshot still exists in the benchmark MinIO.
"""

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path

from . import config, generate
from .canonical import canonical_hash, raw_hash
from .datasets import STORED, SUITES, SYNTHETIC, VECTORS_EXCLUDED, IndexSpec, dataset_key, specs
from .es import Es, Node
from .infra import ensure_minio, log, s3_delete_prefix, s3_exists
from . import pit


@dataclass
class Dataset:
    major: int
    key: str
    dir: Path
    meta: dict

    @property
    def base_path(self) -> str:
        return self.meta["base_path"]

    @property
    def snapshot(self) -> str:
        return self.meta["snapshot"]

    @property
    def indices(self) -> dict:
        return self.meta["indices"]

    def oracle_path(self, index: str) -> Path:
        return self.dir / "oracle" / f"{index}.jsonl"

    def truth_path(self, index: str) -> Path:
        return self.dir / "truth" / f"{index}.json"


def dataset_dir(major: int, key: str) -> Path:
    return config.CACHE_DIR / f"es{major}-{key}"


def load_cached(major: int, scale: str, seed: int, suite: str = "default") -> Dataset | None:
    key = dataset_key(major, scale, seed, config.ES_NODES[major].version, suite)
    d = dataset_dir(major, key)
    meta_file = d / "dataset.json"
    if not (d / "complete").exists() or not meta_file.exists():
        return None
    meta = json.loads(meta_file.read_text())
    if not s3_exists(f"{config.SNAPSHOT_BUCKET}/{meta['base_path']}/index.latest"):
        log(f"cached dataset {d.name} has no snapshot in MinIO any more; rebuilding")
        return None
    return Dataset(major, key, d, meta)


def prepare(scale: str, seed: int, majors=None, rebuild: bool = False, suite: str = "default") -> dict[int, Dataset]:
    ensure_minio()
    out = {}
    for major in majors or SUITES[suite]:
        ds = None if rebuild else load_cached(major, scale, seed, suite)
        if ds is None:
            ds = build(major, scale, seed, suite)
        else:
            log(f"reusing cached dataset {ds.dir.name}")
        out[major] = ds
    return out


def _ingest(es: Es, spec: IndexSpec, ops_batches, truth: generate.Truth) -> None:
    for batch in ops_batches:
        pending: list[generate.Op] = []

        def flush():
            if not pending:
                return
            r = es.post("/_bulk", generate.bulk_body(pending, spec.name))
            if r["errors"]:
                first = next(i for i in r["items"] if next(iter(i.values())).get("error"))
                raise RuntimeError(f"bulk into {spec.name} failed: {json.dumps(first)[:800]}")
            pending.clear()

        for op in batch:
            if generate.needs_single_request(op):
                flush()
                params = {"routing": op.routing} if op.routing else None
                r = es.http.put(f"{es.url}/{spec.name}/_doc/{op.id}", data=op.source.encode(), params=params,
                                headers={"Content-Type": "application/json"}, timeout=120)
                if r.status_code not in (200, 201):
                    raise RuntimeError(f"index {spec.name}/{op.id}: {r.status_code} {r.text[:500]}")
            else:
                pending.append(op)
            truth.apply(op)
        flush()
        if not spec.force_merge:
            es.post(f"/{spec.name}/_refresh")


def _disk_usage(es: Es, index: str) -> dict:
    r = es.post(f"/{index}/_disk_usage", params={"run_expensive_tasks": "true"})
    usage = r[index]
    fields = usage.get("fields", {})

    def stored(name):
        return fields.get(name, {}).get("stored_fields_in_bytes", 0)

    return {"store_size_in_bytes": usage.get("store_size_in_bytes"),
            "source_stored_bytes": stored("_source"), "id_stored_bytes": stored("_id"),
            "stored_fields_total_bytes": usage.get("all_fields", {}).get("stored_fields_in_bytes")}


def _segments(es: Es, index: str) -> tuple[int, int]:
    """(segments, deleted docs still in them: hard and soft deletes, nested children included)."""
    r = es.get(f"/{index}/_segments")
    shards = r["indices"][index]["shards"]
    segments = [seg for copies in shards.values() for copy in copies if copy["routing"]["primary"]
                for seg in copy["segments"].values()]
    return len(segments), sum(seg.get("deleted_docs", 0) for seg in segments)


def capture_oracle(es: Es, spec: IndexSpec, out: Path) -> dict:
    """Per live root doc: (_id, _routing, canonical hash, raw hash) as served by ES itself."""
    rows = []

    def sink(_slice, hits):
        for h in hits:
            rows.append([h.id, h.routing, canonical_hash(h.source), raw_hash(h.source),
                         len(h.source.encode()), '"embedding"' in h.source])

    stats = pit.export(es.url, spec.name, max(spec.shards, 2), sink, source_param=spec.source_param)
    rows.sort(key=lambda r: (r[0] or "", r[1] or ""))
    digest = hashlib.sha256()
    with out.open("w", encoding="utf-8") as f:
        for doc_id, routing, canon, raw, _, _ in rows:
            line = json.dumps([doc_id, routing, canon, raw], ensure_ascii=False)
            f.write(line + "\n")
            digest.update(line.encode() + b"\n")
    return {"count": len(rows), "digest": digest.hexdigest(), "rows": rows, "search_requests": stats["search_requests"]}


def _verify(spec: IndexSpec, oracle: dict, truth: generate.Truth, disk: dict, es: Es, deleted_docs: int) -> list[str]:
    """Prove the dataset is what its spec claims before anything is benchmarked on it."""
    problems = []
    rows = oracle["rows"]
    if (spec.updates or spec.deletes) and not deleted_docs:
        # otherwise merges expunged them and the deleted/resurrection checks would test nothing
        problems.append("updates/deletes left no deleted docs in the snapshot's segments")
    if oracle["count"] != truth.count:
        problems.append(f"ES count {oracle['count']} != generated {truth.count}")
    ids = [r[0] for r in rows]
    if len(set(ids)) != len(ids):
        problems.append("duplicate _id in ES oracle")
    if truth.generated_ids == 0:
        es_ids = {r[0]: r[1] for r in rows}
        if set(es_ids) != set(truth.live):
            problems.append(f"ES ids differ from generated ids ({len(set(es_ids) ^ set(truth.live))} differ)")
        elif any(es_ids[i] != truth.live[i][0] for i in es_ids):
            problems.append("ES routing differs from generated routing")
    if spec.kind == STORED:
        bad = sum(1 for r in rows if truth.live.get(r[0], (None, None))[1] != r[3])
        if bad:
            problems.append(f"{bad} stored sources are not byte-identical to the generated bytes")
        if not disk["source_stored_bytes"]:
            problems.append("no stored _source")
    elif spec.kind == SYNTHETIC:
        if disk["source_stored_bytes"]:
            problems.append("_source is stored: synthetic source not active (license?)")
    elif spec.kind == VECTORS_EXCLUDED:
        if not all(r[5] for r in rows):
            problems.append("oracle sources lack re-inserted vectors")
        r = es.post(f"/{spec.name}/_search", {"size": 3})
        if any("embedding" in h["_source"] for h in r["hits"]["hits"]):
            problems.append("vectors are not excluded from _source by default")
    return problems


def build(major: int, scale: str, seed: int, suite: str = "default") -> Dataset:
    node = config.ES_NODES[major]
    key = dataset_key(major, scale, seed, node.version, suite)
    d = dataset_dir(major, key)
    (d / "oracle").mkdir(parents=True, exist_ok=True)
    (d / "truth").mkdir(parents=True, exist_ok=True)
    (d / "complete").unlink(missing_ok=True)
    base_path = f"es{major}-{key}"
    snapshot = f"bench-{key}"
    meta = {"major": major, "es_version": node.version, "suite": suite, "scale": scale, "seed": seed, "key": key,
            "base_path": base_path, "snapshot": snapshot, "bucket": config.SNAPSHOT_BUCKET, "indices": {}}
    started = time.monotonic()
    s3_delete_prefix(f"{config.SNAPSHOT_BUCKET}/{base_path}")
    with Node(major) as es:
        if major >= 8:
            es.start_trial()
        es.register_repo(config.SNAPSHOT_REPO, base_path, readonly=False)
        for i, spec in enumerate(specs(major, suite)):
            docs = spec.doc_count(scale)
            log(f"es{major}: building {spec.name} ({docs} docs, {spec.note})")
            t0 = time.monotonic()
            es.put(f"/{spec.name}", spec.body())
            truth = generate.Truth()
            index_seed = seed * 1000 + major * 100 + i
            _ingest(es, spec, generate.operations(spec.profile, docs, index_seed, spec.updates, spec.deletes,
                                                  spec.routed), truth)
            if spec.force_merge:
                es.post(f"/{spec.name}/_forcemerge", params={"max_num_segments": "1"}, timeout=3600)
            es.post(f"/{spec.name}/_refresh")
            es.post(f"/{spec.name}/_flush")
            disk = _disk_usage(es, spec.name)
            segments, deleted_docs = _segments(es, spec.name)
            oracle = capture_oracle(es, spec, d / "oracle" / f"{spec.name}.jsonl")
            problems = _verify(spec, oracle, truth, disk, es, deleted_docs)
            if problems:
                raise RuntimeError(f"dataset {spec.name} invalid: {'; '.join(problems)}")
            (d / "truth" / f"{spec.name}.json").write_text(json.dumps(
                {"live": truth.live, "deleted": truth.deleted, "generated_ids": truth.generated_ids},
                ensure_ascii=False))
            meta["indices"][spec.name] = {
                "kind": spec.kind, "profile": spec.profile, "shards": spec.shards, "note": spec.note,
                "count": oracle["count"], "oracle_digest": oracle["digest"], "deleted_ids": len(truth.deleted),
                "segments": segments, "deleted_docs_in_segments": deleted_docs, "disk_usage": disk,
                "source_param": spec.source_param,
                "source_bytes": sum(r[4] for r in oracle["rows"]),
                "build_seconds": round(time.monotonic() - t0, 1)}
            log(f"es{major}: {spec.name} ok: {oracle['count']} live docs, {segments} segments, {deleted_docs} "
                f"deleted docs in them, oracle {oracle['digest'][:12]}")
        r = es.put(f"/_snapshot/{config.SNAPSHOT_REPO}/{snapshot}", {
            "indices": ",".join(meta["indices"]), "include_global_state": False},
            params={"wait_for_completion": "true"}, timeout=3600)
        snap = r["snapshot"]
        if snap["state"] != "SUCCESS" or snap["shards"]["failed"]:
            raise RuntimeError(f"snapshot {snapshot} failed: {json.dumps(snap)[:1000]}")
    meta["build_seconds"] = round(time.monotonic() - started, 1)
    (d / "dataset.json").write_text(json.dumps(meta, indent=2))
    (d / "complete").write_text("")
    log(f"es{major}: dataset {d.name} complete in {meta['build_seconds']}s")
    return Dataset(major, key, d, meta)


def load_oracle(path: Path) -> dict:
    """id -> (routing, canonical hash, raw hash)."""
    out = {}
    with path.open(encoding="utf-8") as f:
        for line in f:
            doc_id, routing, canon, raw = json.loads(line)
            out[doc_id] = (routing, canon, raw)
    return out


def load_truth(path: Path) -> dict:
    return json.loads(path.read_text())
