"""Shadow check: Delta tables against Elasticsearch's own fetch of a read-only restore.

For indices whose stored _source differs from what Elasticsearch serves (excluded vectors put back
by Gefrierschrank, synthetic source), the reference is a restore of the same snapshot on a fresh
gs-bench ES node, read with a PIT and `"_source": {"exclude_vectors": false}`. Per _id: canonical
hash (exact decimals) and the float32 bits of every all-number array. Prints counts only, never
document content.

The repository is registered `readonly`; a filesystem repository (e.g. a committed fixture) is
first copied to a scratch prefix of the bench MinIO, which is removed again, so the source is
never written. Takes the docker slot itself for the whole lifetime of its node, like `gsbench run`;
run it inside the caller's bench hold (GSBENCH_HELD_SLOTS=bench), never under `slot.sh docker`.
"""

import json
import struct
import time
import uuid
from collections import Counter
from pathlib import Path

from . import config, infra
from .canonical import canonical_hash

SHADOW_PREFIX = "shadow-"


def vector_bits(source: str) -> dict:
    """path -> tuple of float32 bit patterns, for every non-empty array holding only numbers."""
    out: dict = {}

    def walk(value, path):
        if isinstance(value, dict):
            for key, child in value.items():
                walk(child, f"{path}.{key}" if path else key)
        elif isinstance(value, list):
            if value and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in value):
                out.setdefault(path, []).append(tuple(struct.unpack(">I", struct.pack(">f", float(v)))[0] for v in value))
            else:
                for child in value:
                    walk(child, path)

    walk(json.loads(source), "")
    return {k: tuple(v) for k, v in out.items()}


def compare(es_docs: dict, rows) -> dict:
    """es_docs: id -> (canonical hash, vector bits); rows: (id, routing, source, kind). Counts only."""
    counts = Counter()
    kinds = Counter()
    seen = set()
    for doc_id, _routing, source, kind in rows:
        counts["rows"] += 1
        kinds[kind or "none"] += 1
        if doc_id in seen:
            counts["duplicates"] += 1
            continue
        seen.add(doc_id)
        expected = es_docs.get(doc_id)
        if expected is None:
            counts["extra"] += 1
            continue
        try:
            if canonical_hash(source) != expected[0]:
                counts["canonical_mismatch"] += 1
            if vector_bits(source) != expected[1]:
                counts["vector_bit_mismatch"] += 1
        except (ValueError, TypeError):
            counts["canonical_mismatch"] += 1
    counts["missing"] = sum(1 for doc_id in es_docs if doc_id not in seen)
    result = {key: counts[key] for key in ("rows", "missing", "extra", "duplicates", "canonical_mismatch",
                                           "vector_bit_mismatch")}
    result["es_docs"] = len(es_docs)
    result["kind_counts"] = dict(sorted(kinds.items()))
    result["passed"] = all(result[k] == 0 for k in ("missing", "extra", "duplicates", "canonical_mismatch",
                                                     "vector_bit_mismatch")) and result["rows"] == len(es_docs)
    return result


def es_documents(url: str, index: str) -> dict:
    from .pit import default_slices, export
    docs: dict = {}

    def sink(_slice_id, hits):
        for hit in hits:
            docs[hit.id] = (canonical_hash(hit.source), vector_bits(hit.source))

    export(url, index, default_slices(url, index), sink, source_param={"exclude_vectors": False})
    return docs


def upload_repository(local: Path, base_path: str) -> None:
    import pyarrow.fs as pafs
    pafs.copy_files(str(local), f"{config.SNAPSHOT_BUCKET}/{base_path}", source_filesystem=pafs.LocalFileSystem(),
                    destination_filesystem=infra.s3fs())


def run(args) -> int:
    from .es import start_node, stop_node
    from .gate import read_rows
    indices = [i for i in args.indices.split(",") if i]
    tables = Path(args.tables)
    scratch = None
    results = {}
    with infra.slot("docker"):
        infra.ensure_minio()
        if args.repo:
            scratch = f"shadow/{uuid.uuid4().hex}"
            infra.log(f"copying the repository to a scratch prefix of {config.SNAPSHOT_BUCKET}")
            upload_repository(Path(args.repo), scratch)
        base_path = scratch or args.s3_base_path
        try:
            es = start_node(args.major)
            try:
                es.start_trial()  # synthetic-source indices need it; vectors do not
                repo = "gs_shadow"
                es.register_repo(repo, base_path, readonly=True)
                snapshots = es.get(f"/_snapshot/{repo}/_all")["snapshots"]
                for index in indices:
                    chosen = [s for s in snapshots if index in s["indices"]
                              and (not args.snapshot or s["snapshot"] == args.snapshot)]
                    if not chosen:
                        raise SystemExit(f"no snapshot of {repo} holds index {index}")
                    snapshot = max(chosen, key=lambda s: s.get("start_time_in_millis", 0))["snapshot"]
                    restored = SHADOW_PREFIX + index
                    started = time.monotonic()
                    es.post(f"/_snapshot/{repo}/{snapshot}/_restore", {
                        "indices": index, "include_global_state": False, "include_aliases": False,
                        "rename_pattern": "(.+)", "rename_replacement": SHADOW_PREFIX + "$1"},
                        params={"wait_for_completion": "true"})
                    es.get(f"/_cluster/health/{restored}", params={"wait_for_status": "yellow", "timeout": "120s"})
                    docs = es_documents(es.url, restored)
                    try:
                        _, rows = read_rows(str(tables / index))
                        result = compare(docs, rows)
                    except Exception as e:  # noqa: BLE001 - a missing or unreadable table is a mismatch
                        result = compare(docs, [])
                        result["table_error"] = type(e).__name__
                    result["seconds"] = round(time.monotonic() - started, 1)
                    results[index] = result
                    infra.log(f"{index}: {json.dumps(result)}")
                    es.delete(f"/{restored}")
            finally:
                stop_node(args.major)
        finally:
            if scratch:
                infra.s3_delete_prefix(f"{config.SNAPSHOT_BUCKET}/{scratch}")
    print(json.dumps(results, indent=2, sort_keys=True))
    if args.out:
        Path(args.out).write_text(json.dumps(results, indent=2, sort_keys=True) + "\n")
    return 0 if all(r["passed"] for r in results.values()) else 1
