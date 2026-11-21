"""Per-document correctness gate. A route's numbers count only for indices that pass it.

Checks per index, against the oracle Elasticsearch served before the snapshot was taken:
- row count equals the live root count;
- (_id, _routing) set equality (no missing, extra or resurrected deleted ids);
- no duplicate _id;
- stored source: sha256 of the exported _source UTF-8 bytes equals the raw hash (which the
  dataset build proved equal to the generator's bytes);
- every index: canonical hash equals ES's (synthetic source and re-inserted vectors included);
- _source_kind as expected, where the table has that column.
"""

import fnmatch
import json
import os
import shutil
import tempfile
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import yaml

from . import config
from .canonical import canonical_hash, raw_hash
from .datasets import STORED, SYNTHETIC, VECTORS_EXCLUDED

EXPECTED_KIND = {STORED: {"stored"}, SYNTHETIC: {"synthetic"},
                 VECTORS_EXCLUDED: {"stored", "stored+vectors", "stored+vectors-reserialized"}}
COLUMNS = ("_id", "_routing", "_source", "_source_kind")


def read_rows(table: str, storage_options: dict | None = None) -> tuple[bool, list]:
    """(has _source_kind column, lazy rows of (_id, _routing, _source, _source_kind))."""
    from deltalake import DeltaTable
    dt = DeltaTable(table, storage_options=storage_options)
    dataset = dt.to_pyarrow_dataset()
    names = [c for c in COLUMNS if c in dataset.schema.names]
    has_kind = "_source_kind" in names

    def rows():
        for batch in dataset.to_batches(columns=names, batch_size=4096):
            cols = [batch.column(n).to_pylist() for n in names]
            if not has_kind:
                cols.append([None] * batch.num_rows)
            yield from zip(*cols)

    return has_kind, rows()


def check_rows(rows, oracle: dict, deleted: dict, kind: str, has_kind: bool = True) -> dict:
    """oracle: id -> (routing, canonical, raw); deleted: id -> routing (ids whose last op was a delete)."""
    expected_kind = EXPECTED_KIND[kind]
    seen: dict = {}
    counts = {"rows": 0, "duplicates": 0, "extra_ids": 0, "resurrected_deleted": 0, "null_ids": 0,
              "routing_mismatch": 0, "raw_mismatch": 0, "canonical_mismatch": 0, "kind_mismatch": 0,
              "invalid_json": 0}
    for doc_id, routing, source, source_kind in rows:
        counts["rows"] += 1
        if doc_id is None:
            counts["null_ids"] += 1
            continue
        if doc_id in seen:
            counts["duplicates"] += 1
            continue
        seen[doc_id] = routing
        expected = oracle.get(doc_id)
        if expected is None:
            counts["extra_ids"] += 1
            if doc_id in deleted:
                counts["resurrected_deleted"] += 1
            continue
        if routing != expected[0]:
            counts["routing_mismatch"] += 1
        if source is None:
            counts["canonical_mismatch"] += 1
            counts["raw_mismatch"] += kind == STORED
            continue
        if kind == STORED and raw_hash(source) != expected[2]:
            counts["raw_mismatch"] += 1
        try:
            if canonical_hash(source) != expected[1]:
                counts["canonical_mismatch"] += 1
        except ValueError:
            counts["invalid_json"] += 1
            counts["canonical_mismatch"] += 1
        if has_kind and (source_kind is None or (expected_kind is not None and source_kind not in expected_kind)):
            counts["kind_mismatch"] += 1
    counts["missing_ids"] = sum(1 for i in oracle if i not in seen)
    counts["expected_rows"] = len(oracle)
    checks = {
        "row_count": counts["rows"] == len(oracle),
        "id_routing_set": counts["missing_ids"] == 0 and counts["extra_ids"] == 0 and counts["null_ids"] == 0
                          and counts["routing_mismatch"] == 0,
        "no_duplicate_id": counts["duplicates"] == 0,
        "raw_hash": counts["raw_mismatch"] == 0 if kind == STORED else None,
        "canonical_hash": counts["canonical_mismatch"] == 0,
        "source_kind": counts["kind_mismatch"] == 0 if has_kind else None,
    }
    return {"passed": all(v for v in checks.values() if v is not None), "checks": checks, "counts": counts}


def gate_table(table: str | None, oracle_path: str, truth_path: str, kind: str, storage_options=None) -> dict:
    from .snapshot import load_oracle, load_truth
    oracle = load_oracle(Path(oracle_path))
    deleted = load_truth(Path(truth_path))["deleted"]
    try:
        if not table:
            raise FileNotFoundError("no table")
        has_kind, rows = read_rows(table, storage_options)
        result = check_rows(rows, oracle, deleted, kind, has_kind)
        table_error = None
    except Exception as e:  # noqa: BLE001 - a missing/unreadable table is a gate failure, not a crash
        result = check_rows([], oracle, deleted, kind, False)
        table_error = type(e).__name__
    if table_error:
        result["table_error"] = table_error
    return result


# Memory of one gate process per byte of its oracle file: the oracle and seen-id dicts take about
# 2.5x (measured on 200k oracle lines); the rest covers Arrow batches and the interpreter.
GATE_BYTES_PER_ORACLE_BYTE = 5


def gate_workers(jobs: list[dict], available: int | None = None) -> int:
    """Parallel gate processes that fit in half the available memory (at least one)."""
    import psutil
    available = psutil.virtual_memory().available if available is None else available
    largest = max(Path(j["oracle"]).stat().st_size for j in jobs) * GATE_BYTES_PER_ORACLE_BYTE
    return max(1, min(len(jobs), os.cpu_count() or 1, int(available // 2 // max(largest, 1))))


def gate_many(jobs: list[dict], workers: int = 0) -> list[dict]:
    """jobs: dicts with table, oracle, truth, kind. Runs in parallel processes, as many as memory allows."""
    if not jobs:
        return []
    with ProcessPoolExecutor(max_workers=workers or gate_workers(jobs)) as pool:
        futures = [pool.submit(gate_table, j["table"], j["oracle"], j["truth"], j["kind"], j.get("storage_options"))
                   for j in jobs]
        return [f.result() for f in futures]


# ── expected failures ───────────────────────────────────────────────────────────────────────────

def load_expected(path: Path = config.EXPECTED_FAILURES) -> list[dict]:
    if not path.exists():
        return []
    return yaml.safe_load(path.read_text()).get("expected", []) or []


def matching_expected(entries: list[dict], label: str, version: str | None, dataset: str,
                      worker_jar: bool) -> list[dict]:
    """Every entry that applies to this index, in file order."""
    out = []
    for e in entries:
        if not fnmatch.fnmatch(dataset, e["dataset"]):
            continue
        if "label" in e and not fnmatch.fnmatch(label or "", e["label"]):
            continue
        if "version" in e and not fnmatch.fnmatch(version or "", e["version"]):
            continue
        if "worker_jar" in e and e["worker_jar"] != ("present" if worker_jar else "absent"):
            continue
        out.append(e)
    return out


def match_expected(entries: list[dict], label: str, version: str | None, dataset: str, worker_jar: bool) -> dict | None:
    found = matching_expected(entries, label, version, dataset, worker_jar)
    return found[0] if found else None


LOUD_OUTCOMES = ("QUARANTINED", "FAILED")
# A loud failure may lose rows (the quarantined or failed shards), never corrupt the rows it committed.
CORRUPTION_COUNTS = ("duplicates", "extra_ids", "null_ids", "routing_mismatch", "raw_mismatch",
                     "canonical_mismatch", "kind_mismatch", "invalid_json")


def only_rows_missing(counts: dict) -> bool:
    return all(counts.get(k, 0) == 0 for k in CORRUPTION_COUNTS)


def resolve(result: dict, outcome: str | None, entry: dict | None) -> str:
    """Final status of one index on the GS route, from its gate result and export outcome.

    pass / fail; xfail (the expected failure happened, and every failed check is one the entry
    tolerates); loud-failure (expected, the export ended FAILED or QUARANTINED, and every row it
    did commit is correct); xpass (the expected failure did not happen)."""
    if entry is None:
        return "pass" if result["passed"] else "fail"
    if result["passed"]:
        return "xpass"
    if entry["outcome"] == "xfail":
        failed = {k for k, v in result["checks"].items() if v is False}
        tolerated = entry.get("tolerates")
        return "xfail" if tolerated is None or failed <= set(tolerated) else "fail"
    if entry["outcome"] == "loud-failure":
        return "loud-failure" if outcome in LOUD_OUTCOMES and only_rows_missing(result["counts"]) else "fail"
    raise ValueError(f"unknown expected outcome {entry['outcome']}")


def resolve_any(result: dict, outcome: str | None, entries: list[dict]) -> tuple[str, dict | None]:
    """resolve() against every matching entry: the first one that accepts the failure wins, so a
    narrow entry (e.g. an xfail limited to canonical_hash) does not shadow a broader one."""
    if not entries:
        return resolve(result, outcome, None), None
    for e in entries:
        status = resolve(result, outcome, e)
        if status != "fail":
            return status, e
    return "fail", entries[0]


WORST = ["fail", "xpass", "loud-failure", "xfail", "pass"]


def worst(statuses: list[str]) -> str:
    return min(statuses, key=WORST.index) if statuses else "fail"


# ── self-test ───────────────────────────────────────────────────────────────────────────────────

def _corruptions(rows: list[dict], deleted: dict, kind: str) -> dict[str, tuple[list[dict], set[str]]]:
    """Corrupted copies of `rows` and the exact set of checks each must fail."""
    content = {"raw_hash", "canonical_hash"} if kind == STORED else {"canonical_hash"}

    def flip(text: str) -> str:
        pos = next(k for k, ch in enumerate(text) if ch.isdigit())
        return text[:pos] + ("1" if text[pos] != "1" else "2") + text[pos + 1:]

    cases = {
        "clean": (rows, set()),
        "dropped row": (rows[1:], {"row_count", "id_routing_set"}),
        "flipped byte": ([dict(rows[0], _source=flip(rows[0]["_source"]))] + rows[1:], content),
        "duplicated row": (rows + [rows[len(rows) // 2]], {"row_count", "no_duplicate_id"}),
    }
    if kind == STORED:
        # canonically equal, so only the byte-exact check may catch it
        respaced = rows[0]["_source"].replace(",", ", ", 1)
        cases["whitespace only"] = ([dict(rows[0], _source=respaced)] + rows[1:], {"raw_hash"})
    if deleted:
        gone, routing = next(iter(deleted.items()))
        cases["resurrected deleted id"] = (rows + [dict(rows[0], _id=gone, _routing=routing)],
                                           {"row_count", "id_routing_set"})
    return cases


def _generated_table() -> tuple[list[dict], dict, dict]:
    from . import generate
    truth = generate.Truth()
    sources = {}
    for batch in generate.operations("logs", 300, 7, updates=0.1, deletes=0.1, routed=0.3, batch=100):
        for op in batch:
            truth.apply(op)
            if op.kind == "index":
                sources[op.id] = op.source
            else:
                sources.pop(op.id, None)
    oracle = {i: (r, canonical_hash(sources[i]), sha) for i, (r, sha) in truth.live.items()}
    rows = [{"_id": i, "_routing": oracle[i][0], "_source": sources[i], "_source_kind": "stored"} for i in sorted(oracle)]
    return rows, oracle, truth.deleted


def self_test(verbose: bool = True, table: str | None = None, oracle_path: str | None = None,
              truth_path: str | None = None, kind: str = STORED) -> bool:
    """Corrupt copies of a table in every way the gate targets; each copy must fail exactly the
    checks aimed at its corruption, and the clean copy must pass. Without `table` the rows come
    from the generator; with it, from an exported table (all its columns kept) and its dataset oracle."""
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake

    schema = None
    if table is None:
        rows, oracle, deleted = _generated_table()
    else:
        from .snapshot import load_oracle, load_truth
        arrow = DeltaTable(table).to_pyarrow_table()
        schema, rows = arrow.schema, arrow.to_pylist()
        oracle, deleted = load_oracle(Path(oracle_path)), load_truth(Path(truth_path))["deleted"]
    ok = True
    tmp = Path(tempfile.mkdtemp(prefix="gsbench-selftest-"))
    try:
        for name, (case_rows, must_fail) in _corruptions(rows, deleted, kind).items():
            path = tmp / name.replace(" ", "-")
            write_deltalake(str(path), pa.Table.from_pylist(case_rows, schema=schema))
            has_kind, table_rows = read_rows(str(path))
            result = check_rows(table_rows, oracle, deleted, kind, has_kind)
            failed = {k for k, v in result["checks"].items() if v is False}
            detected = failed == must_fail
            if name == "resurrected deleted id":
                detected = detected and result["counts"]["resurrected_deleted"] == 1
            ok &= detected
            if verbose:
                print(f"  {'ok  ' if detected else 'MISS'} {name:24s} failed checks: {sorted(failed) or '-'}"
                      + ("" if detected else f" (expected {sorted(must_fail) or '-'})"))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return ok


def summary_line(result: dict) -> str:
    c = result["counts"]
    failed = [k for k, v in result["checks"].items() if v is False]
    return (f"rows {c['rows']}/{c['expected_rows']} missing {c['missing_ids']} extra {c['extra_ids']} "
            f"dup {c['duplicates']} raw-bad {c['raw_mismatch']} canon-bad {c['canonical_mismatch']}"
            + (f" FAILED {','.join(failed)}" if failed else ""))


def dumps(obj) -> str:
    return json.dumps(obj, indent=2, sort_keys=True)
