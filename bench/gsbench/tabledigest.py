"""Content digests of exported Delta tables, so two exports can be compared table by table.

Every digest is order-independent and ignores file names and commit timing: two exports that wrote the
same rows, schema, shard tags and shard transactions digest equal however the rows were split into
files. Schema, tags and transactions come from replaying the raw Delta log (latest checkpoint plus the
JSON commits after it) rather than from delta-rs, which neither exposes the raw schema string nor
AddFile tags nor the full SetTransaction list. Digests are hashes only: no document content is printed.
"""

import datetime
import decimal
import fnmatch
import hashlib
import json
import os
import re
import sys
from pathlib import Path

DIGEST_HEX = 16
ROW_FIELDS = ("rows", "rowDigest")
_CHECKPOINT = re.compile(r"^(\d{20})\.checkpoint(?:\.(\d{10})\.(\d{10}))?\.parquet$")
_COMMIT = re.compile(r"^(\d{20})\.json$")


def find_tables(root: Path) -> list[Path]:
    """Every directory under root (root included) holding a _delta_log, sorted by relative path."""
    found = []
    for dirpath, dirnames, _ in os.walk(root):
        if "_delta_log" in dirnames:
            found.append(Path(dirpath))
            dirnames.remove("_delta_log")
    return sorted(found, key=lambda p: relative_name(root, p))


def relative_name(root: Path, table: Path) -> str:
    return table.relative_to(root).as_posix()


def _short(h) -> str:
    return h.hexdigest()[:DIGEST_HEX]


def _json_default(value):
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, (decimal.Decimal, datetime.timedelta)):
        return str(value)
    raise TypeError(f"cannot digest a value of type {type(value).__name__}")


def row_hash(row: dict) -> str:
    encoded = json.dumps(row, sort_keys=True, separators=(",", ":"), default=_json_default)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _sorted_hash(items: list[str]) -> str:
    h = hashlib.sha256()
    for item in sorted(items):
        h.update(item.encode("utf-8"))
        h.update(b"\n")
    return _short(h)


def _as_dict(value) -> dict:
    """A Delta map as a dict: JSON commits give dicts, Parquet checkpoints lists of (key, value) pairs."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return dict(value)


def _checkpoint_actions(log: Path) -> tuple[int, list[dict]]:
    """(version, actions) of the newest complete checkpoint, or (-1, []) without one."""
    parts: dict[int, dict[int, Path]] = {}
    expected: dict[int, int] = {}
    for f in log.iterdir():
        m = _CHECKPOINT.match(f.name)
        if m:
            version = int(m.group(1))
            parts.setdefault(version, {})[int(m.group(2) or 1)] = f
            expected[version] = int(m.group(3) or 1)
    complete = [v for v, p in parts.items() if len(p) == expected[v]]
    if not complete:
        return -1, []
    import pyarrow.parquet as pq
    version = max(complete)
    actions = []
    for _, f in sorted(parts[version].items()):
        actions.extend(pq.read_table(f).to_pylist())
    return version, actions


def replay_log(table: Path) -> dict:
    """Active AddFiles (path -> tags), SetTransaction app ids and the schema string of the table."""
    log = table / "_delta_log"
    checkpoint, actions = _checkpoint_actions(log)
    commits = sorted((int(m.group(1)), f) for f in log.iterdir() if (m := _COMMIT.match(f.name)))
    expected_next = checkpoint + 1
    for version, f in commits:
        if version <= checkpoint:
            continue
        if version != expected_next:
            raise ValueError(f"{table}: Delta log has no commit {expected_next} (next is {version})")
        expected_next += 1
        with f.open(encoding="utf-8") as lines:
            actions.extend(json.loads(line) for line in lines if line.strip())
    if expected_next == 0:
        raise ValueError(f"{table}: empty Delta log")
    files: dict[str, dict] = {}
    txns: set[str] = set()
    schema = None
    for action in actions:
        if action.get("add"):
            files[action["add"]["path"]] = _as_dict(action["add"].get("tags"))
        elif action.get("remove"):
            files.pop(action["remove"]["path"], None)
        elif action.get("txn"):
            txns.add(action["txn"]["appId"])
        elif action.get("metaData"):
            schema = action["metaData"]["schemaString"]
    if schema is None:
        raise ValueError(f"{table}: Delta log has no metaData action")
    return {"files": files, "txns": txns, "schema": schema}


def _row_digest(table: Path, files: int) -> tuple[int, str]:
    from deltalake import DeltaTable
    dt = DeltaTable(str(table))
    if len(dt.file_uris()) != files:
        raise ValueError(f"{table}: delta-rs sees {len(dt.file_uris())} files, the log replay {files}")
    hashes = []
    for batch in dt.to_pyarrow_dataset().to_batches(batch_size=4096):
        hashes.extend(row_hash(row) for row in batch.to_pylist())
    return len(hashes), _sorted_hash(hashes)


def digest_table(table: Path, name: str) -> dict:
    log = replay_log(table)
    rows, row_digest = _row_digest(table, len(log["files"]))
    tags = [json.dumps(t, sort_keys=True, separators=(",", ":")) for t in log["files"].values()]
    return {"table": name, "rows": rows, "rowDigest": row_digest,
            "schema": _short(hashlib.sha256(log["schema"].encode("utf-8"))),
            "tags": _sorted_hash(tags), "txns": _sorted_hash(list(log["txns"])), "files": len(log["files"])}


def digest(root: str, out: str, tables: str | None = None) -> int:
    root_path = Path(root)
    if not root_path.is_dir():
        print(f"digest: {root} is not a directory", file=sys.stderr)
        return 2
    patterns = [t for t in (tables or "").split(",") if t]
    selected = [(relative_name(root_path, t), t) for t in find_tables(root_path)]
    if patterns:
        selected = [(n, t) for n, t in selected if any(fnmatch.fnmatchcase(n, p) for p in patterns)]
    lines = [json.dumps(digest_table(t, n)) for n, t in selected]
    out_path = Path(out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("".join(line + "\n" for line in lines), encoding="utf-8")
    print(f"digested {len(lines)} tables under {root} into {out}")
    return 0


def load_digests(path: str) -> dict[str, dict]:
    """Digest lines by table; a directory stands for the digests.jsonl that fixture-digests.sh writes."""
    p = Path(path)
    if p.is_dir():
        p = p / "digests.jsonl"
    result = {}
    for line in p.read_text(encoding="utf-8").splitlines():
        if line.strip():
            record = json.loads(line)
            result[record["table"]] = record
    return result


def load_run_reports(root: Path) -> list[tuple[Path, dict]]:
    """Every per-index run report (a JSON object with indexName and shards) under root, at any depth."""
    found = []
    for dirpath, dirnames, filenames in os.walk(root):
        if "_delta_log" in dirnames:
            dirnames.remove("_delta_log")
        for name in filenames:
            if not name.endswith(".json"):
                continue
            path = Path(dirpath) / name
            try:
                report = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if isinstance(report, dict) and "indexName" in report and isinstance(report.get("shards"), list):
                found.append((path, report))
    return sorted(found, key=lambda entry: entry[0])


def summarize_report(shards: list[dict]) -> dict:
    modes: dict[str, int] = {}
    for shard in shards:
        mode = shard.get("fetch") or "absent"
        modes[mode] = modes.get(mode, 0) + 1
    return {"shards": len(shards), "fetch": dict(sorted(modes.items())),
            "bytesFetched": sum(shard.get("bytesFetched") or 0 for shard in shards),
            "bytesProcessed": sum(shard.get("bytesProcessed") or 0 for shard in shards)}


def fetch_modes(root: str, per_report: bool = False) -> int:
    """Prints the fetch modes and byte sums of every shard of every run report under root (content-free)."""
    root_path = Path(root)
    if not root_path.is_dir():
        print(f"fetch-modes: {root} is not a directory", file=sys.stderr)
        return 2
    reports = load_run_reports(root_path)
    every = []
    for path, report in reports:
        shards = report["shards"]
        every.extend(shards)
        if per_report:
            print(json.dumps({"report": relative_name(root_path, path), **summarize_report(shards)}))
    print(json.dumps({"reports": len(reports), **summarize_report(every)}))
    return 0


def compare_digests(a: str, b: str, rows_only: bool = False, strict: bool = False) -> int:
    left, right = load_digests(a), load_digests(b)
    common = sorted(left.keys() & right.keys())
    mismatches = 0
    for name in common:
        keys = ROW_FIELDS if rows_only else sorted((left[name].keys() | right[name].keys()) - {"table"})
        diffs = [f"{k} {left[name].get(k)} != {right[name].get(k)}" for k in keys if left[name].get(k) != right[name].get(k)]
        if diffs:
            mismatches += 1
            print(f"MISMATCH {name}: " + "; ".join(diffs))
    only_a = sorted(left.keys() - right.keys())
    only_b = sorted(right.keys() - left.keys())
    for name in only_a:
        print(f"ONLY-A {name}")
    for name in only_b:
        print(f"ONLY-B {name}")
    print(f"{len(common)} common tables, {mismatches} mismatching, {len(only_a)} only in A, {len(only_b)} only in B")
    if mismatches or (strict and (only_a or only_b)):
        return 1
    return 0
