"""Product invariants a benchmark run can check on the side, beyond the per-document gate:
nothing is written to the source repository, no Parquet file outlives its failed shard, and
reports and manifests carry no document content.
"""

import json
import re
from pathlib import Path

# Values only documents carry (generator constants), never field names or index metadata.
CONTENT_MARKERS = ("shop.example.com", "com.example.", "Mozilla/5.0", "okhttp/", "python-requests/")
_SPLIT = re.compile(r"[\s\"'\[\]{}(),:;=<>\\|/.]+")


def _report_text(path: Path) -> str:
    text = path.read_text(encoding="utf-8", errors="replace")
    try:
        # unescape \uXXXX so non-ASCII ids match
        return json.dumps(json.loads(text), ensure_ascii=False)
    except ValueError:
        return text


def report_leaks(report_dirs: list[Path], oracle_paths: list[Path]) -> dict[str, dict]:
    """{report file: {"ids": n, "markers": [...]}} for every report or manifest that contains a
    document _id of the oracles (purely numeric ids excluded: they collide with counters) or a
    content marker. Only counts are returned, never the leaked text itself."""
    tokens: dict[str, set[str]] = {}
    texts: dict[str, str] = {}
    for d in report_dirs:
        for f in sorted(Path(d).rglob("*")):
            if f.is_file():
                texts[str(f)] = _report_text(f)
                tokens[str(f)] = {t for t in _SPLIT.split(texts[str(f)]) if t and not t.isdigit()}
    if not texts:
        return {}
    every = set().union(*tokens.values())
    leaked_ids = set()
    for oracle in oracle_paths:
        with open(oracle, encoding="utf-8") as f:
            for line in f:
                doc_id = json.loads(line)[0]
                if doc_id in every:
                    leaked_ids.add(doc_id)
    out = {}
    for name, text in texts.items():
        ids = len(tokens[name] & leaked_ids)
        markers = [m for m in CONTENT_MARKERS if m in text]
        if ids or markers:
            out[name] = {"ids": ids, "markers": markers}
    return out


def violations(result: dict) -> list[str]:
    """Human-readable invariant violations of a finished run (empty when all hold)."""
    out = []
    for rep in result.get("routes", {}).get("gs", {}).get("repeats", []):
        for major, m in rep["majors"].items():
            if m.get("source_writes"):
                out.append(f"gs rep {rep['rep']} {major}: wrote to the snapshot repository {m['source_writes']}")
            for name, i in m["indices"].items():
                if i["output"].get("orphan_files"):
                    out.append(f"gs rep {rep['rep']} {name}: {i['output']['orphan_files']} Parquet file(s) no "
                               "AddFile references (uncommitted output left behind)")
    for f, leak in result.get("report_leaks", {}).items():
        out.append(f"gs report {Path(f).name} ({Path(f).parent.parent.name}): {leak['ids']} document id(s), "
                   f"content markers {leak['markers'] or '-'}")
    return out


def schema_changes(a: dict, b: dict) -> list[str]:
    """Indices whose Gefrierschrank document-table column schema differs between two results."""
    def schemas(r):
        out = {}
        for rep in r.get("routes", {}).get("gs", {}).get("repeats", []):
            for m in rep["majors"].values():
                for name, i in m["indices"].items():
                    if i["output"].get("schema"):
                        out.setdefault(name, i["output"]["schema"])
        return out

    sa, sb = schemas(a), schemas(b)
    return [f"{name}: {sa[name]} -> {sb[name]}" for name in sorted(set(sa) & set(sb)) if sa[name] != sb[name]]
