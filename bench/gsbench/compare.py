"""`gsbench compare A.json B.json`: ratios B/A, and a non-zero exit on unexpected gate failures,
invariant violations or a changed document-table schema in B."""

import json
from pathlib import Path

from . import invariants
from .report import fmt_b, fmt_n, fmt_s


def _m(summary):
    return summary["median"] if summary else None


def _ratio(a, b) -> str:
    if a in (None, 0) or b is None:
        return "-"
    return f"{b / a:.2f}x"


def _row(label, a, b, fmt=fmt_s) -> str:
    return f"| {label} | {fmt(a)} | {fmt(b)} | {_ratio(a, b)} |"


def render(a: dict, b: dict) -> tuple[str, list[str]]:
    out = [f"# gsbench compare: A = {a['label']} ({a['jar'].get('build_info')}), "
           f"B = {b['label']} ({b['jar'].get('build_info')})", ""]
    da = (a.get("suite", "default"), a["scale"], a["seed"])
    db = (b.get("suite", "default"), b["scale"], b["seed"])
    if da != db:
        out += [f"**Warning:** different datasets (A {'/'.join(map(str, da))}, B {'/'.join(map(str, db))}).", ""]
    for d in sorted(set(a.get("datasets", {})) & set(b.get("datasets", {}))):
        if a["datasets"][d]["key"] != b["datasets"][d]["key"]:
            out += [f"**Warning:** dataset {d} differs (key {a['datasets'][d]['key']} vs {b['datasets'][d]['key']}).", ""]
    sa, sb = a["summary"], b["summary"]
    if "gs" in sa and "gs" in sb:
        ga, gb = sa["gs"], sb["gs"]
        out += ["## Gefrierschrank route (B/A: <1 is better for time, CPU, memory, disk, requests)", "",
                "| metric | A | B | B/A |", "|---|---|---|---|",
                _row("wall s", _m(ga["wall_s"]), _m(gb["wall_s"])),
                _row("CPU s", _m(ga["cpu_s"]), _m(gb["cpu_s"])),
                _row("peak RSS", _m(ga["peak_rss_bytes"]), _m(gb["peak_rss_bytes"]), fmt_b),
                _row("peak disk", _m(ga["peak_disk_bytes"]), _m(gb["peak_disk_bytes"]), fmt_b),
                _row("S3 requests", _m(ga["s3_requests"]), _m(gb["s3_requests"]), fmt_n),
                _row("S3 GET", _m(ga["s3_get"]), _m(gb["s3_get"]), fmt_n),
                _row("S3 bytes read", _m(ga["s3_tx_bytes"]), _m(gb["s3_tx_bytes"]), fmt_b),
                _row("output bytes", _m(ga["output_bytes"]), _m(gb["output_bytes"]), fmt_b),
                _row("data files", _m(ga["output_parquet_files"]), _m(gb["output_parquet_files"]), fmt_n),
                _row("AddFiles", _m(ga["output_add_files"]), _m(gb["output_add_files"]), fmt_n), ""]
    if "es" in sa and "es" in sb:
        ea, eb = sa["es"], sb["es"]
        out += ["## Baseline route (same datasets: ratios near 1.00 show machine noise)", "",
                "| phase | A wall s | B wall s | B/A |", "|---|---|---|---|"]
        for phase in ("restore", "export", "transform"):
            out.append(_row(phase, _m(ea[phase]["wall_s"]), _m(eb[phase]["wall_s"])))
        out += [_row("total", _m(ea["total"]["wall_s"]), _m(eb["total"]["wall_s"])), ""]
    pa, pb = sa["per_index"], sb["per_index"]
    # Route totals include indices a build got wrong (a quarantine can take seconds where a correct
    # export takes minutes), so the like-for-like rows only sum indices that pass in A and in B.
    common = [n for n in pb if _m((pa.get(n) or {}).get("gs", {}).get("elapsed_s"))
              and _m((pb[n].get("gs") or {}).get("elapsed_s"))]
    if common:
        ta = sum(_m(pa[n]["gs"]["elapsed_s"]) for n in common)
        tb = sum(_m(pb[n]["gs"]["elapsed_s"]) for n in common)
        out += [f"Like for like, the {len(common)} indices Gefrierschrank exports correctly in A and B "
                f"(summed per-index elapsed): A {fmt_s(ta)} s, B {fmt_s(tb)} s, B/A {_ratio(ta, tb)}.", ""]
    for tag, s in (("A", sa), ("B", sb)):
        if "gs" in s and "es" in s and _m(s["gs"]["wall_s"]):
            line = (f"- {tag}: baseline/Gefrierschrank route totals: wall "
                    f"{_m(s['es']['total']['wall_s']) / _m(s['gs']['wall_s']):.2f}x, "
                    f"CPU {_m(s['es']['total']['cpu_s']) / _m(s['gs']['cpu_s']):.2f}x")
            gb, eb = s["gs"].get("both_passing_elapsed_s"), s["es"].get("both_passing_export_transform_s")
            if _m(gb) and _m(eb):
                line += (f"; over the {len(s.get('both_passing', []))} indices both routes export correctly "
                         f"{_m(eb) / _m(gb):.2f}x wall")
            out.append(line)
    out += ["", "## Per index", "",
            "| index | GS gate A | GS gate B | ES gate B | GS elapsed A | B | B/A | output A | B | files A | B |",
            "|---|---|---|---|---|---|---|---|---|---|---|"]
    for name in pb:
        x, y = (pa.get(name) or {}).get("gs") or {}, pb[name].get("gs") or {}
        ye = pb[name].get("es") or {}
        ea_, eb_ = _m(x.get("elapsed_s")), _m(y.get("elapsed_s"))
        out.append(f"| {name} | {x.get('status', '-')} | {y.get('status', '-')} | {ye.get('status', '-')} "
                   f"| {fmt_s(ea_)} | {fmt_s(eb_)} | {_ratio(ea_, eb_)} | {fmt_b(x.get('output_bytes'))} "
                   f"| {fmt_b(y.get('output_bytes'))} | {fmt_n(x.get('parquet_files'))} | {fmt_n(y.get('parquet_files'))} |")
    failures = [f"{route}/{i}" for route, ix in b["gate"].items() for i, g in ix.items() if g["status"] == "fail"]
    xpass = [f"{route}/{i}" for route, ix in b["gate"].items() for i, g in ix.items() if g["status"] == "xpass"]
    out.append("")
    if failures:
        out.append(f"**Unexpected gate failures in B:** {', '.join(failures)}")
    if xpass:
        out.append(f"Expected failures that now pass in B (update expected-failures.yaml): {', '.join(xpass)}")
    if not failures:
        out.append("No unexpected gate failures in B.")
    violated = list(b.get("invariants") or [])
    violated += [f"document-table schema changed: {c}" for c in invariants.schema_changes(a, b)]
    if violated:
        out += ["", "**Invariants violated in B:**", *[f"- {v}" for v in violated]]
    elif "invariants" in b:
        out.append("Invariants hold in B (no source writes, no orphan Parquet files, no report leaks"
                   + (", document-table schema as in A)." if "invariants" in a else ")."))
    return "\n".join(out) + "\n", failures + violated


def main(path_a: str, path_b: str, output: str | None = None) -> int:
    a = json.loads(Path(path_a).read_text())
    b = json.loads(Path(path_b).read_text())
    text, failures = render(a, b)
    print(text)
    if output:
        Path(output).write_text(text)
    return 1 if failures else 0
