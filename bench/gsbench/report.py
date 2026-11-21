"""Markdown rendering of one result JSON."""

# CPU seconds per wall second of one export slice above which the exporter, not ES, bounds it
CLIENT_BOUND = 0.8


def fmt_s(v) -> str:
    return "-" if v is None else f"{v:.1f}"


def fmt_b(v) -> str:
    if v is None:
        return "-"
    for unit, scale in (("GB", 1e9), ("MB", 1e6), ("KB", 1e3)):
        if abs(v) >= scale:
            return f"{v / scale:.1f} {unit}"
    return f"{v:.0f} B"


def fmt_n(v) -> str:
    return "-" if v is None else f"{v:,.0f}"


def med(summary, fmt=fmt_s) -> str:
    if not summary:
        return "-"
    if summary["n"] == 1 or summary["min"] == summary["max"]:
        return fmt(summary["median"])
    return f"{fmt(summary['median'])} ({fmt(summary['min'])}-{fmt(summary['max'])}, ±{summary['spread_pct']}%)"


def s3_apis(s3: dict) -> str:
    parts = []
    for bucket, v in s3.items():
        apis = ", ".join(f"{api} {n:,}" for api, n in v["requests"].items())
        parts.append(f"{bucket}: {apis or '-'} (read {fmt_b(v['tx_bytes'])}, written {fmt_b(v['rx_bytes'])})")
    return "; ".join(parts) or "-"


def ratio(g: dict, e: dict) -> str:
    """Baseline export+transform over Gefrierschrank per-index elapsed (medians; restore excluded)."""
    if not (g.get("elapsed_s") and e.get("export_s") and e.get("transform_s")) or not g["elapsed_s"]["median"]:
        return "-"
    return f"{(e['export_s']['median'] + e['transform_s']['median']) / g['elapsed_s']['median']:.2f}x"


def render(r: dict) -> str:
    s = r["summary"]
    m = r["machine"]
    out = [f"# gsbench {r['run_id']}", ""]
    out.append(f"- jar: `{r['jar']['name']}` {r['jar'].get('build_info') or ''} sha256 {r['jar']['sha256'][:12]}")
    if r.get("worker_jar"):
        out.append(f"- worker jar: `{r['worker_jar']['name']}` sha256 {r['worker_jar']['sha256'][:12]}")
    out.append(f"- suite {r.get('suite', 'default')}, scale {r['scale']}, seed {r['seed']}, {r['repeat']} repeat(s), "
               f"target {r['target']}, ES export slices {r['slices']}")
    if r.get("jvm_args") or r.get("gs_args"):
        out.append(f"- jvm args `{' '.join(r.get('jvm_args') or [])}`, gs args `{' '.join(r.get('gs_args') or [])}`")
    out.append(f"- machine: {m.get('cpu')}, {m.get('cores')} cores, {fmt_b(m.get('memory_bytes'))}; docker VM "
               f"{(m.get('docker') or {}).get('cpus')} CPUs {fmt_b((m.get('docker') or {}).get('memory_bytes'))}; "
               f"{m.get('java')}")
    out.append(f"- load average start {r['load_start']} end {r['load_end']}; dataset prepare {r['prepare_seconds']}s, "
               f"whole run {r['seconds']}s"
               + (f" (of which {r['bench_slot_wait_seconds']}s waiting for the bench slot)"
                  if r.get("bench_slot_wait_seconds") else ""))
    out.append("- values: median (min-max, ±spread) over repeats; wall and CPU in seconds")
    out.append("")

    gs_last = {n: i for m in (r.get("routes", {}).get("gs", {}).get("repeats") or [{}])[-1].get("majors", {}).values()
               for n, i in m["indices"].items()}
    out += ["## Gate", "", "| index | ES | kind | docs | rows GS / baseline | GS | GS outcome (quarantined/shards) "
            "| baseline | expected |", "|---|---|---|---|---|---|---|---|---|"]
    for name, p in s["per_index"].items():
        g = p.get("gs", {})
        e = p.get("es", {})
        exp = (r["gate"].get("gs", {}).get(name) or {}).get("expected")
        exp_txt = f"{exp['finding']} {exp['outcome']}" if exp else ""
        outcome = g.get("outcome", "")
        last = gs_last.get(name) or {}
        if last.get("total_shards") is not None:
            outcome += f" {last.get('quarantined_shards')}/{last.get('total_shards')}"
        if g.get("quarantine_errors"):
            outcome += " " + ",".join(g["quarantine_errors"])
        out.append(f"| {name} | {p['major']} | {p['kind']} | {fmt_n(p['docs'])} | {fmt_n(g.get('rows'))} / "
                   f"{fmt_n(e.get('rows'))} | {g.get('status', '-')} | {outcome} | {e.get('status', '-')} | {exp_txt} |")
    violations = r.get("invariants")
    if violations is not None:
        out += ["", "Invariants (no write to the snapshot repository, no Parquet file without AddFile, no "
                "document ids or content in reports and manifests): "
                + ("all hold." if not violations else "**VIOLATED**")]
        out += [f"- {v}" for v in violations]
    fails = [(route, i, g) for route, ix in r["gate"].items() for i, g in ix.items() if g["status"] != "pass"]
    if fails:
        out += ["", "Non-passing checks:", ""]
        for route, i, g in fails:
            failed = [k for k, v in (g.get("checks") or {}).items() if v is False]
            c = g.get("counts") or {}
            out.append(f"- {route}/{i}: {g['status']}; failed {', '.join(failed) or '-'}; rows {c.get('rows')}/"
                       f"{c.get('expected_rows')}, missing {c.get('missing_ids')}, extra {c.get('extra_ids')}, "
                       f"raw-bad {c.get('raw_mismatch')}, canonical-bad {c.get('canonical_mismatch')}"
                       + (f", table {g['table_error']}" if g.get("table_error") else ""))
    out.append("")

    if "gs" in s:
        g = s["gs"]
        out += ["## Gefrierschrank route (all majors, whole process)", "",
                "| metric | value |", "|---|---|",
                f"| wall s | {med(g['wall_s'])} |",
                f"| CPU s (user+sys) | {med(g['cpu_s'])} |",
                f"| peak RSS | {med(g['peak_rss_bytes'], fmt_b)} |",
                f"| peak temp+output disk | {med(g['peak_disk_bytes'], fmt_b)} |",
                f"| S3 requests (GET) | {med(g['s3_requests'], fmt_n)} ({med(g['s3_get'], fmt_n)}) |",
                f"| S3 bytes read | {med(g['s3_tx_bytes'], fmt_b)} |",
                f"| output bytes | {med(g['output_bytes'], fmt_b)} |",
                *([f"| S3 output requests / bytes written | {med(g['s3_output_requests'], fmt_n)} / "
                   f"{med(g['s3_output_rx_bytes'], fmt_b)} |"] if "s3_output_requests" in g else []),
                f"| data files (AddFiles) | {med(g['output_parquet_files'], fmt_n)} ({med(g['output_add_files'], fmt_n)}) |",
                f"| gate-passing indices: summed per-index elapsed s | {med(g['passing_elapsed_s'])} |", ""]
        reps = r["routes"]["gs"]["repeats"]
        out += ["| snapshot | wall s | CPU s | peak RSS | peak disk | exit codes |", "|---|---|---|---|---|---|"]
        for mj, v in g["per_major"].items():
            codes = ",".join(str(rep["majors"][mj]["exit_code"]) for rep in reps)
            out.append(f"| {mj} | {med(v['wall_s'])} | {med(v['cpu_s'])} | {med(v['peak_rss_bytes'], fmt_b)} "
                       f"| {med(v['peak_disk_bytes'], fmt_b)} | {codes} |")
        out += ["", "S3 requests per API (last repeat): " + s3_apis(reps[-1]["total"]["s3"]), ""]
    if "es" in s:
        e = s["es"]
        out += ["## Baseline route (restore -> PIT/search_after NDJSON -> delta-rs)", "",
                "| phase | wall s | CPU s | peak RSS |", "|---|---|---|---|"]
        for phase in ("restore", "export", "transform"):
            v = e[phase]
            out.append(f"| {phase} | {med(v['wall_s'])} | {med(v['cpu_s'])} | {med(v['peak_rss_bytes'], fmt_b)} |")
        t = e["total"]
        es_reps = r["routes"]["es"]["repeats"]
        out.append(f"| **total** | {med(t['wall_s'])} | {med(t['cpu_s'])} | {med(t['peak_rss_bytes'], fmt_b)} |")
        out += ["", f"Peak disk (ES data + NDJSON + Delta) {med(t['peak_disk_bytes'], fmt_b)}; S3 requests "
                f"{med(e['s3_requests'], fmt_n)} (GET {med(e['s3_get'], fmt_n)}), bytes read {med(e['s3_tx_bytes'], fmt_b)}; "
                f"output {med(e['output_bytes'], fmt_b)} in {med(e['output_parquet_files'], fmt_n)} files; "
                f"gate-passing indices export+transform {med(e['passing_export_transform_s'])} s. "
                "ES CPU is the container cgroup; peak RSS of export = ES container + exporter. "
                "Export wall and CPU run from PIT open to close with warm slice workers (one process per slice); "
                f"starting the workers took {med(e.get('client_startup_s'))} s in total and is not in the totals. "
                f"Busiest slice: {e.get('max_slice_cpu_per_wall')} CPU s per wall s"
                + (" (**client-bound**: the export phase measures the exporter, not ES)"
                   if (e.get("max_slice_cpu_per_wall") or 0) > CLIENT_BOUND else "") + "."
                + (" Starting the ES nodes (not in the totals): "
                   + ", ".join(f"{v} {s_:.0f} s" for v, s_ in e["node_startup_s"].items()) + "."
                   if e.get("node_startup_s") else ""), "",
                "S3 requests per API during restore (last repeat): " + s3_apis(es_reps[-1]["total"]["s3"]), ""]
    if "gs" in s and "es" in s:
        gw, ew = s["gs"]["wall_s"], s["es"]["total"]["wall_s"]
        gc, ec = s["gs"]["cpu_s"], s["es"]["total"]["cpu_s"]
        if gw and ew:
            out.append(f"Baseline / Gefrierschrank, route totals: wall {ew['median'] / gw['median']:.2f}x, "
                       f"CPU {ec['median'] / gc['median']:.2f}x (>1 means Gefrierschrank is cheaper; totals include "
                       "indices a route got wrong).")
        gb, eb = s["gs"].get("both_passing_elapsed_s"), s["es"].get("both_passing_export_transform_s")
        if gb and eb and gb["median"]:
            out.append(f"Baseline / Gefrierschrank over the {len(s.get('both_passing', []))} indices both routes "
                       f"export correctly: {eb['median'] / gb['median']:.2f}x wall (baseline export+transform "
                       f"{fmt_s(eb['median'])} s vs Gefrierschrank per-index elapsed {fmt_s(gb['median'])} s; restore, "
                       "node and JVM start excluded).")
        out.append("")

    out += ["## Per index (numbers only for gate-passing indices)", "",
            "| index | docs | source | GS elapsed s | GS output | GS files | ES export s | export CPU s ES / client "
            "| busiest slice | ES transform s | ES output | ES/GS time |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for name, p in s["per_index"].items():
        g, e = p.get("gs") or {}, p.get("es") or {}
        cpu = (f"{med(e.get('export_es_cpu_s'))} / {med(e.get('export_client_cpu_s'))}"
               if e.get("export_es_cpu_s") else "-")
        busiest = e.get("max_slice_cpu_per_wall")
        out.append(f"| {name} | {fmt_n(p['docs'])} | {fmt_b(p['source_bytes'])} | {med(g.get('elapsed_s'))} "
                   f"| {fmt_b(g.get('output_bytes'))} | {fmt_n(g.get('parquet_files'))} | {med(e.get('export_s'))} "
                   f"| {cpu} | {'-' if busiest is None else f'{busiest:.2f}'} "
                   f"| {med(e.get('transform_s'))} | {fmt_b(e.get('output_bytes'))} | {ratio(g, e)} |")
    out.append("")
    return "\n".join(out)
