import json

from gsbench import compare


def _summary(v):
    return {"median": v, "min": v, "max": v, "n": 1, "spread_pct": 0.0}


def _result(label, wall, status):
    gs = {k: _summary(wall) for k in ("wall_s", "cpu_s", "peak_rss_bytes", "peak_disk_bytes", "s3_requests",
                                       "s3_get", "s3_tx_bytes", "output_bytes", "output_parquet_files",
                                       "output_add_files", "passing_elapsed_s")}
    return {"label": label, "jar": {"build_info": f"gefrierschrank {label}"}, "scale": "smoke", "seed": 42,
            "datasets": {"es9": {"key": "k"}},
            "summary": {"gs": gs, "per_index": {"logs-lz4": {"gs": {"status": status, "elapsed_s": _summary(wall)}}}},
            "gate": {"gs": {"logs-lz4": {"status": status}}}}


def test_ratios_and_exit_codes(tmp_path):
    a, b, bad = tmp_path / "a.json", tmp_path / "b.json", tmp_path / "bad.json"
    a.write_text(json.dumps(_result("main", 10.0, "pass")))
    b.write_text(json.dumps(_result("candidate", 5.0, "pass")))
    bad.write_text(json.dumps(_result("broken", 5.0, "fail")))
    text, failures = compare.render(json.loads(a.read_text()), json.loads(b.read_text()))
    assert "0.50x" in text and not failures
    assert compare.main(str(a), str(b)) == 0
    assert compare.main(str(a), str(bad)) == 1


def _with_gs_table(r, schema, violations=()):
    r["invariants"] = list(violations)
    r["routes"] = {"gs": {"repeats": [{"rep": 1, "majors": {"es9": {"indices": {
        "logs-lz4": {"output": {"schema": schema}}}}}}]}}
    return r


def test_invariant_violations_and_schema_changes_fail_the_comparison():
    schema = [["_id", "string", True], ["_source", "string", True]]
    a = _with_gs_table(_result("main", 10.0, "pass"), schema)
    same = _with_gs_table(_result("candidate", 5.0, "pass"), schema)
    text, failures = compare.render(a, same)
    assert not failures and "Invariants hold in B" in text
    changed = _with_gs_table(_result("candidate", 5.0, "pass"), schema[:1])
    assert compare.render(a, changed)[1] == [f"document-table schema changed: logs-lz4: {schema} -> {schema[:1]}"]
    wrote = _with_gs_table(_result("candidate", 5.0, "pass"), schema, ["gs rep 1 es9: wrote to the snapshot"])
    text, failures = compare.render(a, wrote)
    assert failures and "Invariants violated in B" in text
