import json

from gsbench import invariants


def test_report_leaks_find_ids_and_content_but_not_numbers(tmp_path):
    oracle = tmp_path / "oracle.jsonl"
    oracle.write_text("\n".join(json.dumps([i, None, "c", "r"], ensure_ascii=False)
                                for i in ["AAAAAAABxY-z_9kLmNoPq", "dök-7-🧊", "1000000042"]) + "\n", encoding="utf-8")
    reports = tmp_path / "report"
    reports.mkdir()
    (reports / "manifest.json").write_text(json.dumps({"rows": 1000000042, "indexName": "logs-zstd"}))
    (reports / "clean.json").write_text(json.dumps({"errorClass": "NullPointerException", "shards": [{"shard": 0}]}))
    (reports / "leak.json").write_text(json.dumps({"message": "bad doc dök-7-\U0001f9ca in shop.example.com"}))
    (reports / "leak2.json").write_text('{"m":"id=AAAAAAABxY-z_9kLmNoPq."}')
    leaks = invariants.report_leaks([reports], [oracle])
    assert {k.rsplit("/", 1)[1]: v for k, v in leaks.items()} == {
        "leak.json": {"ids": 1, "markers": ["shop.example.com"]}, "leak2.json": {"ids": 1, "markers": []}}


def _result(writes=None, orphans=0, schema=None):
    idx = {"output": {"orphan_files": orphans, "schema": schema or [["_id", "string", True]]}}
    return {"routes": {"gs": {"repeats": [{"rep": 1, "majors": {"es9": {"source_writes": writes or {},
                                                                         "indices": {"logs-lz4": idx}}}}]}}}


def test_violations_and_schema_changes():
    assert invariants.violations(_result()) == []
    v = invariants.violations(_result(writes={"putobject": 1}, orphans=2))
    assert len(v) == 2 and "snapshot repository" in v[0] and "2 Parquet" in v[1]
    assert invariants.schema_changes(_result(), _result()) == []
    changed = invariants.schema_changes(_result(), _result(schema=[["_id", "binary", True]]))
    assert changed and changed[0].startswith("logs-lz4")
    assert invariants.schema_changes({}, _result()) == []
