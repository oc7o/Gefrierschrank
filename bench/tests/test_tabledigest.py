import json
import uuid

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from gsbench import cli, tabledigest

SCHEMA = pa.schema([("_id", pa.string()), ("_source", pa.string()), ("_shard", pa.int64())])
SCHEMA_STRING = json.dumps({"type": "struct", "fields": [
    {"name": "_id", "type": "string", "nullable": True, "metadata": {}},
    {"name": "_source", "type": "string", "nullable": True, "metadata": {}},
    {"name": "_shard", "type": "long", "nullable": True, "metadata": {}}]})


def _shard_rows(shard, n=6, changed=None):
    return [{"_id": f"{shard}-{i}", "_source": changed if (changed and i == 0) else f'{{"n": {i}, "s": {shard}}}',
             "_shard": shard} for i in range(n)]


def _commit(table, version, actions):
    (table / "_delta_log" / f"{version:020d}.json").write_text("".join(json.dumps(a) + "\n" for a in actions))


def _add(table, rows, key):
    name = f"part-{uuid.uuid4()}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), table / name)
    return {"add": {"path": name, "partitionValues": {}, "size": (table / name).stat().st_size,
                    "modificationTime": 0, "dataChange": True, "tags": {"gefrierschrank.shard": key}}}


def make_table(table, shards=((0, None), (1, None)), files_per_shard=1, reverse=False):
    """A Delta table written the way the exporter does: one commit per shard, its key as txn and file tag."""
    (table / "_delta_log").mkdir(parents=True)
    _commit(table, 0, [{"protocol": {"minReaderVersion": 1, "minWriterVersion": 2}},
                       {"metaData": {"id": str(uuid.uuid4()), "format": {"provider": "parquet", "options": {}},
                                     "schemaString": SCHEMA_STRING, "partitionColumns": [], "configuration": {}}}])
    for version, (shard, changed) in enumerate(shards, start=1):
        rows = _shard_rows(shard, changed=changed)
        if reverse:
            rows.reverse()
        key = f"archive-v1:shard-{shard}"
        chunk = -(-len(rows) // files_per_shard)
        adds = [_add(table, rows[i:i + chunk], key) for i in range(0, len(rows), chunk)]
        _commit(table, version, adds + [{"txn": {"appId": key, "version": 0}}])
    return table


def _digest(table):
    return tabledigest.digest_table(table, "t")


def test_row_hash_ignores_key_order():
    assert tabledigest.row_hash({"a": 1, "b": "x"}) == tabledigest.row_hash({"b": "x", "a": 1})
    assert tabledigest.row_hash({"a": 1}) != tabledigest.row_hash({"a": 2})


def test_digest_fields(tmp_path):
    d = _digest(make_table(tmp_path / "t"))
    assert list(d) == ["table", "rows", "rowDigest", "schema", "tags", "txns", "files"]
    assert d["rows"] == 12 and d["files"] == 2
    assert all(len(d[k]) == 16 for k in ("rowDigest", "schema", "tags", "txns"))


def test_rows_split_differently_digest_equal_rows_but_not_files_and_tags(tmp_path):
    a = _digest(make_table(tmp_path / "a"))
    b = _digest(make_table(tmp_path / "b", files_per_shard=3, reverse=True))
    assert (a["rows"], a["rowDigest"], a["schema"], a["txns"]) == (b["rows"], b["rowDigest"], b["schema"], b["txns"])
    assert (a["files"], b["files"]) == (2, 6)
    assert a["tags"] != b["tags"]


def test_changed_value_changes_only_the_row_digest(tmp_path):
    a = _digest(make_table(tmp_path / "a"))
    b = _digest(make_table(tmp_path / "b", shards=((0, None), (1, '{"n": 0, "s": 9}'))))
    assert a["rowDigest"] != b["rowDigest"]
    assert {k: v for k, v in a.items() if k != "rowDigest"} == {k: v for k, v in b.items() if k != "rowDigest"}


def test_shard_key_set_changes_tags_and_txns(tmp_path):
    a = _digest(make_table(tmp_path / "a"))
    b = _digest(make_table(tmp_path / "b", shards=((0, None), (2, None))))
    assert a["tags"] != b["tags"] and a["txns"] != b["txns"]


def test_removed_files_are_excluded_and_paths_ignored(tmp_path):
    table = make_table(tmp_path / "t")
    before = _digest(table)
    first = json.loads((table / "_delta_log" / f"{1:020d}.json").read_text().splitlines()[0])["add"]
    rows = pq.read_table(table / first["path"]).to_pylist()
    readd = _add(table, rows, first["tags"]["gefrierschrank.shard"])
    _commit(table, 3, [{"remove": {"path": first["path"], "dataChange": True}}, readd])
    (table / first["path"]).unlink()
    assert _digest(table) == before


def test_checkpoint_replaces_the_commits_it_covers(tmp_path):
    from deltalake import DeltaTable
    table = make_table(tmp_path / "t", shards=((0, None), (1, None), (2, None)))
    before = _digest(table)
    DeltaTable(str(table)).create_checkpoint()
    for version in range(4):
        (table / "_delta_log" / f"{version:020d}.json").unlink()
    assert any(f.name.endswith(".checkpoint.parquet") for f in (table / "_delta_log").iterdir())
    assert _digest(table) == before
    _commit(table, 4, [_add(table, _shard_rows(3), "archive-v1:shard-3"), {"txn": {"appId": "archive-v1:shard-3",
                                                                                     "version": 0}}])
    after = _digest(table)
    assert (after["rows"], after["files"]) == (24, 4)


def test_missing_commit_is_an_error(tmp_path):
    table = make_table(tmp_path / "t")
    (table / "_delta_log" / f"{1:020d}.json").unlink()
    with pytest.raises(ValueError, match="no commit 1"):
        _digest(table)


def _lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def test_digest_finds_nested_tables_sorted_and_filters(tmp_path):
    root = tmp_path / "root"
    make_table(root / "b")
    make_table(root / "a")
    make_table(root / "a" / "_export_metadata", shards=((0, None),))
    out = tmp_path / "out" / "d.jsonl"
    assert tabledigest.digest(str(root), str(out)) == 0
    assert [d["table"] for d in _lines(out)] == ["a", "a/_export_metadata", "b"]
    assert tabledigest.digest(str(root), str(out), "b,*/_export_metadata") == 0
    assert [d["table"] for d in _lines(out)] == ["a/_export_metadata", "b"]
    assert tabledigest.digest(str(root / "a"), str(out)) == 0
    assert [d["table"] for d in _lines(out)] == [".", "_export_metadata"]


def _write(path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    return str(path)


def _record(table, **changes):
    r = {"table": table, "rows": 3, "rowDigest": "0" * 16, "schema": "1" * 16, "tags": "2" * 16,
         "txns": "3" * 16, "files": 1}
    r.update(changes)
    return r


def test_compare_digests_exit_codes(tmp_path, capsys):
    a = _write(tmp_path / "a.jsonl", [_record("x"), _record("y")])
    same = _write(tmp_path / "same.jsonl", [_record("y"), _record("x")])
    tags = _write(tmp_path / "tags.jsonl", [_record("x", tags="f" * 16), _record("y")])
    rows = _write(tmp_path / "rows.jsonl", [_record("x", rowDigest="f" * 16), _record("y")])
    one_sided = _write(tmp_path / "one.jsonl", [_record("x"), _record("y"), _record("z")])
    assert tabledigest.compare_digests(a, same) == 0
    assert tabledigest.compare_digests(a, tags) == 1
    assert "MISMATCH x: tags" in capsys.readouterr().out
    assert tabledigest.compare_digests(a, tags, rows_only=True) == 0
    assert tabledigest.compare_digests(a, rows, rows_only=True) == 1
    assert tabledigest.compare_digests(a, one_sided) == 0
    assert "ONLY-B z" in capsys.readouterr().out
    assert tabledigest.compare_digests(a, one_sided, strict=True) == 1


def test_compare_digests_reads_a_fixture_digests_directory(tmp_path):
    (tmp_path / "run").mkdir()
    _write(tmp_path / "run" / "digests.jsonl", [_record("x")])
    assert tabledigest.compare_digests(str(tmp_path / "run"), _write(tmp_path / "a.jsonl", [_record("x")])) == 0


def test_cli_dispatches_digest_and_compare_digests(tmp_path):
    make_table(tmp_path / "root" / "t")
    out = tmp_path / "d.jsonl"
    assert cli.main(["digest", str(tmp_path / "root"), "--out", str(out)]) == 0
    assert cli.main(["compare-digests", str(out), str(out), "--strict"]) == 0
    assert cli.main(["compare-digests", str(out), str(out), "--rows-only"]) == 0


def _report(path, index, shards):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"indexName": index, "shards": shards}), encoding="utf-8")


def _shard(fetch, fetched=10, processed=10):
    return {"shardId": 0, "outcome": "COMMITTED", "bytesProcessed": processed, "bytesFetched": fetched, "fetch": fetch}


def test_fetch_modes_counts_reports_at_any_depth(tmp_path, capsys):
    # retired-shell-repo writes its reports one level deeper (before/, after/) than the other fixtures;
    # a fixed-depth glob missed them once and undercounted the selective shards.
    reports = tmp_path / "reports"
    _report(reports / "core" / "a-repo" / "a.json", "a", [_shard("selective", 4, 10), _shard("full")])
    _report(reports / "core" / "shell-repo" / "before" / "b.json", "b", [_shard("selective", 3, 10)])
    _report(reports / "core" / "shell-repo" / "after" / "b.json", "b", [_shard("completed"), _shard("preflight", 2, 0)])
    _report(reports / "x.json", "x", [{"shardId": 0, "bytesProcessed": 5}])
    (reports / "core" / "a-repo" / "manifest.json").write_text(json.dumps({"indices": ["a"]}), encoding="utf-8")
    (reports / "core" / "a-repo" / "t" / "_delta_log").mkdir(parents=True)
    _report(reports / "core" / "a-repo" / "t" / "_delta_log" / "y.json", "y", [_shard("full")])
    (reports / "broken.json").write_text("{", encoding="utf-8")

    assert tabledigest.fetch_modes(str(reports), per_report=True) == 0
    lines = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [line["report"] for line in lines[:-1]] == [
        "core/a-repo/a.json", "core/shell-repo/after/b.json", "core/shell-repo/before/b.json", "x.json"]
    assert lines[-1] == {"reports": 4, "shards": 6,
                         "fetch": {"absent": 1, "completed": 1, "full": 1, "preflight": 1, "selective": 2},
                         "bytesFetched": 29, "bytesProcessed": 45}


def test_cli_dispatches_fetch_modes(tmp_path, capsys):
    _report(tmp_path / "r" / "a.json", "a", [_shard("selective")])
    assert cli.main(["fetch-modes", str(tmp_path / "r")]) == 0
    assert json.loads(capsys.readouterr().out)["fetch"] == {"selective": 1}
    assert cli.main(["fetch-modes", str(tmp_path / "missing")]) == 2
