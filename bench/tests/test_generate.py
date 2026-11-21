import json

from gsbench import generate


def _run(profile, seed, **kw):
    truth = generate.Truth()
    ops = []
    for batch in generate.operations(profile, 400, seed, batch=64, **kw):
        for op in batch:
            truth.apply(op)
            ops.append(op)
    return ops, truth


def test_same_seed_same_documents_and_truth():
    a_ops, a = _run("logs", 3, updates=0.1, deletes=0.1, routed=0.3)
    b_ops, b = _run("logs", 3, updates=0.1, deletes=0.1, routed=0.3)
    assert a_ops == b_ops
    assert a.live == b.live and a.deleted == b.deleted


def test_different_seed_differs():
    _, a = _run("logs", 3)
    _, b = _run("logs", 4)
    assert a.live != b.live


def test_last_write_wins_and_deletes_leave_truth():
    ops, truth = _run("nested", 5, updates=0.2, deletes=0.1, routed=1.0)
    inserted = {op.id for op in ops if op.kind == "index"}
    deleted = {op.id for op in ops if op.kind == "delete"}
    assert deleted and not (deleted & set(truth.live))
    assert set(truth.live) == inserted - deleted
    last = {}
    for op in ops:
        if op.kind == "index":
            last[op.id] = op.source
    for doc_id, (_, sha) in truth.live.items():
        assert generate.hashlib.sha256(last[doc_id].encode()).hexdigest() == sha
    assert all(r is not None for r, _ in truth.live.values())


def test_sources_are_json_objects_with_exact_number_literals():
    ops, _ = _run("logs", 9)
    styles = {"pretty": 0, "ascii": 0}
    for op in ops:
        json.loads(op.source)
        assert "@@RAW" not in op.source
        styles["pretty"] += "\n  " in op.source
        styles["ascii"] += "\\u" in op.source
    assert all(styles.values()), styles


def test_bulk_body_carries_single_line_sources_and_routing():
    ops, _ = _run("logs", 11, routed=1.0)
    bulk = [op for op in ops[:50] if not generate.needs_single_request(op)]
    lines = generate.bulk_body(bulk, "idx").decode().splitlines()
    assert len(lines) == 2 * len(bulk)
    meta = json.loads(lines[0])["index"]
    assert meta["_index"] == "idx" and meta["_id"] == bulk[0].id and meta["routing"] == bulk[0].routing
    assert lines[1] == bulk[0].source


def test_tsdb_documents_have_generated_ids():
    ops, truth = _run("tsdb", 1)
    assert all(op.id is None for op in ops)
    assert truth.count == 400 and not truth.live


def test_dataset_key_covers_oracle_code(monkeypatch):
    from pathlib import Path

    from gsbench import datasets
    here = Path(datasets.__file__).parent
    assert {"canonical.py", "pit.py", "snapshot.py", "generate.py"} <= set(datasets.KEYED_MODULES)
    assert all((here / m).exists() for m in datasets.KEYED_MODULES)
    key = datasets.dataset_key(9, "smoke", 42, "9.3.0")
    real = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes",
                        lambda self: real(self) + (b"#changed" if self.name == "canonical.py" else b""))
    assert datasets.dataset_key(9, "smoke", 42, "9.3.0") != key


def test_frozen_suite_is_single_shard_force_merged_generations_with_one_large_index():
    from gsbench import datasets
    specs = [s for major in datasets.SUITES["frozen"] for s in datasets.specs(major, "frozen")]
    assert len({s.name for s in specs}) == len(specs) >= 20
    assert all(s.shards == 1 and s.force_merge and not (s.updates or s.deletes) for s in specs)
    assert all(s.settings.get("index.codec") == "best_compression" or s.kind == datasets.SYNTHETIC for s in specs)
    sizes = sorted((s.doc_count("standard"), s.name) for s in specs)
    assert sizes[-1] == (3_000_000, "ilm-big-000001") and sizes[-2][0] <= 100_000
    assert {s.major for s in specs if s.kind == datasets.SYNTHETIC} == {9}


def test_suite_selects_specs_majors_and_dataset_key():
    import pytest

    from gsbench import datasets
    assert datasets.suite_majors("default") == (9, 8, 7)
    assert datasets.suite_majors("frozen") == (9, 8)
    assert datasets.suite_majors("frozen", "9") == (9,)
    assert datasets.dataset_key(9, "smoke", 42, "9.3.0", "frozen") != datasets.dataset_key(9, "smoke", 42, "9.3.0")
    with pytest.raises(ValueError):
        datasets.specs(7, "frozen")
