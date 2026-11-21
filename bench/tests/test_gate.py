from gsbench import gate
from gsbench.canonical import canonical_hash, raw_hash
from gsbench.datasets import STORED, SYNTHETIC, VECTORS_EXCLUDED


def _oracle(docs):
    return {i: (r, canonical_hash(s), raw_hash(s)) for i, r, s in docs}


DOCS = [("a", None, '{"x":1}'), ("b", "r1", '{"y": [1, 2]}'), ("c", None, '{"z":"é"}')]


def rows(docs, kind="stored"):
    return [(i, r, s, kind) for i, r, s in docs]


def test_clean_stored_table_passes():
    res = gate.check_rows(rows(DOCS), _oracle(DOCS), {}, STORED)
    assert res["passed"], res


def test_each_corruption_is_detected():
    oracle = _oracle(DOCS)
    cases = {
        "dropped": (rows(DOCS[1:]), {"row_count", "id_routing_set"}),
        "duplicated": (rows(DOCS + DOCS[:1]), {"row_count", "no_duplicate_id"}),
        "reformatted": (rows([("a", None, '{"x": 1}')] + DOCS[1:]), {"raw_hash"}),
        "changed": (rows([("a", None, '{"x":2}')] + DOCS[1:]), {"raw_hash", "canonical_hash"}),
        "routing": (rows([("b", None, '{"y": [1, 2]}')] + DOCS[::2]), {"id_routing_set"}),
        "resurrected": (rows(DOCS + [("gone", None, "{}")]), {"row_count", "id_routing_set"}),
        "kind": (rows(DOCS, "synthetic"), {"source_kind"}),
    }
    for name, (table, expected) in cases.items():
        res = gate.check_rows(table, oracle, {"gone": None}, STORED)
        failed = {k for k, v in res["checks"].items() if v is False}
        assert expected <= failed, (name, failed)
    res = gate.check_rows(rows(DOCS + [("gone", None, "{}")]), oracle, {"gone": None}, STORED)
    assert res["counts"]["resurrected_deleted"] == 1


def test_synthetic_index_checks_canonical_only():
    res = gate.check_rows(rows([("a", None, '{"x": 1.0}')] + DOCS[1:], "synthetic"), _oracle(DOCS), {}, SYNTHETIC)
    assert res["passed"] and res["checks"]["raw_hash"] is None


def test_self_test_detects_all_cases():
    assert gate.self_test(verbose=False)


def _gate(docs, oracle_docs=DOCS, kind=STORED):
    return gate.check_rows(rows(docs), _oracle(oracle_docs), {}, kind)


def test_expected_failure_resolution():
    loud = {"outcome": "loud-failure"}
    xfail = {"outcome": "xfail"}
    clean, lost_one = _gate(DOCS), _gate(DOCS[1:])
    assert gate.resolve(clean, "COMMITTED", None) == "pass"
    assert gate.resolve(lost_one, "COMMITTED", None) == "fail"
    assert gate.resolve(lost_one, "QUARANTINED", loud) == "loud-failure"
    assert gate.resolve(lost_one, "FAILED", loud) == "loud-failure"
    assert gate.resolve(_gate([]), "FAILED", loud) == "loud-failure"
    assert gate.resolve(lost_one, "COMMITTED", loud) == "fail"  # silently wrong content is never tolerated
    assert gate.resolve(lost_one, "COMMITTED", xfail) == "xfail"
    assert gate.resolve(clean, "COMMITTED", xfail) == "xpass"
    assert gate.worst(["pass", "xfail", "fail"]) == "fail"


def test_loud_failure_does_not_excuse_corrupt_committed_rows():
    """One shard quarantined (row "c" missing) while a committed shard carries changed content."""
    loud = {"outcome": "loud-failure"}
    corrupt = _gate([("a", None, '{"x":999}'), DOCS[1]])
    assert corrupt["counts"]["missing_ids"] == 1 and corrupt["counts"]["raw_mismatch"] == 1
    assert gate.resolve(corrupt, "QUARANTINED", loud) == "fail"
    for bad in ([DOCS[0], DOCS[0]],                        # duplicate
                [DOCS[0], ("zz", None, "{}")],             # extra id
                [DOCS[0], ("b", "other", DOCS[1][2])],     # routing changed
                [DOCS[0], (None, None, "{}")]):            # null id
        assert gate.resolve(_gate(bad), "QUARANTINED", loud) == "fail", bad
    assert gate.resolve(_gate([DOCS[0]]), "QUARANTINED", loud) == "loud-failure"
    kind_bad = gate.check_rows(rows([DOCS[0]], "synthetic"), _oracle(DOCS), {}, STORED)
    assert gate.resolve(kind_bad, "QUARANTINED", loud) == "fail"


def test_xfail_limited_to_named_checks_and_later_entries_still_apply():
    stripped = _gate([(i, r, '{"x":1}') for i, r, _ in DOCS])  # content wrong, every row present
    lost = _gate(DOCS[1:])
    xfail_canon = {"outcome": "xfail", "tolerates": ["raw_hash", "canonical_hash"], "finding": "excluded-source-vectors"}
    loud = {"outcome": "loud-failure", "finding": "lucene9-worker-jar"}
    assert gate.resolve(stripped, "COMMITTED", xfail_canon) == "xfail"
    assert gate.resolve(lost, "COMMITTED", xfail_canon) == "fail"
    assert gate.resolve_any(lost, "QUARANTINED", [xfail_canon, loud]) == ("loud-failure", loud)
    assert gate.resolve_any(stripped, "COMMITTED", [xfail_canon, loud]) == ("xfail", xfail_canon)
    assert gate.resolve_any(lost, "COMMITTED", [xfail_canon, loud]) == ("fail", xfail_canon)
    assert gate.resolve_any(lost, "COMMITTED", []) == ("fail", None)


def test_expected_entries_match_on_label_version_and_worker():
    entries = [{"dataset": "logs-*7", "worker_jar": "absent", "finding": "lucene9-worker-jar",
                "outcome": "loud-failure"},
               {"dataset": "vec-default", "label": "main*", "version": "1.0.0", "finding": "excluded-source-vectors",
                "outcome": "xfail"}]
    assert gate.match_expected(entries, "x", "2.0", "logs-default7", False)["finding"] == "lucene9-worker-jar"
    assert gate.match_expected(entries, "x", "2.0", "logs-default7", True) is None
    assert gate.match_expected(entries, "main", "1.0.0", "vec-default", True)["finding"] == "excluded-source-vectors"
    assert gate.match_expected(entries, "candidate", "1.0.0", "vec-default", True) is None


def test_shipped_expected_failures_file_is_valid():
    entries = gate.load_expected()
    assert entries
    for e in entries:
        assert e["outcome"] in ("xfail", "loud-failure") and e["finding"] and e["dataset"]


def test_without_worker_jar_only_1_0_0_tolerates_loud_failures():
    entries = gate.load_expected()
    assert gate.match_expected(entries, "main", "1.0.0", "logs-zstd", False)["finding"] == "lucene9-worker-jar"
    assert gate.match_expected(entries, "main", "1.0.0", "logs-deflate7", False)["finding"] == "lucene9-worker-jar"
    assert gate.match_expected(entries, "main", "1.0.0", "logs-zstd", True) is None
    # Integrated builds read ES7 shards in-process, so they need no worker jar at all.
    assert gate.match_expected(entries, "candidate", "1.0.0", "logs-zstd", False) is None
    assert gate.match_expected(entries, "candidate", "1.0.0", "logs-deflate7", False) is None


def test_only_the_1_1_0_baseline_may_fail_loudly_on_excluded_vectors_and_every_build_passes_logs_logsdb_and_tsdb_hist():
    entries = gate.load_expected()
    vectors = gate.match_expected(entries, "main", "1.1.0", "vec-default", True)
    assert (vectors["finding"], vectors["outcome"]) == ("excluded-source-vectors", "loud-failure")
    failed = gate.check_rows([], _oracle(DOCS), {}, VECTORS_EXCLUDED)
    assert gate.resolve_any(failed, "FAILED", gate.matching_expected(entries, "main", "1.1.0", "vec-default", True))[0] \
        == "loud-failure"
    # Development builds report 1.1.0 until the version bump and must rehydrate; 1.2.0 must pass under any label.
    for label, version in (("rehydration", "1.1.0"), ("feature", "1.1.0"), ("candidate", "1.1.0"),
                           ("candidate-p4", "1.1.0"), ("main", "1.2.0"), ("feature", "1.2.0"), ("", "1.2.0")):
        assert gate.matching_expected(entries, label, version, "vec-default", True) == [], (label, version)
        assert gate.resolve_any(failed, "FAILED", [])[0] == "fail"
    for dataset in ("logs-logsdb", "tsdb-hist"):
        assert gate.match_expected(entries, "candidate", "1.1.0", dataset, False) is None, dataset


def test_rehydrated_vector_rows_pass_with_their_kinds():
    docs = [("a", None, '{"t":"a"}'), ("b", None, '{"t":"b","v":[0.5]}'), ("c", None, '{"o":{"v":[1.0]}}')]
    table = [("a", None, docs[0][2], "stored"), ("b", None, docs[1][2], "stored+vectors"),
             ("c", None, docs[2][2], "stored+vectors-reserialized")]
    assert gate.check_rows(table, _oracle(docs), {}, VECTORS_EXCLUDED)["passed"]
    wrong = [("a", None, docs[0][2], "synthetic")] + table[1:]
    assert gate.check_rows(wrong, _oracle(docs), {}, VECTORS_EXCLUDED)["checks"]["source_kind"] is False


def test_shipped_tolerates_name_real_checks():
    checks = set(gate.check_rows([], {}, {}, STORED)["checks"])
    for e in gate.load_expected():
        assert set(e.get("tolerates", [])) <= checks, e


def test_shipped_vec_default_xfail_does_not_shadow_the_worker_jar_entry():
    entries = gate.matching_expected(gate.load_expected(), "main", "1.0.0", "vec-default", False)
    assert [(e["finding"], e["outcome"]) for e in entries] == [("excluded-source-vectors", "xfail"), ("lucene9-worker-jar", "loud-failure")]
    quarantined = gate.check_rows([], _oracle(DOCS), {}, "stored-vectors-excluded")
    assert gate.resolve_any(quarantined, "QUARANTINED", entries)[0] == "loud-failure"
    assert gate.resolve_any(quarantined, "COMMITTED", entries)[0] == "fail"


def test_gate_workers_fit_in_memory(tmp_path):
    oracle = tmp_path / "o.jsonl"
    oracle.write_bytes(b"x" * 1_000_000)
    jobs = [{"oracle": str(oracle)}] * 11
    assert gate.gate_workers(jobs, available=100_000_000) == min(10, __import__("os").cpu_count())
    assert gate.gate_workers(jobs, available=12_000_000) == 1
    assert gate.gate_workers(jobs, available=1) == 1


def test_self_test_corrupts_copies_of_a_given_table_with_all_its_columns(tmp_path):
    import json

    import pyarrow as pa
    from deltalake import write_deltalake
    docs = [(f"id-{k}", "r" if k % 4 == 0 else None, f'{{"k": {k}, "t": "2026-01-0{k % 9 + 1}"}}') for k in range(40)]
    for kind, source_kind in ((STORED, "stored"), (SYNTHETIC, "synthetic")):
        table = tmp_path / kind / "t"
        write_deltalake(str(table), pa.table({
            "_id": [d[0] for d in docs], "_routing": [d[1] for d in docs], "_source": [d[2] for d in docs],
            "_source_kind": [source_kind] * len(docs), "_doc": pa.array(range(len(docs)), pa.int64())}))
        oracle = tmp_path / kind / "oracle.jsonl"
        oracle.write_text("".join(json.dumps([i, r, canonical_hash(s), raw_hash(s)]) + "\n" for i, r, s in docs))
        truth = tmp_path / kind / "truth.json"
        truth.write_text(json.dumps({"live": {}, "deleted": {"gone-1": None}, "generated_ids": 0}))
        assert gate.self_test(verbose=False, table=str(table), oracle_path=str(oracle), truth_path=str(truth),
                              kind=kind)
