import sys

from gsbench import metrics


def test_cpu_of_reaped_grandchildren_is_counted():
    burn = f'{sys.executable} -c "import time; t=time.process_time()\nwhile time.process_time()-t<0.5: pass"'
    m = metrics.run_measured(["sh", "-c", f"{burn}; true"])
    assert m["exit_code"] == 0
    assert m["cpu_s"] >= 0.45
    assert m["peak_rss_bytes"] > 0


def test_disk_peak_is_sampled(tmp_path):
    out = tmp_path / "out"
    m = metrics.run_measured(["sh", "-c", f"mkdir -p {out}; head -c 3000000 /dev/zero > {out}/f; sleep 1.5; rm {out}/f"],
                             disk_paths=[out])
    assert m["peak_disk_bytes"] >= 3_000_000


def test_minio_delta_per_bucket_and_api():
    before = {"b": {"requests": {"getobject": 5}, "rx_bytes": 10, "tx_bytes": 100}}
    after = {"b": {"requests": {"getobject": 8, "headobject": 2}, "rx_bytes": 10, "tx_bytes": 400},
             "other": {"requests": {"getobject": 1}, "rx_bytes": 0, "tx_bytes": 1}}
    d = metrics.minio_delta(before, after, buckets=("b", "c"))
    assert d == {"b": {"requests": {"getobject": 3, "headobject": 2}, "requests_total": 5, "tx_bytes": 300,
                       "rx_bytes": 0}}


def test_docker_mem_units():
    assert metrics.ContainerMemSampler.parse("1.5GiB ") == int(1.5 * 1024**3)
    assert metrics.ContainerMemSampler.parse("143.1MiB") == int(143.1 * 1024**2)


def test_summary_median_and_spread():
    s = metrics.summarize([10.0, 12.0, 11.0])
    assert s["median"] == 11.0 and s["min"] == 10.0 and s["max"] == 12.0 and s["spread_pct"] == 18.2


def test_source_writes_flags_every_modifying_api_and_received_bytes():
    clean = {"gs-bench-snapshots": {"requests": {"getobject": 463, "headobject": 2, "listobjectsv2": 1},
                                    "requests_total": 466, "tx_bytes": 10, "rx_bytes": 0}}
    assert metrics.source_writes(clean) == {}
    dirty = {"gs-bench-snapshots": {"requests": {"getobject": 3, "putobject": 1, "deletemultipleobjects": 2,
                                                 "newmultipartupload": 1, "copyobject": 1},
                                    "requests_total": 8, "tx_bytes": 10, "rx_bytes": 99}}
    assert metrics.source_writes(dirty) == {"putobject": 1, "deletemultipleobjects": 2, "newmultipartupload": 1,
                                            "copyobject": 1, "rx_bytes": 99}
    metrics.check_s3_read(clean, "x")
    import pytest
    with pytest.raises(RuntimeError):
        metrics.check_s3_read({"gs-bench-snapshots": {"requests": {}, "tx_bytes": 0}}, "x")


def test_output_stats_count_orphans_schema_and_missing_row_stats(tmp_path):
    import pyarrow as pa
    from deltalake import write_deltalake
    table = tmp_path / "t"
    write_deltalake(str(table), pa.table({"_id": ["a", "b"], "_source": ["{}", "{}"]}))
    stats = metrics.delta_output_stats(str(table))
    assert stats["add_files"] == 1 and stats["orphan_files"] == 0 and stats["rows_from_stats"] == 2
    assert stats["schema"] == [["_id", "string", True], ["_source", "string", True]]
    (table / "part-orphan.parquet").write_bytes(b"PAR1")
    assert metrics.delta_output_stats(str(table))["orphan_files"] == 1
    lost = tmp_path / "no-table"
    lost.mkdir()
    (lost / "part-0.parquet").write_bytes(b"PAR1")
    assert metrics.delta_output_stats(str(lost)) == {"exists": False, "orphan_files": 1}
    assert metrics.delta_output_stats(str(tmp_path / "absent")) == {"exists": False, "orphan_files": 0}
