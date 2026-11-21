import contextlib
from types import SimpleNamespace

import pytest

from gsbench import es, infra, shadow
from gsbench.canonical import canonical_hash


class _FirstDockerCall(Exception):
    pass


def test_shadow_takes_the_docker_slot_before_any_docker_call(monkeypatch, tmp_path):
    events = []

    @contextlib.contextmanager
    def slot(pool, slots=1):
        events.append(f"enter {pool}")
        try:
            yield
        finally:
            events.append(f"exit {pool}")

    def docker(*args, **kwargs):
        events.append("docker " + args[0])
        raise _FirstDockerCall()

    monkeypatch.setattr(infra, "slot", slot)
    monkeypatch.setattr(infra, "docker", docker)
    monkeypatch.setattr(es, "docker", docker)
    monkeypatch.setattr(infra, "ensure_minio", lambda: events.append("minio"))
    monkeypatch.setattr(infra, "s3_delete_prefix", lambda path: events.append("cleanup"))
    monkeypatch.setattr(shadow, "upload_repository", lambda local, base: events.append("upload"))
    args = SimpleNamespace(repo=str(tmp_path), s3_base_path=None, snapshot=None, tables=str(tmp_path),
                           indices="vec_root", major=9, out=None)
    with pytest.raises(_FirstDockerCall):
        shadow.run(args)
    assert events[0] == "enter docker", events
    assert events.index("enter docker") < events.index("minio") < events.index("upload")
    assert any(e.startswith("docker ") for e in events)
    assert events[-1] == "exit docker", events
    assert "cleanup" in events, "the scratch copy is removed even when the node fails"


def test_counts_only_and_float32_bits():
    es_docs = {
        "a": (canonical_hash('{"t":"a","v":[0.5,-0.38599998]}'), shadow.vector_bits('{"t":"a","v":[0.5,-0.38599998]}')),
        "b": (canonical_hash('{"t":"b"}'), shadow.vector_bits('{"t":"b"}')),
        "gone": (canonical_hash('{}'), {}),
    }
    rows = [("a", None, '{"v":[0.50,-0.38599998],"t":"a"}', "stored+vectors"),
            ("b", None, '{"t":"b","x":1}', "stored"),
            ("extra", None, '{}', "stored")]
    result = shadow.compare(es_docs, rows)
    assert result == {"rows": 3, "missing": 1, "extra": 1, "duplicates": 0, "canonical_mismatch": 1,
                      "vector_bit_mismatch": 0, "es_docs": 3, "kind_counts": {"stored": 2, "stored+vectors": 1},
                      "passed": False}


def test_vector_bits_tell_float32_values_apart_and_nested_arrays_by_path():
    # One float32 written two ways is equal; a one-ulp drift (cosine re-normalization) is not.
    assert shadow.vector_bits('{"v":[-0.38599998]}') == shadow.vector_bits('{"v":[-0.385999980]}')
    assert shadow.vector_bits('{"v":[-0.386]}') != shadow.vector_bits('{"v":[-0.38599998]}')
    bits = shadow.vector_bits('{"c":[{"v":[1,2]},{"v":[3]}],"tags":["x"]}')
    assert list(bits) == ["c.v"] and len(bits["c.v"]) == 2
