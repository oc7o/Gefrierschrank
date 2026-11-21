"""Benchmark datasets: one snapshot repository base path per ES major, several indices each."""

import copy
import hashlib
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

# tiny exists to exercise the harness itself; its numbers mean nothing
SCALES = {"tiny": 0.05, "smoke": 1, "standard": 10, "large": 40}

STORED = "stored"
SYNTHETIC = "synthetic"
# Created on 9.2+: vectors are stripped from the stored _source; ES re-inserts them on fetch.
VECTORS_EXCLUDED = "stored-vectors-excluded"

KW = {"type": "keyword"}


def logs_properties(major: int, url_path=None, text_keyword_ignore_above: int | None = 1024) -> dict:
    subfield = {"type": "keyword"}
    if text_keyword_ignore_above:
        subfield["ignore_above"] = text_keyword_ignore_above
    return {
        "@timestamp": {"type": "date"},
        "host": {"properties": {"name": KW, "ip": {"type": "ip"},
                                "os": {"properties": {"platform": KW, "version": KW}}}},
        "service": {"properties": {"name": KW, "version": KW, "environment": KW}},
        "log": {"properties": {"level": KW, "logger": KW}},
        "http": {"properties": {
            "request": {"properties": {"method": KW, "bytes": {"type": "long"}}},
            "response": {"properties": {"status_code": {"type": "short"}, "bytes": {"type": "long"}}}}},
        "url": {"properties": {"path": url_path or KW, "domain": KW}},
        "user_agent": {"properties": {"original": {"type": "text", "fields": {"keyword": subfield}}}},
        "client": {"properties": {"ip": {"type": "ip"},
                                  "geo": {"properties": {"country_iso_code": KW, "city_name": KW}}}},
        "trace": {"properties": {"id": KW}},
        "span": {"properties": {"id": KW}},
        "event": {"properties": {"duration": {"type": "long"}, "sequence": {"type": "long"},
                                 "dataset": KW, "outcome": KW}},
        "labels": {"properties": {"k8s_pod": KW, "k8s_namespace": KW,
                                  "note": {"type": "keyword", "ignore_above": 32}}},
        "tags": KW,
        "message": {"type": "match_only_text"},
        "metrics": {"properties": {"amount": {"type": "double"}, "ratio": {"type": "double"}}},
    }


def nested_properties(major: int) -> dict:
    props = logs_properties(major)
    props["items"] = {"type": "nested", "properties": {"sku": KW, "qty": {"type": "integer"}, "name": KW}}
    return props


TSDB_SETTINGS = {
    "index.mode": "time_series",
    "index.routing_path": ["host.name", "service.name"],
    "index.time_series.start_time": "2025-12-01T00:00:00Z",
    "index.time_series.end_time": "2027-12-01T00:00:00Z",
}

TSDB_PROPERTIES = {
    "@timestamp": {"type": "date"},
    "host": {"properties": {"name": {"type": "keyword", "time_series_dimension": True}}},
    "service": {"properties": {"name": {"type": "keyword", "time_series_dimension": True}}},
    "cpu": {"properties": {"pct": {"type": "double", "time_series_metric": "gauge"}}},
    "requests": {"properties": {"count": {"type": "long", "time_series_metric": "counter"}}},
    "latency": {"type": "histogram"},
}


@dataclass
class IndexSpec:
    name: str
    major: int
    docs: int                       # smoke-scale count, multiplied by the scale factor
    profile: str                    # generator profile
    kind: str = STORED
    shards: int = 1
    settings: dict = field(default_factory=dict)
    mappings: dict = field(default_factory=dict)
    updates: float = 0.0
    deletes: float = 0.0
    routed: float = 0.0
    force_merge: bool = False       # single segment per shard, else one refresh per bulk batch
    source_param: object = None     # _source of the export request (vectors must be asked for)
    note: str = ""

    def body(self) -> dict:
        settings = {"index.number_of_shards": self.shards, "index.number_of_replicas": 0,
                    "index.refresh_interval": "-1"}
        settings.update(self.settings)
        return {"settings": settings, "mappings": copy.deepcopy(self.mappings)}

    def doc_count(self, scale: str) -> int:
        return int(self.docs * SCALES[scale])


def specs(major: int, suite: str = "default") -> list[IndexSpec]:
    if suite == "frozen":
        return frozen_specs(major)
    if suite != "default":
        raise ValueError(suite)
    if major == 9:
        return [
            IndexSpec("logs-lz4", 9, 40_000, "logs", shards=2, force_merge=True, routed=0.1,
                      mappings={"properties": logs_properties(9)},
                      note="default codec (LZ4 in release builds), 2 shards, force-merged"),
            IndexSpec("logs-zstd", 9, 40_000, "logs", settings={"index.codec": "best_compression"},
                      updates=0.05, deletes=0.03, routed=0.1, mappings={"properties": logs_properties(9)},
                      note="best_compression (ZSTD), multi-segment, updates + deletes, soft deletes retained"),
            IndexSpec("docs-nested", 9, 15_000, "nested", shards=2, updates=0.02, deletes=0.04, routed=1.0,
                      mappings={"properties": nested_properties(9)},
                      note="nested field, custom routing on every doc, deletes"),
            IndexSpec("logs-logsdb", 9, 20_000, "logs", kind=SYNTHETIC, settings={"index.mode": "logsdb"},
                      mappings={"properties": logs_properties(9, url_path={"type": "wildcard"},
                                                              text_keyword_ignore_above=None)},
                      note="logsdb (synthetic source), wildcard url.path, keyword over ignore_above"),
            IndexSpec("tsdb-hist", 9, 10_000, "tsdb", kind=SYNTHETIC, settings=TSDB_SETTINGS,
                      mappings={"properties": TSDB_PROPERTIES},
                      note="time_series with a histogram metric, generated _id"),
            IndexSpec("vec-default", 9, 5_000, "vector", kind=VECTORS_EXCLUDED,
                      mappings={"properties": {**logs_properties(9), "embedding": {
                          "type": "dense_vector", "dims": 32, "index": True, "similarity": "cosine"}}},
                      source_param={"exclude_vectors": False},
                      note="dense_vector created on 9.3: vectors excluded from the stored _source"),
        ]
    if major == 8:
        return [
            IndexSpec("logs-zstd8", 8, 30_000, "logs", shards=2, force_merge=True, routed=0.1,
                      settings={"index.codec": "best_compression"}, mappings={"properties": logs_properties(8)},
                      note="best_compression (ZSTD since 8.14), 2 shards, force-merged"),
            # not "logs-lz4-8": names matching logs-*-* are claimed by the built-in data stream template
            IndexSpec("logs8-lz4", 8, 30_000, "logs", updates=0.03, deletes=0.02, routed=0.1,
                      mappings={"properties": logs_properties(8)},
                      note="default codec (LZ4), multi-segment, updates + deletes"),
            IndexSpec("synth8", 8, 15_000, "logs", kind=SYNTHETIC,
                      mappings={"_source": {"mode": "synthetic"},
                                "properties": logs_properties(8, text_keyword_ignore_above=None)},
                      note="synthetic source on 8.17"),
        ]
    if major == 7:
        return [
            IndexSpec("logs-default7", 7, 25_000, "logs", shards=2, routed=0.1,
                      mappings={"properties": logs_properties(7)},
                      note="default codec (LZ4), 2 shards, multi-segment"),
            IndexSpec("logs-deflate7", 7, 25_000, "nested", settings={"index.codec": "best_compression"},
                      updates=0.02, deletes=0.03, routed=0.3, mappings={"properties": nested_properties(7)},
                      note="best_compression (DEFLATE), nested docs, updates + deletes"),
        ]
    raise ValueError(major)


# Document counts of the 8.17 generations: rollover by size and age leaves generations of uneven size.
FROZEN_APP8_DOCS = (2_000, 3_000, 5_000, 4_000, 6_000, 3_500, 2_500, 5_500, 4_500, 3_000,
                    6_000, 2_000, 4_000, 5_000, 3_000, 4_500, 2_500, 5_000, 3_500, 4_000)


def frozen_specs(major: int) -> list[IndexSpec]:
    """A repository shaped like an ILM frozen tier: every generation shrunk to one shard and
    force-merged to one segment with best_compression before its snapshot. The ES 9 snapshot holds
    logsdb generations (synthetic source) and one generation far larger than the rest (3M documents
    at --scale standard: 5M did not fit the free disk of the benchmark machine next to the ES
    route's NDJSON and 1.0.0's uncompressed output); the ES 8 snapshot holds stored-source
    generations created on 8.17."""
    if major == 9:
        logsdb = [IndexSpec(f"ilm-logsdb-{i:06d}", 9, 8_000, "logs", kind=SYNTHETIC, force_merge=True,
                            settings={"index.mode": "logsdb"},
                            mappings={"properties": logs_properties(9, url_path={"type": "wildcard"},
                                                                    text_keyword_ignore_above=None)},
                            note="9.3 logsdb generation (synthetic source), 1 shard, force-merged")
                  for i in range(1, 9)]
        big = IndexSpec("ilm-big-000001", 9, 300_000, "logs", force_merge=True,
                        settings={"index.codec": "best_compression"}, mappings={"properties": logs_properties(9)},
                        note="one very large generation: best_compression (ZSTD), 1 shard, force-merged")
        return [big, *logsdb]
    if major == 8:
        return [IndexSpec(f"ilm-app8-{i:06d}", 8, docs, "logs", force_merge=True,
                          settings={"index.codec": "best_compression"}, mappings={"properties": logs_properties(8)},
                          note="8.17 generation: best_compression (ZSTD), 1 shard, force-merged")
                for i, docs in enumerate(FROZEN_APP8_DOCS, 1)]
    raise ValueError(f"the frozen suite has no ES {major} snapshot")


MAJORS = (9, 8, 7)
SUITES = {"default": MAJORS, "frozen": (9, 8)}


def suite_majors(suite: str, majors: str | None = None) -> tuple[int, ...]:
    """The majors named in a --majors comma list, else every major of the suite."""
    return tuple(int(m) for m in majors.split(",")) if majors else SUITES[suite]


# Every module that decides the documents, the snapshot or the oracle hashes: a change to any of
# them must not let an export be gated against oracle files computed by the old code.
KEYED_MODULES = ("generate.py", "datasets.py", "snapshot.py", "pit.py", "canonical.py")


def dataset_key(major: int, scale: str, seed: int, es_version: str, suite: str = "default") -> str:
    """Cache key: the dataset spec, the dataset/oracle code and the ES version that built it."""
    here = Path(__file__).parent
    h = hashlib.sha256()
    h.update(json.dumps({"major": major, "scale": scale, "seed": seed, "es": es_version, "suite": suite,
                         "specs": [asdict(s) for s in specs(major, suite)]}, sort_keys=True).encode())
    for module in KEYED_MODULES:
        h.update((here / module).read_bytes())
    return h.hexdigest()[:12]
