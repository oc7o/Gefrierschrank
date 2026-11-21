"""Fixed names, ports and paths of the benchmark infrastructure.

Everything the harness creates carries the gs-bench prefix and every host port lies in
9100-9119; the shared test MinIO on :9000 is never contacted.
"""

import os
from dataclasses import dataclass
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent.parent
COMPOSE_FILE = BENCH_DIR / "compose.yaml"
CACHE_DIR = Path(os.environ.get("GSBENCH_CACHE", BENCH_DIR / "cache"))
RESULTS_DIR = Path(os.environ.get("GSBENCH_RESULTS", BENCH_DIR / "results"))
EXPECTED_FAILURES = BENCH_DIR / "expected-failures.yaml"

MINIO_URL = "http://localhost:9100"
MINIO_HOST = "localhost:9100"
MINIO_DOCKER_ENDPOINT = "gs-bench-minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
SNAPSHOT_BUCKET = "gs-bench-snapshots"
OUTPUT_BUCKET = "gs-bench-output"
BUCKETS = (SNAPSHOT_BUCKET, OUTPUT_BUCKET)


@dataclass(frozen=True)
class EsNode:
    major: int
    version: str
    service: str
    container: str
    port: int

    @property
    def url(self) -> str:
        return f"http://localhost:{self.port}"

    @property
    def volume(self) -> str:
        return f"gs-bench-{self.service}-data"


ES_NODES = {
    7: EsNode(7, "7.17.9", "es7", "gs-bench-es7", 9110),
    8: EsNode(8, "8.17.0", "es8", "gs-bench-es8", 9111),
    9: EsNode(9, "9.3.0", "es9", "gs-bench-es9", 9112),
}

# ES 9 cannot restore indices created by ES 7, so the baseline restores 7.x snapshots on 8.17.
RESTORE_NODE_FOR_MAJOR = {9: 9, 8: 9, 7: 8}

SNAPSHOT_REPO = "gs_bench"
