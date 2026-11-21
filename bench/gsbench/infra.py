"""Docker, machine-wide slots and the benchmark MinIO."""

import contextlib
import os
import subprocess
import sys
import time

import pyarrow.fs as pafs
import requests

from . import config


def log(message: str) -> None:
    print(f"[gsbench {time.strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


# ── machine-wide slots ──────────────────────────────────────────────────────────────────────────
#
# A shared machine may run several jobs at once; GSBENCH_SLOT names a semaphore script
# (`slot.sh <pool> <n> <cmd...>`). The harness holds a slot by running a tiny child under that
# script which blocks on stdin: closing stdin (or the harness dying) releases it. Without
# GSBENCH_SLOT, slots are no-ops and the caller is responsible for serialization.

_HELD_ENV = "GSBENCH_HELD_SLOTS"


def _held() -> set[str]:
    return {p for p in os.environ.get(_HELD_ENV, "").split(",") if p}


@contextlib.contextmanager
def slot(pool: str, slots: int = 1):
    script = os.environ.get("GSBENCH_SLOT")
    if not script or pool in _held():
        yield
        return
    log(f"waiting for {pool} slot")
    child = subprocess.Popen(
        [script, pool, str(slots), "sh", "-c", "echo GSBENCH_SLOT_ACQUIRED; read _"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
    line = child.stdout.readline()
    if "GSBENCH_SLOT_ACQUIRED" not in line:
        child.kill()
        raise RuntimeError(f"could not acquire {pool} slot via {script}")
    log(f"holding {pool} slot")
    previous = os.environ.get(_HELD_ENV)
    os.environ[_HELD_ENV] = ",".join(sorted(_held() | {pool}))
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(_HELD_ENV, None)
        else:
            os.environ[_HELD_ENV] = previous
        child.stdin.close()
        child.wait(timeout=60)
        log(f"released {pool} slot")


def require_docker_slot() -> None:
    if os.environ.get("GSBENCH_SLOT") and "docker" not in _held():
        raise RuntimeError("docker action outside the docker slot")


# ── docker ──────────────────────────────────────────────────────────────────────────────────────

def docker(*args: str, check: bool = True, capture: bool = True, timeout: int = 600) -> subprocess.CompletedProcess:
    require_docker_slot()
    return subprocess.run(["docker", *args], check=check, capture_output=capture, text=True, timeout=timeout)


def compose(*args: str, profiles: tuple[str, ...] = (), check: bool = True, timeout: int = 600):
    cmd = ["compose", "-f", str(config.COMPOSE_FILE)]
    for p in profiles:
        cmd += ["--profile", p]
    return docker(*cmd, *args, check=check, timeout=timeout)


def container_running(name: str) -> bool:
    out = subprocess.run(["docker", "ps", "-q", "--filter", f"name=^{name}$"], capture_output=True, text=True)
    return bool(out.stdout.strip())


def ensure_minio() -> None:
    if not container_running("gs-bench-minio"):
        with slot("docker"):
            log("starting gs-bench-minio")
            compose("up", "-d", "--wait", "minio")
    deadline = time.time() + 60
    while time.time() < deadline:
        try:
            if requests.get(f"{config.MINIO_URL}/minio/health/live", timeout=2).ok:
                break
        except requests.RequestException:
            pass
        time.sleep(1)
    fs = s3fs()
    for bucket in config.BUCKETS:
        if fs.get_file_info(bucket).type == pafs.FileType.NotFound:
            fs.create_dir(bucket)


def s3fs() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(
        endpoint_override=config.MINIO_HOST, scheme="http", region="us-east-1",
        access_key=config.MINIO_ACCESS_KEY, secret_key=config.MINIO_SECRET_KEY,
        allow_bucket_creation=True, allow_bucket_deletion=True)


def s3_exists(path: str) -> bool:
    try:
        return s3fs().get_file_info(path).type != pafs.FileType.NotFound
    except OSError:
        return False


def s3_delete_prefix(path: str) -> None:
    fs = s3fs()
    if fs.get_file_info(path).type != pafs.FileType.NotFound:
        fs.delete_dir(path)


def minio_env() -> dict[str, str]:
    env = dict(os.environ)
    env.update({
        "AWS_ACCESS_KEY_ID": config.MINIO_ACCESS_KEY,
        "AWS_SECRET_ACCESS_KEY": config.MINIO_SECRET_KEY,
        "AWS_REGION": "us-east-1",
        "TARGET_AWS_ACCESS_KEY_ID": config.MINIO_ACCESS_KEY,
        "TARGET_AWS_SECRET_ACCESS_KEY": config.MINIO_SECRET_KEY,
    })
    env.pop("AWS_SESSION_TOKEN", None)
    env.pop("AWS_PROFILE", None)
    return env


def down(keep_cache: bool = False) -> None:
    import shutil
    # bench first (the pool order everywhere): never pull MinIO or the cache away under another
    # instance's run
    with slot("bench"):
        with slot("docker"):
            log("removing gs-bench containers, volumes and network")
            compose("down", "-v", "--remove-orphans", profiles=("es7", "es8", "es9"), check=False)
            for node in config.ES_NODES.values():
                docker("volume", "rm", "-f", node.volume, check=False)
            docker("volume", "rm", "-f", "gs-bench-minio-data", check=False)
        if not keep_cache and config.CACHE_DIR.exists():
            # Oracles describe snapshots that lived in the removed MinIO volume.
            shutil.rmtree(config.CACHE_DIR)
            log(f"removed {config.CACHE_DIR}")
