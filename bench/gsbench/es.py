"""Minimal Elasticsearch REST client and container lifecycle for the benchmark nodes."""

import json
import time

import requests

from . import config
from .infra import compose, docker, log, slot


class Es:
    def __init__(self, node: config.EsNode):
        self.node = node
        self.url = node.url
        self.http = requests.Session()
        self.startup_s = None

    def request(self, method: str, path: str, body=None, params=None, timeout: float = 600, ok=(200, 201)):
        headers = {}
        data = None
        if isinstance(body, (bytes, str)):
            data = body.encode() if isinstance(body, str) else body
            headers["Content-Type"] = "application/x-ndjson"
        elif body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        r = self.http.request(method, self.url + path, data=data, params=params, headers=headers, timeout=timeout)
        if r.status_code not in ok:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:2000]}")
        return r.json() if r.content else None

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def put(self, path, body=None, **kw):
        return self.request("PUT", path, body, **kw)

    def post(self, path, body=None, **kw):
        return self.request("POST", path, body, **kw)

    def delete(self, path, **kw):
        return self.request("DELETE", path, **kw)

    def wait_ready(self, timeout: float = 240) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                r = self.http.get(self.url + "/_cluster/health", timeout=5)
                if r.ok and r.json().get("status") in ("green", "yellow"):
                    return
            except requests.RequestException:
                pass
            time.sleep(2)
        raise TimeoutError(f"{self.node.container} not healthy after {timeout}s")

    def start_trial(self) -> None:
        # logsdb, synthetic source and excluded source vectors need a non-basic license.
        # A fresh node answers 404 until its self-generated basic license is installed.
        deadline = time.time() + 60
        while (lic := self.get("/_license", ok=(200, 404))) is None or "license" not in lic:
            if time.time() > deadline:
                raise RuntimeError(f"no license on {self.node.container}")
            time.sleep(1)
        if lic["license"]["type"] in ("trial", "enterprise", "platinum"):
            return
        self.post("/_license/start_trial", params={"acknowledge": "true"})
        lic = self.get("/_license")["license"]
        if lic["type"] != "trial":
            raise RuntimeError(f"trial license not active on {self.node.container}: {lic['type']}")

    def register_repo(self, name: str, base_path: str, readonly: bool, extra: dict | None = None) -> None:
        settings = {
            "bucket": config.SNAPSHOT_BUCKET,
            "base_path": base_path,
            "endpoint": config.MINIO_DOCKER_ENDPOINT,
            "protocol": "http",
            "path_style_access": True,
            "readonly": readonly,
        }
        settings.update(extra or {})
        self.put(f"/_snapshot/{name}", {"type": "s3", "settings": settings})

    def cpu_usec(self) -> int:
        """Cumulative CPU of the container's cgroup (v2)."""
        out = docker("exec", self.node.container, "cat", "/sys/fs/cgroup/cpu.stat").stdout
        for line in out.splitlines():
            key, _, value = line.partition(" ")
            if key == "usage_usec":
                return int(value)
        raise RuntimeError("usage_usec missing from cgroup cpu.stat")

    def data_bytes(self) -> int:
        out = docker("exec", self.node.container, "du", "-sb", "/usr/share/elasticsearch/data").stdout
        return int(out.split()[0])


def start_node(major: int, fresh: bool = True) -> Es:
    """Start one ES node (caller holds the docker slot). `fresh` wipes its data volume first."""
    node = config.ES_NODES[major]
    for other in config.ES_NODES.values():
        if other.major != major:
            compose("rm", "-sf", other.service, profiles=(other.service,), check=False)
    if fresh:
        compose("rm", "-sf", node.service, profiles=(node.service,), check=False)
        docker("volume", "rm", "-f", node.volume, check=False)
    log(f"starting {node.container} (ES {node.version})")
    started = time.monotonic()
    compose("up", "-d", node.service, profiles=(node.service,))
    es = Es(node)
    es.wait_ready()
    es.startup_s = round(time.monotonic() - started, 1)
    return es


def stop_node(major: int) -> None:
    node = config.ES_NODES[major]
    log(f"stopping {node.container} and removing its volume")
    compose("rm", "-sf", node.service, profiles=(node.service,), check=False)
    docker("volume", "rm", "-f", node.volume, check=False)


class Node:
    """Context manager: docker slot + a fresh ES node, removed with its volume on exit."""

    def __init__(self, major: int):
        self.major = major
        self._slot = None

    def __enter__(self) -> Es:
        self._slot = slot("docker")
        self._slot.__enter__()
        try:
            return start_node(self.major)
        except BaseException:
            stop_node(self.major)
            self._slot.__exit__(None, None, None)
            raise

    def __exit__(self, *exc):
        try:
            stop_node(self.major)
        finally:
            self._slot.__exit__(*exc)
