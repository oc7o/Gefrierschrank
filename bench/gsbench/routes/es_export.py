"""Baseline export phase: one PIT per index, one process per slice, NDJSON per slice.

Response parsing and NDJSON encoding hold the GIL, so one thread per slice pinned the client at
one core (client CPU about equal to export wall on the LZ4 and force-merged indices) and the
phase measured the Python client instead of Elasticsearch. Each slice therefore runs in its own
process. Workers are spawned (fork hangs in requests' proxy lookup on macOS) and warmed up,
imports and HTTP connection included, before the PIT opens. The reported export window, wall
and client CPU, runs from PIT open to PIT close, like an exporter with a warm worker pool;
process start-up is reported separately.
"""

import argparse
import json
import multiprocessing
import os
import sys
import time

import requests

from .. import pit


def _worker(conn, url: str, index: str, slice_id: int, slices: int, size: int, source_param, path: str) -> None:
    try:
        http = requests.Session()
        http.headers["Accept-Encoding"] = "gzip"
        http.get(f"{url}/", timeout=60).raise_for_status()
        conn.send("ready")
        pit_id = conn.recv()
        if pit_id is None:
            return
        wall0, cpu0 = time.monotonic(), time.process_time()
        docs = pages = chars = 0
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            for hits in pit.slice_pages(url, index, pit_id, slice_id, slices, size=size, source_param=source_param,
                                        http=http):
                pages += 1
                for h in hits:
                    # _source travels as a JSON string: the raw text stays exact, newlines included.
                    line = json.dumps({"_id": h.id, "_routing": h.routing, "_source": h.source}, ensure_ascii=False)
                    f.write(line)
                    f.write("\n")
                    chars += len(line) + 1
                docs += len(hits)
        conn.send({"docs": docs, "search_requests": pages + 1, "ndjson_chars": chars,
                   "wall_s": time.monotonic() - wall0, "cpu_s": time.process_time() - cpu0})
    except BaseException as e:  # noqa: BLE001 - reported to the parent, which fails the export
        try:
            conn.send({"error": f"slice {slice_id}: {type(e).__name__}: {e}"[:2000]})
        except OSError:
            pass


def export(url: str, index: str, out_dir: str, slices: int, size: int = 10_000, source_param=None,
           keep_alive: str = "10m") -> dict:
    ctx = multiprocessing.get_context("spawn")
    spawned = time.monotonic()
    conns, procs = [], []
    for i in range(slices):
        parent, child = ctx.Pipe()
        proc = ctx.Process(target=_worker, name=f"slice-{i}", daemon=True,
                           args=(child, url, index, i, slices, size, source_param,
                                 os.path.join(out_dir, f"slice-{i:03d}.ndjson")))
        proc.start()
        child.close()
        conns.append(parent)
        procs.append(proc)
    try:
        for c in conns:
            msg = c.recv()
            if msg != "ready":
                raise RuntimeError(f"export worker did not start: {msg}")
        startup = time.monotonic() - spawned
        wall0, cpu0 = time.monotonic(), time.process_time()
        pit_id = pit.open_pit(url, index, keep_alive)
        try:
            for c in conns:
                c.send(pit_id)
            results = [c.recv() for c in conns]
        finally:
            pit.close_pit(url, pit_id)
        window = time.monotonic() - wall0
        parent_cpu = time.process_time() - cpu0
    finally:
        for c in conns:
            try:
                c.send(None)
            except OSError:
                pass
        for proc in procs:
            proc.join(timeout=60)
            if proc.is_alive():
                proc.kill()
                proc.join()
    errors = [r["error"] for r in results if "error" in r]
    if errors:
        raise RuntimeError("; ".join(errors))
    per_wall = [r["cpu_s"] / r["wall_s"] for r in results if r["wall_s"] > 0]
    return {
        "docs": sum(r["docs"] for r in results), "slices": slices,
        "search_requests": sum(r["search_requests"] for r in results),
        "ndjson_chars": sum(r["ndjson_chars"] for r in results),
        "seconds": window, "startup_seconds": startup,
        "client_cpu_s": parent_cpu + sum(r["cpu_s"] for r in results), "parent_cpu_s": parent_cpu,
        "slice_wall_s": [round(r["wall_s"], 3) for r in results],
        "slice_cpu_s": [round(r["cpu_s"], 3) for r in results],
        "max_slice_cpu_per_wall": round(max(per_wall), 3) if per_wall else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--index", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--slices", type=int, default=0)
    parser.add_argument("--size", type=int, default=10_000)
    parser.add_argument("--source-param", default=None, help="JSON value for the request's _source")
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    slices = args.slices or pit.default_slices(args.url, args.index)
    stats = export(args.url, args.index, args.out_dir, slices, size=args.size,
                   source_param=json.loads(args.source_param) if args.source_param else None)
    json.dump(stats, sys.stdout)


if __name__ == "__main__":
    main()
