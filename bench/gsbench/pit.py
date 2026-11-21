"""Point-in-time export with sliced search_after, keeping each _source byte-exact.

Used twice: to capture the per-document oracle on the generation cluster, and by the tuned
baseline route (routes/es_export: one PIT per index, slices = max(shards, cores), sort
_shard_doc, size 10000, track_total_hits false, one process per slice, gzip). Elasticsearch
copies a stored JSON _source verbatim into a JSON response, so the response text between
`"_source":` and the next delimiter of the hit object is the stored byte sequence; the walker
below extracts exactly that span instead of re-serializing a parsed object.
"""

import json
import os
import re
import threading
from dataclasses import dataclass
from json.decoder import scanstring

import requests

_WS = re.compile(r"[ \t\n\r]*")
_DECODER = json.JSONDecoder()


@dataclass
class Hit:
    id: str | None
    routing: str | None
    source: str | None
    sort: list | None


def _ws(s: str, i: int) -> int:
    return _WS.match(s, i).end()


def _skip_value(s: str, start: int) -> int:
    return _DECODER.raw_decode(s, _ws(s, start))[1]


def _parse_object(s: str, i: int, on_value) -> int:
    """Walk the object at s[i] == '{'. on_value(key, start) gets the offset right after the colon
    and returns the offset right after the value. Returns the offset after the closing brace."""
    if s[i] != "{":
        raise ValueError(f"expected '{{' at {i}")
    i = _ws(s, i + 1)
    if s[i] == "}":
        return i + 1
    while True:
        if s[i] != '"':
            raise ValueError(f"expected key at {i}")
        key, i = scanstring(s, i + 1)
        i = _ws(s, i)
        if s[i] != ":":
            raise ValueError(f"expected ':' at {i}")
        i = _ws(s, on_value(key, i + 1))
        if s[i] == ",":
            i = _ws(s, i + 1)
        elif s[i] == "}":
            return i + 1
        else:
            raise ValueError(f"expected ',' or '}}' at {i}")


def parse_response(text: str) -> tuple[str | None, list[Hit]]:
    """Return (pit_id, hits) of a search response without re-serializing any _source."""
    pit_id = None
    hits: list[Hit] = []

    def on_hit_member(hit: Hit):
        def on_value(key: str, start: int) -> int:
            end = _skip_value(text, start)
            if key == "_id":
                hit.id = json.loads(text[start:end])
            elif key == "_routing":
                hit.routing = json.loads(text[start:end])
            elif key == "_source":
                # everything up to the next delimiter: leading/trailing whitespace is stored too
                hit.source = text[start:_ws(text, end)]
            elif key == "sort":
                hit.sort = json.loads(text[start:end])
            return end
        return on_value

    def on_hits_array(start: int) -> int:
        i = _ws(text, start)
        if text[i] != "[":
            raise ValueError(f"expected '[' at {i}")
        i = _ws(text, i + 1)
        if text[i] == "]":
            return i + 1
        while True:
            hit = Hit(None, None, None, None)
            i = _ws(text, _parse_object(text, i, on_hit_member(hit)))
            hits.append(hit)
            if text[i] == ",":
                i = _ws(text, i + 1)
            elif text[i] == "]":
                return i + 1
            else:
                raise ValueError(f"expected ',' or ']' at {i}")

    def on_hits_object(key: str, start: int) -> int:
        return on_hits_array(start) if key == "hits" else _skip_value(text, start)

    def on_top(key: str, start: int) -> int:
        nonlocal pit_id
        if key == "hits":
            return _parse_object(text, _ws(text, start), on_hits_object)
        end = _skip_value(text, start)
        if key == "pit_id":
            pit_id = json.loads(text[start:end])
        return end

    _parse_object(text, _ws(text, 0), on_top)
    return pit_id, hits


def open_pit(url: str, index: str, keep_alive: str) -> str:
    r = requests.post(f"{url}/{index}/_pit", params={"keep_alive": keep_alive}, timeout=120)
    r.raise_for_status()
    return r.json()["id"]


def close_pit(url: str, pit_id: str) -> None:
    requests.delete(f"{url}/_pit", json={"id": pit_id}, timeout=120)


def slice_pages(url: str, index: str, pit_id: str, slice_id: int, slices: int, size: int = 10_000,
                keep_alive: str = "10m", source_param=None, http: requests.Session | None = None):
    """Yield the hits of one slice page by page (sort _shard_doc, search_after)."""
    if http is None:
        http = requests.Session()
        http.headers["Accept-Encoding"] = "gzip"
    search_after = None
    while True:
        body = {"size": size, "track_total_hits": False, "pit": {"id": pit_id, "keep_alive": keep_alive},
                "sort": [{"_shard_doc": "asc"}]}
        if source_param is not None:
            body["_source"] = source_param
        if slices > 1:
            body["slice"] = {"id": slice_id, "max": slices}
        if search_after is not None:
            body["search_after"] = search_after
        r = http.post(f"{url}/_search", data=json.dumps(body), timeout=600,
                      headers={"Content-Type": "application/json"})
        if r.status_code != 200:
            raise RuntimeError(f"search on {index} slice {slice_id}: {r.status_code} {r.text[:500]}")
        new_pit, hits = parse_response(r.content.decode("utf-8"))
        pit_id = new_pit or pit_id
        if not hits:
            return
        yield hits
        search_after = hits[-1].sort


def export(url: str, index: str, slices: int, sink, size: int = 10_000, keep_alive: str = "10m",
           source_param=None) -> dict:
    """Stream every live root document of `index` into sink(slice_id, hits), one thread per slice.
    Used for the oracle, where speed does not matter; the timed baseline uses routes/es_export."""
    pit_id = open_pit(url, index, keep_alive)
    errors: list[BaseException] = []
    counts = [0] * slices
    requests_made = [0] * slices

    def run_slice(slice_id: int) -> None:
        try:
            for hits in slice_pages(url, index, pit_id, slice_id, slices, size, keep_alive, source_param):
                requests_made[slice_id] += 1
                sink(slice_id, hits)
                counts[slice_id] += len(hits)
            requests_made[slice_id] += 1
        except BaseException as e:  # noqa: BLE001 - re-raised on the calling thread
            errors.append(e)

    threads = [threading.Thread(target=run_slice, args=(i,), name=f"slice-{i}") for i in range(slices)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    close_pit(url, pit_id)
    if errors:
        raise errors[0]
    return {"docs": sum(counts), "slices": slices, "search_requests": sum(requests_made)}


def shard_count(url: str, index: str) -> int:
    r = requests.get(f"{url}/{index}/_settings", timeout=60)
    r.raise_for_status()
    settings = next(iter(r.json().values()))["settings"]["index"]
    return int(settings["number_of_shards"])


def default_slices(url: str, index: str) -> int:
    return max(shard_count(url, index), os.cpu_count() or 1)
