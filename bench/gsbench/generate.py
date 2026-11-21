"""Seeded, deterministic document generator.

Sources are produced as final strings and sent to Elasticsearch verbatim, so the sha256 of the
exact bytes of each id's last indexed version is the byte-exact ground truth for stored source.
A small share of documents is pretty-printed, oddly spaced or ASCII-escaped on purpose: any
reserialization anywhere in an export shows up as a raw-hash mismatch.
"""

import base64
import hashlib
import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

SERVICES = ["checkout", "cart", "catalog", "payment", "shipping", "auth", "search", "recommendation",
            "inventory", "pricing", "notification", "gateway"]
LEVELS = ["INFO"] * 7 + ["DEBUG", "DEBUG", "WARN", "WARN", "ERROR"]
METHODS = ["GET", "GET", "GET", "POST", "POST", "PUT", "DELETE"]
STATUSES = [200, 200, 200, 200, 200, 200, 201, 204, 301, 400, 401, 403, 404, 500, 503]
AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148",
    "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "okhttp/4.12.0", "curl/8.9.1", "python-requests/2.32.3", "Go-http-client/2.0",
]
COUNTRIES = ["DE", "DE", "DE", "AT", "CH", "FR", "NL", "US", "GB", "PL"]
CITIES = ["Berlin", "Hamburg", "München", "Köln", "Frankfurt am Main", "Wien", "Zürich", "Paris", "Amsterdam", "London"]
UNICODE_WORDS = ["Grüße", "naïve", "façade", "日本語", "中文字符", "Ελληνικά", "русский", "🚀", "🧊❄️", "👩‍💻",
                 "Ω≈ç√", "ℕ⊆ℤ", "𝔘𝔫𝔦𝔠𝔬𝔡𝔢", "\"quoted\"", "back\\slash", "tab\there", "line\nbreak", "</script>"]
SYLLABLES = ["ka", "lo", "mi", "ne", "ru", "sa", "te", "vo", "zi", "pa", "ro", "de", "an", "el", "is", "or",
             "un", "ba", "ce", "fu"]
EPOCH = datetime(2026, 1, 1, tzinfo=timezone.utc)


@dataclass
class Op:
    kind: str                # "index" | "delete"
    id: str | None           # None: Elasticsearch generates it (time series)
    routing: str | None
    source: str | None = None


@dataclass
class Truth:
    """What a correct export of the final index state must contain, from the generator's view."""
    live: dict = field(default_factory=dict)       # id -> (routing, sha256 of raw source bytes)
    deleted: dict = field(default_factory=dict)    # id -> routing of ids whose last op was a delete
    generated_ids: int = 0                         # docs indexed without an id (TSDB)

    def apply(self, op: Op) -> None:
        if op.id is None:
            self.generated_ids += 1
        elif op.kind == "index":
            self.live[op.id] = (op.routing, hashlib.sha256(op.source.encode()).hexdigest())
            self.deleted.pop(op.id, None)
        else:
            self.live.pop(op.id, None)
            self.deleted[op.id] = op.routing

    @property
    def count(self) -> int:
        return len(self.live) + self.generated_ids


class Serializer:
    """json.dumps with exact number literals: RawNumber placeholders are replaced after dumping."""

    def __init__(self):
        self.raw: list[str] = []

    def num(self, literal: str) -> str:
        self.raw.append(literal)
        return f"@@RAW{len(self.raw) - 1}@@"

    def dumps(self, obj, style: str) -> str:
        if style == "pretty":
            text = json.dumps(obj, ensure_ascii=False, indent=2)
        elif style == "spaced":
            text = " " + json.dumps(obj, ensure_ascii=False, separators=(" , ", " : ")) + "\n"
        elif style == "ascii":
            text = json.dumps(obj, ensure_ascii=True, separators=(",", ":"))
        else:
            text = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        for i, literal in enumerate(self.raw):
            text = text.replace(f'"@@RAW{i}@@"', literal, 1)
        return text


class DocFactory:
    def __init__(self, rng: random.Random):
        self.rng = rng
        self.words = ["".join(rng.choice(SYLLABLES) for _ in range(rng.randint(2, 4))) for _ in range(2000)]

    def word(self) -> str:
        return self.words[min(len(self.words) - 1, int(abs(self.rng.gauss(0, 300))))]

    def hex(self, n: int) -> str:
        return "%0*x" % (n, self.rng.getrandbits(4 * n))

    def style(self) -> str:
        r = self.rng.random()
        return "pretty" if r < 0.01 else "spaced" if r < 0.02 else "ascii" if r < 0.04 else "compact"

    def message(self) -> str:
        rng = self.rng
        parts = []
        kind = rng.randrange(4)
        if kind == 0:
            parts.append(f"Processed order {rng.randrange(10_000_000)} in {rng.randrange(2000)} ms "
                         f"(user=u-{rng.randrange(1_000_000)}, items={1 + rng.randrange(12)}, "
                         f"total={rng.randrange(900)}.{10 + rng.randrange(90)} EUR)")
        elif kind == 1:
            parts.append(f"Cache miss for key {self.word()}:{rng.randrange(100_000)}, fetching upstream")
        elif kind == 2:
            parts.append(f"Request to /api/v2/{self.word()} failed with status {rng.choice(STATUSES)}:")
        for _ in range(6 + rng.randrange(18)):
            parts.append(rng.choice(UNICODE_WORDS) if rng.random() < 0.04 else self.word())
        return " ".join(parts)

    def logs(self, ts: datetime, ser: Serializer, with_extras: bool = True) -> dict:
        rng = self.rng
        service = rng.choice(SERVICES)
        host = rng.randrange(120)
        status = rng.choice(STATUSES)
        country = rng.randrange(len(COUNTRIES))
        doc = {
            "@timestamp": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "host": {"name": f"{service}-{host}", "ip": f"10.1.{host // 50}.{host}",
                     "os": {"platform": "linux", "version": f"6.8.0-{40 + host % 7}"}},
            "service": {"name": service, "version": f"4.{host % 13}.{host % 5}", "environment": "production"},
            "log": {"level": rng.choice(LEVELS), "logger": f"com.example.{service}.{self.word().capitalize()}Service"},
            "http": {"request": {"method": rng.choice(METHODS), "bytes": rng.randrange(20_000)},
                     "response": {"status_code": status, "bytes": rng.randrange(200_000)}},
            "url": {"path": f"/api/v2/{self.word()}/{rng.randrange(1_000_000)}/{self.word()}",
                    "domain": "shop.example.com"},
            "user_agent": {"original": rng.choice(AGENTS)},
            "client": {"ip": f"{1 + rng.randrange(223)}.{rng.randrange(256)}.{rng.randrange(256)}.{rng.randrange(256)}",
                       "geo": {"country_iso_code": COUNTRIES[country], "city_name": CITIES[country]}},
            "trace": {"id": self.hex(32)},
            "span": {"id": self.hex(16)},
            "event": {"duration": rng.randrange(2_000_000_000),
                      # beyond 2^53: a float64 round trip anywhere changes the digits
                      "sequence": ser.num(str(rng.randrange(2**53, 2**63 - 1))),
                      "dataset": f"{service}.app", "outcome": "success" if status < 400 else "failure"},
            "labels": {"k8s_pod": f"{service}-{self.hex(9)}-{self.hex(5)}", "k8s_namespace": "shop"},
            "tags": rng.sample(["prod", "eu", "canary", "beta", "blue", "green", "pci"], rng.randint(0, 3)),
            "message": self.message(),
        }
        if with_extras:
            doc["metrics"] = {
                # more digits than a double holds, and an exponent form
                "amount": ser.num(f"{rng.randrange(100_000)}.{rng.randrange(10**18):018d}"),
                "ratio": ser.num(rng.choice(["1.0e-3", "0.1000000000000000055511151231257827", "1E+2", "-0.0",
                                             f"{rng.random():.17f}"])),
            }
            if rng.random() < 0.05:
                doc["labels"]["note"] = " ".join(self.word() for _ in range(12))  # exceeds ignore_above
        return doc

    def items(self) -> list[dict]:
        rng = self.rng
        return [{"sku": f"SKU-{self.hex(8).upper()}", "qty": 1 + rng.randrange(9),
                 "name": rng.choice(UNICODE_WORDS) if rng.random() < 0.1 else self.word()}
                for _ in range(1 + rng.randrange(5))]


def make_id(rng: random.Random, ordinal: int) -> str:
    """Mix of id shapes: ES encodes numeric, URL-safe base64 and other strings differently."""
    r = rng.random()
    if r < 0.35:
        return str(1_000_000_000 + ordinal)
    if r < 0.75:
        return base64.urlsafe_b64encode(ordinal.to_bytes(6, "big") + rng.randbytes(9)).decode()
    if r < 0.8:
        return f"{ordinal:012d}"
    return f"dök-{ordinal}-{rng.choice(['α', 'ß', '🧊', 'x'])}"


def operations(profile: str, docs: int, seed: int, updates: float = 0.0, deletes: float = 0.0,
               routed: float = 0.0, batch: int = 2000):
    """Yield batches of Ops. Initial inserts first, then updates and deletes of random live ids."""
    rng = random.Random(seed)
    factory = DocFactory(rng)
    ts = EPOCH
    routing_of: dict[str, str | None] = {}
    ids: list[str] = []

    def source(ts_now):
        ser = Serializer()
        if profile == "logs":
            doc = factory.logs(ts_now, ser)
        elif profile == "nested":
            doc = factory.logs(ts_now, ser, with_extras=False)
            doc["items"] = factory.items()
        elif profile == "vector":
            doc = factory.logs(ts_now, ser, with_extras=False)
            doc["embedding"] = [ser.num(f"{rng.uniform(-1, 1):.6f}") for _ in range(32)]
        else:
            raise ValueError(profile)
        return ser.dumps(doc, factory.style())

    def tsdb_source(ordinal):
        ser = Serializer()
        host = ordinal % 40
        step = ordinal // 40
        counts = [rng.randrange(1, 50) for _ in range(4)]
        doc = {
            "@timestamp": (EPOCH + timedelta(seconds=10 * step)).isoformat().replace("+00:00", "Z"),
            "host": {"name": f"node-{host:03d}"},
            "service": {"name": SERVICES[host % len(SERVICES)]},
            "cpu": {"pct": ser.num(f"{rng.random():.4f}")},
            "requests": {"count": step * 13 + rng.randrange(13)},
            "latency": {"values": [ser.num(v) for v in ("0.1", "0.5", "2.5", "10.0")], "counts": counts},
        }
        return ser.dumps(doc, "compact")

    out: list[Op] = []
    for ordinal in range(docs):
        if profile == "tsdb":
            out.append(Op("index", None, None, tsdb_source(ordinal)))
        else:
            ts = ts + timedelta(milliseconds=rng.randrange(40))
            doc_id = make_id(rng, ordinal)
            routing = f"tenant-{rng.randrange(16)}" if rng.random() < routed else None
            routing_of[doc_id] = routing
            ids.append(doc_id)
            out.append(Op("index", doc_id, routing, source(ts)))
        if len(out) >= batch:
            yield out
            out = []
    if out:
        yield out
        out = []
    if profile == "tsdb":
        return
    live = list(ids)
    rng.shuffle(live)
    n_upd = int(len(live) * updates)
    n_del = int(len(live) * deletes)
    for doc_id in live[:n_upd]:
        ts = ts + timedelta(milliseconds=rng.randrange(40))
        out.append(Op("index", doc_id, routing_of[doc_id], source(ts)))
        if len(out) >= batch:
            yield out
            out = []
    # some deleted ids were updated first, so their soft-deleted versions pile up in segments
    for doc_id in live[n_upd // 2: n_upd // 2 + n_del]:
        out.append(Op("delete", doc_id, routing_of[doc_id]))
        if len(out) >= batch:
            yield out
            out = []
    if out:
        yield out


def bulk_body(ops: list[Op], index: str) -> bytes:
    """_bulk body for ops whose sources fit on one line (see needs_single_request)."""
    lines = []
    for op in ops:
        meta = {"_index": index}
        if op.id is not None:
            meta["_id"] = op.id
        if op.routing is not None:
            meta["routing"] = op.routing
        if op.kind == "index":
            assert not needs_single_request(op)
            lines.append(json.dumps({"index": meta}, ensure_ascii=False))
            lines.append(op.source)
        else:
            lines.append(json.dumps({"delete": meta}, ensure_ascii=False))
    return ("\n".join(lines) + "\n").encode()


def needs_single_request(op: Op) -> bool:
    """Pretty-printed sources span lines, which _bulk cannot carry verbatim: they are sent
    through the single-document API instead, so the stored bytes still equal the generated ones."""
    return op.kind == "index" and "\n" in op.source
