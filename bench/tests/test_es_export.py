import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from gsbench import metrics
from gsbench.routes import es_export

SOURCES = [f'{{"n": {k}, "s": "é\\n{k}"}}' for k in range(57)]


class FakeEs(BaseHTTPRequestHandler):
    """Just enough of a PIT: every slice gets the docs k % max == id in pages of `size`."""
    opened = closed = 0

    def log_message(self, *args):
        pass

    def _reply(self, obj):
        body = json.dumps(obj).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self._reply({"name": "fake"})

    def do_DELETE(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        FakeEs.closed += 1
        self._reply({"succeeded": True})

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path.startswith("/idx/_pit"):
            FakeEs.opened += 1
            return self._reply({"id": "pit-1"})
        req = json.loads(body)
        sl = req.get("slice", {"id": 0, "max": 1})
        after = (req.get("search_after") or [-1])[0]
        mine = [k for k in range(len(SOURCES)) if k % sl["max"] == sl["id"] and k > after][:req["size"]]
        hits = ",".join('{"_id":"%d",%s"_source":%s,"sort":[%d]}'
                        % (k, '"_routing":"r",' if k % 3 == 0 else "", SOURCES[k], k) for k in mine)
        text = '{"pit_id":"pit-1","hits":{"hits":[' + hits + ']}}'
        data = text.encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def _server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FakeEs)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def test_one_process_per_slice_exports_every_doc_exactly(tmp_path):
    server, url = _server()
    try:
        stats = es_export.export(url, "idx", str(tmp_path), slices=3, size=5)
    finally:
        server.shutdown()
    assert stats["docs"] == len(SOURCES) and stats["slices"] == 3
    assert len(stats["slice_cpu_s"]) == 3 and stats["client_cpu_s"] >= sum(stats["slice_cpu_s"])
    assert FakeEs.opened == FakeEs.closed == 1
    rows = [json.loads(line) for f in sorted(tmp_path.glob("slice-*.ndjson")) for line in f.open(encoding="utf-8")]
    assert sorted(rows, key=lambda r: int(r["_id"])) == [
        {"_id": str(k), "_routing": "r" if k % 3 == 0 else None, "_source": s} for k, s in enumerate(SOURCES)]


def test_exporter_process_cpu_includes_its_slice_workers(tmp_path):
    server, url = _server()
    try:
        m = metrics.run_measured([sys.executable, "-m", "gsbench.routes.es_export", "--url", url, "--index", "idx",
                                  "--out-dir", str(tmp_path / "out"), "--slices", "2", "--size", "7"],
                                 stdout_capture=True, cwd=str(Path(__file__).parent.parent))
    finally:
        server.shutdown()
    assert m["exit_code"] == 0
    stats = json.loads(m["stdout"])
    assert stats["docs"] == len(SOURCES)
    # two spawned interpreters each cost far more than the export itself
    assert m["cpu_s"] > stats["client_cpu_s"] and m["cpu_s"] > 0.1
