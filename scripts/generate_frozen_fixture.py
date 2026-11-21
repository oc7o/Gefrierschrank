"""Generate a small real mounted-snapshot fixture using the local ES/MinIO test services.

Creates uniquely named test indices/repository; never touches existing indices. Requires
an active trial/enterprise license on the local test cluster. Local endpoints only.
Run from the repository root: python3 scripts/generate_frozen_fixture.py
"""
import datetime
import hashlib
import hmac
import json
import pathlib
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET


def es(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request("http://localhost:9200" + path, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def s3_get(path, query=""):
    # Fixed local fixture credentials only; deliberately does not read user AWS credentials.
    now = datetime.datetime.now(datetime.timezone.utc)
    day, timestamp = now.strftime("%Y%m%d"), now.strftime("%Y%m%dT%H%M%SZ")
    digest = hashlib.sha256(b"").hexdigest()
    headers = f"host:localhost:9000\nx-amz-content-sha256:{digest}\nx-amz-date:{timestamp}\n"
    signed = "host;x-amz-content-sha256;x-amz-date"
    canonical = f"GET\n{path}\n{query}\n{headers}\n{signed}\n{digest}"
    scope = f"{day}/us-east-1/s3/aws4_request"
    to_sign = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
    key = b"AWS4minioadmin"
    for part in (day, "us-east-1", "s3", "aws4_request"):
        key = hmac.new(key, part.encode(), hashlib.sha256).digest()
    signature = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
    request = urllib.request.Request("http://localhost:9000" + path + ("?" + query if query else ""), headers={
        "x-amz-date": timestamp, "x-amz-content-sha256": digest,
        "Authorization": f"AWS4-HMAC-SHA256 Credential=minioadmin/{scope}, SignedHeaders={signed}, Signature={signature}"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def main():
    suffix = uuid.uuid4().hex[:8]
    repository = "full-export-test-" + suffix
    index = "frozen-docs-" + suffix
    mounted = "partial-" + index
    prefix = "full-export-test/" + suffix
    output = pathlib.Path("gefrierschrank-core/src/test/resources/fixtures/frozen-repo")
    if output.exists():
        raise RuntimeError("Fixture output already exists; use a fresh checkout or move it first")
    es("PUT", "/_snapshot/" + repository, {"type": "s3", "settings": {
        "bucket": "snapshots", "base_path": prefix, "endpoint": "http://minio:9000",
        "protocol": "http", "path_style_access": True}})
    es("PUT", "/" + index, {"settings": {"number_of_shards": 2, "number_of_replicas": 0},
        "mappings": {"properties": {"children": {"type": "nested", "properties": {"n": {"type": "long"}}}}}})
    es("PUT", f"/{index}/_doc/a?routing=tenant", {"value": 1, "children": [{"n": 1}, {"n": 2}]})
    es("PUT", f"/{index}/_doc/b", {"value": [2, 3], "extra": {"text": "retained"}})
    es("PUT", f"/{index}/_doc/deleted", {"value": 999})
    es("POST", f"/{index}/_refresh")
    es("PUT", f"/_snapshot/{repository}/older?wait_for_completion=true", {"indices": index, "include_global_state": False})
    es("PUT", f"/{index}/_doc/a?routing=tenant", {"value": 10, "children": [{"n": 4}]})
    es("DELETE", f"/{index}/_doc/deleted")
    es("POST", f"/{index}/_refresh")
    es("PUT", f"/_snapshot/{repository}/backing?wait_for_completion=true", {"indices": index, "include_global_state": False})
    es("POST", f"/_snapshot/{repository}/backing/_mount?wait_for_completion=true&storage=full_copy", {
        "index": index, "renamed_index": mounted, "index_settings": {"index.number_of_replicas": 0}})
    es("GET", f"/_cluster/health/{mounted}?wait_for_status=green&timeout=60s")
    es("PUT", f"/_snapshot/{repository}/mounted-backup?wait_for_completion=true", {
        "indices": mounted, "include_global_state": False})
    expected = es("GET", f"/{mounted}/_search?size=100&sort=_doc")["hits"]["hits"]
    # Give the backing index a newer snapshot that the mounted view must NOT follow.
    es("PUT", f"/{index}/_doc/newer-only", {"value": 1234})
    es("POST", f"/{index}/_refresh")
    es("PUT", f"/_snapshot/{repository}/latest?wait_for_completion=true", {"indices": index, "include_global_state": False})
    ns = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
    query = urllib.parse.urlencode({"list-type": "2", "prefix": prefix + "/"}, quote_via=urllib.parse.quote)
    listing = ET.fromstring(s3_get("/snapshots", query))
    if listing.findtext("s:IsTruncated", namespaces=ns) != "false":
        raise RuntimeError("Unexpectedly large fixture listing")
    for item in listing.findall("s:Contents", ns):
        key = item.findtext("s:Key", namespaces=ns)
        target = output / key.removeprefix(prefix + "/")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(s3_get("/snapshots/" + urllib.parse.quote(key, safe="/")))
    (output.parent / "frozen-expected.json").write_text(json.dumps({
        "elasticsearch_version": es("GET", "/")["version"]["number"],
        "mount_storage": "full_copy", "index": index, "mounted_index": mounted,
        "mounted_documents": expected, "latest_document_count": 3}, indent=2))
    print(f"Wrote real mounted snapshot fixture to {output}; mounted rows={len(expected)}")


if __name__ == "__main__":
    main()
