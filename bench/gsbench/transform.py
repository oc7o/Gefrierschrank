"""Baseline transform phase: exported NDJSON -> Delta table via delta-rs (ZSTD, ~256 MiB files).

Runs as its own process so its CPU and RSS are measured in isolation. _source stays a string
column holding the exact exported text, like Gefrierschrank's archive table.
"""

import argparse
import json
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.json as pj
from deltalake import WriterProperties, write_deltalake

SCHEMA = pa.schema([("_id", pa.string()), ("_routing", pa.string()), ("_source", pa.string())])
TARGET_FILE_BYTES = 256 * 1024 * 1024


def batches(files: list[Path]):
    options = pj.ParseOptions(explicit_schema=SCHEMA, unexpected_field_behavior="error", newlines_in_values=False)
    read = pj.ReadOptions(block_size=32 * 1024 * 1024, use_threads=True)
    for f in files:
        if f.stat().st_size == 0:
            continue
        reader = pj.open_json(f, read_options=read, parse_options=options)
        for batch in reader:
            yield batch


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in-dir", required=True)
    parser.add_argument("--table", required=True)
    args = parser.parse_args()
    files = sorted(Path(args.in_dir).glob("*.ndjson"))
    started = time.monotonic()
    rows = 0

    def counted():
        nonlocal rows
        for b in batches(files):
            rows += b.num_rows
            yield b

    reader = pa.RecordBatchReader.from_batches(SCHEMA, counted())
    write_deltalake(args.table, reader, mode="overwrite", target_file_size=TARGET_FILE_BYTES,
                    writer_properties=WriterProperties(compression="ZSTD"))
    json.dump({"rows": rows, "seconds": time.monotonic() - started, "inputs": len(files)}, sys.stdout)


if __name__ == "__main__":
    main()
