#!/usr/bin/env python3
"""Validate local price release coherence without contacting KV or external APIs."""
import hashlib
import json
import argparse
from pathlib import Path


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default=Path(__file__).parents[1] / "data", type=Path)
    args = parser.parse_args()
    data = args.data_dir
    meta = load(data / "meta/latest.json")
    index = load(data / "table/kv-index.json")
    table = load(data / "table/latest.json")
    errors = []
    expected = {"schema_version": 2, "model_version": "price-model-v1"}
    for name, document in (("meta", meta), ("index", index)):
        for key, value in expected.items():
            if document.get(key) != value:
                errors.append(f"{name}: {key} != {value!r}")
    revision = meta.get("data_revision")
    if not revision or index.get("data_revision") != revision:
        errors.append("meta/index data_revision mismatch")
    seen = set()
    item_count = 0
    manifest = []
    for chunk in index.get("chunks", []):
        key, relative = chunk.get("key"), chunk.get("file")
        path = data / "table" / relative
        if not key or key in seen or not path.is_file():
            errors.append(f"invalid chunk declaration: {key} {relative}")
            continue
        seen.add(key)
        document = load(path)
        if document.get("data_revision") != revision:
            errors.append(f"{key}: data_revision mismatch")
        items = document.get("items") or {}
        if len(items) != chunk.get("count"):
            errors.append(f"{key}: item count mismatch")
        item_count += len(items)
        manifest.append({"key": key, "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    table_items = table.get("items") or {}
    if item_count != len(table_items) or meta.get("table_items") != len(table_items):
        errors.append("table item count mismatch")
    report = {"data_revision": revision, "chunks": manifest, "table_items": len(table_items), "errors": errors}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    raise SystemExit(bool(errors))


if __name__ == "__main__":
    main()
