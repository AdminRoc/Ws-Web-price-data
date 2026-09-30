#!/usr/bin/env python3
"""Split the public item catalog into a fixed, conservative set of PRICE_KV values."""

import argparse
import hashlib
import json
from pathlib import Path


MAX_PART_BYTES = 256 * 1024
MAX_PARTS = 8
MAX_REQUEST_BODY_BYTES = 1024 * 1024


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def reject_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def build_catalog_shards(source_path, output_dir, max_part_bytes=MAX_PART_BYTES, max_parts=MAX_PARTS):
    source_path = Path(source_path)
    output_dir = Path(output_dir)
    source_bytes = source_path.read_bytes()
    catalog = json.loads(source_bytes, object_pairs_hook=reject_duplicate_keys)
    if not isinstance(catalog, dict) or not isinstance(catalog.get("items"), dict) or not catalog["items"]:
        raise ValueError("item catalog must be an object with a non-empty items object")
    revision = catalog.get("data_revision")
    if not isinstance(revision, (str, int)) or not str(revision):
        raise ValueError("item catalog has no data_revision")
    if max_part_bytes <= 0 or max_parts <= 0:
        raise ValueError("shard limits must be positive")

    root = {key: value for key, value in catalog.items() if key != "items"}
    prefix = b"{" + b",".join(
        encode(key) + b":" + encode(value) for key, value in root.items()
    ) + b',"items":{'
    suffix = b"}}"
    chunks = []
    current_entries = []
    current_bytes = len(prefix) + len(suffix)

    def flush():
        nonlocal current_entries, current_bytes
        if not current_entries:
            return
        index = len(chunks)
        key = f"price_items_chunk_{index:03d}"
        payload = prefix + b",".join(current_entries) + suffix
        if len(payload) > max_part_bytes:
            raise ValueError(f"catalog part {index} exceeds the safe KV value size")
        # The legacy endpoint envelope escapes the JSON string. Keep margin under
        # the stricter one-MiB Edge Functions request-body quota as well.
        envelope = encode({"key": key, "value": payload.decode("utf-8")})
        if len(envelope) > MAX_REQUEST_BODY_BYTES:
            raise ValueError(f"catalog part {index} exceeds the Edge Functions request-body limit")
        digest = hashlib.sha256(payload).hexdigest()
        chunks.append({
            "key": key,
            "index": index,
            "sha256": digest,
            "bytes": len(payload),
            "item_count": len(current_entries),
            "payload": payload,
        })
        current_entries = []
        current_bytes = len(prefix) + len(suffix)

    for slug, item in catalog["items"].items():
        if not isinstance(slug, str) or not slug or not isinstance(item, dict):
            raise ValueError("item catalog contains an invalid slug or item record")
        entry = encode(slug) + b":" + encode(item)
        entry_bytes = len(entry) + (1 if current_entries else 0)
        if current_entries and current_bytes + entry_bytes > max_part_bytes:
            flush()
            entry_bytes = len(entry)
        if current_bytes + entry_bytes > max_part_bytes:
            raise ValueError(f"item record {slug} exceeds the safe KV value size")
        current_entries.append(entry)
        current_bytes += entry_bytes
    flush()

    if len(chunks) > max_parts:
        raise ValueError(f"item catalog requires {len(chunks)} shards; configured maximum is {max_parts}")

    output_dir.mkdir(parents=True, exist_ok=True)
    for old in output_dir.glob("price_items_chunk_*.json"):
        old.unlink()
    for chunk in chunks:
        (output_dir / f"{chunk['key']}.json").write_bytes(chunk["payload"])

    manifest = {
        "schema_version": 1,
        "data_revision": str(revision),
        "source_sha256": hashlib.sha256(source_bytes).hexdigest(),
        "item_count": len(catalog["items"]),
        "root": root,
        "parts": [{key: value for key, value in chunk.items() if key != "payload"} for chunk in chunks],
    }
    manifest_bytes = encode(manifest)
    if len(manifest_bytes) > max_part_bytes:
        raise ValueError("item catalog manifest exceeds the safe KV value size")
    (output_dir / "price_items_manifest.json").write_bytes(manifest_bytes)
    return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source")
    parser.add_argument("output_dir")
    args = parser.parse_args()
    manifest = build_catalog_shards(args.source, args.output_dir)
    print(
        f"item catalog KV shards verified: items={manifest['item_count']} "
        f"parts={len(manifest['parts'])} bytes={sum(part['bytes'] for part in manifest['parts'])} "
        f"max_parts={MAX_PARTS} per_part_limit={MAX_PART_BYTES}"
    )


if __name__ == "__main__":
    main()
