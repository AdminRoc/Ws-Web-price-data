#!/usr/bin/env python3
"""Prepare bounded, content-addressed PRICE_KV shards for today's snapshot."""

import argparse
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path


MAX_VALUE_BYTES = 768 * 1024


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def build_shards(snapshot_path, meta_path, output_dir):
    snapshot_path = Path(snapshot_path)
    meta_path = Path(meta_path)
    output_dir = Path(output_dir)
    raw = snapshot_path.read_bytes()
    snapshot = json.loads(raw)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))

    day = snapshot.get("date")
    if not isinstance(day, str) or day != meta.get("today"):
        raise ValueError("snapshot date does not match ready metadata")
    generation = hashlib.sha256(raw).hexdigest()
    revision = str(snapshot.get("data_revision", ""))
    if not revision or revision != str(meta.get("data_revision", "")):
        raise ValueError("snapshot revision does not match ready metadata")
    batches = snapshot.get("batches")
    if not isinstance(batches, list) or len(batches) != meta.get("snapshot_batches_today"):
        raise ValueError("snapshot batch count does not match ready metadata")

    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_batches = []
    for batch_index, batch in enumerate(batches):
        items = batch.get("items") if isinstance(batch, dict) else None
        if not isinstance(items, dict) or not items:
            raise ValueError(f"batch {batch_index} has no item map")
        batch_time = batch.get("time")
        if not isinstance(batch_time, str) or not batch_time:
            raise ValueError(f"batch {batch_index} has no timestamp")

        # Split by item identity so every KV value remains below a conservative
        # bound even if the catalog or per-item record grows over time.
        parts = []
        current = {}
        empty_payload = {
            "schema_version": 1,
            "day": day,
            "batch_index": batch_index,
            "part_index": 0,
            "time": batch_time,
            "items": {},
        }
        current_bytes = len(encode(empty_payload))

        def make_part(part_index, part_items):
            payload = {
                "schema_version": 1,
                "day": day,
                "batch_index": batch_index,
                "part_index": part_index,
                "time": batch_time,
                "items": part_items,
            }
            encoded = encode(payload)
            if len(encoded) > MAX_VALUE_BYTES:
                raise ValueError(f"batch {batch_index} part {part_index} exceeds safe KV size")
            digest = hashlib.sha256(encoded).hexdigest()
            key = f"price_snapshot_{day.replace('-', '')}_{batch_index:03d}_{part_index:03d}_{digest[:16]}"
            (output_dir / f"{key}.json").write_bytes(encoded)
            return {"key": key, "sha256": digest, "bytes": len(encoded), "item_count": len(part_items)}

        for slug in sorted(items):
            slug_json = json.dumps(slug, ensure_ascii=False, separators=(",", ":"))
            item_json = json.dumps(items[slug], ensure_ascii=False, separators=(",", ":"), sort_keys=True)
            item_bytes = len((slug_json + ":" + item_json).encode("utf-8"))
            candidate_bytes = current_bytes + item_bytes + (1 if current else 0)
            if candidate_bytes > MAX_VALUE_BYTES:
                if not current:
                    raise ValueError(f"single item {slug!r} exceeds safe KV size")
                parts.append(make_part(len(parts), current))
                current = {slug: items[slug]}
                current_bytes = len(encode({**empty_payload, "part_index": len(parts)})) + item_bytes
                single = {
                    "schema_version": 1,
                    "day": day,
                    "batch_index": batch_index,
                    "part_index": len(parts),
                    "time": batch_time,
                    "items": current,
                }
                if len(encode(single)) > MAX_VALUE_BYTES:
                    raise ValueError(f"single item {slug!r} exceeds safe KV size")
            else:
                current[slug] = items[slug]
                current_bytes = candidate_bytes
        if current:
            parts.append(make_part(len(parts), current))

        if sum(part["item_count"] for part in parts) != len(items):
            raise ValueError(f"batch {batch_index} shard item count mismatch")
        manifest_batches.append({
            "time": batch_time,
            "planned_items": batch.get("planned_items"),
            "captured_items": batch.get("captured_items", len(items)),
            "error_items": batch.get("error_items"),
            "item_count": len(items),
            "parts": parts,
        })

    manifest = {
        "schema_version": 1,
        "data_revision": revision,
        "snapshot_generation": generation,
        "day": day,
        "generated": snapshot.get("generated"),
        "batch_count": len(manifest_batches),
        "batches": manifest_batches,
    }
    manifest_bytes = encode(manifest)
    if len(manifest_bytes) > MAX_VALUE_BYTES:
        raise ValueError("snapshot manifest exceeds safe KV size")
    (output_dir / "price_today_snapshots_manifest.json").write_bytes(manifest_bytes)
    cutoff = (date.fromisoformat(day) - timedelta(days=7)).strftime("%Y%m%d")
    expected = [part for batch in manifest_batches for part in batch["parts"]]
    (output_dir / "price_snapshot_prepare.json").write_bytes(encode({
        "action": "prepare_snapshot_shards",
        "day": day,
        "cutoff": cutoff,
        "shards": expected,
        "manifest": {
            "key": "price_today_snapshots_manifest",
            "bytes": len(manifest_bytes),
            "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        },
    }))
    return manifest, manifest_bytes


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot")
    parser.add_argument("meta")
    parser.add_argument("output_dir")
    args = parser.parse_args()
    manifest, manifest_bytes = build_shards(args.snapshot, args.meta, args.output_dir)
    part_count = sum(len(batch["parts"]) for batch in manifest["batches"])
    print(f"snapshot shard manifest verified: batches={manifest['batch_count']} parts={part_count} bytes={len(manifest_bytes)}")


if __name__ == "__main__":
    main()
