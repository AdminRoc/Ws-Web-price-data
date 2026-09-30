import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from snapshot_kv_shards import MAX_VALUE_BYTES, build_shards


class SnapshotKvShardsTests(unittest.TestCase):
    def make_inputs(self, root, batches):
        snapshot = {
            "date": "2026-09-30",
            "data_revision": 123,
            "generated": "2026-09-30T12:00:00Z",
            "batches": batches,
        }
        meta = {
            "today": snapshot["date"],
            "data_revision": snapshot["data_revision"],
            "snapshot_batches_today": len(batches),
            "last_snapshot": batches[-1]["time"] if batches else None,
        }
        snapshot_path = root / "snapshot.json"
        meta_path = root / "meta.json"
        snapshot_path.write_text(json.dumps(snapshot, separators=(",", ":")), encoding="utf-8")
        meta_path.write_text(json.dumps(meta), encoding="utf-8")
        return snapshot_path, meta_path

    def test_shards_round_trip_with_per_value_limit(self):
        items = {f"item_{i:02d}": {"avg": i, "padding": "x" * 390000} for i in range(3)}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot_path, meta_path = self.make_inputs(root, [{"time": "2026-09-30T12:00:00Z", "items": items}])
            manifest, _ = build_shards(snapshot_path, meta_path, root / "out")
            refs = manifest["batches"][0]["parts"]
            restored = {}
            for ref in refs:
                raw = (root / "out" / f"{ref['key']}.json").read_bytes()
                self.assertLessEqual(len(raw), MAX_VALUE_BYTES)
                payload = json.loads(raw)
                self.assertEqual(len(payload["items"]), ref["item_count"])
                self.assertFalse(set(restored).intersection(payload["items"]))
                restored.update(payload["items"])
            self.assertEqual(restored, items)
            self.assertEqual(manifest["snapshot_generation"], __import__("hashlib").sha256(snapshot_path.read_bytes()).hexdigest())

            shard_files = sorted((root / "out").glob("price_snapshot_????????_???_???_????????????????.json"))
            self.assertEqual(
                {path.stem for path in shard_files},
                {part["key"] for part in refs},
                "the strict publish glob must select only content-addressed data shards",
            )
            self.assertTrue((root / "out" / "price_snapshot_prepare.json").is_file())
            workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/fetch-snapshots.yml").read_text(encoding="utf-8")
            self.assertIn('for part in "$ARTIFACT_DIR"/price_snapshot_????????_???_???_????????????????.json; do', workflow)
            self.assertNotIn('for part in "$ARTIFACT_DIR"/price_snapshot_*.json; do', workflow)

    def test_existing_batch_keys_are_stable_when_new_batch_is_appended(self):
        first = {"time": "2026-09-30T10:00:00Z", "items": {"a": {"avg": 1}}}
        second = {"time": "2026-09-30T12:00:00Z", "items": {"a": {"avg": 2}}}
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot_path, meta_path = self.make_inputs(root, [first])
            old_manifest, _ = build_shards(snapshot_path, meta_path, root / "old")
            snapshot_path, meta_path = self.make_inputs(root, [first, second])
            new_manifest, _ = build_shards(snapshot_path, meta_path, root / "new")
            self.assertEqual(
                old_manifest["batches"][0]["parts"][0]["key"],
                new_manifest["batches"][0]["parts"][0]["key"],
            )

    def test_metadata_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            snapshot_path, meta_path = self.make_inputs(root, [{"time": "t", "items": {"a": {}}}])
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            meta["data_revision"] = 999
            meta_path.write_text(json.dumps(meta), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "revision"):
                build_shards(snapshot_path, meta_path, root / "out")


if __name__ == "__main__":
    unittest.main()
