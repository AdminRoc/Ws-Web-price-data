import json
from pathlib import Path
import tempfile
import unittest

from scripts.item_catalog_kv_shards import build_catalog_shards


class ItemCatalogKVShardTests(unittest.TestCase):
    def make_catalog(self, count=12):
        return {
            "schema_version": 2,
            "data_revision": "run-123",
            "model_version": "price-model-v1",
            "generated": "2026-09-30T00:00:00Z",
            "source": "warframe-market",
            "items": {
                f"item_{index:03d}": {
                    "name": f"Item {index}",
                    "name_zh": "测试物品" + str(index),
                    "tags": ["Mods", "Warframe"],
                    "maxRank": 10,
                }
                for index in range(count)
            },
        }

    def test_round_trip_exact_item_identity_and_bounded_payloads(self):
        catalog = self.make_catalog()
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "items.json"
            output = root / "shards"
            source.write_text(json.dumps(catalog, ensure_ascii=False), encoding="utf-8")
            manifest = build_catalog_shards(source, output, max_part_bytes=900, max_parts=8)
            self.assertGreater(len(manifest["parts"]), 1)

            restored = {}
            for part in manifest["parts"]:
                raw = (output / f"{part['key']}.json").read_bytes()
                self.assertLessEqual(len(raw), 900)
                self.assertEqual(len(raw), part["bytes"])
                value = json.loads(raw)
                self.assertEqual(value["data_revision"], manifest["data_revision"])
                self.assertEqual(len(value["items"]), part["item_count"])
                for slug, record in value["items"].items():
                    self.assertNotIn(slug, restored)
                    restored[slug] = record

            rebuilt = {**manifest["root"], "items": restored}
            self.assertEqual(rebuilt, catalog)
            self.assertEqual(sum(part["item_count"] for part in manifest["parts"]), len(catalog["items"]))

    def test_fails_closed_when_catalog_exceeds_fixed_shard_ceiling(self):
        catalog = self.make_catalog(count=100)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "items.json"
            source.write_text(json.dumps(catalog, ensure_ascii=False), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "configured maximum"):
                build_catalog_shards(source, root / "shards", max_part_bytes=900, max_parts=1)

    def test_duplicate_json_keys_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = root / "items.json"
            source.write_text('{"data_revision":"r1","items":{"a":{},"a":{}}}', encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate JSON object key"):
                build_catalog_shards(source, root / "shards", max_part_bytes=700, max_parts=8)


if __name__ == "__main__":
    unittest.main()
