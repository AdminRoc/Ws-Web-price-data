import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]


class PriceKvWorkflowTest(unittest.TestCase):
    def test_publish_paths_read_back_each_written_key_before_price_meta(self):
        for name in ("fetch-snapshots.yml", "publish-kv.yml"):
            text = (ROOT / ".github/workflows" / name).read_text(encoding="utf-8")
            self.assertIn("https://price.wfspeed.run/api/update-kv", text)
            self.assertIn("https://price.wfspeed.run/api/kv?key=", text)
            self.assertIn("readback hash mismatch for", text)
            self.assertLess(text.index("readback hash mismatch for"), text.index("publish price_meta data/meta/latest.json"))
            self.assertLess(text.index("publish price_table_index"), text.index("publish price_meta data/meta/latest.json"))


if __name__ == "__main__":
    unittest.main()
