import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("validate_price_release.py")


class PriceReleaseValidatorTest(unittest.TestCase):
    def test_rejects_chunk_revision_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            data = Path(directory)
            (data / "meta").mkdir()
            (data / "table/kv").mkdir(parents=True)
            (data / "meta/latest.json").write_text(json.dumps({"schema_version": 2, "model_version": "price-model-v1", "data_revision": "r1", "table_items": 1}), encoding="utf-8")
            (data / "table/kv-index.json").write_text(json.dumps({"schema_version": 2, "model_version": "price-model-v1", "data_revision": "r1", "chunks": [{"key": "price_table_chunk_000", "file": "kv/000.json", "count": 1}]}), encoding="utf-8")
            (data / "table/latest.json").write_text(json.dumps({"items": {"item": {}}}), encoding="utf-8")
            (data / "table/kv/000.json").write_text(json.dumps({"data_revision": "stale", "items": {"item": {}}}), encoding="utf-8")
            result = subprocess.run([sys.executable, str(SCRIPT), "--data-dir", str(data)], text=True, capture_output=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn("data_revision mismatch", result.stdout)


if __name__ == "__main__":
    unittest.main()
