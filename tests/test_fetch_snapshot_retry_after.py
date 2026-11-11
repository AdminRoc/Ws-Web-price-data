import importlib.util
import pathlib
import sys
import types
import unittest
from email.utils import formatdate
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
SPEC = importlib.util.spec_from_file_location("snapshot_retry_under_test", ROOT / "scripts/fetch_snapshots.py")
MODULE = importlib.util.module_from_spec(SPEC)
# These pure retry-delay tests neither need an HTTP client nor make requests.
with mock.patch.dict(sys.modules, {"aiohttp": types.ModuleType("aiohttp")}):
    SPEC.loader.exec_module(MODULE)


class SnapshotRetryAfterTests(unittest.TestCase):
    def delay(self, value, attempt=0):
        response = types.SimpleNamespace(headers={"Retry-After": value})
        with mock.patch.object(MODULE.random, "random", return_value=0):
            return MODULE._backoff(attempt, response)

    def test_server_wait_above_local_cap_is_not_shortened(self):
        self.assertEqual(self.delay("300"), 300)

    def test_http_date_wait_is_honored(self):
        now = 1800000000
        with mock.patch.object(MODULE.time, "time", return_value=now):
            self.assertEqual(self.delay(formatdate(now + 300, usegmt=True)), 300)

    def test_short_header_does_not_shorten_local_exponential_backoff(self):
        self.assertEqual(self.delay("1", attempt=3), MODULE.BACKOFF_CAP)

    def test_invalid_or_negative_headers_keep_bounded_local_backoff(self):
        for value in ("not-a-date", "NaN", "inf", "-2", "0"):
            with self.subTest(value=value):
                self.assertEqual(self.delay(value, attempt=3), MODULE.BACKOFF_CAP)


if __name__ == "__main__":
    unittest.main()
