import importlib.util
import pathlib
import socket
import unittest
import urllib.error
from unittest import mock


ROOT = pathlib.Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("price_release_pointer", ROOT / "scripts/publish_release_pointer.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class Response:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return b"ok"


class PricePointerRetryTests(unittest.TestCase):
    @mock.patch.object(MODULE.time, "sleep")
    @mock.patch.object(MODULE.urllib.request, "urlopen")
    def test_get_retries_transient_timeout(self, urlopen, sleep):
        urlopen.side_effect = [socket.timeout("temporary"), Response()]
        self.assertEqual(MODULE.get("https://example.test", attempts=2, delay=0), b"ok")
        self.assertEqual(urlopen.call_count, 2)
        sleep.assert_called_once_with(0)

    @mock.patch.object(MODULE.time, "sleep")
    @mock.patch.object(MODULE.urllib.request, "urlopen")
    def test_get_does_not_retry_not_found(self, urlopen, _sleep):
        urlopen.side_effect = urllib.error.HTTPError("https://example.test", 404, "missing", {}, None)
        with self.assertRaises(urllib.error.HTTPError):
            MODULE.get("https://example.test", attempts=4, delay=0)
        urlopen.assert_called_once()

    @mock.patch.object(MODULE.time, "sleep")
    def test_pointer_readback_waits_for_expected_value(self, _sleep):
        expected = {"release_id": "new"}
        with mock.patch.object(MODULE, "current_pointer", side_effect=[None, expected]):
            MODULE.verify_pointer_after_write(expected, attempts=2, delay=0)


if __name__ == "__main__":
    unittest.main()
