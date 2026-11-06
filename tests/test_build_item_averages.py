import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
import build_item_averages


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')


def row(avg, *, max_rank=0, avg_zero=None, avg_max=None):
    value = {'avg': avg, 'count': 0 if avg is None else 2,
             'used': 0 if avg is None else 2, 'total': 0 if avg is None else avg * 2}
    if max_rank:
        value.update({'max_rank': max_rank, 'avg_zero': avg_zero, 'avg_max': avg_max})
    return value


def rank_metric(avg):
    return {'avg': avg, 'count': 0 if avg is None else 2,
            'used': 0 if avg is None else 2}


class BuildItemAveragesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.data = Path(self.temp.name) / 'data'
        build_item_averages.DATA = self.data
        build_item_averages.OUTPUT = self.data / 'meta' / 'item-averages.json'
        write_json(self.data / 'meta' / 'latest.json', {
            'data_revision': 'current-revision',
            'generated': '2026-11-06T12:00:00Z',
            'last_snapshot': '2026-11-06T12:00:00Z',
        })
        write_json(self.data / 'meta' / 'items.json', {
            'items': {
                'regular_item': {'maxRank': 0},
                'ranked_item': {'maxRank': 10},
                'retired_item': {'maxRank': 0, 'wm_deleted': True},
            },
        })

    def test_builds_complete_map_and_carries_recent_metrics(self):
        write_json(self.data / 'snapshots' / 'previous.json', {
            'data_revision': 'previous-revision',
            'batches': [{
                'time': '2026-11-06T08:00:00Z',
                'planned_items': 2,
                'captured_items': 2,
                'error_items': 0,
                'items': {
                    'regular_item': row(30),
                    'ranked_item': row(
                        40, max_rank=10, avg_zero=rank_metric(41), avg_max=rank_metric(42),
                    ),
                },
            }],
        })
        write_json(self.data / 'snapshots' / 'current.json', {
            'data_revision': 'current-revision',
            'batches': [{
                'time': '2026-11-06T12:00:00Z',
                'planned_items': 2,
                'captured_items': 2,
                'error_items': 0,
                'items': {
                    'regular_item': row(50),
                    'ranked_item': row(
                        None, max_rank=10, avg_zero=rank_metric(None), avg_max=rank_metric(None),
                    ),
                },
            }],
        })

        result = build_item_averages.build()

        self.assertEqual(result['item_count'], 2)
        self.assertNotIn('retired_item', result['items'])
        self.assertEqual(result['items']['regular_item']['avg'], 50)
        ranked = result['items']['ranked_item']
        self.assertEqual(ranked['avg'], 40)
        self.assertTrue(ranked['stale'])
        self.assertEqual(ranked['avg_zero']['avg'], 41)
        self.assertEqual(ranked['avg_max']['avg'], 42)
        self.assertEqual(result['source']['stale_metric_count'], 3)
        self.assertEqual(result['source']['stale_item_count'], 1)
        self.assertEqual(json.loads(build_item_averages.OUTPUT.read_text(encoding='utf-8')), result)

    def test_incomplete_snapshot_preserves_existing_artifact(self):
        original = b'{"previous":"release"}\n'
        build_item_averages.OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        build_item_averages.OUTPUT.write_bytes(original)
        write_json(self.data / 'snapshots' / 'partial.json', {
            'batches': [{
                'time': '2026-11-06T12:00:00Z',
                'planned_items': 2,
                'captured_items': 1,
                'error_items': 1,
                'items': {'regular_item': row(50)},
            }],
        })

        result = build_item_averages.build()

        self.assertIsNone(result)
        self.assertEqual(build_item_averages.OUTPUT.read_bytes(), original)


if __name__ == '__main__':
    unittest.main()
