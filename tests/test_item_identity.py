import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from item_identity import apply_history_rekeys, build_slug_renames, remap_previous_metadata


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')


class ItemIdentityTests(unittest.TestCase):
    def test_legacy_name_bridge_and_stable_id_mapping(self):
        old_meta = {
            'old_slug': {'name': 'Cleanse Orokin', 'wm_deleted': False},
            'same_slug': {'name': 'Same Name', 'wm_deleted': False},
            'retired_slug': {'name': 'Retired', 'wm_deleted': True},
        }
        old_ids = {'old_slug': 'id-1', 'same_slug': 'id-2'}
        current = [
            {'id': 'id-1', 'slug': 'cleanse_orokin', 'i18n': {'en': {'name': 'Renamed Name'}}},
            {'id': 'id-2', 'slug': 'same_slug', 'i18n': {'en': {'name': 'Same Name'}}},
        ]
        self.assertEqual(build_slug_renames(old_meta, old_ids, current, minimum=1),
                         {'old_slug': 'cleanse_orokin'})

    def test_legacy_bridge_reads_current_ws_web_en_field(self):
        old_meta = {'old_slug': {'name': 'Secura Dual Cestra', 'wm_deleted': False}}
        current = [{'id': 'wm-id-1', 'slug': 'new_slug', 'en': 'Secura Dual Cestra'}]
        self.assertEqual(build_slug_renames(old_meta, {}, current, minimum=1),
                         {'old_slug': 'new_slug'})

    def test_legacy_name_bridge_rejects_ambiguous_mapping(self):
        old_meta = {'old_slug': {'name': 'Same Name', 'wm_deleted': False}}
        current = [
            {'id': 'id-1', 'slug': 'new_one', 'i18n': {'en': {'name': 'Same Name'}}},
            {'id': 'id-2', 'slug': 'new_two', 'i18n': {'en': {'name': 'Same Name'}}},
        ]
        with self.assertRaisesRegex(ValueError, 'lost 1 published identities'):
            build_slug_renames(old_meta, {}, current, minimum=1)

    def test_renamed_item_keeps_its_previous_metadata(self):
        previous = {
            'old_slug': {'name': 'Old English Name', 'name_zh': '旧名', 'wm_deleted': False}
        }
        result = remap_previous_metadata(previous, {'old_slug': 'new_slug'})
        self.assertIs(result['new_slug'], previous['old_slug'])
        self.assertIn('old_slug', result)

    def test_history_rekey_moves_all_public_history_shapes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            write_json(root / 'snapshots' / '2026-09-29.json', {
                'batches': [{'items': {'old_slug': {'avg': 10}}}]
            })
            write_json(root / 'daily' / '2026-09-28.json', {
                'items': {'old_slug': {'avg': 11}}
            })
            write_json(root / 'series' / 'old_slug.json', {
                'slug': 'old_slug', 'name': 'Old', 'name_zh': '旧', 'category': 'other',
                'days': [{'d': '2026-09-28', 'avg': 11}],
            })
            result = apply_history_rekeys(tmp, {'old_slug': 'new_slug'}, {
                'new_slug': {'slug': 'new_slug', 'name': 'New', 'i18n': {'en': {'name': 'New'}}}
            })
            self.assertEqual(result, {'snapshots': 1, 'daily': 1, 'series': 1})
            self.assertIn('new_slug', json.loads((root / 'snapshots' / '2026-09-29.json').read_text(encoding='utf-8'))['batches'][0]['items'])
            self.assertIn('new_slug', json.loads((root / 'daily' / '2026-09-28.json').read_text(encoding='utf-8'))['items'])
            series = json.loads((root / 'series' / 'new_slug.json').read_text(encoding='utf-8'))
            self.assertEqual(series['slug'], 'new_slug')
            self.assertEqual(series['name'], 'New')
            self.assertFalse((root / 'series' / 'old_slug.json').exists())

    def test_any_history_collision_aborts_before_files_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot_path = root / 'snapshots' / '2026-09-29.json'
            original = {'batches': [{'items': {'old_slug': {'avg': 10}, 'new_slug': {'avg': 20}}}]}
            write_json(snapshot_path, original)
            with self.assertRaisesRegex(ValueError, 'historical key collision'):
                apply_history_rekeys(tmp, {'old_slug': 'new_slug'}, {'new_slug': {}})
            self.assertEqual(json.loads(snapshot_path.read_text(encoding='utf-8')), original)


if __name__ == '__main__':
    unittest.main()
