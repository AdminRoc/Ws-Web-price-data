"""Stable Warframe.market item identity checks and safe historical slug migration."""
import json
import os
import tempfile
from pathlib import Path


def english_name(item):
    i18n = item.get('i18n') or {}
    return str(((i18n.get('en') or {}).get('name')) or item.get('en') or
               item.get('name') or item.get('item_name') or '').strip()


def _name_key(name):
    return str(name or '').strip().casefold()


def validate_current_items(items, minimum=1500):
    if len(items) < minimum:
        raise ValueError('WM item manifest below minimum count: %d' % len(items))
    ids = [str(item.get('id') or '') for item in items]
    slugs = [str(item.get('slug') or '') for item in items]
    if any(not value for value in ids) or len(set(ids)) != len(ids):
        raise ValueError('WM item manifest has missing or duplicate stable ids')
    if any(not value for value in slugs) or len(set(slugs)) != len(slugs):
        raise ValueError('WM item manifest has missing or duplicate slugs')


def build_slug_renames(previous_meta, previous_ids, current_items, minimum=1500):
    """Map historical slug -> current slug, rejecting any unprovable identity loss.

    `previous_ids` is the sidecar keyed by the previous slug. On the one-time
    migration from the legacy schema, exact unique English names bridge renamed
    slugs; an unchanged slug is accepted only when its English name also matches.
    """
    validate_current_items(current_items, minimum=minimum)
    current_by_id = {str(item['id']): item for item in current_items}
    current_by_slug = {item['slug']: item for item in current_items}
    current_by_name = {}
    for item in current_items:
        key = _name_key(english_name(item))
        if key:
            current_by_name.setdefault(key, []).append(item)

    renames = {}
    claimed_ids = set()
    unresolved = []
    previous_ids = previous_ids or {}
    for old_slug, old in (previous_meta or {}).items():
        if old.get('wm_deleted') is True:
            continue
        old_id = str(previous_ids.get(old_slug) or old.get('wm_id') or '')
        if old_id:
            current = current_by_id.get(old_id)
        else:
            old_name = _name_key(old.get('name'))
            direct = current_by_slug.get(old_slug)
            if direct and old_name and _name_key(english_name(direct)) == old_name:
                current = direct
            else:
                matches = current_by_name.get(old_name, []) if old_name else []
                current = matches[0] if len(matches) == 1 else None
        if current is None:
            unresolved.append(old_slug)
            continue
        current_id = str(current['id'])
        if current_id in claimed_ids:
            raise ValueError('multiple published price items map to one current stable id')
        claimed_ids.add(current_id)
        if old_slug != current['slug']:
            renames[old_slug] = current['slug']

    if len(set(renames.values())) != len(renames):
        raise ValueError('multiple historical slugs map to one current slug')
    if unresolved:
        raise ValueError('WM item manifest lost %d published identities: %s' %
                         (len(unresolved), ', '.join(unresolved[:8])))
    return renames


def remap_previous_metadata(previous_meta, renames):
    """Keep metadata attached to an item when its public slug changes."""
    remapped = dict(previous_meta or {})
    for old_slug, new_slug in renames.items():
        if old_slug in remapped:
            remapped[new_slug] = remapped[old_slug]
    return remapped


def _load_json(path):
    with open(path, 'r', encoding='utf-8') as stream:
        return json.load(stream)


def _write_json_atomic(path, document):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            json.dump(document, stream, ensure_ascii=False, separators=(',', ':'))
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _rekey_items_map(items, renames, source):
    if not isinstance(items, dict):
        return False
    changed = False
    for old_slug, new_slug in renames.items():
        if old_slug not in items:
            continue
        if new_slug in items:
            raise ValueError('historical key collision in %s: %s and %s' %
                             (source, old_slug, new_slug))
        items[new_slug] = items.pop(old_slug)
        changed = True
    return changed


def plan_history_rekeys(data_dir, renames, current_by_slug):
    """Preflight all history collisions and return a write plan without changing files."""
    if not renames:
        return [], [], {'snapshots': 0, 'daily': 0, 'series': 0}
    root = Path(data_dir)
    doc_updates = []
    counts = {'snapshots': 0, 'daily': 0, 'series': 0}

    for directory, kind in ((root / 'snapshots', 'snapshots'), (root / 'daily', 'daily')):
        if not directory.exists():
            continue
        for path in sorted(directory.glob('*.json')):
            document = _load_json(path)
            changed = False
            if kind == 'snapshots':
                for batch in document.get('batches') or []:
                    changed = _rekey_items_map(batch.get('items'), renames, str(path)) or changed
            else:
                changed = _rekey_items_map(document.get('items'), renames, str(path))
            if changed:
                doc_updates.append((path, document, kind))
                counts[kind] += 1

    series_dir = root / 'series'
    series_moves = []
    if series_dir.exists():
        source_slugs = {old for old in renames if (series_dir / (old + '.json')).is_file()}
        destinations = set()
        for old_slug in source_slugs:
            new_slug = renames[old_slug]
            target = series_dir / (new_slug + '.json')
            if new_slug in destinations:
                raise ValueError('multiple series files target %s' % target)
            destinations.add(new_slug)
            if target.exists():
                raise ValueError('series file collision: %s already exists' % target)
            source = series_dir / (old_slug + '.json')
            document = _load_json(source)
            if not isinstance(document, dict):
                raise ValueError('invalid series document: %s' % source)
            document['slug'] = new_slug
            meta = current_by_slug.get(new_slug) or {}
            i18n = meta.get('i18n') or {}
            en = (i18n.get('en') or {}).get('name') or meta.get('name') or new_slug
            zh = ((i18n.get('zh-hans') or {}).get('name') or
                  (i18n.get('zh') or {}).get('name') or en)
            document['name'] = en
            document['name_zh'] = zh
            document['category'] = meta.get('category') or document.get('category') or 'other'
            series_moves.append((source, target, document))
            counts['series'] += 1

    return doc_updates, series_moves, counts


def apply_history_rekeys(data_dir, renames, current_by_slug):
    """Apply the fully preflighted history migration on the workflow runner."""
    doc_updates, series_moves, counts = plan_history_rekeys(data_dir, renames, current_by_slug)
    for path, document, kind in doc_updates:
        _write_json_atomic(path, document)
    for source, target, document in series_moves:
        _write_json_atomic(target, document)
        source.unlink()
    return counts


def write_identity_sidecar(path, items):
    mapping = {item['slug']: str(item['id']) for item in items}
    _write_json_atomic(path, {'schema_version': 1, 'source': 'warframe.market/v2/items', 'items': mapping})
