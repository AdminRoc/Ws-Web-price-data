#!/usr/bin/env python3
"""Build a consumer-facing average map from a complete WM order snapshot.

The map is an immutable Price-data release artifact. Partial/failed captures do
not replace the previous map and do not block publication of accumulated data.
"""
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
OUTPUT = DATA / "meta" / "item-averages.json"
STALE_FALLBACK_HOURS = 8


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def parse_time(value):
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def valid_metric(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def validate_row(slug, row, max_rank):
    if not isinstance(row, dict) or row.get("error"):
        return False
    if "avg" not in row or (row["avg"] is not None and not valid_metric(row["avg"])):
        return False
    if not valid_metric(row.get("count")) or not valid_metric(row.get("used")):
        return False
    if row["used"] > row["count"]:
        return False
    if not valid_metric(row.get("total")):
        return False
    for key in ("avg_zero", "avg_max"):
        metric = row.get(key)
        if metric is not None:
            if not isinstance(metric, dict):
                return False
            if metric.get("avg") is not None and not valid_metric(metric["avg"]):
                return False
            if not valid_metric(metric.get("count")) or not valid_metric(metric.get("used")):
                return False
            if metric["used"] > metric["count"]:
                return False
    if max_rank > 0:
        if not isinstance(row.get("avg_zero"), dict) or not isinstance(row.get("avg_max"), dict):
            return False
        if row.get("max_rank") != max_rank:
            return False
    return True


def expected_live_items(items_doc):
    items = items_doc.get("items") if isinstance(items_doc, dict) else None
    if not isinstance(items, dict) or not items:
        raise ValueError("price item catalog is missing or empty")
    live = {}
    for slug, item in items.items():
        if not isinstance(slug, str) or not slug or not isinstance(item, dict):
            raise ValueError("price item catalog has an invalid slug entry")
        if item.get("wm_deleted") is True:
            continue
        max_rank = item.get("maxRank", 0)
        if not valid_metric(max_rank):
            raise ValueError("price item catalog has invalid maxRank for " + slug)
        live[slug] = max_rank
    if not live:
        raise ValueError("price item catalog contains no live items")
    return live


def complete_candidates(live, latest):
    snapshots = DATA / "snapshots"
    latest_time = parse_time(latest.get("last_snapshot"))
    candidates = []
    for path in snapshots.glob("*.json"):
        snapshot = read_json(path)
        batches = snapshot.get("batches") if isinstance(snapshot, dict) else None
        if not isinstance(batches, list):
            continue
        for batch in batches:
            if not isinstance(batch, dict):
                continue
            captured_at = parse_time(batch.get("time"))
            if captured_at is None or (latest_time is not None and captured_at > latest_time):
                continue
            rows = batch.get("items")
            if not isinstance(rows, dict):
                continue
            if (batch.get("planned_items") != len(live) or
                    batch.get("captured_items") != len(live) or
                    batch.get("error_items") != 0 or len(rows) != len(live) or
                    set(rows) != set(live)):
                continue
            if not all(validate_row(slug, rows[slug], live[slug]) for slug in live):
                continue
            candidates.append((captured_at, path, batch, rows, snapshot))
    return sorted(candidates, key=lambda item: item[0])


def carry_recent_values(rows, candidates, live):
    """Retain the latest non-empty per-item metric for at most eight hours."""
    result = {slug: dict(row) for slug, row in rows.items()}
    latest_time = candidates[-1][0]
    stale_values = 0
    stale_items = set()
    unresolved = set()
    for slug, row in result.items():
        if row.get("avg") is None:
            unresolved.add((slug, "avg"))
        if live[slug] > 0:
            for key in ("avg_zero", "avg_max"):
                metric = row.get(key)
                if isinstance(metric, dict) and metric.get("avg") is None:
                    unresolved.add((slug, key))

    for captured_at, _path, _batch, previous_rows, _snapshot in reversed(candidates[:-1]):
        age = latest_time - captured_at
        if age.total_seconds() <= 0:
            continue
        if age.total_seconds() > STALE_FALLBACK_HOURS * 3600:
            break
        for slug, key in list(unresolved):
            previous = previous_rows.get(slug)
            value = previous.get(key) if previous else None
            if key != "avg":
                if not previous or previous.get("max_rank") != live[slug]:
                    continue
                value = value.get("avg") if isinstance(value, dict) else None
            if not valid_metric(value):
                continue
            current = result[slug]
            if key == "avg":
                current["avg"] = value
                current["stale"] = True
                current["stale_capture_at"] = captured_at.isoformat().replace("+00:00", "Z")
            else:
                metric = dict(current[key])
                metric["avg"] = value
                metric["stale"] = True
                metric["stale_capture_at"] = captured_at.isoformat().replace("+00:00", "Z")
                current[key] = metric
            unresolved.remove((slug, key))
            stale_values += 1
            stale_items.add(slug)

    return result, stale_values, len(stale_items)


def build(write=True):
    latest = read_json(DATA / "meta" / "latest.json")
    revision = str(latest.get("data_revision", ""))
    if not revision:
        raise ValueError("meta/latest.json is missing data_revision")
    live = expected_live_items(read_json(DATA / "meta" / "items.json"))
    candidates = complete_candidates(live, latest)
    if not candidates:
        print("No complete snapshot matches the current live item catalog; keeping the previous artifact.")
        return None

    captured_at, path, batch, rows, snapshot = candidates[-1]
    rows, stale_values, stale_items = carry_recent_values(rows, candidates, live)
    payload = {
        "schema_version": 1,
        "data_revision": revision,
        "generated": latest.get("generated"),
        "source": {
            "path": path.relative_to(DATA).as_posix(),
            "captured_at": batch["time"],
            "planned_items": batch["planned_items"],
            "captured_items": batch["captured_items"],
            "error_items": batch["error_items"],
            "snapshot_revision": snapshot.get("data_revision"),
            "stale_fallback_hours": STALE_FALLBACK_HOURS,
            "stale_metric_count": stale_values,
            "stale_item_count": stale_items,
        },
        "item_count": len(live),
        "items": {slug: rows[slug] for slug in sorted(live)},
    }
    encoded = (json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
    if write:
        OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT.write_bytes(encoded)
    print(f"Item average map OK: captured_at={batch['time']} items={len(live)} bytes={len(encoded)}")
    return payload


if __name__ == "__main__":
    build()
