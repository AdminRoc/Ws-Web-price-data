#!/usr/bin/env python3
"""Build bounded root and lazy per-directory indexes for a pinned raw release."""
import argparse
import hashlib
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def canonical_bytes(path):
    raw = path.read_bytes()
    json.loads(raw.decode("utf-8"))
    return raw


def entry(path):
    raw = canonical_bytes(path)
    return {"sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def build(write=True):
    latest_path = DATA / "meta/latest.json"
    latest = json.loads(canonical_bytes(latest_path).decode("utf-8"))
    revision = str(latest.get("data_revision", ""))
    if not revision:
        raise ValueError("meta/latest.json is missing data_revision")

    groups = {
        "daily": sorted((DATA / "daily").glob("*.json")),
        "series": sorted((DATA / "series").glob("*.json")),
        "snapshots": sorted((DATA / "snapshots").glob("*.json")),
    }
    if not groups["daily"] or not groups["series"] or not groups["snapshots"]:
        raise ValueError("daily, series, and snapshots must all contain JSON files")

    output = DATA / "release"
    outputs = {}
    indexes = {}
    for group, paths in groups.items():
        files = {path.relative_to(DATA).as_posix(): entry(path) for path in paths}
        index = {"schema_version": 1, "data_revision": revision, "files": files}
        index_path = output / f"{group}-index.json"
        index_bytes = (json.dumps(index, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        outputs[index_path] = index_bytes
        indexes[group] = {
            "path": index_path.relative_to(DATA).as_posix(),
            "sha256": hashlib.sha256(index_bytes).hexdigest(),
            "bytes": len(index_bytes),
            "file_count": len(files),
        }

    fixed = {}
    for path in sorted((DATA / "meta").glob("*.json")):
        fixed[path.relative_to(DATA).as_posix()] = entry(path)
    for relative in ("table/latest.json", f"snapshots/{latest['today']}.json"):
        path = DATA / relative
        fixed[relative] = entry(path)

    manifest = {
        "schema_version": 1,
        "repository": "AdminRoc/Ws-Web-price-data",
        "data_revision": revision,
        "generated": latest.get("generated"),
        "today": latest.get("today"),
        "retention": {key: len(paths) for key, paths in groups.items()},
        "files": fixed,
        "indexes": indexes,
    }
    path = DATA / "release-manifest.json"
    payload = (json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    outputs[path] = payload
    if write:
        output.mkdir(parents=True, exist_ok=True)
        for target, content in outputs.items():
            target.write_bytes(content)
    return manifest, outputs


def verify():
    expected, outputs = build(write=False)
    for target, content in outputs.items():
        if not target.is_file() or target.read_bytes() != content:
            raise ValueError(f"release index or manifest mismatch: {target.relative_to(ROOT)}")
    actual = expected
    print("PRICE RELEASE MANIFEST OK")
    print(f"data_revision={actual['data_revision']} daily={actual['retention']['daily']} series={actual['retention']['series']} snapshots={actual['retention']['snapshots']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.verify:
        verify()
    else:
        manifest, _ = build()
        print(f"Built release manifest for data_revision={manifest['data_revision']}")


if __name__ == "__main__":
    main()
