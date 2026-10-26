#!/usr/bin/env python3
"""Verify a pushed price release and publish only its small EdgeOne KV pointer."""
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "AdminRoc/Ws-Web-price-data"
MANIFEST_PATH = "release-manifest.json"
ENDPOINT = "https://price.wfspeed.run"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def get(url, timeout=45):
    request = urllib.request.Request(url, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def current_pointer():
    url = ENDPOINT + "/api/kv?key=price_release_meta&v=" + str(time.time_ns())
    try:
        return json.loads(get(url, 15).decode("utf-8"))
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def main():
    token = os.environ.get("KV_WRITE_TOKEN", "")
    if not token:
        raise SystemExit("Missing KV_WRITE_TOKEN")
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    if len(commit) != 40:
        raise SystemExit("Invalid release commit SHA")
    remote = subprocess.check_output(["git", "ls-remote", "origin", "refs/heads/main"], cwd=ROOT, text=True).split()
    if not remote or remote[0].lower() != commit.lower():
        raise SystemExit("Candidate release is not the current remote main; refusing pointer publication")

    manifest_file = ROOT / "data" / MANIFEST_PATH
    manifest_raw = manifest_file.read_bytes()
    manifest = json.loads(manifest_raw.decode("utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("repository") != REPOSITORY:
        raise SystemExit("Price release manifest schema or repository mismatch")
    raw_base = f"https://raw.githubusercontent.com/{REPOSITORY}/{commit}/data/"
    remote_manifest = get(raw_base + MANIFEST_PATH)
    if sha(remote_manifest) != sha(manifest_raw):
        raise SystemExit("Pinned raw manifest differs from the pushed release")

    critical = set(manifest.get("files", {}))
    critical.update(ref["path"] for ref in manifest.get("indexes", {}).values())
    for relative in sorted(critical):
        entry = manifest.get("files", {}).get(relative)
        if entry is None:
            entry = next((ref for ref in manifest["indexes"].values() if ref["path"] == relative), None)
        if not entry or not isinstance(entry.get("bytes"), int) or not isinstance(entry.get("sha256"), str):
            raise SystemExit("Incomplete manifest entry: " + relative)
        raw = get(raw_base + urllib.parse.quote(relative, safe="/._-"))
        if len(raw) != entry["bytes"] or sha(raw) != entry["sha256"]:
            raise SystemExit("Pinned raw readback hash mismatch: " + relative)

    latest = json.loads((ROOT / "data/meta/latest.json").read_text(encoding="utf-8"))
    if str(latest.get("data_revision")) != str(manifest.get("data_revision")):
        raise SystemExit("Price metadata and release manifest revisions differ")
    pointer = {
        "schema_version": 1,
        "repository": REPOSITORY,
        "commit_sha": commit,
        "manifest_path": MANIFEST_PATH,
        "manifest_sha256": sha(manifest_raw),
        "manifest_bytes": len(manifest_raw),
        "data_revision": str(manifest["data_revision"]),
        "release_id": os.environ.get("GITHUB_RUN_ID", str(manifest["data_revision"])),
        "published_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    previous = current_pointer()
    if previous:
        if previous.get("repository") != REPOSITORY:
            raise SystemExit("Existing pointer belongs to a different repository")
        old_commit = previous.get("commit_sha", "")
        old_revision = str(previous.get("data_revision", ""))
        new_revision = str(pointer["data_revision"])
        if old_revision.isdigit() and new_revision.isdigit() and int(old_revision) > int(new_revision):
            raise SystemExit("Refusing to move the price release pointer to an older data revision")
        pointer["previous_commit_sha"] = old_commit or None
    else:
        pointer["previous_commit_sha"] = None

    value = json.dumps(pointer, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if len(value.encode("utf-8")) > 65536:
        raise SystemExit("Release pointer exceeds the 64 KiB bound")
    body = json.dumps({"key": "price_release_meta", "value": value}, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        ENDPOINT + "/api/update-kv",
        data=body,
        headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
    except Exception as error:
        print("Pointer write response was inconclusive; checking readback without replay: " + type(error).__name__, file=sys.stderr)
    actual = current_pointer()
    if actual != pointer:
        raise SystemExit("Price release pointer readback does not match the verified release")
    print(f"PRICE RELEASE POINTER OK commit={commit} data_revision={pointer['data_revision']} files_checked={len(critical)}")


if __name__ == "__main__":
    main()
