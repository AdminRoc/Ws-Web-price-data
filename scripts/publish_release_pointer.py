#!/usr/bin/env python3
"""Verify a pushed price release and publish only its small EdgeOne KV pointer."""
import base64
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import socket
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "AdminRoc/Ws-Web-price-data"
MANIFEST_PATH = "release-manifest.json"
ENDPOINT = "https://price.wfspeed.run"


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def get(url, timeout=45, attempts=4, delay=2):
    request = urllib.request.Request(url, headers={"Cache-Control": "no-store", "Pragma": "no-cache"})
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if error.code < 500 or attempt + 1 == attempts:
                raise
        except (urllib.error.URLError, TimeoutError, socket.timeout):
            if attempt + 1 == attempts:
                raise
        time.sleep(delay * (attempt + 1))
    raise RuntimeError("unreachable GET retry state")


def current_pointer():
    url = ENDPOINT + "/api/kv?key=price_release_meta&v=" + str(time.time_ns())
    try:
        return json.loads(get(url, 15).decode("utf-8"))
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return None
        raise


def verify_pointer_after_write(expected, attempts=5, delay=2):
    for attempt in range(attempts):
        try:
            actual = current_pointer()
        except urllib.error.HTTPError as error:
            if error.code < 500 or attempt + 1 == attempts:
                raise
        except (urllib.error.URLError, TimeoutError, socket.timeout):
            if attempt + 1 == attempts:
                raise
        else:
            if actual == expected:
                return
            if actual and actual.get("snapshot"):
                snapshot = dict(actual["snapshot"])
                snapshot.pop("switched_at", None)
                if dict(actual, snapshot=snapshot) == expected:
                    return
        if attempt + 1 < attempts:
            time.sleep(delay * (attempt + 1))
    raise SystemExit("Price release pointer readback mismatch after bounded verification")


CHUNK_BYTES = 512 * 1024
SLOT_CHUNKS = 64


def snapshot_url(commit, slot, start, count):
    return ENDPOINT + "/api/release-artifact?" + urllib.parse.urlencode(
        {"revision": commit, "slot": slot, "start": start, "count": count}
    )


def stage_snapshot(commit, previous, payloads, token):
    active = (previous or {}).get("snapshot") or {}
    slot = "b" if active.get("slot") == "a" else "a"
    files = {}
    chunks = []
    for path, raw in payloads.items():
        parts = [raw[offset:offset + CHUNK_BYTES] for offset in range(0, len(raw), CHUNK_BYTES)]
        if not 1 <= len(parts) <= 16:
            raise SystemExit("price snapshot file exceeds its supported size: " + path)
        files[path] = {"start": len(chunks), "count": len(parts), "sha256": sha(raw)}
        chunks.extend(parts)
    if len(chunks) > SLOT_CHUNKS:
        raise SystemExit("price snapshot exceeds the fixed two-slot KV budget")
    def stage_chunk(index, chunk):
        url = snapshot_url(commit, slot, index, 1)
        try:
            if get(url + "&v=" + str(time.time_ns()), attempts=4) == chunk:
                return
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
        request = urllib.request.Request(url, data=base64.b64encode(chunk), method="POST",
            headers={"Authorization": "Bearer " + token, "Content-Type": "text/plain"})
        for attempt in range(3):
            try:
                with urllib.request.urlopen(request, timeout=30) as response:
                    response.read()
                break
            except (urllib.error.URLError, TimeoutError) as error:
                code = getattr(error, "code", None)
                if code in (400, 401, 403, 409, 413):
                    raise
                print(f"Chunk {index} write response inconclusive ({code}); verifying before any retry", file=sys.stderr)
                time.sleep(5)
                try:
                    if get(url + "&v=" + str(time.time_ns()), attempts=4) == chunk:
                        break
                except urllib.error.HTTPError as read_error:
                    if read_error.code != 404:
                        raise
                if attempt == 2 or current_pointer() != previous:
                    raise SystemExit("KV chunk readback mismatch") from error
    for index, chunk in enumerate(chunks):
        stage_chunk(index, chunk)
    for path, raw in payloads.items():
        file = files[path]
        url = snapshot_url(commit, slot, file["start"], file["count"])
        for attempt in range(7):
            try:
                actual = get(url + "&v=" + str(time.time_ns()), attempts=1)
                if len(actual) == len(raw) and sha(actual) == file["sha256"]:
                    break
            except (urllib.error.URLError, TimeoutError):
                pass
            if attempt == 6:
                raise SystemExit("KV snapshot readback mismatch: " + path)
            time.sleep(11)
            if attempt in (2, 4):
                if current_pointer() != previous:
                    raise SystemExit("Active price pointer changed during repair")
                for index in range(file["start"], file["start"] + file["count"]):
                    stage_chunk(index, chunks[index])
    return {"slot": slot, "files": files}


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

    payloads = {MANIFEST_PATH: manifest_raw}
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
        payloads[relative] = raw

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

    previous = previous
    if previous and previous.get("snapshot") and previous.get("manifest_sha256") == pointer["manifest_sha256"]:
        print("Verified snapshot manifest is already current")
        return
    if previous and previous.get("snapshot"):
        elapsed = time.time() - previous["snapshot"]["switched_at"] / 1000
        if elapsed < 180:
            time.sleep(min(185, 182 - elapsed))
    if current_pointer() != previous:
        raise SystemExit("Active pointer changed before staging")
    pointer["snapshot"] = stage_snapshot(commit, previous, payloads, token)
    if current_pointer() != previous:
        raise SystemExit("Active pointer changed during staging")
    remote = subprocess.check_output(["git", "ls-remote", "origin", "refs/heads/main"], cwd=ROOT, text=True).split()
    if not remote or remote[0].lower() != commit.lower():
        raise SystemExit("Candidate is no longer the remote main")
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
    verify_pointer_after_write(pointer)
    print(f"PRICE RELEASE POINTER OK commit={commit} data_revision={pointer['data_revision']} files_checked={len(critical)}")


if __name__ == "__main__":
    main()
