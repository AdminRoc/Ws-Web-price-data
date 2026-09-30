"""Gate scheduled price captures on the last fully published run."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


WORKFLOW_FILE = "fetch-snapshots.yml"
REQUIRED_STEPS = {
    "抓取全量快照",
    "提交并推送",
    "写入 wfspeed-price EdgeOne KV",
}


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp has no timezone")
    return parsed.astimezone(timezone.utc)


def should_run_capture(
    event_name: str,
    latest_run: dict | None,
    now: datetime,
    min_age: timedelta,
) -> tuple[bool, str]:
    if event_name != "schedule":
        return True, f"{event_name} run: freshness gate does not suppress manual dispatches"
    if latest_run is None:
        return True, "no completed workflow run found; allow scheduled capture"
    if latest_run.get("conclusion") != "success":
        return True, "latest completed workflow run was not successful; allow retry"

    completed_at = latest_run.get("updated_at") or latest_run.get("completed_at")
    if not completed_at:
        return True, "latest successful run has no completion timestamp; allow capture"
    try:
        age = now.astimezone(timezone.utc) - parse_utc(completed_at)
    except (TypeError, ValueError):
        return True, "latest successful run timestamp is invalid; allow capture"

    if age < -timedelta(minutes=5):
        return True, "latest completion timestamp is unexpectedly in the future; allow capture"
    if age < min_age:
        return False, f"last fully published run is only {int(age.total_seconds() // 60)} minutes old"
    return True, f"last fully published run is at least {int(min_age.total_seconds() // 60)} minutes old"


def api_json(url: str, token: str) -> dict:
    request = Request(
        url,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "Ws-Web-price-data-schedule-guard",
        },
    )
    with urlopen(request, timeout=20) as response:
        return json.loads(response.read().decode("utf-8"))


def select_latest_fully_published_run(runs: list[dict], jobs_for_run) -> dict | None:
    for run in runs:
        jobs = jobs_for_run(run)
        producer = next((job for job in jobs if job.get("name") == "fetch"), None)
        if producer and producer.get("conclusion") == "skipped":
            guard = next((job for job in jobs if job.get("name") == "schedule-guard"), None)
            if run.get("conclusion") == "success" and guard and guard.get("conclusion") == "success":
                # This was an intentional freshness skip, not a new data publication.
                continue
        if run.get("conclusion") != "success" or not producer or producer.get("conclusion") != "success":
            return None
        successful_steps = {
            step.get("name")
            for step in producer.get("steps", [])
            if step.get("conclusion") == "success"
        }
        if REQUIRED_STEPS.issubset(successful_steps):
            return run
        return None
    return None


def latest_fully_published_run(repository: str, api_url: str, token: str) -> dict | None:
    base = f"{api_url.rstrip('/')}/repos/{repository}/actions/workflows/{WORKFLOW_FILE}"
    runs = api_json(f"{base}/runs?status=completed&per_page=10", token).get("workflow_runs", [])
    def jobs_for_run(run: dict) -> list[dict]:
        runs_base = f"{api_url.rstrip('/')}/repos/{repository}/actions/runs"
        return api_json(f"{runs_base}/{run['id']}/jobs?per_page=100", token).get("jobs", [])

    return select_latest_fully_published_run(runs, jobs_for_run)


def main() -> int:
    event_name = os.environ.get("EVENT_NAME", "")
    if event_name != "schedule":
        print(f"run_capture=true ({event_name or 'unknown'} run bypasses schedule gate)")
        return write_output(True)

    token = os.environ.get("GH_TOKEN", "")
    repository = os.environ.get("GITHUB_REPOSITORY", "")
    api_url = os.environ.get("GITHUB_API_URL", "https://api.github.com")
    if not token or not repository:
        print("::error::Schedule freshness gate is missing GitHub API configuration")
        return 1

    try:
        latest = latest_fully_published_run(repository, api_url, token)
    except (HTTPError, URLError, TimeoutError, OSError, ValueError, KeyError) as error:
        # Fail before any upstream request or data write; the next schedule/manual run can retry.
        print(f"::error::Could not verify the last published snapshot ({type(error).__name__})")
        return 1

    try:
        min_age = timedelta(minutes=int(os.environ.get("MIN_AGE_MINUTES", "90")))
    except ValueError:
        print("::error::MIN_AGE_MINUTES must be an integer")
        return 1
    run_capture, reason = should_run_capture(
        event_name, latest, datetime.now(timezone.utc), min_age
    )
    print(f"run_capture={str(run_capture).lower()} ({reason})")
    if not run_capture:
        summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary_path:
            with open(summary_path, "a", encoding="utf-8") as summary:
                summary.write(f"### Scheduled Price-data run skipped\n\n{reason}.\n")
    return write_output(run_capture)


def write_output(run_capture: bool) -> int:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        print("::error::GITHUB_OUTPUT is not set")
        return 1
    with open(output_path, "a", encoding="utf-8") as output:
        output.write(f"run_capture={str(run_capture).lower()}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
