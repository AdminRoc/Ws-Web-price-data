import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from snapshot_schedule_guard import (
    latest_fully_published_run,
    select_latest_fully_published_run,
    should_run_capture,
)


NOW = datetime(2026, 9, 30, 10, 0, tzinfo=timezone.utc)
WINDOW = timedelta(minutes=90)


def successful_run(completed_at):
    return {"conclusion": "success", "updated_at": completed_at}


class SnapshotScheduleGuardTests(unittest.TestCase):
    def test_manual_dispatch_always_bypasses_freshness_gate(self):
        run, _ = should_run_capture("workflow_dispatch", None, NOW, WINDOW)
        self.assertTrue(run)

    def test_flagged_cloudflare_dispatch_uses_freshness_gate(self):
        previous = successful_run("2026-09-30T09:30:00Z")
        run, _ = should_run_capture("workflow_dispatch", previous, NOW, WINDOW, "true")
        self.assertFalse(run)

    def test_no_completed_success_allows_capture(self):
        run, _ = should_run_capture("schedule", None, NOW, WINDOW)
        self.assertTrue(run)

    def test_recent_success_skips_scheduled_capture(self):
        previous = successful_run("2026-09-30T09:30:00Z")
        run, _ = should_run_capture("schedule", previous, NOW, WINDOW)
        self.assertFalse(run)

    def test_age_at_threshold_allows_capture(self):
        previous = successful_run("2026-09-30T08:30:00Z")
        run, _ = should_run_capture("schedule", previous, NOW, WINDOW)
        self.assertTrue(run)

    def test_failed_previous_run_allows_retry(self):
        previous = {"conclusion": "failure", "updated_at": "2026-09-30T09:59:00Z"}
        run, _ = should_run_capture("schedule", previous, NOW, WINDOW)
        self.assertTrue(run)

    def test_invalid_or_future_timestamp_does_not_suppress_capture(self):
        invalid = successful_run("not-a-timestamp")
        future = successful_run("2026-09-30T10:10:00Z")
        self.assertTrue(should_run_capture("schedule", invalid, NOW, WINDOW)[0])
        self.assertTrue(should_run_capture("schedule", future, NOW, WINDOW)[0])

    def test_intentional_freshness_skip_does_not_reset_publication_age(self):
        skipped = {"id": 2, "conclusion": "success", "updated_at": "2026-09-30T09:55:00Z"}
        published = {"id": 1, "conclusion": "success", "updated_at": "2026-09-30T08:20:00Z"}
        jobs = {
            2: [
                {"name": "schedule_guard", "conclusion": "success"},
                {"name": "fetch", "conclusion": "skipped", "steps": []},
            ],
            1: [
                {
                    "name": "fetch",
                    "conclusion": "success",
                    "steps": [
                        {"name": name, "conclusion": "success"}
                        for name in (
                            "抓取全量快照",
                            "提交并推送",
                            "写入 wfspeed-price EdgeOne KV",
                        )
                    ],
                }
            ],
        }
        result = select_latest_fully_published_run(
            [skipped, published], lambda run: jobs[run["id"]]
        )
        self.assertEqual(result, published)

    def test_latest_failed_run_does_not_fall_back_to_an_older_success(self):
        failed = {"id": 2, "conclusion": "failure"}
        published = {"id": 1, "conclusion": "success"}
        jobs = {
            2: [{"name": "fetch", "conclusion": "failure", "steps": []}],
            1: [{"name": "fetch", "conclusion": "success", "steps": []}],
        }
        result = select_latest_fully_published_run(
            [failed, published], lambda run: jobs[run["id"]]
        )
        self.assertIsNone(result)

    def test_successful_publish_existing_run_counts_as_a_publication(self):
        run = {"id": 3, "conclusion": "success", "updated_at": "2026-09-30T09:40:00Z"}
        jobs = {
            3: [
                {
                    "name": "fetch",
                    "conclusion": "success",
                    "steps": [
                        {"name": "仅发布已提交数据", "conclusion": "success"},
                        {"name": "写入 wfspeed-price EdgeOne KV", "conclusion": "success"},
                    ],
                }
            ]
        }
        result = select_latest_fully_published_run([run], lambda item: jobs[item["id"]])
        self.assertEqual(result, run)

    @patch("snapshot_schedule_guard.api_json")
    def test_jobs_are_read_from_the_run_jobs_endpoint(self, api_json):
        run = {"id": 321, "conclusion": "success", "updated_at": "2026-09-30T08:20:00Z"}
        steps = [
            {"name": name, "conclusion": "success"}
            for name in ("抓取全量快照", "提交并推送", "写入 wfspeed-price EdgeOne KV")
        ]
        api_json.side_effect = [
            {"workflow_runs": [run]},
            {"jobs": [{"name": "fetch", "conclusion": "success", "steps": steps}]},
        ]
        result = latest_fully_published_run("AdminRoc/Ws-Web-price-data", "https://api.github.com", "dummy")
        self.assertEqual(result, run)
        self.assertEqual(
            api_json.call_args_list[1].args[0],
            "https://api.github.com/repos/AdminRoc/Ws-Web-price-data/actions/runs/321/jobs?per_page=100",
        )


if __name__ == "__main__":
    unittest.main()
