"""Unit tests for deterministic USG-04 interval and runner behavior."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))

from usgs_ingest_runtime import (
    UsgsRunnerError,
    execute_phase,
    require_bronze_ready,
    resolve_run_context,
    write_run_summary,
)


class UsgsIngestRuntimeTest(unittest.TestCase):
    def test_daily_context_uses_three_day_overlap_and_target_day(self) -> None:
        context = {
            "dag_id": "usg_04_usgs_ingest",
            "run_id": "scheduled__2023-09-16T00:15:00Z",
            "data_interval_end": datetime(2023, 9, 16, 0, 15, tzinfo=timezone.utc),
        }
        result = resolve_run_context(context)
        self.assertEqual("2023-09-13T00:00:00Z", result["window_start_utc"])
        self.assertEqual("2023-09-16T00:00:00Z", result["window_end_utc"])
        self.assertEqual("2023-09-15", result["processing_date"])
        self.assertEqual(3, result["revision_overlap_days"])

    def test_seed_clips_overlap_without_changing_target_window(self) -> None:
        context = {
            "run_id": "manual__2023-01-02T00:15:00Z",
            "data_interval_end": datetime(2023, 1, 2, 0, 15, tzinfo=timezone.utc),
        }
        result = resolve_run_context(context)
        self.assertEqual("2023-01-01T00:00:00Z", result["window_start_utc"])
        self.assertEqual("2023-01-02T00:00:00Z", result["window_end_utc"])
        self.assertEqual("2023-01-01", result["processing_date"])

    def test_explicit_backfill_window_is_preserved_for_live_smoke(self) -> None:
        context = {
            "run_id": "usg06-live-smoke",
            "dag_run": {
                "conf": {
                    "is_backfill": True,
                    "window_start_utc": "2023-01-01T00:00:00Z",
                    "window_end_utc": "2023-01-04T00:00:00Z",
                }
            },
        }

        result = resolve_run_context(context)

        self.assertEqual("2023-01-01T00:00:00Z", result["window_start_utc"])
        self.assertEqual("2023-01-04T00:00:00Z", result["window_end_utc"])
        self.assertEqual("2023-01-01T00:00:00Z", result["target_window_start_utc"])
        self.assertTrue(result["is_backfill"])
        self.assertTrue(result["explicit_window"])

    def test_explicit_window_requires_backfill_and_both_utc_bounds(self) -> None:
        with self.assertRaises(UsgsRunnerError):
            resolve_run_context(
                {
                    "dag_run": {
                        "conf": {
                            "window_start_utc": "2023-01-01T00:00:00Z",
                            "window_end_utc": "2023-01-04T00:00:00Z",
                        }
                    }
                }
            )

        with self.assertRaises(UsgsRunnerError):
            resolve_run_context(
                {
                    "dag_run": {
                        "conf": {
                            "is_backfill": True,
                            "window_start_utc": "2023-01-01T00:00:00+00:00",
                            "window_end_utc": "2023-01-04T00:00:00Z",
                        }
                    }
                }
            )

    def test_dry_run_is_safe_and_summary_is_written_outside_xcom(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            environment = {
                "USGS_INGEST_DRY_RUN": "true",
                "USGS_STAGING_ROOT": directory,
            }
            context = resolve_run_context(
                {
                    "run_id": "manual-test",
                    "data_interval_end": datetime(2023, 9, 16, tzinfo=timezone.utc),
                },
                environment,
            )
            fetch = execute_phase("fetch", context, environment=environment)
            verify = execute_phase("verify", context, fetch, environment)
            self.assertEqual("ok", fetch["status"])
            self.assertTrue(require_bronze_ready(verify)["verified"])
            summary_uri = write_run_summary(
                context, {"fetch": fetch, "verify": verify}, environment
            )
            summary = json.loads(Path(summary_uri).read_text(encoding="utf-8"))
            self.assertEqual("BronzeReady", summary["status"])
            self.assertEqual("manual-test", summary["run_id"])

    def test_runner_is_required_when_not_in_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            context = resolve_run_context(
                {"run_id": "manual-test", "logical_date": "2023-09-16T00:15:00Z"},
                {"USGS_STAGING_ROOT": directory},
            )
            with self.assertRaises(UsgsRunnerError):
                execute_phase(
                    "fetch", context, environment={"USGS_STAGING_ROOT": directory}
                )

    def test_non_ready_verification_blocks_publish_gate(self) -> None:
        with self.assertRaises(UsgsRunnerError):
            require_bronze_ready(
                {"phase": "verify", "status": "ok", "bronze_status": "Rejected", "verified": False}
            )


if __name__ == "__main__":
    unittest.main()
