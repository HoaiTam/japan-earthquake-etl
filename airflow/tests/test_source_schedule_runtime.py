"""ORC-02 deterministic intervals, metadata gates and whole-run lease; no live source."""
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
from jma_backfill_runtime import execute_archive
from source_run_guard import SourceRunBusy, lease
from source_schedule_runtime import ReadinessError, ingest_usgs, profile, ready_summary, refresh_jma, resolve_daily
from usgs_ingest_runtime import UsgsRunnerError, resolve_run_context

INVENTORY = Path(__file__).parents[2] / "config/jma/hypocenter_archives_v1.csv"


def ready(archive, run="daily-test", reused=True):
    release = "jma-lm-20251210T014153Z-sha256-" + "a" * 12
    prefix = f"bronze/jma/year={archive['year']}/catalog_release={release}/ingest_date_utc=2026-10-08/run_id=test/attempt=1/"
    return {"run_id": run, "year": archive["year"], "segment": archive["segment"], "attempt": 1,
            "status": "BronzeReady", "bronze_status": "BronzeReady", "verified": True,
            "sha256": "a" * 64, "manifest_sha256": "b" * 64, "catalog_release": release,
            "manifest_key": prefix + "manifest.json", "manifest_uri": "s3://bucket/" + prefix + "manifest.json",
            "raw_object_key": prefix + "archive.zip", "raw_object_uri": "s3://bucket/" + prefix + "archive.zip",
            "record_count_estimate": 0, "publication_reused": reused, "readiness_decision": "UNCHANGED",
            "ingest_invoked": False}


class SourceScheduleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(); self.addCleanup(self.temporary.cleanup)
        self.env = {"JMA_INVENTORY_PATH": str(INVENTORY), "JMA_STAGING_ROOT": self.temporary.name + "/jma",
                    "SOURCE_READINESS_ROOT": self.temporary.name + "/daily", "SOURCE_GUARD_ROOT": self.temporary.name + "/guard"}
        self.context = {"run_id": "daily-test", "data_interval_end": "2023-01-04T07:15:00+07:00"}

    def plan(self, **context):
        return resolve_daily({**self.context, **context}, self.env)

    def test_default_profile_explicit_pilot_and_single_schedule_owner(self):
        self.assertEqual([1997, 2000, 2023], profile(self.env)["years"])
        self.assertEqual("multi-source", profile(self.env)["mode"])
        self.assertEqual("usgs-only", profile({"SOURCE_SCHEDULE_PROFILE": "usgs-only"})["mode"])
        for changes in ({"SOURCE_SCHEDULE_PROFILE": "both"}, {"JMA_READINESS_YEARS": "[true]"},
                        {"JMA_READINESS_YEARS": "[]"}, {"JMA_READINESS_YEARS": "[2024]"},
                        {"JMA_CHECKSUM_AUDIT_WEEKDAY": "7"}, {"PIPELINE_SCHEDULE_CRON": "0 7 * * *"},
                        {"PIPELINE_SCHEDULE_CRON": "*/5 * * * *"}, {"PIPELINE_TIMEZONE": "UTC"}):
            with self.subTest(changes=changes), self.assertRaises((ReadinessError, ValueError)):
                profile(changes)

    def test_interval_end_not_logical_date_and_jst_scope(self):
        plan = self.plan(logical_date="2023-01-03T07:15:00+07:00")
        self.assertEqual("2023-01-03", plan["usgs"]["processing_date"])
        self.assertEqual("2023-01-01T00:00:00Z", plan["usgs"]["window_start_utc"])
        self.assertEqual("2023-01-04T00:00:00Z", plan["usgs"]["window_end_utc"])
        self.assertEqual(4, len(plan["jma"]["archives"]))
        self.assertEqual("1996-12-31T15:00:00Z", plan["jma"]["run_context"]["window_start_utc"])
        self.assertFalse(plan["checksum_audit"])
        self.assertEqual(plan, self.plan(logical_date="2023-01-03T07:15:00+07:00"))

    def test_same_instant_different_timezone_same_processing_day(self):
        first = self.plan()
        second = self.plan(data_interval_end="2023-01-04T09:15:00+09:00")
        self.assertEqual(first, second)

    def test_audit_weekday_uses_interval_end_not_host_now(self):
        self.assertTrue(self.plan(data_interval_end="2023-01-08T07:15:00+07:00")["checksum_audit"])

    def test_missing_naive_future_or_manual_conf_are_rejected(self):
        for changes in ({"data_interval_end": None}, {"data_interval_end": "2023-01-04T00:15:00"},
                        {"data_interval_end": "2099-01-04T00:15:00Z"}, {"run_id": ""},
                        {"dag_run": {"conf": {"years": [1984]}}}):
            with self.subTest(changes=changes), self.assertRaises(ReadinessError): self.plan(**changes)

    def test_overlap_zero_one_three_and_seed_guard(self):
        for overlap, expected in [(0, "2023-01-03"), (1, "2023-01-03"), (3, "2023-01-01")]:
            result = resolve_run_context(self.context, {"PIPELINE_OVERLAP_DAYS": str(overlap)})
            self.assertEqual(expected + "T00:00:00Z", result["window_start_utc"])
        for overlap in (-1, 32):
            with self.assertRaises(UsgsRunnerError): resolve_run_context(self.context, {"PIPELINE_OVERLAP_DAYS": str(overlap)})
        with self.assertRaises(UsgsRunnerError): resolve_run_context({"data_interval_end": "2023-01-01T00:15:00Z"}, {})

    def test_retry_cannot_change_scope_or_profile(self):
        self.plan()
        with self.assertRaises(ReadinessError): self.plan(data_interval_end="2023-01-05T00:15:00Z")
        with self.assertRaises(ReadinessError): resolve_daily(self.context, {**self.env, "PIPELINE_OVERLAP_DAYS": "2"})

    def test_no_change_only_probes_and_pins_exact_manifests(self):
        plan = self.plan()
        with patch("source_schedule_runtime.execute_archive", side_effect=lambda p, a, *args, **kw: ready(a)) as runner:
            result = refresh_jma(plan, environment=self.env)
        self.assertEqual(4, runner.call_count)
        self.assertTrue(all(call.kwargs["phase"] == "probe" for call in runner.call_args_list))
        self.assertTrue(all(not item["ingest_invoked"] for item in result["archives"]))

    def test_only_changed_segment_ingests_and_count_is_not_fabricated(self):
        plan = self.plan()
        def runner(p, a, *args, **kwargs):
            if a["segment"] == "oct-dec" and kwargs.get("phase") == "probe":
                return {"status": "NeedsIngest", "verified": False, "readiness_decision": "CHANGED_OR_UNINITIALIZED"}
            return ready(a, reused=a["segment"] != "oct-dec")
        with patch("source_schedule_runtime.execute_archive", side_effect=runner) as invoke:
            jma = refresh_jma(plan, environment=self.env)
        self.assertEqual(5, invoke.call_count)
        usgs = {"bronze_status": "BronzeReady", "verified": True, "manifest_uri": "s3://bucket/usgs/manifest.json", "manifest_sha256": "c" * 64}
        summary = ready_summary(plan, jma, usgs, self.env)
        self.assertEqual([1997], summary["jma_changed_years"])
        self.assertEqual(5, len(summary["input_manifests"]))
        self.assertFalse(summary["published"])

    def test_failed_probe_or_missing_1997_segment_blocks_summary(self):
        plan = self.plan()
        with patch("source_schedule_runtime.execute_archive", return_value={"status": "FAILED", "verified": False}):
            with self.assertRaises(ReadinessError): refresh_jma(plan, environment=self.env)
        with self.assertRaises(ReadinessError): ready_summary(plan, {"archives": []}, {}, self.env)

    def test_real_usgs_four_phases_and_dry_run_rejected(self):
        plan = self.plan()
        with self.assertRaises(ReadinessError): ingest_usgs(plan, {**self.env, "USGS_INGEST_DRY_RUN": "true"})
        receipt = {"run_id": plan["run_id"], "valid": True, "bronze_status": "BronzeReady", "verified": True,
                   "manifest_uri": "s3://bucket/usgs/manifest.json", "manifest_sha256": "c" * 64, "sha256": "d" * 64,
                   "record_count_estimate": 0}
        with patch("source_schedule_runtime.execute_phase", return_value=receipt) as runner:
            self.assertEqual(receipt, ingest_usgs(plan, self.env))
            self.assertEqual(["fetch", "validate", "upload", "verify"], [call.args[0] for call in runner.call_args_list])
        with patch("source_schedule_runtime.execute_phase", return_value={**receipt, "run_id": "other"}):
            with self.assertRaises(ReadinessError): ingest_usgs(plan, self.env)

    def test_whole_run_lease_retry_foreign_release_and_backfill_contention(self):
        daily = {"dag_id": "daily", "run_id": "a"}; backfill = {"dag_id": "backfill", "run_id": "b"}
        self.assertEqual("ACQUIRED", lease("acquire", daily, self.env)["status"])
        self.assertEqual("ACQUIRED", lease("acquire", daily, self.env)["status"])
        with self.assertRaises(SourceRunBusy): lease("acquire", backfill, self.env)
        self.assertEqual("NOT_OWNER", lease("release", backfill, self.env)["status"])
        with self.assertRaises(SourceRunBusy): lease("acquire", backfill, self.env)
        lease("release", daily, self.env)
        self.assertEqual("ACQUIRED", lease("acquire", backfill, self.env)["status"])
        lease("release", backfill, self.env)
        self.assertEqual("RELEASED", lease("release", backfill, self.env)["status"])

    def test_corrupt_lease_fails_closed(self):
        root = Path(self.env["SOURCE_GUARD_ROOT"]); root.mkdir()
        (root / "owner.json").write_text("invalid")
        with self.assertRaises(ValueError): lease("acquire", {"dag_id": "d", "run_id": "r"}, self.env)
        self.assertEqual("invalid", (root / "owner.json").read_text())

    def test_partial_clear_cannot_use_stale_acquisition_after_release(self):
        identity = {"dag_id": "daily", "run_id": "a"}
        with self.assertRaises(SourceRunBusy): lease("assert", identity, self.env)
        lease("acquire", identity, self.env)
        self.assertEqual("HELD", lease("assert", identity, self.env)["status"])
        lease("release", identity, self.env)
        with self.assertRaises(SourceRunBusy): lease("assert", identity, self.env)

    def test_probe_protocol_preserves_needs_ingest_not_false_bronze_ready(self):
        plan = self.plan()["jma"]; archive = plan["archives"][0]
        result = {"run_id": plan["run_context"]["run_id"], "year": archive["year"], "segment": archive["segment"],
                  "attempt": 1, "status": "NeedsIngest", "verified": False, "readiness_decision": "CHECKSUM_AUDIT"}
        import subprocess
        env = {**self.env, "JMA_INGEST_RUNNER_COMMAND": "java-runner"}
        with patch("jma_backfill_runtime.subprocess.run", return_value=subprocess.CompletedProcess([], 0, json.dumps(result), "")):
            self.assertEqual(result, execute_archive(plan, archive, environment=env, phase="probe"))
            self.assertEqual("FAILED", execute_archive(plan, archive, environment=env)["status"])


if __name__ == "__main__": unittest.main()
