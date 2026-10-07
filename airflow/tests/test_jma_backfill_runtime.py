"""Offline JMA year planning, process boundary, failure summary and handoff tests."""
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
from jma_backfill_runtime import (
    JmaRunnerError, execute_archive, require_complete, resolve_plan, write_run_summary,
)

REPOSITORY = Path(__file__).parents[2]


class JmaBackfillRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.env = {"JMA_STAGING_ROOT": self.temporary.name,
                    "JMA_INVENTORY_PATH": str(REPOSITORY / "config/jma/hypocenter_archives_v1.csv"),
                    "JMA_INGEST_RUNNER_COMMAND": 'java -cp "path with spaces/runner.jar" Runner'}

    def context(self, conf, run_id="manual__jma-test"):
        return {"run_id": run_id, "logical_date": datetime(2026, 10, 7, tzinfo=timezone.utc),
                "dag_run": {"conf": conf}}

    def plan(self, years=(2023,), preview=False, run_id="manual__jma-test"):
        return resolve_plan(self.context({"years": list(years), "preview": preview}, run_id), self.env)

    def result(self, plan, archive, **extra):
        base = f"bronze/jma/year={archive['year']}/catalog_release=release-test/run_id=fixture/attempt=01/"
        return {"run_id": plan["run_context"]["run_id"], "year": archive["year"], "segment": archive["segment"],
                "attempt": 1, "status": "BronzeReady", "bronze_status": "BronzeReady", "verified": True,
                "sha256": "a" * 64, "record_count_estimate": 0, "catalog_release": "release-test",
                "manifest_key": base + "manifest.json", "raw_object_key": base + "archive.zip",
                "manifest_uri": "s3://fixture/" + base + "manifest.json", "raw_object_uri": "s3://fixture/" + base + "archive.zip",
                **extra}

    def execute(self, plan, archive, result):
        completed = subprocess.CompletedProcess([], 0, json.dumps(result) + "\n", "")
        with patch("jma_backfill_runtime.subprocess.run", return_value=completed) as mock:
            public = execute_archive(plan, archive, environment=self.env)
        return public, mock

    def test_year_list_is_deduplicated_sorted_and_1997_keeps_two_segments(self):
        plan = self.plan([2023, 1997, 2023], preview=True)
        self.assertEqual([1997, 2023], plan["years"])
        self.assertEqual(["h199701.zip", "h199710.zip", "h2023.zip"], [item["archive_name"] for item in plan["archives"]])
        self.assertEqual("1996-12-31T15:00:00Z", plan["run_context"]["window_start_utc"])
        self.assertEqual("2023-12-31T15:00:00Z", plan["run_context"]["window_end_utc"])
        self.assertEqual(64, len(plan["run_context"]["inventory_sha256"]))

    def test_inclusive_range_and_preview_default(self):
        plan = resolve_plan(self.context({"start_year": 1996, "end_year": 1998}), self.env)
        self.assertEqual([1996, 1997, 1998], plan["years"])
        self.assertEqual(4, len(plan["archives"]))
        self.assertTrue(plan["preview"])
        self.assertFalse(plan["force_download"])

    def test_invalid_or_implicit_scope_fails_before_any_process(self):
        cases = [{}, {"years": []}, {"years": "2023"}, {"years": [True]}, {"years": [1983]},
                 {"years": [2024]}, {"years": [2023.0]}, {"years": ["2023"]},
                 {"start_year": 2023}, {"start_year": 2023, "end_year": 2022},
                 {"years": [2023], "start_year": 2023, "end_year": 2023},
                 {"years": [2023], "preview": "false"}, {"years": [2023], "force_download": "true"}]
        with patch("jma_backfill_runtime.subprocess.run") as mock:
            for conf in cases:
                with self.subTest(conf=conf), self.assertRaises(JmaRunnerError):
                    resolve_plan(self.context(conf), self.env)
            mock.assert_not_called()

    def test_missing_or_duplicate_inventory_segment_is_rejected(self):
        inventory = Path(self.env["JMA_INVENTORY_PATH"]).read_text()
        path = Path(self.temporary.name) / "inventory.csv"
        selected = [line for line in inventory.splitlines() if ",1997,jan-sep," in line]
        for rows in ([inventory.splitlines()[0], *selected], [inventory.splitlines()[0], *selected, *selected]):
            path.write_text("\n".join(rows) + "\n")
            with self.assertRaises(JmaRunnerError):
                resolve_plan(self.context({"years": [1997]}), {**self.env, "JMA_INVENTORY_PATH": str(path)})

    def test_manual_airflow3_without_logical_date_uses_stable_run_start(self):
        context = self.context({"years": [2023]})
        context.pop("logical_date")
        context["dag_run"] = SimpleNamespace(conf={"years": [2023]}, start_date=datetime(2026, 10, 7, tzinfo=timezone.utc))
        self.assertEqual("2026-10-07", resolve_plan(context, self.env)["run_context"]["processing_date"])

    def test_same_run_cannot_change_scope_and_sanitized_ids_do_not_collide(self):
        first = self.plan([2023], run_id="a/b")
        with self.assertRaises(JmaRunnerError):
            self.plan([2000], run_id="a/b")
        second = self.plan([2023], run_id="a-b")
        self.assertNotEqual(first["run_context"]["run_id_path"], second["run_context"]["run_id_path"])
        self.assertNotIn("/", first["run_context"]["run_id_path"])

    def test_preview_summary_is_not_ready_and_never_calls_runner(self):
        plan = self.plan([1997], preview=True)
        with patch("jma_backfill_runtime.subprocess.run") as mock:
            with self.assertRaises(JmaRunnerError):
                execute_archive(plan, plan["archives"][0], environment=self.env)
            summary = require_complete(write_run_summary(plan, self.env))
            self.assertEqual("PREVIEW", summary["status"])
            self.assertFalse(summary["verified"])
            self.assertEqual(0, summary["ready_archives"])
            mock.assert_not_called()

    def test_process_protocol_and_whitelist_keep_bytes_secrets_out_of_summary(self):
        plan = self.plan()
        archive = plan["archives"][0]
        public, mock = self.execute(plan, archive, self.result(plan, archive, payload="raw", access_key="secret"))
        self.assertNotIn("payload", public)
        self.assertNotIn("access_key", public)
        argv = mock.call_args.args[0]
        self.assertEqual(["java", "-cp", "path with spaces/runner.jar", "Runner", "--context-file"], argv[:-1])
        request = json.loads(Path(argv[-1]).read_text())
        self.assertEqual(archive, request["archive"])
        self.assertEqual(1, request["attempt"])
        self.assertEqual(plan["run_context"], request["run_context"])
        self.assertFalse(mock.call_args.kwargs.get("shell", False))
        self.assertEqual("BronzeReady", require_complete(write_run_summary(plan, self.env))["status"])

    def test_wrong_identity_or_incomplete_verification_cannot_open_gate(self):
        plan = self.plan()
        archive = plan["archives"][0]
        for override in ({"run_id": "other"}, {"year": 2000}, {"segment": "wrong"}, {"attempt": 2},
                         {"verified": False}, {"verified": "true"}, {"sha256": "bad"},
                         {"record_count_estimate": None}, {"record_count_estimate": True}, {"record_count_estimate": -1},
                         {"manifest_uri": None}, {"status": "DOWNLOADED"}, {"bronze_status": "Rejected"}):
            with self.subTest(override=override):
                result, _ = self.execute(plan, archive, self.result(plan, archive, **override))
                self.assertEqual("FAILED", result["status"])
                self.assertFalse(result["verified"])
                with self.assertRaises(JmaRunnerError):
                    require_complete(write_run_summary(plan, self.env))

    def test_dry_run_uri_or_wrong_release_key_cannot_be_bronze_ready(self):
        plan = self.plan()
        archive = plan["archives"][0]
        for override in ({"manifest_uri": "dry-run://jma/manifest.json"}, {"raw_object_key": "bronze/other/archive.zip"},
                         {"catalog_release": "../release"}, {"manifest_uri": "s3://fixture/key?token=secret"}):
            with self.subTest(override=override):
                result, _ = self.execute(plan, archive, self.result(plan, archive, **override))
                self.assertEqual("FAILED", result["status"])
                self.assertNotIn("secret", json.dumps(result))
                with self.assertRaises(JmaRunnerError): require_complete(write_run_summary(plan, self.env))

    def test_process_failure_timeout_and_invalid_output_preserve_safe_failure_evidence(self):
        plan = self.plan()
        archive = plan["archives"][0]
        with patch("jma_backfill_runtime.subprocess.run", side_effect=subprocess.TimeoutExpired("secret-command", 1)):
            result = execute_archive(plan, archive, environment=self.env)
        self.assertEqual("RUNNER_FAILED", result["reason"])
        for completed in (subprocess.CompletedProcess([], 2, "", "Authorization: secret"),
                          subprocess.CompletedProcess([], 0, "not JSON", ""),
                          subprocess.CompletedProcess([], 0, "[]", "")):
            with patch("jma_backfill_runtime.subprocess.run", return_value=completed):
                self.assertEqual("FAILED", execute_archive(plan, archive, environment=self.env)["status"])
        summary = write_run_summary(plan, self.env)
        evidence = Path(summary["summary_uri"].removeprefix("file://")).read_text()
        self.assertNotIn("secret", evidence)

    def test_missing_result_and_one_segment_failure_leave_year_partial(self):
        plan = self.plan([1997, 2023])
        first, second, third = plan["archives"]
        self.execute(plan, first, self.result(plan, first))
        self.execute(plan, third, self.result(plan, third))
        summary = write_run_summary(plan, self.env)
        report = json.loads(Path(summary["summary_uri"].removeprefix("file://")).read_text())
        self.assertEqual(["PARTIAL", "BronzeReady"], [item["status"] for item in report["years"]])
        self.assertEqual(2, report["ready_archives"])
        with self.assertRaises(JmaRunnerError): require_complete(summary)
        self.execute(plan, second, self.result(plan, second))
        self.assertEqual("BronzeReady", require_complete(write_run_summary(plan, self.env))["status"])

    def test_runner_absent_fails_and_invalid_attempt_or_unplanned_archive_never_executes(self):
        plan = self.plan()
        archive = plan["archives"][0]
        result = execute_archive(plan, archive, environment={**self.env, "JMA_INGEST_RUNNER_COMMAND": ""})
        self.assertEqual("FAILED", result["status"])
        for attempt in (0, -1, True):
            with self.assertRaises(JmaRunnerError): execute_archive(plan, archive, attempt, self.env)
        with self.assertRaises(JmaRunnerError):
            execute_archive(plan, {**archive, "year": 2000}, environment=self.env)

    def test_new_attempt_invalidates_previous_success_even_if_process_is_killed(self):
        plan = self.plan()
        archive = plan["archives"][0]
        self.execute(plan, archive, self.result(plan, archive))
        with patch("jma_backfill_runtime.subprocess.run", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                execute_archive(plan, archive, 2, self.env)
        summary = write_run_summary(plan, self.env)
        self.assertEqual("FAILED", summary["status"])
        with self.assertRaises(JmaRunnerError): require_complete(summary)


if __name__ == "__main__": unittest.main()
