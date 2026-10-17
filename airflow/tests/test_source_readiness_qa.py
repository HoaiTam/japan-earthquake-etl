"""Offline harness flow/cleanup tests; these are not scheduler/source evidence."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import ModuleType
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch


class ReadinessQaTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.env = {"SOURCE_READINESS_ROOT": self.temp.name + "/qa-root",
                    "SOURCE_GUARD_ROOT": self.temp.name + "/guard", "PIPELINE_OVERLAP_DAYS": "3"}
        dagrun = ModuleType("airflow.models.dagrun"); dagrun.DagRun = object
        session = ModuleType("airflow.utils.session"); session.create_session = object
        taskinstance = ModuleType("airflow.models.taskinstance"); taskinstance.TaskInstance = object
        trigger = ModuleType("airflow.api.common.trigger_dag"); trigger.trigger_dag = lambda **kwargs: object()
        enums = ModuleType("airflow.utils.types"); enums.DagRunTriggeredByType = SimpleNamespace(TEST="test")
        path = Path(__file__).parents[2] / "compose/airflow/source-readiness-qa.py"
        spec = importlib.util.spec_from_file_location("offline_source_qa", path)
        self.qa = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {"airflow": ModuleType("airflow"), "airflow.models.dagrun": dagrun,
                                      "airflow.models.taskinstance": taskinstance,
                                      "airflow.api.common.trigger_dag": trigger, "airflow.utils.types": enums,
                                      "airflow.utils.session": session}): spec.loader.exec_module(self.qa)
        self.calls = []; self.triggers = []; self.scheduled = False

    def cli(self, *args):
        self.calls.append(args)
        if args[1] == "list-import-errors": return "[]"
        if args[1] == "list": return json.dumps([{"dag_id": name, "is_paused": True} for name in (self.qa.DAG, self.qa.BACKFILL)])
        if args[1] == "unpause" and args[2] == self.qa.DAG: self.scheduled = True
        return "ok"

    def report(self, run_id):
        return {"run_id": run_id, "status": "SourcesReady", "verified": True, "published": False,
                "input_manifests": [{"manifest_uri": "s3://bucket/manifest.json", "sha256": "a" * 64}] * 5,
                "usgs_context": {"window_start_utc": "2023-01-01T00:00:00Z", "window_end_utc": "2023-01-04T00:00:00Z",
                                 "processing_date": "2023-01-03"}, "jma_changed_years": [],
                "jma": {"archives": [{"year": 2023, "segment": "full-year", "manifest_uri": "s3://bucket/jma/manifest.json",
                                      "manifest_sha256": "b" * 64, "sha256": "c" * 64,
                                      "record_count_estimate": 0, "ingest_invoked": False}]}}

    def run_main(self, failed=False, existing=False):
        def runs(name):
            now = datetime.now(timezone.utc)
            end = now.replace(hour=0, minute=15, second=0, microsecond=0)
            if end > now:
                from datetime import timedelta
                end -= timedelta(days=1)
            return [{"run_id": "scheduled__test", "state": "success", "data_interval_end": end.isoformat()}] if (self.scheduled or existing) and name == self.qa.DAG else []
        def report(run_id):
            value = self.report(run_id)
            if existing and run_id == "scheduled__test":
                value.update(profile=self.qa.profile(), data_interval_end=runs(self.qa.DAG)[0]["data_interval_end"])
            return value
        with patch.dict("os.environ", self.env), patch.object(self.qa, "cli", side_effect=self.cli), patch.object(
                self.qa, "trigger_daily", side_effect=lambda run, logical: self.triggers.append((run, logical))), patch.object(
                self.qa, "dagruns", side_effect=runs), patch.object(self.qa, "state", side_effect=lambda name, run:
                "failed" if failed or name == self.qa.BACKFILL else "success"), patch.object(
                self.qa, "task_states", side_effect=lambda name, run: {
                    "jma_year_backfill.acquire_source_lease": "failed", "jma_year_backfill.ingest_archive": "upstream_failed"
                } if name == self.qa.BACKFILL else {key: "success" for key in (
                    "resolve_daily", "acquire_source_lease", "jma_readiness", "usgs_bronze", "sources_ready",
                    "real_etl", "release_source_lease", "completion_gate")}), patch.object(
                self.qa, "summary", side_effect=report), contextlib.redirect_stdout(io.StringIO()):
            self.qa.main()

    def test_valid_existing_scheduled_report_is_reused_and_qa_continues(self):
        self.run_main(existing=True)
        evidence = json.loads(next(Path(self.env["SOURCE_READINESS_ROOT"]).glob("qa/*/workflow-evidence.json")).read_text())
        self.assertEqual("VERIFIED", evidence["status"])
        self.assertEqual("scheduled__test", evidence["daily_run_ids"]["scheduled"])
        self.assertEqual(2, len(self.triggers))

    def test_three_daily_reports_controlled_contention_and_original_pause_restore(self):
        self.run_main()
        evidence = list(Path(self.env["SOURCE_READINESS_ROOT"]).glob("qa/*/workflow-evidence.json"))
        self.assertEqual(1, len(evidence))
        value = json.loads(evidence[0].read_text())
        self.assertEqual("VERIFIED", value["status"]); self.assertFalse(value["published"])
        self.assertEqual({"scheduled", "first", "rerun"}, set(value["daily_run_ids"]))
        self.assertTrue(value["contention"]["holder_preserved"])
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())
        self.assertIn(("dags", "pause", self.qa.DAG, "--yes"), self.calls)
        self.assertIn(("dags", "pause", self.qa.BACKFILL, "--yes"), self.calls)
        logicals = [logical for run, logical in self.triggers]
        self.assertEqual(2, len(logicals)); self.assertNotEqual(*logicals)

    def test_failure_restores_pause_and_never_emits_verified_workflow(self):
        with self.assertRaises(SystemExit): self.run_main(failed=True)
        root = Path(self.env["SOURCE_READINESS_ROOT"])
        self.assertTrue(list(root.glob("qa/*/failure.json")))
        self.assertFalse(list(root.glob("qa/*/workflow-evidence.json")))
        self.assertIn(("dags", "pause", self.qa.DAG, "--yes"), self.calls)
        self.assertIn(("dags", "pause", self.qa.BACKFILL, "--yes"), self.calls)

    def test_cli_error_does_not_echo_stderr_or_credentials(self):
        with patch.object(self.qa.subprocess, "run", return_value=subprocess.CompletedProcess([], 1, "payload", "secret")):
            with self.assertRaisesRegex(RuntimeError, "^AIRFLOW_CLI_FAILED$"): self.qa.cli("dags", "list")

    def test_success_with_zero_task_instances_is_not_runtime_evidence(self):
        with patch.object(self.qa, "state", return_value="success"), patch.object(self.qa, "task_states", return_value={}):
            with self.assertRaisesRegex(RuntimeError, "SOURCE_TASKS_NOT_EXECUTED"): self.qa.wait(self.qa.DAG, "empty")

    def test_fixed_interval_trigger_pins_run_after_not_only_logical_date(self):
        logical = datetime(2023, 1, 4, 0, 16, 0, 123456, tzinfo=timezone.utc)
        with patch.object(self.qa, "trigger_dag", return_value=object()) as trigger:
            self.qa.trigger_daily("fixed", logical)
        self.assertEqual(logical, trigger.call_args.kwargs["run_after"])
        self.assertEqual(logical, trigger.call_args.kwargs["logical_date"])
        self.assertFalse(trigger.call_args.kwargs["replace_microseconds"])
        self.assertEqual({}, trigger.call_args.kwargs["conf"])

    def test_failed_current_interval_stops_before_unpause_or_trigger(self):
        now = datetime.now(timezone.utc)
        end = now.replace(hour=0, minute=15, second=0, microsecond=0)
        if end > now:
            from datetime import timedelta
            end -= timedelta(days=1)
        run = {"run_id": "scheduled__2026-10-13T00:15:00+00:00", "state": "failed",
               "data_interval_end": end.isoformat()}
        with patch.dict("os.environ", self.env), patch.object(self.qa, "cli", side_effect=self.cli), patch.object(
                self.qa, "dagruns", side_effect=lambda name: [run] if name == self.qa.DAG else []), patch.object(
                self.qa, "task_states", return_value={"jma_readiness": "failed"}), patch.object(
                self.qa, "trigger_daily") as trigger, contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit): self.qa.main()
        trigger.assert_not_called()
        self.assertFalse(any(call[1] in {"unpause", "trigger", "clear"} for call in self.calls))
        failure = json.loads(next(Path(self.env["SOURCE_READINESS_ROOT"]).glob("qa/*/failure.json")).read_text())
        self.assertEqual("SCHEDULED_RUN_FAILED_RECOVER_REQUIRED", failure["reason"])
        self.assertEqual("scheduled", failure["phase"])
        self.assertEqual(["jma_readiness"], failure["failed_tasks"])

    def test_previous_interval_failure_does_not_block_today(self):
        end = datetime(2026, 10, 13, 0, 15, tzinfo=timezone.utc)
        runs = [{"run_id": "scheduled__2026-10-11T00:15:00+00:00", "state": "failed",
                 "data_interval_end": "2026-10-12T00:15:00+00:00"},
                {"run_id": "scheduled__2026-10-12T00:15:00+00:00", "state": "success",
                 "data_interval_end": end.isoformat()}]
        self.assertEqual(runs[1]["run_id"], self.qa.scheduled_for_interval(runs, end))
        self.assertIsNone(self.qa.scheduled_for_interval(runs[:1], end))

    def test_current_success_with_missing_report_is_not_silently_reused(self):
        end = datetime(2026, 10, 13, 0, 15, tzinfo=timezone.utc)
        run = {"run_id": "scheduled__2026-10-13T00:15:00+00:00", "state": "success",
               "data_interval_end": end.isoformat()}
        with patch.dict("os.environ", self.env), patch.object(self.qa, "datetime", wraps=datetime) as clock, patch.object(
                self.qa, "cli", side_effect=self.cli), patch.object(self.qa, "dagruns", side_effect=lambda name:
                [run] if name == self.qa.DAG else []), patch.object(self.qa, "summary", side_effect=FileNotFoundError()), patch.object(
                self.qa, "wait") as wait, contextlib.redirect_stdout(io.StringIO()):
            clock.now.return_value = datetime(2026, 10, 13, 6, 0, tzinfo=timezone.utc)
            with self.assertRaises(SystemExit): self.qa.main()
        wait.assert_not_called()
        self.assertFalse(any(call[1] == "unpause" for call in self.calls))
        failure = json.loads(next(Path(self.env["SOURCE_READINESS_ROOT"]).glob("qa/*/failure.json")).read_text())
        self.assertEqual("SCHEDULED_SUCCESS_INVALID_REPORT", failure["reason"])

    def test_failure_details_only_expose_safe_scoped_metadata(self):
        run = "scheduled__2026-10-12T00:15:00+00:00"
        slug = "jma-" + hashlib.sha256(run.encode()).hexdigest()[:32]
        base = Path(self.temp.name) / "jma" / "runs" / slug / "year=2000/segment=full-year"
        base.mkdir(parents=True)
        result = base / "result.json"
        result.write_text(json.dumps({"status": "FAILED", "reason": "JMA_HEAD_CONNECT_TIMEOUT",
                                      "http_head_attempts": 3, "secret": "private-payload"}))
        with patch.dict("os.environ", {"JMA_STAGING_ROOT": self.temp.name + "/jma"}), patch.object(
                self.qa, "task_states", return_value={"jma_readiness": "failed", "private-task": "failed"}):
            details = self.qa.failure_details(self.qa.DAG, run)
            self.assertEqual(2000, details["archive_year"])
            self.assertEqual("JMA_HEAD_CONNECT_TIMEOUT", details["runner_reason"])
            self.assertEqual(3, details["http_head_attempts"])
            self.assertNotIn("private-payload", json.dumps(details))
            result.write_text(json.dumps({"status": "FAILED", "reason": "https://user:secret@host", "http_head_attempts": True}))
            details = self.qa.failure_details(self.qa.DAG, run)
            self.assertEqual("RUNNER_FAILED", details["runner_reason"])
            self.assertNotIn("http_head_attempts", details)
            self.assertNotIn("secret", json.dumps(details))
            self.assertEqual({}, self.qa.failure_details(self.qa.DAG, "../../unsafe"))

    def test_wait_failure_retains_run_identity_without_exception_text(self):
        with patch.object(self.qa, "state", return_value="failed"):
            with self.assertRaises(self.qa.QaFailure) as caught:
                self.qa.wait(self.qa.DAG, "scheduled__2026-10-12T00:15:00+00:00")
        self.assertEqual("DAG_RUN_FAILED", str(caught.exception))
        self.assertEqual(self.qa.DAG, caught.exception.dag_id)

    def test_unexpected_exception_is_redacted_in_terminal_and_evidence(self):
        output = io.StringIO()
        with patch.dict("os.environ", self.env), patch.object(self.qa, "profile", side_effect=ValueError("private-credential")), contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit): self.qa.main()
        failure = next(Path(self.env["SOURCE_READINESS_ROOT"]).glob("qa/*/failure.json")).read_text()
        self.assertIn("QA_UNEXPECTED_FAILURE", failure)
        self.assertNotIn("private-credential", failure + output.getvalue())

    def test_cleanup_failure_preserves_original_reason_and_attempts_both_dags(self):
        def failing_cli(*args):
            if args[1] == "pause":
                self.calls.append(args)
                raise RuntimeError("private-cleanup-error")
            return self.cli(*args)
        run = {"run_id": "scheduled__2026-10-13T00:15:00+00:00", "state": "running"}
        with patch.dict("os.environ", self.env), patch.object(self.qa, "cli", side_effect=failing_cli), patch.object(
                self.qa, "dagruns", return_value=[run]), contextlib.redirect_stdout(io.StringIO()):
            with self.assertRaises(SystemExit): self.qa.main()
        failure = json.loads(next(Path(self.env["SOURCE_READINESS_ROOT"]).glob("qa/*/failure.json")).read_text())
        self.assertEqual("EXISTING_ACTIVE_RUN", failure["reason"])
        self.assertEqual(["PAUSE_STATE_RESTORE_FAILED"], failure["cleanup_failed"])
        self.assertEqual(2, len([call for call in self.calls if call[1] == "pause"]))
        self.assertNotIn("private-cleanup-error", json.dumps(failure))


if __name__ == "__main__": unittest.main()
