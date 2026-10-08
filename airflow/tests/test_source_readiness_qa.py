"""Offline harness flow/cleanup tests; these are not scheduler/source evidence."""
import contextlib
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

    def run_main(self, failed=False):
        def runs(name):
            return [{"run_id": "scheduled__test", "state": "success"}] if self.scheduled and name == self.qa.DAG else []
        with patch.dict("os.environ", self.env), patch.object(self.qa, "cli", side_effect=self.cli), patch.object(
                self.qa, "trigger_daily", side_effect=lambda run, logical: self.triggers.append((run, logical))), patch.object(
                self.qa, "dagruns", side_effect=runs), patch.object(self.qa, "state", side_effect=lambda name, run:
                "failed" if failed or name == self.qa.BACKFILL else "success"), patch.object(
                self.qa, "task_states", side_effect=lambda name, run: {
                    "jma_year_backfill.acquire_source_lease": "failed", "jma_year_backfill.ingest_archive": "upstream_failed"
                } if name == self.qa.BACKFILL else {key: "success" for key in (
                    "resolve_daily", "acquire_source_lease", "jma_readiness", "usgs_bronze", "sources_ready",
                    "release_source_lease", "completion_gate")}), patch.object(
                self.qa, "summary", side_effect=self.report), contextlib.redirect_stdout(io.StringIO()):
            self.qa.main()

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


if __name__ == "__main__": unittest.main()
