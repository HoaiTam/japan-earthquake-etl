"""Offline tests for the live harness, not evidence of actual source/MinIO calls."""
import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

REPO = Path(__file__).parents[2]
spec = importlib.util.spec_from_file_location("jma_live_qa", REPO / "compose/airflow/jma-live-qa.py")
qa = importlib.util.module_from_spec(spec)
spec.loader.exec_module(qa)


class JmaLiveQaTest(unittest.TestCase):
    def test_scope_and_summary_path_are_fixed_and_collision_safe(self):
        self.assertEqual([1997, 2000, 2023], qa.YEARS)
        self.assertNotEqual(qa.summary_path("a/b"), qa.summary_path("a-b"))
        self.assertEqual("run_summary.json", qa.summary_path("../../x").name)
        self.assertNotIn("..", qa.summary_path("../../x").parent.name)

    def test_cli_failure_does_not_leak_stderr(self):
        with patch.object(qa.subprocess, "run", return_value=subprocess.CompletedProcess([], 2, "", "credential=do-not-echo")):
            with self.assertRaisesRegex(RuntimeError, "^AIRFLOW_CLI_FAILED$"):
                qa.cli("dags", "list")

    def test_wait_accepts_state_with_airflow3_conf_and_fails_on_failed(self):
        with patch.object(qa, "cli", side_effect=["running, {}", "success, {}"]), patch.object(qa.time, "sleep"):
            qa.wait_run("qa-test")
        with patch.object(qa, "cli", return_value="failed, {}"):
            with self.assertRaisesRegex(RuntimeError, "JMA_DAG_FAILED"):
                qa.wait_run("qa-test")

    def test_wait_timeout_does_not_cancel_or_clear_running_dag(self):
        with patch.object(qa.time, "monotonic", side_effect=[0, qa.TIMEOUT + 1]), patch.object(qa, "cli") as cli:
            with self.assertRaisesRegex(RuntimeError, "JMA_DAG_TIMEOUT"): qa.wait_run("qa-test")
            cli.assert_not_called()

    def test_verify_uses_java_exact_paths_no_shell_and_requires_nonzero_gate(self):
        with patch.dict(os.environ, {"JAVA_HOME": "/java", "JMA_INVENTORY_PATH": "/inventory.csv"}), \
                patch.object(qa.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, "", "")) as run:
            qa.verify("qa-first", Path("report.json"), Path("baseline.json"))
            args = run.call_args.args[0]
            self.assertIn("ie212.earthquake.spark.jma.JmaBronzeQaVerifier", args)
            self.assertIn("--baseline", args)
            self.assertNotIn("shell", run.call_args.kwargs)
            run.return_value = subprocess.CompletedProcess([], 2, "", "sensitive")
            with self.assertRaisesRegex(RuntimeError, "^JMA_JAVA_READBACK_FAILED$"):
                qa.verify("qa-first", Path("report.json"))

    def test_main_preserves_pause_and_first_readback_precedes_rerun(self):
        for original in (True, False):
            with self.subTest(original=original), tempfile.TemporaryDirectory() as directory:
                calls = []
                def cli(*args):
                    calls.append(args)
                    if args[:2] == ("dags", "list-import-errors"): return "[]"
                    if args[:2] == ("dags", "list"): return json.dumps([{"dag_id": qa.DAG, "is_paused": original}])
                    return ""
                def wait(run):
                    if run.endswith("preview"):
                        path = qa.summary_path(run); path.parent.mkdir(parents=True)
                        path.write_text(json.dumps({"status": "PREVIEW", "verified": False, "planned_archives": 4, "ready_archives": 0}))
                def verify(run, report, baseline=None):
                    calls.append(("verify", run, baseline is not None))
                with patch.dict(os.environ, {"JMA_STAGING_ROOT": directory}), patch.object(qa, "cli", side_effect=cli), \
                        patch.object(qa, "wait_run", side_effect=wait), patch.object(qa, "verify", side_effect=verify):
                    with contextlib.redirect_stdout(io.StringIO()): qa.main()
                triggers = [call for call in calls if call[:2] == ("dags", "trigger")]
                self.assertEqual(3, len(triggers))
                self.assertEqual([True, False, False], [json.loads(call[-1])["preview"] for call in triggers])
                self.assertEqual([False, True], [call[2] for call in calls if call[0] == "verify"])
                self.assertLess(calls.index(next(c for c in calls if c[0] == "verify")), calls.index(triggers[-1]))
                self.assertEqual(("dags", "pause" if original else "unpause", qa.DAG, "--yes"), calls[-1])
                evidence = list(Path(directory).glob("qa/*/workflow-evidence.json"))
                self.assertEqual("VERIFIED", json.loads(evidence[0].read_text())["status"])

    def test_failed_readback_preserves_failure_report_not_verified_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            def cli(*args):
                if args[:2] == ("dags", "list-import-errors"): return "[]"
                if args[:2] == ("dags", "list"): return json.dumps([{"dag_id": qa.DAG, "is_paused": True}])
                return ""
            with patch.dict(os.environ, {"JMA_STAGING_ROOT": directory}), patch.object(qa, "cli", side_effect=cli), \
                    patch.object(qa, "wait_run", side_effect=RuntimeError("failure")):
                with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit): qa.main()
            self.assertEqual([], list(Path(directory).glob("qa/*/workflow-evidence.json")))
            self.assertEqual(1, len(list(Path(directory).glob("qa/*/workflow-failure.json"))))

    def test_pause_restore_failure_cannot_leave_workflow_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            def cli(*args):
                if args[:2] == ("dags", "list-import-errors"): return "[]"
                if args[:2] == ("dags", "list"): return json.dumps([{"dag_id": qa.DAG, "is_paused": True}])
                if args[:2] == ("dags", "pause"): raise RuntimeError("failure")
                return ""
            def wait(run):
                path = qa.summary_path(run); path.parent.mkdir(parents=True)
                path.write_text(json.dumps({"status": "PREVIEW", "verified": False, "planned_archives": 4, "ready_archives": 0}))
            with patch.dict(os.environ, {"JMA_STAGING_ROOT": directory}), patch.object(qa, "cli", side_effect=cli), \
                    patch.object(qa, "wait_run", side_effect=wait), patch.object(qa, "verify"), \
                    contextlib.redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
                qa.main()
            self.assertEqual([], list(Path(directory).glob("qa/*/workflow-evidence.json")))
            failed = list(Path(directory).glob("qa/*/workflow-failure.json"))[0]
            self.assertEqual("PAUSE_RESTORE_FAILED", json.loads(failed.read_text())["reason"])


if __name__ == "__main__": unittest.main()
