import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import silver_integration_qa as qa
from source_run_guard import lease, SourceRunBusy


class SilverIntegrationControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = {"CONFIG_VERSION": "test-v1", "SOURCE_GUARD_ROOT": self.temp.name + "/guard",
                    "BACKFILL_STAGING_ROOT": self.temp.name + "/staging"}
        self.calls = []

    def submit(self, request, root, env):
        content = request.read_bytes()
        self.calls.append(content)
        conf = json.loads(content)
        lease("assert", {"dag_id": "slv_09_integration_qa", "run_id": conf["run_id"]}, env)
        return {"task": "SLV-09", "run_id": conf["run_id"], "scope_sha256": conf["scope_sha256"],
            "silver_status": "SilverReady", "gold_handoff_verified": True, "gold_published": False,
            "master": "spark://spark-master:7077", "java_version": "17.0.18", "idempotent_reuse": len(self.calls) == 2,
            "reconciliation": {"balanced": True}, "datasets": {name: {} for name in
                ("source_observation", "reject_record", "source_link", "canonical_membership")},
            "bundle_manifest_sha256": "a" * 64, "identity_sha256": "b" * 64,
            "gold_canonical_rows": 1, "gold_bridge_rows": 1, "sources": []}

    def test_same_request_twice_under_whole_run_lease_then_release(self):
        with patch.object(qa, "idle", return_value=True), patch.object(qa, "memory", return_value={"events": {"oom_kill": 0}}), \
                patch.object(qa, "submit", side_effect=self.submit):
            report = qa.run(self.env)
        self.assertTrue(report["rerun_unchanged"])
        self.assertEqual(self.calls[0], self.calls[1])
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())
        self.assertTrue(Path(report["report_path"]).is_file())

    def test_submit_has_bounded_small_standalone_profile(self):
        root = Path(self.temp.name)
        def finished(command, **kwargs):
            kwargs["stdout"].write(b'{"ok":true}')
            return subprocess.CompletedProcess(command, 0)
        with patch.object(qa, "java_identity", side_effect=lambda env, _: env), \
                patch.object(qa.subprocess, "run", side_effect=finished) as run:
            self.assertEqual({"ok": True}, qa.submit(root / "input.json", root, self.env))
        command = run.call_args.args[0]
        self.assertEqual(600, run.call_args.kwargs["timeout"])
        self.assertIn("spark.sql.adaptive.enabled=false", command)
        self.assertIn("spark.sql.codegen.wholeStage=false", command)
        self.assertIn("spark.cores.max=1", command)
        self.assertEqual("512m", command[command.index("--executor-memory") + 1])

    def test_contender_is_blocked_before_staging_or_submit(self):
        lease("acquire", {"dag_id": "other", "run_id": "active"}, self.env)
        with patch.object(qa, "submit") as submit, self.assertRaises(SourceRunBusy):
            qa.run(self.env)
        submit.assert_not_called()
        self.assertFalse(Path(self.env["BACKFILL_STAGING_ROOT"]).exists())

    def test_busy_cluster_fails_before_submit_and_releases_own_lease(self):
        with patch.object(qa, "idle", return_value=False), patch.object(qa, "submit") as submit, self.assertRaises(Exception):
            qa.run(self.env)
        submit.assert_not_called()
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_timeout_retains_lease_when_application_might_be_active(self):
        with patch.object(qa, "idle", side_effect=[True, True, False]), \
                patch.object(qa, "memory", return_value={"events": {"oom_kill": 0}}), \
                patch.object(qa, "submit", side_effect=subprocess.TimeoutExpired("spark-submit", 600)), self.assertRaises(Exception):
            qa.run(self.env)
        self.assertTrue((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())
        failure = list(Path(self.env["BACKFILL_STAGING_ROOT"]).rglob("failure.json"))
        self.assertEqual("TimeoutExpired", json.loads(failure[0].read_text())["error_type"])
        self.assertNotIn("spark-submit", failure[0].read_text())

    def test_submit_failure_releases_only_after_cluster_confirmed_idle(self):
        with patch.object(qa, "idle", return_value=True), patch.object(qa, "memory", return_value={"events": {"oom_kill": 0}}), \
                patch.object(qa, "submit", side_effect=RuntimeError("submit failed")), self.assertRaises(Exception):
            qa.run(self.env)
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_changed_rerun_receipt_blocks_final_success_report(self):
        def changed(request, root, env):
            report = self.submit(request, root, env)
            if len(self.calls) == 2:
                report["identity_sha256"] = "c" * 64
            return report
        with patch.object(qa, "idle", return_value=True), patch.object(qa, "memory", return_value={"events": {"oom_kill": 0}}), \
                patch.object(qa, "submit", side_effect=changed), self.assertRaises(Exception):
            qa.run(self.env)
        self.assertFalse(list(Path(self.env["BACKFILL_STAGING_ROOT"]).rglob("integration_report.json")))

    def test_wrong_runtime_or_uncommitted_handoff_receipt_is_not_success(self):
        def wrong(request, root, env):
            report = self.submit(request, root, env); report["java_version"] = "23.0.2"; return report
        with patch.object(qa, "idle", return_value=True), patch.object(qa, "memory", return_value={"events": {"oom_kill": 0}}), \
                patch.object(qa, "submit", side_effect=wrong), self.assertRaises(Exception):
            qa.run(self.env)
        self.assertEqual(1, len(self.calls))


if __name__ == "__main__":
    unittest.main()
