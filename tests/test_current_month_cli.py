import importlib.util
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch, Mock

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("current_month_cli", ROOT / "scripts/current-month.py")
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


class CurrentMonthCliTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.root = Path(self.folder.name).resolve()
        self.addCleanup(self.folder.cleanup)
        self.request = self.root / "request.json"
        self.context = {"dag_id":"orc_02_daily_sources", "run_id":"scheduled__2026-10-21T00:15:00Z"}
        self.request.write_text(json.dumps({"run_context":self.context}))
        self.java = self.root / "jdk/bin/java"
        self.java.parent.mkdir(parents=True)
        self.java.touch()
        self.cp = self.root / "classpath.txt"
        self.cp.write_text("/prepared/dependency.jar")
        for name, obj in (("ROOT", self.root),):
            p = patch.object(cli, name, obj); p.start(); self.addCleanup(p.stop)
        p = patch.object(cli.runtime, "CP", self.cp); p.start(); self.addCleanup(p.stop)
        p = patch.object(cli, "environment", return_value={"JAVA_HOME":str(self.java.parent.parent)})
        p.start(); self.addCleanup(p.stop)

    def test_success_executes_only_prepared_local_java_no_docker_or_build(self):
        def run(command, **kwargs):
            self.assertIn("ie212.earthquake.spark.CurrentMonthSourceSilverJob", command)
            self.assertEqual(command[-3], "--local")
            self.assertFalse(kwargs.get("shell", False))
            self.assertFalse(any("docker" in arg or "mvnw" in arg or "spark-submit" in arg for arg in command))
            kwargs["stdout"].write(b"fixture log")
            self.receipt()
            return subprocess.CompletedProcess(command, 0)
        with patch.object(cli, "verify_runtime"), patch.object(cli.subprocess, "run", side_effect=run), patch("builtins.print"):
            cli.main(["--request",str(self.request)])
        log, = (self.root / "staging/current-month-logs").glob("*.private.log")
        self.assertEqual(log.stat().st_mode & 0o777, 0o600)

    def receipt(self):
        op = "cm1-" + hashlib.sha256((self.context["dag_id"] + "|" + self.context["run_id"]).encode()).hexdigest()[:40]
        state = self.root / "staging/current-month-component/silver/current-month/state" / op
        state.mkdir(parents=True, exist_ok=True)
        raw = json.dumps({"contract_version":"current-month-source-silver-v1", "operation_id":op,
                          "status":"ObservedAsOfReady", "coverage_mode":"OBSERVED_AS_OF", "publication_quality":"PROVISIONAL",
                          "published":False, "research_ready":False, "ml_upstream_eligible":False, "reconciliation":{}}).encode()
        (state / "receipt.json").write_bytes(raw)
        (state / "_SUCCESS").write_bytes(hashlib.sha256(raw).hexdigest().encode())
        return state

    def test_exit_zero_without_receipt_or_with_modified_marker_is_not_success(self):
        with patch.object(cli, "verify_runtime"), patch.object(cli.subprocess, "run", return_value=Mock(returncode=0)):
            with self.assertRaisesRegex(ValueError, "RECEIPT_MISSING"):
                cli.main(["--request",str(self.request)])
        state = self.receipt()
        (state / "_SUCCESS").write_text("bad")
        with self.assertRaisesRegex(ValueError, "MARKER_CHANGED"):
            cli.read_receipt(self.root / "staging/current-month-component", self.request)

    def test_failure_retains_private_log_and_does_not_clean_or_retry(self):
        with patch.object(cli, "verify_runtime"), patch.object(cli.subprocess, "run", return_value=Mock(returncode=2)) as run:
            with self.assertRaisesRegex(ValueError, "CURRENT_MONTH_FAILED"):
                cli.main(["--request",str(self.request)])
            self.assertEqual(run.call_count, 1)
        self.assertEqual(len(list((self.root / "staging/current-month-logs").glob("*.private.log"))), 1)

    def test_missing_prepare_bad_java_and_unsafe_root_fail_before_exec(self):
        with patch.object(cli, "verify_runtime", side_effect=ValueError("PREPARE_REQUIRED")), patch.object(cli.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "PREPARE_REQUIRED"):
                cli.main(["--request",str(self.request)])
            run.assert_not_called()
        for root in (self.root, self.root / "staging"):
            with self.subTest(root=root), self.assertRaisesRegex(ValueError, "SCOPED"):
                cli.main(["--request",str(self.request),"--root",str(root)])
        with patch.object(cli, "environment", return_value={}):
            with self.assertRaisesRegex(ValueError, "JAVA_HOME_REQUIRED"):
                cli.main(["--request",str(self.request)])

    def test_symlink_and_oversized_request_are_rejected(self):
        staging = self.root / "staging"; staging.mkdir()
        link = staging / "link"; link.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError):
            cli.main(["--request",str(self.request),"--root",str(link / "outside")])
        self.request.write_bytes(b" " * 32769)
        with self.assertRaisesRegex(ValueError, "BOUNDED_REQUEST_REQUIRED"):
            cli.main(["--request",str(self.request)])


if __name__ == "__main__":
    unittest.main()
