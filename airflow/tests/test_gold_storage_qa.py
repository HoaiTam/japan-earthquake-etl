import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import gold_storage_qa as qa
from source_run_guard import lease, SourceRunBusy


class GoldControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.capture = root / "capture.json"
        self.capture.write_text('{"features":[]}')
        self.env = {"CONFIG_VERSION": "test-v1", "GOLD_CODE_VERSION": "test-v1",
                    "ICEBERG_CATALOG_NAME": "iceberg", "SOURCE_GUARD_ROOT": str(root / "guard")}
        self.patches = [patch.object(qa, "CAPTURE", self.capture), patch.object(qa, "STAGING", root / "staging")]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def finished(self, command, **kwargs):
        request_path = Path(command[-2])
        request = json.loads(request_path.read_text())
        lease("assert", {"dag_id": "gold_storage_qa", "run_id": request["run_id"]}, kwargs["env"])
        self.assertEqual("512m", command[command.index("--driver-memory") + 1])
        self.assertEqual(1200, kwargs["timeout"])
        self.assertEqual(64, len(request["capture_sha256"]))
        (request_path.parent / "gld-03-report.json").write_text(json.dumps(
            {"rerun_unchanged": True, "commit": {"published": False}}))
        return subprocess.CompletedProcess(command, 0)

    def test_completed_jvm_releases_only_own_lease(self):
        with patch.object(qa.subprocess, "run", side_effect=self.finished):
            report = qa.run(self.env)
        self.assertTrue(Path(report["report_path"]).exists())
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_busy_lease_blocks_before_any_submit(self):
        lease("acquire", {"dag_id": "daily", "run_id": "active"}, self.env)
        with patch.object(qa.subprocess, "run") as submit, self.assertRaises(SourceRunBusy):
            qa.run(self.env)
        submit.assert_not_called()

    def test_timeout_or_interruption_preserves_guard(self):
        for error in (subprocess.TimeoutExpired("spark-submit", 1200), KeyboardInterrupt()):
            with self.subTest(error=type(error).__name__):
                owner = Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json"
                if owner.exists():
                    lease("release", json.loads(owner.read_text()), self.env)
                with patch.object(qa.subprocess, "run", side_effect=error), self.assertRaises(BaseException):
                    qa.run(self.env)
                self.assertTrue(owner.exists())

    def test_nonzero_jvm_does_not_report_success(self):
        with patch.object(qa.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)), self.assertRaises(RuntimeError):
            qa.run(self.env)
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())
        failures = list(qa.STAGING.rglob("failure.json"))
        self.assertEqual(1, json.loads(failures[0].read_text())["exit_code"])


if __name__ == "__main__":
    unittest.main()
