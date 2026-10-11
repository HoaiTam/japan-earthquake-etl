import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import gold_publication_qa as qa
from source_run_guard import lease, SourceRunBusy


class PublicationControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.env = {"SOURCE_GUARD_ROOT": str(root / "guard")}
        self.commit = {"namespace": "iceberg.gold_qa_abc", "gold_run_id": "gold-test", "identity_sha256": "a" * 64,
                       "tables": {"event_current": {"snapshot_id": 1}}}
        self.reference = root / "reference.json"
        self.reference.write_text(json.dumps({"commit": self.commit}))
        item = patch.object(qa, "STAGING", root / "staging")
        item.start()
        self.addCleanup(item.stop)

    def finished(self, command, **kwargs):
        path = Path(command[-1])
        request = json.loads(path.read_text())
        lease("assert", {"dag_id": request["dag_id"], "run_id": request["run_id"]}, kwargs["env"])
        self.assertEqual(1800, kwargs["timeout"])
        result = {**self.commit, "snapshot_bundle": self.commit["tables"], "published": True,
                  "rerun_unchanged": True, "engine": "TRINO", "verify_status": "PASSED"}
        (path.parent / "gld-04-report.json").write_text(json.dumps(result))
        return subprocess.CompletedProcess(command, 0)

    def test_success_checks_exact_handoff_and_releases_own_guard(self):
        with patch.object(qa.subprocess, "run", side_effect=self.finished):
            self.assertTrue(qa.run(self.reference, self.env)["published"])
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_jvm_failure_keeps_guard_for_remote_trino_query(self):
        with patch.object(qa.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)), self.assertRaises(RuntimeError):
            qa.run(self.reference, self.env)
        self.assertTrue((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_timeout_keeps_guard(self):
        with patch.object(qa.subprocess, "run", side_effect=subprocess.TimeoutExpired("spark-submit", 1800)), self.assertRaises(Exception):
            qa.run(self.reference, self.env)
        self.assertTrue((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_busy_or_non_qa_reference_never_submits(self):
        lease("acquire", {"dag_id": "daily", "run_id": "active"}, self.env)
        with patch.object(qa.subprocess, "run") as submit, self.assertRaises(SourceRunBusy):
            qa.run(self.reference, self.env)
        submit.assert_not_called()
        self.commit["namespace"] = "iceberg.gold"
        self.reference.write_text(json.dumps({"commit": self.commit}))
        with self.assertRaises(ValueError):
            qa.run(self.reference, self.env)


if __name__ == "__main__":
    unittest.main()
