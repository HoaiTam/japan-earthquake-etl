import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import dataset_manifest_qa as qa
from source_run_guard import lease


class DatasetManifestControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.env = {"SOURCE_GUARD_ROOT": str(root / "guard")}
        self.reference = root / "reference.json"
        self.publication = {"namespace": "iceberg.gold_qa_abc", "publication_id": "pub_" + "a" * 64,
                            "published": True, "verify_status": "PASSED", "snapshot_bundle": {"event_current": {"snapshot_id": 99}}}
        self.reference.write_text(json.dumps(self.publication))
        self.config = root / "config.json"
        self.config.write_text(json.dumps({"datasets": [{"dataset_split": "REPRODUCTION"}, {"dataset_split": "EXTENSION"}]}))
        item = patch.object(qa, "STAGING", root / "staging")
        item.start()
        self.addCleanup(item.stop)

    def finished(self, command, **kwargs):
        path = Path(command[-1])
        request = json.loads(path.read_text())
        lease("assert", {"dag_id": request["dag_id"], "run_id": request["run_id"]}, kwargs["env"])
        self.assertEqual("iceberg.ml_qa_abc", request["ml_namespace"])
        result = {"rerun_unchanged": True, "manifests": [{"manifest": {
            "gold_publication_id": self.publication["publication_id"], "gold_snapshot_id": 99,
            "dataset_status": "BUILDING"}} for _ in range(2)]}
        (path.parent / "mld-01-report.json").write_text(json.dumps(result))
        return subprocess.CompletedProcess(command, 0)

    def test_success_uses_exact_publication_and_releases_owner(self):
        with patch.object(qa.subprocess, "run", side_effect=self.finished):
            self.assertTrue(qa.run(self.reference, self.config, self.env)["rerun_unchanged"])
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_failed_jvm_and_timeout_retain_lease(self):
        with patch.object(qa.subprocess, "run", side_effect=subprocess.TimeoutExpired("spark-submit", 1800)), self.assertRaises(Exception):
            qa.run(self.reference, self.config, self.env)
        self.assertTrue((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_unpublished_or_production_namespace_never_submits(self):
        self.publication["namespace"] = "iceberg.gold"
        self.reference.write_text(json.dumps(self.publication))
        with patch.object(qa.subprocess, "run") as submit, self.assertRaises(ValueError):
            qa.run(self.reference, self.config, self.env)
        submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
