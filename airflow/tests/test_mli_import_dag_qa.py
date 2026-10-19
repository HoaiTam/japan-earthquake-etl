import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import mli_import_dag_qa as qa
from mli_import_qa import CHECKS


class MliImportDagQaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.patch = patch.object(qa, "STAGING", self.root); self.patch.start(); self.addCleanup(self.patch.stop)
        self.path = self.root / "case/import-request.json"
        self.path.parent.mkdir()
        (self.path.parent / "registry").mkdir()
        (self.path.parent / "registry/dataset.json").write_text("{}")
        self.request = {"dag_id": "mli_03_import_results", "run_id": "mli03-qa-unit", "dataset_id": "ds_fixture",
                        "experiment_run_id": "exp_fixture", "algorithm_name": "WINDOW", "model_config_version": "1",
                        "dataset_manifest_ref": "dataset.json", "dataset_manifest_sha256": hashlib.sha256(b"{}").hexdigest(),
                        "artifact_root": str(self.path.parent / "valid"), "journal_root": str(self.path.parent / "journal"),
                        "ml_namespace": "iceberg.ml_qa_" + "a" * 32}
        self.report = {"task": "MLI-03", "status": "PASSED", "namespace": self.request["ml_namespace"],
                       "synthetic_component_qa": True, "scientific_approval": False, "checks": dict.fromkeys(CHECKS, True),
                       "publication": {"experiment_status": "CANDIDATE", "membership_snapshot_id": 11,
                                       "summary_snapshot_id": 12, "experiment_snapshot_id": 13,
                                       "readback": {"sequence_membership": 2, "sequence_summary": 1, "experiment_run": 1}},
                       "lease_release": {"status": "RELEASED"}, "control_plane": {"status": "CANDIDATE"}}
        self.save()

    def save(self):
        self.path.write_text(json.dumps(self.request))
        (self.path.parent / "acceptance-report.json").write_text(json.dumps(self.report))

    def test_only_already_accepted_exact_qa_request_is_allowed(self):
        request, digest, report = qa.validated_request(self.path)
        self.assertEqual(self.request, request)
        self.assertEqual(hashlib.sha256(self.path.read_bytes()).hexdigest(), digest)
        self.assertEqual(self.report, report)

    def test_storage_and_release_gate_cannot_be_skipped(self):
        for key in ("lease_release", "control_plane"):
            with self.subTest(key=key):
                before = self.report.pop(key); self.save()
                with self.assertRaisesRegex(RuntimeError, "STORAGE_ACCEPTANCE_REQUIRED"): qa.validated_request(self.path)
                self.report[key] = before; self.save()

    def test_manifest_and_artifact_drift_are_rejected(self):
        for key, value in (("dataset_manifest_sha256", "0" * 64), ("artifact_root", "/outside"),
                           ("journal_root", "/outside"), ("dataset_manifest_ref", "other.json")):
            with self.subTest(key=key):
                before = self.request[key]; self.request[key] = value; self.save()
                with self.assertRaisesRegex(RuntimeError, "REQUEST_DRIFT"): qa.validated_request(self.path)
                self.request[key] = before; self.save()

    def test_no_foreign_path_or_symlink(self):
        for path in (self.root.parent / "import-request.json", self.path.with_name("other.json")):
            with self.assertRaisesRegex(RuntimeError, "SCOPED_REQUEST_REQUIRED"): qa.validated_request(path)
        alias = self.root / "alias/import-request.json"; alias.parent.mkdir(); alias.symlink_to(self.path)
        with self.assertRaisesRegex(RuntimeError, "SCOPED_REQUEST_REQUIRED"): qa.validated_request(alias)


if __name__ == "__main__": unittest.main()
