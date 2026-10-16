"""Offline control-plane tests for the MLI-03 import handoff."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import mli_03_import_results as mli


class Mli03ImportResultsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        root = Path(self.temporary.name)
        self.env = {
            "MLI_STAGING_ROOT": str(root / "staging"),
            "SOURCE_GUARD_ROOT": str(root / "guard"),
            "MLI_IMPORT_RUNNER_COMMAND": "java -cp runner.jar ie212.earthquake.spark.ml.MlResultImportJob",
        }
        self.request_path = root / "request.json"
        request = {
            "dag_id": "mli_03_import_results",
            "run_id": "mli03-qa-001",
            "experiment_run_id": "exp_fixture",
            "dataset_id": "ds_fixture",
            "dataset_manifest_ref": "registry/ds_fixture.json",
            "dataset_manifest_sha256": "a" * 64,
            "artifact_root": str(root / "bundle"),
            "journal_root": str(root / "journal"),
            "ml_namespace": "iceberg.ml_qa_0123456789abcdef",
            "algorithm_name": "WINDOW",
            "model_config_version": "1",
        }
        self.request_path.write_text(json.dumps(request), encoding="utf-8")

    def report(self, request):
        root = Path(request["journal_root"])
        root.mkdir(parents=True, exist_ok=True)
        (root / "import-report.json").write_text(json.dumps({
            "experiment_run_id": request["experiment_run_id"], "dataset_id": request["dataset_id"],
            "experiment_status": "CANDIDATE",
            "readback": {"sequence_membership": 2, "sequence_summary": 1, "experiment_run": 1},
        }), encoding="utf-8")

    def test_success_requires_candidate_report_and_releases_lease(self):
        def runner(command, **kwargs):
            request = json.loads(Path(command[-1]).read_text(encoding="utf-8"))
            self.report(request)
            return type("Completed", (), {"returncode": 0})()

        with patch.object(mli.subprocess, "run", side_effect=runner):
            result = mli.run(self.request_path, self.env)
        self.assertEqual("CANDIDATE", result["status"])
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_runner_failure_keeps_lease_for_recovery(self):
        completed = type("Completed", (), {"returncode": 1})()
        with patch.object(mli.subprocess, "run", return_value=completed):
            with self.assertRaisesRegex(mli.Mli03Error, "MLI03_IMPORT_RUNNER_FAILED"):
                mli.run(self.request_path, self.env)
        self.assertTrue((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_resolved_context_rejects_request_mutation(self):
        owner = {"dag_id": "mli_03_import_results", "run_id": "manual__airflow-run-a"}
        descriptor = mli.resolve_request_context(self.request_path, owner, self.env)
        pinned = mli._load_resolved_context(descriptor, self.env, owner)
        self.assertEqual("mli03-qa-001", pinned["request"]["run_id"])

        request = json.loads(self.request_path.read_text(encoding="utf-8"))
        request["run_id"] = "mli03-qa-edited"
        self.request_path.write_text(json.dumps(request), encoding="utf-8")
        with self.assertRaisesRegex(mli.Mli03Error, "MLI03_REQUEST_CHANGED_AFTER_RESOLVE"):
            mli._load_resolved_context(descriptor, self.env, owner)

    def test_separate_airflow_runs_have_distinct_lease_owners(self):
        owner_a = {"dag_id": "mli_03_import_results", "run_id": "manual__airflow-run-a"}
        owner_b = {"dag_id": "mli_03_import_results", "run_id": "manual__airflow-run-b"}
        descriptor_a = mli.resolve_request_context(self.request_path, owner_a, self.env)
        descriptor_b = mli.resolve_request_context(self.request_path, owner_b, self.env)
        pinned_a = mli._load_resolved_context(descriptor_a, self.env, owner_a)
        pinned_b = mli._load_resolved_context(descriptor_b, self.env, owner_b)
        self.assertEqual(pinned_a["request"]["run_id"], pinned_b["request"]["run_id"])
        self.assertNotEqual(pinned_a["lease_owner"], pinned_b["lease_owner"])
        self.assertNotEqual(descriptor_a["context_path"], descriptor_b["context_path"])

    def test_release_requires_exact_owner_and_completion_requires_release_receipt(self):
        owner = {"dag_id": "mli_03_import_results", "run_id": "manual__airflow-run-a"}
        with patch.object(mli, "lease", side_effect=[{"status": "HELD", **owner}, {"status": "NOT_OWNER", **owner}]):
            with self.assertRaisesRegex(mli.Mli03Error, "MLI03_LEASE_RELEASE_UNCONFIRMED"):
                mli._release_owned(owner, self.env)

        pinned = {
            "request": {"run_id": "operation-1", "experiment_run_id": "exp_fixture", "dataset_id": "ds_fixture"},
            "lease_owner": owner,
            "request_sha256": "a" * 64,
        }
        result = {"run_id": "operation-1", "experiment_run_id": "exp_fixture", "dataset_id": "ds_fixture",
                  "status": "CANDIDATE", "lease_owner": owner, "request_sha256": "a" * 64}
        with self.assertRaisesRegex(mli.Mli03Error, "MLI03_LEASE_RELEASE_UNCONFIRMED"):
            mli.completion_result(pinned, result, None)
        complete = mli.completion_result(pinned, result, {"status": "RELEASED", **owner})
        self.assertEqual("COMPLETE", complete["status"])

    def test_caller_cannot_supply_internal_lease_identity(self):
        request = json.loads(self.request_path.read_text(encoding="utf-8"))
        request["_mli03_lease_owner"] = {"dag_id": "fake", "run_id": "fake"}
        self.request_path.write_text(json.dumps(request), encoding="utf-8")
        with self.assertRaisesRegex(mli.Mli03Error, "MLI03_RESERVED_CONTEXT_FIELD"):
            mli.run(self.request_path, self.env)

    def test_pilot_and_non_exported_requests_fail_before_runner(self):
        request = json.loads(self.request_path.read_text(encoding="utf-8"))
        request["dataset_status"] = "DEVELOPMENT_PILOT"
        self.request_path.write_text(json.dumps(request), encoding="utf-8")
        with patch.object(mli.subprocess, "run") as runner:
            with self.assertRaisesRegex(mli.Mli03Error, "MLI03_CALLER_LINEAGE_NOT_ALLOWED"):
                mli.run(self.request_path, self.env)
        runner.assert_not_called()


if __name__ == "__main__":
    unittest.main()
