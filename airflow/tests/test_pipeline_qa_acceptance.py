import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import pipeline_qa_acceptance as qa


class PipelineAcceptanceTest(unittest.TestCase):
    def test_smoke_make_target_never_builds_pulls_or_starts_dependencies(self):
        root = Path(__file__).resolve().parents[2]
        result = qa.subprocess.run(
            ["make", "--dry-run", "smoke-qa-acceptance", "PIPELINE_QA_TASK=QA-02"],
            cwd=root, capture_output=True, text=True, check=True)
        self.assertIn("--component ml --client pipeline-qa-acceptance", result.stdout)
        self.assertIn("run --rm --no-deps --pull never", result.stdout)
        self.assertIn('PIPELINE_QA_TASK="QA-02"', result.stdout)
        for forbidden in ("docker-build", " up ", " start ", " pull ", " init ", "down -v"):
            self.assertNotIn(forbidden, result.stdout)

    def test_foreign_lease_blocks_launch_and_is_not_released(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")
            foreign = {"dag_id": "daily_etl", "run_id": "foreign-owner"}
            qa.lease("acquire", foreign, env)
            with patch.object(qa, "STAGING", root), patch.object(qa.subprocess, "run") as execute:
                with self.assertRaisesRegex(RuntimeError, "SOURCE_RUN_BUSY"):
                    qa.run("QA-02", env)
            execute.assert_not_called()
            self.assertEqual(json.loads((root / "guard/owner.json").read_text()), foreign)

    def test_only_qa02_is_available_in_this_delivery(self):
        self.assertEqual(set(qa.JOBS), {"QA-02"})
        for task in ("QA-03", "QA-04"):
            with self.subTest(task=task), patch.object(qa, "lease") as lease:
                with self.assertRaisesRegex(ValueError, "UNSUPPORTED"):
                    qa.run(task, {})
                lease.assert_not_called()

    def test_matrix_changed_after_execution_is_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / "matrix.json").write_text("{}")
            request = {"matrix_path": str(root / "matrix.json"), "matrix_sha256": "0" * 64}
            with self.assertRaisesRegex(RuntimeError, "MATRIX_CHANGED_AFTER_EXECUTION"):
                qa.validate("QA-02", request, {})

    def test_validation_failure_after_successful_java_releases_only_own_lease(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")

            def execute(command, **kwargs):
                request = json.loads(Path(command[-1]).read_text())
                qa._atomic_json(Path(request["report_path"]), {})
                return SimpleNamespace(returncode=0)

            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", side_effect=execute), \
                    patch.object(qa, "validate", side_effect=RuntimeError("INVALID_REPORT")):
                with self.assertRaisesRegex(RuntimeError, "INVALID_REPORT"):
                    qa.run("QA-02", env)
            self.assertFalse((root / "guard/owner.json").exists())
            self.assertEqual(json.loads(next(root.glob("qa-02/*/lease-release.json")).read_text())["status"], "RELEASED")
            self.assertFalse(list(root.glob("qa-02/*/acceptance.json")))

    def test_acceptance_is_written_only_after_exact_owner_release(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")

            def execute(command, **kwargs):
                request = json.loads(Path(command[-1]).read_text())
                qa._atomic_json(Path(request["report_path"]), {"summary": {"case_count": 8, "passed": 8, "failed": 0}})
                return SimpleNamespace(returncode=0)

            # Control-flow unit test only: publication validation is covered below
            # and by the opt-in runtime command, not mocked acceptance evidence.
            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", side_effect=execute), patch.object(qa, "validate"):
                result = qa.run("QA-02", env)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["lease_release"]["run_id"], result["run_id"])
            self.assertFalse((root / "guard/owner.json").exists())
            self.assertEqual(json.loads(next(root.glob("qa-02/*/acceptance.json")).read_text()), result)

    def test_postrun_validator_requires_existing_output_not_prerun_nonexistence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            matrix = json.loads((Path(qa.__file__).parent / "fixtures/qa_02_rerun_revision_matrix.json").read_text())
            (root / "matrix.json").write_text(json.dumps(matrix))
            (root / "report.json").write_text("{}")
            request = {"namespace": "iceberg.qa02_" + "a" * 32, "matrix_path": str(root / "matrix.json"), "report_path": str(root / "report.json"),
                       "matrix_sha256": qa.hashlib.sha256((root / "matrix.json").read_bytes()).hexdigest()}
            with patch.object(qa.qa02, "_validate_runtime_request", side_effect=AssertionError("pre-run guard used after run")), \
                    patch.object(qa.qa02, "_validate_runtime_report") as validate:
                qa.validate("QA-02", request, {"cases": []})
                validate.assert_called_once()

    def test_unknown_task_cannot_acquire_lease_or_spawn(self):
        with patch.object(qa, "lease") as lease, patch.object(qa.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "UNSUPPORTED"):
                qa.run("main", {})
            lease.assert_not_called()
            run.assert_not_called()

    def test_nonzero_java_exit_keeps_lease_and_never_writes_acceptance(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")
            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", return_value=SimpleNamespace(returncode=2)):
                with self.assertRaisesRegex(RuntimeError, "FAILED_CHECK_SCOPED"):
                    qa.run("QA-02", env)
            owner = json.loads((root / "guard/owner.json").read_text())
            self.assertTrue(owner["run_id"].startswith("qa02-runtime-"))
            self.assertFalse(list(root.glob("qa-02/*/acceptance.json")))
            self.assertEqual(json.loads(next(root.glob("qa-02/*/failure.json")).read_text())["status"], "FAILED_LEASE_RETAINED")

    def test_java_timeout_keeps_exact_lease(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")
            with patch.object(qa, "STAGING", root), patch.object(qa, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(qa.subprocess, "run", side_effect=qa.subprocess.TimeoutExpired("fixed-java", 3600)):
                with self.assertRaisesRegex(RuntimeError, "TIMEOUT_CHECK_PROCESS"):
                    qa.run("QA-02", env)
            self.assertTrue((root / "guard/owner.json").is_file())
            self.assertFalse(list(root.glob("qa-02/*/acceptance.json")))



class PublicationGuardTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        self.namespace = "iceberg.qa02_" + "a" * 32
        self.operation = "qa-operation"
        work = self.root / self.operation
        work.mkdir()
        receipts = []
        for phase in qa.etl.PHASES:
            artifact = {"phase": phase}
            (work / (phase + ".json")).write_text(json.dumps(artifact))
            receipts.append({"artifact_sha256": qa.etl.digest(artifact)})
        tables = {name: {"table": self.namespace + "." + name, "snapshot_id": 4691854184265549327, "count": 1}
                  for name in sorted(qa.etl.TABLES)}
        commit = {"namespace": self.namespace, "gold_run_id": self.operation, "tables": tables,
                  "status": "COMMITTED", "published": False, "identity_sha256": "b" * 64}
        checks = {name: {"actual": 0, "expected": 0} for name in qa.GOLD_BLOCKER_CHECKS}
        checks.update({kind + ":" + name: {"actual": 1, "expected": 1}
                       for name in tables for kind in ("count", "identity")})
        self.record = {
            "context": {"resolver_request": {"namespace": self.namespace}, "operation_id": self.operation},
            "operation_id": self.operation, "receipts": receipts, "commit": commit,
            "silver": {"reconciliation": {"balanced": True}},
            "publication": {"engine": "TRINO", "verify_status": "PASSED", "published": True,
                            "gold_run_id": self.operation, "namespace": self.namespace,
                            "identity_sha256": "b" * 64, "snapshot_bundle": tables, "checks": checks},
        }
        self.repin()
        # Existing production context/receipt contracts have their own tests.
        # These tests exercise the additional artifact/snapshot/Trino guards.
        for name in ("validate_context", "validate_receipt"):
            mock = patch.object(qa.etl, name)
            mock.start()
            self.addCleanup(mock.stop)

    def repin(self):
        digest = qa.hashlib.sha256(json.dumps(self.record["commit"], separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        self.record["publication"].update(bundle_sha256=digest, publication_id="pub_" + digest)
        for phase, field in (("silver", "silver"), ("gold", "commit"), ("publish", "publication")):
            artifact = self.record[field]
            (self.root / self.operation / (phase + ".json")).write_text(json.dumps(artifact))
            self.record["receipts"][qa.etl.PHASES.index(phase)]["artifact_sha256"] = qa.etl.digest(artifact)
        snapshots = {name: pin["snapshot_id"] for name, pin in self.record["commit"]["tables"].items()}
        for receipt in self.record["receipts"][3:]:
            receipt["snapshots"] = snapshots
        self.record["receipts"][-1]["publication_id"] = "pub_" + digest

    def check(self):
        qa.validate_publication(self.record, self.namespace, self.root)

    def test_six_exact_snapshots_and_all_trino_checks_are_accepted(self):
        self.check()

    def test_missing_phase_is_rejected(self):
        self.record["receipts"].pop()
        with self.assertRaisesRegex(RuntimeError, "PHASES_INCOMPLETE"):
            self.check()

    def test_changed_phase_artifact_is_rejected(self):
        (self.root / self.operation / (qa.etl.PHASES[0] + ".json")).write_text("{}")
        with self.assertRaisesRegex(RuntimeError, "ARTIFACT_CHANGED"):
            self.check()

    def test_missing_uniqueness_gate_is_rejected(self):
        del self.record["publication"]["checks"]["unique:event_current"]
        with self.assertRaisesRegex(RuntimeError, "PIN_OR_CHECKS_MISSING"):
            self.check()

    def test_different_published_bundle_is_rejected(self):
        self.record["publication"]["snapshot_bundle"] = {}
        with self.assertRaisesRegex(RuntimeError, "PUBLICATION_INVALID"):
            self.check()

    def test_forged_commit_hash_is_rejected(self):
        self.record["publication"]["bundle_sha256"] = "0" * 64
        with self.assertRaisesRegex(RuntimeError, "PIN_OR_CHECKS_MISSING"):
            self.check()

    def test_boolean_or_out_of_range_snapshot_is_rejected(self):
        for value in (True, 0, 9223372036854775808, "latest"):
            with self.subTest(value=value):
                self.record["commit"]["tables"]["event_current"]["snapshot_id"] = value
                self.repin()
                with self.assertRaisesRegex(RuntimeError, "READBACK_INVALID"):
                    self.check()

    def test_trino_count_mismatch_is_rejected(self):
        self.record["publication"]["checks"]["count:event_current"]["actual"] = 2
        with self.assertRaisesRegex(RuntimeError, "READBACK_INVALID"):
            self.check()

    def test_nonzero_blocker_cannot_pass_by_changing_expected_value(self):
        self.record["publication"]["checks"]["required_fields"] = {"actual": 1, "expected": 1}
        with self.assertRaisesRegex(RuntimeError, "BLOCKER_EXPECTATION_CHANGED"):
            self.check()

    def test_unbalanced_silver_is_rejected(self):
        self.record["silver"]["reconciliation"]["balanced"] = False
        with self.assertRaisesRegex(RuntimeError, "COUNTS_UNBALANCED"):
            self.check()

    def test_record_cannot_substitute_another_valid_phase_artifact(self):
        self.record["silver"]["unbound_field"] = "changed"
        with self.assertRaisesRegex(RuntimeError, "RECORD_ARTIFACT_CHANGED"):
            self.check()

    def test_receipt_snapshots_must_equal_exact_commit(self):
        self.record["receipts"][3]["snapshots"] = {}
        with self.assertRaisesRegex(RuntimeError, "RECEIPT_SNAPSHOT_CHANGED"):
            self.check()

    def test_receipt_publication_id_must_equal_verified_publication(self):
        self.record["receipts"][-1]["publication_id"] = "pub_" + "0" * 64
        with self.assertRaisesRegex(RuntimeError, "RECEIPT_PUBLICATION_CHANGED"):
            self.check()


if __name__ == "__main__":
    unittest.main()
