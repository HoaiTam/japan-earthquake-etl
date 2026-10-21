import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import feature_export_runtime as runtime


class FeatureExportControlTest(unittest.TestCase):
    def setUp(self):
        self.request = {"namespace": "iceberg.mld05_qa_" + "a" * 32, "run_id": "mld05-qa-" + "b" * 32}
        months = [str(2000 + i // 12) + "-" + str(1 + i % 12).zfill(2) for i in range(225)]
        export = {"status": "DATASET_EXPORTED", "dataset_id": "ds_" + "c" * 64, "feature_row_count": 2, "mainshock_count": 1,
                  "reused": False, "registry_snapshot_id": "9007199254740993", "candidate_snapshot_id": "9007199254740995",
                  "mainshock_snapshot_id": "9007199254740997", "dataset_manifest_sha256": "d" * 64,
                  "bundle_manifest_sha256": "e" * 64, "checksums_sha256": "f" * 64}
        self.report = {"task": "MLD-05", "status": "PASS", **self.request, "synthetic_component_qa": True,
                       "research_ready": False, "scientific_approval": False, "required_month_count": 225,
                       "partial_coverage_rejected": True, "trusted_resolver_verified": True,
                       "gold_before_export": {"event_current": 12}, "gold_after_export": {"event_current": 12},
                       "first": export, "rerun": dict(export, reused=True), "partial_dataset": {"dataset_id": "ds_partial"},
                       "publications": [{"context": {"resolver_request": {"target_months": months[i:i + 24]}}} for i in range(0, 225, 24)]}

    def check(self):
        with patch.object(runtime, "validate_publication") as check:
            runtime.validate_qa(self.report, self.request, Path("unused"))
            self.assertEqual(check.call_count, 10)

    def test_ten_receipt_bound_batches_and_exact_reuse_metadata(self):
        self.check()

    def test_exported_resume_requires_preserved_failed_execution_and_full_history_checkpoint(self):
        self.report["first"]["reused"] = True
        self.report["resumed_from_checkpoint"] = True
        with patch.object(runtime, "validate_publication"):
            with tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                with self.assertRaisesRegex(RuntimeError, "UNPROVEN_EXPORT_RESUME"):
                    runtime.validate_qa(self.report, self.request, root)
                (root / "history-complete.json").write_text("{}")
                (root / "execution.json").write_text(json.dumps({"exit_code": 1}))
                runtime.validate_qa(self.report, self.request, root)
                (root / "execution.json").write_text(json.dumps({"exit_code": 0}))
                with self.assertRaisesRegex(RuntimeError, "UNPROVEN_EXPORT_RESUME"):
                    runtime.validate_qa(self.report, self.request, root)

    def test_fake_pass_missing_coverage_or_changed_gold_is_rejected(self):
        for name, value in (("synthetic_component_qa", False), ("research_ready", True), ("partial_coverage_rejected", False), ("gold_after_export", {})):
            original = copy.deepcopy(self.report)
            self.report[name] = value
            with self.assertRaises(RuntimeError):
                self.check()
            self.report = original

    def test_duplicate_months_do_not_make_full_history(self):
        self.report["publications"][-1]["context"]["resolver_request"]["target_months"][0] = "2000-01"
        with self.assertRaisesRegex(RuntimeError, "HISTORY_INCOMPLETE"):
            self.check()

    def test_changed_pin_checksum_and_invalid_long_are_rejected(self):
        for name, value in (("registry_snapshot_id", "9223372036854775808"), ("candidate_snapshot_id", True), ("checksums_sha256", "changed")):
            original = copy.deepcopy(self.report)
            self.report["rerun"][name] = value
            with self.assertRaises(RuntimeError):
                self.check()
            self.report = original

    def test_failed_publication_validator_cannot_be_bypassed(self):
        with patch.object(runtime, "validate_publication", side_effect=RuntimeError("BAD_PUBLICATION")):
            with self.assertRaisesRegex(RuntimeError, "BAD_PUBLICATION"):
                runtime.validate_qa(self.report, self.request, Path("unused"))

    def test_invalid_dataset_rejected_before_any_process(self):
        with patch.object(runtime.subprocess, "run") as process:
            with self.assertRaises(ValueError):
                runtime.run(False, "pilot_bad", "iceberg.ml")
            process.assert_not_called()

    def test_nonzero_retains_whole_run_lease_and_has_no_acceptance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with patch.object(runtime, "STAGING", root / "runs"), patch.dict(os.environ, {"SOURCE_GUARD_ROOT": str(root / "guard")}), \
                    patch.object(runtime, "java_identity", side_effect=lambda env, folder: env), \
                    patch.object(runtime.subprocess, "run", return_value=SimpleNamespace(returncode=1)):
                with self.assertRaisesRegex(RuntimeError, "LEASE_RETAINED"):
                    runtime.run(False, "ds_" + "a" * 64, "iceberg.ml")
            self.assertTrue((root / "guard/owner.json").is_file())
            self.assertEqual(len(list(root.glob("runs/*/execution.json"))), 1)
            self.assertFalse(list(root.glob("runs/*/acceptance.json")))
            self.assertFalse(list(root.glob("runs/*/lease-release.json")))

    def test_timeout_never_releases_or_reports_success(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with patch.object(runtime, "STAGING", root / "runs"), patch.dict(os.environ, {"SOURCE_GUARD_ROOT": str(root / "guard")}), \
                    patch.object(runtime, "java_identity", side_effect=lambda env, folder: env), \
                    patch.object(runtime.subprocess, "run", side_effect=subprocess.TimeoutExpired("controlled", 1)):
                with self.assertRaises(subprocess.TimeoutExpired):
                    runtime.run(False, "ds_" + "a" * 64, "iceberg.ml")
            self.assertTrue((root / "guard/owner.json").is_file())
            self.assertFalse(list(root.glob("runs/*/lease-release.json")))

    def test_resume_requires_checkpoint_and_exact_owned_lease_before_launch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            with patch.object(runtime, "STAGING", root), patch.object(runtime.subprocess, "run") as process:
                with self.assertRaisesRegex(ValueError, "CHECKPOINT_REQUIRED"):
                    runtime.run(True, resume="mld05-qa-" + "a" * 32)
                with self.assertRaisesRegex(ValueError, "EXACT_QA_RESUME_REQUIRED"):
                    runtime.run(True, resume="../escape")
                process.assert_not_called()

    def test_resume_preserves_failed_execution_and_logs_then_releases_after_validation(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp)
            run_id = self.request["run_id"]
            root = base / "runs" / run_id
            root.mkdir(parents=True)
            owner = {"dag_id": "mld05_feature_export", "run_id": run_id}
            request = dict(self.request, lease_owner=owner, dag_id=owner["dag_id"])
            for name, value in (("request.json", request), ("history-complete.json", {}), ("execution.json", {"exit_code": 1})):
                (root / name).write_text(json.dumps(value))
            (root / "stderr.private.log").write_text("original failure retained")
            def completed(*args, **kwargs):
                (root / "runtime-report.json").write_text(json.dumps(self.report))
                return SimpleNamespace(returncode=0)
            with patch.object(runtime, "STAGING", base / "runs"), patch.dict(os.environ, {"SOURCE_GUARD_ROOT": str(base / "guard")}), \
                    patch.object(runtime, "java_identity", side_effect=lambda env, folder: env), \
                    patch.object(runtime.subprocess, "run", side_effect=completed), patch.object(runtime, "validate_publication"):
                runtime.lease("acquire", owner)
                result = runtime.run(True, resume=run_id)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["lease_release"]["status"], "RELEASED")
            self.assertEqual(json.loads((root / "execution.json").read_text()), {"exit_code": 1})
            self.assertEqual((root / "stderr.private.log").read_text(), "original failure retained")
            self.assertEqual(len(list(root.glob("execution-retry-*.json"))), 1)


if __name__ == "__main__":
    unittest.main()
