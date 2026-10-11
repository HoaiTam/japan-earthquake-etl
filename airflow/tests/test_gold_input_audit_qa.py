import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import gold_input_audit_qa as qa
from source_run_guard import lease, SourceRunBusy


class GoldInputAuditControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = {"SOURCE_GUARD_ROOT": str(self.root / "guard"), "ICEBERG_CATALOG_NAME": "iceberg"}
        self.reference = self.root / "silver.json"
        self.reference.write_text(json.dumps({"status": "VERIFIED_BOUNDED_SAMPLE", "attempts": [{"bundle_manifest_sha256": "a" * 64}]}))
        self.runner = self.root / "runner.jar"
        self.runner.write_bytes(b"test-binary")
        for name, value in (("STAGING", self.root / "staging"), ("RUNNER", self.runner)):
            item = patch.object(qa, name, value)
            item.start()
            self.addCleanup(item.stop)

    @property
    def owner(self):
        return Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json"

    def finished(self, command, **kwargs):
        request_path = Path(command[-2])
        request = json.loads(request_path.read_text())
        lease("assert", {key: request[key] for key in ("dag_id", "run_id")}, kwargs["env"])
        self.assertEqual(self.reference, Path(command[-1]))
        self.assertEqual("artifact_" + hashlib.sha256(b"test-binary").hexdigest(), request["code_version"])
        mc_json = '{"applied_mc":2.0}'
        mc_sha = hashlib.sha256(mc_json.encode()).hexdigest()
        pub = "pub_" + "b" * 64
        report = {"status": "VERIFIED_BOUNDED_JMA_PILOT", "scope": "BOUNDED_QA_ONLY", "research_ready": False,
                  "rerun_unchanged": True, "gold_namespace": request["gold_namespace"], "ml_namespace": request["ml_namespace"],
                  "code_version": request["code_version"], "silver_bundle_sha256": "a" * 64,
                  "publication": {"publication_id": pub, "published": True, "verify_status": "PASSED",
                                  "snapshot_bundle": {"event_current": {"table": request["gold_namespace"] + ".event_current", "snapshot_id": 99, "count": 528}}},
                  "datasets": []}
        for split in ("REPRODUCTION", "EXTENSION"):
            manifest = {"dataset_id": "ds_" + ("c" if split == "REPRODUCTION" else "d") * 64,
                        "gold_publication_id": pub, "gold_snapshot_id": 99, "dataset_status": "BUILDING"}
            audit = {**manifest, "dataset_split": split, "input_mode": "PUBLISHED_SNAPSHOT", "candidate_build_allowed": True,
                     "mc_applied": 2.0, "mc_method": "MAXIMUM_CURVATURE",
                     "completeness_overall": {"is_reliable": True, "sample_count": 150},
                     "counts": {"input_event_count": 528, "eligible_event_count": 120, "excluded_event_count": 408},
                     "exclusion_counts_by_primary_reason": {"ML_OUTSIDE_TIME_RANGE": 408},
                     "mc_config_json": mc_json, "mc_config_sha256": mc_sha}
            report["datasets"].append({"manifest": manifest, "audit": audit,
                                       "audited_manifest": {**manifest, **audit["counts"], "mc_value": 2.0, "mc_config_sha256": mc_sha},
                                       "rerun_unchanged": True, "trino_input_count": 528, "trino_eligible_count": 120})
        self.report = report
        (request_path.parent / "mld-02-report.json").write_text(json.dumps(report))
        return subprocess.CompletedProcess(command, 0)

    def test_success_releases_only_own_lease(self):
        with patch.object(qa.subprocess, "run", side_effect=self.finished):
            result = qa.run(self.reference, self.env)
        self.assertEqual("VERIFIED_BOUNDED_JMA_PILOT", result["status"])
        self.assertFalse(self.owner.exists())

    def test_busy_owner_is_not_replaced_or_submitted(self):
        identity = {"dag_id": "another", "run_id": "held"}
        lease("acquire", identity, self.env)
        with patch.object(qa.subprocess, "run") as submit, self.assertRaises(SourceRunBusy):
            qa.run(self.reference, self.env)
        submit.assert_not_called()
        self.assertEqual(identity, json.loads(self.owner.read_text()))

    def test_timeout_and_nonzero_exit_retain_ownership(self):
        for outcome in (subprocess.TimeoutExpired("spark-submit", 1800), subprocess.CompletedProcess([], 1)):
            with self.subTest(outcome=type(outcome).__name__):
                effect = outcome if isinstance(outcome, Exception) else lambda *args, **kwargs: outcome
                with patch.object(qa.subprocess, "run", side_effect=effect), self.assertRaises(Exception):
                    qa.run(self.reference, self.env)
                self.assertTrue(self.owner.exists())
                # Test-only temporary owner cleanup, never part of runtime recovery.
                lease("release", json.loads(self.owner.read_text()), self.env)

    def test_invalid_receipt_retains_ownership(self):
        def corrupted(command, **kwargs):
            result = self.finished(command, **kwargs)
            report_path = Path(command[-2]).parent / "mld-02-report.json"
            self.report["datasets"][0]["audit"]["counts"]["eligible_event_count"] = 121
            report_path.write_text(json.dumps(self.report))
            return result
        with patch.object(qa.subprocess, "run", side_effect=corrupted), self.assertRaises(RuntimeError):
            qa.run(self.reference, self.env)
        self.assertTrue(self.owner.exists())

    def test_unverified_reference_rejected_before_acquisition(self):
        self.reference.write_text('{"status":"fixture"}')
        with patch.object(qa.subprocess, "run") as submit, self.assertRaises(ValueError):
            qa.run(self.reference, self.env)
        submit.assert_not_called()
        self.assertFalse(self.owner.exists())

    def test_unreliable_mc_bad_pin_and_hash_are_rejected(self):
        with patch.object(qa.subprocess, "run", side_effect=self.finished):
            qa.run(self.reference, self.env)
        request = json.loads(next((self.root / "staging").glob("*/request.json")).read_text())
        reference = json.loads(self.reference.read_text())
        for field, value in (("gold_snapshot_id", 100), ("mc_config_sha256", "0" * 64), ("candidate_build_allowed", False),
                             ("mc_applied", float("nan")), ("mc_applied", 3.0)):
            bad = json.loads(json.dumps(self.report))
            bad["datasets"][0]["audit"][field] = value
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                qa.validate_report(bad, request, reference)


if __name__ == "__main__":
    unittest.main()
