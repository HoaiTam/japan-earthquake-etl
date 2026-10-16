"""Offline QA-04 failure/recovery tests over bounded synthetic state."""

import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import qa_04_failure_recovery as qa


class Qa04FailureRecoveryTest(unittest.TestCase):
    def test_matrix_is_complete_and_all_offline_cases_recover(self):
        matrix = qa.load_matrix()
        self.assertEqual(10, len(matrix["cases"]))
        with tempfile.TemporaryDirectory() as root:
            reports = [qa.run_offline_case(item["case_id"], root) for item in matrix["cases"]]
        self.assertTrue(all(report["status"] == "PASS" for report in reports))
        self.assertTrue(all(report["recovery"]["outside_scope_unchanged"] for report in reports))
        self.assertTrue(all(report["mutations"]["deletions"] == [] for report in reports))
        self.assertTrue(all("secret" not in json.dumps(report).lower() for report in reports))

    def test_downstream_gates_and_gold_snapshot_recovery(self):
        fetch = qa.run_offline_case("fetch_timeout")
        self.assertNotIn("SILVER", fetch["fault_observation"]["downstream_calls"])
        archive = qa.run_offline_case("archive_invalid")
        self.assertNotIn("SILVER", archive["fault_observation"]["downstream_calls"])
        silver = qa.run_offline_case("silver_failure")
        self.assertNotIn("GOLD", silver["fault_observation"]["downstream_calls"])

        partial = qa.run_offline_case("gold_commit_partial")
        self.assertEqual(6, len(partial["gold"]["snapshots"]))
        self.assertFalse(partial["fault_observation"]["published"])
        self.assertTrue(partial["gold"]["published"])
        receipt = qa.run_offline_case("gold_commit_receipt_lost")
        self.assertTrue(receipt["gold"]["receipt_present"])
        self.assertEqual(6, len(set(receipt["gold"]["snapshots"].values())))

        trino = qa.run_offline_case("trino_verify_failure")
        self.assertTrue(trino["gold"]["trino_verified"])
        publication = qa.run_offline_case("publication_metadata_failure")
        self.assertTrue(publication["gold"]["published"])

    def test_ml_rejection_never_reuses_rejected_run(self):
        invalid = qa.run_offline_case("ml_bundle_invalid")
        self.assertEqual("REJECTED", invalid["ml"]["original_status"])
        self.assertFalse(invalid["ml"]["original_candidate"])
        self.assertTrue(invalid["ml"]["replacement_candidate"])
        partial = qa.run_offline_case("ml_import_partial")
        self.assertEqual("CANDIDATE", partial["ml"]["original_status"])
        self.assertTrue(partial["ml"]["original_candidate"])

    def test_lease_contender_is_blocked_until_quiescent(self):
        report = qa.run_offline_case("lease_timeout_contender")
        self.assertTrue(report["lease"]["contender_blocked"])
        self.assertTrue(report["lease"]["writer_quiescent"])
        self.assertTrue(report["lease"]["released"])

    def test_runtime_requires_dependencies_and_does_not_launch_when_pending(self):
        with tempfile.TemporaryDirectory() as root:
            request = Path(root) / "request.json"
            request.write_text(json.dumps({
                "task_id": "QA-04", "mode": "runtime", "case_ids": ["fetch_timeout"],
                "namespace": "iceberg.qa04_0123456789abcdef", "run_id": "qa04-runtime-001",
                "operation_id": "op-qa04-runtime-001", "scope_sha256": "a" * 64,
                "input_pins": ["s3://qa/input"], "baseline_snapshots": {"event_current": 41},
                "artifact_refs": ["s3://qa/artifact"], "scope": ["2023-09"],
                "report_path": str(Path(root) / "report.json")
            }), encoding="utf-8")
            with patch.object(qa.subprocess, "run") as runner:
                with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_DEPENDENCY_PENDING"):
                    qa.run_runtime(request, {"ORC_07_STATUS": "Ready", "MLI_03_STATUS": "In Progress"})
            runner.assert_not_called()


if __name__ == "__main__":
    unittest.main()
