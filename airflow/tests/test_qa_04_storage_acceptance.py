import copy
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "dags"))
import qa_04_storage_acceptance as q
import pipeline_qa_acceptance as runner


class Qa04GuardTest(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.root = Path(self.folder.name)
        matrix = json.loads((Path(q.__file__).parent / "fixtures/qa_04_failure_recovery_matrix.json").read_text())
        self.matrix = matrix
        (self.root / "matrix.json").write_text(json.dumps(matrix))
        (self.root / "report.json").write_text("{}")
        self.ns = "iceberg.qa04_" + "a" * 32
        owner = {"dag_id": "pipeline_qa_acceptance", "run_id": "qa04-test"}
        self.request = {"namespace": self.ns, "run_id": "qa04-test", "report_path": str(self.root / "report.json"),
                        "matrix_path": str(self.root / "matrix.json"), "lease_owner": owner,
                        "matrix_sha256": q.hashlib.sha256((self.root / "matrix.json").read_bytes()).hexdigest()}
        def record(index, month):
            return {"operation_id": "op" + str(index), "context": {"operation_id": "op" + str(index), "resolver_request": {"target_months": [month]}},
                    "commit": {"tables": {name: {"snapshot_id": index + 1, "count": 1} for name in q.etl.TABLES}},
                    "silver": {"affected_months": [month]}}
        baseline = record(0, "2023-08")
        self.report = {"task": "QA-04", "schema_version": "qa04-storage-acceptance-v1", "status": "PASS",
                       "namespace": self.ns, "run_id": "qa04-test", "matrix_sha256": self.request["matrix_sha256"],
                       "synthetic_fixture": True, "summary": {"case_count": 10, "passed": 10, "failed": 0, "pending_runtime": 0},
                       "outside_month": "2023-08", "outside_before_sha256": "b" * 64, "outside_after_sha256": "b" * 64,
                       "baseline": baseline, "cases": []}
        parent = baseline
        for index, case in enumerate(matrix["cases"][:7], 1):
            id = case["case_id"]
            recovered = record(index, case["scope"][0])
            pins = q.snapshots(parent)
            partial = dict(pins)
            if id == "gold_commit_partial":
                partial["event_current"] = q.snapshots(recovered)["event_current"]
            if id in {"gold_commit_receipt_lost", "trino_verify_failure", "publication_metadata_failure"}:
                partial = q.snapshots(recovered)
            fault = {"context": recovered["context"], "operation_id": recovered["operation_id"],
                     "baseline_snapshots": pins, "partial_snapshots": partial, "fault_stage": case["fault_stage"],
                     "reason_code": case["reason_code"], "published_before_recovery": False, "downstream_blocked": True,
                     "dataset_gate_reason": "DS_GOLD_NOT_PUBLISHED", "publication_rows_before_recovery": 0,
                     "commit_receipt_present_at_failure": False}
            if id == "fetch_timeout":
                fault["fetch"] = {"reason": "HttpTimeoutException", "same_request": True, "request_count": 2, "recovery_http_status": 200}
            if id == "archive_invalid":
                fault.update(rejected_attempt=1, verified_attempt=2, rejected_before_ready=True)
            self.report["cases"].append({"case_id": id, "status": "PASS", "downstream_blocked": True,
                                         "outside_scope_unchanged": True, "recovery_status": "RECOVERED", "fault": fault,
                                         "recovery": recovered, "outside_before_sha256": "b" * 64, "outside_after_sha256": "b" * 64})
            parent = recovered
        self.report["gold_before_ml"] = q.snapshots(parent)
        self.report["gold_after_ml"] = q.snapshots(parent)
        ns = self.ns.replace(".qa04_", ".qa04_ml_")
        def candidate(id):
            return {"namespace": ns, "experiment_status": "CANDIDATE", "experiment_run_id": id, "dataset_id": "ds_fixture",
                    "readback": {"sequence_membership": 2, "sequence_summary": 1, "experiment_run": 1},
                    "result_bundle_sha256": "c" * 64, "membership_snapshot_id": 101, "summary_snapshot_id": 102, "experiment_snapshot_id": 103}
        pins = dict(zip(sorted(q.ML_TABLES), (101, 102, 103)))
        ml = {"namespace": ns, "synthetic_component_qa": True, "scientific_approval": False,
              "outside_experiment_run_id": "exp_outside", "old_snapshots_readable": True,
              "outside_before_sha256": "d" * 64, "outside_after_sha256": "d" * 64, "baseline": candidate("exp_outside"),
              "ml_bundle_invalid": {"rejections": [
                  {"experiment_run_id": "exp_bad_" + kind, "reason": reason, "candidate_rows": 0,
                   "journal": {"experiment_status": "REJECTED"}, "snapshots_before": pins, "snapshots_after": pins}
                  for kind, reason in (("checksum", "IMP_CHECKSUM_MISMATCH"), ("schema", "IMP_SCHEMA_MISMATCH"))],
                  "recovery": candidate("exp_recovered")},
              "ml_import_partial": {"fault_journal": {"experiment_status": "IMPORT_VALIDATING"},
                                    "candidate_journal": candidate("exp_partial"), "candidate_rows_before_recovery": 0,
                                    "membership_snapshot_before_recovery": 101, "recovery": candidate("exp_partial")}}
        self.report["ml"] = ml
        for id in ("ml_bundle_invalid", "ml_import_partial"):
            self.report["cases"].append({"case_id": id, "status": "PASS", "evidence": ml[id]})
        self.report["cases"].append({"case_id": "lease_timeout_contender", "status": "PASS", "timeout_observed": True,
                                    "writer_alive_at_timeout": True, "controlled_child_has_no_io": True, "contender_blocked": True,
                                    "quiescent": True, "release_deferred_to_final_acceptance": True, "foreign_release": "NOT_OWNER",
                                    "owner_held_after_quiescence": owner, "child_exit_code": -15})
        self.sync()

    def sync(self):
        for case in self.report["cases"][:7]:
            (self.root / (case["case_id"] + "-fault.json")).write_text(json.dumps(case["fault"]))
        (self.root / "ml").mkdir(exist_ok=True)
        (self.root / "ml/ml-evidence.json").write_text(json.dumps(self.report["ml"]))
        (self.root / "lease-timeout-evidence.json").write_text(json.dumps(self.report["cases"][-1]))

    def check(self):
        # Independent production receipt validation is covered by PublicationGuardTest.
        with patch.object(runner, "validate_publication") as publication:
            q.validate(self.request, self.report, publication)
            self.assertEqual(publication.call_count, 8)

    def test_complete_metadata_is_accepted(self):
        self.check()

    def test_bool_pass_without_fault_receipts_is_not_enough(self):
        self.report["cases"][0]["fault"] = {}
        with self.assertRaisesRegex(RuntimeError, "BINDING_INVALID"):
            self.check()

    def test_failed_production_publication_validation_is_not_swallowed(self):
        with self.assertRaisesRegex(RuntimeError, "BAD_PUBLICATION"):
            q.validate(self.request, self.report, lambda *args: (_ for _ in ()).throw(RuntimeError("BAD_PUBLICATION")))

    def test_prepared_or_wrong_identity_is_rejected(self):
        self.report["status"] = "PREPARED"
        with self.assertRaisesRegex(RuntimeError, "IDENTITY"):
            self.check()

    def test_missing_case_is_rejected(self):
        self.report["cases"].pop()
        with self.assertRaisesRegex(RuntimeError, "CASES_INCOMPLETE"):
            self.check()

    def test_gold_without_publication_cannot_activate_dataset(self):
        self.report["cases"][4]["fault"]["dataset_gate_reason"] = "ALLOW"
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "BINDING_INVALID"):
            self.check()

    def test_partial_commit_must_show_both_committed_and_missing_tables(self):
        self.report["cases"][3]["fault"]["partial_snapshots"] = q.snapshots(self.report["cases"][3]["recovery"])
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "PARTIAL_COMMIT_NOT_PROVEN"):
            self.check()

    def test_recovery_must_not_recommit_table_already_written(self):
        self.report["cases"][3]["fault"]["partial_snapshots"]["event_current"] = 999
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "RECOMMITTED_TABLE"):
            self.check()

    def test_receipt_lost_requires_six_actual_commits(self):
        self.report["cases"][4]["fault"]["commit_receipt_present_at_failure"] = True
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "LOST_RECEIPT"):
            self.check()

    def test_scope_drift_is_rejected(self):
        self.report["cases"][1]["recovery"]["silver"]["affected_months"] = ["2023-09"]
        with self.assertRaisesRegex(RuntimeError, "BINDING_INVALID"):
            self.check()

    def test_positive_retention_is_required(self):
        self.report["baseline"]["commit"]["tables"]["event_current"]["count"] = 0
        with self.assertRaisesRegex(RuntimeError, "POSITIVE_BASELINE"):
            self.check()

    def test_ml_invalid_bundle_cannot_be_candidate(self):
        self.report["ml"]["ml_bundle_invalid"]["rejections"][0]["journal"]["experiment_status"] = "CANDIDATE"
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "INVALID_BUNDLE_PUBLISHED"):
            self.check()

    def test_rejected_experiment_requires_new_identity_on_recovery(self):
        self.report["ml"]["ml_bundle_invalid"]["recovery"]["experiment_run_id"] = "exp_bad_checksum"
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "PUBLICATION_INVALID"):
            self.check()

    def test_ml_partial_requires_reused_snapshot_not_reappend(self):
        self.report["ml"]["ml_import_partial"]["membership_snapshot_before_recovery"] = 999
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "PARTIAL_RECOVERY"):
            self.check()

    def test_ml_failure_cannot_change_gold(self):
        self.report["gold_after_ml"] = {}
        with self.assertRaisesRegex(RuntimeError, "GOLD_CHANGED"):
            self.check()

    def test_ml_candidate_requires_all_pins_and_exact_readback(self):
        self.report["ml"]["ml_import_partial"]["recovery"]["membership_snapshot_id"] = True
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "PUBLICATION_INVALID"):
            self.check()

    def test_foreign_release_is_not_success(self):
        self.report["cases"][-1]["foreign_release"] = "RELEASED"
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "LEASE_RECOVERY"):
            self.check()

    def test_unquiescent_writer_cannot_pass(self):
        self.report["cases"][-1]["quiescent"] = False
        self.sync()
        with self.assertRaisesRegex(RuntimeError, "LEASE_RECOVERY"):
            self.check()


class Qa04LeaseControlTest(unittest.TestCase):
    def test_real_child_timeout_contender_and_quiescence_keep_exact_owner(self):
        with tempfile.TemporaryDirectory() as folder:
            env = {"SOURCE_GUARD_ROOT": str(Path(folder) / "guard")}
            owner = {"dag_id": "qa04", "run_id": "unit-child"}
            q.lease("acquire", owner, env)
            result = q.timeout_contender(owner, env)
            self.assertTrue(result["quiescent"])
            self.assertTrue(result["contender_blocked"])
            self.assertEqual(result["foreign_release"], "NOT_OWNER")
            self.assertEqual(q.lease("assert", owner, env)["status"], "HELD")

    def test_unknown_controlled_child_outcome_keeps_global_lease(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            env = dict(os.environ, SOURCE_GUARD_ROOT=str(root / "guard"), ICEBERG_CATALOG_NAME="iceberg")
            with patch.object(runner, "STAGING", root), patch.object(runner, "java_identity", side_effect=lambda e, p: e), \
                    patch.object(runner.subprocess, "run", return_value=SimpleNamespace(returncode=0)), \
                    patch.object(runner.qa04, "finish_java", side_effect=q.ProcessOutcomeUnknown("QA04_CONTROLLED_PROCESS_NOT_QUIESCENT")):
                with self.assertRaises(q.ProcessOutcomeUnknown):
                    runner.run("QA-04", env)
            self.assertTrue((root / "guard/owner.json").exists())
            self.assertFalse(list(root.glob("qa-04/*/acceptance.json")))


if __name__ == "__main__":
    unittest.main()
