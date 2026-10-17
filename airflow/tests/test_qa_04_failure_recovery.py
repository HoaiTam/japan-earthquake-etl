"""Offline QA-04 failure/recovery tests over bounded synthetic state."""

import json
import hashlib
import os
from pathlib import Path
import re
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import qa_04_failure_recovery as qa


EXPECTED_GOLD_TABLES = (
    "event_current", "event_source_bridge", "dim_date", "dim_region",
    "dim_magnitude_band", "dim_depth_band",
)


class Qa04FailureRecoveryTest(unittest.TestCase):
    def _edit_evidence(self, case_result, mutate):
        evidence_path = Path(case_result["evidence"]["path"])
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
        mutate(evidence)
        evidence_bytes = json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        evidence_path.write_bytes(evidence_bytes)
        case_result["evidence"]["sha256"] = hashlib.sha256(evidence_bytes).hexdigest()

    def _validate_fixture(self, root, request, report, matrix):
        qa._validate_runtime_report(
            report, request, hashlib.sha256(Path(root, "request.json").read_bytes()).hexdigest(),
            matrix, hashlib.sha256(qa.MATRIX_PATH.read_bytes()).hexdigest(), Path(root).resolve())

    def _runtime_fixture(self, root, case_ids=None):
        runner_path = Path(root, "qa04-runtime-runner")
        runner_bytes = b"qa04 synthetic runner fixture v1\n"
        runner_path.write_bytes(runner_bytes)
        runner_sha = hashlib.sha256(runner_bytes).hexdigest()
        matrix_bytes = qa.MATRIX_PATH.read_bytes()
        matrix = json.loads(matrix_bytes)
        matrix_sha = hashlib.sha256(matrix_bytes).hexdigest()
        case_ids = case_ids or [item["case_id"] for item in matrix["cases"]]
        scopes = {}
        for case_id in case_ids:
            item = qa._case(matrix, case_id)
            scope = {"scope_tokens": item["scope"]}
            if case_id.startswith("ml_"):
                scope["experiment_run_ids"] = ["exp-" + case_id]
                scope["dataset_refs"] = [{"kind": "dataset-manifest", "reference": "s3://qa04/dataset.json", "sha256": "1" * 64}]
                scope["gold_publication_refs"] = [{"kind": "gold-publication", "reference": "s3://qa04/publication.json", "sha256": "2" * 64}]
            else:
                month = item["scope"][0]
                scope["write_months"] = [month]
                scope["window_start_utc"], scope["window_end_utc"] = qa._month_window(month)
            scopes[case_id] = scope
        request = {
            "schema_version": qa.RUNTIME_REQUEST_VERSION, "task_id": "QA-04", "mode": "runtime",
            "case_ids": case_ids, "matrix_sha256": matrix_sha, "run_id": "qa04-runtime-001",
            "operation_id": "op-qa04-runtime-001", "namespace": "iceberg.qa04_fixture",
            "case_scopes": scopes, "scope_sha256": qa._sha256(scopes),
            "lease_owner": {"dag_id": "qa_04_failure_recovery", "run_id": "scheduled__qa04-runtime-001"},
            "input_pins": [{"kind": "bronze-manifest", "reference": "s3://qa04/manifest.json", "sha256": "a" * 64}],
            "artifact_refs": [{"kind": "bundle", "reference": "s3://qa04/bundle.json", "sha256": "b" * 64},
                              {"kind": "qa04-runtime-runner", "reference": runner_path.as_posix(), "sha256": runner_sha}],
            "baseline_snapshots": [{"table": "iceberg.qa04_fixture.event_current", "state": "PRESENT", "snapshot_id": 41}],
            "report_path": str(Path(root) / "report.json"),
        }
        request_bytes = json.dumps(request, sort_keys=True, separators=(",", ":")).encode()
        request_sha = hashlib.sha256(request_bytes).hexdigest()
        Path(root, "request.json").write_bytes(request_bytes)
        report_cases = []
        for case_id in case_ids:
            item = qa._case(matrix, case_id)
            scope_hash = qa._sha256(scopes[case_id])
            base_snapshot = {"table": "iceberg.qa04_fixture.event_current", "state": "PRESENT", "snapshot_id": 41}
            snapshots = [base_snapshot]
            outside_month = "exp-outside-qa04" if case_id.startswith("ml_") else "2023-08"
            if not case_id.startswith("ml_"):
                year, month = (int(part) for part in item["scope"][0].split("-"))
                outside_month = f"{year - 1:04d}-12" if month == 1 else f"{year:04d}-{month - 1:02d}"
            evidence = {
                "producer": "qa04-runtime-adapter", "schema_version": qa.RUNTIME_REPORT_VERSION,
                "request_sha256": request_sha, "matrix_sha256": matrix_sha, "run_id": request["run_id"],
                "operation_id": request["operation_id"], "case_id": case_id, "scope_sha256": scope_hash,
                "lease_owner": request["lease_owner"], "runner_sha256": runner_sha,
                "fault": {"stage": item["fault_stage"], "reason_code": item["reason_code"], "observed": True},
                "downstream_blocked": True, "recovery_status": "RECOVERED",
                "snapshots_before": snapshots, "snapshots_after": snapshots,
                "readback": {"status": "PASSED", "snapshots": snapshots, "prior_snapshots_readable": True},
                "outside_scope": {"before": {"partition": outside_month, "count": 1,
                                                 "keys_sha256": "c" * 64, "content_sha256": "d" * 64},
                                  "after": {"partition": outside_month, "count": 1,
                                                "keys_sha256": "c" * 64, "content_sha256": "d" * 64}},
                "downstream_calls": [item["fault_stage"]] if item["fault_stage"] in qa.ETL_STAGES or item["fault_stage"] == "ML_IMPORT" else [],
            }
            if case_id in {"gold_commit_partial", "gold_commit_receipt_lost", "trino_verify_failure", "publication_metadata_failure"}:
                fingerprint = "d" * 64
                commit_sha = "e" * 64
                evidence["gold"] = {
                    "operation_id": request["operation_id"], "fingerprint_sha256": fingerprint,
                    "commit_sha256": commit_sha, "published_before_recovery": False,
                    "tables": [{"name": name, "snapshot_id": index + 100} for index, name in enumerate(EXPECTED_GOLD_TABLES)],
                    "counts": {name: 1 for name in EXPECTED_GOLD_TABLES},
                    "publication": {"engine": "TRINO", "verify_status": "PASSED", "published": True,
                                    "bundle_sha256": commit_sha, "scope_sha256": scope_hash,
                                    "table_snapshots": {name: index + 100 for index, name in enumerate(EXPECTED_GOLD_TABLES)},
                                    "trino_checks": {"six_table_snapshot_readback": "PASSED",
                                                     "count_reconciliation": "PASSED",
                                                     "outside_scope_unchanged": "PASSED"}},
                }
                committed = [{"table": "iceberg.qa04_fixture." + name, "state": "PRESENT", "snapshot_id": index + 100}
                             for index, name in enumerate(EXPECTED_GOLD_TABLES)]
                before = [{"table": "iceberg.qa04_fixture." + name, "state": "PRESENT", "snapshot_id": 41}
                          for name in EXPECTED_GOLD_TABLES]
                evidence["snapshots_before"] = before
                evidence["snapshots_after"] = committed
                evidence["readback"]["snapshots"] = committed
            if case_id == "ml_bundle_invalid":
                rejected = scopes[case_id]["experiment_run_ids"][0]
                evidence["ml"] = {"operation_id": request["operation_id"], "fingerprint_sha256": "f" * 64,
                                   "original_status": "REJECTED", "original_candidate": False,
                                   "rejected_experiment_run_ids": [rejected],
                                   "replacement_experiment_run_id": rejected + "-new",
                                   "dataset_refs": scopes[case_id]["dataset_refs"],
                                   "gold_publication_refs": scopes[case_id]["gold_publication_refs"]}
            elif case_id == "ml_import_partial":
                evidence["ml"] = {"operation_id": request["operation_id"], "fingerprint_sha256": "f" * 64,
                                   "status": "CANDIDATE", "candidate": True, "resume_same_fingerprint": True,
                                   "journal_status": "CANDIDATE", "readback_status": "PASSED",
                                   "publication_fingerprint_sha256": "f" * 64,
                                   "dataset_refs": scopes[case_id]["dataset_refs"],
                                   "gold_publication_refs": scopes[case_id]["gold_publication_refs"],
                                   "snapshots": [{"table": "iceberg.qa04_fixture." + name, "state": "PRESENT", "snapshot_id": 201 + index}
                                                 for index, name in enumerate(("sequence_membership", "sequence_summary", "experiment_run"))]}
                evidence["snapshots_after"] = [base_snapshot, *evidence["ml"]["snapshots"]]
                evidence["readback"]["snapshots"] = evidence["snapshots_after"]
            if case_id == "lease_timeout_contender":
                evidence["lease"] = {"owner_retained_before_quiescence": True, "contender_blocked": True,
                                     "writer_quiescent_before_release": True, "release_status": "RELEASED",
                                     "release_owner_matches": True, "owner": request["lease_owner"]}
            evidence_path = Path(root, "evidence", case_id + ".json")
            evidence_path.parent.mkdir(parents=True, exist_ok=True)
            evidence_bytes = json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
            evidence_path.write_bytes(evidence_bytes)
            report_cases.append({"case_id": case_id, "status": "PASS",
                                 "fault": evidence["fault"], "downstream_blocked": True,
                                 "recovery_status": "RECOVERED",
                                 "evidence": {"path": str(evidence_path), "sha256": hashlib.sha256(evidence_bytes).hexdigest()}})
        report = {
            "task": "QA-04", "schema_version": qa.RUNTIME_REPORT_VERSION, "mode": "runtime", "status": "PASS",
            "request_sha256": request_sha, "matrix_sha256": matrix_sha, "run_id": request["run_id"],
            "operation_id": request["operation_id"], "namespace": request["namespace"],
            "scope_sha256": request["scope_sha256"], "cases": report_cases,
            "lease_owner": request["lease_owner"], "runner_sha256": runner_sha,
            "summary": {"total": len(case_ids), "passed": len(case_ids), "failed": 0, "pending": 0},
        }
        Path(root, "report.json").write_text(json.dumps(report), encoding="utf-8")
        return request, report, matrix

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

    def test_interruption_before_quiescence_keeps_exact_owner_and_state(self):
        with tempfile.TemporaryDirectory() as root:
            lease_root = Path(root) / "case"
            lease_root.mkdir()
            env = {"SOURCE_GUARD_ROOT": str(lease_root / "guard")}
            identity = {"dag_id": "qa_04_failure_recovery", "run_id": "qa04-interrupted"}
            qa.lease("acquire", identity, env)
            state = {"writer_quiescent": False, "lease_released": False}
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_WRITER_NOT_QUIESCENT"):
                qa._release_exact_lease(lease_root, state, identity, env)
            owner = json.loads((lease_root / "guard" / "owner.json").read_text())
            self.assertEqual(identity, owner)
            self.assertFalse(state["lease_released"])
            contender = {"dag_id": identity["dag_id"], "run_id": "qa04-contender"}
            with self.assertRaises(qa.SourceRunBusy):
                qa.lease("acquire", contender, env)
            state["writer_quiescent"] = True
            qa._release_exact_lease(lease_root, state, identity, env)
            self.assertTrue(state["lease_released"])
            self.assertFalse((lease_root / "guard" / "owner.json").exists())

    def test_offline_interruption_does_not_release_owner_or_delete_recovery_state(self):
        with tempfile.TemporaryDirectory() as root:
            with patch.object(qa, "_recover_lease", side_effect=KeyboardInterrupt):
                with self.assertRaises(KeyboardInterrupt):
                    qa.run_offline_case("lease_timeout_contender", root)
            case_root = Path(root) / "lease_timeout_contender"
            state = json.loads((case_root / "state.json").read_text())
            owner = json.loads((case_root / "guard" / "owner.json").read_text())
            self.assertFalse(state["writer_quiescent"])
            self.assertFalse(state["lease_released"])
            self.assertEqual({"dag_id": "qa_04_failure_recovery", "run_id": "qa04-lease_timeout_contender-001"}, owner)

    def test_failed_or_wrong_owner_release_never_marks_lease_released(self):
        with tempfile.TemporaryDirectory() as root:
            lease_root = Path(root)
            env = {"SOURCE_GUARD_ROOT": str(lease_root / "guard")}
            owner = {"dag_id": "qa_04_failure_recovery", "run_id": "qa04-release-check"}
            qa.lease("acquire", owner, env)
            state = {"writer_quiescent": True, "lease_released": False}
            real_lease = qa.lease

            def wrong_release(action, identity, environment):
                if action == "release":
                    return {"status": "NOT_OWNER", **identity}
                return real_lease(action, identity, environment)

            with patch.object(qa, "lease", side_effect=wrong_release):
                with self.assertRaisesRegex(qa.Qa04Error, "QA04_LEASE_RELEASE_UNCONFIRMED"):
                    qa._release_exact_lease(lease_root, state, owner, env)
            self.assertFalse(state["lease_released"])
            self.assertTrue((lease_root / "guard" / "owner.json").exists())

    def test_runtime_report_positive_fixture_covers_all_ten_cases(self):
        with tempfile.TemporaryDirectory() as root:
            request, report, matrix = self._runtime_fixture(root)
            staging = Path(root).resolve()
            request_sha = hashlib.sha256(Path(root, "request.json").read_bytes()).hexdigest()
            matrix_sha = hashlib.sha256(qa.MATRIX_PATH.read_bytes()).hexdigest()
            qa._validate_runtime_request(request, matrix)
            qa._validate_runtime_report(report, request, request_sha, matrix, matrix_sha, staging)

    def test_gold_table_names_match_independent_writer_contract(self):
        self.assertEqual(EXPECTED_GOLD_TABLES, qa.GOLD_TABLES)
        repo_root = Path(__file__).parents[2]
        writer_source = (repo_root / "spark/src/main/java/ie212/earthquake/spark/gold/GoldIcebergWriter.java").read_text(encoding="utf-8")
        declaration = re.search(r"public static final List<String> TABLES = List\.of\((.*?)\);", writer_source, re.DOTALL)
        self.assertIsNotNone(declaration)
        self.assertEqual(EXPECTED_GOLD_TABLES, tuple(re.findall(r'"([^\"]+)"', declaration.group(1))))

    def test_gold_rejects_legacy_alias_and_missing_table(self):
        with tempfile.TemporaryDirectory() as root:
            def rename_legacy(evidence):
                def replace(value):
                    if isinstance(value, str):
                        return value.replace("dim_magnitude_band", "magnitude_band").replace("dim_depth_band", "depth_band")
                    if isinstance(value, list):
                        return [replace(item) for item in value]
                    if isinstance(value, dict):
                        return {replace(key): replace(item) for key, item in value.items()}
                    return value
                replaced = replace(evidence)
                evidence.clear()
                evidence.update(replaced)

            request, report, matrix = self._runtime_fixture(root, ["gold_commit_partial"])
            case_result = report["cases"][0]
            self._edit_evidence(case_result, rename_legacy)
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_GOLD_EVIDENCE_INVALID"):
                self._validate_fixture(root, request, report, matrix)

            request, report, matrix = self._runtime_fixture(root, ["gold_commit_partial"])
            case_result = report["cases"][0]
            self._edit_evidence(case_result, lambda evidence: evidence["gold"]["tables"].pop())
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_GOLD_EVIDENCE_INVALID"):
                self._validate_fixture(root, request, report, matrix)

    def test_ml_recovery_requires_three_present_matching_snapshots(self):
        invalid_cases = ("all_absent", "one_absent", "missing_table", "duplicate", "duplicate_absent", "wrong_namespace", "after_drift", "readback_drift",
                         "bad_id_true", "bad_id_zero", "bad_id_negative", "bad_id_overflow")
        for invalid_case in invalid_cases:
            with self.subTest(invalid_case=invalid_case), tempfile.TemporaryDirectory() as root:
                request, report, matrix = self._runtime_fixture(root, ["ml_import_partial"])
                case_result = report["cases"][0]

                def mutate(evidence):
                    ml_refs = evidence["ml"]["snapshots"]
                    ml_tables = {row["table"] for row in ml_refs}
                    after = evidence["snapshots_after"]
                    readback = evidence["readback"]["snapshots"]
                    if invalid_case == "all_absent":
                        evidence["ml"]["snapshots"] = [{"table": row["table"], "state": "ABSENT"} for row in ml_refs]
                        evidence["snapshots_after"] = [row for row in after if row["table"] not in ml_tables] + evidence["ml"]["snapshots"]
                        evidence["readback"]["snapshots"] = list(evidence["snapshots_after"])
                    elif invalid_case == "one_absent":
                        absent = {"table": ml_refs[0]["table"], "state": "ABSENT"}
                        evidence["ml"]["snapshots"][0] = absent
                        evidence["snapshots_after"] = [row for row in after if row["table"] != absent["table"]] + [absent]
                        evidence["readback"]["snapshots"] = list(evidence["snapshots_after"])
                    elif invalid_case == "missing_table":
                        evidence["ml"]["snapshots"].pop()
                    elif invalid_case == "duplicate":
                        evidence["ml"]["snapshots"].append(dict(ml_refs[0], snapshot_id=999))
                    elif invalid_case == "duplicate_absent":
                        absent = {"table": ml_refs[0]["table"], "state": "ABSENT"}
                        evidence["ml"]["snapshots"].append(absent)
                        evidence["snapshots_after"].append(absent)
                        evidence["readback"]["snapshots"].append(absent)
                    elif invalid_case == "wrong_namespace":
                        original_table = ml_refs[0]["table"]
                        for key, refs in (("ml", ml_refs), ("after", after), ("readback", readback)):
                            if key == "ml":
                                refs[0]["table"] = "iceberg.other." + original_table.rsplit(".", 1)[-1]
                            else:
                                match = next(item for item in refs if item["table"] == original_table)
                                match["table"] = "iceberg.other." + match["table"].rsplit(".", 1)[-1]
                    elif invalid_case in {"after_drift", "readback_drift"}:
                        refs = after if invalid_case == "after_drift" else readback
                        next(row for row in refs if row["table"] in ml_tables)["snapshot_id"] = 999
                    else:
                        invalid_id = {"bad_id_true": True, "bad_id_zero": 0, "bad_id_negative": -1,
                                      "bad_id_overflow": 1 << 63}[invalid_case]
                        ml_refs[0]["snapshot_id"] = invalid_id

                self._edit_evidence(case_result, mutate)
                with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_(?:ML_RECOVERY_INVALID|SNAPSHOTS_MISSING|READBACK_MISMATCH)"):
                    self._validate_fixture(root, request, report, matrix)

    def test_outside_scope_partition_must_be_valid_and_outside_each_case_scope(self):
        invalid_partitions = ("2023-09", None, "", 7, "2023-00", "2023-13", "0000-08")
        for partition in invalid_partitions:
            with self.subTest(partition=partition), tempfile.TemporaryDirectory() as root:
                request, report, matrix = self._runtime_fixture(root, ["fetch_timeout"])
                case_result = report["cases"][0]
                self._edit_evidence(case_result, lambda evidence: (
                    evidence["outside_scope"]["before"].update(partition=partition),
                    evidence["outside_scope"]["after"].update(partition=partition)))
                with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_OUTSIDE_SCOPE_CHANGED"):
                    self._validate_fixture(root, request, report, matrix)

        with tempfile.TemporaryDirectory() as root:
            request, report, matrix = self._runtime_fixture(root, ["fetch_timeout"])
            case_result = report["cases"][0]
            self._edit_evidence(case_result, lambda evidence: (
                evidence["outside_scope"]["before"].pop("partition"),
                evidence["outside_scope"]["after"].pop("partition")))
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_OUTSIDE_SCOPE_CHANGED"):
                self._validate_fixture(root, request, report, matrix)

        with tempfile.TemporaryDirectory() as root:
            request, report, matrix = self._runtime_fixture(root, ["ml_import_partial"])
            case_result = report["cases"][0]
            in_scope_id = request["case_scopes"]["ml_import_partial"]["experiment_run_ids"][0]
            self._edit_evidence(case_result, lambda evidence: (
                evidence["outside_scope"]["before"].update(partition=in_scope_id),
                evidence["outside_scope"]["after"].update(partition=in_scope_id)))
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_OUTSIDE_SCOPE_CHANGED"):
                self._validate_fixture(root, request, report, matrix)

        with tempfile.TemporaryDirectory() as root:
            request, report, matrix = self._runtime_fixture(root, ["ml_import_partial"])
            self._validate_fixture(root, request, report, matrix)

    def test_runtime_rejects_offline_report_returned_by_successful_runner(self):
        with tempfile.TemporaryDirectory() as root:
            self._runtime_fixture(root, ["fetch_timeout"])
            request_path = Path(root, "request.json")
            request = json.loads(request_path.read_text(encoding="utf-8"))
            report_path = Path(request["report_path"])
            report_path.unlink()
            offline = qa.run_offline_case("fetch_timeout")
            offline_bytes = json.dumps(offline).encode()

            def fake_runner(command, **kwargs):
                report_path.write_bytes(offline_bytes)
                return qa.subprocess.CompletedProcess(command, 0)

            environment = {
                "QA_04_STAGING_ROOT": root, "ORC_07_STATUS": "Done", "MLI_03_STATUS": "Done",
                "QA_04_RUNTIME_COMMAND": Path(root, "qa04-runtime-runner").as_posix(), "PATH": os.environ.get("PATH", ""),
            }
            with patch.object(qa.subprocess, "run", side_effect=fake_runner):
                with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_REPORT_IDENTITY_MISMATCH"):
                    qa.run_runtime(request_path, environment)

    def test_offline_or_wrong_identity_runtime_reports_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            request, report, matrix = self._runtime_fixture(root, ["fetch_timeout"])
            report["mode"] = "offline_control"
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_REPORT_IDENTITY_MISMATCH"):
                qa._validate_runtime_report(report, request, hashlib.sha256(Path(root, "request.json").read_bytes()).hexdigest(),
                                            matrix, hashlib.sha256(qa.MATRIX_PATH.read_bytes()).hexdigest(), Path(root).resolve())
            _, report, matrix = self._runtime_fixture(root, ["fetch_timeout"])
            report["run_id"] = "other-run"
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_REPORT_IDENTITY_MISMATCH"):
                qa._validate_runtime_report(report, request, hashlib.sha256(Path(root, "request.json").read_bytes()).hexdigest(),
                                            matrix, hashlib.sha256(qa.MATRIX_PATH.read_bytes()).hexdigest(), Path(root).resolve())

    def test_runtime_request_rejects_scope_hash_namespace_snapshot_and_unpinned_input(self):
        mutations = (
            ("scope", lambda request: request["case_scopes"]["fetch_timeout"].update(write_months=["2023-10"])),
            ("namespace", lambda request: request.update(namespace="iceberg.qa04_bad-name")),
            ("baseline", lambda request: request["baseline_snapshots"][0].update(snapshot_id=-5)),
            ("input", lambda request: request["input_pins"][0].pop("sha256")),
            ("duplicate", lambda request: request.update(case_ids=["fetch_timeout", "fetch_timeout"])),
        )
        for label, mutate in mutations:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as root:
                request, _, matrix = self._runtime_fixture(root, ["fetch_timeout"])
                mutate(request)
                with self.assertRaises(qa.Qa04Error):
                    qa._validate_runtime_request(request, matrix)

    def test_malformed_runtime_request_is_rejected_before_runner_invocation(self):
        with tempfile.TemporaryDirectory() as root:
            request, _, _ = self._runtime_fixture(root, ["fetch_timeout"])
            request["namespace"] = "iceberg.qa04_invalid-name"
            Path(root, "request.json").write_text(json.dumps(request), encoding="utf-8")
            environment = {"QA_04_STAGING_ROOT": root, "ORC_07_STATUS": "Done", "MLI_03_STATUS": "Done",
                           "QA_04_RUNTIME_COMMAND": Path(root, "qa04-runtime-runner").as_posix(), "PATH": os.environ.get("PATH", "")}
            with patch.object(qa.subprocess, "run") as runner:
                with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_NAMESPACE_INVALID"):
                    qa.run_runtime(Path(root, "request.json"), environment)
            runner.assert_not_called()

    def test_runtime_runner_checksum_mismatch_is_rejected_before_invocation(self):
        with tempfile.TemporaryDirectory() as root:
            request, _, _ = self._runtime_fixture(root, ["fetch_timeout"])
            request["artifact_refs"][-1]["sha256"] = "0" * 64
            Path(root, "request.json").write_text(json.dumps(request), encoding="utf-8")
            Path(root, "report.json").unlink()
            environment = {"QA_04_STAGING_ROOT": root, "ORC_07_STATUS": "Done", "MLI_03_STATUS": "Done",
                           "QA_04_RUNTIME_COMMAND": Path(root, "qa04-runtime-runner").as_posix(),
                           "PATH": os.environ.get("PATH", "")}
            with patch.object(qa.subprocess, "run") as runner:
                with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_RUNNER_PIN_INVALID"):
                    qa.run_runtime(Path(root, "request.json"), environment)
            runner.assert_not_called()

    def test_runtime_evidence_checksum_readback_and_case_set_are_required(self):
        with tempfile.TemporaryDirectory() as root:
            request, report, matrix = self._runtime_fixture(root, ["fetch_timeout"])
            request_sha = hashlib.sha256(Path(root, "request.json").read_bytes()).hexdigest()
            matrix_sha = hashlib.sha256(qa.MATRIX_PATH.read_bytes()).hexdigest()
            report["cases"][0]["status"] = "PENDING_RUNTIME"
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_CASE_NOT_PASSED"):
                qa._validate_runtime_report(report, request, request_sha, matrix, matrix_sha, Path(root).resolve())
            request, report, matrix = self._runtime_fixture(root, ["fetch_timeout"])
            evidence = report["cases"][0]["evidence"]
            evidence_path = Path(evidence["path"])
            evidence_path.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_EVIDENCE_CHECKSUM_INVALID"):
                qa._validate_runtime_report(report, request, hashlib.sha256(Path(root, "request.json").read_bytes()).hexdigest(),
                                            matrix, matrix_sha, Path(root).resolve())

    def test_gold_publication_and_snapshot_mismatch_is_rejected_after_repinning_evidence(self):
        with tempfile.TemporaryDirectory() as root:
            request, report, matrix = self._runtime_fixture(root, ["gold_commit_partial"])
            case_result = report["cases"][0]
            evidence_path = Path(case_result["evidence"]["path"])
            evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
            evidence["gold"]["publication"]["table_snapshots"]["event_current"] = 999
            evidence_bytes = json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
            evidence_path.write_bytes(evidence_bytes)
            case_result["evidence"]["sha256"] = hashlib.sha256(evidence_bytes).hexdigest()
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_GOLD_PUBLICATION_INVALID"):
                qa._validate_runtime_report(report, request, hashlib.sha256(Path(root, "request.json").read_bytes()).hexdigest(),
                                            matrix, hashlib.sha256(qa.MATRIX_PATH.read_bytes()).hexdigest(), Path(root).resolve())
            request, report, matrix = self._runtime_fixture(root, ["fetch_timeout"])
            report["cases"].append(dict(report["cases"][0]))
            matrix_sha = hashlib.sha256(qa.MATRIX_PATH.read_bytes()).hexdigest()
            with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_REPORT_CASES_INVALID"):
                qa._validate_runtime_report(report, request, hashlib.sha256(Path(root, "request.json").read_bytes()).hexdigest(),
                                            matrix, matrix_sha, Path(root).resolve())

    def test_runtime_requires_dependencies_and_does_not_launch_when_pending(self):
        with tempfile.TemporaryDirectory() as root:
            request = Path(root) / "request.json"
            self._runtime_fixture(root, ["fetch_timeout"])
            request = Path(root) / "request.json"
            original = json.loads(request.read_text(encoding="utf-8"))
            original["report_path"] = str(Path(root) / "report.json")
            request.write_text(json.dumps(original), encoding="utf-8")
            Path(root, "report.json").unlink()
            env = {"QA_04_STAGING_ROOT": root, "ORC_07_STATUS": "Ready", "MLI_03_STATUS": "In Progress"}
            with patch.object(qa.subprocess, "run") as runner:
                with self.assertRaisesRegex(qa.Qa04Error, "QA04_RUNTIME_DEPENDENCY_PENDING"):
                    qa.run_runtime(request, env)
            runner.assert_not_called()


if __name__ == "__main__":
    unittest.main()
