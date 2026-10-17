import json
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
from qa_02_rerun_revision import (RUNTIME_REPORT_VERSION, Qa02Error, load_matrix, prepare_report,
                                   required_assertions, run_live, write_preparation)


MATRIX = Path(__file__).parents[1] / "dags/fixtures/qa_02_rerun_revision_matrix.json"


class Qa02RerunRevisionTest(unittest.TestCase):
    def test_matrix_covers_retry_duplicate_revision_overlap_move_withdrawal_and_retention(self):
        matrix = load_matrix(MATRIX)
        categories = {case["category"] for case in matrix["cases"]}
        self.assertTrue({"retry", "duplicate", "late_revision", "source_overlap", "partition_move", "withdrawal", "retention"} <= categories)
        report = prepare_report(MATRIX)
        self.assertEqual("PREPARED", report["status"])
        self.assertFalse(report["runtime_complete"])
        self.assertEqual(8, report["summary"]["pending_runtime"])

    def test_preparation_writes_bounded_json_and_markdown_evidence_template(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = write_preparation(MATRIX, root / "qa-02.json", root / "qa-02.md")
            stored = json.loads((root / "qa-02.json").read_text(encoding="utf-8"))
            self.assertEqual(report, stored)
            self.assertIn("PENDING_RUNTIME", (root / "qa-02.md").read_text(encoding="utf-8"))

    def test_invalid_duplicate_case_id_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
            matrix["cases"][1]["case_id"] = matrix["cases"][0]["case_id"]
            path.write_text(json.dumps(matrix), encoding="utf-8")
            with self.assertRaisesRegex(Qa02Error, "QA02_CASE_INVALID"):
                load_matrix(path)

    def runtime_request(self, root: Path) -> tuple[Path, dict]:
        matrix_sha256 = hashlib.sha256(MATRIX.read_bytes()).hexdigest()
        report_path = root / "runtime-report.json"
        request = {
            "task_id": "QA-02",
            "request_version": "qa-02-runtime-v1",
            "run_id": "qa02-runtime-001",
            "namespace": "qa-02-rerun-revision",
            "operation_id": "qa02-runtime-001",
            "matrix_path": str(MATRIX),
            "matrix_sha256": matrix_sha256,
            "report_path": str(report_path),
        }
        return report_path, request

    def complete_report(self, request: dict) -> dict:
        matrix = load_matrix(MATRIX)
        cases = []
        for source in matrix["cases"]:
            reconciliation = {assertion: self.assertion_payload(assertion, source["expected"][assertion])
                              for assertion in sorted(required_assertions(source))}
            evidence_path = Path(request["report_path"]).parent / f"{source['case_id']}.evidence.json"
            evidence_path.write_text(json.dumps({"case_id": source["case_id"], "verified": True}), encoding="utf-8")
            evidence_sha256 = hashlib.sha256(evidence_path.read_bytes()).hexdigest()
            cases.append({
                "case_id": source["case_id"], "operation_id": source["operation_id"],
                "category": source["category"], "status": "PASS",
                "input_pins": {"matrix_sha256": request["matrix_sha256"], "run_id": request["run_id"],
                                "namespace": request["namespace"]},
                "snapshots": {"before": [{"table": "gold.event_current", "snapshot_id": "1001"}],
                              "after": [{"table": "gold.event_current", "snapshot_id": "1001"}]},
                "reconciliation": reconciliation,
                "evidence_ref": {"path": str(evidence_path), "sha256": evidence_sha256,
                                 "run_id": request["run_id"], "namespace": request["namespace"],
                                 "case_id": source["case_id"], "operation_id": source["operation_id"]},
            })
        return {
            "task_id": "QA-02", "report_version": RUNTIME_REPORT_VERSION, "status": "PASS",
            "runtime_complete": True, "run_id": request["run_id"], "namespace": request["namespace"],
            "operation_id": request["operation_id"], "matrix_sha256": request["matrix_sha256"],
            "input_pins": {"matrix_sha256": request["matrix_sha256"], "run_id": request["run_id"],
                            "namespace": request["namespace"]},
            "cases": cases,
            "summary": {"case_count": len(cases), "pending_runtime": 0,
                         "passed": len(cases), "failed": 0},
        }

    def assertion_payload(self, assertion: str, expected: str) -> dict:
        details = {}
        if expected == "unchanged":
            before = after = ["event-1"] if assertion == "logical_keys" else 10
        elif expected == "same_or_equivalent":
            before = after = {"table": "gold.event_current", "count": 10}
            details["equivalent"] = True
        elif expected == "same_key_new_revision":
            before = after = ["event-1"]
            details["revision_changed"] = True
        elif expected == "no_double_count":
            before = after = 10
            details["duplicate_count"] = 0
        elif expected == "outside_scope_is_not_modified":
            before = after = {"partition": "2023-10", "count": 1}
            details["scope"] = "outside"
        elif expected == "one_current_row":
            before, after = ((["event-1", "event-2"], ["event-1"])
                             if assertion == "logical_keys" else (2, 1))
        elif expected == "new_revision":
            before, after = {"revision_id": "old", "is_current": False}, {"revision_id": "new", "is_current": True}
        elif expected == "old_revision_retained":
            before, after = {"history_count": 1}, {"retained": True, "history_count": 2}
        elif expected == "new_catalog_release":
            before, after = {"release_id": "jma-old"}, {"release_id": "jma-new"}
        elif expected == "replacement":
            before, after = {"release_id": "jma-old"}, {"release_id": "jma-new", "replacement": True, "is_current": True}
        elif expected == "auditable":
            before, after = {"release_id": "jma-old"}, {"release_id": "jma-old", "auditable": True}
        elif expected == "both_sources_retained":
            before, after = {"sources": ["USGS"]}, {"sources": ["USGS", "JMA"]}
        elif expected == "current":
            before, after = {"state": "old"}, {"state": "current"}
        elif expected == "superseded":
            before, after = {"state": "current"}, {"state": "superseded"}
        elif expected == "withdrawn_or_superseded":
            before, after = {"state": "current"}, {"state": "withdrawn"}
        elif expected == "retained":
            before, after = {"history_count": 1}, {"history_count": 2, "retained": True}
        elif expected == "reconciled":
            before, after = {"canonical_count": 10}, {"canonical_count": 10, "balanced": True}
        elif expected == "old_snapshot_readable":
            before = {"table": "gold.event_current", "snapshot_id": "1001"}
            after = {**before, "readable": True}
        else:
            raise AssertionError(f"Unhandled assertion fixture: {expected}")
        return {"status": "PASS", "check": expected, "before": before, "after": after, "details": details}

    def test_runtime_report_requires_every_case_and_reconciliation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            env = {"QA_02_RUNTIME_COMMAND": "qa-02-runner"}

            def runner(command, **kwargs):
                report = self.complete_report(request)
                report["cases"] = report["cases"][:-1]
                report["summary"]["case_count"] -= 1
                report["summary"]["passed"] -= 1
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner) as called:
                with self.assertRaisesRegex(Qa02Error, "QA02_RUNTIME_CASE_SET_MISMATCH"):
                    run_live(request_path, env)
                self.assertEqual(1, called.call_count)

    def test_runtime_report_accepts_complete_case_set(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(self.complete_report(request)), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                report = run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})
            self.assertEqual("PASS", report["status"])
            self.assertEqual(8, report["summary"]["passed"])

    def test_runtime_report_rejects_stale_matrix_checksum(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            report = self.complete_report(request)
            report["matrix_sha256"] = "0" * 64

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                with self.assertRaisesRegex(Qa02Error, "QA02_RUNTIME_REPORT_MATRIX_MISMATCH"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})

    def test_runtime_request_is_validated_before_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, request = self.runtime_request(root)
            request.pop("report_path")
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            with patch("qa_02_rerun_revision.subprocess.run") as runner:
                with self.assertRaisesRegex(Qa02Error, "QA02_REPORT_REFERENCE_INVALID"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})
                runner.assert_not_called()

    def test_runtime_request_rejects_missing_matrix_before_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _, request = self.runtime_request(root)
            request["matrix_path"] = str(root / "missing-matrix.json")
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            with patch("qa_02_rerun_revision.subprocess.run") as runner:
                with self.assertRaisesRegex(Qa02Error, "QA02_MATRIX_REFERENCE_INVALID"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})
                runner.assert_not_called()

    def test_runtime_report_rejects_failed_case_and_missing_reconciliation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            report = self.complete_report(request)
            report["cases"][0]["status"] = "FAIL"
            report["cases"][0]["reconciliation"] = {}

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                with self.assertRaisesRegex(Qa02Error, "QA02_RUNTIME_CASE_INVALID"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})

    def test_runtime_report_rejects_empty_snapshots(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            report = self.complete_report(request)
            report["cases"][0]["snapshots"]["before"] = []

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                with self.assertRaisesRegex(Qa02Error, "QA02_RUNTIME_SNAPSHOTS_REQUIRED"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})

    def test_runtime_report_rejects_null_assertion_values(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            report = self.complete_report(request)
            report["cases"][0]["reconciliation"]["canonical_count"]["after"] = None

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                with self.assertRaisesRegex(Qa02Error, "QA02_RECONCILIATION_VALUE_INVALID"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})

    def test_runtime_report_rejects_missing_evidence_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            report = self.complete_report(request)
            report["cases"][0]["evidence_ref"]["path"] = str(root / "missing-evidence.json")

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                with self.assertRaisesRegex(Qa02Error, "QA02_RUNTIME_EVIDENCE_REQUIRED"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})

    def test_runtime_report_rejects_evidence_checksum_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            report = self.complete_report(request)
            report["cases"][0]["evidence_ref"]["sha256"] = "0" * 64

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                with self.assertRaisesRegex(Qa02Error, "QA02_RUNTIME_EVIDENCE_CHECKSUM_MISMATCH"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})

    def test_runtime_report_rejects_changed_rerun_count(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            report = self.complete_report(request)
            report["cases"][0]["reconciliation"]["canonical_count"]["after"] = 999

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                with self.assertRaisesRegex(Qa02Error, "QA02_RECONCILIATION_EXPECTATION_FAILED"):
                    run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})

    def test_matrix_rejects_duplicate_operation_and_empty_assertions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
            matrix["cases"][1]["operation_id"] = matrix["cases"][0]["operation_id"]
            matrix["cases"][0]["expected"] = {}
            path.write_text(json.dumps(matrix), encoding="utf-8")
            with self.assertRaisesRegex(Qa02Error, "QA02_CASE_INVALID"):
                load_matrix(path)

    def run_mutated_report(self, mutate):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report_path, request = self.runtime_request(root)
            request_path = root / "request.json"
            request_path.write_text(json.dumps(request), encoding="utf-8")
            report = self.complete_report(request)
            mutate({case["case_id"]: case for case in report["cases"]})

            def runner(command, **kwargs):
                report_path.write_text(json.dumps(report), encoding="utf-8")
                return type("Completed", (), {"returncode": 0})()

            with patch("qa_02_rerun_revision.subprocess.run", side_effect=runner):
                return run_live(request_path, {"QA_02_RUNTIME_COMMAND": "qa-02-runner"})

    def test_retention_rejects_missing_or_invalid_snapshot_identity(self):
        for sides in (("before",), ("after",), ("before", "after")):
            for field, invalid in (("snapshot_id", None), ("snapshot_id", ""),
                                   ("snapshot_id", True), ("snapshot_id", -1),
                                   ("snapshot_id", "latest"), ("snapshot_id", "9223372036854775808"),
                                   ("table", None), ("table", ""), ("table", [])):
                with self.subTest(sides=sides, field=field, invalid=invalid):
                    def mutate(cases):
                        for side in sides:
                            value = cases["retention_outside_scope"]["reconciliation"]["snapshots"][side]
                            if invalid is None:
                                value.pop(field)
                            else:
                                value[field] = invalid

                    with self.assertRaisesRegex(Qa02Error, "QA02_"):
                        self.run_mutated_report(mutate)

    def test_retention_rejects_snapshot_drift_or_unpinned_reference(self):
        for sides in (("after",), ("before", "after")):
            for field, changed in (("snapshot_id", "2002"), ("table", "gold.event_source_bridge")):
                with self.subTest(sides=sides, field=field):
                    def mutate(cases):
                        snapshots = cases["retention_outside_scope"]["reconciliation"]["snapshots"]
                        for side in sides:
                            snapshots[side][field] = changed

                    with self.assertRaisesRegex(Qa02Error, "QA02_"):
                        self.run_mutated_report(mutate)

    def test_retention_requires_old_snapshot_in_both_case_reference_sets(self):
        for side in ("before", "after"):
            with self.subTest(side=side):
                def mutate(cases):
                    cases["retention_outside_scope"]["snapshots"][side][0]["snapshot_id"] = "2002"

                with self.assertRaisesRegex(Qa02Error, "QA02_"):
                    self.run_mutated_report(mutate)

    def test_retention_accepts_same_snapshot_as_integer_or_decimal_string(self):
        def mutate(cases):
            case = cases["retention_outside_scope"]
            case["reconciliation"]["snapshots"]["after"]["snapshot_id"] = 1001
            case["snapshots"]["before"][0]["snapshot_id"] = 1001

        self.assertEqual("PASS", self.run_mutated_report(mutate)["status"])

    def test_old_release_rejects_new_release_or_invalid_identity(self):
        for side in ("before", "after"):
            for invalid in (None, "", "   ", False, "jma-new", "jma-other"):
                with self.subTest(side=side, invalid=invalid):
                    def mutate(cases):
                        value = cases["jma_release_replacement"]["reconciliation"]["old_release"][side]
                        if invalid is None:
                            value.pop("release_id")
                        else:
                            value["release_id"] = invalid

                    with self.assertRaisesRegex(Qa02Error, "QA02_"):
                        self.run_mutated_report(mutate)

    def test_old_release_must_match_replacement_baseline(self):
        def mutate(cases):
            old_release = cases["jma_release_replacement"]["reconciliation"]["old_release"]
            for side in ("before", "after"):
                old_release[side]["release_id"] = "unrelated-release"

        with self.assertRaisesRegex(Qa02Error, "QA02_"):
            self.run_mutated_report(mutate)


if __name__ == "__main__":
    unittest.main()
