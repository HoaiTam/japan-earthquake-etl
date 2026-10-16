import json
import hashlib
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
from qa_02_rerun_revision import (Qa02Error, load_matrix, prepare_report, required_assertions,
                                   run_live, write_preparation)


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
            reconciliation = {
                assertion: {"status": "PASS", "before": "fixture-before", "after": "fixture-after"}
                for assertion in sorted(required_assertions(source))
            }
            cases.append({
                "case_id": source["case_id"], "operation_id": source["operation_id"],
                "category": source["category"], "status": "PASS",
                "input_pins": {"matrix_sha256": request["matrix_sha256"], "run_id": request["run_id"],
                                "namespace": request["namespace"]},
                "snapshots": {"before": ["snapshot-before"], "after": ["snapshot-after"]},
                "reconciliation": reconciliation,
                "evidence_ref": "qa://qa-02-runtime-001/" + source["case_id"],
            })
        return {
            "task_id": "QA-02", "report_version": "qa-02-report-v1", "status": "PASS",
            "runtime_complete": True, "run_id": request["run_id"], "namespace": request["namespace"],
            "operation_id": request["operation_id"], "matrix_sha256": request["matrix_sha256"],
            "input_pins": {"matrix_sha256": request["matrix_sha256"], "run_id": request["run_id"],
                            "namespace": request["namespace"]},
            "cases": cases,
            "summary": {"case_count": len(cases), "pending_runtime": 0,
                         "passed": len(cases), "failed": 0},
        }

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

    def test_matrix_rejects_duplicate_operation_and_empty_assertions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
            matrix["cases"][1]["operation_id"] = matrix["cases"][0]["operation_id"]
            matrix["cases"][0]["expected"] = {}
            path.write_text(json.dumps(matrix), encoding="utf-8")
            with self.assertRaisesRegex(Qa02Error, "QA02_CASE_INVALID"):
                load_matrix(path)


if __name__ == "__main__":
    unittest.main()
