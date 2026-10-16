import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
from qa_02_rerun_revision import Qa02Error, load_matrix, prepare_report, write_preparation


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


if __name__ == "__main__":
    unittest.main()
