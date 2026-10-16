import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
from qa_03_jma_historical_backfill import Qa03Error, load_matrix, prepare_report, write_preparation


MATRIX = Path(__file__).parents[1] / "dags/fixtures/qa_03_jma_historical_matrix.json"


class Qa03JmaHistoricalBackfillTest(unittest.TestCase):
    def test_matrix_has_required_representative_years_and_months(self):
        matrix = load_matrix(MATRIX)
        self.assertEqual({1997, 2000, 2023}, {item["year"] for item in matrix["scope"]})
        self.assertEqual({"1997-09", "1997-10", "2000-01", "2023-09"},
                         {month for item in matrix["scope"] for month in item["months"]})
        report = prepare_report(MATRIX)
        self.assertEqual("PREPARED", report["status"])
        self.assertFalse(report["runtime_complete"])
        self.assertIsNone(report["capacity"]["duration_seconds"])

    def test_preparation_writes_scope_and_capacity_note(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = write_preparation(MATRIX, root / "qa-03.json", root / "qa-03.md")
            self.assertEqual(report, json.loads((root / "qa-03.json").read_text(encoding="utf-8")))
            markdown = (root / "qa-03.md").read_text(encoding="utf-8")
            self.assertIn("1997", markdown)
            self.assertIn("Capacity fields remain empty", markdown)

    def test_missing_representative_month_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.json"
            matrix = json.loads(MATRIX.read_text(encoding="utf-8"))
            matrix["scope"][0]["months"] = []
            path.write_text(json.dumps(matrix), encoding="utf-8")
            with self.assertRaisesRegex(Qa03Error, "QA03_TARGET_MONTHS_REQUIRED"):
                load_matrix(path)


if __name__ == "__main__":
    unittest.main()
