"""Unit tests for DAT-03 boundary integration verification harness."""

import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/verify-catalog-boundaries.py"

spec = importlib.util.spec_from_file_location("catalog_boundaries", MODULE_PATH)
boundaries = importlib.util.module_from_spec(spec)
spec.loader.exec_module(boundaries)


class TestCatalogBoundaries(unittest.TestCase):
    def test_month_to_int_and_reverse(self):
        m_int = boundaries.month_to_int("2000-01")
        self.assertEqual(boundaries.int_to_month(m_int), "2000-01")

        m_int2 = boundaries.month_to_int("2023-12")
        self.assertEqual(boundaries.int_to_month(m_int2), "2023-12")
        self.assertEqual(m_int2 - m_int + 1, 288)

    def test_expand_month_range(self):
        months = boundaries.expand_month_range("2013-08", "2013-10")
        self.assertEqual(months, ["2013-08", "2013-09", "2013-10"])

        with self.assertRaises(ValueError):
            boundaries.expand_month_range("2014-01", "2013-12")

    def test_verify_segment_contiguity_passes_standard_segments(self):
        res = boundaries.verify_segment_contiguity(boundaries.EXPECTED_SEGMENTS)
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(res["total_months"], 288)
        self.assertEqual(len(res["missing_months"]), 0)
        self.assertEqual(len(res["overlapping_months"]), 0)
        self.assertEqual(len(res["unexpected_months"]), 0)

    def test_verify_segment_contiguity_detects_gap(self):
        tampered = [
            {
                "segment_id": "seg1",
                "start_month": "2000-01",
                "end_month": "2008-04",  # Missing 2008-05
                "expected_months": 100,
            },
            {
                "segment_id": "seg2",
                "start_month": "2008-06",
                "end_month": "2023-12",
                "expected_months": 187,
            },
        ]
        res = boundaries.verify_segment_contiguity(tampered)
        self.assertEqual(res["status"], "FAIL")
        self.assertIn("2008-05", res["missing_months"])

    def test_verify_segment_contiguity_detects_overlap(self):
        tampered = [
            {
                "segment_id": "seg1",
                "start_month": "2000-01",
                "end_month": "2008-06",  # Overlaps 2008-06
                "expected_months": 102,
            },
            {
                "segment_id": "seg2",
                "start_month": "2008-06",
                "end_month": "2023-12",
                "expected_months": 187,
            },
        ]
        res = boundaries.verify_segment_contiguity(tampered)
        self.assertEqual(res["status"], "FAIL")
        self.assertIn("2008-06", res["overlapping_months"])

    def test_verify_id_disjointness_passes_clean_sets(self):
        ids1 = {"evt_001", "evt_002", "evt_003"}
        ids2 = {"evt_004", "evt_005", "evt_006"}
        res = boundaries.verify_id_disjointness(ids1, ids2)
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(res["duplicate_count"], 0)

    def test_verify_id_disjointness_detects_duplicate_ids(self):
        ids1 = {"evt_001", "evt_002", "evt_003"}
        ids2 = {"evt_003", "evt_004", "evt_005"}
        res = boundaries.verify_id_disjointness(ids1, ids2)
        self.assertEqual(res["status"], "FAIL")
        self.assertEqual(res["duplicate_count"], 1)
        self.assertIn("evt_003", res["sample_duplicates"])

    def test_verify_boundary_time_window_passes_clean_timestamps(self):
        res = boundaries.verify_boundary_time_window(
            "2008-05",
            "2008-05-31T23:59:58Z",
            "2008-06",
            "2008-06-01T00:00:05Z",
        )
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(len(res["violations"]), 0)

    def test_verify_boundary_time_window_detects_overflow(self):
        res = boundaries.verify_boundary_time_window(
            "2008-05",
            "2008-06-01T00:00:01Z",  # Overflow into next month
            "2008-06",
            "2008-06-01T00:05:00Z",
        )
        self.assertEqual(res["status"], "FAIL")
        self.assertTrue(any("PREV_MONTH_OVERFLOW" in v for v in res["violations"]))

    def test_verify_boundary_time_window_detects_underflow(self):
        res = boundaries.verify_boundary_time_window(
            "2013-07",
            "2013-07-31T23:00:00Z",
            "2013-08",
            "2013-07-31T23:59:59Z",  # Underflow from previous month
        )
        self.assertEqual(res["status"], "FAIL")
        self.assertTrue(any("NEXT_MONTH_UNDERFLOW" in v for v in res["violations"]))

    def test_verify_all_boundaries_end_to_end(self):
        mock_data = {
            "Boundary 1 (Base -> HoaiTam)": {
                "prev_ids": ["b_01", "b_02"],
                "next_ids": ["ht_01", "ht_02"],
                "max_prev_time_utc": "2008-05-31T23:50:00Z",
                "min_next_time_utc": "2008-06-01T00:10:00Z",
            },
            "Boundary 2 (HoaiTam -> ThanhTris)": {
                "prev_ids": ["ht_99"],
                "next_ids": ["tt_01"],
                "max_prev_time_utc": "2013-07-31T22:00:00Z",
                "min_next_time_utc": "2013-08-01T01:00:00Z",
            },
            "Boundary 3 (ThanhTris -> rosy179)": {
                "prev_ids": ["tt_99"],
                "next_ids": ["r_01"],
                "max_prev_time_utc": "2018-09-30T23:45:00Z",
                "min_next_time_utc": "2018-10-01T00:05:00Z",
            },
        }
        report = boundaries.verify_all_boundaries(mock_data)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["contiguity"]["status"], "PASS")
        self.assertEqual(len(report["boundaries"]), 3)


if __name__ == "__main__":
    unittest.main()
