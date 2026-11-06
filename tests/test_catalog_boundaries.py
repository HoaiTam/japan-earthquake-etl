"""Unit and regression tests for DAT-03 cross-boundary catalog integration verification harness."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

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

    # -------------------------------------------------------------------------
    # Theoretical Segment Contiguity Tests
    # -------------------------------------------------------------------------
    def test_verify_segment_contiguity_passes_standard_segments(self):
        res = boundaries.verify_segment_contiguity(boundaries.EXPECTED_SEGMENTS)
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(res["total_months"], 288)
        self.assertEqual(len(res["missing_months"]), 0)
        self.assertEqual(len(res["overlapping_months"]), 0)
        self.assertEqual(len(res["unexpected_months"]), 0)

    def test_verify_segment_contiguity_detects_gap(self):
        tampered = [
            {"segment_id": "seg1", "start_month": "2000-01", "end_month": "2008-04", "expected_months": 100},
            {"segment_id": "seg2", "start_month": "2008-06", "end_month": "2023-12", "expected_months": 187},
        ]
        res = boundaries.verify_segment_contiguity(tampered)
        self.assertEqual(res["status"], "FAIL")
        self.assertIn("2008-05", res["missing_months"])

    def test_verify_segment_contiguity_detects_overlap(self):
        tampered = [
            {"segment_id": "seg1", "start_month": "2000-01", "end_month": "2008-06", "expected_months": 102},
            {"segment_id": "seg2", "start_month": "2008-06", "end_month": "2023-12", "expected_months": 187},
        ]
        res = boundaries.verify_segment_contiguity(tampered)
        self.assertEqual(res["status"], "FAIL")
        self.assertIn("2008-06", res["overlapping_months"])

    # -------------------------------------------------------------------------
    # Package Manifest Contiguity Tests (Real Manifest Verification)
    # -------------------------------------------------------------------------
    def test_verify_manifest_contiguity_passes_clean_4_packages(self):
        manifests = [
            {
                "package_id": "pkg_base_101",
                "start_month": "2000-01",
                "end_month": "2008-05",
                "covered_months": boundaries.expand_month_range("2000-01", "2008-05"),
                "row_count": 1064869,
            },
            {
                "package_id": "pkg_hoaitam_62",
                "start_month": "2008-06",
                "end_month": "2013-07",
                "covered_months": boundaries.expand_month_range("2008-06", "2013-07"),
                "row_count": 381049,
            },
            {
                "package_id": "pkg_thanhtris_62",
                "start_month": "2013-08",
                "end_month": "2018-09",
                "covered_months": boundaries.expand_month_range("2013-08", "2018-09"),
                "row_count": 400000,
            },
            {
                "package_id": "pkg_rosy179_63",
                "start_month": "2018-10",
                "end_month": "2023-12",
                "covered_months": boundaries.expand_month_range("2018-10", "2023-12"),
                "row_count": 420000,
            },
        ]
        res = boundaries.verify_manifest_contiguity(manifests)
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(res["total_months"], 288)
        self.assertEqual(len(res["missing_months"]), 0)
        self.assertEqual(len(res["overlapping_months"]), 0)

    def test_verify_manifest_contiguity_detects_gap_and_overlap(self):
        # Gap: missing 2013-08
        gap_manifests = [
            {
                "package_id": "pkg_hoaitam",
                "start_month": "2008-06",
                "end_month": "2013-07",
                "covered_months": boundaries.expand_month_range("2008-06", "2013-07"),
            },
            {
                "package_id": "pkg_thanhtris",
                "start_month": "2013-09",
                "end_month": "2018-09",
                "covered_months": boundaries.expand_month_range("2013-09", "2018-09"),
            },
        ]
        res_gap = boundaries.verify_manifest_contiguity(gap_manifests)
        self.assertEqual(res_gap["status"], "FAIL")
        self.assertIn("2013-08", res_gap["missing_months"])

    def test_verify_manifest_contiguity_detects_sha_mismatch(self):
        raw = b'{"package": "test"}'
        manifests = [
            {
                "package_id": "pkg_tampered",
                "start_month": "2000-01",
                "end_month": "2008-05",
                "covered_months": boundaries.expand_month_range("2000-01", "2008-05"),
                "manifest_bytes": raw,
                "package_sha256": "bad_sha_0000000000000000000000000000000000000000000000000000000000",
            }
        ]
        res = boundaries.verify_manifest_contiguity(manifests)
        self.assertEqual(res["status"], "FAIL")
        self.assertTrue(any("MANIFEST_SHA256_MISMATCH" in v for v in res["violations"]))

    def test_verify_manifest_contiguity_none_returns_not_checked(self):
        res = boundaries.verify_manifest_contiguity(None)
        self.assertEqual(res["status"], "NOT_CHECKED")
        self.assertEqual(res["reason"], "MISSING_PACKAGE_MANIFESTS")

    # -------------------------------------------------------------------------
    # ID Disjointness & Fail-Closed Checks
    # -------------------------------------------------------------------------
    def test_verify_id_disjointness_passes_clean_sets(self):
        ids1 = ["evt_001", "evt_002", "evt_003"]
        ids2 = ["evt_004", "evt_005", "evt_006"]
        res = boundaries.verify_id_disjointness(ids1, ids2)
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(res["duplicate_count"], 0)

    def test_verify_id_disjointness_detects_duplicate_ids(self):
        ids1 = ["evt_001", "evt_002", "evt_003"]
        ids2 = ["evt_003", "evt_004", "evt_005"]
        res = boundaries.verify_id_disjointness(ids1, ids2)
        self.assertEqual(res["status"], "FAIL")
        self.assertEqual(res["duplicate_count"], 1)
        self.assertIn("evt_003", res["sample_duplicates"])

    def test_verify_id_disjointness_empty_input_fails_closed(self):
        # Empty set without explicit allow_empty fails closed (not assumed pass)
        res_empty = boundaries.verify_id_disjointness([], ["evt_01"])
        self.assertEqual(res_empty["status"], "FAIL")
        self.assertIn("UNVERIFIED_EMPTY_INPUT", res_empty["reason"])

        # None returns NOT_CHECKED
        res_none = boundaries.verify_id_disjointness(None, ["evt_01"])
        self.assertEqual(res_none["status"], "NOT_CHECKED")

        # Explicit allow_empty=True passes legitimately empty month
        res_allowed = boundaries.verify_id_disjointness([], ["evt_01"], allow_empty=True)
        self.assertEqual(res_allowed["status"], "PASS")

    # -------------------------------------------------------------------------
    # Boundary Time Window & Monotonicity Checks
    # -------------------------------------------------------------------------
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

    def test_verify_boundary_time_window_detects_inversion(self):
        res = boundaries.verify_boundary_time_window(
            "2018-09",
            "2018-09-30T23:50:00Z",
            "2018-10",
            "2018-09-30T23:40:00Z",  # Inverted timestamps
        )
        self.assertEqual(res["status"], "FAIL")
        self.assertTrue(any("TIMESTAMP_INVERSION" in v for v in res["violations"]))

    def test_verify_boundary_time_window_missing_timestamp_returns_not_checked(self):
        res = boundaries.verify_boundary_time_window("2008-05", None, "2008-06", None)
        self.assertEqual(res["status"], "NOT_CHECKED")
        self.assertIn("MISSING_BOUNDARY_TIMESTAMPS", res["violations"])

    # -------------------------------------------------------------------------
    # End-to-End Boundary Verification Fail-Closed Tests
    # -------------------------------------------------------------------------
    def test_verify_all_boundaries_no_input_returns_not_checked(self):
        """Calling verify_all_boundaries with no inputs returns NOT_CHECKED and PENDING_CATALOG_MERGE."""
        report = boundaries.verify_all_boundaries(None)
        self.assertEqual(report["status"], "NOT_CHECKED")
        self.assertEqual(report["gate_status"], "PENDING_CATALOG_MERGE")
        self.assertEqual(len(report["boundaries"]), 3)
        for b in report["boundaries"]:
            self.assertEqual(b["status"], "NOT_CHECKED")

    def test_verify_all_boundaries_missing_one_boundary_returns_not_checked(self):
        """When only 2 of 3 boundaries are provided, overall status is NOT_CHECKED."""
        partial_data = {
            "Boundary 1 (Base -> HoaiTam)": {
                "prev_ids": ["b_01"],
                "next_ids": ["ht_01"],
                "max_prev_time_utc": "2008-05-31T23:00:00Z",
                "min_next_time_utc": "2008-06-01T01:00:00Z",
            },
            "Boundary 2 (HoaiTam -> ThanhTris)": {
                "prev_ids": ["ht_02"],
                "next_ids": ["tt_01"],
                "max_prev_time_utc": "2013-07-31T23:00:00Z",
                "min_next_time_utc": "2013-08-01T01:00:00Z",
            },
        }
        report = boundaries.verify_all_boundaries(partial_data)
        self.assertEqual(report["status"], "NOT_CHECKED")
        b3 = [b for b in report["boundaries"] if "Boundary 3" in b["boundary"]][0]
        self.assertEqual(b3["status"], "NOT_CHECKED")

    def test_verify_all_boundaries_full_valid_inputs_passes(self):
        """When all 3 boundaries and 4 manifests are clean and verified, returns PASS."""
        valid_data = {
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
        m1 = b'{"package": "pkg1", "rows": 1064869}'
        m2 = b'{"package": "pkg2", "rows": 381049}'
        m3 = b'{"package": "pkg3", "rows": 400000}'
        m4 = b'{"package": "pkg4", "rows": 420000}'
        manifests = [
            {"package_id": "pkg1", "start_month": "2000-01", "end_month": "2008-05", "covered_months": boundaries.expand_month_range("2000-01", "2008-05"), "row_count": 1064869, "manifest_bytes": m1, "manifest_sha256": hashlib.sha256(m1).hexdigest()},
            {"package_id": "pkg2", "start_month": "2008-06", "end_month": "2013-07", "covered_months": boundaries.expand_month_range("2008-06", "2013-07"), "row_count": 381049, "manifest_bytes": m2, "manifest_sha256": hashlib.sha256(m2).hexdigest()},
            {"package_id": "pkg3", "start_month": "2013-08", "end_month": "2018-09", "covered_months": boundaries.expand_month_range("2013-08", "2018-09"), "row_count": 400000, "manifest_bytes": m3, "manifest_sha256": hashlib.sha256(m3).hexdigest()},
            {"package_id": "pkg4", "start_month": "2018-10", "end_month": "2023-12", "covered_months": boundaries.expand_month_range("2018-10", "2023-12"), "row_count": 420000, "manifest_bytes": m4, "manifest_sha256": hashlib.sha256(m4).hexdigest()},
        ]
        report = boundaries.verify_all_boundaries(boundary_records=valid_data, package_manifests=manifests)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["gate_status"], "VERIFIED_PASS")
        self.assertEqual(report["contiguity"]["status"], "PASS")
        self.assertEqual(len(report["boundaries"]), 3)

    def test_cli_no_input_exits_with_not_checked_code(self):
        """CLI without arguments must return non-zero (code 2) and print NOT_CHECKED message."""
        with patch("sys.argv", ["verify-catalog-boundaries.py"]):
            code = boundaries.main()
            self.assertEqual(code, 2)


    # -------------------------------------------------------------------------
    # Regressions for HoaiTam Review (Head 26e8e92)
    # -------------------------------------------------------------------------
    def test_verify_all_boundaries_rejects_single_fabricated_manifest_and_empty_boundary(self):
        """Fabricated single 288-month manifest with empty boundary and allow_empty must FAIL, not VERIFIED_PASS."""
        fabricated_manifests = [
            {
                "package_id": "fabricated",
                "start_month": "2000-01",
                "end_month": "2023-12",
            }
        ]
        fabricated_boundaries = {
            "Boundary 1 (Base -> HoaiTam)": {"prev_ids": [], "next_ids": [], "allow_empty": True},
            "Boundary 2 (HoaiTam -> ThanhTris)": {"prev_ids": [], "next_ids": [], "allow_empty": True},
            "Boundary 3 (ThanhTris -> rosy179)": {"prev_ids": [], "next_ids": [], "allow_empty": True},
        }

        report = boundaries.verify_all_boundaries(
            boundary_records=fabricated_boundaries,
            package_manifests=fabricated_manifests,
        )

        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["gate_status"], "PENDING_CATALOG_MERGE")
        self.assertNotEqual(report["gate_status"], "VERIFIED_PASS")

        # Contiguity must detect unexpected package count, untrusted package id, and missing covered months
        contiguity_violations = report["contiguity"]["violations"]
        self.assertTrue(any("UNEXPECTED_PACKAGE_COUNT" in v for v in contiguity_violations))
        self.assertTrue(any("UNTRUSTED_OR_UNKNOWN_PACKAGE_ID" in v for v in contiguity_violations))

        # Boundaries must fail because allow_empty has no certified row_count=0 evidence
        for b in report["boundaries"]:
            self.assertEqual(b["disjointness"]["status"], "FAIL")
            self.assertIn("UNVERIFIED_EMPTY_INPUT", b["disjointness"]["reason"])

    def test_verify_all_boundaries_honors_certified_empty_month_from_manifest(self):
        """When manifest certifies row_count=0, allow_empty=True is permitted and passes."""
        m1 = b'{"package": "pkg1", "rows": 0}'
        m2 = b'{"package": "pkg2", "rows": 381049}'
        m3 = b'{"package": "pkg3", "rows": 400000}'
        m4 = b'{"package": "pkg4", "rows": 420000}'
        manifests = [
            {"package_id": "pkg1", "start_month": "2000-01", "end_month": "2008-05", "covered_months": boundaries.expand_month_range("2000-01", "2008-05"), "row_count": 0, "manifest_bytes": m1, "manifest_sha256": hashlib.sha256(m1).hexdigest()},
            {"package_id": "pkg2", "start_month": "2008-06", "end_month": "2013-07", "covered_months": boundaries.expand_month_range("2008-06", "2013-07"), "row_count": 381049, "manifest_bytes": m2, "manifest_sha256": hashlib.sha256(m2).hexdigest()},
            {"package_id": "pkg3", "start_month": "2013-08", "end_month": "2018-09", "covered_months": boundaries.expand_month_range("2013-08", "2018-09"), "row_count": 400000, "manifest_bytes": m3, "manifest_sha256": hashlib.sha256(m3).hexdigest()},
            {"package_id": "pkg4", "start_month": "2018-10", "end_month": "2023-12", "covered_months": boundaries.expand_month_range("2018-10", "2023-12"), "row_count": 420000, "manifest_bytes": m4, "manifest_sha256": hashlib.sha256(m4).hexdigest()},
        ]
        boundaries_data = {
            "Boundary 1 (Base -> HoaiTam)": {
                "prev_ids": [],
                "next_ids": ["ht_01"],
                "allow_empty": True,
                "max_prev_time_utc": None,
                "min_next_time_utc": "2008-06-01T01:00:00Z",
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

        report = boundaries.verify_all_boundaries(
            boundary_records=boundaries_data,
            package_manifests=manifests,
        )
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["gate_status"], "VERIFIED_PASS")
        b1 = report["boundaries"][0]
        self.assertEqual(b1["disjointness"]["status"], "PASS")

    def test_verify_manifest_contiguity_string_bytes_from_cli_json_no_type_error(self):
        """manifest_bytes as string in JSON manifests file must not raise TypeError."""
        manifest_text = '{"package": "content"}'
        expected_sha = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()

        manifests = [
            {
                "package_id": "pkg_base_101",
                "start_month": "2000-01",
                "end_month": "2008-05",
                "covered_months": boundaries.expand_month_range("2000-01", "2008-05"),
                "manifest_bytes": manifest_text,  # String from JSON CLI
                "manifest_sha256": expected_sha,
                "row_count": 100,
            },
            {
                "package_id": "pkg_hoaitam_62",
                "start_month": "2008-06",
                "end_month": "2013-07",
                "covered_months": boundaries.expand_month_range("2008-06", "2013-07"),
                "row_count": 100,
            },
            {
                "package_id": "pkg_thanhtris_62",
                "start_month": "2013-08",
                "end_month": "2018-09",
                "covered_months": boundaries.expand_month_range("2013-08", "2018-09"),
                "row_count": 100,
            },
            {
                "package_id": "pkg_rosy179_63",
                "start_month": "2018-10",
                "end_month": "2023-12",
                "covered_months": boundaries.expand_month_range("2018-10", "2023-12"),
                "row_count": 100,
            },
        ]

        res = boundaries.verify_manifest_contiguity(manifests)
        self.assertEqual(res["status"], "PASS")
        self.assertEqual(len(res["violations"]), 0)

    def test_verify_boundary_time_window_two_sided_extrema_checks(self):
        """Timestamp verification must check both extrema [min_prev, max_prev] and [min_next, max_next]."""
        # 1. min_prev underflow
        res1 = boundaries.verify_boundary_time_window(
            prev_month="2008-05",
            max_prev_dt_iso="2008-05-31T23:00:00Z",
            next_month="2008-06",
            min_next_dt_iso="2008-06-01T01:00:00Z",
            min_prev_dt_iso="2008-04-30T23:59:59Z",  # Underflow!
        )
        self.assertEqual(res1["status"], "FAIL")
        self.assertTrue(any("PREV_MONTH_UNDERFLOW" in v for v in res1["violations"]))

        # 2. max_next overflow
        res2 = boundaries.verify_boundary_time_window(
            prev_month="2008-05",
            max_prev_dt_iso="2008-05-31T23:00:00Z",
            next_month="2008-06",
            min_next_dt_iso="2008-06-01T01:00:00Z",
            max_next_dt_iso="2008-07-01T00:00:01Z",  # Overflow!
        )
        self.assertEqual(res2["status"], "FAIL")
        self.assertTrue(any("NEXT_MONTH_OVERFLOW" in v for v in res2["violations"]))

        # 3. min_prev > max_prev inversion
        res3 = boundaries.verify_boundary_time_window(
            prev_month="2008-05",
            max_prev_dt_iso="2008-05-10T12:00:00Z",
            next_month="2008-06",
            min_next_dt_iso="2008-06-01T01:00:00Z",
            min_prev_dt_iso="2008-05-20T12:00:00Z",  # min > max!
        )
        self.assertEqual(res3["status"], "FAIL")
        self.assertTrue(any("PREV_EXTREMA_INVERSION" in v for v in res3["violations"]))

        # 4. Clean two-sided extrema pass
        res4 = boundaries.verify_boundary_time_window(
            prev_month="2008-05",
            min_prev_dt_iso="2008-05-01T02:00:00Z",
            max_prev_dt_iso="2008-05-31T23:59:00Z",
            next_month="2008-06",
            min_next_dt_iso="2008-06-01T00:01:00Z",
            max_next_dt_iso="2008-06-30T23:59:00Z",
        )
        self.assertEqual(res4["status"], "PASS")
        self.assertEqual(len(res4["violations"]), 0)

    def test_verify_all_boundaries_rejects_fabricated_4_descriptors_without_sha_or_bytes(self):
        """Fabricated 4 descriptors with official package IDs/ranges but missing manifest SHA/bytes and claiming certified_empty must be rejected."""
        fabricated_manifests = [
            {
                "package_id": "catalog_base_101",
                "start_month": "2000-01",
                "end_month": "2008-05",
                "covered_months": boundaries.expand_month_range("2000-01", "2008-05"),
                "row_count": 123,
            },
            {
                "package_id": "hoaitam_62",
                "start_month": "2008-06",
                "end_month": "2013-07",
                "covered_months": boundaries.expand_month_range("2008-06", "2013-07"),
                "row_count": 123,
            },
            {
                "package_id": "thanhtris_62",
                "start_month": "2013-08",
                "end_month": "2018-09",
                "covered_months": boundaries.expand_month_range("2013-08", "2018-09"),
                "row_count": 123,
            },
            {
                "package_id": "rosy179_63",
                "start_month": "2018-10",
                "end_month": "2023-12",
                "covered_months": boundaries.expand_month_range("2018-10", "2023-12"),
                "row_count": 123,
            },
        ]
        fabricated_boundaries = {
            "Boundary 1 (Base -> HoaiTam)": {"prev_ids": [], "next_ids": [], "allow_empty": True, "certified_empty": True},
            "Boundary 2 (HoaiTam -> ThanhTris)": {"prev_ids": [], "next_ids": [], "allow_empty": True, "certified_empty": True},
            "Boundary 3 (ThanhTris -> rosy179)": {"prev_ids": [], "next_ids": [], "allow_empty": True, "certified_empty": True},
        }

        report = boundaries.verify_all_boundaries(
            boundary_records=fabricated_boundaries,
            package_manifests=fabricated_manifests,
            audit_scope="OFFICIAL_CATALOG_GATE",
        )

        self.assertNotEqual(report["gate_status"], "VERIFIED_PASS")
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["gate_status"], "PENDING_CATALOG_MERGE")
        self.assertEqual(report["contiguity"]["status"], "FAIL")
        self.assertTrue(
            any("MISSING_MANIFEST_SHA256" in v or "MISSING_MANIFEST_BYTES" in v for v in report["contiguity"]["violations"])
        )
        for b in report["boundaries"]:
            self.assertIn(b["status"], ["FAIL", "NOT_CHECKED"])


if __name__ == "__main__":
    unittest.main()
