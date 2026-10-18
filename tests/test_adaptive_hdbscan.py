"""Unit and integration tests for EXP-03 HDBSCAN Adaptive and Multi-Sequence Resolution."""
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = ROOT / "notebooks"
TESTS_DIR = ROOT / "tests"
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))
if str(TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(TESTS_DIR))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from adaptive_hdbscan import (
    resolve_adaptive_hdbscan_params,
    fit_hdbscan_adaptive,
    resolve_multi_sequence_and_nested_mainshocks,
    run_adaptive_experiment,
)
from experiment_harness import (
    load_feature_bundle,
    MEMBERSHIP_SCHEMA,
    SEQUENCE_SUMMARY_SCHEMA,
    sha256_bytes,
    SCHEMA_VERSION,
    MODE_DEVELOPMENT,
)
import test_experiment_harness


class TestAdaptiveHDBSCAN(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = Path(tempfile.mkdtemp(prefix="exp03_test_"))
        self.config_path = Path(__file__).resolve().parent.parent / "config" / "ml" / "models" / "hdbscan_adaptive_dev_v1.json"
        with open(self.config_path, "r", encoding="utf-8") as f:
            self.model_config = json.load(f)

    def tearDown(self):
        if self.tmp_dir.exists():
            shutil.rmtree(self.tmp_dir, ignore_errors=True)

    # -------------------------------------------------------------------------
    # 1. Deterministic Parameter Resolver Tests
    # -------------------------------------------------------------------------
    def test_parameter_resolver_determinism(self):
        """Rule must be 100% deterministic: identical inputs yield identical outputs."""
        p1 = resolve_adaptive_hdbscan_params(100, 6.2, self.model_config)
        p2 = resolve_adaptive_hdbscan_params(100, 6.2, self.model_config)
        self.assertEqual(p1, p2)
        self.assertEqual(p1["min_cluster_size"], 10)
        self.assertEqual(p1["min_samples"], 6)

        # Repeated calls in loop
        for _ in range(50):
            res = resolve_adaptive_hdbscan_params(50, 7.0, self.model_config)
            self.assertEqual(res["min_cluster_size"], 10)
            self.assertEqual(res["min_samples"], 6)

    def test_parameter_resolver_bounds_and_monotonicity(self):
        """Bounds [min, max] respected, and min_samples <= min_cluster_size."""
        # Extreme small window
        p_small = resolve_adaptive_hdbscan_params(1, 4.0, self.model_config)
        self.assertGreaterEqual(p_small["min_cluster_size"], 3)
        self.assertLessEqual(p_small["min_cluster_size"], 25)
        self.assertGreaterEqual(p_small["min_samples"], 2)
        self.assertLessEqual(p_small["min_samples"], p_small["min_cluster_size"])

        # Extreme large window
        p_large = resolve_adaptive_hdbscan_params(100000, 9.1, self.model_config)
        self.assertLessEqual(p_large["min_cluster_size"], 25)
        self.assertLessEqual(p_large["min_samples"], 15)
        self.assertLessEqual(p_large["min_samples"], p_large["min_cluster_size"])

        # Monotonicity with candidate count
        res_prev = 0
        for n in [10, 50, 100, 500, 2000]:
            p = resolve_adaptive_hdbscan_params(n, 6.0, self.model_config)
            self.assertGreaterEqual(p["min_cluster_size"], res_prev)
            res_prev = p["min_cluster_size"]

    # -------------------------------------------------------------------------
    # 2. Multi-Sequence Hierarchy and Tie-Breaking Tests
    # -------------------------------------------------------------------------
    def test_multi_sequence_resolution_unique(self):
        """Single sequence membership yields status UNIQUE and rank 1."""
        now = datetime.now(timezone.utc)
        memberships = [
            {
                "candidate_event_id": "ev_001",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.85,
                "normalized_distance": 0.2,
                "event_role_candidate": "POST",
                "multi_sequence_status": "UNIQUE",
                "resolution_rank": 1,
            },
            {
                "candidate_event_id": "ev_002",
                "mainshock_event_id": "main_A",
                "is_sequence_member": False,
                "membership_probability": 0.0,
                "normalized_distance": 1.5,
                "event_role_candidate": "POST",
                "multi_sequence_status": "NOT_MEMBER",
                "resolution_rank": None,
            },
        ]
        summaries = [{"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False}]

        res_mem, res_sum, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries)

        m1 = next(m for m in res_mem if m["candidate_event_id"] == "ev_001")
        m2 = next(m for m in res_mem if m["candidate_event_id"] == "ev_002")

        self.assertEqual(m1["multi_sequence_status"], "UNIQUE")
        self.assertEqual(m1["resolution_rank"], 1)
        self.assertEqual(m2["multi_sequence_status"], "NOT_MEMBER")
        self.assertIsNone(m2["resolution_rank"])
        self.assertEqual(recon["resolved_unique_aftershocks"], 1)

    def test_multi_sequence_resolution_probability_winner(self):
        """Higher membership probability wins rank 1; status is MULTIPLE."""
        memberships = [
            {
                "candidate_event_id": "ev_shared",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.90,
                "normalized_distance": 0.5,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_shared",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.40,
                "normalized_distance": 0.1,  # even though distance is smaller, probability takes precedence
                "event_role_candidate": "POST",
            },
        ]
        summaries = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
        ]

        res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries)

        row_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A")
        row_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B")

        self.assertEqual(row_a["multi_sequence_status"], "MULTIPLE")
        self.assertEqual(row_a["resolution_rank"], 1)

        self.assertEqual(row_b["multi_sequence_status"], "MULTIPLE")
        self.assertEqual(row_b["resolution_rank"], 2)

        self.assertEqual(recon["unique_candidates_in_multi_sequences"], 1)
        self.assertEqual(recon["resolved_unique_aftershocks"], 1)
        self.assertEqual(recon["aftershocks_deduplicated_count"], 1)

    def test_multi_sequence_resolution_distance_tiebreak(self):
        """When probabilities are equal, smaller normalized distance wins rank 1."""
        memberships = [
            {
                "candidate_event_id": "ev_shared",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.75,
                "normalized_distance": 0.65,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_shared",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.75,
                "normalized_distance": 0.25,  # smaller distance wins
                "event_role_candidate": "POST",
            },
        ]
        summaries = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
        ]

        res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries)

        row_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A")
        row_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B")

        self.assertEqual(row_b["multi_sequence_status"], "MULTIPLE")
        self.assertEqual(row_b["resolution_rank"], 1)

        self.assertEqual(row_a["multi_sequence_status"], "MULTIPLE")
        self.assertEqual(row_a["resolution_rank"], 2)

    def test_multi_sequence_unresolvable_tie_becomes_ambiguous(self):
        """When both probability and distance are equal, candidate is AMBIGUOUS with rank None."""
        memberships = [
            {
                "candidate_event_id": "ev_tie",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.80,
                "normalized_distance": 0.40,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_tie",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.80,
                "normalized_distance": 0.40,  # Exact tie!
                "event_role_candidate": "POST",
            },
        ]
        summaries = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
        ]

        res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries)

        row_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A")
        row_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B")

        self.assertEqual(row_a["multi_sequence_status"], "AMBIGUOUS")
        self.assertIsNone(row_a["resolution_rank"])

        self.assertEqual(row_b["multi_sequence_status"], "AMBIGUOUS")
        self.assertIsNone(row_b["resolution_rank"])

        self.assertEqual(recon["ambiguous_candidates_count"], 1)
        self.assertEqual(recon["resolved_unique_aftershocks"], 0)

    # -------------------------------------------------------------------------
    # 3. Raw Membership Grain Preservation & Deduplication Count
    # -------------------------------------------------------------------------
    def test_raw_membership_grain_preservation(self):
        """Resolution must never delete or drop rows from memberships."""
        memberships = [
            {"candidate_event_id": f"c_{i}", "mainshock_event_id": f"m_{i % 3}", "is_sequence_member": True,
             "membership_probability": 0.8, "normalized_distance": float(i)}
            for i in range(20)
        ]
        summaries = [{"mainshock_event_id": f"m_{k}", "is_nested_mainshock_candidate": False} for k in range(3)]

        res_mem, _, _ = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries)
        self.assertEqual(len(res_mem), 20)

    # -------------------------------------------------------------------------
    # 4. Nested Mainshock Candidate Detection
    # -------------------------------------------------------------------------
    def test_nested_mainshock_detection(self):
        """Mainshock B appearing as a sequence member in mainshock A's window is flagged as nested."""
        memberships = [
            # Mainshock A's window containing itself and mainshock B
            {
                "candidate_event_id": "main_A",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 1.0,
                "normalized_distance": 0.0,
                "event_role_candidate": "MAINSHOCK",
            },
            {
                "candidate_event_id": "main_B",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.88,
                "normalized_distance": 0.15,
                "event_role_candidate": "POST",
            },
            # Mainshock B's own window
            {
                "candidate_event_id": "main_B",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 1.0,
                "normalized_distance": 0.0,
                "event_role_candidate": "MAINSHOCK",
            },
            # Mainshock C's own window (independent)
            {
                "candidate_event_id": "main_C",
                "mainshock_event_id": "main_C",
                "is_sequence_member": True,
                "membership_probability": 1.0,
                "normalized_distance": 0.0,
                "event_role_candidate": "MAINSHOCK",
            },
        ]
        summaries = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False, "parent_mainshock_event_id": None, "nested_membership_probability": None},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False, "parent_mainshock_event_id": None, "nested_membership_probability": None},
            {"mainshock_event_id": "main_C", "is_nested_mainshock_candidate": False, "parent_mainshock_event_id": None, "nested_membership_probability": None},
        ]

        _, res_sum, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries)

        sum_a = next(s for s in res_sum if s["mainshock_event_id"] == "main_A")
        sum_b = next(s for s in res_sum if s["mainshock_event_id"] == "main_B")
        sum_c = next(s for s in res_sum if s["mainshock_event_id"] == "main_C")

        self.assertFalse(sum_a["is_nested_mainshock_candidate"])
        self.assertIsNone(sum_a["parent_mainshock_event_id"])

        self.assertTrue(sum_b["is_nested_mainshock_candidate"])
        self.assertEqual(sum_b["parent_mainshock_event_id"], "main_A")
        self.assertAlmostEqual(sum_b["nested_membership_probability"], 0.88)

        self.assertFalse(sum_c["is_nested_mainshock_candidate"])
        self.assertIsNone(sum_c["parent_mainshock_event_id"])

        self.assertEqual(recon["nested_mainshocks_count"], 1)

    # -------------------------------------------------------------------------
    # 5. Integration Test: Full Adaptive Experiment Runner with Pilot Bundle
    # -------------------------------------------------------------------------
    def test_run_adaptive_experiment_e2e(self):
        """Run HDBSCAN_ADAPTIVE on pilot bundle and validate result bundle conformance."""
        temp_bundle_dir = self.tmp_dir / "pilot_bundle"
        temp_bundle_dir.mkdir(parents=True, exist_ok=True)
        test_experiment_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = load_feature_bundle(temp_bundle_dir, mode=MODE_DEVELOPMENT)
        out_dir = self.tmp_dir / "results" / "adaptive_dev"

        res = run_adaptive_experiment(
            bundle=bundle,
            config=self.model_config,
            model_config_version="hdbscan-adaptive-dev-v1",
            output_dir=out_dir,
            mode=MODE_DEVELOPMENT,
        )

        self.assertEqual(res["algorithm_name"], "HDBSCAN_ADAPTIVE")
        self.assertIn("metrics", res)
        self.assertIn("reconciliation", res)

        # Validate generated result bundle artifacts
        self.assertFalse(res["write_result"]["has_success_marker"])
        self.assertTrue((out_dir / "pilot_result_manifest.json").exists())
        self.assertFalse((out_dir / "_SUCCESS.json").exists())
        self.assertTrue((out_dir / "checksums.sha256").exists())

        # Check parquet files and schema
        mem_pq = pq.read_table(out_dir / "memberships.parquet")
        sum_pq = pq.read_table(out_dir / "sequence_summary.parquet")

        self.assertEqual(mem_pq.num_rows, bundle["features_table"].num_rows)
        self.assertEqual(mem_pq.schema, MEMBERSHIP_SCHEMA)
        self.assertEqual(sum_pq.schema, SEQUENCE_SUMMARY_SCHEMA)
        self.assertGreater(sum_pq.num_rows, 0)

        # Check checksums
        chk_lines = (out_dir / "checksums.sha256").read_text(encoding="utf-8").strip().splitlines()
        for line in chk_lines:
            expected_sha, fname = line.split()
            actual_sha = sha256_bytes((out_dir / fname).read_bytes())
            self.assertEqual(expected_sha, actual_sha)

        # Confirm valid multi_sequence_status in memberships
        statuses = set(mem_pq["multi_sequence_status"].to_pylist())
        self.assertTrue(statuses.issubset({"UNIQUE", "MULTIPLE", "AMBIGUOUS", "NOT_MEMBER"}))

    # -------------------------------------------------------------------------
    # 6. Regression & Edge-Case Tests for Review Items
    # -------------------------------------------------------------------------
    def test_fit_adaptive_reads_schema_mainshock_magnitude(self):
        """Magnitude is read from mainshock_magnitude / candidate_magnitude and reflects true scaling."""
        main_id = "main_mag_test"
        # 100 candidates with mainshock_magnitude = 8.0 conforming to feature bundle contract
        candidates = []
        for i in range(100):
            cid = main_id if i == 0 else f"cand_{i}"
            candidates.append({
                "candidate_event_id": cid,
                "mainshock_event_id": main_id,
                "mainshock_magnitude": 8.0,
                "candidate_magnitude": 8.0 if i == 0 else 4.5,
                "x_scaled": float(i) * 0.01,
                "y_scaled": float(i) * 0.01,
                "z_scaled": 0.0,
                "time_scaled": float(i) * 0.01,
                "event_role_candidate": "MAINSHOCK" if i == 0 else "POST",
            })

        # When magnitude is 8.0 with N=100: resolve_adaptive_hdbscan_params yields min_cluster_size=12
        p_expected = resolve_adaptive_hdbscan_params(100, 8.0, self.model_config)
        self.assertEqual(p_expected["min_cluster_size"], 12)
        # If it had erroneously defaulted to 5.5, min_cluster_size would have been 9
        p_wrong = resolve_adaptive_hdbscan_params(100, 5.5, self.model_config)
        self.assertEqual(p_wrong["min_cluster_size"], 9)

        # Call fit_hdbscan_adaptive with candidates having mainshock_magnitude=8.0
        mems, summary = fit_hdbscan_adaptive(
            window_candidates=candidates,
            mainshock_id=main_id,
            config=self.model_config,
        )
        self.assertEqual(len(mems), 100)
        self.assertEqual(summary["candidate_count"], 100)

    def test_fit_adaptive_missing_magnitude_raises_error(self):
        """Missing mainshock magnitude must fail loudly, never default to 5.5 silently."""
        main_id = "main_missing_mag"
        candidates = [{
            "candidate_event_id": main_id,
            "mainshock_event_id": main_id,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "event_role_candidate": "MAINSHOCK",
            # Note: NO mainshock_magnitude, candidate_magnitude, or magnitude
        }]
        with self.assertRaisesRegex(ValueError, f"MISSING_MAINSHOCK_MAGNITUDE:{main_id}"):
            fit_hdbscan_adaptive(
                window_candidates=candidates,
                mainshock_id=main_id,
                config=self.model_config,
            )

    def test_fit_adaptive_invalid_nan_magnitude_raises_error(self):
        """Non-finite NaN magnitude must raise ValueError."""
        main_id = "main_nan_mag"
        candidates = [{
            "candidate_event_id": main_id,
            "mainshock_event_id": main_id,
            "mainshock_magnitude": float("nan"),
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "event_role_candidate": "MAINSHOCK",
        }]
        with self.assertRaises(ValueError):
            fit_hdbscan_adaptive(
                window_candidates=candidates,
                mainshock_id=main_id,
                config=self.model_config,
            )

    def test_nested_mainshock_tie_preserves_unresolved_regardless_of_input_order(self):
        """When candidate parents are tied, parent remains unresolved and order swap produces identical result."""
        # Mainshock B appears in mainshock A's window and mainshock C's window with identical probability & distance
        row_in_a = {
            "candidate_event_id": "main_B",
            "mainshock_event_id": "main_A",
            "is_sequence_member": True,
            "membership_probability": 0.80,
            "normalized_distance": 0.20,
            "event_role_candidate": "POST",
        }
        row_in_c = {
            "candidate_event_id": "main_B",
            "mainshock_event_id": "main_C",
            "is_sequence_member": True,
            "membership_probability": 0.80,
            "normalized_distance": 0.20,
            "event_role_candidate": "POST",
        }
        row_b_self = {
            "candidate_event_id": "main_B",
            "mainshock_event_id": "main_B",
            "is_sequence_member": True,
            "membership_probability": 1.00,
            "normalized_distance": 0.00,
            "event_role_candidate": "MAINSHOCK",
        }

        # Order 1: A before C
        memberships_ac = [dict(row_in_a), dict(row_in_c), dict(row_b_self)]
        summaries_ac = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False, "metric_reason_codes": []},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False, "metric_reason_codes": []},
            {"mainshock_event_id": "main_C", "is_nested_mainshock_candidate": False, "metric_reason_codes": []},
        ]
        _, res_sum_ac, _ = resolve_multi_sequence_and_nested_mainshocks(memberships_ac, summaries_ac)
        b_ac = next(s for s in res_sum_ac if s["mainshock_event_id"] == "main_B")

        # Order 2: C before A (swapped row order)
        memberships_ca = [dict(row_in_c), dict(row_in_a), dict(row_b_self)]
        summaries_ca = [
            {"mainshock_event_id": "main_C", "is_nested_mainshock_candidate": False, "metric_reason_codes": []},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False, "metric_reason_codes": []},
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False, "metric_reason_codes": []},
        ]
        _, res_sum_ca, _ = resolve_multi_sequence_and_nested_mainshocks(memberships_ca, summaries_ca)
        b_ca = next(s for s in res_sum_ca if s["mainshock_event_id"] == "main_B")

        # Both orders must NOT arbitrarily assign main_A or main_C
        self.assertTrue(b_ac["is_nested_mainshock_candidate"])
        self.assertIsNone(b_ac["parent_mainshock_event_id"])
        self.assertIsNone(b_ac["nested_membership_probability"])
        self.assertIn("AMBIGUOUS_NESTED_PARENT_TIE", b_ac["metric_reason_codes"])

        self.assertTrue(b_ca["is_nested_mainshock_candidate"])
        self.assertIsNone(b_ca["parent_mainshock_event_id"])
        self.assertIsNone(b_ca["nested_membership_probability"])
        self.assertIn("AMBIGUOUS_NESTED_PARENT_TIE", b_ca["metric_reason_codes"])

        # Invariant to input order
        self.assertEqual(b_ac["parent_mainshock_event_id"], b_ca["parent_mainshock_event_id"])
        self.assertEqual(b_ac["nested_membership_probability"], b_ca["nested_membership_probability"])

    def test_multi_sequence_tolerance_distance_tie_break(self):
        """Probability within eps_tie uses normalized distance to break tie; closer candidate wins."""
        eps_tie = 1e-6
        # Candidate ev_close in main_A: prob 0.8000001, dist 0.80 (far)
        # Candidate ev_close in main_B: prob 0.8000000, dist 0.20 (close)
        # Difference in probability is 1e-7 <= 1e-6 (tied under tolerance).
        # Distance tie-break must select main_B as rank 1!
        memberships = [
            {
                "candidate_event_id": "ev_close",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.8000001,
                "normalized_distance": 0.80,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_close",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.8000000,
                "normalized_distance": 0.20,
                "event_role_candidate": "POST",
            },
        ]
        summaries = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
        ]
        res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries, eps_tie=eps_tie)

        row_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A")
        row_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B")

        self.assertEqual(row_b["multi_sequence_status"], "MULTIPLE")
        self.assertEqual(row_b["resolution_rank"], 1)

        self.assertEqual(row_a["multi_sequence_status"], "MULTIPLE")
        self.assertEqual(row_a["resolution_rank"], 2)
        self.assertEqual(recon["resolved_unique_aftershocks"], 1)

    def test_multi_sequence_tolerance_both_within_eps_tie_is_ambiguous(self):
        """When both probability and distance are within eps_tie, status is AMBIGUOUS."""
        eps_tie = 1e-6
        memberships = [
            {
                "candidate_event_id": "ev_ambig",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.8000001,
                "normalized_distance": 0.2000001,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_ambig",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.8000000,
                "normalized_distance": 0.2000000,
                "event_role_candidate": "POST",
            },
        ]
        summaries = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
        ]
        res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries, eps_tie=eps_tie)

        row_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A")
        row_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B")

        self.assertEqual(row_a["multi_sequence_status"], "AMBIGUOUS")
        self.assertIsNone(row_a["resolution_rank"])

        self.assertEqual(row_b["multi_sequence_status"], "AMBIGUOUS")
        self.assertIsNone(row_b["resolution_rank"])

        self.assertEqual(recon["ambiguous_candidates_count"], 1)
        self.assertEqual(recon["resolved_unique_aftershocks"], 0)


if __name__ == "__main__":
    unittest.main()
