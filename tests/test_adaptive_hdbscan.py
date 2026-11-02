"""Unit and integration tests for EXP-03 HDBSCAN Adaptive and Multi-Sequence Resolution."""
from datetime import datetime, timezone
import importlib.util
import itertools
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

spec_inspect = importlib.util.spec_from_file_location("inspect_exp03", ROOT / "scripts/inspect-exp03-notebook.py")
inspect_exp03 = importlib.util.module_from_spec(spec_inspect)
spec_inspect.loader.exec_module(inspect_exp03)

from adaptive_hdbscan import (
    find_best_membership_row,
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

    # -------------------------------------------------------------------------
    # 7. Order-Invariance Permutation Tests (>= 3 Sequences)
    # -------------------------------------------------------------------------
    def test_find_best_membership_row_3_sequences_counterexample_permutations(self):
        """HoaiTam counterexample where pairwise comparison violates transitivity.

        Candidates:
          A: prob = 0.8000000, dist = 0.1
          B: prob = 0.80000075, dist = 0.2
          C: prob = 0.8000015, dist = 0.3
        With eps_tie = 1e-6:
          p_max = 0.8000015 (C).
          Candidates within eps_tie of p_max: B (diff = 7.5e-7 <= 1e-6) and C (diff = 0).
          Within {B, C}, minimum distance is 0.2 (B).
          Candidate B is strictly within [0.2, 0.2 + 1e-6]; C has distance 0.3 > 0.2 + 1e-6.
          Therefore, candidate B is the unique winner across all 6 permutations!
        """
        eps_tie = 1e-6
        cand_a = {"mainshock_event_id": "main_A", "membership_probability": 0.8000000, "normalized_distance": 0.1}
        cand_b = {"mainshock_event_id": "main_B", "membership_probability": 0.80000075, "normalized_distance": 0.2}
        cand_c = {"mainshock_event_id": "main_C", "membership_probability": 0.8000015, "normalized_distance": 0.3}

        candidates = [cand_a, cand_b, cand_c]
        permutations = list(itertools.permutations(candidates))
        self.assertEqual(len(permutations), 6)

        for perm in permutations:
            with self.subTest(order=[c["mainshock_event_id"] for c in perm]):
                winner, is_tie = find_best_membership_row(list(perm), eps_tie=eps_tie)
                self.assertFalse(is_tie)
                self.assertIsNotNone(winner)
                self.assertEqual(winner["mainshock_event_id"], "main_B")

    def test_find_best_membership_row_3_sequences_unresolvable_tie_permutations(self):
        """When 3 candidates tie on both probability and distance within tolerance.

        All 6 permutations must yield is_tie=True and winner=None without arbitrary picking.
        """
        eps_tie = 1e-6
        cand_a = {"mainshock_event_id": "main_A", "membership_probability": 0.8000000, "normalized_distance": 0.2000000}
        cand_b = {"mainshock_event_id": "main_B", "membership_probability": 0.8000001, "normalized_distance": 0.2000001}
        cand_c = {"mainshock_event_id": "main_C", "membership_probability": 0.8000002, "normalized_distance": 0.2000002}

        candidates = [cand_a, cand_b, cand_c]
        for perm in itertools.permutations(candidates):
            with self.subTest(order=[c["mainshock_event_id"] for c in perm]):
                winner, is_tie = find_best_membership_row(list(perm), eps_tie=eps_tie)
                self.assertTrue(is_tie)
                self.assertIsNone(winner)

    def test_multi_sequence_resolution_3_sequences_all_permutations(self):
        """Full multi-sequence resolution across all 6 permutations of 3 sequences.

        Input order must never alter the resolved ranks or winner identity.
        """
        eps_tie = 1e-6
        base_rows = [
            {
                "candidate_event_id": "ev_multi_3",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.8000000,
                "normalized_distance": 0.1,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_multi_3",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.80000075,
                "normalized_distance": 0.2,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_multi_3",
                "mainshock_event_id": "main_C",
                "is_sequence_member": True,
                "membership_probability": 0.8000015,
                "normalized_distance": 0.3,
                "event_role_candidate": "POST",
            },
        ]

        for perm in itertools.permutations(base_rows):
            summaries = [
                {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_C", "is_nested_mainshock_candidate": False},
            ]
            # Deep-copy rows for each permutation
            mem_input = [dict(r) for r in perm]
            res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(mem_input, summaries, eps_tie=eps_tie)

            row_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A")
            row_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B")
            row_c = next(m for m in res_mem if m["mainshock_event_id"] == "main_C")

            order_str = "->".join(m["mainshock_event_id"] for m in perm)
            with self.subTest(order=order_str):
                # B must win rank 1 in every permutation
                self.assertEqual(row_b["multi_sequence_status"], "MULTIPLE")
                self.assertEqual(row_b["resolution_rank"], 1)

                # C has next highest probability -> rank 2
                self.assertEqual(row_c["multi_sequence_status"], "MULTIPLE")
                self.assertEqual(row_c["resolution_rank"], 2)

                # A has lowest probability -> rank 3
                self.assertEqual(row_a["multi_sequence_status"], "MULTIPLE")
                self.assertEqual(row_a["resolution_rank"], 3)

                self.assertEqual(recon["resolved_unique_aftershocks"], 1)
                self.assertEqual(recon["ambiguous_candidates_count"], 0)

    def test_multi_sequence_resolution_3_sequences_tie_all_permutations(self):
        """Full multi-sequence resolution with 3-way tie across all 6 permutations.

        Must result in AMBIGUOUS and resolution_rank=None across all permutations.
        """
        eps_tie = 1e-6
        base_rows = [
            {
                "candidate_event_id": "ev_tie_3",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.8500000,
                "normalized_distance": 0.2000000,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_tie_3",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.8500001,
                "normalized_distance": 0.2000001,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "ev_tie_3",
                "mainshock_event_id": "main_C",
                "is_sequence_member": True,
                "membership_probability": 0.8500002,
                "normalized_distance": 0.2000002,
                "event_role_candidate": "POST",
            },
        ]

        for perm in itertools.permutations(base_rows):
            summaries = [
                {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_C", "is_nested_mainshock_candidate": False},
            ]
            mem_input = [dict(r) for r in perm]
            res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(mem_input, summaries, eps_tie=eps_tie)

            order_str = "->".join(m["mainshock_event_id"] for m in perm)
            with self.subTest(order=order_str):
                for m in res_mem:
                    self.assertEqual(m["multi_sequence_status"], "AMBIGUOUS")
                    self.assertIsNone(m["resolution_rank"])

                self.assertEqual(recon["resolved_unique_aftershocks"], 0)
                self.assertEqual(recon["ambiguous_candidates_count"], 1)

    def test_nested_parent_resolution_3_sequences_all_permutations(self):
        """Nested parent resolution with >= 3 candidate parents across all permutations.

        Mainshock main_X appears as sequence member in main_A, main_B, main_C.
        In main_A: prob = 0.8000000, dist = 0.1
        In main_B: prob = 0.80000075, dist = 0.2
        In main_C: prob = 0.8000015, dist = 0.3
        With eps_tie = 1e-6:
          Global resolution selects main_B as the winning parent across all 6 permutations.
        """
        eps_tie = 1e-6
        base_other_memberships = [
            {
                "candidate_event_id": "main_X",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.8000000,
                "normalized_distance": 0.1,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "main_X",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.80000075,
                "normalized_distance": 0.2,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "main_X",
                "mainshock_event_id": "main_C",
                "is_sequence_member": True,
                "membership_probability": 0.8000015,
                "normalized_distance": 0.3,
                "event_role_candidate": "POST",
            },
        ]

        for perm in itertools.permutations(base_other_memberships):
            # Include main_X's own window
            memberships = [
                {
                    "candidate_event_id": "main_X",
                    "mainshock_event_id": "main_X",
                    "is_sequence_member": True,
                    "membership_probability": 1.0,
                    "normalized_distance": 0.0,
                    "event_role_candidate": "MAINSHOCK",
                }
            ] + [dict(r) for r in perm]

            summaries = [
                {"mainshock_event_id": "main_X", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_C", "is_nested_mainshock_candidate": False},
            ]

            _, res_sum, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries, eps_tie=eps_tie)
            sum_x = next(s for s in res_sum if s["mainshock_event_id"] == "main_X")

            order_str = "->".join(m["mainshock_event_id"] for m in perm)
            with self.subTest(order=order_str):
                self.assertTrue(sum_x["is_nested_mainshock_candidate"])
                self.assertEqual(sum_x["parent_mainshock_event_id"], "main_B")
                self.assertAlmostEqual(sum_x["nested_membership_probability"], 0.80000075)

    def test_nested_parent_resolution_3_sequences_tie_all_permutations(self):
        """Nested parent resolution with 3-way tie across all 6 permutations.

        When all 3 candidate parents tie within eps_tie, parent_mainshock_event_id
        must be None and reason code AMBIGUOUS_NESTED_PARENT_TIE added.
        """
        eps_tie = 1e-6
        base_other_memberships = [
            {
                "candidate_event_id": "main_Y",
                "mainshock_event_id": "main_A",
                "is_sequence_member": True,
                "membership_probability": 0.8000000,
                "normalized_distance": 0.2000000,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "main_Y",
                "mainshock_event_id": "main_B",
                "is_sequence_member": True,
                "membership_probability": 0.8000001,
                "normalized_distance": 0.2000001,
                "event_role_candidate": "POST",
            },
            {
                "candidate_event_id": "main_Y",
                "mainshock_event_id": "main_C",
                "is_sequence_member": True,
                "membership_probability": 0.8000002,
                "normalized_distance": 0.2000002,
                "event_role_candidate": "POST",
            },
        ]

        for perm in itertools.permutations(base_other_memberships):
            memberships = [
                {
                    "candidate_event_id": "main_Y",
                    "mainshock_event_id": "main_Y",
                    "is_sequence_member": True,
                    "membership_probability": 1.0,
                    "normalized_distance": 0.0,
                    "event_role_candidate": "MAINSHOCK",
                }
            ] + [dict(r) for r in perm]

            summaries = [
                {"mainshock_event_id": "main_Y", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_C", "is_nested_mainshock_candidate": False},
            ]

            _, res_sum, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries, eps_tie=eps_tie)
            sum_y = next(s for s in res_sum if s["mainshock_event_id"] == "main_Y")

            order_str = "->".join(m["mainshock_event_id"] for m in perm)
            with self.subTest(order=order_str):
                self.assertTrue(sum_y["is_nested_mainshock_candidate"])
                self.assertIsNone(sum_y["parent_mainshock_event_id"])
                self.assertIsNone(sum_y["nested_membership_probability"])
                self.assertIn("AMBIGUOUS_NESTED_PARENT_TIE", sum_y["metric_reason_codes"])

    # -------------------------------------------------------------------------
    # 8. Workflow Regression: Consecutive Runs & Retry Idempotency
    # -------------------------------------------------------------------------
    def test_run_adaptive_experiment_subfolder_and_retry(self):
        """run_adaptive_experiment supports subfolder_by_run_id and safe retry verification."""
        temp_bundle_dir = self.tmp_dir / "pilot_bundle_subfolder"
        temp_bundle_dir.mkdir(parents=True, exist_ok=True)
        test_experiment_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = load_feature_bundle(temp_bundle_dir, mode=MODE_DEVELOPMENT)

        out_root = self.tmp_dir / "results_subfolder"
        out_root.mkdir(parents=True, exist_ok=True)

        # 1. Run with subfolder_by_run_id=True
        res1 = run_adaptive_experiment(
            bundle=bundle,
            config=self.model_config,
            model_config_version="hdbscan-adaptive-dev-v1",
            output_dir=out_root,
            mode=MODE_DEVELOPMENT,
            subfolder_by_run_id=True,
        )
        run_id_1 = res1["run_id"]
        dir_1 = Path(res1["output_dir"])
        self.assertEqual(dir_1, out_root / run_id_1)
        self.assertTrue((dir_1 / "pilot_result_manifest.json").is_file())
        self.assertFalse(res1["write_result"].get("is_reused", False))

        # 2. Second run with same root generates new subfolder without conflict
        res2 = run_adaptive_experiment(
            bundle=bundle,
            config=self.model_config,
            model_config_version="hdbscan-adaptive-dev-v1",
            output_dir=out_root,
            mode=MODE_DEVELOPMENT,
            subfolder_by_run_id=True,
        )
        run_id_2 = res2["run_id"]
        dir_2 = Path(res2["output_dir"])
        self.assertNotEqual(run_id_1, run_id_2)
        self.assertNotEqual(dir_1, dir_2)
        self.assertTrue((dir_2 / "pilot_result_manifest.json").is_file())
        self.assertTrue((dir_1 / "pilot_result_manifest.json").is_file())

        # 3. Retry same run_id_1 with is_retry=True reuses intact bundle
        res3 = run_adaptive_experiment(
            bundle=bundle,
            config=self.model_config,
            model_config_version="hdbscan-adaptive-dev-v1",
            output_dir=out_root,
            mode=MODE_DEVELOPMENT,
            experiment_run_id=run_id_1,
            is_retry=True,
            subfolder_by_run_id=True,
        )
        self.assertEqual(res3["run_id"], run_id_1)
        self.assertTrue(res3["write_result"]["is_reused"])

    def test_notebook_consecutive_runs_with_same_output_root(self):
        """Regression test for comment-6249619054: Running notebook twice consecutively

        with the same output root must both succeed, create separate subfolders per run,
        and strictly preserve the checksums of the first bundle.
        """
        out_root = self.tmp_dir / "notebook_consecutive_runs"
        out_root.mkdir(parents=True, exist_ok=True)
        fixture_dir = self.tmp_dir / "consecutive_fixture_pilot"
        fixture_dir.mkdir(parents=True, exist_ok=True)
        test_experiment_harness.create_pilot_fixture(fixture_dir)

        nb_path = ROOT / "notebooks/exp_03_adaptive_hdbscan.ipynb"
        common_env = {
            "EXP03_OUTPUT_DIR": str(out_root),
            "PILOT_BUNDLE": str(fixture_dir),
            "EXP03_ALLOW_FIXTURE_FALLBACK": "true",
        }

        # 1. First execution
        report1 = inspect_exp03.run_notebook_cells(
            notebook_path=nb_path,
            env_override=common_env,
        )
        self.assertIn(report1["notebook_status"], ("RESTART_RUN_ALL_PASSED", "FALLBACK_EXEC_PASSED"))

        run_dirs_after_1 = sorted([d for d in out_root.iterdir() if d.is_dir()])
        self.assertEqual(len(run_dirs_after_1), 1)
        run_1_dir = run_dirs_after_1[0]

        # Record SHA-256 of all files in run_1_dir
        run_1_checksums = {}
        for p in run_1_dir.iterdir():
            if p.is_file():
                run_1_checksums[p.name] = sha256_bytes(p.read_bytes())

        self.assertIn("pilot_result_manifest.json", run_1_checksums)
        self.assertIn("memberships.parquet", run_1_checksums)
        self.assertIn("sequence_summary.parquet", run_1_checksums)
        self.assertIn("checksums.sha256", run_1_checksums)

        # 2. Second execution with identical input/config and SAME output root
        report2 = inspect_exp03.run_notebook_cells(
            notebook_path=nb_path,
            env_override=common_env,
        )
        self.assertIn(report2["notebook_status"], ("RESTART_RUN_ALL_PASSED", "FALLBACK_EXEC_PASSED"))

        run_dirs_after_2 = sorted([d for d in out_root.iterdir() if d.is_dir()])
        self.assertEqual(len(run_dirs_after_2), 2)
        self.assertIn(run_1_dir, run_dirs_after_2)

        run_2_dir = [d for d in run_dirs_after_2 if d != run_1_dir][0]
        self.assertNotEqual(run_1_dir.name, run_2_dir.name)

        # Verify that EVERY file in the first bundle has EXACTLY the same checksum as before
        for fname, expected_sha in run_1_checksums.items():
            actual_sha = sha256_bytes((run_1_dir / fname).read_bytes())
            self.assertEqual(expected_sha, actual_sha, f"File {fname} in first run was altered by second execution!")

        # 3. Third execution: retry of first execution reuses existing bundle
        retry_env = dict(common_env)
        retry_env.update({
            "EXP03_RUN_ID": run_1_dir.name,
            "EXP03_IS_RETRY": "true",
        })
        report3 = inspect_exp03.run_notebook_cells(
            notebook_path=nb_path,
            env_override=retry_env,
        )
        self.assertIn(report3["notebook_status"], ("RESTART_RUN_ALL_PASSED", "FALLBACK_EXEC_PASSED"))

        run_dirs_after_3 = sorted([d for d in out_root.iterdir() if d.is_dir()])
        self.assertEqual(len(run_dirs_after_3), 2)
        for fname, expected_sha in run_1_checksums.items():
            actual_sha = sha256_bytes((run_1_dir / fname).read_bytes())
            self.assertEqual(expected_sha, actual_sha)

    # -------------------------------------------------------------------------
    # 9. Edge-Case Regression in Window Fitting (fit_hdbscan_adaptive)
    # -------------------------------------------------------------------------
    def test_edge_case_single_candidate_self_only(self):
        """Window containing only the mainshock itself (N=1).

        Must assign role MAINSHOCK, noise label -1, 0 sequence members,
        and summary status FAILED with EXP_MAINSHOCK_CLASSIFIED_AS_NOISE.
        """
        candidates = [
            {
                "candidate_event_id": "main_single",
                "mainshock_magnitude": 6.0,
                "candidate_magnitude": 6.0,
                "delta_time_hours": 0.0,
                "x_scaled": 0.0,
                "y_scaled": 0.0,
                "z_scaled": 0.0,
                "time_scaled": 0.0,
            }
        ]
        mem_rows, sum_row = fit_hdbscan_adaptive(
            window_candidates=candidates,
            mainshock_id="main_single",
            config=self.model_config,
        )
        self.assertEqual(len(mem_rows), 1)
        r = mem_rows[0]
        self.assertEqual(r["candidate_event_id"], "main_single")
        self.assertEqual(r["event_role_candidate"], "MAINSHOCK")
        self.assertTrue(r["is_noise"])
        self.assertEqual(r["cluster_id"], -1)
        self.assertFalse(r["is_mainshock_cluster_member"])
        self.assertFalse(r["is_sequence_member"])
        self.assertEqual(r["multi_sequence_status"], "NOT_MEMBER")
        self.assertIsNone(r["resolution_rank"])

        self.assertEqual(sum_row["window_result_status"], "FAILED")
        self.assertEqual(sum_row["failure_reason_code"], "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")
        self.assertIsNone(sum_row["sequence_member_count"])
        self.assertIsNone(sum_row["aftershock_count"])
        self.assertIsNone(sum_row["foreshock_candidate_count"])

    def test_edge_case_small_sample_under_min_cluster_size(self):
        """Window with candidate count N < min_cluster_size (e.g. N=2 when min is 3).

        HDBSCAN is bypassed/labels all points -1, raw grain preserved,
        window fails with EXP_MAINSHOCK_CLASSIFIED_AS_NOISE.
        """
        candidates = [
            {
                "candidate_event_id": "main_small",
                "mainshock_magnitude": 5.5,
                "delta_time_hours": 0.0,
                "x_scaled": 0.0,
                "y_scaled": 0.0,
                "z_scaled": 0.0,
                "time_scaled": 0.0,
            },
            {
                "candidate_event_id": "cand_after_1",
                "mainshock_magnitude": 5.5,
                "candidate_magnitude": 4.2,
                "delta_time_hours": 2.5,
                "x_scaled": 5.0,
                "y_scaled": 1.0,
                "z_scaled": 0.5,
                "time_scaled": 2.5,
            },
        ]
        mem_rows, sum_row = fit_hdbscan_adaptive(
            window_candidates=candidates,
            mainshock_id="main_small",
            config=self.model_config,
        )
        self.assertEqual(len(mem_rows), 2)
        for r in mem_rows:
            self.assertTrue(r["is_noise"])
            self.assertEqual(r["cluster_id"], -1)
            self.assertFalse(r["is_sequence_member"])

        self.assertEqual(sum_row["window_result_status"], "FAILED")
        self.assertEqual(sum_row["failure_reason_code"], "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")
        self.assertIsNone(sum_row["sequence_member_count"])

    def test_edge_case_mainshock_classified_as_noise_with_valid_cluster(self):
        """Window where candidates form a valid cluster but mainshock is an outlier (noise).

        Window must fail with EXP_MAINSHOCK_CLASSIFIED_AS_NOISE and NO candidate
        in the non-mainshock cluster can be claimed as a sequence member.
        """
        cfg = dict(self.model_config)
        np.random.seed(42)
        b1 = np.random.randn(20, 4) * 0.1
        outlier = [{
            "candidate_event_id": "main_outlier",
            "mainshock_magnitude": 4.5,
            "x_scaled": 100.0,
            "y_scaled": 100.0,
            "z_scaled": 100.0,
            "time_scaled": 100.0,
        }]
        blob = [
            {
                "candidate_event_id": f"c_{i}",
                "mainshock_magnitude": 4.5,
                "delta_time_hours": 1.0 + float(b1[i, 0]),
                "x_scaled": float(b1[i, 0]),
                "y_scaled": float(b1[i, 1]),
                "z_scaled": float(b1[i, 2]),
                "time_scaled": float(b1[i, 3]),
            }
            for i in range(20)
        ]
        mem_rows, sum_row = fit_hdbscan_adaptive(
            window_candidates=outlier + blob,
            mainshock_id="main_outlier",
            config=cfg,
        )
        self.assertEqual(len(mem_rows), 21)
        main_row = next(r for r in mem_rows if r["candidate_event_id"] == "main_outlier")
        self.assertEqual(main_row["cluster_id"], -1)
        self.assertTrue(main_row["is_noise"])

        # Review requirement (PR #116 comment): Ensure candidate blob actually formed
        # at least one non-noise cluster (cluster_id >= 0) to avoid false-positive pass where all are noise.
        non_noise_clusters = {r["cluster_id"] for r in mem_rows if r["cluster_id"] >= 0}
        self.assertGreaterEqual(
            len(non_noise_clusters),
            1,
            "Expected at least one valid non-noise cluster formed by candidate blob",
        )
        blob_rows = [r for r in mem_rows if r["candidate_event_id"] != "main_outlier"]
        self.assertTrue(
            any(r["cluster_id"] in non_noise_clusters for r in blob_rows),
            "Expected candidate blob events to be assigned to valid non-noise cluster",
        )

        for r in mem_rows:
            self.assertFalse(r["is_sequence_member"])
            self.assertFalse(r["is_mainshock_cluster_member"])
            self.assertEqual(r["multi_sequence_status"], "NOT_MEMBER")
            self.assertIsNone(r["resolution_rank"])

        self.assertEqual(sum_row["window_result_status"], "FAILED")
        self.assertEqual(sum_row["failure_reason_code"], "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")
        self.assertIsNone(sum_row["sequence_member_count"])

    def test_edge_case_all_noise_window(self):
        """Window where candidates are dispersed so HDBSCAN assigns all -1.

        Window must fail with EXP_MAINSHOCK_CLASSIFIED_AS_NOISE.
        """
        cfg = dict(self.model_config)
        cfg["allow_single_cluster"] = False
        candidates = [
            {
                "candidate_event_id": f"disp_{i}",
                "mainshock_magnitude": 4.5,
                "x_scaled": float(i * 1000.0),
                "y_scaled": 0.0,
                "z_scaled": 0.0,
                "time_scaled": 0.0,
            }
            for i in range(10)
        ]
        mem_rows, sum_row = fit_hdbscan_adaptive(
            window_candidates=candidates,
            mainshock_id="disp_0",
            config=cfg,
        )
        for r in mem_rows:
            self.assertEqual(r["cluster_id"], -1)
            self.assertTrue(r["is_noise"])
            self.assertFalse(r["is_sequence_member"])
        self.assertEqual(sum_row["window_result_status"], "FAILED")
        self.assertEqual(sum_row["failure_reason_code"], "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")

    def test_edge_case_multi_cluster_only_mainshock_cluster_accepted(self):
        """When multiple clusters form, only candidates in the mainshock's cluster are accepted.

        Candidates in other clusters must have is_sequence_member=False and multi_sequence_status=NOT_MEMBER.
        """
        cfg = dict(self.model_config)
        np.random.seed(42)
        b1 = np.random.randn(20, 4) * 0.1
        b2 = np.random.randn(20, 4) * 0.1 + 20.0
        cands = []
        for i in range(20):
            cands.append({
                "candidate_event_id": "main_A" if i == 0 else f"a_{i}",
                "mainshock_magnitude": 4.5,
                "delta_time_hours": 0.0 if i == 0 else 0.5,
                "x_scaled": float(b1[i, 0]),
                "y_scaled": float(b1[i, 1]),
                "z_scaled": float(b1[i, 2]),
                "time_scaled": float(b1[i, 3]),
            })
        for i in range(20):
            cands.append({
                "candidate_event_id": f"b_{i}",
                "mainshock_magnitude": 4.5,
                "delta_time_hours": 50.0,
                "x_scaled": float(b2[i, 0]),
                "y_scaled": float(b2[i, 1]),
                "z_scaled": float(b2[i, 2]),
                "time_scaled": float(b2[i, 3]),
            })

        mem_rows, sum_row = fit_hdbscan_adaptive(
            window_candidates=cands,
            mainshock_id="main_A",
            config=cfg,
        )
        self.assertEqual(len(mem_rows), 40)
        self.assertEqual(sum_row["window_result_status"], "SUCCESS")

        a_members = [r for r in mem_rows if r["candidate_event_id"].startswith("a_") or r["candidate_event_id"] == "main_A"]
        b_non_members = [r for r in mem_rows if r["candidate_event_id"].startswith("b_")]

        for r in a_members:
            self.assertTrue(r["is_sequence_member"])
            self.assertTrue(r["is_mainshock_cluster_member"])
            self.assertEqual(r["multi_sequence_status"], "UNIQUE")

        for r in b_non_members:
            self.assertFalse(r["is_sequence_member"])
            self.assertFalse(r["is_mainshock_cluster_member"])
            self.assertEqual(r["multi_sequence_status"], "NOT_MEMBER")
            self.assertIsNone(r["resolution_rank"])

        self.assertEqual(sum_row["sequence_member_count"], 20)

    def test_edge_case_empty_post_candidates(self):
        """Window with foreshocks (PRE) and MAINSHOCK, but zero aftershocks (POST).

        Must handle empty POST gracefully: aftershock_count=0, foreshock_candidate_count>0,
        status SUCCESS.
        """
        cfg = dict(self.model_config)
        np.random.seed(42)
        b = np.random.randn(20, 4) * 0.05
        cands = []
        for i in range(20):
            is_main = (i == 0)
            cands.append({
                "candidate_event_id": "main_no_post" if is_main else f"pre_{i}",
                "mainshock_magnitude": 4.5,
                "delta_time_hours": 0.0 if is_main else -float(abs(b[i, 0]) + 0.1),
                "x_scaled": float(b[i, 0]),
                "y_scaled": float(b[i, 1]),
                "z_scaled": float(b[i, 2]),
                "time_scaled": float(b[i, 3]),
            })

        mem_rows, sum_row = fit_hdbscan_adaptive(
            window_candidates=cands,
            mainshock_id="main_no_post",
            config=cfg,
        )
        self.assertEqual(sum_row["window_result_status"], "SUCCESS")
        self.assertEqual(sum_row["aftershock_count"], 0)
        self.assertGreater(sum_row["foreshock_candidate_count"], 0)
        self.assertEqual(sum_row["sequence_member_count"], sum_row["foreshock_candidate_count"] + 1)

        for r in mem_rows:
            if r["candidate_event_id"] == "main_no_post":
                self.assertEqual(r["event_role_candidate"], "MAINSHOCK")
            else:
                self.assertEqual(r["event_role_candidate"], "PRE")

    # -------------------------------------------------------------------------
    # 10. Multi-Sequence Overlap Fixture & Reconciliation
    # -------------------------------------------------------------------------
    def test_multi_sequence_two_window_overlap_reconciliation(self):
        """Two overlapping windows with shared candidate: verify raw grain, reconciliation count,

        and rank assignments.
        """
        # Window A: main_A (self), c_shared, c_unique_A
        # Window B: main_B (self), c_shared, c_unique_B
        all_memberships = [
            {"mainshock_event_id": "main_A", "candidate_event_id": "main_A", "is_sequence_member": True, "membership_probability": 1.0, "normalized_distance": 0.0, "event_role_candidate": "MAINSHOCK"},
            {"mainshock_event_id": "main_A", "candidate_event_id": "c_shared", "is_sequence_member": True, "membership_probability": 0.90, "normalized_distance": 0.1, "event_role_candidate": "POST"},
            {"mainshock_event_id": "main_A", "candidate_event_id": "c_unique_A", "is_sequence_member": True, "membership_probability": 0.80, "normalized_distance": 0.2, "event_role_candidate": "POST"},
            {"mainshock_event_id": "main_B", "candidate_event_id": "main_B", "is_sequence_member": True, "membership_probability": 1.0, "normalized_distance": 0.0, "event_role_candidate": "MAINSHOCK"},
            {"mainshock_event_id": "main_B", "candidate_event_id": "c_shared", "is_sequence_member": True, "membership_probability": 0.40, "normalized_distance": 0.5, "event_role_candidate": "POST"},
            {"mainshock_event_id": "main_B", "candidate_event_id": "c_unique_B", "is_sequence_member": True, "membership_probability": 0.75, "normalized_distance": 0.3, "event_role_candidate": "POST"},
        ]
        summaries = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
        ]
        res_mem, res_sum, recon = resolve_multi_sequence_and_nested_mainshocks(all_memberships, summaries)

        # Raw grain strictly preserved: exactly 6 rows returned
        self.assertEqual(len(res_mem), 6)
        self.assertEqual(recon["total_raw_memberships"], 6)

        # Shared candidate resolution
        shared_in_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A" and m["candidate_event_id"] == "c_shared")
        shared_in_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B" and m["candidate_event_id"] == "c_shared")

        self.assertEqual(shared_in_a["multi_sequence_status"], "MULTIPLE")
        self.assertEqual(shared_in_a["resolution_rank"], 1)
        self.assertEqual(shared_in_b["multi_sequence_status"], "MULTIPLE")
        self.assertEqual(shared_in_b["resolution_rank"], 2)

        # Unique candidates remain UNIQUE with rank 1
        u_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A" and m["candidate_event_id"] == "c_unique_A")
        u_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B" and m["candidate_event_id"] == "c_unique_B")
        self.assertEqual(u_a["multi_sequence_status"], "UNIQUE")
        self.assertEqual(u_a["resolution_rank"], 1)
        self.assertEqual(u_b["multi_sequence_status"], "UNIQUE")
        self.assertEqual(u_b["resolution_rank"], 1)

        # Deduplication count reconciliation: 4 raw aftershocks, 3 unique resolved aftershocks, 1 deduplicated
        self.assertEqual(recon["raw_aftershocks"], 4)
        self.assertEqual(recon["resolved_unique_aftershocks"], 3)
        self.assertEqual(recon["aftershocks_deduplicated_count"], 1)
        self.assertEqual(recon["ambiguous_candidates_count"], 0)

    def test_multi_sequence_two_window_overlap_exact_tie_ambiguous(self):
        """When 2 overlapping sequences tie on probability and distance, candidate is AMBIGUOUS.

        Resolution must be invariant under window order permutation.
        """
        eps_tie = 1e-6
        base_shared = [
            {"mainshock_event_id": "main_A", "candidate_event_id": "c_tie", "is_sequence_member": True, "membership_probability": 0.8000000, "normalized_distance": 0.2, "event_role_candidate": "POST"},
            {"mainshock_event_id": "main_B", "candidate_event_id": "c_tie", "is_sequence_member": True, "membership_probability": 0.8000001, "normalized_distance": 0.2, "event_role_candidate": "POST"},
        ]
        for perm in itertools.permutations(base_shared):
            memberships = list(perm)
            summaries = [
                {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
                {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
            ]
            res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(memberships, summaries, eps_tie=eps_tie)
            self.assertEqual(len(res_mem), 2)
            for m in res_mem:
                self.assertEqual(m["multi_sequence_status"], "AMBIGUOUS")
                self.assertIsNone(m["resolution_rank"])
            self.assertEqual(recon["ambiguous_candidates_count"], 1)
            self.assertEqual(recon["resolved_unique_aftershocks"], 0)

    def test_multi_sequence_candidate_member_in_one_and_noise_in_other(self):
        """Candidate that is a sequence member in Window A but noise in Window B.

        Must resolve as UNIQUE (rank 1) in Window A, and NOT_MEMBER (rank None) in Window B.
        """
        all_memberships = [
            {"mainshock_event_id": "main_A", "candidate_event_id": "c_mixed", "is_sequence_member": True, "membership_probability": 0.85, "normalized_distance": 0.15, "event_role_candidate": "POST"},
            {"mainshock_event_id": "main_B", "candidate_event_id": "c_mixed", "is_sequence_member": False, "membership_probability": 0.0, "normalized_distance": 1.5, "event_role_candidate": "POST"},
        ]
        summaries = [
            {"mainshock_event_id": "main_A", "is_nested_mainshock_candidate": False},
            {"mainshock_event_id": "main_B", "is_nested_mainshock_candidate": False},
        ]
        res_mem, _, recon = resolve_multi_sequence_and_nested_mainshocks(all_memberships, summaries)
        row_a = next(m for m in res_mem if m["mainshock_event_id"] == "main_A")
        row_b = next(m for m in res_mem if m["mainshock_event_id"] == "main_B")

        self.assertEqual(row_a["multi_sequence_status"], "UNIQUE")
        self.assertEqual(row_a["resolution_rank"], 1)
        self.assertEqual(row_b["multi_sequence_status"], "NOT_MEMBER")
        self.assertIsNone(row_b["resolution_rank"])
        self.assertEqual(recon["resolved_unique_aftershocks"], 1)

    # -------------------------------------------------------------------------
    # 11. Determinism & Permutation Invariance
    # -------------------------------------------------------------------------
    def test_candidate_input_order_permutation_invariance_fit_adaptive(self):
        """Permuting candidate order into fit_hdbscan_adaptive must yield identical

        set of sequence member IDs, roles, and summary metrics.
        """
        cfg = dict(self.model_config)
        np.random.seed(42)
        b = np.random.randn(15, 4) * 0.1
        base_candidates = []
        for i in range(15):
            is_main = (i == 0)
            base_candidates.append({
                "candidate_event_id": "m_inv" if is_main else f"c_inv_{i}",
                "mainshock_magnitude": 4.5,
                "delta_time_hours": 0.0 if is_main else float(b[i, 0]),
                "x_scaled": float(b[i, 0]),
                "y_scaled": float(b[i, 1]),
                "z_scaled": float(b[i, 2]),
                "time_scaled": float(b[i, 3]),
            })

        ref_members = None
        ref_roles = None
        ref_status = None
        for perm in list(itertools.permutations(base_candidates[:6])):
            full_perm = list(perm) + base_candidates[6:]
            mem, sum_row = fit_hdbscan_adaptive(
                window_candidates=full_perm,
                mainshock_id="m_inv",
                config=cfg,
            )
            seq_members = {m["candidate_event_id"] for m in mem if m["is_sequence_member"]}
            roles = {m["candidate_event_id"]: m["event_role_candidate"] for m in mem}
            if ref_members is None:
                ref_members = seq_members
                ref_roles = roles
                ref_status = sum_row["window_result_status"]
            else:
                self.assertEqual(seq_members, ref_members)
                self.assertEqual(roles, ref_roles)
                self.assertEqual(sum_row["window_result_status"], ref_status)

    # -------------------------------------------------------------------------
    # 12. Rerun, Retry & Tamper Detection
    # -------------------------------------------------------------------------
    def test_rerun_with_same_config_yields_identical_analytical_outputs(self):
        """Running adaptive experiment on identical bundle with same configuration

        produces bitwise identical analytical columns and retry idempotent reuse.
        """
        temp_bundle_dir = self.tmp_dir / "pilot_bundle_det"
        temp_bundle_dir.mkdir(parents=True, exist_ok=True)
        test_experiment_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = load_feature_bundle(temp_bundle_dir, mode=MODE_DEVELOPMENT)

        out_1 = self.tmp_dir / "res_det_1"
        out_2 = self.tmp_dir / "res_det_2"
        out_1.mkdir(parents=True, exist_ok=True)
        out_2.mkdir(parents=True, exist_ok=True)

        res1 = run_adaptive_experiment(
            bundle=bundle,
            config=self.model_config,
            model_config_version="hdbscan-adaptive-dev-v1",
            output_dir=out_1,
            mode=MODE_DEVELOPMENT,
            experiment_run_id="run_fixed_det",
            execution_id="exec_fixed_det",
        )
        res2 = run_adaptive_experiment(
            bundle=bundle,
            config=self.model_config,
            model_config_version="hdbscan-adaptive-dev-v1",
            output_dir=out_2,
            mode=MODE_DEVELOPMENT,
            experiment_run_id="run_fixed_det",
            execution_id="exec_fixed_det",
        )
        # Compare analytical data columns in memberships.parquet
        t1 = pq.read_table(out_1 / "memberships.parquet")
        t2 = pq.read_table(out_2 / "memberships.parquet")
        analytical_cols = [c for c in t1.column_names if c != "created_at_utc"]
        self.assertEqual(t1.select(analytical_cols).to_pylist(), t2.select(analytical_cols).to_pylist())

        # Compare analytical data columns in sequence_summary.parquet
        s1 = pq.read_table(out_1 / "sequence_summary.parquet")
        s2 = pq.read_table(out_2 / "sequence_summary.parquet")
        sum_cols = [c for c in s1.column_names if c not in ("created_at_utc", "runtime_seconds")]
        self.assertEqual(s1.select(sum_cols).to_pylist(), s2.select(sum_cols).to_pylist())

        # Test retry reuse: retry with same run_id reuses bundle intact with matching sha256
        res3 = run_adaptive_experiment(
            bundle=bundle,
            config=self.model_config,
            model_config_version="hdbscan-adaptive-dev-v1",
            output_dir=out_1,
            mode=MODE_DEVELOPMENT,
            experiment_run_id="run_fixed_det",
            is_retry=True,
        )
        self.assertTrue(res3["write_result"]["is_reused"])

    def test_rerun_tampered_artifact_detection(self):
        """When an artifact file is modified/tampered, retry must detect the corruption

        and refuse to reuse the damaged bundle (raises EXP_COMPLETED_BUNDLE_CONFLICT).
        """
        temp_bundle_dir = self.tmp_dir / "pilot_bundle_tamper"
        temp_bundle_dir.mkdir(parents=True, exist_ok=True)
        test_experiment_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = load_feature_bundle(temp_bundle_dir, mode=MODE_DEVELOPMENT)

        out_root = self.tmp_dir / "results_tamper"
        out_root.mkdir(parents=True, exist_ok=True)

        res1 = run_adaptive_experiment(
            bundle=bundle,
            config=self.model_config,
            model_config_version="hdbscan-adaptive-dev-v1",
            output_dir=out_root,
            mode=MODE_DEVELOPMENT,
            subfolder_by_run_id=True,
        )
        run_id = res1["run_id"]
        run_dir = Path(res1["output_dir"])

        # Tamper with memberships.parquet
        parquet_file = run_dir / "memberships.parquet"
        self.assertTrue(parquet_file.is_file())
        with open(parquet_file, "ab") as f:
            f.write(b"CORRUPTED_TAMPER_BYTES")

        # Retry should detect checksum mismatch and raise EXP_COMPLETED_BUNDLE_CONFLICT
        with self.assertRaises(ValueError) as ctx:
            run_adaptive_experiment(
                bundle=bundle,
                config=self.model_config,
                model_config_version="hdbscan-adaptive-dev-v1",
                output_dir=out_root,
                mode=MODE_DEVELOPMENT,
                experiment_run_id=run_id,
                is_retry=True,
                subfolder_by_run_id=True,
            )
        self.assertIn("EXP_COMPLETED_BUNDLE_CONFLICT", str(ctx.exception))
    # -------------------------------------------------------------------------
    # 12. EXP-02 Baseline Local Gate and Run Manifest Verifications
    # -------------------------------------------------------------------------
    def test_notebook_missing_pilot_bundle_fails_loudly(self):
        """Review requirement: missing or invalid PILOT_BUNDLE path must fail loudly, never fallback silently."""
        nb_path = ROOT / "notebooks/exp_03_adaptive_hdbscan.ipynb"
        with self.assertRaises(FileNotFoundError) as ctx:
            inspect_exp03.run_notebook_cells(
                notebook_path=nb_path,
                env_override={"PILOT_BUNDLE": "/nonexistent/invalid/pilot_path_987654"},
            )
        self.assertIn("PILOT_BUNDLE_NOT_FOUND", str(ctx.exception))

    def test_notebook_pilot_path_is_file_not_dir_fails_loudly(self):
        """Review requirement: PILOT_BUNDLE pointing to a file instead of a directory must fail loudly."""
        nb_path = ROOT / "notebooks/exp_03_adaptive_hdbscan.ipynb"
        dummy_file = self.tmp_dir / "not_a_dir.parquet"
        dummy_file.write_text("dummy", encoding="utf-8")
        with self.assertRaises(FileNotFoundError) as ctx:
            inspect_exp03.run_notebook_cells(
                notebook_path=nb_path,
                env_override={"PILOT_BUNDLE": str(dummy_file)},
            )
        self.assertIn("PILOT_BUNDLE_NOT_FOUND", str(ctx.exception))

    def test_inspect_script_fixture_fallback_denies_colab_migration(self):
        """Review requirement: running on synthetic fixture must block colab migration."""
        nb_path = ROOT / "notebooks/exp_03_adaptive_hdbscan.ipynb"
        out_root = self.tmp_dir / "fixture_fallback_test"
        out_root.mkdir(parents=True, exist_ok=True)
        fixture_dir = self.tmp_dir / "synth_fixture"
        fixture_dir.mkdir(parents=True, exist_ok=True)
        from test_experiment_harness import create_pilot_fixture
        create_pilot_fixture(fixture_dir)

        report = inspect_exp03.run_notebook_cells(
            notebook_path=nb_path,
            env_override={
                "PILOT_BUNDLE": str(fixture_dir),
                "EXP03_OUTPUT_DIR": str(out_root),
                "EXP03_ALLOW_FIXTURE_FALLBACK": "true",
            },
        )
        self.assertFalse(report["pilot_verification"]["is_real_pilot"])
        self.assertFalse(report["colab_migration_allowed"])
        self.assertIn("REAL_PILOT_REQUIRED_FOR_COLAB_MIGRATION", report["migration_block_reasons"])

    def test_inspect_script_missing_exp02_baseline_denies_colab_migration(self):
        """Review requirement: colab migration requires verified EXP-02 baseline local gate."""
        nb_path = ROOT / "notebooks/exp_03_adaptive_hdbscan.ipynb"
        out_root = self.tmp_dir / "exp02_gate_test"
        out_root.mkdir(parents=True, exist_ok=True)
        fixture_dir = self.tmp_dir / "exp02_gate_fixture_pilot"
        fixture_dir.mkdir(parents=True, exist_ok=True)
        test_experiment_harness.create_pilot_fixture(fixture_dir)

        report = inspect_exp03.run_notebook_cells(
            notebook_path=nb_path,
            env_override={
                "EXP03_OUTPUT_DIR": str(out_root),
                "PILOT_BUNDLE": str(fixture_dir),
                "EXP03_ALLOW_FIXTURE_FALLBACK": "true",
                "EXP03_SIMULATE_EXP02_GATE_FAIL": "true",
            },
        )
        self.assertFalse(report["colab_migration_allowed"])
        self.assertIn("EXP02_BASELINE_LOCAL_GATE_REQUIRED", report["migration_block_reasons"])

    def test_exp02_baseline_gate_missing_report_fails_closed(self):
        """Hard gate regression: missing EXP-02 evidence report must fail closed."""
        res = inspect_exp03.check_exp02_baseline_local_gate(
            ROOT,
            env_override={"EXP02_REPORT_PATH": str(self.tmp_dir / "nonexistent_exp02_report.json")},
        )
        self.assertFalse(res["prerequisite_verified"])
        self.assertEqual(res["status"], "EXP02_BASELINE_GATE_FAILED")
        self.assertIn("EXP02_EVIDENCE_REPORT_MISSING", res["failure_reasons"])

    def test_exp02_baseline_gate_corrupted_json_fails_closed(self):
        """Hard gate regression: corrupted EXP-02 evidence report JSON must fail closed."""
        bad_json_file = self.tmp_dir / "corrupted_exp02_report.json"
        bad_json_file.write_text("{invalid json structure", encoding="utf-8")

        res = inspect_exp03.check_exp02_baseline_local_gate(
            ROOT,
            env_override={"EXP02_REPORT_PATH": str(bad_json_file)},
        )
        self.assertFalse(res["prerequisite_verified"])
        self.assertEqual(res["status"], "EXP02_BASELINE_GATE_FAILED")
        self.assertTrue(any("EXP02_REPORT_JSON_CORRUPT" in r for r in res["failure_reasons"]))

    def test_exp02_baseline_gate_mismatched_pilot_or_feature_sha_fails_closed(self):
        """Hard gate regression: report with wrong pilot ID or feature SHA must fail closed."""
        valid_report = json.loads((ROOT / "docs/evidence/EXP-02_COLAB_PARITY_20261102.json").read_text(encoding="utf-8"))

        # Case 1: Wrong pilot ID
        tampered_pilot = dict(valid_report)
        tampered_pilot["pilot"] = dict(valid_report["pilot"])
        tampered_pilot["pilot"]["pilot_id"] = "pilot_wrong_id_12345"

        report_p1 = self.tmp_dir / "tampered_pilot_id.json"
        report_p1.write_text(json.dumps(tampered_pilot), encoding="utf-8")

        res1 = inspect_exp03.check_exp02_baseline_local_gate(ROOT, env_override={"EXP02_REPORT_PATH": str(report_p1)})
        self.assertFalse(res1["prerequisite_verified"])
        self.assertIn("EXP02_REPORT_PILOT_ID_MISMATCH", res1["failure_reasons"])

        # Case 2: Wrong feature SHA
        tampered_feat = dict(valid_report)
        tampered_feat["pilot"] = dict(valid_report["pilot"])
        tampered_feat["pilot"]["feature_sha256"] = "f" * 64

        report_p2 = self.tmp_dir / "tampered_feat_sha.json"
        report_p2.write_text(json.dumps(tampered_feat), encoding="utf-8")

        res2 = inspect_exp03.check_exp02_baseline_local_gate(ROOT, env_override={"EXP02_REPORT_PATH": str(report_p2)})
        self.assertFalse(res2["prerequisite_verified"])
        self.assertIn("EXP02_REPORT_FEATURE_SHA_MISMATCH", res2["failure_reasons"])

    def test_exp02_baseline_gate_handoff_archive_sha_mismatch_fails_closed(self):
        """Hard gate regression: physical handoff archive with wrong SHA must fail closed."""
        fake_archive = self.tmp_dir / "fake_handoff.zip"
        fake_archive.write_bytes(b"corrupted zip bytes for testing")

        res = inspect_exp03.check_exp02_baseline_local_gate(
            ROOT,
            env_override={"EXP02_HANDOFF_ARCHIVE": str(fake_archive)},
        )
        self.assertFalse(res["prerequisite_verified"])
        self.assertEqual(res["status"], "EXP02_BASELINE_GATE_FAILED")
        self.assertIn("EXP02_HANDOFF_ARCHIVE_SHA_MISMATCH", res["failure_reasons"])

    def test_find_experiment_run_rejects_unmatched_subfolder_or_missing_manifest(self):
        """Regression test for comment-6570696486: selection must tie to exact run ID/pins, not blind mtime."""
        out_root = self.tmp_dir / "find_run_test"
        out_root.mkdir(parents=True, exist_ok=True)

        # 1. Valid run directory
        run_1_dir = out_root / "pilot_run_11111111111111111111111111111111"
        run_1_dir.mkdir(parents=True, exist_ok=True)
        (run_1_dir / "exp03_run_summary.json").write_text(json.dumps({
            "experiment_run_id": "pilot_run_11111111111111111111111111111111",
            "pilot_id": inspect_exp03.REAL_PILOT_ID_PIN,
            "output_dir": str(run_1_dir),
        }), encoding="utf-8")
        (run_1_dir / "pilot_result_manifest.json").write_text("{}", encoding="utf-8")

        # 2. Invalid run directory: subfolder name does NOT match experiment_run_id
        run_2_dir = out_root / "subfolder_mismatch"
        run_2_dir.mkdir(parents=True, exist_ok=True)
        (run_2_dir / "exp03_run_summary.json").write_text(json.dumps({
            "experiment_run_id": "pilot_run_22222222222222222222222222222222",
            "pilot_id": inspect_exp03.REAL_PILOT_ID_PIN,
            "output_dir": str(run_2_dir),
        }), encoding="utf-8")
        (run_2_dir / "pilot_result_manifest.json").write_text("{}", encoding="utf-8")

        # Direct search with target_run_id
        found_1 = inspect_exp03.find_latest_experiment_run(out_root, target_run_id="pilot_run_11111111111111111111111111111111")
        self.assertEqual(found_1.get("experiment_run_id"), "pilot_run_11111111111111111111111111111111")

        # Mismatched target run ID must return empty
        found_nonexistent = inspect_exp03.find_latest_experiment_run(out_root, target_run_id="pilot_run_nonexistent")
        self.assertEqual(found_nonexistent, {})

        # Auto-discovery must only pick valid run_1_dir and reject mismatched run_2_dir
        auto_found = inspect_exp03.find_latest_experiment_run(out_root)
        self.assertEqual(auto_found.get("experiment_run_id"), "pilot_run_11111111111111111111111111111111")


if __name__ == "__main__":
    unittest.main()
