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
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))
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


if __name__ == "__main__":
    unittest.main()
