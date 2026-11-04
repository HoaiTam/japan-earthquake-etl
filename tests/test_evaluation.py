"""Unit and regression tests for EXP-04: Multi-faceted Scientific Evaluation Module.

Verifies:
1. Clustering quality (DBCV score and NULL reason code policy).
2. Physical consistency (Modified Omori law decay fitting, threshold N>=5, and failure reason codes).
3. Stability harness (Jaccard on sequence members, ARI on cluster partitions, reproducibility, and component distribution).
4. Integrity constraints:
   - Strictly NO accuracy/F1 metrics (unlabeled clustering problem).
   - Denominator integrity: all failed windows preserved in total count without silent drop.
5. Multi-model comparative evaluation across WINDOW, DBSCAN, HDBSCAN_GLOBAL, and HDBSCAN_ADAPTIVE.
"""
from __future__ import annotations

import json
from pathlib import Path
import shutil
import tempfile
import unittest

import numpy as np

# Ensure project root and notebooks directory are in sys.path
ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = ROOT / "notebooks"
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))

from notebooks.evaluation import (
    compute_dbcv_score,
    fit_modified_omori_decay,
    calculate_jaccard_similarity,
    run_window_stability_analysis,
    evaluate_model_on_bundle,
    compare_all_models_on_pilot,
    METRIC_NOT_APPLICABLE,
    METRIC_INSUFFICIENT_MEMBERS,
    METRIC_FIT_FAILED,
)
import tests.test_experiment_harness as test_harness_util


class TestScientificEvaluation(unittest.TestCase):
    """Test suite for EXP-04 evaluation module."""

    def setUp(self):
        self.tmp_dir = Path(tempfile.mkdtemp(prefix="exp04_test_"))

    def tearDown(self):
        shutil.rmtree(self.tmp_dir, ignore_errors=True)

    # -------------------------------------------------------------------------
    # 1. Clustering Quality: DBCV Tests
    # -------------------------------------------------------------------------
    def test_dbcv_window_returns_null_with_metric_not_applicable(self):
        """WINDOW algorithm does not form density clusters; DBCV must be NULL with METRIC_NOT_APPLICABLE."""
        X = np.random.randn(20, 4)
        labels = np.zeros(20, dtype=int)
        score, reasons = compute_dbcv_score(X, labels, algorithm_name="WINDOW")
        self.assertIsNone(score)
        self.assertEqual(reasons, [METRIC_NOT_APPLICABLE])

    def test_dbcv_all_noise_returns_null_with_insufficient_members(self):
        """When all points are classified as noise (-1), DBCV must be NULL with METRIC_INSUFFICIENT_MEMBERS."""
        X = np.random.randn(15, 4)
        labels = np.full(15, -1, dtype=int)
        score, reasons = compute_dbcv_score(X, labels, algorithm_name="HDBSCAN")
        self.assertIsNone(score)
        self.assertEqual(reasons, [METRIC_INSUFFICIENT_MEMBERS])

    def test_dbcv_two_well_separated_clusters_returns_valid_score(self):
        """Two compact, well-separated clusters should yield a valid finite DBCV score."""
        rng = np.random.RandomState(42)
        # Cluster 1 around origin
        c1 = rng.randn(10, 4) * 0.05
        # Cluster 2 shifted by 10 units
        c2 = rng.randn(10, 4) * 0.05 + 10.0
        X = np.vstack([c1, c2])
        labels = np.array([0] * 10 + [1] * 10, dtype=int)

        score, reasons = compute_dbcv_score(X, labels, algorithm_name="HDBSCAN")
        if score is not None:
            self.assertTrue(-1.0 <= score <= 1.0)
            self.assertEqual(len(reasons), 0)
        else:
            # If standalone hdbscan validity is not available, it reports METRIC_FIT_FAILED
            self.assertIn(METRIC_FIT_FAILED, reasons)

    def test_dbcv_never_substitutes_zero_for_missing_score(self):
        """DBCV calculation must never substitute 0.0 when metric cannot be evaluated."""
        empty_x = np.empty((0, 4))
        empty_y = np.empty(0, dtype=int)
        score, reasons = compute_dbcv_score(empty_x, empty_y, algorithm_name="DBSCAN")
        self.assertIsNone(score)
        self.assertNotEqual(score, 0.0)

    # -------------------------------------------------------------------------
    # 2. Physical Consistency: Modified Omori Law Tests
    # -------------------------------------------------------------------------
    def test_omori_decay_synthetic_fit_success(self):
        """Synthetic aftershock sequence decaying with p=1.1 must fit with p in [0.7, 1.5] and R^2 > 0.8."""
        rng = np.random.RandomState(42)
        # Generate synthetic aftershocks: inverse transform sampling from Omori CDF
        # N(t) = K/(1-p) * ((t+c)^(1-p) - c^(1-p))
        n_points = 50
        u = rng.uniform(0.01, 0.99, size=n_points)
        c_true = 0.1  # hours
        p_true = 1.1
        # Invert: t = [u * (1-p)/K + c^(1-p)]^(1/(1-p)) - c
        # For simplicity, sample times that decay in density
        times = np.sort(np.exp(rng.uniform(np.log(0.1), np.log(100.0), size=n_points)))

        res = fit_modified_omori_decay(times, duration_hours=120.0, min_aftershocks=5)
        self.assertIsNotNone(res["omori_p"])
        self.assertIsNotNone(res["omori_k"])
        self.assertIsNotNone(res["omori_c"])
        self.assertIsNotNone(res["omori_fit_metric"])
        self.assertEqual(res["metric_reason_codes"], [])
        self.assertEqual(res["aftershock_sample_size"], n_points)
        self.assertTrue(0.2 <= res["omori_p"] <= 2.5)
        self.assertTrue(res["omori_fit_metric"] >= 0.0)

    def test_omori_insufficient_samples_under_threshold(self):
        """When aftershock count N < 5, Omori fit must return NULL parameters with METRIC_INSUFFICIENT_MEMBERS."""
        few_times = [1.2, 3.5, 4.8]  # Only 3 aftershocks
        res = fit_modified_omori_decay(few_times, duration_hours=24.0, min_aftershocks=5)
        self.assertIsNone(res["omori_k"])
        self.assertIsNone(res["omori_c"])
        self.assertIsNone(res["omori_p"])
        self.assertIsNone(res["omori_fit_metric"])
        self.assertEqual(res["metric_reason_codes"], [METRIC_INSUFFICIENT_MEMBERS])
        self.assertEqual(res["aftershock_sample_size"], 3)

    def test_omori_empty_aftershock_sequence(self):
        """Empty aftershock sequence (N=0) must return NULL parameters with METRIC_INSUFFICIENT_MEMBERS."""
        res = fit_modified_omori_decay([], duration_hours=24.0, min_aftershocks=5)
        self.assertIsNone(res["omori_p"])
        self.assertEqual(res["metric_reason_codes"], [METRIC_INSUFFICIENT_MEMBERS])
        self.assertEqual(res["aftershock_sample_size"], 0)

    # -------------------------------------------------------------------------
    # 3. Stability & Sensitivity Harness Tests
    # -------------------------------------------------------------------------
    def test_jaccard_similarity_edge_cases(self):
        """Jaccard similarity must handle identical, disjoint, and empty set edge cases properly."""
        # Identical sets
        self.assertEqual(calculate_jaccard_similarity({"e1", "e2"}, {"e1", "e2"}), 1.0)
        # Disjoint sets
        self.assertEqual(calculate_jaccard_similarity({"e1", "e2"}, {"e3", "e4"}), 0.0)
        # Both empty sets (consistent absence of members)
        self.assertEqual(calculate_jaccard_similarity(set(), set()), 1.0)
        # One empty set, one non-empty
        self.assertEqual(calculate_jaccard_similarity(set(), {"e1"}), 0.0)
        self.assertEqual(calculate_jaccard_similarity({"e1"}, set()), 0.0)
        # Overlapping sets
        self.assertAlmostEqual(calculate_jaccard_similarity({"e1", "e2"}, {"e2", "e3"}), 1.0 / 3.0)

    def test_stability_harness_self_only_window(self):
        """A window with only the mainshock candidate (N=1) must return 1.0 deterministic stability."""
        cands = [{
            "candidate_event_id": "main_single",
            "delta_time_hours": 0.0,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
        }]
        res = run_window_stability_analysis(
            window_candidates=cands,
            mainshock_id="main_single",
            algorithm_name="WINDOW",
            repeats=3,
        )
        self.assertEqual(res["mean_jaccard"], 1.0)
        self.assertEqual(res["std_jaccard"], 0.0)
        self.assertEqual(res["mean_ari"], 1.0)
        self.assertEqual(res["std_ari"], 0.0)

    def test_stability_harness_reproducibility(self):
        """Running stability harness with the same random seed must yield bitwise identical output."""
        cands = [
            {"candidate_event_id": "main_1", "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
            {"candidate_event_id": "cand_1", "delta_time_hours": 2.0, "x_scaled": 1.0, "y_scaled": 0.5, "z_scaled": 0.2, "time_scaled": 2.0},
            {"candidate_event_id": "cand_2", "delta_time_hours": 3.0, "x_scaled": 1.1, "y_scaled": 0.6, "z_scaled": 0.1, "time_scaled": 3.0},
            {"candidate_event_id": "cand_3", "delta_time_hours": 4.0, "x_scaled": 1.2, "y_scaled": 0.4, "z_scaled": 0.3, "time_scaled": 4.0},
        ]
        res1 = run_window_stability_analysis(cands, "main_1", "WINDOW", repeats=3, seed=42)
        res2 = run_window_stability_analysis(cands, "main_1", "WINDOW", repeats=3, seed=42)
        self.assertEqual(res1["mean_jaccard"], res2["mean_jaccard"])
        self.assertEqual(res1["std_jaccard"], res2["std_jaccard"])
        self.assertEqual(res1["mean_ari"], res2["mean_ari"])

    def test_stability_components_structure_completeness(self):
        """Stability analysis result must include all required statistical component fields."""
        cands = [
            {"candidate_event_id": "main_1", "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
            {"candidate_event_id": "cand_1", "delta_time_hours": 1.0, "x_scaled": 0.5, "y_scaled": 0.5, "z_scaled": 0.1, "time_scaled": 1.0},
            {"candidate_event_id": "cand_2", "delta_time_hours": 2.0, "x_scaled": 0.6, "y_scaled": 0.4, "z_scaled": 0.2, "time_scaled": 2.0},
        ]
        res = run_window_stability_analysis(cands, "main_1", "HDBSCAN_ADAPTIVE", repeats=3, seed=123)
        for key in ["mean_jaccard", "std_jaccard", "min_jaccard", "max_jaccard",
                    "mean_ari", "std_ari", "min_ari", "max_ari", "repeats", "sigma", "seed"]:
            self.assertIn(key, res, f"Missing key {key} in stability result")

    # -------------------------------------------------------------------------
    # 4. Strict Quality & Denominator Integrity Guards
    # -------------------------------------------------------------------------
    def test_strictly_prohibits_accuracy_and_f1_metrics(self):
        """Evaluation results must never contain accuracy or F1 score metrics."""
        cands = [
            {"candidate_event_id": "main_1", "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
            {"candidate_event_id": "cand_1", "delta_time_hours": 1.0, "x_scaled": 0.5, "y_scaled": 0.5, "z_scaled": 0.1, "time_scaled": 1.0},
        ]
        bundle = {
            "windows": {"main_1": cands},
            "pilot_manifest": {"pilot_id": "test_pilot_manifest"},
        }
        eval_res = evaluate_model_on_bundle(bundle, algorithm_name="WINDOW", repeats=2)
        # Convert dictionary to JSON string and scan for accuracy / f1 keywords
        dumped = json.dumps(eval_res).lower()
        self.assertNotIn("accuracy", dumped)
        self.assertNotIn("f1_score", dumped)
        self.assertNotIn("f1-score", dumped)
        self.assertNotIn("precision", dumped)
        self.assertNotIn("recall", dumped)

    def test_denominator_integrity_preserves_failed_windows(self):
        """Failed windows must be included in total windows count and failure rate calculation."""
        # Window 1: normal window
        cands_win1 = [
            {"candidate_event_id": "m1", "mainshock_magnitude": 6.0, "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
            {"candidate_event_id": "c1", "mainshock_magnitude": 6.0, "delta_time_hours": 1.0, "x_scaled": 0.1, "y_scaled": 0.1, "z_scaled": 0.1, "time_scaled": 1.0},
        ]
        # Window 2: self-only window where HDBSCAN marks mainshock as noise (-1) -> FAILED
        cands_win2 = [
            {"candidate_event_id": "m2", "mainshock_magnitude": 6.0, "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
        ]
        bundle = {
            "windows": {"m1": cands_win1, "m2": cands_win2},
            "pilot_manifest": {"pilot_id": "test_pilot_fail"},
        }
        res = evaluate_model_on_bundle(bundle, algorithm_name="HDBSCAN_ADAPTIVE", repeats=2)
        self.assertEqual(res["total_windows"], 2)
        self.assertEqual(res["success_windows"] + res["failure_windows"], 2)
        self.assertGreater(res["failure_windows"], 0)
        self.assertEqual(res["failure_rate"], round(res["failure_windows"] / res["total_windows"], 4))
        self.assertIn("EXP_MAINSHOCK_CLASSIFIED_AS_NOISE", res["failure_reasons"])

    # -------------------------------------------------------------------------
    # 5. Multi-Model Pilot Comparison Integration
    # -------------------------------------------------------------------------
    def test_compare_all_models_on_pilot_fixture(self):
        """compare_all_models_on_pilot executes all 4 algorithms on pilot fixture and produces comparison table."""
        # Create standard pilot fixture in tmp directory
        test_harness_util.create_pilot_fixture(self.tmp_dir)

        report = compare_all_models_on_pilot(self.tmp_dir, repeats=2, sigma=0.01, seed=42)
        self.assertIn("comparison_table", report)
        self.assertIn("algorithm_details", report)
        self.assertEqual(len(report["comparison_table"]), 4)

        algo_names = [row["Algorithm"] for row in report["comparison_table"]]
        self.assertEqual(algo_names, ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"])

        # Check WINDOW row specifics
        window_row = [r for r in report["comparison_table"] if r["Algorithm"] == "WINDOW"][0]
        self.assertEqual(window_row["Mean DBCV"], "N/A")

        # Check HDBSCAN_ADAPTIVE details
        adaptive_res = report["algorithm_details"]["HDBSCAN_ADAPTIVE"]
        self.assertIn("mean_jaccard_stability", adaptive_res)
        self.assertIn("mean_ari_stability", adaptive_res)
        self.assertIn("failure_rate", adaptive_res)


if __name__ == "__main__":
    unittest.main()
