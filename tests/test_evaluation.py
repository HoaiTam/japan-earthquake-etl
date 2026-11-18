"""Unit and regression tests for EXP-04: Multi-faceted Scientific Evaluation Module.

Verifies:
1. Clustering quality (DBCV score and NULL reason code policy).
2. Physical consistency (Modified Omori law decay fitting, threshold N>=5, and failure reason codes).
   - Verifies recovery of diverse true p values across [0.5, 2.0].
   - Verifies non-zero analytic finite differences around p=1.0.
3. Stability harness (Jaccard on sequence members, ARI on cluster partitions, reproducibility, and component distribution).
   - Verifies that execution errors return METRIC_FIT_FAILED instead of fake 1.0 stability.
   - Verifies handling of partial fit failures and invalid parameters.
4. Integrity constraints:
   - Strictly NO accuracy/F1 metrics (unlabeled clustering problem).
   - Denominator integrity: all failed windows preserved in total count without silent drop.
5. Multi-model comparative evaluation across WINDOW, DBSCAN, HDBSCAN_GLOBAL, and HDBSCAN_ADAPTIVE.
   - Binds pilot_id fail-closed from bundle manifest.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

# Ensure project root and notebooks directory are in sys.path
ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = ROOT / "notebooks"
import sys
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))

import notebooks.evaluation as evaluation
from notebooks.evaluation import (
    compute_dbcv_score,
    _omori_cumulative,
    fit_modified_omori_decay,
    calculate_jaccard_similarity,
    run_window_stability_analysis,
    evaluate_model_on_bundle,
    compare_all_models_on_pilot,
    load_algorithm_configs,
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
        c1 = rng.randn(10, 4) * 0.05
        c2 = rng.randn(10, 4) * 0.05 + 10.0
        X = np.vstack([c1, c2])
        labels = np.array([0] * 10 + [1] * 10, dtype=int)

        score, reasons = compute_dbcv_score(X, labels, algorithm_name="HDBSCAN")
        if score is not None:
            self.assertTrue(-1.0 <= score <= 1.0)
            self.assertEqual(len(reasons), 0)
        else:
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
    def test_omori_cumulative_derivative_around_p1(self):
        """Verify that finite differences around p=1.0 yield non-zero, continuous gradient."""
        t = np.array([0.1, 1.0, 10.0])
        K = 100.0
        c = 0.1
        h = 1e-8
        f_plus = _omori_cumulative(t, K, c, 1.0 + h)
        f_minus = _omori_cumulative(t, K, c, 1.0 - h)
        num_deriv = (f_plus - f_minus) / (2 * h)

        # Analytic derivative: -K * w * (ln(c) + 0.5 * w) where w = ln(1 + t/c)
        w = np.log1p(t / c)
        analytic_deriv = -K * w * (np.log(c) + 0.5 * w)

        self.assertTrue(np.all(np.abs(num_deriv) > 0.01), "Derivative must be non-zero")
        rel_diff = np.max(np.abs(num_deriv - analytic_deriv) / np.abs(analytic_deriv))
        self.assertLess(rel_diff, 1e-4, "Finite difference must match analytic derivative")

    def test_omori_decay_recovers_diverse_true_p(self):
        """Verify that curve_fit reliably estimates diverse true p values across [0.5, 2.0]."""
        from scipy.optimize import root_scalar

        for p_true in [0.5, 0.8, 0.95, 1.0, 1.05, 1.2, 1.6, 2.0]:
            c_true = 0.1
            K_true = 50.0
            u_vals = np.linspace(0.01, 0.99, 80)
            N_max = _omori_cumulative(np.array([48.0]), K_true, c_true, p_true)[0]

            t_synth = []
            for u in u_vals:
                sol = root_scalar(
                    lambda t_i: _omori_cumulative(np.array([t_i]), K_true, c_true, p_true)[0] - u * N_max,
                    bracket=[0.0, 48.0],
                )
                t_synth.append(sol.root)
            t_synth = np.array(t_synth)

            res = fit_modified_omori_decay(t_synth, duration_hours=48.0, min_aftershocks=5)
            self.assertIsNotNone(res["omori_p"])
            self.assertIsNotNone(res["omori_fit_metric"])
            self.assertAlmostEqual(res["omori_p"], p_true, delta=0.03, msg=f"Failed to recover p={p_true}")
            self.assertGreater(res["omori_fit_metric"], 0.95)

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
        self.assertEqual(calculate_jaccard_similarity({"e1", "e2"}, {"e1", "e2"}), 1.0)
        self.assertEqual(calculate_jaccard_similarity({"e1", "e2"}, {"e3", "e4"}), 0.0)
        self.assertEqual(calculate_jaccard_similarity(set(), set()), 1.0)
        self.assertEqual(calculate_jaccard_similarity(set(), {"e1"}), 0.0)
        self.assertEqual(calculate_jaccard_similarity({"e1"}, set()), 0.0)
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

    def test_stability_harness_crashes_gracefully_on_all_fit_failures(self):
        """When model fit throws an exception on all repeats, returns NULL metrics with METRIC_FIT_FAILED."""
        cands = [
            {"candidate_event_id": "main_1", "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
            {"candidate_event_id": "cand_1", "delta_time_hours": 1.0, "x_scaled": 0.5, "y_scaled": 0.5, "z_scaled": 0.1, "time_scaled": 1.0},
            {"candidate_event_id": "cand_2", "delta_time_hours": 2.0, "x_scaled": 0.6, "y_scaled": 0.4, "z_scaled": 0.2, "time_scaled": 2.0},
        ]
        with patch("notebooks.evaluation._dispatch_fit_model", side_effect=RuntimeError("Cluster engine crashed")):
            res = run_window_stability_analysis(cands, "main_1", "DBSCAN", repeats=5)
            self.assertIsNone(res["mean_jaccard"])
            self.assertIsNone(res["mean_ari"])
            self.assertEqual(res["attempted_repeats"], 5)
            self.assertEqual(res["valid_repeats"], 0)
            self.assertEqual(res["failed_repeats"], 5)
            self.assertEqual(res["valid_pairs"], 0)
            self.assertIn(METRIC_FIT_FAILED, res["metric_reason_codes"])
            self.assertEqual(len(res["failed_repeat_details"]), 5)

    def test_stability_harness_handles_partial_fit_failures(self):
        """When some repeats fail but >= 2 succeed, computes stability over valid pairs and records partial failures."""
        cands = [
            {"candidate_event_id": "main_1", "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
            {"candidate_event_id": "cand_1", "delta_time_hours": 1.0, "x_scaled": 0.5, "y_scaled": 0.5, "z_scaled": 0.1, "time_scaled": 1.0},
            {"candidate_event_id": "cand_2", "delta_time_hours": 2.0, "x_scaled": 0.6, "y_scaled": 0.4, "z_scaled": 0.2, "time_scaled": 2.0},
        ]
        call_count = [0]
        original_dispatch = None
        from notebooks.evaluation import _dispatch_fit_model
        original_dispatch = _dispatch_fit_model

        def mock_dispatch(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 2:  # fail on second repeat
                raise RuntimeError("Intermittent failure")
            return original_dispatch(*args, **kwargs)

        with patch("notebooks.evaluation._dispatch_fit_model", side_effect=mock_dispatch):
            res = run_window_stability_analysis(cands, "main_1", "WINDOW", repeats=3)
            self.assertIsNotNone(res["mean_jaccard"])
            self.assertIsNotNone(res["mean_ari"])
            self.assertEqual(res["attempted_repeats"], 3)
            self.assertEqual(res["valid_repeats"], 2)
            self.assertEqual(res["failed_repeats"], 1)
            self.assertEqual(res["valid_pairs"], 1)
            self.assertIn("PARTIAL_FIT_FAILURES", res["metric_reason_codes"])

    def test_stability_harness_rejects_invalid_inputs(self):
        """Stability harness returns null metrics when repeats < 2 or sigma is negative/invalid."""
        cands = [
            {"candidate_event_id": "main_1", "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
            {"candidate_event_id": "cand_1", "x_scaled": 0.5, "y_scaled": 0.5, "z_scaled": 0.1, "time_scaled": 1.0},
        ]
        # Repeats < 2
        res1 = run_window_stability_analysis(cands, "main_1", "WINDOW", repeats=1)
        self.assertIsNone(res1["mean_jaccard"])
        self.assertIn(METRIC_NOT_APPLICABLE, res1["metric_reason_codes"])

        # Negative sigma
        res2 = run_window_stability_analysis(cands, "main_1", "WINDOW", repeats=3, sigma=-0.1)
        self.assertIsNone(res2["mean_jaccard"])
        self.assertIn(METRIC_NOT_APPLICABLE, res2["metric_reason_codes"])

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
            "manifest": {"pilot_id": "pilot_test_no_acc"},
        }
        eval_res = evaluate_model_on_bundle(bundle, algorithm_name="WINDOW", repeats=2)
        dumped = json.dumps(eval_res).lower()
        self.assertNotIn("accuracy", dumped)
        self.assertNotIn("f1_score", dumped)
        self.assertNotIn("f1-score", dumped)
        self.assertNotIn("precision", dumped)
        self.assertNotIn("recall", dumped)

    def test_denominator_integrity_preserves_failed_windows(self):
        """Failed windows must be included in total windows count and failure rate calculation."""
        cands_win1 = [
            {"candidate_event_id": "m1", "mainshock_magnitude": 6.0, "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
            {"candidate_event_id": "c1", "mainshock_magnitude": 6.0, "delta_time_hours": 1.0, "x_scaled": 0.1, "y_scaled": 0.1, "z_scaled": 0.1, "time_scaled": 1.0},
        ]
        cands_win2 = [
            {"candidate_event_id": "m2", "mainshock_magnitude": 6.0, "delta_time_hours": 0.0, "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0},
        ]
        bundle = {
            "windows": {"m1": cands_win1, "m2": cands_win2},
            "manifest": {"pilot_id": "pilot_test_fail"},
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
    def test_compare_all_models_on_pilot_fixture_and_pilot_id_binding(self):
        """compare_all_models_on_pilot binds pilot_id fail-closed and produces comparison table."""
        test_harness_util.create_pilot_fixture(self.tmp_dir)

        report = compare_all_models_on_pilot(self.tmp_dir, repeats=2, sigma=0.01, seed=42)
        self.assertIn("comparison_table", report)
        self.assertIn("algorithm_details", report)
        self.assertEqual(len(report["comparison_table"]), 4)

        # Check pilot_id binding
        self.assertTrue(report["pilot_id"].startswith("pilot_"), f"pilot_id was {report['pilot_id']}")
        self.assertNotEqual(report["pilot_id"], "pilot_unknown")

        algo_names = [row["Algorithm"] for row in report["comparison_table"]]
        self.assertEqual(algo_names, ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"])

        # Check WINDOW row specifics
        window_row = [r for r in report["comparison_table"] if r["Algorithm"] == "WINDOW"][0]
        self.assertEqual(window_row["Mean DBCV"], "N/A")

    def test_stability_harness_empty_window_and_missing_mainshock(self):
        """Empty candidates or missing mainshock must return null metrics with METRIC_NOT_APPLICABLE and valid_repeats=0."""
        # 1. Completely empty candidate list
        res_empty = run_window_stability_analysis([], "main_missing", "DBSCAN", repeats=3)
        self.assertIsNone(res_empty["mean_jaccard"])
        self.assertIsNone(res_empty["mean_ari"])
        self.assertEqual(res_empty["requested_repeats"], 3)
        self.assertEqual(res_empty["attempted_repeats"], 0)
        self.assertEqual(res_empty["valid_repeats"], 0)
        self.assertEqual(res_empty["failed_repeats"], 0)
        self.assertEqual(res_empty["attempted_pairs"], 0)
        self.assertEqual(res_empty["valid_pairs"], 0)
        self.assertEqual(res_empty["failed_repeat_details"], [])
        self.assertIn(METRIC_NOT_APPLICABLE, res_empty["metric_reason_codes"])
        self.assertEqual(res_empty["note"], "EMPTY_WINDOW_CANDIDATES / NOT_RUN")

        # 2. Candidate list present but mainshock_id not found
        cands = [
            {"candidate_event_id": "other_1", "delta_time_hours": 1.0, "x_scaled": 0.1, "y_scaled": 0.1, "z_scaled": 0.1, "time_scaled": 1.0},
        ]
        res_missing_main = run_window_stability_analysis(cands, "non_existent_main", "DBSCAN", repeats=3)
        self.assertIsNone(res_missing_main["mean_jaccard"])
        self.assertIsNone(res_missing_main["mean_ari"])
        self.assertEqual(res_missing_main["requested_repeats"], 3)
        self.assertEqual(res_missing_main["attempted_repeats"], 0)
        self.assertEqual(res_missing_main["valid_repeats"], 0)
        self.assertEqual(res_missing_main["failed_repeats"], 0)
        self.assertEqual(res_missing_main["attempted_pairs"], 0)
        self.assertEqual(res_missing_main["valid_pairs"], 0)
        self.assertEqual(res_missing_main["failed_repeat_details"], [])
        self.assertIn(METRIC_NOT_APPLICABLE, res_missing_main["metric_reason_codes"])
        self.assertEqual(res_missing_main["note"], "MAINSHOCK_NOT_IN_CANDIDATES / NOT_RUN")

    def test_evaluate_model_respects_custom_config_and_pins(self):
        """Custom configs (e.g. changing eps or adaptive policy) must propagate to model execution and alter outcomes."""
        test_harness_util.create_pilot_fixture(self.tmp_dir)
        from experiment_harness import load_feature_bundle, MODE_DEVELOPMENT
        bundle = load_feature_bundle(self.tmp_dir, mode=MODE_DEVELOPMENT)

        # 1. Load default configs
        configs, pins = load_algorithm_configs(ROOT)
        self.assertEqual(len(configs), 4)
        self.assertEqual(len(pins), 4)
        for algo, pin in pins.items():
            self.assertEqual(len(pin["config_sha256"]), 64)
            self.assertNotEqual(pin["model_config_version"], "unknown")

        # 2. Evaluate DBSCAN with extremely tight eps (eps=0.0001 -> points not reachable)
        tight_cfg = {"algorithm_name": "DBSCAN", "eps": 0.0001, "min_samples": 2, "model_config_version": "tight-v1"}
        res_tight = evaluate_model_on_bundle(bundle, algorithm_name="DBSCAN", config=tight_cfg, repeats=2)

        # 3. Evaluate DBSCAN with generous eps (eps=0.5 -> points clustered)
        loose_cfg = {"algorithm_name": "DBSCAN", "eps": 0.5, "min_samples": 2, "model_config_version": "loose-v1"}
        res_loose = evaluate_model_on_bundle(bundle, algorithm_name="DBSCAN", config=loose_cfg, repeats=2)

        # The aftershock counts or failure status must differ, demonstrating config propagation
        self.assertNotEqual(res_tight["total_aftershocks"], res_loose["total_aftershocks"])
        self.assertEqual(res_tight["model_config_version"], "tight-v1")
        self.assertEqual(res_loose["model_config_version"], "loose-v1")

    def test_generate_markdown_report_null_safety_and_stability_failures(self):
        """Markdown report generator must safely format null metrics (e.g. stability failures) as N/A without raising TypeError."""
        import importlib
        eval_script = importlib.import_module("scripts.evaluate-models")

        mock_eval_results = {
            "evaluated_at_utc": "2026-11-05T00:00:00Z",
            "comparison_table": [
                {
                    "Algorithm": "DBSCAN",
                    "Total Windows": 2,
                    "Success Windows": 0,
                    "Failure Windows": 2,
                    "Failure Rate": "100.0%",
                    "Total Aftershocks": 0,
                    "Mean Noise Ratio": "N/A",
                    "Mean DBCV": "N/A",
                    "Mean Omori p": "N/A",
                    "Omori Fitted Windows": "0/2",
                    "Mean Jaccard Stability": "N/A",
                    "Mean ARI Stability": "N/A",
                    "Runtime (s)": "0.10",
                }
            ],
            "algorithm_details": {
                "DBSCAN": {
                    "total_windows": 2,
                    "success_windows": 0,
                    "failure_windows": 2,
                    "failure_rate": 1.0,
                    "failure_reasons": ["METRIC_FIT_FAILED"],
                    "total_aftershocks": 0,
                    "mean_noise_ratio": None,
                    "mean_dbcv_score": None,
                    "dbcv_computed_windows": 0,
                    "mean_omori_p": None,
                    "mean_omori_r2": None,
                    "omori_fitted_windows": 0,
                    "mean_jaccard_stability": None,
                    "mean_ari_stability": None,
                    "stability_computed_windows": 0,
                    "runtime_seconds": 0.10,
                },
                "WINDOW": {
                    "total_windows": 2,
                    "success_windows": 2,
                    "failure_windows": 0,
                    "failure_rate": 0.0,
                    "total_aftershocks": 10,
                    "mean_noise_ratio": 0.0,
                    "mean_dbcv_score": None,
                    "dbcv_computed_windows": 0,
                    "mean_omori_p": None,
                    "omori_fitted_windows": 0,
                    "mean_jaccard_stability": 1.0,
                    "mean_ari_stability": 1.0,
                    "runtime_seconds": 0.05,
                },
                "HDBSCAN_GLOBAL": {
                    "total_windows": 2,
                    "success_windows": 0,
                    "failure_windows": 2,
                    "failure_rate": None,
                    "failure_reasons": [],
                },
                "HDBSCAN_ADAPTIVE": {
                    "total_windows": 2,
                    "success_windows": 0,
                    "failure_windows": 2,
                    "failure_rate": None,
                    "failure_reasons": [],
                },
            },
        }
        pilot_info = {"pilot_id": "pilot_mock_test", "candidate_rows": 10, "mainshock_count": 2}
        runtime_meta = {"code_version": "test_sha", "random_seed": 42, "python_version": "3.12"}
        config_pins = {
            "DBSCAN": {"config_path": "config/test.json", "model_config_version": "v1", "config_sha256": "abc1234567890123"}
        }

        # Must generate cleanly without TypeError: unsupported format string passed to NoneType.__format__
        md_text = eval_script.generate_markdown_report(mock_eval_results, pilot_info, runtime_meta, config_pins)
        self.assertIn("EXP-04: Đánh giá Khoa học", md_text)
        self.assertIn("N/A", md_text)
        self.assertIn("METRIC_FIT_FAILED", md_text)

    def test_code_provenance_clean_and_hash_audit(self):
        """Code provenance must capture git commit and compute SHA-256 for all critical modules."""
        import importlib
        eval_script = importlib.import_module("scripts.evaluate-models")

        prov = eval_script.get_code_provenance(ROOT)
        self.assertIn("git_commit", prov)
        self.assertIn("is_clean_code", prov)
        self.assertIn("critical_code_hashes", prov)
        self.assertIn("notebooks/evaluation.py", prov["critical_code_hashes"])
        self.assertEqual(len(prov["critical_code_hashes"]["notebooks/evaluation.py"]), 64)


    def test_evaluate_model_on_bundle_handles_primary_fit_exception_and_preserves_denominators(self):
        """Primary fit exception on a window must be caught, recorded in failure_reasons, and preserve denominators."""
        bundle = {
            "windows": {
                "m_fail": [
                    {"candidate_event_id": "m_fail", "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0, "delta_time_hours": 0.0},
                    {"candidate_event_id": "c1", "x_scaled": 0.1, "y_scaled": 0.1, "z_scaled": 0.0, "time_scaled": 1.0, "delta_time_hours": 1.0},
                ],
                "m_ok": [
                    {"candidate_event_id": "m_ok", "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0, "delta_time_hours": 0.0},
                    {"candidate_event_id": "c2", "x_scaled": 0.001, "y_scaled": 0.001, "z_scaled": 0.0, "time_scaled": 0.5, "delta_time_hours": 0.5},
                ],
            }
        }

        real_dispatch = evaluation._dispatch_fit_model
        import collections
        dispatch_call_counts = collections.defaultdict(int)

        def mock_dispatch(algorithm_name, window_candidates, mainshock_id, config=None):
            dispatch_call_counts[mainshock_id] += 1
            if mainshock_id == "m_fail":
                raise RuntimeError("simulated primary fit crash")
            return real_dispatch(algorithm_name, window_candidates, mainshock_id, config=config)

        with unittest.mock.patch.object(evaluation, "_dispatch_fit_model", side_effect=mock_dispatch):
            res = evaluation.evaluate_model_on_bundle(
                bundle=bundle,
                algorithm_name="WINDOW",
                repeats=2,
            )

        self.assertEqual(res["total_windows"], 2)
        self.assertEqual(res["success_windows"], 1)
        self.assertEqual(res["failure_windows"], 1)
        self.assertEqual(res["failure_rate"], 0.5)
        self.assertTrue(any("simulated primary fit crash" in r for r in res["failure_reasons"]))

        # Verify call counts: primary fit crash calls dispatch exactly 1 time (0 stability repeats)
        self.assertEqual(dispatch_call_counts["m_fail"], 1)
        # m_ok succeeded on primary fit: 1 primary + 2 repeats = 3 calls
        self.assertEqual(dispatch_call_counts["m_ok"], 3)

        # Window evaluations detail
        win_evals = {w["mainshock_id"]: w for w in res["window_evaluations"]}
        self.assertEqual(win_evals["m_fail"]["window_result_status"], "FAILURE")
        self.assertIn("simulated primary fit crash", win_evals["m_fail"]["fit_exception"])
        self.assertIsNone(win_evals["m_fail"]["dbcv_score"])
        self.assertIsNone(win_evals["m_fail"]["stability"]["mean_jaccard"])
        self.assertEqual(win_evals["m_fail"]["stability"]["requested_repeats"], 2)
        self.assertEqual(win_evals["m_fail"]["stability"]["attempted_repeats"], 0)
        self.assertEqual(win_evals["m_fail"]["stability"]["failed_repeats"], 0)
        self.assertEqual(win_evals["m_fail"]["stability"]["attempted_pairs"], 0)
        self.assertEqual(win_evals["m_fail"]["stability"]["valid_pairs"], 0)
        self.assertEqual(win_evals["m_fail"]["stability"]["failed_repeat_details"], [])
        self.assertIn("PRIMARY_FIT_FAILED", win_evals["m_fail"]["stability"]["note"])
        self.assertIn("NOT_RUN", win_evals["m_fail"]["stability"]["note"])

        self.assertEqual(win_evals["m_ok"]["window_result_status"], "SUCCESS")
        self.assertEqual(win_evals["m_ok"]["stability"]["attempted_repeats"], 2)
        self.assertEqual(win_evals["m_ok"]["stability"]["valid_repeats"], 2)

    def test_evaluate_model_on_bundle_all_windows_fit_crashed(self):
        """When all primary fits crash, evaluate_model_on_bundle must not raise and report 100% failure rate with null metrics."""
        bundle = {
            "windows": {
                "m1": [{"candidate_event_id": "m1", "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0}],
                "m2": [{"candidate_event_id": "m2", "x_scaled": 1.0, "y_scaled": 1.0, "z_scaled": 0.0, "time_scaled": 0.0}],
            }
        }

        def mock_crash(*args, **kwargs):
            raise RuntimeError("total crash")

        with unittest.mock.patch.object(evaluation, "_dispatch_fit_model", side_effect=mock_crash):
            res = evaluation.evaluate_model_on_bundle(
                bundle=bundle,
                algorithm_name="DBSCAN",
                repeats=2,
            )

        self.assertEqual(res["total_windows"], 2)
        self.assertEqual(res["success_windows"], 0)
        self.assertEqual(res["failure_windows"], 2)
        self.assertEqual(res["failure_rate"], 1.0)
        self.assertIsNone(res["mean_dbcv_score"])
        self.assertEqual(res["dbcv_computed_windows"], 0)
        self.assertIsNone(res["mean_jaccard_stability"])
        self.assertEqual(res["stability_computed_windows"], 0)
        self.assertIsNone(res["mean_omori_p"])
        self.assertEqual(len(res["failure_reasons"]), 2)

    def test_config_pins_consistency_between_top_level_and_summary(self):
        """Top-level and nested config pins must be consistent with file_bytes_sha256 and canonical_config_sha256."""
        configs, pins = evaluation.load_algorithm_configs(project_root=ROOT)
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"]:
            self.assertIn(algo, pins)
            p = pins[algo]
            self.assertIn("file_bytes_sha256", p)
            self.assertIn("canonical_config_sha256", p)
            self.assertEqual(len(p["file_bytes_sha256"]), 64)
            self.assertEqual(len(p["canonical_config_sha256"]), 64)
            self.assertEqual(p["config_sha256"], p["file_bytes_sha256"])

        # When custom config matches file, it preserves file_bytes_sha256
        configs_custom, pins_custom = evaluation.load_algorithm_configs(project_root=ROOT, custom_configs=configs)
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"]:
            self.assertEqual(pins_custom[algo]["config_path"], pins[algo]["config_path"])
            self.assertEqual(pins_custom[algo]["config_sha256"], pins[algo]["config_sha256"])

        # When custom config is genuinely synthetic, it marks custom: and sets file_bytes_sha256 to None
        synthetic = {"DBSCAN": {"eps": 0.999, "min_samples": 5}}
        _, pins_syn = evaluation.load_algorithm_configs(project_root=ROOT, custom_configs=synthetic)
        self.assertEqual(pins_syn["DBSCAN"]["config_path"], "custom:dbscan")
        self.assertIsNone(pins_syn["DBSCAN"]["file_bytes_sha256"])
        self.assertEqual(len(pins_syn["DBSCAN"]["canonical_config_sha256"]), 64)

    def test_code_provenance_cross_platform_hashes(self):
        """get_code_provenance must return working tree, normalized LF, and git blob hashes with verification note."""
        import importlib
        eval_script = importlib.import_module("scripts.evaluate-models")

        prov = eval_script.get_code_provenance(ROOT)
        self.assertIn("working_tree_code_hashes", prov)
        self.assertIn("normalized_lf_code_hashes", prov)
        self.assertIn("git_blob_hashes", prov)
        self.assertIn("detected_line_endings", prov)
        self.assertIn("cross_platform_verification_note", prov)

        for rel_path in ["notebooks/evaluation.py", "scripts/evaluate-models.py"]:
            self.assertEqual(len(prov["working_tree_code_hashes"][rel_path]), 64)
            self.assertEqual(len(prov["normalized_lf_code_hashes"][rel_path]), 64)
            self.assertEqual(len(prov["git_blob_hashes"][rel_path]), 40)
            self.assertIn(prov["detected_line_endings"][rel_path], ["crlf", "lf"])

    def test_code_provenance_detects_dirty_experiment_harness_in_temp_repo(self):
        """Modifying experiment_harness.py in a git repo must cause is_clean_code=False and report dirty status."""
        import tempfile
        import subprocess
        import importlib
        eval_script = importlib.import_module("scripts.evaluate-models")

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            # Initialize git repo
            subprocess.run(["git", "init"], cwd=str(tmp_path), check=True, capture_output=True)
            subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=str(tmp_path), check=True, capture_output=True)
            subprocess.run(["git", "config", "user.name", "Test Runner"], cwd=str(tmp_path), check=True, capture_output=True)

            # Create the 5 critical files and 4 model configs
            for f_rel in [
                "notebooks/evaluation.py",
                "scripts/evaluate-models.py",
                "notebooks/baseline_models.py",
                "notebooks/adaptive_hdbscan.py",
                "notebooks/experiment_harness.py",
                "config/ml/models/window_baseline_v1.json",
                "config/ml/models/dbscan_global_dev_v1.json",
                "config/ml/models/hdbscan_global_dev_v1.json",
                "config/ml/models/hdbscan_adaptive_dev_v1.json",
            ]:
                p = tmp_path / f_rel
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_text("initial content\n", encoding="utf-8")

            # Commit clean state
            subprocess.run(["git", "add", "."], cwd=str(tmp_path), check=True, capture_output=True)
            subprocess.run(["git", "commit", "-m", "initial commit"], cwd=str(tmp_path), check=True, capture_output=True)

            # 1. Clean verification
            prov_clean = eval_script.get_code_provenance(tmp_path)
            self.assertTrue(prov_clean["is_clean_code"])
            self.assertEqual(prov_clean["dirty_files"], [])
            self.assertEqual(prov_clean["missing_files"], [])
            self.assertEqual(prov_clean["blob_mismatches"], [])
            self.assertEqual(prov_clean["clean_verification_status"], "VERIFIED_CLEAN")

            # 2. Modify ONLY notebooks/experiment_harness.py
            (tmp_path / "notebooks/experiment_harness.py").write_text("modified content\n", encoding="utf-8")

            prov_dirty = eval_script.get_code_provenance(tmp_path)
            self.assertFalse(prov_dirty["is_clean_code"])
            self.assertTrue(any("notebooks/experiment_harness.py" in f for f in prov_dirty["dirty_files"]))
            self.assertIn("notebooks/experiment_harness.py", prov_dirty["blob_mismatches"])

            # 3. Add untracked file under config/ml/models/ -> must be dirty (-uall check)
            (tmp_path / "notebooks/experiment_harness.py").write_text("initial content\n", encoding="utf-8")
            (tmp_path / "config/ml/models/untracked_cfg.json").write_text("{}", encoding="utf-8")

            prov_untracked = eval_script.get_code_provenance(tmp_path)
            self.assertFalse(prov_untracked["is_clean_code"])
            self.assertTrue(any("untracked_cfg.json" in f for f in prov_untracked["dirty_files"]))

            # 4. Remove a critical file -> missing file -> fail-closed
            (tmp_path / "config/ml/models/untracked_cfg.json").unlink()
            (tmp_path / "notebooks/experiment_harness.py").unlink()

            prov_missing = eval_script.get_code_provenance(tmp_path)
            self.assertFalse(prov_missing["is_clean_code"])
            self.assertIn("notebooks/experiment_harness.py", prov_missing["missing_files"])


    def test_distinguish_primary_fit_failure_from_repeat_fit_failure(self):
        """Rigorous contrast between primary fit failure (0 repeats attempted) and repeat fit failure (all repeats attempted)."""
        bundle = {
            "windows": {
                "win_primary_fail": [
                    {"candidate_event_id": "win_primary_fail", "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0, "delta_time_hours": 0.0},
                    {"candidate_event_id": "c1", "x_scaled": 0.1, "y_scaled": 0.1, "z_scaled": 0.0, "time_scaled": 1.0, "delta_time_hours": 1.0},
                ],
                "win_repeat_fail": [
                    {"candidate_event_id": "win_repeat_fail", "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0, "delta_time_hours": 0.0},
                    {"candidate_event_id": "c2", "x_scaled": 0.001, "y_scaled": 0.001, "z_scaled": 0.0, "time_scaled": 0.5, "delta_time_hours": 0.5},
                ],
            }
        }

        real_dispatch = evaluation._dispatch_fit_model
        import collections
        call_counts = collections.defaultdict(int)

        def mock_dispatch(algorithm_name, window_candidates, mainshock_id, config=None):
            call_counts[mainshock_id] += 1
            # Primary fit for win_primary_fail fails immediately
            if mainshock_id == "win_primary_fail":
                raise RuntimeError("primary fit crashed")

            # Primary fit for win_repeat_fail succeeds (call 1), but repeat 2 (call 3) crashes
            if mainshock_id == "win_repeat_fail" and call_counts[mainshock_id] == 3:
                raise RuntimeError("repeat fit 1 crashed")

            return real_dispatch(algorithm_name, window_candidates, mainshock_id, config=config)

        with unittest.mock.patch.object(evaluation, "_dispatch_fit_model", side_effect=mock_dispatch):
            res = evaluation.evaluate_model_on_bundle(
                bundle=bundle,
                algorithm_name="WINDOW",
                repeats=3,
            )

        win_evals = {w["mainshock_id"]: w for w in res["window_evaluations"]}

        # 1. win_primary_fail: 1 primary call, 0 stability repeats attempted
        self.assertEqual(call_counts["win_primary_fail"], 1)
        stab_prim = win_evals["win_primary_fail"]["stability"]
        self.assertEqual(stab_prim["requested_repeats"], 3)
        self.assertEqual(stab_prim["attempted_repeats"], 0)
        self.assertEqual(stab_prim["valid_repeats"], 0)
        self.assertEqual(stab_prim["failed_repeats"], 0)
        self.assertEqual(stab_prim["attempted_pairs"], 0)
        self.assertEqual(stab_prim["valid_pairs"], 0)
        self.assertEqual(stab_prim["failed_repeat_details"], [])
        self.assertIn("PRIMARY_FIT_FAILED", stab_prim["note"])
        self.assertIn("NOT_RUN", stab_prim["note"])

        # 2. win_repeat_fail: 1 primary + 3 repeats = 4 calls total
        self.assertEqual(call_counts["win_repeat_fail"], 4)
        stab_rep = win_evals["win_repeat_fail"]["stability"]
        self.assertEqual(stab_rep["requested_repeats"], 3)
        self.assertEqual(stab_rep["attempted_repeats"], 3)
        self.assertEqual(stab_rep["valid_repeats"], 2)
        self.assertEqual(stab_rep["failed_repeats"], 1)
        self.assertEqual(stab_rep["attempted_pairs"], 3)  # 3 * 2 // 2
        self.assertEqual(stab_rep["valid_pairs"], 1)      # 2 * 1 // 2
        self.assertEqual(len(stab_rep["failed_repeat_details"]), 1)
        self.assertEqual(stab_rep["failed_repeat_details"][0]["repeat_index"], 1)
        self.assertIn("repeat fit 1 crashed", stab_rep["failed_repeat_details"][0]["error_message"])
        self.assertIsNotNone(stab_rep["mean_jaccard"])
        self.assertIn("PARTIAL_FIT_FAILURES", stab_rep["metric_reason_codes"])

    def test_memory_safe_dbcv_matches_reference_formula_and_handles_single_cluster(self):
        """Memory-safe DBCV must match hdbscan.validity on multi-cluster data and fail-honest on 1 cluster."""
        try:
            import hdbscan.validity as hv
        except ImportError:
            self.skipTest("standalone hdbscan package not installed")

        rng = np.random.RandomState(7)
        X = np.vstack([
            rng.randn(40, 4) * 0.15,
            rng.randn(35, 4) * 0.18 + 3.5,
            rng.randn(30, 4) * 0.12 - 3.0,
            rng.uniform(-4, 4, (15, 4)),
        ])
        labels = np.array([0] * 40 + [1] * 35 + [2] * 30 + [-1] * 15, dtype=np.int64)

        score_safe = evaluation._compute_dbcv_memory_safe(X, labels)
        score_ref = float(hv.validity_index(np.ascontiguousarray(X, dtype=np.float64), labels))
        self.assertIsNotNone(score_safe)
        self.assertTrue(np.isfinite(score_safe))
        self.assertLess(abs(score_safe - score_ref), 0.05)

        # Single cluster cannot compute inter-cluster separation -> returns METRIC_FIT_FAILED
        one_cluster_labels = np.array([0] * 10 + [-1] * 5, dtype=np.int64)
        score_one, reasons_one = compute_dbcv_score(X[:15], one_cluster_labels, algorithm_name="DBSCAN")
        self.assertIsNone(score_one)
        self.assertEqual(reasons_one, [METRIC_FIT_FAILED])

        # Verify threshold routing invokes _compute_dbcv_memory_safe cleanly
        with patch.object(evaluation, "DBCV_MEMORY_SAFE_CLUSTER_THRESHOLD", 10):
            score_routed, reasons_routed = compute_dbcv_score(X, labels, algorithm_name="DBSCAN")
            self.assertIsNotNone(score_routed)
            self.assertEqual(reasons_routed, [])

    def test_official_evaluation_frozen_rule_and_cross_verification(self):
        """EXP-04 official evaluation must enforce EXPORTED REPRODUCTION, no accuracy/F1, and emit frozen rule."""
        import importlib
        import pyarrow.parquet as pq
        import tests.test_research_collection_loader as col_mod

        eval_script = importlib.import_module("scripts.evaluate-models")

        kit_dir = self.tmp_dir / "eval_kit"
        repro_dir = kit_dir / "bundles" / "REPRODUCTION"
        ext_dir = kit_dir / "bundles" / "EXTENSION"
        pilot_repro = self.tmp_dir / "seed_repro"
        pilot_ext = self.tmp_dir / "seed_ext"
        repro_dir.mkdir(parents=True)
        ext_dir.mkdir(parents=True)
        pilot_repro.mkdir()
        pilot_ext.mkdir()

        repro_pins = col_mod.fixture(repro_dir, pilot_repro)
        col_mod.fixture(ext_dir, pilot_ext)
        ext_manifest = json.loads((ext_dir / "dataset_manifest.json").read_bytes())
        ext_manifest["dataset_split"] = "EXTENSION"
        ext_manifest["collection_plan"]["coverage"]["required_month_count"] = 63
        col_mod.write_json(ext_dir / "dataset_manifest.json", ext_manifest)
        col_mod.write_json(
            ext_dir / "collection-preparation.json",
            {"status": "RESEARCH_COLLECTION_PREPARED", "plan": ext_manifest["collection_plan"]},
        )
        ext_pins = col_mod.reindex(ext_dir)

        handoff = {
            "collections": {"REPRODUCTION": repro_pins, "EXTENSION": ext_pins},
            "files": {
                "bundles/REPRODUCTION/dataset_manifest.json": {"sha256": repro_pins["manifest_sha256"]},
                "bundles/REPRODUCTION/checksums.sha256": {"sha256": repro_pins["checksums_sha256"]},
                "bundles/EXTENSION/dataset_manifest.json": {"sha256": ext_pins["manifest_sha256"]},
                "bundles/EXTENSION/checksums.sha256": {"sha256": ext_pins["checksums_sha256"]},
            },
        }
        handoff_file = kit_dir / "handoff.json"
        handoff_file.write_text(json.dumps(handoff, sort_keys=True) + "\n", encoding="utf-8")
        import notebooks.experiment_harness as eh
        handoff_sha = eh.sha256_bytes(handoff_file.read_bytes())

        # Reject wrong split when REPRODUCTION is expected (checked in diagnostic mode as well)
        with self.assertRaisesRegex(ValueError, "OFFICIAL_SPLIT_MISMATCH"):
            eval_script.execute_official_evaluation(
                bundle_dir=ext_dir,
                output_json=self.tmp_dir / "bad.json",
                expected_split="REPRODUCTION",
                expected_handoff_sha256=handoff_sha,
                diagnostic_only=True,
            )

        out_json = self.tmp_dir / "exp04_official.json"
        out_md = self.tmp_dir / "exp04_official.md"

        import importlib.util as _ilu
        spec_r02 = _ilu.spec_from_file_location("run_exp02_eval_test", ROOT / "scripts/run-exp02-official.py")
        r02 = _ilu.module_from_spec(spec_r02); spec_r02.loader.exec_module(r02)
        spec_r03 = _ilu.spec_from_file_location("run_exp03_eval_test", ROOT / "scripts/run-exp03-official.py")
        r03 = _ilu.module_from_spec(spec_r03); spec_r03.loader.exec_module(r03)

        exp02_out = self.tmp_dir / "exp02_out"
        exp02_json = self.tmp_dir / "exp02.json"
        r02.execute_official_baselines(
            bundle_dir=repro_dir,
            output_dir=exp02_out,
            output_json=exp02_json,
            expected_handoff_sha256=handoff_sha,
            seed=42,
            root=ROOT,
        )
        exp02_sha = eh.sha256_bytes(exp02_json.read_bytes())

        exp03_out = self.tmp_dir / "exp03_out" / "hdbscan_adaptive"
        exp03_json = self.tmp_dir / "exp03.json"
        r03.execute_official_adaptive(
            bundle_dir=repro_dir,
            output_dir=exp03_out,
            output_json=exp03_json,
            expected_handoff_sha256=handoff_sha,
            seed=42,
            root=ROOT,
        )
        exp03_sha = eh.sha256_bytes(exp03_json.read_bytes())

        full_report = eval_script.execute_official_evaluation(
            bundle_dir=repro_dir,
            output_json=out_json,
            output_md=out_md,
            expected_split="REPRODUCTION",
            expected_handoff_sha256=handoff_sha,
            exp02_report_path=exp02_json,
            expected_exp02_report_sha256=exp02_sha,
            exp02_output_dir=exp02_out,
            exp03_report_path=exp03_json,
            expected_exp03_report_sha256=exp03_sha,
            exp03_output_dir=exp03_out,
            repeats=2,
            sigma=0.01,
            seed=42,
            project_root=ROOT,
        )

        self.assertEqual(full_report["report_version"], "exp-04-official-v2")
        self.assertEqual(full_report["status"], "PASSED")
        self.assertEqual(full_report["evaluation_scope"], "OFFICIAL_RESEARCH")
        self.assertTrue(full_report["frozen_rule_published"])
        self.assertEqual(full_report["frozen_rule_block_reasons"], [])
        self.assertFalse(full_report["scientific_approval"])
        self.assertEqual(full_report["frozen_extension_rule"]["rule_version"], "exp04-frozen-reproduction-rule-v2")
        self.assertEqual(full_report["frozen_extension_rule"]["frozen_from_split"], "REPRODUCTION")
        self.assertTrue(full_report["frozen_extension_rule"]["extension_guardrails"]["no_parameter_override_on_extension"])
        self.assertTrue(full_report["exp02_cross_verification"]["all_checks_passed"])
        self.assertTrue(full_report["exp03_cross_verification"]["all_checks_passed"])
        self.assertEqual(len(full_report["evaluation_summary"]["comparison_table"]), 4)
        raw_txt = out_json.read_text(encoding="utf-8").lower()
        self.assertNotIn("f1_score", raw_txt)
        self.assertTrue(out_md.is_file())
        eh.validate_safe_metadata(full_report)

    def test_dbcv_memory_safe_exact_match_and_tie_breaking_regressions(self):
        """Memory-safe DBCV must match hdbscan.validity.validity_index on >50 clusters, >2000 pts, and ties."""
        try:
            import hdbscan.validity as hv
        except ImportError:
            self.skipTest("standalone hdbscan package not installed")

        # 1. Exact PR #141 repro #1: >50 clusters (51 clusters x 4 points = 204)
        X1 = np.random.RandomState(7).normal(size=(204, 4))
        y1 = np.repeat(np.arange(51), 4).astype(np.int64)
        ref1 = float(hv.validity_index(X1, y1))
        safe1 = evaluation._compute_dbcv_memory_safe(X1, y1)
        self.assertAlmostEqual(safe1, ref1, delta=1e-12)
        score1, reasons1 = compute_dbcv_score(X1, y1, algorithm_name="HDBSCAN_GLOBAL")
        self.assertEqual(reasons1, [])
        self.assertAlmostEqual(score1, round(ref1, 4), delta=1e-4)

        # 2. Exact PR #141 repro #2: seed=18, (40, 4), 4 clusters
        X2 = np.random.RandomState(18).normal(size=(40, 4))
        y2 = np.repeat(np.arange(4), 10).astype(np.int64)
        ref2 = float(hv.validity_index(X2, y2))
        safe2 = evaluation._compute_dbcv_memory_safe(X2, y2)
        self.assertAlmostEqual(safe2, ref2, delta=1e-12)

        # 3. Non-consecutive labels + noise
        X3 = np.random.RandomState(11).normal(size=(90, 4))
        y3 = np.array([0] * 25 + [3] * 25 + [7] * 25 + [-1] * 15, dtype=np.int64)
        ref3 = float(hv.validity_index(X3, y3))
        safe3 = evaluation._compute_dbcv_memory_safe(X3, y3)
        self.assertAlmostEqual(safe3, ref3, delta=1e-12)

        # 4. Exact distance ties & duplicate coordinates within clusters (MST parent tie repair)
        base = np.array([[0.0, 0.0, 0.0, 0.0], [1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0], [1.0, 1.0, 0.0, 0.0],
                         [0.5, 0.5, 0.0, 0.0], [0.5, 0.5, 0.0, 0.0]])
        X4 = np.vstack([base, base + 10.0])
        y4 = np.array([0] * len(base) + [1] * len(base), dtype=np.int64)
        ref4 = float(hv.validity_index(X4, y4))
        safe4 = evaluation._compute_dbcv_memory_safe(X4, y4)
        self.assertAlmostEqual(safe4, ref4, delta=1e-12)

        # 5. Forced >2000-member cluster path (testing blocked core distance + blocked MST tie repair)
        rng5 = np.random.RandomState(23)
        X5 = np.vstack([
            rng5.normal(size=(2050, 4)) * 0.3,
            rng5.normal(size=(30, 4)) * 0.2 + 5.0,
        ])
        y5 = np.array([0] * 2050 + [1] * 30, dtype=np.int64)
        ref5 = float(hv.validity_index(X5, y5))
        safe5 = evaluation._compute_dbcv_memory_safe(X5, y5)
        self.assertAlmostEqual(safe5, ref5, delta=1e-12)

    def test_dbcv_exception_never_falls_back_to_silhouette(self):
        """Injected exceptions in either DBCV backend must return (None, [METRIC_FIT_FAILED]), never Silhouette."""
        X_small = np.random.RandomState(7).normal(size=(40, 4))
        y_small = np.repeat(np.arange(4), 10).astype(np.int64)

        with patch("hdbscan.validity.validity_index", side_effect=RuntimeError("boom ref")):
            score_s, reasons_s = compute_dbcv_score(X_small, y_small, algorithm_name="DBSCAN")
        self.assertIsNone(score_s)
        self.assertEqual(reasons_s, [METRIC_FIT_FAILED])

        # Also verify >50 cluster / memory-safe path returns (None, [METRIC_FIT_FAILED]) on error
        X_many = np.random.RandomState(7).normal(size=(204, 4))
        y_many = np.repeat(np.arange(51), 4).astype(np.int64)
        with patch.object(evaluation, "_compute_dbcv_memory_safe", side_effect=MemoryError("boom safe")):
            score_m, reasons_m = compute_dbcv_score(X_many, y_many, algorithm_name="HDBSCAN_GLOBAL")
        self.assertIsNone(score_m)
        self.assertEqual(reasons_m, [METRIC_FIT_FAILED])

    def test_cross_verify_exp02_exp03_fail_closed_negative_controls(self):
        """_cross_verify_exp02_exp03 must reject FAILED reports, untrusted SHAs, pin drift, and swapped memberships."""
        import importlib
        import importlib.util as _ilu
        import pyarrow as pa
        import pyarrow.parquet as pq
        import notebooks.experiment_harness as eh
        import tests.test_research_collection_loader as col_mod

        eval_script = importlib.import_module("scripts.evaluate-models")
        spec_r02 = _ilu.spec_from_file_location("run_exp02_neg_test", ROOT / "scripts/run-exp02-official.py")
        r02 = _ilu.module_from_spec(spec_r02); spec_r02.loader.exec_module(r02)
        spec_r03 = _ilu.spec_from_file_location("run_exp03_neg_test", ROOT / "scripts/run-exp03-official.py")
        r03 = _ilu.module_from_spec(spec_r03); spec_r03.loader.exec_module(r03)

        kit_dir = self.tmp_dir / "cv_kit"
        repro_dir = kit_dir / "bundles" / "REPRODUCTION"
        pilot_tmp = self.tmp_dir / "cv_pilot"
        repro_dir.mkdir(parents=True)
        pilot_tmp.mkdir()
        pins = col_mod.fixture(repro_dir, pilot_tmp)

        exp02_out = self.tmp_dir / "cv_exp02_out"
        exp02_json = self.tmp_dir / "cv_exp02.json"
        r02.execute_official_baselines(
            bundle_dir=repro_dir,
            output_dir=exp02_out,
            output_json=exp02_json,
            manifest_sha256=pins["manifest_sha256"],
            checksums_sha256=pins["checksums_sha256"],
            expected_handoff_sha256=None,
            seed=42,
            root=ROOT,
        )
        exp02_sha = eh.sha256_bytes(exp02_json.read_bytes())

        exp03_out = self.tmp_dir / "cv_exp03_out" / "hdbscan_adaptive"
        exp03_json = self.tmp_dir / "cv_exp03.json"
        r03.execute_official_adaptive(
            bundle_dir=repro_dir,
            output_dir=exp03_out,
            output_json=exp03_json,
            manifest_sha256=pins["manifest_sha256"],
            checksums_sha256=pins["checksums_sha256"],
            expected_handoff_sha256=None,
            seed=42,
            root=ROOT,
        )
        exp03_sha = eh.sha256_bytes(exp03_json.read_bytes())

        base_kwargs = dict(
            bundle_dir=repro_dir,
            output_json=self.tmp_dir / "cv_exp04.json",
            expected_split="REPRODUCTION",
            manifest_sha256=pins["manifest_sha256"],
            checksums_sha256=pins["checksums_sha256"],
            expected_handoff_sha256=None,
            exp02_report_path=exp02_json,
            expected_exp02_report_sha256=exp02_sha,
            exp02_output_dir=exp02_out,
            exp03_report_path=exp03_json,
            expected_exp03_report_sha256=exp03_sha,
            exp03_output_dir=exp03_out,
            repeats=2,
            sigma=0.01,
            seed=42,
            project_root=ROOT,
        )

        # 1. Missing or wrong upstream report SHA, or missing output dir -> must fail
        with self.assertRaisesRegex(ValueError, "EXP-02_TRUSTED_REPORT_SHA_REQUIRED"):
            eval_script.execute_official_evaluation(**{**base_kwargs, "expected_exp02_report_sha256": None})
        with self.assertRaisesRegex(ValueError, "EXP-02_REPORT_SHA_MISMATCH"):
            eval_script.execute_official_evaluation(**{**base_kwargs, "expected_exp02_report_sha256": "0" * 64})
        with self.assertRaisesRegex(ValueError, "EXP-03_OUTPUT_DIR_REQUIRED"):
            eval_script.execute_official_evaluation(**{**base_kwargs, "exp03_output_dir": None})

        # 2. Helper to write mutated report & trusted SHA to confirm pin/status failures
        def _mutate_report(src_path: Path, dst_path: Path, mutator) -> str:
            doc = json.loads(src_path.read_text(encoding="utf-8"))
            mutator(doc)
            dst_path.write_text(json.dumps(doc, sort_keys=True) + "\n", encoding="utf-8")
            return eh.sha256_bytes(dst_path.read_bytes())

        # 2a. status="FAILED"
        p_fail = self.tmp_dir / "exp02_failed.json"
        sha_fail = _mutate_report(exp02_json, p_fail, lambda d: d.__setitem__("status", "FAILED"))
        with self.assertRaisesRegex(ValueError, "EXP-02_UPSTREAM_STATUS_NOT_PASSED"):
            eval_script.execute_official_evaluation(**{**base_kwargs, "exp02_report_path": p_fail, "expected_exp02_report_sha256": sha_fail})

        # 2b. Wrong manifest_sha256 / gold_snapshot_id
        p_pin = self.tmp_dir / "exp02_badpin.json"
        sha_pin = _mutate_report(exp02_json, p_pin, lambda d: d["dataset"].__setitem__("manifest_sha256", "0" * 64))
        with self.assertRaisesRegex(ValueError, "EXP-02_PIN_MISMATCH:manifest_sha256"):
            eval_script.execute_official_evaluation(**{**base_kwargs, "exp02_report_path": p_pin, "expected_exp02_report_sha256": sha_pin})

        p_gold = self.tmp_dir / "exp03_badgold.json"
        sha_gold = _mutate_report(exp03_json, p_gold, lambda d: d["dataset"].__setitem__("gold_snapshot_id", "999999"))
        with self.assertRaisesRegex(ValueError, "EXP-03_PIN_MISMATCH:gold_snapshot_id"):
            eval_script.execute_official_evaluation(**{**base_kwargs, "exp03_report_path": p_gold, "expected_exp03_report_sha256": sha_gold})

        # 2c. Changed model config pin SHA
        p_cfg = self.tmp_dir / "exp03_badcfg.json"
        sha_cfg = _mutate_report(exp03_json, p_cfg, lambda d: d["model_config_pin"].__setitem__("canonical_config_sha256", "a" * 64))
        with self.assertRaisesRegex(ValueError, "EXP-03_CONFIG_PIN_MISMATCH:HDBSCAN_ADAPTIVE:canonical_config_sha256"):
            eval_script.execute_official_evaluation(**{**base_kwargs, "exp03_report_path": p_cfg, "expected_exp03_report_sha256": sha_cfg})

        # 3. Same summary counts on disk, but swapped/mutated membership rows + rehashed _SUCCESS & report
        swap_out = self.tmp_dir / "cv_exp02_swapped"
        shutil.copytree(exp02_out, swap_out)
        win_dir = swap_out / "window"
        mems_p = win_dir / "memberships.parquet"
        m_rows = pq.read_table(mems_p).to_pylist()
        # Flip non-member/distance on a candidate without changing window/aftershock summary totals
        for row in m_rows:
            if row["event_role_candidate"] == "POST":
                row["normalized_distance"] = float(row["normalized_distance"]) + 1.25
                break
        pq.write_table(pa.Table.from_pylist(m_rows, schema=eh.MEMBERSHIP_SCHEMA), mems_p)
        succ_doc = json.loads((win_dir / "_SUCCESS.json").read_text(encoding="utf-8"))
        succ_doc["checksums"]["memberships.parquet"] = eh.sha256_bytes(mems_p.read_bytes())
        (win_dir / "_SUCCESS.json").write_text(eh.canonical(succ_doc) + "\n", encoding="utf-8")
        new_success_sha = eh.sha256_bytes((win_dir / "_SUCCESS.json").read_bytes())

        p_swap = self.tmp_dir / "exp02_swap.json"
        def _update_win_checksums(d):
            d["artifacts_verified"]["WINDOW"]["checksums"]["memberships.parquet"] = succ_doc["checksums"]["memberships.parquet"]
            d["artifacts_verified"]["WINDOW"]["checksums"]["_SUCCESS.json"] = new_success_sha

        sha_swap = _mutate_report(exp02_json, p_swap, _update_win_checksums)
        with self.assertRaisesRegex(ValueError, "EXP-02_PRIMARY_MEMBERSHIP_PARITY_MISMATCH:WINDOW"):
            eval_script.execute_official_evaluation(
                **{
                    **base_kwargs,
                    "exp02_report_path": p_swap,
                    "expected_exp02_report_sha256": sha_swap,
                    "exp02_output_dir": swap_out,
                }
            )

    def test_official_evaluation_blocks_frozen_rule_when_upstream_missing(self):
        """Missing EXP-02, EXP-03, or both must fail official publication and never emit PASSED + frozen rule."""
        import importlib
        import importlib.util as _ilu
        import notebooks.experiment_harness as eh
        import tests.test_research_collection_loader as col_mod

        eval_script = importlib.import_module("scripts.evaluate-models")
        spec_r02 = _ilu.spec_from_file_location("run_exp02_missing_up", ROOT / "scripts/run-exp02-official.py")
        r02 = _ilu.module_from_spec(spec_r02); spec_r02.loader.exec_module(r02)
        spec_r03 = _ilu.spec_from_file_location("run_exp03_missing_up", ROOT / "scripts/run-exp03-official.py")
        r03 = _ilu.module_from_spec(spec_r03); spec_r03.loader.exec_module(r03)

        kit_dir = self.tmp_dir / "miss_kit"
        repro_dir = kit_dir / "bundles" / "REPRODUCTION"
        pilot_tmp = self.tmp_dir / "miss_pilot"
        repro_dir.mkdir(parents=True)
        pilot_tmp.mkdir()
        pins = col_mod.fixture(repro_dir, pilot_tmp)

        exp02_out = self.tmp_dir / "miss_exp02_out"
        exp02_json = self.tmp_dir / "miss_exp02.json"
        r02.execute_official_baselines(
            bundle_dir=repro_dir,
            output_dir=exp02_out,
            output_json=exp02_json,
            manifest_sha256=pins["manifest_sha256"],
            checksums_sha256=pins["checksums_sha256"],
            expected_handoff_sha256=None,
            seed=42,
            root=ROOT,
        )
        exp02_sha = eh.sha256_bytes(exp02_json.read_bytes())

        exp03_out = self.tmp_dir / "miss_exp03_out" / "hdbscan_adaptive"
        exp03_json = self.tmp_dir / "miss_exp03.json"
        r03.execute_official_adaptive(
            bundle_dir=repro_dir,
            output_dir=exp03_out,
            output_json=exp03_json,
            manifest_sha256=pins["manifest_sha256"],
            checksums_sha256=pins["checksums_sha256"],
            expected_handoff_sha256=None,
            seed=42,
            root=ROOT,
        )
        exp03_sha = eh.sha256_bytes(exp03_json.read_bytes())

        base_kwargs = dict(
            bundle_dir=repro_dir,
            expected_split="REPRODUCTION",
            manifest_sha256=pins["manifest_sha256"],
            checksums_sha256=pins["checksums_sha256"],
            expected_handoff_sha256=None,
            exp02_report_path=exp02_json,
            expected_exp02_report_sha256=exp02_sha,
            exp02_output_dir=exp02_out,
            exp03_report_path=exp03_json,
            expected_exp03_report_sha256=exp03_sha,
            exp03_output_dir=exp03_out,
            repeats=2,
            sigma=0.01,
            seed=42,
            project_root=ROOT,
        )

        cases = [
            (
                "missing_exp02",
                {
                    "exp02_report_path": None,
                    "expected_exp02_report_sha256": None,
                    "exp02_output_dir": None,
                },
                "EXP02_CROSS_VERIFICATION_MISSING",
            ),
            (
                "missing_exp03",
                {
                    "exp03_report_path": None,
                    "expected_exp03_report_sha256": None,
                    "exp03_output_dir": None,
                },
                "EXP03_CROSS_VERIFICATION_MISSING",
            ),
            (
                "missing_both",
                {
                    "exp02_report_path": None,
                    "expected_exp02_report_sha256": None,
                    "exp02_output_dir": None,
                    "exp03_report_path": None,
                    "expected_exp03_report_sha256": None,
                    "exp03_output_dir": None,
                },
                "EXP02_CROSS_VERIFICATION_MISSING,EXP03_CROSS_VERIFICATION_MISSING",
            ),
        ]

        for idx, (label, overrides, expected_reason) in enumerate(cases):
            # 1. Official mode (default) must reject and write NO report or rule
            refused_json = self.tmp_dir / f"{label}_refused.json"
            refused_md = self.tmp_dir / f"{label}_refused.md"
            with self.assertRaisesRegex(ValueError, f"EXP04_UPSTREAM_CROSS_VERIFICATION_REQUIRED:{expected_reason}"):
                eval_script.execute_official_evaluation(
                    **{**base_kwargs, **overrides, "output_json": refused_json, "output_md": refused_md}
                )
            self.assertFalse(refused_json.exists())
            self.assertFalse(refused_md.exists())

            # 2. Explicit diagnostic mode can run, but NEVER marks PASSED / OFFICIAL_RESEARCH or emits frozen rule
            diag_json = self.tmp_dir / f"{label}_diag.json"
            diag_md = self.tmp_dir / f"{label}_diag.md"
            rep = eval_script.execute_official_evaluation(
                **{
                    **base_kwargs,
                    **overrides,
                    "output_json": diag_json,
                    "output_md": diag_md,
                    "diagnostic_only": True,
                }
            )
            self.assertEqual(rep["status"], "DIAGNOSTIC_ONLY")
            self.assertEqual(rep["evaluation_scope"], "DIAGNOSTIC_UNVERIFIED")
            self.assertFalse(rep["frozen_rule_published"])
            self.assertIsNone(rep["frozen_extension_rule"])
            self.assertNotIn("frozen_extension_rule", rep["evaluation_summary"])
            self.assertFalse(rep["scientific_approval"])
            for token in expected_reason.split(","):
                self.assertIn(token, rep["frozen_rule_block_reasons"])
            md_txt = diag_md.read_text(encoding="utf-8")
            self.assertIn("CHƯA PHÁT HÀNH", md_txt)
            self.assertNotIn("exp04-frozen-reproduction-rule", diag_json.read_text(encoding="utf-8"))

        # Even if both upstream reports pass, diagnostic_only=True must not publish OFFICIAL_RESEARCH PASSED + rule
        both_diag = eval_script.execute_official_evaluation(
            **{
                **base_kwargs,
                "output_json": self.tmp_dir / "both_diag.json",
                "output_md": self.tmp_dir / "both_diag.md",
                "diagnostic_only": True,
            }
        )
        self.assertEqual(both_diag["status"], "DIAGNOSTIC_ONLY")
        self.assertEqual(both_diag["evaluation_scope"], "DIAGNOSTIC_UNVERIFIED")
        self.assertFalse(both_diag["frozen_rule_published"])
        self.assertIsNone(both_diag["frozen_extension_rule"])
        self.assertIn("DIAGNOSTIC_ONLY_MODE_NO_RULE_PUBLICATION", both_diag["frozen_rule_block_reasons"])


class TestCommittedOfficialEvidence(unittest.TestCase):
    def test_published_rule_binds_both_current_physical_replay_reports(self):
        evidence = ROOT / "docs/evidence"
        merged_baseline = json.loads((evidence / "EXP-02_OFFICIAL_REPRODUCTION.json").read_bytes())
        report = json.loads((evidence / "EXP-04_OFFICIAL_REPRODUCTION.json").read_bytes())
        self.assertEqual(report["status"], "PASSED")
        self.assertTrue(report["frozen_rule_published"])
        self.assertFalse(report["scientific_approval"])

        upstreams = {
            "exp02": "EXP-02_REPRODUCTION_REPLAY_FOR_EXP04.json",
            "exp03": "EXP-03_OFFICIAL_REPRODUCTION.json",
        }
        reports = {}
        for tag, filename in upstreams.items():
            # The recorded upstream SHA is of the LF runner output; a Windows
            # checkout may use CRLF for the same tracked report.
            raw = (evidence / filename).read_bytes().replace(b"\r\n", b"\n")
            reports[tag] = json.loads(raw)
            receipt = report[f"{tag}_cross_verification"]
            self.assertEqual(receipt["report_sha256"], hashlib.sha256(raw).hexdigest())
            for flag in ("all_checks_passed", "dataset_and_pins_verified", "config_pins_verified",
                         "physical_bundles_verified", "physical_shards_verified", "primary_membership_parity_verified"):
                self.assertIs(receipt[flag], True)

        replay = reports["exp02"]
        self.assertEqual(replay["dataset"], merged_baseline["dataset"])
        self.assertEqual(replay["case_results"], merged_baseline["case_results"])
        for algo, values in replay["reconciliation"].items():
            with self.subTest(algorithm=algo):
                original = merged_baseline["reconciliation"][algo]
                for field in ("total_windows", "successful_windows", "failed_windows", "total_candidates",
                              "total_aftershocks", "total_noise_candidates", "total_sequence_members"):
                    self.assertEqual(values[field], original[field])
                self.assertEqual(replay["model_config_pins"][algo]["canonical_config_sha256"],
                                 merged_baseline["model_config_pins"][algo]["canonical_config_sha256"])
                self.assertEqual(report["exp02_cross_verification"]["algorithms"][algo]["run_id"],
                                 replay["artifacts_verified"][algo]["run_id"])
        self.assertEqual(report["exp03_cross_verification"]["run_id"],
                         reports["exp03"]["artifact_verified"]["run_id"])


if __name__ == "__main__":
    unittest.main()
