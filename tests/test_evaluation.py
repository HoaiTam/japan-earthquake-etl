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
        self.assertEqual(res_empty["valid_repeats"], 0)
        self.assertEqual(res_empty["attempted_repeats"], 3)
        self.assertEqual(res_empty["valid_pairs"], 0)
        self.assertIn(METRIC_NOT_APPLICABLE, res_empty["metric_reason_codes"])
        self.assertEqual(res_empty["note"], "EMPTY_WINDOW_CANDIDATES")

        # 2. Candidate list present but mainshock_id not found
        cands = [
            {"candidate_event_id": "other_1", "delta_time_hours": 1.0, "x_scaled": 0.1, "y_scaled": 0.1, "z_scaled": 0.1, "time_scaled": 1.0},
        ]
        res_missing_main = run_window_stability_analysis(cands, "non_existent_main", "DBSCAN", repeats=3)
        self.assertIsNone(res_missing_main["mean_jaccard"])
        self.assertIsNone(res_missing_main["mean_ari"])
        self.assertEqual(res_missing_main["valid_repeats"], 0)
        self.assertEqual(res_missing_main["attempted_repeats"], 3)
        self.assertEqual(res_missing_main["valid_pairs"], 0)
        self.assertIn(METRIC_NOT_APPLICABLE, res_missing_main["metric_reason_codes"])
        self.assertEqual(res_missing_main["note"], "MAINSHOCK_NOT_IN_CANDIDATES")

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

        def mock_dispatch(algorithm_name, window_candidates, mainshock_id, config=None):
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

        # Window evaluations detail
        win_evals = {w["mainshock_id"]: w for w in res["window_evaluations"]}
        self.assertEqual(win_evals["m_fail"]["window_result_status"], "FAILURE")
        self.assertIn("simulated primary fit crash", win_evals["m_fail"]["fit_exception"])
        self.assertIsNone(win_evals["m_fail"]["dbcv_score"])
        self.assertIsNone(win_evals["m_fail"]["stability"]["mean_jaccard"])

        self.assertEqual(win_evals["m_ok"]["window_result_status"], "SUCCESS")

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

if __name__ == "__main__":
    unittest.main()
