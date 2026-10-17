"""EXP-02: Unit and contract tests for Window, DBSCAN, and HDBSCAN Global baseline models.

Tests offline without network, MinIO, Docker or live cloud APIs.
Covers:
- Per-mainshock window fitting.
- Mainshock cluster isolation and noise rule (EXP_MAINSHOCK_CLASSIFIED_AS_NOISE).
- Null probability policy (NULL for WINDOW/DBSCAN; finite [0, 1] for HDBSCAN).
- Multiple clusters in window and empty POST sequences.
- Input immutability.
- Multi-baseline execution and local smoke report generation.
"""
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]

spec_harness = importlib.util.spec_from_file_location("experiment_harness", ROOT / "notebooks/experiment_harness.py")
harness = importlib.util.module_from_spec(spec_harness)
spec_harness.loader.exec_module(harness)

spec_models = importlib.util.spec_from_file_location("baseline_models", ROOT / "notebooks/baseline_models.py")
models = importlib.util.module_from_spec(spec_models)
spec_models.loader.exec_module(models)

spec_test_harness = importlib.util.spec_from_file_location("test_experiment_harness", ROOT / "tests/test_experiment_harness.py")
test_harness = importlib.util.module_from_spec(spec_test_harness)
spec_test_harness.loader.exec_module(test_harness)

MID = "evt_" + "1" * 32
PRE_EVT = "evt_" + "0" * 32
POST_EVT_1 = "evt_" + "2" * 32
POST_EVT_2 = "evt_" + "3" * 32
FAR_NOISE = "evt_" + "9" * 32


def create_window_candidates() -> list[dict]:
    """Create a controlled 4-D window candidate list around mainshock MID."""
    return [
        # Self-event (MAINSHOCK) at origin
        {
            "candidate_event_id": MID,
            "delta_time_hours": 0.0,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.5,
        },
        # PRE event (candidate foreshock)
        {
            "candidate_event_id": PRE_EVT,
            "delta_time_hours": -5.0,
            "x_scaled": 0.05,
            "y_scaled": 0.02,
            "z_scaled": 0.01,
            "time_scaled": -5.0 / 720.0,
            "candidate_magnitude": 3.0,
        },
        # POST event 1 (close aftershock)
        {
            "candidate_event_id": POST_EVT_1,
            "delta_time_hours": 2.0,
            "x_scaled": 0.04,
            "y_scaled": 0.03,
            "z_scaled": 0.01,
            "time_scaled": 2.0 / 720.0,
            "candidate_magnitude": 4.2,
        },
        # POST event 2 (close aftershock)
        {
            "candidate_event_id": POST_EVT_2,
            "delta_time_hours": 10.0,
            "x_scaled": 0.06,
            "y_scaled": 0.05,
            "z_scaled": -0.02,
            "time_scaled": 10.0 / 720.0,
            "candidate_magnitude": 3.8,
        },
        # Far noise event
        {
            "candidate_event_id": FAR_NOISE,
            "delta_time_hours": 300.0,
            "x_scaled": 5.0,
            "y_scaled": 5.0,
            "z_scaled": 3.0,
            "time_scaled": 300.0 / 720.0,
            "candidate_magnitude": 2.5,
        },
    ]


class BaselineModelsTest(unittest.TestCase):
    def setUp(self):
        self.candidates = create_window_candidates()
        self.temp_dir = tempfile.TemporaryDirectory()
        self.out_dir = Path(self.temp_dir.name)

    def tearDown(self):
        self.temp_dir.cleanup()

    # -------------------------------------------------------------------------
    # 1. Window Baseline Tests
    # -------------------------------------------------------------------------
    def test_window_baseline_identifies_post_and_preserves_nulls(self):
        mem_rows, sum_row = models.fit_window_baseline(self.candidates, MID)
        self.assertEqual(len(mem_rows), 5)
        self.assertEqual(sum_row["candidate_count"], 5)
        self.assertEqual(sum_row["window_result_status"], "SUCCESS")
        self.assertTrue(sum_row["productive_sequence_detected"])
        self.assertEqual(sum_row["aftershock_count"], 3)
        self.assertEqual(sum_row["foreshock_candidate_count"], 0)
        self.assertEqual(sum_row["sequence_member_count"], 4)  # Mainshock + 3 POSTs (including far noise)

        # In WINDOW baseline: cluster_id, noise, and probability must be strictly None
        for r in mem_rows:
            self.assertIsNone(r["cluster_id"])
            self.assertIsNone(r["is_noise"])
            self.assertIsNone(r["is_mainshock_cluster_member"])
            self.assertIsNone(r["membership_probability"])

        self.assertIsNone(sum_row["cluster_count"])
        self.assertIsNone(sum_row["noise_count"])
        self.assertIsNone(sum_row["noise_ratio"])
        self.assertIsNone(sum_row["mean_membership_probability"])

    # -------------------------------------------------------------------------
    # 2. DBSCAN Baseline Tests
    # -------------------------------------------------------------------------
    def test_dbscan_baseline_cluster_and_noise_separation(self):
        # eps=0.2 easily clusters the dense group around origin and excludes FAR_NOISE
        config = {"eps": 0.2, "min_samples": 2, "metric": "euclidean"}
        mem_rows, sum_row = models.fit_dbscan_baseline(self.candidates, MID, config=config)

        self.assertEqual(sum_row["window_result_status"], "SUCCESS")
        self.assertEqual(sum_row["candidate_count"], 5)
        self.assertGreater(sum_row["cluster_count"], 0)
        self.assertEqual(sum_row["noise_count"], 1)  # FAR_NOISE is noise
        self.assertTrue(sum_row["productive_sequence_detected"])

        # Check mainshock
        main_row = next(r for r in mem_rows if r["candidate_event_id"] == MID)
        self.assertTrue(main_row["is_mainshock_cluster_member"])
        self.assertTrue(main_row["is_sequence_member"])
        self.assertFalse(main_row["is_noise"])
        self.assertGreaterEqual(main_row["cluster_id"], 0)
        self.assertIsNone(main_row["membership_probability"])

        # Check noise event
        noise_row = next(r for r in mem_rows if r["candidate_event_id"] == FAR_NOISE)
        self.assertTrue(noise_row["is_noise"])
        self.assertEqual(noise_row["cluster_id"], -1)
        self.assertFalse(noise_row["is_sequence_member"])
        self.assertFalse(noise_row["is_mainshock_cluster_member"])
        self.assertEqual(noise_row["multi_sequence_status"], "NOT_MEMBER")

    def test_dbscan_mainshock_classified_as_noise(self):
        # Set min_samples high or eps very small so mainshock has no neighbors and becomes noise (-1)
        config = {"eps": 0.001, "min_samples": 10, "metric": "euclidean"}
        mem_rows, sum_row = models.fit_dbscan_baseline(self.candidates, MID, config=config)

        # Mainshock is noise
        main_row = next(r for r in mem_rows if r["candidate_event_id"] == MID)
        self.assertTrue(main_row["is_noise"])
        self.assertEqual(main_row["cluster_id"], -1)

        # CONTRACT RULE: if mainshock is noise, NO candidate can be sequence member
        self.assertEqual(sum_row["window_result_status"], "FAILED")
        self.assertEqual(sum_row["failure_reason_code"], "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")
        self.assertIsNone(sum_row["productive_sequence_detected"])
        self.assertIsNone(sum_row["sequence_member_count"])
        self.assertIsNone(sum_row["aftershock_count"])

        for r in mem_rows:
            self.assertFalse(r["is_sequence_member"])
            self.assertFalse(r["is_mainshock_cluster_member"])
            self.assertEqual(r["multi_sequence_status"], "NOT_MEMBER")

    # -------------------------------------------------------------------------
    # 3. HDBSCAN Global Baseline Tests
    # -------------------------------------------------------------------------
    def test_hdbscan_global_baseline_probabilities_and_mainshock_cluster(self):
        config = {"min_cluster_size": 2, "min_samples": 2, "cluster_selection_epsilon": 0.1, "allow_single_cluster": True}
        mem_rows, sum_row = models.fit_hdbscan_global_baseline(self.candidates, MID, config=config)

        self.assertEqual(sum_row["window_result_status"], "SUCCESS")
        self.assertEqual(sum_row["candidate_count"], 5)
        self.assertIsNotNone(sum_row["mean_membership_probability"])
        self.assertGreaterEqual(sum_row["mean_membership_probability"], 0.0)
        self.assertLessEqual(sum_row["mean_membership_probability"], 1.0)

        for r in mem_rows:
            self.assertIsNotNone(r["membership_probability"])
            self.assertGreaterEqual(r["membership_probability"], 0.0)
            self.assertLessEqual(r["membership_probability"], 1.0)

        main_row = next(r for r in mem_rows if r["candidate_event_id"] == MID)
        self.assertTrue(main_row["is_mainshock_cluster_member"])
        self.assertTrue(main_row["is_sequence_member"])

    def test_hdbscan_multiple_clusters_isolates_only_mainshock_cluster(self):
        # Create a second distinct cluster far away from mainshock
        candidates_with_second_cluster = list(self.candidates)
        candidates_with_second_cluster.extend([
            {"candidate_event_id": "evt_c2_1", "delta_time_hours": 100.0, "x_scaled": 10.0, "y_scaled": 10.0, "z_scaled": 1.0, "time_scaled": 0.1},
            {"candidate_event_id": "evt_c2_2", "delta_time_hours": 101.0, "x_scaled": 10.05, "y_scaled": 10.02, "z_scaled": 1.0, "time_scaled": 0.101},
            {"candidate_event_id": "evt_c2_3", "delta_time_hours": 102.0, "x_scaled": 10.02, "y_scaled": 10.04, "z_scaled": 1.0, "time_scaled": 0.102},
        ])
        config = {"min_cluster_size": 2, "min_samples": 2, "cluster_selection_epsilon": 0.1, "allow_single_cluster": True}
        mem_rows, sum_row = models.fit_hdbscan_global_baseline(candidates_with_second_cluster, MID, config=config)

        main_label = next(r["cluster_id"] for r in mem_rows if r["candidate_event_id"] == MID)
        self.assertGreaterEqual(main_label, 0)

        # Verify candidates in second cluster are NOT sequence members
        c2_members = [r for r in mem_rows if r["candidate_event_id"].startswith("evt_c2")]
        for r in c2_members:
            self.assertNotEqual(r["cluster_id"], main_label)
            self.assertFalse(r["is_mainshock_cluster_member"])
            self.assertFalse(r["is_sequence_member"])
            self.assertEqual(r["multi_sequence_status"], "NOT_MEMBER")

    # -------------------------------------------------------------------------
    # 4. Empty POST Cases
    # -------------------------------------------------------------------------
    def test_empty_post_candidates(self):
        # Window with only MAINSHOCK and PRE event (no POST aftershocks)
        pre_only_candidates = [
            {
                "candidate_event_id": MID,
                "delta_time_hours": 0.0,
                "x_scaled": 0.0,
                "y_scaled": 0.0,
                "z_scaled": 0.0,
                "time_scaled": 0.0,
                "candidate_magnitude": 6.5,
            },
            {
                "candidate_event_id": PRE_EVT,
                "delta_time_hours": -2.0,
                "x_scaled": 0.01,
                "y_scaled": 0.01,
                "z_scaled": 0.005,
                "time_scaled": -2.0 / 720.0,
                "candidate_magnitude": 3.0,
            },
        ]
        # WINDOW baseline
        mem_win, sum_win = models.fit_window_baseline(pre_only_candidates, MID)
        self.assertEqual(sum_win["window_result_status"], "SUCCESS")
        self.assertEqual(sum_win["aftershock_count"], 0)
        self.assertEqual(sum_win["foreshock_candidate_count"], 0)
        self.assertEqual(sum_win["sequence_member_count"], 1)  # Only mainshock
        self.assertFalse(sum_win["productive_sequence_detected"])

        # DBSCAN baseline
        cfg_db = {"eps": 0.5, "min_samples": 2}
        mem_db, sum_db = models.fit_dbscan_baseline(pre_only_candidates, MID, config=cfg_db)
        self.assertEqual(sum_db["window_result_status"], "SUCCESS")
        self.assertEqual(sum_db["aftershock_count"], 0)
        self.assertEqual(sum_db["foreshock_candidate_count"], 1)  # PRE is in mainshock cluster
        self.assertEqual(sum_db["sequence_member_count"], 2)  # Mainshock + foreshock
        self.assertFalse(sum_db["productive_sequence_detected"])

        # HDBSCAN baseline
        cfg_hdb = {"min_cluster_size": 2, "min_samples": 2, "cluster_selection_epsilon": 0.1, "allow_single_cluster": True}
        mem_hdb, sum_hdb = models.fit_hdbscan_global_baseline(pre_only_candidates, MID, config=cfg_hdb)
        self.assertEqual(sum_hdb["window_result_status"], "SUCCESS")
        self.assertEqual(sum_hdb["aftershock_count"], 0)
        self.assertFalse(sum_hdb["productive_sequence_detected"])

    # -------------------------------------------------------------------------
    # 5. Input Immutability Test
    # -------------------------------------------------------------------------
    def test_input_candidates_are_not_mutated(self):
        original_repr = [dict(c) for c in self.candidates]
        models.fit_window_baseline(self.candidates, MID)
        models.fit_dbscan_baseline(self.candidates, MID)
        models.fit_hdbscan_global_baseline(self.candidates, MID)

        self.assertEqual(self.candidates, original_repr)

    # -------------------------------------------------------------------------
    # 6. Multi-Baseline Experiment & Local Smoke Report
    # -------------------------------------------------------------------------
    def test_run_all_baselines_and_generate_local_smoke_report(self):
        # Load synthetic pilot bundle
        temp_bundle_dir = self.out_dir / "pilot_bundle"
        temp_bundle_dir.mkdir()
        pid = test_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = harness.load_feature_bundle(temp_bundle_dir, mode=harness.MODE_DEVELOPMENT)

        results = {}
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
            cfg_path = ROOT / f"config/ml/models/{algo.lower() if algo != 'HDBSCAN_GLOBAL' else 'hdbscan_global'}_dev_v1.json"
            if not cfg_path.is_file():
                cfg_path = ROOT / f"config/ml/models/{algo.lower() if algo != 'HDBSCAN_GLOBAL' else 'hdbscan_global'}_baseline_v1.json"
            cfg = json.loads(cfg_path.read_text(encoding="utf-8")) if cfg_path.is_file() else {}

            res = models.run_baseline_experiment(
                bundle=bundle,
                algorithm_name=algo,
                config=cfg,
                model_config_version=f"{algo.lower()}_v1",
                output_dir=self.out_dir / algo.lower(),
                mode=harness.MODE_DEVELOPMENT,
            )
            results[algo] = res
            self.assertFalse(res["write_result"]["has_success_marker"])
            self.assertTrue((self.out_dir / algo.lower() / "pilot_result_manifest.json").exists())
            self.assertFalse((self.out_dir / algo.lower() / "_SUCCESS.json").exists())

        # Generate local smoke report
        smoke_report_file = self.out_dir / "local-smoke-report.json"
        report = models.generate_local_smoke_report(
            bundle=bundle,
            algorithm_results=results,
            output_path=smoke_report_file,
            code_version="test-exp02",
        )
        self.assertEqual(report["local_smoke_status"], "PASSED")
        self.assertTrue(report["colab_migration_allowed"])
        self.assertEqual(report["pilot_id"], pid)
        self.assertEqual(sorted(report["algorithms_evaluated"]), ["DBSCAN", "HDBSCAN_GLOBAL", "WINDOW"])

        # Colab adapter verifies local smoke gate successfully
        adapter = harness.ColabPathAdapter()
        verified = adapter.verify_local_smoke_gate(smoke_report_file)
        self.assertEqual(verified["local_smoke_status"], "PASSED")


if __name__ == "__main__":
    unittest.main()
