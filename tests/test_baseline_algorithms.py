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
            fresh_kernel_acceptance=True,
        )
        self.assertEqual(report["local_smoke_status"], "PASSED")
        self.assertTrue(report["colab_migration_allowed"])
        self.assertEqual(report["pilot_id"], pid)
        self.assertEqual(sorted(report["algorithms_evaluated"]), ["DBSCAN", "HDBSCAN_GLOBAL", "WINDOW"])

        # Colab adapter verifies local smoke gate successfully
        adapter = harness.ColabPathAdapter()
        verified = adapter.verify_local_smoke_gate(smoke_report_file)
        self.assertEqual(verified["local_smoke_status"], "PASSED")
        self.assertIn("case_results", verified)

    # -------------------------------------------------------------------------
    # 7. Multi-Sequence Overlap and Run ID Synchronization Tests (Review P1)
    # -------------------------------------------------------------------------
    def test_multi_sequence_overlap_marked_ambiguous_and_rank_none(self):
        mid1 = "evt_m1_" + "1" * 25
        mid2 = "evt_m2_" + "2" * 25
        shared_cand = "evt_shared_" + "9" * 21
        exclusive_cand = "evt_excl_" + "8" * 23

        # Construct two windows with one shared candidate
        rows = [
            # Window 1: mainshock 1 + shared candidate (POST)
            {
                "mainshock_event_id": mid1,
                "candidate_event_id": mid1,
                "delta_time_hours": 0.0,
                "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0,
                "candidate_magnitude": 6.0,
            },
            {
                "mainshock_event_id": mid1,
                "candidate_event_id": shared_cand,
                "delta_time_hours": 2.0,
                "x_scaled": 0.02, "y_scaled": 0.02, "z_scaled": 0.01, "time_scaled": 0.01,
                "candidate_magnitude": 4.0,
            },
            # Window 2: mainshock 2 + shared candidate (POST) + exclusive candidate (POST)
            {
                "mainshock_event_id": mid2,
                "candidate_event_id": mid2,
                "delta_time_hours": 0.0,
                "x_scaled": 0.0, "y_scaled": 0.0, "z_scaled": 0.0, "time_scaled": 0.0,
                "candidate_magnitude": 6.5,
            },
            {
                "mainshock_event_id": mid2,
                "candidate_event_id": shared_cand,
                "delta_time_hours": 4.0,
                "x_scaled": 0.03, "y_scaled": 0.01, "z_scaled": 0.01, "time_scaled": 0.02,
                "candidate_magnitude": 4.0,
            },
            {
                "mainshock_event_id": mid2,
                "candidate_event_id": exclusive_cand,
                "delta_time_hours": 6.0,
                "x_scaled": 0.05, "y_scaled": 0.04, "z_scaled": 0.02, "time_scaled": 0.03,
                "candidate_magnitude": 3.8,
            },
        ]

        import pyarrow as pa
        features_table = pa.Table.from_pylist(rows)
        mock_bundle = {
            "mode": harness.MODE_DEVELOPMENT,
            "bundle_id": "pilot_overlap_test",
            "source_dataset_id": "ds_overlap_test",
            "features_table": features_table,
            "manifest": {"coverage_status": "PARTIAL", "checksums": {}},
        }

        cfg_win = {"model_config_version": "window_v1"}
        out_win = self.out_dir / "overlap_window"
        res = models.run_baseline_experiment(
            bundle=mock_bundle,
            algorithm_name="WINDOW",
            config=cfg_win,
            model_config_version="window_v1",
            output_dir=out_win,
            mode=harness.MODE_DEVELOPMENT,
        )

        all_mems = res["memberships"]
        self.assertEqual(len(all_mems), 5)

        # Shared candidate rows
        shared_rows = [m for m in all_mems if m["candidate_event_id"] == shared_cand]
        self.assertEqual(len(shared_rows), 2)
        for sr in shared_rows:
            self.assertTrue(sr["is_sequence_member"])
            self.assertEqual(sr["multi_sequence_status"], "AMBIGUOUS")
            self.assertIsNone(sr["resolution_rank"])

        # Exclusive candidate row
        excl_row = next(m for m in all_mems if m["candidate_event_id"] == exclusive_cand)
        self.assertTrue(excl_row["is_sequence_member"])
        self.assertEqual(excl_row["multi_sequence_status"], "UNIQUE")
        self.assertEqual(excl_row["resolution_rank"], 1)

        # PyArrow roundtrip adheres to MLI-01 MEMBERSHIP_SCHEMA
        table = pa.Table.from_pylist(all_mems, schema=harness.MEMBERSHIP_SCHEMA)
        self.assertEqual(table.num_rows, 5)

    def test_runner_synchronizes_run_id_across_memberships_and_manifest(self):
        temp_bundle_dir = self.out_dir / "pilot_bundle_sync"
        temp_bundle_dir.mkdir()
        test_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = harness.load_feature_bundle(temp_bundle_dir, mode=harness.MODE_DEVELOPMENT)

        cfg_win = {"model_config_version": "window_v1"}
        out_win = self.out_dir / "sync_window"
        res = models.run_baseline_experiment(
            bundle=bundle,
            algorithm_name="WINDOW",
            config=cfg_win,
            model_config_version="window_v1",
            output_dir=out_win,
            mode=harness.MODE_DEVELOPMENT,
        )

        effective_run_id = res["run_id"]
        self.assertTrue(effective_run_id.startswith("pilot_run_"))
        self.assertNotEqual(effective_run_id, "temp_run")

        # Check in-memory rows
        for m in res["memberships"]:
            self.assertEqual(m["experiment_run_id"], effective_run_id)
        for s in res["summaries"]:
            self.assertEqual(s["experiment_run_id"], effective_run_id)

        # Check on-disk manifest and config
        manifest = json.loads((out_win / "pilot_result_manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["pilot_run_id"], effective_run_id)

        config_data = json.loads((out_win / "experiment_config.json").read_text(encoding="utf-8"))
        self.assertEqual(config_data["experiment_run_id"], effective_run_id)

    # -------------------------------------------------------------------------
    # 8. Local Smoke Report Gate Regression Tests (Review P1)
    # -------------------------------------------------------------------------
    def test_local_smoke_report_rejects_empty_or_missing_algorithms(self):
        temp_bundle_dir = self.out_dir / "pilot_bundle_gate"
        temp_bundle_dir.mkdir()
        test_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = harness.load_feature_bundle(temp_bundle_dir, mode=harness.MODE_DEVELOPMENT)

        out_rep = self.out_dir / "rep_fail.json"

        # 1. Empty algorithm_results
        with self.assertRaisesRegex(ValueError, "MISSING_REQUIRED_BASELINE_ALGORITHMS"):
            models.generate_local_smoke_report(bundle, {}, out_rep)

        # 2. Incomplete baseline algorithms (missing HDBSCAN_GLOBAL)
        with self.assertRaisesRegex(ValueError, "MISSING_REQUIRED_BASELINE_ALGORITHMS"):
            models.generate_local_smoke_report(bundle, {"WINDOW": {}, "DBSCAN": {}}, out_rep)

    def test_local_smoke_report_rejects_corrupted_roles_and_count_mismatch(self):
        temp_bundle_dir = self.out_dir / "pilot_bundle_reconcile"
        temp_bundle_dir.mkdir()
        test_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = harness.load_feature_bundle(temp_bundle_dir, mode=harness.MODE_DEVELOPMENT)

        results = {}
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
            res = models.run_baseline_experiment(
                bundle=bundle,
                algorithm_name=algo,
                config={"model_config_version": f"{algo.lower()}_v1"},
                model_config_version=f"{algo.lower()}_v1",
                output_dir=self.out_dir / f"reconcile_{algo.lower()}",
                mode=harness.MODE_DEVELOPMENT,
            )
            results[algo] = res

        out_rep = self.out_dir / "rep_reconcile.json"

        # Count mismatch
        corrupted_results = dict(results)
        corrupted_win = dict(results["WINDOW"])
        corrupted_win["metrics"] = dict(results["WINDOW"]["metrics"], total_candidates=999)
        corrupted_results["WINDOW"] = corrupted_win

        with self.assertRaisesRegex(ValueError, "COUNT_MISMATCH"):
            models.generate_local_smoke_report(bundle, corrupted_results, out_rep)

        # Corrupted role
        corrupted_mems = [dict(m) for m in results["WINDOW"]["memberships"]]
        corrupted_mems[0]["event_role_candidate"] = "INVALID_ROLE"
        corrupted_win_role = dict(results["WINDOW"], memberships=corrupted_mems)
        corrupted_results_role = dict(results, WINDOW=corrupted_win_role)

        with self.assertRaisesRegex(ValueError, "INVALID_EVENT_ROLE"):
            models.generate_local_smoke_report(bundle, corrupted_results_role, out_rep)

    def test_colab_adapter_rejects_incomplete_smoke_report(self):
        adapter = harness.ColabPathAdapter()

        # Missing report
        with self.assertRaisesRegex(ValueError, "COLAB_GATE_BLOCKED: Missing local smoke report"):
            adapter.verify_local_smoke_gate(self.out_dir / "nonexistent.json")

        # Report missing required algorithms
        bad_report_path = self.out_dir / "bad_report.json"
        bad_report_path.write_text(json.dumps({
            "report_version": "exp-02-local-smoke-v2",
            "local_smoke_status": "PASSED",
            "colab_migration_allowed": True,
            "fresh_kernel_acceptance": True,
            "bundle_checksum": "a" * 64,
            "algorithms_evaluated": ["WINDOW"],
            "reconciliation": {"WINDOW": {"total_candidates": 2}},
        }), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "COLAB_GATE_BLOCKED: Report missing required algorithms"):
            adapter.verify_local_smoke_gate(bad_report_path)

        # Report with empty reconciliation
        bad_reconcile_path = self.out_dir / "bad_reconcile.json"
        bad_reconcile_path.write_text(json.dumps({
            "report_version": "exp-02-local-smoke-v2",
            "local_smoke_status": "PASSED",
            "colab_migration_allowed": True,
            "fresh_kernel_acceptance": True,
            "bundle_checksum": "a" * 64,
            "algorithms_evaluated": ["DBSCAN", "HDBSCAN_GLOBAL", "WINDOW"],
            "reconciliation": {},
        }), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "COLAB_GATE_BLOCKED: Missing or incomplete reconciliation"):
            adapter.verify_local_smoke_gate(bad_reconcile_path)

    def test_local_smoke_report_rejects_missing_manifest_checksum_entry(self):
        temp_bundle_dir = self.out_dir / "pilot_bundle_missing_cs"
        temp_bundle_dir.mkdir()
        test_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = harness.load_feature_bundle(temp_bundle_dir, mode=harness.MODE_DEVELOPMENT)

        results = {}
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
            results[algo] = models.run_baseline_experiment(
                bundle=bundle,
                algorithm_name=algo,
                config={"model_config_version": f"{algo.lower()}_v1"},
                model_config_version=f"{algo.lower()}_v1",
                output_dir=self.out_dir / f"missing_cs_{algo.lower()}",
                mode=harness.MODE_DEVELOPMENT,
            )

        # Corrupt WINDOW manifest by deleting experiment_metrics.json checksum entry
        win_manifest_path = Path(results["WINDOW"]["write_result"]["manifest_path"])
        manifest_data = json.loads(win_manifest_path.read_text(encoding="utf-8"))
        del manifest_data["checksums"]["experiment_metrics.json"]
        win_manifest_path.write_text(json.dumps(manifest_data), encoding="utf-8")

        out_rep = self.out_dir / "rep_missing_cs.json"
        with self.assertRaisesRegex(ValueError, "MISSING_MANIFEST_CHECKSUM_ENTRY:WINDOW:experiment_metrics.json"):
            models.generate_local_smoke_report(bundle, results, out_rep)

    def test_local_smoke_report_rejects_corrupted_physical_json_and_parquet(self):
        temp_bundle_dir = self.out_dir / "pilot_bundle_phys_corrupt"
        temp_bundle_dir.mkdir()
        test_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = harness.load_feature_bundle(temp_bundle_dir, mode=harness.MODE_DEVELOPMENT)

        results = {}
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
            results[algo] = models.run_baseline_experiment(
                bundle=bundle,
                algorithm_name=algo,
                config={"model_config_version": f"{algo.lower()}_v1"},
                model_config_version=f"{algo.lower()}_v1",
                output_dir=self.out_dir / f"phys_corrupt_{algo.lower()}",
                mode=harness.MODE_DEVELOPMENT,
            )

        # 1. Non-JSON string written to experiment_metrics.json
        metrics_file = self.out_dir / "phys_corrupt_window" / "experiment_metrics.json"
        metrics_file.write_text("THIS IS NOT A VALID JSON STRING", encoding="utf-8")
        # Update manifest to match new checksum so it passes checksum check but fails physical json parse
        win_manifest_path = Path(results["WINDOW"]["write_result"]["manifest_path"])
        manifest_data = json.loads(win_manifest_path.read_text(encoding="utf-8"))
        manifest_data["checksums"]["experiment_metrics.json"] = harness.sha256_bytes(metrics_file.read_bytes())
        win_manifest_path.write_text(json.dumps(manifest_data), encoding="utf-8")

        out_rep = self.out_dir / "rep_corrupt_json.json"
        with self.assertRaisesRegex(ValueError, "CORRUPT_PHYSICAL_METRICS_JSON:WINDOW"):
            models.generate_local_smoke_report(bundle, results, out_rep)

        # Restore valid metrics json so test 2 isolates parquet corruption
        valid_metrics_content = harness.canonical({
            "schema_version": "1.0",
            "experiment_run_id": results["WINDOW"]["run_id"],
            "dataset_id": bundle["source_dataset_id"],
            "algorithm_name": "WINDOW",
            "membership_row_count": 2,
            "summary_row_count": 1,
            "metrics": results["WINDOW"]["metrics"],
        })
        metrics_file.write_text(valid_metrics_content, encoding="utf-8")
        manifest_data["checksums"]["experiment_metrics.json"] = harness.sha256_bytes(metrics_file.read_bytes())

        # 2. Corrupted parquet file
        mems_file = self.out_dir / "phys_corrupt_window" / "memberships.parquet"
        mems_file.write_bytes(b"INVALID_PARQUET_HEADER_BYTES")
        manifest_data["checksums"]["memberships.parquet"] = harness.sha256_bytes(mems_file.read_bytes())
        win_manifest_path.write_text(json.dumps(manifest_data), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "CORRUPT_PHYSICAL_MEMBERSHIPS_PARQUET:WINDOW"):
            models.generate_local_smoke_report(bundle, results, out_rep)

    def test_local_smoke_report_extracts_non_null_bundle_checksum_and_fingerprint(self):
        temp_bundle_dir = self.out_dir / "pilot_bundle_cs_extract"
        temp_bundle_dir.mkdir()
        test_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = harness.load_feature_bundle(temp_bundle_dir, mode=harness.MODE_DEVELOPMENT)

        results = {}
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
            results[algo] = models.run_baseline_experiment(
                bundle=bundle,
                algorithm_name=algo,
                config={"model_config_version": f"{algo.lower()}_v1"},
                model_config_version=f"{algo.lower()}_v1",
                output_dir=self.out_dir / f"cs_extract_{algo.lower()}",
                mode=harness.MODE_DEVELOPMENT,
            )

        out_rep = self.out_dir / "rep_cs_extract.json"
        rep = models.generate_local_smoke_report(bundle, results, out_rep, fresh_kernel_acceptance=True)

        self.assertIsNotNone(rep["bundle_checksum"])
        self.assertEqual(len(rep["bundle_checksum"]), 64)
        self.assertEqual(rep["local_smoke_status"], "PASSED")
        self.assertTrue(rep["colab_migration_allowed"])
        self.assertTrue(rep["fresh_kernel_acceptance"])
        self.assertIn("bundle_fingerprint", rep)
        self.assertEqual(rep["bundle_fingerprint"]["features_sha256"], rep["bundle_checksum"])

        adapter = harness.ColabPathAdapter()
        verified = adapter.verify_local_smoke_gate(out_rep)
        self.assertEqual(verified["bundle_checksum"], rep["bundle_checksum"])

    def test_fallback_mode_smoke_report_blocks_colab_gate(self):
        temp_bundle_dir = self.out_dir / "pilot_bundle_fallback_gate"
        temp_bundle_dir.mkdir()
        test_harness.create_pilot_fixture(temp_bundle_dir)
        bundle = harness.load_feature_bundle(temp_bundle_dir, mode=harness.MODE_DEVELOPMENT)

        results = {}
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
            results[algo] = models.run_baseline_experiment(
                bundle=bundle,
                algorithm_name=algo,
                config={"model_config_version": f"{algo.lower()}_v1"},
                model_config_version=f"{algo.lower()}_v1",
                output_dir=self.out_dir / f"fallback_{algo.lower()}",
                mode=harness.MODE_DEVELOPMENT,
            )

        out_rep = self.out_dir / "rep_fallback.json"
        rep = models.generate_local_smoke_report(bundle, results, out_rep, fresh_kernel_acceptance=False)

        self.assertEqual(rep["local_smoke_status"], "FALLBACK_EXEC_WARNING")
        self.assertFalse(rep["colab_migration_allowed"])
        self.assertFalse(rep["fresh_kernel_acceptance"])

        adapter = harness.ColabPathAdapter()
        with self.assertRaisesRegex(ValueError, "COLAB_GATE_BLOCKED: Local smoke status is FALLBACK_EXEC_WARNING"):
            adapter.verify_local_smoke_gate(out_rep)

    def test_colab_adapter_rejects_null_or_invalid_bundle_checksum(self):
        adapter = harness.ColabPathAdapter()
        bad_report_path = self.out_dir / "bad_checksum_report.json"
        bad_report_path.write_text(json.dumps({
            "report_version": "exp-02-local-smoke-v2",
            "local_smoke_status": "PASSED",
            "colab_migration_allowed": True,
            "fresh_kernel_acceptance": True,
            "bundle_checksum": None,
            "algorithms_evaluated": ["DBSCAN", "HDBSCAN_GLOBAL", "WINDOW"],
            "reconciliation": {
                "WINDOW": {"total_candidates": 2},
                "DBSCAN": {"total_candidates": 2},
                "HDBSCAN_GLOBAL": {"total_candidates": 2},
            },
        }), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "COLAB_GATE_BLOCKED: bundle_checksum is missing, null, or invalid"):
            adapter.verify_local_smoke_gate(bad_report_path)


if __name__ == "__main__":
    unittest.main()
