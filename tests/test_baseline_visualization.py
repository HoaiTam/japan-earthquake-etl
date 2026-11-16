"""Unit, contract, and negative regression tests for COL-03: Pilot baseline metrics and visualization."""
import csv
import json
import math
from pathlib import Path
import tempfile
import unittest

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT / "notebooks"))
sys.path.insert(0, str(ROOT / "tests"))

from experiment_harness import (
    load_feature_bundle,
    MODE_DEVELOPMENT,
    VECTOR_COLUMNS,
    sha256_bytes,
)
from baseline_models import run_baseline_experiment
from baseline_visualization import (
    classify_candidate_plot_groups,
    compute_density_metric,
    compute_dbcv,
    extract_baseline_metrics,
    export_metrics_table,
    plot_spatial_and_temporal_views,
    plot_algorithm_comparison_summary,
    create_handoff_manifest,
    select_plot_result_dirs,
    verify_result_bundle,
)
from test_experiment_harness import create_pilot_fixture


class TestBaselineVisualization(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work_dir = Path(self.tmp.name)
        self.bundle_dir = self.work_dir / "bundle"
        self.bundle_dir.mkdir(parents=True, exist_ok=True)
        create_pilot_fixture(self.bundle_dir)
        self.bundle = load_feature_bundle(self.bundle_dir, mode=MODE_DEVELOPMENT)

        # Generate a sample baseline result inside temporary work_dir
        self.results_dir = self.work_dir / "results"
        cfg_win = {"algorithm_name": "WINDOW", "algorithm_version": "1.0.0", "model_config_version": "win-v1"}
        cfg_db = {"algorithm_name": "DBSCAN", "algorithm_version": "1.0.0", "model_config_version": "db-v1", "eps": 5.0, "min_samples": 1, "metric": "euclidean"}

        run_baseline_experiment(self.bundle, "WINDOW", cfg_win, "win-v1", self.results_dir / "window", mode=MODE_DEVELOPMENT)
        run_baseline_experiment(self.bundle, "DBSCAN", cfg_db, "db-v1", self.results_dir / "dbscan", mode=MODE_DEVELOPMENT)

    def _sync_manifest_checksums(self, target_dir: Path):
        """Synchronizes artifact SHA-256 checksums in pilot_result_manifest.json."""
        manifest_p = target_dir / "pilot_result_manifest.json"
        if not manifest_p.is_file():
            return
        manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
        checksums = {}
        for art in ["memberships.parquet", "sequence_summary.parquet", "experiment_config.json", "experiment_metrics.json"]:
            f = target_dir / art
            if f.is_file():
                checksums[art] = sha256_bytes(f.read_bytes())
        manifest["checksums"] = checksums
        manifest_p.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    def test_density_metric_null_policies(self):
        """Verifies null metric policy: WINDOW has null with reason; noise has null with reason."""
        dummy_vecs = np.array([[0.0, 0.0, 0.0, 0.0], [0.1, 0.2, 0.3, 0.4]])
        labels_noise = np.array([-1, -1])

        # WINDOW policy
        res_win = compute_density_metric(dummy_vecs, labels_noise, "WINDOW")
        self.assertIsNone(res_win["score"])
        self.assertEqual(res_win["reason"], "WINDOW_BASELINE_NOT_DENSITY_BASED")

        # Noise / no productive cluster policy
        res_noise = compute_density_metric(dummy_vecs, labels_noise, "DBSCAN")
        self.assertIsNone(res_noise["score"])
        self.assertEqual(res_noise["reason"], "NO_PRODUCTIVE_CLUSTERS")

    def test_density_metric_single_cluster_null_policy(self):
        """Verifies that a single cluster cannot compute DBCV and returns null with explicit reason."""
        vecs = np.array([
            [0.1, 0.1, 0.0, 0.0],
            [0.12, 0.09, 0.0, 0.0],
            [0.11, 0.11, 0.0, 0.0],
            [0.13, 0.10, 0.0, 0.0],
        ])
        labels_single = np.array([0, 0, 0, 0])
        res = compute_density_metric(vecs, labels_single, "DBSCAN")
        self.assertIsNone(res["score"])
        self.assertEqual(res["reason"], "DBCV_REQUIRES_AT_LEAST_TWO_CLUSTERS")

    def test_density_metric_valid_multi_clusters(self):
        """Verifies that valid multi-clusters yield a non-null numerical DBCV score in [-1.0, 1.0]."""
        # Create 2 well-separated clusters
        c1 = np.array([
            [0.0, 0.0, 0.0, 0.0],
            [0.05, 0.02, 0.01, 0.0],
            [0.02, 0.04, 0.02, 0.01],
            [0.03, 0.03, 0.01, 0.02],
        ])
        c2 = np.array([
            [5.0, 5.0, 5.0, 5.0],
            [5.05, 5.02, 5.01, 5.0],
            [5.02, 5.04, 5.02, 5.01],
            [5.03, 5.03, 5.01, 5.02],
        ])
        vecs = np.vstack([c1, c2])
        labels = np.array([0, 0, 0, 0, 1, 1, 1, 1])

        res = compute_density_metric(vecs, labels, "DBSCAN")
        self.assertIsNotNone(res["score"])
        self.assertTrue(-1.0 <= res["score"] <= 1.0)
        self.assertEqual(res["reason"], "VALID_DBCV_SCORE")
        self.assertIsNotNone(res["silhouette_score"])
        self.assertEqual(res["silhouette_reason"], "VALID_MULTI_CLUSTER_SCORE")

    def test_dbcv_noise_population_impact_probe(self):
        """Regression test from reviewer probe: DBCV score must penalize noise population across entire window."""
        rng = np.random.RandomState(7)
        c1 = rng.normal(loc=0.0, scale=0.1, size=(20, 4))
        c2 = rng.normal(loc=5.0, scale=0.1, size=(20, 4))
        noise = rng.uniform(low=20.0, high=30.0, size=(40, 4))

        X_clean = np.vstack([c1, c2])
        y_clean = np.array([0] * 20 + [1] * 20)

        X_noisy = np.vstack([c1, c2, noise])
        y_noisy = np.array([0] * 20 + [1] * 20 + [-1] * 40)

        # 1. Test via compute_density_metric
        res_clean = compute_density_metric(X_clean, y_clean, "DBSCAN")
        res_noisy = compute_density_metric(X_noisy, y_noisy, "DBSCAN")

        self.assertIsNotNone(res_clean["score"])
        self.assertIsNotNone(res_noisy["score"])
        self.assertAlmostEqual(res_clean["score"], 0.9748, places=3)
        self.assertAlmostEqual(res_noisy["score"], 0.4874, places=3)
        # Noise (40/80 = 50%) scales validity down by exactly half
        self.assertAlmostEqual(res_noisy["score"], res_clean["score"] * 0.5, places=3)

        # 2. Test via fallback compute_dbcv
        fb_clean, fb_clean_reason = compute_dbcv(X_clean, y_clean)
        fb_noisy, fb_noisy_reason = compute_dbcv(X_noisy, y_noisy)
        self.assertEqual(fb_clean_reason, "VALID_DBCV_SCORE")
        self.assertEqual(fb_noisy_reason, "VALID_DBCV_SCORE")
        self.assertAlmostEqual(fb_clean, 0.9748, places=3)
        self.assertAlmostEqual(fb_noisy, 0.4874, places=3)

    def test_dbcv_fallback_parity_with_reference(self):
        """Verifies parity between fallback compute_dbcv and reference implementation across diverse cluster structures."""
        try:
            import hdbscan.validity as hdb_validity
            has_hdbscan = True
        except ImportError:
            has_hdbscan = False

        test_cases = [
            # (seed, n_clusters, dim, n_noise)
            (42, 2, 4, 0),
            (101, 2, 4, 30),
            (202, 3, 3, 20),
            (303, 4, 5, 40),
        ]

        for seed, n_clusters, dim, n_noise in test_cases:
            rng = np.random.RandomState(seed)
            pts_list = []
            labels_list = []
            for c in range(n_clusters):
                c_size = rng.randint(10, 25)
                center = rng.uniform(-10, 10, size=dim)
                pts_list.append(rng.normal(loc=center, scale=0.15, size=(c_size, dim)))
                labels_list.extend([c] * c_size)
            if n_noise > 0:
                pts_list.append(rng.uniform(-30, 30, size=(n_noise, dim)))
                labels_list.extend([-1] * n_noise)

            X = np.vstack(pts_list)
            y = np.array(labels_list)

            fb_score, fb_reason = compute_dbcv(X, y)
            self.assertEqual(fb_reason, "VALID_DBCV_SCORE")
            self.assertIsNotNone(fb_score)
            self.assertTrue(-1.0 <= fb_score <= 1.0)

            if has_hdbscan:
                ref_score = float(hdb_validity.validity_index(X, y))
                self.assertAlmostEqual(fb_score, ref_score, places=3)

    def test_dbcv_insufficient_samples_per_cluster(self):
        """Verifies that a cluster with only 1 sample returns INSUFFICIENT_CLUSTER_SAMPLES."""
        vecs = np.array([
            [0.0, 0.0, 0.0, 0.0],
            [0.1, 0.1, 0.0, 0.0],
            [0.2, 0.1, 0.0, 0.0],
            [5.0, 5.0, 5.0, 5.0],  # only 1 sample for cluster 1
        ])
        labels = np.array([0, 0, 0, 1])
        score, reason = compute_dbcv(vecs, labels)
        self.assertIsNone(score)
        self.assertEqual(reason, "INSUFFICIENT_CLUSTER_SAMPLES")

    def test_extract_and_export_metrics_table(self):
        """Verifies metrics extraction across result directories and export to valid CSV and JSON."""
        rows = extract_baseline_metrics(self.results_dir, self.bundle)
        self.assertGreater(len(rows), 0)

        # Check required fields
        for r in rows:
            self.assertIn("algorithm_name", r)
            self.assertIn("mainshock_event_id", r)
            self.assertIn("total_candidates", r)
            self.assertIn("dbcv_score", r)
            self.assertIn("dbcv_reason", r)
            self.assertIn("silhouette_score", r)
            self.assertIn("silhouette_reason", r)

        out_metrics = self.work_dir / "metrics_out"
        csv_path, json_path = export_metrics_table(rows, out_metrics)
        self.assertTrue(csv_path.is_file())
        self.assertTrue(json_path.is_file())

        # Verify CSV readability
        with csv_path.open("r", encoding="utf-8") as f:
            reader = list(csv.DictReader(f))
            self.assertEqual(len(reader), len(rows))

        # Verify JSON readability
        payload = json.loads(json_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["total_rows"], len(rows))

    def test_failed_window_dbcv_is_strictly_null(self):
        """Verifies that a failed window (e.g. mainshock classified as noise or empty sequence) yields strictly null DBCV."""
        fail_dir = self.work_dir / "results_fail"
        fail_dir.mkdir(parents=True, exist_ok=True)
        sub_fail = fail_dir / "dbscan_fail"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub_fail, dirs_exist_ok=True)

        mem_df = pq.read_table(sub_fail / "memberships.parquet").to_pandas()
        ms_id = mem_df["mainshock_event_id"].iloc[0]
        mem_df["is_noise"] = True
        mem_df["cluster_id"] = -1
        mem_df["is_sequence_member"] = False
        mem_df["is_mainshock_cluster_member"] = False
        pq.write_table(pa.Table.from_pandas(mem_df), sub_fail / "memberships.parquet")

        orig_sum = pq.read_table(sub_fail / "sequence_summary.parquet").to_pandas()
        orig_sum["is_mainshock_noise"] = True
        orig_sum["window_result_status"] = "FAILED"
        orig_sum["failure_reason_code"] = "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE"
        orig_sum["aftershock_count"] = 0
        orig_sum["sequence_member_count"] = 0
        orig_sum["noise_count"] = len(mem_df)
        pq.write_table(pa.Table.from_pandas(orig_sum), sub_fail / "sequence_summary.parquet")

        self._sync_manifest_checksums(sub_fail)

        rows = extract_baseline_metrics(fail_dir, self.bundle)
        for r in rows:
            self.assertEqual(r["window_status"], "NO_PRODUCTIVE_SEQUENCE")
            self.assertIsNone(r["dbcv_score"])
            self.assertEqual(r["dbcv_reason"], "NO_PRODUCTIVE_CLUSTERS")

    def test_unknown_candidate_grain_raises_error(self):
        """Negative test: Changing candidate_event_id to unknown ID must fail-closed immediately."""
        bad_dir = self.work_dir / "results_bad_candidate"
        bad_dir.mkdir(parents=True, exist_ok=True)
        sub = bad_dir / "dbscan_bad"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        mem_df = pq.read_table(sub / "memberships.parquet").to_pandas()
        mem_df.loc[0, "candidate_event_id"] = "unknown_probe_event_99999"
        pq.write_table(pa.Table.from_pandas(mem_df), sub / "memberships.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(bad_dir, self.bundle)
        self.assertIn("UNKNOWN_CANDIDATE_GRAIN", str(ctx.exception))

    def test_duplicate_grain_raises_error(self):
        """Negative test: Duplicate (mainshock_event_id, candidate_event_id) grains must fail-closed."""
        dup_dir = self.work_dir / "results_dup"
        dup_dir.mkdir(parents=True, exist_ok=True)
        sub = dup_dir / "dbscan_dup"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        mem_df = pq.read_table(sub / "memberships.parquet").to_pandas()
        dup_df = mem_df._append(mem_df.iloc[0], ignore_index=True)
        pq.write_table(pa.Table.from_pandas(dup_df), sub / "memberships.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(dup_dir, self.bundle)
        self.assertIn("DUPLICATE_GRAIN_DETECTED", str(ctx.exception))

    def test_missing_or_non_finite_feature_raises_error(self):
        """Negative test: Non-finite feature (NaN or Inf) in feature bundle must fail-closed."""
        bad_bundle = dict(self.bundle)
        feat_df = bad_bundle["features_table"].to_pandas()
        feat_df.loc[0, "x_scaled"] = np.nan
        bad_bundle["features_table"] = pa.Table.from_pandas(feat_df)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(self.results_dir, bad_bundle)
        self.assertIn("NON_FINITE_FEATURE_DETECTED", str(ctx.exception))

    def test_plot_spatial_and_temporal_views(self):
        """Verifies spatial and timeline depth plotting produces valid non-empty image files."""
        out_plots = self.work_dir / "plots_out"
        mem_parquet = self.results_dir / "dbscan/memberships.parquet"

        spatial_p, timeline_p = plot_spatial_and_temporal_views(
            bundle=self.bundle,
            memberships_parquet=mem_parquet,
            algorithm_name="DBSCAN",
            output_dir=out_plots,
        )
        self.assertTrue(spatial_p.is_file())
        self.assertTrue(timeline_p.is_file())
        self.assertGreater(spatial_p.stat().st_size, 1000)
        self.assertGreater(timeline_p.stat().st_size, 1000)

    def test_plotting_fails_on_grain_mismatch(self):
        """Negative test: Plotting must fail-closed if memberships contain unknown grains."""
        out_plots = self.work_dir / "plots_out_bad"
        bad_dir = self.work_dir / "dbscan_plot_bad"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", bad_dir, dirs_exist_ok=True)
        bad_mem_p = bad_dir / "memberships.parquet"

        mem_df = pq.read_table(bad_mem_p).to_pandas()
        mem_df.loc[0, "candidate_event_id"] = "unknown_event_plot_999"
        pq.write_table(pa.Table.from_pandas(mem_df), bad_mem_p)
        self._sync_manifest_checksums(bad_dir)

        with self.assertRaises(ValueError) as ctx:
            plot_spatial_and_temporal_views(
                bundle=self.bundle,
                memberships_parquet=bad_mem_p,
                algorithm_name="DBSCAN",
                output_dir=out_plots,
            )
        self.assertIn("UNKNOWN_CANDIDATE_GRAIN", str(ctx.exception))

    def test_plot_algorithm_comparison_summary(self):
        """Verifies comparison summary bar chart generation."""
        out_plots = self.work_dir / "plots_out"
        rows = extract_baseline_metrics(self.results_dir, self.bundle)
        chart_p = plot_algorithm_comparison_summary(rows, out_plots)
        self.assertTrue(chart_p.is_file())
        self.assertGreater(chart_p.stat().st_size, 1000)

    def test_create_handoff_manifest_checksums(self):
        """Verifies handoff manifest generation and physical file checksum integrity."""
        out_handoff = self.work_dir / "handoff_out"
        out_handoff.mkdir(parents=True, exist_ok=True)
        file_a = out_handoff / "test_a.txt"
        file_a.write_text("sample content a", encoding="utf-8")

        manifest_p = create_handoff_manifest(
            output_dir=out_handoff,
            pilot_id="pilot_test",
            exported_files=[file_a],
            recipient="ThanhTris",
        )
        self.assertTrue(manifest_p.is_file())
        manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
        self.assertEqual(manifest["intended_recipient"], "ThanhTris")
        self.assertIn("test_a.txt", manifest["files"])

    def test_missing_result_manifest_raises_error(self):
        """Negative test: Missing pilot_result_manifest.json must fail closed."""
        target_dir = self.work_dir / "results_no_manifest"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", target_dir, dirs_exist_ok=True)
        (target_dir / "pilot_result_manifest.json").unlink()

        with self.assertRaises(ValueError) as ctx:
            verify_result_bundle(target_dir, self.bundle)
        self.assertIn("MISSING_RESULT_MANIFEST", str(ctx.exception))

    def test_foreign_pilot_id_raises_error(self):
        """Negative test: Manifest with foreign pilot_id must fail closed."""
        target_dir = self.work_dir / "results_foreign_pilot"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", target_dir, dirs_exist_ok=True)
        manifest_p = target_dir / "pilot_result_manifest.json"
        manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
        manifest["pilot_id"] = "foreign_pilot_id_999"
        manifest_p.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            verify_result_bundle(target_dir, self.bundle)
        self.assertIn("FOREIGN_PILOT_ID", str(ctx.exception))

    def test_checksum_mismatch_raises_error(self):
        """Negative test: Artifact content altered without updating checksum in manifest must fail closed."""
        target_dir = self.work_dir / "results_stale_checksum"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", target_dir, dirs_exist_ok=True)
        cfg_p = target_dir / "experiment_config.json"
        cfg = json.loads(cfg_p.read_text(encoding="utf-8"))
        cfg["tampered"] = True
        cfg_p.write_text(json.dumps(cfg), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            verify_result_bundle(target_dir, self.bundle)
        self.assertIn("CHECKSUM_MISMATCH", str(ctx.exception))

    def test_missing_required_artifact_in_manifest_raises_error(self):
        """Negative test: Manifest missing required artifact checksum must fail closed."""
        target_dir = self.work_dir / "results_missing_artifact_entry"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", target_dir, dirs_exist_ok=True)
        manifest_p = target_dir / "pilot_result_manifest.json"
        manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
        del manifest["checksums"]["sequence_summary.parquet"]
        manifest_p.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            verify_result_bundle(target_dir, self.bundle)
        self.assertIn("MISSING_REQUIRED_ARTIFACT_IN_MANIFEST", str(ctx.exception))

    def test_corrupt_manifest_raises_error(self):
        """Negative test: Corrupt manifest JSON must fail closed."""
        target_dir = self.work_dir / "results_corrupt_manifest"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", target_dir, dirs_exist_ok=True)
        manifest_p = target_dir / "pilot_result_manifest.json"
        manifest_p.write_text("NOT_VALID_JSON{{{", encoding="utf-8")

        with self.assertRaises(ValueError) as ctx:
            verify_result_bundle(target_dir, self.bundle)
        self.assertIn("CORRUPT_RESULT_MANIFEST", str(ctx.exception))

    def test_missing_or_corrupt_config_raises_error(self):
        """Negative test: Empty or invalid config must fail closed."""
        target_dir = self.work_dir / "results_empty_config"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", target_dir, dirs_exist_ok=True)
        cfg_p = target_dir / "experiment_config.json"
        cfg_p.write_text("{}", encoding="utf-8")
        self._sync_manifest_checksums(target_dir)

        with self.assertRaises(ValueError) as ctx:
            verify_result_bundle(target_dir, self.bundle)
        self.assertIn("INVALID_OR_EMPTY_CONFIG", str(ctx.exception))

    def test_config_manifest_algo_mismatch_raises_error(self):
        """Negative test: Algorithm name mismatch between config and manifest must fail closed."""
        target_dir = self.work_dir / "results_algo_mismatch"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", target_dir, dirs_exist_ok=True)
        cfg_p = target_dir / "experiment_config.json"
        cfg = json.loads(cfg_p.read_text(encoding="utf-8"))
        cfg["algorithm_name"] = "HDBSCAN_GLOBAL"
        cfg_p.write_text(json.dumps(cfg), encoding="utf-8")
        self._sync_manifest_checksums(target_dir)

        with self.assertRaises(ValueError) as ctx:
            verify_result_bundle(target_dir, self.bundle)
        self.assertIn("ALGORITHM_NAME_MISMATCH", str(ctx.exception))

    def test_empty_summary_raises_error(self):
        """Negative test: sequence_summary.parquet being empty must fail closed."""
        target_dir = self.work_dir / "results_empty_summary"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_empty_sum"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        pq.write_table(pa.Table.from_pandas(orig_sum.iloc[0:0]), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("EMPTY_SUMMARY_TABLE", str(ctx.exception))

    def test_unknown_summary_window_raises_error(self):
        """Negative test: sequence_summary containing unknown mainshock window must fail closed."""
        target_dir = self.work_dir / "results_unknown_window"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_unknown"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        orig_sum.loc[0, "mainshock_event_id"] = "unknown_ms_9999"
        pq.write_table(pa.Table.from_pandas(orig_sum), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("UNKNOWN_SUMMARY_WINDOW", str(ctx.exception))

    def test_missing_summary_window_raises_error(self):
        """Negative test: sequence_summary missing expected mainshock window must fail closed."""
        target_dir = self.work_dir / "results_missing_window"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_missing"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        # Create bundle with an extra expected mainshock
        bundle_multi = dict(self.bundle)
        feat_df = bundle_multi["features_table"].to_pandas()
        feat_df2 = feat_df.copy()
        feat_df2["mainshock_event_id"] = "ms_extra_expected"
        bundle_multi["features_table"] = pa.Table.from_pandas(feat_df._append(feat_df2, ignore_index=True))

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, bundle_multi)
        self.assertIn("MISSING_SUMMARY_WINDOW", str(ctx.exception))

    def test_duplicate_summary_window_raises_error(self):
        """Negative test: sequence_summary containing duplicate mainshock windows must fail closed."""
        target_dir = self.work_dir / "results_dup_summary"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_dup_win"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        dup_sum = orig_sum._append(orig_sum.iloc[0], ignore_index=True)
        pq.write_table(pa.Table.from_pandas(dup_sum), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("DUPLICATE_SUMMARY_WINDOW", str(ctx.exception))

    def test_candidate_count_reconciliation_mismatch_raises_error(self):
        """Negative test: summary candidate_count != actual memberships count must fail closed."""
        target_dir = self.work_dir / "results_cand_mismatch"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_cm"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        orig_sum.loc[0, "candidate_count"] = 99999
        pq.write_table(pa.Table.from_pandas(orig_sum), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("CANDIDATE_COUNT_RECONCILIATION_FAILED", str(ctx.exception))

    def test_sequence_member_count_reconciliation_mismatch_raises_error(self):
        """Negative test: summary sequence_member_count != actual count in memberships must fail closed."""
        target_dir = self.work_dir / "results_seq_mismatch"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_sm"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        orig_sum.loc[0, "sequence_member_count"] = 99999
        pq.write_table(pa.Table.from_pandas(orig_sum), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("SEQUENCE_MEMBER_COUNT_RECONCILIATION_FAILED", str(ctx.exception))

    def test_aftershock_count_reconciliation_mismatch_raises_error(self):
        """Negative test: summary aftershock_count != actual aftershocks count must fail closed."""
        target_dir = self.work_dir / "results_af_mismatch"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_af"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        orig_sum.loc[0, "aftershock_count"] = 99999
        pq.write_table(pa.Table.from_pandas(orig_sum), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("AFTERSHOCK_COUNT_RECONCILIATION_FAILED", str(ctx.exception))

    def test_noise_count_reconciliation_mismatch_raises_error(self):
        """Negative test: summary noise_count != actual noise count in memberships must fail closed."""
        target_dir = self.work_dir / "results_noise_mismatch"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_nm"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        orig_sum.loc[0, "noise_count"] = 99999
        pq.write_table(pa.Table.from_pandas(orig_sum), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("NOISE_COUNT_RECONCILIATION_FAILED", str(ctx.exception))

    def test_mainshock_noise_reconciliation_mismatch_raises_error(self):
        """Negative test: summary is_mainshock_noise != actual memberships status must fail closed."""
        target_dir = self.work_dir / "results_ms_noise_mismatch"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_mnm"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        orig_sum.loc[0, "is_mainshock_noise"] = True
        pq.write_table(pa.Table.from_pandas(orig_sum), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("MAINSHOCK_NOISE_RECONCILIATION_FAILED", str(ctx.exception))

    def test_status_mismatch_raises_error(self):
        """Negative test: summary claims FAILED but memberships has valid aftershocks must fail closed."""
        target_dir = self.work_dir / "results_status_mismatch"
        target_dir.mkdir(parents=True, exist_ok=True)
        sub = target_dir / "dbscan_stat"
        import shutil
        shutil.copytree(self.results_dir / "dbscan", sub, dirs_exist_ok=True)

        orig_sum = pq.read_table(sub / "sequence_summary.parquet").to_pandas()
        orig_sum.loc[0, "window_result_status"] = "FAILED"
        pq.write_table(pa.Table.from_pandas(orig_sum), sub / "sequence_summary.parquet")
        self._sync_manifest_checksums(sub)

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(target_dir, self.bundle)
        self.assertIn("STATUS_RECONCILIATION_FAILED", str(ctx.exception))

    def test_empty_handoff_files_raises_error(self):
        """Negative test: Creating handoff manifest with empty files list must fail closed."""
        out_handoff = self.work_dir / "handoff_empty"
        out_handoff.mkdir(parents=True, exist_ok=True)
        with self.assertRaises(ValueError) as ctx:
            create_handoff_manifest(
                output_dir=out_handoff,
                pilot_id="pilot_test",
                exported_files=[],
                recipient="ThanhTris",
            )
        self.assertIn("EMPTY_HANDOFF_FILES", str(ctx.exception))

    def test_handoff_manifest_source_provenance(self):
        """Verifies source provenance embedding into handoff_manifest.json."""
        out_handoff = self.work_dir / "handoff_prov"
        out_handoff.mkdir(parents=True, exist_ok=True)
        file_a = out_handoff / "test.csv"
        file_a.write_text("data", encoding="utf-8")

        mock_prov = {
            "window": {
                "algorithm_name": "WINDOW",
                "model_config_version": "win-v1",
                "manifest_sha256": "abc123",
            }
        }
        manifest_p = create_handoff_manifest(
            output_dir=out_handoff,
            pilot_id="pilot_test",
            exported_files=[file_a],
            recipient="ThanhTris",
            source_provenance=mock_prov,
        )
        data = json.loads(manifest_p.read_text(encoding="utf-8"))
        self.assertIn("source_provenance", data)
        self.assertEqual(data["source_provenance"], mock_prov)

    def test_notebook_workflow_end_to_end_and_source_provenance(self):
        """Regression test: Executes notebooks/col_03_baseline_metrics_plots.ipynb end-to-end and verifies handoff source_provenance pins."""
        import importlib.util
        import os

        nb_path = ROOT / "notebooks/col_03_baseline_metrics_plots.ipynb"
        self.assertTrue(nb_path.is_file())
        nb_data = json.loads(nb_path.read_text(encoding="utf-8"))

        # 1. Ensure tracked notebook is clean of outputs and execution counts
        code_cells = []
        for cell in nb_data.get("cells", []):
            if cell.get("cell_type") == "code":
                self.assertIsNone(cell.get("execution_count"), "Tracked notebook must have null execution_count")
                self.assertEqual(cell.get("outputs", []), [], "Tracked notebook must have empty outputs")
                code_cells.append("".join(cell.get("source", [])))
        self.assertGreaterEqual(len(code_cells), 4)

        def _verify_notebook_output(out_dir: Path):
            manifest_p = out_dir / "handoff_manifest.json"
            self.assertTrue(manifest_p.is_file(), f"Missing handoff_manifest.json in {out_dir}")
            manifest = json.loads(manifest_p.read_text(encoding="utf-8"))
            self.assertEqual(manifest["pilot_id"], self.bundle["bundle_id"])
            self.assertEqual(manifest["intended_recipient"], "ThanhTris")
            self.assertFalse(manifest["research_ready"])
            self.assertFalse(manifest["official_exported"])
            self.assertFalse(manifest["scientific_approval"])

            # Verify all exported files exist and match SHA-256
            self.assertGreaterEqual(manifest["total_files"], 7)
            for fname, expected_sha in manifest["files"].items():
                fpath = out_dir / fname
                self.assertTrue(fpath.is_file(), f"Missing exported file {fname}")
                self.assertEqual(sha256_bytes(fpath.read_bytes()), expected_sha)

            # Verify source_provenance pins match exact source result bundles
            self.assertIn("source_provenance", manifest)
            prov = manifest["source_provenance"]
            self.assertEqual(set(prov.keys()), {"dbscan", "window"})

            for sub_name, expected_algo, expected_ver in [
                ("dbscan", "DBSCAN", "db-v1"),
                ("window", "WINDOW", "win-v1"),
            ]:
                sub_dir = self.results_dir / sub_name
                res_manifest = json.loads((sub_dir / "pilot_result_manifest.json").read_text(encoding="utf-8"))
                p_entry = prov[sub_name]
                self.assertEqual(p_entry["pilot_id"], self.bundle["bundle_id"])
                self.assertEqual(p_entry["algorithm_name"], expected_algo)
                self.assertEqual(p_entry["model_config_version"], expected_ver)
                self.assertEqual(p_entry["pilot_run_id"], res_manifest["pilot_run_id"])
                self.assertEqual(p_entry["code_version"], res_manifest["code_version"])
                self.assertEqual(
                    p_entry["manifest_sha256"],
                    sha256_bytes((sub_dir / "pilot_result_manifest.json").read_bytes()),
                )
                self.assertEqual(
                    p_entry["config_sha256"],
                    sha256_bytes((sub_dir / "experiment_config.json").read_bytes()),
                )
                self.assertEqual(
                    p_entry["metrics_sha256"],
                    sha256_bytes((sub_dir / "experiment_metrics.json").read_bytes()),
                )
                self.assertEqual(
                    p_entry["memberships_sha256"],
                    sha256_bytes((sub_dir / "memberships.parquet").read_bytes()),
                )
                self.assertEqual(
                    p_entry["sequence_summary_sha256"],
                    sha256_bytes((sub_dir / "sequence_summary.parquet").read_bytes()),
                )

        # 2. Execute notebook cells via isolated exec namespace
        nb_exec_out = self.work_dir / "nb_exec_out"
        env_override = {
            "PILOT_BUNDLE": str(self.bundle_dir),
            "RESULTS_DIR": str(self.results_dir),
            "COL03_OUTPUT_DIR": str(nb_exec_out),
        }
        old_env = os.environ.copy()
        old_cwd = Path.cwd()
        try:
            os.environ.update(env_override)
            os.chdir(ROOT)
            exec_globals = {"__name__": "__main__", "__file__": str(nb_path)}
            for idx, code in enumerate(code_cells):
                exec(compile(code, f"<col03_cell_{idx}>", "exec"), exec_globals)
        finally:
            os.chdir(old_cwd)
            os.environ.clear()
            os.environ.update(old_env)

        _verify_notebook_output(nb_exec_out)

        # 3. Also execute via inspect-exp01-notebook runner (uses nbclient kernel when installed)
        runner_spec = importlib.util.spec_from_file_location(
            "inspect_runner", ROOT / "scripts/inspect-exp01-notebook.py"
        )
        runner_mod = importlib.util.module_from_spec(runner_spec)
        runner_spec.loader.exec_module(runner_mod)

        nb_runner_out = self.work_dir / "nb_runner_out"
        report = runner_mod.run_notebook_cells(
            nb_path,
            {
                "PILOT_BUNDLE": str(self.bundle_dir),
                "RESULTS_DIR": str(self.results_dir),
                "COL03_OUTPUT_DIR": str(nb_runner_out),
            },
        )
        self.assertTrue(report["source_notebook_outputs_cleared"])
        _verify_notebook_output(nb_runner_out)

    def test_dependency_pins_aligned_between_ci_and_colab_lock(self):
        """Verifies that pandas and matplotlib are pinned identically in CI and Colab lock files."""
        def _parse_pins(path: Path):
            pins = {}
            for raw in path.read_text(encoding="utf-8").splitlines():
                line = raw.strip()
                if not line or line.startswith("#") or line.startswith("-r "):
                    continue
                if "==" in line:
                    pkg, ver = line.split("==", 1)
                    pins[pkg.strip()] = ver.strip()
            return pins

        ci_pins = _parse_pins(ROOT / "tests/ci/requirements.txt")
        colab_pins = _parse_pins(ROOT / "notebooks/requirements-colab-lock.txt")
        model_pins = _parse_pins(ROOT / "notebooks/requirements-lock.txt")

        self.assertIn("pandas", ci_pins, "pandas must be pinned in tests/ci/requirements.txt for COL-03 CI")
        self.assertIn("matplotlib", ci_pins, "matplotlib must be pinned in tests/ci/requirements.txt")
        self.assertEqual(ci_pins["pandas"], colab_pins.get("pandas"))
        self.assertEqual(ci_pins["matplotlib"], colab_pins.get("matplotlib"))

        for shared_pkg in ("pyarrow", "numpy", "scipy", "scikit-learn"):
            self.assertEqual(ci_pins.get(shared_pkg), model_pins.get(shared_pkg))

    def test_window_plot_includes_pre_and_reconciles_all_plotted_grains(self):
        """Regression test: WINDOW fixture with MAINSHOCK + PRE (is_sequence_member=False, is_noise=None) must plot both grains."""
        from unittest.mock import patch
        import matplotlib.pyplot as plt

        win_pre_bundle = dict(self.bundle)
        feat_df = win_pre_bundle["features_table"].to_pandas().copy()
        # Convert second candidate to a valid PRE event (t = -3.5h, dx = -8.0km, dy = 4.0km)
        feat_df.loc[1, "relative_time_role"] = "PRE"
        feat_df.loc[1, "delta_time_hours"] = -3.5
        feat_df.loc[1, "time_scaled"] = -3.5 / 720.0
        feat_df.loc[1, "dx_km"] = -8.0
        feat_df.loc[1, "dy_km"] = 4.0
        feat_df.loc[1, "candidate_depth_km"] = 25.0
        win_pre_bundle["features_table"] = pa.Table.from_pandas(feat_df, schema=self.bundle["features_table"].schema)

        res_dir = self.work_dir / "win_pre_results"
        cfg_win = {"algorithm_name": "WINDOW", "algorithm_version": "1.0.0", "model_config_version": "win-pre-v1"}
        run_baseline_experiment(win_pre_bundle, "WINDOW", cfg_win, "win-pre-v1", res_dir / "window", mode=MODE_DEVELOPMENT)

        mem_p = res_dir / "window/memberships.parquet"
        mem_df = pq.read_table(mem_p).to_pandas()
        pre_mem = mem_df[mem_df["event_role_candidate"] == "PRE"]
        self.assertEqual(len(pre_mem), 1)
        self.assertFalse(bool(pre_mem["is_sequence_member"].iloc[0]))
        self.assertTrue(pre_mem["is_noise"].isna().iloc[0])

        captured_axes = []
        orig_close = plt.close

        def _spy_close(fig=None):
            if fig is not None and fig.axes:
                ax = fig.axes[0]
                col_map = {col.get_label(): col.get_offsets().copy() for col in ax.collections}
                captured_axes.append(col_map)
            orig_close(fig)

        out_plots = self.work_dir / "win_pre_plots"
        with patch("matplotlib.pyplot.close", side_effect=_spy_close):
            sp_p, tl_p = plot_spatial_and_temporal_views(win_pre_bundle, mem_p, "WINDOW", out_plots)

        self.assertTrue(sp_p.is_file())
        self.assertTrue(tl_p.is_file())
        self.assertEqual(len(captured_axes), 2)

        spatial_cols, timeline_cols = captured_axes
        self.assertIn("PRE Candidates", spatial_cols)
        self.assertIn("Mainshock (dx=0, dy=0)", spatial_cols)
        self.assertNotIn("Sequence Aftershocks", spatial_cols)
        np.testing.assert_allclose(spatial_cols["PRE Candidates"], np.array([[-8.0, 4.0]]))
        np.testing.assert_allclose(spatial_cols["Mainshock (dx=0, dy=0)"], np.array([[0.0, 0.0]]))

        self.assertIn("PRE Candidates", timeline_cols)
        self.assertIn("Mainshock (t=0)", timeline_cols)
        np.testing.assert_allclose(timeline_cols["PRE Candidates"], np.array([[-3.5, 25.0]]))
        np.testing.assert_allclose(timeline_cols["Mainshock (t=0)"], np.array([[0.0, 80.0]]))

    def test_plot_distinguishes_all_five_candidate_groups_and_reconciles_grains(self):
        """Regression test: Verifies 5-group partition (PRE, non-sequence POST, sequence members, noise, MAINSHOCK) and 1-to-1 coordinate rendering."""
        from unittest.mock import patch
        import matplotlib.pyplot as plt
        import shutil

        bundle_5 = dict(self.bundle)
        base_df = bundle_5["features_table"].to_pandas()
        ms_row = base_df.iloc[0].to_dict()

        specs = [
            # (cid, role, dx, dy, dt, depth, is_seq, is_noise, cluster_id)
            (ms_row["mainshock_event_id"], "MAINSHOCK", 0.0, 0.0, 0.0, 30.0, True, False, 0),
            ("evt_pre_cand_01", "PRE", -6.0, 3.0, -4.0, 22.0, False, False, 1),
            ("evt_seq_post_02", "POST", 7.0, -5.0, 2.5, 35.0, True, False, 0),
            ("evt_nonseq_post_03", "POST", 18.0, 12.0, 9.0, 42.0, False, False, 1),
            ("evt_noise_post_04", "POST", -25.0, -19.0, 15.0, 14.0, False, True, -1),
        ]

        feat_rows = []
        for cid, role, dx, dy, dt, depth, _, _, _ in specs:
            r = dict(ms_row)
            r["candidate_event_id"] = cid
            r["relative_time_role"] = role
            r["is_mainshock"] = (role == "MAINSHOCK")
            r["dx_km"] = dx
            r["dy_km"] = dy
            r["delta_time_hours"] = dt
            r["candidate_depth_km"] = depth
            r["x_scaled"] = dx / 50.0
            r["y_scaled"] = dy / 50.0
            r["time_scaled"] = dt / 720.0
            feat_rows.append(r)

        feat_5_df = base_df.iloc[0:0]._append(feat_rows, ignore_index=True)
        bundle_5["features_table"] = pa.Table.from_pandas(feat_5_df, schema=self.bundle["features_table"].schema)

        target_dir = self.work_dir / "dbscan_5groups"
        shutil.copytree(self.results_dir / "dbscan", target_dir, dirs_exist_ok=True)

        base_mem_df = pq.read_table(target_dir / "memberships.parquet").to_pandas()
        mem_rows = []
        for cid, role, _, _, _, _, is_seq, is_noise, cid_cluster in specs:
            m = dict(base_mem_df.iloc[0])
            m["candidate_event_id"] = cid
            m["event_role_candidate"] = role
            m["is_sequence_member"] = is_seq
            m["is_mainshock_cluster_member"] = is_seq
            m["is_noise"] = is_noise
            m["cluster_id"] = cid_cluster
            mem_rows.append(m)

        mem_5_df = base_mem_df.iloc[0:0]._append(mem_rows, ignore_index=True)
        pq.write_table(pa.Table.from_pandas(mem_5_df), target_dir / "memberships.parquet")
        self._sync_manifest_checksums(target_dir)

        merged = feat_5_df.merge(mem_5_df, on=["mainshock_event_id", "candidate_event_id"], suffixes=("", "_mem"))
        groups = classify_candidate_plot_groups(merged)
        self.assertEqual(list(groups["mainshock"]["candidate_event_id"]), [ms_row["mainshock_event_id"]])
        self.assertEqual(list(groups["pre_candidates"]["candidate_event_id"]), ["evt_pre_cand_01"])
        self.assertEqual(list(groups["sequence_members"]["candidate_event_id"]), ["evt_seq_post_02"])
        self.assertEqual(list(groups["post_non_sequence"]["candidate_event_id"]), ["evt_nonseq_post_03"])
        self.assertEqual(list(groups["noise"]["candidate_event_id"]), ["evt_noise_post_04"])

        captured_axes = []
        orig_close = plt.close

        def _spy_close(fig=None):
            if fig is not None and fig.axes:
                ax = fig.axes[0]
                col_map = {col.get_label(): col.get_offsets().copy() for col in ax.collections}
                captured_axes.append(col_map)
            orig_close(fig)

        out_plots = self.work_dir / "plots_5groups"
        with patch("matplotlib.pyplot.close", side_effect=_spy_close):
            plot_spatial_and_temporal_views(bundle_5, target_dir / "memberships.parquet", "DBSCAN", out_plots)

        self.assertEqual(len(captured_axes), 2)
        spatial_cols, timeline_cols = captured_axes

        expected_spatial = {
            "Noise Candidates": np.array([[-25.0, -19.0]]),
            "Non-Sequence POST": np.array([[18.0, 12.0]]),
            "PRE Candidates": np.array([[-6.0, 3.0]]),
            "Sequence Members": np.array([[7.0, -5.0]]),
            "Mainshock (dx=0, dy=0)": np.array([[0.0, 0.0]]),
        }
        self.assertEqual(set(spatial_cols.keys()), set(expected_spatial.keys()))
        self.assertNotIn("Sequence Aftershocks", spatial_cols)
        for label, coords in expected_spatial.items():
            np.testing.assert_allclose(spatial_cols[label], coords)

        expected_timeline = {
            "Noise Candidates": np.array([[15.0, 14.0]]),
            "Non-Sequence POST": np.array([[9.0, 42.0]]),
            "PRE Candidates": np.array([[-4.0, 22.0]]),
            "Sequence Members": np.array([[2.5, 35.0]]),
            "Mainshock (t=0)": np.array([[0.0, 30.0]]),
        }
        self.assertEqual(set(timeline_cols.keys()), set(expected_timeline.keys()))
        for label, coords in expected_timeline.items():
            np.testing.assert_allclose(timeline_cols[label], coords)

    def test_select_plot_result_dirs_represents_all_algorithms_despite_alphabetical_dbscan_dirs(self):
        """Regression test: With 4 DBSCAN configs (dbscan_c1..c4), HDBSCAN, and WINDOW, selection must include all 3 algorithms in both CLI and notebook."""
        import os
        import subprocess
        import sys

        sweep_dir = self.work_dir / "multi_algo_sweep"
        for i in range(1, 5):
            cfg_db = {
                "algorithm_name": "DBSCAN",
                "algorithm_version": "1.0.0",
                "model_config_version": f"db-c{i}-v1",
                "eps": 5.0,
                "min_samples": 1,
                "metric": "euclidean",
            }
            run_baseline_experiment(self.bundle, "DBSCAN", cfg_db, f"db-c{i}-v1", sweep_dir / f"dbscan_c{i}", mode=MODE_DEVELOPMENT)

        # HDBSCAN c1 fails (min_cluster_size=5 > 2 rows), HDBSCAN c2 succeeds (min_cluster_size=2, min_samples=1)
        cfg_hdb1 = {
            "algorithm_name": "HDBSCAN_GLOBAL",
            "algorithm_version": "1.0.0",
            "model_config_version": "hdb-c1-v1",
            "min_cluster_size": 5,
            "min_samples": 5,
            "allow_single_cluster": True,
        }
        cfg_hdb2 = {
            "algorithm_name": "HDBSCAN_GLOBAL",
            "algorithm_version": "1.0.0",
            "model_config_version": "hdb-c2-v1",
            "min_cluster_size": 2,
            "min_samples": 1,
            "allow_single_cluster": True,
        }
        run_baseline_experiment(self.bundle, "HDBSCAN_GLOBAL", cfg_hdb1, "hdb-c1-v1", sweep_dir / "hdbscan_c1", mode=MODE_DEVELOPMENT)
        run_baseline_experiment(self.bundle, "HDBSCAN_GLOBAL", cfg_hdb2, "hdb-c2-v1", sweep_dir / "hdbscan_c2", mode=MODE_DEVELOPMENT)

        cfg_win = {
            "algorithm_name": "WINDOW",
            "algorithm_version": "1.0.0",
            "model_config_version": "win-v1",
        }
        run_baseline_experiment(self.bundle, "WINDOW", cfg_win, "win-v1", sweep_dir / "window_control", mode=MODE_DEVELOPMENT)

        selected = select_plot_result_dirs(sweep_dir, bundle=self.bundle, max_dirs=4)
        selected_names = [p.name for p in selected]
        self.assertEqual(selected_names, ["window_control", "dbscan_c1", "hdbscan_c2", "dbscan_c2"])

        # Verify CLI runner produces plots for all 3 algorithms
        cli_out = self.work_dir / "cli_multi_out"
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts/inspect-col03-metrics.py"),
                "--pilot-bundle",
                str(self.bundle_dir),
                "--results-dir",
                str(sweep_dir),
                "--output-dir",
                str(cli_out),
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        cli_manifest = json.loads((cli_out / "handoff_manifest.json").read_text(encoding="utf-8"))
        expected_plot_files = (
            "spatial_distribution_window_control.png",
            "timeline_depth_window_control.png",
            "spatial_distribution_dbscan_c1.png",
            "timeline_depth_dbscan_c1.png",
            "spatial_distribution_hdbscan_c2.png",
            "timeline_depth_hdbscan_c2.png",
            "spatial_distribution_dbscan_c2.png",
            "timeline_depth_dbscan_c2.png",
        )
        for expected_file in expected_plot_files:
            self.assertIn(expected_file, cli_manifest["files"])

        # Also verify notebook execution produces the exact same selected algorithm plots
        nb_path = ROOT / "notebooks/col_03_baseline_metrics_plots.ipynb"
        nb_data = json.loads(nb_path.read_text(encoding="utf-8"))
        code_cells = ["".join(c.get("source", [])) for c in nb_data.get("cells", []) if c.get("cell_type") == "code"]
        nb_multi_out = self.work_dir / "nb_multi_out"
        old_env = os.environ.copy()
        old_cwd = Path.cwd()
        try:
            os.environ.update({
                "PILOT_BUNDLE": str(self.bundle_dir),
                "RESULTS_DIR": str(sweep_dir),
                "COL03_OUTPUT_DIR": str(nb_multi_out),
            })
            os.chdir(ROOT)
            exec_globals = {"__name__": "__main__", "__file__": str(nb_path)}
            for idx, code in enumerate(code_cells):
                exec(compile(code, f"<col03_multi_cell_{idx}>", "exec"), exec_globals)
        finally:
            os.chdir(old_cwd)
            os.environ.clear()
            os.environ.update(old_env)

        nb_manifest = json.loads((nb_multi_out / "handoff_manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(set(nb_manifest["files"].keys()), set(cli_manifest["files"].keys()))
        for expected_file in expected_plot_files:
            self.assertIn(expected_file, nb_manifest["files"])


if __name__ == "__main__":
    unittest.main()
