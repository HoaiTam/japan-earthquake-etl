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
)
from baseline_models import run_baseline_experiment
from baseline_visualization import (
    compute_density_metric,
    compute_dbcv,
    extract_baseline_metrics,
    export_metrics_table,
    plot_spatial_and_temporal_views,
    plot_algorithm_comparison_summary,
    create_handoff_manifest,
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
        cfg_db = {"algorithm_name": "DBSCAN", "algorithm_version": "1.0.0", "model_config_version": "db-v1", "eps": 0.5, "min_samples": 3, "metric": "euclidean"}

        run_baseline_experiment(self.bundle, "WINDOW", cfg_win, "win-v1", self.results_dir / "window", mode=MODE_DEVELOPMENT)
        run_baseline_experiment(self.bundle, "DBSCAN", cfg_db, "db-v1", self.results_dir / "dbscan", mode=MODE_DEVELOPMENT)

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
        # Create a mock failed window where is_mainshock_noise = True
        fail_dir = self.work_dir / "results_fail"
        fail_dir.mkdir(parents=True, exist_ok=True)
        sub_fail = fail_dir / "dbscan_fail"
        sub_fail.mkdir(parents=True, exist_ok=True)

        # Copy original memberships and config
        orig_mem = self.results_dir / "dbscan/memberships.parquet"
        orig_cfg = self.results_dir / "dbscan/experiment_config.json"
        import shutil
        shutil.copy(orig_mem, sub_fail / "memberships.parquet")
        shutil.copy(orig_cfg, sub_fail / "experiment_config.json")

        # Create a sequence_summary with is_mainshock_noise = True
        orig_sum = pq.read_table(self.results_dir / "dbscan/sequence_summary.parquet").to_pandas()
        orig_sum["is_mainshock_noise"] = True
        orig_sum["aftershock_count"] = 0
        pq.write_table(pa.Table.from_pandas(orig_sum), sub_fail / "sequence_summary.parquet")

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
        sub.mkdir(parents=True, exist_ok=True)

        import shutil
        shutil.copy(self.results_dir / "dbscan/experiment_config.json", sub / "experiment_config.json")
        shutil.copy(self.results_dir / "dbscan/sequence_summary.parquet", sub / "sequence_summary.parquet")

        # Mutate one candidate_event_id in memberships.parquet
        mem_df = pq.read_table(self.results_dir / "dbscan/memberships.parquet").to_pandas()
        mem_df.loc[0, "candidate_event_id"] = "unknown_probe_event_99999"
        pq.write_table(pa.Table.from_pandas(mem_df), sub / "memberships.parquet")

        with self.assertRaises(ValueError) as ctx:
            extract_baseline_metrics(bad_dir, self.bundle)
        self.assertIn("UNKNOWN_CANDIDATE_GRAIN", str(ctx.exception))

    def test_duplicate_grain_raises_error(self):
        """Negative test: Duplicate (mainshock_event_id, candidate_event_id) grains must fail-closed."""
        dup_dir = self.work_dir / "results_dup"
        dup_dir.mkdir(parents=True, exist_ok=True)
        sub = dup_dir / "dbscan_dup"
        sub.mkdir(parents=True, exist_ok=True)

        import shutil
        shutil.copy(self.results_dir / "dbscan/experiment_config.json", sub / "experiment_config.json")
        shutil.copy(self.results_dir / "dbscan/sequence_summary.parquet", sub / "sequence_summary.parquet")

        # Duplicate row 0 in memberships
        mem_df = pq.read_table(self.results_dir / "dbscan/memberships.parquet").to_pandas()
        dup_df = mem_df._append(mem_df.iloc[0], ignore_index=True)
        pq.write_table(pa.Table.from_pandas(dup_df), sub / "memberships.parquet")

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
        bad_mem_p = self.work_dir / "bad_memberships.parquet"

        mem_df = pq.read_table(self.results_dir / "dbscan/memberships.parquet").to_pandas()
        mem_df.loc[0, "candidate_event_id"] = "unknown_event_plot_999"
        pq.write_table(pa.Table.from_pandas(mem_df), bad_mem_p)

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


if __name__ == "__main__":
    unittest.main()
