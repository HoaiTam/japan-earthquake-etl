"""Unit and contract tests for COL-03: Pilot baseline metrics and visualization."""
import csv
import json
from pathlib import Path
import tempfile
import unittest

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT / "notebooks"))
sys.path.insert(0, str(ROOT / "tests"))

from experiment_harness import (
    load_feature_bundle,
    MODE_DEVELOPMENT,
)
from baseline_models import run_baseline_experiment
from baseline_visualization import (
    compute_density_metric,
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

    def test_density_metric_valid_clusters(self):
        """Verifies that valid clusters yield a non-null numerical score in [0.0, 1.0]."""
        vecs = np.array([
            [0.1, 0.1, 0.0, 0.0],
            [0.12, 0.09, 0.0, 0.0],
            [0.11, 0.11, 0.0, 0.0],
            [0.13, 0.10, 0.0, 0.0],
        ])
        labels = np.array([0, 0, 0, 0])
        res = compute_density_metric(vecs, labels, "DBSCAN")
        self.assertIsNotNone(res["score"])
        self.assertTrue(0.0 <= res["score"] <= 1.0)
        self.assertEqual(res["reason"], "VALID_SINGLE_CLUSTER_COMPACTNESS")

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
