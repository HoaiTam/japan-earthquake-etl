"""Unit and contract tests for COL-02: Bounded baseline parameter sweep."""
import json
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
import sys
sys.path.insert(0, str(ROOT / "notebooks"))
sys.path.insert(0, str(ROOT / "tests"))

from experiment_harness import (
    load_feature_bundle,
    MODE_DEVELOPMENT,
    MODE_OFFICIAL,
)
from baseline_models import VECTOR_COLUMNS
from parameter_sweep import (
    SWEEP_CONFIG_FILES,
    load_sweep_config,
    run_bounded_parameter_sweep,
    verify_sweep_rerun_invariants,
)
from test_experiment_harness import create_pilot_fixture


class TestParameterSweep(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.work_dir = Path(self.tmp.name)
        self.bundle_dir = self.work_dir / "bundle"
        self.bundle_dir.mkdir(parents=True, exist_ok=True)
        create_pilot_fixture(self.bundle_dir)
        self.bundle = load_feature_bundle(self.bundle_dir, mode=MODE_DEVELOPMENT)

    def test_sweep_configs_validity_and_bound(self):
        """Validates that all designated sweep config files exist, have correct schemas, and count <= 8."""
        param_configs = [k for k in SWEEP_CONFIG_FILES if k != "WINDOW_CONTROL"]
        self.assertEqual(len(param_configs), 8, "Must define exactly 8 parameter configurations")
        dbscan_cfgs = [k for k in param_configs if k.startswith("DBSCAN")]
        hdbscan_cfgs = [k for k in param_configs if k.startswith("HDBSCAN")]
        self.assertEqual(len(dbscan_cfgs), 4, "Must have exactly 4 DBSCAN configs")
        self.assertEqual(len(hdbscan_cfgs), 4, "Must have exactly 4 HDBSCAN configs")

        for key, rel_path in SWEEP_CONFIG_FILES.items():
            cfg = load_sweep_config(ROOT, rel_path)
            self.assertIn("algorithm_name", cfg)
            self.assertIn("model_config_version", cfg)
            if cfg["algorithm_name"] != "WINDOW":
                self.assertIn("metric", cfg)

    def test_run_bounded_sweep_and_generate_matrix(self):
        """Runs the sweep over all 8 configs + WINDOW control; verifies matrix structure and isolation."""
        out_dir = self.work_dir / "sweep_out"
        sweep = run_bounded_parameter_sweep(
            bundle=self.bundle,
            project_root=ROOT,
            output_dir=out_dir,
            allow_overwrite=True,
        )
        matrix = sweep["matrix"]
        self.assertEqual(matrix["sweep_version"], "col-02-sweep-v1")
        self.assertEqual(len(matrix["configurations"]), 9) # 8 sweep configs + 1 control

        # Ensure all configuration output directories exist and contain standard artifacts
        for row in matrix["configurations"]:
            cfg_dir = Path(row["output_directory"])
            self.assertTrue(cfg_dir.is_dir())
            self.assertTrue((cfg_dir / "memberships.parquet").is_file())
            self.assertTrue((cfg_dir / "sequence_summary.parquet").is_file())
            self.assertTrue((cfg_dir / "experiment_config.json").is_file())
            self.assertTrue((cfg_dir / "experiment_metrics.json").is_file())
            self.assertTrue((cfg_dir / "pilot_result_manifest.json").is_file())
            self.assertFalse((cfg_dir / "_SUCCESS.json").exists())

    def test_sweep_rerun_invariants_pass(self):
        """Verifies deterministic rerun invariance for a sweep configuration."""
        out_a = self.work_dir / "rerun_a"
        out_b = self.work_dir / "rerun_b"

        res_a = run_bounded_parameter_sweep(
            bundle=self.bundle,
            project_root=ROOT,
            output_dir=out_a,
            config_keys=["DBSCAN_C1"],
            allow_overwrite=True,
        )
        res_b = run_bounded_parameter_sweep(
            bundle=self.bundle,
            project_root=ROOT,
            output_dir=out_b,
            config_keys=["DBSCAN_C1"],
            allow_overwrite=True,
        )

        invariants = verify_sweep_rerun_invariants(
            res_a["results"]["DBSCAN_C1"],
            res_b["results"]["DBSCAN_C1"],
        )
        self.assertEqual(invariants["status"], "RERUN_INVARIANTS_PASSED")

    def test_sweep_rejects_exceeding_max_configs(self):
        """Verifies that exceeding the 8 parameter configs threshold is rejected."""
        dummy_keys = [f"DBSCAN_C{i}" for i in range(10)]
        with self.assertRaisesRegex(ValueError, "EXCEEDED_MAX_8_PARAMETER_CONFIGS"):
            run_bounded_parameter_sweep(
                bundle=self.bundle,
                project_root=ROOT,
                output_dir=self.work_dir / "out",
                config_keys=dummy_keys,
            )

    def test_sweep_rejects_official_bundle(self):
        """Verifies that running a sweep against official mode or official bundle is blocked."""
        official_bundle = dict(self.bundle)
        official_bundle["mode"] = MODE_OFFICIAL
        official_bundle["is_official"] = True
        with self.assertRaisesRegex(ValueError, "SWEEP_REQUIRES_DEVELOPMENT_MODE"):
            run_bounded_parameter_sweep(
                bundle=official_bundle,
                project_root=ROOT,
                output_dir=self.work_dir / "out",
            )


if __name__ == "__main__":
    unittest.main()
