"""Unit and contract tests for COL-02: Bounded baseline parameter sweep."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
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


def _hash_directory_tree(dir_path: Path) -> dict:
    hashes = {}
    for p in sorted(dir_path.rglob("*")):
        if p.is_file():
            rel = p.relative_to(dir_path).as_posix()
            hashes[rel] = hashlib.sha256(p.read_bytes()).hexdigest()
    return hashes


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
            allow_overwrite=False,
        )
        matrix = sweep["matrix"]
        self.assertEqual(matrix["sweep_version"], "col-02-sweep-v1")
        self.assertEqual(len(matrix["configurations"]), 9)  # 8 sweep configs + 1 control

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
            allow_overwrite=False,
        )
        res_b = run_bounded_parameter_sweep(
            bundle=self.bundle,
            project_root=ROOT,
            output_dir=out_b,
            config_keys=["DBSCAN_C1"],
            allow_overwrite=False,
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

    def test_sweep_default_fail_closed_on_existing_output_and_preserves_hashes(self):
        """Verifies that rerun into existing output without allow_overwrite fails-closed and preserves all hashes."""
        out_dir = self.work_dir / "sweep_preserve_test"
        sweep = run_bounded_parameter_sweep(
            bundle=self.bundle,
            project_root=ROOT,
            output_dir=out_dir,
            allow_overwrite=False,
        )
        self.assertEqual(len(sweep["matrix"]["configurations"]), 9)
        before_hashes = _hash_directory_tree(out_dir)
        self.assertGreater(len(before_hashes), 10)

        # Calling again with default allow_overwrite=False must fail closed
        with self.assertRaisesRegex(ValueError, "EXP_COMPLETED_BUNDLE_CONFLICT"):
            run_bounded_parameter_sweep(
                bundle=self.bundle,
                project_root=ROOT,
                output_dir=out_dir,
                config_keys=["DBSCAN_C1"],
                allow_overwrite=False,
            )

        # Verify 100% of the directory tree and hashes are untouched
        after_hashes = _hash_directory_tree(out_dir)
        self.assertEqual(before_hashes, after_hashes, "Existing files/hashes must be strictly preserved")

        # Verify matrix file still contains all 9 configs and was not overwritten
        matrix_data = json.loads((out_dir / "sweep_results_matrix.json").read_text(encoding="utf-8"))
        self.assertEqual(matrix_data["total_configurations"], 9)

    def test_cli_rejects_missing_or_invalid_pilot_bundle(self):
        """Verifies that the CLI runner rejects explicit nonexistent pilot paths or invalid arguments."""
        cli_script = ROOT / "scripts/inspect-col02-sweep.py"

        # 1. Nonexistent explicit path
        nonexistent = self.work_dir / "nonexistent_pilot_dir"
        res1 = subprocess.run(
            [sys.executable, str(cli_script), "--pilot-bundle", str(nonexistent)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res1.returncode, 2)
        self.assertIn("does not exist", res1.stderr)

        # 2. File instead of directory
        dummy_file = self.work_dir / "dummy_file.txt"
        dummy_file.write_text("hello", encoding="utf-8")
        res2 = subprocess.run(
            [sys.executable, str(cli_script), "--pilot-bundle", str(dummy_file)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res2.returncode, 2)
        self.assertIn("not a directory", res2.stderr)

        # 3. Missing both --pilot-bundle and --synthetic-fixture
        res3 = subprocess.run(
            [sys.executable, str(cli_script)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res3.returncode, 2)
        self.assertIn("Missing input dataset", res3.stderr)

        # 4. Conflicting arguments
        res4 = subprocess.run(
            [sys.executable, str(cli_script), "--pilot-bundle", str(self.bundle_dir), "--synthetic-fixture"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res4.returncode, 2)

    def test_cli_synthetic_fixture_and_real_pilot_modes(self):
        """Verifies CLI execution in explicit synthetic fixture mode vs real pilot mode."""
        cli_script = ROOT / "scripts/inspect-col02-sweep.py"

        # Synthetic fixture mode
        synth_out = self.work_dir / "synth_cli_out"
        res_synth = subprocess.run(
            [sys.executable, str(cli_script), "--synthetic-fixture", "--output-dir", str(synth_out)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res_synth.returncode, 0, res_synth.stderr)
        synth_report = json.loads((synth_out / "col02-sweep-report.json").read_text(encoding="utf-8"))
        self.assertEqual(synth_report["input_kind"], "SYNTHETIC_FIXTURE")
        self.assertTrue(synth_report["is_synthetic_fixture"])

        # Real pilot mode
        real_out = self.work_dir / "real_cli_out"
        res_real = subprocess.run(
            [sys.executable, str(cli_script), "--pilot-bundle", str(self.bundle_dir), "--output-dir", str(real_out)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res_real.returncode, 0, res_real.stderr)
        real_report = json.loads((real_out / "col02-sweep-report.json").read_text(encoding="utf-8"))
        self.assertEqual(real_report["input_kind"], "REAL_PILOT")
        self.assertFalse(real_report["is_synthetic_fixture"])

    def test_cli_default_rejects_overwrite_and_preserves_hashes(self):
        """Verifies CLI fails closed when target output directory already exists, unless --allow-overwrite is given."""
        cli_script = ROOT / "scripts/inspect-col02-sweep.py"
        target_out = self.work_dir / "cli_overwrite_test"

        # First run succeeds
        res1 = subprocess.run(
            [sys.executable, str(cli_script), "--synthetic-fixture", "--output-dir", str(target_out)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res1.returncode, 0, res1.stderr)
        hashes_before = _hash_directory_tree(target_out)

        # Second run without --allow-overwrite fails-closed
        res2 = subprocess.run(
            [sys.executable, str(cli_script), "--synthetic-fixture", "--output-dir", str(target_out)],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(res2.returncode, 0)
        self.assertIn("EXP_COMPLETED_BUNDLE_CONFLICT", res2.stderr)

        # Hashes strictly preserved
        hashes_after = _hash_directory_tree(target_out)
        self.assertEqual(hashes_before, hashes_after)

        # Third run with --allow-overwrite succeeds
        res3 = subprocess.run(
            [sys.executable, str(cli_script), "--synthetic-fixture", "--output-dir", str(target_out), "--allow-overwrite"],
            capture_output=True,
            text=True,
        )
        self.assertEqual(res3.returncode, 0, res3.stderr)


if __name__ == "__main__":
    unittest.main()
