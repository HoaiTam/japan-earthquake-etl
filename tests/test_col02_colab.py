"""Offline negative/contract tests; these do NOT certify an actual Colab session."""
import json
import ast
import contextlib
import io
from pathlib import Path
import shutil
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "notebooks"))
import col02_runtime as c
import colab_runtime as cr
import col02_portability as cpu


class Col02PortabilityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.work = Path(self.temp.name).resolve()

    def test_source_closure_contains_every_config_and_runtime(self):
        self.assertEqual(len(c.KEYS), 9)
        self.assertEqual(len(c.RUNS), 10)
        self.assertTrue(set(c.SWEEP_CONFIG_FILES.values()) <= set(c.SOURCES))
        self.assertTrue({"notebooks/parameter_sweep.py", "notebooks/col02_runtime.py", "scripts/col02-colab.py"} <= set(c.SOURCES))
        self.assertLess(len(c.SOURCES) + len(c.result_files("reference")) + 5, 128)
        self.assertEqual(len(c.config_pins(ROOT)), 9)

    def test_source_and_config_drift_change_identity(self):
        for n in c.SOURCES:
            target = self.work / n
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / n, target)
        original = c.identity(self.work)
        path = self.work / c.SWEEP_CONFIG_FILES["DBSCAN_C1"]
        path.write_bytes(path.read_bytes() + b"\n")
        self.assertNotEqual(c.identity(self.work), original)
        self.assertNotEqual(c.config_pins(self.work), c.config_pins(ROOT))

    def test_reject_local_host_as_cloud(self):
        with patch.object(c.subprocess, "run") as run:
            run.return_value.returncode = 1
            with self.assertRaisesRegex(ValueError, "ACTUAL_COLAB_REQUIRED"):
                c.cloud_probe()
            args = run.call_args.args[0]
            self.assertEqual(args[:2], [sys._base_executable, "-I"])

    def test_existing_work_preserved(self):
        sentinel = self.work / "sentinel"
        sentinel.write_text("keep")
        with self.assertRaisesRegex(ValueError, "NEW_WORK_REQUIRED"):
            c.sweep(ROOT, self.work / "missing.zip", self.work, "HoaiTam", "LOCAL_FRESH_KERNEL")
        self.assertEqual(sentinel.read_text(), "keep")

    def test_wrong_pilot_before_fit(self):
        archive = self.work / "fake.zip"
        archive.write_bytes(b"fixture is not the pilot")
        with patch.object(cr, "environment", return_value={}), self.assertRaisesRegex(ValueError, "REAL_PILOT_REQUIRED"):
            c.sweep(ROOT, archive, self.work / "new", "HoaiTam", "LOCAL_FRESH_KERNEL")
        self.assertFalse((self.work / "new").exists())

    def test_source_symlinks_rejected(self):
        (self.work / "link").symlink_to(ROOT / "Makefile")
        with self.assertRaisesRegex(ValueError, "SYMLINK_REJECTED"):
            c.file_map(self.work)

    def test_zip_external_pin_and_traversal(self):
        archive = self.work / "bad.zip"
        with zipfile.ZipFile(archive, "x") as z: z.writestr("../escape", "bad")
        with self.assertRaisesRegex(ValueError, "EXTERNAL_ZIP_PIN"):
            cr.extract(archive, self.work / "a", "0" * 64)
        with self.assertRaisesRegex(ValueError, "UNSAFE_ZIP_MEMBER"):
            cr.extract(archive, self.work / "b", cr.sha(archive))

    def test_zip_export_readback_and_no_overwrite(self):
        folder = self.work / "folder"
        folder.mkdir()
        (folder / "result.txt").write_text("fixture")
        archive = self.work / "output.zip"
        self.assertEqual(c.archive_output(folder, archive)["status"], "OUTPUT_ZIP_READBACK_VERIFIED")
        with self.assertRaisesRegex(ValueError, "NEW_ZIP_REQUIRED"): c.archive_output(folder, archive)

    def make_report(self):
        return {"version": c.VERSION, "status": "SWEEP_AND_RERUN_PASSED", "runtime_kind": "LOCAL_FRESH_KERNEL",
                "source": c.identity(ROOT), "config_sha256": c.config_pins(ROOT), "pilot": cr.PILOT,
                "candidate_rows": 279, "mainshocks": 2, "configurations": 9, "parameter_configurations": 8,
                "input_unchanged": True, "random_seed": 42, "rerun": {"status": "RERUN_INVARIANTS_PASSED"},
                "numerical_runtime_policy": {"policy_id": cpu.POLICY_ID, "numpy_version": "2.2.2",
                    "architecture": "arm64", "x86_disable_features": list(cpu.X86_FEATURES),
                    "environment_value": "", "actual_cpu_features": {}}, **c.FLAGS}

    def test_cpu_policy_rejects_claimed_flag_when_actual_dispatch_is_active(self):
        proof = {"policy_id": cpu.POLICY_ID, "numpy_version": "2.2.2", "architecture": "x86_64",
                 "x86_disable_features": list(cpu.X86_FEATURES), "environment_value": cpu.ENV_VALUE,
                 "actual_cpu_features": {name: False for name in cpu.X86_FEATURES}}
        cpu.validate_evidence(proof)
        proof["actual_cpu_features"]["AVX2"] = True
        with self.assertRaisesRegex(ValueError, "DISPATCH_STILL_ACTIVE"): cpu.validate_evidence(proof)
        proof["actual_cpu_features"]["AVX2"] = False
        proof["numpy_version"] = "2.3.0"
        with self.assertRaisesRegex(ValueError, "RUNTIME_POLICY_CHANGED"): cpu.validate_evidence(proof)

    def test_cpu_policy_does_not_overwrite_foreign_setting(self):
        with patch.object(cpu.platform, "machine", return_value="x86_64"), patch.dict(cpu.os.environ, {"NPY_DISABLE_CPU_FEATURES": "FMA3"}):
            with self.assertRaisesRegex(ValueError, "FOREIGN_NUMPY_CPU_POLICY"): cpu.configure_process()

    def test_report_binds_pilot_source_counts_and_development_flags(self):
        report = self.make_report()
        c.validate_report(ROOT, report, "LOCAL_FRESH_KERNEL")
        for key, value in (("candidate_rows", 2), ("research_ready", True), ("scientific_approval", True),
                           ("configurations", 8), ("input_unchanged", False), ("random_seed", 7)):
            broken = dict(report, **{key: value})
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "INVALID_SWEEP_REPORT"):
                c.validate_report(ROOT, broken, "LOCAL_FRESH_KERNEL")
        with self.assertRaisesRegex(ValueError, "INVALID_SWEEP_REPORT"):
            c.validate_report(ROOT, report, "GOOGLE_COLAB")

    def test_handoff_not_accepted_from_passthrough_status(self):
        cr.save(self.work / "handoff.json", {"version": c.VERSION, "files": {}, "source": {}, "pilot": cr.PILOT})
        with self.assertRaisesRegex(ValueError, "HANDOFF_FILE_SET"):
            c.verify_handoff(self.work)

    def test_export_excludes_inputs_environment_and_private_extras(self):
        run = self.work / "run"
        expected = c.result_files("results") | {"results/sweep_results_matrix.json", "sweep-report.json",
                                              "parity-report.json", "pilot-receipt.json"}
        for name in expected:
            path = run / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("offline unit fixture")
        (run / "portable-matrix.json").write_text("portable fixture")
        for name in (".env", "kit/pilot.zip", "venv/private.txt", "private.log"):
            path = run / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("MUST NOT EXPORT")
        output = c.export_output(run, self.work / "export")
        actual = c.file_map(output)
        self.assertEqual(set(actual), expected | {"output-checksums.json"})
        self.assertEqual(len(actual), 75)
        self.assertEqual((output / "results/sweep_results_matrix.json").read_text(), "portable fixture")
        with self.assertRaisesRegex(ValueError, "NEW_OUTPUT_REQUIRED"):
            c.export_output(run, output)

    def test_readback_rejects_missing_and_changed_payload_before_parity(self):
        output = self.work / "output"
        expected = c.result_files("results") | {"results/sweep_results_matrix.json", "sweep-report.json",
                                              "parity-report.json", "pilot-receipt.json"}
        for name in expected:
            path = output / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("offline unit fixture")
        cr.save(output / "output-checksums.json", c.file_map(output))
        path = output / "results/dbscan_c1/memberships.parquet"
        path.write_text("CHANGED")
        with patch.object(c, "verify_handoff", return_value={}), patch.object(c, "validate_gate"), patch.object(cr, "environment", return_value={}):
            with self.assertRaisesRegex(ValueError, "OUTPUT_HASHES_CHANGED"):
                c.readback(self.work / "handoff", output)
            path.unlink()
            with self.assertRaisesRegex(ValueError, "OUTPUT_HASHES_CHANGED"):
                c.readback(self.work / "handoff", output)

    def test_notebooks_cleared_and_cloud_not_using_drive_mount_or_host_install(self):
        for name in ("col_02_portability_local.ipynb", "col_02_colab_acceptance.ipynb"):
            data = cr.load(ROOT / "notebooks" / name)
            for cell in data["cells"]:
                if cell["cell_type"] == "code":
                    self.assertIsNone(cell["execution_count"])
                    self.assertEqual(cell["outputs"], [])
                    compile("".join(cell["source"]), name, "exec")
        cloud = (ROOT / "notebooks/col_02_colab_acceptance.ipynb").read_text()
        self.assertNotIn("drive.mount", cloud)
        self.assertIn("--python", cloud)
        self.assertIn("--without-pip", cloud)
        self.assertIn("TRUSTED_SHA", cloud)

    def test_child_failure_stderr_is_preserved_before_raising(self):
        notebook = cr.load(ROOT / "notebooks/col_02_colab_acceptance.ipynb")
        tree = ast.parse("".join(notebook["cells"][2]["source"]))
        helper = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "checked")
        scope = {"subprocess": subprocess}
        exec(compile(ast.Module(body=[helper], type_ignores=[]), "col02-cell2", "exec"), scope)
        child = subprocess.CompletedProcess(["fixture"], 1, stdout="GATE_STARTED", stderr="EXACT_CHILD_REASON")
        captured = io.StringIO()
        with patch.object(subprocess, "run", return_value=child) as run, contextlib.redirect_stdout(captured):
            with self.assertRaises(subprocess.CalledProcessError): scope["checked"](["fixture"])
        self.assertIn("GATE_STARTED", captured.getvalue())
        self.assertIn("EXACT_CHILD_REASON", captured.getvalue())
        self.assertTrue(run.call_args.kwargs["capture_output"])

    def test_result_configs_matrix_and_rerun_checks(self):
        # Minimal offline records exercise bindings; Parquet/schema coverage is in EXP-01 tests.
        rows = []
        for key in c.RUNS:
            config_key = "DBSCAN_C1" if key == "RERUN_DBSCAN_C1" else key
            cfg = cr.load(ROOT / c.SWEEP_CONFIG_FILES[config_key])
            folder = self.work / key.lower()
            folder.mkdir()
            config = {"model_config": cfg, "model_config_version": cfg["model_config_version"], "experiment_run_id": key}
            cr.save(folder / "experiment_config.json", config)
            metrics = {"total_candidates": 279, "windows_processed": 2, "successful_windows": 1,
                       "failed_windows": 1, "total_sequence_members": 1, "total_aftershocks": 0, "total_noise": 278}
            cr.save(folder / "experiment_metrics.json", {"metrics": metrics})
            if key in c.KEYS:
                rows.append({"config_key": key, "algorithm_name": cfg["algorithm_name"],
                             "model_config_version": cfg["model_config_version"], "config_sha256": cr.digest(cfg), **metrics})
        matrix = {"pilot_id": cr.PILOT["pilot_id"], "total_configurations": 9, "configurations": rows}
        cr.save(self.work / "sweep_results_matrix.json", matrix)
        with patch.object(cr, "verify_result", return_value=([], [])), patch.object(cr, "parity", return_value={}):
            self.assertEqual(len(c.verify_results(ROOT, self.work, "unused")), 10)
            rows[0]["total_noise"] = 1
            (self.work / "sweep_results_matrix.json").write_text(json.dumps(matrix))
            with self.assertRaisesRegex(ValueError, "MATRIX_METRICS_CHANGED"):
                c.verify_results(ROOT, self.work, "unused")
            rows[0]["total_noise"] = 278
            rows.pop()
            (self.work / "sweep_results_matrix.json").write_text(json.dumps(matrix))
            with self.assertRaisesRegex(ValueError, "MATRIX_CONFIG_SET"):
                c.verify_results(ROOT, self.work, "unused")


if __name__ == "__main__": unittest.main()
