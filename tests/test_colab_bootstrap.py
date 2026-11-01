"""Offline contract fixtures only; these never constitute Colab runtime acceptance."""
import copy
import importlib.util
import json
from pathlib import Path
import stat
import tempfile
import types
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("exp01_bootstrap_test", ROOT / "notebooks/colab_runtime.py")
c = importlib.util.module_from_spec(spec); spec.loader.exec_module(c)


class ColabBootstrapTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()

    def gate(self):
        source = c.source_identity(ROOT); expected = c.pins(ROOT)
        env = {"direct_packages": expected, "pip_check": "PASSED", "requirements_sha256": {
            n: c.sha(ROOT / "notebooks" / n) for n in ("requirements-lock.txt", "requirements-colab-lock.txt")}}
        report = {"report_version": "exp-02-local-smoke-v2", "runner_mode": "nbclient", "fresh_kernel_acceptance": True,
                  "local_smoke_status": "PASSED", "colab_migration_allowed": True, "pilot_id": c.PILOT["pilot_id"],
                  "bundle_checksum": c.PILOT["feature_sha256"], "bundle_fingerprint": {"checksums_index_sha256": c.PILOT["index_sha256"]},
                  "code_version": "source-" + source["sha256"], "algorithms_evaluated": list(c.MODELS),
                  "package_versions": expected, "reconciliation": {a: {"total_candidates": 279, "windows_processed": 2} for a in c.MODELS}}
        path = self.root / "smoke.json"; c.save(path, report)
        gate = {"version": c.VERSION, "source": source, "pilot": c.PILOT, "local_smoke_sha256": c.sha(path),
                "environment": env, "inspection": {"runner_mode": "nbclient", "fresh_kernel_acceptance": True,
                "notebook_status": "RESTART_RUN_ALL_PASSED", "notebook_sha256": c.sha(ROOT / "notebooks/exp_01_portability_local.ipynb")}}
        return gate, path, env

    def test_exact_source_configuration_and_package_binding(self):
        gate, path, env = self.gate()
        self.assertEqual(c.PILOT["pilot_id"], c.validate_gate(ROOT, gate, path, c.sha(path), env)["pilot_id"])
        changed = copy.deepcopy(gate); changed["source"]["files"]["notebooks/baseline_models.py"] = "a"*64
        with self.assertRaisesRegex(ValueError, "SOURCE_OR_LOCAL"): c.validate_gate(ROOT, changed, path, c.sha(path), env)

    def test_generic_passed_report_is_not_a_cloud_gate(self):
        gate, path, env = self.gate(); path.write_text('{"status":"PASSED"}')
        with self.assertRaisesRegex(ValueError, "REPORT_PIN_CHANGED"):
            c.validate_gate(ROOT, gate, path, gate["local_smoke_sha256"], env)

    def test_synthetic_fallback_or_missing_algorithm_rejected(self):
        gate, path, env = self.gate(); original = c.load(path)
        for key, value in (("runner_mode", "isolated_exec"), ("fresh_kernel_acceptance", False),
                           ("algorithms_evaluated", ["WINDOW"]), ("pilot_id", "synthetic-pilot")):
            report = {**original, key: value}; path.write_text(json.dumps(report)); gate["local_smoke_sha256"] = c.sha(path)
            with self.assertRaisesRegex(ValueError, "NOT_REAL_PILOT"): c.validate_gate(ROOT, gate, path, c.sha(path), env)

    def test_package_drift_blocked_even_when_report_status_passed(self):
        gate, path, env = self.gate(); other = copy.deepcopy(env); other["direct_packages"]["numpy"] = "9.0"
        with self.assertRaisesRegex(ValueError, "PACKAGE_PARITY"): c.validate_gate(ROOT, gate, path, c.sha(path), other)

    def test_local_report_package_values_are_also_bound(self):
        gate, path, env = self.gate(); report = c.load(path); report["package_versions"]["numpy"] = "0.0"
        path.write_text(json.dumps(report)); gate["local_smoke_sha256"] = c.sha(path)
        with self.assertRaisesRegex(ValueError, "REPORT_PACKAGES"): c.validate_gate(ROOT, gate, path, c.sha(path), env)

    def archive(self, name="safe.zip", member="file.txt", data=b"public-fixture"):
        path = self.root / name
        with zipfile.ZipFile(path, "w") as z: z.writestr(member, data)
        return path

    def test_checksum_must_pass_before_extraction(self):
        p = self.archive(); dest = self.root / "out"
        with self.assertRaisesRegex(ValueError, "EXTERNAL_ZIP_PIN"): c.extract(p, dest, "0"*64)
        self.assertFalse(dest.exists())

    def test_safe_zip_and_no_overwrite(self):
        p = self.archive(); dest = self.root / "out"
        c.extract(p, dest, c.sha(p)); self.assertEqual(b"public-fixture", (dest / "file.txt").read_bytes())
        with self.assertRaisesRegex(ValueError, "NEW_DIRECTORY"): c.extract(p, dest, c.sha(p))

    def test_path_traversal_windows_and_aliases_rejected(self):
        for i, member in enumerate(("../escape", "/absolute", "C:/drive", "folder\\file", "folder/../escape", "folder//alias")):
            p = self.archive(str(i)+".zip", member)
            with self.assertRaisesRegex(ValueError, "UNSAFE_ZIP"): c.extract(p, self.root / str(i), c.sha(p))

    def test_symlink_zip_duplicate_and_size_bound_rejected(self):
        p = self.root / "link.zip"
        with zipfile.ZipFile(p, "w") as z:
            info = zipfile.ZipInfo("link"); info.external_attr = (stat.S_IFLNK | 0o777) << 16; z.writestr(info, "target")
        with self.assertRaisesRegex(ValueError, "UNSAFE_ZIP"): c.extract(p, self.root / "out", c.sha(p))
        p = self.archive("large.zip")
        with self.assertRaisesRegex(ValueError, "SIZE_BOUND"): c.extract(p, self.root / "large", c.sha(p), 1)

    def test_unsupported_plain_host_cannot_claim_colab(self):
        with patch.object(c.subprocess, "run", return_value=types.SimpleNamespace(returncode=1)):
            with self.assertRaisesRegex(ValueError, "REAL_COLAB_RUNTIME_REQUIRED"):
                c.run_cloud(self.root, self.root / "out", "HoaiTam")

    def test_invalid_recipient_blocked(self):
        with self.assertRaisesRegex(ValueError, "RECIPIENT_REQUIRED"):
            c.run_cloud(self.root, self.root / "out", "somebody-else")

    def rows(self):
        rows = [{"mainshock_event_id": "main", "candidate_event_id": name, "cluster_id": cluster,
                 "is_noise": cluster == -1, "membership_probability": probability, "is_sequence_member": member,
                 "experiment_run_id": "run1", "created_at_utc": "old", "event_role_candidate": "POST"}
                for name, cluster, probability, member in (("a", 0, .9, True), ("b", 0, .8, True), ("c", -1, 0., False))]
        return rows, [{"mainshock_event_id": "main", "aftershock_count": 2, "runtime_seconds": 10., "created_at_utc": "old"}]

    def test_cluster_label_permutation_and_runtime_do_not_break_parity(self):
        left = self.rows(); right = copy.deepcopy(left)
        for row in right[0]:
            if row["cluster_id"] == 0: row["cluster_id"] = 99
            row["experiment_run_id"] = "other-run"; row["created_at_utc"] = "new"
        right[1][0]["runtime_seconds"] = .1
        self.assertEqual("PASSED", c.parity(left, right)["status"])

    def test_same_count_but_different_membership_or_role_fails(self):
        for field, value in (("is_noise", True), ("event_role_candidate", "PRE"), ("candidate_event_id", "changed")):
            left = self.rows(); right = copy.deepcopy(left); right[0][0][field] = value
            with self.assertRaises(ValueError): c.parity(left, right)

    def test_cluster_partition_change_cannot_hide_behind_labels(self):
        left = self.rows(); right = copy.deepcopy(left); right[0][0]["cluster_id"] = 1
        with self.assertRaisesRegex(ValueError, "PARTITION_MISMATCH"): c.parity(left, right)

    def test_probability_tolerance_and_nonfinite_values(self):
        left = self.rows(); right = copy.deepcopy(left); right[0][0]["membership_probability"] += 1e-9
        self.assertEqual("PASSED", c.parity(left, right)["status"])
        right[0][0]["membership_probability"] = float("nan")
        with self.assertRaisesRegex(ValueError, "NUMERIC_MISMATCH"): c.parity(left, right)

    def test_duplicate_grain_rejected(self):
        left = self.rows(); left[0].append(left[0][0])
        with self.assertRaisesRegex(ValueError, "DUPLICATE_RESULT_GRAIN"): c.parity(left, left)

    def test_schema_preserving_summary_failure_is_not_dropped(self):
        left = self.rows(); right = copy.deepcopy(left); right[1][0]["aftershock_count"] = 1
        with self.assertRaisesRegex(ValueError, "SUMMARY_MISMATCH"): c.parity(left, right)

    def test_new_notebooks_are_cleared_and_code_compiles(self):
        for name in ("exp_01_portability_local.ipynb", "exp_01_colab_bootstrap.ipynb"):
            notebook = c.load(ROOT / "notebooks" / name)
            for cell in notebook["cells"]:
                if cell["cell_type"] == "code":
                    self.assertIsNone(cell["execution_count"]); self.assertEqual([], cell["outputs"])
                    code = "".join(cell["source"]); compile(code, name, "exec")
                    self.assertNotIn("create_pilot_fixture", code)

    def test_plotting_dependencies_are_pinned_without_changing_existing_model_lock(self):
        p = c.pins(ROOT); self.assertEqual("2.2.2", p["numpy"])
        self.assertEqual("2.2.3", p["pandas"]); self.assertEqual("3.10.0", p["matplotlib"])
        self.assertEqual('25.3', p['pip'])

    def test_bootstrap_uses_target_pip_without_host_install_or_ensurepip(self):
        notebook = c.load(ROOT / 'notebooks/exp_01_colab_bootstrap.ipynb')
        cell = ''.join(next(x for x in notebook['cells'] if x['id'] == 'isolated-env')['source'])
        self.assertIn("'-m', 'venv', '--without-pip'", cell)
        self.assertIn("'--isolated', '--python', str(PYTHON), 'install'", cell)
        self.assertNotIn('--system-site-packages', cell)
        self.assertIn('capture_output=True', cell)

    def test_json_duplicate_and_nonfinite_are_rejected(self):
        p = self.root / "test.json"
        for value in ('{"a":1,"a":2}', '{"a":NaN}'):
            p.write_text(value)
            with self.assertRaises(ValueError): c.load(p)

    def copy_fixture(self):
        output = self.root / 'output'; output.mkdir()
        names = {'colab-report.json', 'pilot-receipt.json', 'environment-plot-smoke.png'} | {
            'results/'+a.lower()+'/'+n for a in c.MODELS for n in c.ARTIFACTS}
        for name in names:
            target = output / name; target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b'public-output-fixture')
        c.save(output / 'output-checksums.json', {n: c.sha(output / n) for n in names})
        (output / 'private-cache').write_bytes(b'must-not-copy')
        return output, names

    def test_output_copy_exact_allowlist_readback_excludes_cache(self):
        output, names = self.copy_fixture(); destination = self.root / 'copy'
        result = c.output_copy(output, destination, self.root / 'input')
        self.assertEqual('OUTPUT_COPY_READBACK_VERIFIED', result['status'])
        self.assertEqual(25, result['files'])
        self.assertEqual(names | {'output-checksums.json'}, {
            p.relative_to(destination).as_posix() for p in destination.rglob('*') if p.is_file()})
        for name in names: self.assertEqual(c.sha(output / name), c.sha(destination / name))

    def test_output_copy_rejects_changed_input_and_unsafe_destination(self):
        output, _ = self.copy_fixture(); input_root = self.root / 'input'; input_root.mkdir()
        for destination in (output / 'nested', input_root / 'nested', input_root):
            with self.assertRaisesRegex(ValueError, 'COPY_SCOPE_INVALID'):
                c.output_copy(output, destination, input_root)
        (output / 'colab-report.json').write_bytes(b'changed')
        with self.assertRaisesRegex(ValueError, 'OUTPUT_CHANGED'):
            c.output_copy(output, self.root / 'copy', input_root)
        self.assertFalse((self.root / 'copy').exists())

    def test_output_copy_rejects_expanded_checksum_allowlist(self):
        output, _ = self.copy_fixture(); index = output / 'output-checksums.json'
        value = c.load(index); value['private-cache'] = c.sha(output / 'private-cache')
        index.write_text(json.dumps(value))
        with self.assertRaisesRegex(ValueError, 'COPY_SCOPE_INVALID'):
            c.output_copy(output, self.root / 'copy', self.root / 'input')

    def test_duplicate_zip_entries_are_rejected(self):
        import warnings
        path = self.root / 'duplicates.zip'
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', UserWarning)
            with zipfile.ZipFile(path, 'w') as z:
                z.writestr('file', b'first'); z.writestr('file', b'second')
        with self.assertRaisesRegex(ValueError, 'ZIP_ENTRY_SET'):
            c.extract(path, self.root / 'extracted', c.sha(path))


if __name__ == "__main__": unittest.main()
