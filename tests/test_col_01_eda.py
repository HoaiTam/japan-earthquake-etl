import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import importlib.util
import zipfile
import os
import subprocess

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'notebooks'), str(ROOT / 'tests')]
from col_01_eda import inspect
from test_experiment_harness import create_pilot_fixture
spec = importlib.util.spec_from_file_location('col01_cli', ROOT / 'scripts/col01-eda.py')
cli = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cli)


class Col01Test(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.bundle = self.root / 'bundle'
        self.bundle.mkdir()
        create_pilot_fixture(self.bundle)

    def test_fixture_report_and_protected_input(self):
        report = inspect(self.bundle, self.root / 'output', 'test-commit')
        self.assertEqual(report['duplicate_pairs'], 0)
        self.assertFalse(report['research_ready'])
        self.assertTrue(report['input_unchanged'])
        self.assertEqual((self.root / 'output/window-counts.png').read_bytes()[:8], b'\x89PNG\r\n\x1a\n')
        self.assertEqual(json.loads((self.root / 'output/pilot-eda.json').read_text())['pilot_id'], report['pilot_id'])
        with self.assertRaisesRegex(ValueError, 'OUTPUT_MUST_BE_OUTSIDE'):
            inspect(self.bundle, self.bundle / 'output', 'test-commit')

    def test_reject_tampered_input(self):
        (self.bundle / 'coverage_report.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'CHECKSUM_MISMATCH'):
            inspect(self.bundle, self.root / 'output', 'test-commit')
        self.assertFalse((self.root / 'output').exists())

    def test_wrong_pilot_rejected_before_output(self):
        with self.assertRaisesRegex(ValueError, 'PILOT_ID_MISMATCH'):
            inspect(self.bundle, self.root / 'output', 'test-commit', expected_pilot_id='wrong')
        self.assertFalse((self.root / 'output').exists())

    def test_existing_output_not_overwritten(self):
        out = self.root / 'output'
        out.mkdir()
        (out / 'keep').write_text('unchanged')
        with self.assertRaisesRegex(ValueError, 'OUTPUT_MUST_BE_EMPTY'):
            inspect(self.bundle, out, 'test-commit')
        self.assertEqual((out / 'keep').read_text(), 'unchanged')

    def test_missing_input_no_fixture_fallback(self):
        with self.assertRaises(FileNotFoundError):
            inspect(self.root / 'missing', self.root / 'output', 'test-commit')

    def test_code_identity_required(self):
        with self.assertRaisesRegex(ValueError, 'CODE_VERSION_REQUIRED'):
            inspect(self.bundle, self.root / 'output', '')

    def test_symlink_input_is_not_resolved_away(self):
        linked = self.root / 'linked'
        linked.symlink_to(self.bundle, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'SYMLINK_REJECTED'):
            inspect(linked, self.root / 'output', 'test-commit')

    def test_pack_rejects_fixture_as_real_pilot(self):
        with self.assertRaisesRegex(ValueError, 'TRUSTED_PILOT_REQUIRED'):
            cli.pack(self.bundle, self.root / 'handoff.zip', 'test-commit')

    def test_fresh_child_rejects_inherited_inline_backend_before_import(self):
        code = ("import os,sys;sys.path.insert(0,sys.argv[1]);"
                "from col_01_eda import inspect;"
                "r=inspect(sys.argv[2],sys.argv[3],'test-commit');"
                "assert r['input_unchanged'];"
                "assert os.environ['MPLBACKEND']=='module://missing_inline_backend'")
        env = dict(os.environ, MPLBACKEND='module://missing_inline_backend',
                   MPLCONFIGDIR=str(self.root / 'mpl-cache'))
        result = subprocess.run([sys.executable, '-c', code, str(ROOT / 'notebooks'),
                                 str(self.bundle), str(self.root / 'child-output')],
                                env=env, capture_output=True, text=True, timeout=90)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.root / 'child-output/window-counts.png').is_file())

    def test_pack_allowlist_and_hashes(self):
        # Explicitly replace trusted pins ONLY for synthetic packaging regression.
        pid = json.loads((self.bundle / 'pilot_manifest.json').read_text())['pilot_id']
        with patch.multiple(cli, PILOT=pid, FEATURE_SHA=cli.sha(self.bundle / cli.BUNDLE_FILES[0]), INDEX_SHA=cli.sha(self.bundle / 'checksums.sha256')):
            result = cli.pack(self.bundle, self.root / 'handoff.zip', 'test-commit')
        with zipfile.ZipFile(self.root / 'handoff.zip') as z:
            self.assertEqual(set(z.namelist()), set(result['manifest']['files']) | {'handoff.json'})
            import hashlib
            for name, pin in result['manifest']['files'].items():
                self.assertEqual(hashlib.sha256(z.read(name)).hexdigest(), pin)
        self.assertFalse(any('secret' in name or '.env' in name for name in result['manifest']['files']))

    def test_notebook_cleared_and_cloud_verification_present(self):
        nb = json.loads((ROOT / 'notebooks/col_01_pilot_eda.ipynb').read_text())
        for cell in nb['cells']:
            if cell['cell_type'] == 'code':
                self.assertIsNone(cell['execution_count'])
                self.assertEqual(cell['outputs'], [])
        source = ''.join(''.join(c['source']) for c in nb['cells'])
        self.assertIn('import google.colab', source)
        self.assertIn('COL01_OUTPUT_ZIP_VERIFIED', source)
        self.assertIn("'--python', PYTHON, 'check'", source)
        self.assertNotIn("checked([PYTHON, '-m', 'pip', 'check'])", source)


if __name__ == '__main__':
    unittest.main()
