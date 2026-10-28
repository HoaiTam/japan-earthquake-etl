import importlib.util
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("team_setup", ROOT / "scripts/dat03-team-setup.py")
setup = importlib.util.module_from_spec(spec); spec.loader.exec_module(setup)


class TeamSetupTest(unittest.TestCase):
    def test_assignments_cover_remaining_original_scope(self):
        plans = [setup.plan(owner) for owner in setup.OWNERS]
        self.assertEqual([p['assignment']['months'] for p in plans], [62, 62, 63])
        self.assertEqual([len(p['remaining_months']) for p in plans], [22, 62, 63])
        self.assertEqual(sum(len(p['remaining_months']) for p in plans), 147)

    def test_tris_exact_interval(self):
        p = setup.plan('ThanhTris')
        self.assertEqual(p['remaining_months'][0], '2013-08')
        self.assertEqual(p['remaining_months'][-1], '2018-09')
        self.assertEqual([len(g) for g in p['transport_groups_max_24_months']], [24, 24, 14])

    def test_rosy_exact_interval_and_halo_separate(self):
        p = setup.plan('rosy179')
        self.assertEqual(p['remaining_months'][0], '2018-10')
        self.assertEqual(p['remaining_months'][-1], '2023-12')
        self.assertNotIn('2024-01', p['remaining_months'])
        self.assertNotIn('USGS_2023_TRANSPORT_PINS', p['remaining_gates'])
        self.assertIn('WORKER_RUNTIME_ACCEPTANCE', p['remaining_gates'])

    def test_plan_does_not_claim_runtime_or_drive(self):
        for owner in setup.OWNERS:
            p = setup.plan(owner)
            self.assertFalse(p['execution_enabled'])
            self.assertFalse(p['research_ready'])
            self.assertEqual(p['runtime'], 'NOT_CHECKED')
            self.assertEqual(p['drive_identity'], 'NOT_CHECKED')

    def test_invalid_owner(self):
        with self.assertRaises(ValueError): setup.plan('../HoaiTam')

    def test_invalid_months(self):
        for start, end in [('2013-13', '2014-01'), ('2013-01', '2013-01'), ('2014-01', '2013-01'), ('1999-01', '2000-01')]:
            with self.assertRaises(ValueError): setup.months(start, end)

    def test_full_commit_required(self):
        with self.assertRaisesRegex(ValueError, 'FULL_COMMIT_REQUIRED'):
            setup.check_code('main')

    def test_wrong_commit(self):
        with patch.object(setup.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, 'b'*40, '')):
            with self.assertRaisesRegex(ValueError, 'WRONG_CODE_RELEASE'):
                setup.check_code('a'*40)

    def test_dirty_code(self):
        with patch.object(setup.subprocess, 'run', side_effect=[subprocess.CompletedProcess([], 0, 'a'*40, ''), subprocess.CompletedProcess([], 0, ' M scripts/dat03-team-setup.py', '')]):
            with self.assertRaisesRegex(ValueError, 'DIRTY_CHECKOUT'):
                setup.check_code('a'*40)

    def test_source_check_never_bypasses_code_pin(self):
        with tempfile.TemporaryDirectory() as temp, patch.object(setup, 'check_code', side_effect=ValueError('DAT03_SETUP_WRONG_CODE_RELEASE')):
            with self.assertRaisesRegex(ValueError, 'WRONG_CODE_RELEASE'):
                setup.verify_inputs('ThanhTris', Path(temp), 'a'*40)

    def test_config_tamper_rejected(self):
        with patch.object(setup, 'sha', return_value='0'*64):
            with self.assertRaisesRegex(ValueError, 'CONFIG_PIN_CHANGED'):
                setup.load_config()


if __name__ == '__main__':
    unittest.main()
