"""Metadata/control gates only; no Docker, SQL, Spark or network."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import zipfile
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('portable_gold', Path(__file__).resolve().parents[1] / 'scripts/dat03-portable-gold.py')
app = importlib.util.module_from_spec(spec); spec.loader.exec_module(app)
recovery_spec = importlib.util.spec_from_file_location('gold_recovery', Path(__file__).resolve().parents[1] / 'scripts/dat03-portable-gold-recovery.py')
recovery = importlib.util.module_from_spec(recovery_spec); recovery_spec.loader.exec_module(recovery)


class PortableGoldTests(unittest.TestCase):
    def test_operation_root_must_be_exact_new_identity(self):
        with self.assertRaisesRegex(ValueError, 'EXACT_GOLD_OPERATION'):
            app.operation(Path(tempfile.gettempdir()).resolve() / 'not-a-run')
        _, inside = app.operation(Path(tempfile.gettempdir()).resolve() / ('a' * 32))
        self.assertEqual(inside, '/opt/pipeline/staging/dat03-portable-gold/' + 'a' * 32)

    def test_worker_rejects_bounds_before_any_mutation(self):
        for seconds in (0, 599, 6001, True):
            with self.assertRaisesRegex(ValueError, 'BOUNDED_GOLD_WORKER'):
                app.worker(Path('/opt/pipeline/staging/dat03-portable-gold') / ('a' * 32), 'b' * 64, 'c' * 32, 'run', seconds)

    def test_bad_mode_cannot_supply_arbitrary_class_or_sql(self):
        for mode in ('sql', 'release', 'skip-verify'):
            with self.assertRaisesRegex(ValueError, 'BOUNDED_GOLD_WORKER'):
                app.worker(Path('/opt/pipeline/staging/dat03-portable-gold') / ('a' * 32), 'b' * 64, 'c' * 32, mode, 600)

    def test_recovery_is_not_a_generic_retry_flag(self):
        with self.assertRaisesRegex(ValueError, 'EXPLICIT_NARROW_CONTINUATION'):
            app.worker(Path('/opt/pipeline/staging/dat03-portable-gold') / ('a' * 32), 'b' * 64, 'c' * 32, 'recover', 600)

    @staticmethod
    def candidate():
        return {'status': 'PUBLISHED', 'records': [{'phase': 'commit', 'result': {'commit_sha256': 'a' * 64, 'reused': True}},
            {'phase': 'publication', 'result': {'commit_sha256': 'a' * 64, 'publication_sha256': 'b' * 64,
                'snapshots': {'event_current': {'snapshot_id': 9007199254740993}}, 'reused': True}}]}

    def test_rerun_keeps_int64_snapshot_exact(self):
        app.verify_rerun(self.candidate(), self.candidate())
        changed = self.candidate(); changed['records'][1]['result']['snapshots']['event_current']['snapshot_id'] += 1
        with self.assertRaisesRegex(ValueError, 'SNAPSHOTS_CHANGED'):
            app.verify_rerun(self.candidate(), changed)

    def test_rerun_requires_same_commit_bytes(self):
        changed = self.candidate(); changed['records'][0]['result']['commit_sha256'] = 'd' * 64
        with self.assertRaisesRegex(ValueError, 'COMMIT_CHANGED'):
            app.verify_rerun(self.candidate(), changed)

    def test_rerun_cannot_claim_reuse_without_real_flag(self):
        changed = self.candidate(); changed['records'][0]['result']['reused'] = False
        with self.assertRaisesRegex(ValueError, 'COMMIT_CHANGED'):
            app.verify_rerun(self.candidate(), changed)

    def test_rerun_cannot_omit_a_phase(self):
        changed = self.candidate(); changed['records'].pop()
        with self.assertRaises(ValueError): app.verify_rerun(self.candidate(), changed)

    def test_wrong_evidence_pin_fails_before_creating_operation(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp).resolve(); evidence = base / 'evidence.json'; evidence.write_text('{}')
            target = base / ('a' * 32)
            with self.assertRaisesRegex(ValueError, 'EVIDENCE_CHANGED'): app.prepare(target, evidence, '0' * 64)
            self.assertFalse(target.exists())

    def test_unaccepted_session_is_not_silently_skipped(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve(); folder = root / 'host-sessions' / ('a' * 32); folder.mkdir(parents=True)
            self.assertFalse(app.resolved_session(root, folder))

    def test_resolution_cannot_reference_an_arbitrary_path(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve(); folder = root / 'host-sessions' / ('a' * 32); folder.mkdir(parents=True)
            app.save(folder / 'resolution.json', {'recovery_session': '../escape'})
            with self.assertRaisesRegex(ValueError, 'EXACT_RECOVERY_SESSION'):
                app.resolved_session(root, folder)

    def test_continuation_is_exactly_writer_and_entry_adapter(self):
        with tempfile.TemporaryDirectory() as temp:
            base = Path(temp).resolve(); a = {n: b'old' for n in recovery.CHANGED}; a['ie212/earthquake/spark/gold/Transform.class'] = b'stable'
            b = {**a, **{n: b'fixed' for n in recovery.CHANGED}, **{n: b'validator' for n in recovery.ADDED}}
            def jar(name, values):
                path = base / name
                with zipfile.ZipFile(path, 'w') as output:
                    for k, v in values.items(): output.writestr(k, v)
                return path
            old, current = jar('old.jar', a), jar('new.jar', b)
            recovery.compatibility(old, current)
            b['ie212/earthquake/spark/gold/Transform.class'] = b'changed'
            with self.assertRaisesRegex(ValueError, 'CHANGED_TRANSFORM'):
                recovery.compatibility(old, jar('bad.jar', b))

    def test_recovery_bad_inspection_pin_cannot_write_files(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve() / ('a' * 32); root.mkdir(); inspection = root / 'inspection.json'; app.save(inspection, {})
            with self.assertRaisesRegex(ValueError, 'PINNED_READ_ONLY_INSPECTION'):
                recovery.prepare(root, inspection, '0' * 64, 'b' * 32)
            self.assertFalse((root / 'continuations').exists())


if __name__ == '__main__': unittest.main()
