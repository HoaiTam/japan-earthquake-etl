"""Offline controller gates. No Docker, SQL, cloud, or scientific fixtures."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('prepared_ml', Path(__file__).resolve().parents[1] / 'scripts/dat03-prepared-ml.py')
app = importlib.util.module_from_spec(spec); spec.loader.exec_module(app)

class PreparedMLTests(unittest.TestCase):
    def test_exact_operation(self):
        with self.assertRaisesRegex(ValueError, 'EXACT_PREPARED_ML_OPERATION'): app.operation(Path(tempfile.gettempdir()).resolve() / 'not-a-run')
    def test_no_arbitrary_worker_or_budget(self):
        root = Path('/opt/pipeline/staging/dat03-prepared-ml') / ('a' * 32)
        for mode, seconds in [('skip-coverage', 6000), ('preflight', True), ('preflight', 6001), ('build', 0)]:
            with self.assertRaisesRegex(ValueError, 'BOUNDED_PREPARED_ML_WORKER'):
                app.worker(root, 'b'*64, 'REPRODUCTION', mode, 'c'*32, seconds, '')
    def test_foreign_split_rejected_before_runtime(self):
        with self.assertRaisesRegex(ValueError, 'BOUNDED_ML_SESSION'):
            app.run(Path(tempfile.gettempdir()).resolve() / ('a' * 32), 'PILOT', 'build', 6000)
    def test_bad_evidence_cannot_create_operation(self):
        with tempfile.TemporaryDirectory() as temp:
            base=Path(temp).resolve(); e=base/'evidence.json';app.save(e, {});target=base/('a'*32)
            with self.assertRaisesRegex(ValueError, 'TRUSTED_GOLD_EVIDENCE'): app.prepare(target,e,'0'*64)
            self.assertFalse(target.exists())
    def test_lease_is_unique_and_not_the_gold_owner(self):
        self.assertEqual(app.owner(Path('/tmp')/('a'*32)), {'dag_id':'mli_02_build_dataset','run_id':'dat03-ml-'+'a'*32})

if __name__ == '__main__': unittest.main()
