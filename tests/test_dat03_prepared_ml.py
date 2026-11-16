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
    def test_90k_receipt_cannot_be_unmeasured_or_for_another_runner(self):
        value={'profile_version':'dat03-whole-window-90k-v1','status':'PROFILE_BENCHMARK_PASSED','feature_rows':90000,
               'candidate_semantics_unchanged':True,'synthetic_component_qa':True,'s3_writes':False,'scientific_approval':False,
               'official_exported':False,'null_guard_fault_injections_passed':2,'services_unchanged':True,'runner_sha256':'a'*64,'scaling_sha256':'b'*64}
        app.verify_profile(value,'a'*64,'b'*64)
        for key, bad in [('feature_rows',90001),('feature_rows',True),('candidate_semantics_unchanged',False),('null_guard_fault_injections_passed',0),('runner_sha256','c'*64),('scaling_sha256','c'*64),('profile_version','arbitrary-100k')]:
            with self.assertRaises(ValueError):app.verify_profile({**value,key:bad},'a'*64,'b'*64)
    def test_retained_checkpoint_cannot_release_lease_via_another_split_preflight(self):
        intent={'split':'REPRODUCTION','mode':'build'}
        app.require_resume_context(intent,'REPRODUCTION','build')
        for split,mode in [('EXTENSION','build'),('REPRODUCTION','preflight'),('REPRODUCTION','rerun')]:
            with self.assertRaisesRegex(ValueError,'SAME_SPLIT_BUILD'):app.require_resume_context(intent,split,mode)

if __name__ == '__main__': unittest.main()
