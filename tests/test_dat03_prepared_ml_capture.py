"""Portable-capture metadata gates only: no Docker, lake, source data or runtime claims."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
import json
import subprocess
import sys

spec=importlib.util.spec_from_file_location('capture_ml',Path(__file__).resolve().parents[1]/'scripts/capture-dat03-prepared-ml.py')
app=importlib.util.module_from_spec(spec);spec.loader.exec_module(app)


class CaptureTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
        self.session='a'*32;self.p=self.root/'host-sessions'/self.session/'runtime';self.p.mkdir(parents=True)
        a=app.a;a.save(self.root/'request.json',{'fixture':True});request=a.sha(self.root/'request.json')
        self.report={'status':'DATASET_EXPORTED','dataset_split':'REPRODUCTION','request_sha256':request,'completed_steps':10,'gold_snapshots_unchanged':True,'dataset_id':'ds_'+'b'*64,
                     'final_readback':{'dataset_id':'ds_'+'b'*64,'status':'DATASET_EXPORTED','exported':True,'trusted_handoff_verified':True,'quality_gate_status':'PASSED','invalid_feature_count':0}}
        a.save(self.p/'report.json',self.report);a.save(self.p/'execution.json',{'exit_code':0,'stop_reason':None})
        a.save(self.p/'candidate.json',{'request_sha256':request,'split':'REPRODUCTION','report_sha256':a.sha(self.p/'report.json'),'execution_sha256':a.sha(self.p/'execution.json')})
        a.save(self.p.parent/'acceptance.json',{'status':'DATASET_EXPORTED','lease_retained':False,'report_sha256':a.sha(self.p/'report.json'),
              'execution_sha256':a.sha(self.p/'execution.json'),'candidate_sha256':a.sha(self.p/'candidate.json'),'request_sha256':request})
        self.expected=a.sha(self.p.parent/'acceptance.json')
    def test_complete_sha_chain_is_required(self):
        self.assertEqual('ds_'+'b'*64,app.validate_session(self.root,self.session,self.expected,'REPRODUCTION')['dataset_id'])
        with self.assertRaisesRegex(ValueError,'EXTERNAL_ACCEPTANCE'):app.validate_session(self.root,self.session,'0'*64,'REPRODUCTION')
    def test_changed_report_is_not_rehashed_as_an_accepted_export(self):
        (self.p/'report.json').write_bytes(b'{}')
        with self.assertRaises(ValueError):app.validate_session(self.root,self.session,self.expected,'REPRODUCTION')
    def test_foreign_split_and_unbounded_session_fail_closed(self):
        with self.assertRaises(ValueError):app.validate_session(self.root,self.session,self.expected,'EXTENSION')
        with self.assertRaises(ValueError):app.validate_session(self.root,'../foreign',self.expected,'REPRODUCTION')
    def test_changed_candidate_chain_fails(self):
        (self.p/'candidate.json').write_bytes(b'{}')
        with self.assertRaises(ValueError):app.validate_session(self.root,self.session,self.expected,'REPRODUCTION')
    def remote_fixture(self):
        p=self.root/'portable';p.mkdir();lines=[]
        for name in ('dataset_manifest.json','collection-preparation.json','export-checksums.sha256'):
            (p/name).write_bytes(b'{}');lines.append(app.a.sha(p/name)+'  '+name+'\n')
        (p/'checksums.sha256').write_text(''.join(lines))
        return p
    def remote_index(self,p):
        pins={'bundle_manifest_sha256':app.a.sha(p/'dataset_manifest.json'),'checksums_sha256':app.a.sha(p/'checksums.sha256')}
        return subprocess.run([sys.executable,'-B','-c',app.REMOTE_INDEX,str(p),json.dumps(pins)],capture_output=True,text=True,timeout=10)
    def test_remote_index_captures_only_exact_allowlisted_files(self):
        p=self.remote_fixture();result=self.remote_index(p);self.assertEqual(0,result.returncode,result.stderr)
        self.assertEqual(4,len(json.loads(result.stdout)))
        (p/'private.env').write_bytes(b'not-for-capture')
        self.assertNotEqual(0,self.remote_index(p).returncode)
    def test_remote_traversal_and_symlink_are_rejected_before_copy(self):
        p=self.remote_fixture();original=(p/'checksums.sha256').read_bytes()
        (p/'checksums.sha256').write_bytes(original+b'0'*64+b'  ../source.zip\n')
        self.assertNotEqual(0,self.remote_index(p).returncode)
        (p/'checksums.sha256').write_bytes(original);(p/'collection-preparation.json').unlink()
        (p/'collection-preparation.json').symlink_to(p/'dataset_manifest.json')
        self.assertNotEqual(0,self.remote_index(p).returncode)


if __name__=='__main__':unittest.main()
