"""Offline QA-05 guards. Never Docker, source API, or data writes."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import qa05_profile as q


class ResourceProfileTest(unittest.TestCase):
    def setUp(self):
        self.config_path = ROOT / 'config/qa/resource-profile-v1.json'
        self.c, self.ev, _ = q.configuration(ROOT, self.config_path)

    def test_config_real_six_table_pin_and_budgets(self):
        self.assertEqual(set(self.ev['tables']), q.TABLES)
        self.assertEqual(self.c['suite_window_seconds'], 600)
        self.assertEqual(self.ev['tables']['event_current']['count'], 4488650)

    def test_reject_changed_config_schema_budgets_and_profiles(self):
        variants = [dict(self.c, suite_window_seconds=3600), dict(self.c, query_window_seconds=True),
                    dict(self.c, repetitions=1), dict(self.c, evidence='../private.json'),
                    dict(self.c, evidence_sha256='0'*64), dict(self.c, unexpected=True),
                    dict(self.c, profiles=[{'name':'baseline','task_concurrency':True},{'name':'candidate','task_concurrency':2}])]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); (root/'config/qa').mkdir(parents=True); (root/'docs/evidence').mkdir(parents=True)
            (root/self.c['evidence']).write_bytes((ROOT/self.c['evidence']).read_bytes())
            for config in variants:
                with self.subTest(config=config):
                    path=root/'config/qa/test.json'; path.write_text(json.dumps(config))
                    with self.assertRaises(ValueError): q.configuration(root,path)

    def test_symlink_config_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            target=Path(tmp).resolve()/'link.json'; target.symlink_to(self.config_path)
            with self.assertRaises(ValueError): q.configuration(ROOT,target)

    def test_snapshot_overflow_and_boolean_rejected(self):
        for value in (True, 0, '1.23', str(2**63)):
            ev=copy.deepcopy(self.ev); ev['tables']['event_current']['snapshot_id']=value
            with self.subTest(value=value), self.assertRaises(ValueError): q.workload(ev)

    def test_sql_is_fixed_select_pinned_sorted_no_user_sql(self):
        queries=q.workload(self.ev)
        self.assertEqual(set(queries),{'fact_summary','monthly','bands'})
        for sql in queries.values():
            self.assertTrue(sql.startswith('SELECT ')); self.assertNotIn(';',sql)
            self.assertIn('FOR VERSION AS OF 7580315846938041321',sql)
        self.assertIn('sum(latitude)',queries['fact_summary'])
        self.assertIn('ORDER BY 1,2',queries['monthly'])

    def test_docker_units(self):
        for text,expected in [('512MiB',512*1024**2),('1.5GiB',int(1.5*1024**3)),('3MB',3000000),('12B',12)]:
            self.assertEqual(q.byte_size(text),expected)
        for bad in ('','-1B','NaNMiB','unknown','12TB'):
            with self.assertRaises(ValueError): q.byte_size(bad)

    def test_result_signature_counts_exact_float_sum_stable(self):
        self.assertEqual(q.result_signature([[279,0.10000000001]]),q.result_signature([[279,0.10000000002]]))
        self.assertNotEqual(q.result_signature([[279]]),q.result_signature([[278]]))
        for bad in (float('nan'),float('inf')):
            with self.assertRaises(ValueError): q.result_signature([[bad]])

    def test_compare_ab_ba_results(self):
        queries=self.records()
        result=q.comparison(queries)
        self.assertEqual(result['bands']['candidate_vs_baseline_ratio'],.5)

    @staticmethod
    def records():
        return [{'workload':name,'warmup':False,'profile':profile,'wall_seconds':seconds,'result_sha256':'a'*64}
                for name in ('fact_summary','monthly','bands') for _ in range(2)
                for profile,seconds in [('baseline',2),('candidate',1)]]

    def test_incomplete_mismatch_timeout_reject(self):
        for mode in ('missing','hash','time'):
            values=self.records()
            if mode=='missing': values.pop()
            elif mode=='hash': values[-1]['result_sha256']='b'*64
            else: values[-1]['wall_seconds']=61
            with self.subTest(mode=mode),self.assertRaises(ValueError): q.comparison(values)

    def test_current_snapshot_changed_reject(self):
        remote=Mock(); remote.load.return_value={'metadata':{'current-snapshot-id':1},'metadata-location':'s3://example/metadata'}
        with self.assertRaisesRegex(ValueError,'CURRENT_SNAPSHOT_CHANGED'): q.table_identity(remote,self.ev)

    def test_metadata_identity_all_tables(self):
        remote=Mock();remote.load.side_effect=lambda ns,name:{'metadata':{'current-snapshot-id':self.ev['tables'][name]['snapshot_id']},'metadata-location':'s3://example/'+name}
        result=q.table_identity(remote,self.ev)
        self.assertEqual(len(result),6); self.assertEqual(remote.load.call_count,6)
        self.assertNotIn('s3://',json.dumps(result))

    def test_file_and_partition_reconciliation(self):
        ev=copy.deepcopy(self.ev); ev['tables']={'event_current':{'count':100,'snapshot_id':'123'}}
        remote=Mock();remote.query.side_effect=[[[2,100,2000,500,1500,2,0]],[[2]]]
        result=q.file_profile(remote,ev)['event_current']
        self.assertEqual(result['records'],100); self.assertEqual(result['partitions'],2)
        self.assertEqual(remote.query.call_count,2)

    def test_file_count_or_delete_or_partition_mismatch_reject(self):
        ev=copy.deepcopy(self.ev);ev['tables']={'event_current':{'count':100,'snapshot_id':'123'}}
        for files,partitions in [([0,100,2000,500,1500,0,0],[[1]]),([2,99,2000,500,1500,2,0],[[2]]),
                                 ([2,100,2000,500,1500,2,1],[[2]]),([2,100,2000,500,1500,2,0],[[3]])]:
            remote=Mock();remote.query.side_effect=[[files],partitions]
            with self.assertRaises(ValueError):q.file_profile(remote,ev)

    def test_disk_gate_blocks_before_docker(self):
        remote=Mock()
        with tempfile.TemporaryDirectory() as tmp,patch.object(q.shutil,'disk_usage',return_value=Mock(free=q.RESERVE-1)):
            root=Path(tmp).resolve()
            with self.assertRaisesRegex(ValueError,'HOST_DISK_RESERVE'): q.run(root,self.config_path,root/'staging/new',remote)
        remote.services.assert_not_called()

    def test_existing_output_and_outside_staging_reject(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve(); old=root/'staging/old';old.mkdir(parents=True)
            for output in (old,root/'staging',root/'outside'):
                with self.assertRaises(ValueError):q.run(root,self.config_path,output,Mock())

    def test_monitor_disk_guard_and_metric_failure(self):
        m=q.Monitor(Mock(),ROOT);m.started=0
        with patch.object(q.shutil,'disk_usage',return_value=Mock(free=1)):
            with self.assertRaises(ValueError):m.sample()
        m.failed=True
        with self.assertRaises(ValueError):m.finish()

    def test_query_worker_is_readonly_and_bounded(self):
        compile(q.QUERY_AGENT,'qa05-worker','exec')
        self.assertIn('query_max_run_time=60s',q.QUERY_AGENT)
        self.assertIn('query_max_memory=256MB',q.QUERY_AGENT)
        self.assertNotIn('put_object',q.QUERY_AGENT)
        self.assertNotIn('DELETE',q.QUERY_AGENT.split("sql=p['sql']",1)[0])

    def test_remote_failure_not_retried(self):
        remote=q.Runtime()
        with patch.object(q.subprocess,'run',return_value=Mock(returncode=1)) as called:
            with self.assertRaisesRegex(ValueError,'NO_RETRY'):remote.query_profile('SELECT 1',1)
        self.assertEqual(called.call_count,1)

    def test_ci_and_make_wiring(self):
        make=(ROOT/'Makefile').read_text()
        self.assertIn('test-qa-05',make.split('ci-docs-base:',1)[1])
        self.assertIn('python3 -B -m unittest tests.test_qa05_profile',make)

    def fake_runtime(self):
        remote=Mock()
        remote.services.return_value={'service':'unchanged'}
        remote.oom_counters.return_value={'trino':{'oom':0,'oom_kill':0}}
        remote.metrics.return_value={'trino':{'memory_bytes':100,'cpu_percent':1}}
        def query(sql,concurrency):
            count=self.ev['tables']['event_current']['count']
            rows=[[count,42,100.0,0.0,9.0]] if 'count(*), count(magnitude)' in sql else [[2000,1,count,100.0]]
            return {'state':'FINISHED','wall_seconds':1,'rows':rows,'query_id':'fixture','engine':{}}
        remote.query_profile.side_effect=query
        return remote

    def mock_run(self,root,remote):
        from contextlib import ExitStack
        stack=ExitStack()
        stack.enter_context(patch.object(q.shutil,'disk_usage',return_value=Mock(free=10*1024**3)))
        stack.enter_context(patch.object(q,'configuration',return_value=(self.c,self.ev,'c'*64)))
        stack.enter_context(patch.object(q,'table_identity',return_value={'pin':'unchanged'}))
        stack.enter_context(patch.object(q,'file_profile',return_value={'files':'fixture'}))
        return stack

    def test_complete_suite_and_checkpoints_without_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve(); remote=self.fake_runtime()
            with self.mock_run(root,remote):
                result=q.run(root,self.config_path,root/'staging/new',remote)
            self.assertEqual(result['status'],'PROFILE_ACCEPTED')
            self.assertEqual(remote.query_profile.call_count,14)
            self.assertEqual(len(list((root/'staging/new').glob('query-*.json'))),14)
            self.assertFalse(result['lake_writes'])
            self.assertTrue((root/'staging/new/receipt.json').is_file())

    def test_failure_keeps_checkpoint_and_does_not_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve(); remote=self.fake_runtime()
            remote.query_profile.side_effect=ValueError('QA05_QUERY_FAILED_NO_RETRY')
            with self.mock_run(root,remote),self.assertRaises(ValueError):
                q.run(root,self.config_path,root/'staging/new',remote)
            self.assertEqual(remote.query_profile.call_count,1)
            self.assertTrue((root/'staging/new/intent.json').is_file())
            self.assertTrue((root/'staging/new/failed.json').is_file())
            self.assertFalse((root/'staging/new/receipt.json').exists())

    def test_metadata_drift_blocks_acceptance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();remote=self.fake_runtime()
            with self.mock_run(root,remote),patch.object(q,'table_identity',side_effect=[{'pin':1},{'pin':2}]):
                with self.assertRaisesRegex(ValueError,'METADATA_OR_SNAPSHOT_CHANGED'):
                    q.run(root,self.config_path,root/'staging/new',remote)
            self.assertFalse((root/'staging/new/receipt.json').exists())

    def test_new_oom_blocks_acceptance(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();remote=self.fake_runtime()
            remote.oom_counters.side_effect=[{'oom':0},{'oom':1}]
            with self.mock_run(root,remote),self.assertRaisesRegex(ValueError,'SERVICE_OR_OOM_CHANGED'):
                q.run(root,self.config_path,root/'staging/new',remote)
            self.assertFalse((root/'staging/new/receipt.json').exists())


if __name__=='__main__': unittest.main()
