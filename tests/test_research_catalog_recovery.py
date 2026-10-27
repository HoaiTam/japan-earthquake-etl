"""Offline safety tests. No Docker, SQL or ETL process is permitted."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch, Mock
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("catalog_recovery", ROOT / "scripts/research-catalog-recovery.py")
r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)


class CatalogRecoveryTest(unittest.TestCase):
    def test_exact_failed_evidence_set_and_contiguous_targets(self):
        names = r.failure_names("a"*32, 101, r.c.module_months()[101:107])
        self.assertEqual(12, len(names))
        self.assertIn("months/2008-06/gold.json", names)
        self.assertIn("months/2008-05/checkpoint.json", names)
        self.assertNotIn("months/2008-06/verify.json", names)
        for initial, targets in ((True,["2000-02"]), (0,["2000-01"]), (101,["2008-07"]), (101,["../.env"]), (287,["2023-12","2024-01"])):
            with self.assertRaises(RuntimeError): r.failure_names("a"*32, initial, targets)

    def test_only_added_recovery_class_no_transport_or_etl_replacement(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve(); old = root/"base.jar"; new = root/"new.jar"
            entries = {"engine.class":b"same", "gold/TrinoSqlClient.class":b"same", "META-INF/MANIFEST.MF":b"same"}
            with zipfile.ZipFile(old,"w") as z:
                for name, data in entries.items(): z.writestr(name,data)
            for mutation in (None, "engine.class", "gold/TrinoSqlClient.class", "META-INF/MANIFEST.MF", "extra.class", "missing"):
                changed = {**entries, r.ENTRY:b"new"}
                if mutation == "missing": changed.pop("engine.class")
                elif mutation: changed[mutation] = b"different"
                with zipfile.ZipFile(new,"w") as z:
                    for name, data in changed.items(): z.writestr(name,data)
                if mutation is None: r.compatible(old,new)
                else:
                    with self.assertRaises(RuntimeError): r.compatible(old,new)

    def fixture(self, root):
        failed = "a"*32; recovered = "b"*32
        prior = root/"sessions"/failed; prior.mkdir(parents=True)
        accepted = root/"sessions"/recovered; accepted.mkdir()
        for name in ("intent.json","stdout.private","stderr.private"): (prior/name).write_text("preserve")
        r.e.save(prior/"execution.json", {"exit_code":1,"timed_out":False})
        r.e.save(root/"request.json", {"run_id":"run"})
        r.e.save(accepted/"execution.json", {"exit_code":0,"timed_out":False})
        r.e.save(accepted/"result.json", {"recovered_session":failed,"recovery_request_sha256":"e"*64})
        acceptance = {"status":"CHECKPOINT_ACCEPTED","recovered_session":failed,"recovery_request_sha256":"e"*64,
                      "request_sha256":r.e.sha(root/"request.json"), "result_sha256":r.e.sha(accepted/"result.json"),
                      "execution_sha256":r.e.sha(accepted/"execution.json"),
                      "lease_release":{"status":"RELEASED","dag_id":r.c.DAG,"run_id":"run"}}
        r.e.save(accepted/"acceptance.json", acceptance)
        pins = {str(p.relative_to(root)):r.e.sha(p) for p in prior.iterdir()}
        proof = {"version":r.LEGACY_VERSION,"recovered_session":failed,"recovery_session":recovered,
                 "recovery_request_sha256":"e"*64,"acceptance_sha256":r.e.sha(accepted/"acceptance.json"),"failed_files":pins}
        (root/"session-resolutions").mkdir(); r.e.save(root/"session-resolutions"/(failed+".json"), proof)
        return prior, accepted, proof

    def test_failed_execution_can_only_be_resolved_by_separate_accepted_recovery(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve(); prior, accepted, proof = self.fixture(root)
            before = {p.name:p.read_bytes() for p in prior.iterdir()}
            with patch.object(r.c,"session_result",return_value=r.c.load(accepted/"result.json")):
                r.c.resolved_failure(root,{"run_id":"run"},prior)
            self.assertEqual(before,{p.name:p.read_bytes() for p in prior.iterdir()})
            self.assertFalse((prior/"acceptance.json").exists())

    def test_unresolved_changed_failure_or_unreleased_recovery_is_rejected(self):
        for mutation in ("missing-proof", "failed-log", "missing-failure-pin", "foreign-release", "failed-recovery", "wrong-result"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve(); prior, accepted, proof = self.fixture(root)
                resolution = root/"session-resolutions"/(prior.name+".json")
                result = r.c.load(accepted/"result.json")
                if mutation == "missing-proof": resolution.unlink()
                elif mutation == "failed-log": (prior/"stderr.private").write_text("changed")
                elif mutation == "missing-failure-pin":
                    proof["failed_files"].pop(str((prior/"execution.json").relative_to(root))); resolution.write_text(json.dumps(proof))
                elif mutation == "foreign-release":
                    data = r.c.load(accepted/"acceptance.json"); data["lease_release"]["run_id"] = "foreign"; (accepted/"acceptance.json").write_text(json.dumps(data))
                elif mutation == "failed-recovery": (accepted/"execution.json").write_text('{"exit_code":1,"timed_out":false}')
                else: result["recovered_session"] = "c"*32
                with patch.object(r.c,"session_result",return_value=result), self.assertRaises((RuntimeError,FileNotFoundError)):
                    r.c.resolved_failure(root,{"run_id":"run"},prior)

    def test_changed_failed_bytes_never_reach_runtime(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve(); (root/"input.json").write_text("original")
            request = {"failed_files":{"input.json":r.e.sha(root/"input.json")}}
            r.unchanged(root,request); (root/"input.json").write_text("changed")
            with self.assertRaisesRegex(RuntimeError,"FAILED_EVIDENCE_CHANGED"): r.unchanged(root,request)

    def test_v2_transport_exception_is_narrow_and_related_failures_are_explicit(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve();old=root/'base.jar';new=root/'new.jar'
            entries={r.TRANSPORT:b'old-client','publisher.class':b'unchanged'}
            with zipfile.ZipFile(old,'w') as z:
                for k,v in entries.items():z.writestr(k,v)
            with zipfile.ZipFile(new,'w') as z:
                for k,v in {**entries,r.TRANSPORT:b'new-client',r.ENTRY:b'recovery'}.items():z.writestr(k,v)
            with self.assertRaises(RuntimeError):r.compatible(old,new)
            r.compatible(old,new,r.SQL_PROFILE)
            with self.assertRaises(RuntimeError):r.compatible(old,new,'unbounded')
            with zipfile.ZipFile(new,'w') as z:
                for k,v in {**entries,r.TRANSPORT:b'new-client','publisher.class':b'changed',r.ENTRY:b'recovery'}.items():z.writestr(k,v)
            with self.assertRaises(RuntimeError):r.compatible(old,new,r.SQL_PROFILE)
        names=r.failure_names('a'*32,113,r.c.module_months()[113:119],['d'*32])
        self.assertEqual(16,len(names));self.assertIn('sessions/'+'d'*32+'/execution.json',names)
        for related in (['a'*32],['d'*32,'d'*32],['../bad'],['b'*32,'c'*32,'d'*32]):
            with self.assertRaises(RuntimeError):r.failure_names('a'*32,113,r.c.module_months()[113:119],related)

    def test_v2_same_accepted_recovery_resolves_two_pinned_failures_without_rewriting_them(self):
        for tamper in (None,'missing-secondary-pin','changed-secondary','different-resolved-set'):
            with self.subTest(tamper=tamper), tempfile.TemporaryDirectory() as temp:
                root=Path(temp).resolve();prior,accepted,proof=self.fixture(root)
                other=root/'sessions'/('d'*32);other.mkdir()
                for name in ('intent.json','stdout.private','stderr.private'):(other/name).write_text('retained')
                r.e.save(other/'execution.json',{'exit_code':1,'timed_out':False})
                ids=[prior.name,other.name]
                result=r.c.load(accepted/'result.json');result['resolved_failed_sessions']=ids
                (accepted/'result.json').write_text(json.dumps(result))
                acceptance=r.c.load(accepted/'acceptance.json');acceptance['resolved_failed_sessions']=ids;acceptance['result_sha256']=r.e.sha(accepted/'result.json')
                (accepted/'acceptance.json').write_text(json.dumps(acceptance))
                proof.update(version=r.VERSION,primary_failed_session=prior.name,resolved_failed_sessions=ids,acceptance_sha256=r.e.sha(accepted/'acceptance.json'))
                proof['failed_files'].update({str(p.relative_to(root)):r.e.sha(p) for p in other.iterdir()})
                if tamper=='missing-secondary-pin':proof['failed_files'].pop(str((other/'execution.json').relative_to(root)))
                if tamper=='changed-secondary':(other/'stderr.private').write_text('changed')
                if tamper=='different-resolved-set':result['resolved_failed_sessions']=[prior.name]
                for previous in (prior,other):
                    pin={**proof,'recovered_session':previous.name}
                    (root/'session-resolutions'/(previous.name+'.json')).write_text(json.dumps(pin))
                    with patch.object(r.c,'session_result',return_value=result):
                        if tamper:
                            with self.assertRaises(RuntimeError):r.c.resolved_failure(root,{'run_id':'run'},previous)
                        else:r.c.resolved_failure(root,{'run_id':'run'},previous)
                self.assertFalse((prior/'acceptance.json').exists());self.assertFalse((other/'acceptance.json').exists())

    def test_failed_java_keeps_lease_and_never_creates_validation_or_acceptance(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve(); attempt = root/"attempt"; attempt.mkdir()
            catalog = root/"catalog"; (catalog/"sessions").mkdir(parents=True)
            continuation = root/"continuation"; continuation.mkdir()
            (continuation/"continuation-request.json").write_text("pinned")
            request = {"catalog_root":str(catalog),"continuation_root":str(continuation),"recovery_session":"b"*32,
                       "failed_session":"a"*32,"continuation_request_sha256":r.e.sha(continuation/"continuation-request.json"),"base_runner_sha256":"c"*64}
            (attempt/"recovery-request.json").write_text("pinned")
            r.e.save(attempt/"preflight.json",{"status":"READY_FOR_PUBLICATION_RECOVERY","checks":dict.fromkeys(map(str,range(13)),0)})
            child = Mock(); child.wait.return_value = 1
            def validated(path, catalog=None): return (request,{"run_id":"run"}) if catalog else request
            with patch.object(r,"validate",side_effect=validated), patch.object(r.c,"continuation_inputs",return_value={"runner_sha256":"c"*64}), \
                 patch.object(r,"compatible"), patch.object(r,"unchanged"), patch.object(r.c,"quiet"), \
                 patch.object(r.shutil,"disk_usage",return_value=Mock(free=10*1024**3)), patch.object(r.subprocess,"Popen",return_value=child), \
                 self.assertRaisesRegex(RuntimeError,"RECOVERY_FAILED_LEASE_RETAINED"):
                r.worker(attempt,"run")
            session = catalog/"sessions"/("b"*32)
            self.assertEqual(1,r.c.load(session/"execution.json")["exit_code"])
            self.assertFalse((session/"validation.json").exists()); self.assertFalse((session/"acceptance.json").exists())
            self.assertFalse((catalog/"session-resolutions").exists())
            child.wait.assert_called_once()

    def test_changed_service_blocks_finalize_and_latest_acceptance(self):
        with tempfile.TemporaryDirectory() as temp:
            deployment = Path(temp).resolve(); attempt = deployment/"staging/dat03-catalog-recovery"/("a"*32); attempt.mkdir(parents=True)
            (attempt/"recovery-request.json").write_text("pinned")
            request = {"catalog_root":"/opt/pipeline/staging/dat03-catalog/"+"b"*32}
            rollout = Mock(); rollout.services.side_effect = [{"started":"before"},{"started":"changed"}]
            store = Mock(); store.capture.return_value = (None,{})
            with patch.object(r,"validate",return_value=request), patch.object(r.c,"module",return_value=rollout), \
                 patch.object(r.c,"observations",return_value=store), patch.object(r.subprocess,"run") as command, \
                 patch.object(r.shutil,"disk_usage",return_value=Mock(free=10*1024**3)), \
                 self.assertRaisesRegex(RuntimeError,"SERVICES_CHANGED_LEASE_RETAINED"):
                r.run(deployment,attempt,"run")
            self.assertEqual(1,command.call_count)
            self.assertEqual("run",command.call_args.args[0][-1]);store.publish_latest.assert_not_called()

    def test_v2_missing_or_false_slow_query_probe_never_starts_recovery_java(self):
        for value in (None,True,1):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as temp:
                root=Path(temp).resolve();attempt=root/'attempt';attempt.mkdir();catalog=root/'catalog';(catalog/'sessions').mkdir(parents=True)
                control=root/'control';control.mkdir();(control/'continuation-request.json').write_text('pinned')
                request={'version':r.VERSION,'catalog_root':str(catalog),'continuation_root':str(control),'recovery_session':'b'*32,
                         'failed_session':'a'*32,'continuation_request_sha256':r.e.sha(control/'continuation-request.json'),
                         'base_runner_sha256':'c'*64,'sql_read_profile':r.SQL_PROFILE}
                proof={'status':'READY_FOR_PUBLICATION_RECOVERY','checks':dict.fromkeys(map(str,range(13)),0),'sql_read_profile':r.SQL_PROFILE}
                if value is not None:proof['sql_read_probe']={'primary_provenance:2':{'actual':value,'expected':0}}
                r.e.save(attempt/'preflight.json',proof)
                def validated(path,catalog=None):return (request,{'run_id':'run'}) if catalog else request
                with patch.object(r,'validate',side_effect=validated),patch.object(r.c,'continuation_inputs',return_value={'runner_sha256':'c'*64}), \
                        patch.object(r,'compatible'),patch.object(r,'unchanged'),patch.object(r.c,'quiet'), \
                        patch.object(r.shutil,'disk_usage',return_value=Mock(free=10*1024**3)),patch.object(r.subprocess,'Popen') as child, \
                        self.assertRaisesRegex(RuntimeError,'RECOVERY_SQL_READ_PROBE_REQUIRED'):
                    r.worker(attempt,'run')
                child.assert_not_called();self.assertFalse((catalog/'sessions'/('b'*32)).exists())

    def test_recovery_has_no_spark_writer_or_service_lifecycle_calls(self):
        source = (ROOT/"spark/src/main/java/ie212/earthquake/spark/usgs/ResearchCatalogRecoveryJob.java").read_text()
        self.assertNotIn(".getOrCreate(",source); self.assertNotIn("spark-submit",Path(r.__file__).read_text())
        for forbidden in (".resolve(spark", "gold.commit(", "new JmaBronzeWriter", "new UsgsBronzeWriter", "lease(\"acquire\"", '"docker", "start"', '"docker", "restart"'):
            self.assertNotIn(forbidden,source+Path(r.__file__).read_text())
        self.assertIn('before == after',Path(r.__file__).read_text())
        self.assertIn('publisher.verify(',source); self.assertIn('publisher.publish(',source)


if __name__ == "__main__": unittest.main()
