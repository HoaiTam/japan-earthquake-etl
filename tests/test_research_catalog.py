"""Catalog control-plane regression. All Docker/ETL calls are mocked or forbidden."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import types
import zipfile
import unittest
from unittest.mock import patch, Mock

ROOT = Path(__file__).resolve().parents[1]
def script(name):
    s = importlib.util.spec_from_file_location(name.replace("-", "_"), ROOT / "scripts" / (name + ".py"))
    m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
c = script("research-catalog"); r = script("research-rollout")


class ResearchCatalogTest(unittest.TestCase):
    def test_catalog_overlay_keeps_parser_and_ml_bytes_from_frozen_base(self):
        base="ie212/earthquake/spark/"
        changed={base+n+".class" for n in ("usgs/ResearchCatalogJob","usgs/ResearchCatalogState","orchestration/FullMonthBronzeReader",
                 "orchestration/FullMonthStateResolver","orchestration/RealEtlAdapter","gold/GoldPublisher","gold/GoldVerificationPlan")}
        added={base+n+".class" for n in ("usgs/ResearchCatalogBatchPlan","usgs/ResearchCatalogCompatibility","orchestration/FullMonthBronzeReader$JmaStructureCache")}
        parser=base+"silver/CanonicalObservationTransformer.class";ml=base+"ml/ResultBundleValidator.class"
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve();original=root/"original.jar";compiled=root/"compiled.jar";target=root/"catalog.jar"
            entries={name:("original:"+name).encode() for name in changed|{parser,ml}}
            with zipfile.ZipFile(original,"w") as z:
                for name,data in entries.items():z.writestr(name,data)
            current={**entries,parser:b"different-parser",ml:b"different-ml",base+"ml/NewCollection.class":b"new-ml"}
            current[base+"gold/GoldPublisher.class"]=b"reviewed-catalog-change"
            current.update({name:b"new-catalog" for name in added})
            with zipfile.ZipFile(compiled,"w") as z:
                for name,data in current.items():z.writestr(name,data)
            c.overlay_catalog(original,compiled,target)
            with zipfile.ZipFile(target) as z:
                self.assertEqual(entries[parser],z.read(parser));self.assertEqual(entries[ml],z.read(ml))
                self.assertNotIn(base+"ml/NewCollection.class",z.namelist())
                self.assertEqual(b"reviewed-catalog-change",z.read(base+"gold/GoldPublisher.class"))
            proof=c.compatibility(original,target)
            self.assertEqual(1,len(proof["changed_entries"]));self.assertEqual(3,len(proof["added_entries"]))
            with self.assertRaises(FileExistsError):c.overlay_catalog(original,compiled,target)

    def report(self):
        old = {"tables": {t: {"snapshot_id": 9007199254740993, "count": 105282} for t in c.TABLES}}
        new = {"scope": {"baseline_snapshots": {t: 9007199254740993 for t in c.TABLES}, "affected_months": ["2023-12"]},
               "tables": {t: {"snapshot_id": 9007199254740997} for t in c.TABLES}}
        return {"namespace": "iceberg.dat03_e2e_" + "a"*32,
                "cases": [{"first": {"commit": old}}]*3 + [{"first": {"commit": new}}]}

    def test_exact_source_and_transfer_file_sets_are_bounded(self):
        self.assertEqual(37, len(c.NAMES)); self.assertEqual(119, len(c.files()))
        self.assertEqual(288, len(c.module_months())); self.assertEqual("2018-10", c.module_months()[225])
        for name in ("h1999", "h2025", "../../.env", "usgs-2024-01"):
            with self.assertRaises(RuntimeError): c.source_files(name)
        self.assertNotIn(".env",c.files());self.assertNotIn("docker",c.TRANSFER)
        self.assertIn('p.mkdir(mode=0o700)',c.TRANSFER);self.assertNotIn("extract",c.TRANSFER)

    def test_capacity_generator_matches_canonical_generator_for_both_tables_and_all_buckets(self):
        report=self.report()
        for copy_count in (1,32,43,64):
            for table in c.TABLES:
                actual=c.expected_queries(report,copy_count,table)
                expected=[r.capacity_sql(dict(report,cases=[dict(row,month=month) for row,month in zip(report["cases"],("2000-01","2011-04","2016-04","2023-12"))]),copy_count,b,table)[0] for b in "0123456789abcdef"]
                self.assertEqual(expected,actual)

    def capacity_fixture(self,folder):
        (folder/"capacity").mkdir();report=self.report();c.e.save(folder/"bounded-report.json",report)
        request={"trino_capacity":[]}
        for table in c.TABLES:
            q={"version":"dat03-trino-stress-v2","table":table,"strategy":"buckets-v1","query_max_run_time_seconds":90,
               "total_probe_budget_seconds":2400,"report_sha256":c.e.sha(folder/"bounded-report.json"),"copies":43,"stress_rows_per_side":4527126,
               "queries":c.expected_queries(report,43,table)}
            c.e.save(folder/f"capacity/{table}-request.json",q)
            proof={"version":q["version"],"table":table,"strategy":"buckets-v1","status":"STRESS_PROBE_PASSED",
                   "request_sha256":c.e.sha(folder/f"capacity/{table}-request.json"),"required_query_count":16,"completed_query_count":16,
                   "postflight_status":"PASSED_UNCHANGED","services_before":{"exact":1},"services_after":{"exact":1},"data_mutations":False,
                   "lease_acquired_or_released":False,"full_capacity_accepted":False,
                   "queries":[{"number":i,"exit_code":0,"status":"STRESS_PROBE_PASSED"} for i in range(16)]}
            c.e.save(folder/f"capacity/{table}-receipt.json",proof)
            for i in range(16):c.e.save(folder/f"capacity/{table}-{i:02d}-stdout.private",{"changed_rows":0})
            request["trino_capacity"].append({"table":table,"request_sha256":c.e.sha(folder/f"capacity/{table}-request.json"),
                                             "receipt_sha256":c.e.sha(folder/f"capacity/{table}-receipt.json")})
        return request,{"splits":{"REPRODUCTION":{"profile_accepted_rows":3127011},"EXTENSION":{"profile_accepted_rows":1361003}}}, {"sum_response_features_including_halo_duplicates":1809}

    def test_real_scale_requires_both_tables_all_buckets_and_actual_integral_zero_readbacks(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp).resolve();request,plan,summary=self.capacity_fixture(folder)
            self.assertEqual(4489823,c.capacity(folder,request,plan,summary))
            file=folder/"capacity/event_source_bridge-15-stdout.private";file.write_text('{"changed_rows":true}')
            with self.assertRaisesRegex(RuntimeError,"READBACK_CHANGED"):c.capacity(folder,request,plan,summary)

    def test_old_32copy_probe_is_not_sufficient_for_full_profile_even_when_it_passed(self):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp).resolve();request,plan,summary=self.capacity_fixture(folder)
            q=folder/"capacity/event_current-request.json";data=c.load(q);data.update(copies=32,stress_rows_per_side=3369024,queries=c.expected_queries(self.report(),32,"event_current"));q.write_text(json.dumps(data))
            request["trino_capacity"][0]["request_sha256"]=c.e.sha(q)
            with self.assertRaisesRegex(RuntimeError,"WORKLOAD_INSUFFICIENT"):c.capacity(folder,request,plan,summary)

    def test_partial_forged_or_changed_probe_never_opens_rollout_gate(self):
        for kind in ("prefix","restart","memory","relaxed-time","sql"):
            with tempfile.TemporaryDirectory() as temp:
                folder=Path(temp).resolve();request,plan,summary=self.capacity_fixture(folder)
                receipt=folder/"capacity/event_current-receipt.json";proof=c.load(receipt)
                q=folder/"capacity/event_current-request.json";query=c.load(q)
                if kind=="prefix":proof["queries"].pop();proof["completed_query_count"]=15
                elif kind=="restart":proof["services_after"]={"new":1}
                elif kind=="memory":proof["status"]="BLOCKED_MEMORY"
                elif kind=="relaxed-time":query["query_max_run_time_seconds"]=300
                else:query["queries"][0]="SELECT 0"
                q.write_text(json.dumps(query));proof["request_sha256"]=c.e.sha(q);receipt.write_text(json.dumps(proof))
                request["trino_capacity"][0].update(request_sha256=c.e.sha(q),receipt_sha256=c.e.sha(receipt))
                with self.assertRaises(RuntimeError):c.capacity(folder,request,plan,summary)

    def test_worker_rejects_foreign_root_or_invalid_limits_before_any_process_or_lease(self):
        with patch.object(c.subprocess,"Popen",side_effect=AssertionError("no process")):
            for root,seconds,months in ((Path("/tmp/foreign"),6600,1),(Path("/opt/pipeline/staging/dat03-catalog/"+"a"*32),7200,1),
                                       (Path("/opt/pipeline/staging/dat03-catalog/"+"a"*32),6600,True)):
                with self.assertRaises(RuntimeError):c.worker(root,seconds,months)

    def test_strict_json_symlink_and_metadata_bounds(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve();p=root/"file.json"
            for value in ('{"pin":1,"pin":2}','{"pin":NaN}'):
                p.write_text(value)
                with self.assertRaises((ValueError,RuntimeError)):c.load(p)
            p.write_text('{"ok":1}');(root/"link.json").symlink_to(p)
            with self.assertRaises(RuntimeError):c.load(root/"link.json")
            with self.assertRaises(RuntimeError):c.load(p,4)

    def trial(self, outcome="success"):
        with tempfile.TemporaryDirectory() as temp:
            folder=Path(temp).resolve();c.e.save(folder/"request.json",{"fixture":True})
            deps=folder/"deps";deps.mkdir()
            for name in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar","iceberg-aws-bundle-1.10.1.jar"):(deps/name).write_bytes(b"fixture-dependency")
            request={"version":c.VERSION,"run_id":"dat03-catalog-"+"a"*32,"runner_sha256":"b"*64}
            def lease(action,owner):return {"status":"ACQUIRED" if action=="acquire" else "RELEASED",**owner}
            lease_call=Mock(side_effect=lease)
            def spawn(command,**kwargs):
                self.assertTrue(kwargs["start_new_session"]);session=Path(command[-3])
                self.assertEqual(c.CHECK,command[command.index("--class")+1])
                self.assertNotIn("docker",command);self.assertNotIn("build",command)
                result={"pending_month":None,"request_identity_sha256":c.e.digest(request),"completed_months":0,"months":[],
                        "current_snapshots":{t:0 for t in c.e.TABLES},"status":"CHECKPOINTED","full_research_catalog":False,
                        "official_exported":False,"research_ready":False}
                c.e.save(session/"result.json",result)
                wait=Mock(side_effect=[subprocess.TimeoutExpired(command,1),-15] if outcome=="timeout" else [1 if outcome=="failure" else 0])
                return types.SimpleNamespace(wait=wait,pid=12345)
            original_path=c.Path
            def path(value):return deps if str(value)=="/opt/spark/jobs/iceberg" else original_path(value)
            with patch.object(c,"Path",side_effect=path),patch.object(c.re,"fullmatch",return_value=True),patch.object(c,"validate",return_value=request), \
                    patch.object(c,"quiet") as quiet,patch.object(c.subprocess,"Popen",side_effect=spawn), \
                    patch.object(c.shutil,"disk_usage",return_value=types.SimpleNamespace(free=10*1024**3)), \
                    patch.object(c.os,"killpg") as kill,patch.dict("sys.modules",{"source_run_guard":types.SimpleNamespace(lease=lease_call)}):
                if outcome=="success":c.worker(folder,6000,1)
                else:
                    with self.assertRaisesRegex(RuntimeError,"FAILED_LEASE_RETAINED"):c.worker(folder,6000,1)
                self.assertEqual(["acquire","release"] if outcome=="success" else ["acquire"],[call.args[0] for call in lease_call.call_args_list])
                self.assertEqual(outcome=="timeout",kill.called)
                self.assertEqual(2 if outcome=="success" else 1,quiet.call_count)
            sessions=list((folder/"sessions").iterdir());self.assertEqual(1,len(sessions))
            self.assertEqual(outcome=="success",(sessions[0]/"acceptance.json").exists())
            self.assertTrue((sessions[0]/"execution.json").is_file())

    def test_successful_quiescent_checkpoint_releases_only_own_lease(self):self.trial()
    def test_java_nonzero_retains_lease_and_logs_without_retry(self):self.trial("failure")
    def test_timeout_terminates_only_owned_process_group_and_never_releases_lease(self):self.trial("timeout")

    def test_invalid_host_session_limits_cannot_contact_docker(self):
        with patch.object(c,"module",side_effect=AssertionError("no Docker/module load")):
            for seconds,months in ((True,1),(7200,1),(6000,0),(6000,True)):
                with self.assertRaises(RuntimeError):c.run(ROOT,ROOT,seconds,months)

    def test_continuation_keeps_all_original_parser_canonical_and_dependency_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve();original=root/"old.jar";current=root/"new.jar"
            base="ie212/earthquake/spark/"
            with zipfile.ZipFile(original,"w") as z:
                z.writestr(base+"usgs/ResearchCatalogJob.class",b"old")
                z.writestr(base+"gold/GoldInputReader.class",b"unchanged")
            with zipfile.ZipFile(current,"w") as z:
                z.writestr(base+"usgs/ResearchCatalogJob.class",b"new")
                z.writestr(base+"gold/GoldInputReader.class",b"unchanged")
                z.writestr(base+"usgs/ResearchCatalogBatchPlan.class",b"batch")
            result=c.compatibility(original,current)
            self.assertEqual(1,result["matched_original_entries"]);self.assertEqual(1,len(result["changed_entries"]));self.assertEqual(1,len(result["added_entries"]))
            with zipfile.ZipFile(current,"w") as z:
                z.writestr(base+"usgs/ResearchCatalogJob.class",b"new")
                z.writestr(base+"gold/GoldInputReader.class",b"changed")
            with self.assertRaisesRegex(RuntimeError,"UNREVIEWED_DATA_PLANE_CHANGE"):c.compatibility(original,current)

    def test_continuation_transfer_is_exact_new_metadata_not_source_or_deployment_replacement(self):
        self.assertIn("dat03-catalog-continuation",c.CONTINUATION_TRANSFER)
        self.assertIn('p.mkdir(mode=0o700)',c.CONTINUATION_TRANSFER)
        self.assertIn('len(files)==5',c.CONTINUATION_TRANSFER)
        self.assertNotIn("inputs/",c.CONTINUATION_TRANSFER)
        for word in ("compose", "restart", "extractall", "unlink", "rmtree"):self.assertNotIn(word,c.CONTINUATION_TRANSFER)


if __name__=="__main__":unittest.main()
