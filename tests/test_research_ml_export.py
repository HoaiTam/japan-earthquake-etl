"""Synthetic control-plane fixtures only. Never a real full-catalog/ML acceptance."""
import importlib.util
import json
import os
from pathlib import Path
import re
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

ROOT=Path(__file__).resolve().parents[1]
SPEC=importlib.util.spec_from_file_location("research_ml_export_test",ROOT/"scripts/research-ml-export.py")
m=importlib.util.module_from_spec(SPEC);SPEC.loader.exec_module(m)
c=m.support()


class ResearchMlExportTest(unittest.TestCase):
    def test_transfer_contains_only_eight_pinned_inputs_not_credentials(self):
        self.assertEqual(8,len(m.FILES));self.assertEqual(8,len(set(m.FILES)))
        for forbidden in (".env","credential","restart","extractall","rmtree","compose"):
            self.assertNotIn(forbidden,m.TRANSFER)

    def test_invalid_budget_stops_before_docker_or_lease(self):
        for value in (True,3600,7200):
            with patch.object(m,"support",return_value=c),patch.object(c,"load",side_effect=AssertionError("no read")):
                with self.assertRaisesRegex(RuntimeError,"EXACT_SESSION"):
                    m.worker(Path("/opt/pipeline/staging/dat03-ml-export/"+"a"*32),value)

    def test_partial_catalog_blocks_prepare_before_directory_or_docker(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve();catalog=root/"staging/dat03-catalog"/("a"*32)
            observation=catalog/"runtime";observation.mkdir(parents=True)
            for folder in (catalog,observation):c.e.save(folder/"request.json",{"fixture":True})
            with patch.object(m,"support",return_value=c),patch.object(c,"validate"), \
                    patch.object(m,"full_catalog",side_effect=RuntimeError("RESEARCH_EXPORT_FULL_288_MONTH_ACCEPTANCE_REQUIRED")), \
                    patch.object(m.subprocess,"run",side_effect=AssertionError("no Docker")):
                with self.assertRaisesRegex(RuntimeError,"FULL_288"):
                    m.prepare(root,catalog,observation,observation/"sessions"/("b"*32),"REPRODUCTION")
            self.assertFalse((root/"staging/dat03-ml-export").exists())

    def test_low_physical_host_disk_blocks_resume_before_inputs_or_docker(self):
        with tempfile.TemporaryDirectory() as temp:
            deployment=Path(temp).resolve();root=deployment/"staging/dat03-ml-export"/("a"*32)
            with patch.object(m,"support",return_value=c),patch.object(c,"load",side_effect=AssertionError("no inputs read")), \
                    patch.object(m.shutil,"disk_usage",return_value=types.SimpleNamespace(free=4*1024**3-1)):
                with self.assertRaisesRegex(RuntimeError,"HOST_DISK_RESERVE"):
                    m.run(deployment,root,6000,True)

    def test_full_catalog_receipt_rejects_boolean_exit_code_and_partial_count(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve();session=root/"sessions"/("a"*32);session.mkdir(parents=True)
            request={"run_id":"fixture"};c.e.save(root/"request.json",request)
            c.e.save(session/"result.json",{"fixture":True});c.e.save(session/"execution.json",{"exit_code":True,"timed_out":False})
            pin={"status":"CHECKPOINT_ACCEPTED","request_sha256":c.e.sha(root/"request.json"),
                 "result_sha256":c.e.sha(session/"result.json"),"execution_sha256":c.e.sha(session/"execution.json"),
                 "lease_release":{"status":"RELEASED","dag_id":c.DAG,"run_id":"fixture"}}
            c.e.save(session/"acceptance.json",pin)
            result={"completed_months":288,"full_research_catalog":True,"pending_month":None}
            with patch.object(m,"support",return_value=c),patch.object(c,"validate",return_value=request),patch.object(c,"session_result",return_value=result):
                with self.assertRaisesRegex(RuntimeError,"FULL_288"):m.full_catalog(root,session)

    def test_foreign_session_cannot_be_rebound_to_a_valid_local_session_name(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve();catalog=root/"catalog";session=root/"foreign/sessions"/("a"*32)
            with self.assertRaisesRegex(RuntimeError,"EXACT_CATALOG_SESSION"):
                m.full_catalog(catalog,session)

    def test_prepare_rejects_foreign_session_before_rebinding_or_creating_attempt(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp).resolve();catalog=root/"staging/dat03-catalog"/("a"*32)
            observation=catalog/"runtime";observation.mkdir(parents=True)
            session=root/"foreign/sessions"/("b"*32)
            with patch.object(m,"support",return_value=c),patch.object(c,"validate",side_effect=AssertionError("no input reads")):
                with self.assertRaisesRegex(RuntimeError,"EXACT_CATALOG_SESSION"):
                    m.prepare(root,catalog,observation,session,"REPRODUCTION")
            self.assertFalse((root/"staging/dat03-ml-export").exists())

    def trial(self,outcome="checkpoint",held=None):
        with tempfile.TemporaryDirectory() as temp:
            base=Path(temp).resolve();root=base/"export"/("a"*32);root.mkdir(parents=True)
            catalog=base/"catalog"/("b"*32);source=catalog/"sessions"/("c"*32);source.mkdir(parents=True)
            c.e.save(catalog/"request.json",{"fixture":True});c.e.save(source/"acceptance.json",{"fixture":True})
            (root/"runner.jar").write_bytes(b"fixture-code");(root/"feature-scaling.json").write_bytes(b"{}")
            request={"version":m.VERSION,"catalog_root":str(catalog),"catalog_request_sha256":c.e.sha(catalog/"request.json"),
                     "runner_sha256":c.e.sha(root/"runner.jar"),"scaling_sha256":c.e.sha(root/"feature-scaling.json"),
                     "dataset_split":"REPRODUCTION","observation_cutoff_utc":"2026-10-25T00:00:00Z"}
            c.e.save(root/"request.json",request)
            c.e.save(root/"catalog-acceptance-pin.json",{"session":source.name,"catalog_request_sha256":request["catalog_request_sha256"],
                                                     "acceptance_sha256":c.e.sha(source/"acceptance.json")})
            guard=base/"guard";guard.mkdir();owner={"dag_id":"mli_02_build_dataset","run_id":"dat03-collection-"+root.name}
            if held is not None:c.e.save(guard/"owner.json",owner if held=="own" else {"dag_id":"foreign","run_id":"other"})
            deps=base/"deps";deps.mkdir()
            if outcome!="missing-deps":
                for name in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar","iceberg-aws-bundle-1.10.1.jar"):(deps/name).write_bytes(b"dependency")
            def lease(action,pin):
                self.assertEqual(owner,pin)
                if action=="acquire":c.e.save(guard/"owner.json",pin)
                else:(guard/"owner.json").unlink()
                return {"status":"ACQUIRED" if action=="acquire" else "RELEASED",**pin}
            lease_call=Mock(side_effect=lease)
            def child(command,out,err,seconds):
                self.assertEqual(6120,seconds);self.assertIn("local[1]",command)
                if outcome in ("failure","timeout"):return (1,outcome=="timeout")
                reports=root/"sessions";reports.mkdir(exist_ok=True)
                exported=outcome in ("exported","bad-handoff","bad-count")
                report={"version":m.VERSION,"status":"DATASET_EXPORTED" if exported else "CHECKPOINTED",
                        "request_sha256":c.e.sha(root/"request.json"),"runner_sha256":request["runner_sha256"],"catalog_completed_months":288,
                        "dataset_split":"REPRODUCTION","dataset_id":"ds_"+"d"*64,"required_steps":10,"completed_steps":10 if exported else 3,
                        "synthetic_component_qa":False,"scientific_approval":False,
                        "final_readback":{"status":"DATASET_EXPORTED","trusted_handoff_verified":outcome in ("exported","bad-count"),
                                          "quality_gate_status":"PASSED","invalid_feature_count":0,"feature_row_count":5}}
                if outcome=="bad-count":report["final_readback"].update(invalid_feature_count=False,feature_row_count=True)
                c.e.save(reports/"12345678-1234-1234-1234-123456789abc.json",report)
                return (0,False)
            child_call=Mock(side_effect=child)
            original_path=m.Path;original_match=re.fullmatch
            def path(value):return deps if str(value)=="/opt/spark/jobs/iceberg" else original_path(value)
            def match(pattern,value,*args):
                if pattern.startswith("/opt/pipeline/staging/dat03-") and value in (str(root),str(catalog)):return True
                return original_match(pattern,value,*args)
            expected_error=("FOREIGN_LEASE" if held=="foreign" else "DEPS_OR_DISK" if outcome=="missing-deps" else
                            "HANDOFF_NOT_ACCEPTED" if outcome in ("bad-handoff","bad-count") else "FAILED_OWN_LEASE_RETAINED")
            should_fail=held=="foreign" or outcome in ("failure","timeout","missing-deps","bad-handoff","bad-count")
            with patch.object(m,"support",return_value=c),patch.object(m,"full_catalog",return_value={"completed_months":288}), \
                    patch.object(c,"validate"),patch.object(c,"quiet") as quiet,patch.object(m,"Path",side_effect=path), \
                    patch.object(m.re,"fullmatch",side_effect=match),patch.object(m,"module",return_value=types.SimpleNamespace(bounded_child=child_call)), \
                    patch.object(m.shutil,"disk_usage",return_value=types.SimpleNamespace(free=8*1024**3)), \
                    patch.dict(os.environ,{"SOURCE_GUARD_ROOT":str(guard)}), \
                    patch.dict("sys.modules",{"source_run_guard":types.SimpleNamespace(lease=lease_call)}):
                if should_fail:
                    with self.assertRaisesRegex(RuntimeError,expected_error):m.worker(root,6000)
                else:m.worker(root,6000)
            actions=[call.args[0] for call in lease_call.call_args_list]
            if held=="foreign" or outcome=="missing-deps":
                self.assertEqual([],actions);child_call.assert_not_called();self.assertFalse((root/"execution-sessions").exists())
            else:
                expected=[] if held=="own" else ["acquire"]
                if outcome=="exported":expected.append("release")
                self.assertEqual(expected,actions)
                self.assertEqual(outcome!="exported",(guard/"owner.json").exists())
                sessions=list((root/"execution-sessions").iterdir());self.assertEqual(1,len(sessions))
                self.assertEqual(not should_fail,(sessions[0]/"acceptance.json").exists())

    def test_clean_checkpoint_retains_own_lease(self):self.trial()
    def test_successful_handoff_releases_only_own_lease(self):self.trial("exported")
    def test_failed_job_never_releases_or_retries(self):self.trial("failure")
    def test_timeout_never_releases_or_retries(self):self.trial("timeout")
    def test_false_handoff_keeps_lease(self):self.trial("bad-handoff")
    def test_boolean_counts_cannot_release_lease(self):self.trial("bad-count")
    def test_missing_dependencies_stop_before_lease(self):self.trial("missing-deps")
    def test_foreign_lease_stops_without_child(self):self.trial(held="foreign")
    def test_resume_own_clean_checkpoint_does_not_acquire_twice(self):self.trial(held="own")


if __name__=="__main__":unittest.main()
