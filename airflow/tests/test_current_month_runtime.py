"""Synthetic metadata/control-plane tests, never scheduled acceptance evidence."""
from copy import deepcopy
from enum import Enum
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).parents[1] / "dags"))
import current_month_runtime as r
from source_schedule_runtime import resolve_daily
from source_run_guard import lease
from test_source_schedule_runtime import ready


class CurrentMonthRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name).resolve()
        self.env={"CURRENT_MONTH_DAILY_MODE":"operational","CURRENT_MONTH_CODE_VERSION":"a"*40,
                  "CURRENT_MONTH_RUNNER_SHA256":"d"*64,
                  "CURRENT_MONTH_DAILY_ROOT":str(self.root / "daily"),"SOURCE_GUARD_ROOT":str(self.root / "guard"),
                  "SOURCE_READINESS_ROOT":str(self.root / "sources"),"JMA_STAGING_ROOT":str(self.root / "jma"),
                  "JMA_INVENTORY_PATH":str(Path(__file__).parents[2] / "config/jma/hypocenter_archives_v1.csv"),
                  "ICEBERG_CATALOG_NAME":"iceberg"}
        self.context={"run_id":"scheduled__2026-10-20T00:15:00Z","data_interval_start":"2026-10-20T00:15:00Z",
                      "data_interval_end":"2026-10-21T00:15:00Z"}
        self.context["dag_run"]={**self.context,"dag_id":r.DAG,"run_type":"scheduled","conf":{}}
        self.plan=r.attach(resolve_daily(self.context,self.env),self.context,self.env)
        self.folder=r.folder_for(self.plan,self.env)
        self.identity={"dag_id":r.DAG,"run_id":self.plan["run_id"]};lease("acquire",self.identity,self.env)

    def capture(self):
        selection={"request":r.read(self.folder / "source-request.json")}
        return {"contract_version":"current-month-capture-receipt-v1","operation_id":self.plan["current_month"]["operation_id"],
                "selection":selection,"selection_sha256":r.digest(selection)}

    def sources(self):
        capture=self.capture();r.immutable(self.folder / "capture.json",capture)
        jma={"archives":[ready(a,run=self.plan["run_id"]) for a in self.plan["jma"]["archives"]]}
        return r.sources(self.plan,jma,capture,self.env)

    def results(self):
        op=self.plan["current_month"]["operation_id"]
        silver={"operation_id":op,"status":"ObservedAsOfReady","reconciliation":{"balanced":True},
                "registry_sha256":"b"*64,"event_cutoff_utc":"2026-10-21T00:00:00Z"}
        gold={"operation_id":op,"status":"Published","published":True,"coverage_mode":"OBSERVED_AS_OF","publication_quality":"PROVISIONAL",
              "research_ready":False,"ml_upstream_eligible":False,"snapshot_bundle":{t:{} for t in r.TABLES},
              "publication_uri":"s3://bucket/silver/current-month/pub","publication_sha256":"c"*64}
        return silver,gold

    def test_mode_mutual_exclusion_and_no_planner_runtime_import(self):
        self.assertFalse(r.enabled({}))
        for override in ({"CURRENT_MONTH_DAILY_MODE":"real"},{"ORC07_DAILY_MODE":"real"},{"SOURCE_SCHEDULE_PROFILE":"usgs-only"}):
            with self.assertRaises(RuntimeError):r.enabled({**self.env,**override})
        self.assertNotIn("import current_month_contract",Path(r.__file__).read_text())

    def test_real_dagrun_metadata_required_and_cannot_change_interval_conf(self):
        for change in ({"run_type":"manual"},{"conf":{"published":True}},{"run_id":"other"},{"data_interval_end":"2026-10-22T00:15:00Z"}):
            ctx=deepcopy(self.context);ctx["dag_run"].update(change)
            with self.subTest(change=change),self.assertRaises(RuntimeError):r.run_metadata(ctx)
        ctx=deepcopy(self.context);ctx.pop("dag_run")
        with self.assertRaises(RuntimeError):r.run_metadata(ctx)

    def test_sdk_enum_run_type_is_normalized_without_allowing_manual(self):
        class DagRunType(str,Enum):
            SCHEDULED="scheduled"
            MANUAL="manual"
        ctx=deepcopy(self.context);ctx["dag_run"]["run_type"]=DagRunType.SCHEDULED
        self.assertEqual("scheduled",r.run_metadata(ctx)["run_type"])
        ctx["dag_run"]["run_type"]=DagRunType.MANUAL
        with self.assertRaises(RuntimeError):r.run_metadata(ctx)

    def test_context_request_reused_and_code_profile_changes_rejected(self):
        self.assertEqual(self.plan,r.attach(resolve_daily(self.context,self.env),self.context,self.env))
        for override in ({"CURRENT_MONTH_CODE_VERSION":"b"*40},{"CURRENT_MONTH_CODE_VERSION":"latest"}):
            with self.assertRaises(RuntimeError):r.attach(self.plan,self.context,{**self.env,**override})
        self.assertIsNone(r.read(self.folder / "source-request.json")["baseline"])

    def test_capture_only_invokes_java_and_requires_same_selected_request(self):
        with patch.object(r,"invoke",return_value=self.capture()) as runner:
            self.assertEqual(self.capture(),r.capture(self.plan,self.env));self.assertEqual("capture",runner.call_args.args[0])
        wrong=self.capture();wrong["selection_sha256"]="x"
        with patch.object(r,"invoke",return_value=wrong),self.assertRaises(RuntimeError):r.capture(self.plan,self.env)

    def test_baseline_change_and_foreign_lease_block_before_source(self):
        r._atomic_json(r.root(self.env) / "published.json",{"foreign":True})
        with patch.object(r,"invoke") as runner,self.assertRaises(RuntimeError):r.capture(self.plan,self.env)
        runner.assert_not_called()
        lease("release",self.identity,self.env);lease("acquire",{"dag_id":"foreign","run_id":"other"},self.env)
        with patch.object(r,"invoke") as runner,self.assertRaises(Exception):r.capture(self.plan,self.env)
        runner.assert_not_called()

    def test_jma_scope_missing_or_foreign_owner_blocks(self):
        capture=self.capture();r.immutable(self.folder / "capture.json",capture)
        for archives in ([],[{"year":2023,"segment":"full-year","run_id":"other"}]):
            with self.assertRaises((RuntimeError,KeyError)):r.sources(self.plan,{"archives":archives},capture,self.env)

    def test_same_run_silver_gold_then_release_updates_exact_published_head(self):
        summary=self.sources();silver,gold=self.results()
        with patch.object(r,"invoke",side_effect=[silver,gold]) as runner:
            result=r.execute(self.plan,summary,self.env)
        self.assertEqual(["silver","gold"],[c.args[0] for c in runner.call_args_list])
        self.assertEqual("RELEASED",r.release(result,self.env)["status"])
        self.assertEqual(silver["registry_sha256"],r.head(self.env)["registry_sha256"])
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_failures_keep_lease_and_do_not_produce_complete_summary(self):
        summary=self.sources();silver,gold=self.results()
        for phase in (0,1):
            with patch.object(r,"invoke",side_effect=[RuntimeError("fail")] if phase==0 else [silver,RuntimeError("fail")]):
                with self.assertRaisesRegex(RuntimeError,"INCOMPLETE_LEASE_RETAINED"):r.execute(self.plan,summary,self.env)
            self.assertEqual("HELD",lease("assert",self.identity,self.env)["status"])
            self.assertFalse((self.folder / "execution.json").exists())
            self.assertIsNone(r.head(self.env))

    def test_provisional_publication_cannot_promote_eligibility(self):
        summary=self.sources();silver,gold=self.results();gold["ml_upstream_eligible"]=True
        with patch.object(r,"invoke",side_effect=[silver,gold]),self.assertRaises(RuntimeError):r.execute(self.plan,summary,self.env)
        self.assertIsNone(r.head(self.env))

    def test_unknown_or_incomplete_result_cannot_release_lease(self):
        with self.assertRaises(RuntimeError):r.release({"context":self.plan["current_month"],"summary":{}},self.env)
        self.assertEqual("HELD",lease("assert",self.identity,self.env)["status"])


if __name__ == "__main__":unittest.main()
