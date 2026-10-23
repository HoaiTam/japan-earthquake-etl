"""QA v2 synthetic proofs; these are NOT a real scheduler or Trino acceptance."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).parents[1] / "dags"))
import current_month_runtime as r
import qa_01_current_month as qa
from qa_01_daily_e2e import TASKS


class OperationalCollectorTest(unittest.TestCase):
    def setUp(self):
        self.run={"dag_id":r.DAG,"run_id":"scheduled__2026-10-20T00:15:00Z","run_type":"scheduled","conf":{},"state":"success",
                  "data_interval_start":"2026-10-20T00:15:00Z","data_interval_end":"2026-10-21T00:15:00Z","tasks":{t:"success" for t in TASKS}}
        exact={k:self.run[k] for k in ("dag_id","run_id","run_type","conf","data_interval_start","data_interval_end")}
        self.op="cm1-"+hashlib.sha256((r.DAG+"|"+self.run["run_id"]).encode()).hexdigest()[:40]
        self.context={"contract_version":r.VERSION,"operation_id":self.op,"run_context":exact,"profile":{"years":[2023]}}
        self.context["context_sha256"]=r.digest(self.context)
        request={"contract_version":"current-month-source-silver-v1","run_context":exact,"baseline":None}
        self.capture={"selection":{"request":request}};self.capture["selection_sha256"]=r.digest(self.capture["selection"])
        self.manifest={"sha256":"a"*64,"catalog_release":"release1","raw_object_uri":"s3://bucket/bronze/jma/archive.zip"}
        self.raw=json.dumps(self.manifest).encode()
        self.archive={**self.manifest,"year":2023,"segment":"full-year","run_id":self.run["run_id"],"bronze_status":"BronzeReady",
                      "verified":True,"manifest_uri":"s3://bucket/bronze/jma/manifest.json","manifest_sha256":hashlib.sha256(self.raw).hexdigest()}
        self.sources={"run_id":self.run["run_id"],"operation_id":self.op,"context_sha256":self.context["context_sha256"],
                      "verified":True,"status":"SourcesReady","jma_role":"HISTORICAL_READINESS_ONLY","required_event_sources":["USGS"],
                      "jma":{"archives":[self.archive]},"capture_sha256":r.digest(self.capture)}
        self.silver={"reconciliation":{"balanced":True,"baseline_unique_revisions":0,"capture_records":2,"capture_unique_revisions":2,
            "capture_repeated_ids":0,"reused_revision_ids":0,"output_revisions":2,"current":2,"history":0,"memberships":2,"links":0,"parser_rejects":0,"quality_rejects":0},
            "event_cutoff_utc":"2026-10-21T00:00:00Z","observation_cutoff_utc":"2026-10-21T01:00:00Z"}
        flags={"publication_quality":"PROVISIONAL","coverage_mode":"OBSERVED_AS_OF","research_ready":False,"ml_upstream_eligible":False}
        self.gold={**flags,"published":True,"snapshot_bundle":{t:{"snapshot_id":1,"count":2} for t in r.TABLES},"event_count":2,
                   "publication_uri":"s3://bucket/silver/current-month/pub"}
        self.summary={**flags,"published":True,"operation_id":self.op,"context_sha256":self.context["context_sha256"]}
        checks={prefix+t:{"sql":"SELECT 0","expected":2 if prefix=="count:" else 1 if prefix in {"identity:","policy:"} else 0,
                         "actual":2 if prefix=="count:" else 1 if prefix in {"identity:","policy:"} else 0} for prefix in ("count:","identity:","policy:","unique:") for t in r.TABLES}
        for name in ("required_fields","bridge_lineage","source_counts","primary_provenance","utc_jst","dimensions","aggregate_count","changed_run_scope",
                     "outside:event_current","outside:event_source_bridge","operational:source_cutoff","operational:coverage","versioned_policy"):
            checks[name]={"sql":"SELECT 0","expected":0,"actual":0}
        for name,want in (("silver_to_gold:event_current",2),("silver_to_gold:event_source_bridge",2),("natural_roi",2),("publication_row",1),("versioned_serving",2)):
            checks[name]={"sql":"SELECT 1","expected":want,"actual":want}
        self.audit={**flags,"contract_version":"current-month-audit-v1","status":"PASSED","read_only":True,"operation_id":self.op,
                    "silver":self.silver,"gold":self.gold,"source_request":request,"verification":{**flags,"published":False,"checks":checks},
                    "publication":{**flags,"published":True,"publication_id":"pub_"+"a"*64}}
        self.seal()

    def seal(self):
        self.summary.update(source_summary_sha256=r.digest(self.sources),silver_sha256=r.digest(self.silver),gold_sha256=r.digest(self.gold))

    def verify(self):
        return qa.verify(self.run,self.context,self.sources,self.capture,self.silver,self.gold,self.summary,self.audit,lambda uri:(self.raw,self.manifest))

    def test_same_run_actual_chain_counts_policy_and_historical_jma_readback(self):
        result=self.verify();self.assertEqual("PASSED",result["status"]);self.assertEqual("qa-01-v2",result["report_version"])
        self.assertEqual(42,len(result["sql_readback"]));self.assertFalse(result["ml_upstream_eligible"])

    def test_manual_failed_skipped_and_conf_runs_cannot_pass(self):
        for key,value in (("run_type","manual"),("state","failed"),("conf",{"override":True}),("tasks",{t:"skipped" for t in TASKS})):
            with self.subTest(key=key):
                saved=self.run[key];self.run[key]=value
                with self.assertRaises(RuntimeError):self.verify()
                self.run[key]=saved

    def test_cross_run_or_changed_capture_is_rejected(self):
        self.audit["source_request"]=deepcopy(self.audit["source_request"])
        self.audit["source_request"]["run_context"]["run_id"]="other"
        with self.assertRaises(RuntimeError):self.verify()

    def test_ml_promotion_and_missing_sql_blocker_rejected(self):
        self.audit["ml_upstream_eligible"]=True
        with self.assertRaises(RuntimeError):self.verify()
        self.audit["ml_upstream_eligible"]=False;self.audit["verification"]["checks"].pop("operational:coverage")
        with self.assertRaises(RuntimeError):self.verify()

    def test_unbalanced_counts_and_parser_rejects_cannot_be_sealed_as_valid(self):
        for key in ("history","parser_rejects","capture_records"):
            self.silver["reconciliation"][key]+=1;self.seal()
            with self.assertRaises(RuntimeError):self.verify()
            self.silver["reconciliation"][key]-=1

    def test_nonzero_blocker_or_false_counter_expectation_is_rejected(self):
        c=self.audit["verification"]["checks"]["operational:coverage"];c.update(expected=1,actual=1)
        with self.assertRaises(RuntimeError):self.verify()

    def test_jma_changed_manifest_or_owner_cannot_mean_current_event_coverage(self):
        self.raw+=b" "
        with self.assertRaises(RuntimeError):self.verify()
        self.raw=self.raw[:-1];self.sources["jma"]["archives"][0]["run_id"]="other";self.seal()
        with self.assertRaises(RuntimeError):self.verify()


if __name__ == "__main__":unittest.main()
