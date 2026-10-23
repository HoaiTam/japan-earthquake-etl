"""QA-01 v2 read-only collector for the SAME scheduler-created daily run.

No notebook/ML eligibility, no DAG trigger/state edits, capture or publication.
Java revalidates Bronze/Silver and executes independent exact-snapshot Trino SQL.
"""
import hashlib
import os
from pathlib import Path
import re
import tempfile
from urllib.parse import urlsplit

import current_month_runtime as runtime


def require(value, reason):
    if not value:
        raise RuntimeError("QA01V2_"+reason)


def verify(run, context, sources, capture, silver, gold, summary, audit, jma_readback):
    from qa_01_daily_e2e import validate_run
    validate_run(run)
    exact = {k: run[k] for k in ("dag_id","run_id","run_type","data_interval_start","data_interval_end")}
    exact["conf"] = run.get("conf") or {}
    exact["data_interval_start"] = runtime.stamp(exact["data_interval_start"])
    exact["data_interval_end"] = runtime.stamp(exact["data_interval_end"])
    require(exact["conf"] == {} and context.get("contract_version") == runtime.VERSION
            and context.get("run_context") == exact
            and context.get("context_sha256") == runtime.digest({k:v for k,v in context.items() if k != "context_sha256"}), "ACTUAL_RUN_CONTEXT_CHANGED")
    op = "cm1-"+hashlib.sha256((runtime.DAG+"|"+run["run_id"]).encode()).hexdigest()[:40]
    require(context.get("operation_id") == op and summary.get("operation_id") == op
            and summary.get("context_sha256") == context["context_sha256"]
            and sources.get("context_sha256") == context["context_sha256"]
            and sources.get("run_id") == run["run_id"] and sources.get("operation_id") == op
            and sources.get("verified") is True and sources.get("status") == "SourcesReady"
            and sources.get("jma_role") == "HISTORICAL_READINESS_ONLY" and sources.get("required_event_sources") == ["USGS"], "SOURCE_CONTEXT_CHANGED")
    require(summary.get("source_summary_sha256") == runtime.digest(sources)
            and summary.get("silver_sha256") == runtime.digest(silver) and summary.get("gold_sha256") == runtime.digest(gold)
            and sources.get("capture_sha256") == runtime.digest(capture), "LOCAL_RECEIPT_SHA_CHANGED")
    require(audit.get("contract_version") == "current-month-audit-v1" and audit.get("status") == "PASSED"
            and audit.get("read_only") is True and audit.get("operation_id") == op
            and audit.get("silver") == silver and audit.get("gold") == gold
            and audit.get("source_request") == capture["selection"]["request"]
            and audit["source_request"]["run_context"] == exact
            and capture.get("selection_sha256") == runtime.digest(capture["selection"]), "SAME_RUN_READBACK_REQUIRED")
    for item in (summary,gold,audit,audit["verification"],audit["publication"]):
        require(item.get("coverage_mode") == "OBSERVED_AS_OF" and item.get("publication_quality") == "PROVISIONAL"
                and item.get("research_ready") is False and item.get("ml_upstream_eligible") is False, "POLICY_PROMOTION_FORBIDDEN")
    require(summary.get("published") is True and gold.get("published") is True
            and audit["publication"].get("published") is True
            and audit["verification"].get("published") is False, "PUBLICATION_REQUIRED")
    n = silver["reconciliation"]
    for key,value in n.items():
        if key != "balanced":
            require(type(value) is int and 0 <= value <= 100000, "RECONCILIATION_COUNT_INVALID")
    require(n.get("balanced") is True and n["output_revisions"] == n["current"]+n["history"]
            and n["memberships"] == n["current"] and n["links"] == n["parser_rejects"] == n["quality_rejects"] == 0
            and n["capture_records"] == n["capture_unique_revisions"]+n["capture_repeated_ids"]
            and n["output_revisions"] == n["baseline_unique_revisions"]+n["capture_unique_revisions"]-n["reused_revision_ids"], "RECONCILIATION_UNBALANCED")
    checks = audit["verification"].get("checks",{})
    expected = {prefix+t for prefix in ("count:","identity:","policy:","unique:") for t in runtime.TABLES}
    expected |= {"required_fields","bridge_lineage","source_counts","primary_provenance","utc_jst","dimensions","aggregate_count",
                 "changed_run_scope","outside:event_current","outside:event_source_bridge","operational:source_cutoff","operational:coverage",
                 "silver_to_gold:event_current","silver_to_gold:event_source_bridge","natural_roi","publication_row","versioned_serving","versioned_policy"}
    require(expected <= set(checks) and set(gold["snapshot_bundle"]) == runtime.TABLES, "SQL_PROOF_INCOMPLETE")
    for name in expected:
        c = checks[name]
        want = gold["snapshot_bundle"][name[6:]]["count"] if name.startswith("count:") else 1 if name.startswith(("identity:","policy:")) or name == "publication_row" else n["current"] if name.startswith("silver_to_gold:") else gold["event_count"] if name in {"natural_roi","versioned_serving"} else 0
        require(type(c.get("expected")) is int and c["expected"] == want and type(c.get("actual")) is int and c["actual"] == want
                and isinstance(c.get("sql"),str) and c["sql"].startswith("SELECT "), "SQL_PROOF_FAILED")
    archives = sources["jma"]["archives"]
    wanted = {(a["year"],a["segment"]) for a in archives}
    require(wanted and len(wanted) == len(archives)
            and wanted == {(year,segment) for year in context["profile"]["years"]
                           for segment in (("jan-sep","oct-dec") if year == 1997 else ("full-year",))}, "JMA_SCOPE_CHANGED")
    for item in archives:
        require(item.get("run_id") == run["run_id"] and item.get("verified") is True
                and item.get("bronze_status") == "BronzeReady", "JMA_READINESS_REQUIRED")
        raw, manifest = jma_readback(item["manifest_uri"])
        require(hashlib.sha256(raw).hexdigest() == item["manifest_sha256"] and manifest.get("sha256") == item["sha256"]
                and manifest.get("catalog_release") == item["catalog_release"]
                and manifest.get("raw_object_uri") == item["raw_object_uri"], "JMA_PIN_CHANGED")
    return {"task_id":"QA-01","report_version":"qa-01-v2","status":"PASSED","runtime_complete":True,"scheduled_run":True,
            "run":run,"operation_id":op,"context_sha256":context["context_sha256"],"operational_only":True,
            "runner_sha256":context.get("runner_sha256"),"code_version":context.get("code_version"),"profile":context["profile"],
            "coverage_mode":"OBSERVED_AS_OF","publication_quality":"PROVISIONAL","research_ready":False,"ml_upstream_eligible":False,
            "required_event_sources":["USGS"],"jma_role":"HISTORICAL_READINESS_ONLY","jma_historical_readiness":archives,
            "event_cutoff_utc":silver["event_cutoff_utc"],"observation_cutoff_utc":silver["observation_cutoff_utc"],
            "source_selection":capture["selection"],"silver_counts":n,"snapshot_bundle":gold["snapshot_bundle"],
            "publication_id":audit["publication"]["publication_id"],"publication_uri":gold["publication_uri"],
            "natural_roi_event_count":gold["event_count"],"sql_readback":list(checks.values()),
            "audit":audit,"official_ml_dataset_exported":False}


def collect(run, environment=None):
    env = dict(os.environ if environment is None else environment)
    op = "cm1-"+hashlib.sha256((runtime.DAG+"|"+run["run_id"]).encode()).hexdigest()[:40]
    folder = runtime.root(env) / op
    context,sources,capture,silver,gold,summary = (runtime.read(folder / name) for name in
        ("context.json","sources.json","capture.json","silver.json","gold.json","summary.json"))
    require(runtime.read(folder / "execution.json") == {"state":"COMPLETE","summary_sha256":runtime.digest(summary)}, "DAILY_OUTCOME_INCOMPLETE")
    # Separate, new private output folder; audit Java has no write-capable phase or source fetcher.
    base = runtime.root(env) / "audits";base.mkdir(parents=True,exist_ok=True)
    audit = runtime.invoke("audit",folder / "gold-request.json",Path(tempfile.mkdtemp(prefix="qa01-",dir=base)),env)
    require(audit.get("gold_request") == runtime.read(folder / "gold-request.json"), "GOLD_REQUEST_CHANGED")
    import boto3
    from botocore.config import Config
    client = boto3.client("s3", endpoint_url=env["MINIO_ENDPOINT"],aws_access_key_id=env["MINIO_ACCESS_KEY"],aws_secret_access_key=env["MINIO_SECRET_KEY"],
                          region_name=env["S3_REGION"],config=Config(connect_timeout=5,read_timeout=10,retries={"max_attempts":1}))
    def jma_readback(uri):
        from qa_01_daily_e2e import load_bytes
        p = urlsplit(uri);require(p.scheme == "s3" and p.netloc == env["DATA_BUCKET"] and not p.username and not p.query and not p.fragment
            and p.path.lstrip("/").startswith(env["BRONZE_PREFIX"].strip("/")+"/jma/"), "JMA_URI_OUTSIDE_SCOPE")
        response = client.get_object(Bucket=p.netloc,Key=p.path.lstrip("/"))
        try:
            require(response["ContentLength"] <= 1048576, "JMA_METADATA_LIMIT")
            raw = response["Body"].read(1048577);return raw,load_bytes(raw)
        finally:
            response["Body"].close()
    return verify(run,context,sources,capture,silver,gold,summary,audit,jma_readback)
