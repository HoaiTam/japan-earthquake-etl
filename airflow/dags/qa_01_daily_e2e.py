"""QA-01 read-only acceptance collector. This module does not define a DAG.

Read one scheduler-created daily run; never trigger/clear/mark success, publish,
release leases, or treat ORC-07 DAG.test evidence as deployment acceptance.
"""
import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import time
from urllib.parse import urlsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler

import real_etl_runtime as runtime
from etl_pipeline_runtime import _unique_json_object, _uri

DAG = "orc_02_daily_sources"
TASKS = {"resolve_daily", "acquire_source_lease", "jma_readiness", "usgs_bronze",
         "sources_ready", "real_etl", "release_source_lease", "completion_gate"}
LIMIT = 1024 * 1024


def require(condition, reason):
    if not condition:
        raise RuntimeError("QA01_" + reason)


def utc(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "UTC_REQUIRED")
    return parsed.astimezone(timezone.utc)


def load_bytes(raw):
    require(0 < len(raw) <= LIMIT, "BOUNDED_METADATA_REQUIRED")
    result = json.loads(raw, object_pairs_hook=_unique_json_object,
                        parse_constant=lambda _: require(False, "FINITE_JSON_REQUIRED"))
    require(isinstance(result, dict), "METADATA_OBJECT_REQUIRED")
    return result


def local_json(root, *parts):
    target = Path(root).joinpath(*parts)
    require(target.resolve().is_relative_to(Path(root).resolve())
            and not any(p.is_symlink() for p in (target, *target.parents))
            and target.is_file() and target.stat().st_size <= LIMIT, "LOCAL_EVIDENCE_UNAVAILABLE")
    return load_bytes(target.read_bytes())


def validate_schedule(run):
    require(run.get("dag_id") == DAG and run.get("run_type") == "scheduled"
            and isinstance(run.get("run_id"), str)
            and re.fullmatch(r"scheduled__[0-9T:.+Z-]{10,80}", run["run_id"]), "SCHEDULED_RUN_REQUIRED")
    start, end = utc(run["data_interval_start"]), utc(run["data_interval_end"])
    require(end - start == timedelta(days=1) and end <= datetime.now(timezone.utc), "CLOSED_DAILY_INTERVAL_REQUIRED")


def validate_run(run):
    validate_schedule(run)
    require(run.get("state") == "success" and set(run.get("tasks", {})) == TASKS
            and all(s == "success" for s in run["tasks"].values()), "DAILY_TASKS_NOT_SUCCESSFUL")


def validate_sources(run, source):
    require(source.get("run_id") == run["run_id"] and source.get("status") == "SourcesReady"
            and source.get("verified") is True and source.get("published") is False, "SOURCE_IDENTITY_CHANGED")
    require(utc(source["data_interval_end"]) == utc(run["data_interval_end"]), "SOURCE_INTERVAL_CHANGED")
    context = source["usgs_context"]
    require(context.get("dag_id") == DAG and context.get("run_id") == run["run_id"]
            and utc(context["window_end_utc"]) == utc(run["data_interval_end"]).replace(hour=0, minute=0, second=0, microsecond=0)
            and utc(context["window_start_utc"]) < utc(context["window_end_utc"]), "SOURCE_WINDOW_CHANGED")
    archives = source["jma"]["archives"]
    require(isinstance(archives, list) and 0 < len(archives) <= 64, "JMA_INPUTS_REQUIRED")
    fresh = [{"source_system": "USGS", "manifest_uri": source["usgs"]["manifest_uri"],
              "manifest_sha256": source["usgs"]["manifest_sha256"], "sha256": source["usgs"]["sha256"],
              **{k: context[k] for k in ("window_start_utc", "window_end_utc")}}]
    for item in [source["usgs"], *archives]:
        require(item.get("run_id") == run["run_id"], "SOURCE_OWNER_CHANGED")
    fresh += [{"source_system": "JMA_BULLETIN", **{k: item[k] for k in
               ("manifest_uri", "manifest_sha256", "sha256", "year", "segment", "catalog_release")}} for item in archives]
    require(len({p["manifest_uri"] for p in fresh}) == len(fresh), "DUPLICATE_INPUT_PIN")
    for pin in fresh:
        _uri(pin["manifest_uri"], "real")
        require(all(re.fullmatch(r"[a-f0-9]{64}", str(pin[k])) for k in ("manifest_sha256", "sha256")), "INPUT_SHA_REQUIRED")
    return fresh


def count(value):
    require(type(value) is int and 0 <= value <= 9223372036854775807, "INVALID_COUNT")
    return value


def reconcile(value):
    require(value.get("balanced") is True, "SILVER_NOT_BALANCED")
    names = ("selected_input_records", "input_unique_revisions", "repeated_input_observation_ids",
             "output_revisions", "revisions_outside_affected_months", "current", "history", "canonical", "memberships", "links")
    n = {k: count(value[k]) for k in names}
    require(n["selected_input_records"] == n["input_unique_revisions"] + n["repeated_input_observation_ids"]
            and n["input_unique_revisions"] == n["output_revisions"] + n["revisions_outside_affected_months"]
            and n["output_revisions"] == n["current"] + n["history"]
            and n["memberships"] == n["current"] and n["canonical"] <= n["current"], "SILVER_COUNT_MISMATCH")
    sources = value["sources"]
    require(isinstance(sources, list) and 0 < len(sources) <= 64, "SOURCE_COUNTS_REQUIRED")
    selected = unique = repeated = 0
    for item in sources:
        require(item.get("source_system") in {"USGS", "JMA_BULLETIN"}, "SOURCE_COUNTS_REQUIRED")
        full, chosen, outside, rejects, added, duplicates = (count(item[k]) for k in
            ("full_bronze_records", "selected_records", "outside_temporal_selection", "parser_rejects", "unique_revisions_added", "repeated_observation_ids"))
        require(full == chosen + outside and rejects == 0 and chosen == added + duplicates, "SOURCE_COUNT_MISMATCH")
        selected += chosen; unique += added; repeated += duplicates
    require(selected == n["selected_input_records"] and unique == n["input_unique_revisions"]
            and repeated == n["repeated_input_observation_ids"], "SOURCE_COUNT_MISMATCH")
    return value


def verify_chain(run, source, context, summary, read_object, scalar, bucket, silver_prefix):
    """Dependency-injected readback for unit tests; never reads a caller's passed flag."""
    validate_run(run)
    fresh = validate_sources(run, source)
    runtime.validate_context(context)
    require(context["dag_id"] == DAG and context["run_id"] == run["run_id"] and context["trigger"] == "daily"
            and context["source_summary_sha256"] == runtime.digest(source)
            and all(context[k] == source["usgs_context"][k] for k in ("window_start_utc", "window_end_utc")), "RUN_CONTEXT_CHANGED")
    request = context["resolver_request"]; op = context["operation_id"]
    require(all(pin in request["bronze_inputs"] for pin in fresh), "DAILY_INPUT_PIN_MISSING")
    require(summary.get("operation_id") == op and summary.get("context_sha256") == context["context_sha256"]
            and summary.get("status") == "Published" and summary.get("published") is True, "PUBLICATION_REQUIRED")
    base = "s3://" + bucket + "/" + silver_prefix.strip("/") + "/real-etl/" + op + "/"
    require(load_bytes(read_object(base + "context.json")) == context, "STORED_CONTEXT_CHANGED")
    receipts = summary["receipts"]
    require(isinstance(receipts, list) and len(receipts) == 6, "SIX_PHASES_REQUIRED")
    artifacts = {}; evidence = []; upstream = None
    for phase, receipt in zip(runtime.PHASES, receipts):
        runtime.validate_receipt(phase, context, upstream, receipt)
        require(receipt["artifact_uri"] == base + phase + ".json", "CROSS_RUN_ARTIFACT")
        require(load_bytes(read_object(base + phase + "-receipt.json")) == receipt, "STORED_RECEIPT_CHANGED")
        raw = read_object(receipt["artifact_uri"]); artifact = load_bytes(raw)
        require(runtime.digest(artifact) == receipt["artifact_sha256"], "ARTIFACT_SHA_CHANGED")
        artifacts[phase] = artifact
        evidence.append({"phase": phase, "uri": receipt["artifact_uri"], "sha256": receipt["artifact_sha256"]})
        upstream = receipt
    for phase, status in (("readiness", "SourcesReady"), ("bronze", "BronzeVerified")):
        artifact = artifacts[phase]
        require(artifact.get("status") == status and [i["pin"] for i in artifact["bronze_inputs"]] == request["bronze_inputs"], "BRONZE_PINS_CHANGED")
        for item in artifact["bronze_inputs"]:
            count(item["count"])
            require(item["status"] == "BronzeReady", "BRONZE_NOT_READY")
    silver = artifacts["silver"]
    require(silver.get("run_id") == op and silver.get("status") == "FullMonthReady"
            and silver.get("scope_completeness") == "COMPLETE_MONTHS"
            and silver.get("request_sha256") == runtime.digest(request), "FULL_MONTH_PROOF_REQUIRED")
    counts = reconcile(silver["reconciliation"])
    require(receipts[2]["counts"] == counts, "SILVER_RECEIPT_CHANGED")
    gold = artifacts["gold"]; verification = artifacts["verify"]; publication = artifacts["publish"]
    namespace = request["namespace"]
    require(gold.get("gold_run_id") == op and gold.get("namespace") == namespace
            and gold.get("status") == "COMMITTED" and gold.get("published") is False
            and re.fullmatch(r"[a-f0-9]{64}", str(gold.get("identity_sha256")))
            and set(gold["tables"]) == runtime.TABLES, "COMMIT_IDENTITY_CHANGED")
    snapshots = {k: gold["tables"][k]["snapshot_id"] for k in runtime.TABLES}
    require(snapshots == receipts[3]["snapshots"] == summary["snapshots"], "SNAPSHOTS_CHANGED")
    commit_uri = "s3://" + bucket + "/" + silver_prefix.strip("/") + "/gold-commits/" + op + "/commit.json"
    commit_raw = read_object(commit_uri)
    require(load_bytes(commit_raw) == gold, "COMMIT_JOURNAL_CHANGED")
    bundle_sha = hashlib.sha256(commit_raw).hexdigest(); publication_id = "pub_" + bundle_sha
    for document in (verification, publication):
        require(document.get("gold_run_id") == op and document.get("namespace") == namespace
                and document.get("identity_sha256") == gold["identity_sha256"]
                and document.get("bundle_sha256") == bundle_sha and document.get("snapshot_bundle") == gold["tables"]
                and document.get("engine") == "TRINO" and document.get("verify_status") == "PASSED", "TRINO_REPORT_CHANGED")
        checks = document.get("checks", {})
        required = {p + t for p in ("count:", "identity:", "unique:") for t in runtime.TABLES}
        required |= {"required_fields", "bridge_lineage", "source_counts", "primary_provenance", "utc_jst", "dimensions", "aggregate_count", "changed_run_scope", "outside:event_current", "outside:event_source_bridge"}
        require(required <= set(checks), "TRINO_CHECKS_INCOMPLETE")
        for name in required:
            expected = gold["tables"][name[6:]]["count"] if name.startswith("count:") else 1 if name.startswith("identity:") else 0
            require(type(checks[name].get("expected")) is int and checks[name]["expected"] == expected, "TRINO_EXPECTATION_CHANGED")
        for check in checks.values():
            require(isinstance(check.get("sql"), str) and check["sql"].startswith("SELECT ")
                    and count(check["actual"]) == count(check["expected"]), "TRINO_BLOCKER_FAILED")
    require(verification.get("published") is False and publication.get("published") is True
            and publication.get("publication_id") == publication_id == summary["publication_id"] == receipts[5]["publication_id"], "PUBLICATION_ID_CHANGED")
    journal_uri = "s3://" + bucket + "/" + silver_prefix.strip("/") + "/gold-verification/" + op + "/publication.json"
    require(load_bytes(read_object(journal_uri)) == publication, "PUBLICATION_JOURNAL_CHANGED")
    # Generate our own bounded SELECTs. Never execute SQL supplied by an evidence file.
    queries = []
    def checked(name, sql, expected):
        expected = count(expected); actual = scalar(sql)
        require(type(actual) is int and actual == expected, "KPI_SQL_MISMATCH")
        queries.append({"name": name, "sql": sql, "expected": expected, "actual": actual})
    def ref(name):
        require(gold["tables"][name].get("table") == namespace + "." + name, "TABLE_CHANGED")
        return namespace + "." + name + " FOR VERSION AS OF " + str(snapshots[name])
    for name in sorted(runtime.TABLES):
        checked("count:" + name, "SELECT count(*) FROM " + ref(name), gold["tables"][name]["count"])
        require(verification["checks"]["count:" + name]["expected"] == gold["tables"][name]["count"], "VERIFICATION_COUNT_CHANGED")
        sql = "SELECT count(*) FROM " + namespace + '."' + name + "$snapshots\" WHERE snapshot_id=" + str(snapshots[name])
        sql += " AND summary['gold.operation_id']='" + op + "' AND summary['gold.identity_sha256']='" + gold["identity_sha256"] + "'"
        checked("identity:" + name, sql, 1)
    months = silver["affected_months"]
    require(isinstance(months, list) and 0 < len(months) <= 1200
            and all(re.fullmatch(r"[12][0-9]{3}-(0[1-9]|1[0-2])", m) for m in months), "EXACT_AFFECTED_MONTHS_REQUIRED")
    month_sql = ",".join("'" + m + "'" for m in months)
    checked("silver_to_gold_canonical", "SELECT count(*) FROM " + ref("event_current")
            + " WHERE format_datetime(event_time_utc AT TIME ZONE 'UTC','yyyy-MM') IN (" + month_sql + ")", counts["canonical"])
    checked("silver_to_gold_memberships", "SELECT count(*) FROM " + ref("event_source_bridge")
            + " WHERE event_month_utc IN (" + month_sql + ")", counts["memberships"])
    natural = count(publication["event_count"])
    checked("natural_roi", "SELECT count(*) FROM " + ref("event_current")
            + " WHERE is_natural_earthquake AND is_in_study_area", natural)
    checked("versioned_serving_view", "SELECT count(*) FROM " + namespace + ".v_" + bundle_sha[:24] + "_earthquake_event_current", natural)
    checked("publication_row", "SELECT count(*) FROM " + namespace + ".publication_status WHERE publication_id='" + publication_id
            + "' AND gold_run_id='" + op + "' AND verify_status='PASSED' AND bundle_sha256='" + bundle_sha
            + "' AND snapshot_id=" + str(snapshots["event_current"]) + " AND event_count=" + str(natural)
            + " AND (SELECT count(*) FROM " + namespace + ".publication_status WHERE publication_id='" + publication_id + "')=1", 1)
    return {"task_id": "QA-01", "report_version": "qa-01-v1", "status": "PASSED", "runtime_complete": True,
            "scheduled_run": True, "run": run, "operation_id": op, "context_sha256": context["context_sha256"],
            "source_summary_sha256": runtime.digest(source), "bronze_inputs": artifacts["bronze"]["bronze_inputs"],
            "silver_counts": counts, "affected_months": months, "namespace": namespace, "snapshots": snapshots,
            "publication_id": publication_id, "natural_roi_event_count": natural, "artifacts": evidence,
            "sql_readback": queries, "ml_upstream_eligible": True,
            "official_ml_dataset_exported": False}


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def trino_scalar(sql, environment, timeout_seconds=60):
    host = environment["TRINO_HOST"]; port = environment["TRINO_INTERNAL_PORT"]
    require(re.fullmatch(r"[A-Za-z0-9.-]+", host) and port.isdigit() and 0 < int(port) < 65536, "TRINO_ENDPOINT_INVALID")
    base = "http://" + host + ":" + port
    opener = build_opener(ProxyHandler({}), NoRedirect()); url = base + "/v1/statement"; rows = []
    headers = {"X-Trino-User": "qa01_readback", "X-Trino-Time-Zone": "UTC", "Content-Type": "text/plain"}
    data = sql.encode(); deadline = time.monotonic() + timeout_seconds
    for _ in range(200):
        require(time.monotonic() < deadline, "TRINO_TIMEOUT")
        with opener.open(Request(url, data=data, headers=headers), timeout=10) as response:
            raw = response.read(LIMIT + 1)
        result = load_bytes(raw); require("error" not in result, "TRINO_QUERY_FAILED")
        rows.extend(result.get("data", [])); require(len(rows) <= 1, "TRINO_SCALAR_REQUIRED")
        if not result.get("nextUri"):
            require(len(rows) == 1 and len(rows[0]) == 1 and type(rows[0][0]) is int, "TRINO_SCALAR_REQUIRED")
            return rows[0][0]
        url = result["nextUri"]; parsed = urlsplit(url)
        require(parsed.scheme == "http" and parsed.netloc == urlsplit(base).netloc
                and parsed.path.startswith("/v1/statement/") and not parsed.fragment, "TRINO_NEXT_URI_REJECTED")
        data = None
    raise RuntimeError("QA01_TRINO_PAGE_LIMIT")


def collect(run_id, environment=None):
    env = os.environ if environment is None else environment
    require(isinstance(run_id, str) and re.fullmatch(r"scheduled__[0-9T:.+Z-]{10,80}", run_id), "SCHEDULED_RUN_REQUIRED")
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.session import create_session
    with create_session() as session:
        row = session.query(DagRun).filter_by(dag_id=DAG, run_id=run_id).one_or_none()
        require(row is not None, "RUN_NOT_FOUND")
        run = {"dag_id": DAG, "run_id": run_id, "run_type": str(getattr(row.run_type,"value",row.run_type)), "state": str(getattr(row.state,"value",row.state)),
               "data_interval_start": row.data_interval_start.isoformat() if row.data_interval_start else "",
               "data_interval_end": row.data_interval_end.isoformat() if row.data_interval_end else "",
               "tasks": {r.task_id: str(r.state) for r in session.query(TaskInstance).filter_by(dag_id=DAG, run_id=run_id).all()}}
        actual_conf = getattr(row,"conf",None)
    validate_schedule(run)
    import current_month_runtime as current_month
    op2 = "cm1-" + hashlib.sha256((DAG+"|"+run_id).encode()).hexdigest()[:40]
    if (current_month.root(env) / op2 / "context.json").is_file():
        import qa_01_current_month
        run["conf"] = actual_conf or {}
        return qa_01_current_month.collect(run,env)
    # A seven-task SourcesReady run predates ORC-07. Keep its actual task set;
    # do not backfill a fictional successful real_etl TaskInstance.
    legacy = set(run["tasks"]) == TASKS - {"real_etl"} and run["state"] == "success" \
        and all(s == "success" for s in run["tasks"].values())
    if not legacy:
        validate_run(run)
    slug = "daily-" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    source = local_json(env.get("SOURCE_READINESS_ROOT", "/opt/pipeline/staging/readiness"), slug, "run_summary.json")
    validate_sources(run, source)
    op = runtime.operation(DAG, run_id); root = runtime.root(env)
    if legacy or not (root / op / "summary.json").is_file():
        return {"task_id": "QA-01", "report_version": "qa-01-v1", "status": "NOT_READY", "runtime_complete": False,
                "reason": "QA01_DAILY_DEPLOYMENT_PRE_ORC07" if legacy else "QA01_REAL_ETL_EVIDENCE_MISSING",
                "run": run, "operation_id": op, "missing_tasks": sorted(TASKS - set(run["tasks"])),
                "source_summary_sha256": runtime.digest(source), "source_status": "SourcesReady",
                "daily_mode_now": env.get("ORC07_DAILY_MODE", "disabled"), "ml_upstream_eligible": False}
    context = local_json(root, op, "context.json"); summary = local_json(root, op, "summary.json")
    import boto3
    from botocore.config import Config
    client = boto3.client("s3", endpoint_url=env["MINIO_ENDPOINT"], aws_access_key_id=env["MINIO_ACCESS_KEY"],
                          aws_secret_access_key=env["MINIO_SECRET_KEY"], region_name=env["S3_REGION"],
                          config=Config(connect_timeout=5, read_timeout=10, retries={"max_attempts": 1}))
    deadline = time.monotonic() + 220
    def read_object(uri):
        require(time.monotonic() < deadline, "COLLECTION_TIMEOUT")
        _uri(uri, "real"); parsed = urlsplit(uri)
        require(parsed.netloc == env["DATA_BUCKET"] and parsed.path.lstrip("/").startswith(env["SILVER_PREFIX"].strip("/") + "/"), "OBJECT_OUTSIDE_SCOPE")
        response = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        try:
            require(0 < response["ContentLength"] <= LIMIT, "BOUNDED_METADATA_REQUIRED")
            return response["Body"].read(LIMIT + 1)
        finally:
            response["Body"].close()
    def scalar(sql):
        remaining = deadline - time.monotonic()
        require(remaining > 0, "COLLECTION_TIMEOUT")
        return trino_scalar(sql, env, min(remaining, 60))
    return verify_chain(run, source, context, summary, read_object, scalar, env["DATA_BUCKET"], env["SILVER_PREFIX"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    try:
        report = collect(args.run_id)
    except Exception as error:
        reason = str(error)
        report = {"task_id": "QA-01", "report_version": "qa-01-v1", "status": "FAILED", "runtime_complete": False,
                  "reason": reason if re.fullmatch(r"(?:QA01_|QA01V2_|ASOF_)[A-Z0-9_]+", reason) else "QA01_COLLECTION_FAILED", "ml_upstream_eligible": False}
    print("QA01_REPORT=" + json.dumps(report, sort_keys=True, allow_nan=False))
    raise SystemExit(0 if report["status"] == "PASSED" else 1)
