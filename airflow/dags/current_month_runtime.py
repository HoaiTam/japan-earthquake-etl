"""Operational daily control plane. Java owns all source bytes, ETL and SQL gates.

Independent runtime protocol; never imports the DESIGN_ONLY planner or weakens v1.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from etl_pipeline_runtime import EtlContractError, _json_bytes, _unique_json_object
from jma_backfill_runtime import _atomic_json, _ready
from source_run_guard import lease

VERSION = "current-month-daily-v1"
DAG = "orc_02_daily_sources"
TABLES = {"event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band"}


def require(value, reason):
    if not value:
        raise RuntimeError("ASOF_" + reason)


def digest(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    require(len(raw) <= 1048576, "BOUNDED_DAILY_METADATA_REQUIRED")
    return hashlib.sha256(raw).hexdigest()


def enabled(environment=None):
    env = os.environ if environment is None else environment
    mode = env.get("CURRENT_MONTH_DAILY_MODE", "disabled")
    require(mode in {"disabled", "operational"}, "DAILY_MODE_INVALID")
    if mode == "operational":
        require(env.get("ORC07_DAILY_MODE", "disabled") == "disabled"
                and env.get("SOURCE_SCHEDULE_PROFILE", "multi-source") == "multi-source", "DAILY_MODES_CONFLICT")
    return mode == "operational"


def root(env):
    path = Path(env.get("CURRENT_MONTH_DAILY_ROOT", "/opt/pipeline/staging/current-month-daily"))
    require(not any(p.is_symlink() for p in (path, *path.parents)), "UNSAFE_DAILY_ROOT")
    return path


def read(path):
    require(not path.is_symlink() and path.is_file() and path.stat().st_size <= 1048576, "BOUNDED_DAILY_METADATA_REQUIRED")
    return json.loads(path.read_bytes(), object_pairs_hook=_unique_json_object)


def immutable(path, value):
    if path.exists():
        require(read(path) == value, "DAILY_IDENTITY_CHANGED")
    else:
        _atomic_json(path, value)


def stamp(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "UTC_REQUIRED")
    return parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def run_metadata(context):
    run = context.get("dag_run")
    require(run is not None, "ACTUAL_DAGRUN_REQUIRED")
    def field(key):
        return run.get(key) if isinstance(run, dict) else getattr(run, key, None)
    kind=field("run_type")
    require(getattr(kind,"value",kind) == "scheduled" and field("conf") in ({}, None), "SCHEDULED_EMPTY_CONF_REQUIRED")
    run_id = context.get("run_id")
    require(isinstance(run_id, str) and re.fullmatch(r"scheduled__\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", run_id)
            and field("run_id") == run_id and field("dag_id") == DAG, "EXACT_SCHEDULED_OWNER_REQUIRED")
    start, end = (stamp(context[k]) for k in ("data_interval_start", "data_interval_end"))
    for key, value in (("data_interval_start", start), ("data_interval_end", end)):
        require(field(key) is not None and stamp(field(key)) == value, "DAGRUN_INTERVAL_CHANGED")
    a, b = (datetime.fromisoformat(x.replace("Z", "+00:00")) for x in (start, end))
    require(b-a == timedelta(days=1) and b <= datetime.now(timezone.utc)
            and (b.hour > 0 or b.minute >= 15) and b.second == b.microsecond == 0, "CLOSED_DAILY_INTERVAL_REQUIRED")
    return {"dag_id": DAG, "run_id": run_id, "run_type": "scheduled", "data_interval_start": start,
            "data_interval_end": end, "conf": {}}


def head(env):
    path = root(env) / "published.json"
    return read(path) if path.exists() else None


def attach(plan, context, environment=None):
    env = os.environ if environment is None else environment
    require(enabled(env), "DAILY_NOT_ENABLED")
    run = run_metadata(context); require(plan["run_id"] == run["run_id"], "PLAN_OWNER_CHANGED")
    code = env.get("CURRENT_MONTH_CODE_VERSION", "")
    runner = env.get("CURRENT_MONTH_RUNNER_SHA256", "")
    require(re.fullmatch(r"[a-f0-9]{40}", code) and re.fullmatch(r"[a-f0-9]{64}",runner), "PINNED_CODE_VERSION_REQUIRED")
    op = "cm1-" + hashlib.sha256((DAG+"|"+run["run_id"]).encode()).hexdigest()[:40]
    folder = root(env) / op
    if (folder / "context.json").exists():
        saved = read(folder / "context.json")
        require(saved["run_context"] == run and saved["code_version"] == code and saved["runner_sha256"] == runner
                and saved["profile"] == plan["profile"], "DAILY_IDENTITY_CHANGED")
    else:
        prior = head(env)
        if prior is not None:
            require(set(prior) == {"operation_id", "registry_sha256", "publication_sha256", "event_cutoff_utc"}
                    and re.fullmatch(r"cm1-[a-f0-9]{40}", prior["operation_id"])
                    and all(re.fullmatch(r"[a-f0-9]{64}", prior[k]) for k in ("registry_sha256", "publication_sha256")), "PINNED_BASELINE_REQUIRED")
            require(prior["event_cutoff_utc"] <= run["data_interval_end"][:10]+"T00:00:00Z", "BASELINE_CUTOFF_REGRESSION")
        saved = {"contract_version": VERSION, "operation_id": op, "run_context": run,
                 "code_version": code, "runner_sha256":runner,"profile": plan["profile"], "baseline": prior}
        saved["context_sha256"] = digest(saved)
        immutable(folder / "context.json", saved)
    require(saved["context_sha256"] == digest({k:v for k,v in saved.items() if k != "context_sha256"}), "DAILY_CONTEXT_CHANGED")
    request = {"contract_version": "current-month-source-silver-v1", "run_context": run,
               "baseline": None if saved["baseline"] is None else {k:saved["baseline"][k] for k in ("operation_id","registry_sha256")}}
    immutable(folder / "source-request.json", request)
    return {**plan, "current_month": saved}


def folder_for(plan, env):
    c = plan["current_month"]; path = root(env) / c["operation_id"]
    require(read(path / "context.json") == c and c["context_sha256"] == digest({k:v for k,v in c.items() if k != "context_sha256"}), "DAILY_CONTEXT_CHANGED")
    return path


def assert_owner(plan, env):
    lease("assert", {"dag_id": DAG, "run_id": plan["run_id"]}, env)
    folder_for(plan, env)
    require(plan["current_month"]["runner_sha256"] == env.get("CURRENT_MONTH_RUNNER_SHA256")
            and plan["current_month"]["code_version"] == env.get("CURRENT_MONTH_CODE_VERSION"), "DAILY_RUNTIME_CHANGED")


def invoke(phase, path, folder, environment):
    require(phase in {"capture", "silver", "gold", "audit"}, "PHASE_REQUIRED")
    spark = Path("/opt/spark/bin/spark-submit")
    require(spark.is_file(), "SPARK_NOT_PREPARED")
    jars = sorted(Path("/opt/spark/jobs/iceberg").glob("*.jar"))
    require(len(jars) == 2, "ICEBERG_NOT_PREPARED")
    runner = Path("/opt/spark/jobs/japan-earthquake-etl-runner.jar")
    require(runner.is_file() and hashlib.sha256(runner.read_bytes()).hexdigest() == environment.get("CURRENT_MONTH_RUNNER_SHA256"), "RUNNER_ARTIFACT_CHANGED")
    command = [str(spark), "--master", "local[1]", "--driver-memory", "768m", "--conf", "spark.driver.host=127.0.0.1",
               "--conf", "spark.driver.bindAddress=127.0.0.1", "--conf", "spark.ui.enabled=false", "--jars", ",".join(map(str,jars)),
               "--class", "ie212.earthquake.spark.CurrentMonthGoldJob" if phase == "gold" else "ie212.earthquake.spark.CurrentMonthDailyJob",
               "/opt/spark/jobs/japan-earthquake-etl-runner.jar"]
    command += [str(path)] if phase == "gold" else [phase, str(path)]
    timeout = int(environment.get("CURRENT_MONTH_PHASE_TIMEOUT_SECONDS", "1800"))
    require(1 <= timeout <= 7200, "BOUNDED_TIMEOUT_REQUIRED")
    return _invoke_command(phase, command, folder, environment, timeout)


def _invoke_command(phase, command, folder, environment, timeout):
    """Keep every attempt's private logs; a failed phase never releases its lease.

    A timeout is not proof that every descendant/external writer is quiescent.
    Recovery still requires the operator checks, not automatic task replay.
    """
    require(phase in {"capture", "silver", "gold", "audit"}, "PHASE_REQUIRED")
    require(not any(p.is_symlink() for p in (folder, *folder.parents)), "UNSAFE_DAILY_ROOT")
    attempts = folder / "phase-attempts"
    require(not attempts.is_symlink(), "UNSAFE_DAILY_ROOT")
    attempts.mkdir(parents=True, exist_ok=True)
    attempt = Path(tempfile.mkdtemp(prefix=phase+"-", dir=attempts))
    with (attempt / "stdout.private").open("w+b") as out, (attempt / "stderr.private").open("wb") as err:
        os.chmod(out.name,0o600);os.chmod(err.name,0o600)
        try:
            result = subprocess.run(command, cwd="/opt/pipeline", env=dict(environment), stdout=out, stderr=err,
                                    timeout=timeout, check=False, shell=False)
        except subprocess.TimeoutExpired:
            immutable(attempt / "outcome.json", {"phase":phase,"state":"FAILED_LEASE_RETAINED",
                                                 "reason":"ASOF_PHASE_TIMEOUT_LEASE_RETAINED","timeout_seconds":timeout})
            raise RuntimeError("ASOF_PHASE_TIMEOUT_LEASE_RETAINED") from None
        except BaseException:
            immutable(attempt / "outcome.json", {"phase":phase,"state":"FAILED_LEASE_RETAINED",
                                                 "reason":"ASOF_PHASE_INTERRUPTED_LEASE_RETAINED"})
            raise
        reason = "ASOF_PHASE_NONZERO_LEASE_RETAINED" if result.returncode != 0 else (
            "ASOF_PHASE_OUTPUT_GUARD_LEASE_RETAINED" if out.tell() > 1048576 else None)
        if reason is not None:
            immutable(attempt / "outcome.json", {"phase":phase,"state":"FAILED_LEASE_RETAINED","reason":reason})
            raise RuntimeError(reason)
        try:
            out.seek(0); value = json.load(out, object_pairs_hook=_unique_json_object)
        except (ValueError, UnicodeError, EtlContractError):
            immutable(attempt / "outcome.json", {"phase":phase,"state":"FAILED_LEASE_RETAINED",
                                                 "reason":"ASOF_PHASE_INVALID_JSON_LEASE_RETAINED"})
            raise RuntimeError("ASOF_PHASE_INVALID_JSON_LEASE_RETAINED") from None
        immutable(attempt / "outcome.json", {"phase":phase,"state":"RETURNED_JSON"})
        return value


def capture(plan, environment=None):
    env = dict(os.environ if environment is None else environment);assert_owner(plan,env);folder = folder_for(plan,env)
    c = plan["current_month"]
    require(head(env) == c["baseline"] or (folder / "summary.json").exists(), "BASELINE_CHANGED_BEFORE_CAPTURE")
    result = invoke("capture",folder / "source-request.json",folder,env)
    require(result.get("contract_version") == "current-month-capture-receipt-v1" and result.get("operation_id") == c["operation_id"]
            and result.get("selection_sha256") == digest(result["selection"])
            and result["selection"]["request"] == read(folder / "source-request.json"), "CAPTURE_RECEIPT_CHANGED")
    immutable(folder / "capture.json",result);return result


def sources(plan, jma, usgs, environment=None):
    env = os.environ if environment is None else environment;assert_owner(plan,env);folder = folder_for(plan,env)
    expected = {(a["year"],a["segment"]) for a in plan["jma"]["archives"]}
    require(len(jma["archives"]) == len(expected) and {(a["year"],a["segment"]) for a in jma["archives"]} == expected
            and all(_ready(a) and a["run_id"] == plan["run_id"] and re.fullmatch(r"[a-f0-9]{64}", str(a.get("manifest_sha256")))
                    and a.get("manifest_uri", "").startswith("s3://") for a in jma["archives"]), "HISTORICAL_JMA_READINESS_REQUIRED")
    require(read(folder / "capture.json") == usgs, "CAPTURE_RECEIPT_CHANGED")
    result = {"contract_version": VERSION, "status": "SourcesReady", "verified": True, "published": False,
              "run_id": plan["run_id"], "operation_id": plan["current_month"]["operation_id"],
              "context_sha256": plan["current_month"]["context_sha256"], "jma_role": "HISTORICAL_READINESS_ONLY",
              "jma": jma, "capture_sha256": digest(usgs), "required_event_sources": ["USGS"]}
    immutable(folder / "sources.json",result);return result


def execute(plan, summary, environment=None):
    env = dict(os.environ if environment is None else environment);assert_owner(plan,env);folder = folder_for(plan,env)
    require(read(folder / "sources.json") == summary, "SOURCES_CHANGED")
    try:
        silver = invoke("silver",folder / "source-request.json",folder,env)
        require(silver.get("operation_id") == plan["current_month"]["operation_id"] and silver.get("status") == "ObservedAsOfReady"
                and silver.get("reconciliation", {}).get("balanced") is True, "SILVER_REQUIRED")
        immutable(folder / "silver.json",silver)
        prior = plan["current_month"]["baseline"]
        request = {"contract_version": "current-month-gold-v1", "operation_id": silver["operation_id"], "registry_sha256": silver["registry_sha256"],
                   "namespace": env["ICEBERG_CATALOG_NAME"]+".operational_current_month", "code_version": plan["current_month"]["code_version"],
                   "baseline_publication": None if prior is None else {k:prior[k] for k in ("operation_id","publication_sha256")}}
        immutable(folder / "gold-request.json",request);assert_owner(plan,env)
        gold = invoke("gold",folder / "gold-request.json",folder,env)
        require(gold.get("operation_id") == silver["operation_id"] and gold.get("status") == "Published" and gold.get("published") is True
                and gold.get("publication_quality") == "PROVISIONAL" and gold.get("coverage_mode") == "OBSERVED_AS_OF"
                and gold.get("ml_upstream_eligible") is False and gold.get("research_ready") is False
                and set(gold.get("snapshot_bundle",{})) == TABLES, "PROVISIONAL_PUBLICATION_REQUIRED")
        immutable(folder / "gold.json",gold)
        result = {"contract_version": VERSION, "operation_id": gold["operation_id"], "context_sha256": plan["current_month"]["context_sha256"],
                  "source_summary_sha256": digest(summary), "silver_sha256": digest(silver), "gold_sha256": digest(gold),
                  "published": True, "status": "Published", "publication_quality": "PROVISIONAL", "coverage_mode": "OBSERVED_AS_OF",
                  "research_ready": False, "ml_upstream_eligible": False, "publication_uri": gold["publication_uri"],
                  "verification_report_uri": gold["publication_uri"]}
        immutable(folder / "summary.json",result)
        immutable(folder / "execution.json",{"state":"COMPLETE","summary_sha256":digest(result)})
        return {"context":plan["current_month"],"summary":result}
    except BaseException:
        _atomic_json(folder / "failure.json",{"state":"FAILED_LEASE_RETAINED","reason":"ASOF_DAILY_INCOMPLETE"})
        raise RuntimeError("ASOF_DAILY_INCOMPLETE_LEASE_RETAINED") from None


def release(result, environment=None):
    env = os.environ if environment is None else environment;c = result["context"];folder = root(env) / c["operation_id"]
    require(read(folder / "context.json") == c and read(folder / "summary.json") == result["summary"]
            and read(folder / "execution.json") == {"state":"COMPLETE","summary_sha256":digest(result["summary"])}, "OUTCOME_UNCONFIRMED_LEASE_RETAINED")
    identity = {"dag_id":DAG,"run_id":c["run_context"]["run_id"]};lease("assert",identity,env)
    silver = read(folder / "silver.json");gold = read(folder / "gold.json")
    new = {"operation_id":c["operation_id"],"registry_sha256":silver["registry_sha256"],
           "publication_sha256":gold["publication_sha256"],"event_cutoff_utc":silver["event_cutoff_utc"]}
    require(head(env) in (c["baseline"],new), "PUBLISHED_HEAD_CHANGED_LEASE_RETAINED")
    _atomic_json(root(env) / "published.json",new)
    return lease("release",identity,env)
