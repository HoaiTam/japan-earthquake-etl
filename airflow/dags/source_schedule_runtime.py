"""ORC-02 scheduling/readiness metadata. Source bytes and validation stay in Java."""
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from zoneinfo import ZoneInfo

from jma_backfill_runtime import JmaRunnerError, _atomic_json, _ready, execute_archive, resolve_plan
from usgs_ingest_runtime import execute_phase, require_bronze_ready, resolve_run_context


class ReadinessError(RuntimeError):
    pass


def profile(environment=None):
    env = os.environ if environment is None else environment
    mode = env.get("SOURCE_SCHEDULE_PROFILE", "multi-source")
    if mode not in {"multi-source", "usgs-only"}:
        raise ReadinessError("SOURCE_SCHEDULE_PROFILE must be multi-source or usgs-only")
    tz = env.get("PIPELINE_TIMEZONE", "Asia/Ho_Chi_Minh")
    ZoneInfo(tz)
    cron = env.get("PIPELINE_SCHEDULE_CRON", "15 7 * * *")
    match = re.fullmatch(r"(\d{1,2}) (\d{1,2}) \* \* \*", cron)
    if tz != "Asia/Ho_Chi_Minh" or not match:
        raise ReadinessError("daily profile requires Asia/Ho_Chi_Minh and one daily hour/minute")
    minute, hour = map(int, match.groups())
    if not (0 <= minute < 60 and 7 <= hour <= 23 and (hour > 7 or minute >= 15)):
        raise ReadinessError("daily run must be at/after 00:15 UTC on the processing boundary")
    try:
        years = json.loads(env.get("JMA_READINESS_YEARS", "[1997,2000,2023]"))
        audit = int(env.get("JMA_CHECKSUM_AUDIT_WEEKDAY", "6"))
    except ValueError as exc:
        raise ReadinessError("invalid JMA readiness profile") from exc
    if (not isinstance(years, list) or not years
            or any(type(year) is not int or not 1984 <= year <= 2023 for year in years) or not 0 <= audit <= 6):
        raise ReadinessError("explicit JMA watchlist 1984..2023 and audit weekday 0..6 required")
    return {"profile_version": "orc-02-v1", "mode": mode, "timezone": tz, "cron": cron,
            "years": sorted(set(years)), "audit_weekday": audit, "max_active_runs": 1}


def _root(plan, env):
    return Path(env.get("SOURCE_READINESS_ROOT", "/opt/pipeline/staging/readiness")) / plan["run_id_path"]


def resolve_daily(context, environment=None):
    env = os.environ if environment is None else environment
    settings = profile(env)
    dag_run = context.get("dag_run")
    conf = dag_run.get("conf", {}) if isinstance(dag_run, dict) else getattr(dag_run, "conf", {})
    if conf:
        raise ReadinessError("daily conf must be empty; use scoped USGS/JMA backfill DAGs")
    interval = context.get("data_interval_end")
    if interval is None:
        raise ReadinessError("daily run requires a stable data_interval_end")
    end = datetime.fromisoformat(str(interval).replace("Z", "+00:00"))
    if end.tzinfo is None or end.astimezone(timezone.utc) > datetime.now(timezone.utc):
        raise ReadinessError("daily data interval must be timezone-aware and not in the future")
    run_id = context.get("run_id")
    if not isinstance(run_id, str) or not run_id or any(ord(char) < 32 for char in run_id):
        raise ReadinessError("stable run_id required")
    usgs = resolve_run_context({**context, "dag_id": "orc_02_daily_sources"}, env)
    usgs["run_id_path"] = "usgs-" + hashlib.sha256((usgs["dag_id"] + "|" + run_id).encode()).hexdigest()[:32]
    # Java JMA runner consumes exact historical archive scope (is_backfill=true),
    # independent of the USGS target-day scope. This is NOT a Gold partition scope.
    audit = end.astimezone(timezone.utc).weekday() == settings["audit_weekday"]
    jma_context = {**context, "logical_date": end, "dag_run": {"conf": {
        "years": settings["years"], "preview": False, "force_download": audit}}}
    try:
        jma = resolve_plan(jma_context, env)
    except JmaRunnerError as exc:
        raise ReadinessError("JMA daily plan could not be pinned") from exc
    result = {"run_id": run_id, "run_id_path": "daily-" + hashlib.sha256(run_id.encode()).hexdigest()[:32],
              "profile": settings, "data_interval_end": end.astimezone(timezone.utc).isoformat(),
              "usgs": usgs, "jma": jma, "checksum_audit": audit}
    target = _root(result, env) / "plan.json"
    if target.exists() and json.loads(target.read_text()) != result:
        raise ReadinessError("daily plan changed; use a new run_id")
    _atomic_json(target, result)
    return result


def refresh_jma(plan, attempt=1, environment=None):
    env = os.environ if environment is None else environment
    results = []
    for archive in plan["jma"]["archives"]:
        probe = execute_archive(plan["jma"], archive, attempt, env, phase="probe")
        decision = probe.get("readiness_decision")
        invoked = probe.get("status") == "NeedsIngest"
        result = execute_archive(plan["jma"], archive, attempt, env) if invoked else probe
        if (not _ready(result) or not str(result.get("manifest_uri", "")).startswith("s3://")
                or not re.fullmatch(r"[0-9a-f]{64}", str(result.get("manifest_sha256", "")))):
            raise ReadinessError("JMA readiness failed; no downstream handoff")
        results.append({**result, "readiness_decision": decision, "ingest_invoked": invoked})
    _atomic_json(_root(plan, env) / "jma-readiness.json", {"archives": results})
    return {"archives": results}


def ingest_usgs(plan, environment=None):
    env = os.environ if environment is None else environment
    if str(env.get("USGS_INGEST_DRY_RUN", "false")).lower() not in {"false", "0"}:
        raise ReadinessError("daily readiness requires real Java/MinIO; dry-run forbidden")
    result = None
    for phase in ("fetch", "validate", "upload", "verify"):
        result = execute_phase(phase, plan["usgs"], result, env)
        if phase == "validate" and result.get("valid") is not True:
            raise ReadinessError("USGS validation failed")
    require_bronze_ready(result)
    if (result.get("run_id") != plan["run_id"] or not str(result.get("manifest_uri", "")).startswith("s3://")
            or not re.fullmatch(r"[0-9a-f]{64}", str(result.get("manifest_sha256", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(result.get("sha256", "")))
            or type(result.get("record_count_estimate")) is not int or result["record_count_estimate"] < 0):
        raise ReadinessError("USGS real readiness identity differs")
    _atomic_json(_root(plan, env) / "usgs-readiness.json", result)
    return result


def ready_summary(plan, jma, usgs, environment=None):
    env = os.environ if environment is None else environment
    expected = {(item["year"], item["segment"]) for item in plan["jma"]["archives"]}
    actual = {(item["year"], item["segment"]) for item in jma["archives"]}
    if (len(jma["archives"]) != len(expected) or actual != expected
            or not all(_ready(item) and item["run_id"] == plan["run_id"] for item in jma["archives"])):
        raise ReadinessError("JMA archive scope is incomplete")
    require_bronze_ready(usgs)
    changed = sorted({item["year"] for item in jma["archives"] if item["ingest_invoked"] and not item["publication_reused"]})
    summary = {"run_id": plan["run_id"], "status": "SourcesReady", "verified": True,
               "published": False, "profile": plan["profile"], "data_interval_end": plan["data_interval_end"],
               "usgs_context": plan["usgs"], "jma_scope": plan["jma"]["years"],
               "checksum_audit": plan["checksum_audit"], "jma_changed_years": changed,
               "input_manifests": [{"source_system": "USGS", "manifest_uri": usgs["manifest_uri"],
                                    "sha256": usgs["manifest_sha256"]}] + [
                   {"source_system": "JMA_BULLETIN", "manifest_uri": item["manifest_uri"],
                    "sha256": item["manifest_sha256"]} for item in jma["archives"]],
               "jma": jma, "usgs": usgs}
    _atomic_json(_root(plan, env) / "run_summary.json", summary)
    return summary
