"""ORC-03 bounded, exact-scope control plane. Payload processing stays in Java."""
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
from urllib.parse import urlsplit

import etl_pipeline_runtime as etl
import jma_backfill_runtime as jma
import usgs_ingest_runtime as usgs


class BackfillError(RuntimeError):
    """Only safe reason codes cross the orchestration boundary."""


USGS_SETTINGS = ("USGS_API_BASE_URL", "USGS_MIN_LATITUDE", "USGS_MAX_LATITUDE", "USGS_MIN_LONGITUDE",
                 "USGS_MAX_LONGITUDE", "USGS_EVENT_TYPE", "USGS_REQUEST_LIMIT", "USGS_MAX_WINDOW_DAYS")


def require(condition, reason):
    if not condition:
        raise BackfillError(reason)


def digest(value):
    return hashlib.sha256(etl._json_bytes(value)).hexdigest()


def keys(value, fields):
    require(isinstance(value, dict) and set(value) == set(fields), "INVALID_BACKFILL_FIELDS")


def label(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._~-]{0,99}", value)
            and ".." not in value, "INVALID_OPERATION_ID")


def pins(value):
    require(isinstance(value, list) and 0 < len(value) <= 64, "BOUNDED_INPUTS_REQUIRED")
    for item in value:
        source = item.get("source_system") if isinstance(item, dict) else None
        extra = {"year", "segment", "catalog_release"} if source == "JMA_BULLETIN" else {
            "window_start_utc", "window_end_utc"}
        keys(item, {"source_system", "manifest_uri", "manifest_sha256", "sha256"} | extra)
        require(source in etl.SOURCES, "UNSUPPORTED_SOURCE")
        etl._uri(item["manifest_uri"], "real")
        require(item["manifest_uri"].endswith("/manifest.json"), "EXACT_MANIFEST_REQUIRED")
        for field in ("sha256", "manifest_sha256"):
            require(isinstance(item[field], str) and re.fullmatch(r"[0-9a-f]{64}", item[field]),
                    "INVALID_PIN_CHECKSUM")
        if source == "JMA_BULLETIN":
            year = jma._year(item["year"])
            require(item["segment"] in ({"jan-sep", "oct-dec"} if year == 1997 else {"full-year"}),
                    "INVALID_JMA_SEGMENT")
            label(item["catalog_release"])
            require(f"/jma/year={year}/catalog_release={item['catalog_release']}/" in item["manifest_uri"],
                    "RELEASE_URI_MISMATCH")
        else:
            start, end = interval(item)
            require(start < end, "INVALID_USGS_SCOPE")
    require(len({item["manifest_uri"] for item in value}) == len(value), "DUPLICATE_INPUT")
    return deepcopy(value)


def interval(value):
    start, end = [datetime.fromisoformat(etl._utc(value[field]).replace("Z", "+00:00"))
                  for field in ("window_start_utc", "window_end_utc")]
    require(start < end <= datetime.now(timezone.utc), "INVALID_UTC_INTERVAL")
    return start, end


def utc(value):
    return value.isoformat().replace("+00:00", "Z")


def resolve(context, environment=None):
    """Pure preview: no source/store access or persistent staging writes."""
    env = os.environ if environment is None else environment
    dag_run = context.get("dag_run")
    conf = dag_run.get("conf", {}) if isinstance(dag_run, dict) else getattr(dag_run, "conf", {})
    require(isinstance(conf, dict), "INVALID_BACKFILL_CONFIGURATION")
    require(not set(conf) - {"operation_id", "processing_date", "action", "preview", "usgs", "jma",
                             "bronze_inputs", "reprocess"}, "UNSUPPORTED_BACKFILL_CONFIGURATION")
    operation = conf.get("operation_id"); label(operation)
    action = conf.get("action")
    require(action in {"ingest", "reuse", "reprocess"}, "EXPLICIT_ACTION_REQUIRED")
    preview = conf.get("preview", True)
    require(type(preview) is bool, "JSON_BOOLEAN_REQUIRED")
    processing = conf.get("processing_date")
    try:
        require(date.fromisoformat(processing).isoformat() == processing, "INVALID_PROCESSING_DATE")
    except (TypeError, ValueError):
        raise BackfillError("INVALID_PROCESSING_DATE") from None
    body = {"contract_version": "orc-03-v1", "operation_id": operation, "action": action,
            "processing_date": processing, "config_version": env.get("CONFIG_VERSION", "1"),
            "bronze_target": {"bucket": env.get("DATA_BUCKET", ""), "prefix": env.get("BRONZE_PREFIX", "")},
            "usgs": [], "jma": None, "bronze_inputs": [], "reprocess": None}
    if body["bronze_target"]["bucket"]:
        require(re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", body["bronze_target"]["bucket"]),
                "INVALID_BUCKET")
    if body["bronze_target"]["prefix"]:
        require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", body["bronze_target"]["prefix"])
                and ".." not in body["bronze_target"]["prefix"]
                and not body["bronze_target"]["prefix"].endswith("/"), "INVALID_BRONZE_PREFIX")
    seed = digest({"conf": {k: v for k, v in conf.items() if k != "preview"},
                   "config_version": body["config_version"]})
    if action == "ingest":
        require("bronze_inputs" not in conf and "reprocess" not in conf
                and ("usgs" in conf or "jma" in conf), "INGEST_SCOPE_REQUIRED")
        if "usgs" in conf:
            scope = conf["usgs"]
            keys(scope, {"window_start_utc", "window_end_utc", "chunk_days"})
            start, end = interval(scope)
            days = scope["chunk_days"]
            try:
                maximum = int(env.get("USGS_MAX_WINDOW_DAYS", "3"))
            except ValueError:
                raise BackfillError("INVALID_USGS_CONFIGURATION") from None
            require(type(days) is int and 1 <= days <= maximum <= 31, "INVALID_CHUNK_SIZE")
            # Bind public request settings to the operation. Never hash/log credentials.
            body["usgs_settings"] = {key: env.get(key, "") for key in USGS_SETTINGS}
            api = urlsplit(body["usgs_settings"]["USGS_API_BASE_URL"])
            require(not body["usgs_settings"]["USGS_API_BASE_URL"] or (
                api.scheme == "https" and api.hostname and not api.username and not api.password
                and not api.query and not api.fragment), "UNSAFE_PUBLIC_API_URL")
            while start < end:
                require(len(body["usgs"]) < 32, "TOO_MANY_USGS_CHUNKS_SPLIT_OPERATION")
                stop = min(start + timedelta(days=days), end)
                child = "bf-usgs-" + digest({"seed": seed, "start": utc(start), "end": utc(stop),
                                             "settings": body["usgs_settings"]})
                child_context = usgs.resolve_run_context({"dag_id": "orc_03_backfill", "run_id": child,
                    "dag_run": {"conf": {"is_backfill": True, "window_start_utc": utc(start),
                                           "window_end_utc": utc(stop)}}}, env)
                child_context["processing_date"] = processing
                body["usgs"].append(child_context)
                start = stop
        if "jma" in conf:
            scope = conf["jma"]
            require(isinstance(scope, dict) and not set(scope) - {"years", "segments", "force_download"}
                    and "years" in scope, "INVALID_JMA_SCOPE")
            child = "bf-jma-" + seed
            # Reuse the JMA-04 planner; its staging side effect is isolated in a temporary directory.
            with tempfile.TemporaryDirectory(prefix="orc03-preview-") as temporary:
                subplan = jma.resolve_plan({"run_id": child, "logical_date": processing + "T00:00:00Z",
                    "dag_run": {"conf": {"years": scope["years"], "preview": False,
                                           "force_download": scope.get("force_download", False)}}},
                    {**env, "JMA_STAGING_ROOT": temporary})
            if "segments" in scope:
                selected = scope["segments"]
                require(isinstance(selected, list) and 0 < len(selected) <= 41, "INVALID_JMA_SEGMENTS")
                for item in selected:
                    keys(item, {"year", "segment"})
                    require(type(item["year"]) is int and isinstance(item["segment"], str),
                            "INVALID_JMA_SEGMENT")
                pairs = {(item["year"], item["segment"]) for item in selected}
                available = {(item["year"], item["segment"]) for item in subplan["archives"]}
                require(len(pairs) == len(selected) and pairs <= available
                        and {item[0] for item in pairs} == set(subplan["years"]), "JMA_SCOPE_CHANGED")
                subplan["archives"] = [item for item in subplan["archives"]
                                       if (item["year"], item["segment"]) in pairs]
                subplan["run_context"]["window_start_utc"] = min(utc(datetime.fromisoformat(
                    item["native_start_jst"]).astimezone(timezone.utc)) for item in subplan["archives"])
                subplan["run_context"]["window_end_utc"] = max(utc(datetime.fromisoformat(
                    item["native_end_jst"]).astimezone(timezone.utc)) for item in subplan["archives"])
            body["jma"] = subplan
    else:
        require("usgs" not in conf and "jma" not in conf, "REUSE_CANNOT_DOWNLOAD")
        body["bronze_inputs"] = pins(conf.get("bronze_inputs"))
        require((action == "reprocess") == ("reprocess" in conf), "EXPLICIT_REPROCESS_SCOPE_REQUIRED")
        if action == "reprocess":
            request = conf["reprocess"]
            keys(request, {"window_start_utc", "window_end_utc", "output_scope", "gold_partitions",
                           "existing_silver_manifests", "baseline_snapshots"})
            real_conf = {"mode": "real", "window_start_utc": request["window_start_utc"],
                "window_end_utc": request["window_end_utc"], "processing_date": processing, "is_backfill": True,
                "input_scope": {"bronze_manifests": [{k: item[k] for k in (
                    "source_system", "manifest_uri", "sha256")} for item in body["bronze_inputs"]]},
                "output_scope": deepcopy(request["output_scope"])}
            # Validate ORC-01 without requiring an adapter for a non-executing preview.
            etl_context = etl.resolve_run_context({"run_id": "bf-etl-" + seed,
                "dag_run": {"conf": real_conf}}, {**env, "ETL_PHASE_RUNNER_COMMAND": "validation-only"})
            tables = etl_context["output_scope"]["gold_tables"]
            partitions = request["gold_partitions"]
            require(isinstance(partitions, list) and 0 < len(partitions) <= 64, "EXPLICIT_GOLD_PARTITIONS_REQUIRED")
            for part in partitions:
                keys(part, {"table", "event_year_utc", "event_month_utc"})
                require(part["table"] in tables and type(part["event_year_utc"]) is int
                        and 1900 <= part["event_year_utc"] <= 2200 and type(part["event_month_utc"]) is int
                        and 1 <= part["event_month_utc"] <= 12, "INVALID_GOLD_PARTITION")
            require({part["table"] for part in partitions} == set(tables)
                    and len({digest(part) for part in partitions}) == len(partitions), "INCOMPLETE_GOLD_SCOPE")
            existing = request["existing_silver_manifests"]
            require(isinstance(existing, list) and len(existing) <= 64, "INVALID_EXISTING_SILVER_SCOPE")
            for item in existing:
                keys(item, {"manifest_uri", "sha256"}); etl._uri(item["manifest_uri"], "real")
                require(isinstance(item["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", item["sha256"]),
                        "INVALID_EXISTING_SILVER_CHECKSUM")
            require(len({i["manifest_uri"] for i in existing}) == len(existing), "DUPLICATE_EXISTING_SILVER")
            snapshots = request["baseline_snapshots"]
            require(isinstance(snapshots, list) and len(snapshots) == len(tables), "EXPLICIT_BASELINE_REQUIRED")
            for item in snapshots:
                keys(item, {"table", "snapshot_id"})
                sid = item["snapshot_id"]
                require(item["table"] in tables and (sid is None or (isinstance(sid, str)
                    and re.fullmatch(r"[1-9][0-9]{0,18}", sid) and int(sid) <= 9223372036854775807)),
                    "INVALID_BASELINE_SNAPSHOT")
            require({i["table"] for i in snapshots} == set(tables), "INCOMPLETE_BASELINE")
            require(not any(i["snapshot_id"] is not None for i in snapshots) or bool(existing),
                    "EXISTING_SILVER_CONTEXT_REQUIRED")
            body["reprocess"] = {**deepcopy(request), "etl_context": etl_context}
    require(len(body["usgs"]) + (len(body["jma"]["archives"]) if body["jma"] else 0) <= 64,
            "TOO_MANY_TOTAL_INPUTS_SPLIT_OPERATION")
    body["scope_sha256"] = digest(body)
    return {**body, "preview": preview}


def validate_plan(plan):
    label(plan["operation_id"])
    require(plan.get("scope_sha256") == digest({k: v for k, v in plan.items()
            if k not in {"scope_sha256", "preview"}}), "PLAN_CHANGED")


def root(plan, env):
    return Path(env.get("BACKFILL_STAGING_ROOT", "/opt/pipeline/staging/backfill")) / digest(plan["operation_id"])


def pin_operation(plan, environment=None):
    env = os.environ if environment is None else environment
    validate_plan(plan); require(plan["preview"] is False, "PREVIEW_CANNOT_EXECUTE")
    require(plan["config_version"] == env.get("CONFIG_VERSION", "1")
            and plan["bronze_target"] == {"bucket": env.get("DATA_BUCKET", ""), "prefix": env.get("BRONZE_PREFIX", "")}
            and all(plan["bronze_target"].values()), "EXECUTION_ENVIRONMENT_CHANGED_OR_UNRESOLVED")
    if plan["usgs"]:
        require(plan["usgs_settings"] == {key: env.get(key, "") for key in USGS_SETTINGS},
                "USGS_EXECUTION_SETTINGS_CHANGED")
    if plan["action"] == "reprocess":
        require(bool(env.get("BACKFILL_ETL_RUNNER_COMMAND", "").strip()), "SCOPED_ETL_ADAPTER_NOT_CONFIGURED")
    if plan["usgs"]:
        require(env.get("USGS_INGEST_DRY_RUN", "false").lower() in {"false", "0"}, "DRY_RUN_FORBIDDEN")
    target = root(plan, env) / "plan.json"
    immutable = {k: v for k, v in plan.items() if k != "preview"}
    if target.exists():
        require(json.loads(target.read_text()) == immutable, "OPERATION_SCOPE_CHANGED_USE_NEW_ID")
    else:
        jma._atomic_json(target, immutable)
    # Invalidate stale success before any side effect. Each execution must read back anew.
    jma._atomic_json(root(plan, env) / "run_summary.json", {"status": "RUNNING", "published": False,
                    "scope_sha256": plan["scope_sha256"]})


def invoke(command, request, target, env):
    require(bool(command.strip()), "ADAPTER_NOT_CONFIGURED")
    etl._json_bytes(request)  # Bound the entire metadata request before writing or spawning.
    jma._atomic_json(target, request)
    try:
        timeout = int(env.get("BACKFILL_RUNNER_TIMEOUT_SECONDS", "3600"))
        argv = shlex.split(command)
        require(bool(argv) and 1 <= timeout <= 86400, "INVALID_ADAPTER_CONFIGURATION")
        with tempfile.TemporaryFile(dir=target.parent) as output:
            process = subprocess.run(argv + ["--context-file", str(target)], stdout=output,
                stderr=subprocess.DEVNULL, check=False, shell=False, timeout=timeout, env=dict(env))
            require(process.returncode == 0, "BACKFILL_ADAPTER_FAILED")
            require(output.tell() <= etl.MAX_METADATA_BYTES, "METADATA_TOO_LARGE")
            output.seek(0)
            return json.load(output, object_pairs_hook=etl._unique_json_object)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        raise BackfillError("BACKFILL_ADAPTER_DID_NOT_COMPLETE") from None


def verify_pinned(plan, selected, env, metadata=None):
    pins(selected)
    request = {"contract_version": "orc-03-v1", "scope_sha256": plan["scope_sha256"], "bronze_inputs": selected}
    if metadata is not None:
        request["observability_version"] = "orc-04-v1"
    result = invoke(env.get("BRONZE_REUSE_RUNNER_COMMAND", "/opt/pipeline/bin/bronze-reuse-verifier"), request,
        root(plan, env) / "bronze-verify-input.json", env)
    has_report = "observability" in result
    report = result.pop("observability", None)
    keys(result, {"contract_version", "scope_sha256", "status", "verified", "bronze_inputs"})
    require(result["verified"] is True and result == {"contract_version": "orc-03-v1", "scope_sha256": plan["scope_sha256"],
        "status": "BronzeVerified", "verified": True, "bronze_inputs": selected}, "BRONZE_PIN_NOT_VERIFIED")
    if has_report:
        require(metadata is not None, "INVALID_READBACK_REPORT")
        from backfill_observability import readback_details
        metadata.extend(readback_details(selected, report))
    return selected


def execute_sources(plan, environment=None, attempt=1, metadata=None):
    env = os.environ if environment is None else environment
    validate_plan(plan); require(plan["preview"] is False, "PREVIEW_CANNOT_EXECUTE")
    selected = deepcopy(plan["bronze_inputs"])
    for context in plan["usgs"]:
        result = None
        for phase in usgs.PHASES:
            result = usgs.execute_phase(phase, context, result, env)
            if phase == "validate":
                require(result.get("valid") is True, "USGS_VALIDATION_FAILED")
        usgs.require_bronze_ready(result)
        require(result.get("run_id") == context["run_id"], "USGS_IDENTITY_CHANGED")
        selected.append({"source_system": "USGS", "manifest_uri": result["manifest_uri"],
            "manifest_sha256": result["manifest_sha256"], "sha256": result["sha256"],
            "window_start_utc": context["window_start_utc"], "window_end_utc": context["window_end_utc"]})
    if plan["jma"]:
        # JMA-04 workflow API and Java runner, including archive cache and immutable publication reuse.
        for archive in plan["jma"]["archives"]:
            result = jma.execute_archive(plan["jma"], archive, attempt, env)
            require(jma._ready(result), "JMA_ARCHIVE_NOT_READY")
            selected.append({"source_system": "JMA_BULLETIN", "manifest_uri": result["manifest_uri"],
                "manifest_sha256": result["manifest_sha256"], "sha256": result["sha256"],
                "year": archive["year"], "segment": archive["segment"], "catalog_release": result["catalog_release"]})
        jma.require_complete(jma.write_run_summary(plan["jma"], env))
    verified = verify_pinned(plan, selected, env, metadata) if metadata is not None else verify_pinned(plan, selected, env)
    result = {"status": "BronzeVerified", "scope_sha256": plan["scope_sha256"], "bronze_inputs": verified}
    jma._atomic_json(root(plan, env) / "bronze-result.json", result)
    return result


def execute_reprocess(plan, bronze, environment=None, telemetry_context=None, attempt=1):
    env = os.environ if environment is None else environment
    validate_plan(plan); require(plan["preview"] is False, "PREVIEW_CANNOT_EXECUTE")
    require(bronze == {"status": "BronzeVerified", "scope_sha256": plan["scope_sha256"],
                      "bronze_inputs": plan["bronze_inputs"]}, "REPROCESS_INPUT_CHANGED")
    request = plan["reprocess"]; require(request is not None, "REPROCESS_SCOPE_REQUIRED")
    upstream = None
    for phase in etl.PHASES:
        def operation():
            result = invoke(env.get("BACKFILL_ETL_RUNNER_COMMAND", ""),
                {"contract_version": "orc-03-v1", "phase": phase, "plan": plan, "bronze": bronze,
                 "upstream": upstream, "upstream_sha256": digest(upstream) if upstream else None},
                root(plan, env) / f"{phase}-input.json", env)
            keys(result, {"etl_receipt", "scope_receipt"})
            expected = {"contract_version": "orc-03-v1", "scope_sha256": plan["scope_sha256"],
                        "operation_id": plan["operation_id"], "phase": phase,
                        "baseline_verified": True, "idempotency_verified": True, "outside_scope_unchanged": True}
            require(isinstance(result["scope_receipt"], dict) and result["scope_receipt"] == expected
                    and all(result["scope_receipt"].get(field) is True for field in (
                        "baseline_verified", "idempotency_verified", "outside_scope_unchanged")), "SCOPED_WRITE_GATE_FAILED")
            etl.validate_result(phase, request["etl_context"], upstream["etl_receipt"] if upstream else None,
                                result["etl_receipt"])
            return result
        if telemetry_context is not None:
            import run_observability as obs
            result = obs.observe(telemetry_context, phase, operation,
                lambda value: obs.phase_details(phase, request["etl_context"], value["etl_receipt"]), env, attempt)
        else:
            result = operation()
        upstream = result
    return etl.publication_summary(request["etl_context"], upstream["etl_receipt"])


def execute(plan, environment=None, attempt=1, telemetry_context=None):
    """Caller holds the whole-run shared source lease. No runtime mock mode."""
    env = os.environ if environment is None else environment
    validate_plan(plan); require(plan["preview"] is False, "PREVIEW_CANNOT_EXECUTE")
    try:
        if telemetry_context is not None:
            from backfill_observability import context
            require(telemetry_context == context(plan, telemetry_context["run_id"]), "PLAN_CHANGED")
        if telemetry_context is None:
            pin_operation(plan, env)
            bronze = execute_sources(plan, env, attempt)
        else:
            import run_observability as obs
            metadata = []
            def source():
                from source_run_guard import lease
                lease("assert", {"dag_id": telemetry_context["dag_id"], "run_id": telemetry_context["run_id"]}, env)
                pin_operation(plan, env)
                return execute_sources(plan, env, attempt, metadata)
            bronze = obs.observe(telemetry_context, "source", source,
                lambda value: {"bronze_pins": value["bronze_inputs"],
                               **({"bronze_inputs": metadata} if metadata else {})}, env, attempt)
        publication = (execute_reprocess(plan, bronze, env, telemetry_context, attempt)
                       if plan["action"] == "reprocess" else None)
        summary = {"operation_id": plan["operation_id"], "scope_sha256": plan["scope_sha256"],
                   "status": "Published" if publication else "BronzeVerified", "verified": True,
                   "published": publication is not None, "bronze_inputs": bronze["bronze_inputs"],
                   "publication": publication}
        def save():
            jma._atomic_json(root(plan, env) / "run_summary.json", summary)
            return summary
        if telemetry_context is not None:
            return obs.observe(telemetry_context, "execution", save,
                               lambda value: {"receipt_sha256": digest(value)}, env, attempt)
        return save()
    except Exception:
        if (root(plan, env) / "plan.json").exists():
            jma._atomic_json(root(plan, env) / "run_summary.json", {"operation_id": plan["operation_id"],
                "scope_sha256": plan["scope_sha256"], "status": "FAILED", "published": False,
                "reason": "BACKFILL_INCOMPLETE"})
        raise


def preview_summary(plan):
    validate_plan(plan)
    return {"status": "PREVIEW", "verified": False, "published": False, "plan": plan,
            "writes": ("Bronze only; no Silver/Gold write" if plan["action"] == "ingest" else
                       "none; exact readback only" if plan["action"] == "reuse" else plan["reprocess"]),
            "adapter_ready": None, "note": "Offline preview does not verify objects or prove coverage; host preview uses example settings."}
