"""ORC-03 telemetry binding. Preview stays pure; only validated metadata is persisted."""
from datetime import datetime, timezone, timedelta
import json
import os

import backfill_runtime as runtime
import etl_pipeline_runtime as etl
import run_observability as obs
from source_run_guard import lease


def context(plan, run_id):
    runtime.validate_plan(plan)
    runtime.require(plan["preview"] is False, "PREVIEW_CANNOT_EXECUTE")
    intervals = list(plan["usgs"])
    if plan["jma"]:
        intervals.append(plan["jma"]["run_context"])
    for pin in plan["bronze_inputs"]:
        if pin["source_system"] == "USGS":
            intervals.append(pin)
        else:
            year, segment = pin["year"], pin["segment"]
            start_month = 10 if segment == "oct-dec" else 1
            stop = datetime(year, 10, 1) if segment == "jan-sep" else datetime(year + 1, 1, 1)
            def utc(value):
                return value.replace(tzinfo=timezone(timedelta(hours=9))).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
            intervals.append({"window_start_utc": utc(datetime(year, start_month, 1)), "window_end_utc": utc(stop)})
    if plan["reprocess"]:
        intervals = [plan["reprocess"]["etl_context"]]
    runtime.require(bool(intervals), "INVALID_UTC_INTERVAL")
    identity = {"dag_id": "orc_03_backfill", "run_id": obs.label(run_id), "mode": "real",
                "config_version": plan["config_version"], "processing_date": plan["processing_date"],
                "window_start_utc": min(item["window_start_utc"] for item in intervals),
                "window_end_utc": max(item["window_end_utc"] for item in intervals), "is_backfill": True,
                "operation_id": plan["operation_id"], "scope_sha256": plan["scope_sha256"], "action": plan["action"]}
    identity["context_sha256"] = runtime.digest(identity)
    return obs.identity(identity)


def resolve_observed(airflow_context, environment=None, attempt=1):
    # Resolve first: preview must not create a journal, lock, lease or summary.
    try:
        plan = runtime.resolve(airflow_context, environment)
    except Exception as error:
        raise obs.ObservabilityError(obs.safe_reason(error)) from None
    if not plan["preview"]:
        resolved = context(plan, airflow_context["run_id"])
        obs.observe(obs.unresolved_context(airflow_context, "orc_03_backfill"), "resolve", lambda: plan,
                    lambda value: {"resolved_context_sha256": resolved["context_sha256"]}, environment, attempt)
    return plan


def readback_details(selected, report):
    runtime.require(isinstance(report, dict) and set(report) == {"version", "bronze_inputs"}
                    and report["version"] == obs.VERSION, "INVALID_READBACK_REPORT")
    rows = report["bronze_inputs"]
    runtime.require(isinstance(rows, list) and len(rows) == len(selected), "INVALID_READBACK_REPORT")
    for pin, row in zip(selected, rows):
        runtime.require(isinstance(row, dict) and set(row) == set(pin) | {"raw_object_uri", "record_count_estimate"}
                        and all(row.get(key) == value for key, value in pin.items()), "INVALID_READBACK_REPORT")
        obs.uri(row["raw_object_uri"], "real")
        obs.number(row["record_count_estimate"])
    return rows


def source_lease(plan, run_id, action, environment=None, attempt=1):
    resolved = context(plan, run_id)
    def operation():
        result = lease(action, {"dag_id": resolved["dag_id"], "run_id": run_id}, environment)
        obs.require(result["status"] == ("ACQUIRED" if action == "acquire" else "RELEASED"),
                    "SOURCE_LEASE_NOT_RELEASED")
        return result
    return obs.observe(resolved, "lease" if action == "acquire" else "cleanup", operation,
                       lambda result: {}, environment, attempt)


def complete(plan, run_id, result, released, environment=None, attempt=1):
    env = os.environ if environment is None else environment
    resolved = context(plan, run_id)
    def operation():
        obs.require(released.get("status") == "RELEASED", "SOURCE_LEASE_NOT_RELEASED")
        summary = obs.read_summary(resolved, env)
        required = {"resolve", "lease", "source", "execution", "cleanup"}
        if plan["action"] == "reprocess":
            required.update(etl.PHASES)
        obs.require(required <= {p["phase"] for p in summary["phases"] if p["status"] == "SUCCEEDED"},
                    "MISSING_UPSTREAM_GATE")
        execution = next(p for p in summary["phases"] if p["phase"] == "execution")
        obs.require(execution["receipt_sha256"] == runtime.digest(result), "PUBLICATION_INPUT_CHANGED")
        target = runtime.root(plan, env) / "run_summary.json"
        obs.require(target.stat().st_size <= etl.MAX_METADATA_BYTES and json.loads(target.read_text()) == result,
                    "BACKFILL_COMPLETION_FAILED")
        runtime.keys(result, {"operation_id", "scope_sha256", "status", "verified", "published", "bronze_inputs", "publication"})
        expected = "Published" if plan["action"] == "reprocess" else "BronzeVerified"
        obs.require(result["scope_sha256"] == plan["scope_sha256"] and result["operation_id"] == plan["operation_id"]
                    and result["verified"] is True and result["status"] == expected
                    and result["published"] is (plan["action"] == "reprocess"), "BACKFILL_COMPLETION_FAILED")
        pins = [pin for row in summary["sources"] for pin in row.get("bronze_pins", [])]
        obs.require(sorted(pins, key=lambda x: x["manifest_uri"]) == sorted(result["bronze_inputs"], key=lambda x: x["manifest_uri"]),
                    "BRONZE_INPUT_CHANGED")
        if plan["action"] == "reprocess":
            publication = result["publication"]
            obs.require(publication["run_id"] == plan["reprocess"]["etl_context"]["run_id"]
                        and publication["context_sha256"] == plan["reprocess"]["etl_context"]["context_sha256"]
                        and publication["publication_status"] == "Published" and publication["published"] is True
                        and publication["snapshots"] == summary["snapshots"]
                        and publication["publication_uri"] == summary["publication_uri"]
                        and publication["verification_report_uri"] == summary["verification_report_uri"], "PUBLICATION_INPUT_CHANGED")
        else:
            obs.require(result["publication"] is None, "PUBLICATION_MODE_MISMATCH")
        return result
    return obs.observe(resolved, "complete", operation,
                       lambda value: {"completion_status": value["status"]}, env, attempt)
