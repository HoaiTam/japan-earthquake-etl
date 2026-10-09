"""ORC-04 bounded control-plane telemetry. Never reads business records.

Only validated, explicitly projected metadata enters the journal/log. Counts
are scoped gauges, not incrementing counters: retries must not double them.
"""
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import tempfile
import time
from urllib.parse import urlsplit

VERSION = "orc-04-v1"
PHASES = ("resolve", "readiness", "bronze", "silver", "gold", "verify", "publish", "complete")
BACKFILL_PHASES = ("resolve", "lease", "source", *PHASES[1:-1], "execution", "cleanup", "complete")
SOURCES = ("JMA_BULLETIN", "USGS")
COUNT_FIELDS = ("input", "fetched", "parsed", "parse_error", "ignored", "valid", "rejected",
                "duplicate", "superseded", "current", "linked", "unlinked")
MAX_BYTES = 262144
LOGGER = logging.getLogger("earthquake.run_summary")


class ObservabilityError(RuntimeError):
    """Only fixed, non-sensitive reason codes may leave this boundary."""


def require(condition, reason):
    if not condition:
        raise ObservabilityError(reason)


def label(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:~+|/@=-]{0,249}", value),
            "INVALID_TELEMETRY_LABEL")
    return value


def uri(value, mode):
    require(isinstance(value, str) and len(value) <= 1024, "INVALID_TELEMETRY_URI")
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise ObservabilityError("INVALID_TELEMETRY_URI") from None
    require(parsed.scheme == ("mock" if mode == "mock" else "s3") and parsed.netloc
            and parsed.path.strip("/") and not parsed.query and not parsed.fragment
            and not parsed.username and not parsed.password
            and all(re.fullmatch(r"[A-Za-z0-9._=/~-]+", part) for part in (parsed.netloc, parsed.path))
            and not any(part in {".", ".."} for part in parsed.path.split("/")), "INVALID_TELEMETRY_URI")
    return value


def number(value):
    require(type(value) is int and 0 <= value <= 9223372036854775807, "INVALID_TELEMETRY_COUNT")
    return value


def validate_counts(report, phase, context, upstream=None):
    """Optional versioned receipt extension; absence means not_reported, not zero.

Source rows describe ALL records considered in this exact replacement scope.
Linked means current observation memberships, never candidate/link pairs.
"""
    require(isinstance(report, dict) and set(report) == {"version", "sources", "gold"}
            and report["version"] == VERSION, "INVALID_COUNT_REPORT")
    require(phase in {"silver", "gold", "verify", "publish"}, "UNEXPECTED_COUNT_REPORT")
    rows = report["sources"]
    require(isinstance(rows, list), "INVALID_COUNT_REPORT")
    if phase == "silver":
        require(len(rows) == 2 and all(isinstance(row, dict) and isinstance(row.get("source_system"), str)
                                      for row in rows)
                and {row["source_system"] for row in rows}
                == set(SOURCES) and report["gold"] is None, "INCOMPLETE_SOURCE_COUNTS")
        bronze = upstream["artifacts"]["bronze_inputs"] if upstream else []
        for row in rows:
            require(isinstance(row, dict) and set(row) == {"source_system", "counts", "reject_reasons",
                                                         "catalog_releases"}, "INVALID_COUNT_REPORT")
            counts = row["counts"]
            require(isinstance(counts, dict) and set(counts) == set(COUNT_FIELDS), "INVALID_COUNT_REPORT")
            for key, value in counts.items():
                if key == "fetched" and value is None:  # Bronze reuse is not a network fetch.
                    continue
                number(value)
            require(counts["fetched"] is None or counts["fetched"] <= counts["input"],
                    "COUNT_RECONCILIATION_FAILED")
            require(counts["input"] == counts["parsed"] + counts["parse_error"] + counts["ignored"]
                    and counts["parsed"] == counts["valid"] + counts["rejected"]
                    and counts["valid"] == counts["duplicate"] + counts["superseded"] + counts["current"]
                    and counts["current"] == counts["linked"] + counts["unlinked"], "COUNT_RECONCILIATION_FAILED")
            if bronze:
                expected = sum(item["record_count_estimate"] for item in bronze
                               if item["source_system"] == row["source_system"])
                require(counts["input"] == expected, "BRONZE_SILVER_COUNT_MISMATCH")
            reasons = row["reject_reasons"]
            require(isinstance(reasons, dict) and len(reasons) <= 64, "INVALID_REJECT_REASONS")
            for code, value in reasons.items():
                require(isinstance(code, str) and re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", code),
                        "INVALID_REJECT_REASONS")
                number(value)
            # Exclusive PRIMARY quality-reject reasons. Parser errors/ignored are separate.
            require(sum(reasons.values()) == counts["rejected"], "REJECT_REASON_COUNT_MISMATCH")
            releases = row["catalog_releases"]
            require(isinstance(releases, list) and len(releases) <= 64
                    and all(isinstance(item, str) for item in releases)
                    and len(set(releases)) == len(releases),
                    "INVALID_CATALOG_RELEASES")
            for release in releases:
                label(release)
    else:
        require(rows == [], "UNEXPECTED_SOURCE_COUNTS")
        gold = report["gold"]
        require(isinstance(gold, dict) and set(gold) == {"current", "bridge"}, "INVALID_GOLD_COUNTS")
        current, bridge = (number(gold[key]) for key in ("current", "bridge"))
        require(0 <= current <= bridge and (bridge == 0 or current > 0), "GOLD_COUNT_RECONCILIATION_FAILED")
        previous = upstream.get("observability") if upstream else None
        require(upstream is None or previous is not None, "MISSING_UPSTREAM_COUNTS")
        if previous is not None and phase == "gold":
            require(bridge == sum(row["counts"]["current"] for row in previous["sources"]),
                    "SILVER_GOLD_COUNT_MISMATCH")
        elif previous is not None:
            require(gold == previous["gold"], "VERIFIED_COUNT_CHANGED")
    return deepcopy(report)


def _bytes(value):
    try:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError):
        raise ObservabilityError("INVALID_TELEMETRY_JSON") from None
    require(len(payload) <= MAX_BYTES, "TELEMETRY_TOO_LARGE")
    return payload


def _atomic(path, value):
    payload = _bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".summary-", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(payload)
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def identity(context):
    """Project an ORC-01 validated context, never dag_run.conf/environment."""
    require(context.get("mode") in {"mock", "real", "replay", "unresolved"}, "INVALID_TELEMETRY_CONTEXT")
    if context["mode"] != "unresolved":
        require(isinstance(context.get("context_sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", context["context_sha256"]), "INVALID_TELEMETRY_CONTEXT")
        for key in ("window_start_utc", "window_end_utc"):
            utc_time(context.get(key))
        try:
            require(datetime.strptime(context["processing_date"], "%Y-%m-%d").strftime("%Y-%m-%d")
                    == context["processing_date"], "INVALID_TELEMETRY_CONTEXT")
        except (ValueError, TypeError, KeyError):
            raise ObservabilityError("INVALID_TELEMETRY_CONTEXT") from None
        require(type(context.get("is_backfill")) is bool, "INVALID_TELEMETRY_CONTEXT")
    result = {"dag_id": label(context["dag_id"]), "run_id": label(context["run_id"]),
            "mode": context["mode"], "context_sha256": context["context_sha256"],
            "config_version": label(context["config_version"]),
            "window_start_utc": context["window_start_utc"], "window_end_utc": context["window_end_utc"],
            "processing_date": context["processing_date"], "is_backfill": context["is_backfill"]}
    if result["dag_id"] == "orc_03_backfill" and result["mode"] != "unresolved":
        require(result["mode"] == "real" and result["is_backfill"] is True
                and context.get("action") in {"ingest", "reuse", "reprocess"}
                and isinstance(context.get("scope_sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", context["scope_sha256"]), "INVALID_TELEMETRY_CONTEXT")
        result.update(operation_id=label(context["operation_id"]), scope_sha256=context["scope_sha256"],
                      action=context["action"])
    return result


def phases_for(run):
    return BACKFILL_PHASES if run["dag_id"] == "orc_03_backfill" else PHASES


def utc_time(value):
    require(isinstance(value, str) and len(value) <= 40 and value.endswith("Z"), "INVALID_TELEMETRY_CONTEXT")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ObservabilityError("INVALID_TELEMETRY_CONTEXT") from None
    require(parsed.utcoffset().total_seconds() == 0, "INVALID_TELEMETRY_CONTEXT")
    return value


def validate_details(details, phase, run):
    """Second allowlist at the persistence boundary, including nested metadata."""
    allowed = {"bronze_inputs", "silver_manifests", "snapshots", "verification_report_uri",
               "publication_uri", "counts", "completion_status", "resolved_context_sha256", "receipt_sha256",
               "bronze_pins"}
    require(isinstance(details, dict) and set(details) <= allowed, "INVALID_COUNT_REPORT")
    if "resolved_context_sha256" in details:
        value = details["resolved_context_sha256"]
        require(phase == "resolve" and isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value),
                "INVALID_TELEMETRY_CONTEXT")
    if "receipt_sha256" in details:
        value = details["receipt_sha256"]
        require(phase not in {"resolve", "complete"} and isinstance(value, str)
                and re.fullmatch(r"[0-9a-f]{64}", value), "INVALID_TELEMETRY_CONTEXT")
    for key in ("verification_report_uri", "publication_uri"):
        if key in details:
            uri(details[key], run["mode"])
    if "completion_status" in details:
        status = details["completion_status"]
        allowed_statuses = {"real": {"SourcesReady", "Published"}, "mock": {"MockComplete"},
                            "replay": {"MetadataReplayComplete"}, "unresolved": set()}
        if run["dag_id"] == "orc_03_backfill":
            allowed_statuses["real"] = {"Published"} if run.get("action") == "reprocess" else {"BronzeVerified"}
        require(phase == "complete" and status in allowed_statuses[run["mode"]], "PUBLICATION_MODE_MISMATCH")
    if "bronze_pins" in details:
        require(run["dag_id"] == "orc_03_backfill" and phase == "source", "INVALID_COUNT_REPORT")
        from backfill_runtime import pins
        pins(details["bronze_pins"])
    for key in ("bronze_inputs", "silver_manifests", "snapshots"):
        if key not in details:
            continue
        items = details[key]
        require(isinstance(items, list) and 0 < len(items) <= 64, "INVALID_COUNT_REPORT")
        for item in items:
            require(isinstance(item, dict), "INVALID_COUNT_REPORT")
            if key == "bronze_inputs":
                fields = {"source_system", "manifest_uri", "raw_object_uri", "manifest_id", "ingest_run_id",
                          "receipt_run_id", "sha256", "manifest_sha256", "record_count_estimate",
                          "catalog_release", "year", "segment", "window_start_utc", "window_end_utc"}
                require(set(item) <= fields and item.get("source_system") in SOURCES, "INVALID_COUNT_REPORT")
                uri(item.get("manifest_uri"), run["mode"])
                uri(item.get("raw_object_uri"), run["mode"])
                number(item.get("record_count_estimate"))
                for field, value in item.items():
                    if field in {"sha256", "manifest_sha256"}:
                        require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "INVALID_COUNT_REPORT")
                    elif field in {"manifest_id", "ingest_run_id", "receipt_run_id", "catalog_release", "segment"}:
                        label(value)
                    elif field in {"window_start_utc", "window_end_utc"}:
                        utc_time(value)
                    elif field == "year":
                        number(value)
            elif key == "silver_manifests":
                require(set(item) == {"dataset", "manifest_uri", "readback_verified"}
                        and item["dataset"] in {"source_observation", "reject_record", "source_link", "canonical_membership"}
                        and item["readback_verified"] is True, "INVALID_COUNT_REPORT")
                uri(item["manifest_uri"], run["mode"])
            else:
                require(set(item) == {"table", "snapshot_id", "committed"} and item["committed"] is True
                        and isinstance(item["table"], str) and re.fullmatch(
                            r"[A-Za-z_][A-Za-z0-9_]*\.gold\.[A-Za-z_][A-Za-z0-9_]*", item["table"]), "INVALID_COUNT_REPORT")
                value = item["snapshot_id"]
                pattern = r"mock-[1-9][0-9]*" if run["mode"] == "mock" else r"[1-9][0-9]{0,18}"
                require(isinstance(value, str) and re.fullmatch(pattern, value), "INVALID_COUNT_REPORT")
    if "counts" in details:
        validate_counts(details["counts"], phase, run)
    _bytes(details)


def _root(run, env):
    key = hashlib.sha256((run["dag_id"] + "|" + run["run_id"]).encode()).hexdigest()
    return Path(env.get("RUN_SUMMARY_ROOT", "/opt/pipeline/staging/run-summary")) / key


@contextmanager
def _locked(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".lock").open("a+b") as lock:
        os.chmod(root / ".lock", 0o600)
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def _summary(state):
    phases = state["phases"]
    order = phases_for(state["run"])
    events = [phases[phase] for phase in order if phase in phases]
    failed = next((event for event in events if event["status"] == "FAILED"), None)
    complete = phases.get("complete", {})
    status = "FAILED" if failed else complete.get("details", {}).get("completion_status", "RUNNING")
    published = status == "Published" and state["run"]["mode"] == "real"
    sources = {source: {"source_system": source, "counts": {key: None for key in COUNT_FIELDS},
                        "reject_reasons": {}, "catalog_releases": [], "bronze_inputs": [],
                        "reconciliation": "not_reported"} for source in SOURCES}
    snapshots, silver, verification, publication = [], [], None, None
    gold = {"current": None, "bridge": None, "published": None, "reconciliation": "not_reported"}
    for event in events:
        if event["status"] != "SUCCEEDED":
            continue
        details = event["details"]
        for item in details.get("bronze_pins", []):
            row = sources[item["source_system"]]
            row.setdefault("bronze_pins", []).append(item)
            if "catalog_release" in item and item["catalog_release"] not in row["catalog_releases"]:
                row["catalog_releases"].append(item["catalog_release"])
        for item in details.get("bronze_inputs", []):
            row = sources[item["source_system"]]
            existing = next((old for old in row["bronze_inputs"] if old["manifest_uri"] == item["manifest_uri"]), None)
            if existing is not None:
                require(existing["record_count_estimate"] == item["record_count_estimate"]
                        and existing.get("sha256") == item.get("sha256"), "BRONZE_INPUT_CHANGED")
            else:
                row["bronze_inputs"].append(item)
                row["counts"]["input"] = (row["counts"]["input"] or 0) + item["record_count_estimate"]
            if "catalog_release" in item and item["catalog_release"] not in row["catalog_releases"]:
                row["catalog_releases"].append(item["catalog_release"])
        report = details.get("counts")
        if report:
            for row in report["sources"]:
                sources[row["source_system"]].update(deepcopy(row), reconciliation="passed")
            if report["gold"] is not None:
                gold.update(report["gold"], reconciliation="passed")
        silver = details.get("silver_manifests", silver)
        snapshots = details.get("snapshots", snapshots)
        verification = details.get("verification_report_uri", verification)
        publication = details.get("publication_uri", publication)
    if published and gold["current"] is not None:
        gold["published"] = gold["current"]
    stopped = failed is not None or status != "RUNNING"
    verification_status = "passed" if verification is not None else (
        "failed" if phases.get("verify", {}).get("status") == "FAILED" else "not_reported")
    return {"summary_version": VERSION, **state["run"], "status": status, "published": published,
            "evidence_kind": ("synthetic" if state["run"]["mode"] == "mock" else
                              "saved_metadata" if state["run"]["mode"] == "replay" else "adapter_metadata"),
            "reason": failed["reason"] if failed else None, "failed_phase": failed["phase"] if failed else None,
            "started_at_utc": state["started_at_utc"], "updated_at_utc": state["updated_at_utc"],
            "duration_seconds": sum(event["duration_seconds"] for event in events),
            "duration_semantics": "sum_of_current_attempt_phase_execution_excludes_scheduler_wait",
            "wall_duration_seconds": max(0.0, (datetime.fromisoformat(state["updated_at_utc"].replace("Z", "+00:00"))
                                                - datetime.fromisoformat(state["started_at_utc"].replace("Z", "+00:00"))).total_seconds()),
            "freshness": {"latest_event_time_utc": None, "snapshot_committed_at_utc": None,
                          "snapshot_age_seconds": None, "status": "not_reported"},
            "sources": list(sources.values()), "gold_counts": gold,
            "silver_manifests": silver, "snapshots": snapshots,
            "gold_verification_status": verification_status,
            "verification_report_uri": verification, "publication_uri": publication,
            "phases": [{**{key: value for key, value in event.items() if key != "details"},
                        "receipt_sha256": event["details"].get("receipt_sha256")} for event in events],
            "not_executed_phases": [phase for phase in order if phase not in phases] if stopped else [],
            "metrics_kind": "per_run_scoped_gauges_not_cumulative_counters",
            "sequence": state["sequence"]}


def _record(run, phase, attempt, status, reason, duration, details, env, token=None):
    order = phases_for(run)
    require(phase in order and type(attempt) is int and 1 <= attempt <= 10000, "INVALID_TELEMETRY_ATTEMPT")
    require(math.isfinite(duration) and duration >= 0, "INVALID_TELEMETRY_DURATION")
    validate_details(details, phase, run)
    root = _root(run, env)
    with _locked(root):
        target = root / "state.json"
        if target.exists():
            require(target.stat().st_size <= MAX_BYTES, "TELEMETRY_TOO_LARGE")
            state = json.loads(target.read_text())
            # Resolve is bound to an opaque config hash. Promote once to a validated context.
            if state["run"]["mode"] == "unresolved" and run["mode"] != "unresolved":
                require(state["phases"].get("resolve", {}).get("status") == "SUCCEEDED",
                        "RUN_SUMMARY_CONTEXT_CHANGED")
                require(state["phases"]["resolve"]["details"].get("resolved_context_sha256") == run["context_sha256"],
                        "RUN_SUMMARY_CONTEXT_CHANGED")
                state["run"] = run
            elif phase == "resolve" and status == "RUNNING":
                # A manual clear of resolve begins a new generation, preserving its journal.
                state["run"] = run
            else:
                require(state["run"] == run, "RUN_SUMMARY_CONTEXT_CHANGED")
        else:
            state = {"run": run, "started_at_utc": _utc_now(), "sequence": 0, "phases": {}}
        if status == "RUNNING":
            # Retain append-only history, but never carry stale downstream success into reruns.
            for later in order[order.index(phase):]:
                state["phases"].pop(later, None)
        else:
            require(state["phases"].get(phase, {}).get("token") == token, "STALE_TELEMETRY_ATTEMPT")
        state["sequence"] += 1
        event = {"event": "pipeline_phase", "phase": phase, "attempt": attempt, "status": status,
                 "reason": reason, "duration_seconds": round(duration, 6), "timestamp_utc": _utc_now(),
                 "details": details, "token": token or state["sequence"]}
        previous = state["phases"].get(phase, {})
        event["started_at_utc"] = previous.get("started_at_utc", event["timestamp_utc"])
        event["finished_at_utc"] = None if status == "RUNNING" else event["timestamp_utc"]
        state["phases"][phase] = event
        state["updated_at_utc"] = event["timestamp_utc"]
        summary = _summary(state)
        # Fail-closed if persistence fails; no success gate bypass. Journals contain only projections.
        _atomic(root / "events" / f'{state["sequence"]:08d}.json', {**run, **event})
        _atomic(target, state)
        _atomic(root / "run_summary.json", summary)
    LOGGER.info(_bytes({"summary_version": VERSION, **run,
                        **{key: value for key, value in event.items() if key != "details"}}).decode())
    return event["token"], summary


def observe(context, phase, operation, project, environment=None, attempt=1):
    """Wrap one validated operation; sanitize exceptions before Airflow traceback/logging."""
    env = os.environ if environment is None else environment
    run = identity(context)
    started = time.monotonic()
    try:
        token, _ = _record(run, phase, attempt, "RUNNING", None, 0.0, {}, env)
    except Exception as error:
        reason = safe_reason(error)
        raise ObservabilityError(reason if reason != "PIPELINE_OPERATION_FAILED" else "RUN_SUMMARY_WRITE_FAILED") from None
    try:
        result = operation()
        details = project(result)
        _, summary = _record(run, phase, attempt, "SUCCEEDED", None, time.monotonic() - started,
                             details, env, token)
        if phase == "complete":
            LOGGER.info(_bytes({"event": "pipeline_run_summary", **summary}).decode())
        return result
    except Exception as error:
        reason = safe_reason(error)
        try:
            _, summary = _record(run, phase, attempt, "FAILED", reason, time.monotonic() - started,
                                 {}, env, token)
            LOGGER.info(_bytes({"event": "pipeline_run_summary", **summary}).decode())
        except Exception:
            raise ObservabilityError("RUN_SUMMARY_WRITE_FAILED") from None
        raise ObservabilityError(reason) from None


# Fixed registry, never echo an arbitrary exception, adapter stderr or connection URL.
SAFE_REASONS = {
    "SCOPED_ETL_ADAPTER_NOT_CONFIGURED", "SCOPED_WRITE_GATE_FAILED", "REPROCESS_INPUT_CHANGED",
    "INVALID_BACKFILL_FIELDS", "INVALID_OPERATION_ID", "BOUNDED_INPUTS_REQUIRED", "INVALID_PIN_CHECKSUM",
    "INVALID_BACKFILL_CONFIGURATION", "UNSUPPORTED_BACKFILL_CONFIGURATION", "EXPLICIT_ACTION_REQUIRED",
    "JSON_BOOLEAN_REQUIRED", "INGEST_SCOPE_REQUIRED", "INVALID_CHUNK_SIZE", "INVALID_USGS_CONFIGURATION",
    "TOO_MANY_USGS_CHUNKS_SPLIT_OPERATION", "INVALID_JMA_SCOPE", "INVALID_JMA_SEGMENTS", "INVALID_JMA_SEGMENT",
    "TOO_MANY_TOTAL_INPUTS_SPLIT_OPERATION", "REUSE_CANNOT_DOWNLOAD", "EXPLICIT_REPROCESS_SCOPE_REQUIRED",
    "USGS_VALIDATION_FAILED", "USGS_IDENTITY_CHANGED", "JMA_ARCHIVE_NOT_READY", "ADAPTER_NOT_CONFIGURED",
    "BRONZE_PIN_NOT_VERIFIED", "BACKFILL_ADAPTER_FAILED", "BACKFILL_ADAPTER_DID_NOT_COMPLETE",
    "OPERATION_SCOPE_CHANGED_USE_NEW_ID", "EXECUTION_ENVIRONMENT_CHANGED_OR_UNRESOLVED",
    "PREVIEW_CANNOT_EXECUTE", "PLAN_CHANGED", "DRY_RUN_FORBIDDEN", "USGS_EXECUTION_SETTINGS_CHANGED",
    "BACKFILL_COMPLETION_FAILED", "SOURCE_LEASE_NOT_RELEASED", "INVALID_READBACK_REPORT",
    "ADAPTER_DID_NOT_COMPLETE", "ADAPTER_PROCESS_FAILED", "INVALID_ADAPTER_JSON", "METADATA_TOO_LARGE",
    "QUALITY_GATE_FAILED", "PHASE_FAILED", "BRONZE_NOT_READY", "SILVER_NOT_READY", "SOURCE_NOT_READY",
    "CONTEXT_CHANGED", "RESULT_CONTEXT_MISMATCH", "RESULT_UPSTREAM_MISMATCH", "MISSING_UPSTREAM_GATE",
    "INVALID_METADATA_FIELDS", "PUBLICATION_MODE_MISMATCH", "PUBLICATION_INPUT_CHANGED",
    "VERIFIED_SNAPSHOT_CHANGED", "UNCOMMITTED_GOLD_SNAPSHOT", "INCOMPLETE_GOLD_BUNDLE",
    "BRONZE_INPUT_CHANGED", "SILVER_SCOPE_CHANGED", "GOLD_SCOPE_CHANGED", "GOLD_INPUT_CHANGED",
    "REAL_ADAPTER_NOT_CONFIGURED", "INVALID_ARTIFACT_URI", "INVALID_COUNT_REPORT", "INVALID_TELEMETRY_COUNT",
    "COUNT_RECONCILIATION_FAILED", "BRONZE_SILVER_COUNT_MISMATCH", "REJECT_REASON_COUNT_MISMATCH",
    "SILVER_GOLD_COUNT_MISMATCH", "VERIFIED_COUNT_CHANGED", "MISSING_UPSTREAM_COUNTS",
    "RUN_SUMMARY_CONTEXT_CHANGED", "STALE_TELEMETRY_ATTEMPT", "INCOMPLETE_SOURCE_COUNTS",
    "INVALID_EXECUTION_MODE", "INVALID_UTC_INTERVAL", "UTC_Z_REQUIRED", "INVALID_PROCESSING_DATE",
    "INVALID_BACKFILL_FLAG", "UNSUPPORTED_RUN_CONFIGURATION", "EXPLICIT_REAL_SCOPE_REQUIRED",
    "INVALID_RUN_CONFIGURATION", "INVALID_INPUT_CHECKSUM", "INVALID_SILVER_SCOPE", "INVALID_GOLD_TABLE_SCOPE",
    "UNSUPPORTED_CONTRACT_VERSION", "UNSUPPORTED_SOURCE", "BOTH_SOURCES_REQUIRED", "INVALID_LABEL",
    "INVALID_REJECT_REASONS", "INVALID_CATALOG_RELEASES", "UNEXPECTED_COUNT_REPORT", "UNEXPECTED_SOURCE_COUNTS",
    "INVALID_TELEMETRY_CONTEXT", "INVALID_TELEMETRY_LABEL", "INVALID_TELEMETRY_URI", "STALE_RUN_SUMMARY",
    "INVALID_JSON_METADATA", "DUPLICATE_JSON_FIELD", "INVALID_ADAPTER_CONFIGURATION",
}


def safe_reason(error):
    # Only contract/telemetry/backfill reason-code types can supply allowlisted reasons.
    value = str(error) if type(error).__name__ in {"EtlContractError", "ObservabilityError", "BackfillError"} else None
    if value in SAFE_REASONS:
        return value
    return {"ReadinessError": "SOURCE_READINESS_FAILED", "SourceRunBusy": "SOURCE_LEASE_BUSY",
            "UsgsRunnerError": "USGS_RUNNER_FAILED", "JmaRunnerError": "JMA_RUNNER_FAILED"}.get(
                type(error).__name__, "PIPELINE_OPERATION_FAILED")


def phase_details(phase, context, result):
    """Input has passed ORC-01 validators. Deliberately no generic dict/log serializer."""
    artifacts = result["artifacts"]
    details = {"receipt_sha256": hashlib.sha256(_bytes(result)).hexdigest()}
    if phase == "bronze":
        details["bronze_inputs"] = [{key: item[key] for key in (
            "source_system", "manifest_uri", "raw_object_uri", "manifest_id", "ingest_run_id",
            "sha256", "record_count_estimate")} for item in artifacts["bronze_inputs"]]
    if phase == "silver":
        details["silver_manifests"] = deepcopy(artifacts["datasets"])
    if phase in {"gold", "verify", "publish"}:
        details["snapshots"] = deepcopy(artifacts["snapshots"])
    if phase == "verify":
        details["verification_report_uri"] = artifacts["report_uri"]
    if phase == "publish":
        details["verification_report_uri"] = artifacts["verification_report_uri"]
        details["publication_uri"] = artifacts["publication_uri"]
    if "observability" in result:
        details["counts"] = deepcopy(result["observability"])
    return details


def read_summary(context, environment=None):
    env = os.environ if environment is None else environment
    root = _root(identity(context), env)
    with _locked(root):
        path = root / "run_summary.json"
        require(path.stat().st_size <= MAX_BYTES, "TELEMETRY_TOO_LARGE")
        value = json.loads(path.read_text())
        state_path = root / "state.json"
        require(state_path.stat().st_size <= MAX_BYTES, "TELEMETRY_TOO_LARGE")
        state = json.loads(state_path.read_text())
        require(value["sequence"] == state["sequence"], "STALE_RUN_SUMMARY")
        require(value["context_sha256"] == context["context_sha256"], "RUN_SUMMARY_CONTEXT_CHANGED")
        require(state["run"] == identity(context), "RUN_SUMMARY_CONTEXT_CHANGED")
        for phase, event in state["phases"].items():
            require(phase in phases_for(state["run"]) and event["phase"] == phase, "STALE_RUN_SUMMARY")
            validate_details(event["details"], phase, state["run"])
        require(value == _summary(state), "STALE_RUN_SUMMARY")
        return value


def unresolved_context(airflow_context, dag_id="orc_01_etl_pipeline"):
    """No raw conf, environment values or exception strings in failed resolve summaries."""
    raw_id = airflow_context.get("run_id")
    try:
        run_id = label(raw_id)
    except ObservabilityError:
        run_id = "invalid-" + hashlib.sha256(str(raw_id).encode()).hexdigest()
    return {"dag_id": dag_id, "run_id": run_id, "mode": "unresolved", "context_sha256": None,
            "config_version": "unresolved", "window_start_utc": None, "window_end_utc": None,
            "processing_date": None, "is_backfill": None}


def source_context(plan):
    usgs = plan["usgs"]
    return {"dag_id": "orc_02_daily_sources", "run_id": plan["run_id"], "mode": "real",
            "context_sha256": hashlib.sha256(_bytes(plan)).hexdigest(),
            "config_version": usgs["config_version"], "window_start_utc": usgs["window_start_utc"],
            "window_end_utc": usgs["window_end_utc"], "processing_date": usgs["processing_date"],
            "is_backfill": False}


def source_details(source, results, run_context=None):
    """Project only verified readiness receipts. No estimates => no invented metrics."""
    require(source in SOURCES and isinstance(results, list) and 0 < len(results) <= 64,
            "INVALID_COUNT_REPORT")
    inputs = []
    for item in results:
        require(item.get("bronze_status") == "BronzeReady" and item.get("verified") is True,
                "BRONZE_NOT_READY")
        output = {"source_system": source, "manifest_uri": uri(item.get("manifest_uri"), "real"),
                  "raw_object_uri": uri(item.get("raw_object_uri"), "real"),
                  "receipt_run_id": label(item.get("run_id")),
                  "record_count_estimate": number(item.get("record_count_estimate"))}
        for key in ("sha256", "manifest_sha256"):
            value = item.get(key)
            require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "INVALID_COUNT_REPORT")
            output[key] = value
        if source == "JMA_BULLETIN":
            output["catalog_release"] = label(item.get("catalog_release"))
            output["year"] = number(item.get("year"))
            output["segment"] = label(item.get("segment"))
        elif run_context is not None:
            output["window_start_utc"] = run_context["window_start_utc"]
            output["window_end_utc"] = run_context["window_end_utc"]
        inputs.append(output)
    require(len({item["manifest_uri"] for item in inputs}) == len(inputs), "INVALID_COUNT_REPORT")
    return {"bronze_inputs": inputs}


def dag_failure_summary(airflow_context, environment=None):
    """Airflow DAG callback fallback for terminal failures, including failed cleanup.

No exception/XCom/config inspection. Scheduler/manual-state changes may not
invoke callbacks; recovery of abandoned RUNNING runs is an ORC-05 concern.
"""
    env = os.environ if environment is None else environment
    dag = airflow_context.get("dag")
    dag_id = getattr(dag, "dag_id", airflow_context.get("dag_id"))
    if dag_id not in {"orc_01_etl_pipeline", "orc_02_daily_sources", "orc_03_backfill"}:
        return
    fallback = unresolved_context(airflow_context, dag_id)
    root = _root(fallback, env)
    # Successful preview never creates telemetry, including from a later DAG callback.
    if dag_id == "orc_03_backfill" and not (root / "state.json").is_file():
        return
    try:
        with _locked(root):
            path = root / "state.json"
            require(path.exists() and path.stat().st_size <= MAX_BYTES, "MISSING_UPSTREAM_GATE")
            run = json.loads(path.read_text())["run"]
        token, _ = _record(run, "complete", 1, "RUNNING", None, 0.0, {}, env)
        _, summary = _record(run, "complete", 1, "FAILED", "DAG_FAILED", 0.0, {}, env, token)
        LOGGER.info(_bytes({"event": "pipeline_run_summary", **summary}).decode())
    except Exception:
        LOGGER.error("RUN_SUMMARY_CALLBACK_FAILED")
