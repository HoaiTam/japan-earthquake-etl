"""ORC-01 metadata-only orchestration boundary; no raw data transformation.

Java/Spark and Trino adapters own processing and readback. A mock receipt is
never interchangeable with a real receipt, even when all simulated gates pass.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
from typing import Any, Mapping
from urllib.parse import urlsplit

from run_observability import (ObservabilityError, observe, phase_details, read_summary,
                               unresolved_context, validate_counts)


CONTRACT_VERSION = "1.0"
DAG_ID = "orc_01_etl_pipeline"
PHASES = ("readiness", "bronze", "silver", "gold", "verify", "publish")
SOURCES = {"USGS", "JMA_BULLETIN"}
SILVER_DATASETS = {"source_observation", "reject_record", "source_link", "canonical_membership"}
VERIFY_CHECKS = {"readable", "counts_reconciled", "canonical_unique",
                 "required_fields_valid", "scope_respected", "lineage_resolved"}
BRONZE_CHECKS = {"object_write_completed", "raw_readback_verified", "checksum_verified",
                 "source_structure_valid", "manifest_consistent"}
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "orc_01_mock_v1.json"
MAX_METADATA_BYTES = 65536
SAFE_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:~+|/@=-]{0,249}")
TABLE_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\.gold\.[A-Za-z_][A-Za-z0-9_]*")


class EtlContractError(RuntimeError):
    """Safe reason codes only: never include adapter output or credentials."""


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise EtlContractError(reason)


def _keys(value: Any, expected: set[str]) -> None:
    _require(isinstance(value, dict) and set(value) == expected, "INVALID_METADATA_FIELDS")


def _label(value: Any) -> None:
    _require(isinstance(value, str) and SAFE_LABEL.fullmatch(value) is not None, "INVALID_LABEL")


def _json_bytes(value: Any) -> bytes:
    try:
        payload = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (TypeError, ValueError):
        raise EtlContractError("INVALID_JSON_METADATA") from None
    _require(len(payload) <= MAX_METADATA_BYTES, "METADATA_TOO_LARGE")
    return payload


def _digest(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _utc(value: Any) -> str:
    _require(isinstance(value, str) and value.endswith("Z"), "UTC_Z_REQUIRED")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise EtlContractError("INVALID_UTC_INTERVAL") from None
    _require(parsed.utcoffset() is not None and parsed.utcoffset().total_seconds() == 0,
             "UTC_Z_REQUIRED")
    return parsed.isoformat().replace("+00:00", "Z")


def _uri(value: Any, mode: str) -> None:
    _require(isinstance(value, str) and len(value) <= 1024, "INVALID_ARTIFACT_URI")
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise EtlContractError("INVALID_ARTIFACT_URI") from None
    _require(parsed.scheme == ("mock" if mode == "mock" else "s3") and bool(parsed.netloc)
             and bool(parsed.path.strip("/")) and not parsed.query and not parsed.fragment
             and not parsed.username and not parsed.password
             and all(re.fullmatch(r"[A-Za-z0-9._=/~-]+", part) is not None
                     for part in (parsed.netloc, parsed.path))
             and not any(part in {".", ".."} for part in parsed.path.split("/"))
             and "*" not in value, "INVALID_ARTIFACT_URI")


def _list(value: Any) -> list:
    _require(isinstance(value, list) and 0 < len(value) <= 64, "INVALID_ARTIFACT_LIST")
    return value


def _uris(value: Any, mode: str) -> list[str]:
    items = _list(value)
    for item in items:
        _uri(item, mode)
    _require(len(items) == len(set(items)), "DUPLICATE_ARTIFACT")
    return items


def load_mock_fixture() -> dict[str, Any]:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    _require(fixture.get("contract_version") == CONTRACT_VERSION, "UNSUPPORTED_CONTRACT_VERSION")
    return fixture


def _validate_context(context: dict[str, Any]) -> None:
    _keys(context, {"contract_version", "dag_id", "run_id", "mode", "window_start_utc",
                    "window_end_utc", "interval_semantics", "processing_date", "is_backfill",
                    "config_version", "input_scope", "output_scope", "context_sha256"})
    _require(context["contract_version"] == CONTRACT_VERSION and context["dag_id"] == DAG_ID,
             "UNSUPPORTED_CONTRACT_VERSION")
    _require(isinstance(context["mode"], str) and context["mode"] in {"mock", "real"}, "INVALID_EXECUTION_MODE")
    for field in ("run_id", "config_version"):
        _label(context[field])
    start, end = (_utc(context[field]) for field in ("window_start_utc", "window_end_utc"))
    _require(start == context["window_start_utc"] and end == context["window_end_utc"]
             and datetime.fromisoformat(start.replace("Z", "+00:00")) <
             datetime.fromisoformat(end.replace("Z", "+00:00"))
             and datetime.fromisoformat(end.replace("Z", "+00:00")) <= datetime.now(timezone.utc)
             and context["interval_semantics"] == "[start,end)", "INVALID_UTC_INTERVAL")
    _require(type(context["is_backfill"]) is bool, "INVALID_BACKFILL_FLAG")
    try:
        _require(date.fromisoformat(context["processing_date"]).isoformat() == context["processing_date"],
                 "INVALID_PROCESSING_DATE")
    except (TypeError, ValueError):
        raise EtlContractError("INVALID_PROCESSING_DATE") from None
    inputs = context["input_scope"]
    _keys(inputs, {"bronze_manifests"})
    manifests = _list(inputs["bronze_manifests"])
    for manifest in manifests:
        _keys(manifest, {"source_system", "manifest_uri", "sha256"})
        _require(isinstance(manifest["source_system"], str) and manifest["source_system"] in SOURCES,
                 "UNSUPPORTED_SOURCE")
        _uri(manifest["manifest_uri"], context["mode"])
        _require(isinstance(manifest["sha256"], str)
                 and re.fullmatch(r"[0-9a-f]{64}", manifest["sha256"]) is not None, "INVALID_INPUT_CHECKSUM")
    _require({item["source_system"] for item in manifests} == SOURCES, "BOTH_SOURCES_REQUIRED")
    _uris([item["manifest_uri"] for item in manifests], context["mode"])
    outputs = context["output_scope"]
    _keys(outputs, {"silver_partitions", "gold_tables"})
    partitions = _list(outputs["silver_partitions"])
    for partition in partitions:
        _keys(partition, {"event_year_utc", "event_month_utc", "source_system"})
        _require(type(partition["event_year_utc"]) is int and 1900 <= partition["event_year_utc"] <= 2200
                 and type(partition["event_month_utc"]) is int and 1 <= partition["event_month_utc"] <= 12
                 and isinstance(partition["source_system"], str)
                 and partition["source_system"] in SOURCES, "INVALID_SILVER_SCOPE")
    _require(len({_digest(item) for item in partitions}) == len(partitions), "DUPLICATE_PARTITION")
    tables = _list(outputs["gold_tables"])
    _require(all(isinstance(table, str) and TABLE_NAME.fullmatch(table) for table in tables)
             and len(set(tables)) == len(tables) and len({table.split(".")[0] for table in tables}) == 1
             and {table.split(".")[-1] for table in tables} >= {"event_current", "event_source_bridge"},
             "INVALID_GOLD_TABLE_SCOPE")
    _require(context["context_sha256"] == _digest({key: value for key, value in context.items()
                                                 if key != "context_sha256"}), "CONTEXT_CHANGED")


def resolve_run_context(airflow_context: Mapping[str, Any],
                        environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Resolve explicit scopes only. ORC-02 owns automatic source/window planning."""
    env = os.environ if environment is None else environment
    dag_run = airflow_context.get("dag_run")
    conf = dag_run.get("conf", {}) if isinstance(dag_run, Mapping) else getattr(dag_run, "conf", {})
    _require(isinstance(conf, dict), "INVALID_RUN_CONFIGURATION")
    allowed = {"mode", "window_start_utc", "window_end_utc", "processing_date", "is_backfill",
               "input_scope", "output_scope"}
    _require(not set(conf) - allowed, "UNSUPPORTED_RUN_CONFIGURATION")
    mode = conf.get("mode", "mock")
    _require(isinstance(mode, str) and mode in {"mock", "real"}, "INVALID_EXECUTION_MODE")
    parameters = {**deepcopy(load_mock_fixture()["conf"]), **deepcopy(conf)} if mode == "mock" else conf
    _require(allowed <= set(parameters), "EXPLICIT_REAL_SCOPE_REQUIRED")
    result = {"contract_version": CONTRACT_VERSION, "dag_id": DAG_ID,
              "run_id": airflow_context.get("run_id"), **deepcopy(parameters),
              "interval_semantics": "[start,end)", "config_version": env.get("CONFIG_VERSION", "1")}
    for field in ("window_start_utc", "window_end_utc"):
        result[field] = _utc(result[field])
    result["context_sha256"] = _digest(result)
    _validate_context(result)
    if mode == "real":
        _require(bool(env.get("ETL_PHASE_RUNNER_COMMAND", "").strip()), "REAL_ADAPTER_NOT_CONFIGURED")
    return result


def _all_passed(checks: Any, required: set[str]) -> None:
    _keys(checks, required)
    _require(all(value is True for value in checks.values()), "QUALITY_GATE_FAILED")


def _snapshots(value: Any, context: dict[str, Any]) -> None:
    snapshots = _list(value)
    for snapshot in snapshots:
        _keys(snapshot, {"table", "snapshot_id", "committed"})
        pattern = r"mock-[1-9][0-9]*" if context["mode"] == "mock" else r"[1-9][0-9]*"
        _require(isinstance(snapshot["table"], str) and TABLE_NAME.fullmatch(snapshot["table"]) is not None,
                 "INVALID_GOLD_TABLE_SCOPE")
        _require(isinstance(snapshot["snapshot_id"], str)
                 and re.fullmatch(pattern, snapshot["snapshot_id"]) is not None
                 and snapshot["committed"] is True, "UNCOMMITTED_GOLD_SNAPSHOT")
        if context["mode"] == "real":
            _require(len(snapshot["snapshot_id"]) <= 19
                     and int(snapshot["snapshot_id"]) <= 9223372036854775807, "INVALID_SNAPSHOT_ID")
    tables = [item["table"] for item in snapshots]
    _require(len(tables) == len(set(tables))
             and set(tables) == set(context["output_scope"]["gold_tables"]), "INCOMPLETE_GOLD_BUNDLE")


def _validate_artifacts(phase: str, context: dict[str, Any], artifacts: dict[str, Any],
                        upstream: dict[str, Any] | None) -> None:
    mode = context["mode"]
    manifests = context["input_scope"]["bronze_manifests"]
    pinned_uris = [item["manifest_uri"] for item in manifests]
    previous = upstream["artifacts"] if upstream is not None else None
    if phase == "readiness":
        _keys(artifacts, {"sources_ready", "bronze_manifest_uris"})
        _require(artifacts["sources_ready"] == sorted(SOURCES)
                 and artifacts["bronze_manifest_uris"] == pinned_uris, "SOURCE_NOT_READY")
    elif phase == "bronze":
        _keys(artifacts, {"bronze_inputs"})
        inputs = _list(artifacts["bronze_inputs"])
        for item in inputs:
            _keys(item, {"source_system", "manifest_uri", "raw_object_uri", "manifest_id", "ingest_run_id",
                         "sha256", "bronze_status", "validation", "record_count_estimate"})
            _uri(item["raw_object_uri"], mode)
            _label(item["manifest_id"])
            _label(item["ingest_run_id"])
            _require(item["bronze_status"] == "BronzeReady" and isinstance(item["sha256"], str)
                     and re.fullmatch(r"[0-9a-f]{64}", item["sha256"]) is not None
                     and type(item["record_count_estimate"]) is int and item["record_count_estimate"] >= 0,
                     "BRONZE_NOT_READY")
            _all_passed(item["validation"], BRONZE_CHECKS)
        _require([{key: item[key] for key in ("source_system", "manifest_uri", "sha256")} for item in inputs]
                 == manifests, "BRONZE_INPUT_CHANGED")
    elif phase == "silver":
        _keys(artifacts, {"input_manifest_uris", "datasets", "partitions", "quality_passed"})
        _require(artifacts["input_manifest_uris"] == pinned_uris
                 and artifacts["partitions"] == context["output_scope"]["silver_partitions"],
                 "SILVER_SCOPE_CHANGED")
        _require(artifacts["quality_passed"] is True, "SILVER_NOT_READY")
        datasets = _list(artifacts["datasets"])
        for dataset in datasets:
            _keys(dataset, {"dataset", "manifest_uri", "readback_verified"})
            _require(isinstance(dataset["dataset"], str) and dataset["dataset"] in SILVER_DATASETS,
                     "INCOMPLETE_SILVER_BUNDLE")
            _uri(dataset["manifest_uri"], mode)
            _require(dataset["readback_verified"] is True, "SILVER_NOT_READY")
        _require(len(datasets) == len(SILVER_DATASETS)
                 and {item["dataset"] for item in datasets} == SILVER_DATASETS, "INCOMPLETE_SILVER_BUNDLE")
        _uris([item["manifest_uri"] for item in datasets], mode)
    elif phase == "gold":
        _keys(artifacts, {"input_manifest_uris", "snapshots", "scope_verified"})
        _uris(artifacts["input_manifest_uris"], mode)
        if previous is not None:
            _require(artifacts["input_manifest_uris"] == [item["manifest_uri"] for item in previous["datasets"]],
                     "GOLD_INPUT_CHANGED")
        _require(artifacts["scope_verified"] is True, "GOLD_SCOPE_CHANGED")
        _snapshots(artifacts["snapshots"], context)
    elif phase == "verify":
        _keys(artifacts, {"snapshots", "engine", "checks", "report_uri"})
        _snapshots(artifacts["snapshots"], context)
        _require(artifacts["engine"] == "Trino", "TRINO_VERIFICATION_REQUIRED")
        _all_passed(artifacts["checks"], VERIFY_CHECKS)
        _uri(artifacts["report_uri"], mode)
        if previous is not None:
            _require(artifacts["snapshots"] == previous["snapshots"], "VERIFIED_SNAPSHOT_CHANGED")
    elif phase == "publish":
        _keys(artifacts, {"snapshots", "verification_report_uri", "publication_uri", "published", "publication_status"})
        _snapshots(artifacts["snapshots"], context)
        for field in ("verification_report_uri", "publication_uri"):
            _uri(artifacts[field], mode)
        _require(artifacts["published"] is (mode == "real")
                 and artifacts["publication_status"] == ("Published" if mode == "real" else "MockComplete"),
                 "PUBLICATION_MODE_MISMATCH")
        if previous is not None:
            _require(artifacts["snapshots"] == previous["snapshots"]
                     and artifacts["verification_report_uri"] == previous["report_uri"], "PUBLICATION_INPUT_CHANGED")


def _validate_envelope(phase: str, context: dict[str, Any], result: Any) -> None:
    fields = {"contract_version", "run_id", "context_sha256", "mode", "phase", "status",
              "upstream_sha256", "artifacts"}
    _keys(result, fields | ({"observability"} if isinstance(result, dict) and "observability" in result else set()))
    _require(all(result[field] == context[field] for field in
                 ("contract_version", "run_id", "context_sha256", "mode")), "RESULT_CONTEXT_MISMATCH")
    _require(result["phase"] == phase and result["status"] == "ok", "PHASE_FAILED")
    _json_bytes(result)
    if "observability" in result:
        try:
            validate_counts(result["observability"], phase, context)
        except ObservabilityError as error:
            raise EtlContractError(str(error)) from None


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        _require(key not in result, "DUPLICATE_JSON_FIELD")
        result[key] = value
    return result


def validate_result(phase: str, context: dict[str, Any], upstream: dict[str, Any] | None,
                    result: Any) -> dict[str, Any]:
    _validate_context(context)
    _require(phase in PHASES, "UNKNOWN_PHASE")
    position = PHASES.index(phase)
    if position == 0:
        _require(upstream is None, "UNEXPECTED_UPSTREAM")
    else:
        _require(upstream is not None, "MISSING_UPSTREAM_GATE")
        _validate_envelope(PHASES[position - 1], context, upstream)
        _validate_artifacts(PHASES[position - 1], context, upstream["artifacts"], None)
    _validate_envelope(phase, context, result)
    _require(result["upstream_sha256"] == (_digest(upstream) if upstream is not None else None),
             "RESULT_UPSTREAM_MISMATCH")
    _validate_artifacts(phase, context, result["artifacts"], upstream)
    if "observability" in result:
        try:
            validate_counts(result["observability"], phase, context, upstream)
        except ObservabilityError as error:
            raise EtlContractError(str(error)) from None
    return deepcopy(result)


def mock_result(phase: str, context: dict[str, Any], upstream: dict[str, Any] | None) -> dict[str, Any]:
    """Deterministic, small fixture receipts. No source/store/catalog access."""
    _require(context["mode"] == "mock", "MOCK_IN_REAL_MODE")
    result = {"contract_version": CONTRACT_VERSION, "run_id": context["run_id"],
            "context_sha256": context["context_sha256"], "mode": "mock", "phase": phase,
            "status": "ok", "upstream_sha256": _digest(upstream) if upstream is not None else None,
            "artifacts": deepcopy(load_mock_fixture()["artifacts"][phase])}
    report = load_mock_fixture().get("observability", {}).get(phase)
    if report is not None:
        result["observability"] = deepcopy(report)
    return result


def _write_request(phase: str, context: dict[str, Any], upstream: dict[str, Any] | None,
                   environment: Mapping[str, str]) -> Path:
    # Hash the full Airflow run ID: no path traversal, sanitization collision or truncation.
    run_key = hashlib.sha256(context["run_id"].encode()).hexdigest()
    root = Path(environment.get("ETL_STAGING_ROOT", "/opt/pipeline/staging/etl")) / run_key
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"{phase}-input.json"
    payload = _json_bytes({"contract_version": CONTRACT_VERSION, "phase": phase,
                           "run_context": context, "upstream": upstream,
                           "upstream_sha256": _digest(upstream) if upstream is not None else None})
    with tempfile.NamedTemporaryFile(dir=root, prefix=f".{phase}-", delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(payload)
    temporary.replace(target)
    return target


def execute_phase(phase: str, context: dict[str, Any], upstream: dict[str, Any] | None = None,
                  environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """One strict phase per task; process stderr/stdout never enters Airflow logs."""
    env = os.environ if environment is None else environment
    _validate_context(context)
    _require(phase in PHASES, "UNKNOWN_PHASE")
    position = PHASES.index(phase)
    if position:
        _require(upstream is not None, "MISSING_UPSTREAM_GATE")
        _validate_envelope(PHASES[position - 1], context, upstream)
        _validate_artifacts(PHASES[position - 1], context, upstream["artifacts"], None)
    else:
        _require(upstream is None, "UNEXPECTED_UPSTREAM")
    if context["mode"] == "mock":
        result = mock_result(phase, context, upstream)
    else:
        command = env.get("ETL_PHASE_RUNNER_COMMAND", "").strip()
        _require(bool(command), "REAL_ADAPTER_NOT_CONFIGURED")
        try:
            argv = shlex.split(command)
            timeout = int(env.get("ETL_PHASE_RUNNER_TIMEOUT_SECONDS", "3600"))
            _require(bool(argv) and 1 <= timeout <= 86400, "INVALID_ADAPTER_CONFIGURATION")
        except ValueError:
            raise EtlContractError("INVALID_ADAPTER_CONFIGURATION") from None
        request_path = _write_request(phase, context, upstream, env)
        argv.extend(["--phase", phase, "--context-file", str(request_path)])
        # Spool output instead of unbounded capture in worker RAM. Metadata has a 64 KiB cap.
        with tempfile.TemporaryFile(dir=request_path.parent) as output:
            try:
                completed = subprocess.run(argv, check=False, stdout=output, stderr=subprocess.DEVNULL,
                                           timeout=timeout, env=dict(env), shell=False)
            except (OSError, subprocess.TimeoutExpired):
                raise EtlContractError("ADAPTER_DID_NOT_COMPLETE") from None
            _require(completed.returncode == 0, "ADAPTER_PROCESS_FAILED")
            _require(output.tell() <= MAX_METADATA_BYTES, "METADATA_TOO_LARGE")
            output.seek(0)
            try:
                result = json.load(output, object_pairs_hook=_unique_json_object)
            except (ValueError, UnicodeError):
                raise EtlContractError("INVALID_ADAPTER_JSON") from None
    return validate_result(phase, context, upstream, result)


def publication_summary(context: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    """Safe final XCom; not a replacement for persistent publication_status."""
    _validate_context(context)
    _validate_envelope("publish", context, result)
    _validate_artifacts("publish", context, result["artifacts"], None)
    return {"run_id": context["run_id"], "mode": context["mode"],
            "context_sha256": context["context_sha256"], **deepcopy(result["artifacts"])}


def resolve_observed_context(airflow_context, environment=None, attempt=1):
    return observe(unresolved_context(airflow_context), "resolve",
                   lambda: resolve_run_context(airflow_context, environment),
                   lambda result: {"resolved_context_sha256": result["context_sha256"]},
                   environment, attempt)


def execute_observed_phase(phase, context, upstream=None, environment=None, attempt=1):
    return observe(context, phase, lambda: execute_phase(phase, context, upstream, environment),
                   lambda result: phase_details(phase, context, result), environment, attempt)


def observed_publication_summary(context, result, environment=None, attempt=1):
    def complete():
        summary = read_summary(context, environment)
        passed = {event["phase"] for event in summary["phases"] if event["status"] == "SUCCEEDED"}
        _require(set(PHASES) <= passed, "MISSING_UPSTREAM_GATE")
        publish_event = next(event for event in summary["phases"] if event["phase"] == "publish")
        _require(publish_event["receipt_sha256"] == _digest(result), "PUBLICATION_INPUT_CHANGED")
        final = publication_summary(context, result)
        _require(final["snapshots"] == summary["snapshots"]
                 and final["verification_report_uri"] == summary["verification_report_uri"]
                 and final["publication_uri"] == summary["publication_uri"], "PUBLICATION_INPUT_CHANGED")
        return final
    return observe(context, "complete", complete,
                   lambda final: {"completion_status": final["publication_status"]}, environment, attempt)
