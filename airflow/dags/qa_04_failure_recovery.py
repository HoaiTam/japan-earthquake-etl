"""Bounded failure/recovery harness for QA-04.

The offline runner exercises the control-plane recovery rules with a small,
deterministic state store.  It never writes lakehouse data and never accepts a
runner executable from a request.  Runtime execution is an explicit adapter
boundary that requires ORC-07 and MLI-03 to be complete before it can run.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
from typing import Any, Mapping

from jma_backfill_runtime import _atomic_json
from source_run_guard import SourceRunBusy, lease


class Qa04Error(RuntimeError):
    """Fail-closed QA-04 request or recovery error."""


MAX_REQUEST_BYTES = 64 * 1024
MAX_REPORT_BYTES = 2 * 1024 * 1024
SAFE_CASE = frozenset(
    {
        "fetch_timeout",
        "archive_invalid",
        "silver_failure",
        "gold_commit_partial",
        "gold_commit_receipt_lost",
        "trino_verify_failure",
        "publication_metadata_failure",
        "ml_bundle_invalid",
        "ml_import_partial",
        "lease_timeout_contender",
    }
)
ETL_STAGES = ("FETCH", "ARCHIVE", "SILVER", "GOLD", "TRINO", "PUBLICATION")
GOLD_TABLES = (
    "event_current",
    "event_source_bridge",
    "dim_date",
    "dim_region",
    "magnitude_band",
    "depth_band",
)
MATRIX_PATH = Path(__file__).parent / "fixtures" / "qa_04_failure_recovery_matrix.json"
RUNTIME_REQUEST_VERSION = "qa-04-runtime-v1"
RUNTIME_REPORT_VERSION = "qa-04-report-v1"
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
NAMESPACE_RE = re.compile(r"^iceberg\.qa04_[a-z0-9_]+$")
IDENTITY_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
MAX_SNAPSHOT_ID = (1 << 63) - 1


def _sha256(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _read_json(path: Path, limit: int) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise Qa04Error("QA04_REQUEST_NOT_BOUNDED")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Qa04Error("QA04_INVALID_JSON") from error
    if not isinstance(value, dict):
        raise Qa04Error("QA04_OBJECT_REQUIRED")
    return value


def load_matrix(path: str | Path = MATRIX_PATH) -> dict[str, Any]:
    matrix = _read_json(Path(path), MAX_REQUEST_BYTES)
    validate_matrix(matrix)
    return matrix


def validate_matrix(matrix: Mapping[str, Any]) -> None:
    if matrix.get("task_id") != "QA-04" or matrix.get("version") != "qa04-v1":
        raise Qa04Error("QA04_MATRIX_ID_INVALID")
    cases = matrix.get("cases")
    if not isinstance(cases, list) or {item.get("case_id") for item in cases if isinstance(item, dict)} != SAFE_CASE:
        raise Qa04Error("QA04_CASE_MATRIX_INCOMPLETE")
    for item in cases:
        if not isinstance(item, dict) or item.get("case_id") not in SAFE_CASE:
            raise Qa04Error("QA04_CASE_INVALID")
        for key in ("fault_stage", "reason_code", "recovery_boundary", "expected_blocker"):
            if not isinstance(item.get(key), str) or not item[key]:
                raise Qa04Error("QA04_CASE_CONTRACT_INVALID")
        scope = item.get("scope")
        if not isinstance(scope, list) or not scope or any(not isinstance(x, str) for x in scope):
            raise Qa04Error("QA04_SCOPE_REQUIRED")


def _case(matrix: Mapping[str, Any], case_id: str) -> dict[str, Any]:
    if case_id not in SAFE_CASE:
        raise Qa04Error("QA04_CASE_NOT_ALLOWED")
    return next(item for item in matrix["cases"] if item["case_id"] == case_id)


def _persist(root: Path, state: dict[str, Any]) -> None:
    _atomic_json(root / "state.json", state)


def _event(root: Path, state: dict[str, Any], event: dict[str, Any]) -> None:
    state["events"].append(event)
    _atomic_json(root / "events" / f"{len(state['events']):03d}.json", event)
    _persist(root, state)


def _new_state(case: Mapping[str, Any], run_id: str, operation_id: str) -> dict[str, Any]:
    scope = {"case_id": case["case_id"], "affected_months": case["scope"], "window": "[2023-09-01,2023-10-01)"}
    baseline = {
        "outside_scope_keys": ["2023-08:event-outside", "2023-10:event-outside"],
        "outside_scope_sha256": _sha256(["2023-08:event-outside", "2023-10:event-outside"]),
        "gold_snapshots": {name: 41 for name in GOLD_TABLES},
        "gold_counts": {name: 1 for name in GOLD_TABLES},
    }
    return {
        "task": "QA-04",
        "case_id": case["case_id"],
        "run_id": run_id,
        "operation_id": operation_id,
        "scope": scope,
        "scope_sha256": _sha256(scope),
        "baseline": baseline,
        "completed_stages": [],
        "downstream_calls": [],
        "events": [],
        "fault_consumed": False,
        "gold_snapshots": {},
        "gold_counts": {},
        "gold_receipt_present": False,
        "trino_verified": False,
        "published": False,
        "ml_status": "NOT_STARTED",
        "ml_candidate": False,
        "lease_released": False,
        "writer_quiescent": False,
        "fault_observation": {},
    }


def _record_stage(root: Path, state: dict[str, Any], stage: str) -> None:
    if stage not in state["downstream_calls"]:
        state["downstream_calls"].append(stage)
    _persist(root, state)


def _inject_fault(root: Path, state: dict[str, Any], case: Mapping[str, Any], stage: str) -> None:
    if state["fault_consumed"] or case["fault_stage"] != stage:
        return
    state["fault_consumed"] = True
    reason = case["reason_code"]
    state["fault_observation"] = {
        "stage": stage,
        "published": state["published"],
        "ml_candidate": state["ml_candidate"],
        "gold_snapshots": dict(state["gold_snapshots"]),
        "downstream_calls": list(state["downstream_calls"]),
    }
    if case["case_id"] == "gold_commit_partial":
        state["gold_snapshots"][GOLD_TABLES[0]] = 101
        state["gold_counts"][GOLD_TABLES[0]] = 1
        state["gold_receipt_present"] = False
    elif case["case_id"] == "gold_commit_receipt_lost":
        state["gold_snapshots"] = {name: 101 + index for index, name in enumerate(GOLD_TABLES)}
        state["gold_counts"] = {name: 1 for name in GOLD_TABLES}
        state["gold_receipt_present"] = False
    elif case["case_id"] == "ml_bundle_invalid":
        state["ml_status"] = "REJECTED"
    elif case["case_id"] == "ml_import_partial":
        state["ml_status"] = "IMPORT_VALIDATING"
        state["ml_snapshots"] = {"sequence_membership": 201}
    _event(root, state, {"status": "FAILED", "stage": stage, "reason_code": reason, "downstream_called": False})
    raise Qa04Error(reason)


def _run_stage(root: Path, state: dict[str, Any], case: Mapping[str, Any], stage: str) -> None:
    if stage in state["completed_stages"]:
        return
    _record_stage(root, state, stage)
    _inject_fault(root, state, case, stage)
    if stage == "FETCH":
        state["bronze_verified"] = True
    elif stage == "ARCHIVE":
        state["archive_verified"] = True
    elif stage == "SILVER":
        state["silver_verified"] = True
    elif stage == "GOLD":
        for index, name in enumerate(GOLD_TABLES):
            state["gold_snapshots"].setdefault(name, 101 + index)
            state["gold_counts"].setdefault(name, 1)
        state["gold_receipt_present"] = True
    elif stage == "TRINO":
        state["trino_verified"] = True
    elif stage == "PUBLICATION":
        state["published"] = True
    state["completed_stages"].append(stage)
    _event(root, state, {"status": "SUCCEEDED", "stage": stage, "reason_code": "NONE"})


def _run_etl(root: Path, state: dict[str, Any], case: Mapping[str, Any]) -> None:
    for stage in ETL_STAGES:
        _run_stage(root, state, case, stage)


def _seed_published_gold(root: Path, state: dict[str, Any]) -> None:
    state["completed_stages"] = list(ETL_STAGES)
    state["downstream_calls"] = list(ETL_STAGES)
    state["bronze_verified"] = True
    state["archive_verified"] = True
    state["silver_verified"] = True
    state["gold_snapshots"] = {name: 101 + index for index, name in enumerate(GOLD_TABLES)}
    state["gold_counts"] = {name: 1 for name in GOLD_TABLES}
    state["gold_receipt_present"] = True
    state["trino_verified"] = True
    state["published"] = True
    _persist(root, state)


def _run_ml(root: Path, state: dict[str, Any], case: Mapping[str, Any]) -> None:
    state["downstream_calls"].append("ML_IMPORT")
    _inject_fault(root, state, case, "ML_IMPORT")
    if case["case_id"] == "ml_import_partial":
        state["ml_snapshots"].setdefault("sequence_summary", 202)
        state["ml_status"] = "CANDIDATE"
        state["ml_candidate"] = True
    else:
        state["ml_status"] = "CANDIDATE"
        state["ml_candidate"] = True
    _event(root, state, {"status": "SUCCEEDED", "stage": "ML_IMPORT", "reason_code": "NONE"})


def _recover_invalid_ml(root: Path, state: dict[str, Any]) -> None:
    state["replacement_run_id"] = state["run_id"] + "-replacement"
    state["replacement_operation_id"] = state["operation_id"] + "-replacement"
    state["replacement_status"] = "CANDIDATE"
    state["replacement_candidate"] = True
    _event(root, state, {"status": "RECOVERED", "stage": "ML_IMPORT", "reason_code": "NEW_RUN_ID_REQUIRED"})


def _recover_lease(root: Path, state: dict[str, Any], env: Mapping[str, str], identity: Mapping[str, str]) -> None:
    state["writer_quiescent"] = False
    contender = {"dag_id": identity["dag_id"], "run_id": identity["run_id"] + "-contender"}
    try:
        lease("acquire", contender, env)
    except SourceRunBusy:
        state["contender_blocked"] = True
    else:
        raise Qa04Error("QA04_CONTENDER_NOT_BLOCKED")
    state["fault_observation"] = {
        "stage": "LEASE",
        "published": state["published"],
        "ml_candidate": state["ml_candidate"],
        "gold_snapshots": dict(state["gold_snapshots"]),
        "downstream_calls": list(state["downstream_calls"]),
    }
    _event(root, state, {"status": "FAILED", "stage": "LEASE", "reason_code": "TIMEOUT_LEASE_RETAINED"})
    _persist(root, state)
    state["writer_quiescent"] = True
    lease("assert", identity, env)


def _assert_scope(state: Mapping[str, Any]) -> None:
    current = state["baseline"]["outside_scope_keys"]
    if _sha256(current) != state["baseline"]["outside_scope_sha256"]:
        raise Qa04Error("QA04_OUTSIDE_SCOPE_CHANGED")
    if state["scope_sha256"] != _sha256(state["scope"]):
        raise Qa04Error("QA04_SCOPE_IDENTITY_CHANGED")


def _safe_report(root: Path, state: Mapping[str, Any], case: Mapping[str, Any]) -> dict[str, Any]:
    _assert_scope(state)
    report = {
        "task": "QA-04",
        "case_id": state["case_id"],
        "status": "PASS",
        "mode": "offline_control",
        "fault": {"stage": case["fault_stage"], "reason_code": case["reason_code"]},
        "fault_observation": state["fault_observation"],
        "recovery": {
            "boundary": case["recovery_boundary"],
            "same_scope": True,
            "same_operation_id": state["case_id"] != "ml_bundle_invalid",
            "outside_scope_unchanged": True,
        },
        "downstream_calls": state["downstream_calls"],
        "gold": {
            "published": state["published"],
            "snapshots": state["gold_snapshots"],
            "counts": state["gold_counts"],
            "receipt_present": state["gold_receipt_present"],
            "trino_verified": state["trino_verified"],
        },
        "ml": {
            "original_status": state["ml_status"],
            "original_candidate": state["ml_candidate"],
            "replacement_candidate": state.get("replacement_candidate", False),
            "snapshots": state.get("ml_snapshots", {}),
        },
        "lease": {
            "contender_blocked": state.get("contender_blocked", False),
            "writer_quiescent": state.get("writer_quiescent", True),
            "released": state["lease_released"],
        },
        "mutations": {"lake_writes": False, "deletions": [], "outside_scope_writes": []},
        "scope": state["scope"],
        "event_count": len(state["events"]),
        "report_path": str(root / "recovery-report.json"),
    }
    _atomic_json(root / "recovery-report.json", report)
    return report


def run_offline_case(case_id: str, output_root: str | Path | None = None) -> dict[str, Any]:
    matrix = load_matrix()
    case = _case(matrix, case_id)
    temporary: tempfile.TemporaryDirectory[str] | None = None
    if output_root is None:
        temporary = tempfile.TemporaryDirectory(prefix=f"qa04-{case_id}-")
        root = Path(temporary.name)
    else:
        root = Path(output_root) / case_id
        root.mkdir(parents=True, exist_ok=True)
    (root / "events").mkdir(parents=True, exist_ok=True)
    run_id = f"qa04-{case_id}-001"
    operation_id = f"op-{case_id}-001"
    state = _new_state(case, run_id, operation_id)
    _persist(root, state)
    env = {"SOURCE_GUARD_ROOT": str(root / "guard")}
    identity = {"dag_id": "qa_04_failure_recovery", "run_id": run_id}
    lease("acquire", identity, env)
    try:
        if case_id == "lease_timeout_contender":
            _recover_lease(root, state, env, identity)
        elif case_id.startswith("ml_"):
            _seed_published_gold(root, state)
            try:
                _run_ml(root, state, case)
            except Qa04Error:
                if case_id == "ml_bundle_invalid":
                    _recover_invalid_ml(root, state)
                else:
                    _run_ml(root, state, {**case, "fault_stage": "NONE"})
        else:
            try:
                _run_etl(root, state, case)
            except Qa04Error:
                _run_etl(root, state, {**case, "fault_stage": "NONE"})
        state["writer_quiescent"] = True
        _persist(root, state)
        _release_exact_lease(root, state, identity, env)
        return _safe_report(root, state, case)
    except BaseException:
        # An interrupted writer may still be active. Keep both state and owner
        # until a later recovery proves quiescence and releases this exact owner.
        _persist(root, state)
        raise
    finally:
        if temporary is not None:
            if state.get("lease_released"):
                temporary.cleanup()
            else:
                temporary._finalizer.detach()


def _release_exact_lease(root: Path, state: dict[str, Any], identity: Mapping[str, str],
                         env: Mapping[str, str]) -> None:
    if state.get("writer_quiescent") is not True:
        raise Qa04Error("QA04_WRITER_NOT_QUIESCENT")
    held = lease("assert", identity, env)
    if held.get("status") != "HELD" or any(held.get(key) != identity[key] for key in identity):
        raise Qa04Error("QA04_LEASE_OWNER_MISMATCH")
    released = lease("release", identity, env)
    if released.get("status") != "RELEASED" or any(released.get(key) != identity[key] for key in identity):
        raise Qa04Error("QA04_LEASE_RELEASE_UNCONFIRMED")
    state["lease_released"] = True
    _persist(root, state)


def _validate_runtime_request(request: Mapping[str, Any], matrix: Mapping[str, Any]) -> None:
    allowed_fields = {"schema_version", "task_id", "mode", "case_ids", "matrix_sha256", "run_id",
                      "operation_id", "namespace", "case_scopes", "scope_sha256", "lease_owner",
                      "input_pins", "artifact_refs", "baseline_snapshots", "report_path"}
    if set(request) != allowed_fields:
        raise Qa04Error("QA04_RUNTIME_REQUEST_FIELDS_INVALID")
    if request.get("schema_version") != RUNTIME_REQUEST_VERSION or request.get("task_id") != "QA-04" or request.get("mode") != "runtime":
        raise Qa04Error("QA04_RUNTIME_REQUEST_INVALID")
    case_ids = request.get("case_ids")
    if (not isinstance(case_ids, list) or not case_ids
            or any(not isinstance(case, str) or case not in SAFE_CASE for case in case_ids)
            or len(case_ids) != len(set(case_ids))):
        raise Qa04Error("QA04_RUNTIME_CASES_INVALID")
    namespace = request.get("namespace")
    if not isinstance(namespace, str) or not NAMESPACE_RE.fullmatch(namespace):
        raise Qa04Error("QA04_RUNTIME_NAMESPACE_INVALID")
    for key in ("run_id", "operation_id"):
        if not isinstance(request.get(key), str) or not IDENTITY_RE.fullmatch(request[key]):
            raise Qa04Error("QA04_RUNTIME_IDENTITY_INVALID")
    lease_owner = request.get("lease_owner")
    if (not isinstance(lease_owner, dict) or set(lease_owner) != {"dag_id", "run_id"}
            or any(not isinstance(value, str) or not IDENTITY_RE.fullmatch(value) for value in lease_owner.values())):
        raise Qa04Error("QA04_RUNTIME_LEASE_IDENTITY_INVALID")
    if lease_owner["run_id"] == request["run_id"]:
        raise Qa04Error("QA04_RUNTIME_LEASE_IDENTITY_INVALID")
    if not isinstance(request.get("matrix_sha256"), str) or not SHA256_RE.fullmatch(request["matrix_sha256"]):
        raise Qa04Error("QA04_RUNTIME_MATRIX_PIN_INVALID")
    scopes = request.get("case_scopes")
    if not isinstance(scopes, dict) or set(scopes) != set(case_ids):
        raise Qa04Error("QA04_RUNTIME_SCOPE_INVALID")
    for case_id in case_ids:
        case = _case(matrix, case_id)
        scope = scopes[case_id]
        if not isinstance(scope, dict) or scope.get("scope_tokens") != case["scope"]:
            raise Qa04Error("QA04_RUNTIME_SCOPE_INVALID")
        if case_id.startswith("ml_"):
            if set(scope) != {"scope_tokens", "experiment_run_ids", "dataset_refs", "gold_publication_refs"}:
                raise Qa04Error("QA04_RUNTIME_ML_SCOPE_INVALID")
            run_ids = scope.get("experiment_run_ids")
            if (not isinstance(run_ids, list) or not run_ids
                    or any(not isinstance(value, str) or not IDENTITY_RE.fullmatch(value) for value in run_ids)
                    or len(run_ids) != len(set(run_ids))):
                raise Qa04Error("QA04_RUNTIME_ML_SCOPE_INVALID")
            for ref_key in ("dataset_refs", "gold_publication_refs"):
                refs = scope.get(ref_key)
                if not _valid_pins(refs):
                    raise Qa04Error("QA04_RUNTIME_ML_SCOPE_INVALID")
        else:
            if set(scope) != {"scope_tokens", "write_months", "window_start_utc", "window_end_utc"}:
                raise Qa04Error("QA04_RUNTIME_SCOPE_INVALID")
            months = scope.get("write_months")
            if months != case["scope"] or any(not isinstance(value, str) or not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value)
                                                for value in months or []):
                raise Qa04Error("QA04_RUNTIME_SCOPE_INVALID")
            start = scope.get("window_start_utc")
            end = scope.get("window_end_utc")
            expected_start, expected_end = _month_window(months[0]) if len(months) == 1 else (None, None)
            if not expected_start or start != expected_start or end != expected_end:
                raise Qa04Error("QA04_RUNTIME_WINDOW_INVALID")
    if request.get("scope_sha256") != _sha256(scopes):
        raise Qa04Error("QA04_RUNTIME_SCOPE_HASH_MISMATCH")
    for key in ("input_pins", "artifact_refs"):
        if not _valid_pins(request.get(key)):
            raise Qa04Error("QA04_RUNTIME_PIN_INVALID")
    runtime_runners = [pin for pin in request["artifact_refs"] if pin["kind"] == "qa04-runtime-runner"]
    if len(runtime_runners) != 1 or not Path(runtime_runners[0]["reference"]).is_absolute():
        raise Qa04Error("QA04_RUNTIME_RUNNER_PIN_INVALID")
    snapshots = request.get("baseline_snapshots")
    if not isinstance(snapshots, list) or not snapshots:
        raise Qa04Error("QA04_RUNTIME_BASELINE_INVALID")
    seen_tables: set[str] = set()
    for item in snapshots:
        if (not isinstance(item, dict) or not isinstance(item.get("table"), str)
                or not re.fullmatch(re.escape(namespace) + r"\.[a-z][a-z0-9_]*", item["table"])):
            raise Qa04Error("QA04_RUNTIME_BASELINE_INVALID")
        if item["table"] in seen_tables:
            raise Qa04Error("QA04_RUNTIME_BASELINE_INVALID")
        seen_tables.add(item["table"])
        if item.get("state") == "PRESENT":
            if set(item) != {"table", "state", "snapshot_id"}:
                raise Qa04Error("QA04_RUNTIME_BASELINE_INVALID")
            snapshot_id = item.get("snapshot_id")
            if isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or not 1 <= snapshot_id <= MAX_SNAPSHOT_ID:
                raise Qa04Error("QA04_RUNTIME_BASELINE_INVALID")
        elif item.get("state") != "ABSENT" or set(item) != {"table", "state"}:
            raise Qa04Error("QA04_RUNTIME_BASELINE_INVALID")
    report_path = request.get("report_path")
    if not isinstance(report_path, str) or not report_path:
        raise Qa04Error("QA04_RUNTIME_REPORT_PATH_INVALID")


def _valid_pins(values: Any) -> bool:
    if not isinstance(values, list) or not values:
        return False
    for pin in values:
        if not isinstance(pin, dict) or set(pin) != {"kind", "reference", "sha256"}:
            return False
        if not all(isinstance(pin.get(field), str) and pin[field] for field in ("kind", "reference")):
            return False
        if not isinstance(pin.get("sha256"), str) or not SHA256_RE.fullmatch(pin["sha256"]):
            return False
        reference = pin["reference"]
        authority = reference.split("://", 1)[1].split("/", 1)[0] if "://" in reference else ""
        if any(mark in reference for mark in ("*", "?", "#")) or "@" in authority:
            return False
    return True


def _month_window(month: str) -> tuple[str, str]:
    year, month_number = (int(part) for part in month.split("-"))
    if month_number == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month_number + 1
    return f"{year:04d}-{month_number:02d}-01T00:00:00Z", f"{next_year:04d}-{next_month:02d}-01T00:00:00Z"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(131072), b""):
            digest.update(block)
    return digest.hexdigest()


def _inside_staging(path_value: str, staging_root: Path, *, must_exist: bool) -> Path:
    path = Path(path_value)
    if not path.is_absolute():
        path = staging_root / path
    try:
        resolved_root = staging_root.resolve(strict=True)
        resolved = path.resolve(strict=must_exist)
        resolved.relative_to(resolved_root)
    except (OSError, ValueError):
        raise Qa04Error("QA04_RUNTIME_PATH_UNSAFE") from None
    cursor = path
    while cursor != cursor.parent:
        if cursor.is_symlink():
            raise Qa04Error("QA04_RUNTIME_PATH_UNSAFE")
        if cursor.resolve(strict=False) == resolved_root:
            break
        cursor = cursor.parent
    if must_exist and (not resolved.is_file()):
        raise Qa04Error("QA04_RUNTIME_PATH_UNSAFE")
    if not must_exist and resolved.exists():
        raise Qa04Error("QA04_RUNTIME_REPORT_EXISTS")
    return resolved


def _validate_runtime_report(report: Mapping[str, Any], request: Mapping[str, Any], request_sha: str,
                             matrix: Mapping[str, Any], matrix_sha: str, staging_root: Path) -> None:
    runner_sha = next(pin["sha256"] for pin in request["artifact_refs"] if pin["kind"] == "qa04-runtime-runner")
    expected_identity = {"task": "QA-04", "schema_version": RUNTIME_REPORT_VERSION, "mode": "runtime",
                         "request_sha256": request_sha, "matrix_sha256": matrix_sha,
                         "run_id": request["run_id"], "operation_id": request["operation_id"],
                         "namespace": request["namespace"], "scope_sha256": request["scope_sha256"],
                         "lease_owner": request["lease_owner"], "runner_sha256": runner_sha}
    if any(report.get(key) != value for key, value in expected_identity.items()):
        raise Qa04Error("QA04_RUNTIME_REPORT_IDENTITY_MISMATCH")
    if report.get("status") != "PASS":
        raise Qa04Error("QA04_RUNTIME_REPORT_INVALID")
    results = report.get("cases")
    if not isinstance(results, list) or len(results) != len(request["case_ids"]):
        raise Qa04Error("QA04_RUNTIME_REPORT_CASES_INVALID")
    by_id = {item.get("case_id"): item for item in results if isinstance(item, dict)}
    if len(by_id) != len(results) or set(by_id) != set(request["case_ids"]):
        raise Qa04Error("QA04_RUNTIME_REPORT_CASES_INVALID")
    for case_id in request["case_ids"]:
        expected = _case(matrix, case_id)
        result = by_id[case_id]
        if result.get("status") != "PASS" or result.get("fault") != {"stage": expected["fault_stage"], "reason_code": expected["reason_code"], "observed": True}:
            raise Qa04Error("QA04_RUNTIME_CASE_NOT_PASSED")
        if result.get("downstream_blocked") is not True or result.get("recovery_status") != "RECOVERED":
            raise Qa04Error("QA04_RUNTIME_CASE_RECOVERY_INVALID")
        evidence_ref = result.get("evidence")
        if not isinstance(evidence_ref, dict) or not isinstance(evidence_ref.get("path"), str):
            raise Qa04Error("QA04_RUNTIME_EVIDENCE_MISSING")
        if not isinstance(evidence_ref.get("sha256"), str) or not SHA256_RE.fullmatch(evidence_ref["sha256"]):
            raise Qa04Error("QA04_RUNTIME_EVIDENCE_CHECKSUM_INVALID")
        evidence_path = _inside_staging(evidence_ref["path"], staging_root, must_exist=True)
        raw = evidence_path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != evidence_ref["sha256"]:
            raise Qa04Error("QA04_RUNTIME_EVIDENCE_CHECKSUM_INVALID")
        evidence = _read_json(evidence_path, MAX_REPORT_BYTES)
        required = {"schema_version": RUNTIME_REPORT_VERSION, "request_sha256": request_sha,
                    "matrix_sha256": matrix_sha, "run_id": request["run_id"],
                    "operation_id": request["operation_id"], "case_id": case_id,
                    "scope_sha256": _sha256(request["case_scopes"][case_id]),
                    "lease_owner": request["lease_owner"], "runner_sha256": runner_sha}
        if evidence.get("producer") != "qa04-runtime-adapter" or any(evidence.get(key) != value for key, value in required.items()):
            raise Qa04Error("QA04_RUNTIME_EVIDENCE_IDENTITY_MISMATCH")
        readback = evidence.get("readback")
        if not isinstance(readback, dict) or readback.get("status") != "PASSED":
            raise Qa04Error("QA04_RUNTIME_READBACK_MISSING")
        outside_scope = evidence.get("outside_scope")
        if not isinstance(outside_scope, dict):
            raise Qa04Error("QA04_RUNTIME_OUTSIDE_SCOPE_CHANGED")
        if not _valid_snapshot_refs(evidence.get("snapshots_before")) or not _valid_snapshot_refs(evidence.get("snapshots_after")):
            raise Qa04Error("QA04_RUNTIME_SNAPSHOTS_MISSING")
        after_ids = {(item["table"], item.get("snapshot_id")) for item in evidence["snapshots_after"] if item["state"] == "PRESENT"}
        readback_ids = readback.get("snapshots")
        if (not _valid_snapshot_refs(readback_ids)
                or {(item["table"], item.get("snapshot_id")) for item in readback_ids if item["state"] == "PRESENT"} != after_ids
                or readback.get("prior_snapshots_readable") is not True):
            raise Qa04Error("QA04_RUNTIME_READBACK_MISMATCH")
        if (evidence.get("fault") != {"stage": expected["fault_stage"], "reason_code": expected["reason_code"], "observed": True}
                or evidence.get("downstream_blocked") is not True
                or evidence.get("recovery_status") != "RECOVERED"):
            raise Qa04Error("QA04_RUNTIME_EVIDENCE_CONTENT_INVALID")
        downstream_calls = evidence.get("downstream_calls")
        if (not isinstance(downstream_calls, list) or any(not isinstance(stage, str) for stage in downstream_calls)
                or set(downstream_calls) - set(ETL_STAGES) - {"ML_IMPORT"}):
            raise Qa04Error("QA04_RUNTIME_DOWNSTREAM_EVIDENCE_INVALID")
        if expected["fault_stage"] in ETL_STAGES:
            failed_index = ETL_STAGES.index(expected["fault_stage"])
            if any(stage in ETL_STAGES[failed_index + 1:] for stage in downstream_calls):
                raise Qa04Error("QA04_RUNTIME_DOWNSTREAM_EVIDENCE_INVALID")
        before_scope = evidence["outside_scope"].get("before")
        after_scope = evidence["outside_scope"].get("after")
        if (not isinstance(before_scope, dict) or before_scope != after_scope
                or isinstance(before_scope.get("count"), bool) or not isinstance(before_scope.get("count"), int)
                or before_scope["count"] < 0
                or not SHA256_RE.fullmatch(str(before_scope.get("keys_sha256", "")))
                or not SHA256_RE.fullmatch(str(before_scope.get("content_sha256", "")))):
            raise Qa04Error("QA04_RUNTIME_OUTSIDE_SCOPE_CHANGED")
        before_by_table = {(item["table"], item["state"], item.get("snapshot_id")) for item in evidence["snapshots_before"]}
        for baseline in request["baseline_snapshots"]:
            expected_baseline = (baseline["table"], baseline["state"], baseline.get("snapshot_id"))
            if expected_baseline not in before_by_table:
                raise Qa04Error("QA04_RUNTIME_BASELINE_EVIDENCE_MISMATCH")
        _validate_special_case_evidence(case_id, evidence, request)
    summary = report.get("summary")
    if summary != {"total": len(request["case_ids"]), "passed": len(request["case_ids"]), "failed": 0, "pending": 0}:
        raise Qa04Error("QA04_RUNTIME_SUMMARY_MISMATCH")


def _valid_snapshot_refs(value: Any) -> bool:
    if not isinstance(value, list) or not value:
        return False
    seen: set[tuple[str, str, int | None]] = set()
    for item in value:
        if not isinstance(item, dict) or not isinstance(item.get("table"), str) or not item["table"]:
            return False
        if item.get("state") == "PRESENT":
            if set(item) != {"table", "state", "snapshot_id"}:
                return False
            snapshot_id = item.get("snapshot_id")
            if isinstance(snapshot_id, bool) or not isinstance(snapshot_id, int) or not 1 <= snapshot_id <= MAX_SNAPSHOT_ID:
                return False
        elif item.get("state") == "ABSENT":
            if set(item) != {"table", "state"}:
                return False
            snapshot_id = None
        else:
            return False
        identity = (item["table"], item["state"], snapshot_id)
        if identity in seen:
            return False
        seen.add(identity)
    return True


def _validate_special_case_evidence(case_id: str, evidence: Mapping[str, Any], request: Mapping[str, Any]) -> None:
    if case_id in {"gold_commit_partial", "gold_commit_receipt_lost", "trino_verify_failure", "publication_metadata_failure"}:
        gold = evidence.get("gold")
        if (not isinstance(gold, dict) or gold.get("operation_id") != request["operation_id"]
                or not SHA256_RE.fullmatch(str(gold.get("fingerprint_sha256", "")))
                or gold.get("published_before_recovery") is not False):
            raise Qa04Error("QA04_RUNTIME_GOLD_EVIDENCE_INVALID")
        tables = gold.get("tables")
        if not isinstance(tables, list) or len(tables) != len(GOLD_TABLES):
            raise Qa04Error("QA04_RUNTIME_GOLD_EVIDENCE_INVALID")
        by_name = {table.get("name"): table for table in tables if isinstance(table, dict)}
        if set(by_name) != set(GOLD_TABLES) or any(not isinstance(row.get("snapshot_id"), int) or isinstance(row.get("snapshot_id"), bool) or not 1 <= row["snapshot_id"] <= MAX_SNAPSHOT_ID for row in by_name.values()):
            raise Qa04Error("QA04_RUNTIME_GOLD_EVIDENCE_INVALID")
        counts = gold.get("counts")
        if (not isinstance(counts, dict) or set(counts) != set(GOLD_TABLES)
                or any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts.values())):
            raise Qa04Error("QA04_RUNTIME_GOLD_EVIDENCE_INVALID")
        publication = gold.get("publication")
        if (not isinstance(publication, dict) or publication.get("engine") != "TRINO"
                or publication.get("verify_status") != "PASSED" or publication.get("published") is not True
                or not SHA256_RE.fullmatch(str(publication.get("bundle_sha256", "")))
                or publication.get("bundle_sha256") != gold.get("commit_sha256")
                or publication.get("scope_sha256") != evidence.get("scope_sha256")
                or publication.get("trino_checks") != {"six_table_snapshot_readback": "PASSED",
                                                         "count_reconciliation": "PASSED",
                                                         "outside_scope_unchanged": "PASSED"}):
            raise Qa04Error("QA04_RUNTIME_GOLD_PUBLICATION_INVALID")
        published_snapshots = publication.get("table_snapshots")
        expected_snapshots = {name: row["snapshot_id"] for name, row in by_name.items()}
        if published_snapshots != expected_snapshots:
            raise Qa04Error("QA04_RUNTIME_GOLD_PUBLICATION_INVALID")
        report_snapshots = {item["table"].rsplit(".", 1)[-1]: item.get("snapshot_id") for item in evidence["snapshots_after"]
                            if item["state"] == "PRESENT" and item["table"].startswith(request["namespace"] + ".")}
        if report_snapshots != expected_snapshots:
            raise Qa04Error("QA04_RUNTIME_GOLD_PUBLICATION_INVALID")
    if case_id.startswith("ml_"):
        ml = evidence.get("ml")
        if not isinstance(ml, dict) or ml.get("operation_id") != request["operation_id"] or not SHA256_RE.fullmatch(str(ml.get("fingerprint_sha256", ""))):
            raise Qa04Error("QA04_RUNTIME_ML_EVIDENCE_INVALID")
        scope = request["case_scopes"][case_id]
        if ml.get("dataset_refs") != scope["dataset_refs"] or ml.get("gold_publication_refs") != scope["gold_publication_refs"]:
            raise Qa04Error("QA04_RUNTIME_ML_EVIDENCE_INVALID")
        if case_id == "ml_bundle_invalid" and (ml.get("original_status") != "REJECTED" or ml.get("original_candidate") is not False or ml.get("replacement_experiment_run_id") in ml.get("rejected_experiment_run_ids", [])):
            raise Qa04Error("QA04_RUNTIME_ML_REJECTION_INVALID")
        if case_id == "ml_bundle_invalid" and ml.get("replacement_experiment_run_id") in request["case_scopes"][case_id]["experiment_run_ids"]:
            raise Qa04Error("QA04_RUNTIME_ML_REJECTION_INVALID")
        if case_id == "ml_import_partial" and (ml.get("status") != "CANDIDATE" or ml.get("candidate") is not True or ml.get("resume_same_fingerprint") is not True):
            raise Qa04Error("QA04_RUNTIME_ML_RECOVERY_INVALID")
        if case_id == "ml_import_partial" and (ml.get("journal_status") != "CANDIDATE"
                or ml.get("readback_status") != "PASSED" or ml.get("publication_fingerprint_sha256") != ml.get("fingerprint_sha256")):
            raise Qa04Error("QA04_RUNTIME_ML_RECOVERY_INVALID")
        if case_id == "ml_import_partial":
            ml_snapshots = ml.get("snapshots")
            if (not _valid_snapshot_refs(ml_snapshots)
                    or {item["table"] for item in ml_snapshots} != {
                        request["namespace"] + ".sequence_membership", request["namespace"] + ".sequence_summary",
                        request["namespace"] + ".experiment_run"}):
                raise Qa04Error("QA04_RUNTIME_ML_RECOVERY_INVALID")
    if case_id == "lease_timeout_contender":
        lease_state = evidence.get("lease")
        if (not isinstance(lease_state, dict) or lease_state.get("owner") != request["lease_owner"]
                or lease_state.get("owner_retained_before_quiescence") is not True
                or lease_state.get("contender_blocked") is not True
                or lease_state.get("writer_quiescent_before_release") is not True
                or lease_state.get("release_status") != "RELEASED"
                or lease_state.get("release_owner_matches") is not True):
            raise Qa04Error("QA04_RUNTIME_LEASE_EVIDENCE_INVALID")


def run_runtime(request_path: str | Path, environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    staging_root_value = env.get("QA_04_STAGING_ROOT")
    if not staging_root_value:
        raise Qa04Error("QA04_RUNTIME_STAGING_ROOT_REQUIRED")
    staging_root = Path(staging_root_value)
    try:
        staging_root = staging_root.resolve(strict=True)
    except OSError:
        raise Qa04Error("QA04_RUNTIME_STAGING_ROOT_INVALID") from None
    request_file = _inside_staging(str(request_path), staging_root, must_exist=True)
    request_bytes = request_file.read_bytes()
    if len(request_bytes) > MAX_REQUEST_BYTES:
        raise Qa04Error("QA04_REQUEST_NOT_BOUNDED")
    request_sha = hashlib.sha256(request_bytes).hexdigest()
    request = _read_json(request_file, MAX_REQUEST_BYTES)
    matrix_bytes = MATRIX_PATH.read_bytes()
    matrix_sha = hashlib.sha256(matrix_bytes).hexdigest()
    matrix = json.loads(matrix_bytes)
    validate_matrix(matrix)
    if request.get("matrix_sha256") != matrix_sha:
        raise Qa04Error("QA04_RUNTIME_MATRIX_PIN_MISMATCH")
    _validate_runtime_request(request, matrix)
    report_file = _inside_staging(request["report_path"], staging_root, must_exist=False)
    if report_file == request_file:
        raise Qa04Error("QA04_RUNTIME_REPORT_PATH_INVALID")
    if env.get("ORC_07_STATUS") != "Done" or env.get("MLI_03_STATUS") != "Done":
        raise Qa04Error("QA04_RUNTIME_DEPENDENCY_PENDING")
    command_text = env.get("QA_04_RUNTIME_COMMAND", "").strip()
    if not command_text:
        raise Qa04Error("QA04_RUNTIME_COMMAND_REQUIRED")
    try:
        timeout = int(env.get("QA_04_RUNTIME_TIMEOUT_SECONDS", "3600"))
    except ValueError:
        raise Qa04Error("QA04_RUNTIME_TIMEOUT_INVALID") from None
    if not 1 <= timeout <= 3600:
        raise Qa04Error("QA04_RUNTIME_TIMEOUT_INVALID")
    command = shlex.split(command_text)
    if not command:
        raise Qa04Error("QA04_RUNTIME_COMMAND_REQUIRED")
    runner_pin = next(pin for pin in request["artifact_refs"] if pin["kind"] == "qa04-runtime-runner")
    runner_path = Path(runner_pin["reference"])
    if (Path(command[0]) != runner_path or runner_path.is_symlink() or not runner_path.is_file()
            or _file_sha256(runner_path) != runner_pin["sha256"]):
        raise Qa04Error("QA04_RUNTIME_RUNNER_PIN_INVALID")
    descriptor, pinned_name = tempfile.mkstemp(prefix="qa04-request-", suffix=".json", dir=staging_root)
    pinned_path = Path(pinned_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(request_bytes)
            stream.flush()
            os.fsync(stream.fileno())
        pinned_env = dict(env)
        pinned_env["QA_04_REQUEST_SHA256"] = request_sha
        try:
            completed = subprocess.run(command + [str(pinned_path)], env=pinned_env, stdout=subprocess.DEVNULL,
                                       stderr=subprocess.DEVNULL, timeout=timeout, check=False, shell=False)
        except (OSError, subprocess.TimeoutExpired):
            raise Qa04Error("QA04_RUNTIME_RUNNER_FAILED") from None
        if completed.returncode != 0:
            raise Qa04Error("QA04_RUNTIME_RUNNER_FAILED")
        if request_file.read_bytes() != request_bytes or hashlib.sha256(MATRIX_PATH.read_bytes()).hexdigest() != matrix_sha:
            raise Qa04Error("QA04_RUNTIME_INPUT_DRIFT")
        report = _read_json(report_file, MAX_REPORT_BYTES)
        _validate_runtime_report(report, request, request_sha, matrix, matrix_sha, staging_root)
        return report
    finally:
        try:
            pinned_path.unlink()
        except FileNotFoundError:
            pass


def prepare_report(matrix_path: str | Path, output_json: str | Path, output_markdown: str | Path) -> dict[str, Any]:
    matrix = load_matrix(matrix_path)
    report = {
        "task": "QA-04",
        "status": "PREPARED",
        "runtime_request_contract": RUNTIME_REQUEST_VERSION,
        "runtime_report_contract": RUNTIME_REPORT_VERSION,
        "evidence_scope": "offline fixture and control-plane contract only",
        "runtime_dependencies": {"ORC-05": "Done", "ORC-07": "Ready", "MLI-03": "In Progress",
                                 "PR-79": "merged; runtime acceptance remains incomplete"},
        "cases": [
            {"case_id": item["case_id"], "status": "PENDING_RUNTIME", "fault_stage": item["fault_stage"],
             "reason_code": item["reason_code"], "scope": item["scope"]}
            for item in matrix["cases"]
        ],
    }
    _atomic_json(Path(output_json), report)
    lines = ["# QA-04 prepared evidence", "", "Status: `PREPARED`",
             "", f"Runtime request: `{RUNTIME_REQUEST_VERSION}`; runtime report: `{RUNTIME_REPORT_VERSION}`.",
             "", "Runtime evidence remains pending until ORC-07 and MLI-03 are Done.",
             "PR #79 is merged; MLI-03 runtime acceptance remains incomplete.",
             "", "| Case | Fault | Status |", "|---|---|---|"]
    lines.extend(f"| `{item['case_id']}` | `{item['reason_code']}` | `PENDING_RUNTIME` |" for item in report["cases"])
    Path(output_markdown).parent.mkdir(parents=True, exist_ok=True)
    Path(output_markdown).write_text("\n".join(lines) + "\n", encoding="utf-8")
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--matrix", default=str(MATRIX_PATH))
    prepare.add_argument("--json", required=True)
    prepare.add_argument("--markdown", required=True)
    offline = sub.add_parser("offline")
    offline.add_argument("--case", choices=sorted(SAFE_CASE), required=True)
    offline.add_argument("--root", required=True)
    runtime = sub.add_parser("runtime")
    runtime.add_argument("--request", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            print(json.dumps(prepare_report(args.matrix, args.json, args.markdown), sort_keys=True))
        elif args.command == "offline":
            print(json.dumps(run_offline_case(args.case, args.root), sort_keys=True))
        else:
            print(json.dumps(run_runtime(args.request), sort_keys=True))
    except Qa04Error as error:
        print(str(error))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
