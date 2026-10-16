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
        state["lease_released"] = True
        lease("release", identity, env)
        return _safe_report(root, state, case)
    except BaseException:
        # The caller can inspect the persisted state after a fault. Normal
        # successful recovery releases only the exact owner above.
        if not state["lease_released"]:
            lease("release", identity, env)
        raise
    finally:
        if temporary is not None:
            # Keep report data in the return value; the caller supplies an
            # output root when it needs files for evidence.
            temporary.cleanup()


def _validate_runtime_request(request: Mapping[str, Any]) -> None:
    if request.get("task_id") != "QA-04" or request.get("mode") != "runtime":
        raise Qa04Error("QA04_RUNTIME_REQUEST_INVALID")
    case_ids = request.get("case_ids")
    if not isinstance(case_ids, list) or not case_ids or any(case not in SAFE_CASE for case in case_ids):
        raise Qa04Error("QA04_RUNTIME_CASES_INVALID")
    namespace = request.get("namespace")
    if not isinstance(namespace, str) or not namespace.startswith("iceberg.qa04_"):
        raise Qa04Error("QA04_RUNTIME_NAMESPACE_INVALID")
    for key in ("run_id", "operation_id", "scope_sha256", "report_path"):
        if not isinstance(request.get(key), str) or not request[key]:
            raise Qa04Error("QA04_RUNTIME_IDENTITY_INVALID")
    for key in ("input_pins", "baseline_snapshots", "artifact_refs", "scope"):
        if not isinstance(request.get(key), (list, dict)) or not request[key]:
            raise Qa04Error("QA04_RUNTIME_PINS_REQUIRED")


def run_runtime(request_path: str | Path, environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    request = _read_json(Path(request_path), MAX_REQUEST_BYTES)
    _validate_runtime_request(request)
    if env.get("ORC_07_STATUS") != "Done" or env.get("MLI_03_STATUS") != "Done":
        raise Qa04Error("QA04_RUNTIME_DEPENDENCY_PENDING")
    command_text = env.get("QA_04_RUNTIME_COMMAND", "").strip()
    if not command_text:
        raise Qa04Error("QA04_RUNTIME_COMMAND_REQUIRED")
    timeout = int(env.get("QA_04_RUNTIME_TIMEOUT_SECONDS", "3600"))
    if not 1 <= timeout <= 3600:
        raise Qa04Error("QA04_RUNTIME_TIMEOUT_INVALID")
    command = shlex.split(command_text)
    if not command:
        raise Qa04Error("QA04_RUNTIME_COMMAND_REQUIRED")
    try:
        completed = subprocess.run(command + [str(request_path)], env=env, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL, timeout=timeout, check=False, shell=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise Qa04Error("QA04_RUNTIME_RUNNER_FAILED") from error
    if completed.returncode != 0:
        raise Qa04Error("QA04_RUNTIME_RUNNER_FAILED")
    report_path = Path(request["report_path"])
    report = _read_json(report_path, MAX_REPORT_BYTES)
    if report.get("task") != "QA-04" or report.get("status") != "PASS":
        raise Qa04Error("QA04_RUNTIME_REPORT_INVALID")
    return report


def prepare_report(matrix_path: str | Path, output_json: str | Path, output_markdown: str | Path) -> dict[str, Any]:
    matrix = load_matrix(matrix_path)
    report = {
        "task": "QA-04",
        "status": "PREPARED",
        "evidence_scope": "offline fixture and control-plane contract only",
        "runtime_dependencies": {"ORC-05": "Done", "ORC-07": "Ready", "MLI-03": "In Progress"},
        "cases": [
            {"case_id": item["case_id"], "status": "PENDING_RUNTIME", "fault_stage": item["fault_stage"],
             "reason_code": item["reason_code"], "scope": item["scope"]}
            for item in matrix["cases"]
        ],
    }
    _atomic_json(Path(output_json), report)
    lines = ["# QA-04 prepared evidence", "", "Status: `PREPARED`", "", "Runtime evidence remains pending until ORC-07 and MLI-03 are Done.", "", "| Case | Fault | Status |", "|---|---|---|"]
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
