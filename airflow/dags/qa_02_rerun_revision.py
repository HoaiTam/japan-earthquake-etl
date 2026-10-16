"""Prepare and, when a real adapter is supplied, verify QA-02 evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
from typing import Any, Mapping


class Qa02Error(RuntimeError):
    pass


SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
SAFE_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SAFE_TABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_.]{0,127}\Z")
SAFE_SNAPSHOT = re.compile(r"[1-9][0-9]{0,31}\Z")
RUNTIME_REQUEST_VERSION = "qa-02-runtime-v1"
RUNTIME_REPORT_VERSION = "qa-02-report-v2"
SUPPORTED_CASE_ASSERTIONS = {
    "retry": {"logical_keys", "canonical_count", "snapshots"},
    "duplicate": {"logical_keys", "current", "canonical_count", "history"},
    "late_revision": {"logical_keys", "current", "history"},
    "source_overlap": {"canonical_count", "lineage"},
    "partition_move": {"old_partition", "new_partition", "history"},
    "withdrawal": {"current", "history", "canonical_count"},
    "retention": {"scope", "snapshots"},
}


def required_assertions(case: Mapping[str, Any]) -> set[str]:
    category = case.get("category")
    if category == "late_revision":
        if str(case.get("case_id", "")).startswith("jma_"):
            return {"current", "release", "old_release"}
        return {"current", "history", "logical_keys"}
    return set(SUPPORTED_CASE_ASSERTIONS[category])


def _load(path: Path, limit: int = 256 * 1024) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise Qa02Error("QA02_MATRIX_NOT_BOUNDED")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise Qa02Error("QA02_MATRIX_INVALID_JSON") from error
    if not isinstance(value, dict):
        raise Qa02Error("QA02_MATRIX_OBJECT_REQUIRED")
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(262144), b""):
                digest.update(block)
    except OSError as error:
        raise Qa02Error("QA02_MATRIX_READ_FAILED") from error
    return digest.hexdigest()


def _evidence_sha256(path: Path) -> str:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 4 * 1024 * 1024:
        raise Qa02Error("QA02_EVIDENCE_INVALID")
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(262144), b""):
                digest.update(block)
    except OSError as error:
        raise Qa02Error("QA02_EVIDENCE_INVALID") from error
    return digest.hexdigest()


def _safe_path(value: Any, reason: str, *, must_exist: bool) -> Path:
    if not isinstance(value, str) or not value or ".." in Path(value).parts:
        raise Qa02Error(reason)
    path = Path(value)
    if path.is_symlink() or (must_exist and not path.is_file()):
        raise Qa02Error(reason)
    if not must_exist and path.exists():
        raise Qa02Error(reason)
    if not path.parent.is_dir():
        raise Qa02Error(reason)
    return path


def load_matrix(path: str | Path) -> dict[str, Any]:
    matrix = _load(Path(path))
    if matrix.get("task_id") != "QA-02" or matrix.get("matrix_version") != "qa-02-v1":
        raise Qa02Error("QA02_MATRIX_VERSION_MISMATCH")
    cases = matrix.get("cases")
    if not isinstance(cases, list) or not cases:
        raise Qa02Error("QA02_CASES_REQUIRED")
    seen: set[str] = set()
    operations: set[str] = set()
    for case in cases:
        if not isinstance(case, dict):
            raise Qa02Error("QA02_CASE_INVALID")
        case_id, operation_id = case.get("case_id"), case.get("operation_id")
        category = case.get("category")
        expected = case.get("expected")
        if (not isinstance(case_id, str) or not SAFE_ID.fullmatch(case_id) or case_id in seen
                or not isinstance(operation_id, str) or not SAFE_ID.fullmatch(operation_id)
                or operation_id in operations or category not in SUPPORTED_CASE_ASSERTIONS
                or not isinstance(expected, dict)
                or not required_assertions(case) <= set(expected)):
            raise Qa02Error("QA02_CASE_INVALID")
        seen.add(case_id)
        operations.add(operation_id)
    input_pins = matrix.get("input_pins")
    if not isinstance(input_pins, dict) or not isinstance(input_pins.get("namespace"), str) \
            or not input_pins["namespace"] or not isinstance(matrix.get("dependency_gate"), dict):
        raise Qa02Error("QA02_MATRIX_PINS_REQUIRED")
    return matrix


def prepare_report(matrix_path: str | Path) -> dict[str, Any]:
    matrix = load_matrix(matrix_path)
    matrix_sha256 = _sha256(Path(matrix_path))
    cases = [{"case_id": case["case_id"], "operation_id": case["operation_id"],
              "category": case["category"], "status": "PENDING_RUNTIME",
              "expected": case["expected"]} for case in matrix["cases"]]
    return {
        "task_id": "QA-02", "report_version": RUNTIME_REPORT_VERSION, "status": "PREPARED",
        "runtime_complete": False, "synthetic_fixture": True,
        "matrix_sha256": matrix_sha256,
        "dependency_gate": matrix["dependency_gate"], "input_pins": matrix["input_pins"],
        "cases": cases,
        "summary": {"case_count": len(cases), "pending_runtime": len(cases), "passed": 0, "failed": 0},
        "next_gate": "Run through ORC-07 real adapters in a QA namespace, then attach before/after reconciliation.",
    }


def write_preparation(matrix_path: str | Path, json_path: str | Path, markdown_path: str | Path) -> dict[str, Any]:
    report = prepare_report(matrix_path)
    json_target, markdown_target = Path(json_path), Path(markdown_path)
    json_target.parent.mkdir(parents=True, exist_ok=True)
    markdown_target.parent.mkdir(parents=True, exist_ok=True)
    json_target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = ["# QA-02 - Rerun, duplicate and late revision", "", f"Status: **{report['status']}**", "",
             f"Matrix SHA-256: `{report['matrix_sha256']}`", "",
             "Runtime acceptance is pending ORC-07 real adapters. Synthetic cases are a test plan, not a pass.", "",
             "| Case | Category | Operation | Status |", "|---|---|---|---|"]
    lines.extend(f"| {case['case_id']} | {case['category']} | `{case['operation_id']}` | {case['status']} |" for case in report["cases"])
    lines.extend(["", "## Reconciliation to record", "", "- logical keys and current/history rows", "- canonical count and source overlap", "- partition contents and exact snapshot IDs", "- KPI before/after and retained old snapshots", ""])
    markdown_target.write_text("\n".join(lines), encoding="utf-8")
    return report


def _validate_runtime_request(request: Mapping[str, Any]) -> tuple[dict[str, Any], Path, Path]:
    if request.get("task_id") != "QA-02" or request.get("request_version") != RUNTIME_REQUEST_VERSION:
        raise Qa02Error("QA02_RUNTIME_REQUEST_INVALID")
    for field in ("run_id", "namespace", "operation_id"):
        if not isinstance(request.get(field), str) or not SAFE_ID.fullmatch(request[field]):
            raise Qa02Error("QA02_RUNTIME_IDENTITY_INVALID")
    matrix_path = _safe_path(request.get("matrix_path"), "QA02_MATRIX_REFERENCE_INVALID", must_exist=True)
    report_path = _safe_path(request.get("report_path"), "QA02_REPORT_REFERENCE_INVALID", must_exist=False)
    matrix_sha256 = request.get("matrix_sha256")
    if not isinstance(matrix_sha256, str) or not SAFE_SHA256.fullmatch(matrix_sha256):
        raise Qa02Error("QA02_MATRIX_CHECKSUM_REQUIRED")
    if _sha256(matrix_path) != matrix_sha256:
        raise Qa02Error("QA02_MATRIX_CHECKSUM_MISMATCH")
    matrix = load_matrix(matrix_path)
    if matrix["input_pins"].get("namespace") != request["namespace"]:
        raise Qa02Error("QA02_RUNTIME_NAMESPACE_MISMATCH")
    return matrix, matrix_path, report_path


def _non_empty(value: Any) -> bool:
    return value is not None and not (isinstance(value, (str, list, dict)) and not value)


def _count(value: Any) -> int:
    if isinstance(value, bool):
        raise Qa02Error("QA02_RECONCILIATION_VALUE_INVALID")
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, dict) and isinstance(value.get("count"), int) \
            and not isinstance(value["count"], bool) and value["count"] >= 0:
        return value["count"]
    raise Qa02Error("QA02_RECONCILIATION_VALUE_INVALID")


def _keys(value: Any) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value) \
            or len(set(value)) != len(value):
        raise Qa02Error("QA02_RECONCILIATION_VALUE_INVALID")
    return value


def _snapshot_refs(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise Qa02Error("QA02_RUNTIME_SNAPSHOTS_REQUIRED")
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in value:
        if not isinstance(item, dict) or not SAFE_TABLE.fullmatch(str(item.get("table", ""))):
            raise Qa02Error("QA02_RUNTIME_SNAPSHOTS_REQUIRED")
        snapshot_id = item.get("snapshot_id")
        if isinstance(snapshot_id, int) and not isinstance(snapshot_id, bool) and snapshot_id > 0:
            snapshot_text = str(snapshot_id)
        elif isinstance(snapshot_id, str) and SAFE_SNAPSHOT.fullmatch(snapshot_id):
            snapshot_text = snapshot_id
        else:
            raise Qa02Error("QA02_RUNTIME_SNAPSHOTS_REQUIRED")
        key = (item["table"], snapshot_text)
        if key in seen:
            raise Qa02Error("QA02_RUNTIME_SNAPSHOTS_REQUIRED")
        seen.add(key)
        result.append({"table": item["table"], "snapshot_id": snapshot_id})
    return result


def _canonical(value: Any) -> str:
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    except (TypeError, ValueError) as error:
        raise Qa02Error("QA02_RECONCILIATION_VALUE_INVALID") from error


def _assert_runtime_assertion(value: Any, expected: str, assertion: str) -> None:
    if not isinstance(value, dict) or value.get("status") != "PASS" \
            or value.get("check") != expected or "before" not in value or "after" not in value:
        raise Qa02Error("QA02_RECONCILIATION_INCOMPLETE")
    before, after = value["before"], value["after"]
    if not _non_empty(before) or not _non_empty(after):
        raise Qa02Error("QA02_RECONCILIATION_VALUE_INVALID")
    details = value.get("details")
    if not isinstance(details, dict):
        raise Qa02Error("QA02_RECONCILIATION_DETAILS_REQUIRED")

    if expected == "same_key_new_revision":
        if assertion != "logical_keys" or _keys(before) != _keys(after) \
                or details.get("revision_changed") is not True:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return

    if expected in {"unchanged", "same_or_equivalent", "no_double_count", "outside_scope_is_not_modified"}:
        if assertion == "canonical_count":
            if _count(before) != _count(after):
                raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        elif assertion == "logical_keys":
            if _keys(before) != _keys(after):
                raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        elif _canonical(before) != _canonical(after):
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        if expected == "no_double_count" and details.get("duplicate_count") != 0:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        if expected == "outside_scope_is_not_modified" and details.get("scope") != "outside":
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        if expected == "same_or_equivalent" and details.get("equivalent") is not True:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return

    if expected == "one_current_row":
        if assertion == "logical_keys":
            if len(_keys(after)) != 1:
                raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        elif _count(after) != 1:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "new_revision":
        if not isinstance(before, dict) or not isinstance(after, dict) \
                or not isinstance(before.get("revision_id"), str) or not isinstance(after.get("revision_id"), str) \
                or before["revision_id"] == after["revision_id"] or after.get("is_current") is not True:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "old_revision_retained":
        if not isinstance(after, dict) or after.get("retained") is not True \
                or _count(after.get("history_count")) < 1:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "new_catalog_release":
        if not isinstance(before, dict) or not isinstance(after, dict) \
                or not isinstance(before.get("release_id"), str) or not isinstance(after.get("release_id"), str) \
                or before["release_id"] == after["release_id"]:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "replacement":
        if not isinstance(after, dict) or after.get("replacement") is not True \
                or after.get("is_current") is not True:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "auditable":
        if not isinstance(after, dict) or after.get("auditable") is not True \
                or not isinstance(after.get("release_id"), str):
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "both_sources_retained":
        sources = after.get("sources") if isinstance(after, dict) else None
        if not isinstance(sources, list) or not {"USGS", "JMA"}.issubset(sources):
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected in {"current", "superseded", "withdrawn_or_superseded"}:
        if not isinstance(after, dict) or after.get("state") not in {
                "current", "superseded", "withdrawn", "withdrawn_or_superseded"}:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        if expected == "current" and after.get("state") != "current":
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        if expected == "superseded" and after.get("state") != "superseded":
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        if expected == "withdrawn_or_superseded" \
                and after.get("state") not in {"withdrawn", "withdrawn_or_superseded", "superseded"}:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "retained":
        if not isinstance(after, dict) or after.get("retained") is not True:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "reconciled":
        if not isinstance(after, dict) or after.get("balanced") is not True:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    if expected == "old_snapshot_readable":
        if not isinstance(before, dict) or not isinstance(after, dict) \
                or before.get("snapshot_id") != after.get("snapshot_id") or after.get("readable") is not True:
            raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_FAILED")
        return
    raise Qa02Error("QA02_RECONCILIATION_EXPECTATION_UNSUPPORTED")


def _validate_evidence_ref(value: Any, request: Mapping[str, Any], case: Mapping[str, Any],
                           report_path: Path) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("path"), str) \
            or not SAFE_SHA256.fullmatch(str(value.get("sha256", ""))):
        raise Qa02Error("QA02_RUNTIME_EVIDENCE_REQUIRED")
    try:
        raw_path = Path(value["path"])
    except (TypeError, ValueError) as error:
        raise Qa02Error("QA02_RUNTIME_EVIDENCE_REQUIRED") from error
    if ".." in raw_path.parts:
        raise Qa02Error("QA02_RUNTIME_EVIDENCE_REQUIRED")
    try:
        evidence_path = raw_path.resolve()
        report_root = report_path.parent.resolve()
    except (OSError, ValueError) as error:
        raise Qa02Error("QA02_RUNTIME_EVIDENCE_REQUIRED") from error
    if report_root not in evidence_path.parents or evidence_path.is_symlink() \
            or not evidence_path.is_file():
        raise Qa02Error("QA02_RUNTIME_EVIDENCE_REQUIRED")
    if value.get("run_id") != request["run_id"] or value.get("namespace") != request["namespace"] \
            or value.get("case_id") != case["case_id"] or value.get("operation_id") != case["operation_id"]:
        raise Qa02Error("QA02_RUNTIME_EVIDENCE_IDENTITY_MISMATCH")
    if _evidence_sha256(evidence_path) != value["sha256"]:
        raise Qa02Error("QA02_RUNTIME_EVIDENCE_CHECKSUM_MISMATCH")


def _validate_runtime_report(report: Mapping[str, Any], request: Mapping[str, Any],
                             matrix: Mapping[str, Any], report_path: Path) -> dict[str, Any]:
    if (report.get("task_id") != "QA-02" or report.get("report_version") != RUNTIME_REPORT_VERSION
            or report.get("runtime_complete") is not True or report.get("status") != "PASS"):
        raise Qa02Error("QA02_RUNTIME_REPORT_INCOMPLETE")
    for field in ("run_id", "namespace", "operation_id"):
        if report.get(field) != request.get(field):
            raise Qa02Error("QA02_RUNTIME_REPORT_IDENTITY_MISMATCH")
    if report.get("matrix_sha256") != request["matrix_sha256"]:
        raise Qa02Error("QA02_RUNTIME_REPORT_MATRIX_MISMATCH")
    report_pins = report.get("input_pins")
    if not isinstance(report_pins, dict) or report_pins.get("matrix_sha256") != request["matrix_sha256"] \
            or report_pins.get("run_id") != request["run_id"] or report_pins.get("namespace") != request["namespace"]:
        raise Qa02Error("QA02_RUNTIME_INPUT_PINS_REQUIRED")
    expected = {case["case_id"]: case for case in matrix["cases"]}
    cases = report.get("cases")
    if not isinstance(cases, list) or len(cases) != len(expected) \
            or any(not isinstance(case, dict) or not isinstance(case.get("case_id"), str) for case in cases) \
            or {case["case_id"] for case in cases} != set(expected):
        raise Qa02Error("QA02_RUNTIME_CASE_SET_MISMATCH")
    seen_operations: set[str] = set()
    for case in cases:
        if not isinstance(case, dict):
            raise Qa02Error("QA02_RUNTIME_CASE_INVALID")
        case_id = case.get("case_id")
        source = expected[case_id]
        if (case.get("operation_id") != source["operation_id"] or case.get("category") != source["category"]
                or case.get("status") != "PASS" or case["operation_id"] in seen_operations):
            raise Qa02Error("QA02_RUNTIME_CASE_INVALID")
        seen_operations.add(case["operation_id"])
        pins = case.get("input_pins")
        if not isinstance(pins, dict) or pins.get("matrix_sha256") != request["matrix_sha256"] \
                or pins.get("run_id") != request["run_id"] or pins.get("namespace") != request["namespace"]:
            raise Qa02Error("QA02_RUNTIME_CASE_PINS_REQUIRED")
        snapshots = case.get("snapshots")
        if not isinstance(snapshots, dict):
            raise Qa02Error("QA02_RUNTIME_SNAPSHOTS_REQUIRED")
        _snapshot_refs(snapshots.get("before"))
        _snapshot_refs(snapshots.get("after"))
        _validate_evidence_ref(case.get("evidence_ref"), request, case, report_path)
        reconciliation = case.get("reconciliation")
        if not isinstance(reconciliation, dict):
            raise Qa02Error("QA02_RECONCILIATION_INCOMPLETE")
        for assertion in required_assertions(source):
            _assert_runtime_assertion(reconciliation.get(assertion), source["expected"][assertion], assertion)
    summary = report.get("summary")
    if not isinstance(summary, dict) or summary.get("case_count") != len(cases) \
            or summary.get("passed") != len(cases) or summary.get("failed") != 0 \
            or summary.get("pending_runtime") != 0:
        raise Qa02Error("QA02_RUNTIME_SUMMARY_MISMATCH")
    return dict(report)


def run_live(request_path: str | Path, environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    request = _load(Path(request_path), 64 * 1024)
    matrix, _, report_path = _validate_runtime_request(request)
    command_value = env.get("QA_02_RUNTIME_COMMAND", "")
    command_text = command_value.strip() if isinstance(command_value, str) else ""
    if not command_text:
        raise Qa02Error("QA02_RUNTIME_COMMAND_REQUIRED")
    try:
        command = shlex.split(command_text)
    except ValueError as error:
        raise Qa02Error("QA02_RUNTIME_COMMAND_INVALID") from error
    if not command:
        raise Qa02Error("QA02_RUNTIME_COMMAND_REQUIRED")
    try:
        timeout = int(env.get("QA_02_RUNTIME_TIMEOUT_SECONDS", "3600"))
    except (TypeError, ValueError) as error:
        raise Qa02Error("QA02_RUNTIME_TIMEOUT_INVALID") from error
    if not 1 <= timeout <= 3600:
        raise Qa02Error("QA02_RUNTIME_TIMEOUT_INVALID")
    try:
        completed = subprocess.run(command + [str(Path(request_path))], env=env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                   timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        raise Qa02Error("QA02_RUNTIME_TIMEOUT") from error
    if completed.returncode != 0:
        raise Qa02Error("QA02_RUNTIME_FAILED")
    return _validate_runtime_report(_load(report_path, 4 * 1024 * 1024), request, matrix, report_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix", type=Path)
    parser.add_argument("--json", type=Path)
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--live", type=Path, dest="live_request")
    args = parser.parse_args()
    if args.live_request is not None:
        print(json.dumps(run_live(args.live_request), sort_keys=True))
        return
    if not args.matrix or not args.json or not args.markdown:
        parser.error("--matrix, --json and --markdown are required for preparation")
    print(json.dumps(write_preparation(args.matrix, args.json, args.markdown), sort_keys=True))


if __name__ == "__main__":
    main()
