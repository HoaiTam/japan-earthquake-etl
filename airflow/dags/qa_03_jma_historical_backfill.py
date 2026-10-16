"""Prepare QA-03 evidence and fail closed on incomplete runtime receipts."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import subprocess
from typing import Any, Mapping
from urllib.parse import urlsplit


class Qa03Error(RuntimeError):
    """Safe QA-03 reason code; never includes request values or secrets."""


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MATRIX = ROOT / "airflow/dags/fixtures/qa_03_jma_historical_matrix.json"
DEFAULT_INVENTORY = ROOT / "config/jma/hypocenter_archives_v1.csv"
MATRIX_VERSION = "qa-03-v2"
REQUEST_VERSION = "qa-03-runtime-v1"
REPORT_VERSION = "qa-03-report-v2"
HALO_SECONDS = 16
MAX_MATRIX_BYTES = 256 * 1024
MAX_REQUEST_BYTES = 64 * 1024
MAX_REPORT_BYTES = 4 * 1024 * 1024
MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
MAX_INVENTORY_BYTES = 2 * 1024 * 1024
SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
SAFE_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
SAFE_SNAPSHOT = re.compile(r"[1-9][0-9]{0,18}\Z")
REQUIRED_CASES = {
    "resume_after_failure": {"same_verified_archive", "duplicate_count_delta", "readback"},
    "reuse_verified_archive": {"archive_sha256_before", "archive_sha256_after", "download_count", "verification_retained"},
    "changed_release": {"old_catalog_release", "new_catalog_release", "old_snapshot_readable", "old_snapshot_id", "new_snapshot_id"},
    "same_release_different_bytes": {"rejected", "downstream_blocked", "data_unchanged", "old_sha256", "new_sha256"},
    "retention_outside_scope": {"outside_scope_before", "outside_scope_after", "snapshots"},
    "resource_guard": {"batch_months", "guard_triggered", "recovery_passed", "scope_months"},
}
EXPECTED_SCOPE = {
    (1997, "jan-sep", "1997-09"),
    (1997, "oct-dec", "1997-10"),
    (2000, "full-year", "2000-01"),
    (2023, "full-year", "2023-09"),
}


def _fail(reason: str) -> None:
    raise Qa03Error(reason)


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path, reason: str) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(262144), b""):
                digest.update(block)
    except OSError as error:
        raise Qa03Error(reason) from error
    return digest.hexdigest()


def _check_path_components(path: Path) -> bool:
    current = path
    while True:
        if current.is_symlink():
            return False
        parent = current.parent
        if parent == current:
            return True
        current = parent


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            _fail("QA03_JSON_DUPLICATE_KEY")
        result[key] = value
    return result


def _load(path: Path, limit: int = MAX_MATRIX_BYTES, kind: str = "MATRIX") -> dict[str, Any]:
    if not _check_path_components(path) or not path.is_file() or path.stat().st_size > limit:
        _fail(f"QA03_{kind}_NOT_BOUNDED")
    try:
        value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
    except (OSError, ValueError) as error:
        raise Qa03Error(f"QA03_{kind}_INVALID_JSON") from error
    if not isinstance(value, dict):
        _fail(f"QA03_{kind}_OBJECT_REQUIRED")
    return value


def _inventory_rows(path: Path = DEFAULT_INVENTORY) -> tuple[list[dict[str, str]], str]:
    if not _check_path_components(path) or not path.is_file():
        _fail("QA03_INVENTORY_UNAVAILABLE")
    try:
        if path.stat().st_size > MAX_INVENTORY_BYTES:
            _fail("QA03_INVENTORY_NOT_BOUNDED")
        raw = path.read_bytes()
        rows = list(csv.DictReader(raw.decode("utf-8-sig").splitlines()))
    except (OSError, UnicodeError, csv.Error) as error:
        raise Qa03Error("QA03_INVENTORY_INVALID") from error
    required = {"year", "segment", "native_start_jst", "native_end_jst", "archive_name", "member_name"}
    if not rows or not required <= set(rows[0]):
        _fail("QA03_INVENTORY_SCHEMA_INVALID")
    return rows, _sha256_bytes(raw)


def _inventory_index(rows: list[dict[str, str]]) -> dict[tuple[int, str], dict[str, str]]:
    index: dict[tuple[int, str], dict[str, str]] = {}
    try:
        for row in rows:
            key = (int(row["year"]), row["segment"])
            if key in index:
                _fail("QA03_INVENTORY_DUPLICATE_SLOT")
            start = datetime.fromisoformat(row["native_start_jst"])
            end = datetime.fromisoformat(row["native_end_jst"])
            if start.utcoffset() != timedelta(hours=9) or end.utcoffset() != timedelta(hours=9) or start >= end:
                _fail("QA03_INVENTORY_BOUNDS_INVALID")
            index[key] = row
    except (KeyError, TypeError, ValueError) as error:
        raise Qa03Error("QA03_INVENTORY_INVALID") from error
    return index


def _month_range(month: str) -> tuple[datetime, datetime]:
    if not isinstance(month, str) or not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", month):
        _fail("QA03_MONTH_INVALID")
    year, number = (int(part) for part in month.split("-"))
    if not 1984 <= year <= 2023:
        _fail("QA03_MONTH_INVALID")
    next_year, next_month = (year + 1, 1) if number == 12 else (year, number + 1)
    start = datetime(year, number, 1, tzinfo=timezone.utc)
    end = datetime(next_year, next_month, 1, tzinfo=timezone.utc)
    return start, end


def _iso(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def _scope_and_coverage(matrix: Mapping[str, Any], rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    index = _inventory_index(rows)
    scope = matrix.get("scope")
    if not isinstance(scope, list) or len(scope) != len(EXPECTED_SCOPE):
        _fail("QA03_REPRESENTATIVE_SCOPE_REQUIRED")
    seen: set[tuple[int, str, str]] = set()
    selected: list[dict[str, Any]] = []
    coverage: list[dict[str, Any]] = []
    for item in scope:
        if not isinstance(item, dict):
            _fail("QA03_SCOPE_ENTRY_INVALID")
        year, segment, months = item.get("year"), item.get("segment"), item.get("months")
        if isinstance(year, bool) or not isinstance(year, int) or not isinstance(segment, str) or not isinstance(months, list):
            _fail("QA03_SCOPE_ENTRY_INVALID")
        if not months or any(not isinstance(month, str) for month in months) or len(months) != len(set(months)):
            _fail("QA03_SCOPE_MONTHS_INVALID")
        slot = (year, segment)
        row = index.get(slot)
        if row is None:
            _fail("QA03_SCOPE_ARCHIVE_NOT_IN_INVENTORY")
        if item.get("archive_identity") != row["archive_name"]:
            _fail("QA03_SCOPE_ARCHIVE_IDENTITY_MISMATCH")
        for month in months:
            start, end = _month_range(month)
            if int(month[:4]) != year:
                _fail("QA03_SCOPE_YEAR_MONTH_MISMATCH")
            allowed = EXPECTED_SCOPE
            identity = (year, segment, month)
            if identity not in allowed or identity in seen:
                _fail("QA03_SCOPE_SEGMENT_MONTH_MISMATCH")
            seen.add(identity)
            halo_start, halo_end = start - timedelta(seconds=HALO_SECONDS), end + timedelta(seconds=HALO_SECONDS)
            required_archives = []
            for inventory_row in rows:
                native_start = datetime.fromisoformat(inventory_row["native_start_jst"]).astimezone(timezone.utc)
                native_end = datetime.fromisoformat(inventory_row["native_end_jst"]).astimezone(timezone.utc)
                if native_start < halo_end and halo_start < native_end:
                    required_archives.append(inventory_row["archive_name"])
            required_archives.sort()
            declared = item.get("support_archives")
            if not isinstance(declared, list) or any(not isinstance(value, str) for value in declared):
                _fail("QA03_SUPPORT_ARCHIVES_REQUIRED")
            if sorted(declared) != required_archives:
                _fail("QA03_ARCHIVE_HALO_COVERAGE_MISMATCH")
            selected.append({"year": year, "segment": segment, "month": month,
                             "archive_identity": row["archive_name"], "member_name": row["member_name"]})
            coverage.append({"month": month, "utc_start": _iso(start), "utc_end": _iso(end),
                             "halo_start_utc": _iso(halo_start), "halo_end_utc": _iso(halo_end),
                             "required_archives": required_archives})
    if seen != EXPECTED_SCOPE:
        _fail("QA03_TARGET_MONTHS_REQUIRED")
    return selected, coverage


def load_matrix(path: str | Path, inventory_path: str | Path = DEFAULT_INVENTORY) -> dict[str, Any]:
    matrix_path, inventory_file = Path(path), Path(inventory_path)
    matrix = _load(matrix_path)
    if matrix.get("task_id") != "QA-03" or matrix.get("matrix_version") != MATRIX_VERSION:
        _fail("QA03_MATRIX_VERSION_MISMATCH")
    rows, inventory_sha256 = _inventory_rows(inventory_file)
    selected, coverage = _scope_and_coverage(matrix, rows)
    declared_hash = matrix.get("inventory_sha256")
    if declared_hash != inventory_sha256:
        _fail("QA03_INVENTORY_CHECKSUM_MISMATCH")
    if matrix.get("halo_seconds") != HALO_SECONDS:
        _fail("QA03_HALO_CONTRACT_MISMATCH")
    pins = matrix.get("input_pins")
    if not isinstance(pins, dict) or not isinstance(pins.get("namespace"), str) or not SAFE_ID.fullmatch(pins["namespace"]):
        _fail("QA03_MATRIX_PINS_REQUIRED")
    if not isinstance(matrix.get("dependency_gate"), dict):
        _fail("QA03_DEPENDENCY_GATE_REQUIRED")
    cases = matrix.get("cases")
    if not isinstance(cases, list) or len(cases) != len(REQUIRED_CASES):
        _fail("QA03_CASES_REQUIRED")
    seen_ids: set[str] = set()
    seen_operations: set[str] = set()
    for case in cases:
        if not isinstance(case, dict):
            _fail("QA03_CASE_INVALID")
        case_id, operation_id, expected = case.get("case_id"), case.get("operation_id"), case.get("expected")
        required = REQUIRED_CASES.get(case_id) if isinstance(case_id, str) else None
        if (required is None or case_id in seen_ids or not isinstance(operation_id, str)
                or not SAFE_ID.fullmatch(operation_id) or operation_id in seen_operations
                or not isinstance(expected, dict) or not required <= set(expected)):
            _fail("QA03_CASE_INVALID")
        seen_ids.add(case_id)
        seen_operations.add(operation_id)
    if seen_ids != set(REQUIRED_CASES):
        _fail("QA03_CASES_REQUIRED")
    matrix = dict(matrix)
    matrix["_matrix_sha256"] = _sha256_file(matrix_path, "QA03_MATRIX_READ_FAILED")
    matrix["_inventory_rows"] = rows
    matrix["_selected_scope"] = selected
    matrix["_computed_coverage"] = coverage
    return matrix


def prepare_report(matrix_path: str | Path) -> dict[str, Any]:
    matrix = load_matrix(matrix_path)
    cases = [{"case_id": case["case_id"], "operation_id": case["operation_id"],
              "status": "PENDING_RUNTIME", "expected": case["expected"]} for case in matrix["cases"]]
    return {
        "task_id": "QA-03", "report_version": REPORT_VERSION, "status": "PREPARED",
        "runtime_complete": False, "synthetic_revision_fixture": True,
        "matrix_sha256": matrix["_matrix_sha256"], "inventory_sha256": matrix["inventory_sha256"],
        "dependency_gate": matrix["dependency_gate"], "input_pins": matrix["input_pins"],
        "scope": matrix["_selected_scope"], "coverage": matrix["_computed_coverage"],
        "cases": cases,
        "capacity": {"duration_seconds": None, "peak_memory_mb": None, "download_count": None,
                     "download_reused": None, "verified_archive_reused": None, "resource_failures": None},
        "snapshots": {"bootstrap": True, "refs": []},
        "summary": {"scope_count": len(matrix["_selected_scope"]), "case_count": len(cases),
                    "pending_runtime": len(cases), "passed": 0, "failed": 0},
        "next_gate": "Run closed-month batches through ORC-07 real adapters and attach Bronze-to-Published counts/snapshots.",
    }


def write_preparation(matrix_path: str | Path, json_path: str | Path, markdown_path: str | Path) -> dict[str, Any]:
    report = prepare_report(matrix_path)
    json_target, markdown_target = Path(json_path), Path(markdown_path)
    json_target.parent.mkdir(parents=True, exist_ok=True)
    markdown_target.parent.mkdir(parents=True, exist_ok=True)
    json_target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    lines = ["# QA-03 - JMA historical backfill", "", f"Status: **{report['status']}**", "",
             f"Matrix SHA-256: `{report['matrix_sha256']}`", f"Inventory SHA-256: `{report['inventory_sha256']}`", "",
             "Selected months are representative coverage; they do not prove the full historical period.", "",
             "| Year | Segment | Month | Archive identity | Required coverage archives |",
             "|---:|---|---|---|---|"]
    for row, coverage in zip(report["scope"], report["coverage"], strict=True):
        lines.append(f"| {row['year']} | {row['segment']} | {row['month']} | `{row['archive_identity']}` | {', '.join(coverage['required_archives'])} |")
    lines.extend(["", "## Runtime cases", "", "| Case | Status |", "|---|---|"])
    lines.extend(f"| {case['case_id']} | {case['status']} |" for case in report["cases"])
    lines.extend(["", "Capacity fields are null until measured by the runtime adapter.",
                  "ORC-07 real adapters are required before a Bronze-to-Published PASS.", ""])
    markdown_target.write_text("\n".join(lines), encoding="utf-8")
    return report


def _safe_path(value: Any, reason: str, *, must_exist: bool) -> Path:
    if not isinstance(value, str) or not value:
        _fail(reason)
    path = Path(value)
    if ".." in path.parts or not _check_path_components(path):
        _fail(reason)
    if must_exist and (not path.is_file() or path.is_symlink()):
        _fail(reason)
    if not must_exist and (path.exists() or not path.parent.is_dir()):
        _fail(reason)
    return path


def _utc_instant(value: Any) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        _fail("QA03_RUNTIME_CUTOFF_INVALID")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise Qa03Error("QA03_RUNTIME_CUTOFF_INVALID") from error
    if parsed.utcoffset() != timedelta(0):
        _fail("QA03_RUNTIME_CUTOFF_INVALID")
    return parsed


def _validate_request(request_path: Path) -> tuple[dict[str, Any], dict[str, Any], Path, Path, str]:
    request = _load(request_path, MAX_REQUEST_BYTES, "REQUEST")
    if request.get("task_id") != "QA-03" or request.get("request_version") != REQUEST_VERSION:
        _fail("QA03_RUNTIME_REQUEST_INVALID")
    for field in ("run_id", "operation_id", "namespace"):
        if not isinstance(request.get(field), str) or not SAFE_ID.fullmatch(request[field]):
            _fail("QA03_RUNTIME_IDENTITY_INVALID")
    matrix_path = _safe_path(request.get("matrix_path"), "QA03_MATRIX_REFERENCE_INVALID", must_exist=True)
    inventory_path = _safe_path(request.get("inventory_path"), "QA03_INVENTORY_REFERENCE_INVALID", must_exist=True)
    report_path = _safe_path(request.get("report_path"), "QA03_REPORT_REFERENCE_INVALID", must_exist=False)
    evidence_dir = report_path.parent / (report_path.name + ".evidence")
    if evidence_dir.exists() or not _check_path_components(evidence_dir):
        _fail("QA03_REPORT_REFERENCE_INVALID")
    for field in ("matrix_sha256", "inventory_sha256"):
        if not isinstance(request.get(field), str) or not SAFE_SHA256.fullmatch(request[field]):
            _fail("QA03_RUNTIME_CHECKSUM_REQUIRED")
    if _sha256_file(matrix_path, "QA03_MATRIX_READ_FAILED") != request["matrix_sha256"]:
        _fail("QA03_MATRIX_CHECKSUM_MISMATCH")
    rows, inventory_sha = _inventory_rows(inventory_path)
    if inventory_sha != request["inventory_sha256"]:
        _fail("QA03_INVENTORY_CHECKSUM_MISMATCH")
    matrix = load_matrix(matrix_path, inventory_path)
    if matrix["input_pins"].get("namespace") != request["namespace"]:
        _fail("QA03_RUNTIME_NAMESPACE_MISMATCH")
    if matrix["_matrix_sha256"] != request["matrix_sha256"] or matrix["inventory_sha256"] != request["inventory_sha256"]:
        _fail("QA03_RUNTIME_PIN_MISMATCH")
    _utc_instant(request.get("source_cutoff_utc"))
    refs = request.get("input_refs")
    if not isinstance(refs, list) or not refs:
        _fail("QA03_RUNTIME_INPUT_REFS_REQUIRED")
    expected_archives = {name for entry in matrix["_computed_coverage"] for name in entry["required_archives"]}
    refs_by_archive: dict[str, dict[str, Any]] = {}
    for ref in refs:
        if not isinstance(ref, dict):
            _fail("QA03_RUNTIME_INPUT_REF_INVALID")
        archive_name, uri, checksum = ref.get("archive_name"), ref.get("uri"), ref.get("sha256")
        try:
            parsed = urlsplit(uri) if isinstance(uri, str) else None
        except ValueError:
            parsed = None
        if (not isinstance(archive_name, str) or archive_name not in expected_archives or archive_name in refs_by_archive
                or parsed is None or parsed.scheme not in {"s3", "file"} or parsed.username or parsed.password
                or parsed.query or parsed.fragment or (parsed.scheme == "s3" and not parsed.netloc)
                or not parsed.path or not isinstance(checksum, str) or not SAFE_SHA256.fullmatch(checksum)
                or not SAFE_SHA256.fullmatch(str(ref.get("raw_sha256", "")))
                or not isinstance(ref.get("catalog_release"), str) or not ref["catalog_release"]):
            _fail("QA03_RUNTIME_INPUT_REF_INVALID")
        refs_by_archive[archive_name] = ref
    if set(refs_by_archive) != expected_archives:
        _fail("QA03_RUNTIME_INPUT_REF_COVERAGE_MISMATCH")
    request_sha = _sha256_file(request_path, "QA03_REQUEST_READ_FAILED")
    return request, matrix, report_path, request_path, request_sha


def _finite_number(value: Any, *, positive: bool = False) -> bool:
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and (value > 0 if positive else value >= 0))


def _validate_snapshots(value: Any, *, allow_bootstrap: bool = False) -> None:
    if not isinstance(value, dict) or not isinstance(value.get("refs"), list) or not isinstance(value.get("bootstrap"), bool):
        _fail("QA03_RUNTIME_SNAPSHOTS_INVALID")
    if value.get("bootstrap") is True:
        if not allow_bootstrap or value["refs"]:
            _fail("QA03_RUNTIME_SNAPSHOTS_INVALID")
        return
    refs = value["refs"]
    if not refs:
        _fail("QA03_RUNTIME_SNAPSHOTS_REQUIRED")
    seen: set[tuple[str, str]] = set()
    for ref in refs:
        if not isinstance(ref, dict) or not isinstance(ref.get("table"), str) or not SAFE_ID.fullmatch(ref["table"]):
            _fail("QA03_RUNTIME_SNAPSHOTS_INVALID")
        snapshot_id = ref.get("snapshot_id")
        if isinstance(snapshot_id, int) and not isinstance(snapshot_id, bool) and 0 < snapshot_id <= 2**63 - 1:
            snapshot_text = str(snapshot_id)
        elif isinstance(snapshot_id, str) and SAFE_SNAPSHOT.fullmatch(snapshot_id) and int(snapshot_id) <= 2**63 - 1:
            snapshot_text = snapshot_id
        else:
            _fail("QA03_RUNTIME_SNAPSHOTS_INVALID")
        identity = (ref["table"], snapshot_text)
        if identity in seen:
            _fail("QA03_RUNTIME_SNAPSHOTS_INVALID")
        seen.add(identity)


def _validate_assertions(case_id: str, assertions: Mapping[str, Any], selected_months: set[str]) -> None:
    required = REQUIRED_CASES[case_id]
    if not required <= set(assertions):
        _fail("QA03_RUNTIME_ASSERTIONS_INCOMPLETE")
    if case_id == "resume_after_failure":
        duplicate_delta = assertions["duplicate_count_delta"]
        if (assertions["same_verified_archive"] is not True or isinstance(duplicate_delta, bool)
                or not isinstance(duplicate_delta, int) or duplicate_delta != 0 or assertions["readback"] is not True):
            _fail("QA03_RUNTIME_ASSERTION_FAILED")
    elif case_id == "reuse_verified_archive":
        downloads = assertions["download_count"]
        if (assertions["archive_sha256_before"] != assertions["archive_sha256_after"]
                or not SAFE_SHA256.fullmatch(str(assertions["archive_sha256_before"]))
                or isinstance(downloads, bool) or not isinstance(downloads, int) or downloads != 0
                or assertions["verification_retained"] is not True):
            _fail("QA03_RUNTIME_ASSERTION_FAILED")
    elif case_id == "changed_release":
        if (not all(isinstance(assertions[key], str) and assertions[key] for key in ("old_catalog_release", "new_catalog_release"))
                or assertions["old_catalog_release"] == assertions["new_catalog_release"]
                or assertions["old_snapshot_readable"] is not True):
            _fail("QA03_RUNTIME_ASSERTION_FAILED")
        for key in ("old_snapshot_id", "new_snapshot_id"):
            value = assertions[key]
            if (isinstance(value, bool) or not isinstance(value, (int, str))
                    or not SAFE_SNAPSHOT.fullmatch(str(value)) or int(value) > 2**63 - 1):
                _fail("QA03_RUNTIME_ASSERTION_FAILED")
    elif case_id == "same_release_different_bytes":
        if not all(assertions[key] is True for key in ("rejected", "downstream_blocked", "data_unchanged")):
            _fail("QA03_RUNTIME_ASSERTION_FAILED")
        if (not SAFE_SHA256.fullmatch(str(assertions["old_sha256"])) or not SAFE_SHA256.fullmatch(str(assertions["new_sha256"]))
                or assertions["old_sha256"] == assertions["new_sha256"]):
            _fail("QA03_RUNTIME_ASSERTION_FAILED")
    elif case_id == "retention_outside_scope":
        before, after = assertions["outside_scope_before"], assertions["outside_scope_after"]
        for value in (before, after):
            if (not isinstance(value, dict) or not isinstance(value.get("partition"), str)
                    or isinstance(value.get("count"), bool) or not isinstance(value.get("count"), int)
                    or value["count"] < 0 or not SAFE_SHA256.fullmatch(str(value.get("content_sha256", "")))):
                _fail("QA03_RUNTIME_ASSERTION_FAILED")
        if before != after:
            _fail("QA03_RUNTIME_ASSERTION_FAILED")
        _validate_snapshots(assertions["snapshots"])
    elif case_id == "resource_guard":
        batch, scope = assertions["batch_months"], assertions["scope_months"]
        if (not isinstance(batch, list) or len(batch) != 1 or not isinstance(batch[0], str)
                or not isinstance(scope, list) or batch[0] not in scope
                or any(month not in selected_months for month in scope)
                or assertions["guard_triggered"] is not True or assertions["recovery_passed"] is not True):
            _fail("QA03_RUNTIME_ASSERTION_FAILED")


def _validate_runtime_report(report: Mapping[str, Any], request: Mapping[str, Any], matrix: Mapping[str, Any],
                             report_path: Path) -> None:
    if (report.get("task_id") != "QA-03" or report.get("report_version") != REPORT_VERSION
            or report.get("runtime_complete") is not True or report.get("status") != "PASS"):
        _fail("QA03_RUNTIME_REPORT_INCOMPLETE")
    identity = report.get("identity")
    expected_identity = {"run_id": request["run_id"], "operation_id": request["operation_id"],
                         "namespace": request["namespace"], "matrix_sha256": request["matrix_sha256"],
                         "inventory_sha256": request["inventory_sha256"], "source_cutoff_utc": request["source_cutoff_utc"]}
    if identity != expected_identity:
        _fail("QA03_RUNTIME_REPORT_IDENTITY_MISMATCH")
    if report.get("scope") != matrix["_selected_scope"]:
        _fail("QA03_RUNTIME_REPORT_SCOPE_MISMATCH")
    if report.get("verified_input_refs") != request["input_refs"]:
        _fail("QA03_RUNTIME_INPUT_PINS_MISMATCH")
    runtime_coverage = report.get("coverage")
    if not isinstance(runtime_coverage, list) or len(runtime_coverage) != len(matrix["_computed_coverage"]):
        _fail("QA03_RUNTIME_COVERAGE_INCOMPLETE")
    input_refs_by_archive = {ref["archive_name"]: ref for ref in request["input_refs"]}
    coverage_by_month = {entry.get("month"): entry for entry in runtime_coverage if isinstance(entry, dict)}
    if set(coverage_by_month) != {entry["month"] for entry in matrix["_computed_coverage"]}:
        _fail("QA03_RUNTIME_COVERAGE_INCOMPLETE")
    for expected_coverage in matrix["_computed_coverage"]:
        entry = coverage_by_month[expected_coverage["month"]]
        if any(entry.get(key) != value for key, value in expected_coverage.items()):
            _fail("QA03_RUNTIME_COVERAGE_MISMATCH")
        if entry.get("source_cutoff_utc") != request["source_cutoff_utc"]:
            _fail("QA03_RUNTIME_COVERAGE_MISMATCH")
        archive_pins = entry.get("verified_archives")
        if not isinstance(archive_pins, list):
            _fail("QA03_RUNTIME_ARCHIVE_PINS_REQUIRED")
        archive_names: set[str] = set()
        for pin in archive_pins:
            if not isinstance(pin, dict):
                _fail("QA03_RUNTIME_ARCHIVE_PIN_INVALID")
            name = pin.get("archive_name")
            if (not isinstance(name, str) or name in archive_names or pin.get("verified") is not True
                    or not isinstance(pin.get("catalog_release"), str) or not pin["catalog_release"]
                    or not SAFE_SHA256.fullmatch(str(pin.get("manifest_sha256", "")))
                    or not SAFE_SHA256.fullmatch(str(pin.get("raw_sha256", "")))
                    or name not in input_refs_by_archive
                    or pin.get("catalog_release") != input_refs_by_archive[name]["catalog_release"]
                    or pin.get("manifest_sha256") != input_refs_by_archive[name]["sha256"]
                    or pin.get("raw_sha256") != input_refs_by_archive[name]["raw_sha256"]
                    or pin.get("input_ref_sha256") != input_refs_by_archive[name]["sha256"]):
                _fail("QA03_RUNTIME_ARCHIVE_PIN_INVALID")
            archive_names.add(name)
        if archive_names != set(expected_coverage["required_archives"]):
            _fail("QA03_RUNTIME_ARCHIVE_COVERAGE_MISMATCH")
    cases = report.get("cases")
    if not isinstance(cases, list) or len(cases) != len(REQUIRED_CASES):
        _fail("QA03_RUNTIME_CASES_INCOMPLETE")
    seen: set[str] = set()
    expected_operations = {case["case_id"]: case["operation_id"] for case in matrix["cases"]}
    for case in cases:
        if not isinstance(case, dict):
            _fail("QA03_RUNTIME_CASE_INVALID")
        case_id = case.get("case_id")
        if (case_id not in REQUIRED_CASES or case_id in seen or case.get("status") != "PASS"
                or case.get("operation_id") != expected_operations.get(case_id)):
            _fail("QA03_RUNTIME_CASE_INVALID")
        seen.add(case_id)
        assertions = case.get("assertions")
        if not isinstance(assertions, dict):
            _fail("QA03_RUNTIME_ASSERTIONS_INCOMPLETE")
        _validate_assertions(case_id, assertions, {item["month"] for item in matrix["_selected_scope"]})
        _validate_snapshots(case.get("snapshots_before"), allow_bootstrap=True)
        _validate_snapshots(case.get("snapshots_after"))
        evidence = case.get("evidence")
        if not isinstance(evidence, dict):
            _fail("QA03_RUNTIME_EVIDENCE_INVALID")
        name, checksum = evidence.get("file"), evidence.get("sha256")
        if (not isinstance(name, str) or PurePosixPath(name).name != name or name in ("", ".", "..")
                or not isinstance(checksum, str) or not SAFE_SHA256.fullmatch(checksum)):
            _fail("QA03_RUNTIME_EVIDENCE_INVALID")
        evidence_path = report_path.parent / (report_path.name + ".evidence") / name
        if not _check_path_components(evidence_path) or not evidence_path.is_file() or evidence_path.stat().st_size > MAX_EVIDENCE_BYTES:
            _fail("QA03_RUNTIME_EVIDENCE_INVALID")
        if _sha256_file(evidence_path, "QA03_RUNTIME_EVIDENCE_INVALID") != checksum:
            _fail("QA03_RUNTIME_EVIDENCE_CHECKSUM_MISMATCH")
        evidence_doc = _load(evidence_path, MAX_EVIDENCE_BYTES, "EVIDENCE")
        if (evidence_doc.get("identity") != identity or evidence_doc.get("case_id") != case_id
                or evidence_doc.get("operation_id") != case.get("operation_id")
                or evidence_doc.get("scope") != matrix["_selected_scope"]
                or evidence_doc.get("input_refs") != request["input_refs"]
                or evidence_doc.get("assertions") != assertions
                or evidence_doc.get("snapshots_before") != case["snapshots_before"]
                or evidence_doc.get("snapshots_after") != case["snapshots_after"]):
            _fail("QA03_RUNTIME_EVIDENCE_CONTENT_MISMATCH")
    if seen != set(REQUIRED_CASES):
        _fail("QA03_RUNTIME_CASES_INCOMPLETE")
    capacity = report.get("capacity")
    required_counts = ("download_count", "download_reused", "verified_archive_reused", "resource_failures")
    if (not isinstance(capacity, dict) or not _finite_number(capacity.get("duration_seconds"), positive=True)
            or not _finite_number(capacity.get("peak_memory_mb"), positive=True)
            or any(isinstance(capacity.get(key), bool) or not isinstance(capacity.get(key), int) or capacity[key] < 0 for key in required_counts)):
        _fail("QA03_RUNTIME_CAPACITY_INVALID")
    summary = report.get("summary")
    expected_summary = {"scope_count": len(matrix["_selected_scope"]), "case_count": len(REQUIRED_CASES),
                       "passed": len(REQUIRED_CASES), "failed": 0, "pending_runtime": 0}
    if summary != expected_summary:
        _fail("QA03_RUNTIME_SUMMARY_MISMATCH")
    _validate_snapshots(report.get("snapshots"))


def run_live(request_path: str | Path, environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    env = dict(os.environ if environment is None else environment)
    request_file = Path(request_path)
    request, matrix, report_path, _, request_sha = _validate_request(request_file)
    matrix_path = Path(request["matrix_path"])
    inventory_path = Path(request["inventory_path"])
    command_text = env.get("QA_03_RUNTIME_COMMAND", "").strip()
    if not command_text:
        _fail("QA03_RUNTIME_COMMAND_REQUIRED")
    command = shlex.split(command_text)
    if not command:
        _fail("QA03_RUNTIME_COMMAND_REQUIRED")
    try:
        timeout = int(env.get("QA_03_RUNTIME_TIMEOUT_SECONDS", "3600"))
    except ValueError as error:
        raise Qa03Error("QA03_RUNTIME_TIMEOUT_INVALID") from error
    if not 1 <= timeout <= 3600:
        _fail("QA03_RUNTIME_TIMEOUT_INVALID")
    try:
        completed = subprocess.run(command + [str(request_file)], env=env, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL, timeout=timeout, check=False)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise Qa03Error("QA03_RUNTIME_FAILED") from error
    if completed.returncode != 0:
        _fail("QA03_RUNTIME_FAILED")
    if _sha256_file(request_file, "QA03_REQUEST_READ_FAILED") != request_sha:
        _fail("QA03_RUNTIME_REQUEST_DRIFT")
    if _sha256_file(matrix_path, "QA03_MATRIX_READ_FAILED") != request["matrix_sha256"]:
        _fail("QA03_MATRIX_CHECKSUM_MISMATCH")
    if _inventory_rows(inventory_path)[1] != request["inventory_sha256"]:
        _fail("QA03_INVENTORY_CHECKSUM_MISMATCH")
    report = _load(report_path, MAX_REPORT_BYTES, "REPORT")
    _validate_runtime_report(report, request, matrix, report_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix", type=Path)
    parser.add_argument("--json", type=Path)
    parser.add_argument("--markdown", type=Path)
    parser.add_argument("--live", type=Path, dest="live_request")
    args = parser.parse_args()
    if args.live_request is not None:
        report = run_live(args.live_request)
        print(json.dumps({"task_id": "QA-03", "status": "PASS", "runtime_complete": True,
                          "case_count": len(report["cases"])}, sort_keys=True))
        return
    if not args.matrix or not args.json or not args.markdown:
        parser.error("--matrix, --json and --markdown are required for preparation")
    print(json.dumps(write_preparation(args.matrix, args.json, args.markdown), sort_keys=True))


if __name__ == "__main__":
    main()
