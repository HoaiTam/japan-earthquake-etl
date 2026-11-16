#!/usr/bin/env python3
"""Offline/local preflight verifier for the DEMO-01 demo package.

Validates that every representative run, snapshot pin, SQL file, static report,
fallback plan, and scientific disclaimer referenced by the demo package exists
and agrees with repository evidence without executing ETL or needing Docker/network.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "config" / "demo" / "demo_package_manifest_v1.json"

FORBIDDEN_SECRET_PATTERNS = (
    re.compile(r"minio_root_password", re.IGNORECASE),
    re.compile(r"minio_secret_key", re.IGNORECASE),
    re.compile(r"airflow_fernet_key", re.IGNORECASE),
    re.compile(r"airflow_api_jwt_secret", re.IGNORECASE),
    re.compile(r"aws_secret_access_key", re.IGNORECASE),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    re.compile(r"(?:password|secret_key|access_token|refresh_token)\s*[:=]\s*['\"]?[A-Za-z0-9_/+=-]{8,}", re.IGNORECASE),
)


def _resolve_repo_path(root: Path, relative_path: str) -> Path:
    """Resolve a relative repository path and fail closed on traversal."""
    candidate = Path(relative_path)
    if candidate.is_absolute():
        raise ValueError(f"DEMO_PATH_NOT_RELATIVE:{relative_path}")
    resolved_root = root.resolve()
    resolved = (resolved_root / candidate).resolve()
    if not resolved.is_relative_to(resolved_root):
        raise ValueError(f"DEMO_PATH_ESCAPES_ROOT:{relative_path}")
    if not resolved.exists():
        raise FileNotFoundError(f"DEMO_MISSING_FILE:{relative_path}")
    return resolved


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _check_secret_hygiene(text: str, label: str) -> None:
    for pattern in FORBIDDEN_SECRET_PATTERNS:
        if pattern.search(text):
            raise ValueError(f"DEMO_SECRET_PATTERN_DETECTED:{label}")


def _validate_boundary_and_policy(manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != "demo-package-v1":
        raise ValueError("DEMO_INVALID_SCHEMA_VERSION")
    if manifest.get("task_id") != "DEMO-01":
        raise ValueError("DEMO_INVALID_TASK_ID")
    if manifest.get("depends_on_power_bi") is not False:
        raise ValueError("DEMO_POWER_BI_MUST_BE_OPTIONAL")
    if manifest.get("stage_40_year_rerun_required") is not False:
        raise ValueError("DEMO_FORBIDS_STAGE_40_YEAR_RERUN")

    boundary = manifest.get("scientific_boundary")
    if not isinstance(boundary, dict):
        raise ValueError("DEMO_MISSING_SCIENTIFIC_BOUNDARY")
    for flag in ("retrospective_candidate_only", "no_earthquake_prediction", "no_physical_causality_claim", "denominator_integrity_required"):
        if boundary.get(flag) is not True:
            raise ValueError(f"DEMO_BOUNDARY_FLAG_INVALID:{flag}")

    forbidden_metrics = set(boundary.get("forbidden_metrics") or [])
    if not {"accuracy", "f1"} <= forbidden_metrics:
        raise ValueError("DEMO_FORBIDDEN_METRICS_MUST_INCLUDE_ACCURACY_AND_F1")

    statement = str(boundary.get("statement") or "").lower()
    if "candidate" not in statement or ("khong du doan" not in statement and "không dự đoán" not in statement):
        raise ValueError("DEMO_BOUNDARY_STATEMENT_INCOMPLETE")

    pending = manifest.get("pending_official_gates") or []
    if manifest.get("official_release_ready") is True and pending:
        raise ValueError("DEMO_CANNOT_BE_OFFICIAL_READY_WITH_PENDING_GATES")


def _check_qa01_pin(root: Path, pin: dict[str, Any]) -> dict[str, Any]:
    path = _resolve_repo_path(root, pin["evidence_file"])
    data = _load_json(path)
    actual_run_id = (data.get("run") or {}).get("run_id")
    if actual_run_id != pin["run_id"]:
        raise ValueError(f"DEMO_PIN_MISMATCH:QA-01.run_id:{actual_run_id}!={pin['run_id']}")
    for field in ("operation_id", "publication_id", "status", "coverage_mode", "natural_roi_event_count", "research_ready", "ml_upstream_eligible"):
        if data.get(field) != pin[field]:
            raise ValueError(f"DEMO_PIN_MISMATCH:QA-01.{field}")
    bundle = data.get("snapshot_bundle") or {}
    for table_name, expected_snap in (pin.get("snapshot_ids") or {}).items():
        actual_snap = str((bundle.get(table_name) or {}).get("snapshot_id") or "")
        if actual_snap != str(expected_snap):
            raise ValueError(f"DEMO_PIN_MISMATCH:QA-01.snapshot.{table_name}")
    return {"task_id": "QA-01", "status": "VERIFIED", "run_id": actual_run_id, "snapshot_count": len(pin.get("snapshot_ids") or {})}


def _check_runtime_qa_pin(root: Path, key: str, pin: dict[str, Any]) -> dict[str, Any]:
    path = _resolve_repo_path(root, pin["evidence_file"])
    data = _load_json(path)
    acceptance = data.get("acceptance") or {}
    summary = acceptance.get("summary") or {}
    checks = (
        ("run_id", acceptance.get("run_id"), pin["run_id"]),
        ("status", data.get("status"), pin["status"]),
        ("case_count", summary.get("case_count"), pin["case_count"]),
        ("passed", summary.get("passed"), pin["passed"]),
    )
    for field, actual, expected in checks:
        if actual != expected:
            raise ValueError(f"DEMO_PIN_MISMATCH:{key}.{field}:{actual}!={expected}")
    return {"task_id": pin["task_id"], "status": "VERIFIED", "run_id": pin["run_id"], "cases_passed": f"{pin['passed']}/{pin['case_count']}"}


def _check_pilot_and_ml_pins(root: Path, ev: dict[str, Any]) -> list[dict[str, Any]]:
    results = []

    pilot_pin = ev["pilot_dataset"]
    pilot_data = _load_json(_resolve_repo_path(root, pilot_pin["evidence_file"]))
    for field in ("pilot_id", "archive_name", "archive_sha256", "feature_count", "mainshock_count", "allowed_usage", "research_ready", "scientific_approval"):
        if pilot_data.get(field) != pilot_pin[field]:
            raise ValueError(f"DEMO_PIN_MISMATCH:pilot_dataset.{field}")
    results.append({"task_id": "DAT-02", "status": "VERIFIED_DEVELOPMENT_ONLY", "pilot_id": pilot_pin["pilot_id"]})

    exp02_pin = ev["pilot_baseline_parity"]
    exp02_data = _load_json(_resolve_repo_path(root, exp02_pin["evidence_file"]))
    if exp02_data.get("status") != exp02_pin["status"] or (exp02_data.get("pilot") or {}).get("pilot_id") != exp02_pin["pilot_id"]:
        raise ValueError("DEMO_PIN_MISMATCH:pilot_baseline_parity")
    results.append({"task_id": "EXP-02", "status": "VERIFIED_PILOT_PARITY", "pilot_id": exp02_pin["pilot_id"]})

    exp03_pin = ev["pilot_adaptive_smoke"]
    exp03_data = _load_json(_resolve_repo_path(root, exp03_pin["evidence_file"]))
    exp03_run = (exp03_data.get("experiment_summary") or {}).get("experiment_run_id")
    if exp03_run != exp03_pin["experiment_run_id"]:
        raise ValueError("DEMO_PIN_MISMATCH:pilot_adaptive_smoke.experiment_run_id")
    for field in ("notebook_status", "local_smoke_status", "runner_mode", "fresh_kernel_acceptance"):
        if exp03_data.get(field) != exp03_pin[field]:
            raise ValueError(f"DEMO_PIN_MISMATCH:pilot_adaptive_smoke.{field}")
    results.append({"task_id": "EXP-03", "status": "VERIFIED_LOCAL_SMOKE", "experiment_run_id": exp03_run})

    exp04_pin = ev["pilot_scientific_evaluation"]
    exp04_data = _load_json(_resolve_repo_path(root, exp04_pin["evidence_file"]))
    pv = exp04_data.get("pilot_verification") or {}
    if exp04_data.get("evaluation_scope") != exp04_pin["evaluation_scope"] or pv.get("pilot_id") != exp04_pin["pilot_id"]:
        raise ValueError("DEMO_PIN_MISMATCH:pilot_scientific_evaluation")
    if pv.get("candidate_rows") != exp04_pin["candidate_rows"] or pv.get("mainshock_count") != exp04_pin["mainshock_count"]:
        raise ValueError("DEMO_PIN_MISMATCH:pilot_scientific_evaluation.counts")
    results.append({"task_id": "EXP-04", "status": "VERIFIED_PILOT_EVALUATION", "pilot_id": exp04_pin["pilot_id"]})

    mli04_pin = ev["static_report_and_ml_sql"]
    report_text = _resolve_repo_path(root, mli04_pin["report_file"]).read_text(encoding="utf-8")
    if "candidate" not in report_text.lower() or "PENDING OFFICIAL IMPORT" not in report_text:
        raise ValueError("DEMO_PIN_MISMATCH:MLI-04_STATIC_REPORT_BOUNDARY")
    _resolve_repo_path(root, mli04_pin["sql_verification_file"])
    for view_path in mli04_pin.get("sql_views") or []:
        _resolve_repo_path(root, view_path)
    results.append({"task_id": "MLI-04", "status": "VERIFIED_TEMPLATE_PENDING_OFFICIAL_SQL_PARITY"})
    return results


def _check_orc07_pin(root: Path, pin: dict[str, Any]) -> dict[str, Any]:
    text = _resolve_repo_path(root, pin["evidence_file"]).read_text(encoding="utf-8")
    for required_token in (pin["run_id"], pin["operation_id"], pin["publication_id"], pin["namespace"], str(pin["canonical_events"])):
        if required_token not in text:
            raise ValueError(f"DEMO_PIN_MISMATCH:ORC-07.missing_token:{required_token}")
    return {"task_id": "ORC-07", "status": "VERIFIED", "run_id": pin["run_id"]}


def _check_fallback_modes(manifest: dict[str, Any]) -> list[str]:
    fallbacks = manifest.get("fallback_modes") or {}
    required = ("network_or_source_api_outage", "drive_or_colab_unavailable", "docker_or_memory_constrained", "power_bi_unavailable")
    missing = [name for name in required if name not in fallbacks or not (fallbacks[name] or {}).get("label_required")]
    if missing:
        raise ValueError(f"DEMO_MISSING_FALLBACK_MODES:{','.join(missing)}")
    return list(required)


def _check_optional_local_receipt(local_receipt_path: Path | None) -> dict[str, Any]:
    if local_receipt_path is None:
        return {"checked": False, "status": "NOT_REQUESTED"}
    if not local_receipt_path.exists():
        return {"checked": True, "status": "ABSENT", "path": str(local_receipt_path)}
    receipt = _load_json(local_receipt_path)
    return {
        "checked": True,
        "status": str(receipt.get("status") or "UNKNOWN"),
        "recipient": receipt.get("recipient"),
        "all_shards_verified": receipt.get("all_shards_verified"),
        "handoff_sha256": receipt.get("handoff_sha256"),
    }


def run_preflight(
    manifest_path: Path = DEFAULT_MANIFEST,
    root: Path = ROOT,
    local_receipt_path: Path | None = None,
) -> dict[str, Any]:
    """Run all offline DEMO-01 manifest, pin, fallback, and boundary checks."""
    raw_manifest = manifest_path.read_text(encoding="utf-8")
    _check_secret_hygiene(raw_manifest, manifest_path.name)
    manifest = json.loads(raw_manifest)
    _validate_boundary_and_policy(manifest)

    for rel_path in manifest.get("required_repo_files") or []:
        _resolve_repo_path(root, rel_path)

    ev = manifest.get("representative_evidence") or {}
    verified_runs = [
        _check_qa01_pin(root, ev["daily_operational_v2"]),
        _check_orc07_pin(root, ev["bounded_real_etl"]),
        _check_runtime_qa_pin(root, "idempotency_rerun_qa", ev["idempotency_rerun_qa"]),
        _check_runtime_qa_pin(root, "historical_backfill_qa", ev["historical_backfill_qa"]),
        _check_runtime_qa_pin(root, "failure_recovery_qa", ev["failure_recovery_qa"]),
    ]
    verified_runs.extend(_check_pilot_and_ml_pins(root, ev))
    fallback_modes = _check_fallback_modes(manifest)

    pending_gates = manifest.get("pending_official_gates") or []
    overall_status = "PASS_OFFICIAL_READY" if not pending_gates else "PASS_WITH_PENDING_GATES"
    report = {
        "schema_version": "demo-preflight-report-v1",
        "task_id": "DEMO-01",
        "overall_status": overall_status,
        "demo_package_prepared": True,
        "official_release_ready": bool(manifest.get("official_release_ready", False)),
        "depends_on_power_bi": bool(manifest.get("depends_on_power_bi", False)),
        "stage_40_year_rerun_required": bool(manifest.get("stage_40_year_rerun_required", False)),
        "verified_evidence_count": len(verified_runs),
        "verified_runs": verified_runs,
        "fallback_modes_verified": fallback_modes,
        "pending_official_gates": pending_gates,
        "local_research_receipt": _check_optional_local_receipt(local_receipt_path),
    }
    _check_secret_hygiene(json.dumps(report, ensure_ascii=False), "preflight_report")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST, help="Path to demo package manifest JSON.")
    parser.add_argument("--local-receipt", type=Path, default=None, help="Optional path to external DAT-03 v4 team receipt JSON.")
    parser.add_argument("--output-report", type=Path, default=None, help="Optional path to write preflight JSON summary.")
    parser.add_argument("--json", action="store_true", help="Print JSON summary to stdout.")
    args = parser.parse_args()

    try:
        report = run_preflight(manifest_path=args.manifest, root=ROOT, local_receipt_path=args.local_receipt)
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        error_payload = {"task_id": "DEMO-01", "overall_status": "FAILED", "error": str(exc)}
        print(json.dumps(error_payload, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1

    serialized = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output_report is not None:
        args.output_report.parent.mkdir(parents=True, exist_ok=True)
        args.output_report.write_text(serialized + "\n", encoding="utf-8")

    if args.json:
        print(serialized)
    else:
        print(
            f"task={report['task_id']} status={report['overall_status']} "
            f"verified_runs={report['verified_evidence_count']} "
            f"pending_gates={len(report['pending_official_gates'])} "
            f"official_release_ready={report['official_release_ready']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
