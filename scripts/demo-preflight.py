#!/usr/bin/env python3
"""Offline/local preflight verifier for the DEMO-01 demo package.

Validates that every representative run, snapshot pin, SQL file, static report,
fallback plan, and scientific disclaimer referenced by the demo package exists
and agrees with repository evidence without executing ETL or needing Docker/network.

V1 retains its preparation-only behavior. V2 derives readiness from pinned
research, SQL parity and end-to-end acceptance evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "config" / "demo" / "demo_package_manifest_v1.json"
CURRENT_MANIFEST = ROOT / "config" / "demo" / "demo_package_manifest_v2.json"
PREPARED_PENDING_STATUS = "PREPARED_PENDING_OFFICIAL_GATES"
REQUIRED_PENDING_GATES = ("EXP-02", "EXP-03", "EXP-04", "MLI-04", "MLQ-01")
VALID_ROSTER = ("HoaiTam", "ThanhTris", "rosy179")
REQUIRED_DAT03_SPLITS = ("REPRODUCTION", "EXTENSION")
REQUIRED_ML_ALGORITHMS = frozenset(("WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"))
ML_SNAPSHOT_FIELDS = ("experiment_snapshot_id", "membership_snapshot_id", "summary_snapshot_id")
ML_BUNDLE_CONTRACT = "spark/src/main/resources/ml/result-bundle-v1.json"
REQUIRED_QA01_TABLES = (
    "event_current",
    "event_source_bridge",
    "dim_date",
    "dim_region",
    "dim_magnitude_band",
    "dim_depth_band",
)
REQUIRED_DAT03_COLLECTION_FIELDS = (
    "dataset_id",
    "collection_id",
    "manifest_sha256",
    "checksums_sha256",
    "shard_count",
    "feature_row_count",
    "mainshock_count",
    "timestamp_reader_profile",
)
REQUIRED_DAT03_COLLECTION_INT_FIELDS = ("shard_count", "feature_row_count", "mainshock_count")
REQUIRED_GOLD_PIN_FIELDS = (
    "gold_snapshot_id",
    "publication_bundle_sha256",
    "gold_publication_id",
    "physical_gold_table_name",
)
INT64_MAX = 9223372036854775807

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
    """Resolve a relative repository path and fail closed on traversal or symlinks."""
    candidate = Path(relative_path)
    if candidate.is_absolute():
        raise ValueError(f"DEMO_PATH_NOT_RELATIVE:{relative_path}")
    resolved_root = root.resolve()
    resolved = (resolved_root / candidate).resolve()
    if not resolved.is_relative_to(resolved_root):
        raise ValueError(f"DEMO_PATH_ESCAPES_ROOT:{relative_path}")
    if resolved.is_symlink() or any(p.is_symlink() for p in (resolved_root / candidate).parents if p != resolved_root and p.is_relative_to(resolved_root)):
        raise ValueError(f"DEMO_PATH_SYMLINK_REJECTED:{relative_path}")
    if not resolved.exists():
        raise FileNotFoundError(f"DEMO_MISSING_FILE:{relative_path}")
    return resolved


def _load_json_strict(path: Path, error_code: str = "DEMO_INVALID_JSON") -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{error_code}:{exc}") from exc
    if not isinstance(data, dict):
        raise ValueError(f"{error_code}:TOP_LEVEL_OBJECT_REQUIRED")
    return data


def _check_secret_hygiene(text: str, label: str) -> None:
    for pattern in FORBIDDEN_SECRET_PATTERNS:
        if pattern.search(text):
            raise ValueError(f"DEMO_SECRET_PATTERN_DETECTED:{label}")


def _validate_boundary_and_policy(manifest: dict[str, Any]) -> None:
    version = manifest.get("schema_version")
    if version not in {"demo-package-v1", "demo-package-v2"}:
        raise ValueError("DEMO_INVALID_SCHEMA_VERSION")
    if manifest.get("task_id") != "DEMO-01":
        raise ValueError("DEMO_INVALID_TASK_ID")
    if manifest.get("package_status") not in (
        {PREPARED_PENDING_STATUS} if version == "demo-package-v1" else {PREPARED_PENDING_STATUS, "PASS_OFFICIAL_READY"}
    ):
        raise ValueError("DEMO_INVALID_PACKAGE_STATUS")
    if version == "demo-package-v1" and manifest.get("official_release_ready") is not False:
        raise ValueError("DEMO_OFFICIAL_RELEASE_NOT_ALLOWED_BEFORE_GATES")
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

    if version == "demo-package-v2":
        return
    pending = manifest.get("pending_official_gates")
    if not isinstance(pending, list) or not pending:
        raise ValueError("DEMO_MISSING_REQUIRED_PENDING_GATES:EMPTY")
    present_gate_ids = {
        str(item.get("task_id") or "")
        for item in pending
        if isinstance(item, dict) and str(item.get("reason") or "").strip()
    }
    missing_gates = [gate for gate in REQUIRED_PENDING_GATES if gate not in present_gate_ids]
    if missing_gates:
        raise ValueError(f"DEMO_MISSING_REQUIRED_PENDING_GATES:{','.join(missing_gates)}")


def _normalize_int64_snapshot(value: Any, label: str) -> str:
    """Validate and normalize an Iceberg int64 snapshot ID without float conversion."""
    if isinstance(value, bool):
        raise ValueError(f"DEMO_PIN_INVALID_INT64_SNAPSHOT:{label}")
    if isinstance(value, int):
        raw = str(value)
    elif isinstance(value, str):
        raw = value.strip()
        if raw != value or not raw.isdigit():
            raise ValueError(f"DEMO_PIN_INVALID_INT64_SNAPSHOT:{label}")
    else:
        raise ValueError(f"DEMO_PIN_INVALID_INT64_SNAPSHOT:{label}")
    if not re.fullmatch(r"[1-9][0-9]{0,18}", raw) or int(raw) > INT64_MAX:
        raise ValueError(f"DEMO_PIN_INVALID_INT64_SNAPSHOT:{label}")
    return raw


def _require_exact_keys(mapping: Any, required: tuple[str, ...], error_prefix: str) -> dict[str, Any]:
    """Require mapping to be a dict containing the exact expected key set."""
    if not isinstance(mapping, dict) or set(mapping.keys()) != set(required):
        found = sorted(mapping.keys()) if isinstance(mapping, dict) else type(mapping).__name__
        raise ValueError(f"{error_prefix}:expected={list(required)}:got={found}")
    return mapping


def _validate_collection_fields(col: Any, split_name: str, error_prefix: str) -> dict[str, Any]:
    """Ensure every DAT-03 split collection entry carries complete non-empty fields."""
    if not isinstance(col, dict):
        raise ValueError(f"{error_prefix}:{split_name}:NOT_OBJECT")
    for field in REQUIRED_DAT03_COLLECTION_FIELDS:
        val = col.get(field)
        if field in REQUIRED_DAT03_COLLECTION_INT_FIELDS:
            if type(val) is not int or val <= 0:
                raise ValueError(f"{error_prefix}:{split_name}.{field}:INVALID_INT")
        elif not isinstance(val, str) or not val.strip():
            raise ValueError(f"{error_prefix}:{split_name}.{field}:EMPTY_OR_INVALID")
    return col


def _validate_gold_pin_shape(gold_pin: Any, error_prefix: str) -> dict[str, str]:
    """Validate structure and int64 snapshot fidelity of a DAT-03 Gold pin."""
    pin = _require_exact_keys(gold_pin, REQUIRED_GOLD_PIN_FIELDS, error_prefix)
    pub_sha = pin.get("publication_bundle_sha256")
    pub_id = pin.get("gold_publication_id")
    table_name = pin.get("physical_gold_table_name")
    if not isinstance(pub_sha, str) or not re.fullmatch(r"[a-f0-9]{64}", pub_sha):
        raise ValueError(f"{error_prefix}.publication_bundle_sha256")
    if pub_id != f"pub_{pub_sha}":
        raise ValueError(f"{error_prefix}.gold_publication_id")
    if not isinstance(table_name, str) or not re.fullmatch(r"iceberg\.[A-Za-z0-9_]+\.event_current", table_name):
        raise ValueError(f"{error_prefix}.physical_gold_table_name")
    snap_id = _normalize_int64_snapshot(pin.get("gold_snapshot_id"), f"{error_prefix}.gold_snapshot_id")
    return {
        "gold_publication_id": pub_id,
        "gold_snapshot_id": snap_id,
        "physical_gold_table_name": table_name,
        "publication_bundle_sha256": pub_sha,
    }


def _check_qa01_pin(root: Path, pin: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(pin, dict):
        raise ValueError("DEMO_PIN_MISMATCH:QA-01.NOT_OBJECT")
    path = _resolve_repo_path(root, pin["evidence_file"])
    data = _load_json_strict(path)
    actual_run_id = (data.get("run") or {}).get("run_id")
    if actual_run_id != pin["run_id"]:
        raise ValueError(f"DEMO_PIN_MISMATCH:QA-01.run_id:{actual_run_id}!={pin['run_id']}")
    for field in ("operation_id", "publication_id", "status", "coverage_mode", "natural_roi_event_count", "research_ready", "ml_upstream_eligible"):
        if data.get(field) != pin[field]:
            raise ValueError(f"DEMO_PIN_MISMATCH:QA-01.{field}")
    snapshot_ids = _require_exact_keys(
        pin.get("snapshot_ids"),
        REQUIRED_QA01_TABLES,
        "DEMO_PIN_MISMATCH:QA-01.snapshot_ids_set_mismatch",
    )
    bundle = _require_exact_keys(
        data.get("snapshot_bundle"),
        REQUIRED_QA01_TABLES,
        "DEMO_PIN_MISMATCH:QA-01.evidence_snapshot_bundle_incomplete",
    )
    for table_name in REQUIRED_QA01_TABLES:
        expected_snap = _normalize_int64_snapshot(snapshot_ids[table_name], f"QA-01.{table_name}")
        actual_snap = _normalize_int64_snapshot(
            (bundle.get(table_name) or {}).get("snapshot_id"),
            f"QA-01.evidence.{table_name}",
        )
        if actual_snap != expected_snap:
            raise ValueError(f"DEMO_PIN_MISMATCH:QA-01.snapshot.{table_name}")
    return {"task_id": "QA-01", "status": "VERIFIED", "run_id": actual_run_id, "snapshot_count": len(REQUIRED_QA01_TABLES)}


def _check_runtime_qa_pin(root: Path, key: str, pin: dict[str, Any]) -> dict[str, Any]:
    path = _resolve_repo_path(root, pin["evidence_file"])
    data = _load_json_strict(path)
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


def _load_authoritative_dat03_gold_and_collections(root: Path) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    """Derive authoritative Gold and split export pins from DAT-03 acceptance evidence."""
    official_doc = _load_json_strict(_resolve_repo_path(root, "docs/evidence/DAT-03_OFFICIAL_COLLECTIONS_20261115.json"))
    gold_filename = official_doc.get("gold_evidence")
    if not isinstance(gold_filename, str) or "/" in gold_filename or "\\" in gold_filename or not gold_filename.endswith(".json"):
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.authoritative_gold_evidence_ref")
    gold_doc = _load_json_strict(_resolve_repo_path(root, f"docs/evidence/{gold_filename}"))

    commit_sha = (gold_doc.get("first_publication") or {}).get("commit_sha256")
    namespace = gold_doc.get("namespace")
    gold_snap = _normalize_int64_snapshot(
        ((gold_doc.get("tables") or {}).get("event_current") or {}).get("snapshot_id"),
        "DAT-03.portable_gold.event_current",
    )
    postflight_snap = _normalize_int64_snapshot(
        ((official_doc.get("independent_postflight") or {}).get("results") or {}).get("gold_event_current"),
        "DAT-03.official_collections.gold_event_current",
    )
    if postflight_snap != gold_snap:
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.authoritative_gold_snapshot_divergence")

    auth_gold = _validate_gold_pin_shape(
        {
            "gold_publication_id": f"pub_{commit_sha}",
            "gold_snapshot_id": gold_snap,
            "physical_gold_table_name": f"{namespace}.event_current",
            "publication_bundle_sha256": commit_sha,
        },
        "DEMO_PIN_MISMATCH:DAT-03.authoritative_gold_pin",
    )

    preflight_by_split: dict[str, dict[str, Any]] = {}
    for entry in official_doc.get("preflights") or []:
        if isinstance(entry, dict) and entry.get("dataset_split") in REQUIRED_DAT03_SPLITS:
            preflight_by_split[entry["dataset_split"]] = entry
    build_by_split: dict[str, dict[str, Any]] = {}
    for session in official_doc.get("accepted_build_and_rerun_sessions") or []:
        if (
            isinstance(session, dict)
            and session.get("mode") == "build"
            and session.get("status") == "DATASET_EXPORTED"
            and session.get("dataset_split") in REQUIRED_DAT03_SPLITS
        ):
            build_by_split[session["dataset_split"]] = session.get("final_readback") or {}

    if set(preflight_by_split) != set(REQUIRED_DAT03_SPLITS) or set(build_by_split) != set(REQUIRED_DAT03_SPLITS):
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.official_collections_splits_incomplete")

    for split_name in REQUIRED_DAT03_SPLITS:
        cov = preflight_by_split[split_name].get("coverage") or {}
        if cov.get("published_commit_sha256") != auth_gold["publication_bundle_sha256"]:
            raise ValueError(f"DEMO_PIN_MISMATCH:DAT-03.{split_name}.authoritative_commit_mismatch")
        export_snap = _normalize_int64_snapshot(
            build_by_split[split_name].get("gold_snapshot_id"),
            f"DAT-03.official_collections.{split_name}.gold_snapshot_id",
        )
        if export_snap != auth_gold["gold_snapshot_id"]:
            raise ValueError(f"DEMO_PIN_MISMATCH:DAT-03.{split_name}.authoritative_gold_snapshot_mismatch")

    return auth_gold, build_by_split


def _check_dat03_release_pin(root: Path, pin: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(pin, dict):
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.NOT_OBJECT")
    path = _resolve_repo_path(root, pin["evidence_file"])
    data = _load_json_strict(path)
    release = data.get("release") or {}
    if not isinstance(pin.get("zip_sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", pin["zip_sha256"]):
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.zip_sha256")
    if release.get("zip_sha256") != pin["zip_sha256"]:
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.zip_sha256")
    if not isinstance(pin.get("handoff_sha256"), str) or not re.fullmatch(r"[a-f0-9]{64}", pin["handoff_sha256"]):
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.handoff_sha256")
    if release.get("handoff_sha256") != pin["handoff_sha256"]:
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.handoff_sha256")
    if pin.get("scientific_approval") is not False:
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.manifest_scientific_approval")
    reg = data.get("regression") or {}
    if reg.get("scientific_approval") is not False:
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.scientific_approval")

    auth_gold, auth_builds = _load_authoritative_dat03_gold_and_collections(root)
    manifest_gold = _validate_gold_pin_shape(pin.get("gold_pin"), "DEMO_PIN_MISMATCH:DAT-03.gold_pin")
    for field in REQUIRED_GOLD_PIN_FIELDS:
        if manifest_gold[field] != auth_gold[field]:
            raise ValueError(f"DEMO_PIN_MISMATCH:DAT-03.gold_pin.{field}")

    acceptance_file = pin.get("final_acceptance_file")
    if acceptance_file is not None:
        acc_doc = _load_json_strict(_resolve_repo_path(root, acceptance_file))
        if acc_doc.get("task_status") != "Done" or acc_doc.get("scientific_approval") is not False:
            raise ValueError("DEMO_PIN_MISMATCH:DAT-03.final_acceptance_status")
        if acc_doc.get("zip_sha256") != release["zip_sha256"] or acc_doc.get("handoff_sha256") != release["handoff_sha256"]:
            raise ValueError("DEMO_PIN_MISMATCH:DAT-03.final_acceptance_hashes")
        if set(acc_doc.get("exported_splits") or []) != set(REQUIRED_DAT03_SPLITS):
            raise ValueError("DEMO_PIN_MISMATCH:DAT-03.final_acceptance_splits")
    if pin.get("receipt_version") != "dat03-team-research-v1":
        raise ValueError("DEMO_PIN_MISMATCH:DAT-03.receipt_version")

    pin_collections = _require_exact_keys(
        pin.get("collections"),
        REQUIRED_DAT03_SPLITS,
        "DEMO_PIN_MISMATCH:DAT-03.collections_set_mismatch",
    )
    actual_collections = _require_exact_keys(
        reg.get("collections"),
        REQUIRED_DAT03_SPLITS,
        "DEMO_PIN_MISMATCH:DAT-03.evidence_collections_set_mismatch",
    )

    validated_collections: dict[str, dict[str, Any]] = {}
    for split_name in REQUIRED_DAT03_SPLITS:
        expected_col = _validate_collection_fields(pin_collections[split_name], split_name, "DEMO_PIN_MISMATCH:DAT-03")
        actual_col = _validate_collection_fields(actual_collections[split_name], split_name, "DEMO_PIN_MISMATCH:DAT-03.evidence")
        build_rb = auth_builds[split_name]
        for field in REQUIRED_DAT03_COLLECTION_FIELDS:
            if actual_col.get(field) != expected_col[field]:
                raise ValueError(f"DEMO_PIN_MISMATCH:DAT-03.{split_name}.{field}")
            build_field = "bundle_manifest_sha256" if field == "manifest_sha256" else field
            if build_field in build_rb and build_rb.get(build_field) != expected_col[field]:
                raise ValueError(f"DEMO_PIN_MISMATCH:DAT-03.{split_name}.official_export.{field}")
        validated_collections[split_name] = dict(actual_col)

    validated_release = {
        "receipt_version": str(pin.get("receipt_version") or "dat03-team-research-v1"),
        "handoff_sha256": release["handoff_sha256"],
        "gold_pin": auth_gold,
        "collections": validated_collections,
    }
    return (
        {
            "task_id": "DAT-03",
            "status": "VERIFIED_PAIRED_RELEASE_V4",
            "handoff_sha256": release["handoff_sha256"],
        },
        validated_release,
    )


def _check_pilot_and_ml_pins(root: Path, ev: dict[str, Any]) -> list[dict[str, Any]]:
    results = []

    pilot_pin = ev["pilot_dataset"]
    pilot_data = _load_json_strict(_resolve_repo_path(root, pilot_pin["evidence_file"]))
    for field in ("pilot_id", "archive_name", "archive_sha256", "feature_count", "mainshock_count", "allowed_usage", "research_ready", "scientific_approval"):
        if pilot_data.get(field) != pilot_pin[field]:
            raise ValueError(f"DEMO_PIN_MISMATCH:pilot_dataset.{field}")
    results.append({"task_id": "DAT-02", "status": "VERIFIED_DEVELOPMENT_ONLY", "pilot_id": pilot_pin["pilot_id"]})

    exp02_pin = ev["pilot_baseline_parity"]
    exp02_data = _load_json_strict(_resolve_repo_path(root, exp02_pin["evidence_file"]))
    if exp02_data.get("status") != exp02_pin["status"] or (exp02_data.get("pilot") or {}).get("pilot_id") != exp02_pin["pilot_id"]:
        raise ValueError("DEMO_PIN_MISMATCH:pilot_baseline_parity")
    results.append({"task_id": "EXP-02", "status": "VERIFIED_PILOT_PARITY", "pilot_id": exp02_pin["pilot_id"]})

    exp03_pin = ev["pilot_adaptive_smoke"]
    exp03_data = _load_json_strict(_resolve_repo_path(root, exp03_pin["evidence_file"]))
    exp03_run = (exp03_data.get("experiment_summary") or {}).get("experiment_run_id")
    if exp03_run != exp03_pin["experiment_run_id"]:
        raise ValueError("DEMO_PIN_MISMATCH:pilot_adaptive_smoke.experiment_run_id")
    for field in ("notebook_status", "local_smoke_status", "runner_mode", "fresh_kernel_acceptance"):
        if exp03_data.get(field) != exp03_pin[field]:
            raise ValueError(f"DEMO_PIN_MISMATCH:pilot_adaptive_smoke.{field}")
    results.append({"task_id": "EXP-03", "status": "VERIFIED_LOCAL_SMOKE", "experiment_run_id": exp03_run})

    exp04_pin = ev["pilot_scientific_evaluation"]
    exp04_data = _load_json_strict(_resolve_repo_path(root, exp04_pin["evidence_file"]))
    pv = exp04_data.get("pilot_verification") or {}
    if exp04_data.get("evaluation_scope") != exp04_pin["evaluation_scope"] or pv.get("pilot_id") != exp04_pin["pilot_id"]:
        raise ValueError("DEMO_PIN_MISMATCH:pilot_scientific_evaluation")
    if pv.get("candidate_rows") != exp04_pin["candidate_rows"] or pv.get("mainshock_count") != exp04_pin["mainshock_count"]:
        raise ValueError("DEMO_PIN_MISMATCH:pilot_scientific_evaluation.counts")
    results.append({"task_id": "EXP-04", "status": "VERIFIED_PILOT_EVALUATION", "pilot_id": exp04_pin["pilot_id"]})

    mli04_pin = ev["static_report_and_ml_sql"]
    if mli04_pin.get("official_sql_parity_verified") is not False:
        raise ValueError("DEMO_MLI04_PARITY_MUST_BE_FALSE_BEFORE_OFFICIAL_IMPORT")
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


def _verify_local_receipt(
    local_receipt_path: Path | None,
    release_pin: dict[str, Any],
    expected_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    """Fail-closed validation of an optional external DAT-03 v4 team receipt JSON.

    Never fabricates verification: rejects missing, malformed, failed, wrong-handoff,
    or tampered collection/Gold pins, and notes that JSON alone does not prove kit shards
    remain present on disk (which requires running verify_dataset.py on the kit).
    """
    if expected_receipt_sha256 is not None and local_receipt_path is None:
        raise ValueError("DEMO_EXPECTED_RECEIPT_SHA_REQUIRES_LOCAL_RECEIPT")
    if local_receipt_path is None:
        return {"checked": False, "status": "NOT_REQUESTED", "proves_kit_on_disk": False}

    receipt_path = local_receipt_path.expanduser()
    if receipt_path.is_symlink():
        raise ValueError("DEMO_RECEIPT_SYMLINK_REJECTED")
    if not receipt_path.is_file():
        raise FileNotFoundError(f"DEMO_RECEIPT_MISSING:{receipt_path}")

    raw_bytes = receipt_path.read_bytes()
    if len(raw_bytes) == 0 or len(raw_bytes) > 512 * 1024:
        raise ValueError("DEMO_RECEIPT_SIZE_BOUND_EXCEEDED")
    actual_receipt_sha256 = hashlib.sha256(raw_bytes).hexdigest()

    if expected_receipt_sha256 is not None:
        if not re.fullmatch(r"[a-f0-9]{64}", expected_receipt_sha256):
            raise ValueError("DEMO_EXPECTED_RECEIPT_SHA_INVALID_FORMAT")
        if actual_receipt_sha256 != expected_receipt_sha256:
            raise ValueError(f"DEMO_RECEIPT_SHA_MISMATCH:{actual_receipt_sha256}!={expected_receipt_sha256}")

    receipt = _load_json_strict(receipt_path, error_code="DEMO_RECEIPT_INVALID_JSON")
    _check_secret_hygiene(json.dumps(receipt, ensure_ascii=False), "local_receipt")

    if receipt.get("version") != release_pin["receipt_version"]:
        raise ValueError("DEMO_RECEIPT_VERSION_MISMATCH")
    if receipt.get("status") != "TEAM_RESEARCH_VERIFIED":
        raise ValueError(f"DEMO_RECEIPT_STATUS_NOT_VERIFIED:{receipt.get('status')}")
    if receipt.get("all_shards_verified") is not True:
        raise ValueError("DEMO_RECEIPT_ALL_SHARDS_NOT_VERIFIED")
    if receipt.get("scientific_approval") is not False:
        raise ValueError("DEMO_RECEIPT_SCIENTIFIC_APPROVAL_MUST_BE_FALSE")
    if receipt.get("recipient") not in VALID_ROSTER:
        raise ValueError(f"DEMO_RECEIPT_INVALID_RECIPIENT:{receipt.get('recipient')}")
    if receipt.get("handoff_sha256") != release_pin["handoff_sha256"]:
        raise ValueError("DEMO_RECEIPT_HANDOFF_SHA_MISMATCH")
    fingerprint = str(receipt.get("machine_fingerprint_sha256") or "")
    if not re.fullmatch(r"[a-f0-9]{64}", fingerprint):
        raise ValueError("DEMO_RECEIPT_INVALID_MACHINE_FINGERPRINT")

    collections = _require_exact_keys(
        receipt.get("collections"),
        REQUIRED_DAT03_SPLITS,
        "DEMO_RECEIPT_COLLECTIONS_SET_MISMATCH",
    )
    expected_collections = _require_exact_keys(
        release_pin.get("collections"),
        REQUIRED_DAT03_SPLITS,
        "DEMO_PIN_MISMATCH:DAT-03.collections_set_mismatch",
    )

    expected_gold = _validate_gold_pin_shape(release_pin.get("gold_pin"), "DEMO_PIN_MISMATCH:DAT-03.gold_pin")
    for split_name in REQUIRED_DAT03_SPLITS:
        expected_col = _validate_collection_fields(expected_collections.get(split_name), split_name, "DEMO_PIN_MISMATCH:DAT-03")
        actual_col = collections.get(split_name)
        if not isinstance(actual_col, dict):
            raise ValueError(f"DEMO_RECEIPT_INVALID_COLLECTION:{split_name}")
        if actual_col.get("status") != "COLLECTION_VERIFIED" or actual_col.get("all_shards_verified") is not True:
            raise ValueError(f"DEMO_RECEIPT_COLLECTION_NOT_VERIFIED:{split_name}")
        if actual_col.get("dataset_split") != split_name:
            raise ValueError(f"DEMO_RECEIPT_SPLIT_MISMATCH:{split_name}")
        if actual_col.get("timestamps_losslessly_normalized") is not True:
            raise ValueError(f"DEMO_RECEIPT_TIMESTAMPS_NOT_NORMALIZED:{split_name}")
        _validate_collection_fields(actual_col, split_name, "DEMO_RECEIPT_INVALID_COLLECTION")
        for field in REQUIRED_DAT03_COLLECTION_FIELDS:
            if actual_col.get(field) != expected_col[field]:
                raise ValueError(f"DEMO_RECEIPT_COLLECTION_MISMATCH:{split_name}.{field}")
        actual_gold = _validate_gold_pin_shape(actual_col.get("gold_pin"), f"DEMO_RECEIPT_GOLD_PIN_MISMATCH:{split_name}")
        if actual_gold != expected_gold:
            raise ValueError(f"DEMO_RECEIPT_GOLD_PIN_MISMATCH:{split_name}")

    auth_scope = (
        "EXTERNAL_RECEIPT_SHA_AND_PINS_VERIFIED"
        if expected_receipt_sha256 is not None
        else "SCHEMA_AND_V4_PINS_VERIFIED_SELF_REPORTED_LOCAL_FILE"
    )
    return {
        "checked": True,
        "status": "TEAM_RESEARCH_VERIFIED",
        "verification_scope": auth_scope,
        "recipient": receipt["recipient"],
        "recipient_self_reported": bool(receipt.get("recipient_self_reported", False)),
        "all_shards_verified": True,
        "handoff_sha256": receipt["handoff_sha256"],
        "receipt_sha256": actual_receipt_sha256,
        "proves_kit_on_disk": False,
    }


def _pinned_official_json(root, pin):
    path = _resolve_repo_path(root, pin["path"])
    raw = path.read_bytes().replace(b"\r\n", b"\n")
    if hashlib.sha256(raw).hexdigest() != pin["sha256_lf"]:
        raise ValueError("DEMO_OFFICIAL_SHA_MISMATCH")
    result = json.loads(raw)
    if not isinstance(result, dict):
        raise ValueError("DEMO_OFFICIAL_EVIDENCE_INVALID")
    return result


def _extension_bundle_fingerprint(root, case, pin, model_config_version):
    marker_path = case.get("success_marker_path")
    if not isinstance(marker_path, str) or Path(marker_path).name != "_SUCCESS.json":
        raise ValueError("DEMO_MLQ01_EXTENSION_MARKER")
    # EXP-05 pins the original artifact bytes, including the line endings.
    raw = _resolve_repo_path(root, marker_path).read_bytes()
    if hashlib.sha256(raw).hexdigest() != pin["success_marker_sha256"]:
        raise ValueError("DEMO_MLQ01_EXTENSION_MARKER_SHA")
    marker = json.loads(raw)
    contract = _load_json_strict(_resolve_repo_path(root, ML_BUNDLE_CONTRACT))
    identity = ("dataset_id", "experiment_run_id", "algorithm_name")
    if (not isinstance(marker, dict) or marker.get("status") != "COMPLETED"
            or marker.get("schema_version") != contract["schema_version"]
            or any(marker.get(field) != case[field] for field in identity)
            or not isinstance(model_config_version, str) or not model_config_version.strip()
            or marker.get("model_config_version") != model_config_version):
        raise ValueError("DEMO_MLQ01_EXTENSION_MARKER")
    checksums = marker.get("checksums")
    required = set(contract["required_artifacts"])
    if (not isinstance(checksums, dict) or not required.issubset(checksums)
            or any(name not in required
                   and not re.fullmatch(r"optional-model-artifacts/[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?", name)
                   for name in checksums)
            or any(not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest)
                   for digest in checksums.values())):
        raise ValueError("DEMO_MLQ01_EXTENSION_MARKER_CHECKSUMS")

    # ResultBundleContract uses Java TreeMap.toString() for the checksums.
    checksums_input = "{" + ", ".join(f"{name}={checksums[name]}" for name in sorted(checksums)) + "}"
    fields = [marker[field] for field in identity]
    return hashlib.sha256("\n".join([*fields, model_config_version, checksums_input]).encode("utf-8")).hexdigest()


def _validate_mlq_extension(root, case, report, exp05_sha):
    if not isinstance(case, dict) or case.get("exp05_sha256_lf") != exp05_sha:
        raise ValueError("DEMO_MLQ01_EXTENSION_PIN")
    algo, shard = case.get("algorithm_name"), case.get("shard_id")
    if not isinstance(algo, str) or algo not in REQUIRED_ML_ALGORITHMS or not isinstance(shard, str):
        raise ValueError("DEMO_MLQ01_EXTENSION_PIN")
    bundle = report["algorithm_comparisons"][algo]["bundle_verification"]
    pin = bundle["shard_verifications"].get(shard)
    dataset = report["dataset"]
    if (pin is None or bundle.get("physical_shards_verified") is not True
            or any(case.get(field) != pin[field] for field in ("experiment_run_id", "success_marker_sha256"))
            or any(case.get(field) != dataset[field] for field in ("dataset_id", "collection_id"))
            or str(case.get("gold_snapshot_id")) != str(dataset["gold_snapshot_id"])):
        raise ValueError("DEMO_MLQ01_EXTENSION_PIN")
    import_pin = case.get("import_receipt")
    if (not isinstance(import_pin, dict) or not isinstance(import_pin.get("path"), str)
            or not isinstance(import_pin.get("sha256_lf"), str)):
        raise ValueError("DEMO_MLQ01_EXTENSION_IMPORT")
    model_version = report["algorithm_config_pins"][algo]["model_config_version"]
    fingerprint = _extension_bundle_fingerprint(root, case, pin, model_version)
    receipt = _pinned_official_json(root, import_pin)
    if receipt.get("result_bundle_sha256") != fingerprint:
        raise ValueError("DEMO_MLQ01_EXTENSION_BUNDLE_FINGERPRINT")
    scope = receipt.get("collection_scope")
    counts = receipt.get("readback")
    namespace = receipt.get("namespace")
    if (not isinstance(namespace, str) or not re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", namespace)
            or receipt.get("experiment_status") != "CANDIDATE"
            or any(receipt.get(field) != case[field] for field in ("experiment_run_id", "dataset_id", "algorithm_name"))
            or not isinstance(scope, dict) or scope.get("scope") != "WHOLE_WINDOW_SHARD"
            or scope.get("collection_id") != dataset["collection_id"] or scope.get("shard_id") != shard
            or not isinstance(counts, dict)
            or type(counts.get("experiment_run")) is not int or counts["experiment_run"] != 1
            or type(counts.get("sequence_membership")) is not int
            or counts["sequence_membership"] != pin["membership_row_count"]
            or type(counts.get("sequence_summary")) is not int
            or counts["sequence_summary"] != pin["summary_row_count"]):
        raise ValueError("DEMO_MLQ01_EXTENSION_IMPORT")
    return {"experiment_run_id": case["experiment_run_id"], "dataset_id": case["dataset_id"],
            "collection_id": case["collection_id"], "gold_snapshot_id": str(dataset["gold_snapshot_id"]),
            "algorithm_name": algo, "shard_id": shard, "namespace": namespace,
            "success_marker_sha256": pin["success_marker_sha256"], "import_receipt_sha256_lf": import_pin["sha256_lf"],
            "result_bundle_sha256": fingerprint,
            "snapshots": {field: _normalize_int64_snapshot(receipt.get(field), field) for field in ML_SNAPSHOT_FIELDS}}


def _adaptive_cohort_sha256(exp03):
    dataset = exp03["dataset"]
    entries = sorted(exp03["shard_verifications"].items())
    versions = {pin["collection_scope"]["version"] for _, pin in entries}
    pins = [[shard, pin["experiment_run_id"], pin["success_marker_sha256"]] for shard, pin in entries]
    if len(versions) != 1 or any(not re.fullmatch(r"[0-9a-f]{64}", pin[2]) for pin in pins):
        raise ValueError("DEMO_EXP03_ADAPTIVE_COHORT_PINS")
    # Use the same compact JSON identity as MLI-03 and the MLI-04 collector.
    identity = ["mli03-adaptive-cohort-v1", dataset["dataset_id"], next(iter(versions)),
                dataset["collection_id"], "HDBSCAN_ADAPTIVE",
                exp03["model_config_pin"]["model_config_version"], pins]
    return hashlib.sha256(json.dumps(identity, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()


def _validate_official_evidence(root, manifest, release):
    refs = manifest["official_evidence"]
    cohort = release["collections"]["REPRODUCTION"]
    gold = release["gold_pin"]["gold_snapshot_id"]
    required = {
        "EXP-02": "EXP-02_REPRODUCTION_REPLAY_FOR_EXP04",
        "EXP-03": "EXP-03_OFFICIAL_REPRODUCTION",
        "EXP-04": "EXP-04_OFFICIAL_REPRODUCTION",
    }
    if set(refs) - (required.keys() | {"EXP-05", "MLI-04", "MLQ-01"}):
        raise ValueError("DEMO_OFFICIAL_UNEXPECTED_TASK")
    evidence, verified, expected = {}, [], {}
    for task, name in required.items():
        ref = refs[task]
        if ref["path"] != f"docs/evidence/{name}.json":
            raise ValueError("DEMO_OFFICIAL_SOURCE_MISMATCH")
        report = _pinned_official_json(root, ref)
        identity = report["dataset"]
        if (report["task_id"] != task or report["status"] != "PASSED" or
                report["evaluation_scope"] != "OFFICIAL_RESEARCH" or report["scientific_approval"] is not False
                or identity["dataset_split"] != "REPRODUCTION" or identity["dataset_status"] != "EXPORTED"
                or any(identity[k] != cohort[k] for k in REQUIRED_DAT03_COLLECTION_FIELDS)
                or str(identity["gold_snapshot_id"]) != gold):
            raise ValueError("DEMO_OFFICIAL_RESEARCH_IDENTITY")
        evidence[task] = report
        verified.append({"task_id": task, "status": "VERIFIED_OFFICIAL_RESEARCH",
                         "dataset_id": cohort["dataset_id"], "sha256_lf": ref["sha256_lf"]})
    exp04 = evidence["EXP-04"]
    if exp04.get("frozen_rule_published") is not True:
        raise ValueError("DEMO_OFFICIAL_FREEZE_NOT_PUBLISHED")
    for task in ("EXP-02", "EXP-03"):
        cross = exp04[task.lower().replace("-", "") + "_cross_verification"]
        if cross.get("all_checks_passed") is not True or cross["report_sha256"] != refs[task]["sha256_lf"]:
            raise ValueError("DEMO_OFFICIAL_CROSS_VERIFY_MISMATCH")
        shards = evidence[task]["shard_verifications"]
        if task == "EXP-03":
            shards = {"HDBSCAN_ADAPTIVE": shards}
        for algo, entries in shards.items():
            if len(entries) != cohort["shard_count"]:
                raise ValueError("DEMO_OFFICIAL_SHARD_COVERAGE")
            for shard, entry in entries.items():
                rid = entry["experiment_run_id"]
                if rid in expected:
                    raise ValueError("DEMO_OFFICIAL_DUPLICATE_RUN")
                expected[rid] = (algo, shard, entry)
    if {info[0] for info in expected.values()} != REQUIRED_ML_ALGORITHMS:
        raise ValueError("DEMO_OFFICIAL_ALGORITHM_COVERAGE")

    exp05 = None
    if "EXP-05" in refs:
        # EXP-05 is a research artifact. MLQ-01 still needs its own pinned import receipt.
        exp05_pin = refs["EXP-05"]
        if exp05_pin["path"] != "docs/evidence/EXP-05_EXTENSION_EVALUATION.json":
            raise ValueError("DEMO_OFFICIAL_SOURCE_MISMATCH")
        exp05 = _pinned_official_json(root, exp05_pin)
        extension = exp05["dataset"]
        release_extension = release["collections"]["EXTENSION"]
        freeze = exp05["frozen_rule_verification"]
        if (exp05["task_id"] != "EXP-05" or exp05["status"] != "PASSED"
                or exp05["evaluation_scope"] != "OFFICIAL_EXTENSION_OUT_OF_PERIOD"
                or exp05["scientific_approval"] is not False
                or extension["dataset_split"] != "EXTENSION" or extension["dataset_status"] != "EXPORTED"
                or any(extension[k] != release_extension[k] for k in REQUIRED_DAT03_COLLECTION_FIELDS)
                or str(extension["gold_snapshot_id"]) != gold
                or extension["gold_publication_id"] != release["gold_pin"]["gold_publication_id"]
                or freeze.get("gate_status") != "PASSED"
                or freeze.get("exp04_report_normalized_lf_sha256") != refs["EXP-04"]["sha256_lf"]
                or freeze.get("parameter_tuning_applied_on_extension") is not False):
            raise ValueError("DEMO_OFFICIAL_EXTENSION_IDENTITY")
        verified.append({"task_id": "EXP-05", "status": "VERIFIED_OFFICIAL_RESEARCH",
                         "dataset_id": extension["dataset_id"], "sha256_lf": exp05_pin["sha256_lf"]})

    pending = []
    sql_runs = {}
    if "MLI-04" not in refs:
        pending.append({"task_id": "MLI-04", "reason": "Chua co official SQL parity tren exact snapshots"})
    else:
        report = _pinned_official_json(root, refs["MLI-04"])
        if (report.get("schema_version") != "mli04-readback-v1" or report.get("task_id") != "MLI-04"
                or report.get("status") != "VERIFIED" or report.get("missing_run_ids") != []
                or report.get("scientific_approval") is not False
                or report.get("dataset_id") != cohort["dataset_id"] or report.get("collection_id") != cohort["collection_id"]
                or report.get("exp04_sha256_lf") != refs["EXP-04"]["sha256_lf"]):
            raise ValueError("DEMO_MLI04_NOT_VERIFIED")
        adaptive_digest = _adaptive_cohort_sha256(evidence["EXP-03"])
        for row in report["runs"]:
            rid = row["experiment_run_id"]
            if rid in sql_runs or rid not in expected:
                raise ValueError("DEMO_MLI04_RUN_MISMATCH")
            algo, shard, pin = expected[rid]
            counts = row["actual_counts"]
            if ((row["algorithm_name"], row["shard_id"], row["success_marker_sha256"]) !=
                    (algo, shard, pin["success_marker_sha256"])
                    or row["experiment_status"] not in {"CANDIDATE", "APPROVED"}
                    or counts != row["expected_counts"] or counts["membership_rows"] != pin["membership_row_count"]
                    or counts["summary_rows"] != pin["summary_row_count"]
                    or len(set(row["query_ids"])) != 4 or not all(row["query_ids"])):
                raise ValueError("DEMO_MLI04_RUN_MISMATCH")
            if algo == "HDBSCAN_ADAPTIVE" and row.get("adaptive_cohort_sha256") != adaptive_digest:
                raise ValueError("DEMO_MLI04_ADAPTIVE_COHORT_MISMATCH")
            sql_runs[rid] = {k: _normalize_int64_snapshot(row["snapshots"][k], k) for k in ML_SNAPSHOT_FIELDS}
        if sql_runs.keys() != expected.keys():
            raise ValueError("DEMO_MLI04_RUN_COVERAGE")
        verified.append({"task_id": "MLI-04", "status": "VERIFIED", "run_ids": sorted(sql_runs),
                         "adaptive_cohort_sha256": adaptive_digest})

    if "MLQ-01" not in refs:
        pending.append({"task_id": "MLQ-01", "reason": "Chua co receipt nghiem thu ML E2E"})
    else:
        report = _pinned_official_json(root, refs["MLQ-01"])
        if (not sql_runs or report.get("task_id") != "MLQ-01" or report.get("status") != "PASSED"
                or report.get("scientific_approval") is not False
                or report.get("mli04_sha256_lf") != refs["MLI-04"]["sha256_lf"]
                or report.get("dataset_id") != cohort["dataset_id"] or str(report.get("gold_snapshot_id")) != gold
                or any(report.get("checks", {}).get(key) is not True for key in (
                    "gold_to_report_lineage", "count_parity", "reject_without_candidate",
                    "rerun_no_duplicate", "extension_rule_preserved"))):
            raise ValueError("DEMO_MLQ01_NOT_VERIFIED")
        runs = report["runs"]
        if not isinstance(runs, dict) or not runs or any(
            rid not in sql_runs or not isinstance(snapshots, dict)
            or {k: _normalize_int64_snapshot(snapshots[k], k) for k in sql_runs[rid]} != sql_runs[rid]
            for rid, snapshots in runs.items()
        ):
            raise ValueError("DEMO_MLQ01_RUN_MISMATCH")
        algorithms = {expected[rid][0] for rid in runs}
        if algorithms != REQUIRED_ML_ALGORITHMS:
            raise ValueError("DEMO_MLQ01_ALGORITHM_COVERAGE")
        if exp05 is None:
            raise ValueError("DEMO_MLQ01_EXTENSION_PIN")
        extension_lineage = _validate_mlq_extension(root, report.get("extension"), exp05, exp05_pin["sha256_lf"])
        verified.append({"task_id": "MLQ-01", "status": "VERIFIED", "run_ids": sorted(runs),
                         "algorithms": sorted(algorithms), "extension": extension_lineage})
    return verified, pending


def run_preflight(
    manifest_path: Path = DEFAULT_MANIFEST,
    root: Path = ROOT,
    local_receipt_path: Path | None = None,
    expected_receipt_sha256: str | None = None,
) -> dict[str, Any]:
    """Run all offline DEMO-01 manifest, pin, fallback, receipt, and boundary checks."""
    raw_manifest = manifest_path.read_text(encoding="utf-8")
    _check_secret_hygiene(raw_manifest, manifest_path.name)
    manifest = _load_json_strict(manifest_path, error_code="DEMO_INVALID_MANIFEST_JSON")
    _validate_boundary_and_policy(manifest)

    for rel_path in manifest.get("required_repo_files") or []:
        _resolve_repo_path(root, rel_path)

    ev = manifest.get("representative_evidence") or {}
    dat03_summary, validated_release_pin = _check_dat03_release_pin(root, ev["official_paired_release_v4"])
    verified_runs = [
        _check_qa01_pin(root, ev["daily_operational_v2"]),
        _check_orc07_pin(root, ev["bounded_real_etl"]),
        _check_runtime_qa_pin(root, "idempotency_rerun_qa", ev["idempotency_rerun_qa"]),
        _check_runtime_qa_pin(root, "historical_backfill_qa", ev["historical_backfill_qa"]),
        _check_runtime_qa_pin(root, "failure_recovery_qa", ev["failure_recovery_qa"]),
        dat03_summary,
    ]
    verified_runs.extend(_check_pilot_and_ml_pins(root, ev))
    fallback_modes = _check_fallback_modes(manifest)

    pending_gates = manifest["pending_official_gates"]
    ready = False
    if manifest["schema_version"] == "demo-package-v2":
        official, pending_gates = _validate_official_evidence(root, manifest, validated_release_pin)
        verified_runs.extend(official)
        ready = not pending_gates
        if (manifest["pending_official_gates"] != pending_gates
                or manifest["official_release_ready"] is not ready
                or manifest["package_status"] != ("PASS_OFFICIAL_READY" if ready else PREPARED_PENDING_STATUS)):
            raise ValueError("DEMO_OFFICIAL_READINESS_MISMATCH")
    receipt_info = _verify_local_receipt(
        local_receipt_path=local_receipt_path,
        release_pin=validated_release_pin,
        expected_receipt_sha256=expected_receipt_sha256,
    )
    report = {
        "schema_version": manifest["schema_version"].replace("demo-package-", "demo-preflight-report-"),
        "task_id": "DEMO-01",
        "overall_status": "PASS_OFFICIAL_READY" if ready else PREPARED_PENDING_STATUS,
        "demo_package_prepared": True,
        "official_release_ready": ready,
        "depends_on_power_bi": False,
        "stage_40_year_rerun_required": False,
        "verified_evidence_count": len(verified_runs),
        "verified_runs": verified_runs,
        "fallback_modes_verified": fallback_modes,
        "pending_official_gates": pending_gates,
        "local_research_receipt": receipt_info,
    }
    _check_secret_hygiene(json.dumps(report, ensure_ascii=False), "preflight_report")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=CURRENT_MANIFEST, help="Path to demo package manifest JSON.")
    parser.add_argument("--local-receipt", type=Path, default=None, help="Optional path to external DAT-03 v4 team receipt JSON.")
    parser.add_argument("--expected-receipt-sha256", default=None, help="Optional trusted SHA-256 hex digest for --local-receipt.")
    parser.add_argument("--output-report", type=Path, default=None, help="Optional path to write preflight JSON summary.")
    parser.add_argument("--json", action="store_true", help="Print JSON summary to stdout.")
    args = parser.parse_args()

    try:
        report = run_preflight(
            manifest_path=args.manifest,
            root=ROOT,
            local_receipt_path=args.local_receipt,
            expected_receipt_sha256=args.expected_receipt_sha256,
        )
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
