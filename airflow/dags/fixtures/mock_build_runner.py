"""Mock external runner script for testing MLI-02 dataset build protocol."""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys

DAGS_DIR = Path(__file__).resolve().parents[1]
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

import ml_dataset_runtime as runtime


def main() -> None:
    if len(sys.argv) < 3:
        sys.exit(1)

    step_name = sys.argv[1]
    request_file = Path(sys.argv[2])
    request = json.loads(request_file.read_text(encoding="utf-8"))
    context = request["context"]
    staging_root = Path(context["staging_root"])
    dataset_id = context["dataset_id"]
    run_id = context["run_id"]
    snapshot_id = context["gold_publication"]["gold_snapshot_id"]
    attempt = request["attempt"]
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    report_path = staging_root / f"report_{step_name}.json"

    if step_name == "wait_gold_published":
        report = {
            "step": step_name,
            "status": "VERIFIED_PUBLISHED",
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "gold_publication_id": context["gold_publication"]["gold_publication_id"],
            "attempt": attempt,
            "published": True,
            "verified_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_01_wait_gold_published.json", report)
    elif step_name == "resolve_gold_snapshot":
        report = {
            "step": step_name,
            "status": "SNAPSHOT_PINNED",
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "registry_snapshot_id": f"reg_gold_{snapshot_id}",
            "attempt": attempt,
            "identity_payload_sha256": context["identity_payload_sha256"],
            "dataset_status": "BUILDING",
            "pinned_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_02_resolve_gold_snapshot.json", report)
    elif step_name == "audit_gold_input":
        report = {
            "step": step_name,
            "status": "AUDIT_PASSED",
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "attempt": attempt,
            "counts": {
                "input_event_count": 528,
                "eligible_event_count": 176,
                "excluded_event_count": 352,
            },
            "audited_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_03_audit_gold_input.json", report)
    elif step_name == "resolve_mc":
        report = {
            "step": step_name,
            "status": "MC_RESOLVED",
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "attempt": attempt,
            "mc_value": 0.6,
            "mc_method": "MAXIMUM_CURVATURE",
            "mc_method_version": "maximum-curvature-v1",
            "mc_config_json": "{}",
            "mc_config_sha256": "f5d0a12f8a7bc8458375f0d8876005f2548ba0bce78f380005fa95192f4168ff",
            "resolved_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_04_resolve_mc.json", report)
    elif step_name == "select_mainshocks":
        report = {
            "step": step_name,
            "status": "MAINSHOCKS_SELECTED",
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "mainshock_snapshot_id": f"main_{snapshot_id}_v1",
            "attempt": attempt,
            "mainshock_count": 2,
            "mainshock_ids": ["evt_001", "evt_002"],
            "selected_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_05_select_mainshocks.json", report)
    elif step_name == "build_candidate_windows":
        report = {
            "step": step_name,
            "status": "WINDOWS_BUILT",
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "candidate_snapshot_id": f"cand_{snapshot_id}_v1",
            "attempt": attempt,
            "candidate_row_count": 142,
            "built_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_06_build_candidate_windows.json", report)
    elif step_name == "build_spacetime_features":
        report = {
            "step": step_name,
            "status": "FEATURES_BUILT",
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "attempt": attempt,
            "feature_row_count": 142,
            "feature_schema_sha256": runtime.sha256_hex("features_schema_v1"),
            "built_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_07_build_spacetime_features.json", report)
    elif step_name == "validate_feature_snapshot":
        report = {
            "step": step_name,
            "status": "FEATURE_SNAPSHOT_VALIDATED",
            "quality_gate_status": "PASSED",
            "invalid_feature_count": 0,
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "attempt": attempt,
            "candidate_row_count": 142,
            "feature_row_count": 142,
            "validated_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_08_validate_feature_snapshot.json", report)
    elif step_name == "export_parquet":
        attempt_dir = staging_root / "export_attempts" / f"attempt_{attempt}"
        if attempt_dir.exists() and any(attempt_dir.iterdir()):
            sys.exit(2)
        attempt_dir.mkdir(parents=True, exist_ok=True)
        part_path = attempt_dir / "features-part-00000.parquet"
        sel_step = runtime._read_step(staging_root / "step_05_select_mainshocks.json")
        mainshocks = [{"mainshock_event_id": mid} for mid in sel_step.get("mainshock_ids", ["evt_001", "evt_002"])]
        runtime._materialize_features_parquet(part_path, context, row_count=142, mainshocks=mainshocks)
        part_sha = hashlib.sha256(part_path.read_bytes()).hexdigest()
        checksums_content = f"{part_sha}  features-part-00000.parquet\n"
        (attempt_dir / "checksums.sha256").write_text(checksums_content, encoding="utf-8")
        report = {
            "step": step_name,
            "status": "PARQUET_EXPORTED",
            "run_id": run_id,
            "dataset_id": dataset_id,
            "gold_snapshot_id": snapshot_id,
            "attempt": attempt,
            "attempt_dir": str(attempt_dir),
            "exported_at_utc": now_iso,
        }
        runtime._atomic_json(staging_root / "step_09_export_parquet.json", report)
    elif step_name == "mark_dataset_exported":
        step_export_file = staging_root / "step_09_export_parquet.json"
        if step_export_file.is_file():
            step_export = runtime._read_step(step_export_file)
            attempt_dir = Path(step_export.get("attempt_dir", staging_root / "mock_attempt"))
        else:
            attempt_dir = staging_root / "mock_attempt"
        report = runtime.promote_and_publish_dataset(context, attempt_dir, attempt, dict(os.environ))
        runtime._atomic_json(staging_root / "step_10_mark_dataset_exported.json", report)
    else:
        sys.exit(3)

    runtime._atomic_json(report_path, report)


if __name__ == "__main__":
    main()
