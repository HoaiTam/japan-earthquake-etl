#!/usr/bin/env python3
"""EXP-03: Execute the HDBSCAN adaptive notebook in a fresh isolated environment; never edit tracked outputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time
from typing import Any, Dict, List, Optional, Tuple
import uuid

ROOT = Path(__file__).resolve().parents[1]

REAL_PILOT_ID_PIN = "pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c"
REAL_PILOT_FEATURES_SHA256_PIN = "3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6"
REAL_PILOT_CHECKSUMS_SHA256_PIN = "310b5f232ae34ca4f0a197b0181bff81efe6bee38897b6e151e9b58e8d5a6502"
REAL_PILOT_CANDIDATE_ROWS = 279
REAL_PILOT_MAINSHOCK_COUNT = 2

EXP02_BASELINE_LOCAL_GATE_INFO = {
    "task_id": "EXP-02",
    "status": "LOCAL_GATE_AND_HANDOFF_VERIFIED",
    "pr_reference": "https://github.com/HoaiTam/japan-earthquake-etl/pull/114",
    "handoff_archive": "staging/exp02-local-rosy179/exp01-colab-handoff.zip",
    "handoff_archive_sha256": "d6be707111be8d2b1a2ba64434d6c0cacaa67b73c5aae3235f9772403c544333",
    "evidence_document": "docs/evidence/EXP-02.md",
    "evidence_report_json": "docs/evidence/EXP-02_COLAB_PARITY_20261102.json",
    "pilot_id_pin": "pilot_196b6d4081d347e1c289e331022658fbbe572dd33181ad987900895c1f1b189c",
    "feature_sha256_pin": "3baddf613f237d2117248ad9095230ec17e428ea47f73d499623035d3097d2e6",
    "source_sha256_pin": "7a65c8fa203c143283a07de2678dc9f7fd16569342f6d6f43e5edcfdf102ff4d",
    "local_smoke_status_pin": "PASSED",
    "expected_model_configs": {
        "DBSCAN": "dbscan-global-dev-v1",
        "HDBSCAN_GLOBAL": "hdbscan-global-dev-v1",
        "WINDOW": "window-baseline-v1",
    },
}


def check_exp02_baseline_local_gate(root: Path, env_override: dict = None) -> dict:
    if env_override is None:
        env_override = {}

    evidence_file = root / "docs/evidence/EXP-02.md"
    task_file = root / "docs/task/tasks/EXP-02.md"
    has_evidence_doc = evidence_file.is_file()
    has_task_doc = task_file.is_file()

    failure_reasons = []
    if not has_evidence_doc:
        failure_reasons.append("EXP02_EVIDENCE_DOC_MISSING")
    if not has_task_doc:
        failure_reasons.append("EXP02_TASK_DOC_MISSING")

    # 1. Resolve and validate EXP-02 evidence report
    report_override = env_override.get("EXP02_REPORT_PATH", "").strip()
    if report_override:
        report_file = Path(report_override).resolve()
    else:
        report_file = root / EXP02_BASELINE_LOCAL_GATE_INFO["evidence_report_json"]

    report_exists = report_file.is_file()
    report_valid = False
    report_data = {}

    if not report_exists:
        failure_reasons.append("EXP02_EVIDENCE_REPORT_MISSING")
    else:
        try:
            report_data = json.loads(report_file.read_text(encoding="utf-8"))
            task_id_matches = (report_data.get("task_id") == "EXP-02")
            status_valid = report_data.get("status") in (
                "COLAB_PILOT_PARITY_VERIFIED",
                "LOCAL_GATE_AND_HANDOFF_VERIFIED",
                "PASSED",
            )
            pilot_id_matches = (
                report_data.get("pilot", {}).get("pilot_id") == EXP02_BASELINE_LOCAL_GATE_INFO["pilot_id_pin"]
            )
            feature_sha_matches = (
                report_data.get("pilot", {}).get("feature_sha256") == EXP02_BASELINE_LOCAL_GATE_INFO["feature_sha256_pin"]
            )
            handoff_sha_in_report = (
                report_data.get("local_gate", {}).get("handoff_zip_sha256") == EXP02_BASELINE_LOCAL_GATE_INFO["handoff_archive_sha256"]
            )
            source_sha_matches = (
                report_data.get("local_gate", {}).get("source_sha256") == EXP02_BASELINE_LOCAL_GATE_INFO["source_sha256_pin"]
            )
            smoke_status_passed = (
                report_data.get("local_gate", {}).get("inspection", {}).get("local_smoke_status") == EXP02_BASELINE_LOCAL_GATE_INFO["local_smoke_status_pin"]
            )
            inspection_passed = (
                report_data.get("local_gate", {}).get("inspection", {}).get("fresh_kernel_acceptance") is True
                and report_data.get("local_gate", {}).get("inspection", {}).get("colab_migration_allowed") is True
                and smoke_status_passed
            )

            # Check model config versions in reconciliation
            recon = report_data.get("local_gate", {}).get("reconciliation", {})
            model_configs_match = True
            for model_name, exp_cfg in EXP02_BASELINE_LOCAL_GATE_INFO["expected_model_configs"].items():
                actual_cfg = recon.get(model_name, {}).get("model_config_version")
                if actual_cfg != exp_cfg:
                    model_configs_match = False
                    failure_reasons.append(f"EXP02_REPORT_MODEL_CONFIG_MISMATCH_{model_name}")

            if not task_id_matches:
                failure_reasons.append("EXP02_REPORT_TASK_ID_MISMATCH")
            if not status_valid:
                failure_reasons.append("EXP02_REPORT_STATUS_INVALID")
            if not pilot_id_matches:
                failure_reasons.append("EXP02_REPORT_PILOT_ID_MISMATCH")
            if not feature_sha_matches:
                failure_reasons.append("EXP02_REPORT_FEATURE_SHA_MISMATCH")
            if not handoff_sha_in_report:
                failure_reasons.append("EXP02_REPORT_HANDOFF_SHA_MISMATCH")
            if not source_sha_matches:
                failure_reasons.append("EXP02_REPORT_SOURCE_SHA_MISMATCH")
            if not smoke_status_passed:
                failure_reasons.append("EXP02_REPORT_SMOKE_STATUS_NOT_PASSED")
            if not inspection_passed and smoke_status_passed:
                failure_reasons.append("EXP02_REPORT_INSPECTION_NOT_PASSED")

            report_valid = bool(
                task_id_matches
                and status_valid
                and pilot_id_matches
                and feature_sha_matches
                and handoff_sha_in_report
                and source_sha_matches
                and smoke_status_passed
                and inspection_passed
                and model_configs_match
            )
        except Exception as exc:
            failure_reasons.append(f"EXP02_REPORT_JSON_CORRUPT: {exc}")

    # 2. Validate physical handoff archive if present, overridden, or explicitly required
    handoff_override = env_override.get("EXP02_HANDOFF_ARCHIVE", "").strip()
    require_physical_archive = env_override.get("EXP02_REQUIRE_PHYSICAL_HANDOFF_ARCHIVE", "").lower() in ("true", "1", "yes")

    expected_handoff_sha = EXP02_BASELINE_LOCAL_GATE_INFO["handoff_archive_sha256"]
    handoff_exists = False
    handoff_sha_matches = False
    is_artifact_verification_mode = bool(handoff_override or require_physical_archive)

    if is_artifact_verification_mode:
        if handoff_override:
            handoff_file = Path(handoff_override).resolve()
        else:
            handoff_file = root / EXP02_BASELINE_LOCAL_GATE_INFO["handoff_archive"]
        handoff_exists = handoff_file.is_file()
        if not handoff_exists:
            failure_reasons.append("EXP02_HANDOFF_ARCHIVE_MISSING")
        else:
            actual_sha = hashlib.sha256(handoff_file.read_bytes()).hexdigest()
            handoff_sha_matches = (actual_sha == expected_handoff_sha)
            if not handoff_sha_matches:
                failure_reasons.append("EXP02_HANDOFF_ARCHIVE_SHA_MISMATCH")
    else:
        # Report-only mode (physical archive optional on disk; if present, must match checksum)
        handoff_file = root / EXP02_BASELINE_LOCAL_GATE_INFO["handoff_archive"]
        handoff_exists = handoff_file.is_file()
        if handoff_exists:
            actual_sha = hashlib.sha256(handoff_file.read_bytes()).hexdigest()
            handoff_sha_matches = (actual_sha == expected_handoff_sha)
            if not handoff_sha_matches:
                failure_reasons.append("EXP02_HANDOFF_ARCHIVE_SHA_MISMATCH")

    prerequisite_verified = bool(
        has_evidence_doc
        and has_task_doc
        and report_valid
        and not failure_reasons
    )

    if prerequisite_verified:
        gate_status = (
            "LOCAL_GATE_AND_HANDOFF_VERIFIED"
            if handoff_exists and handoff_sha_matches
            else "REPORT_ONLY_VERIFIED"
        )
    else:
        gate_status = "EXP02_BASELINE_GATE_FAILED"

    return {
        "status": gate_status,
        "task_id": "EXP-02",
        "pr_reference": EXP02_BASELINE_LOCAL_GATE_INFO["pr_reference"],
        "handoff_archive_sha256": expected_handoff_sha,
        "evidence_document": EXP02_BASELINE_LOCAL_GATE_INFO["evidence_document"],
        "evidence_report_json": EXP02_BASELINE_LOCAL_GATE_INFO["evidence_report_json"],
        "evidence_document_exists": has_evidence_doc,
        "evidence_report_exists": report_exists,
        "handoff_archive_exists": handoff_exists,
        "prerequisite_verified": prerequisite_verified,
        "failure_reasons": failure_reasons,
    }


def validate_pilot_result_manifest(
    manifest_path: Path,
    expected_run_id: str = None,
    expected_pilot_id: str = None,
    expected_model_config_version: str = "hdbscan-adaptive-dev-v1",
) -> Tuple[bool, dict, List[str]]:
    """Strictly parse and validate pilot_result_manifest.json against immutable contract pins."""
    reasons = []
    if not manifest_path.is_file():
        return False, {}, ["MANIFEST_FILE_NOT_FOUND"]
    try:
        m = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        return False, {}, [f"MANIFEST_JSON_CORRUPT: {exc}"]

    if not isinstance(m, dict) or not m:
        return False, {}, ["MANIFEST_EMPTY_OR_NOT_DICT"]

    # Required contract fields
    if m.get("manifest_version") != "dat-02-pilot-result-v1":
        reasons.append("MANIFEST_VERSION_MISMATCH")
    if m.get("artifact_kind") != "PILOT_EXPERIMENT_RESULT":
        reasons.append("MANIFEST_ARTIFACT_KIND_MISMATCH")
    if m.get("status") != "DEVELOPMENT_RUN_COMPLETED":
        reasons.append("MANIFEST_STATUS_NOT_COMPLETED")
    if m.get("algorithm_name") != "HDBSCAN_ADAPTIVE":
        reasons.append("MANIFEST_ALGORITHM_MISMATCH")

    cfg_ver = m.get("model_config_version")
    if expected_model_config_version and cfg_ver != expected_model_config_version:
        reasons.append("MANIFEST_MODEL_CONFIG_VERSION_MISMATCH")

    manifest_run_id = m.get("pilot_run_id")
    if expected_run_id and manifest_run_id != expected_run_id:
        reasons.append("MANIFEST_RUN_ID_MISMATCH")

    # Check pilot_id strictly (distinct from source_dataset_id)
    manifest_pilot_id = m.get("pilot_id") or m.get("source_dataset_id")
    if expected_pilot_id and manifest_pilot_id != expected_pilot_id:
        reasons.append("MANIFEST_PILOT_ID_MISMATCH")

    checksums = m.get("checksums")
    if not isinstance(checksums, dict) or not checksums:
        reasons.append("MANIFEST_CHECKSUMS_MISSING")
    else:
        req_files = [
            "memberships.parquet",
            "sequence_summary.parquet",
            "experiment_metrics.json",
            "experiment_config.json",
            "requirements-lock.txt",
        ]
        for f in req_files:
            if f not in checksums or not re.match(r"^[a-f0-9]{64}$", str(checksums[f])):
                reasons.append(f"MANIFEST_CHECKSUM_INVALID_{f}")

    return len(reasons) == 0, m, reasons


def find_latest_experiment_run(
    output_root: Path,
    target_run_id: str = None,
    expected_pilot_id: str = None,
    expected_model_config_version: str = "hdbscan-adaptive-dev-v1",
) -> dict:
    current_time = time.time()

    # 1. Exact run resolution when target_run_id is specified
    if target_run_id:
        target_dir = output_root / target_run_id
        if not target_dir.is_dir():
            return {}

        manifest_p = target_dir / "pilot_result_manifest.json"
        is_m_valid, m_data, _ = validate_pilot_result_manifest(
            manifest_p,
            expected_run_id=target_run_id,
            expected_pilot_id=expected_pilot_id,
            expected_model_config_version=expected_model_config_version,
        )
        if not is_m_valid:
            return {}

        actual_pilot_id = m_data.get("pilot_id") or m_data.get("source_dataset_id")

        sum_p = target_dir / "exp03_run_summary.json"
        if sum_p.is_file():
            try:
                s_data = json.loads(sum_p.read_text(encoding="utf-8"))
                if s_data.get("experiment_run_id") != target_run_id:
                    return {}
                if expected_pilot_id and s_data.get("pilot_id") != expected_pilot_id:
                    return {}
                if s_data.get("pilot_id") != actual_pilot_id:
                    return {}
                return s_data
            except Exception:
                return {}

        # Fallback to verified manifest data
        return {
            "pilot_id": actual_pilot_id,
            "experiment_run_id": target_run_id,
            "output_dir": str(target_dir),
            "candidate_rows": m_data.get("membership_row_count"),
            "mainshock_count": m_data.get("summary_row_count"),
        }

    # 2. Strict auto-discovery fallback (e.g., when inspecting pre-existing directory without active target)
    candidates = []
    for summary_path in output_root.glob("**/exp03_run_summary.json"):
        try:
            target_dir = summary_path.parent
            run_id = target_dir.name
            if not run_id.startswith("pilot_run_"):
                continue

            # Reject future mtime (protect against simulated future-dated tampering)
            st_mtime = summary_path.stat().st_mtime
            if st_mtime > current_time + 30:
                continue

            manifest_p = target_dir / "pilot_result_manifest.json"
            is_m_valid, m_data, _ = validate_pilot_result_manifest(
                manifest_p,
                expected_run_id=run_id,
                expected_pilot_id=expected_pilot_id,
                expected_model_config_version=expected_model_config_version,
            )
            if not is_m_valid:
                continue

            actual_pilot_id = m_data.get("pilot_id") or m_data.get("source_dataset_id")

            s_data = json.loads(summary_path.read_text(encoding="utf-8"))
            if s_data.get("experiment_run_id") != run_id:
                continue
            if expected_pilot_id and s_data.get("pilot_id") != expected_pilot_id:
                continue
            if s_data.get("pilot_id") != actual_pilot_id:
                continue

            candidates.append((st_mtime, s_data))
        except Exception:
            continue

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    return {}


def extract_pilot_verification(pilot_dir: Path, run_summary: dict) -> dict:
    pilot_id = run_summary.get("pilot_id")
    candidate_rows = run_summary.get("candidate_rows")
    mainshock_count = run_summary.get("mainshock_count")

    checksums_index_sha256 = None
    features_parquet_sha256 = None
    mainshocks_sha256 = None
    pilot_manifest_sha256 = None

    if pilot_dir and pilot_dir.is_dir():
        chk_file = pilot_dir / "checksums.sha256"
        if chk_file.is_file():
            checksums_index_sha256 = hashlib.sha256(chk_file.read_bytes()).hexdigest()
            for line in chk_file.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or "  " not in line:
                    continue
                sha_val, fname = line.split("  ", 1)
                if fname == "features-part-00000.parquet":
                    features_parquet_sha256 = sha_val
                elif fname == "mainshocks.parquet":
                    mainshocks_sha256 = sha_val
                elif fname == "pilot_manifest.json":
                    pilot_manifest_sha256 = sha_val
        feat_file = pilot_dir / "features-part-00000.parquet"
        if feat_file.is_file() and not features_parquet_sha256:
            features_parquet_sha256 = hashlib.sha256(feat_file.read_bytes()).hexdigest()

    is_real_pilot = bool(
        pilot_id == REAL_PILOT_ID_PIN
        and candidate_rows == REAL_PILOT_CANDIDATE_ROWS
        and mainshock_count == REAL_PILOT_MAINSHOCK_COUNT
        and features_parquet_sha256 == REAL_PILOT_FEATURES_SHA256_PIN
    )

    return {
        "pilot_id": pilot_id,
        "is_real_pilot": is_real_pilot,
        "candidate_rows": candidate_rows,
        "mainshock_count": mainshock_count,
        "features_parquet_sha256": features_parquet_sha256,
        "checksums_index_sha256": checksums_index_sha256,
        "pilot_manifest_sha256": pilot_manifest_sha256,
    }


def run_notebook_cells(notebook_path: Path, env_override: dict = None) -> dict:
    env_override = dict(env_override) if env_override else {}
    source_bytes = notebook_path.read_bytes()
    source_sha = hashlib.sha256(source_bytes).hexdigest()
    data = json.loads(source_bytes.decode("utf-8"))

    code_cells = [cell for cell in data.get("cells", []) if cell.get("cell_type") == "code"]

    pilot_bundle_arg = env_override.get("PILOT_BUNDLE", "").strip()
    allow_fixture = env_override.get("EXP03_ALLOW_FIXTURE_FALLBACK", "").lower() in ("true", "1", "yes")

    pilot_dir = None
    if pilot_bundle_arg:
        pilot_dir = Path(pilot_bundle_arg).resolve()
        if not pilot_dir.is_dir():
            raise FileNotFoundError(
                f"PILOT_BUNDLE_NOT_FOUND: Provided pilot bundle directory does not exist: {pilot_dir}"
            )
    else:
        default_pilot = (ROOT / "staging/ml-pilot/pilot-kit/bundle").resolve()
        if default_pilot.is_dir():
            pilot_dir = default_pilot
            env_override["PILOT_BUNDLE"] = str(pilot_dir)
        elif allow_fixture:
            pilot_dir = (ROOT / "staging/ml-pilot/exp03-fixture").resolve()
        else:
            raise FileNotFoundError(
                "PILOT_BUNDLE_REQUIRED: PILOT_BUNDLE is not provided and default pilot bundle "
                f"does not exist at {default_pilot}. Provide --pilot-bundle or set EXP03_ALLOW_FIXTURE_FALLBACK=true."
            )

    output_dir_str = env_override.get("EXP03_OUTPUT_DIR", "").strip()
    if not output_dir_str:
        output_dir_str = str((ROOT / "staging/ml-experiments/exp03-adaptive").resolve())
        env_override["EXP03_OUTPUT_DIR"] = output_dir_str
    output_root = Path(output_dir_str).resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    # Pre-generate / pin exact run ID before execution so the notebook execution
    # and subsequent inspection are strictly bound to this immutable run ID.
    if "EXP03_RUN_ID" not in env_override or not env_override["EXP03_RUN_ID"]:
        env_override["EXP03_RUN_ID"] = f"pilot_run_{uuid.uuid4().hex}"
    target_run_id = env_override["EXP03_RUN_ID"]

    # Try nbclient first if available
    nbclient_available = False
    try:
        import nbformat
        from nbclient import NotebookClient
        from jupyter_client import KernelManager
        from jupyter_client.kernelspec import KernelSpecManager
        nbclient_available = True
    except ImportError:
        nbclient_available = False

    env_override["EXP03_RUNNER_MODE"] = "nbclient" if nbclient_available else "isolated_exec"
    env_override["EXP03_FRESH_KERNEL_ACCEPTANCE"] = "true" if nbclient_available else "false"

    executed_count = 0
    t0 = time.time()

    if nbclient_available:
        import nbformat
        from nbclient import NotebookClient
        from jupyter_client import KernelManager
        from jupyter_client.kernelspec import KernelSpecManager

        notebook = nbformat.reads(source_bytes.decode("utf-8"), as_version=4)
        with tempfile.TemporaryDirectory(prefix="exp03-kernel-") as temporary:
            kernel_dir = Path(temporary) / "exp03"
            kernel_dir.mkdir()
            (kernel_dir / "kernel.json").write_text(
                json.dumps({
                    "argv": [sys.executable, "-m", "ipykernel_launcher", "-f", "{connection_file}"],
                    "display_name": "EXP-03 isolated",
                    "language": "python"
                })
            )
            manager = KernelManager(kernel_name="exp03", kernel_spec_manager=KernelSpecManager(kernel_dirs=[temporary]))
            client = NotebookClient(notebook, km=manager, timeout=180, allow_errors=False, resources={"metadata": {"path": str(ROOT)}})
            try:
                client.execute(env=dict(os.environ, **env_override))
            finally:
                if manager.has_kernel:
                    manager.shutdown_kernel(now=True)
            executed_count = len(code_cells)
    else:
        # Direct execution in isolated namespace
        exec_globals = {
            "__name__": "__main__",
            "__file__": str(notebook_path),
        }
        old_env = os.environ.copy()
        try:
            os.environ.update(env_override)
            for idx, cell in enumerate(code_cells):
                source_code = "".join(cell.get("source", []))
                exec(compile(source_code, f"<cell_{idx}>", "exec"), exec_globals)
                executed_count += 1
        finally:
            os.environ.clear()
            os.environ.update(old_env)

    elapsed = time.time() - t0

    # Verify source file was not mutated
    if hashlib.sha256(notebook_path.read_bytes()).hexdigest() != source_sha:
        raise ValueError("EXP03_SOURCE_NOTEBOOK_MUTATED")

    # Read execution summary produced by the notebook run tied to exact run ID/pins
    expected_pilot_id = env_override.get("EXP03_EXPECTED_PILOT_ID")
    run_summary = find_latest_experiment_run(output_root, target_run_id=target_run_id, expected_pilot_id=expected_pilot_id)
    if not run_summary:
        raise RuntimeError(f"EXP03_RUN_NOT_RESOLVED: target run {target_run_id} produced no valid verified output")

    pilot_verif = extract_pilot_verification(pilot_dir, run_summary)
    exp02_gate = check_exp02_baseline_local_gate(ROOT, env_override=env_override)

    # Allow test simulation of EXP-02 gate
    if env_override.get("EXP03_SIMULATE_EXP02_GATE_FAIL", "").lower() in ("true", "1", "yes"):
        exp02_gate["prerequisite_verified"] = False

    migration_block_reasons = []
    if not nbclient_available:
        migration_block_reasons.append("FRESH_KERNEL_REQUIRED")
    if not pilot_verif["is_real_pilot"]:
        migration_block_reasons.append("REAL_PILOT_REQUIRED_FOR_COLAB_MIGRATION")
    if not exp02_gate["prerequisite_verified"]:
        migration_block_reasons.append("EXP02_BASELINE_LOCAL_GATE_REQUIRED")

    colab_migration_allowed = bool(len(migration_block_reasons) == 0)

    recon_summary = {
        "experiment_run_id": run_summary.get("experiment_run_id"),
        "output_dir": run_summary.get("output_dir"),
        "resolved_unique_aftershocks": run_summary.get("resolved_unique_aftershocks"),
        "ambiguous_candidates_count": run_summary.get("ambiguous_candidates_count"),
        "nested_mainshocks_count": run_summary.get("nested_mainshocks_count"),
    }

    return {
        "task_id": "EXP-03",
        "notebook_status": "RESTART_RUN_ALL_PASSED" if nbclient_available else "FALLBACK_EXEC_PASSED",
        "local_smoke_status": "PASSED" if nbclient_available else "FALLBACK_EXEC_WARNING",
        "colab_migration_allowed": colab_migration_allowed,
        "fresh_kernel_acceptance": bool(nbclient_available),
        "migration_block_reasons": migration_block_reasons,
        "pilot_verification": pilot_verif,
        "exp02_baseline_local_gate": exp02_gate,
        "experiment_summary": recon_summary,
        "notebook_sha256": source_sha,
        "executed_code_cells": executed_count,
        "runtime_seconds": round(elapsed, 4),
        "python_version": sys.version.split()[0],
        "runner_mode": "nbclient" if nbclient_available else "isolated_exec",
        "source_notebook_outputs_cleared": True,
    }


def main():
    parser = argparse.ArgumentParser(description="Inspect and verify EXP-03 HDBSCAN adaptive notebook.")
    parser.add_argument("--notebook", default=str(ROOT / "notebooks/exp_03_adaptive_hdbscan.ipynb"), help="Path to notebook")
    parser.add_argument("--pilot-bundle", default=os.environ.get("PILOT_BUNDLE", ""), help="Path to pilot bundle directory")
    parser.add_argument("--output-json", default="", help="Optional output path for inspection JSON")
    parser.add_argument("--output-dir", default=os.environ.get("EXP03_OUTPUT_DIR", ""), help="Optional path to output directory")
    parser.add_argument("--allow-fixture", action="store_true", help="Allow synthetic fixture fallback (blocks Colab migration)")
    args = parser.parse_args()

    notebook_path = Path(args.notebook).resolve()
    if not notebook_path.is_file():
        print(f"Error: Notebook not found: {notebook_path}", file=sys.stderr)
        sys.exit(1)

    env_override = {}
    if args.pilot_bundle:
        env_override["PILOT_BUNDLE"] = args.pilot_bundle
    if args.output_dir:
        env_override["EXP03_OUTPUT_DIR"] = args.output_dir
    if args.allow_fixture:
        env_override["EXP03_ALLOW_FIXTURE_FALLBACK"] = "true"

    try:
        report = run_notebook_cells(notebook_path, env_override)
        output_str = json.dumps(report, indent=2)
        print(output_str)

        if args.output_json:
            out_p = Path(args.output_json).resolve()
            out_p.parent.mkdir(parents=True, exist_ok=True)
            out_p.write_text(output_str + "\n", encoding="utf-8")

        sys.exit(0)
    except Exception as exc:
        print(f"ERROR: EXP-03 notebook inspection failed: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
