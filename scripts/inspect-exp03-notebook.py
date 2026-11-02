#!/usr/bin/env python3
"""EXP-03: Execute the HDBSCAN adaptive notebook in a fresh isolated environment; never edit tracked outputs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time

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
            inspection_passed = (
                report_data.get("local_gate", {}).get("inspection", {}).get("fresh_kernel_acceptance") is True
                and report_data.get("local_gate", {}).get("inspection", {}).get("colab_migration_allowed") is True
            )

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
            if not inspection_passed:
                failure_reasons.append("EXP02_REPORT_INSPECTION_NOT_PASSED")

            report_valid = bool(
                task_id_matches
                and status_valid
                and pilot_id_matches
                and feature_sha_matches
                and handoff_sha_in_report
                and inspection_passed
            )
        except Exception as exc:
            failure_reasons.append(f"EXP02_REPORT_JSON_CORRUPT: {exc}")

    # 2. Validate physical handoff archive if present or required
    handoff_override = env_override.get("EXP02_HANDOFF_ARCHIVE", "").strip()
    if handoff_override:
        handoff_file = Path(handoff_override).resolve()
    else:
        handoff_file = root / EXP02_BASELINE_LOCAL_GATE_INFO["handoff_archive"]

    handoff_exists = handoff_file.is_file()
    handoff_sha_matches = False
    expected_handoff_sha = EXP02_BASELINE_LOCAL_GATE_INFO["handoff_archive_sha256"]

    require_physical_archive = env_override.get("EXP02_REQUIRE_PHYSICAL_HANDOFF_ARCHIVE", "").lower() in ("true", "1", "yes")
    if handoff_exists:
        actual_sha = hashlib.sha256(handoff_file.read_bytes()).hexdigest()
        handoff_sha_matches = (actual_sha == expected_handoff_sha)
        if not handoff_sha_matches:
            failure_reasons.append("EXP02_HANDOFF_ARCHIVE_SHA_MISMATCH")
    elif require_physical_archive:
        failure_reasons.append("EXP02_HANDOFF_ARCHIVE_MISSING")

    prerequisite_verified = bool(
        has_evidence_doc
        and has_task_doc
        and report_valid
        and (not handoff_exists or handoff_sha_matches)
        and (not require_physical_archive or handoff_exists)
        and not failure_reasons
    )

    return {
        "status": EXP02_BASELINE_LOCAL_GATE_INFO["status"] if prerequisite_verified else "EXP02_BASELINE_GATE_FAILED",
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


def find_latest_experiment_run(output_root: Path, target_run_id: str = None, expected_pilot_id: str = None) -> dict:
    if target_run_id:
        target_dir = output_root / target_run_id
        sum_p = target_dir / "exp03_run_summary.json"
        if sum_p.is_file():
            try:
                data = json.loads(sum_p.read_text(encoding="utf-8"))
                if data.get("experiment_run_id") == target_run_id:
                    if expected_pilot_id and data.get("pilot_id") != expected_pilot_id:
                        return {}
                    return data
            except Exception:
                return {}
        manifest_p = target_dir / "pilot_result_manifest.json"
        if manifest_p.is_file():
            try:
                m_data = json.loads(manifest_p.read_text(encoding="utf-8"))
                if m_data.get("pilot_run_id") == target_run_id:
                    return {
                        "pilot_id": m_data.get("source_dataset_id"),
                        "experiment_run_id": m_data.get("pilot_run_id"),
                        "output_dir": str(target_dir),
                    }
            except Exception:
                return {}
        return {}

    # When target_run_id is not specified, resolve only verified runs matching exact run ID naming and pins
    candidates = []
    for summary_path in output_root.glob("**/exp03_run_summary.json"):
        try:
            data = json.loads(summary_path.read_text(encoding="utf-8"))
            run_id = data.get("experiment_run_id")
            if not run_id or not isinstance(run_id, str) or not run_id.startswith("pilot_run_"):
                continue
            if summary_path.parent.name != run_id:
                continue
            if expected_pilot_id and data.get("pilot_id") != expected_pilot_id:
                continue
            if not (summary_path.parent / "pilot_result_manifest.json").is_file():
                continue
            candidates.append((summary_path.stat().st_mtime, data))
        except Exception:
            continue

    if candidates:
        candidates.sort(key=lambda x: x[0], reverse=True)
        return candidates[0][1]

    manifest_candidates = []
    for manifest_path in output_root.glob("**/pilot_result_manifest.json"):
        try:
            m_data = json.loads(manifest_path.read_text(encoding="utf-8"))
            run_id = m_data.get("pilot_run_id")
            if not run_id or manifest_path.parent.name != run_id:
                continue
            if expected_pilot_id and m_data.get("source_dataset_id") != expected_pilot_id:
                continue
            manifest_candidates.append((manifest_path.stat().st_mtime, {
                "pilot_id": m_data.get("source_dataset_id"),
                "experiment_run_id": run_id,
                "output_dir": str(manifest_path.parent),
            }))
        except Exception:
            continue

    if manifest_candidates:
        manifest_candidates.sort(key=lambda x: x[0], reverse=True)
        return manifest_candidates[0][1]

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


def run_notebook_cells(notebook_path: Path, env_override: dict) -> dict:
    source_bytes = notebook_path.read_bytes()
    source_sha = hashlib.sha256(source_bytes).hexdigest()
    data = json.loads(source_bytes.decode("utf-8"))

    # Ensure no committed execution counts or outputs in tracked source
    for cell in data.get("cells", []):
        if cell.get("cell_type") == "code":
            if cell.get("outputs") or cell.get("execution_count") is not None:
                raise ValueError("EXP03_SOURCE_NOTEBOOK_MUST_BE_CLEARED")

    code_cells = [cell for cell in data.get("cells", []) if cell.get("cell_type") == "code"]

    # Validate pilot bundle path upfront: fail loudly if invalid or missing when required
    bundle_arg = env_override.get("PILOT_BUNDLE", "").strip()
    allow_fixture = env_override.get("EXP03_ALLOW_FIXTURE_FALLBACK", "").lower() in ("true", "1", "yes")

    if bundle_arg:
        pilot_dir = Path(bundle_arg).resolve()
        if not pilot_dir.is_dir():
            raise FileNotFoundError(f"PILOT_BUNDLE_NOT_FOUND: directory does not exist: {pilot_dir}")
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
    target_run_id = env_override.get("EXP03_RUN_ID")
    expected_pilot_id = env_override.get("EXP03_EXPECTED_PILOT_ID")
    run_summary = find_latest_experiment_run(output_root, target_run_id=target_run_id, expected_pilot_id=expected_pilot_id)
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
