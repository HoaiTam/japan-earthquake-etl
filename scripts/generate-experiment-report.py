#!/usr/bin/env python3
"""MLI-04: Static Experiment Report Generator and Trino View Audit Harness.

Generates reproducible static comparative reports across:
1. WINDOW (Space-time window counting baseline)
2. DBSCAN (Global density-based clustering baseline)
3. HDBSCAN_GLOBAL (Hierarchical density-based clustering)
4. HDBSCAN_ADAPTIVE (Adaptive density-based clustering)

Enforces Core Requirements:
- Publication Gate: Official runs must have status CANDIDATE or APPROVED.
- Strict Row-level Gate: Every row rendered in comparison table must belong to an approved run of the exact dataset.
- Fail-Closed: Missing metadata, cross-dataset rows, foreign runs, or unapproved states cannot produce OFFICIAL_PUBLICATION.
- Schema Compatibility: Seamlessly accepts output from v_algorithm_comparison (SQL view) or EXP-04 pilot format.
- Denominator Integrity: Failed/skipped windows are preserved in total window count.
- Honest NULL Policy: DBCV and Omori parameters are NEVER replaced with 0 if inapplicable/unfitted.
- Metric Zero Integrity: Legitimate 0.0 numeric values are strictly preserved, not coerced to N/A.
- Truthful Parity Status: SQL parity is marked VERIFIED only with verified snapshot evidence, otherwise PENDING/CHƯA NGHIỆM THU.
- Scientific Guardrails: Output explicitly disclaims earthquake prediction and physical causality.
- Strictly NO Accuracy or F1 scores (unlabeled density clustering).
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def get_git_commit(root: Optional[Path] = None) -> str:
    """Retrieve git commit pin if available."""
    try:
        import subprocess
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(root or PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except Exception:
        return "UNKNOWN_COMMIT"


def has_any_key(d: Any, *keys: str) -> bool:
    """Return True if any of the specified keys exist in mapping d, even if value is None."""
    if d is None:
        return False
    for k in keys:
        try:
            if k in d:
                return True
        except TypeError:
            pass
    return False


def get_field(d: Optional[Dict[str, Any]], *keys: str, default: Any = None) -> Any:
    """Extract first non-None key value from dict, strictly preserving falsy 0.0 values."""
    if not d or not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return default


def format_metric(
    val: Any,
    metric_reason: Optional[str] = None,
    fmt: str = ".4f",
    suffix: str = "",
) -> str:
    """Format metrics, strictly preserving honest NULLs and valid 0.0 values."""
    if val is None or val == "None":
        if metric_reason:
            return f"N/A ({metric_reason})"
        return "N/A"
    if isinstance(val, bool):
        return str(val)
    if isinstance(val, (int, float)):
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return f"N/A ({metric_reason})" if metric_reason else "N/A"
        if fmt == ".1f" and suffix == "%":
            if abs(val) <= 1.0 and val != 0.0:
                return f"{val * 100:.1f}%"
            return f"{val:.1f}%"
        return f"{val:{fmt}}{suffix}"
    if isinstance(val, str):
        return val
    return str(val)


def normalize_comparison_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a single comparison row from either v_algorithm_comparison SQL view or EXP-04 pilot format."""
    if not has_any_key(row, "algorithm_name", "Algorithm", "algorithm"):
        raise ValueError(f"Missing required algorithm field ('algorithm_name' or 'algorithm') in row: {row}")
    algo = get_field(row, "algorithm_name", "Algorithm", "algorithm")
    if not algo:
        raise ValueError(f"Missing required algorithm field ('algorithm_name' or 'algorithm') in row: {row}")

    run_id = get_field(row, "experiment_run_id", "Run ID", "run_id")
    dataset_id = get_field(row, "dataset_id")
    config_ver = get_field(row, "model_config_version", "model_config", "config_version")
    code_ver = get_field(row, "code_version")
    seed = get_field(row, "random_seed")
    status = get_field(row, "experiment_status", "status")

    tot_win = get_field(row, "total_windows", "Total Windows", default=0)
    succ_win = get_field(row, "success_windows", "Success Windows", default=0)
    fail_win = get_field(row, "failure_windows", "Failure Windows", default=0)
    skipped_win = get_field(row, "skipped_windows", "Skipped Windows", default=0)
    fail_rate = get_field(row, "failure_rate", "Failure Rate", default=0.0)

    # Distinguish field absence from legitimate NULL (e.g. all windows failed)
    if not has_any_key(row, "raw_aftershock_memberships", "total_aftershocks", "Total Aftershocks"):
        raise ValueError(f"Missing required aftershocks field ('raw_aftershock_memberships' or 'total_aftershocks') in row: {row}")
    tot_after = get_field(row, "raw_aftershock_memberships", "total_aftershocks", "Total Aftershocks")

    mean_noise = get_field(row, "avg_noise_ratio", "mean_noise_ratio", "Mean Noise Ratio")
    mean_dbcv = get_field(row, "avg_dbcv_score", "mean_dbcv_score", "mean_dbcv", "Mean DBCV")
    mean_omori_p = get_field(row, "avg_omori_p", "mean_omori_p", "Mean Omori p")
    omori_win = get_field(row, "omori_fitted_windows", "Omori Fitted Windows")
    mean_jaccard = get_field(row, "mean_jaccard_stability", "Mean Jaccard Stability")
    mean_ari = get_field(row, "mean_ari_stability", "Mean ARI Stability")

    # Distinguish field absence from legitimate NULL runtime
    if not has_any_key(row, "avg_runtime_seconds", "runtime_seconds", "Runtime (s)"):
        raise ValueError(f"Missing required runtime field ('avg_runtime_seconds' or 'runtime_seconds') in row: {row}")
    runtime = get_field(row, "avg_runtime_seconds", "runtime_seconds", "Runtime (s)")

    return {
        "algorithm": str(algo),
        "experiment_run_id": run_id,
        "dataset_id": dataset_id,
        "model_config_version": config_ver,
        "code_version": code_ver,
        "random_seed": seed,
        "experiment_status": status,
        "total_windows": tot_win,
        "success_windows": succ_win,
        "failure_windows": fail_win,
        "skipped_windows": skipped_win,
        "failure_rate": fail_rate,
        "total_aftershocks": tot_after,
        "mean_noise_ratio": mean_noise,
        "mean_dbcv": mean_dbcv,
        "mean_omori_p": mean_omori_p,
        "omori_fitted_windows": omori_win,
        "mean_jaccard_stability": mean_jaccard,
        "mean_ari_stability": mean_ari,
        "runtime_seconds": runtime,
    }


def validate_publication_gate(
    metadata: Dict[str, Any],
    comparison_table: List[Dict[str, Any]],
    runs: Optional[List[Dict[str, Any]]] = None,
    mode: str = "official",
) -> None:
    """Enforce strict publication gate and row-level consistency (fail-closed)."""
    if mode == "development":
        return

    # 1. Require valid non-empty dataset ID and code pin in metadata
    dataset_id = metadata.get("dataset_id")
    if not dataset_id or dataset_id in ("UNKNOWN_DATASET", "unknown", ""):
        raise ValueError(
            "PUBLICATION_GATE_VIOLATION: Official report requires a valid, registered dataset_id in metadata. "
            f"Found '{dataset_id}'."
        )

    git_commit = metadata.get("git_commit")
    if not git_commit or git_commit in ("unknown", ""):
        raise ValueError(
            "PUBLICATION_GATE_VIOLATION: Official report requires a valid git_commit or code_version pin in metadata. "
            f"Found '{git_commit}'."
        )

    # 2. Require non-empty comparison table
    if not comparison_table or len(comparison_table) == 0:
        raise ValueError(
            "PUBLICATION_GATE_VIOLATION: Official report requires a non-empty list of comparison rows. "
            "Empty run results cannot be published as OFFICIAL_PUBLICATION."
        )

    # 3. If explicit runs list is provided, validate each run
    valid_run_map: Dict[str, Dict[str, Any]] = {}
    if runs:
        for run in runs:
            run_id = run.get("experiment_run_id")
            if not run_id:
                raise ValueError("PUBLICATION_GATE_VIOLATION: Run entry missing experiment_run_id.")
            status = run.get("status") or run.get("experiment_status")
            if status not in ("CANDIDATE", "APPROVED"):
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Run '{run_id}' has unapproved status '{status}'. "
                    "Only 'CANDIDATE' or 'APPROVED' runs may be included in official reports."
                )
            run_dataset = run.get("dataset_id")
            if run_dataset and run_dataset != dataset_id:
                raise ValueError(
                    f"CROSS_DATASET_VIOLATION: Run '{run_id}' belongs to dataset '{run_dataset}', "
                    f"which does not match report dataset '{dataset_id}'."
                )
            valid_run_map[run_id] = run

    # 4. Strictly validate every single row rendered in comparison_table
    for idx, row in enumerate(comparison_table):
        row_run_id = row.get("experiment_run_id")
        if not row_run_id:
            raise ValueError(
                f"PUBLICATION_GATE_VIOLATION: Comparison row #{idx + 1} ({row.get('algorithm')}) "
                "missing required experiment_run_id in official report."
            )

        row_dataset = row.get("dataset_id")
        if row_dataset and row_dataset != dataset_id:
            raise ValueError(
                f"CROSS_DATASET_VIOLATION: Row for run '{row_run_id}' has dataset '{row_dataset}', "
                f"which does not match report dataset '{dataset_id}'."
            )

        # If runs list was provided, cross-check row against registry run
        if valid_run_map:
            if row_run_id not in valid_run_map:
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row references run '{row_run_id}' not declared in verified runs list."
                )
            reg_run = valid_run_map[row_run_id]

            # 1. Status check: cannot be REJECTED or unapproved; must match registry if specified
            row_status = row.get("experiment_status") or row.get("status")
            reg_status = reg_run.get("experiment_status") or reg_run.get("status")
            if row_status and row_status != reg_status:
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row for run '{row_run_id}' has conflicting status '{row_status}', "
                    f"does not match registry run status '{reg_status}'."
                )
            if row_status and row_status not in ("CANDIDATE", "APPROVED"):
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row for run '{row_run_id}' has unapproved status '{row_status}'."
                )

            # 2. Algorithm check
            reg_algo = reg_run.get("algorithm_name") or reg_run.get("algorithm")
            row_algo = row.get("algorithm") or row.get("algorithm_name")
            if reg_algo and row_algo and str(reg_algo).upper() != str(row_algo).upper():
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row for run '{row_run_id}' has conflicting algorithm '{row_algo}', "
                    f"registry has '{reg_algo}'."
                )

            # 3. Model config version check
            reg_config = reg_run.get("model_config_version") or reg_run.get("config_version")
            row_config = row.get("model_config_version") or row.get("config_version")
            if reg_config and row_config and reg_config != row_config:
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row for run '{row_run_id}' has conflicting config version '{row_config}', "
                    f"registry has '{reg_config}'."
                )

            # 4. Code version check
            reg_code = reg_run.get("code_version")
            row_code = row.get("code_version")
            if reg_code and row_code and reg_code != row_code:
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row for run '{row_run_id}' has conflicting code version '{row_code}', "
                    f"registry has '{reg_code}'."
                )

            # 5. Random seed check
            reg_seed = reg_run.get("random_seed")
            row_seed = row.get("random_seed")
            if reg_seed is not None and row_seed is not None and reg_seed != row_seed:
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row for run '{row_run_id}' has conflicting random seed '{row_seed}', "
                    f"registry has '{reg_seed}'."
                )
        else:
            # When NO registry runs list is provided:
            # Row MUST have explicit status CANDIDATE or APPROVED, and mandatory identity
            row_status = row.get("experiment_status") or row.get("status")
            if not row_status or row_status not in ("CANDIDATE", "APPROVED"):
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row for run '{row_run_id}' missing or invalid experiment_status: '{row_status}'. "
                    "Official report without external registry requires explicit 'CANDIDATE' or 'APPROVED' status on every row."
                )
            if not row.get("model_config_version") and not metadata.get("configs"):
                raise ValueError(
                    f"PUBLICATION_GATE_VIOLATION: Row for run '{row_run_id}' missing required model_config_version."
                )


def check_sql_parity_verification(
    verification_evidence: Optional[Dict[str, Any]],
    dataset_id: Optional[str],
    comparison_table: List[Dict[str, Any]],
    runs: Optional[List[Dict[str, Any]]] = None,
) -> bool:
    """Validate verification evidence against dataset, runs, and committed snapshot pins."""
    if not isinstance(verification_evidence, dict):
        return False
    if verification_evidence.get("status") != "PASSED":
        return False

    # 1. Dataset binding: dataset_id is mandatory and must match report dataset_id
    ev_dataset = verification_evidence.get("dataset_id")
    if not ev_dataset or ev_dataset in ("UNKNOWN_DATASET", "unknown") or ev_dataset != dataset_id:
        return False

    # 2. Must contain valid positive snapshot IDs
    ev_mem_snap = verification_evidence.get("membership_snapshot_id")
    ev_sum_snap = verification_evidence.get("summary_snapshot_id")
    if not ev_mem_snap or not ev_sum_snap:
        return False

    # 3. Coverage of rendered runs:
    rendered_run_ids = {r.get("experiment_run_id") for r in comparison_table if r.get("experiment_run_id")}
    if not rendered_run_ids:
        return False

    # Check run coverage in evidence if specified
    ev_runs = verification_evidence.get("verified_runs") or verification_evidence.get("runs")
    if ev_runs is not None:
        if isinstance(ev_runs, list):
            ev_run_set = {
                r.get("experiment_run_id") if isinstance(r, dict) else str(r)
                for r in ev_runs
            }
            if not rendered_run_ids.issubset(ev_run_set):
                return False
        else:
            return False

    # 4. Cross-check snapshots with run registry / table pins
    run_pin_map: Dict[str, Dict[str, Any]] = {}
    if runs:
        for r in runs:
            if isinstance(r, dict) and r.get("experiment_run_id"):
                run_pin_map[r["experiment_run_id"]] = r
    for r in comparison_table:
        rid = r.get("experiment_run_id")
        if rid and rid not in run_pin_map:
            run_pin_map[rid] = r

    for rid in rendered_run_ids:
        run_info = run_pin_map.get(rid, {})
        reg_mem_snap = run_info.get("membership_snapshot_id")
        reg_sum_snap = run_info.get("summary_snapshot_id")

        if reg_mem_snap is not None and str(reg_mem_snap) != str(ev_mem_snap):
            return False
        if reg_sum_snap is not None and str(reg_sum_snap) != str(ev_sum_snap):
            return False

    return True


def extract_normalized_payload(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize input JSON from EXP-04 pilot format or standard Trino export format."""
    # Check if EXP-04 pilot format
    if "evaluation_summary" in raw_data:
        eval_sum = raw_data["evaluation_summary"]
        pilot_ver = raw_data.get("pilot_verification", {})
        code_prov = raw_data.get("code_provenance", {})
        config_pins = raw_data.get("algorithm_config_pins", {})
        runtime_meta = raw_data.get("runtime_metadata", {})

        is_pilot = (
            raw_data.get("evaluation_scope") == "PILOT_DEVELOPMENT"
            or str(pilot_ver.get("pilot_id", "")).startswith("pilot_")
        )

        dataset_id = get_field(pilot_ver, "pilot_id") or get_field(eval_sum, "pilot_id") or "pilot_dev"
        timestamp = get_field(eval_sum, "evaluated_at_utc") or datetime.now(timezone.utc).isoformat()
        git_commit = get_field(code_prov, "git_commit") or get_field(runtime_meta, "code_version")

        norm_metadata = {
            "dataset_id": dataset_id,
            "evaluation_timestamp_utc": timestamp,
            "git_commit": git_commit,
            "random_seed": get_field(raw_data.get("evaluation_parameters", {}), "random_seed", default=42),
            "total_input_candidates": get_field(pilot_ver, "candidate_rows", default="N/A"),
            "total_mainshocks": get_field(pilot_ver, "mainshock_count", default="N/A"),
            "research_ready": not is_pilot,
            "data_scope": "DEVELOPMENT_ONLY" if is_pilot else "OFFICIAL",
            "configs": config_pins,
        }

        # Normalize comparison table
        raw_table = eval_sum.get("comparison_table", [])
        norm_table = [normalize_comparison_row(row) for row in raw_table]

        # Normalize model details
        raw_details = eval_sum.get("algorithm_details", {})
        norm_details = {}
        for algo, d in raw_details.items():
            dbcv_val = get_field(d, "mean_dbcv_score", "mean_dbcv")
            noise_val = get_field(d, "mean_noise_ratio")
            omori_p_val = get_field(d, "mean_omori_p")
            omori_r2_val = get_field(d, "mean_omori_r2")
            fail_rate_val = get_field(d, "failure_rate", default=0.0)
            jaccard_val = get_field(d, "mean_jaccard_stability")
            ari_val = get_field(d, "mean_ari_stability")
            fitted_win = get_field(d, "omori_fitted_windows", default=0)
            tot_win = get_field(d, "total_windows", default=0)

            fail_rate_str = f"{fail_rate_val * 100:.1f}%" if isinstance(fail_rate_val, (int, float)) else str(fail_rate_val)
            noise_str = format_metric(noise_val, fmt=".1f", suffix="%")
            dbcv_str = format_metric(dbcv_val, fmt=".4f")
            omori_p_str = format_metric(omori_p_val, fmt=".2f")
            omori_r2_str = format_metric(omori_r2_val, fmt=".2f")
            jaccard_str = format_metric(jaccard_val, fmt=".4f")
            ari_str = format_metric(ari_val, fmt=".4f")

            norm_details[algo] = {
                "total_windows": tot_win,
                "success_windows": get_field(d, "success_windows", default=0),
                "failure_windows": get_field(d, "failure_windows", default=0),
                "failure_rate": fail_rate_str,
                "failure_reasons_observed": get_field(d, "failure_reasons", default=[]),
                "total_aftershocks_found": get_field(d, "total_aftershocks", default=0),
                "mean_noise_ratio": noise_str,
                "mean_dbcv": dbcv_str,
                "dbcv_computed_windows": get_field(d, "dbcv_computed_windows", default="N/A"),
                "mean_omori_p": omori_p_str,
                "mean_omori_r2": omori_r2_str,
                "omori_fitted_windows": f"{fitted_win}/{tot_win}",
                "mean_jaccard_stability": jaccard_str,
                "mean_ari_stability": ari_str,
                "runtime_seconds": get_field(d, "runtime_seconds", default=0.0),
            }

        return {
            "metadata": norm_metadata,
            "comparison_table": norm_table,
            "model_analyses": norm_details,
            "code_provenance": code_prov,
            "runs": raw_data.get("runs", []),
            "verification_evidence": raw_data.get("verification_evidence"),
        }

    # Standard / Trino SQL export format
    raw_metadata = raw_data.get("metadata", {})
    raw_table = raw_data.get("comparison_table", [])
    norm_table = [normalize_comparison_row(row) for row in raw_table]

    norm_metadata = {
        "dataset_id": get_field(raw_metadata, "dataset_id") or (norm_table[0].get("dataset_id") if norm_table else None),
        "evaluation_timestamp_utc": get_field(raw_metadata, "evaluation_timestamp_utc") or datetime.now(timezone.utc).isoformat(),
        "git_commit": get_field(raw_metadata, "git_commit") or (norm_table[0].get("code_version") if norm_table else None) or get_git_commit(),
        "random_seed": get_field(raw_metadata, "random_seed", default=42),
        "total_input_candidates": get_field(raw_metadata, "total_input_candidates", default="N/A"),
        "total_mainshocks": get_field(raw_metadata, "total_mainshocks", default=len(norm_table)),
        "research_ready": raw_metadata.get("research_ready", True),
        "data_scope": raw_metadata.get("data_scope", "OFFICIAL"),
        "configs": raw_metadata.get("configs", {}),
    }

    return {
        "metadata": norm_metadata,
        "comparison_table": norm_table,
        "model_analyses": raw_data.get("model_analyses", {}),
        "code_provenance": raw_data.get("code_provenance", {}),
        "runs": raw_data.get("runs", []),
        "verification_evidence": raw_data.get("verification_evidence") or raw_data.get("trino_verification"),
    }


def build_markdown_report(
    eval_data: Dict[str, Any],
    mode: str = "auto",
    report_title: Optional[str] = None,
) -> str:
    """Render comprehensive Markdown static report adhering to MLI-04 criteria."""
    normalized = extract_normalized_payload(eval_data)
    metadata = normalized.get("metadata", {})
    comparison_table = normalized.get("comparison_table", [])
    model_analyses = normalized.get("model_analyses", {})
    code_provenance = normalized.get("code_provenance", {})
    runs = normalized.get("runs", [])
    verification_evidence = normalized.get("verification_evidence")

    # Detect whether dataset is development pilot
    is_development = (
        mode == "development"
        or metadata.get("research_ready") is False
        or "DEVELOPMENT" in str(metadata.get("data_scope", "")).upper()
        or str(metadata.get("dataset_id", "")).startswith("pilot_")
        or eval_data.get("evaluation_scope") == "PILOT_DEVELOPMENT"
    )

    # Fail-closed publication gate check
    if mode == "official":
        if is_development:
            raise ValueError(
                "OFFICIAL_MODE_CONFLICT: Mode is set to 'official', but dataset is labeled DEVELOPMENT_ONLY/pilot. "
                "Pilot experiments cannot be published under official mode."
            )
        validate_publication_gate(metadata, comparison_table, runs=runs, mode="official")
    elif mode == "auto":
        if not is_development:
            # In auto mode, if input is not development, enforce gate fail-closed
            validate_publication_gate(metadata, comparison_table, runs=runs, mode="official")

    scope_badge = "DEVELOPMENT_ONLY (research_ready=false)" if is_development else "OFFICIAL_PUBLICATION"
    timestamp_utc = metadata.get("evaluation_timestamp_utc") or datetime.now(timezone.utc).isoformat()
    dataset_id = metadata.get("dataset_id") or "UNKNOWN_DATASET"
    git_commit = metadata.get("git_commit") or code_provenance.get("git_commit") or get_git_commit()
    seed = metadata.get("random_seed", 42)
    total_candidates = metadata.get("total_input_candidates", "N/A")
    total_mainshocks = metadata.get("total_mainshocks", "N/A")

    title = report_title or (
        "Báo cáo Thực nghiệm So sánh Mô hình Phân cụm Chuỗi Động đất"
        if not is_development
        else "Báo cáo Thực nghiệm Pilot So sánh Mô hình Phân cụm Chuỗi Động đất (DEVELOPMENT_ONLY)"
    )

    lines: List[str] = []
    lines.append(f"# {title}\n")
    lines.append("## 1. Siêu dữ liệu Thực nghiệm và Cổng Kiểm định (Metadata & Publication Gate)\n")
    lines.append("- **Mã Task:** [MLI-04](../task/tasks/MLI-04.md) / [EXP-04](../task/tasks/EXP-04.md)")
    lines.append(f"- **Phạm vi Báo cáo:** `{scope_badge}`")
    lines.append(f"- **Thời điểm Khởi tạo (UTC):** `{timestamp_utc}`")
    lines.append(f"- **Dataset ID:** `{dataset_id}`")
    lines.append(f"- **Git Commit Pin:** `{git_commit}`")
    lines.append(f"- **Random Seed:** `{seed}`")
    lines.append(f"- **Số lượng Cửa sổ Mainshock:** `{total_mainshocks}`")
    lines.append(f"- **Tổng số Sự kiện Ứng viên:** `{total_candidates}`")

    # Config Pins
    configs = metadata.get("configs", {})
    if configs:
        lines.append("\n### Cấu hình Thuật toán đã Ghim (Algorithm Config Pins):")
        for algo, cfg in sorted(configs.items()):
            cfg_path = cfg.get("config_path", "unknown")
            cfg_ver = cfg.get("model_config_version") or cfg.get("version", "unknown")
            file_sha = (cfg.get("file_bytes_sha256") or cfg.get("config_sha256") or cfg.get("file_sha256", "unknown"))[:16]
            canon_sha = (cfg.get("canonical_config_sha256") or cfg.get("canonical_sha256", "unknown"))[:16]
            lines.append(f"- **{algo}:** `{cfg_path}` (Phiên bản: `{cfg_ver}`, File SHA: `{file_sha}...`, Canon SHA: `{canon_sha}...`)")

    # Critical Scientific Disclaimer
    lines.append("\n> [!IMPORTANT]")
    lines.append("> **RANH GIỚI KHOA HỌC BẮT BUỘC:**")
    lines.append("> 1. **Kết quả hồi cứu (Retrospective Candidates):** Tất cả các cụm chuỗi được phân loại là **ứng viên chuỗi sau chấn (candidate aftershock sequences)** mang tính thống kê hồi cứu trên dữ liệu quan trắc lịch sử.")
    lines.append("> 2. **Tuyệt đối KHÔNG dự đoán động đất:** Mô hình hoàn toàn không dự báo thời gian, tọa độ hoặc độ lớn của các trận động đất trong tương lai, và không đưa ra tuyên bố về quan hệ nhân quả vật lý trực tiếp.")
    lines.append("> 3. **Không dùng Accuracy / F1:** Bài toán phân cụm mật độ không giám sát không có nhãn ground-truth tự nhiên tuyệt đối. Đánh giá dựa trên tính nhất quán vật lý (Luật Omori), chất lượng phân cụm (DBCV), độ ổn định (Jaccard, ARI) và tỷ lệ lỗi.")
    lines.append("> 4. **Bảo toàn mẫu số (Denominator Integrity):** Toàn bộ các cửa sổ thất bại được giữ nguyên trong mẫu số tính tỷ lệ lỗi, không bị ẩn khỏi báo cáo.")

    # Comparative Table
    has_runs = any(r.get("experiment_run_id") for r in comparison_table)
    lines.append("\n## 2. Bảng Đối soát So sánh 4 Thuật toán (Algorithm Comparison Table)\n")
    if has_runs:
        lines.append("| Thuật toán | Run ID | Cấu hình | Seed | Tổng Cửa sổ | Thành công | Thất bại | Tỷ lệ Lỗi | Tổng Sau chấn | Nhiễu TB | DBCV TB | Omori p TB | Cửa sổ Omori | Độ ổn định Jaccard | Độ ổn định ARI | Thời gian (s) |")
        lines.append("| :--- | :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")
    else:
        lines.append("| Thuật toán | Tổng Cửa sổ | Thành công | Thất bại | Tỷ lệ Lỗi | Tổng Sau chấn | Nhiễu TB | DBCV TB | Omori p TB | Cửa sổ Omori | Độ ổn định Jaccard | Độ ổn định ARI | Thời gian (s) |")
        lines.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

    for row in comparison_table:
        algo = row.get("algorithm", "UNKNOWN")
        run_id_str = row.get("experiment_run_id") or "N/A"
        cfg_str = row.get("model_config_version") or "N/A"
        seed_str = str(row.get("random_seed")) if row.get("random_seed") is not None else "N/A"
        tot_win = row.get("total_windows", 0)
        succ_win = row.get("success_windows", 0)
        fail_win = row.get("failure_windows", 0)
        fail_rate = format_metric(row.get("failure_rate", 0), fmt=".1f", suffix="%")
        tot_after = row.get("total_aftershocks")
        if tot_after is None:
            tot_after_str = "N/A"
        elif isinstance(tot_after, (int, float)):
            tot_after_str = str(int(tot_after))
        else:
            tot_after_str = str(tot_after)

        noise_str = format_metric(row.get("mean_noise_ratio"), fmt=".1f", suffix="%")
        dbcv_str = format_metric(row.get("mean_dbcv"), fmt=".4f")
        omori_p_str = format_metric(row.get("mean_omori_p"), fmt=".2f")
        omori_fitted = row.get("omori_fitted_windows")
        if omori_fitted is None:
            omori_fitted = f"{row.get('omori_fitted_count', 0)}/{tot_win}"
        elif isinstance(omori_fitted, (int, float)):
            omori_fitted = f"{int(omori_fitted)}/{tot_win}"
        else:
            omori_fitted = str(omori_fitted)

        jaccard_str = format_metric(row.get("mean_jaccard_stability"), fmt=".4f")
        ari_str = format_metric(row.get("mean_ari_stability"), fmt=".4f")
        runtime = row.get("runtime_seconds")
        runtime_str = format_metric(runtime, fmt=".2f")

        if has_runs:
            lines.append(
                f"| **{algo}** | `{run_id_str}` | `{cfg_str}` | `{seed_str}` | {tot_win} | {succ_win} | {fail_win} | {fail_rate} | {tot_after_str} | {noise_str} | {dbcv_str} | {omori_p_str} | {omori_fitted} | {jaccard_str} | {ari_str} | {runtime_str} |"
            )
        else:
            lines.append(
                f"| **{algo}** | {tot_win} | {succ_win} | {fail_win} | {fail_rate} | {tot_after_str} | {noise_str} | {dbcv_str} | {omori_p_str} | {omori_fitted} | {jaccard_str} | {ari_str} | {runtime_str} |"
            )

    # Detailed Analysis
    if model_analyses:
        lines.append("\n## 3. Phân tích Chi tiết Từng Mô hình\n")
        for algo, detail in sorted(model_analyses.items()):
            lines.append(f"### Mô hình {algo}\n")
            tot = detail.get("total_windows", 0)
            succ = detail.get("success_windows", 0)
            fail = detail.get("failure_windows", 0)
            fail_rate = format_metric(detail.get("failure_rate"), fmt=".1f", suffix="%")

            lines.append(f"- **Tổng số cửa sổ:** `{tot}` (Thành công: `{succ}`, Thất bại: `{fail}`, Tỷ lệ lỗi: `{fail_rate}`)")
            reasons = detail.get("failure_reasons_observed", []) or ["Không có"]
            lines.append(f"- **Lý do thất bại ghi nhận:** `{reasons}`")
            lines.append(f"- **Tổng số sau chấn (POST sequence members):** `{detail.get('total_aftershocks_found', 0)}`")

            noise_str = format_metric(detail.get("mean_noise_ratio"), fmt=".1f", suffix="%")
            lines.append(f"- **Tỷ lệ nhiễu trung bình:** `{noise_str}`")

            dbcv_str = format_metric(detail.get("mean_dbcv"), fmt=".4f")
            lines.append(f"- **DBCV trung bình:** `{dbcv_str}` (Cửa sổ tính được: `{detail.get('dbcv_computed_windows', 'N/A')}`)")

            omori_p = format_metric(detail.get("mean_omori_p"), fmt=".2f")
            omori_r2 = format_metric(detail.get("mean_omori_r2"), fmt=".2f")
            lines.append(f"- **Modified Omori law decay exponent p:** `{omori_p}` (R² fit: `{omori_r2}`, Cửa sổ fit: `{detail.get('omori_fitted_windows', 'N/A')}`)")

            lines.append(f"- **Độ ổn định Jaccard (Feature Perturbation):** `{format_metric(detail.get('mean_jaccard_stability'), fmt='.4f')}`")
            lines.append(f"- **Độ ổn định ARI (Cluster Partitioning):** `{format_metric(detail.get('mean_ari_stability'), fmt='.4f')}`")
            lines.append(f"- **Thời gian thực thi:** `{format_metric(detail.get('runtime_seconds'), fmt='.2f')}` giây\n")

    # Disclaimers & Notes
    lines.append("## 4. Đối soát Trino Views và Phân giải Đa chuỗi (Lineage & Resolution Notes)\n")

    # Check whether explicit verified evidence exists adhering to contract
    has_verified_parity = check_sql_parity_verification(
        verification_evidence=verification_evidence,
        dataset_id=dataset_id,
        comparison_table=comparison_table,
        runs=runs,
    )

    if is_development:
        lines.append("1. **Trạng thái Đối soát Trino Views (SQL Parity):**")
        lines.append("   - **Trạng thái: CHƯA NGHIỆM THU (PENDING OFFICIAL IMPORT).**")
        lines.append("   - Báo cáo này được kết xuất trong môi trường thử nghiệm (development pilot) trên local/Colab và chưa được đối soát trực tiếp với bảng Iceberg `ml.*` qua Trino.")
        lines.append("   - Việc kiểm chứng count parity và SQL readback chính thức chỉ được chứng nhận khi dataset chính thức và kết quả thực nghiệm được import vào Iceberg thông qua [MLQ-01](../task/tasks/MLQ-01.md).")
    elif has_verified_parity:
        lines.append("1. **Khớp nối Trino Serving Views (SQL Parity):**")
        lines.append("   - **Trạng thái: ĐÃ ĐỐI SOÁT (VERIFIED).**")
        lines.append("   - Toàn bộ các chỉ số trong bảng đối soát trên khớp chính xác với kết quả truy vấn từ các serving view `iceberg.ml.v_algorithm_comparison` và `iceberg.ml.v_experiment_runs` cho exact snapshots đã commit:")
        lines.append(f"     - Membership snapshot: `{verification_evidence.get('membership_snapshot_id')}`")
        lines.append(f"     - Summary snapshot: `{verification_evidence.get('summary_snapshot_id')}`")
    else:
        lines.append("1. **Khớp nối Trino Serving Views (SQL Parity):**")
        lines.append("   - **Trạng thái: CHƯA ĐỐI SOÁT (PENDING READBACK / NOT VERIFIED).**")
        lines.append("   - Báo cáo này tổng hợp từ kết quả thực nghiệm và chưa có bằng chứng kiểm định readback trên Iceberg snapshot.")
        lines.append("   - Cổng công bố `experiment_status IN ('CANDIDATE', 'APPROVED')` đảm bảo tư cách xuất bản của các run, nhưng tính toàn vẹn count parity với serving views cần được kiểm chứng độc lập qua [MLQ-01](../task/tasks/MLQ-01.md).")

    lines.append("2. **Phân giải Đa chuỗi (Multi-Sequence Resolution):**")
    lines.append("   - Báo cáo phân biệt rõ giữa **Raw Aftershock Memberships** (tổng số lần gán nhãn POST độc lập theo từng cửa sổ) và **Unique Resolved Aftershocks** (`resolution_rank = 1` gán cho một mainshock duy nhất).")
    lines.append("   - Các trường hợp không thể phân giải dứt khoát (`multi_sequence_status = 'AMBIGUOUS'`) có `resolution_rank = NULL` và tuyệt đối không được tính vào số dư chấn duy nhất đã phân giải.")
    lines.append("3. **Bảo tồn Trạng thái Thất bại:**")
    lines.append("   - Các cửa sổ có mainshock bị phân cụm mật độ gán nhãn `-1` (nhiễu) được ghi nhận mã `EXP_MAINSHOCK_CLASSIFIED_AS_NOISE` và bảo toàn trong mẫu số.")

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MLI-04: Generate static experiment comparison report from evaluation JSON or Trino queries."
    )
    parser.add_argument(
        "--input-json",
        type=Path,
        default=PROJECT_ROOT / "docs" / "evidence" / "EXP-04_PILOT_EVALUATION.json",
        help="Path to evaluation results JSON file.",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=PROJECT_ROOT / "docs" / "evidence" / "MLI-04_STATIC_REPORT.md",
        help="Destination path for generated Markdown report.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional destination path for structured summary JSON.",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "development", "official"],
        default="auto",
        help="Report mode: 'development' (pilot, labeled DEVELOPMENT_ONLY) or 'official' (requires CANDIDATE/APPROVED status).",
    )
    parser.add_argument(
        "--title",
        type=str,
        default=None,
        help="Custom title for the static report.",
    )

    args = parser.parse_args()

    if not args.input_json.exists():
        print(f"ERROR: Input JSON file not found: {args.input_json}", file=sys.stderr)
        return 1

    try:
        with open(args.input_json, "r", encoding="utf-8") as f:
            eval_data = json.load(f)
    except Exception as e:
        print(f"ERROR: Failed to read input JSON: {e}", file=sys.stderr)
        return 1

    # Render report (which enforces fail-closed publication gate)
    try:
        md_content = build_markdown_report(eval_data, mode=args.mode, report_title=args.title)
    except ValueError as err:
        print(f"ERROR: {err}", file=sys.stderr)
        return 2

    # Ensure parent directory exists
    args.output_md.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output_md, "w", encoding="utf-8", newline="") as f:
        f.write(md_content)

    print(f"SUCCESS: Generated static report at {args.output_md}")

    if args.output_json:
        normalized = extract_normalized_payload(eval_data)
        summary_payload = {
            "metadata": normalized.get("metadata", {}),
            "comparison_table": normalized.get("comparison_table", []),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "mode": args.mode,
        }
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output_json, "w", encoding="utf-8") as f:
            json.dump(summary_payload, f, indent=2, ensure_ascii=False)
        print(f"SUCCESS: Generated summary JSON at {args.output_json}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
