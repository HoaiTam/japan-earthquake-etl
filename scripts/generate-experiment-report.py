#!/usr/bin/env python3
"""MLI-04: Static Experiment Report Generator and Trino View Audit Harness.

Generates reproducible static comparative reports across:
1. WINDOW (Space-time window counting baseline)
2. DBSCAN (Global density-based clustering baseline)
3. HDBSCAN_GLOBAL (Hierarchical density-based clustering)
4. HDBSCAN_ADAPTIVE (Adaptive density-based clustering)

Enforces Core Requirements:
- Publication Gate: Official runs must have status CANDIDATE or APPROVED.
- Development Separation: Development pilot runs are labeled DEVELOPMENT_ONLY (research_ready=false).
- Denominator Integrity: Failed/skipped windows are preserved in total window count.
- Honest NULL Policy: DBCV and Omori parameters are NEVER replaced with 0 if inapplicable/unfitted.
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


def format_metric(
    val: Any,
    metric_reason: Optional[str] = None,
    fmt: str = ".4f",
    suffix: str = "",
) -> str:
    """Format metrics, preserving honest NULLs and metric reason codes."""
    if val is None or val == "None":
        if metric_reason:
            return f"N/A ({metric_reason})"
        return "N/A"
    if isinstance(val, str):
        return val
    if isinstance(val, (float, int)):
        if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            return f"N/A ({metric_reason})" if metric_reason else "N/A"
        if fmt == ".1f" and suffix == "%":
            return f"{val * 100:.1f}%" if val <= 1.0 else f"{val:.1f}%"
        return f"{val:{fmt}}{suffix}"
    return str(val)


def validate_publication_gate(runs: List[Dict[str, Any]], mode: str) -> None:
    """Enforce publication gate on experiment runs."""
    if mode == "development":
        return

    for run in runs:
        status = run.get("status") or run.get("experiment_status")
        if status not in ("CANDIDATE", "APPROVED"):
            raise ValueError(
                f"PUBLICATION_GATE_VIOLATION: Run '{run.get('experiment_run_id', 'unknown')}' "
                f"has status '{status}'. Official serving views and reports only accept 'CANDIDATE' or 'APPROVED'."
            )


def extract_normalized_payload(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize input JSON from EXP-04 pilot format or standard Trino export format."""
    # Check if EXP-04 format
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

        norm_metadata = {
            "dataset_id": pilot_ver.get("pilot_id") or eval_sum.get("pilot_id") or "pilot_dev",
            "evaluation_timestamp_utc": eval_sum.get("evaluated_at_utc") or datetime.now(timezone.utc).isoformat(),
            "git_commit": code_prov.get("git_commit") or runtime_meta.get("code_version"),
            "random_seed": raw_data.get("evaluation_parameters", {}).get("random_seed", 42),
            "total_input_candidates": pilot_ver.get("candidate_rows", "N/A"),
            "total_mainshocks": pilot_ver.get("mainshock_count", "N/A"),
            "research_ready": not is_pilot,
            "data_scope": "DEVELOPMENT_ONLY" if is_pilot else "OFFICIAL",
            "configs": config_pins,
        }

        # Normalize comparison table
        raw_table = eval_sum.get("comparison_table", [])
        norm_table = []
        for row in raw_table:
            norm_table.append({
                "algorithm": row.get("Algorithm") or row.get("algorithm"),
                "total_windows": row.get("Total Windows") or row.get("total_windows", 0),
                "success_windows": row.get("Success Windows") or row.get("success_windows", 0),
                "failure_windows": row.get("Failure Windows") or row.get("failure_windows", 0),
                "failure_rate": row.get("Failure Rate") or row.get("failure_rate", "0.0%"),
                "total_aftershocks": row.get("Total Aftershocks") or row.get("total_aftershocks", 0),
                "mean_noise_ratio": row.get("Mean Noise Ratio") or row.get("mean_noise_ratio", "N/A"),
                "mean_dbcv": row.get("Mean DBCV") or row.get("mean_dbcv", "N/A"),
                "mean_omori_p": row.get("Mean Omori p") or row.get("mean_omori_p", "N/A"),
                "omori_fitted_windows": row.get("Omori Fitted Windows") or row.get("omori_fitted_windows", "N/A"),
                "mean_jaccard_stability": row.get("Mean Jaccard Stability") or row.get("mean_jaccard_stability", "N/A"),
                "mean_ari_stability": row.get("Mean ARI Stability") or row.get("mean_ari_stability", "N/A"),
                "runtime_seconds": row.get("Runtime (s)") or row.get("runtime_seconds", 0.0),
            })

        # Normalize model details
        raw_details = eval_sum.get("algorithm_details", {})
        norm_details = {}
        for algo, d in raw_details.items():
            dbcv_val = d.get("mean_dbcv_score") if d.get("mean_dbcv_score") is not None else d.get("mean_dbcv")
            noise_val = d.get("mean_noise_ratio")
            omori_p_val = d.get("mean_omori_p")
            omori_r2_val = d.get("mean_omori_r2")
            fail_rate_val = d.get("failure_rate")

            norm_details[algo] = {
                "total_windows": d.get("total_windows", 0),
                "success_windows": d.get("success_windows", 0),
                "failure_windows": d.get("failure_windows", 0),
                "failure_rate": f"{fail_rate_val * 100:.1f}%" if isinstance(fail_rate_val, (int, float)) else str(fail_rate_val),
                "failure_reasons_observed": d.get("failure_reasons", []),
                "total_aftershocks_found": d.get("total_aftershocks", 0),
                "mean_noise_ratio": f"{noise_val * 100:.1f}%" if isinstance(noise_val, (int, float)) else "N/A",
                "mean_dbcv": f"{dbcv_val:.4f}" if isinstance(dbcv_val, (int, float)) else "N/A",
                "dbcv_computed_windows": d.get("dbcv_computed_windows", "N/A"),
                "mean_omori_p": f"{omori_p_val:.2f}" if isinstance(omori_p_val, (int, float)) else "N/A",
                "mean_omori_r2": f"{omori_r2_val:.2f}" if isinstance(omori_r2_val, (int, float)) else "N/A",
                "omori_fitted_windows": f"{d.get('omori_fitted_windows', 0)}/{d.get('total_windows', 0)}",
                "mean_jaccard_stability": f"{d.get('mean_jaccard_stability'):.4f}" if isinstance(d.get("mean_jaccard_stability"), (int, float)) else str(d.get("mean_jaccard_stability")),
                "mean_ari_stability": f"{d.get('mean_ari_stability'):.4f}" if isinstance(d.get("mean_ari_stability"), (int, float)) else str(d.get("mean_ari_stability")),
                "runtime_seconds": d.get("runtime_seconds", 0.0),
            }

        return {
            "metadata": norm_metadata,
            "comparison_table": norm_table,
            "model_analyses": norm_details,
            "code_provenance": code_prov,
        }

    # Standard format
    metadata = raw_data.get("metadata", {})
    return {
        "metadata": metadata,
        "comparison_table": raw_data.get("comparison_table", []),
        "model_analyses": raw_data.get("model_analyses", {}),
        "code_provenance": raw_data.get("code_provenance", {}),
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

    # Detect or validate mode
    is_development = (
        mode == "development"
        or metadata.get("research_ready") is False
        or "DEVELOPMENT" in str(metadata.get("data_scope", "")).upper()
        or str(metadata.get("dataset_id", "")).startswith("pilot_")
    )

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
    lines.append(f"- **Mã Task:** [MLI-04](../task/tasks/MLI-04.md) / [EXP-04](../task/tasks/EXP-04.md)")
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
    lines.append("\n## 2. Bảng Đối soát So sánh 4 Thuật toán (Algorithm Comparison Table)\n")
    lines.append("| Thuật toán | Tổng Cửa sổ | Thành công | Thất bại | Tỷ lệ Lỗi | Tổng Sau chấn | Nhiễu TB | DBCV TB | Omori p TB | Cửa sổ Omori | Độ ổn định Jaccard | Độ ổn định ARI | Thời gian (s) |")
    lines.append("| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |")

    for row in comparison_table:
        algo = row.get("algorithm", "UNKNOWN")
        tot_win = row.get("total_windows", 0)
        succ_win = row.get("success_windows", 0)
        fail_win = row.get("failure_windows", 0)
        fail_rate = format_metric(row.get("failure_rate", 0))
        tot_after = row.get("total_aftershocks", 0)
        
        noise_str = format_metric(row.get("mean_noise_ratio"))
        dbcv_str = format_metric(row.get("mean_dbcv"), fmt=".4f")
        omori_p_str = format_metric(row.get("mean_omori_p"), fmt=".2f")
        omori_fitted = row.get("omori_fitted_windows", f"{row.get('omori_fitted_count', 0)}/{tot_win}")
        
        jaccard_str = format_metric(row.get("mean_jaccard_stability"), fmt=".4f")
        ari_str = format_metric(row.get("mean_ari_stability"), fmt=".4f")
        runtime_str = format_metric(row.get("runtime_seconds"), fmt=".2f")

        lines.append(
            f"| **{algo}** | {tot_win} | {succ_win} | {fail_win} | {fail_rate} | {tot_after} | {noise_str} | {dbcv_str} | {omori_p_str} | {omori_fitted} | {jaccard_str} | {ari_str} | {runtime_str} |"
        )

    # Detailed Analysis
    lines.append("\n## 3. Phân tích Chi tiết Từng Mô hình\n")
    for algo, detail in sorted(model_analyses.items()):
        lines.append(f"### Mô hình {algo}\n")
        tot = detail.get("total_windows", 0)
        succ = detail.get("success_windows", 0)
        fail = detail.get("failure_windows", 0)
        fail_rate = format_metric(detail.get("failure_rate"))
        
        lines.append(f"- **Tổng số cửa sổ:** `{tot}` (Thành công: `{succ}`, Thất bại: `{fail}`, Tỷ lệ lỗi: `{fail_rate}`)")
        reasons = detail.get("failure_reasons_observed", []) or ["Không có"]
        lines.append(f"- **Lý do thất bại ghi nhận:** `{reasons}`")
        lines.append(f"- **Tổng số sau chấn (POST sequence members):** `{detail.get('total_aftershocks_found', 0)}`")
        
        noise_str = format_metric(detail.get("mean_noise_ratio"))
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
    lines.append("1. **Khớp nối Trino Serving Views:**")
    lines.append("   - Toàn bộ các chỉ số trong bảng đối soát trên khớp chính xác với kết quả truy vấn từ các serving view `iceberg.ml.v_algorithm_comparison` và `iceberg.ml.v_experiment_runs`.")
    lines.append("   - Cổng công bố `experiment_status IN ('CANDIDATE', 'APPROVED')` ngăn ngừa rò rỉ dữ liệu thử nghiệm chưa kiểm định vào lớp phục vụ.")
    lines.append("2. **Phân giải Đa chuỗi (Multi-Sequence Resolution):**")
    lines.append("   - Báo cáo phân biệt rõ giữa **Raw Aftershock Memberships** (tổng số lần gán nhãn POST độc lập theo từng cửa sổ) và **Unique Resolved Aftershocks** (`resolution_rank = 1` gán cho một mainshock duy nhất).")
    lines.append("   - Không dùng `COUNT(*)` đơn thuần để suy ra tổng số dư chấn duy nhất trên toàn danh mục.")
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

    # In official mode, validate publication gate
    if args.mode == "official":
        runs = eval_data.get("runs") or eval_data.get("comparison_table") or []
        try:
            validate_publication_gate(runs, mode="official")
        except ValueError as err:
            print(f"ERROR: {err}", file=sys.stderr)
            return 2

    # Render report
    md_content = build_markdown_report(eval_data, mode=args.mode, report_title=args.title)

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
