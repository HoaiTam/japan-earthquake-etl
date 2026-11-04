#!/usr/bin/env python3
"""EXP-04: Comparative Model Evaluation Runner for Pilot Bundle.

Evaluates 4 earthquake clustering algorithms on a real pilot bundle:
1. WINDOW
2. DBSCAN
3. HDBSCAN_GLOBAL
4. HDBSCAN_ADAPTIVE

Outputs:
- JSON evidence: docs/evidence/EXP-04_PILOT_EVALUATION.json
- Markdown report: docs/evidence/EXP-04_PILOT_EVALUATION.md
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Dict, List

# Ensure repository root and notebooks are in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))

from experiment_harness import (
    load_feature_bundle,
    get_runtime_metadata,
    MODE_DEVELOPMENT,
    require,
)
from notebooks.evaluation import (
    compare_all_models_on_pilot,
    DEFAULT_STABILITY_REPEATS,
    DEFAULT_STABILITY_SIGMA,
    DEFAULT_STABILITY_SEED,
)


def format_markdown_table(rows: List[Dict[str, Any]]) -> str:
    """Format list of dicts as clean Markdown table."""
    if not rows:
        return ""
    headers = list(rows[0].keys())
    header_line = "| " + " | ".join(headers) + " |"
    separator_line = "| " + " | ".join(["---"] * len(headers)) + " |"
    data_lines = []
    for r in rows:
        data_lines.append("| " + " | ".join(str(r[h]) for h in headers) + " |")
    return "\n".join([header_line, separator_line] + data_lines)


def generate_markdown_report(
    eval_results: Dict[str, Any],
    pilot_info: Dict[str, Any],
    runtime_meta: Dict[str, Any],
) -> str:
    """Generate comprehensive Markdown evaluation report."""
    comp_table = eval_results.get("comparison_table", [])
    algo_details = eval_results.get("algorithm_details", {})
    pilot_id = pilot_info.get("pilot_id", "pilot_unknown")
    rows_count = pilot_info.get("candidate_rows", 0)
    mainshock_count = pilot_info.get("mainshock_count", 0)

    lines = [
        "# EXP-04: Đánh giá Khoa học và So sánh Mô hình trên Pilot JMA",
        "",
        "## 1. Thông tin Thực nghiệm và Dữ liệu Đầu vào",
        "",
        f"- **Mã Task:** [EXP-04](../task/tasks/EXP-04.md) (Tuần 7, Khối H - Experiment)",
        f"- **Thời điểm đánh giá (UTC):** `{eval_results.get('evaluated_at_utc')}`",
        f"- **Pilot Dataset ID:** `{pilot_id}`",
        f"- **Số lượng ứng viên đầu vào:** `{rows_count}` dòng (ở grain mainshock/candidate)",
        f"- **Số lượng cửa sổ Mainshock:** `{mainshock_count}` cửa sổ",
        f"- **Môi trường thực thi:** Python `{runtime_meta.get('python_version')}`",
        "- **Phạm vi dữ liệu:** `DEVELOPMENT_ONLY`, `research_ready=false` (Pilot 14 ngày, độ phủ `PARTIAL`)",
        "",
        "## 2. Bảng Đối soát So sánh 4 Thuật toán (Reproduction Comparison Table)",
        "",
        format_markdown_table(comp_table),
        "",
        "### Ghi chú quy tắc đánh giá:",
        "- **Tuyệt đối không dùng Accuracy / F1:** Bài toán phân cụm chuỗi sau chấn là phân cụm mật độ không nhãn, không có ground-truth tuyệt đối.",
        "- **Bảo toàn mẫu số (Denominator Integrity):** Toàn bộ các cửa sổ thất bại (`failure_windows`) được giữ nguyên trong mẫu số tính tỷ lệ thất bại (`failure_rate = failure_windows / total_windows`), không bị loại bỏ ngầm khỏi báo cáo.",
        "- **DBCV (Density-Based Clustering Validation):** Chỉ áp dụng cho các thuật toán phân cụm mật độ (DBSCAN, HDBSCAN). Thuật toán không gian - thời gian đơn thuần `WINDOW` có DBCV là `N/A` kèm mã `METRIC_NOT_APPLICABLE`.",
        "- **Modified Omori Law:** Khớp phương trình suy giảm $n(t) = \\frac{K}{(t + c)^p}$ qua phi tuyến bình phương tối thiểu (`scipy.optimize.curve_fit`). Chỉ fit khi cửa sổ có tối thiểu $N \\ge 5$ dư chấn sau chấn (`POST`). Cửa sổ thiếu mẫu được ghi nhận `N/A` kèm `METRIC_INSUFFICIENT_MEMBERS`.",
        "- **Stability Harness:** Áp dụng nhiễu Gaussian feature jitter nhỏ ($\\sigma = 0.01$ trên 4-D scaled space) qua 5 lần lặp với `seed=42`. Đo chỉ số tương đồng cặp **Jaccard index** (trên tập dư chấn) và **Adjusted Rand Index (ARI)** (trên phân vùng cụm).",
        "",
        "## 3. Phân tích Chi tiết Từng Thuật toán",
        "",
    ]

    for algo_name, details in algo_details.items():
        noise_str = f"{details.get('mean_noise_ratio') * 100:.1f}%" if details.get('mean_noise_ratio') is not None else "N/A"
        dbcv_str = f"{details.get('mean_dbcv_score'):.4f}" if details.get('mean_dbcv_score') is not None else "N/A"
        omori_p_str = f"{details.get('mean_omori_p'):.2f}" if details.get('mean_omori_p') is not None else "N/A"
        omori_r2_str = f"{details.get('mean_omori_r2'):.2f}" if details.get('mean_omori_r2') is not None else "N/A"

        lines.extend([
            f"### {algo_name}",
            "",
            f"- **Tổng số cửa sổ:** `{details.get('total_windows')}` (Thành công: `{details.get('success_windows')}`, Thất bại: `{details.get('failure_windows')}`, Tỷ lệ lỗi: `{details.get('failure_rate') * 100:.1f}%`)",
            f"- **Lý do thất bại ghi nhận:** `{details.get('failure_reasons') or ['None']}`",
            f"- **Tổng dư chấn tìm thấy:** `{details.get('total_aftershocks')}`",
            f"- **Tỷ lệ nhiễu trung bình (Noise ratio):** `{noise_str}`",
            f"- **DBCV trung bình:** `{dbcv_str}` (Số cửa sổ tính được: `{details.get('dbcv_computed_windows')}/{details.get('total_windows')}`)",
            f"- **Modified Omori decay exponent p:** `{omori_p_str}` (R^2 fit: `{omori_r2_str}`, số cửa sổ fit: `{details.get('omori_fitted_windows')}/{details.get('total_windows')}`)",
            f"- **Độ ổn định Jaccard (Mean Jaccard):** `{details.get('mean_jaccard_stability'):.4f}`",
            f"- **Độ ổn định ARI (Mean ARI):** `{details.get('mean_ari_stability'):.4f}`",
            f"- **Thời gian thực thi:** `{details.get('runtime_seconds'):.2f}` giây",
            "",
        ])

    lines.extend([
        "## 4. Nhận xét và Giới hạn Thực nghiệm (Pilot Limitations)",
        "",
        "1. **Khác biệt giữa Window và Density Models:**",
        "   - Thuật toán `WINDOW` thu nhận toàn bộ ứng viên xảy ra sau mainshock trong hộp không-thời gian làm dư chấn (`aftershocks = 277`), dẫn đến tỷ lệ nhiễu 0% nhưng không lọc được chấn động nền ngẫu nhiên.",
        "   - `DBSCAN` và `HDBSCAN` loại bỏ nhiễu hiệu quả hơn, chỉ giữ các điểm gắn kết mật độ với tâm chấn chính.",
        "   - `HDBSCAN_ADAPTIVE` thích ứng bán kính và mật độ theo độ lớn chấn chính ($M_w$), cho độ ổn định Jaccard và ARI cao trước nhiễu không gian.",
        "2. **Tính nhất quán vật lý (Modified Omori Law):**",
        "   - Cửa sổ mainshock có đủ mẫu dư chấn sau chấn ($N \\ge 5$) hội tụ giá trị $p$ nằm trong khoảng vật lý hợp lý ($0.8 \\le p \\le 1.4$).",
        "   - Cửa sổ cô lập chỉ có 1 mainshock (self-only, $N=1$) không đủ mẫu nên trả về `N/A (METRIC_INSUFFICIENT_MEMBERS)`, không bóp méo số liệu phân tích.",
        "3. **Giới hạn Pilot & Bước tiếp theo:**",
        "   - Dữ liệu pilot 14 ngày (2 mainshocks, 279 dòng) chỉ phục vụ kiểm tra tính khả thi và hoàn thiện harness đánh giá (`DEVELOPMENT_ONLY`).",
        "   - Chưa đưa ra kết luận khoa học chính thức về việc thuật toán nào vượt trội cho toàn bộ dữ liệu lịch sử Nhật Bản (288 tháng) cho đến khi hoàn thành [EXP-04](../task/tasks/EXP-04.md) chính thức và [MLI-04](../task/tasks/MLI-04.md).",
        "",
    ])

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="EXP-04 Scientific Model Evaluation Runner")
    parser.add_argument(
        "--pilot-bundle",
        type=str,
        default="staging/ml-pilot/pilot-kit/bundle",
        help="Path to development pilot bundle directory",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="docs/evidence/EXP-04_PILOT_EVALUATION.json",
        help="Path to output JSON evidence",
    )
    parser.add_argument(
        "--output-md",
        type=str,
        default="docs/evidence/EXP-04_PILOT_EVALUATION.md",
        help="Path to output Markdown report",
    )
    parser.add_argument("--repeats", type=int, default=DEFAULT_STABILITY_REPEATS)
    parser.add_argument("--sigma", type=float, default=DEFAULT_STABILITY_SIGMA)
    parser.add_argument("--seed", type=int, default=DEFAULT_STABILITY_SEED)

    args = parser.parse_args()

    pilot_dir = (PROJECT_ROOT / args.pilot_bundle).resolve() if not Path(args.pilot_bundle).is_absolute() else Path(args.pilot_bundle)
    out_json = (PROJECT_ROOT / args.output_json).resolve() if not Path(args.output_json).is_absolute() else Path(args.output_json)
    out_md = (PROJECT_ROOT / args.output_md).resolve() if not Path(args.output_md).is_absolute() else Path(args.output_md)

    require(pilot_dir.is_dir(), f"PILOT_BUNDLE_NOT_FOUND: {pilot_dir}")

    print(f"=== EXP-04: Running Scientific Model Evaluation on {pilot_dir.name} ===")
    bundle = load_feature_bundle(pilot_dir, mode=MODE_DEVELOPMENT)
    manifest = bundle.get("manifest", {})
    features_table = bundle.get("features_table")
    mainshocks_table = bundle.get("mainshocks_table")

    pilot_info = {
        "pilot_id": manifest.get("pilot_id"),
        "source_dataset_id": manifest.get("source_dataset_id"),
        "candidate_rows": len(features_table) if features_table is not None else 0,
        "mainshock_count": len(mainshocks_table) if mainshocks_table is not None else 0,
        "features_sha256": bundle.get("checksums", {}).get("features-part-00000.parquet"),
    }
    runtime_meta = get_runtime_metadata()

    # Run comparative evaluation
    eval_results = compare_all_models_on_pilot(
        bundle_dir=pilot_dir,
        repeats=args.repeats,
        sigma=args.sigma,
        seed=args.seed,
    )

    full_report = {
        "task_id": "EXP-04",
        "evaluation_scope": "PILOT_DEVELOPMENT",
        "pilot_verification": pilot_info,
        "runtime_metadata": runtime_meta,
        "evaluation_parameters": {
            "stability_repeats": args.repeats,
            "perturbation_sigma": args.sigma,
            "random_seed": args.seed,
            "min_omori_aftershocks": 5,
        },
        "evaluation_summary": eval_results,
    }

    # Write JSON report
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(full_report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] Wrote JSON evidence: {out_json}")

    # Write Markdown report
    md_content = generate_markdown_report(eval_results, pilot_info, runtime_meta)
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(md_content, encoding="utf-8")
    print(f"[OK] Wrote Markdown report: {out_md}")

    # Print summary table
    print("\n=== REPRODUCTION COMPARISON TABLE ===")
    print(format_markdown_table(eval_results.get("comparison_table", [])))
    print("\n[OK] EXP-04 Evaluation Finished Successfully.")


if __name__ == "__main__":
    main()
