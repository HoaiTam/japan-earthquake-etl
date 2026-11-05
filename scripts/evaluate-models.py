#!/usr/bin/env python3
"""EXP-04: Scientific Model Evaluation CLI Runner.

Executes comparative evaluation across 4 aftershock sequence clustering models
on the development pilot bundle:
1. WINDOW (Space-time counting window baseline)
2. DBSCAN (Density-based spatial clustering baseline)
3. HDBSCAN_GLOBAL (Hierarchical density-based clustering with global density)
4. HDBSCAN_ADAPTIVE (Adaptive parameter scaling based on candidate count and magnitude)

Enforces:
- Denominator integrity: all failed windows preserved in total count.
- Strictly NO accuracy/F1 metrics.
- Physical consistency: Modified Omori law decay fit (threshold N >= 5).
- Geometric clustering quality: DBCV score (WINDOW = METRIC_NOT_APPLICABLE).
- Stability harness: Jaccard and ARI over feature perturbation.
- Exact fail-closed config pins, code commit pin, and pilot dataset identity.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
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
    load_algorithm_configs,
    DEFAULT_STABILITY_REPEATS,
    DEFAULT_STABILITY_SIGMA,
    DEFAULT_STABILITY_SEED,
)


def get_code_provenance(project_root: Optional[Path] = None) -> Dict[str, Any]:
    """Capture full code provenance: git commit hash, clean/dirty tree status, and critical code hashes."""
    root = project_root or PROJECT_ROOT
    critical_files = [
        "notebooks/evaluation.py",
        "scripts/evaluate-models.py",
        "notebooks/baseline_models.py",
        "notebooks/adaptive_hdbscan.py",
        "notebooks/experiment_harness.py",
    ]
    file_hashes: Dict[str, str] = {}
    for rel_path in critical_files:
        f = root / rel_path
        if f.is_file():
            file_hashes[rel_path] = hashlib.sha256(f.read_bytes()).hexdigest()
        else:
            file_hashes[rel_path] = "MISSING"

    git_commit = "unknown"
    is_dirty = True
    dirty_files: List[str] = []
    try:
        res = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        c = res.stdout.strip()
        if re.fullmatch(r"[a-f0-9]{40}", c):
            git_commit = c

        status_res = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "-c",
                "core.autocrlf=true",
                "-c",
                "core.fileMode=false",
                "status",
                "--porcelain",
                "-uno",
                "--",
                "notebooks/evaluation.py",
                "scripts/evaluate-models.py",
                "notebooks/baseline_models.py",
                "notebooks/adaptive_hdbscan.py",
                "config/ml/models/",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        status_lines = [line.strip() for line in status_res.stdout.splitlines() if line.strip()]
        dirty_files = [line.split()[-1] for line in status_lines]
        is_dirty = len(dirty_files) > 0
    except Exception:
        pass

    return {
        "git_commit": git_commit,
        "is_clean_code": not is_dirty,
        "dirty_files": dirty_files,
        "critical_code_hashes": file_hashes,
    }


def _fmt_float(val: Any, fmt: str = ".4f", default: str = "N/A") -> str:
    """Safely format float or return default when None or non-finite."""
    if val is None:
        return default
    try:
        f_val = float(val)
        import math
        if not math.isfinite(f_val):
            return default
        return f"{f_val:{fmt}}"
    except (ValueError, TypeError):
        return default


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
    config_pins: Dict[str, Dict[str, Any]],
    code_prov: Optional[Dict[str, Any]] = None,
) -> str:
    """Generate comprehensive Markdown evaluation report."""
    comp_table = eval_results.get("comparison_table", [])
    algo_details = eval_results.get("algorithm_details", {})
    pilot_id = pilot_info.get("pilot_id", "pilot_unknown")
    rows_count = pilot_info.get("candidate_rows", 0)
    mainshock_count = pilot_info.get("mainshock_count", 0)

    clean_str = "clean" if (code_prov and code_prov.get("is_clean_code")) else "modified/tracked"

    lines = [
        "# EXP-04: Đánh giá Khoa học và So sánh Mô hình trên Pilot JMA",
        "",
        "## 1. Thông tin Thực nghiệm và Dữ liệu Đầu vào",
        "",
        f"- **Mã Task:** [EXP-04](../task/tasks/EXP-04.md) (Tuần 7, Khối H - Experiment)",
        f"- **Thời điểm đánh giá (UTC):** `{eval_results.get('evaluated_at_utc')}`",
        f"- **Pilot Dataset ID:** `{pilot_id}`",
        f"- **Git Commit Pin:** `{runtime_meta.get('code_version')}` ({clean_str})",
        f"- **Random Seed:** `{runtime_meta.get('random_seed')}`",
        f"- **Số lượng ứng viên đầu vào:** `{rows_count}` dòng (ở grain mainshock/candidate)",
        f"- **Số lượng cửa sổ Mainshock:** `{mainshock_count}` cửa sổ",
        f"- **Môi trường thực thi:** Python `{runtime_meta.get('python_version')}`",
        "- **Phạm vi dữ liệu:** `DEVELOPMENT_ONLY`, `research_ready=false` (Pilot 14 ngày, độ phủ `PARTIAL`)",
        "",
        "### Cấu hình thuật toán (Config Pins):",
    ]

    for algo, c_info in config_pins.items():
        lines.append(
            f"- **{algo}:** `{c_info['config_path']}` (Version: `{c_info['model_config_version']}`, SHA: `{c_info['config_sha256'][:16]}...`)"
        )

    lines.extend([
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
        "- **Stability Harness:** Áp dụng nhiễu Gaussian feature jitter nhỏ ($\\sigma = 0.01$ trên 4-D scaled space) qua 5 lần lặp với `seed=42`. Đo chỉ số tương đồng cặp **Jaccard index** (trên tập dư chấn) và **Adjusted Rand Index (ARI)** (trên phân vùng cụm). Phân biệt rõ lỗi thực thi (báo `METRIC_FIT_FAILED`) với kết quả nhiễu vật lý.",
        "",
        "## 3. Phân tích Chi tiết Từng Thuật toán",
        "",
    ])

    for algo_name, details in algo_details.items():
        noise_val = details.get("mean_noise_ratio")
        noise_str = f"{noise_val * 100:.1f}%" if noise_val is not None and math.isfinite(noise_val) else "N/A"
        dbcv_str = _fmt_float(details.get("mean_dbcv_score"), ".4f")
        omori_p_str = _fmt_float(details.get("mean_omori_p"), ".2f")
        omori_r2_str = _fmt_float(details.get("mean_omori_r2"), ".2f")
        jaccard_str = _fmt_float(details.get("mean_jaccard_stability"), ".4f")
        ari_str = _fmt_float(details.get("mean_ari_stability"), ".4f")
        runtime_str = _fmt_float(details.get("runtime_seconds"), ".2f")

        f_rate = details.get("failure_rate")
        fail_rate_str = f"{f_rate * 100:.1f}%" if f_rate is not None and math.isfinite(f_rate) else "N/A"

        lines.extend([
            f"### {algo_name}",
            "",
            f"- **Tổng số cửa sổ:** `{details.get('total_windows', 0)}` (Thành công: `{details.get('success_windows', 0)}`, Thất bại: `{details.get('failure_windows', 0)}`, Tỷ lệ lỗi: `{fail_rate_str}`)",
            f"- **Lý do thất bại ghi nhận:** `{details.get('failure_reasons') or ['None']}`",
            f"- **Tổng dư chấn tìm thấy:** `{details.get('total_aftershocks', 0)}`",
            f"- **Tỷ lệ nhiễu trung bình (Noise ratio):** `{noise_str}`",
            f"- **DBCV trung bình:** `{dbcv_str}` (Số cửa sổ tính được: `{details.get('dbcv_computed_windows', 0)}/{details.get('total_windows', 0)}`)",
            f"- **Modified Omori decay exponent p:** `{omori_p_str}` (R^2 fit: `{omori_r2_str}`, số cửa sổ fit: `{details.get('omori_fitted_windows', 0)}/{details.get('total_windows', 0)}`)",
            f"- **Độ ổn định Jaccard (Mean Jaccard):** `{jaccard_str}`",
            f"- **Độ ổn định ARI (Mean ARI):** `{ari_str}`",
            f"- **Thời gian thực thi:** `{runtime_str}` giây",
            "",
        ])

    lines.extend([
        "## 4. Nhận xét và Giới hạn Thực nghiệm (Pilot Limitations)",
        "",
        "1. **Phân tích so sánh thực tế giữa các mô hình trên Pilot Bundle:**",
    ])

    window_res = algo_details.get("WINDOW", {})
    lines.append(
        f"   - **WINDOW:** Thu nhận toàn bộ ứng viên xảy ra sau mainshock trong hộp không-thời gian "
        f"({window_res.get('total_aftershocks', 0)} sau chấn trên {window_res.get('total_windows', 0)} cửa sổ), "
        f"tỷ lệ nhiễu đạt 0% nhưng không lọc được chấn động nền ngẫu nhiên không gắn kết mật độ."
    )
    dbscan_res = algo_details.get("DBSCAN", {})
    dbscan_noise_val = dbscan_res.get("mean_noise_ratio")
    dbscan_noise = f"{dbscan_noise_val * 100:.1f}%" if dbscan_noise_val is not None and math.isfinite(dbscan_noise_val) else "N/A"
    dbscan_j_str = _fmt_float(dbscan_res.get("mean_jaccard_stability"), ".4f")
    dbscan_a_str = _fmt_float(dbscan_res.get("mean_ari_stability"), ".4f")
    if dbscan_j_str != "N/A" and dbscan_a_str != "N/A":
        stab_desc = f"Độ ổn định Jaccard đạt {dbscan_j_str} và ARI đạt {dbscan_a_str}."
    else:
        dbscan_fail_reasons = dbscan_res.get("failure_reasons", [])
        stab_desc = f"Độ ổn định Jaccard và ARI ghi nhận N/A (lý do: {', '.join(dbscan_fail_reasons) or 'METRIC_FIT_FAILED'})."

    lines.append(
        f"   - **DBSCAN:** Lọc nhiễu hiệu quả (tỷ lệ nhiễu trung bình {dbscan_noise}), "
        f"giữ lại {dbscan_res.get('total_aftershocks', 0)} sau chấn với {dbscan_res.get('success_windows', 0)}/{dbscan_res.get('total_windows', 0)} cửa sổ thành công. "
        f"{stab_desc}"
    )
    hdb_res = algo_details.get("HDBSCAN_GLOBAL", {})
    hdb_fail_val = hdb_res.get("failure_rate")
    hdb_fail_str = f"{hdb_fail_val * 100:.1f}%" if hdb_fail_val is not None and math.isfinite(hdb_fail_val) else "N/A"
    lines.append(
        f"   - **HDBSCAN_GLOBAL:** Đạt {hdb_res.get('success_windows', 0)}/{hdb_res.get('total_windows', 0)} cửa sổ thành công trên cấu hình mặc định "
        f"(tỷ lệ thất bại {hdb_fail_str}, lý do chính: {', '.join(hdb_res.get('failure_reasons', [])) or 'none'})."
    )
    adapt_res = algo_details.get("HDBSCAN_ADAPTIVE", {})
    adapt_fail_val = adapt_res.get("failure_rate")
    adapt_fail_str = f"{adapt_fail_val * 100:.1f}%" if adapt_fail_val is not None and math.isfinite(adapt_fail_val) else "N/A"
    lines.append(
        f"   - **HDBSCAN_ADAPTIVE:** Đạt {adapt_res.get('success_windows', 0)}/{adapt_res.get('total_windows', 0)} cửa sổ thành công trên cấu hình dev "
        f"(tỷ lệ thất bại {adapt_fail_str}, lý do: {', '.join(adapt_res.get('failure_reasons', [])) or 'none'}). "
        f"Cần tinh chỉnh tham số thích ứng (adaptive policy / min_cluster_size) cho các cụm thưa trước khi áp dụng trên tập dữ liệu chính thức."
    )

    lines.extend([
        "",
        "2. **Tính nhất quán vật lý (Modified Omori Law):**",
    ])
    omori_fitted_total = sum(r.get("omori_fitted_windows", 0) for r in algo_details.values())
    if omori_fitted_total > 0:
        fitted_examples = [
            f"`{algo}` (p={_fmt_float(r.get('mean_omori_p'), '.2f')}, R²={_fmt_float(r.get('mean_omori_r2'), '.2f')})"
            for algo, r in algo_details.items()
            if r.get("mean_omori_p") is not None and math.isfinite(r.get("mean_omori_p"))
        ]
        lines.append(
            f"   - Các mô hình có cửa sổ sau chấn đủ mẫu ($N \\ge 5$) đã thực hiện khớp phân rã Omori: {', '.join(fitted_examples)}. "
            f"Các cửa sổ ít hơn 5 sau chấn trả về `METRIC_INSUFFICIENT_MEMBERS` để bảo toàn tính trung thực thống kê."
        )
    else:
        lines.append(
            "   - Không có cửa sổ nào đủ mẫu sau chấn ($N \\ge 5$) để khớp phân rã Omori; toàn bộ trả về `METRIC_INSUFFICIENT_MEMBERS`."
        )

    lines.extend([
        "",
        "3. **Giới hạn Pilot & Bước tiếp theo:**",
        "   - Dữ liệu pilot 14 ngày (2 mainshocks, 279 dòng) chỉ phục vụ kiểm tra tính khả thi và hoàn thiện harness đánh giá (`DEVELOPMENT_ONLY`).",
        "   - Kết quả phân cụm chỉ mang tính ứng viên chuỗi sau chấn (candidate aftershock sequences) hồi cứu, không tuyên bố dự đoán động đất hoặc quan hệ nhân quả vật lý.",
        "   - Việc đánh giá và so sánh khoa học chính thức trên toàn bộ 288 tháng thuộc [EXP-04](../task/tasks/EXP-04.md) và [MLI-04](../task/tasks/MLI-04.md) sau khi official dataset export được nghiệm thu.",
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

    print(f"=== EXP-04: Running Scientific Model Evaluation on {pilot_dir.name} ===")
    bundle = load_feature_bundle(pilot_dir, mode=MODE_DEVELOPMENT)
    manifest = bundle.get("manifest")
    require(manifest is not None and isinstance(manifest, dict), "BUNDLE_MANIFEST_MISSING")

    pilot_id = manifest.get("pilot_id")
    require(pilot_id is not None and pilot_id.startswith("pilot_"), f"INVALID_PILOT_ID:{pilot_id}")

    features_table = bundle.get("features_table")
    mainshocks_table = bundle.get("mainshocks_table")

    pilot_info = {
        "pilot_id": pilot_id,
        "source_dataset_id": manifest.get("source_dataset_id"),
        "candidate_rows": len(features_table) if features_table is not None else 0,
        "mainshock_count": len(mainshocks_table) if mainshocks_table is not None else 0,
        "features_sha256": bundle.get("checksums", {}).get("features-part-00000.parquet"),
    }

    code_prov = get_code_provenance(PROJECT_ROOT)
    configs, config_pins = load_algorithm_configs(project_root=PROJECT_ROOT)
    runtime_meta = get_runtime_metadata(code_version=code_prov["git_commit"], random_seed=args.seed)
    runtime_meta["is_clean_code"] = code_prov["is_clean_code"]
    runtime_meta["critical_code_hashes"] = code_prov["critical_code_hashes"]

    # Run comparative evaluation with exact model configs
    eval_results = compare_all_models_on_pilot(
        bundle_dir=pilot_dir,
        model_configs=configs,
        repeats=args.repeats,
        sigma=args.sigma,
        seed=args.seed,
        project_root=PROJECT_ROOT,
    )
    require(eval_results["pilot_id"] == pilot_id, "PILOT_ID_MISMATCH_IN_EVALUATION")

    full_report = {
        "task_id": "EXP-04",
        "evaluation_scope": "PILOT_DEVELOPMENT",
        "pilot_verification": pilot_info,
        "runtime_metadata": runtime_meta,
        "code_provenance": code_prov,
        "algorithm_config_pins": config_pins,
        "evaluation_parameters": {
            "stability_repeats": args.repeats,
            "perturbation_sigma": args.sigma,
            "random_seed": args.seed,
            "min_omori_aftershocks": 5,
        },
        "evaluation_summary": eval_results,
    }

    # Format JSON and Markdown in memory first to ensure atomic generation
    full_report_json = json.dumps(full_report, indent=2, ensure_ascii=False)
    md_content = generate_markdown_report(eval_results, pilot_info, runtime_meta, config_pins, code_prov=code_prov)

    # Write JSON report
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(full_report_json, encoding="utf-8")
    print(f"[OK] Wrote JSON evidence: {out_json}")

    # Write Markdown report
    out_md.parent.mkdir(parents=True, exist_ok=True)
    out_md.write_text(md_content, encoding="utf-8")
    print(f"[OK] Wrote Markdown report: {out_md}")

    # Print summary table
    print("\n=== REPRODUCTION COMPARISON TABLE ===")
    print(format_markdown_table(eval_results.get("comparison_table", [])))
    print("\n[OK] EXP-04 Evaluation Finished Successfully.")


if __name__ == "__main__":
    main()
