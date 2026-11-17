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
from typing import Any, Dict, List, Optional

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
    MODE_OFFICIAL,
    require,
    sha256_bytes,
    validate_safe_metadata,
)
from notebooks.evaluation import (
    compare_all_models_on_pilot,
    compare_all_models_on_official,
    load_algorithm_configs,
    DEFAULT_STABILITY_REPEATS,
    DEFAULT_STABILITY_SIGMA,
    DEFAULT_STABILITY_SEED,
)

DEFAULT_HANDOFF_SHA256 = "6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297"


def get_code_provenance(project_root: Optional[Path] = None) -> Dict[str, Any]:
    """Capture full code provenance: git commit hash, clean/dirty tree status, and cross-platform code hashes."""
    root = project_root or PROJECT_ROOT
    critical_files = [
        "notebooks/evaluation.py",
        "scripts/evaluate-models.py",
        "notebooks/baseline_models.py",
        "notebooks/adaptive_hdbscan.py",
        "notebooks/experiment_harness.py",
    ]
    model_config_files = [
        "config/ml/models/window_baseline_v1.json",
        "config/ml/models/dbscan_global_dev_v1.json",
        "config/ml/models/hdbscan_global_dev_v1.json",
        "config/ml/models/hdbscan_adaptive_dev_v1.json",
    ]
    all_monitored_files = critical_files + model_config_files

    raw_file_hashes: Dict[str, str] = {}
    lf_file_hashes: Dict[str, str] = {}
    git_blob_hashes: Dict[str, str] = {}
    head_blob_hashes: Dict[str, str] = {}
    detected_line_endings: Dict[str, str] = {}

    missing_files: List[str] = []
    blob_mismatches: List[str] = []

    for rel_path in critical_files:
        f = root / rel_path
        if f.is_file():
            raw_bytes = f.read_bytes()
            lf_bytes = raw_bytes.replace(b"\r\n", b"\n")
            raw_file_hashes[rel_path] = hashlib.sha256(raw_bytes).hexdigest()
            lf_file_hashes[rel_path] = hashlib.sha256(lf_bytes).hexdigest()
            detected_line_endings[rel_path] = "crlf" if b"\r\n" in raw_bytes else "lf"

            git_blob = "unknown"
            try:
                res = subprocess.run(
                    [
                        "git",
                        "-C",
                        str(root),
                        "-c",
                        "core.autocrlf=true",
                        "hash-object",
                        f"--path={rel_path}",
                        str(f),
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                blob_candidate = res.stdout.strip()
                if re.fullmatch(r"[a-f0-9]{40}", blob_candidate):
                    git_blob = blob_candidate
            except Exception:
                pass
            git_blob_hashes[rel_path] = git_blob
        else:
            raw_file_hashes[rel_path] = "MISSING"
            lf_file_hashes[rel_path] = "MISSING"
            git_blob_hashes[rel_path] = "MISSING"
            detected_line_endings[rel_path] = "unknown"
            missing_files.append(rel_path)

    for rel_path in model_config_files:
        f = root / rel_path
        if f.is_file():
            git_blob = "unknown"
            try:
                res = subprocess.run(
                    [
                        "git",
                        "-C",
                        str(root),
                        "-c",
                        "core.autocrlf=true",
                        "hash-object",
                        f"--path={rel_path}",
                        str(f),
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                blob_candidate = res.stdout.strip()
                if re.fullmatch(r"[a-f0-9]{40}", blob_candidate):
                    git_blob = blob_candidate
            except Exception:
                pass
            git_blob_hashes[rel_path] = git_blob
        else:
            git_blob_hashes[rel_path] = "MISSING"
            missing_files.append(rel_path)

    git_commit = "unknown"
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

        # Query committed HEAD blob for all monitored files
        for rel_path in all_monitored_files:
            try:
                head_res = subprocess.run(
                    ["git", "-C", str(root), "rev-parse", f"HEAD:{rel_path}"],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                hb = head_res.stdout.strip()
                if re.fullmatch(r"[a-f0-9]{40}", hb):
                    head_blob_hashes[rel_path] = hb
                else:
                    head_blob_hashes[rel_path] = "UNKNOWN"
            except Exception:
                head_blob_hashes[rel_path] = "UNCOMMITTED"

            wt_blob = git_blob_hashes.get(rel_path)
            if wt_blob != head_blob_hashes[rel_path] or wt_blob in ("unknown", "MISSING"):
                blob_mismatches.append(rel_path)

        # Run git status with -uall across critical files, experiment loader, and model configs
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
                "-uall",
                "--",
                "notebooks/evaluation.py",
                "scripts/evaluate-models.py",
                "notebooks/baseline_models.py",
                "notebooks/adaptive_hdbscan.py",
                "notebooks/experiment_harness.py",
                "config/ml/models/",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        status_lines = [line.strip() for line in status_res.stdout.splitlines() if line.strip()]
        for line in status_lines:
            parts = line.split(maxsplit=1)
            if len(parts) >= 2:
                dirty_files.append(parts[1].strip())
            else:
                dirty_files.append(line)
    except Exception:
        pass

    has_valid_commit = bool(re.fullmatch(r"[a-f0-9]{40}", git_commit))
    has_dirty_status = len(dirty_files) > 0
    has_missing_files = len(missing_files) > 0
    has_blob_mismatches = len(blob_mismatches) > 0

    is_clean_code = (
        has_valid_commit
        and not has_dirty_status
        and not has_missing_files
        and not has_blob_mismatches
    )
    all_dirty_or_mismatched = sorted(list(set(dirty_files + blob_mismatches + missing_files)))

    return {
        "git_commit": git_commit,
        "is_clean_code": is_clean_code,
        "clean_verification_status": "VERIFIED_CLEAN" if is_clean_code else "DIRTY_OR_UNVERIFIED",
        "dirty_files": all_dirty_or_mismatched,
        "missing_files": missing_files,
        "blob_mismatches": blob_mismatches,
        "critical_code_hashes": raw_file_hashes,
        "working_tree_code_hashes": raw_file_hashes,
        "normalized_lf_code_hashes": lf_file_hashes,
        "git_blob_hashes": git_blob_hashes,
        "head_blob_hashes": head_blob_hashes,
        "detected_line_endings": detected_line_endings,
        "cross_platform_verification_note": (
            "Working-tree hashes depend on local filesystem line endings (CRLF on Windows vs LF on Unix/macOS). "
            "For cross-platform reproducibility, verify against git_blob_hashes (via `git hash-object <file>`) "
            "or normalized_lf_code_hashes (via `sha256sum` after LF conversion)."
        ),
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
        file_sha_str = c_info.get("file_bytes_sha256") or c_info.get("config_sha256", "N/A")
        canon_sha_str = c_info.get("canonical_config_sha256", "N/A")
        lines.append(
            f"- **{algo}:** `{c_info['config_path']}` (Version: `{c_info['model_config_version']}`, "
            f"File SHA: `{file_sha_str[:16]}...`, Canonical SHA: `{canon_sha_str[:16]}...`)"
        )

    if code_prov:
        lines.extend([
            "",
            "### Mã nguồn & Kiểm chứng Đa Nền tảng (Cross-Platform Verification):",
            f"- **Git Commit:** `{code_prov.get('git_commit')}` (`is_clean_code={code_prov.get('is_clean_code')}`)",
            f"- **Lưu ý kiểm chứng đa nền tảng:** {code_prov.get('cross_platform_verification_note', '')}",
            "",
            "| Tệp nguồn | Working Tree SHA-256 (Local OS) | Normalized LF SHA-256 | Git Blob SHA-1 | Line Ending |",
            "| :--- | :--- | :--- | :--- | :--- |",
        ])
        wt_hashes = code_prov.get("working_tree_code_hashes", {})
        lf_hashes = code_prov.get("normalized_lf_code_hashes", {})
        blob_hashes = code_prov.get("git_blob_hashes", {})
        endings = code_prov.get("detected_line_endings", {})
        for f_path in wt_hashes:
            wt_short = wt_hashes.get(f_path, "N/A")[:16] + "..." if len(wt_hashes.get(f_path, "")) > 16 else wt_hashes.get(f_path, "N/A")
            lf_short = lf_hashes.get(f_path, "N/A")[:16] + "..." if len(lf_hashes.get(f_path, "")) > 16 else lf_hashes.get(f_path, "N/A")
            b_short = blob_hashes.get(f_path, "N/A")[:16] + "..." if len(blob_hashes.get(f_path, "")) > 16 else blob_hashes.get(f_path, "N/A")
            le_str = endings.get(f_path, "unknown").upper()
            lines.append(f"| `{f_path}` | `{wt_short}` | `{lf_short}` | `{b_short}` | {le_str} |")

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


def generate_official_markdown_report(
    full_report: Dict[str, Any],
) -> str:
    """Render reproducible Markdown evidence report for EXP-04 official REPRODUCTION evaluation."""
    eval_results = full_report["evaluation_summary"]
    ds = full_report["dataset"]
    runtime_meta = full_report["runtime_metadata"]
    config_pins = full_report["algorithm_config_pins"]
    code_prov = full_report.get("code_provenance")
    eval_params = full_report["evaluation_parameters"]
    comp_table = eval_results.get("comparison_table", [])
    algo_details = eval_results.get("algorithm_details", {})
    frozen_rule = full_report["frozen_extension_rule"]
    exp02_cross = full_report.get("exp02_cross_verification")
    exp03_cross = full_report.get("exp03_cross_verification")

    clean_str = "clean" if (code_prov and code_prov.get("is_clean_code")) else "modified/tracked"

    lines = [
        "# EXP-04: Đánh giá Khoa học & So sánh 4 Mô hình trên Official REPRODUCTION",
        "",
        "## 1. Thông tin Thực nghiệm và Danh tính Dataset Chính thức",
        "",
        "- **Mã Task:** [EXP-04](../task/tasks/EXP-04.md) (Khối H - Experiment, P0 Core)",
        f"- **Trạng thái:** `{full_report['status']}` (`{full_report['evaluation_scope']}`)",
        f"- **Thời điểm đánh giá (UTC):** `{eval_results.get('evaluated_at_utc')}`",
        f"- **Dataset Split / Status:** `{ds.get('dataset_split')}` (`{ds.get('dataset_status')}`)",
        f"- **Dataset ID:** `{ds.get('dataset_id')}`",
        f"- **Collection ID:** `{ds.get('collection_id')}` (`{ds.get('shard_count')}` shards)",
        f"- **Gold Publication ID:** `{ds.get('gold_publication_id')}` (Snapshot: `{ds.get('gold_snapshot_id')}`)",
        f"- **Số lượng ứng viên đầu vào:** `{ds.get('feature_row_count')}` dòng (`{ds.get('mainshock_count')}` mainshock windows, Global $M_c = {ds.get('mc_value')}$)",
        f"- **Manifest / Checksums SHA-256:** `{ds.get('manifest_sha256')}` / `{ds.get('checksums_sha256')}`",
    ]
    if full_report.get("handoff_sha256"):
        lines.append(f"- **Team Handoff SHA-256:** `{full_report['handoff_sha256']}`")
    lines.extend([
        f"- **Git Commit Pin:** `{runtime_meta.get('code_version')}` ({clean_str})",
        f"- **Random Seed / Repeats / Sigma:** `{eval_params['random_seed']}` / `{eval_params['stability_repeats']}` / `{eval_params['perturbation_sigma']}`",
        f"- **Python / Packages:** Python `{runtime_meta.get('python_version')}`, `scikit-learn=={runtime_meta.get('package_versions', {}).get('scikit-learn')}`, `hdbscan=={runtime_meta.get('package_versions', {}).get('hdbscan')}`, `scipy=={runtime_meta.get('package_versions', {}).get('scipy')}`",
        f"- **Scientific Approval:** `{str(full_report.get('scientific_approval', False)).lower()}`",
        "",
        "### Cấu hình Thuật toán được Khóa (`frozen_extension_rule` cho EXP-05):",
    ])

    for algo, c_info in config_pins.items():
        file_sha_str = c_info.get("file_bytes_sha256") or c_info.get("config_sha256", "N/A")
        canon_sha_str = c_info.get("canonical_config_sha256", "N/A")
        lines.append(
            f"- **{algo}:** `{c_info['config_path']}` (Version: `{c_info['model_config_version']}`, "
            f"File SHA: `{file_sha_str[:16]}...`, Canonical SHA: `{canon_sha_str[:16]}...`)"
        )

    lines.extend([
        "",
        "## 2. Bảng Đối soát So sánh 4 Thuật toán (Official Reproduction Comparison Table)",
        "",
        format_markdown_table(comp_table),
        "",
        "### Nguyên tắc Đánh giá & Toàn vẹn Mẫu số:",
        "- **Không dùng Accuracy / F1:** Clustering chuỗi sau chấn không có ground-truth nhãn cố định.",
        "- **Bảo toàn mẫu số (`Denominator Integrity`):** Toàn bộ `194` windows (bao gồm window `FAILED` / mainshock noise) giữ nguyên trong mẫu số `failure_rate = failure_windows / total_windows`.",
        "- **Honest NULL & Reason Codes:** Mọi giá trị thiếu/không áp dụng (`WINDOW` DBCV -> `METRIC_NOT_APPLICABLE`, thiếu mẫu Omori $N<5$ hoặc all-noise -> `METRIC_INSUFFICIENT_MEMBERS`, fit thất bại -> `METRIC_FIT_FAILED`) giữ `NULL`/`N/A`, tuyệt đối không ép về `0`.",
        "- **DBCV Memory-Safe Bound:** Sử dụng `hdbscan.validity.validity_index` trực tiếp cho cụm kích thước $\\le 2000$ điểm và thuật toán chunked $O(N)$-memory theo cùng công thức Moulavi (2014) cho cụm lớn hơn để tránh bùng nổ bộ nhớ trên cụm DBSCAN lớn.",
        "",
        "## 3. Phân tích Chi tiết Từng Thuật toán (kèm Reason Codes & Sample Sizes)",
        "",
    ])

    for algo_name, details in algo_details.items():
        noise_val = details.get("mean_noise_ratio")
        noise_str = f"{noise_val * 100:.1f}%" if noise_val is not None and math.isfinite(noise_val) else "N/A"
        dbcv_str = _fmt_float(details.get("mean_dbcv_score"), ".4f")
        omori_p_str = _fmt_float(details.get("mean_omori_p"), ".4f")
        omori_r2_str = _fmt_float(details.get("mean_omori_r2"), ".4f")
        jaccard_str = _fmt_float(details.get("mean_jaccard_stability"), ".4f")
        ari_str = _fmt_float(details.get("mean_ari_stability"), ".4f")
        runtime_str = _fmt_float(details.get("runtime_seconds"), ".2f")
        f_rate = details.get("failure_rate")
        fail_rate_str = f"{f_rate * 100:.1f}%" if f_rate is not None and math.isfinite(f_rate) else "N/A"

        lines.extend([
            f"### {algo_name}",
            "",
            f"- **Cửa sổ:** `{details.get('total_windows', 0)}` (Thành công: `{details.get('success_windows', 0)}`, Thất bại: `{details.get('failure_windows', 0)}`, Tỷ lệ thất bại: `{fail_rate_str}`)",
            f"- **Phân bổ Failure Reason:** `{json.dumps(details.get('failure_reason_counts', {}), ensure_ascii=False, sort_keys=True)}`",
            f"- **Tổng dư chấn POST per-window:** `{details.get('total_aftershocks', 0)}` | **Noise ratio trung bình:** `{noise_str}`",
            f"- **DBCV trung bình:** `{dbcv_str}` (tính được trên `{details.get('dbcv_computed_windows', 0)}/{details.get('total_windows', 0)}` windows; N/A reasons: `{json.dumps(details.get('dbcv_reason_counts', {}), ensure_ascii=False, sort_keys=True)}`)",
            f"- **Modified Omori decay:** mean `p` = `{omori_p_str}`, mean $R^2$ = `{omori_r2_str}` (fit được trên `{details.get('omori_fitted_windows', 0)}/{details.get('total_windows', 0)}` windows; N/A reasons: `{json.dumps(details.get('omori_reason_counts', {}), ensure_ascii=False, sort_keys=True)}`)",
            f"- **Độ ổn định (5x Gaussian jitter $\\sigma={eval_params['perturbation_sigma']}$):** Mean Jaccard = `{jaccard_str}`, Mean ARI = `{ari_str}` (`{details.get('stability_computed_windows', 0)}/{details.get('total_windows', 0)}` windows; reasons: `{json.dumps(details.get('stability_reason_counts', {}), ensure_ascii=False, sort_keys=True)}`)",
            f"- **Runtime (gồm fit + DBCV + Omori + 5x stability):** `{runtime_str}`s",
            "",
        ])

    if exp02_cross or exp03_cross:
        lines.extend(["## 4. Đối soát Liên kết với Official Runs EXP-02 & EXP-03", ""])
        if exp02_cross:
            lines.append(
                f"- **EXP-02 Baseline Parity (`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`):** `all_algos_exact_match={exp02_cross.get('all_algos_exact_match')}`"
            )
        if exp03_cross:
            ms_rec = exp03_cross.get("multi_sequence_reconciliation", {})
            lines.append(
                f"- **EXP-03 `HDBSCAN_ADAPTIVE` Cross-Check & Global Entity Resolution (`{exp03_cross.get('run_id')}`):** "
                f"`exact_match={exp03_cross.get('exact_match')}`, "
                f"`raw_aftershocks={ms_rec.get('raw_aftershocks')}`, "
                f"`resolved_unique_aftershocks={ms_rec.get('resolved_unique_aftershocks')}`, "
                f"`deduplicated_aftershocks={ms_rec.get('aftershocks_deduplicated_count')}`, "
                f"`ambiguous_candidates={ms_rec.get('ambiguous_candidates_count')}`, "
                f"`nested_mainshocks={ms_rec.get('nested_mainshocks_count')}`."
            )
        lines.append("")

    lines.extend([
        "## 5. Frozen Rule cho EXP-05 & Giới hạn Khoa học",
        "",
        f"- **Rule ID:** `{frozen_rule['rule_version']}` (khóa từ `{frozen_rule['frozen_from_split']}` dataset `{frozen_rule['frozen_from_dataset_id']}`).",
        f"- **Contract Bất biến:** `window_model_version={frozen_rule['locked_feature_contract']['window_model_version']}`, `scaling_config_version={frozen_rule['locked_feature_contract']['scaling_config_version']}`, `feature_version={frozen_rule['locked_feature_contract']['feature_version']}`, `mc_value={frozen_rule['locked_feature_contract']['mc_value']}`.",
        "- **Nguyên tắc đọc kết quả:** Không chọn \"mô hình thắng\" chỉ dựa trên một chỉ số đơn lẻ (Window có `0%` mainshock-failure nhưng không lọc nhiễu nền; DBSCAN/HDBSCAN đánh đổi giữa lọc nhiễu, DBCV, khớp Omori và tỷ lệ mainshock bị xếp nhiễu). Kết quả chỉ là ứng viên chuỗi sau chấn hồi cứu; bước chạy khóa tham số sang giai đoạn 2018–2023 thuộc [EXP-05](../task/tasks/EXP-05.md).",
        "",
    ])
    return "\n".join(lines)


def _cross_verify_exp02_exp03(
    eval_results: Dict[str, Any],
    exp02_report_path: Optional[Path],
    exp03_report_path: Optional[Path],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Verify official primary counts in EXP-04 match committed/generated EXP-02 & EXP-03 reports."""
    details = eval_results["algorithm_details"]
    ds_id = eval_results["dataset_id"]

    exp02_cross: Optional[Dict[str, Any]] = None
    if exp02_report_path is not None:
        exp02_data = json.loads(exp02_report_path.read_text(encoding="utf-8"))
        require(exp02_data.get("dataset", {}).get("dataset_id") == ds_id, "EXP02_REPORT_DATASET_ID_MISMATCH")
        per_algo: Dict[str, Any] = {}
        all_ok = True
        for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
            rep_m = exp02_data["reconciliation"][algo]
            ev_m = details[algo]
            matches = (
                rep_m["total_windows"] == ev_m["total_windows"]
                and rep_m["successful_windows"] == ev_m["success_windows"]
                and rep_m["failed_windows"] == ev_m["failure_windows"]
                and rep_m["total_aftershocks"] == ev_m["total_aftershocks"]
            )
            all_ok = all_ok and matches
            per_algo[algo] = {
                "run_id": exp02_data["artifacts_verified"][algo]["run_id"],
                "matches": matches,
                "exp02_aftershocks": rep_m["total_aftershocks"],
                "exp04_aftershocks": ev_m["total_aftershocks"],
            }
        require(all_ok, f"EXP02_EXP04_RECONCILIATION_MISMATCH:{per_algo}")
        exp02_cross = {"all_algos_exact_match": all_ok, "algorithms": per_algo}

    exp03_cross: Optional[Dict[str, Any]] = None
    if exp03_report_path is not None:
        exp03_data = json.loads(exp03_report_path.read_text(encoding="utf-8"))
        require(exp03_data.get("dataset", {}).get("dataset_id") == ds_id, "EXP03_REPORT_DATASET_ID_MISMATCH")
        rep_m = exp03_data["reconciliation"]
        ev_m = details["HDBSCAN_ADAPTIVE"]
        matches = (
            rep_m["total_windows"] == ev_m["total_windows"]
            and rep_m["successful_windows"] == ev_m["success_windows"]
            and rep_m["failed_windows"] == ev_m["failure_windows"]
            and rep_m["raw_aftershocks"] == ev_m["total_aftershocks"]
        )
        require(matches, "EXP03_EXP04_RECONCILIATION_MISMATCH")
        exp03_cross = {
            "run_id": exp03_data["artifact_verified"]["run_id"],
            "exact_match": matches,
            "multi_sequence_reconciliation": exp03_data["multi_sequence_reconciliation"],
        }

    return exp02_cross, exp03_cross


def execute_official_evaluation(
    *,
    bundle_dir: Path,
    output_json: Path,
    output_md: Optional[Path] = None,
    expected_split: str = "REPRODUCTION",
    handoff_path: Optional[Path] = None,
    expected_handoff_sha256: Optional[str] = DEFAULT_HANDOFF_SHA256,
    manifest_sha256: Optional[str] = None,
    checksums_sha256: Optional[str] = None,
    exp02_report_path: Optional[Path] = None,
    exp03_report_path: Optional[Path] = None,
    repeats: int = DEFAULT_STABILITY_REPEATS,
    sigma: float = DEFAULT_STABILITY_SIGMA,
    seed: int = DEFAULT_STABILITY_SEED,
    project_root: Path = PROJECT_ROOT,
) -> Dict[str, Any]:
    """Execute official EXP-04 multi-faceted evaluation and emit locked config freeze report."""
    code_prov = get_code_provenance(project_root)
    configs, config_pins = load_algorithm_configs(project_root=project_root)
    runtime_meta = get_runtime_metadata(code_version=code_prov["git_commit"], random_seed=seed)
    runtime_meta["is_clean_code"] = code_prov["is_clean_code"]
    runtime_meta["critical_code_hashes"] = code_prov["critical_code_hashes"]

    def _on_algo_done(algo_name: str, res: Dict[str, Any]) -> None:
        print(
            f"[{algo_name}] windows={res['total_windows']} success={res['success_windows']} "
            f"fail={res['failure_windows']} dbcv={res['mean_dbcv_score']} "
            f"omori_p={res['mean_omori_p']} jaccard={res['mean_jaccard_stability']} "
            f"time={res['runtime_seconds']}s",
            flush=True,
        )

    eval_results = compare_all_models_on_official(
        bundle_dir=bundle_dir,
        expected_split=expected_split,
        manifest_sha256=manifest_sha256,
        checksums_sha256=checksums_sha256,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
        model_configs=configs,
        config_pins=config_pins,
        repeats=repeats,
        sigma=sigma,
        seed=seed,
        project_root=project_root,
        progress_callback=_on_algo_done,
    )

    exp02_cross, exp03_cross = _cross_verify_exp02_exp03(
        eval_results,
        exp02_report_path=exp02_report_path,
        exp03_report_path=exp03_report_path,
    )

    actual_handoff_sha: Optional[str] = None
    resolved_handoff = handoff_path or (bundle_dir.resolve().parent.parent / "handoff.json")
    if resolved_handoff.is_file():
        actual_handoff_sha = sha256_bytes(resolved_handoff.read_bytes())

    # Summarize without giant raw window_evaluations or keep compact window diagnostics?
    # Note: keep concise summary + compact per-window records (without local filesystem paths).
    full_report: Dict[str, Any] = {
        "report_version": "exp-04-official-v1",
        "task_id": "EXP-04",
        "status": "PASSED",
        "evaluation_scope": "OFFICIAL_RESEARCH",
        "scientific_approval": False,
        "dataset": eval_results["dataset_info"],
        "handoff_sha256": actual_handoff_sha,
        "runtime_metadata": runtime_meta,
        "code_provenance": code_prov,
        "algorithm_config_pins": config_pins,
        "evaluation_parameters": {
            "stability_repeats": repeats,
            "perturbation_sigma": sigma,
            "random_seed": seed,
            "min_omori_aftershocks": 5,
        },
        "frozen_extension_rule": eval_results["frozen_extension_rule"],
        "exp02_cross_verification": exp02_cross,
        "exp03_cross_verification": exp03_cross,
        "evaluation_summary": eval_results,
    }
    validate_safe_metadata(full_report)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(full_report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    if output_md is not None:
        md_content = generate_official_markdown_report(full_report)
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(md_content, encoding="utf-8")

    return full_report


def main():
    parser = argparse.ArgumentParser(description="EXP-04 Scientific Model Evaluation Runner")
    parser.add_argument(
        "--mode",
        choices=["pilot", "official"],
        default="pilot",
        help="Evaluation mode: pilot (DEVELOPMENT_PILOT) or official (EXPORTED research collection)",
    )
    parser.add_argument(
        "--pilot-bundle",
        type=str,
        default="staging/ml-pilot/pilot-kit/bundle",
        help="Path to development pilot bundle directory (when --mode pilot)",
    )
    parser.add_argument(
        "--bundle",
        type=str,
        default=None,
        help="Path to official EXPORTED collection directory (when --mode official)",
    )
    parser.add_argument("--expected-split", type=str, default="REPRODUCTION")
    parser.add_argument("--handoff", type=str, default=None)
    parser.add_argument(
        "--expected-handoff-sha256",
        type=str,
        default=DEFAULT_HANDOFF_SHA256,
        help="Expected handoff.json SHA-256 (pass empty string to skip when testing fixtures)",
    )
    parser.add_argument("--manifest-sha256", type=str, default=None)
    parser.add_argument("--checksums-sha256", type=str, default=None)
    parser.add_argument("--exp02-report", type=str, default=None, help="Optional EXP-02 official JSON for parity check")
    parser.add_argument("--exp03-report", type=str, default=None, help="Optional EXP-03 official JSON for parity check")
    parser.add_argument(
        "--output-json",
        type=str,
        default=None,
        help="Path to output JSON evidence",
    )
    parser.add_argument(
        "--output-md",
        type=str,
        default=None,
        help="Path to output Markdown report",
    )
    parser.add_argument("--repeats", type=int, default=DEFAULT_STABILITY_REPEATS)
    parser.add_argument("--sigma", type=float, default=DEFAULT_STABILITY_SIGMA)
    parser.add_argument("--seed", type=int, default=DEFAULT_STABILITY_SEED)

    args = parser.parse_args()

    if args.mode == "official":
        require(bool(args.bundle), "OFFICIAL_MODE_REQUIRES_BUNDLE")
        bundle_path = (PROJECT_ROOT / args.bundle).resolve() if not Path(args.bundle).is_absolute() else Path(args.bundle)
        default_json = "docs/evidence/EXP-04_OFFICIAL_REPRODUCTION.json"
        default_md = "docs/evidence/EXP-04_OFFICIAL_REPRODUCTION.md"
        out_json_arg = args.output_json or default_json
        out_md_arg = args.output_md or default_md
        out_json = (PROJECT_ROOT / out_json_arg).resolve() if not Path(out_json_arg).is_absolute() else Path(out_json_arg)
        out_md = (PROJECT_ROOT / out_md_arg).resolve() if not Path(out_md_arg).is_absolute() else Path(out_md_arg)
        handoff_p = Path(args.handoff).resolve() if args.handoff else None
        exp02_p = Path(args.exp02_report).resolve() if args.exp02_report else None
        exp03_p = Path(args.exp03_report).resolve() if args.exp03_report else None
        exp_handoff_sha = args.expected_handoff_sha256.strip() or None

        print(f"=== EXP-04: Running Official Scientific Model Evaluation on {bundle_path.name} ({args.expected_split}) ===", flush=True)
        full_report = execute_official_evaluation(
            bundle_dir=bundle_path,
            output_json=out_json,
            output_md=out_md,
            expected_split=args.expected_split,
            handoff_path=handoff_p,
            expected_handoff_sha256=exp_handoff_sha,
            manifest_sha256=args.manifest_sha256,
            checksums_sha256=args.checksums_sha256,
            exp02_report_path=exp02_p,
            exp03_report_path=exp03_p,
            repeats=args.repeats,
            sigma=args.sigma,
            seed=args.seed,
            project_root=PROJECT_ROOT,
        )
        print(f"[OK] Wrote Official JSON evidence: {out_json}")
        print(f"[OK] Wrote Official Markdown report: {out_md}")
        print("\n=== OFFICIAL REPRODUCTION COMPARISON TABLE ===")
        print(format_markdown_table(full_report["evaluation_summary"].get("comparison_table", [])))
        print("\n[OK] EXP-04 Official Evaluation Finished Successfully.")
        return

    out_json_arg = args.output_json or "docs/evidence/EXP-04_PILOT_EVALUATION.json"
    out_md_arg = args.output_md or "docs/evidence/EXP-04_PILOT_EVALUATION.md"
    pilot_dir = (PROJECT_ROOT / args.pilot_bundle).resolve() if not Path(args.pilot_bundle).is_absolute() else Path(args.pilot_bundle)
    out_json = (PROJECT_ROOT / out_json_arg).resolve() if not Path(out_json_arg).is_absolute() else Path(out_json_arg)
    out_md = (PROJECT_ROOT / out_md_arg).resolve() if not Path(out_md_arg).is_absolute() else Path(out_md_arg)

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

    # Run comparative evaluation with exact model configs and propagate pins
    eval_results = compare_all_models_on_pilot(
        bundle_dir=pilot_dir,
        model_configs=configs,
        config_pins=config_pins,
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
