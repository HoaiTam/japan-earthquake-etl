#!/usr/bin/env python3
"""EXP-05 Official Runner: Frozen out-of-period evaluation on EXTENSION (2018-10-01..2023-12-31).

Enforces the fail-closed REPRODUCTION freeze gate (`exp04-frozen-reproduction-rule-v2`),
executes all 4 frozen algorithms (`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`, `HDBSCAN_ADAPTIVE`)
into sibling MLI-03 result bundles without tuning or parameter overrides, evaluates quality,
Omori physical consistency, and stability across all EXTENSION windows, and compares splits.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))

from evaluation import (  # noqa: E402
    DEFAULT_STABILITY_REPEATS,
    DEFAULT_STABILITY_SEED,
    DEFAULT_STABILITY_SIGMA,
    MIN_OMORI_AFTERSHOCKS,
)
from experiment_harness import (  # noqa: E402
    collect_code_provenance,
    get_runtime_metadata,
    require,
    resolve_verified_handoff_sha256,
    validate_safe_metadata,
)
from extension_evaluation import (  # noqa: E402
    ALGORITHM_ORDER,
    assert_no_forbidden_metrics,
    run_extension_bundles_and_evaluate,
)

DEFAULT_HANDOFF_SHA256 = "6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297"
DEFAULT_EXP04_REPORT_CRLF_SHA256 = "b5ec696aa844c6ff1541d71e8fa98f1eea6e3f69587cdc1be19a51524f6257f5"
DEFAULT_EXP04_REPORT_LF_SHA256 = "e99ba8ac14f62f132369faaa0b4b37f4378908e8a97d950623e1d75678f6f3b1"

MODEL_CONFIG_FILES = [
    "config/ml/models/window_baseline_v1.json",
    "config/ml/models/dbscan_global_dev_v1.json",
    "config/ml/models/hdbscan_global_dev_v1.json",
    "config/ml/models/hdbscan_adaptive_dev_v1.json",
]

CRITICAL_PROVENANCE_FILES = [
    "notebooks/experiment_harness.py",
    "notebooks/research_collection.py",
    "notebooks/baseline_models.py",
    "notebooks/adaptive_hdbscan.py",
    "notebooks/evaluation.py",
    "notebooks/extension_evaluation.py",
    "scripts/run-exp05-extension.py",
    *MODEL_CONFIG_FILES,
]


def get_code_provenance(root: Path) -> Dict[str, Any]:
    """Capture strict git commit, blob hashes, LF-normalized hashes, and clean status."""
    return collect_code_provenance(
        root,
        CRITICAL_PROVENANCE_FILES,
        config_files=MODEL_CONFIG_FILES,
        status_paths=[*CRITICAL_PROVENANCE_FILES, "config/ml/models/"],
        clean_status_label="VERIFIED_CLEAN",
    )


def resolve_default_exp04_report_sha256(
    exp04_report_path: Path,
    explicit_sha256: Optional[str] = None,
) -> str:
    """Resolve trusted EXP-04 report SHA-256 while respecting OS line-ending checkouts."""
    if isinstance(explicit_sha256, str) and explicit_sha256.strip():
        return explicit_sha256.strip()
    require(exp04_report_path.is_file(), f"EXP05_EXP04_REPORT_NOT_FOUND:{exp04_report_path}")
    raw = exp04_report_path.read_bytes()
    return DEFAULT_EXP04_REPORT_CRLF_SHA256 if b"\r\n" in raw else DEFAULT_EXP04_REPORT_LF_SHA256


def _fmt_float(val: Any, fmt: str = ".4f", default: str = "N/A") -> str:
    if val is None or isinstance(val, bool):
        return default
    try:
        f = float(val)
    except (TypeError, ValueError):
        return default
    return f"{f:{fmt}}" if math.isfinite(f) else default


def _fmt_delta(val: Any, fmt: str = "+.4f", default: str = "N/A") -> str:
    if val is None or isinstance(val, bool):
        return default
    try:
        f = float(val)
    except (TypeError, ValueError):
        return default
    return f"{f:{fmt}}" if math.isfinite(f) else default


def format_markdown_table(rows: List[Dict[str, Any]]) -> str:
    """Format list of dicts as clean GitHub-flavored Markdown table."""
    if not rows:
        return ""
    headers = list(rows[0].keys())
    header_line = "| " + " | ".join(headers) + " |"
    sep_line = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = ["| " + " | ".join(str(r.get(h, "")) for h in headers) + " |" for r in rows]
    return "\n".join([header_line, sep_line, *body])


def render_extension_markdown(report: Dict[str, Any]) -> str:
    """Render reproducible Markdown evidence report for EXP-05 official EXTENSION run."""
    ds = report["dataset"]
    rt = report["runtime_metadata"]
    prov = report.get("code_provenance") or {}
    fg = report["frozen_rule_verification"]
    drift = report["data_drift"]
    r_prof = drift["reproduction"]
    e_prof = drift["extension"]
    r_dist = r_prof["window_candidate_distribution"]
    e_dist = e_prof["window_candidate_distribution"]
    shifts = drift["shifts"]
    algo_comps = report["algorithm_comparisons"]
    ms_summary = report.get("adaptive_multi_sequence_summary") or {}

    lines: List[str] = [
        "# EXP-05 — Đánh giá Out-of-Period (2018–2023) theo Rule đã Khóa trên Tập EXTENSION",
        "",
        "## 1. Phạm vi, Danh tính Dataset & Freeze Gate",
        "",
        "- **Task:** [EXP-05](../task/tasks/EXP-05.md) (Tuần 7, Khối H — Out-of-period evaluation, P0 Core)",
        f"- **Report Version / Trạng thái:** `{report.get('report_version')}` / `{report['status']}` (`{report['evaluation_scope']}`)",
        f"- **Thời điểm chạy (UTC):** `{rt.get('timestamp_utc')}`",
        f"- **Git Commit Pin / Code Status:** `{prov.get('git_commit')}` (`{prov.get('clean_verification_status')}`, `is_clean_code={prov.get('is_clean_code')}`)",
        f"- **Môi trường Python:** Python `{rt.get('python_version')}`, `scikit-learn=={rt.get('package_versions', {}).get('scikit-learn')}`, `hdbscan=={rt.get('package_versions', {}).get('hdbscan')}`, `pyarrow=={rt.get('package_versions', {}).get('pyarrow')}`",
        f"- **Scientific Approval:** `{str(report['scientific_approval']).lower()}`",
        f"- **Frozen Rule Source:** `{fg['frozen_rule_version']}` từ [EXP-04_OFFICIAL_REPRODUCTION.json](./EXP-04_OFFICIAL_REPRODUCTION.json) (`SHA-256={fg['exp04_report_sha256']}`, `LF SHA-256={fg['exp04_report_normalized_lf_sha256']}`)",
        f"- **Parameter Tuning trên EXTENSION:** `{str(fg['parameter_tuning_applied_on_extension']).lower()}` (`config_freeze={fg['config_freeze_verified']}`, `eval_param_freeze={fg['evaluation_param_freeze_verified']}`, `feature_contract_freeze={fg['feature_contract_freeze_verified']}`, `exp02_cross_verify={fg.get('exp02_cross_verify_passed')}`, `exp03_cross_verify={fg.get('exp03_cross_verify_passed')}`)",
        "",
        "### Thông số `EXTENSION` Collection (`EXPORTED`)",
        "",
        f"- **Dataset Split / Status:** `{ds['dataset_split']}` / `{ds['dataset_status']}`",
        f"- **Dataset ID:** `{ds['dataset_id']}` (khác biệt hoàn toàn với REPRODUCTION `{fg['frozen_from_dataset_id']}`)",
        f"- **Collection ID:** `{ds['collection_id']}` (`{ds['shard_count']}` shards)",
        f"- **Gold Publication ID / Snapshot ID:** `{ds.get('gold_publication_id')}` / `{ds.get('gold_snapshot_id')}` (`gold_pin_verified={fg['gold_pin_verified']}`)",
        f"- **Khoảng thời gian split / Quan sát thực tế:** `{e_prof.get('period_start_utc')}` .. `{e_prof.get('period_end_utc_exclusive')}` (`{e_prof.get('required_month_count')}` tháng; mainshock `{e_prof.get('observed_first_mainshock_utc')}` .. `{e_prof.get('observed_last_mainshock_utc')}`)",
        f"- **Khối lượng dữ liệu:** `{ds['mainshock_count']}` mainshocks, `{ds['feature_row_count']}` candidate rows (`feature_version={ds['feature_version']}`, `window_model_version={ds['window_model_version']}`, `scaling_config_version={ds['scaling_config_version']}`)",
        f"- **Manifest / Checksums / Handoff SHA-256:** `{ds['manifest_sha256']}` / `{ds['checksums_sha256']}` / `{report.get('handoff_sha256')}`",
        "",
        "## 2. Biến động Dữ liệu giữa `REPRODUCTION` và `EXTENSION` (Data Drift)",
        "",
        "| Đặc trưng phân phối | REPRODUCTION (2000–2018) | EXTENSION (2018–2023) | Chênh lệch / Tỷ lệ |",
        "|---|---:|---:|---:|",
        f"| Mainshock Windows | {r_prof['mainshock_count']} | {e_prof['mainshock_count']} | {shifts['mainshock_count_delta']:+d} |",
        f"| Tổng Candidate Rows | {r_prof['feature_row_count']} | {e_prof['feature_row_count']} | {shifts['feature_row_count_delta']:+d} ({shifts['feature_row_ratio']:.2f}x) |",
        f"| Global Completeness $M_c$ | {r_prof['mc_value']} | {e_prof['mc_value']} | {_fmt_delta(shifts['mc_value_delta'], '+.2f')} |",
        f"| Candidates/Window (Min / Median / Mean / Max) | {r_dist['min']} / {r_dist['median']} / {r_dist['mean']} / {r_dist['max']} | {e_dist['min']} / {e_dist['median']} / {e_dist['mean']} / {e_dist['max']} | Mean `{shifts['mean_candidates_per_window_ratio']}x`, Max `{shifts['max_candidates_per_window_ratio']}x` |",
        f"| Mega-Windows (`>10k` / `>50k` rows) | {r_dist['windows_gt_10k']} / {r_dist['windows_gt_50k']} | {e_dist['windows_gt_10k']} / {e_dist['windows_gt_50k']} | {shifts['windows_gt_10k_delta']:+d} / {shifts['windows_gt_50k_delta']:+d} |",
        "",
        "## 3. Kết quả 4 Thuật toán trên `EXTENSION` và Đối chiếu Delta với `REPRODUCTION`",
        "",
    ]

    ext_rows: List[Dict[str, Any]] = []
    delta_rows: List[Dict[str, Any]] = []
    for algo in ALGORITHM_ORDER:
        comp = algo_comps[algo]
        repro = comp["reproduction"]
        ext = comp["extension"]
        d = comp["deltas"]
        ext_rows.append({
            "Algorithm": algo,
            "Success/Total": f"{ext['success_windows']}/{ext['total_windows']}",
            "Failure Rate": f"{100.0 * ext['failure_rate']:.1f}%",
            "Aftershocks (POST)": ext["total_aftershocks"],
            "Mean Noise": f"{100.0 * ext['mean_noise_ratio']:.1f}%" if ext["mean_noise_ratio"] is not None else "N/A",
            "Mean DBCV (n)": f"{_fmt_float(ext['mean_dbcv_score'])} ({ext['dbcv_computed_windows']})",
            "Omori p / R² (n)": f"{_fmt_float(ext['mean_omori_p'], '.2f')} / {_fmt_float(ext['mean_omori_r2'], '.2f')} ({ext['omori_fitted_windows']})",
            "Jaccard / ARI": f"{_fmt_float(ext['mean_jaccard_stability'])} / {_fmt_float(ext['mean_ari_stability'])}",
            "Runtime (s)": _fmt_float(ext["runtime_seconds"], ".2f"),
        })
        delta_rows.append({
            "Algorithm": algo,
            "Fail Rate Delta": _fmt_delta(d["failure_rate"]),
            "Noise Ratio Delta": _fmt_delta(d["mean_noise_ratio"]),
            "DBCV Delta": _fmt_delta(d["mean_dbcv_score"]),
            "Omori p Delta": _fmt_delta(d["mean_omori_p"], "+.2f"),
            "Jaccard Delta": _fmt_delta(d["mean_jaccard_stability"]),
            "ARI Delta": _fmt_delta(d["mean_ari_stability"]),
            "Runtime Ratio (Ext/Repro)": f"{d['runtime_ratio']:.2f}x" if d["runtime_ratio"] is not None else "N/A",
        })

    lines.append(format_markdown_table(ext_rows))
    lines.extend(["", "### Delta `EXTENSION` trừ `REPRODUCTION`", "", format_markdown_table(delta_rows)])

    r_ms = ms_summary.get("reproduction") or {}
    e_ms = ms_summary.get("extension") or {}
    if e_ms:
        lines.extend([
            "",
            "## 4. Phân giải Đa chuỗi Toàn cục `HDBSCAN_ADAPTIVE` trên `EXTENSION`",
            "",
            "| Chỉ số | REPRODUCTION | EXTENSION |",
            "|---|---:|---:|",
            f"| Raw Sequence Members | {r_ms.get('raw_sequence_members', 'N/A')} | {e_ms.get('raw_sequence_members')} |",
            f"| Raw Aftershocks (POST trước khử trùng lặp) | {r_ms.get('raw_aftershocks', 'N/A')} | {e_ms.get('raw_aftershocks')} |",
            f"| Resolved Unique Aftershocks (Rank 1) | {r_ms.get('resolved_unique_aftershocks', 'N/A')} | {e_ms.get('resolved_unique_aftershocks')} |",
            f"| Aftershocks Deduplicated (Secondary Ranks) | {r_ms.get('aftershocks_deduplicated_count', 'N/A')} | {e_ms.get('aftershocks_deduplicated_count')} |",
            f"| Ambiguous Candidates / Aftershocks | {r_ms.get('ambiguous_candidates_count', 'N/A')} / {r_ms.get('ambiguous_aftershocks_count', 'N/A')} | {e_ms.get('ambiguous_candidates_count')} / {e_ms.get('ambiguous_aftershocks_count')} |",
            f"| Nested Mainshocks Detected | {r_ms.get('nested_mainshocks_count', 'N/A')} | {e_ms.get('nested_mainshocks_count')} |",
        ])

    lines.extend([
        "",
        "## 5. Bảng Quyết định & Các Trường hợp Thất bại (Decision Table)",
        "",
        format_markdown_table(report["decision_table"]),
        "",
        "### Chi tiết Failure Cases & Honest NULL Reasons trên `EXTENSION`",
        "",
    ])
    for algo in ALGORITHM_ORDER:
        fc = report["failure_cases_summary"][algo]
        lines.append(
            f"- **{algo}:** failures=`{fc['extension_failures']}`, failure_reasons=`{fc['extension_failure_reasons']}`, "
            f"dbcv_null_reasons=`{fc['extension_dbcv_null_reasons']}`, omori_null_reasons=`{fc['extension_omori_null_reasons']}`, "
            f"stability_null_reasons=`{fc['extension_stability_null_reasons']}`."
        )

    lines.extend([
        "",
        "## 6. Đối soát Bundle MLI-03 Full-Split & Per-Shard",
        "",
        "| Algorithm | Full Run ID | Full `_SUCCESS.json` SHA-256 (prefix) | Primary Membership SHA-256 (prefix) | Shards Verified | Parity |",
        "|---|---|---|---|---:|---|",
    ])
    for algo in ALGORITHM_ORDER:
        bv = algo_comps[algo]["bundle_verification"]
        lines.append(
            f"| `{algo}` | `{bv['experiment_run_id']}` | `{bv['full_bundle_success_sha256'][:16]}...` | `{bv['primary_membership_sha256'][:16]}...` | {bv['shard_count']} (`{bv['physical_shards_verified']}`) | `{bv['primary_membership_parity_verified']}` |"
        )

    lines.extend([
        "",
        "- **Phạm vi kiểm chứng MLI-03:** Bảng trên phản ánh kiểm tra readback/checksum/Parquet/primary-digest phía Python và `MlResultBundleReader` / `ResultBundleContract` offline (không thay thế bước chạy Spark/Airflow import thật các bundle `EXTENSION` vào Iceberg `ml.*` hay validator overlap xuyên shard cho từng shard `hdbscan_adaptive_shards/` tách lẻ).",
        "",
        "## 7. Ranh giới Phương pháp luận & Hạn chế Khoa học",
        "",
        "1. **Out-of-Period Temporal Generalization Check (Unsupervised):** Kiểm thử trên `EXTENSION` đo tính ổn định tổng quát hóa theo thời gian khi khóa nguyên Rule/Config từ `EXP-04`; **không phải** độ chính xác có nhãn (cấm tuyệt đối `accuracy`/`F1`).",
        "2. **Không Tune lại Tham số:** Toàn bộ 4 cấu hình và tham số stability/Omori giữ nguyên theo `exp04-frozen-reproduction-rule-v2`; các dịch chuyển như hạ $M_c$ (`0.5 -> 0.4`) và mega-windows (`83.504` rows) phản ánh đúng thực tế vận hành ngoài mẫu.",
        "3. **Ứng viên Chuỗi Hồi cứu & Không Phê duyệt Độc lập:** Mọi đầu ra chỉ là ứng viên chuỗi sau chấn hồi cứu phục vụ kiểm chứng khoa học ([HDBSCAN_WORKSTREAM.md](../task/HDBSCAN_WORKSTREAM.md)), **không** tuyên bố dự đoán động đất hay quan hệ nhân quả vật lý (`scientific_approval=false`).",
        "",
    ])
    return "\n".join(lines)


def execute_official_extension(
    *,
    bundle_dir: Path,
    exp04_report_path: Path,
    expected_exp04_report_sha256: Optional[str],
    output_dir: Path,
    output_json: Path,
    output_md: Optional[Path] = None,
    handoff_path: Optional[Path] = None,
    expected_handoff_sha256: Optional[str] = DEFAULT_HANDOFF_SHA256,
    manifest_sha256: Optional[str] = None,
    checksums_sha256: Optional[str] = None,
    repeats: int = DEFAULT_STABILITY_REPEATS,
    sigma: float = DEFAULT_STABILITY_SIGMA,
    seed: int = DEFAULT_STABILITY_SEED,
    min_omori_aftershocks: int = MIN_OMORI_AFTERSHOCKS,
    allow_overwrite: bool = False,
    is_retry: bool = False,
    project_root: Path = PROJECT_ROOT,
) -> Dict[str, Any]:
    """Execute full EXP-05 official EXTENSION pipeline and persist JSON + Markdown evidence."""
    trusted_exp04_sha = resolve_default_exp04_report_sha256(
        exp04_report_path,
        explicit_sha256=expected_exp04_report_sha256,
    )
    code_prov = get_code_provenance(project_root)
    code_ver = code_prov["git_commit"]
    runtime_meta = get_runtime_metadata(code_version=code_ver, random_seed=seed)
    runtime_meta["is_clean_code"] = code_prov["is_clean_code"]
    runtime_meta["critical_code_hashes"] = code_prov["critical_code_hashes"]

    def _log_progress(stage: str, payload: Dict[str, Any]) -> None:
        print(f"[EXP-05] stage={stage} details={json.dumps(payload, default=str)}", flush=True)

    run_res = run_extension_bundles_and_evaluate(
        bundle_dir=bundle_dir,
        exp04_report_path=exp04_report_path,
        expected_exp04_report_sha256=trusted_exp04_sha,
        output_dir=output_dir,
        manifest_sha256=manifest_sha256,
        checksums_sha256=checksums_sha256,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
        custom_configs=None,
        repeats=repeats,
        sigma=sigma,
        seed=seed,
        min_omori_aftershocks=min_omori_aftershocks,
        code_version=code_ver,
        allow_overwrite=allow_overwrite,
        is_retry=is_retry,
        project_root=project_root,
        progress_callback=_log_progress,
    )

    actual_handoff_sha = resolve_verified_handoff_sha256(
        bundle_dir,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
    )
    fg = run_res["freeze_gate"]
    comp = run_res["comparison"]

    full_report = {
        "report_version": "exp05-official-extension-v1",
        "task_id": "EXP-05",
        "status": "PASSED",
        "evaluation_scope": "OFFICIAL_EXTENSION_OUT_OF_PERIOD",
        "scientific_approval": False,
        "dataset": run_res["extension_bundle_dataset"],
        "handoff_sha256": actual_handoff_sha,
        "runtime_metadata": runtime_meta,
        "code_provenance": code_prov,
        "algorithm_config_pins": fg["config_pins"],
        "evaluation_parameters": fg["evaluation_parameters"],
        "frozen_rule_verification": comp["freeze_gate"],
        "data_drift": comp["data_drift"],
        "algorithm_comparisons": comp["algorithm_comparisons"],
        "adaptive_multi_sequence_summary": comp["adaptive_multi_sequence_summary"],
        "decision_table": comp["decision_table"],
        "failure_cases_summary": comp["failure_cases_summary"],
        "guardrails": comp["guardrails"],
        "extension_evaluation_summary": run_res["extension_evaluation"],
    }
    assert_no_forbidden_metrics(full_report)
    validate_safe_metadata(full_report)

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(full_report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    if output_md is not None:
        md_content = render_extension_markdown(full_report).rstrip("\n")
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(md_content + "\n", encoding="utf-8", newline="\n")

    return full_report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path, help="Path to EXPORTED EXTENSION collection bundle")
    parser.add_argument(
        "--exp04-report",
        type=Path,
        default=PROJECT_ROOT / "docs/evidence/EXP-04_OFFICIAL_REPRODUCTION.json",
        help="Path to frozen EXP-04 official reproduction JSON report",
    )
    parser.add_argument(
        "--expected-exp04-report-sha256",
        type=str,
        default=None,
        help="Explicit trusted SHA-256 of EXP-04 JSON report (defaults to pinned LF/CRLF hashes)",
    )
    parser.add_argument("--handoff", type=Path, default=None, help="Optional path to handoff.json")
    parser.add_argument(
        "--expected-handoff-sha256",
        type=str,
        default=DEFAULT_HANDOFF_SHA256,
        help="Expected SHA-256 of handoff.json (pass empty string to skip on test fixtures)",
    )
    parser.add_argument("--manifest-sha256", type=str, default=None)
    parser.add_argument("--checksums-sha256", type=str, default=None)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "staging/ml-experiments/exp05-official-extension",
        help="Staging root for EXP-05 sibling MLI-03 result bundles",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=PROJECT_ROOT / "docs/evidence/EXP-05_EXTENSION_EVALUATION.json",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=PROJECT_ROOT / "docs/evidence/EXP-05_EXTENSION_EVALUATION.md",
    )
    parser.add_argument("--repeats", type=int, default=DEFAULT_STABILITY_REPEATS)
    parser.add_argument("--sigma", type=float, default=DEFAULT_STABILITY_SIGMA)
    parser.add_argument("--seed", type=int, default=DEFAULT_STABILITY_SEED)
    parser.add_argument("--min-omori-aftershocks", type=int, default=MIN_OMORI_AFTERSHOCKS)
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--retry-reuse", action="store_true", help="Reuse matching completed bundle outputs")
    args = parser.parse_args()

    exp_handoff_sha = args.expected_handoff_sha256.strip() or None
    out_dir = (PROJECT_ROOT / args.output_dir).resolve() if not args.output_dir.is_absolute() else args.output_dir.resolve()
    out_json = (PROJECT_ROOT / args.output_json).resolve() if not args.output_json.is_absolute() else args.output_json.resolve()
    out_md = (PROJECT_ROOT / args.output_md).resolve() if not args.output_md.is_absolute() else args.output_md.resolve()
    exp04_p = (PROJECT_ROOT / args.exp04_report).resolve() if not args.exp04_report.is_absolute() else args.exp04_report.resolve()
    bundle_p = (PROJECT_ROOT / args.bundle).resolve() if not args.bundle.is_absolute() else args.bundle.resolve()

    print(f"=== EXP-05: Running Frozen Out-of-Period Evaluation on {bundle_p.name} ===", flush=True)
    report = execute_official_extension(
        bundle_dir=bundle_p,
        exp04_report_path=exp04_p,
        expected_exp04_report_sha256=args.expected_exp04_report_sha256,
        output_dir=out_dir,
        output_json=out_json,
        output_md=out_md,
        handoff_path=args.handoff,
        expected_handoff_sha256=exp_handoff_sha,
        manifest_sha256=args.manifest_sha256,
        checksums_sha256=args.checksums_sha256,
        repeats=args.repeats,
        sigma=args.sigma,
        seed=args.seed,
        min_omori_aftershocks=args.min_omori_aftershocks,
        allow_overwrite=args.allow_overwrite,
        is_retry=args.retry_reuse,
        project_root=PROJECT_ROOT,
    )
    print(f"[OK] Wrote Official JSON evidence: {out_json}", flush=True)
    print(f"[OK] Wrote Official Markdown report: {out_md}", flush=True)
    print("\n=== EXP-05 DECISION TABLE ===", flush=True)
    print(format_markdown_table(report["decision_table"]), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
