#!/usr/bin/env python3
"""EXP-02 Official Runner: Execute WINDOW, DBSCAN, and HDBSCAN_GLOBAL on REPRODUCTION EXPORTED collection."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOKS_DIR = PROJECT_ROOT / "notebooks"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))

from experiment_harness import (  # noqa: E402
    MODE_OFFICIAL,
    canonical,
    collect_code_provenance,
    load_feature_bundle,
    require,
    resolve_verified_handoff_sha256,
    sha256_bytes,
    validate_safe_metadata,
)
from baseline_models import (  # noqa: E402
    generate_official_baseline_report,
    run_baseline_experiment,
)

DEFAULT_HANDOFF_SHA256 = "6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297"
BASELINE_CONFIG_PATHS = {
    "WINDOW": "config/ml/models/window_baseline_v1.json",
    "DBSCAN": "config/ml/models/dbscan_global_dev_v1.json",
    "HDBSCAN_GLOBAL": "config/ml/models/hdbscan_global_dev_v1.json",
}
CRITICAL_PROVENANCE_FILES = [
    "notebooks/experiment_harness.py",
    "notebooks/research_collection.py",
    "notebooks/baseline_models.py",
    "scripts/run-exp02-official.py",
    "config/ml/models/window_baseline_v1.json",
    "config/ml/models/dbscan_global_dev_v1.json",
    "config/ml/models/hdbscan_global_dev_v1.json",
]


def resolve_git_commit(root: Path) -> str:
    """Resolve current HEAD commit SHA-1 or fallback to dev-local."""
    try:
        res = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            timeout=15,
        )
        commit = res.stdout.strip()
        if re.fullmatch(r"[a-f0-9]{40}", commit):
            return commit
    except Exception:
        pass
    return "dev-local"


def get_code_provenance(root: Path) -> Dict[str, Any]:
    """Capture strict git commit, blob hashes, LF-normalized hashes, and clean status for critical EXP-02 files."""
    return collect_code_provenance(
        root,
        CRITICAL_PROVENANCE_FILES,
        clean_status_label="CLEAN_VERIFIED",
    )


def load_baseline_configs(root: Path) -> tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Load baseline model configs and compute exact file/canonical SHA-256 pins."""
    configs: Dict[str, Dict[str, Any]] = {}
    pins: Dict[str, Dict[str, Any]] = {}
    for algo, rel_path in BASELINE_CONFIG_PATHS.items():
        cfg_file = root / rel_path
        require(cfg_file.is_file(), f"MISSING_MODEL_CONFIG:{rel_path}")
        raw = cfg_file.read_bytes()
        cfg = json.loads(raw.decode("utf-8"))
        configs[algo] = cfg
        pins[algo] = {
            "config_path": rel_path,
            "algorithm_version": str(cfg.get("algorithm_version", "1.0.0")),
            "model_config_version": str(cfg.get("model_config_version")),
            "file_bytes_sha256": hashlib.sha256(raw).hexdigest(),
            "canonical_config_sha256": sha256_bytes(canonical(cfg).encode("utf-8")),
        }
    return configs, pins


def render_official_markdown(report: Dict[str, Any]) -> str:
    """Render reproducible Markdown evidence report for EXP-02 official REPRODUCTION run."""
    ds = report["dataset"]
    prov = report.get("code_provenance") or {}
    lines: List[str] = [
        "# EXP-02 — Kết quả Official Baselines trên REPRODUCTION Collection",
        "",
        "- **Task:** [EXP-02](../task/tasks/EXP-02.md) (Khối H - Experiment, P0 Core)",
        f"- **Report Version / Trạng thái:** `{report.get('report_version')}` / `{report['status']}` (`{report['evaluation_scope']}`)",
        f"- **Thời điểm tạo (UTC):** `{report['created_at_utc']}`",
        f"- **Git Commit Pin / Code Status:** `{report['code_version']}` (`{prov.get('clean_verification_status', 'UNSPECIFIED')}`)",
        f"- **Python / HDBSCAN Runtime:** Python `{report['python_version']}`, `scikit-learn=={report['package_versions'].get('scikit-learn')}`, `hdbscan=={report['package_versions'].get('hdbscan')}`, `pyarrow=={report['package_versions'].get('pyarrow')}`",
        f"- **Random Seed:** `{report['random_seed']}`",
        f"- **Scientific Approval:** `{str(report['scientific_approval']).lower()}`",
        "",
        "## 1. Danh tính Dataset & Pins",
        "",
        f"- **Split:** `{ds['dataset_split']}` (`{ds['dataset_status']}`)",
        f"- **Dataset ID:** `{ds['dataset_id']}`",
        f"- **Collection ID:** `{ds['collection_id']}`",
        f"- **Gold Publication ID:** `{ds['gold_publication_id']}` (Snapshot: `{ds['gold_snapshot_id']}`)",
        f"- **Tổng số dòng đặc trưng:** `{ds['feature_row_count']}` (`{ds['mainshock_count']}` mainshock windows, `{ds['shard_count']}` shards)",
        f"- **Global Completeness $M_c$:** `{ds['mc_value']}`",
        f"- **Window / Scaling / Feature Versions:** `{ds['window_model_version']}` / `{ds['scaling_config_version']}` / `{ds['feature_version']}`",
        f"- **Timestamp Reader Profile:** `{ds['timestamp_reader_profile']}`",
        f"- **Manifest SHA-256:** `{ds['manifest_sha256']}`",
        f"- **Checksums SHA-256:** `{ds['checksums_sha256']}`",
    ]
    if ds.get("handoff_sha256"):
        lines.append(f"- **Team Handoff SHA-256:** `{ds['handoff_sha256']}`")

    lines.extend(["", "## 2. Cấu hình Thuật toán & Code Provenance đã Khóa", ""])
    for algo, pin in report.get("model_config_pins", {}).items():
        lines.append(
            f"- **{algo}:** `{pin['config_path']}` (`{pin['model_config_version']}`, File SHA: `{pin['file_bytes_sha256']}`)"
        )
    if prov:
        lines.append(f"- **Clean Verification:** `is_clean_code={prov.get('is_clean_code')}` (`git_commit={prov.get('git_commit')}`)")
        for rel, bsha in (prov.get("git_blob_hashes") or {}).items():
            lf_sha = (prov.get("normalized_lf_code_hashes") or {}).get(rel)
            lines.append(f"  - `{rel}`: git_blob=`{bsha}`, lf_sha256=`{lf_sha}`")

    lines.extend([
        "",
        "## 3. Bảng Đối soát Kết quả Official Baselines",
        "",
        "| Thuật toán | Run ID | Tổng Ứng viên | Windows | Thành công | Mainshock Noise | Empty POST | Seq Members | Aftershocks (POST) | Noise Events | Runtime (s) |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ])
    for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
        m = report["reconciliation"][algo]
        c = report["case_results"][algo]
        run_id = report["artifacts_verified"][algo]["run_id"]
        lines.append(
            f"| **{algo}** | `{run_id}` | {m['total_candidates']} | {m['total_windows']} | "
            f"{m['successful_windows']} | {c['mainshock_noise_windows']} | {c['empty_post_windows']} | "
            f"{m['total_sequence_members']} | {m['total_aftershocks']} | {m['total_noise_candidates']} | {m['runtime_seconds']:.2f} |"
        )

    lines.extend([
        "",
        "## 4. Per-Shard Official Bundles (`WHOLE_WINDOW_SHARD` — Sibling Layout Hợp đồng MLI-01/MLI-03)",
        "",
        "- Cả full-split bundle (`<algo>/`) và từng shard bundle (`<algo>_shards/<shard_id>/`) đều tách thành sibling directories không lồng nhau, tương thích trực tiếp `MlResultBundleReader` (`MLI-03`).",
        "",
        "| Thuật toán | Shard | Relative Dir | Run ID | Rows | Mainshocks | `_SUCCESS.json` SHA-256 (prefix) |",
        "|---|---|---|---|---:|---:|---|",
    ])
    for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
        for shard_id, s_info in sorted(report.get("shard_verifications", {}).get(algo, {}).items()):
            lines.append(
                f"| `{algo}` | `{shard_id}` | `{s_info['relative_dir']}` | `{s_info['experiment_run_id']}` | {s_info['membership_row_count']} | {s_info['summary_row_count']} | `{s_info['success_marker_sha256'][:16]}...` |"
            )

    lines.extend([
        "",
        "## 5. Tuân thủ Hợp đồng & Giới hạn Khoa học",
        "",
        "1. **Per-Mainshock Isolation:** Tất cả mô hình đều fit độc lập cho từng mainshock window trên 4-D normalized vectors `[x_scaled, y_scaled, z_scaled, time_scaled]`, không có dữ liệu đầu vào nào bị biến đổi.",
        "2. **Mainshock Noise Policy:** Mọi cửa sổ có mainshock mang nhãn `-1` đều giữ `window_result_status = FAILED`, `failure_reason_code = EXP_MAINSHOCK_CLASSIFIED_AS_NOISE`, `0` sequence members; tuyệt đối không chọn cụm gần nhất.",
        "3. **Null Probability Policy:** `WINDOW` và `DBSCAN` giữ `membership_probability = NULL`; `HDBSCAN_GLOBAL` ghi xác suất hữu hạn trong `[0.0, 1.0]`.",
        "4. **Ranh giới Khoa học:** Các kết quả chỉ là **ứng viên chuỗi sau chấn (candidate sequences)** mang tính hồi cứu trên tập `REPRODUCTION`; không tuyên bố dự đoán động đất hoặc quan hệ nhân quả vật lý. Việc giải quyết đa chuỗi thích nghi thuộc [EXP-03](../task/tasks/EXP-03.md) và đánh giá khoa học DBCV/Omori/Stability thuộc [EXP-04](../task/tasks/EXP-04.md).",
        "",
    ])
    return "\n".join(lines)


def execute_official_baselines(
    *,
    bundle_dir: Path,
    output_dir: Path,
    output_json: Path,
    output_md: Optional[Path] = None,
    handoff_path: Optional[Path] = None,
    expected_handoff_sha256: Optional[str] = DEFAULT_HANDOFF_SHA256,
    manifest_sha256: Optional[str] = None,
    checksums_sha256: Optional[str] = None,
    seed: int = 42,
    allow_overwrite: bool = False,
    is_retry: bool = True,
    root: Path = PROJECT_ROOT,
) -> Dict[str, Any]:
    """Run all three EXP-02 official baseline algorithms on EXPORTED REPRODUCTION bundle."""
    bundle = load_feature_bundle(
        bundle_dir,
        mode=MODE_OFFICIAL,
        manifest_sha256=manifest_sha256,
        checksums_sha256=checksums_sha256,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
    )
    require(bundle.get("dataset_split") == "REPRODUCTION",
            f"EXP02_REQUIRES_REPRODUCTION_SPLIT: got {bundle.get('dataset_split')}")

    configs, config_pins = load_baseline_configs(root)
    code_prov = get_code_provenance(root)
    code_ver = code_prov["git_commit"]
    output_dir.mkdir(parents=True, exist_ok=True)

    results: Dict[str, Dict[str, Any]] = {}
    for algo in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"]:
        cfg = configs[algo]
        algo_out = output_dir / algo.lower()
        shard_out = output_dir / f"{algo.lower()}_shards"
        results[algo] = run_baseline_experiment(
            bundle=bundle,
            algorithm_name=algo,
            config=cfg,
            model_config_version=cfg["model_config_version"],
            output_dir=algo_out,
            shard_output_dir=shard_out,
            mode=MODE_OFFICIAL,
            code_version=code_ver,
            random_seed=seed,
            execution_id=f"exp02_official_{algo.lower()}_seed{seed}",
            allow_overwrite=allow_overwrite,
            is_retry=is_retry,
        )

    actual_handoff_sha = resolve_verified_handoff_sha256(
        bundle_dir,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
    )

    report = generate_official_baseline_report(
        bundle=bundle,
        algorithm_results=results,
        output_path=output_json,
        code_version=code_ver,
        random_seed=seed,
        model_configs=configs,
        model_config_pins=config_pins,
        code_provenance=code_prov,
        handoff_sha256=actual_handoff_sha,
    )
    validate_safe_metadata(report)

    if output_md is not None:
        md_content = render_official_markdown(report)
        output_md.parent.mkdir(parents=True, exist_ok=True)
        output_md.write_text(md_content, encoding="utf-8")

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", required=True, type=Path, help="Path to EXPORTED REPRODUCTION collection bundle")
    parser.add_argument("--handoff", type=Path, default=None, help="Optional path to handoff.json")
    parser.add_argument(
        "--expected-handoff-sha256",
        default=DEFAULT_HANDOFF_SHA256,
        help="Trusted SHA-256 of handoff.json (required when resolving pins from handoff.json)",
    )
    parser.add_argument("--manifest-sha256", default=None, help="Optional explicit dataset_manifest.json SHA-256")
    parser.add_argument("--checksums-sha256", default=None, help="Optional explicit checksums.sha256 SHA-256")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "staging/ml-experiments/exp02-official-reproduction-v2",
        help="Destination directory for official result bundles",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=PROJECT_ROOT / "docs/evidence/EXP-02_OFFICIAL_REPRODUCTION.json",
        help="Destination path for official JSON evidence report",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=PROJECT_ROOT / "docs/evidence/EXP-02_OFFICIAL_REPRODUCTION.md",
        help="Destination path for official Markdown evidence report",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--no-retry-reuse", action="store_true")
    args = parser.parse_args()

    exp_handoff_sha = args.expected_handoff_sha256.strip() or None
    report = execute_official_baselines(
        bundle_dir=args.bundle,
        output_dir=args.output_dir,
        output_json=args.output_json,
        output_md=args.output_md,
        handoff_path=args.handoff,
        expected_handoff_sha256=exp_handoff_sha,
        manifest_sha256=args.manifest_sha256,
        checksums_sha256=args.checksums_sha256,
        seed=args.seed,
        allow_overwrite=args.allow_overwrite,
        is_retry=not args.no_retry_reuse,
    )
    print(json.dumps({
        "status": report["status"],
        "dataset_id": report["dataset"]["dataset_id"],
        "dataset_split": report["dataset"]["dataset_split"],
        "feature_row_count": report["dataset"]["feature_row_count"],
        "mainshock_count": report["dataset"]["mainshock_count"],
        "reconciliation": report["reconciliation"],
        "case_results": report["case_results"],
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
