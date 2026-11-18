#!/usr/bin/env python3
"""EXP-03 Official Runner: Execute HDBSCAN_ADAPTIVE and global multi-sequence resolution on REPRODUCTION EXPORTED collection."""
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
from adaptive_hdbscan import (  # noqa: E402
    generate_official_adaptive_report,
    run_adaptive_experiment,
)

DEFAULT_HANDOFF_SHA256 = "6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297"
ADAPTIVE_CONFIG_PATH = "config/ml/models/hdbscan_adaptive_dev_v1.json"
CRITICAL_PROVENANCE_FILES = [
    "notebooks/experiment_harness.py",
    "notebooks/research_collection.py",
    "notebooks/baseline_models.py",
    "notebooks/adaptive_hdbscan.py",
    "scripts/run-exp03-official.py",
    ADAPTIVE_CONFIG_PATH,
]


def get_code_provenance(root: Path) -> Dict[str, Any]:
    """Capture strict git commit, blob hashes, LF-normalized hashes, and clean status for EXP-03 files."""
    return collect_code_provenance(
        root,
        CRITICAL_PROVENANCE_FILES,
        clean_status_label="CLEAN_VERIFIED",
    )


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


def load_adaptive_config(root: Path) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """Load HDBSCAN_ADAPTIVE config and compute exact file/canonical SHA-256 pins."""
    cfg_file = root / ADAPTIVE_CONFIG_PATH
    require(cfg_file.is_file(), f"MISSING_MODEL_CONFIG:{ADAPTIVE_CONFIG_PATH}")
    raw = cfg_file.read_bytes()
    cfg = json.loads(raw.decode("utf-8"))
    pin = {
        "config_path": ADAPTIVE_CONFIG_PATH,
        "algorithm_name": str(cfg.get("algorithm_name", "HDBSCAN_ADAPTIVE")),
        "algorithm_version": str(cfg.get("algorithm_version", "1.0.0")),
        "model_config_version": str(cfg.get("model_config_version")),
        "file_bytes_sha256": hashlib.sha256(raw).hexdigest(),
        "canonical_config_sha256": sha256_bytes(canonical(cfg).encode("utf-8")),
    }
    return cfg, pin


def render_official_markdown(report: Dict[str, Any]) -> str:
    """Render reproducible Markdown evidence report for EXP-03 official REPRODUCTION run."""
    ds = report["dataset"]
    prov = report.get("code_provenance") or {}
    pin = report.get("model_config_pin", {})
    m = report["reconciliation"]
    rec = report["multi_sequence_reconciliation"]
    c = report["case_results"]
    art = report["artifact_verified"]
    pstats = report.get("adaptive_param_stats", {})

    lines: List[str] = [
        "# EXP-03 — Kết quả Official HDBSCAN Adaptive & Multi-Sequence trên REPRODUCTION Collection",
        "",
        "- **Task:** [EXP-03](../task/tasks/EXP-03.md) (Khối H - Experiment, P0 Core)",
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

    lines.extend([
        "",
        "## 2. Cấu hình Thuật toán & Code Provenance đã Khóa",
        "",
        f"- **HDBSCAN_ADAPTIVE:** `{pin.get('config_path')}` (`{pin.get('model_config_version')}`, File SHA: `{pin.get('file_bytes_sha256')}`, Canonical SHA: `{pin.get('canonical_config_sha256')}`)",
    ])
    if prov:
        lines.append(f"- **Clean Verification:** `is_clean_code={prov.get('is_clean_code')}` (`git_commit={prov.get('git_commit')}`)")
        for rel, bsha in (prov.get("git_blob_hashes") or {}).items():
            lf_sha = (prov.get("normalized_lf_code_hashes") or {}).get(rel)
            lines.append(f"  - `{rel}`: git_blob=`{bsha}`, lf_sha256=`{lf_sha}`")
    if pstats:
        mcls = pstats.get("min_cluster_size", {})
        msmp = pstats.get("min_samples", {})
        lines.append(
            f"- **Dải tham số thích nghi tính trên `{pstats.get('window_count')}` windows (deterministic, không manual override):** "
            f"`min_cluster_size` min=`{mcls.get('min')}` / median=`{mcls.get('median')}` / max=`{mcls.get('max')}`; "
            f"`min_samples` min=`{msmp.get('min')}` / median=`{msmp.get('median')}` / max=`{msmp.get('max')}`."
        )

    lines.extend([
        "",
        "## 3. Đối soát Window Fit & Multi-Sequence Entity Resolution Toàn Cục",
        "",
        "| Chỉ số | Giá trị |",
        "|---|---:|",
        f"| **Run ID (Full Split)** | `{art['run_id']}` |",
        f"| **Tổng Ứng viên (Raw Grain)** | {m['total_candidates']} |",
        f"| **Windows / Thành công / Mainshock Noise / Empty POST** | {m['total_windows']} / {m['successful_windows']} / {c['mainshock_noise_windows']} / {c['empty_post_windows']} |",
        f"| **Raw Sequence Members (tất cả windows)** | {rec['raw_sequence_members']} |",
        f"| **Raw Aftershocks (POST trước khử trùng lặp)** | {rec['raw_aftershocks']} |",
        f"| **Ứng viên xuất hiện trong Multi-Sequences** | {rec['unique_candidates_in_multi_sequences']} |",
        f"| **Resolved Unique Aftershocks (Rank 1 POST)** | {rec['resolved_unique_aftershocks']} |",
        f"| **Aftershocks Deduplicated (Secondary Ranks)** | {rec['aftershocks_deduplicated_count']} |",
        f"| **Ambiguous Candidates / Ambiguous Aftershocks** | {rec['ambiguous_candidates_count']} / {rec['ambiguous_aftershocks_count']} |",
        f"| **Nested Mainshocks Detected** | {rec['nested_mainshocks_count']} |",
        f"| **Noise Events** | {m['total_noise_candidates']} |",
        f"| **Runtime (s)** | {m['runtime_seconds']:.2f} |",
        "",
        "## 4. Per-Shard Official Bundles (`WHOLE_WINDOW_SHARD` — Sibling Layout Hợp đồng MLI-01/MLI-03)",
        "",
        "- Full-split bundle (`hdbscan_adaptive/`) và từng shard bundle (`hdbscan_adaptive_shards/<shard_id>/`) tách thành thư mục đồng cấp không lồng nhau, tương thích trực tiếp `MlResultBundleReader` (`MLI-03`).",
        "",
        "| Shard | Relative Dir | Run ID | Rows | Mainshocks | `_SUCCESS.json` SHA-256 (prefix) |",
        "|---|---|---|---:|---:|---|",
    ])
    for shard_id, s_info in sorted(report.get("shard_verifications", {}).items()):
        lines.append(
            f"| `{shard_id}` | `{s_info.get('relative_dir')}` | `{s_info['experiment_run_id']}` | {s_info['membership_row_count']} | {s_info['summary_row_count']} | `{s_info['success_marker_sha256'][:16]}...` |"
        )

    lines.extend([
        "",
        "## 5. Tuân thủ Hợp đồng & Giới hạn Khoa học",
        "",
        "1. **Deterministic Adaptive Rule:** `min_cluster_size` và `min_samples` tính xác định theo `(candidate_count, mainshock_magnitude)` từ `hdbscan-adaptive-dev-v1`; không chỉnh tay theo từng sequence và không chạm dữ liệu `EXTENSION`.",
        "2. **Global Multi-Sequence & Raw Grain Preservation:** Lưu đủ toàn bộ `156710` raw membership pairs; phân giải đa chuỗi toàn cục trước khi tách `WHOLE_WINDOW_SHARD` theo `probability -> normalized_distance -> AMBIGUOUS` (giữ `resolution_rank = NULL` cho các trường hợp hòa).",
        "3. **Nested Mainshock Evidence:** Giữ toàn bộ `parent_mainshock_event_id` và `nested_membership_probability` trên `sequence_summary.parquet` (và gắn `AMBIGUOUS_NESTED_PARENT_TIE` nếu hòa parent).",
        "4. **Ranh giới MLI-03 Full-Split vs Shard:** `MlResultBundleReader.read` đọc được cả full-split (`hdbscan_adaptive/`) lẫn từng shard (`hdbscan_adaptive_shards/<shard_id>/`). `ResultBundleContract.validate` kiểm tra đầy đủ trạng thái toàn cục trên full-split bundle; với các shard tách riêng có candidate giao cắt giữa nhiều shard (ví dụ `13.097` candidate dùng chung giữa `shard_00000` và `shard_00001`), việc giữ nhãn đã resolve toàn cục (`MULTIPLE`/`AMBIGUOUS`) khiến bản thân slice đơn lẻ chứa các dòng đơn có status liên-shard.",
        "5. **Ranh giới Khoa học:** Kết quả chỉ là ứng viên chuỗi sau chấn hồi cứu; không tuyên bố adaptive tốt hơn hay dự đoán động đất trước khi hoàn tất đánh giá định lượng ở [EXP-04](../task/tasks/EXP-04.md).",
        "",
    ])
    return "\n".join(lines)


def execute_official_adaptive(
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
    """Run EXP-03 official HDBSCAN_ADAPTIVE + global resolution on EXPORTED REPRODUCTION."""
    bundle = load_feature_bundle(
        bundle_dir,
        mode=MODE_OFFICIAL,
        manifest_sha256=manifest_sha256,
        checksums_sha256=checksums_sha256,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
    )
    require(bundle.get("dataset_split") == "REPRODUCTION",
            f"EXP03_REQUIRES_REPRODUCTION_SPLIT: got {bundle.get('dataset_split')}")

    cfg, cfg_pin = load_adaptive_config(root)
    code_prov = get_code_provenance(root)
    code_ver = code_prov["git_commit"]
    output_dir.mkdir(parents=True, exist_ok=True)
    shard_out = output_dir.parent / f"{output_dir.name}_shards"

    result = run_adaptive_experiment(
        bundle=bundle,
        config=cfg,
        model_config_version=cfg["model_config_version"],
        output_dir=output_dir,
        mode=MODE_OFFICIAL,
        code_version=code_ver,
        random_seed=seed,
        execution_id=f"exp03_official_hdbscan_adaptive_seed{seed}",
        allow_overwrite=allow_overwrite,
        is_retry=is_retry,
        shard_output_dir=shard_out,
    )

    actual_handoff_sha = resolve_verified_handoff_sha256(
        bundle_dir,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
    )

    report = generate_official_adaptive_report(
        bundle=bundle,
        result=result,
        output_path=output_json,
        code_version=code_ver,
        random_seed=seed,
        model_config=cfg,
        model_config_pin=cfg_pin,
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
        help="Expected SHA-256 of handoff.json (pass empty string to skip when testing synthetic fixtures)",
    )
    parser.add_argument("--manifest-sha256", default=None, help="Optional explicit dataset_manifest.json SHA-256")
    parser.add_argument("--checksums-sha256", default=None, help="Optional explicit checksums.sha256 SHA-256")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "staging/ml-experiments/exp03-official-reproduction-v2/hdbscan_adaptive",
        help="Destination directory for official HDBSCAN_ADAPTIVE result bundles",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=PROJECT_ROOT / "docs/evidence/EXP-03_OFFICIAL_REPRODUCTION.json",
        help="Destination path for official JSON evidence report",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=PROJECT_ROOT / "docs/evidence/EXP-03_OFFICIAL_REPRODUCTION.md",
        help="Destination path for official Markdown evidence report",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--allow-overwrite", action="store_true")
    parser.add_argument("--no-retry-reuse", action="store_true")
    args = parser.parse_args()

    exp_handoff_sha = args.expected_handoff_sha256.strip() or None
    report = execute_official_adaptive(
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
        "multi_sequence_reconciliation": report["multi_sequence_reconciliation"],
        "adaptive_param_stats": report["adaptive_param_stats"],
    }, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
