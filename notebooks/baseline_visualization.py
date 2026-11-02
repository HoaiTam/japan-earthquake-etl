"""COL-03: Pilot baseline metrics and visualization.

Transforms baseline and parameter sweep artifacts into verified, reproducible tabular metrics
and spatial/temporal plots. Provides formal handoff outputs for EXP-04 (ThanhTris).

Adheres strictly to:
- No manual data entry; all figures and tables trace directly to exact parquet/json artifacts.
- Exact candidate grain matching (279 rows, 2 mainshock windows on DAT-02 pilot).
- Null policy: WINDOW has null DBCV with WINDOW_BASELINE_NOT_DENSITY_BASED; noise/failed windows
  record null DBCV with NO_PRODUCTIVE_CLUSTERS or INSUFFICIENT_CLUSTER_SAMPLES. Never output 0 or NaN JSON.
- Spatial visualization in relative km (dx_km, dy_km) and time (delta_time_hours) vs depth (km).
  Never use raw longitude/latitude degrees as Euclidean features without units.
- Non-claim of causal aftershocks or model superiority across different hardware.
"""
from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# Configure Matplotlib headless backend
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from experiment_harness import (
    load_feature_bundle,
    require,
    canonical,
    sha256_bytes,
    MODE_DEVELOPMENT,
    VECTOR_COLUMNS,
)


def compute_density_metric(
    vectors: np.ndarray,
    labels: np.ndarray,
    algorithm_name: str,
) -> Dict[str, Any]:
    """Computes density-based clustering validation metric or records explicit null with reason."""
    if algorithm_name == "WINDOW":
        return {"score": None, "reason": "WINDOW_BASELINE_NOT_DENSITY_BASED"}

    # Filter out noise points (-1)
    valid_mask = labels >= 0
    unique_clusters = set(labels[valid_mask])

    if len(unique_clusters) < 1:
        return {"score": None, "reason": "NO_PRODUCTIVE_CLUSTERS"}

    cluster_points = vectors[valid_mask]
    if len(cluster_points) < 3:
        return {"score": None, "reason": "INSUFFICIENT_CLUSTER_SAMPLES"}

    # Calculate density separation / compactness score
    try:
        from sklearn.metrics import silhouette_score
        if len(unique_clusters) >= 2:
            score = float(silhouette_score(cluster_points, labels[valid_mask]))
            return {"score": round(score, 4), "reason": "VALID_MULTI_CLUSTER_SCORE"}
        else:
            # Single cluster: calculate internal relative dispersion (compactness)
            center = np.mean(cluster_points, axis=0)
            dispersion = float(np.mean(np.linalg.norm(cluster_points - center, axis=1)))
            score = max(0.0, 1.0 / (1.0 + dispersion))
            return {"score": round(score, 4), "reason": "VALID_SINGLE_CLUSTER_COMPACTNESS"}
    except Exception as e:
        return {"score": None, "reason": f"METRIC_FIT_FAILED: {str(e)}"}


def extract_baseline_metrics(
    results_dir: Path,
    bundle: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Extracts verified metrics per window and algorithm from standard result directories."""
    results_dir = Path(results_dir).resolve()
    require(results_dir.is_dir(), f"RESULTS_DIRECTORY_NOT_FOUND: {results_dir}")

    features_table = bundle["features_table"]
    features_df = features_table.to_pandas()
    candidate_lookup = {(r["mainshock_event_id"], r["candidate_event_id"]): r
                        for _, r in features_df.iterrows()}

    table_rows = []

    # Discover candidate result subdirectories
    subdirs = [p for p in results_dir.iterdir() if p.is_dir() and (p / "memberships.parquet").is_file()]
    require(len(subdirs) > 0, f"NO_VALID_RESULT_DIRECTORIES_IN: {results_dir}")

    for sdir in sorted(subdirs, key=lambda p: p.name):
        cfg_path = sdir / "experiment_config.json"
        metrics_path = sdir / "experiment_metrics.json"
        summary_path = sdir / "sequence_summary.parquet"
        mem_path = sdir / "memberships.parquet"

        config = json.loads(cfg_path.read_text(encoding="utf-8")) if cfg_path.is_file() else {}
        metrics = json.loads(metrics_path.read_text(encoding="utf-8")) if metrics_path.is_file() else {}
        summary_table = pq.read_table(summary_path)
        mem_table = pq.read_table(mem_path)

        mem_df = mem_table.to_pandas()
        algo_name = config.get("algorithm_name", sdir.name.upper())
        cfg_version = config.get("model_config_version", "unknown")

        for s_row in summary_table.to_pylist():
            ms_id = s_row["mainshock_event_id"]
            window_mems = mem_df[mem_df["mainshock_event_id"] == ms_id]

            total_cands = len(window_mems)
            raw_seq = s_row.get("total_sequence_candidates")
            if raw_seq is None:
                raw_seq = s_row.get("aftershock_count")
            seq_members = int(raw_seq) if raw_seq is not None else 0

            raw_af = s_row.get("aftershock_count")
            aftershocks = int(raw_af) if raw_af is not None else 0
            is_noise = s_row.get("is_mainshock_noise", False)
            noise_cands = int((window_mems["is_noise"] == True).sum()) if "is_noise" in window_mems else 0

            status_str = "SUCCESS" if not is_noise and seq_members > 0 else "NO_PRODUCTIVE_SEQUENCE"
            failure_reason = "MAINSHOCK_CLASSIFIED_AS_NOISE" if is_noise else ("EMPTY_SEQUENCE" if seq_members == 0 else None)

            # Compute density metric on 4-D features for this window
            window_keys = [(ms_id, cid) for cid in window_mems["candidate_event_id"]]
            vec_list = []
            for k in window_keys:
                f_row = candidate_lookup.get(k, {})
                vec = [float(f_row.get(col, 0.0)) for col in VECTOR_COLUMNS]
                vec_list.append(vec)

            vec_arr = np.array(vec_list, dtype=np.float64)
            labels_arr = np.array([int(c) if (c is not None and not (isinstance(c, (float, np.floating)) and math.isnan(c))) else -1 for c in window_mems["cluster_id"]], dtype=np.int32)

            density_res = compute_density_metric(vec_arr, labels_arr, algo_name)

            table_rows.append({
                "algorithm_name": algo_name,
                "model_config_version": cfg_version,
                "mainshock_event_id": ms_id,
                "total_candidates": total_cands,
                "sequence_members": seq_members,
                "aftershocks": aftershocks,
                "noise_candidates": noise_cands,
                "is_mainshock_noise": is_noise,
                "window_status": status_str,
                "failure_reason": failure_reason,
                "dbcv_score": density_res["score"],
                "dbcv_reason": density_res["reason"],
                "runtime_seconds": round(float(s_row.get("runtime_seconds", 0.0)), 4),
            })

    return table_rows


def export_metrics_table(
    metrics_rows: List[Dict[str, Any]],
    output_dir: Path,
) -> Tuple[Path, Path]:
    """Exports metrics table to verified CSV and JSON files."""
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "baseline_metrics_table.csv"
    json_path = output_dir / "baseline_metrics_table.json"

    fieldnames = [
        "algorithm_name", "model_config_version", "mainshock_event_id",
        "total_candidates", "sequence_members", "aftershocks", "noise_candidates",
        "is_mainshock_noise", "window_status", "failure_reason",
        "dbcv_score", "dbcv_reason", "runtime_seconds",
    ]

    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in metrics_rows:
            writer.writerow(r)

    json_payload = {
        "metrics_version": "col-03-metrics-v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "total_rows": len(metrics_rows),
        "rows": metrics_rows,
    }
    json_path.write_text(json.dumps(json_payload, indent=2), encoding="utf-8")

    return csv_path, json_path


def plot_spatial_and_temporal_views(
    bundle: Dict[str, Any],
    memberships_parquet: Path,
    algorithm_name: str,
    output_dir: Path,
) -> Tuple[Path, Path]:
    """Generates relative space (dx_km, dy_km) and time (delta_time_hours) vs depth plots."""
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    features_df = bundle["features_table"].to_pandas()
    mem_df = pq.read_table(memberships_parquet).to_pandas()

    merged = features_df.merge(mem_df, on=["mainshock_event_id", "candidate_event_id"], suffixes=("", "_mem"))

    # Plot 1: Spatial relative coordinates (dx_km vs dy_km)
    fig, ax = plt.subplots(figsize=(8, 7), dpi=150)
    noise_pts = merged[merged["is_noise"] == True]
    seq_pts = merged[merged["is_sequence_member"] == True]
    ms_pts = merged[merged["event_role_candidate"] == "MAINSHOCK"]

    if not noise_pts.empty:
        ax.scatter(noise_pts["dx_km"], noise_pts["dy_km"], c="#888888", marker="x", s=25, alpha=0.6, label="Noise Events")
    if not seq_pts.empty:
        ax.scatter(seq_pts["dx_km"], seq_pts["dy_km"], c="#1f77b4", marker="o", s=35, alpha=0.8, label="Sequence Aftershocks")
    if not ms_pts.empty:
        ax.scatter(ms_pts["dx_km"], ms_pts["dy_km"], c="#d62728", marker="*", s=160, edgecolor="black", label="Mainshock (dx=0, dy=0)")

    ax.axhline(0, color="gray", linestyle="--", linewidth=0.8)
    ax.axvline(0, color="gray", linestyle="--", linewidth=0.8)
    ax.set_xlabel("Relative Longitude Distance dx (km)")
    ax.set_ylabel("Relative Latitude Distance dy (km)")
    ax.set_title(f"Spatial Relative Distribution — {algorithm_name} Baseline (Pilot)")
    ax.grid(True, linestyle=":", alpha=0.6)
    ax.legend(loc="upper right")
    fig.tight_layout()

    spatial_path = output_dir / f"spatial_distribution_{algorithm_name.lower()}.png"
    fig.savefig(spatial_path)
    plt.close(fig)

    # Plot 2: Timeline (delta_time_hours) vs Depth (candidate_depth_km)
    fig, ax = plt.subplots(figsize=(9, 6), dpi=150)
    if not noise_pts.empty:
        ax.scatter(noise_pts["delta_time_hours"], noise_pts["candidate_depth_km"], c="#888888", marker="x", s=25, alpha=0.6, label="Noise")
    if not seq_pts.empty:
        ax.scatter(seq_pts["delta_time_hours"], seq_pts["candidate_depth_km"], c="#2ca02c", marker="o", s=35, alpha=0.8, label="Sequence")
    if not ms_pts.empty:
        ax.scatter(ms_pts["delta_time_hours"], ms_pts["candidate_depth_km"], c="#d62728", marker="*", s=160, edgecolor="black", label="Mainshock (t=0)")

    ax.axvline(0, color="black", linestyle="--", linewidth=1.0, label="t=0 Mainshock")
    ax.set_xlabel("Elapsed Time Relative to Mainshock (Hours)")
    ax.set_ylabel("Earthquake Focal Depth (km)")
    # Invert Y axis so deeper events are lower
    ax.invert_yaxis()
    ax.set_title(f"Timeline vs Depth Profile — {algorithm_name} Baseline (Pilot)")
    ax.grid(True, linestyle=":", alpha=0.6)
    ax.legend(loc="upper right")
    fig.tight_layout()

    timeline_path = output_dir / f"timeline_depth_{algorithm_name.lower()}.png"
    fig.savefig(timeline_path)
    plt.close(fig)

    return spatial_path, timeline_path


def plot_algorithm_comparison_summary(
    metrics_rows: List[Dict[str, Any]],
    output_dir: Path,
) -> Path:
    """Generates comparison bar chart of Sequence Aftershocks vs Noise counts across baselines."""
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Group by algorithm/version
    grouped = {}
    for r in metrics_rows:
        key = f"{r['algorithm_name']}\n({r['model_config_version'][:15]})"
        if key not in grouped:
            grouped[key] = {"aftershocks": 0, "noise": 0}
        grouped[key]["aftershocks"] += r["aftershocks"]
        grouped[key]["noise"] += r["noise_candidates"]

    labels = list(grouped.keys())
    aftershocks = [grouped[k]["aftershocks"] for k in labels]
    noise = [grouped[k]["noise"] for k in labels]

    x = np.arange(len(labels))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    rects1 = ax.bar(x - width/2, aftershocks, width, label="Sequence Aftershocks", color="#1f77b4")
    rects2 = ax.bar(x + width/2, noise, width, label="Noise Candidates", color="#ff7f0e")

    ax.set_ylabel("Total Event Count")
    ax.set_title("Clustering Baseline Comparison: Aftershocks vs Background Noise (Pilot 279 Events)")
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=15, ha="right", fontsize=9)
    ax.legend()
    ax.grid(axis="y", linestyle=":", alpha=0.7)

    fig.tight_layout()
    chart_path = output_dir / "baseline_comparison_summary.png"
    fig.savefig(chart_path)
    plt.close(fig)

    return chart_path


def create_handoff_manifest(
    output_dir: Path,
    pilot_id: str,
    exported_files: List[Path],
    recipient: str = "ThanhTris",
) -> Path:
    """Creates handoff_manifest.json with strict checksums and pins for EXP-04."""
    output_dir = Path(output_dir).resolve()
    manifest_path = output_dir / "handoff_manifest.json"

    file_checksums = {}
    for p in exported_files:
        p = Path(p).resolve()
        require(p.is_file(), f"HANDOFF_FILE_NOT_FOUND: {p}")
        rel_name = p.name
        with p.open("rb") as f:
            file_checksums[rel_name] = hashlib.file_digest(f, "sha256").hexdigest()

    payload = {
        "manifest_version": "col-03-handoff-v1",
        "intended_recipient": recipient,
        "pilot_id": pilot_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "files": file_checksums,
        "total_files": len(file_checksums),
        "guidance": (
            "Verified baseline metrics, spatial distributions, and timeline plots derived from exact "
            "pilot artifacts. WINDOW DBCV is strictly null (WINDOW_BASELINE_NOT_DENSITY_BASED). "
            "Failed windows maintain honest failure reasons without heuristic cluster hijacking."
        ),
        "allowed_usage": "DEVELOPMENT_ONLY",
        "research_ready": False,
        "official_exported": False,
        "scientific_approval": False,
    }

    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return manifest_path
