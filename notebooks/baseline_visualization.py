"""COL-03: Pilot baseline metrics and visualization.

Transforms baseline and parameter sweep artifacts into verified, reproducible tabular metrics
and spatial/temporal plots. Provides formal handoff outputs for EXP-04 (ThanhTris).

Adheres strictly to:
- No manual data entry; all figures and tables trace directly to exact parquet/json artifacts.
- Exact candidate grain matching (279 rows, 2 mainshock windows on DAT-02 pilot).
- Fail-closed validation: unknown candidate grain, duplicate grains, missing or non-finite features
  immediately raise ValueError before any metric calculation or plotting.
- Genuine DBCV calculation (Moulavi et al., 2014) evaluated on 4-D spatio-temporal coordinates.
  DBCV requires at least 2 clusters; single clusters or non-density baselines (WINDOW) return null
  with explicit reasons. Failed windows (NO_PRODUCTIVE_SEQUENCE) strictly yield null DBCV.
  Silhouette score is tracked in separate fields (silhouette_score / silhouette_reason) if available,
  never substituted for DBCV.
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
from scipy.spatial.distance import cdist
from scipy.sparse.csgraph import minimum_spanning_tree

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


def compute_dbcv(
    X: np.ndarray,
    labels: np.ndarray,
    dim: Optional[int] = None,
) -> Tuple[Optional[float], str]:
    """Computes exact Density-Based Clustering Validation (DBCV) index (Moulavi et al., 2014).

    Evaluates cluster validity on spatio-temporal coordinates X (shape N x D) and labels (shape N,).
    Noise points are marked as -1.
    Requires at least 2 distinct clusters with label >= 0.
    """
    valid_mask = labels >= 0
    valid_labels = labels[valid_mask]
    valid_X = X[valid_mask]
    unique_clusters = np.unique(valid_labels)

    if len(unique_clusters) < 1:
        return None, "NO_PRODUCTIVE_CLUSTERS"

    if len(unique_clusters) < 2:
        return None, "DBCV_REQUIRES_AT_LEAST_TWO_CLUSTERS"

    d = dim if dim is not None else X.shape[1]
    clusters_pts = {}
    clusters_core_dist = {}

    for c in unique_clusters:
        pts = valid_X[valid_labels == c]
        n_pts = len(pts)
        if n_pts < 2:
            return None, "INSUFFICIENT_CLUSTER_SAMPLES"
        clusters_pts[c] = pts
        dist_mat = cdist(pts, pts)
        np.fill_diagonal(dist_mat, np.inf)
        inv_d = np.power(np.maximum(dist_mat, 1e-12), -d)
        core_dists = np.power(np.sum(inv_d, axis=1) / (n_pts - 1), -1.0 / d)
        clusters_core_dist[c] = core_dists

    # Density Sparseness of Cluster (DSC) via MST of Mutual Reachability Distances
    dsc = {}
    for c in unique_clusters:
        pts = clusters_pts[c]
        cd = clusters_core_dist[c]
        dist_mat = cdist(pts, pts)
        mrd_mat = np.maximum(dist_mat, np.maximum(cd[:, None], cd[None, :]))
        mst = minimum_spanning_tree(mrd_mat)
        dsc[c] = float(np.max(mst.data)) if mst.nnz > 0 else 0.0

    # Density Separation of Pair of Clusters (DSPC)
    dspc = {}
    for i, c1 in enumerate(unique_clusters):
        for c2 in unique_clusters[i + 1:]:
            pts1 = clusters_pts[c1]
            pts2 = clusters_pts[c2]
            cd1 = clusters_core_dist[c1]
            cd2 = clusters_core_dist[c2]
            dist_mat = cdist(pts1, pts2)
            mrd_mat = np.maximum(dist_mat, np.maximum(cd1[:, None], cd2[None, :]))
            min_sep = float(np.min(mrd_mat))
            dspc[(c1, c2)] = min_sep
            dspc[(c2, c1)] = min_sep

    # Validity index for each cluster and overall weighted average
    total_pts = len(valid_X)
    v_c = {}
    for c in unique_clusters:
        min_sep = min(dspc[(c, other)] for other in unique_clusters if other != c)
        c_dsc = dsc[c]
        denom = max(min_sep, c_dsc)
        v_c[c] = (min_sep - c_dsc) / denom if denom > 1e-12 else 0.0

    dbcv_score = sum((len(clusters_pts[c]) / total_pts) * v_c[c] for c in unique_clusters)
    if math.isnan(dbcv_score) or math.isinf(dbcv_score):
        return None, "METRIC_FIT_FAILED"

    return round(float(dbcv_score), 4), "VALID_DBCV_SCORE"


def compute_density_metric(
    vectors: np.ndarray,
    labels: np.ndarray,
    algorithm_name: str,
) -> Dict[str, Any]:
    """Computes genuine density-based clustering validation (DBCV) or records explicit null with reason.

    Also tracks secondary silhouette metrics in separate fields; never substitutes silhouette for DBCV.
    """
    algo_upper = str(algorithm_name).upper()
    if "WINDOW" in algo_upper:
        return {
            "score": None,
            "reason": "WINDOW_BASELINE_NOT_DENSITY_BASED",
            "silhouette_score": None,
            "silhouette_reason": "WINDOW_BASELINE_NOT_DENSITY_BASED",
        }

    if vectors is None or len(vectors) == 0 or labels is None or len(labels) == 0:
        return {
            "score": None,
            "reason": "NO_PRODUCTIVE_CLUSTERS",
            "silhouette_score": None,
            "silhouette_reason": "NO_PRODUCTIVE_CLUSTERS",
        }

    valid_mask = labels >= 0
    unique_clusters = np.unique(labels[valid_mask])

    if len(unique_clusters) < 1:
        return {
            "score": None,
            "reason": "NO_PRODUCTIVE_CLUSTERS",
            "silhouette_score": None,
            "silhouette_reason": "NO_PRODUCTIVE_CLUSTERS",
        }

    cluster_points = vectors[valid_mask]
    if len(cluster_points) < 3:
        return {
            "score": None,
            "reason": "INSUFFICIENT_CLUSTER_SAMPLES",
            "silhouette_score": None,
            "silhouette_reason": "INSUFFICIENT_CLUSTER_SAMPLES",
        }

    # 1. Compute Genuine DBCV Metric
    dbcv_val = None
    dbcv_reason = "METRIC_FIT_FAILED"
    try:
        try:
            import hdbscan.validity as hdb_validity
            X_contig = np.ascontiguousarray(cluster_points, dtype=np.float64)
            labels_int = np.ascontiguousarray(labels[valid_mask], dtype=np.int64)
            if len(unique_clusters) >= 2:
                raw_score = float(hdb_validity.validity_index(X_contig, labels_int))
                if not (math.isnan(raw_score) or math.isinf(raw_score)):
                    dbcv_val = round(raw_score, 4)
                    dbcv_reason = "VALID_DBCV_SCORE"
                else:
                    dbcv_val, dbcv_reason = compute_dbcv(vectors, labels)
            else:
                dbcv_val = None
                dbcv_reason = "DBCV_REQUIRES_AT_LEAST_TWO_CLUSTERS"
        except (ImportError, Exception):
            dbcv_val, dbcv_reason = compute_dbcv(vectors, labels)
    except Exception as e:
        dbcv_val = None
        dbcv_reason = f"METRIC_FIT_FAILED: {str(e)}"

    # 2. Compute Separate Silhouette Metric (explicitly separate from DBCV)
    sil_val = None
    sil_reason = "NO_PRODUCTIVE_CLUSTERS"
    try:
        from sklearn.metrics import silhouette_score
        if len(unique_clusters) >= 2 and len(cluster_points) > len(unique_clusters):
            sil_val = round(float(silhouette_score(cluster_points, labels[valid_mask])), 4)
            sil_reason = "VALID_MULTI_CLUSTER_SCORE"
        else:
            sil_val = None
            sil_reason = "SILHOUETTE_REQUIRES_AT_LEAST_TWO_CLUSTERS"
    except Exception as e:
        sil_val = None
        sil_reason = f"SILHOUETTE_FAILED: {str(e)}"

    return {
        "score": dbcv_val,
        "reason": dbcv_reason,
        "silhouette_score": sil_val,
        "silhouette_reason": sil_reason,
    }


def extract_baseline_metrics(
    results_dir: Path,
    bundle: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Extracts verified metrics per window and algorithm from standard result directories."""
    results_dir = Path(results_dir).resolve()
    require(results_dir.is_dir(), f"RESULTS_DIRECTORY_NOT_FOUND: {results_dir}")

    # 1. Validate Feature Bundle and build candidate lookup (Fail-Closed)
    features_table = bundle["features_table"]
    features_df = features_table.to_pandas()

    feat_grains = list(zip(features_df["mainshock_event_id"], features_df["candidate_event_id"]))
    if len(feat_grains) != len(set(feat_grains)):
        raise ValueError("DUPLICATE_GRAIN_DETECTED: Feature bundle contains duplicate (mainshock_event_id, candidate_event_id) grains")

    for col in VECTOR_COLUMNS:
        if col not in features_df.columns:
            raise ValueError(f"MISSING_REQUIRED_FEATURE_COLUMN: Column '{col}' not found in feature bundle")
        vals = features_df[col].to_numpy()
        if np.any(np.isnan(vals)) or np.any(np.isinf(vals)):
            raise ValueError(f"NON_FINITE_FEATURE_DETECTED: Column '{col}' contains NaN or infinite values in feature bundle")

    candidate_lookup = {}
    for _, r in features_df.iterrows():
        k = (r["mainshock_event_id"], r["candidate_event_id"])
        vec = []
        for col in VECTOR_COLUMNS:
            v = r[col]
            if v is None or not (isinstance(v, (int, float, np.floating, np.integer)) and math.isfinite(v)):
                raise ValueError(f"NON_FINITE_OR_MISSING_FEATURE: Column '{col}' in candidate '{r['candidate_event_id']}' is missing or non-finite ({v})")
            vec.append(float(v))
        candidate_lookup[k] = vec

    feat_set = set(candidate_lookup.keys())

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

        # 2. Strict grain parity check between memberships and feature bundle (Fail-Closed)
        mem_grains = list(zip(mem_df["mainshock_event_id"], mem_df["candidate_event_id"]))
        if len(mem_grains) != len(set(mem_grains)):
            raise ValueError(f"DUPLICATE_GRAIN_DETECTED: Result '{sdir.name}' contains duplicate grains in memberships.parquet")

        mem_set = set(mem_grains)
        unknown_in_mem = mem_set - feat_set
        if unknown_in_mem:
            raise ValueError(f"UNKNOWN_CANDIDATE_GRAIN: Result '{sdir.name}' contains {len(unknown_in_mem)} unknown candidate grains not present in feature bundle: {list(unknown_in_mem)[:3]}")

        missing_in_mem = feat_set - mem_set
        if missing_in_mem:
            raise ValueError(f"MISSING_CANDIDATE_GRAIN: Result '{sdir.name}' is missing {len(missing_in_mem)} candidate grains present in feature bundle: {list(missing_in_mem)[:3]}")

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
                if k not in candidate_lookup:
                    raise ValueError(f"UNKNOWN_CANDIDATE_GRAIN: Candidate '{k[1]}' for mainshock '{k[0]}' not found in feature bundle")
                vec_list.append(candidate_lookup[k])

            vec_arr = np.array(vec_list, dtype=np.float64)
            labels_arr = np.array([int(c) if (c is not None and not (isinstance(c, (float, np.floating)) and math.isnan(c))) else -1 for c in window_mems["cluster_id"]], dtype=np.int32)

            # Fail-closed null policy for failed/no-productive windows:
            if status_str != "SUCCESS":
                density_res = {
                    "score": None,
                    "reason": "NO_PRODUCTIVE_CLUSTERS",
                    "silhouette_score": None,
                    "silhouette_reason": "NO_PRODUCTIVE_CLUSTERS",
                }
            else:
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
                "silhouette_score": density_res.get("silhouette_score"),
                "silhouette_reason": density_res.get("silhouette_reason"),
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
        "dbcv_score", "dbcv_reason",
        "silhouette_score", "silhouette_reason",
        "runtime_seconds",
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

    feat_grains = set(zip(features_df["mainshock_event_id"], features_df["candidate_event_id"]))
    mem_grains = set(zip(mem_df["mainshock_event_id"], mem_df["candidate_event_id"]))

    if len(mem_grains) != len(mem_df):
        raise ValueError("DUPLICATE_GRAIN_DETECTED: memberships.parquet contains duplicate grains")

    unknown_grains = mem_grains - feat_grains
    if unknown_grains:
        raise ValueError(f"UNKNOWN_CANDIDATE_GRAIN: memberships.parquet contains grains not found in feature bundle: {list(unknown_grains)[:3]}")

    missing_grains = feat_grains - mem_grains
    if missing_grains:
        raise ValueError(f"MISSING_CANDIDATE_GRAIN: memberships.parquet is missing feature grains: {list(missing_grains)[:3]}")

    merged = features_df.merge(mem_df, on=["mainshock_event_id", "candidate_event_id"], suffixes=("", "_mem"))
    if len(merged) != len(features_df):
        raise ValueError(f"MERGE_ROW_COUNT_MISMATCH: Expected {len(features_df)} rows, got {len(merged)}")

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
