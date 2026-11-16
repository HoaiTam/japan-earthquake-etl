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
    Noise points are marked as -1. The denominator for cluster weighting uses the entire population N
    (including noise), preserving exact scientific semantics and parity with hdbscan.validity.
    Requires at least 2 distinct clusters with label >= 0, and at least 2 samples per cluster.
    """
    if X is None or len(X) == 0 or labels is None or len(labels) == 0:
        return None, "NO_PRODUCTIVE_CLUSTERS"

    labels = np.asarray(labels, dtype=np.int64)
    X = np.asarray(X, dtype=np.float64)
    n_samples = float(len(X))

    valid_mask = labels >= 0
    unique_clusters = np.unique(labels[valid_mask])

    if len(unique_clusters) < 1:
        return None, "NO_PRODUCTIVE_CLUSTERS"

    if len(unique_clusters) < 2:
        return None, "DBCV_REQUIRES_AT_LEAST_TWO_CLUSTERS"

    for c in unique_clusters:
        if np.sum(labels == c) < 2:
            return None, "INSUFFICIENT_CLUSTER_SAMPLES"

    d = dim if dim is not None else X.shape[1]
    core_distances = {}
    density_sparseness = {}
    mst_internal_nodes = {}

    for c in unique_clusters:
        pts = X[labels == c]
        n_c = len(pts)
        dist_mat = cdist(pts, pts)
        inv_d_mat = np.zeros_like(dist_mat)
        mask = dist_mat != 0
        inv_d_mat[mask] = (1.0 / dist_mat[mask]) ** d
        sum_inv = inv_d_mat.sum(axis=1) / (n_c - 1)
        cd = np.zeros(n_c)
        pos = sum_inv > 0
        cd[pos] = sum_inv[pos] ** (-1.0 / d)
        core_distances[c] = cd

        mrd = np.maximum(dist_mat, np.maximum(cd[:, None], cd[None, :]))
        mst = minimum_spanning_tree(mrd).toarray()
        adj = (mst > 0) | (mst.T > 0)
        deg = adj.sum(axis=1)
        internal_nodes = np.where(deg > 1)[0]
        if len(internal_nodes) == 0:
            internal_nodes = np.array([0])
        mst_internal_nodes[c] = internal_nodes

        internal_mask = np.zeros((n_c, n_c), dtype=bool)
        internal_mask[np.ix_(internal_nodes, internal_nodes)] = True
        internal_edges_weights = mst[(mst > 0) & (internal_mask | internal_mask.T)]
        if len(internal_edges_weights) > 0:
            density_sparseness[c] = float(np.max(internal_edges_weights))
        else:
            density_sparseness[c] = float(np.max(mst[mst > 0])) if np.any(mst > 0) else 0.0

    min_density_sep = {}
    for c in unique_clusters:
        seps = []
        for other in unique_clusters:
            if other == c:
                continue
            pts_c = X[labels == c][mst_internal_nodes[c]]
            pts_other = X[labels == other][mst_internal_nodes[other]]
            dist_cross = cdist(pts_c, pts_other)
            cd_c = core_distances[c][mst_internal_nodes[c]]
            cd_other = core_distances[other][mst_internal_nodes[other]]
            mr_cross = np.maximum(dist_cross, np.maximum(cd_c[:, None], cd_other[None, :]))
            seps.append(np.min(mr_cross))
        min_density_sep[c] = min(seps)

    result = 0.0
    for c in unique_clusters:
        sep = min_density_sep[c]
        dsc = density_sparseness[c]
        denom = max(sep, dsc)
        val = (sep - dsc) / denom if denom > 1e-12 else 0.0
        c_size = float(np.sum(labels == c))
        result += (c_size / n_samples) * val

    if math.isnan(result) or math.isinf(result):
        return None, "METRIC_FIT_FAILED"

    return round(float(result), 4), "VALID_DBCV_SCORE"


def compute_density_metric(
    vectors: np.ndarray,
    labels: np.ndarray,
    algorithm_name: str,
) -> Dict[str, Any]:
    """Computes genuine density-based clustering validation (DBCV) or records explicit null with reason.

    Also tracks secondary silhouette metrics in separate fields; never substitutes silhouette for DBCV.
    Evaluates DBCV on the entire window population (including noise as -1), maintaining exact scientific
    semantics and parity with Moulavi et al. (2014) and hdbscan.validity.
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

    labels = np.asarray(labels, dtype=np.int64)
    vectors = np.asarray(vectors, dtype=np.float64)

    valid_mask = labels >= 0
    unique_clusters = np.unique(labels[valid_mask])

    if len(unique_clusters) < 1:
        return {
            "score": None,
            "reason": "NO_PRODUCTIVE_CLUSTERS",
            "silhouette_score": None,
            "silhouette_reason": "NO_PRODUCTIVE_CLUSTERS",
        }

    if len(unique_clusters) < 2:
        return {
            "score": None,
            "reason": "DBCV_REQUIRES_AT_LEAST_TWO_CLUSTERS",
            "silhouette_score": None,
            "silhouette_reason": "SILHOUETTE_REQUIRES_AT_LEAST_TWO_CLUSTERS",
        }

    cluster_points = vectors[valid_mask]
    cluster_labels = labels[valid_mask]

    for c in unique_clusters:
        if np.sum(labels == c) < 2:
            return {
                "score": None,
                "reason": "INSUFFICIENT_CLUSTER_SAMPLES",
                "silhouette_score": None,
                "silhouette_reason": "INSUFFICIENT_CLUSTER_SAMPLES",
            }

    # 1. Compute Genuine DBCV Metric on entire window (vectors and labels with -1 for noise)
    dbcv_val = None
    dbcv_reason = "METRIC_FIT_FAILED"
    try:
        try:
            import hdbscan.validity as hdb_validity
            X_contig = np.ascontiguousarray(vectors, dtype=np.float64)
            labels_int = np.ascontiguousarray(labels, dtype=np.int64)
            raw_score = float(hdb_validity.validity_index(X_contig, labels_int))
            if not (math.isnan(raw_score) or math.isinf(raw_score)):
                dbcv_val = round(raw_score, 4)
                dbcv_reason = "VALID_DBCV_SCORE"
            else:
                dbcv_val, dbcv_reason = compute_dbcv(vectors, labels)
        except (ImportError, Exception):
            dbcv_val, dbcv_reason = compute_dbcv(vectors, labels)
    except Exception as e:
        dbcv_val = None
        dbcv_reason = f"METRIC_FIT_FAILED: {str(e)}"

    # 2. Compute Separate Silhouette Metric (explicitly separate from DBCV, on clustered points only)
    sil_val = None
    sil_reason = "NO_PRODUCTIVE_CLUSTERS"
    try:
        from sklearn.metrics import silhouette_score
        if len(unique_clusters) >= 2 and len(cluster_points) > len(unique_clusters):
            sil_val = round(float(silhouette_score(cluster_points, cluster_labels)), 4)
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


def verify_result_bundle(
    result_dir: Path,
    bundle: Dict[str, Any],
) -> Dict[str, Any]:
    """Verifies result manifest, artifact checksums, pilot identity, and config integrity.

    Fails closed if:
    - pilot_result_manifest.json is missing or corrupted.
    - Foreign pilot ID (manifest pilot_id does not match feature bundle).
    - Missing required artifact files (memberships, sequence_summary, config, metrics).
    - Any artifact SHA-256 checksum does not match manifest.
    - Config is missing, empty, or conflicts with manifest metadata.
    """
    result_dir = Path(result_dir).resolve()
    manifest_p = result_dir / "pilot_result_manifest.json"
    require(manifest_p.is_file() and not manifest_p.is_symlink(), f"MISSING_RESULT_MANIFEST: Result '{result_dir.name}' is missing pilot_result_manifest.json")

    try:
        manifest_bytes = manifest_p.read_bytes()
        manifest_data = json.loads(manifest_bytes.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"CORRUPT_RESULT_MANIFEST: Result '{result_dir.name}' has invalid JSON: {str(e)}")

    require(isinstance(manifest_data, dict) and bool(manifest_data), f"INVALID_RESULT_MANIFEST: Result '{result_dir.name}' manifest is empty or not a dict")

    # 1. Pilot ID Verification
    bundle_pid = bundle.get("bundle_id")
    res_pid = manifest_data.get("pilot_id")
    require(bool(res_pid), f"MISSING_PILOT_ID: Result '{result_dir.name}' manifest is missing pilot_id")
    if bundle_pid:
        require(res_pid == bundle_pid, f"FOREIGN_PILOT_ID: Result '{result_dir.name}' pilot_id '{res_pid}' does not match feature bundle pilot_id '{bundle_pid}'")

    # 2. Checksum Verification of Artifacts
    checksums = manifest_data.get("checksums")
    require(isinstance(checksums, dict) and bool(checksums), f"MISSING_MANIFEST_CHECKSUMS: Result '{result_dir.name}' has no checksums map")

    required_artifacts = ["memberships.parquet", "sequence_summary.parquet", "experiment_config.json", "experiment_metrics.json"]
    for art in required_artifacts:
        require(art in checksums, f"MISSING_REQUIRED_ARTIFACT_IN_MANIFEST: '{art}' not in checksums of '{result_dir.name}'")

    for fname, expected_hash in checksums.items():
        target_f = result_dir / fname
        require(target_f.is_file() and not target_f.is_symlink(), f"MISSING_MANIFEST_FILE: Result '{result_dir.name}' is missing '{fname}'")
        actual_hash = sha256_bytes(target_f.read_bytes())
        require(actual_hash == expected_hash, f"CHECKSUM_MISMATCH: Result '{result_dir.name}' file '{fname}': expected {expected_hash}, got {actual_hash}")

    # 3. Experiment Config & Metrics Verification
    cfg_p = result_dir / "experiment_config.json"
    require(cfg_p.is_file(), f"MISSING_CONFIG_FILE: Result '{result_dir.name}' missing experiment_config.json")
    try:
        config = json.loads(cfg_p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"CORRUPT_CONFIG_FILE: Result '{result_dir.name}' config is invalid JSON: {str(e)}")
    require(isinstance(config, dict) and bool(config), f"INVALID_OR_EMPTY_CONFIG: Result '{result_dir.name}' has empty or invalid config")

    algo_name = config.get("algorithm_name")
    require(bool(algo_name), f"MISSING_ALGORITHM_NAME_IN_CONFIG: Result '{result_dir.name}'")
    require(algo_name == manifest_data.get("algorithm_name"), f"ALGORITHM_NAME_MISMATCH: config '{algo_name}' != manifest '{manifest_data.get('algorithm_name')}' in '{result_dir.name}'")

    cfg_version = config.get("model_config_version")
    require(bool(cfg_version), f"MISSING_MODEL_CONFIG_VERSION_IN_CONFIG: Result '{result_dir.name}'")
    require(cfg_version == manifest_data.get("model_config_version"), f"MODEL_CONFIG_VERSION_MISMATCH: config '{cfg_version}' != manifest '{manifest_data.get('model_config_version')}' in '{result_dir.name}'")

    metrics_p = result_dir / "experiment_metrics.json"
    require(metrics_p.is_file(), f"MISSING_METRICS_FILE: Result '{result_dir.name}' missing experiment_metrics.json")
    try:
        metrics = json.loads(metrics_p.read_text(encoding="utf-8"))
    except Exception as e:
        raise ValueError(f"CORRUPT_METRICS_FILE: Result '{result_dir.name}' metrics is invalid JSON: {str(e)}")
    require(isinstance(metrics, dict), f"INVALID_METRICS_FILE: Result '{result_dir.name}' metrics is not a dict")

    run_id = manifest_data.get("pilot_run_id") or manifest_data.get("experiment_run_id")
    if metrics.get("experiment_run_id"):
        require(metrics.get("experiment_run_id") == run_id, f"RUN_ID_MISMATCH: metrics '{metrics.get('experiment_run_id')}' != manifest '{run_id}' in '{result_dir.name}'")

    return {
        "algorithm_name": algo_name,
        "model_config_version": cfg_version,
        "pilot_run_id": run_id,
        "pilot_id": res_pid,
        "code_version": manifest_data.get("code_version", "unknown"),
        "manifest_sha256": sha256_bytes(manifest_bytes),
        "config_sha256": checksums.get("experiment_config.json"),
        "metrics_sha256": checksums.get("experiment_metrics.json"),
        "memberships_sha256": checksums.get("memberships.parquet"),
        "sequence_summary_sha256": checksums.get("sequence_summary.parquet"),
    }


def extract_baseline_metrics(
    results_dir: Path,
    bundle: Dict[str, Any],
    return_provenance: bool = False,
) -> Union[List[Dict[str, Any]], Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
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

    expected_mainshocks = set(features_df["mainshock_event_id"].unique())
    if bundle.get("mainshocks_table"):
        expected_mainshocks.update(bundle["mainshocks_table"].to_pandas()["mainshock_event_id"].unique())
    require(len(expected_mainshocks) > 0, "NO_EXPECTED_MAINSHOCKS: Feature bundle has no mainshock windows")

    table_rows = []
    source_provenance = {}

    # Discover candidate result subdirectories
    subdirs = [p for p in results_dir.iterdir() if p.is_dir() and (p / "memberships.parquet").is_file()]
    require(len(subdirs) > 0, f"NO_VALID_RESULT_DIRECTORIES_IN: {results_dir}")

    for sdir in sorted(subdirs, key=lambda p: p.name):
        prov = verify_result_bundle(sdir, bundle)
        source_provenance[sdir.name] = prov

        summary_path = sdir / "sequence_summary.parquet"
        mem_path = sdir / "memberships.parquet"

        summary_table = pq.read_table(summary_path)
        mem_table = pq.read_table(mem_path)

        mem_df = mem_table.to_pandas()
        summary_rows = summary_table.to_pylist()

        algo_name = prov["algorithm_name"]
        cfg_version = prov["model_config_version"]

        # Summary completeness and parity
        require(len(summary_rows) > 0, f"EMPTY_SUMMARY_TABLE: Result '{sdir.name}' has empty sequence_summary.parquet")

        summary_mids = [r["mainshock_event_id"] for r in summary_rows]
        if len(summary_mids) != len(set(summary_mids)):
            raise ValueError(f"DUPLICATE_SUMMARY_WINDOW: Result '{sdir.name}' contains duplicate mainshocks in sequence_summary: {summary_mids}")

        unknown_mids = set(summary_mids) - expected_mainshocks
        if unknown_mids:
            raise ValueError(f"UNKNOWN_SUMMARY_WINDOW: Result '{sdir.name}' contains unknown mainshock in sequence_summary: {list(unknown_mids)}")

        missing_mids = expected_mainshocks - set(summary_mids)
        if missing_mids:
            raise ValueError(f"MISSING_SUMMARY_WINDOW: Result '{sdir.name}' is missing expected mainshocks in sequence_summary: {list(missing_mids)}")

        # Strict grain parity check between memberships and feature bundle (Fail-Closed)
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

        # Reconciliation between summary and memberships
        for s_row in summary_rows:
            ms_id = s_row["mainshock_event_id"]
            window_mems = mem_df[mem_df["mainshock_event_id"] == ms_id]
            require(len(window_mems) > 0, f"EMPTY_WINDOW_MEMBERSHIPS: Result '{sdir.name}' window '{ms_id}' has no rows in memberships")

            total_cands = len(window_mems)
            sum_cands = s_row.get("candidate_count")
            if sum_cands is not None and not (isinstance(sum_cands, float) and math.isnan(sum_cands)):
                if int(sum_cands) != total_cands:
                    raise ValueError(f"CANDIDATE_COUNT_RECONCILIATION_FAILED: Result '{sdir.name}' window '{ms_id}': summary candidate_count={sum_cands} != memberships={total_cands}")

            actual_seq_mems = int((window_mems["is_sequence_member"] == True).sum())
            actual_aftershocks = int(((window_mems["is_sequence_member"] == True) & (window_mems["event_role_candidate"] == "POST")).sum())
            actual_noise = int((window_mems["is_noise"] == True).sum()) if "is_noise" in window_mems else 0

            ms_cand = window_mems[(window_mems["event_role_candidate"] == "MAINSHOCK") | (window_mems["candidate_event_id"] == ms_id)]
            if not ms_cand.empty and "is_noise" in ms_cand:
                val = ms_cand["is_noise"].values[0]
                actual_ms_noise = bool(val) if (val is not None and not (isinstance(val, (float, np.floating)) and math.isnan(val))) else False
            else:
                actual_ms_noise = False
            sum_ms_noise = bool(s_row.get("is_mainshock_noise", False)) or (s_row.get("failure_reason_code") == "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE")
            if actual_ms_noise != sum_ms_noise:
                raise ValueError(f"MAINSHOCK_NOISE_RECONCILIATION_FAILED: Result '{sdir.name}' window '{ms_id}': summary is_mainshock_noise={sum_ms_noise} != memberships actual_ms_noise={actual_ms_noise}")

            sum_status = s_row.get("window_result_status")
            if sum_status == "SUCCESS":
                if actual_ms_noise:
                    raise ValueError(f"STATUS_RECONCILIATION_FAILED: Result '{sdir.name}' window '{ms_id}': summary claims SUCCESS but mainshock is noise in memberships")
                sum_seq = s_row.get("sequence_member_count") if s_row.get("sequence_member_count") is not None else s_row.get("total_sequence_candidates")
                if sum_seq is not None and not (isinstance(sum_seq, float) and math.isnan(sum_seq)):
                    if int(sum_seq) != actual_seq_mems:
                        raise ValueError(f"SEQUENCE_MEMBER_COUNT_RECONCILIATION_FAILED: Result '{sdir.name}' window '{ms_id}': summary sequence_member_count={sum_seq} != memberships={actual_seq_mems}")
                sum_af = s_row.get("aftershock_count")
                if sum_af is not None and not (isinstance(sum_af, float) and math.isnan(sum_af)):
                    if int(sum_af) != actual_aftershocks:
                        raise ValueError(f"AFTERSHOCK_COUNT_RECONCILIATION_FAILED: Result '{sdir.name}' window '{ms_id}': summary aftershock_count={sum_af} != memberships={actual_aftershocks}")
                sum_noise = s_row.get("noise_count")
                if sum_noise is not None and not (isinstance(sum_noise, float) and math.isnan(sum_noise)):
                    if int(sum_noise) != actual_noise:
                        raise ValueError(f"NOISE_COUNT_RECONCILIATION_FAILED: Result '{sdir.name}' window '{ms_id}': summary noise_count={sum_noise} != memberships={actual_noise}")
            elif sum_status in {"FAILED", "NO_PRODUCTIVE_SEQUENCE"}:
                if not actual_ms_noise and actual_aftershocks > 0:
                    raise ValueError(f"STATUS_RECONCILIATION_FAILED: Result '{sdir.name}' window '{ms_id}': summary claims {sum_status} but memberships contain valid aftershocks ({actual_aftershocks})")

            # Determine window status and failure reason for tabular metrics
            status_str = "SUCCESS" if (not actual_ms_noise and actual_aftershocks > 0) or (not actual_ms_noise and algo_name == "WINDOW" and actual_seq_mems > 0) else "NO_PRODUCTIVE_SEQUENCE"
            failure_reason = "MAINSHOCK_CLASSIFIED_AS_NOISE" if actual_ms_noise else ("EMPTY_SEQUENCE" if (actual_seq_mems == 0 or actual_aftershocks == 0) else None)

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
                "sequence_members": actual_seq_mems,
                "aftershocks": actual_aftershocks,
                "noise_candidates": actual_noise,
                "is_mainshock_noise": actual_ms_noise,
                "window_status": status_str,
                "failure_reason": failure_reason,
                "dbcv_score": density_res["score"],
                "dbcv_reason": density_res["reason"],
                "silhouette_score": density_res.get("silhouette_score"),
                "silhouette_reason": density_res.get("silhouette_reason"),
                "runtime_seconds": round(float(s_row.get("runtime_seconds", 0.0)), 4),
            })

    if return_provenance:
        return table_rows, source_provenance
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


CANONICAL_ALGORITHM_ORDER = ("WINDOW", "DBSCAN", "HDBSCAN_GLOBAL")


def select_plot_result_dirs(
    results_dir: Path,
    bundle: Optional[Dict[str, Any]] = None,
    max_dirs: Optional[int] = 4,
) -> List[Path]:
    """Selects representative result subdirectories for spatial and timeline plotting.

    Groups candidate result directories by `algorithm_name` declared in `pilot_result_manifest.json`
    (verified via `verify_result_bundle` when `bundle` is provided), orders algorithm families in
    canonical baseline order (WINDOW, DBSCAN, HDBSCAN_GLOBAL, then others alphabetically),
    prioritizes configurations with at least one SUCCESS window in `sequence_summary.parquet`,
    and interleaves (round-robin) across algorithm families so alphabetical directory names
    never crowd out WINDOW or HDBSCAN_GLOBAL.
    """
    results_dir = Path(results_dir).resolve()
    require(results_dir.is_dir(), f"RESULTS_DIRECTORY_NOT_FOUND: {results_dir}")

    subdirs = sorted(
        [p for p in results_dir.iterdir() if p.is_dir() and (p / "memberships.parquet").is_file()],
        key=lambda p: p.name,
    )
    require(len(subdirs) > 0, f"NO_VALID_RESULT_DIRECTORIES_IN: {results_dir}")

    buckets: Dict[str, List[Tuple[bool, str, Path]]] = {}
    for sdir in subdirs:
        if bundle is not None:
            prov = verify_result_bundle(sdir, bundle)
            algo_name = str(prov["algorithm_name"]).upper()
        else:
            manifest_p = sdir / "pilot_result_manifest.json"
            require(manifest_p.is_file(), f"MISSING_RESULT_MANIFEST: Result '{sdir.name}' is missing pilot_result_manifest.json")
            manifest_data = json.loads(manifest_p.read_text(encoding="utf-8"))
            algo_name = str(manifest_data.get("algorithm_name") or "").upper()
            require(bool(algo_name), f"MISSING_ALGORITHM_NAME_IN_MANIFEST: Result '{sdir.name}'")

        has_success = False
        summary_p = sdir / "sequence_summary.parquet"
        if summary_p.is_file():
            summary_rows = pq.read_table(summary_p).to_pylist()
            has_success = any(r.get("window_result_status") == "SUCCESS" for r in summary_rows)

        buckets.setdefault(algo_name, []).append((not has_success, sdir.name, sdir))

    for algo_name in buckets:
        buckets[algo_name].sort(key=lambda item: (item[0], item[1]))

    ordered_algos: List[str] = [a for a in CANONICAL_ALGORITHM_ORDER if a in buckets]
    ordered_algos.extend(sorted(a for a in buckets if a not in CANONICAL_ALGORITHM_ORDER))

    selected: List[Path] = []
    round_idx = 0
    while True:
        added_in_round = False
        for algo_name in ordered_algos:
            items = buckets[algo_name]
            if round_idx < len(items):
                selected.append(items[round_idx][2])
                added_in_round = True
                if max_dirs is not None and len(selected) >= max_dirs:
                    return selected
        if not added_in_round:
            break
        round_idx += 1

    return selected


def classify_candidate_plot_groups(merged_df: Any) -> Dict[str, Any]:
    """Partitions merged candidate feature+membership rows into 5 mutually exclusive,
    collectively exhaustive display groups so every candidate grain is represented once:
    - 'noise': non-mainshock candidates with is_noise == True
    - 'post_non_sequence': non-mainshock, non-noise, non-sequence POST candidates
    - 'pre_candidates': non-mainshock, non-noise PRE candidates (foreshock candidates)
    - 'sequence_members': non-mainshock, non-noise sequence member POST candidates
    - 'mainshock': mainshock candidates (event_role_candidate == 'MAINSHOCK' or self-id)
    """
    role_series = merged_df["event_role_candidate"].fillna("POST").astype(str)
    is_ms = (role_series == "MAINSHOCK") | (
        merged_df["candidate_event_id"].astype(str) == merged_df["mainshock_event_id"].astype(str)
    )
    if "is_noise" in merged_df.columns:
        is_noise_raw = merged_df["is_noise"].map(lambda v: bool(v) if (v is not None and not (isinstance(v, (float, np.floating)) and math.isnan(v))) else False)
    else:
        is_noise_raw = merged_df.index.map(lambda _: False)
    is_noise = is_noise_raw & (~is_ms)

    is_pre = (~is_ms) & (~is_noise) & (role_series == "PRE")

    if "is_sequence_member" in merged_df.columns:
        is_seq_raw = merged_df["is_sequence_member"].map(lambda v: bool(v) if (v is not None and not (isinstance(v, (float, np.floating)) and math.isnan(v))) else False)
    else:
        is_seq_raw = merged_df.index.map(lambda _: False)

    is_seq = (~is_ms) & (~is_noise) & (~is_pre) & is_seq_raw
    is_post_non_seq = (~is_ms) & (~is_noise) & (~is_pre) & (~is_seq)

    groups = {
        "noise": merged_df[is_noise],
        "post_non_sequence": merged_df[is_post_non_seq],
        "pre_candidates": merged_df[is_pre],
        "sequence_members": merged_df[is_seq],
        "mainshock": merged_df[is_ms],
    }

    total_partitioned = sum(len(df) for df in groups.values())
    require(
        total_partitioned == len(merged_df),
        f"PLOT_GROUP_PARTITION_COUNT_MISMATCH: Partitioned {total_partitioned} rows vs {len(merged_df)} merged rows",
    )
    return groups


def plot_spatial_and_temporal_views(
    bundle: Dict[str, Any],
    memberships_parquet: Path,
    algorithm_name: str,
    output_dir: Path,
) -> Tuple[Path, Path]:
    """Generates relative space (dx_km, dy_km) and time (delta_time_hours) vs depth plots."""
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    memberships_parquet = Path(memberships_parquet).resolve()

    # Strict physical artifact verification
    verify_result_bundle(memberships_parquet.parent, bundle)

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

    groups = classify_candidate_plot_groups(merged)
    noise_pts = groups["noise"]
    post_non_seq_pts = groups["post_non_sequence"]
    pre_pts = groups["pre_candidates"]
    seq_pts = groups["sequence_members"]
    ms_pts = groups["mainshock"]

    partition_grains = set()
    for gdf in groups.values():
        g_set = set(zip(gdf["mainshock_event_id"], gdf["candidate_event_id"]))
        require(len(partition_grains & g_set) == 0, "OVERLAPPING_PLOT_GROUP_GRAINS: Candidate grain appears in multiple plot groups")
        partition_grains.update(g_set)
    require(partition_grains == feat_grains, "PLOT_GROUP_GRAIN_MISMATCH: Plot groups do not cover exact feature grains")

    # Plot 1: Spatial relative coordinates (dx_km vs dy_km)
    fig, ax = plt.subplots(figsize=(8, 7), dpi=150)
    if not noise_pts.empty:
        ax.scatter(noise_pts["dx_km"], noise_pts["dy_km"], c="#888888", marker="x", s=25, alpha=0.6, label="Noise Candidates")
    if not post_non_seq_pts.empty:
        ax.scatter(post_non_seq_pts["dx_km"], post_non_seq_pts["dy_km"], c="#ff7f0e", marker="s", s=28, alpha=0.75, label="Non-Sequence POST")
    if not pre_pts.empty:
        ax.scatter(pre_pts["dx_km"], pre_pts["dy_km"], c="#9467bd", marker="^", s=32, alpha=0.8, label="PRE Candidates")
    if not seq_pts.empty:
        ax.scatter(seq_pts["dx_km"], seq_pts["dy_km"], c="#1f77b4", marker="o", s=35, alpha=0.8, label="Sequence Members")
    if not ms_pts.empty:
        ax.scatter(ms_pts["dx_km"], ms_pts["dy_km"], c="#d62728", marker="*", s=160, edgecolor="black", label="Mainshock (dx=0, dy=0)")

    plotted_spatial_count = sum(len(col.get_offsets()) for col in ax.collections)
    require(
        plotted_spatial_count == len(features_df),
        f"SPATIAL_PLOT_POINT_COUNT_MISMATCH: Plotted {plotted_spatial_count} points, expected {len(features_df)}",
    )

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
        ax.scatter(noise_pts["delta_time_hours"], noise_pts["candidate_depth_km"], c="#888888", marker="x", s=25, alpha=0.6, label="Noise Candidates")
    if not post_non_seq_pts.empty:
        ax.scatter(post_non_seq_pts["delta_time_hours"], post_non_seq_pts["candidate_depth_km"], c="#ff7f0e", marker="s", s=28, alpha=0.75, label="Non-Sequence POST")
    if not pre_pts.empty:
        ax.scatter(pre_pts["delta_time_hours"], pre_pts["candidate_depth_km"], c="#9467bd", marker="^", s=32, alpha=0.8, label="PRE Candidates")
    if not seq_pts.empty:
        ax.scatter(seq_pts["delta_time_hours"], seq_pts["candidate_depth_km"], c="#2ca02c", marker="o", s=35, alpha=0.8, label="Sequence Members")
    if not ms_pts.empty:
        ax.scatter(ms_pts["delta_time_hours"], ms_pts["candidate_depth_km"], c="#d62728", marker="*", s=160, edgecolor="black", label="Mainshock (t=0)")

    plotted_timeline_count = sum(len(col.get_offsets()) for col in ax.collections)
    require(
        plotted_timeline_count == len(features_df),
        f"TIMELINE_PLOT_POINT_COUNT_MISMATCH: Plotted {plotted_timeline_count} points, expected {len(features_df)}",
    )

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
    source_provenance: Optional[Dict[str, Any]] = None,
) -> Path:
    """Creates handoff_manifest.json with strict checksums and pins for EXP-04."""
    require(len(exported_files) > 0, "EMPTY_HANDOFF_FILES: Cannot create handoff manifest with empty files list")
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
    if source_provenance:
        payload["source_provenance"] = source_provenance

    manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return manifest_path
