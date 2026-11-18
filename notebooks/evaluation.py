"""EXP-04: Multi-faceted Scientific Evaluation Module.

Implements rigorous scientific model evaluation for aftershock sequence clustering:
1. Intrinsic geometric clustering quality: DBCV (Density-Based Clustering Validation)
   evaluated on normalized 4-D spatio-temporal coordinates.
   - For WINDOW algorithm, DBCV is NULL with reason METRIC_NOT_APPLICABLE.
   - For windows with all points classified as noise (-1), DBCV is NULL with reason
     METRIC_INSUFFICIENT_MEMBERS.
2. Physical consistency: Modified Omori law decay fit (n(t) = K / (t + c)^p).
   - Fit cumulative empirical aftershock rate against integrated Omori formula.
   - Requires at least 5 candidate aftershocks (N >= 5); otherwise returns NULL with
     reason METRIC_INSUFFICIENT_MEMBERS.
   - Analytic continuous formulation around p=1 with degree-5 Taylor expansion
     to ensure smooth non-zero gradient during non-linear optimization.
3. Stability and sensitivity harness:
   - Scale-aware Gaussian perturbation on space-time features across R repeats.
   - Evaluates pairwise Jaccard similarity across sequence member sets and
     Adjusted Rand Index (ARI) across cluster assignments.
   - Distinguishes execution failures from legitimate all-noise clusterings;
     failed fits never become fake noise labels or fake perfect stability.
   - If valid repeats < 2, returns NULL metrics with METRIC_FIT_FAILED.
4. Denominator integrity:
   - Failure rate = failure_windows / total_windows (all failed windows preserved).
   - Strictly NO classification accuracy or F1 score metrics (unsupervised clustering).
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import numpy as np
from scipy.optimize import curve_fit
from scipy.spatial.distance import cdist
from sklearn.metrics import adjusted_rand_score

# Constant Reason Codes
METRIC_NOT_APPLICABLE = "METRIC_NOT_APPLICABLE"
METRIC_INSUFFICIENT_MEMBERS = "METRIC_INSUFFICIENT_MEMBERS"
METRIC_FIT_FAILED = "METRIC_FIT_FAILED"

MIN_OMORI_AFTERSHOCKS = 5
DEFAULT_STABILITY_REPEATS = 5
DEFAULT_STABILITY_SIGMA = 0.01
DEFAULT_STABILITY_SEED = 42
DBCV_MEMORY_SAFE_CLUSTER_THRESHOLD = 2000
DBCV_FAST_CLUSTER_COUNT_THRESHOLD = 50
DBCV_BLOCK_SIZE = 512

VECTOR_COLUMNS = ["x_scaled", "y_scaled", "z_scaled", "time_scaled"]

DEFAULT_MODEL_CONFIG_PATHS = {
    "WINDOW": "config/ml/models/window_baseline_v1.json",
    "DBSCAN": "config/ml/models/dbscan_global_dev_v1.json",
    "HDBSCAN_GLOBAL": "config/ml/models/hdbscan_global_dev_v1.json",
    "HDBSCAN_ADAPTIVE": "config/ml/models/hdbscan_adaptive_dev_v1.json",
}


def load_algorithm_configs(
    project_root: Optional[Union[str, Path]] = None,
    custom_configs: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Load, validate, and compute exact SHA-256 and versions for all algorithm configs.

    Returns:
        (configs, config_pins)
        - configs: {algo_name: config_dict}
        - config_pins: {algo_name: {config_path, model_config_version, algorithm_version, config_sha256}}
    """
    root = Path(project_root) if project_root else Path(__file__).resolve().parent.parent
    configs: Dict[str, Dict[str, Any]] = {}
    config_pins: Dict[str, Dict[str, Any]] = {}

    for algo, rel_path in DEFAULT_MODEL_CONFIG_PATHS.items():
        abs_path = root / rel_path
        file_bytes_sha256 = None
        file_cfg = None

        if abs_path.is_file():
            raw_file_bytes = abs_path.read_bytes()
            file_bytes_sha256 = hashlib.sha256(raw_file_bytes).hexdigest()
            try:
                file_cfg = json.loads(raw_file_bytes.decode("utf-8"))
            except Exception:
                file_cfg = None

        if custom_configs and algo in custom_configs:
            cfg = dict(custom_configs[algo])
        else:
            if not abs_path.is_file():
                raise FileNotFoundError(f"CONFIG_FILE_MISSING:{abs_path}")
            cfg = file_cfg if file_cfg is not None else json.loads(abs_path.read_bytes().decode("utf-8"))

        canonical_str = json.dumps(cfg, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        canonical_sha256 = hashlib.sha256(canonical_str.encode("utf-8")).hexdigest()

        if custom_configs and algo in custom_configs:
            # If custom config matches the on-disk file, recognize it as file-backed
            if file_cfg is not None and cfg == file_cfg:
                cfg_path = str(rel_path).replace("\\", "/")
                pin_sha256 = file_bytes_sha256
            else:
                cfg_path = f"custom:{algo.lower()}"
                pin_sha256 = canonical_sha256
                file_bytes_sha256 = None
        else:
            cfg_path = str(rel_path).replace("\\", "/")
            pin_sha256 = file_bytes_sha256

        configs[algo] = cfg
        config_pins[algo] = {
            "config_path": cfg_path,
            "algorithm_name": str(cfg.get("algorithm_name", algo)),
            "model_config_version": str(cfg.get("model_config_version", "unknown")),
            "algorithm_version": str(cfg.get("algorithm_version", "unknown")),
            "config_sha256": pin_sha256,
            "file_bytes_sha256": file_bytes_sha256,
            "canonical_config_sha256": canonical_sha256,
        }

    return configs, config_pins


# =============================================================================
# 1. Intrinsic Geometric Quality: DBCV Score
# =============================================================================
def compute_dbcv_score(
    X: np.ndarray,
    labels: np.ndarray,
    algorithm_name: str,
) -> Tuple[Optional[float], List[str]]:
    """Compute Density-Based Clustering Validation (DBCV) score.

    Rules:
    - WINDOW algorithm does not compute density clusters: returns (None, [METRIC_NOT_APPLICABLE]).
    - If all points are noise (-1) or no valid cluster with >= 2 points exists:
      returns (None, [METRIC_INSUFFICIENT_MEMBERS]).
    - Never substitutes 0.0 for a missing or non-applicable DBCV score.
    """
    algo_upper = algorithm_name.upper()
    if "WINDOW" in algo_upper:
        return None, [METRIC_NOT_APPLICABLE]

    if X is None or len(X) == 0 or labels is None or len(labels) == 0:
        return None, [METRIC_INSUFFICIENT_MEMBERS]

    labels_int = np.ascontiguousarray(labels, dtype=np.int64)
    uniq_lbls, uniq_counts = np.unique(labels_int, return_counts=True)
    non_noise_mask = uniq_lbls != -1
    cluster_labels = uniq_lbls[non_noise_mask]
    cluster_sizes = uniq_counts[non_noise_mask]

    if len(cluster_labels) == 0:
        return None, [METRIC_INSUFFICIENT_MEMBERS]

    # Verify at least one cluster has >= 2 points and detect single-point/single-cluster partitions
    max_cluster_size = int(cluster_sizes.max())
    min_cluster_size = int(cluster_sizes.min())
    if max_cluster_size < 2:
        return None, [METRIC_INSUFFICIENT_MEMBERS]
    if min_cluster_size < 2 or len(cluster_labels) < 2:
        return None, [METRIC_FIT_FAILED]

    try:
        X_contig = np.ascontiguousarray(X, dtype=np.float64)

        if (
            max_cluster_size > DBCV_MEMORY_SAFE_CLUSTER_THRESHOLD
            or len(cluster_labels) > DBCV_FAST_CLUSTER_COUNT_THRESHOLD
        ):
            score = _compute_dbcv_memory_safe(X_contig, labels_int)
        else:
            import hdbscan.validity as hdb_validity
            score = float(hdb_validity.validity_index(X_contig, labels_int))

        if score is None or math.isnan(score) or math.isinf(score):
            return None, [METRIC_FIT_FAILED]
        return round(float(score), 4), []
    except Exception:
        return None, [METRIC_FIT_FAILED]


def _safe_all_points_core_distance(Xc: np.ndarray, block_size: int = DBCV_BLOCK_SIZE) -> np.ndarray:
    """Compute Moulavi (2014) all-points core distances using O(block * N) memory."""
    m, d = Xc.shape
    inv_sum = np.zeros(m, dtype=np.float64)
    for start in range(0, m, block_size):
        end = min(start + block_size, m)
        if d == 4:
            sq = cdist(Xc[start:end], Xc, metric="sqeuclidean")
            nz = sq != 0.0
            np.multiply(sq, sq, out=sq, where=nz)
            np.divide(1.0, sq, out=sq, where=nz)
            inv_sum[start:end] = sq.sum(axis=1)
        else:
            dmat = cdist(Xc[start:end], Xc, metric="euclidean")
            nz = dmat != 0.0
            np.power(dmat, float(d), out=dmat, where=nz)
            np.divide(1.0, dmat, out=dmat, where=nz)
            inv_sum[start:end] = dmat.sum(axis=1)
    inv_sum /= float(m - 1)
    core = np.zeros(m, dtype=np.float64)
    pos = inv_sum > 0.0
    if np.any(pos):
        core[pos] = np.power(inv_sum[pos], -1.0 / d)
    return core


def _repair_mst_ties_blocked(
    Xc: np.ndarray,
    c_dist: np.ndarray,
    mst: np.ndarray,
    block_size: int = 256,
) -> np.ndarray:
    """Apply hdbscan.validity.internal_minimum_spanning_tree parent tie-repair in bounded memory."""
    m = len(Xc)
    if len(mst) <= 1:
        return mst.copy()

    repaired = mst.copy()
    in_tree = np.zeros(m, dtype=bool)
    for start in range(1, len(repaired), block_size):
        end = min(start + block_size, len(repaired))
        v_batch = mst[start:end, 1].astype(np.intp)
        w_batch = mst[start:end, 2]
        dists = cdist(Xc[v_batch], Xc, metric="euclidean")
        np.maximum(dists, c_dist[v_batch, None], out=dists)
        np.maximum(dists, c_dist[None, :], out=dists)
        close_mask = np.isclose(dists, w_batch[:, None])
        for offset, idx in enumerate(range(start, end)):
            in_tree[int(mst[idx - 1, 0])] = True
            in_tree[int(mst[idx - 1, 1])] = True
            v = int(v_batch[offset])
            valid = close_mask[offset] & in_tree
            valid[v] = False
            if np.any(valid):
                repaired[idx, 0] = float(np.argmax(valid))
    return repaired


def _compute_dbcv_memory_safe(X: np.ndarray, labels: np.ndarray) -> Optional[float]:
    """Memory-bounded exact Moulavi (2014) DBCV matching hdbscan.validity.validity_index."""
    import hdbscan.validity as hdb_validity
    from hdbscan._hdbscan_linkage import mst_linkage_core_vector
    from hdbscan.dist_metrics import DistanceMetric

    non_noise = np.flatnonzero(labels != -1)
    if len(non_noise) < 2:
        return float("nan")

    nn_labels = labels[non_noise]
    order = np.argsort(nn_labels, kind="mergesort")
    sorted_lbls = nn_labels[order]
    sorted_idxs = non_noise[order]
    split_pts = np.flatnonzero(np.diff(sorted_lbls)) + 1
    groups = np.split(sorted_idxs, split_pts)
    k = len(groups)
    if k < 2:
        # Separation between clusters is undefined when fewer than 2 clusters exist (matches validity_index NaN)
        return float("nan")

    dm_metric = DistanceMetric.get_metric("euclidean")
    node_coords: List[np.ndarray] = []
    node_cores: List[np.ndarray] = []
    node_cids: List[np.ndarray] = []
    sparseness = np.empty(k, dtype=np.float64)

    for ci, idxs in enumerate(groups):
        if len(idxs) < 2:
            return float("nan")
        Xc = np.ascontiguousarray(X[idxs], dtype=np.float64)
        if len(idxs) <= DBCV_MEMORY_SAFE_CLUSTER_THRESHOLD:
            # Exact per-cluster reference path (<= 32 MB for m <= 2000) so tie-breaking & core distances are identical
            dist_mat = cdist(Xc, Xc, metric="euclidean")
            c_dist = hdb_validity.all_points_core_distance(dist_mat.copy(), d=Xc.shape[1])
            mr_dists = np.maximum(dist_mat, c_dist[:, None])
            np.maximum(mr_dists, c_dist[None, :], out=mr_dists)
            nodes_arr, edges = hdb_validity.internal_minimum_spanning_tree(mr_dists)
            nodes = np.asarray(nodes_arr, dtype=np.intp)
        else:
            c_dist = _safe_all_points_core_distance(Xc)
            mst = mst_linkage_core_vector(Xc, c_dist, dm_metric, 1.0)
            repaired = _repair_mst_ties_blocked(Xc, c_dist, mst)
            deg = np.bincount(repaired[:, :2].flatten().astype(np.intp), minlength=len(Xc))
            internal_mask = deg > 1
            if not np.any(internal_mask):
                nodes = np.array([0], dtype=np.intp)
                edges = repaired
            else:
                nodes = np.flatnonzero(internal_mask)
                u_idx = repaired[:, 0].astype(np.intp)
                v_idx = repaired[:, 1].astype(np.intp)
                edge_sel = internal_mask[u_idx] & internal_mask[v_idx]
                edges = repaired[edge_sel] if np.any(edge_sel) else repaired
        node_coords.append(Xc[nodes])
        node_cores.append(c_dist[nodes])
        node_cids.append(np.full(len(nodes), ci, dtype=np.int32))
        sparseness[ci] = float(edges[:, 2].max())

    # Flatten internal MST nodes across all clusters so inter-cluster separation runs in O(M/B) BLAS blocks
    # instead of O(k^2) Python pairwise cluster loops.
    all_X = np.ascontiguousarray(np.vstack(node_coords), dtype=np.float64)
    all_core = np.ascontiguousarray(np.concatenate(node_cores), dtype=np.float64)
    all_cid = np.concatenate(node_cids)

    min_sep = np.full(k, np.inf, dtype=np.float64)
    total_nodes = len(all_X)
    for s in range(0, total_nodes, DBCV_BLOCK_SIZE):
        e = min(s + DBCV_BLOCK_SIZE, total_nodes)
        dm = cdist(all_X[s:e], all_X, metric="euclidean")
        np.maximum(dm, all_core[s:e, None], out=dm)
        np.maximum(dm, all_core[None, :], out=dm)
        dm[all_cid[s:e, None] == all_cid[None, :]] = np.inf
        row_min = dm.min(axis=1)
        np.minimum.at(min_sep, all_cid[s:e], row_min)

    n_tot = float(X.shape[0])
    score = 0.0
    for i, idxs in enumerate(groups):
        min_s = float(min_sep[i])
        sp = float(sparseness[i])
        denom = max(min_s, sp)
        if denom == 0.0 or math.isinf(denom):
            return float("nan")
        v_i = (min_s - sp) / denom
        score += (len(idxs) / n_tot) * v_i
    return float(score)


# =============================================================================
# 2. Physical Consistency: Modified Omori Law Decay Fit
# =============================================================================
def _omori_cumulative(t: np.ndarray, K: float, c: float, p: float) -> np.ndarray:
    """Cumulative form of Modified Omori Law: N(t) = integral_0^t K / (tau + c)^p dtau.

    For p != 1: N(t) = [K / (1 - p)] * [ (t + c)^(1 - p) - c^(1 - p) ]
    For p == 1: N(t) = K * ln( (t + c) / c )

    Numerically stable and continuous for all p > 0:
    Let u = 1 - p, w = ln(1 + t/c).
    integral = c^u * (exp(u*w) - 1) / u.
    For small |u*w| < 1e-4, use degree-5 Taylor expansion so that finite differences
    and analytic derivatives with respect to p around p=1.0 are exact, continuous, and non-zero.
    """
    eps = 1e-6
    c = max(c, eps)
    K = max(K, eps)
    t_eff = np.maximum(t, 0.0)
    w = np.log1p(t_eff / c)
    u = 1.0 - p
    ew = u * w

    small = np.abs(ew) < 1e-4
    taylor = w * (1.0 + ew * (0.5 + ew * (1.0 / 6.0 + ew * (1.0 / 24.0 + ew * (1.0 / 120.0 + ew / 720.0)))))
    with np.errstate(divide="ignore", invalid="ignore"):
        integral = np.where(small, taylor, np.expm1(ew) / u)

    c_u = np.exp(u * np.log(c))
    return K * c_u * integral


def fit_modified_omori_decay(
    aftershock_times_hours: Union[List[float], np.ndarray],
    duration_hours: Optional[float] = None,
    min_aftershocks: int = MIN_OMORI_AFTERSHOCKS,
) -> Dict[str, Any]:
    """Fit Modified Omori Law decay n(t) = K / (t + c)^p on candidate aftershocks.

    Requirements:
    - Input: relative aftershock times t_i > 0 (hours from mainshock).
    - Threshold: Requires at least min_aftershocks (default 5).
    - If N < min_aftershocks: returns null parameters with METRIC_INSUFFICIENT_MEMBERS.
    - If fit fails to converge: returns null parameters with METRIC_FIT_FAILED.
    - Never silently drops window; preserves sample size in output metadata.
    """
    if aftershock_times_hours is None:
        times_arr = np.array([], dtype=np.float64)
    else:
        times_arr = np.asarray(aftershock_times_hours, dtype=np.float64)

    # Filter strictly positive times
    times_arr = times_arr[times_arr > 0]
    n_aftershocks = len(times_arr)

    if n_aftershocks < min_aftershocks:
        return {
            "omori_k": None,
            "omori_c": None,
            "omori_p": None,
            "omori_fit_metric": None,
            "metric_reason_codes": [METRIC_INSUFFICIENT_MEMBERS],
            "aftershock_sample_size": n_aftershocks,
        }

    # Sort times in ascending order
    times_sorted = np.sort(times_arr)
    # Empirical cumulative counts
    counts_empirical = np.arange(1, n_aftershocks + 1, dtype=np.float64)

    # Initial guess and parameter bounds
    k0 = float(n_aftershocks)
    c0 = 0.1  # hours (~6 minutes)
    p0 = 1.0  # standard Omori decay exponent
    p_bounds = (
        [1e-3, 1e-4, 0.2],    # Lower bounds [K, c, p]
        [1e6, 100.0, 2.5],    # Upper bounds [K, c, p]
    )

    try:
        popt, _ = curve_fit(
            _omori_cumulative,
            times_sorted,
            counts_empirical,
            p0=[k0, c0, p0],
            bounds=p_bounds,
            maxfev=3000,
        )
        fitted_k, fitted_c, fitted_p = popt

        # Calculate goodness of fit: R^2 on cumulative curve
        pred_counts = _omori_cumulative(times_sorted, fitted_k, fitted_c, fitted_p)
        ss_res = np.sum((counts_empirical - pred_counts)**2)
        ss_tot = np.sum((counts_empirical - np.mean(counts_empirical))**2)
        r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 0 else 0.0

        if math.isnan(fitted_k) or math.isnan(fitted_c) or math.isnan(fitted_p) or math.isnan(r2):
            return {
                "omori_k": None,
                "omori_c": None,
                "omori_p": None,
                "omori_fit_metric": None,
                "metric_reason_codes": [METRIC_FIT_FAILED],
                "aftershock_sample_size": n_aftershocks,
            }

        return {
            "omori_k": round(float(fitted_k), 4),
            "omori_c": round(float(fitted_c), 4),
            "omori_p": round(float(fitted_p), 4),
            "omori_fit_metric": round(float(r2), 4),
            "metric_reason_codes": [],
            "aftershock_sample_size": n_aftershocks,
        }
    except Exception:
        return {
            "omori_k": None,
            "omori_c": None,
            "omori_p": None,
            "omori_fit_metric": None,
            "metric_reason_codes": [METRIC_FIT_FAILED],
            "aftershock_sample_size": n_aftershocks,
        }


# =============================================================================
# 3. Model Stability & Sensitivity Harness
# =============================================================================
def calculate_jaccard_similarity(set_a: Set[str], set_b: Set[str]) -> float:
    """Compute Jaccard similarity index between two sequence member ID sets.

    Edge cases:
    - Both sets empty: returns 1.0 (unanimous agreement on absence of members).
    - One set empty, other non-empty: returns 0.0.
    """
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a.intersection(set_b))
    union = len(set_a.union(set_b))
    return float(intersection / union) if union > 0 else 1.0


def _dispatch_fit_model(
    algorithm_name: str,
    window_candidates: List[Dict[str, Any]],
    mainshock_id: str,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Helper to execute model fit for a single window."""
    algo_upper = algorithm_name.upper()

    try:
        from notebooks.baseline_models import (
            fit_window_baseline,
            fit_dbscan_baseline,
            fit_hdbscan_global_baseline,
        )
        from notebooks.adaptive_hdbscan import fit_hdbscan_adaptive
    except ImportError:
        from baseline_models import (
            fit_window_baseline,
            fit_dbscan_baseline,
            fit_hdbscan_global_baseline,
        )
        from adaptive_hdbscan import fit_hdbscan_adaptive

    if algo_upper == "WINDOW":
        return fit_window_baseline(window_candidates, mainshock_id, config=config)
    elif algo_upper == "DBSCAN":
        return fit_dbscan_baseline(window_candidates, mainshock_id, config=config)
    elif algo_upper == "HDBSCAN_GLOBAL":
        return fit_hdbscan_global_baseline(window_candidates, mainshock_id, config=config)
    elif algo_upper == "HDBSCAN_ADAPTIVE":
        return fit_hdbscan_adaptive(window_candidates, mainshock_id, config=config)
    else:
        raise ValueError(f"UNKNOWN_ALGORITHM:{algorithm_name}")


def run_window_stability_analysis(
    window_candidates: List[Dict[str, Any]],
    mainshock_id: str,
    algorithm_name: str,
    config: Optional[Dict[str, Any]] = None,
    repeats: int = DEFAULT_STABILITY_REPEATS,
    sigma: float = DEFAULT_STABILITY_SIGMA,
    seed: int = DEFAULT_STABILITY_SEED,
) -> Dict[str, Any]:
    """Run perturbation stability analysis for a single mainshock window.

    Applies small Gaussian feature jitter (sigma=0.01 on 4-D space-time features)
    over R repeats, evaluating:
    1. Pairwise Jaccard similarity across sequence member sets.
    2. Pairwise Adjusted Rand Index (ARI) across cluster assignments.
    Returns complete components without lossy single composite score.

    Failure & exception policy:
    - repeats must be >= 2.
    - sigma must be finite and >= 0.0.
    - If fitting raises an exception, the repeat is recorded as failed.
      Failures are NOT converted into fake noise labels or empty member sets.
    - If valid repeats < 2: returns null metrics with METRIC_FIT_FAILED.
    """
    if repeats < 2:
        return {
            "mean_jaccard": None,
            "std_jaccard": None,
            "min_jaccard": None,
            "max_jaccard": None,
            "mean_ari": None,
            "std_ari": None,
            "min_ari": None,
            "max_ari": None,
            "repeats": repeats,
            "requested_repeats": repeats,
            "sigma": sigma,
            "seed": seed,
            "attempted_repeats": 0,
            "valid_repeats": 0,
            "failed_repeats": 0,
            "attempted_pairs": 0,
            "valid_pairs": 0,
            "metric_reason_codes": [METRIC_NOT_APPLICABLE],
            "failed_repeat_details": [],
            "note": "INVALID_REPEATS_LESS_THAN_2 / NOT_RUN",
        }

    if not math.isfinite(sigma) or sigma < 0.0:
        return {
            "mean_jaccard": None,
            "std_jaccard": None,
            "min_jaccard": None,
            "max_jaccard": None,
            "mean_ari": None,
            "std_ari": None,
            "min_ari": None,
            "max_ari": None,
            "repeats": repeats,
            "requested_repeats": repeats,
            "sigma": sigma,
            "seed": seed,
            "attempted_repeats": 0,
            "valid_repeats": 0,
            "failed_repeats": 0,
            "attempted_pairs": 0,
            "valid_pairs": 0,
            "metric_reason_codes": [METRIC_NOT_APPLICABLE],
            "failed_repeat_details": [],
            "note": "INVALID_SIGMA / NOT_RUN",
        }

    if not window_candidates:
        return {
            "mean_jaccard": None,
            "std_jaccard": None,
            "min_jaccard": None,
            "max_jaccard": None,
            "mean_ari": None,
            "std_ari": None,
            "min_ari": None,
            "max_ari": None,
            "repeats": repeats,
            "requested_repeats": repeats,
            "sigma": sigma,
            "seed": seed,
            "attempted_repeats": 0,
            "valid_repeats": 0,
            "failed_repeats": 0,
            "attempted_pairs": 0,
            "valid_pairs": 0,
            "metric_reason_codes": [METRIC_NOT_APPLICABLE],
            "failed_repeat_details": [],
            "note": "EMPTY_WINDOW_CANDIDATES / NOT_RUN",
        }

    mainshock_present = any(
        c.get("candidate_event_id", c.get("event_id")) == mainshock_id
        for c in window_candidates
    )
    if not mainshock_present:
        return {
            "mean_jaccard": None,
            "std_jaccard": None,
            "min_jaccard": None,
            "max_jaccard": None,
            "mean_ari": None,
            "std_ari": None,
            "min_ari": None,
            "max_ari": None,
            "repeats": repeats,
            "requested_repeats": repeats,
            "sigma": sigma,
            "seed": seed,
            "attempted_repeats": 0,
            "valid_repeats": 0,
            "failed_repeats": 0,
            "attempted_pairs": 0,
            "valid_pairs": 0,
            "metric_reason_codes": [METRIC_NOT_APPLICABLE],
            "failed_repeat_details": [],
            "note": "MAINSHOCK_NOT_IN_CANDIDATES / NOT_RUN",
        }

    valid_runs: List[Tuple[Set[str], List[int]]] = []
    failed_repeats: List[Dict[str, Any]] = []

    for r in range(repeats):
        rng = np.random.RandomState(seed + r * 100)
        perturbed_candidates = []
        for c in window_candidates:
            cid = c.get("candidate_event_id", c.get("event_id"))
            is_mainshock = (cid == mainshock_id)
            c_copy = dict(c)
            if not is_mainshock and sigma > 0:
                for col in VECTOR_COLUMNS:
                    val = float(c.get(col, 0.0))
                    jitter = float(rng.normal(0.0, sigma))
                    c_copy[col] = val + jitter
            perturbed_candidates.append(c_copy)

        try:
            mem_rows, _ = _dispatch_fit_model(
                algorithm_name=algorithm_name,
                window_candidates=perturbed_candidates,
                mainshock_id=mainshock_id,
                config=config,
            )
            # Extract sequence member candidate IDs
            members = {r["candidate_event_id"] for r in mem_rows if r.get("is_sequence_member", False)}
            # Extract ordered cluster labels (-1 for noise/None)
            labels = [int(r.get("cluster_id") if r.get("cluster_id") is not None else -1) for r in mem_rows]
            valid_runs.append((members, labels))
        except Exception as ex:
            failed_repeats.append({
                "repeat_index": r,
                "error_type": type(ex).__name__,
                "error_message": str(ex),
            })

    attempted_pairs = repeats * (repeats - 1) // 2
    n_valid = len(valid_runs)
    n_failed = len(failed_repeats)

    if n_valid < 2:
        return {
            "mean_jaccard": None,
            "std_jaccard": None,
            "min_jaccard": None,
            "max_jaccard": None,
            "mean_ari": None,
            "std_ari": None,
            "min_ari": None,
            "max_ari": None,
            "repeats": repeats,
            "requested_repeats": repeats,
            "sigma": sigma,
            "seed": seed,
            "attempted_repeats": repeats,
            "valid_repeats": n_valid,
            "failed_repeats": n_failed,
            "attempted_pairs": attempted_pairs,
            "valid_pairs": 0,
            "metric_reason_codes": [METRIC_FIT_FAILED],
            "failed_repeat_details": failed_repeats,
        }

    # Compute pairwise similarities only across valid runs
    jaccard_scores = []
    ari_scores = []

    for i in range(n_valid):
        for j in range(i + 1, n_valid):
            j_score = calculate_jaccard_similarity(valid_runs[i][0], valid_runs[j][0])
            jaccard_scores.append(j_score)

            a_labels = valid_runs[i][1]
            b_labels = valid_runs[j][1]
            ari_score = adjusted_rand_score(a_labels, b_labels)
            ari_scores.append(float(ari_score))

    mean_j = float(np.mean(jaccard_scores))
    std_j = float(np.std(jaccard_scores))
    min_j = float(np.min(jaccard_scores))
    max_j = float(np.max(jaccard_scores))

    mean_a = float(np.mean(ari_scores))
    std_a = float(np.std(ari_scores))
    min_a = float(np.min(ari_scores))
    max_a = float(np.max(ari_scores))

    reason_codes = []
    if n_failed > 0:
        reason_codes.append("PARTIAL_FIT_FAILURES")

    return {
        "mean_jaccard": round(mean_j, 4),
        "std_jaccard": round(std_j, 4),
        "min_jaccard": round(min_j, 4),
        "max_jaccard": round(max_j, 4),
        "mean_ari": round(mean_a, 4),
        "std_ari": round(std_a, 4),
        "min_ari": round(min_a, 4),
        "max_ari": round(max_a, 4),
        "repeats": repeats,
        "requested_repeats": repeats,
        "sigma": sigma,
        "seed": seed,
        "attempted_repeats": repeats,
        "valid_repeats": n_valid,
        "failed_repeats": n_failed,
        "attempted_pairs": attempted_pairs,
        "valid_pairs": len(jaccard_scores),
        "metric_reason_codes": reason_codes,
        "failed_repeat_details": failed_repeats,
    }


# =============================================================================
# 4. Multi-Model Pilot Evaluation & Comparison Table Engine
# =============================================================================
def _opt_round(val: Optional[float], digits: int = 10) -> Optional[float]:
    if val is None:
        return None
    f = float(val)
    if not math.isfinite(f):
        return None
    return round(f, digits)


def compute_primary_membership_digest(rows: List[Dict[str, Any]]) -> str:
    """Compute canonical SHA-256 over primary per-window clustering membership outcomes."""
    records = sorted(
        (
            str(r.get("mainshock_event_id") or ""),
            str(r.get("candidate_event_id") or ""),
            str(r.get("event_role_candidate") or ""),
            bool(r.get("is_sequence_member")),
            None if r.get("cluster_id") is None else int(r["cluster_id"]),
            None if r.get("is_noise") is None else bool(r["is_noise"]),
            None if r.get("is_mainshock_cluster_member") is None else bool(r["is_mainshock_cluster_member"]),
            _opt_round(r.get("membership_probability")),
            _opt_round(r.get("normalized_distance")),
        )
        for r in rows
    )
    h = hashlib.sha256()
    for rec in records:
        h.update(json.dumps(rec, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def compute_primary_summary_digest(rows: List[Dict[str, Any]]) -> str:
    """Compute canonical SHA-256 over primary per-window sequence summary counts & statuses."""
    records = sorted(
        (
            str(r.get("mainshock_event_id") or ""),
            str(r.get("window_result_status") or ""),
            r.get("productive_sequence_detected"),
            None if r.get("candidate_count") is None else int(r["candidate_count"]),
            None if r.get("cluster_count") is None else int(r["cluster_count"]),
            None if r.get("noise_count") is None else int(r["noise_count"]),
            None if r.get("sequence_member_count") is None else int(r["sequence_member_count"]),
            None if r.get("aftershock_count") is None else int(r["aftershock_count"]),
            None if r.get("foreshock_candidate_count") is None else int(r["foreshock_candidate_count"]),
            r.get("failure_reason_code"),
        )
        for r in rows
    )
    h = hashlib.sha256()
    for rec in records:
        h.update(json.dumps(rec, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def evaluate_model_on_bundle(
    bundle: Dict[str, Any],
    algorithm_name: str,
    config: Optional[Dict[str, Any]] = None,
    repeats: int = DEFAULT_STABILITY_REPEATS,
    sigma: float = DEFAULT_STABILITY_SIGMA,
    seed: int = DEFAULT_STABILITY_SEED,
) -> Dict[str, Any]:
    """Evaluate a specific algorithm across all windows in a feature bundle.

    Adheres strictly to denominator integrity:
    - denominator: total_windows includes both successful and failed window fits.
    - failure_rate: failure_windows / total_windows.
    - preserves all failed window diagnostics and reason codes.
    """
    t_start = time.time()
    raw_windows = bundle.get("windows")

    if raw_windows is None and bundle.get("features_table") is not None:
        features_table = bundle["features_table"]
        grouped_windows = {}
        for row in features_table.to_pylist():
            m_id = row.get("mainshock_event_id")
            if m_id:
                if m_id not in grouped_windows:
                    grouped_windows[m_id] = []
                grouped_windows[m_id].append(row)
        raw_windows = grouped_windows

    if raw_windows is None:
        raise ValueError("BUNDLE_HAS_NO_WINDOWS: features_table or windows dictionary required")

    total_windows = len(raw_windows)
    success_windows = 0
    failure_windows = 0
    failure_reasons: List[str] = []
    failure_reason_counts: Counter = Counter()
    dbcv_reason_counts: Counter = Counter()
    omori_reason_counts: Counter = Counter()
    stability_reason_counts: Counter = Counter()

    total_candidates = 0
    total_aftershocks = 0
    noise_ratios: List[float] = []

    dbcv_scores: List[float] = []
    omori_p_values: List[float] = []
    omori_fit_metrics: List[float] = []
    omori_fit_success_count = 0

    jaccard_scores: List[float] = []
    ari_scores: List[float] = []

    window_evaluations = []
    primary_membership_rows: List[Dict[str, Any]] = []
    primary_summary_rows: List[Dict[str, Any]] = []

    for mainshock_id, cands in raw_windows.items():
        n_cands = len(cands)
        total_candidates += n_cands

        # 1. Fit algorithm with exception handling to preserve failed window diagnostics
        fit_exception = None
        try:
            mem_rows, sum_row = _dispatch_fit_model(
                algorithm_name=algorithm_name,
                window_candidates=cands,
                mainshock_id=mainshock_id,
                config=config,
            )
            primary_membership_rows.extend(mem_rows)
            primary_summary_rows.append(sum_row)
        except Exception as exc:
            fit_exception = exc
            mem_rows = []
            sum_row = {
                "window_result_status": "FAILURE",
                "failure_reason_code": f"FIT_EXCEPTION: {type(exc).__name__}: {str(exc)}",
                "total_aftershocks": 0,
                "noise_count": 0,
            }

        status = sum_row.get("window_result_status", "UNKNOWN")
        fail_code = sum_row.get("failure_reason_code")

        if status == "SUCCESS":
            success_windows += 1
        else:
            failure_windows += 1
            if fail_code:
                failure_reasons.append(fail_code)
                failure_reason_counts[fail_code] += 1

        aftershock_cnt = sum_row.get("aftershock_count") or sum_row.get("total_aftershocks", 0) or 0
        total_aftershocks += aftershock_cnt

        noise_ratio = None
        if fit_exception is None:
            noise_ratio = sum_row.get("noise_ratio")
            if noise_ratio is None:
                noise_cnt = sum_row.get("noise_count") or sum_row.get("noise_candidates")
                if noise_cnt is not None and n_cands > 0:
                    noise_ratio = float(noise_cnt / n_cands)
                elif "WINDOW" in algorithm_name.upper():
                    noise_ratio = 0.0
            if noise_ratio is not None:
                noise_ratios.append(noise_ratio)

        # 2. Intrinsic Quality: DBCV
        dbcv = None
        dbcv_reasons = []
        if fit_exception is not None:
            dbcv_reasons = [f"FIT_EXCEPTION: {type(fit_exception).__name__}"]
        else:
            X_vec = np.array([[float(c.get(col, 0.0)) for col in VECTOR_COLUMNS] for c in cands], dtype=np.float64)
            labels_arr = np.array([int(r.get("cluster_id") if r.get("cluster_id") is not None else -1) for r in mem_rows], dtype=int)

            dbcv, dbcv_reasons = compute_dbcv_score(
                X=X_vec,
                labels=labels_arr,
                algorithm_name=algorithm_name,
            )
            if dbcv is not None:
                dbcv_scores.append(dbcv)
        if dbcv is None:
            for r_code in dbcv_reasons:
                dbcv_reason_counts[r_code] += 1

        # 3. Physical Consistency: Modified Omori Law
        if fit_exception is not None:
            omori_res = {
                "omori_p": None,
                "omori_fit_metric": None,
                "status": "NOT_FITTED",
                "reasons": [f"FIT_EXCEPTION: {type(fit_exception).__name__}"],
            }
        else:
            cand_time_map = {
                c.get("candidate_event_id", c.get("event_id")): float(c.get("delta_time_hours", 0.0))
                for c in cands
            }
            post_aftershock_times = [
                cand_time_map[r["candidate_event_id"]]
                for r in mem_rows
                if r.get("is_sequence_member", False)
                and r.get("event_role_candidate") == "POST"
                and r["candidate_event_id"] in cand_time_map
            ]
            duration_hours = sum_row.get("sequence_duration_hours")
            omori_res = fit_modified_omori_decay(
                aftershock_times_hours=post_aftershock_times,
                duration_hours=duration_hours,
                min_aftershocks=MIN_OMORI_AFTERSHOCKS,
            )
            if omori_res.get("omori_p") is not None:
                omori_p_values.append(omori_res["omori_p"])
                omori_fit_metrics.append(omori_res["omori_fit_metric"])
                omori_fit_success_count += 1
        if omori_res.get("omori_p") is None:
            for r_code in (omori_res.get("metric_reason_codes") or omori_res.get("reasons") or []):
                omori_reason_counts[r_code] += 1

        # 4. Stability & Sensitivity Harness
        if fit_exception is not None:
            stability_res = {
                "mean_jaccard": None,
                "std_jaccard": None,
                "min_jaccard": None,
                "max_jaccard": None,
                "mean_ari": None,
                "std_ari": None,
                "min_ari": None,
                "max_ari": None,
                "repeats": repeats,
                "requested_repeats": repeats,
                "sigma": sigma,
                "seed": seed,
                "attempted_repeats": 0,
                "valid_repeats": 0,
                "failed_repeats": 0,
                "attempted_pairs": 0,
                "valid_pairs": 0,
                "metric_reason_codes": [METRIC_FIT_FAILED],
                "failed_repeat_details": [],
                "note": f"PRIMARY_FIT_FAILED: {type(fit_exception).__name__} / NOT_RUN",
            }
        else:
            stability_res = run_window_stability_analysis(
                window_candidates=cands,
                mainshock_id=mainshock_id,
                algorithm_name=algorithm_name,
                config=config,
                repeats=repeats,
                sigma=sigma,
                seed=seed,
            )
            if stability_res.get("mean_jaccard") is not None:
                jaccard_scores.append(stability_res["mean_jaccard"])
            if stability_res.get("mean_ari") is not None:
                ari_scores.append(stability_res["mean_ari"])
        for r_code in (stability_res.get("metric_reason_codes") or []):
            stability_reason_counts[r_code] += 1

        # Window evaluation record
        window_eval = {
            "mainshock_id": mainshock_id,
            "candidate_count": n_cands,
            "window_result_status": status,
            "failure_reason_code": fail_code,
            "aftershock_count": aftershock_cnt,
            "noise_ratio": noise_ratio,
            "dbcv_score": dbcv,
            "dbcv_reason_codes": dbcv_reasons,
            "omori_fit": omori_res,
            "stability": stability_res,
            "fit_exception": str(fit_exception) if fit_exception is not None else None,
        }
        window_evaluations.append(window_eval)

    elapsed_seconds = round(time.time() - t_start, 4)

    # Denominator integrity: failure_rate = failure_windows / total_windows
    failure_rate = round(float(failure_windows / total_windows), 4) if total_windows > 0 else 0.0
    mean_noise_ratio = round(float(np.mean(noise_ratios)), 4) if noise_ratios else None
    mean_dbcv = round(float(np.mean(dbcv_scores)), 4) if dbcv_scores else None
    mean_omori_p = round(float(np.mean(omori_p_values)), 4) if omori_p_values else None
    mean_omori_r2 = round(float(np.mean(omori_fit_metrics)), 4) if omori_fit_metrics else None
    mean_jaccard = round(float(np.mean(jaccard_scores)), 4) if jaccard_scores else None
    mean_ari = round(float(np.mean(ari_scores)), 4) if ari_scores else None

    return {
        "algorithm_name": algorithm_name,
        "model_config_version": config.get("model_config_version") if config else None,
        "total_windows": total_windows,
        "success_windows": success_windows,
        "failure_windows": failure_windows,
        "failure_rate": failure_rate,
        "failure_reasons": failure_reasons,
        "failure_reason_counts": dict(sorted(failure_reason_counts.items())),
        "total_candidates": total_candidates,
        "total_aftershocks": total_aftershocks,
        "mean_noise_ratio": mean_noise_ratio,
        "mean_dbcv_score": mean_dbcv,
        "dbcv_computed_windows": len(dbcv_scores),
        "dbcv_reason_counts": dict(sorted(dbcv_reason_counts.items())),
        "mean_omori_p": mean_omori_p,
        "mean_omori_r2": mean_omori_r2,
        "omori_fitted_windows": omori_fit_success_count,
        "omori_reason_counts": dict(sorted(omori_reason_counts.items())),
        "mean_jaccard_stability": mean_jaccard,
        "mean_ari_stability": mean_ari,
        "stability_computed_windows": len(jaccard_scores),
        "stability_reason_counts": dict(sorted(stability_reason_counts.items())),
        "primary_membership_sha256": compute_primary_membership_digest(primary_membership_rows),
        "primary_window_summary_sha256": compute_primary_summary_digest(primary_summary_rows),
        "runtime_seconds": elapsed_seconds,
        "window_evaluations": window_evaluations,
    }


def _build_comparison_table(
    algorithms: List[str],
    comparison_results: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build standardized null-safe comparison table rows across algorithms."""
    comparison_table = []
    for algo in algorithms:
        r = comparison_results[algo]
        j_val = r.get("mean_jaccard_stability")
        a_val = r.get("mean_ari_stability")
        j_str = f"{j_val:.4f}" if j_val is not None and math.isfinite(j_val) else "N/A"
        a_str = f"{a_val:.4f}" if a_val is not None and math.isfinite(a_val) else "N/A"

        f_rate = r.get("failure_rate")
        fail_str = f"{f_rate * 100:.1f}%" if f_rate is not None and math.isfinite(f_rate) else "N/A"

        noise_val = r.get("mean_noise_ratio")
        noise_str = f"{noise_val * 100:.1f}%" if noise_val is not None and math.isfinite(noise_val) else "N/A"

        dbcv_val = r.get("mean_dbcv_score")
        dbcv_str = f"{dbcv_val:.4f}" if dbcv_val is not None and math.isfinite(dbcv_val) else "N/A"

        omori_p = r.get("mean_omori_p")
        omori_p_str = f"{omori_p:.2f}" if omori_p is not None and math.isfinite(omori_p) else "N/A"

        runtime_val = r.get("runtime_seconds")
        runtime_str = f"{runtime_val:.2f}" if runtime_val is not None and math.isfinite(runtime_val) else "N/A"

        comparison_table.append({
            "Algorithm": algo,
            "Total Windows": r.get("total_windows", 0),
            "Success Windows": r.get("success_windows", 0),
            "Failure Windows": r.get("failure_windows", 0),
            "Failure Rate": fail_str,
            "Total Aftershocks": r.get("total_aftershocks", 0),
            "Mean Noise Ratio": noise_str,
            "Mean DBCV": dbcv_str,
            "Mean Omori p": omori_p_str,
            "Omori Fitted Windows": f"{r.get('omori_fitted_windows', 0)}/{r.get('total_windows', 0)}",
            "Mean Jaccard Stability": j_str,
            "Mean ARI Stability": a_str,
            "Runtime (s)": runtime_str,
        })
    return comparison_table


def compare_all_models_on_pilot(
    bundle_dir: Union[str, Path],
    model_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    config_pins: Optional[Dict[str, Dict[str, Any]]] = None,
    repeats: int = DEFAULT_STABILITY_REPEATS,
    sigma: float = DEFAULT_STABILITY_SIGMA,
    seed: int = DEFAULT_STABILITY_SEED,
    project_root: Optional[Union[str, Path]] = None,
) -> Dict[str, Any]:
    """Execute comparative evaluation of the 4 models on a pilot bundle."""
    from experiment_harness import load_feature_bundle, MODE_DEVELOPMENT, require

    bundle = load_feature_bundle(bundle_dir, mode=MODE_DEVELOPMENT)
    manifest = bundle.get("manifest") or bundle.get("pilot_manifest")
    require(manifest is not None and isinstance(manifest, dict), "BUNDLE_MANIFEST_MISSING")

    pilot_id = manifest.get("pilot_id")
    require(
        pilot_id is not None and isinstance(pilot_id, str) and pilot_id.startswith("pilot_"),
        f"INVALID_OR_MISSING_PILOT_ID:{pilot_id}",
    )

    algorithms = ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"]
    if config_pins is not None and model_configs is not None:
        configs = model_configs
        resolved_pins = config_pins
    else:
        configs, resolved_pins = load_algorithm_configs(project_root=project_root, custom_configs=model_configs)

    comparison_results = {
        algo: evaluate_model_on_bundle(
            bundle=bundle,
            algorithm_name=algo,
            config=configs.get(algo),
            repeats=repeats,
            sigma=sigma,
            seed=seed,
        )
        for algo in algorithms
    }

    return {
        "pilot_id": pilot_id,
        "evaluated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "comparison_table": _build_comparison_table(algorithms, comparison_results),
        "algorithm_details": comparison_results,
        "algorithm_config_pins": resolved_pins,
    }


def compare_all_models_on_official(
    bundle_dir: Union[str, Path],
    *,
    expected_split: str = "REPRODUCTION",
    manifest_sha256: Optional[str] = None,
    checksums_sha256: Optional[str] = None,
    handoff_path: Optional[Union[str, Path]] = None,
    expected_handoff_sha256: Optional[str] = None,
    model_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    config_pins: Optional[Dict[str, Dict[str, Any]]] = None,
    repeats: int = DEFAULT_STABILITY_REPEATS,
    sigma: float = DEFAULT_STABILITY_SIGMA,
    seed: int = DEFAULT_STABILITY_SEED,
    project_root: Optional[Union[str, Path]] = None,
    progress_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """Execute multi-faceted scientific evaluation across all windows of an EXPORTED official collection."""
    from experiment_harness import load_feature_bundle, MODE_OFFICIAL, require

    bundle = load_feature_bundle(
        bundle_dir,
        mode=MODE_OFFICIAL,
        manifest_sha256=manifest_sha256,
        checksums_sha256=checksums_sha256,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
    )
    require(bundle.get("mode") == MODE_OFFICIAL and bundle.get("is_official") is True, "OFFICIAL_BUNDLE_REQUIRED")
    manifest = bundle.get("manifest")
    require(isinstance(manifest, dict), "BUNDLE_MANIFEST_MISSING")
    require(manifest.get("dataset_status") == "EXPORTED", "OFFICIAL_EXPORTED_DATASET_REQUIRED")
    require(
        manifest.get("dataset_split") == expected_split,
        f"OFFICIAL_SPLIT_MISMATCH: expected {expected_split}, got {manifest.get('dataset_split')}",
    )

    dataset_id = str(bundle.get("source_dataset_id") or manifest.get("dataset_id"))
    require(bool(dataset_id), "MISSING_OFFICIAL_DATASET_ID")

    algorithms = ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"]
    if config_pins is not None and model_configs is not None:
        configs = model_configs
        resolved_pins = config_pins
    else:
        configs, resolved_pins = load_algorithm_configs(project_root=project_root, custom_configs=model_configs)

    comparison_results: Dict[str, Dict[str, Any]] = {}
    for algo in algorithms:
        res = evaluate_model_on_bundle(
            bundle=bundle,
            algorithm_name=algo,
            config=configs.get(algo),
            repeats=repeats,
            sigma=sigma,
            seed=seed,
        )
        comparison_results[algo] = res
        if progress_callback is not None:
            progress_callback(algo, res)

    frozen_extension_rule = {
        "rule_version": "exp04-frozen-reproduction-rule-v2",
        "frozen_from_split": manifest.get("dataset_split"),
        "frozen_from_dataset_id": dataset_id,
        "frozen_from_collection_id": bundle.get("collection_id"),
        "gold_publication_id": manifest.get("gold_publication_id"),
        "gold_snapshot_id": str(manifest.get("gold_snapshot_id")) if manifest.get("gold_snapshot_id") is not None else None,
        "locked_feature_contract": {
            "window_model_version": manifest.get("window_model_version"),
            "scaling_config_version": manifest.get("scaling_config_version"),
            "feature_version": manifest.get("feature_version"),
            "mc_value": manifest.get("collection_plan", {}).get("global_mc", {}).get("mc_value"),
        },
        "locked_algorithm_configs": {
            algo: {
                "config_path": resolved_pins[algo]["config_path"],
                "model_config_version": resolved_pins[algo]["model_config_version"],
                "algorithm_version": resolved_pins[algo]["algorithm_version"],
                "file_bytes_sha256": resolved_pins[algo]["file_bytes_sha256"],
                "canonical_config_sha256": resolved_pins[algo]["canonical_config_sha256"],
            }
            for algo in algorithms
        },
        "extension_guardrails": {
            "no_parameter_override_on_extension": True,
            "no_accuracy_or_f1_metric": True,
            "preserve_failed_windows_in_denominator": True,
            "preserve_honest_nulls_with_reason_codes": True,
            "retrospective_candidate_only": True,
            "scientific_approval": False,
        },
    }

    return {
        "dataset_id": dataset_id,
        "dataset_info": {
            "dataset_id": dataset_id,
            "dataset_split": manifest.get("dataset_split"),
            "dataset_status": manifest.get("dataset_status"),
            "collection_id": bundle.get("collection_id"),
            "shard_count": len(bundle.get("shards", {})) if bundle.get("is_collection") else 1,
            "feature_row_count": bundle["features_table"].num_rows,
            "mainshock_count": bundle["mainshocks_table"].num_rows,
            "mc_value": manifest.get("collection_plan", {}).get("global_mc", {}).get("mc_value"),
            "window_model_version": manifest.get("window_model_version"),
            "scaling_config_version": manifest.get("scaling_config_version"),
            "feature_version": manifest.get("feature_version"),
            "manifest_sha256": bundle.get("manifest_sha256"),
            "checksums_sha256": bundle.get("checksums_sha256"),
            "gold_publication_id": manifest.get("gold_publication_id"),
            "gold_snapshot_id": str(manifest.get("gold_snapshot_id")) if manifest.get("gold_snapshot_id") is not None else None,
            "timestamp_reader_profile": bundle.get("timestamp_reader_profile"),
        },
        "evaluated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "comparison_table": _build_comparison_table(algorithms, comparison_results),
        "algorithm_details": comparison_results,
        "algorithm_config_pins": resolved_pins,
        "frozen_extension_rule": frozen_extension_rule,
    }
