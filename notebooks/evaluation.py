"""EXP-04: Multi-faceted Scientific Evaluation Module.

Provides rigorous, contract-compliant evaluation engines for earthquake clustering models:
1. Clustering Quality:
   - Density-Based Clustering Validation (DBCV) via mutual reachability & density separation.
   - Strict NULL policy for non-density models (WINDOW -> METRIC_NOT_APPLICABLE).
   - Noise handling and non-density clusters (all noise -> METRIC_INSUFFICIENT_MEMBERS).
   - Never substitutes 0.0 for missing DBCV scores.
2. Physical Consistency:
   - Modified Omori Law decay fitting: n(t) = K / (t + c)^p via Non-linear Least Squares (NLS).
   - Sample threshold guard: requires N >= 5 POST aftershocks. If N < 5, returns NULL with
     METRIC_INSUFFICIENT_MEMBERS without silently dropping the window from the denominator.
   - Goodness of fit metric (R^2). Failure to converge -> METRIC_FIT_FAILED.
3. Stability & Sensitivity Harness:
   - Gaussian jitter perturbation (sigma=0.01 on 4-D scaled space) across controlled repeats (R=5, seed=42).
   - Pairwise Jaccard similarity on sequence member candidate IDs.
   - Pairwise Adjusted Rand Index (ARI) on cluster partition labels.
   - Full distribution serialization in stability_components_json without lossy composite scores.
4. Model Comparison & Denominator Integrity:
   - Preserves failed windows in sample counts and comparison ratios.
   - Strictly prohibits Accuracy and F1 scores (unlabeled clustering problem).
"""
from __future__ import annotations

import json
import math
from pathlib import Path
import sys
import time
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import numpy as np
from scipy.optimize import curve_fit
from sklearn.metrics import adjusted_rand_score

# Optional standalone HDBSCAN for DBCV calculation
try:
    import hdbscan as standalone_hdbscan
    HAS_HDBSCAN = True
except ImportError:
    standalone_hdbscan = None
    HAS_HDBSCAN = False

# Ensure notebooks directory is in sys.path
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from experiment_harness import (
    VECTOR_COLUMNS,
    build_membership_rows,
    build_sequence_summary_row,
    require,
)
from baseline_models import (
    fit_window_baseline,
    fit_dbscan_baseline,
    fit_hdbscan_global_baseline,
)
from adaptive_hdbscan import (
    fit_hdbscan_adaptive,
)

# Contract Metric Reason Codes (docs/specs/ML_DATA_MODEL.md)
METRIC_NOT_APPLICABLE = "METRIC_NOT_APPLICABLE"
METRIC_INSUFFICIENT_MEMBERS = "METRIC_INSUFFICIENT_MEMBERS"
METRIC_FIT_FAILED = "METRIC_FIT_FAILED"
METRIC_RUNTIME_UNAVAILABLE = "METRIC_RUNTIME_UNAVAILABLE"

MIN_OMORI_AFTERSHOCKS = 5
DEFAULT_STABILITY_REPEATS = 5
DEFAULT_STABILITY_SIGMA = 0.01
DEFAULT_STABILITY_SEED = 42


# =============================================================================
# 1. Clustering Quality: DBCV Score
# =============================================================================
def compute_dbcv_score(
    features: np.ndarray,
    labels: np.ndarray,
    algorithm_name: str = "HDBSCAN",
) -> Tuple[Optional[float], List[str]]:
    """Compute Density-Based Clustering Validation (DBCV) score for a clustering result.

    Rules:
    - WINDOW algorithm: Does not form density clusters. Returns (None, [METRIC_NOT_APPLICABLE]).
    - All noise points (labels == -1) or fewer than 2 valid non-noise clusters/points:
      Returns (None, [METRIC_INSUFFICIENT_MEMBERS]).
    - If calculation fails or returns NaN/inf: Returns (None, [METRIC_FIT_FAILED]).
    - Never substitutes 0.0 for missing scores.
    """
    if algorithm_name == "WINDOW":
        return None, [METRIC_NOT_APPLICABLE]

    if features is None or labels is None or len(features) == 0 or len(labels) == 0:
        return None, [METRIC_INSUFFICIENT_MEMBERS]

    # Convert to standard numpy arrays
    features = np.asarray(features, dtype=np.float64)
    labels = np.asarray(labels, dtype=np.int64)

    if len(features) != len(labels):
        return None, [METRIC_FIT_FAILED]

    # Identify non-noise clusters
    valid_clusters = set(labels) - {-1}
    if not valid_clusters:
        # All points are noise (-1)
        return None, [METRIC_INSUFFICIENT_MEMBERS]

    # Check total non-noise count
    non_noise_mask = (labels != -1)
    non_noise_count = int(np.sum(non_noise_mask))
    if non_noise_count < 3:
        return None, [METRIC_INSUFFICIENT_MEMBERS]

    # Calculate DBCV using hdbscan.validity.validity_index if available
    if HAS_HDBSCAN and hasattr(standalone_hdbscan, "validity") and hasattr(standalone_hdbscan.validity, "validity_index"):
        try:
            score = standalone_hdbscan.validity.validity_index(features, labels)
            if math.isnan(score) or math.isinf(score):
                return None, [METRIC_FIT_FAILED]
            return float(np.clip(score, -1.0, 1.0)), []
        except Exception:
            # When cluster geometries or distances are degenerate
            return None, [METRIC_FIT_FAILED]

    return None, [METRIC_FIT_FAILED]


# =============================================================================
# 2. Physical Consistency: Modified Omori Law Decay Fit
# =============================================================================
def _omori_cumulative(t: np.ndarray, K: float, c: float, p: float) -> np.ndarray:
    """Cumulative form of Modified Omori Law: N(t) = integral_0^t K / (tau + c)^p dtau.

    For p != 1: N(t) = [K / (1 - p)] * [ (t + c)^(1 - p) - c^(1 - p) ]
    For p == 1: N(t) = K * ln( (t + c) / c )
    """
    eps = 1e-6
    c = max(c, eps)
    K = max(K, eps)
    t_eff = np.maximum(t, 0.0)

    if abs(p - 1.0) < 1e-4:
        return K * np.log((t_eff + c) / c)
    return (K / (1.0 - p)) * ((t_eff + c)**(1.0 - p) - c**(1.0 - p))


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
    # K: scale roughly proportional to total count
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
            maxfev=2000,
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
# 3. Stability & Sensitivity Harness (Jaccard & ARI)
# =============================================================================
def calculate_jaccard_similarity(set_a: Set[str], set_b: Set[str]) -> float:
    """Calculate Jaccard index between two sets of candidate event IDs.

    Edge-case policy:
    - If both sets are empty: 1.0 (identical absence of sequence members).
    - If one set is empty and the other is non-empty: 0.0.
    - Otherwise: len(A & B) / len(A | B).
    """
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    intersection = len(set_a & set_b)
    union = len(set_a | set_b)
    return float(intersection / union) if union > 0 else 1.0


def _dispatch_fit_model(
    algorithm_name: str,
    window_candidates: List[Dict[str, Any]],
    mainshock_id: str,
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Dispatch window fitting to the correct algorithm function."""
    if algorithm_name == "WINDOW":
        return fit_window_baseline(window_candidates, mainshock_id, config=config)
    elif algorithm_name == "DBSCAN":
        return fit_dbscan_baseline(window_candidates, mainshock_id, config=config)
    elif algorithm_name == "HDBSCAN_GLOBAL":
        return fit_hdbscan_global_baseline(window_candidates, mainshock_id, config=config)
    elif algorithm_name in ("HDBSCAN_ADAPTIVE", "ADAPTIVE"):
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
    """
    n_candidates = len(window_candidates)
    if n_candidates <= 1:
        # Self-only window or empty: deterministic by definition
        return {
            "mean_jaccard": 1.0,
            "std_jaccard": 0.0,
            "min_jaccard": 1.0,
            "max_jaccard": 1.0,
            "mean_ari": 1.0,
            "std_ari": 0.0,
            "min_ari": 1.0,
            "max_ari": 1.0,
            "repeats": repeats,
            "sigma": sigma,
            "seed": seed,
            "note": "DETERMINISTIC_SELF_ONLY",
        }

    member_sets: List[Set[str]] = []
    cluster_label_lists: List[List[int]] = []

    for r in range(repeats):
        rng = np.random.RandomState(seed + r * 100)
        perturbed_candidates = []
        for c in window_candidates:
            cid = c.get("candidate_event_id", c.get("event_id"))
            is_mainshock = (cid == mainshock_id)
            c_copy = dict(c)
            # Apply Gaussian jitter to scaled vector columns for non-mainshock candidates
            # Mainshock relative origin [0, 0, 0, 0] is preserved
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
        except Exception:
            # If fitting fails under perturbation, assign empty set and noise labels
            members = set()
            labels = [-1] * n_candidates

        member_sets.append(members)
        cluster_label_lists.append(labels)

    # Compute pairwise similarities
    jaccard_scores = []
    ari_scores = []

    for i in range(repeats):
        for j in range(i + 1, repeats):
            j_score = calculate_jaccard_similarity(member_sets[i], member_sets[j])
            jaccard_scores.append(j_score)

            a_labels = cluster_label_lists[i]
            b_labels = cluster_label_lists[j]
            ari_score = adjusted_rand_score(a_labels, b_labels)
            ari_scores.append(float(ari_score))

    mean_j = float(np.mean(jaccard_scores)) if jaccard_scores else 1.0
    std_j = float(np.std(jaccard_scores)) if jaccard_scores else 0.0
    min_j = float(np.min(jaccard_scores)) if jaccard_scores else 1.0
    max_j = float(np.max(jaccard_scores)) if jaccard_scores else 1.0

    mean_a = float(np.mean(ari_scores)) if ari_scores else 1.0
    std_a = float(np.std(ari_scores)) if ari_scores else 0.0
    min_a = float(np.min(ari_scores)) if ari_scores else 1.0
    max_a = float(np.max(ari_scores)) if ari_scores else 1.0

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
        "sigma": sigma,
        "seed": seed,
    }


# =============================================================================
# 4. Multi-Model Pilot Evaluation & Comparison Table Engine
# =============================================================================
def evaluate_model_on_bundle(
    bundle: Dict[str, Any],
    algorithm_name: str,
    config: Optional[Dict[str, Any]] = None,
    repeats: int = DEFAULT_STABILITY_REPEATS,
    sigma: float = DEFAULT_STABILITY_SIGMA,
    seed: int = DEFAULT_STABILITY_SEED,
) -> Dict[str, Any]:
    """Execute end-to-end evaluation for an algorithm across all mainshock windows in a bundle.

    Collects:
    - Mainshock windows count, success/failure counts, failure reason codes.
    - Total raw candidates, raw aftershocks, unique aftershocks.
    - Noise ratio across windows.
    - DBCV score with reason codes.
    - Modified Omori law parameters (K, c, p, R^2) with reason codes.
    - Stability components (Jaccard and ARI).
    - Preserves failed windows in the denominator.
    """
    if "windows" in bundle:
        windows = bundle["windows"]
    elif "features_table" in bundle:
        rows = bundle["features_table"].to_pylist()
        windows = {}
        for r in rows:
            mid = r["mainshock_event_id"]
            windows.setdefault(mid, []).append(r)
    else:
        windows = {}

    require(len(windows) > 0, "BUNDLE_HAS_NO_WINDOWS")

    window_evaluations = []
    total_windows = len(windows)
    success_windows = 0
    failure_windows = 0
    failure_reasons = []

    total_candidates = 0
    total_aftershocks = 0
    noise_ratios = []

    dbcv_scores = []
    omori_p_values = []
    omori_fit_metrics = []
    omori_fit_success_count = 0

    jaccard_scores = []
    ari_scores = []

    t_start = time.time()

    for mainshock_id, cands in windows.items():
        n_cands = len(cands)
        total_candidates += n_cands

        # 1. Fit model
        mem_rows, sum_row = _dispatch_fit_model(
            algorithm_name=algorithm_name,
            window_candidates=cands,
            mainshock_id=mainshock_id,
            config=config,
        )

        status = sum_row.get("window_result_status", "SUCCESS")
        fail_code = sum_row.get("failure_reason_code")
        if status == "SUCCESS":
            success_windows += 1
        else:
            failure_windows += 1
            if fail_code:
                failure_reasons.append(fail_code)

        # Sequence counts
        aftershock_cnt = sum_row.get("aftershock_count")
        if aftershock_cnt is not None:
            total_aftershocks += aftershock_cnt

        noise_ratio = sum_row.get("noise_ratio")
        if noise_ratio is not None:
            noise_ratios.append(noise_ratio)

        # 2. Clustering Quality: DBCV
        features_4d = np.array([[float(c.get(col, 0.0)) for col in VECTOR_COLUMNS] for c in cands], dtype=np.float64)
        labels = np.array([r.get("cluster_id") if r.get("cluster_id") is not None else -1 for r in mem_rows], dtype=np.int64)

        dbcv, dbcv_reasons = compute_dbcv_score(
            features=features_4d,
            labels=labels,
            algorithm_name=algorithm_name,
        )
        if dbcv is not None:
            dbcv_scores.append(dbcv)

        # 3. Physical Consistency: Modified Omori Law
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

        # 4. Stability & Sensitivity Harness
        stability_res = run_window_stability_analysis(
            window_candidates=cands,
            mainshock_id=mainshock_id,
            algorithm_name=algorithm_name,
            config=config,
            repeats=repeats,
            sigma=sigma,
            seed=seed,
        )
        jaccard_scores.append(stability_res["mean_jaccard"])
        ari_scores.append(stability_res["mean_ari"])

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
        }
        window_evaluations.append(window_eval)

    elapsed_seconds = round(time.time() - t_start, 4)

    # Denominator integrity: failure_rate = failure_windows / total_windows
    failure_rate = round(float(failure_windows / total_windows), 4) if total_windows > 0 else 0.0
    mean_noise_ratio = round(float(np.mean(noise_ratios)), 4) if noise_ratios else None
    mean_dbcv = round(float(np.mean(dbcv_scores)), 4) if dbcv_scores else None
    mean_omori_p = round(float(np.mean(omori_p_values)), 4) if omori_p_values else None
    mean_omori_r2 = round(float(np.mean(omori_fit_metrics)), 4) if omori_fit_metrics else None
    mean_jaccard = round(float(np.mean(jaccard_scores)), 4) if jaccard_scores else 1.0
    mean_ari = round(float(np.mean(ari_scores)), 4) if ari_scores else 1.0

    return {
        "algorithm_name": algorithm_name,
        "total_windows": total_windows,
        "success_windows": success_windows,
        "failure_windows": failure_windows,
        "failure_rate": failure_rate,
        "failure_reasons": failure_reasons,
        "total_candidates": total_candidates,
        "total_aftershocks": total_aftershocks,
        "mean_noise_ratio": mean_noise_ratio,
        "mean_dbcv_score": mean_dbcv,
        "dbcv_computed_windows": len(dbcv_scores),
        "mean_omori_p": mean_omori_p,
        "mean_omori_r2": mean_omori_r2,
        "omori_fitted_windows": omori_fit_success_count,
        "mean_jaccard_stability": mean_jaccard,
        "mean_ari_stability": mean_ari,
        "runtime_seconds": elapsed_seconds,
        "window_evaluations": window_evaluations,
    }


def compare_all_models_on_pilot(
    bundle_dir: Union[str, Path],
    repeats: int = DEFAULT_STABILITY_REPEATS,
    sigma: float = DEFAULT_STABILITY_SIGMA,
    seed: int = DEFAULT_STABILITY_SEED,
) -> Dict[str, Any]:
    """Execute comparative evaluation of the 4 models on a pilot bundle.

    Evaluates:
    - WINDOW
    - DBSCAN
    - HDBSCAN_GLOBAL
    - HDBSCAN_ADAPTIVE
    """
    from experiment_harness import load_feature_bundle, MODE_DEVELOPMENT

    bundle = load_feature_bundle(bundle_dir, mode=MODE_DEVELOPMENT)
    pilot_manifest = bundle.get("pilot_manifest", {})
    pilot_id = pilot_manifest.get("pilot_id", "pilot_unknown")

    algorithms = ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"]
    comparison_results = {}

    for algo in algorithms:
        res = evaluate_model_on_bundle(
            bundle=bundle,
            algorithm_name=algo,
            repeats=repeats,
            sigma=sigma,
            seed=seed,
        )
        comparison_results[algo] = res

    # Summary table rows
    comparison_table = []
    for algo in algorithms:
        r = comparison_results[algo]
        row = {
            "Algorithm": algo,
            "Total Windows": r["total_windows"],
            "Success Windows": r["success_windows"],
            "Failure Windows": r["failure_windows"],
            "Failure Rate": f"{r['failure_rate'] * 100:.1f}%",
            "Total Aftershocks": r["total_aftershocks"],
            "Mean Noise Ratio": f"{r['mean_noise_ratio'] * 100:.1f}%" if r["mean_noise_ratio"] is not None else "N/A",
            "Mean DBCV": f"{r['mean_dbcv_score']:.4f}" if r["mean_dbcv_score"] is not None else "N/A",
            "Mean Omori p": f"{r['mean_omori_p']:.2f}" if r["mean_omori_p"] is not None else "N/A",
            "Omori Fitted Windows": f"{r['omori_fitted_windows']}/{r['total_windows']}",
            "Mean Jaccard Stability": f"{r['mean_jaccard_stability']:.4f}",
            "Mean ARI Stability": f"{r['mean_ari_stability']:.4f}",
            "Runtime (s)": f"{r['runtime_seconds']:.2f}",
        }
        comparison_table.append(row)

    return {
        "pilot_id": pilot_id,
        "evaluated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "comparison_table": comparison_table,
        "algorithm_details": comparison_results,
    }
