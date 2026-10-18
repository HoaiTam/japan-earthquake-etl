"""EXP-03: HDBSCAN Adaptive model and Multi-Sequence Entity Resolution.

Strictly adheres to:
- Deterministic per-window parameter scaling from candidate count and mainshock magnitude
  without per-sequence manual overrides.
- Multi-sequence hierarchy tie-breaking: probability -> normalized 4-D distance -> ambiguous.
- True ambiguous unresolvable ties assigned status AMBIGUOUS (rank None, no arbitrary picking).
- Preservation of raw membership grain: every (experiment_run_id, mainshock_event_id, candidate_event_id)
  is retained.
- Nested mainshock candidate detection and parent linkage attached to sequence_summary.
- Complete before/after reconciliation count for deduplication auditing.
- Conformance to result-bundle-v1 and JUPYTER_PILOT_WORKFLOW.md.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import functools
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

import numpy as np

# Import HDBSCAN: use sklearn.cluster if available, else hdbscan
try:
    from sklearn.cluster import HDBSCAN as SklearnHDBSCAN
    HAS_SKLEARN_HDBSCAN = True
except ImportError:
    SklearnHDBSCAN = None
    HAS_SKLEARN_HDBSCAN = False

try:
    import hdbscan as standalone_hdbscan
    HAS_STANDALONE_HDBSCAN = True
except ImportError:
    standalone_hdbscan = None
    HAS_STANDALONE_HDBSCAN = False

# Ensure notebooks directory is in sys.path
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from experiment_harness import (
    VECTOR_COLUMNS,
    build_sequence_summary_row,
    write_experiment_result,
    canonical,
    get_runtime_metadata,
    require,
    sha256_bytes,
    MODE_DEVELOPMENT,
    MODE_OFFICIAL,
)


def euclidean_distance_4d(vec: List[float]) -> float:
    """Euclidean norm of a 4-D normalized vector [x_scaled, y_scaled, z_scaled, time_scaled]."""
    return math.sqrt(sum(v * v for v in vec))


# -----------------------------------------------------------------------------
# 1. Deterministic Adaptive Parameter Resolver
# -----------------------------------------------------------------------------
def resolve_adaptive_hdbscan_params(
    candidate_count: int,
    mainshock_magnitude: float,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, int]:
    """Deterministically compute min_cluster_size and min_samples for a window.

    Pure mathematical function:
    - Independent of sequence identity (no manual override per event).
    - Logarithmic candidate scaling + magnitude excess scaling.
    - min_cluster_size = clip(floor(base + k_N * ln(N + 1) + k_M * max(0, M - M0)), min_cl, max_cl)
    - min_samples = max(min_s, min(min_cluster_size, floor(ratio * min_cluster_size)))
    """
    require(candidate_count >= 1, f"INVALID_CANDIDATE_COUNT:{candidate_count}")
    cfg = config or {}
    policy = cfg.get("adaptive_policy", {})

    base_cl = float(policy.get("min_cluster_size_base", 3))
    min_cl = int(policy.get("min_cluster_size_min", 3))
    max_cl = int(policy.get("min_cluster_size_max", 25))
    k_n = float(policy.get("candidate_count_factor", 1.5))
    k_m = float(policy.get("magnitude_factor", 1.0))
    m0 = float(policy.get("magnitude_threshold", 5.5))

    ratio_s = float(policy.get("min_samples_ratio", 0.6))
    min_s = int(policy.get("min_samples_min", 2))
    max_s = int(policy.get("min_samples_max", 15))

    mag = float(mainshock_magnitude)
    mag_excess = max(0.0, mag - m0)

    # Scaling formula
    raw_cl = base_cl + k_n * math.log(candidate_count + 1.0) + k_m * mag_excess
    cl_size = max(min_cl, min(max_cl, int(math.floor(raw_cl))))

    raw_s = math.floor(ratio_s * cl_size)
    samples = max(min_s, min(max_s, min(cl_size, int(raw_s))))

    return {
        "min_cluster_size": cl_size,
        "min_samples": samples,
    }


# -----------------------------------------------------------------------------
# 2. Window Fitting for HDBSCAN Adaptive
# -----------------------------------------------------------------------------
def fit_hdbscan_adaptive(
    window_candidates: List[Dict[str, Any]],
    mainshock_id: str,
    config: Optional[Dict[str, Any]] = None,
    run_id: str = "run_adaptive",
    dataset_id: str = "ds_unknown",
    model_config_version: str = "hdbscan-adaptive-dev-v1",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Fit HDBSCAN with adaptive parameters on 4-D normalized relative vector per window.

    - Resolves min_cluster_size and min_samples deterministically from window properties.
    - Fits HDBSCAN on relative space-time 4-D features.
    - Identifies mainshock cluster: if mainshock is noise (-1), window is FAILED with
      EXP_MAINSHOCK_CLASSIFIED_AS_NOISE.
    - Returns initial raw memberships and preliminary sequence_summary.
    """
    t0 = time.time()
    n = len(window_candidates)
    require(n > 0, "EMPTY_WINDOW_CANDIDATES")

    cfg = config or {}
    metric = str(cfg.get("metric", "euclidean"))
    cluster_selection_epsilon = float(cfg.get("cluster_selection_epsilon", 0.0))
    allow_single_cluster = bool(cfg.get("allow_single_cluster", True))

    # Locate mainshock candidate
    mainshock_idx = None
    mainshock_candidate = None
    for idx, c in enumerate(window_candidates):
        cid = c.get("candidate_event_id", c.get("event_id"))
        if cid == mainshock_id:
            mainshock_idx = idx
            mainshock_candidate = c
            break
    require(mainshock_idx is not None, f"MAINSHOCK_NOT_IN_CANDIDATES:{mainshock_id}")

    # Read mainshock magnitude from contract fields: mainshock_magnitude, candidate_magnitude, or magnitude
    raw_mag = mainshock_candidate.get("mainshock_magnitude")
    if raw_mag is None:
        raw_mag = mainshock_candidate.get("candidate_magnitude")
    if raw_mag is None:
        for c in window_candidates:
            if c.get("mainshock_magnitude") is not None:
                raw_mag = c.get("mainshock_magnitude")
                break
    if raw_mag is None:
        raw_mag = mainshock_candidate.get("magnitude")

    require(raw_mag is not None, f"MISSING_MAINSHOCK_MAGNITUDE:{mainshock_id}")
    try:
        mainshock_mag = float(raw_mag)
        require(not math.isnan(mainshock_mag) and not math.isinf(mainshock_mag), f"INVALID_MAINSHOCK_MAGNITUDE:{raw_mag}")
    except (ValueError, TypeError):
        raise ValueError(f"INVALID_MAINSHOCK_MAGNITUDE:{raw_mag}")

    # Resolve adaptive parameters
    params = resolve_adaptive_hdbscan_params(
        candidate_count=n,
        mainshock_magnitude=mainshock_mag,
        config=cfg,
    )
    min_cluster_size = params["min_cluster_size"]
    min_samples = params["min_samples"]

    # Extract 4-D feature vectors
    X = np.array([[float(c.get(col, 0.0)) for col in VECTOR_COLUMNS] for c in window_candidates], dtype=np.float64)

    # Fit HDBSCAN
    if n < min_cluster_size:
        labels = [-1] * n
        probabilities = [0.0] * n
    else:
        if HAS_SKLEARN_HDBSCAN:
            hdb = SklearnHDBSCAN(
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                cluster_selection_epsilon=cluster_selection_epsilon,
                allow_single_cluster=allow_single_cluster,
            )
            hdb.fit(X)
            labels = [int(lbl) for lbl in hdb.labels_]
            probabilities = [float(p) for p in hdb.probabilities_]
        elif HAS_STANDALONE_HDBSCAN:
            hdb = standalone_hdbscan.HDBSCAN(
                min_cluster_size=min_cluster_size,
                min_samples=min_samples,
                cluster_selection_epsilon=cluster_selection_epsilon,
                allow_single_cluster=allow_single_cluster,
            )
            hdb.fit(X)
            labels = [int(lbl) for lbl in hdb.labels_]
            probabilities = [float(p) for p in hdb.probabilities_]
        else:
            raise RuntimeError("No HDBSCAN implementation available (sklearn.cluster.HDBSCAN or hdbscan)")

    mainshock_label = labels[mainshock_idx]
    mainshock_is_noise = (mainshock_label == -1)

    created_at = datetime.now(timezone.utc)
    mem_rows = []

    for idx, c in enumerate(window_candidates):
        cid = c.get("candidate_event_id", c.get("event_id"))
        is_self = (cid == mainshock_id)
        delta_t = float(c.get("delta_time_hours", 0.0))
        role = "MAINSHOCK" if is_self else ("PRE" if delta_t < 0 else "POST")

        label = labels[idx]
        is_noise = (label == -1)
        prob = max(0.0, min(1.0, float(probabilities[idx])))

        norm_dist = euclidean_distance_4d(X[idx].tolist())

        if mainshock_is_noise:
            is_main_member = False
            is_seq_member = False
        else:
            is_main_member = (label >= 0 and label == mainshock_label)
            is_seq_member = is_main_member

        row = {
            "schema_version": "1.0",
            "experiment_run_id": run_id,
            "dataset_id": dataset_id,
            "mainshock_event_id": mainshock_id,
            "candidate_event_id": cid,
            "algorithm_name": "HDBSCAN_ADAPTIVE",
            "model_config_version": model_config_version,
            "cluster_id": int(label),
            "is_noise": is_noise,
            "is_mainshock_cluster_member": is_main_member,
            "is_sequence_member": is_seq_member,
            "membership_probability": prob,
            "event_role_candidate": role,
            "normalized_distance": max(0.0, norm_dist),
            # Default single-window status, to be updated during multi-sequence resolution
            "multi_sequence_status": "UNIQUE" if is_seq_member else "NOT_MEMBER",
            "resolution_rank": 1 if is_seq_member else None,
            "created_at_utc": created_at,
        }
        mem_rows.append(row)

    elapsed = time.time() - t0

    if mainshock_is_noise:
        summary_row = build_sequence_summary_row(
            experiment_run_id=run_id,
            dataset_id=dataset_id,
            mainshock_event_id=mainshock_id,
            candidate_count=n,
            algorithm_name="HDBSCAN_ADAPTIVE",
            model_config_version=model_config_version,
            membership_rows=mem_rows,
            window_result_status="FAILED",
            failure_reason_code="EXP_MAINSHOCK_CLASSIFIED_AS_NOISE",
            runtime_seconds=elapsed,
            created_at_utc=created_at,
        )
    else:
        summary_row = build_sequence_summary_row(
            experiment_run_id=run_id,
            dataset_id=dataset_id,
            mainshock_event_id=mainshock_id,
            candidate_count=n,
            algorithm_name="HDBSCAN_ADAPTIVE",
            model_config_version=model_config_version,
            membership_rows=mem_rows,
            window_result_status="SUCCESS",
            runtime_seconds=elapsed,
            created_at_utc=created_at,
        )

    return mem_rows, summary_row


# -----------------------------------------------------------------------------
# 3. Multi-Sequence Entity Resolution & Nested Mainshock Detection
# -----------------------------------------------------------------------------
def find_best_membership_row(
    rows: List[Dict[str, Any]],
    eps_tie: float = 1e-6,
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """Find the best membership row in a candidate set using global, order-invariant resolution.

    Mathematical guarantee:
    - Never uses pairwise comparator sorting that could break transitivity.
    - Operates globally on the candidate set:
      1. Determines maximum probability p_max across all rows.
      2. Groups candidates within eps_tie of p_max:
         top_prob = [r for r in rows if p_max - p(r) <= eps_tie]
      3. Within top_prob, determines minimum normalized distance d_min.
      4. Groups candidates within eps_tie of d_min:
         best_candidates = [r for r in top_prob if d(r) - d_min <= eps_tie]
      5. If len(best_candidates) == 1:
           returns (best_candidates[0], False)  # Unique winner found
         Else:
           returns (None, True)  # Unresolvable tie (>1 tied winners)
    """
    if not rows:
        return None, False
    if len(rows) == 1:
        return rows[0], False

    p_max = max(0.0 if r.get("membership_probability") is None else float(r["membership_probability"]) for r in rows)
    top_prob = [
        r for r in rows
        if p_max - (0.0 if r.get("membership_probability") is None else float(r["membership_probability"])) <= eps_tie
    ]

    d_min = min(float(r.get("normalized_distance", 0.0)) for r in top_prob)
    best_candidates = [
        r for r in top_prob
        if float(r.get("normalized_distance", 0.0)) - d_min <= eps_tie
    ]

    if len(best_candidates) == 1:
        return best_candidates[0], False
    return None, True


def compare_membership_rows(
    r1: Dict[str, Any],
    r2: Dict[str, Any],
    eps_tie: float = 1e-6,
) -> int:
    """Compare two candidate membership rows under tolerance hierarchy.

    Hierarchy:
    1. Membership probability (higher is better):
       If |p1 - p2| > eps_tie, the one with strictly higher probability wins.
    2. Normalized distance (lower is better):
       If probabilities are within eps_tie (considered tied), the one with
       strictly smaller normalized_distance (|d1 - d2| > eps_tie) wins.
    3. Unresolvable tie:
       If both |p1 - p2| <= eps_tie and |d1 - d2| <= eps_tie, returns 0 (tie).

    Returns:
      -1 if r1 is strictly better than r2
      +1 if r2 is strictly better than r1
       0 if tied under both criteria within eps_tie
    """
    p1 = 0.0 if r1.get("membership_probability") is None else float(r1["membership_probability"])
    p2 = 0.0 if r2.get("membership_probability") is None else float(r2["membership_probability"])

    diff_p = p1 - p2
    if diff_p > eps_tie:
        return -1
    elif diff_p < -eps_tie:
        return 1

    d1 = float(r1.get("normalized_distance", 0.0))
    d2 = float(r2.get("normalized_distance", 0.0))

    diff_d = d1 - d2
    if diff_d < -eps_tie:
        return -1
    elif diff_d > eps_tie:
        return 1

    return 0


def resolve_multi_sequence_and_nested_mainshocks(
    all_memberships: List[Dict[str, Any]],
    all_summaries: List[Dict[str, Any]],
    eps_tie: float = 1e-6,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    """Perform deterministic entity resolution across all mainshock windows in a run.

    Resolution hierarchy:
    1. membership_probability (descending): higher probability wins.
    2. normalized_distance (ascending): smaller 4-D distance wins.
    3. Unresolvable tie: both probability and distance are equal within eps_tie.
       Assign multi_sequence_status = 'AMBIGUOUS', resolution_rank = None.
       No arbitrary picking!

    Constraints satisfied:
    - Every raw membership row is preserved (no dropped rows).
    - Non-members (is_sequence_member=False) have status NOT_MEMBER and rank None.
    - Single-membership events have status UNIQUE and rank 1.
    - Resolvable multi-sequence events have status MULTIPLE, with distinct ranks 1..K containing 1.
    - Nested mainshocks: Mainshocks occurring in another sequence are flagged in sequence_summary.
    - Order-invariant: global grouping within tolerance avoids non-transitivity cycles for >=3 candidates.
    - Returns updated memberships, summaries, and audit reconciliation metrics.
    """
    # Group membership rows by candidate_event_id
    candidate_to_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in all_memberships:
        cid = r["candidate_event_id"]
        candidate_to_rows[cid].append(r)

    # Track counts for reconciliation audit
    raw_sequence_members = 0
    raw_aftershocks = 0
    unique_candidates_multi_seq = 0
    resolved_unique_aftershocks = 0
    ambiguous_candidates_count = 0
    ambiguous_aftershocks_count = 0

    for cid, rows in candidate_to_rows.items():
        # Separate member vs non-member rows
        member_rows = [r for r in rows if r["is_sequence_member"]]
        non_member_rows = [r for r in rows if not r["is_sequence_member"]]

        # Update non-member rows
        for r in non_member_rows:
            r["multi_sequence_status"] = "NOT_MEMBER"
            r["resolution_rank"] = None

        if len(member_rows) == 0:
            continue

        raw_sequence_members += len(member_rows)
        raw_aftershocks += sum(1 for r in member_rows if r.get("event_role_candidate") == "POST")

        if len(member_rows) == 1:
            # Single sequence member
            r = member_rows[0]
            r["multi_sequence_status"] = "UNIQUE"
            r["resolution_rank"] = 1
            if r.get("event_role_candidate") == "POST":
                resolved_unique_aftershocks += 1
        else:
            # Multi-sequence candidate
            unique_candidates_multi_seq += 1

            # Global order-invariant winner resolution
            best_row, is_tie = find_best_membership_row(member_rows, eps_tie=eps_tie)

            if is_tie or best_row is None:
                # Unresolvable tie for top rank: cannot objectively declare a winner
                ambiguous_candidates_count += 1
                for r in member_rows:
                    r["multi_sequence_status"] = "AMBIGUOUS"
                    r["resolution_rank"] = None
                    if r.get("event_role_candidate") == "POST":
                        ambiguous_aftershocks_count += 1
            else:
                # Successfully resolved rank 1 via global hierarchy
                # Iteratively peel off remaining ranks; break remaining ties deterministically
                ordered_rows = []
                remaining = list(member_rows)
                while remaining:
                    best, tie = find_best_membership_row(remaining, eps_tie=eps_tie)
                    if tie or best is None:
                        # Lower ranks tie: sort deterministically by mainshock_event_id to guarantee order-invariance
                        tied_candidates = sorted(remaining, key=lambda r: str(r.get("mainshock_event_id", "")))
                        ordered_rows.extend(tied_candidates)
                        break
                    ordered_rows.append(best)
                    remaining = [r for r in remaining if r is not best]

                for rank_idx, r in enumerate(ordered_rows, start=1):
                    r["multi_sequence_status"] = "MULTIPLE"
                    r["resolution_rank"] = rank_idx
                    if rank_idx == 1 and r.get("event_role_candidate") == "POST":
                        resolved_unique_aftershocks += 1

    # -------------------------------------------------------------------------
    # Nested Mainshock Evidence Detection
    # -------------------------------------------------------------------------
    # Map mainshock_event_id to its sequence_summary row
    summary_by_mainshock = {s["mainshock_event_id"]: s for s in all_summaries}

    nested_count = 0
    for mid, summary in summary_by_mainshock.items():
        # Check if this mainshock mid appears as a sequence member in other mainshocks' windows
        other_memberships = [
            r for r in all_memberships
            if r["candidate_event_id"] == mid
            and r["mainshock_event_id"] != mid
            and r["is_sequence_member"]
        ]

        if other_memberships:
            # Check if one of the other memberships has resolution_rank == 1
            resolved_parent_row = next((r for r in other_memberships if r.get("resolution_rank") == 1), None)

            if resolved_parent_row is None:
                # No other membership has rank 1 (either mid was rank 1 in its own window,
                # or mid's memberships were marked AMBIGUOUS due to a tie).
                if len(other_memberships) == 1:
                    resolved_parent_row = other_memberships[0]
                else:
                    best_parent, is_parent_tie = find_best_membership_row(other_memberships, eps_tie=eps_tie)
                    if is_parent_tie:
                        # Tie between candidate parents: CANNOT assign an arbitrary parent!
                        resolved_parent_row = None
                    else:
                        resolved_parent_row = best_parent

            summary["is_nested_mainshock_candidate"] = True
            nested_count += 1

            if resolved_parent_row is not None:
                summary["parent_mainshock_event_id"] = resolved_parent_row["mainshock_event_id"]
                summary["nested_membership_probability"] = resolved_parent_row.get("membership_probability")
            else:
                summary["parent_mainshock_event_id"] = None
                summary["nested_membership_probability"] = None
                reasons = list(summary.get("metric_reason_codes") or [])
                if "AMBIGUOUS_NESTED_PARENT_TIE" not in reasons:
                    reasons.append("AMBIGUOUS_NESTED_PARENT_TIE")
                summary["metric_reason_codes"] = reasons
        else:
            summary["is_nested_mainshock_candidate"] = False
            summary["parent_mainshock_event_id"] = None
            summary["nested_membership_probability"] = None

    reconciliation = {
        "total_raw_memberships": len(all_memberships),
        "raw_sequence_members": raw_sequence_members,
        "raw_aftershocks": raw_aftershocks,
        "unique_candidates_in_multi_sequences": unique_candidates_multi_seq,
        "resolved_unique_aftershocks": resolved_unique_aftershocks,
        "ambiguous_candidates_count": ambiguous_candidates_count,
        "ambiguous_aftershocks_count": ambiguous_aftershocks_count,
        "aftershocks_deduplicated_count": raw_aftershocks - resolved_unique_aftershocks - ambiguous_aftershocks_count,
        "nested_mainshocks_count": nested_count,
    }

    return all_memberships, all_summaries, reconciliation


# -----------------------------------------------------------------------------
# 4. HDBSCAN Adaptive Experiment Runner
# -----------------------------------------------------------------------------
def run_adaptive_experiment(
    bundle: Dict[str, Any],
    config: Dict[str, Any],
    model_config_version: str,
    output_dir: Union[str, Path],
    mode: str = MODE_DEVELOPMENT,
    code_version: Optional[str] = None,
    random_seed: Optional[int] = None,
    experiment_run_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    is_retry: bool = False,
    allow_overwrite: bool = False,
    subfolder_by_run_id: bool = False,
) -> Dict[str, Any]:
    """Execute HDBSCAN Adaptive with full entity resolution across all windows in bundle."""
    algorithm_name = "HDBSCAN_ADAPTIVE"
    features_table = bundle["features_table"]
    rows = features_table.to_pylist()
    source_ds_id = bundle.get("source_dataset_id", bundle.get("bundle_id"))

    runtime_meta = get_runtime_metadata(code_version=code_version, random_seed=random_seed)
    code_ver = code_version or runtime_meta["code_version"]

    if experiment_run_id:
        effective_run_id = experiment_run_id
    else:
        exec_token = execution_id or uuid.uuid4().hex[:16]
        run_id_payload = {
            "dataset_id": source_ds_id,
            "algorithm_name": algorithm_name,
            "algorithm_version": "1.0.0",
            "model_config_version": model_config_version,
            "model_config": config,
            "code_version": code_ver,
            "mode": mode,
            "random_seed": random_seed,
            "execution_token": exec_token,
        }
        identity_digest = sha256_bytes(canonical(run_id_payload).encode("utf-8"))[:32]
        effective_run_id = f"pilot_run_{identity_digest}" if mode == MODE_DEVELOPMENT else f"exp_run_{identity_digest}"

    target_dir = (Path(output_dir) / effective_run_id) if subfolder_by_run_id else Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    # Group candidates by mainshock_event_id
    windows: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        mid = r["mainshock_event_id"]
        windows.setdefault(mid, []).append(r)

    all_memberships: List[Dict[str, Any]] = []
    all_summaries: List[Dict[str, Any]] = []

    t_start = time.time()
    for mid, candidates in windows.items():
        mem, sum_row = fit_hdbscan_adaptive(
            window_candidates=candidates,
            mainshock_id=mid,
            config=config,
            run_id=effective_run_id,
            dataset_id=source_ds_id,
            model_config_version=model_config_version,
        )
        all_memberships.extend(mem)
        all_summaries.append(sum_row)

    total_fit_runtime = time.time() - t_start

    # Apply global multi-sequence resolution and nested mainshock linking
    resolved_memberships, resolved_summaries, recon = resolve_multi_sequence_and_nested_mainshocks(
        all_memberships=all_memberships,
        all_summaries=all_summaries,
    )

    total_runtime = time.time() - t_start

    # Metrics computation
    total_candidates = len(resolved_memberships)
    member_count = recon["raw_sequence_members"]
    raw_aftershocks = recon["raw_aftershocks"]
    resolved_aftershocks = recon["resolved_unique_aftershocks"]
    noise_count = sum(1 for m in resolved_memberships if m.get("is_noise") is True)

    successful_windows = sum(1 for s in resolved_summaries if s["window_result_status"] == "SUCCESS")
    failed_windows = sum(1 for s in resolved_summaries if s["window_result_status"] == "FAILED")
    skipped_windows = sum(1 for s in resolved_summaries if s["window_result_status"] == "SKIPPED")

    metrics = {
        "algorithm_name": algorithm_name,
        "model_config_version": model_config_version,
        "total_windows": len(windows),
        "windows_processed": len(windows),
        "successful_windows": successful_windows,
        "failed_windows": failed_windows,
        "skipped_windows": skipped_windows,
        "total_candidates": total_candidates,
        "total_sequence_members": member_count,
        "raw_aftershocks": raw_aftershocks,
        "resolved_unique_aftershocks": resolved_aftershocks,
        "deduplicated_aftershocks": recon["aftershocks_deduplicated_count"],
        "ambiguous_candidates_count": recon["ambiguous_candidates_count"],
        "nested_mainshocks_count": recon["nested_mainshocks_count"],
        "total_noise": noise_count,
        "total_noise_candidates": noise_count,
        "runtime_seconds": round(total_runtime, 4),
        "reconciliation": recon,
    }

    # Write result bundle
    write_res = write_experiment_result(
        output_dir=target_dir,
        bundle=bundle,
        algorithm_name=algorithm_name,
        algorithm_version="1.0.0",
        model_config_version=model_config_version,
        model_config=config,
        memberships=resolved_memberships,
        summaries=resolved_summaries,
        metrics=metrics,
        mode=mode,
        code_version=code_ver,
        random_seed=random_seed,
        experiment_run_id=effective_run_id,
        execution_id=execution_id,
        is_retry=is_retry,
        allow_overwrite=allow_overwrite,
    )

    return {
        "algorithm_name": algorithm_name,
        "run_id": effective_run_id,
        "output_dir": target_dir,
        "metrics": metrics,
        "write_result": write_res,
        "summaries": resolved_summaries,
        "memberships": resolved_memberships,
        "reconciliation": recon,
    }
