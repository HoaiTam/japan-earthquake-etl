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
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union

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

    mainshock_mag = float(mainshock_candidate.get("magnitude", 5.5))

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

            # Sort member rows by hierarchy:
            # 1. -membership_probability (None treated as -1.0)
            # 2. normalized_distance
            def sort_key(row: Dict[str, Any]) -> Tuple[float, float]:
                p = row.get("membership_probability")
                prob_val = -1.0 if p is None else float(p)
                dist_val = float(row.get("normalized_distance", 0.0))
                return (-prob_val, dist_val)

            sorted_members = sorted(member_rows, key=sort_key)

            # Check for unresolvable tie between the top two candidates
            top1 = sorted_members[0]
            top2 = sorted_members[1]

            p1 = top1.get("membership_probability")
            p2 = top2.get("membership_probability")
            p1_val = 0.0 if p1 is None else float(p1)
            p2_val = 0.0 if p2 is None else float(p2)

            d1_val = float(top1.get("normalized_distance", 0.0))
            d2_val = float(top2.get("normalized_distance", 0.0))

            is_prob_tie = abs(p1_val - p2_val) < eps_tie
            is_dist_tie = abs(d1_val - d2_val) < eps_tie

            if is_prob_tie and is_dist_tie:
                # Unresolvable tie: cannot objectively declare a winner
                ambiguous_candidates_count += 1
                for r in member_rows:
                    r["multi_sequence_status"] = "AMBIGUOUS"
                    r["resolution_rank"] = None
                    if r.get("event_role_candidate") == "POST":
                        ambiguous_aftershocks_count += 1
            else:
                # Successfully resolved via probability or distance
                for rank_idx, r in enumerate(sorted_members, start=1):
                    r["multi_sequence_status"] = "MULTIPLE"
                    r["resolution_rank"] = rank_idx
                    if rank_idx == 1 and r.get("event_role_candidate") == "POST":
                        resolved_unique_aftershocks += 1

    # -------------------------------------------------------------------------
    # Nested Mainshock Evidence Detection
    # -------------------------------------------------------------------------
    # Map mainshock_event_id to its sequence_summary row
    summary_by_mainshock = {s["mainshock_event_id"]: s for s in all_summaries}
    all_mainshock_ids = set(summary_by_mainshock.keys())

    nested_count = 0
    for mid, summary in summary_by_mainshock.items():
        # Check if this mainshock mid appears as a candidate in other mainshocks' windows
        other_memberships = [
            r for r in all_memberships
            if r["candidate_event_id"] == mid
            and r["mainshock_event_id"] != mid
            and r["is_sequence_member"]
        ]

        if other_memberships:
            # Find the best parent window: rank 1 winner, or highest probability / lowest distance
            resolved_parent_row = next((r for r in other_memberships if r.get("resolution_rank") == 1), None)
            if resolved_parent_row is None:
                # If ambiguous or no rank 1, pick the one with max probability / min distance
                resolved_parent_row = max(
                    other_memberships,
                    key=lambda r: (r.get("membership_probability") or 0.0, -r.get("normalized_distance", 0.0))
                )

            summary["is_nested_mainshock_candidate"] = True
            summary["parent_mainshock_event_id"] = resolved_parent_row["mainshock_event_id"]
            summary["nested_membership_probability"] = resolved_parent_row.get("membership_probability")
            nested_count += 1
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
) -> Dict[str, Any]:
    """Execute HDBSCAN Adaptive with full entity resolution across all windows in bundle."""
    algorithm_name = "HDBSCAN_ADAPTIVE"
    features_table = bundle["features_table"]
    rows = features_table.to_pylist()
    source_ds_id = bundle.get("source_dataset_id", bundle.get("bundle_id"))

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
            run_id="temp_run",
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
        output_dir=output_dir,
        bundle=bundle,
        algorithm_name=algorithm_name,
        algorithm_version="1.0.0",
        model_config_version=model_config_version,
        model_config=config,
        memberships=resolved_memberships,
        summaries=resolved_summaries,
        metrics=metrics,
        mode=mode,
        code_version=code_version,
        random_seed=random_seed,
    )

    return {
        "algorithm_name": algorithm_name,
        "run_id": write_res["run_id"],
        "metrics": metrics,
        "write_result": write_res,
        "summaries": resolved_summaries,
        "memberships": resolved_memberships,
        "reconciliation": recon,
    }
