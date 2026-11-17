"""EXP-02: Implementations for Window, DBSCAN, and HDBSCAN Global baseline models.

Strictly adheres to:
- Per-mainshock window fitting on 4-D relative space-time vectors.
- Mainshock cluster isolation: only the cluster containing the mainshock is sequence.
- Mainshock classified as noise (-1): records EXP_MAINSHOCK_CLASSIFIED_AS_NOISE and 0 sequence members.
- Non-mutation of input bundle.
- Null probability policy: NULL for WINDOW and DBSCAN; finite [0, 1] for HDBSCAN.
- Conformance to result-bundle-v1 and JUPYTER_PILOT_WORKFLOW.md.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

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

from sklearn.cluster import DBSCAN

# Ensure notebooks directory is in sys.path for experiment_harness
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from experiment_harness import (
    VECTOR_COLUMNS,
    build_membership_rows,
    build_sequence_summary_row,
    write_experiment_result,
    canonical,
    sha256_bytes,
    get_runtime_metadata,
    read_json_safe,
    require,
    validate_safe_metadata,
    MODE_DEVELOPMENT,
    MODE_OFFICIAL,
)


def euclidean_distance_4d(vec: List[float]) -> float:
    """Euclidean norm of a 4-D normalized vector [x_scaled, y_scaled, z_scaled, time_scaled]."""
    return math.sqrt(sum(v * v for v in vec))


# -----------------------------------------------------------------------------
# 1. WINDOW Baseline
# -----------------------------------------------------------------------------
def fit_window_baseline(
    window_candidates: List[Dict[str, Any]],
    mainshock_id: str,
    config: Optional[Dict[str, Any]] = None,
    run_id: str = "run_window",
    dataset_id: str = "ds_unknown",
    model_config_version: str = "window-baseline-v1",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Standard space-time counting window baseline.

    - Every POST candidate in the window is considered a candidate aftershock.
    - PRE events are candidate foreshocks (not sequence aftershocks).
    - Mainshock is role MAINSHOCK, sequence member.
    - cluster_id, is_noise, is_mainshock_cluster_member, membership_probability are NULL.
    """
    t0 = time.time()
    n = len(window_candidates)
    require(n > 0, "EMPTY_WINDOW_CANDIDATES")

    mem_rows = []
    created_at = datetime.now(timezone.utc)

    for c in window_candidates:
        cid = c.get("candidate_event_id", c.get("event_id"))
        is_self = (cid == mainshock_id)
        delta_t = float(c.get("delta_time_hours", 0.0))
        role = "MAINSHOCK" if is_self else ("PRE" if delta_t < 0 else "POST")

        vec = [float(c.get(col, 0.0)) for col in VECTOR_COLUMNS]
        norm_dist = euclidean_distance_4d(vec)

        # In WINDOW baseline: POST candidates in window are candidate sequence members
        is_member = is_self or (role == "POST")

        row = {
            "schema_version": "1.0",
            "experiment_run_id": run_id,
            "dataset_id": dataset_id,
            "mainshock_event_id": mainshock_id,
            "candidate_event_id": cid,
            "algorithm_name": "WINDOW",
            "model_config_version": model_config_version,
            "cluster_id": None,
            "is_noise": None,
            "is_mainshock_cluster_member": None,
            "is_sequence_member": is_member,
            "membership_probability": None,
            "event_role_candidate": role,
            "normalized_distance": max(0.0, norm_dist),
            "multi_sequence_status": "UNIQUE" if is_member else "NOT_MEMBER",
            "resolution_rank": 1 if is_member else None,
            "created_at_utc": created_at,
        }
        mem_rows.append(row)

    elapsed = time.time() - t0
    summary_row = build_sequence_summary_row(
        experiment_run_id=run_id,
        dataset_id=dataset_id,
        mainshock_event_id=mainshock_id,
        candidate_count=n,
        algorithm_name="WINDOW",
        model_config_version=model_config_version,
        membership_rows=mem_rows,
        window_result_status="SUCCESS",
        runtime_seconds=elapsed,
        created_at_utc=created_at,
    )
    return mem_rows, summary_row


# -----------------------------------------------------------------------------
# 2. DBSCAN Baseline
# -----------------------------------------------------------------------------
def fit_dbscan_baseline(
    window_candidates: List[Dict[str, Any]],
    mainshock_id: str,
    config: Optional[Dict[str, Any]] = None,
    run_id: str = "run_dbscan",
    dataset_id: str = "ds_unknown",
    model_config_version: str = "dbscan-global-dev-v1",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """DBSCAN global baseline fitted on 4-D normalized relative vector per window.

    - Cluster containing mainshock is sequence; if mainshock is -1, sequence is empty
      and window failure EXP_MAINSHOCK_CLASSIFIED_AS_NOISE is recorded.
    - Membership probabilities are strictly NULL.
    """
    t0 = time.time()
    n = len(window_candidates)
    require(n > 0, "EMPTY_WINDOW_CANDIDATES")

    cfg = config or {}
    eps = float(cfg.get("eps", 0.5))
    min_samples = int(cfg.get("min_samples", 3))
    metric = str(cfg.get("metric", "euclidean"))

    # Extract 4-D feature matrix
    X = np.array([[float(c.get(col, 0.0)) for col in VECTOR_COLUMNS] for c in window_candidates], dtype=np.float64)

    # Find mainshock index
    mainshock_idx = None
    for idx, c in enumerate(window_candidates):
        cid = c.get("candidate_event_id", c.get("event_id"))
        if cid == mainshock_id:
            mainshock_idx = idx
            break
    require(mainshock_idx is not None, f"MAINSHOCK_NOT_IN_CANDIDATES:{mainshock_id}")

    # Fit DBSCAN
    db = DBSCAN(eps=eps, min_samples=min_samples, metric=metric)
    db.fit(X)
    labels = [int(lbl) for lbl in db.labels_]

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

        norm_dist = euclidean_distance_4d(X[idx].tolist())

        if mainshock_is_noise:
            # Mainshock classified as noise: NO candidate is member of mainshock cluster
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
            "algorithm_name": "DBSCAN",
            "model_config_version": model_config_version,
            "cluster_id": int(label),
            "is_noise": is_noise,
            "is_mainshock_cluster_member": is_main_member,
            "is_sequence_member": is_seq_member,
            "membership_probability": None,
            "event_role_candidate": role,
            "normalized_distance": max(0.0, norm_dist),
            "multi_sequence_status": "UNIQUE" if is_seq_member else "NOT_MEMBER",
            "resolution_rank": 1 if is_seq_member else None,
            "created_at_utc": created_at,
        }
        mem_rows.append(row)

    elapsed = time.time() - t0

    if mainshock_is_noise:
        # Per contract line 176: label==-1 require EXP_MAINSHOCK_CLASSIFIED_AS_NOISE
        summary_row = build_sequence_summary_row(
            experiment_run_id=run_id,
            dataset_id=dataset_id,
            mainshock_event_id=mainshock_id,
            candidate_count=n,
            algorithm_name="DBSCAN",
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
            algorithm_name="DBSCAN",
            model_config_version=model_config_version,
            membership_rows=mem_rows,
            window_result_status="SUCCESS",
            runtime_seconds=elapsed,
            created_at_utc=created_at,
        )

    return mem_rows, summary_row


# -----------------------------------------------------------------------------
# 3. HDBSCAN Global Baseline
# -----------------------------------------------------------------------------
def fit_hdbscan_global_baseline(
    window_candidates: List[Dict[str, Any]],
    mainshock_id: str,
    config: Optional[Dict[str, Any]] = None,
    run_id: str = "run_hdbscan_global",
    dataset_id: str = "ds_unknown",
    model_config_version: str = "hdbscan-global-dev-v1",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """HDBSCAN global baseline fitted on 4-D normalized relative vector per window.

    - Computes cluster assignments and membership probabilities in [0.0, 1.0].
    - Cluster containing mainshock is sequence; if mainshock is -1, sequence is empty
      and window failure EXP_MAINSHOCK_CLASSIFIED_AS_NOISE is recorded.
    """
    t0 = time.time()
    n = len(window_candidates)
    require(n > 0, "EMPTY_WINDOW_CANDIDATES")

    cfg = config or {}
    min_cluster_size = int(cfg.get("min_cluster_size", 3))
    min_samples = int(cfg.get("min_samples", 2))
    cluster_selection_epsilon = float(cfg.get("cluster_selection_epsilon", 0.0))
    allow_single_cluster = bool(cfg.get("allow_single_cluster", True))

    X = np.array([[float(c.get(col, 0.0)) for col in VECTOR_COLUMNS] for c in window_candidates], dtype=np.float64)

    mainshock_idx = None
    for idx, c in enumerate(window_candidates):
        cid = c.get("candidate_event_id", c.get("event_id"))
        if cid == mainshock_id:
            mainshock_idx = idx
            break
    require(mainshock_idx is not None, f"MAINSHOCK_NOT_IN_CANDIDATES:{mainshock_id}")

    # Initialize HDBSCAN model
    if n < min_cluster_size:
        # If fewer candidates than min_cluster_size, HDBSCAN naturally marks all as noise
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
            "algorithm_name": "HDBSCAN_GLOBAL",
            "model_config_version": model_config_version,
            "cluster_id": int(label),
            "is_noise": is_noise,
            "is_mainshock_cluster_member": is_main_member,
            "is_sequence_member": is_seq_member,
            "membership_probability": prob,
            "event_role_candidate": role,
            "normalized_distance": max(0.0, norm_dist),
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
            algorithm_name="HDBSCAN_GLOBAL",
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
            algorithm_name="HDBSCAN_GLOBAL",
            model_config_version=model_config_version,
            membership_rows=mem_rows,
            window_result_status="SUCCESS",
            runtime_seconds=elapsed,
            created_at_utc=created_at,
        )

    return mem_rows, summary_row


# -----------------------------------------------------------------------------
# 4. Multi-Algorithm Experiment Runner
# -----------------------------------------------------------------------------
def _annotate_multi_sequence_status(memberships: List[Dict[str, Any]]) -> None:
    """Annotate multi_sequence_status and resolution_rank in-place on membership rows."""
    seq_member_window_counts: Dict[str, int] = Counter()
    for m in memberships:
        if m["is_sequence_member"]:
            seq_member_window_counts[m["candidate_event_id"]] += 1

    for m in memberships:
        if m["is_sequence_member"]:
            if seq_member_window_counts[m["candidate_event_id"]] > 1:
                m["multi_sequence_status"] = "AMBIGUOUS"
                m["resolution_rank"] = None
            else:
                m["multi_sequence_status"] = "UNIQUE"
                m["resolution_rank"] = 1
        else:
            m["multi_sequence_status"] = "NOT_MEMBER"
            m["resolution_rank"] = None


def _compute_baseline_metrics(
    algorithm_name: str,
    model_config_version: str,
    memberships: List[Dict[str, Any]],
    summaries: List[Dict[str, Any]],
    total_runtime: float,
    total_windows: int,
) -> Dict[str, Any]:
    """Compute reconciled count metrics for a baseline run."""
    total_candidates = len(memberships)
    member_count = sum(1 for m in memberships if m["is_sequence_member"])
    aftershock_count = sum(
        1 for m in memberships if m["is_sequence_member"] and m["event_role_candidate"] == "POST"
    )
    noise_count = sum(1 for m in memberships if m.get("is_noise") is True)
    ambiguous_candidates = len(
        {m["candidate_event_id"] for m in memberships if m.get("multi_sequence_status") == "AMBIGUOUS"}
    )

    successful_windows = sum(1 for s in summaries if s["window_result_status"] == "SUCCESS")
    failed_windows = sum(1 for s in summaries if s["window_result_status"] == "FAILED")
    skipped_windows = sum(1 for s in summaries if s["window_result_status"] == "SKIPPED")

    return {
        "algorithm_name": algorithm_name,
        "model_config_version": model_config_version,
        "total_windows": total_windows,
        "windows_processed": total_windows,
        "successful_windows": successful_windows,
        "failed_windows": failed_windows,
        "skipped_windows": skipped_windows,
        "total_candidates": total_candidates,
        "total_sequence_members": member_count,
        "total_aftershocks": aftershock_count,
        "total_noise": noise_count,
        "total_noise_candidates": noise_count,
        "ambiguous_candidates_count": ambiguous_candidates,
        "runtime_seconds": round(total_runtime, 4),
    }


def _derive_run_id(
    *,
    source_ds_id: str,
    algorithm_name: str,
    model_config_version: str,
    config: Dict[str, Any],
    code_ver: str,
    mode: str,
    random_seed: Optional[int],
    execution_token: str,
    collection_scope: Optional[Dict[str, Any]] = None,
) -> str:
    """Build deterministic run ID matching experiment_harness.write_experiment_result."""
    run_id_payload: Dict[str, Any] = {
        "dataset_id": source_ds_id,
        "algorithm_name": algorithm_name,
        "algorithm_version": "1.0.0",
        "model_config_version": model_config_version,
        "model_config": config,
        "code_version": code_ver,
        "mode": mode,
        "random_seed": random_seed,
        "execution_token": execution_token,
    }
    if collection_scope is not None:
        run_id_payload["collection_scope"] = collection_scope
    identity_digest = sha256_bytes(canonical(run_id_payload).encode("utf-8"))[:32]
    return f"pilot_run_{identity_digest}" if mode == MODE_DEVELOPMENT else f"exp_run_{identity_digest}"


def run_baseline_experiment(
    bundle: Dict[str, Any],
    algorithm_name: str,
    config: Dict[str, Any],
    model_config_version: str,
    output_dir: Union[str, Path],
    mode: str = MODE_DEVELOPMENT,
    code_version: Optional[str] = None,
    random_seed: Optional[int] = None,
    experiment_run_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    allow_overwrite: bool = False,
    is_retry: bool = False,
) -> Dict[str, Any]:
    """Execute a baseline algorithm across all mainshock windows in a loaded bundle.

    For multi-shard dat03-collection-export-v1 bundles in OFFICIAL mode, writes both:
    - Per-shard WHOLE_WINDOW_SHARD official bundles under <output_dir>/shards/<shard_id>/
    - Full-split aggregated official bundle under <output_dir>/
    """
    require(algorithm_name in ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"], f"UNSUPPORTED_BASELINE_ALGO:{algorithm_name}")

    features_table = bundle["features_table"]
    rows = features_table.to_pylist()
    source_ds_id = bundle.get("source_dataset_id", bundle.get("bundle_id"))
    collection_scope = bundle.get("collection_scope")

    runtime_meta = get_runtime_metadata(code_version=code_version, random_seed=random_seed)
    code_ver = code_version or runtime_meta["code_version"]
    exec_token = execution_id or uuid.uuid4().hex[:16]

    if experiment_run_id:
        effective_run_id = experiment_run_id
    else:
        effective_run_id = _derive_run_id(
            source_ds_id=source_ds_id,
            algorithm_name=algorithm_name,
            model_config_version=model_config_version,
            config=config,
            code_ver=code_ver,
            mode=mode,
            random_seed=random_seed,
            execution_token=exec_token,
            collection_scope=collection_scope,
        )

    # Group candidates by mainshock_event_id
    windows: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        mid = r["mainshock_event_id"]
        windows.setdefault(mid, []).append(r)

    all_memberships: List[Dict[str, Any]] = []
    all_summaries: List[Dict[str, Any]] = []
    window_mems_by_mid: Dict[str, List[Dict[str, Any]]] = {}
    window_sum_by_mid: Dict[str, Dict[str, Any]] = {}

    t_start = time.time()
    for mid, candidates in windows.items():
        if algorithm_name == "WINDOW":
            mem, sum_row = fit_window_baseline(
                candidates, mid, config=config, run_id=effective_run_id, dataset_id=source_ds_id, model_config_version=model_config_version
            )
        elif algorithm_name == "DBSCAN":
            mem, sum_row = fit_dbscan_baseline(
                candidates, mid, config=config, run_id=effective_run_id, dataset_id=source_ds_id, model_config_version=model_config_version
            )
        else:  # HDBSCAN_GLOBAL
            mem, sum_row = fit_hdbscan_global_baseline(
                candidates, mid, config=config, run_id=effective_run_id, dataset_id=source_ds_id, model_config_version=model_config_version
            )

        window_mems_by_mid[mid] = mem
        window_sum_by_mid[mid] = sum_row
        all_memberships.extend(mem)
        all_summaries.append(sum_row)

    total_runtime = time.time() - t_start

    # Write per-shard WHOLE_WINDOW_SHARD official bundles if running on a multi-shard collection
    shard_runs: Dict[str, Dict[str, Any]] = {}
    if mode == MODE_OFFICIAL and bundle.get("is_collection") and isinstance(bundle.get("shards"), dict):
        out_root = Path(output_dir).resolve()
        for shard_id, shard_bundle in bundle["shards"].items():
            shard_mids = shard_bundle["mainshocks_table"]["mainshock_event_id"].to_pylist()
            shard_mems = [dict(m) for mid in shard_mids for m in window_mems_by_mid[mid]]
            shard_sums = [dict(window_sum_by_mid[mid]) for mid in shard_mids]
            _annotate_multi_sequence_status(shard_mems)
            shard_runtime = sum(float(s.get("runtime_seconds", 0.0)) for s in shard_sums)
            shard_metrics = _compute_baseline_metrics(
                algorithm_name,
                model_config_version,
                shard_mems,
                shard_sums,
                shard_runtime,
                len(shard_mids),
            )
            shard_metrics["collection_scope"] = shard_bundle["collection_scope"]

            shard_run_id = (
                f"{experiment_run_id}_{shard_id}"
                if experiment_run_id
                else _derive_run_id(
                    source_ds_id=source_ds_id,
                    algorithm_name=algorithm_name,
                    model_config_version=model_config_version,
                    config=config,
                    code_ver=code_ver,
                    mode=mode,
                    random_seed=random_seed,
                    execution_token=f"{exec_token}_{shard_id}",
                    collection_scope=shard_bundle["collection_scope"],
                )
            )
            shard_dir = out_root / "shards" / shard_id
            shard_write = write_experiment_result(
                output_dir=shard_dir,
                bundle=shard_bundle,
                algorithm_name=algorithm_name,
                algorithm_version="1.0.0",
                model_config_version=model_config_version,
                model_config=config,
                memberships=shard_mems,
                summaries=shard_sums,
                metrics=shard_metrics,
                mode=MODE_OFFICIAL,
                code_version=code_version,
                random_seed=random_seed,
                experiment_run_id=shard_run_id,
                execution_id=f"{exec_token}_{shard_id}",
                allow_overwrite=allow_overwrite,
                is_retry=is_retry,
            )
            shard_runs[shard_id] = {
                "shard_id": shard_id,
                "relative_dir": f"shards/{shard_id}",
                "run_id": shard_write["run_id"],
                "collection_scope": shard_bundle["collection_scope"],
                "membership_row_count": len(shard_mems),
                "summary_row_count": len(shard_sums),
                "has_success_marker": shard_write["has_success_marker"],
                "checksums": shard_write["checksums"],
                "is_reused": shard_write["is_reused"],
                "metrics": shard_metrics,
            }

    # Multi-sequence overlap detection across the entire loaded bundle (full split)
    _annotate_multi_sequence_status(all_memberships)

    metrics = _compute_baseline_metrics(
        algorithm_name,
        model_config_version,
        all_memberships,
        all_summaries,
        total_runtime,
        len(windows),
    )
    if shard_runs:
        metrics["collection_id"] = bundle.get("collection_id")
        metrics["dataset_split"] = bundle.get("dataset_split")
        metrics["shard_count"] = len(shard_runs)
        metrics["shard_runs"] = {
            sid: {
                "relative_dir": info["relative_dir"],
                "run_id": info["run_id"],
                "membership_row_count": info["membership_row_count"],
                "summary_row_count": info["summary_row_count"],
                "successful_windows": info["metrics"]["successful_windows"],
                "failed_windows": info["metrics"]["failed_windows"],
                "total_sequence_members": info["metrics"]["total_sequence_members"],
                "total_aftershocks": info["metrics"]["total_aftershocks"],
                "total_noise_candidates": info["metrics"]["total_noise_candidates"],
            }
            for sid, info in shard_runs.items()
        }

    # Write full-bundle result with synchronized effective_run_id
    write_res = write_experiment_result(
        output_dir=output_dir,
        bundle=bundle,
        algorithm_name=algorithm_name,
        algorithm_version="1.0.0",
        model_config_version=model_config_version,
        model_config=config,
        memberships=all_memberships,
        summaries=all_summaries,
        metrics=metrics,
        mode=mode,
        code_version=code_version,
        random_seed=random_seed,
        experiment_run_id=effective_run_id,
        execution_id=exec_token,
        allow_overwrite=allow_overwrite,
        is_retry=is_retry,
    )

    result = {
        "algorithm_name": algorithm_name,
        "run_id": effective_run_id,
        "metrics": metrics,
        "write_result": write_res,
        "summaries": all_summaries,
        "memberships": all_memberships,
    }
    if shard_runs:
        result["shard_runs"] = shard_runs
    return result


# -----------------------------------------------------------------------------
# 5. Shared Bundle Reconciliation & Report Generators (Pilot & Official)
# -----------------------------------------------------------------------------
def _extract_bundle_grains_and_checksum(bundle: Dict[str, Any]) -> Tuple[Counter, set, int, str]:
    """Extract exact grain multiplicity, mainshock set, row count, and bundle checksum."""
    require("bundle_id" in bundle and "features_table" in bundle, "INVALID_BUNDLE_STRUCTURE")
    features_table = bundle["features_table"]
    feature_count = features_table.num_rows
    require(feature_count > 0, "EMPTY_PILOT_BUNDLE")

    features_pydict = features_table.to_pydict()
    require("mainshock_event_id" in features_pydict and "candidate_event_id" in features_pydict,
            "INVALID_FEATURES_TABLE_SCHEMA: missing mainshock_event_id or candidate_event_id")
    input_grains = Counter(
        zip(features_pydict["mainshock_event_id"], features_pydict["candidate_event_id"])
    )
    input_mainshocks = set(features_pydict["mainshock_event_id"])

    checksum_map = dict(bundle.get("checksums") or {})
    if not checksum_map and "bundle_dir" in bundle:
        cpath = Path(bundle["bundle_dir"]) / "checksums.sha256"
        if cpath.is_file():
            for line in cpath.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and "  " in line:
                    digest, name = line.split("  ", 1)
                    checksum_map[name] = digest

    feature_checksum = (
        checksum_map.get("features-part-00000.parquet")
        or checksum_map.get("features.parquet")
        or bundle.get("manifest", {}).get("feature_content_sha256")
    )
    if not feature_checksum:
        for fname, fdigest in checksum_map.items():
            if fname.endswith("features.parquet") or (fname.startswith("features") and fname.endswith(".parquet")):
                feature_checksum = fdigest
                break

    if not feature_checksum:
        manifest_checksums = bundle.get("manifest", {}).get("checksums", {})
        feature_checksum = (
            manifest_checksums.get("features-part-00000.parquet")
            or manifest_checksums.get("features.parquet")
        )

    require(feature_checksum is not None and len(feature_checksum) == 64,
            f"MISSING_INPUT_BUNDLE_CHECKSUM: Unable to extract features parquet checksum for bundle {bundle.get('bundle_id')}")
    return input_grains, input_mainshocks, feature_count, feature_checksum


def _verify_and_reconcile_algo_bundle(
    algo: str,
    res: Dict[str, Any],
    *,
    input_grains: Counter,
    input_mainshocks: set,
    feature_count: int,
    source_ds_id: str,
    bundle_id: str,
    mode: str,
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Verify physical artifacts, checksums, lineage, grain, roles, probabilities, and window summaries."""
    require(isinstance(res, dict), f"INVALID_RESULT_STRUCTURE:{algo}")
    require("metrics" in res and "memberships" in res and "summaries" in res and "write_result" in res,
            f"INCOMPLETE_RESULT_STRUCTURE:{algo}")

    metrics = res["metrics"]
    write_res = res["write_result"]
    memberships = res["memberships"]
    summaries = res["summaries"]
    run_id = res.get("run_id")

    require(bool(run_id), f"MISSING_RUN_ID:{algo}")
    require(write_res.get("run_id") == run_id, f"RUN_ID_MISMATCH:{algo}:{write_res.get('run_id')}!={run_id}")

    total_cands = metrics.get("total_candidates", 0)
    require(total_cands == feature_count,
            f"COUNT_MISMATCH:{algo}: metrics.total_candidates ({total_cands}) != features count ({feature_count})")
    require(len(memberships) == feature_count,
            f"MEMBERSHIP_COUNT_MISMATCH:{algo}: len(memberships) ({len(memberships)}) != features count ({feature_count})")

    if mode == MODE_OFFICIAL:
        marker_path = write_res.get("success_marker_path")
        marker_name = "_SUCCESS.json"
        require(write_res.get("has_success_marker") is True, f"MISSING_SUCCESS_MARKER:{algo}")
    else:
        marker_path = write_res.get("manifest_path")
        marker_name = "pilot_result_manifest.json"
        require(not write_res.get("has_success_marker", False), f"ILLEGAL_SUCCESS_MARKER_IN_PILOT:{algo}")

    require(marker_path is not None and Path(marker_path).is_file(),
            f"MISSING_MANIFEST_FILE:{algo}:{marker_path}")

    algo_out_dir = Path(marker_path).parent
    manifest_data = json.loads(Path(marker_path).read_text(encoding="utf-8"))
    checksums = manifest_data.get("checksums", {})

    if mode == MODE_OFFICIAL:
        require(manifest_data.get("status") == "COMPLETED", f"OFFICIAL_STATUS_NOT_COMPLETED:{algo}")
        require(manifest_data.get("dataset_id") == source_ds_id,
                f"MANIFEST_DATASET_ID_MISMATCH:{algo}:{manifest_data.get('dataset_id')}!={source_ds_id}")
        require(manifest_data.get("membership_row_count") == feature_count,
                f"MANIFEST_MEMBERSHIP_COUNT_MISMATCH:{algo}")
        require(manifest_data.get("summary_row_count") == len(summaries),
                f"MANIFEST_SUMMARY_COUNT_MISMATCH:{algo}")
        validate_safe_metadata(manifest_data)
    else:
        if "pilot_id" in manifest_data:
            require(manifest_data.get("pilot_id") == bundle_id,
                    f"MANIFEST_PILOT_ID_MISMATCH:{algo}:{manifest_data.get('pilot_id')}!={bundle_id}")
        if "source_dataset_id" in manifest_data:
            require(manifest_data.get("source_dataset_id") == source_ds_id,
                    f"MANIFEST_DATASET_ID_MISMATCH:{algo}:{manifest_data.get('source_dataset_id')}!={source_ds_id}")

    manifest_run_id = manifest_data.get("pilot_run_id") or manifest_data.get("experiment_run_id")
    require(manifest_run_id == run_id,
            f"MANIFEST_RUN_ID_MISMATCH:{algo}:{manifest_run_id}!={run_id}")
    require(manifest_data.get("algorithm_name") == algo,
            f"MANIFEST_ALGO_MISMATCH:{algo}:{manifest_data.get('algorithm_name')}!={algo}")

    standard_files = [
        "memberships.parquet",
        "sequence_summary.parquet",
        "experiment_metrics.json",
        "experiment_config.json",
    ]
    if mode == MODE_OFFICIAL:
        standard_files.append("requirements-lock.txt")

    algo_checksums: Dict[str, str] = {}
    for sf in standard_files:
        target_file = algo_out_dir / sf
        require(target_file.is_file(), f"MISSING_REQUIRED_ARTIFACT:{algo}:{sf}")
        actual_sha = sha256_bytes(target_file.read_bytes())
        require(sf in checksums and bool(checksums[sf]), f"MISSING_MANIFEST_CHECKSUM_ENTRY:{algo}:{sf}")
        require(checksums[sf] == actual_sha, f"CHECKSUM_MISMATCH:{algo}:{sf}:{checksums[sf]}!={actual_sha}")
        algo_checksums[sf] = actual_sha

    algo_checksums[marker_name] = sha256_bytes(Path(marker_path).read_bytes())

    metrics_file = algo_out_dir / "experiment_metrics.json"
    try:
        disk_metrics = json.loads(metrics_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"CORRUPT_PHYSICAL_METRICS_JSON:{algo}:{exc}")
    require(isinstance(disk_metrics, dict), f"INVALID_PHYSICAL_METRICS_TYPE:{algo}")
    require(disk_metrics.get("dataset_id") == source_ds_id,
            f"METRICS_DATASET_ID_MISMATCH:{algo}:{disk_metrics.get('dataset_id')}!={source_ds_id}")
    require(disk_metrics.get("experiment_run_id") == run_id,
            f"METRICS_RUN_ID_MISMATCH:{algo}:{disk_metrics.get('experiment_run_id')}!={run_id}")
    require(disk_metrics.get("algorithm_name") == algo,
            f"METRICS_ALGO_MISMATCH:{algo}:{disk_metrics.get('algorithm_name')}!={algo}")
    if mode == MODE_OFFICIAL:
        validate_safe_metadata(disk_metrics)

    disk_inner_metrics = disk_metrics.get("metrics") if isinstance(disk_metrics.get("metrics"), dict) else disk_metrics
    disk_total_cands = disk_inner_metrics.get("total_candidates", disk_metrics.get("membership_row_count"))
    require(disk_total_cands == feature_count,
            f"PHYSICAL_METRICS_COUNT_MISMATCH:{algo}:{disk_total_cands}!={feature_count}")
    require(disk_metrics.get("membership_row_count") == feature_count,
            f"PHYSICAL_METRICS_MEMBERSHIP_COUNT_MISMATCH:{algo}:{disk_metrics.get('membership_row_count')}!={feature_count}")
    require(disk_metrics.get("summary_row_count") == len(summaries),
            f"PHYSICAL_METRICS_SUMMARY_COUNT_MISMATCH:{algo}:{disk_metrics.get('summary_row_count')}!={len(summaries)}")

    config_file = algo_out_dir / "experiment_config.json"
    try:
        disk_config = json.loads(config_file.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"CORRUPT_PHYSICAL_CONFIG_JSON:{algo}:{exc}")
    require(isinstance(disk_config, dict), f"INVALID_PHYSICAL_CONFIG_TYPE:{algo}")
    require(disk_config.get("algorithm_name") == algo, f"PHYSICAL_CONFIG_ALGO_MISMATCH:{algo}")
    require(disk_config.get("dataset_id") == source_ds_id,
            f"CONFIG_DATASET_ID_MISMATCH:{algo}:{disk_config.get('dataset_id')}!={source_ds_id}")
    require(disk_config.get("experiment_run_id") == run_id,
            f"CONFIG_RUN_ID_MISMATCH:{algo}:{disk_config.get('experiment_run_id')}!={run_id}")
    if mode == MODE_OFFICIAL:
        validate_safe_metadata(disk_config)

    mems_file = algo_out_dir / "memberships.parquet"
    try:
        disk_mems_table = pq.read_table(mems_file)
    except Exception as exc:
        raise ValueError(f"CORRUPT_PHYSICAL_MEMBERSHIPS_PARQUET:{algo}:{exc}")
    require(disk_mems_table.num_rows == feature_count,
            f"PHYSICAL_MEMBERSHIP_ROW_COUNT_MISMATCH:{algo}:{disk_mems_table.num_rows}!={feature_count}")

    disk_mems_pydict = disk_mems_table.to_pydict()
    require("mainshock_event_id" in disk_mems_pydict and "candidate_event_id" in disk_mems_pydict,
            f"PHYSICAL_MEMBERSHIP_MISSING_GRAIN_COLUMNS:{algo}")
    disk_grains = Counter(
        zip(disk_mems_pydict["mainshock_event_id"], disk_mems_pydict["candidate_event_id"])
    )
    require(disk_grains == input_grains, f"PHYSICAL_MEMBERSHIP_GRAIN_MISMATCH:{algo}")
    require(set(disk_mems_pydict.get("dataset_id", [])) == {source_ds_id},
            f"PHYSICAL_MEMBERSHIP_DATASET_ID_MISMATCH:{algo}")
    require(set(disk_mems_pydict.get("experiment_run_id", [])) == {run_id},
            f"PHYSICAL_MEMBERSHIP_RUN_ID_MISMATCH:{algo}")
    require(set(disk_mems_pydict.get("algorithm_name", [])) == {algo},
            f"PHYSICAL_MEMBERSHIP_ALGO_MISMATCH:{algo}")

    summ_file = algo_out_dir / "sequence_summary.parquet"
    try:
        disk_summ_table = pq.read_table(summ_file)
    except Exception as exc:
        raise ValueError(f"CORRUPT_PHYSICAL_SUMMARY_PARQUET:{algo}:{exc}")
    require(disk_summ_table.num_rows == len(summaries),
            f"PHYSICAL_SUMMARY_ROW_COUNT_MISMATCH:{algo}:{disk_summ_table.num_rows}!={len(summaries)}")

    disk_summ_pydict = disk_summ_table.to_pydict()
    require("mainshock_event_id" in disk_summ_pydict,
            f"PHYSICAL_SUMMARY_MISSING_MAINSHOCK_COLUMN:{algo}")
    disk_summary_mainshocks = set(disk_summ_pydict["mainshock_event_id"])
    require(disk_summary_mainshocks == input_mainshocks,
            f"PHYSICAL_SUMMARY_MAINSHOCK_SET_MISMATCH:{algo}")
    require(set(disk_summ_pydict.get("dataset_id", [])) == {source_ds_id},
            f"PHYSICAL_SUMMARY_DATASET_ID_MISMATCH:{algo}")
    require(set(disk_summ_pydict.get("experiment_run_id", [])) == {run_id},
            f"PHYSICAL_SUMMARY_RUN_ID_MISMATCH:{algo}")
    require(set(disk_summ_pydict.get("algorithm_name", [])) == {algo},
            f"PHYSICAL_SUMMARY_ALGO_MISMATCH:{algo}")

    artifact_info = {
        "relative_dir": algo.lower(),
        "manifest_file": marker_name,
        "checksums": algo_checksums,
        "has_success_marker": bool(write_res.get("has_success_marker", False)),
        "run_id": run_id,
    }

    mem_grains = Counter((m["mainshock_event_id"], m["candidate_event_id"]) for m in memberships)
    require(mem_grains == input_grains, f"MEMBERSHIP_GRAIN_MISMATCH:{algo}")
    for m in memberships:
        require(m.get("dataset_id") == source_ds_id,
                f"MEMBERSHIP_DATASET_ID_MISMATCH:{algo}:{m.get('dataset_id')}!={source_ds_id}")
        require(m.get("experiment_run_id") == run_id,
                f"MEMBERSHIP_RUN_ID_MISMATCH:{algo}:{m.get('experiment_run_id')}!={run_id}")
        require(m.get("algorithm_name") == algo,
                f"MEMBERSHIP_ALGO_MISMATCH:{algo}:{m.get('algorithm_name')}!={algo}")

    summary_mainshocks = set(s["mainshock_event_id"] for s in summaries)
    require(summary_mainshocks == input_mainshocks, f"SUMMARY_MAINSHOCK_SET_MISMATCH:{algo}")
    for s in summaries:
        require(s.get("dataset_id") == source_ds_id,
                f"SUMMARY_DATASET_ID_MISMATCH:{algo}:{s.get('dataset_id')}!={source_ds_id}")
        require(s.get("experiment_run_id") == run_id,
                f"SUMMARY_RUN_ID_MISMATCH:{algo}:{s.get('experiment_run_id')}!={run_id}")
        require(s.get("algorithm_name") == algo,
                f"SUMMARY_ALGO_MISMATCH:{algo}:{s.get('algorithm_name')}!={algo}")

    for m in memberships:
        role = m.get("event_role_candidate")
        require(role in {"PRE", "MAINSHOCK", "POST"}, f"INVALID_EVENT_ROLE:{algo}:{role}")
        cid = m.get("candidate_event_id")
        mid = m.get("mainshock_event_id")
        if cid == mid:
            require(role == "MAINSHOCK", f"ROLE_MISMATCH_MAINSHOCK:{algo}:{cid}")

        prob = m.get("membership_probability")
        if algo in {"WINDOW", "DBSCAN"}:
            require(prob is None, f"NON_NULL_PROBABILITY_POLICY_VIOLATION:{algo}:{prob}")
        else:
            require(prob is not None and 0.0 <= prob <= 1.0, f"INVALID_HDBSCAN_PROBABILITY:{algo}:{prob}")

    window_map: Dict[str, List[Dict[str, Any]]] = {}
    for m in memberships:
        window_map.setdefault(m["mainshock_event_id"], []).append(m)

    require(len(summaries) == len(window_map),
            f"SUMMARY_WINDOW_COUNT_MISMATCH:{algo}: {len(summaries)} summaries != {len(window_map)} windows")

    summary_by_mid = {s["mainshock_event_id"]: s for s in summaries}
    mainshock_noise_count = 0
    empty_post_count = 0

    for mid, w_mems in window_map.items():
        require(mid in summary_by_mid, f"MISSING_SUMMARY_FOR_WINDOW:{algo}:{mid}")
        s = summary_by_mid[mid]
        require(s["candidate_count"] == len(w_mems),
                f"WINDOW_CANDIDATE_COUNT_MISMATCH:{algo}:{mid}:{s['candidate_count']}!={len(w_mems)}")

        w_status = s["window_result_status"]
        require(w_status in {"SUCCESS", "FAILED", "SKIPPED"}, f"INVALID_WINDOW_STATUS:{algo}:{w_status}")

        if w_status == "SUCCESS":
            seq_members = [m for m in w_mems if m["is_sequence_member"]]
            require(s["sequence_member_count"] == len(seq_members),
                    f"SEQ_MEMBER_COUNT_MISMATCH:{algo}:{mid}:{s['sequence_member_count']}!={len(seq_members)}")

            posts = [m for m in seq_members if m["event_role_candidate"] == "POST"]
            require(s["aftershock_count"] == len(posts),
                    f"AFTERSHOCK_COUNT_MISMATCH:{algo}:{mid}:{s['aftershock_count']}!={len(posts)}")

            pres = [m for m in seq_members if m["event_role_candidate"] == "PRE"]
            require(s["foreshock_candidate_count"] == len(pres),
                    f"FORESHOCK_COUNT_MISMATCH:{algo}:{mid}:{s['foreshock_candidate_count']}!={len(pres)}")

            if len(posts) == 0:
                empty_post_count += 1
                require(s["productive_sequence_detected"] is False, f"EMPTY_POST_MUST_NOT_BE_PRODUCTIVE:{algo}:{mid}")
            else:
                require(s["productive_sequence_detected"] is True, f"NON_EMPTY_POST_MUST_BE_PRODUCTIVE:{algo}:{mid}")

            if algo in {"DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"}:
                noises = [m for m in w_mems if m.get("is_noise") is True]
                require(s["noise_count"] == len(noises),
                        f"NOISE_COUNT_MISMATCH:{algo}:{mid}:{s['noise_count']}!={len(noises)}")
        elif w_status == "FAILED":
            reason = s.get("failure_reason_code")
            require(bool(reason), f"FAILED_WINDOW_MISSING_REASON_CODE:{algo}:{mid}")
            if reason == "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE":
                mainshock_noise_count += 1
                for m in w_mems:
                    require(not m["is_sequence_member"], f"MAINSHOCK_NOISE_WINDOW_MUST_HAVE_NO_MEMBERS:{algo}:{mid}")

    case_summary = {
        "total_windows": len(window_map),
        "successful_windows": sum(1 for s in summaries if s["window_result_status"] == "SUCCESS"),
        "failed_windows": sum(1 for s in summaries if s["window_result_status"] == "FAILED"),
        "mainshock_noise_windows": mainshock_noise_count,
        "empty_post_windows": empty_post_count,
    }
    return metrics, case_summary, artifact_info


def generate_local_smoke_report(
    bundle: Dict[str, Any],
    algorithm_results: Dict[str, Dict[str, Any]],
    output_path: Union[str, Path],
    code_version: Optional[str] = None,
    fresh_kernel_acceptance: Optional[bool] = None,
) -> Dict[str, Any]:
    """Generate the official local-smoke-report.json technical report for Colab gate."""
    required_algos = {"WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"}
    require(isinstance(algorithm_results, dict) and len(algorithm_results) > 0,
            "MISSING_REQUIRED_BASELINE_ALGORITHMS: algorithm_results is empty")
    require(set(algorithm_results.keys()) == required_algos,
            f"MISSING_REQUIRED_BASELINE_ALGORITHMS: expected {sorted(required_algos)}, got {sorted(algorithm_results.keys())}")

    input_grains, input_mainshocks, feature_count, bundle_checksum = _extract_bundle_grains_and_checksum(bundle)
    source_ds_id = bundle.get("source_dataset_id", bundle.get("bundle_id"))
    pilot_id = bundle.get("bundle_id")

    if fresh_kernel_acceptance is None:
        env_fk = os.environ.get("EXP02_FRESH_KERNEL_ACCEPTANCE", "").lower()
        if env_fk in {"1", "true", "yes"}:
            fresh_kernel_acceptance = True
        elif env_fk in {"0", "false", "no"}:
            fresh_kernel_acceptance = False
        else:
            fresh_kernel_acceptance = False

    runner_mode = os.environ.get("EXP02_RUNNER_MODE", "nbclient" if fresh_kernel_acceptance else "isolated_exec")
    local_smoke_status = "PASSED" if fresh_kernel_acceptance else "FALLBACK_EXEC_WARNING"
    colab_allowed = bool(fresh_kernel_acceptance)

    meta = get_runtime_metadata(code_version=code_version)
    reconciliation: Dict[str, Any] = {}
    case_results: Dict[str, Any] = {}
    artifacts_verified: Dict[str, Any] = {}

    for algo in sorted(required_algos):
        metrics, case_summary, artifact_info = _verify_and_reconcile_algo_bundle(
            algo,
            algorithm_results[algo],
            input_grains=input_grains,
            input_mainshocks=input_mainshocks,
            feature_count=feature_count,
            source_ds_id=source_ds_id,
            bundle_id=pilot_id,
            mode=MODE_DEVELOPMENT,
        )
        reconciliation[algo] = metrics
        case_results[algo] = case_summary
        artifacts_verified[algo] = {
            "relative_dir": artifact_info["relative_dir"],
            "manifest_file": artifact_info["manifest_file"],
            "checksums": artifact_info["checksums"],
            "has_success_marker": artifact_info["has_success_marker"],
        }

    checksum_index_sha = None
    if "bundle_dir" in bundle:
        cindex = Path(bundle["bundle_dir"]) / "checksums.sha256"
        if cindex.is_file():
            checksum_index_sha = sha256_bytes(cindex.read_bytes())

    report = {
        "report_version": "exp-02-local-smoke-v2",
        "pilot_id": pilot_id,
        "source_dataset_id": source_ds_id,
        "bundle_checksum": bundle_checksum,
        "bundle_fingerprint": {
            "features_sha256": bundle_checksum,
            "pilot_id": pilot_id,
            "checksums_index_sha256": checksum_index_sha,
        },
        "code_version": meta["code_version"],
        "python_version": meta["python_version"],
        "package_versions": meta["package_versions"],
        "algorithms_evaluated": sorted(algorithm_results.keys()),
        "reconciliation": reconciliation,
        "case_results": case_results,
        "artifacts_verified": artifacts_verified,
        "coverage_status": bundle.get("manifest", {}).get("coverage_status", "PARTIAL"),
        "local_smoke_status": local_smoke_status,
        "colab_migration_allowed": colab_allowed,
        "fresh_kernel_acceptance": bool(fresh_kernel_acceptance),
        "runner_mode": runner_mode,
        "technical_summary": "Window, DBSCAN, and HDBSCAN Global baselines fitted per-mainshock window without mutating inputs; counts reconciled; roles, null probabilities, and physical case outcomes verified.",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    out_file = Path(output_path).resolve()
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(canonical(report) + "\n", encoding="utf-8")
    return report


def verify_official_collection_shards(
    bundle: Dict[str, Any],
    res: Dict[str, Any],
    algo: str,
    *,
    expected_feature_count: int,
    expected_mainshock_count: int,
) -> Dict[str, Any]:
    """Verify per-shard WHOLE_WINDOW_SHARD official bundles for a multi-shard collection."""
    full_out_dir = Path(res["write_result"]["success_marker_path"]).parent
    shard_runs = res.get("shard_runs", {})
    require(set(shard_runs.keys()) == set(bundle["shards"].keys()),
            f"MISSING_SHARD_RUNS:{algo}")
    algo_shard_checks: Dict[str, Any] = {}
    shard_row_sum = 0
    shard_win_sum = 0
    for shard_id, shard_bundle in bundle["shards"].items():
        shard_dir = full_out_dir / "shards" / shard_id
        success_path = shard_dir / "_SUCCESS.json"
        cfg_path = shard_dir / "experiment_config.json"
        require(success_path.is_file() and cfg_path.is_file(),
                f"MISSING_SHARD_BUNDLE_FILES:{algo}:{shard_id}")
        shard_success = read_json_safe(success_path.read_bytes())
        shard_cfg = read_json_safe(cfg_path.read_bytes())
        require(shard_cfg.get("collection_scope") == shard_bundle["collection_scope"],
                f"SHARD_COLLECTION_SCOPE_MISMATCH:{algo}:{shard_id}")
        expected_rows = shard_bundle["features_table"].num_rows
        expected_mains = shard_bundle["mainshocks_table"].num_rows
        require(shard_success.get("membership_row_count") == expected_rows,
                f"SHARD_MEMBERSHIP_COUNT_MISMATCH:{algo}:{shard_id}")
        require(shard_success.get("summary_row_count") == expected_mains,
                f"SHARD_SUMMARY_COUNT_MISMATCH:{algo}:{shard_id}")
        shard_row_sum += expected_rows
        shard_win_sum += expected_mains
        algo_shard_checks[shard_id] = {
            "experiment_run_id": shard_success["experiment_run_id"],
            "relative_dir": f"shards/{shard_id}",
            "collection_scope": shard_bundle["collection_scope"],
            "membership_row_count": expected_rows,
            "summary_row_count": expected_mains,
            "success_marker_sha256": sha256_bytes(success_path.read_bytes()),
            "checksums": shard_success["checksums"],
        }
    require(shard_row_sum == expected_feature_count and shard_win_sum == expected_mainshock_count,
            f"SHARD_TOTALS_MISMATCH:{algo}")
    return algo_shard_checks


def generate_official_baseline_report(
    bundle: Dict[str, Any],
    algorithm_results: Dict[str, Dict[str, Any]],
    output_path: Union[str, Path],
    *,
    code_version: Optional[str] = None,
    random_seed: Optional[int] = None,
    model_config_pins: Optional[Dict[str, Any]] = None,
    handoff_sha256: Optional[str] = None,
) -> Dict[str, Any]:
    """Verify and generate official EXP-02 report across all shards and full split."""
    require(bundle.get("mode") == MODE_OFFICIAL and bundle.get("is_official") is True,
            "OFFICIAL_BUNDLE_REQUIRED")
    manifest = bundle.get("manifest", {})
    require(manifest.get("dataset_status") == "EXPORTED", "OFFICIAL_EXPORTED_DATASET_REQUIRED")

    required_algos = {"WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"}
    require(isinstance(algorithm_results, dict) and set(algorithm_results.keys()) == required_algos,
            f"MISSING_REQUIRED_BASELINE_ALGORITHMS: expected {sorted(required_algos)}")

    input_grains, input_mainshocks, feature_count, _ = _extract_bundle_grains_and_checksum(bundle)
    source_ds_id = bundle.get("source_dataset_id", bundle.get("bundle_id"))

    meta = get_runtime_metadata(code_version=code_version, random_seed=random_seed)
    reconciliation: Dict[str, Any] = {}
    case_results: Dict[str, Any] = {}
    artifacts_verified: Dict[str, Any] = {}
    shard_verifications: Dict[str, Any] = {}

    for algo in sorted(required_algos):
        res = algorithm_results[algo]
        metrics, case_summary, artifact_info = _verify_and_reconcile_algo_bundle(
            algo,
            res,
            input_grains=input_grains,
            input_mainshocks=input_mainshocks,
            feature_count=feature_count,
            source_ds_id=source_ds_id,
            bundle_id=source_ds_id,
            mode=MODE_OFFICIAL,
        )
        reconciliation[algo] = metrics
        case_results[algo] = case_summary
        artifacts_verified[algo] = artifact_info

        # Verify every per-shard WHOLE_WINDOW_SHARD bundle if input is a multi-shard collection
        if bundle.get("is_collection") and isinstance(bundle.get("shards"), dict):
            shard_verifications[algo] = verify_official_collection_shards(
                bundle,
                res,
                algo,
                expected_feature_count=feature_count,
                expected_mainshock_count=len(input_mainshocks),
            )

    report: Dict[str, Any] = {
        "report_version": "exp-02-official-v1",
        "task_id": "EXP-02",
        "status": "PASSED",
        "evaluation_scope": "OFFICIAL_RESEARCH",
        "scientific_approval": False,
        "dataset": {
            "dataset_id": source_ds_id,
            "dataset_split": manifest.get("dataset_split"),
            "dataset_status": manifest.get("dataset_status"),
            "collection_id": bundle.get("collection_id"),
            "shard_count": len(bundle.get("shards", {})) if bundle.get("is_collection") else 1,
            "feature_row_count": feature_count,
            "mainshock_count": len(input_mainshocks),
            "mc_value": bundle.get("manifest", {}).get("collection_plan", {}).get("global_mc", {}).get("mc_value"),
            "window_model_version": manifest.get("window_model_version"),
            "scaling_config_version": manifest.get("scaling_config_version"),
            "feature_version": manifest.get("feature_version"),
            "manifest_sha256": bundle.get("manifest_sha256"),
            "checksums_sha256": bundle.get("checksums_sha256"),
            "handoff_sha256": handoff_sha256,
            "gold_publication_id": manifest.get("gold_publication_id"),
            "gold_snapshot_id": str(manifest.get("gold_snapshot_id")) if manifest.get("gold_snapshot_id") is not None else None,
            "timestamp_reader_profile": bundle.get("timestamp_reader_profile"),
        },
        "code_version": meta["code_version"],
        "python_version": meta["python_version"],
        "package_versions": meta["package_versions"],
        "random_seed": random_seed,
        "model_config_pins": model_config_pins or {},
        "algorithms_evaluated": sorted(required_algos),
        "reconciliation": reconciliation,
        "case_results": case_results,
        "artifacts_verified": artifacts_verified,
        "shard_verifications": shard_verifications,
        "technical_summary": (
            "Official EXP-02 baselines (WINDOW, DBSCAN, HDBSCAN_GLOBAL) executed per-mainshock "
            "on EXPORTED research collection without mutating inputs or dropping failure windows."
        ),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    validate_safe_metadata(report)

    out_file = Path(output_path).resolve()
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")
    return report
