"""COL-02: Bounded baseline parameter sweep on development pilot.

Adheres strictly to:
- Maximum 8 parameter configurations (4 DBSCAN, 4 HDBSCAN_GLOBAL).
- WINDOW baseline as invariant control.
- Fixed 4-D feature vector [x_scaled, y_scaled, z_scaled, time_scaled].
- Non-mutation of input candidate bundle.
- Each configuration writes to an isolated output directory with distinct run_id and config hash.
- Invariant verification across reruns (deterministic partition matching and numerical tolerance).
"""
from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

# Add current dir to sys.path
CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from experiment_harness import (
    load_feature_bundle,
    require,
    canonical,
    sha256_bytes,
    MODE_DEVELOPMENT,
)
from baseline_models import (
    run_baseline_experiment,
    VECTOR_COLUMNS,
)

SWEEP_CONFIG_FILES = {
    "WINDOW_CONTROL": "config/ml/models/window_baseline_v1.json",
    "DBSCAN_C1": "config/ml/sweeps/dbscan_sweep_c1.json",
    "DBSCAN_C2": "config/ml/sweeps/dbscan_sweep_c2.json",
    "DBSCAN_C3": "config/ml/sweeps/dbscan_sweep_c3.json",
    "DBSCAN_C4": "config/ml/sweeps/dbscan_sweep_c4.json",
    "HDBSCAN_C1": "config/ml/sweeps/hdbscan_sweep_c1.json",
    "HDBSCAN_C2": "config/ml/sweeps/hdbscan_sweep_c2.json",
    "HDBSCAN_C3": "config/ml/sweeps/hdbscan_sweep_c3.json",
    "HDBSCAN_C4": "config/ml/sweeps/hdbscan_sweep_c4.json",
}


def load_sweep_config(project_root: Path, relative_path: str) -> Dict[str, Any]:
    full_path = project_root / relative_path
    require(full_path.is_file(), f"MISSING_SWEEP_CONFIG_FILE: {relative_path}")
    data = json.loads(full_path.read_text(encoding="utf-8"))
    require("algorithm_name" in data and "model_config_version" in data, "INVALID_SWEEP_CONFIG")
    return data


def run_bounded_parameter_sweep(
    bundle: Dict[str, Any],
    project_root: Path,
    output_dir: Path,
    config_keys: Optional[List[str]] = None,
    allow_overwrite: bool = True,
    code_version: str = "col02-sweep-v1",
    random_seed: int = 42,
) -> Dict[str, Any]:
    """Runs the bounded parameter sweep over the designated configurations."""
    require(bundle.get("mode") == MODE_DEVELOPMENT, "SWEEP_REQUIRES_DEVELOPMENT_MODE")
    require(not bundle.get("is_official", True), "OFFICIAL_DATASET_NOT_PERMITTED_FOR_SWEEP")
    features = bundle["features_table"]
    candidate_count = features.num_rows
    require(candidate_count > 0, "EMPTY_BUNDLE_CANDIDATES")

    keys = config_keys or list(SWEEP_CONFIG_FILES.keys())
    # Max 8 parameter configurations (excluding WINDOW_CONTROL)
    param_configs = [k for k in keys if k != "WINDOW_CONTROL"]
    require(len(param_configs) <= 8, f"EXCEEDED_MAX_8_PARAMETER_CONFIGS: {len(param_configs)}")

    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    sweep_results: Dict[str, Any] = {}
    matrix_rows: List[Dict[str, Any]] = []

    for key in keys:
        cfg_rel = SWEEP_CONFIG_FILES[key]
        cfg = load_sweep_config(project_root, cfg_rel)
        algo_name = cfg["algorithm_name"]
        cfg_version = cfg["model_config_version"]
        sub_out = output_dir / key.lower()

        t0 = time.time()
        res = run_baseline_experiment(
            bundle=bundle,
            algorithm_name=algo_name,
            config=cfg,
            model_config_version=cfg_version,
            output_dir=sub_out,
            mode=MODE_DEVELOPMENT,
            allow_overwrite=allow_overwrite,
            code_version=code_version,
            random_seed=random_seed,
        )
        elapsed = time.time() - t0

        m = res["metrics"]
        require(m["total_candidates"] == candidate_count, "CANDIDATE_POPULATION_MUTATED")
        require(not res["write_result"]["has_success_marker"], "FORBIDDEN_SUCCESS_MARKER_IN_SWEEP")

        row = {
            "config_key": key,
            "algorithm_name": algo_name,
            "model_config_version": cfg_version,
            "config_sha256": hashlib.sha256(canonical(cfg).encode("utf-8")).hexdigest(),
            "total_candidates": m["total_candidates"],
            "windows_processed": m["windows_processed"],
            "successful_windows": m["successful_windows"],
            "failed_windows": m["failed_windows"],
            "total_sequence_members": m["total_sequence_members"],
            "total_aftershocks": m["total_aftershocks"],
            "total_noise": m.get("total_noise", m.get("total_noise_candidates", 0)),
            "runtime_seconds": round(elapsed, 4),
            "output_directory": str(sub_out),
        }
        # Record hyperparameter details
        if algo_name == "DBSCAN":
            row["eps"] = cfg.get("eps")
            row["min_samples"] = cfg.get("min_samples")
        elif algo_name == "HDBSCAN_GLOBAL":
            row["min_cluster_size"] = cfg.get("min_cluster_size")
            row["min_samples"] = cfg.get("min_samples")

        matrix_rows.append(row)
        sweep_results[key] = res

    # Write summary matrix
    matrix_path = output_dir / "sweep_results_matrix.json"
    matrix_payload = {
        "sweep_version": "col-02-sweep-v1",
        "pilot_id": bundle["bundle_id"],
        "total_configurations": len(matrix_rows),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "configurations": matrix_rows,
    }
    matrix_path.write_text(json.dumps(matrix_payload, indent=2), encoding="utf-8")

    return {
        "matrix": matrix_payload,
        "results": sweep_results,
        "matrix_path": matrix_path,
    }


def verify_sweep_rerun_invariants(
    run_a: Dict[str, Any],
    run_b: Dict[str, Any],
    rtol: float = 1e-6,
    atol: float = 1e-8,
) -> Dict[str, Any]:
    """Verifies that rerunning a sweep configuration produces identical partitions and counts."""
    m_a, m_b = run_a["metrics"], run_b["metrics"]
    require(m_a["total_candidates"] == m_b["total_candidates"], "RERUN_CANDIDATE_COUNT_MISMATCH")
    require(m_a["windows_processed"] == m_b["windows_processed"], "RERUN_WINDOW_COUNT_MISMATCH")
    require(m_a["successful_windows"] == m_b["successful_windows"], "RERUN_SUCCESSFUL_WINDOWS_MISMATCH")
    require(m_a["failed_windows"] == m_b["failed_windows"], "RERUN_FAILED_WINDOWS_MISMATCH")
    require(m_a["total_sequence_members"] == m_b["total_sequence_members"], "RERUN_SEQUENCE_MEMBERS_MISMATCH")
    require(m_a.get("total_noise") == m_b.get("total_noise"), "RERUN_NOISE_COUNT_MISMATCH")

    # Grain-level verification
    rows_a = run_a["memberships"]
    rows_b = run_b["memberships"]
    require(len(rows_a) == len(rows_b), "RERUN_MEMBERSHIP_LENGTH_MISMATCH")

    def keyed(rows):
        return {(r["mainshock_event_id"], r["candidate_event_id"]): r for r in rows}

    dict_a, dict_b = keyed(rows_a), keyed(rows_b)
    require(dict_a.keys() == dict_b.keys(), "RERUN_GRAIN_KEY_MISMATCH")

    for key, ra in dict_a.items():
        rb = dict_b[key]
        require(ra["event_role_candidate"] == rb["event_role_candidate"], "RERUN_ROLE_MISMATCH")
        require(ra["is_noise"] == rb["is_noise"], "RERUN_NOISE_FLAG_MISMATCH")
        require(ra["is_mainshock_cluster_member"] == rb["is_mainshock_cluster_member"], "RERUN_CLUSTER_MEMBER_MISMATCH")
        prob_a, prob_b = ra["membership_probability"], rb["membership_probability"]
        if prob_a is not None and prob_b is not None:
            require(math.isclose(float(prob_a), float(prob_b), rel_tol=rtol, abs_tol=atol), "RERUN_PROBABILITY_TOLERANCE_EXCEEDED")
        else:
            require(prob_a is None and prob_b is None, "RERUN_NULL_PROBABILITY_POLICY_VIOLATED")

    # Partition structure invariance (label-permutation independent)
    def cluster_partitions(rows_dict):
        parts = {}
        for (ms_id, cand_id), r in rows_dict.items():
            cid = r["cluster_id"]
            if cid is not None and not r["is_noise"]:
                parts.setdefault((ms_id, cid), set()).add((ms_id, cand_id))
        return {frozenset(s) for s in parts.values()}

    require(cluster_partitions(dict_a) == cluster_partitions(dict_b), "RERUN_CLUSTER_PARTITION_MISMATCH")

    return {
        "status": "RERUN_INVARIANTS_PASSED",
        "total_candidates": m_a["total_candidates"],
        "rtol": rtol,
        "atol": atol,
    }
