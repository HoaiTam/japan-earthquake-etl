"""EXP-01: Shared reproducible experiment harness and Colab path adapter.

Provides:
- Mode-aware bundle loader (explicit DEVELOPMENT vs OFFICIAL).
- Runtime metadata & dependency pinning.
- Skeletal output builders for memberships & sequence summary (conforming to result-bundle-v1).
- Mode-aware result writers (pilot_result_manifest.json vs official _SUCCESS.json).
- Parameterized Colab path adapter with local smoke gate enforcement.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
from importlib.metadata import version as pkg_version
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Tuple, Union
import uuid

import pyarrow as pa
import pyarrow.parquet as pq

# -----------------------------------------------------------------------------
# Constants & Enums (CON-03-ML / MLI-01 result-bundle-v1)
# -----------------------------------------------------------------------------
SCHEMA_VERSION = "1.0"
VECTOR_COLUMNS = ["x_scaled", "y_scaled", "z_scaled", "time_scaled"]

MODE_DEVELOPMENT = "DEVELOPMENT"
MODE_OFFICIAL = "OFFICIAL"
VALID_MODES = {MODE_DEVELOPMENT, MODE_OFFICIAL}

ALGORITHMS = ["WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"]
ROLES = ["PRE", "MAINSHOCK", "POST"]
MULTI_SEQUENCE_STATUSES = ["NOT_MEMBER", "UNIQUE", "MULTIPLE", "AMBIGUOUS"]
WINDOW_RESULT_STATUSES = ["SUCCESS", "FAILED", "SKIPPED"]
FAILURE_REASON_CODES = [
    "EXP_MAINSHOCK_CLASSIFIED_AS_NOISE",
    "EXP_EMPTY_POST_SEQUENCE",
    "EXP_WINDOW_FAILED",
]
METRIC_REASON_CODES = [
    "METRIC_NOT_APPLICABLE",
    "METRIC_INSUFFICIENT_MEMBERS",
    "METRIC_FIT_FAILED",
    "METRIC_RUNTIME_UNAVAILABLE",
]

# Security pattern to reject credential leaks or local absolute home/user paths
UNSAFE_TEXT_PATTERN = re.compile(
    r"(?is).*(?:[a-z]:[\\/]|/Users/|/home/|://[^/]+@|[?&](?:token|password|secret|signature)=|gh[pousr]_[A-Za-z0-9]{36,}).*"
)
UNSAFE_KEY_PATTERN = re.compile(
    r"(?i).*(?:password|access_token|secret_key|private_key|credential).*"
)


def canonical(value: Any) -> str:
    """Deterministic JSON serialization with sorted keys and no whitespace variation."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def sha256_bytes(data: bytes) -> str:
    """Calculate lowercase hexadecimal SHA-256 digest of bytes."""
    return hashlib.sha256(data).hexdigest()


def require(condition: bool, reason: str) -> None:
    """Fail-fast precondition check."""
    if not condition:
        raise ValueError(reason)


def validate_safe_metadata(node: Any) -> None:
    """Ensure no credentials or hardcoded local home paths are present in metadata."""
    if isinstance(node, str):
        require(not UNSAFE_TEXT_PATTERN.match(node), f"EXP_UNSAFE_METADATA:{node}")
    elif isinstance(node, dict):
        for key, value in node.items():
            require(not UNSAFE_KEY_PATTERN.match(str(key)), f"EXP_UNSAFE_METADATA_KEY:{key}")
            validate_safe_metadata(value)
    elif isinstance(node, list):
        for item in node:
            validate_safe_metadata(item)


def read_json_safe(content: bytes) -> Dict[str, Any]:
    """Parse JSON with duplicate key detection and strict finite parsing."""
    def check_pairs(items: List[Tuple[str, Any]]) -> Dict[str, Any]:
        result = {}
        for k, v in items:
            require(k not in result, "EXP_DUPLICATE_JSON_KEY:" + k)
            result[k] = v
        return result

    parsed = json.loads(content, object_pairs_hook=check_pairs,
                        parse_constant=lambda _: require(False, "EXP_NONFINITE_JSON"))
    validate_safe_metadata(parsed)
    return parsed


# -----------------------------------------------------------------------------
# Arrow Schemas (Matching spark/src/main/resources/ml/result-bundle-v1.json)
# -----------------------------------------------------------------------------
MEMBERSHIP_FIELDS = [
    pa.field("schema_version", pa.string(), nullable=False),
    pa.field("experiment_run_id", pa.string(), nullable=False),
    pa.field("dataset_id", pa.string(), nullable=False),
    pa.field("mainshock_event_id", pa.string(), nullable=False),
    pa.field("candidate_event_id", pa.string(), nullable=False),
    pa.field("algorithm_name", pa.string(), nullable=False),
    pa.field("model_config_version", pa.string(), nullable=False),
    pa.field("cluster_id", pa.int64(), nullable=True),
    pa.field("is_noise", pa.bool_(), nullable=True),
    pa.field("is_mainshock_cluster_member", pa.bool_(), nullable=True),
    pa.field("is_sequence_member", pa.bool_(), nullable=False),
    pa.field("membership_probability", pa.float64(), nullable=True),
    pa.field("event_role_candidate", pa.string(), nullable=False),
    pa.field("normalized_distance", pa.float64(), nullable=False),
    pa.field("multi_sequence_status", pa.string(), nullable=False),
    pa.field("resolution_rank", pa.int32(), nullable=True),
    pa.field("created_at_utc", pa.timestamp("ms", tz="UTC"), nullable=False),
]
MEMBERSHIP_SCHEMA = pa.schema(MEMBERSHIP_FIELDS)

SEQUENCE_SUMMARY_FIELDS = [
    pa.field("schema_version", pa.string(), nullable=False),
    pa.field("experiment_run_id", pa.string(), nullable=False),
    pa.field("dataset_id", pa.string(), nullable=False),
    pa.field("mainshock_event_id", pa.string(), nullable=False),
    pa.field("algorithm_name", pa.string(), nullable=False),
    pa.field("model_config_version", pa.string(), nullable=False),
    pa.field("window_result_status", pa.string(), nullable=False),
    pa.field("productive_sequence_detected", pa.bool_(), nullable=True),
    pa.field("productive_definition_version", pa.string(), nullable=False),
    pa.field("candidate_count", pa.int64(), nullable=False),
    pa.field("cluster_count", pa.int32(), nullable=True),
    pa.field("noise_count", pa.int64(), nullable=True),
    pa.field("noise_ratio", pa.float64(), nullable=True),
    pa.field("sequence_member_count", pa.int64(), nullable=True),
    pa.field("aftershock_count", pa.int64(), nullable=True),
    pa.field("foreshock_candidate_count", pa.int64(), nullable=True),
    pa.field("sequence_duration_hours", pa.float64(), nullable=True),
    pa.field("largest_aftershock_magnitude", pa.float64(), nullable=True),
    pa.field("magnitude_difference", pa.float64(), nullable=True),
    pa.field("spatial_extent_km", pa.float64(), nullable=True),
    pa.field("mean_membership_probability", pa.float64(), nullable=True),
    pa.field("dbcv_score", pa.float64(), nullable=True),
    pa.field("omori_k", pa.float64(), nullable=True),
    pa.field("omori_c", pa.float64(), nullable=True),
    pa.field("omori_p", pa.float64(), nullable=True),
    pa.field("omori_fit_metric", pa.float64(), nullable=True),
    pa.field("stability_components_json", pa.string(), nullable=True),
    pa.field("runtime_seconds", pa.float64(), nullable=False),
    pa.field("peak_memory_mb", pa.float64(), nullable=True),
    pa.field("is_nested_mainshock_candidate", pa.bool_(), nullable=False),
    pa.field("parent_mainshock_event_id", pa.string(), nullable=True),
    pa.field("nested_membership_probability", pa.float64(), nullable=True),
    pa.field("metric_reason_codes", pa.list_(pa.field("element", pa.string(), nullable=False)), nullable=True),
    pa.field("failure_reason_code", pa.string(), nullable=True),
    pa.field("created_at_utc", pa.timestamp("ms", tz="UTC"), nullable=False),
]
SEQUENCE_SUMMARY_SCHEMA = pa.schema(SEQUENCE_SUMMARY_FIELDS)


# -----------------------------------------------------------------------------
# Runtime Metadata & Pinning
# -----------------------------------------------------------------------------
def get_runtime_metadata(
    code_version: Optional[str] = None,
    random_seed: Optional[int] = None,
    timestamp_utc: Optional[str] = None,
) -> Dict[str, Any]:
    """Capture environment, python, package versions, and execution parameters."""
    packages = ["pyarrow", "numpy", "scipy", "scikit-learn", "hdbscan", "nbformat", "nbclient"]
    pkg_map = {}
    for p in packages:
        try:
            pkg_map[p] = pkg_version(p)
        except Exception:
            pkg_map[p] = "not-installed"

    return {
        "schema_version": SCHEMA_VERSION,
        "python_version": sys.version.split()[0],
        "package_versions": pkg_map,
        "code_version": code_version or "dev-local",
        "random_seed": random_seed,
        "timestamp_utc": timestamp_utc or datetime.now(timezone.utc).isoformat(),
        "vector_columns": VECTOR_COLUMNS,
    }


# -----------------------------------------------------------------------------
# Bundle Loader (Mode-Aware: DEVELOPMENT vs OFFICIAL)
# -----------------------------------------------------------------------------
def load_feature_bundle(bundle_dir: Union[str, Path], mode: str = MODE_DEVELOPMENT) -> Dict[str, Any]:
    """Load and strictly validate a feature bundle according to the declared mode.

    Fails before model fitting if checksum, schema, finite constraint or mode contracts fail.
    - DEVELOPMENT mode: requires DAT-02 DEVELOPMENT_PILOT bundle.
    - OFFICIAL mode: requires MLD-05 EXPORTED official dataset bundle.
    """
    require(mode in VALID_MODES, f"INVALID_MODE:{mode}")
    root = Path(bundle_dir).resolve()
    require(root.is_dir() and not root.is_symlink(), "EXP_EXPLICIT_DIRECTORY_REQUIRED")

    checksum_file = root / "checksums.sha256"
    require(checksum_file.is_file() and not checksum_file.is_symlink(), "EXP_MISSING_CHECKSUMS_FILE")

    # Verify checksums.sha256 format and values
    checksum_entries: Dict[str, str] = {}
    for line in checksum_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        require(re.fullmatch(r"[a-f0-9]{64}  [A-Za-z0-9_.-]+", line) is not None, "EXP_CHECKSUM_FORMAT")
        digest, name = line.split("  ")
        require(name not in checksum_entries, "EXP_DUPLICATE_CHECKSUM_ENTRY:" + name)
        checksum_entries[name] = digest

    # Verify every indexed file matches checksum
    for name, digest in checksum_entries.items():
        file_path = root / name
        require(file_path.is_file() and not file_path.is_symlink(), "EXP_MISSING_INDEXED_FILE:" + name)
        require(sha256_bytes(file_path.read_bytes()) == digest, "EXP_CHECKSUM_MISMATCH:" + name)

    if mode == MODE_DEVELOPMENT:
        # Check for official dataset manifest FIRST to give explicit mode rejection
        found_files = {p.name for p in root.iterdir()}
        require("dataset_manifest.json" not in found_files, "DEVELOPMENT_LOADER_REJECTS_OFFICIAL_DATASET")

        # Check development pilot file set
        expected_files = {"features-part-00000.parquet", "mainshocks.parquet", "pilot_manifest.json",
                          "coverage_report.json", "checksums.sha256"}
        require(found_files == expected_files, "EXP_PILOT_FILE_SET_MISMATCH")
        require(set(checksum_entries.keys()) == expected_files - {"checksums.sha256"}, "EXP_PILOT_CHECKSUM_SET_MISMATCH")

        # Reuse pilot_bundle loader verification logic
        notebooks_dir = Path(__file__).resolve().parent
        if str(notebooks_dir) not in sys.path:
            sys.path.insert(0, str(notebooks_dir))
        try:
            from pilot_bundle import load_bundle as load_pilot
            pilot_data = load_pilot(root)
        except ImportError:
            import importlib.util
            spec = importlib.util.spec_from_file_location("pilot_bundle", notebooks_dir / "pilot_bundle.py")
            if spec and spec.loader:
                pilot_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(pilot_mod)
                pilot_data = pilot_mod.load_bundle(root)
            else:
                raise ImportError("Cannot load pilot_bundle module from " + str(notebooks_dir / "pilot_bundle.py"))

        manifest = pilot_data["manifest"]
        require(manifest["artifact_kind"] == "DEVELOPMENT_PILOT", "EXP_NOT_DEVELOPMENT_PILOT")
        require(manifest["allowed_usage"] == "DEVELOPMENT_ONLY", "EXP_NOT_DEVELOPMENT_ONLY")
        require(manifest["research_ready"] is False, "EXP_PILOT_CANNOT_BE_RESEARCH_READY")

        return {
            "mode": MODE_DEVELOPMENT,
            "bundle_id": manifest["pilot_id"],
            "source_dataset_id": manifest["source_dataset_id"],
            "manifest": manifest,
            "features_table": pilot_data["features"],
            "mainshocks_table": pilot_data["mainshocks"],
            "coverage": pilot_data["coverage"],
            "bundle_dir": root,
            "is_official": False,
            "checksums": checksum_entries,
        }

    else:  # mode == MODE_OFFICIAL
        found_files = {p.name for p in root.iterdir() if p.is_file()}
        require("pilot_manifest.json" not in found_files, "OFFICIAL_LOADER_REJECTS_PILOT_BUNDLE")

        # Official bundle MUST have a non-empty checksums.sha256 file
        require(len(checksum_entries) > 0, "EXP_EMPTY_CHECKSUMS_FILE")

        # Every file in the bundle directory (except checksums.sha256) MUST be indexed
        expected_indexed_files = found_files - {"checksums.sha256"}
        require(expected_indexed_files == set(checksum_entries.keys()), "EXP_OFFICIAL_CHECKSUM_SET_MISMATCH")

        # Check official dataset export manifest
        manifest_path = root / "dataset_manifest.json"
        require(manifest_path.is_file(), "EXP_MISSING_DATASET_MANIFEST")
        require("dataset_manifest.json" in checksum_entries, "EXP_MISSING_DATASET_MANIFEST_CHECKSUM")

        manifest = read_json_safe(manifest_path.read_bytes())
        require(manifest.get("schema_version") == "1.0", "EXP_OFFICIAL_SCHEMA_VERSION_MISMATCH")
        require(manifest.get("dataset_status") == "EXPORTED", "EXP_DATASET_NOT_EXPORTED")
        require(manifest.get("artifact_kind") != "DEVELOPMENT_PILOT", "OFFICIAL_LOADER_REJECTS_PILOT_BUNDLE")
        require(manifest.get("allowed_usage") != "DEVELOPMENT_ONLY", "EXP_OFFICIAL_CANNOT_BE_DEVELOPMENT_ONLY")

        # Load features parquet
        feature_parts = sorted(root.glob("features-part-*.parquet"))
        require(len(feature_parts) > 0, "EXP_NO_FEATURE_PARQUET_FOUND")
        for fp in feature_parts:
            require(fp.name in checksum_entries, f"EXP_MISSING_FEATURE_PARQUET_CHECKSUM:{fp.name}")

        tables = [pq.read_table(p) for p in feature_parts]
        features_table = pa.concat_tables(tables)

        # Validate required columns according to MLD-05 contract
        id_cols = ["dataset_id", "mainshock_event_id", "candidate_event_id"]
        required_cols = id_cols + VECTOR_COLUMNS
        for col_name in required_cols:
            require(col_name in features_table.column_names, f"EXP_MISSING_REQUIRED_COL:{col_name}")

        # Validate PyArrow data types and null/empty constraints for ID columns
        for col_name in id_cols:
            col = features_table[col_name]
            require(pa.types.is_string(col.type) or pa.types.is_large_string(col.type),
                    f"EXP_INVALID_ID_COL_TYPE:{col_name}:{col.type}")
            require(col.null_count == 0, f"EXP_NULL_REQUIRED_ID_COL:{col_name}")
            for chunk in col.chunks:
                for val in chunk.to_pylist():
                    require(val is not None and len(val.strip()) > 0, f"EXP_EMPTY_REQUIRED_ID_COL:{col_name}")

        # Validate PyArrow floating types, null check, and finite constraints for vector columns
        for col_name in VECTOR_COLUMNS:
            col = features_table[col_name]
            require(pa.types.is_floating(col.type), f"EXP_INVALID_VECTOR_COL_TYPE:{col_name}:{col.type}")
            require(col.null_count == 0, f"EXP_NULL_VECTOR_COL:{col_name}")
            for chunk in col.chunks:
                for val in chunk.to_pylist():
                    require(val is not None and math.isfinite(val), f"EXP_NONFINITE_FEATURE:{col_name}")

        # Validate feature counts
        expected_rows = manifest.get("feature_row_count")
        if expected_rows is not None:
            require(features_table.num_rows == expected_rows, "EXP_FEATURE_ROW_COUNT_MISMATCH")

        # Lineage check: dataset_id in all rows must strictly match manifest dataset_id
        dataset_ids_in_rows = set(features_table["dataset_id"].to_pylist())
        require(dataset_ids_in_rows == {manifest["dataset_id"]},
                f"EXP_DATASET_LINEAGE_MISMATCH:{dataset_ids_in_rows}!={manifest['dataset_id']}")

        # Unique grain check: (dataset_id, mainshock_event_id, candidate_event_id)
        # and self-event zero-vector check
        pydict = features_table.to_pydict()
        n_rows = features_table.num_rows
        seen_grains = set()
        window_self_found: Dict[str, bool] = {}

        for i in range(n_rows):
            d_id = pydict["dataset_id"][i]
            m_id = pydict["mainshock_event_id"][i]
            c_id = pydict["candidate_event_id"][i]

            grain_key = (d_id, m_id, c_id)
            require(grain_key not in seen_grains, f"EXP_DUPLICATE_GRAIN:{grain_key}")
            seen_grains.add(grain_key)

            if c_id == m_id:
                # Mainshock self-event must have zero vector
                window_self_found[m_id] = True
                for vcol in VECTOR_COLUMNS:
                    val = float(pydict[vcol][i])
                    require(abs(val) < 1e-6, f"EXP_NONZERO_SELF_VECTOR:{vcol}={val}")
            else:
                if m_id not in window_self_found:
                    window_self_found[m_id] = False

        # Every mainshock window must contain its own self event
        for mid, has_self in window_self_found.items():
            require(has_self, f"EXP_MISSING_SELF_EVENT:{mid}")

        return {
            "mode": MODE_OFFICIAL,
            "bundle_id": manifest["dataset_id"],
            "source_dataset_id": manifest["dataset_id"],
            "manifest": manifest,
            "features_table": features_table,
            "mainshocks_table": None,
            "coverage": None,
            "bundle_dir": root,
            "is_official": True,
            "checksums": checksum_entries,
        }


# -----------------------------------------------------------------------------
# Skeletal Output Builders (Memberships & Summary)
# -----------------------------------------------------------------------------
def build_membership_rows(
    experiment_run_id: str,
    dataset_id: str,
    mainshock_event_id: str,
    candidate_records: List[Dict[str, Any]],
    algorithm_name: str,
    model_config_version: str,
    cluster_labels: Optional[List[int]] = None,
    probabilities: Optional[List[float]] = None,
    normalized_distances: Optional[List[float]] = None,
    created_at_utc: Optional[datetime] = None,
) -> List[Dict[str, Any]]:
    """Build valid membership dictionary rows conforming to result-bundle-v1."""
    require(algorithm_name in ALGORITHMS, f"INVALID_ALGORITHM:{algorithm_name}")
    n = len(candidate_records)
    if cluster_labels is None:
        cluster_labels = [-1] * n
    if probabilities is None:
        probabilities = [None] * n
    if normalized_distances is None:
        normalized_distances = [0.0] * n
    if created_at_utc is None:
        created_at_utc = datetime.now(timezone.utc)

    is_window = algorithm_name == "WINDOW"
    is_hdb = algorithm_name.startswith("HDBSCAN")

    rows = []
    main_cluster_label = None

    # First find mainshock cluster label
    for i, c in enumerate(candidate_records):
        cid = c.get("candidate_event_id", c.get("event_id"))
        if cid == mainshock_event_id:
            main_cluster_label = cluster_labels[i]
            break

    for i, c in enumerate(candidate_records):
        cid = c.get("candidate_event_id", c.get("event_id"))
        is_self = cid == mainshock_event_id
        delta_t = c.get("delta_time_hours", 0.0)
        role = "MAINSHOCK" if is_self else ("PRE" if delta_t < 0 else "POST")

        label = cluster_labels[i]
        prob = probabilities[i]
        dist = max(0.0, float(normalized_distances[i]))

        if is_window:
            cluster_id_val = None
            is_noise_val = None
            is_main_cluster_val = None
            is_member_val = True  # WINDOW baseline considers post in window a candidate member
            prob_val = None
            multi_status = "UNIQUE" if is_member_val else "NOT_MEMBER"
        else:
            is_noise_val = (label == -1)
            cluster_id_val = int(label)
            is_main_cluster_val = (main_cluster_label is not None and main_cluster_label >= 0 and label == main_cluster_label)
            # Sequence member if belongs to mainshock cluster
            is_member_val = bool(is_main_cluster_val)
            prob_val = float(prob) if (is_hdb and prob is not None) else None
            multi_status = "UNIQUE" if is_member_val else "NOT_MEMBER"

        row = {
            "schema_version": SCHEMA_VERSION,
            "experiment_run_id": experiment_run_id,
            "dataset_id": dataset_id,
            "mainshock_event_id": mainshock_event_id,
            "candidate_event_id": cid,
            "algorithm_name": algorithm_name,
            "model_config_version": model_config_version,
            "cluster_id": cluster_id_val,
            "is_noise": is_noise_val,
            "is_mainshock_cluster_member": is_main_cluster_val,
            "is_sequence_member": is_member_val,
            "membership_probability": prob_val,
            "event_role_candidate": role,
            "normalized_distance": dist,
            "multi_sequence_status": multi_status,
            "resolution_rank": 1 if is_member_val else None,
            "created_at_utc": created_at_utc,
        }
        rows.append(row)

    return rows


def build_sequence_summary_row(
    experiment_run_id: str,
    dataset_id: str,
    mainshock_event_id: str,
    candidate_count: int,
    algorithm_name: str,
    model_config_version: str,
    membership_rows: List[Dict[str, Any]],
    window_result_status: str = "SUCCESS",
    failure_reason_code: Optional[str] = None,
    runtime_seconds: float = 0.01,
    created_at_utc: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Build a sequence summary dictionary row conforming to result-bundle-v1."""
    require(window_result_status in WINDOW_RESULT_STATUSES, f"INVALID_WINDOW_STATUS:{window_result_status}")
    require(algorithm_name in ALGORITHMS, f"INVALID_ALGORITHM:{algorithm_name}")
    if created_at_utc is None:
        created_at_utc = datetime.now(timezone.utc)

    is_window = algorithm_name == "WINDOW"
    is_hdb = algorithm_name.startswith("HDBSCAN")

    if window_result_status == "SUCCESS":
        members = [r for r in membership_rows if r["is_sequence_member"]]
        posts = [r for r in members if r["event_role_candidate"] == "POST"]
        pres = [r for r in members if r["event_role_candidate"] == "PRE"]

        seq_member_count = len(members)
        aftershock_count = len(posts)
        foreshock_count = len(pres)
        productive_seq = aftershock_count > 0

        if is_window:
            cluster_count_val = None
            noise_count_val = None
            noise_ratio_val = None
        else:
            noise_items = [r for r in membership_rows if r.get("is_noise") is True]
            noise_count_val = len(noise_items)
            distinct_clusters = {r["cluster_id"] for r in membership_rows if r.get("cluster_id") is not None and r["cluster_id"] >= 0}
            cluster_count_val = len(distinct_clusters)
            noise_ratio_val = float(noise_count_val) / float(candidate_count) if candidate_count > 0 else 0.0

        if is_hdb and seq_member_count > 0:
            probs = [r["membership_probability"] for r in members if r["membership_probability"] is not None]
            mean_prob_val = float(sum(probs) / len(probs)) if probs else 1.0
        else:
            mean_prob_val = None

        summary_row = {
            "schema_version": SCHEMA_VERSION,
            "experiment_run_id": experiment_run_id,
            "dataset_id": dataset_id,
            "mainshock_event_id": mainshock_event_id,
            "algorithm_name": algorithm_name,
            "model_config_version": model_config_version,
            "window_result_status": "SUCCESS",
            "productive_sequence_detected": productive_seq,
            "productive_definition_version": "post_over_mc_v1",
            "candidate_count": int(candidate_count),
            "cluster_count": cluster_count_val,
            "noise_count": noise_count_val,
            "noise_ratio": noise_ratio_val,
            "sequence_member_count": int(seq_member_count),
            "aftershock_count": int(aftershock_count),
            "foreshock_candidate_count": int(foreshock_count),
            "sequence_duration_hours": None,
            "largest_aftershock_magnitude": None,
            "magnitude_difference": None,
            "spatial_extent_km": None,
            "mean_membership_probability": mean_prob_val,
            "dbcv_score": None,
            "omori_k": None,
            "omori_c": None,
            "omori_p": None,
            "omori_fit_metric": None,
            "stability_components_json": None,
            "runtime_seconds": float(runtime_seconds),
            "peak_memory_mb": None,
            "is_nested_mainshock_candidate": False,
            "parent_mainshock_event_id": None,
            "nested_membership_probability": None,
            "metric_reason_codes": ["METRIC_NOT_APPLICABLE"],
            "failure_reason_code": None,
            "created_at_utc": created_at_utc,
        }
    else:  # FAILED or SKIPPED
        require(failure_reason_code is not None and len(failure_reason_code) > 0, "MISSING_FAILURE_REASON_CODE")
        summary_row = {
            "schema_version": SCHEMA_VERSION,
            "experiment_run_id": experiment_run_id,
            "dataset_id": dataset_id,
            "mainshock_event_id": mainshock_event_id,
            "algorithm_name": algorithm_name,
            "model_config_version": model_config_version,
            "window_result_status": window_result_status,
            "productive_sequence_detected": None,
            "productive_definition_version": "post_over_mc_v1",
            "candidate_count": int(candidate_count),
            "cluster_count": None,
            "noise_count": None,
            "noise_ratio": None,
            "sequence_member_count": None,
            "aftershock_count": None,
            "foreshock_candidate_count": None,
            "sequence_duration_hours": None,
            "largest_aftershock_magnitude": None,
            "magnitude_difference": None,
            "spatial_extent_km": None,
            "mean_membership_probability": None,
            "dbcv_score": None,
            "omori_k": None,
            "omori_c": None,
            "omori_p": None,
            "omori_fit_metric": None,
            "stability_components_json": None,
            "runtime_seconds": float(runtime_seconds),
            "peak_memory_mb": None,
            "is_nested_mainshock_candidate": False,
            "parent_mainshock_event_id": None,
            "nested_membership_probability": None,
            "metric_reason_codes": ["METRIC_NOT_APPLICABLE"],
            "failure_reason_code": failure_reason_code,
            "created_at_utc": created_at_utc,
        }

    return summary_row


# -----------------------------------------------------------------------------
# Mode-Aware Result Writer (Pilot Manifest vs Official _SUCCESS.json)
# -----------------------------------------------------------------------------
def write_experiment_result(
    output_dir: Union[str, Path],
    bundle: Dict[str, Any],
    algorithm_name: str,
    algorithm_version: str,
    model_config_version: str,
    model_config: Dict[str, Any],
    memberships: Union[List[Dict[str, Any]], pa.Table],
    summaries: Union[List[Dict[str, Any]], pa.Table],
    metrics: Dict[str, Any],
    mode: str = MODE_DEVELOPMENT,
    code_version: Optional[str] = None,
    random_seed: Optional[int] = None,
    package_version_override: Optional[str] = None,
    optional_model_artifacts: Optional[Dict[str, bytes]] = None,
    experiment_run_id: Optional[str] = None,
    execution_id: Optional[str] = None,
    timestamp_utc: Optional[str] = None,
    completed_at_utc: Optional[str] = None,
    is_retry: bool = False,
    allow_overwrite: bool = False,
) -> Dict[str, Any]:
    """Write experiment outputs conforming to mode boundaries.

    CRITICAL RULES:
    - DEVELOPMENT mode: writes pilot_result_manifest.json. NEVER writes _SUCCESS.json.
      Never claims RESULT_READY or CANDIDATE.
    - OFFICIAL mode: writes all required artifacts, verifies row counts and checksums,
      and writes _SUCCESS.json strictly as the final atomic marker.
    - Idempotent Retry: if the output directory already contains an intact completed bundle
      for the target run, verifies checksums and returns reuse without modifying disk.
    - Immutability Protection: writing into an existing completed bundle directory without
      verified retry or explicit allow_overwrite raises EXP_COMPLETED_BUNDLE_CONFLICT.
    - New Execution Identity: each new execution receives a unique run ID, preventing run ID reuse.
    """
    require(mode in VALID_MODES, f"INVALID_MODE:{mode}")
    require(mode == bundle.get("mode"), f"MODE_MISMATCH_WITH_BUNDLE:{mode}!={bundle.get('mode')}")
    require(algorithm_name in ALGORITHMS, f"INVALID_ALGORITHM:{algorithm_name}")

    out_path = Path(output_dir).resolve()
    out_path.mkdir(parents=True, exist_ok=True)

    source_dataset_id = bundle.get("source_dataset_id", bundle.get("bundle_id"))

    # 1. Strictly validate dataset_id lineage across memberships and summaries before checking retry/reuse
    if isinstance(memberships, list):
        for r in memberships:
            row_ds = r.get("dataset_id")
            require(row_ds == source_dataset_id,
                    f"EXP_DATASET_LINEAGE_MISMATCH:{row_ds}!={source_dataset_id}")
    elif isinstance(memberships, pa.Table):
        require("dataset_id" in memberships.column_names, "EXP_DATASET_LINEAGE_MISMATCH:missing_dataset_id")
        if memberships.num_rows > 0:
            row_dataset_ids = set(memberships["dataset_id"].to_pylist())
            require(row_dataset_ids == {source_dataset_id},
                    f"EXP_DATASET_LINEAGE_MISMATCH:{row_dataset_ids}!={source_dataset_id}")
    else:
        raise TypeError(f"Unsupported memberships type: {type(memberships)}")

    if isinstance(summaries, list):
        for s in summaries:
            row_ds = s.get("dataset_id")
            require(row_ds == source_dataset_id,
                    f"EXP_DATASET_LINEAGE_MISMATCH:{row_ds}!={source_dataset_id}")
    elif isinstance(summaries, pa.Table):
        require("dataset_id" in summaries.column_names, "EXP_DATASET_LINEAGE_MISMATCH:missing_dataset_id")
        if summaries.num_rows > 0:
            row_dataset_ids = set(summaries["dataset_id"].to_pylist())
            require(row_dataset_ids == {source_dataset_id},
                    f"EXP_DATASET_LINEAGE_MISMATCH:{row_dataset_ids}!={source_dataset_id}")
    else:
        raise TypeError(f"Unsupported summaries type: {type(summaries)}")

    # 2. Protect immutable completed bundles & support idempotent retry reuse
    success_marker_file = out_path / ("_SUCCESS.json" if mode == MODE_OFFICIAL else "pilot_result_manifest.json")
    if success_marker_file.is_file():
        is_marker_valid = False
        existing_run_id = None
        existing_ds_id = None
        existing_checksums = None
        try:
            existing_marker = json.loads(success_marker_file.read_text(encoding="utf-8"))
            if isinstance(existing_marker, dict):
                if mode == MODE_OFFICIAL:
                    if (existing_marker.get("status") == "COMPLETED" and
                        existing_marker.get("schema_version") == SCHEMA_VERSION):
                        existing_run_id = existing_marker.get("experiment_run_id")
                        existing_ds_id = existing_marker.get("dataset_id")
                        existing_checksums = existing_marker.get("checksums")
                        is_marker_valid = bool(existing_run_id and existing_ds_id)
                else:
                    if (existing_marker.get("status") == "DEVELOPMENT_RUN_COMPLETED" and
                        existing_marker.get("artifact_kind") == "PILOT_EXPERIMENT_RESULT"):
                        existing_run_id = existing_marker.get("pilot_run_id")
                        existing_ds_id = existing_marker.get("source_dataset_id")
                        existing_checksums = existing_marker.get("checksums")
                        is_marker_valid = bool(existing_run_id and existing_ds_id)
        except Exception:
            is_marker_valid = False

        standard_required_files = [
            "memberships.parquet",
            "sequence_summary.parquet",
            "experiment_metrics.json",
            "experiment_config.json",
            "requirements-lock.txt",
        ]
        is_checksums_valid = False
        sha_hex_pattern = re.compile(r"^[a-f0-9]{64}$")
        if is_marker_valid and isinstance(existing_checksums, dict) and len(existing_checksums) > 0:
            if all(rf in existing_checksums for rf in standard_required_files):
                all_entries_valid = True
                for rel_name, exp_sha in existing_checksums.items():
                    if (not isinstance(rel_name, str) or not isinstance(exp_sha, str) or
                        not rel_name.strip() or
                        ".." in rel_name or rel_name.startswith(("/", "\\")) or
                        not sha_hex_pattern.match(exp_sha)):
                        all_entries_valid = False
                        break
                    try:
                        resolved_f_path = (out_path / rel_name).resolve()
                        if not resolved_f_path.is_relative_to(out_path):
                            all_entries_valid = False
                            break
                    except Exception:
                        all_entries_valid = False
                        break
                is_checksums_valid = all_entries_valid

        all_files_intact = False
        if is_checksums_valid:
            intact = True
            for rel_name, exp_sha in existing_checksums.items():
                f_path = out_path / rel_name
                if not f_path.is_file():
                    intact = False
                    break
                try:
                    if sha256_bytes(f_path.read_bytes()) != exp_sha:
                        intact = False
                        break
                except Exception:
                    intact = False
                    break
            all_files_intact = intact

        config_matches = False
        if all_files_intact:
            try:
                cfg_path = out_path / "experiment_config.json"
                cfg_data = json.loads(cfg_path.read_text(encoding="utf-8"))
                if (cfg_data.get("dataset_id") == source_dataset_id and
                    existing_ds_id == source_dataset_id and
                    cfg_data.get("algorithm_name") == algorithm_name and
                    cfg_data.get("algorithm_version") == algorithm_version and
                    cfg_data.get("model_config_version") == model_config_version and
                    canonical(cfg_data.get("model_config")) == canonical(model_config) and
                    cfg_data.get("random_seed") == random_seed and
                    (code_version is None or cfg_data.get("code_version") == code_version)):
                    config_matches = True
            except Exception:
                config_matches = False

        run_id_matches = (experiment_run_id is None or experiment_run_id == existing_run_id)
        caller_allows_reuse = (is_retry or (experiment_run_id is not None and experiment_run_id == existing_run_id))

        can_reuse = (caller_allows_reuse and run_id_matches and is_marker_valid and
                     is_checksums_valid and all_files_intact and config_matches)

        if can_reuse:
            return {
                "mode": mode,
                "run_id": existing_run_id,
                "success_marker_path": success_marker_file if mode == MODE_OFFICIAL else None,
                "manifest_path": success_marker_file if mode == MODE_DEVELOPMENT else None,
                "has_success_marker": (mode == MODE_OFFICIAL),
                "checksums": existing_checksums,
                "is_reused": True,
            }

        if not allow_overwrite:
            run_label = existing_run_id or "unknown_run"
            require(False, f"EXP_COMPLETED_BUNDLE_CONFLICT: Output directory {out_path} already contains a completed bundle for run '{run_label}' that cannot be reused or overwritten.")

    runtime_meta = get_runtime_metadata(code_version=code_version, random_seed=random_seed, timestamp_utc=timestamp_utc)
    code_ver = code_version or runtime_meta["code_version"]
    pkg_ver = package_version_override or runtime_meta["package_versions"].get("scikit-learn", "1.6.1")

    if experiment_run_id:
        effective_run_id = experiment_run_id
    else:
        # Generate unique run ID for new execution, avoiding collisions across runs with identical config
        exec_token = execution_id or uuid.uuid4().hex[:16]
        run_id_payload = {
            "dataset_id": source_dataset_id,
            "algorithm_name": algorithm_name,
            "algorithm_version": algorithm_version,
            "model_config_version": model_config_version,
            "model_config": model_config,
            "code_version": code_ver,
            "mode": mode,
            "random_seed": random_seed,
            "execution_token": exec_token,
        }
        identity_digest = sha256_bytes(canonical(run_id_payload).encode("utf-8"))[:32]
        effective_run_id = f"pilot_run_{identity_digest}" if mode == MODE_DEVELOPMENT else f"exp_run_{identity_digest}"

    # Synchronize effective_run_id across membership records
    if isinstance(memberships, list):
        for r in memberships:
            r["experiment_run_id"] = effective_run_id
        memberships_table = pa.Table.from_pylist(memberships, schema=MEMBERSHIP_SCHEMA)
    elif isinstance(memberships, pa.Table):
        pydict = memberships.to_pydict()
        pydict["experiment_run_id"] = [effective_run_id] * memberships.num_rows
        memberships_table = pa.Table.from_pydict(pydict, schema=MEMBERSHIP_SCHEMA)
    else:
        raise TypeError(f"Unsupported memberships type: {type(memberships)}")

    # Synchronize effective_run_id across summary records
    if isinstance(summaries, list):
        for s in summaries:
            s["experiment_run_id"] = effective_run_id
        summary_table = pa.Table.from_pylist(summaries, schema=SEQUENCE_SUMMARY_SCHEMA)
    elif isinstance(summaries, pa.Table):
        pydict = summaries.to_pydict()
        pydict["experiment_run_id"] = [effective_run_id] * summaries.num_rows
        summary_table = pa.Table.from_pydict(pydict, schema=SEQUENCE_SUMMARY_SCHEMA)
    else:
        raise TypeError(f"Unsupported summaries type: {type(summaries)}")

    # Reconciliation and integrity checks before writing
    if mode == MODE_OFFICIAL:
        require(memberships_table.num_rows > 0, "EXP_MISSING_MEMBERSHIP_ROWS")
        require(summary_table.num_rows > 0, "EXP_MISSING_SUMMARY_ROWS")
        mem_mids = set(memberships_table["mainshock_event_id"].to_pylist())
        sum_mids = set(summary_table["mainshock_event_id"].to_pylist())
        require(mem_mids == sum_mids, f"EXP_SUMMARY_WINDOW_MISMATCH:{mem_mids}!={sum_mids}")

    # Clean previous _SUCCESS.json if exists to avoid stale markers
    stale_success = out_path / "_SUCCESS.json"
    if stale_success.exists():
        stale_success.unlink()

    # 1. Write Parquet files
    memberships_file = out_path / "memberships.parquet"
    summary_file = out_path / "sequence_summary.parquet"
    pq.write_table(memberships_table, memberships_file)
    pq.write_table(summary_table, summary_file)

    # 2. Write requirements-lock.txt
    lock_file = out_path / "requirements-lock.txt"
    repo_lock = Path(__file__).resolve().parent / "requirements-lock.txt"
    if repo_lock.is_file():
        lock_content = repo_lock.read_text(encoding="utf-8")
    else:
        lock_content = "\n".join(f"{k}=={v}" for k, v in runtime_meta["package_versions"].items()) + "\n"
    lock_file.write_text(lock_content, encoding="utf-8")

    # 3. Write experiment_config.json
    config_file = out_path / "experiment_config.json"
    config_payload = {
        "schema_version": SCHEMA_VERSION,
        "experiment_run_id": effective_run_id,
        "dataset_id": source_dataset_id,
        "algorithm_name": algorithm_name,
        "algorithm_version": algorithm_version,
        "model_config_version": model_config_version,
        "model_config": model_config,
        "package_version": pkg_ver,
        "code_version": code_ver,
        "random_seed": random_seed,
        "mode": mode,
        "runtime_environment": runtime_meta,
        "runtime_environment_json": canonical(runtime_meta),
    }
    config_file.write_text(canonical(config_payload) + "\n", encoding="utf-8")

    # 4. Write experiment_metrics.json
    metrics_file = out_path / "experiment_metrics.json"
    metrics_payload = {
        "schema_version": SCHEMA_VERSION,
        "experiment_run_id": effective_run_id,
        "dataset_id": source_dataset_id,
        "membership_row_count": memberships_table.num_rows,
        "summary_row_count": summary_table.num_rows,
        "algorithm_name": algorithm_name,
        "metrics": metrics,
    }
    metrics_file.write_text(canonical(metrics_payload) + "\n", encoding="utf-8")

    # 5. Write optional model artifacts if provided (flat files only)
    if optional_model_artifacts:
        opt_dir = out_path / "optional-model-artifacts"
        opt_dir.mkdir(exist_ok=True)
        for art_name, art_bytes in optional_model_artifacts.items():
            require("/" not in art_name and "\\" not in art_name and ".." not in art_name,
                    f"EXP_UNSAFE_MODEL_ARTIFACT_NAME:{art_name}")
            (opt_dir / art_name).write_bytes(art_bytes)

    # 6. Compute checksums of all non-marker files
    checksums: Dict[str, str] = {}
    standard_files = ["memberships.parquet", "sequence_summary.parquet", "experiment_metrics.json",
                      "experiment_config.json", "requirements-lock.txt"]
    for sf in standard_files:
        checksums[sf] = sha256_bytes((out_path / sf).read_bytes())

    if optional_model_artifacts:
        for art_name in sorted(optional_model_artifacts.keys()):
            rel_path = f"optional-model-artifacts/{art_name}"
            checksums[rel_path] = sha256_bytes((out_path / rel_path).read_bytes())

    completed_at = completed_at_utc or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    if mode == MODE_DEVELOPMENT:
        # Write checksums.sha256 file for development pilot
        checksums_txt = "".join(f"{checksums[k]}  {k}\n" for k in sorted(checksums.keys()))
        (out_path / "checksums.sha256").write_text(checksums_txt, encoding="utf-8")

        # DEVELOPMENT PILOT: MUST NOT write _SUCCESS.json
        manifest_file = out_path / "pilot_result_manifest.json"
        pilot_manifest = {
            "manifest_version": "dat-02-pilot-result-v1",
            "artifact_kind": "PILOT_EXPERIMENT_RESULT",
            "allowed_usage": "DEVELOPMENT_ONLY",
            "research_ready": False,
            "pilot_run_id": effective_run_id,
            "pilot_id": bundle.get("bundle_id"),
            "source_dataset_id": source_dataset_id,
            "algorithm_name": algorithm_name,
            "algorithm_version": algorithm_version,
            "model_config_version": model_config_version,
            "membership_row_count": memberships_table.num_rows,
            "summary_row_count": summary_table.num_rows,
            "status": "DEVELOPMENT_RUN_COMPLETED",
            "completed_at_utc": completed_at,
            "checksums": checksums,
            "code_version": code_ver,
        }
        manifest_file.write_text(canonical(pilot_manifest) + "\n", encoding="utf-8")

        # Double check: ensure _SUCCESS.json was definitely NOT written
        require(not (out_path / "_SUCCESS.json").exists(), "EXP_DEVELOPMENT_CANNOT_WRITE_SUCCESS_MARKER")

        return {
            "mode": MODE_DEVELOPMENT,
            "run_id": effective_run_id,
            "manifest_path": manifest_file,
            "has_success_marker": False,
            "checksums": checksums,
        }

    else:  # mode == MODE_OFFICIAL
        # In OFFICIAL mode, checksums.sha256 MUST NOT exist as a separate file
        # because MlResultBundleReader rejects unexpected artifacts (IMP_UNSAFE_ARTIFACT_METADATA).
        # Checksums are recorded inside _SUCCESS.json.
        stale_chk = out_path / "checksums.sha256"
        if stale_chk.exists():
            stale_chk.unlink()

        # Read-after-write verification
        read_mem = pq.read_table(memberships_file)
        read_sum = pq.read_table(summary_file)
        require(read_mem.num_rows == memberships_table.num_rows, "EXP_READBACK_MEMBERSHIP_ROW_MISMATCH")
        require(read_sum.num_rows == summary_table.num_rows, "EXP_READBACK_SUMMARY_ROW_MISMATCH")
        for sf in standard_files:
            actual_sha = sha256_bytes((out_path / sf).read_bytes())
            require(actual_sha == checksums[sf], f"EXP_READBACK_CHECKSUM_MISMATCH:{sf}")

        # OFFICIAL RUN: write _SUCCESS.json as final atomic marker
        artifact_uri = f"s3://lakehouse/ml-experiments/experiment-run-{effective_run_id}"
        success_marker = {
            "schema_version": SCHEMA_VERSION,
            "experiment_run_id": effective_run_id,
            "dataset_id": source_dataset_id,
            "algorithm_name": algorithm_name,
            "algorithm_version": algorithm_version,
            "model_config_version": model_config_version,
            "package_version": pkg_ver,
            "membership_row_count": memberships_table.num_rows,
            "summary_row_count": summary_table.num_rows,
            "status": "COMPLETED",
            "completed_at_utc": completed_at,
            "artifact_uri": artifact_uri,
            "checksums": checksums,
            "code_version": code_ver,
        }
        validate_safe_metadata(success_marker)
        stale_success.write_text(canonical(success_marker) + "\n", encoding="utf-8")

        return {
            "mode": MODE_OFFICIAL,
            "run_id": effective_run_id,
            "success_marker_path": stale_success,
            "has_success_marker": True,
            "checksums": checksums,
        }


# -----------------------------------------------------------------------------
# Colab Path Adapter with Local Smoke Gate Enforcement
# -----------------------------------------------------------------------------
class ColabPathAdapter:
    """Adapter for parameterized path resolution across local and Colab environments.

    Enforces that Colab migration is blocked until local smoke status is PASSED.
    Guarantees no credentials or hardcoded home directory paths are introduced.
    """

    @staticmethod
    def verify_local_smoke_gate(smoke_report_path: Union[str, Path]) -> Dict[str, Any]:
        """Verify that local development smoke report exists and passed before Colab migration."""
        report_file = Path(smoke_report_path).resolve()
        require(report_file.is_file(), f"COLAB_GATE_BLOCKED: Missing local smoke report at {smoke_report_path}")
        report = read_json_safe(report_file.read_bytes())

        status = report.get("local_smoke_status", report.get("notebook_status"))
        allowed = report.get("colab_migration_allowed", True)
        fresh_kernel = report.get("fresh_kernel_acceptance")

        is_passed = (status in {"PASSED", "RESTART_RUN_ALL_PASSED"}) and bool(allowed)
        require(is_passed, f"COLAB_GATE_BLOCKED: Local smoke status is {status}, migration allowed={allowed}")
        require(fresh_kernel is not False,
                f"COLAB_GATE_BLOCKED: Fresh kernel acceptance failed (fresh_kernel_acceptance={fresh_kernel})")

        # If this is an EXP-02 local smoke report, enforce mandatory algorithm coverage and non-empty reconciliation
        report_ver = str(report.get("report_version", ""))
        if "local-smoke" in report_ver or "algorithms_evaluated" in report:
            require(fresh_kernel is True,
                    f"COLAB_GATE_BLOCKED: Fresh kernel acceptance is required for Colab migration (fresh_kernel_acceptance={fresh_kernel})")
            bundle_cs = report.get("bundle_checksum")
            require(isinstance(bundle_cs, str) and len(bundle_cs) == 64,
                    f"COLAB_GATE_BLOCKED: bundle_checksum is missing, null, or invalid in local smoke report: {bundle_cs}")

            algos = report.get("algorithms_evaluated")
            require(isinstance(algos, list),
                    "COLAB_GATE_BLOCKED: Report missing evaluated algorithms list")
            required_algos = {"WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"}
            require(required_algos.issubset(set(algos)),
                    f"COLAB_GATE_BLOCKED: Report missing required algorithms: {sorted(required_algos - set(algos))}")

            reconciliation = report.get("reconciliation")
            require(isinstance(reconciliation, dict) and len(reconciliation) >= 3,
                    "COLAB_GATE_BLOCKED: Missing or incomplete reconciliation in local smoke report")
            for req_algo in required_algos:
                require(req_algo in reconciliation, f"COLAB_GATE_BLOCKED: Missing reconciliation for {req_algo}")
                metrics = reconciliation[req_algo]
                require(isinstance(metrics, dict), f"COLAB_GATE_BLOCKED: Invalid metrics structure for {req_algo}")
                require(metrics.get("total_candidates", 0) > 0, f"COLAB_GATE_BLOCKED: Zero candidates in {req_algo}")

        return report

    @staticmethod
    def resolve_paths(
        input_bundle: Optional[Union[str, Path]] = None,
        output_dir: Optional[Union[str, Path]] = None,
        drive_mount: Optional[Union[str, Path]] = None,
        is_colab: bool = False,
    ) -> Tuple[Path, Path]:
        """Resolve bundle input and output directories with parameter support."""
        # Check for credential patterns and unsafe paths on raw inputs first
        for param in [input_bundle, output_dir, drive_mount]:
            if param is not None:
                validate_safe_metadata(str(param))

        if is_colab:
            base_drive = Path(drive_mount) if drive_mount else Path("/content/drive/MyDrive")
            resolved_input = base_drive / (input_bundle if input_bundle else "ml-pilot")
            resolved_output = base_drive / (output_dir if output_dir else "ml-experiment-output")
        else:
            resolved_input = Path(input_bundle if input_bundle else "staging/ml-pilot")
            resolved_output = Path(output_dir if output_dir else "staging/ml-experiment-output")

        # Check resolved paths as well
        validate_safe_metadata(str(resolved_input))
        validate_safe_metadata(str(resolved_output))
        return resolved_input, resolved_output


# -----------------------------------------------------------------------------
# CLI Entrypoint for Inspection and Smoke Runs
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path, help="Path to feature bundle directory")
    parser.add_argument("--mode", choices=[MODE_DEVELOPMENT, MODE_OFFICIAL], default=MODE_DEVELOPMENT)
    args = parser.parse_args()

    loaded = load_feature_bundle(args.bundle, mode=args.mode)
    print(f"SUCCESS: Loaded {args.mode} bundle '{loaded['bundle_id']}' with {loaded['features_table'].num_rows} features.")
