"""EXP-05: Out-of-Period Temporal Generalization & Frozen Rule Extension Evaluation.

Enforces fail-closed validation against the frozen EXP-04 REPRODUCTION rule
(`exp04-frozen-reproduction-rule-v2`), executes all four frozen algorithms
(`WINDOW`, `DBSCAN`, `HDBSCAN_GLOBAL`, `HDBSCAN_ADAPTIVE`) without parameter
tuning on an EXPORTED `EXTENSION` collection, verifies sibling MLI-03 physical
bundles and primary membership digests, and builds comparative Reproduction vs
Extension diagnostics without accuracy/F1 or earthquake-prediction claims.
"""
from __future__ import annotations

import gc
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pyarrow.parquet as pq

NOTEBOOKS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = NOTEBOOKS_DIR.parent
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(NOTEBOOKS_DIR))

from adaptive_hdbscan import (  # noqa: E402
    generate_official_adaptive_report,
    run_adaptive_experiment,
)
from baseline_models import run_baseline_experiment  # noqa: E402
from evaluation import (  # noqa: E402
    DEFAULT_MODEL_CONFIG_PATHS,
    METRIC_FIT_FAILED,
    METRIC_INSUFFICIENT_MEMBERS,
    METRIC_NOT_APPLICABLE,
    MIN_OMORI_AFTERSHOCKS,
    compare_all_models_on_official,
    compute_primary_membership_digest,
    compute_primary_summary_digest,
    load_algorithm_configs,
)
from experiment_harness import (  # noqa: E402
    MODE_OFFICIAL,
    canonical,
    load_feature_bundle,
    read_json_safe,
    require,
    sha256_bytes,
    validate_safe_metadata,
)

FROZEN_EXTENSION_RULE_VERSION = "exp04-frozen-reproduction-rule-v2"

ALGORITHM_ORDER: Tuple[str, ...] = (
    "WINDOW",
    "DBSCAN",
    "HDBSCAN_GLOBAL",
    "HDBSCAN_ADAPTIVE",
)

ALGO_OUTPUT_SUBDIRS: Dict[str, str] = {
    "WINDOW": "window",
    "DBSCAN": "dbscan",
    "HDBSCAN_GLOBAL": "hdbscan_global",
    "HDBSCAN_ADAPTIVE": "hdbscan_adaptive",
}

FORBIDDEN_SUPERVISED_METRIC_KEYS = frozenset({
    "accuracy",
    "acc",
    "f1",
    "f1_score",
    "precision",
    "recall",
    "roc_auc",
    "auc",
})

STANDARD_BUNDLE_FILES: Tuple[str, ...] = (
    "memberships.parquet",
    "sequence_summary.parquet",
    "experiment_config.json",
    "requirements-lock.txt",
)


def assert_no_forbidden_metrics(node: Any, path: str = "$") -> None:
    """Reject any accidental supervised classification metric key in output payloads."""
    if isinstance(node, dict):
        for k, v in node.items():
            key_norm = str(k).strip().lower()
            if key_norm in FORBIDDEN_SUPERVISED_METRIC_KEYS:
                raise ValueError(f"EXP05_FORBIDDEN_SUPERVISED_METRIC:{path}.{k}")
            assert_no_forbidden_metrics(v, f"{path}.{k}")
    elif isinstance(node, list):
        for idx, item in enumerate(node):
            assert_no_forbidden_metrics(item, f"{path}[{idx}]")


def _safe_float(val: Any) -> Optional[float]:
    if val is None or isinstance(val, bool):
        return None
    try:
        out = float(val)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _compute_delta(ext_val: Any, repro_val: Any, ndigits: int = 4) -> Optional[float]:
    e = _safe_float(ext_val)
    r = _safe_float(repro_val)
    if e is None or r is None:
        return None
    return round(e - r, ndigits)


def _format_utc_iso(val: Any) -> Optional[str]:
    if val is None:
        return None
    if hasattr(val, "as_py"):
        val = val.as_py()
    if hasattr(val, "isoformat"):
        text = val.isoformat()
        if text.endswith("+00:00"):
            text = text[:-6] + "Z"
        return text
    return str(val)


def _compute_count_distribution(counts: List[int]) -> Dict[str, Any]:
    if not counts:
        return {
            "window_count": 0,
            "min": None,
            "p25": None,
            "median": None,
            "mean": None,
            "p95": None,
            "max": None,
            "windows_gt_10k": 0,
            "windows_gt_50k": 0,
        }
    arr = np.asarray(counts, dtype=float)
    return {
        "window_count": int(len(counts)),
        "min": int(np.min(arr)),
        "p25": round(float(np.percentile(arr, 25)), 2),
        "median": round(float(np.median(arr)), 2),
        "mean": round(float(np.mean(arr)), 2),
        "p95": round(float(np.percentile(arr, 95)), 2),
        "max": int(np.max(arr)),
        "windows_gt_10k": int(sum(1 for c in counts if c > 10000)),
        "windows_gt_50k": int(sum(1 for c in counts if c > 50000)),
    }


def _extract_mc_value(manifest: Dict[str, Any]) -> float:
    mc = manifest.get("mc_value")
    if mc is None:
        mc = (
            manifest.get("collection_plan", {})
            .get("global_mc", {})
            .get("mc_value")
        )
    require(
        mc is not None and not isinstance(mc, bool),
        f"EXP05_INVALID_MC_VALUE:{mc}",
    )
    mc_f = _safe_float(mc)
    require(mc_f is not None and mc_f > 0.0, f"EXP05_INVALID_MC_VALUE:{mc}")
    return float(mc_f)


def compute_split_distribution_profile(bundle: Dict[str, Any]) -> Dict[str, Any]:
    """Compute observed candidate-window and mainshock statistics for a loaded collection bundle."""
    manifest = bundle["manifest"]
    coverage = bundle.get("coverage") or manifest.get("collection_plan", {}).get("coverage") or {}
    mainshocks = bundle["mainshocks_table"].to_pylist()
    counts = [int(row["candidate_count"]) for row in mainshocks]
    mags = [float(row["magnitude"]) for row in mainshocks if row.get("magnitude") is not None]
    times = [t for t in (_format_utc_iso(row.get("event_time_utc")) for row in mainshocks) if t is not None]

    mc_val = _extract_mc_value(manifest)
    return {
        "dataset_split": manifest.get("dataset_split"),
        "dataset_id": manifest.get("dataset_id"),
        "collection_id": bundle.get("collection_id") or manifest.get("collection_id"),
        "shard_count": len(bundle.get("shards") or {}),
        "mainshock_count": int(manifest["mainshock_count"]),
        "feature_row_count": int(manifest["feature_row_count"]),
        "mc_value": mc_val,
        "window_model_version": manifest.get("window_model_version"),
        "scaling_config_version": manifest.get("scaling_config_version"),
        "feature_version": manifest.get("feature_version"),
        "gold_publication_id": manifest.get("gold_publication_id"),
        "gold_snapshot_id": (
            str(manifest["gold_snapshot_id"])
            if manifest.get("gold_snapshot_id") is not None
            else None
        ),
        "period_start_utc": manifest.get("period_start_utc"),
        "period_end_utc_exclusive": manifest.get("period_end_utc_exclusive"),
        "required_month_count": coverage.get("required_month_count"),
        "coverage_status": coverage.get("status"),
        "observed_first_mainshock_utc": min(times) if times else None,
        "observed_last_mainshock_utc": max(times) if times else None,
        "mainshock_magnitude_stats": {
            "min": round(float(min(mags)), 2) if mags else None,
            "median": round(float(np.median(mags)), 2) if mags else None,
            "mean": round(float(np.mean(mags)), 2) if mags else None,
            "max": round(float(max(mags)), 2) if mags else None,
        },
        "window_candidate_distribution": _compute_count_distribution(counts),
    }


def extract_reproduction_distribution_profile(exp04_report: Dict[str, Any]) -> Dict[str, Any]:
    """Extract comparable baseline split profile from verified EXP-04 REPRODUCTION evidence."""
    ds = exp04_report["dataset"]
    details = exp04_report["evaluation_summary"]["algorithm_details"]
    ref_algo = "WINDOW" if "WINDOW" in details else next(iter(details))
    win_evals = details[ref_algo].get("window_evaluations") or []
    counts = [int(w["candidate_count"]) for w in win_evals]
    require(
        len(counts) == int(ds["mainshock_count"]),
        f"EXP05_REPRO_WINDOW_COUNT_MISMATCH:{len(counts)}!={ds['mainshock_count']}",
    )
    return {
        "dataset_split": ds.get("dataset_split"),
        "dataset_id": ds.get("dataset_id"),
        "collection_id": ds.get("collection_id"),
        "shard_count": int(ds["shard_count"]),
        "mainshock_count": int(ds["mainshock_count"]),
        "feature_row_count": int(ds["feature_row_count"]),
        "mc_value": _safe_float(ds.get("mc_value")),
        "window_model_version": ds.get("window_model_version"),
        "scaling_config_version": ds.get("scaling_config_version"),
        "feature_version": ds.get("feature_version"),
        "gold_publication_id": ds.get("gold_publication_id"),
        "gold_snapshot_id": (
            str(ds["gold_snapshot_id"]) if ds.get("gold_snapshot_id") is not None else None
        ),
        "window_candidate_distribution": _compute_count_distribution(counts),
    }


def _config_bytes_match_pin(
    config_file: Path,
    live_file_sha256: str,
    locked_file_sha256: str,
    *,
    allow_lf_crlf_normalization: bool,
) -> bool:
    """Compare raw file SHA-256 while tolerating deterministic git CRLF/LF checkout conversion."""
    if live_file_sha256 == locked_file_sha256:
        return True
    if not allow_lf_crlf_normalization:
        return False
    raw = config_file.read_bytes()
    lf_bytes = raw.replace(b"\r\n", b"\n")
    if hashlib.sha256(lf_bytes).hexdigest() == locked_file_sha256:
        return True
    crlf_bytes = lf_bytes.replace(b"\n", b"\r\n")
    return hashlib.sha256(crlf_bytes).hexdigest() == locked_file_sha256


def _is_sha256_hex(val: Any) -> bool:
    return isinstance(val, str) and re.fullmatch(r"[a-f0-9]{64}", val.strip()) is not None


def _verify_exp04_upstream_cross_checks(exp04_report: Dict[str, Any]) -> None:
    """Enforce fail-closed EXP-02 and EXP-03 upstream cross-verification blocks inside EXP-04 report."""
    eval_summary = exp04_report.get("evaluation_summary")
    require(isinstance(eval_summary, dict), "EXP05_EXP04_EVAL_SUMMARY_MISSING")
    eval_details = eval_summary.get("algorithm_details")
    require(isinstance(eval_details, dict), "EXP05_EXP04_ALGORITHM_DETAILS_MISSING")

    exp02 = exp04_report.get("exp02_cross_verification")
    require(
        isinstance(exp02, dict) and bool(exp02),
        "EXP05_EXP02_CROSS_VERIFY_REQUIRED: EXP-04 report missing exp02_cross_verification",
    )
    require(
        _is_sha256_hex(exp02.get("report_sha256")) and exp02.get("report_sha256_verified") is True,
        "EXP05_EXP02_CROSS_VERIFY_FAILED:report_sha256",
    )
    require(
        exp02.get("upstream_status") == "PASSED"
        and exp02.get("upstream_evaluation_scope") == "OFFICIAL_RESEARCH"
        and exp02.get("upstream_scientific_approval") is False,
        "EXP05_EXP02_CROSS_VERIFY_FAILED:upstream_status_or_scope",
    )
    for flag in (
        "dataset_and_pins_verified",
        "config_pins_verified",
        "physical_bundles_verified",
        "physical_shards_verified",
        "primary_membership_parity_verified",
        "all_algos_exact_match",
        "all_checks_passed",
    ):
        require(exp02.get(flag) is True, f"EXP05_EXP02_CROSS_VERIFY_FAILED:{flag}={exp02.get(flag)}")

    exp02_algos = exp02.get("algorithms")
    require(isinstance(exp02_algos, dict), "EXP05_EXP02_CROSS_VERIFY_FAILED:algorithms")
    for algo in ("WINDOW", "DBSCAN", "HDBSCAN_GLOBAL"):
        entry = exp02_algos.get(algo)
        det = eval_details.get(algo)
        require(
            isinstance(entry, dict) and isinstance(det, dict),
            f"EXP05_EXP02_CROSS_VERIFY_FAILED:missing_{algo}",
        )
        for aflag in (
            "matches",
            "physical_bundle_verified",
            "physical_shards_verified",
            "primary_membership_parity_verified",
        ):
            require(
                entry.get(aflag) is True,
                f"EXP05_EXP02_CROSS_VERIFY_FAILED:{algo}:{aflag}={entry.get(aflag)}",
            )
        require(
            all(_is_sha256_hex(entry.get(key)) and entry[key] == det.get(key)
                for key in ("primary_membership_sha256", "primary_window_summary_sha256"))
            and entry.get("exp04_aftershocks") == entry.get("exp02_aftershocks") == det.get("total_aftershocks"),
            f"EXP05_EXP02_CROSS_VERIFY_FAILED:{algo}:digest_or_aftershock_mismatch",
        )

    exp03 = exp04_report.get("exp03_cross_verification")
    require(
        isinstance(exp03, dict) and bool(exp03),
        "EXP05_EXP03_CROSS_VERIFY_REQUIRED: EXP-04 report missing exp03_cross_verification",
    )
    require(
        _is_sha256_hex(exp03.get("report_sha256")) and exp03.get("report_sha256_verified") is True,
        "EXP05_EXP03_CROSS_VERIFY_FAILED:report_sha256",
    )
    require(
        exp03.get("upstream_status") == "PASSED"
        and exp03.get("upstream_evaluation_scope") == "OFFICIAL_RESEARCH"
        and exp03.get("upstream_scientific_approval") is False,
        "EXP05_EXP03_CROSS_VERIFY_FAILED:upstream_status_or_scope",
    )
    for flag in (
        "dataset_and_pins_verified",
        "config_pins_verified",
        "physical_bundles_verified",
        "physical_shards_verified",
        "primary_membership_parity_verified",
        "exact_match",
        "all_checks_passed",
    ):
        require(exp03.get(flag) is True, f"EXP05_EXP03_CROSS_VERIFY_FAILED:{flag}={exp03.get(flag)}")

    adapt_det = eval_details.get("HDBSCAN_ADAPTIVE")
    require(isinstance(adapt_det, dict), "EXP05_EXP03_CROSS_VERIFY_FAILED:missing_HDBSCAN_ADAPTIVE")
    require(
        all(_is_sha256_hex(exp03.get(key)) and exp03[key] == adapt_det.get(key)
            for key in ("primary_membership_sha256", "primary_window_summary_sha256"))
        and isinstance(exp03.get("multi_sequence_reconciliation"), dict),
        "EXP05_EXP03_CROSS_VERIFY_FAILED:digest_or_reconciliation_mismatch",
    )


def verify_reproduction_freeze_gate(
    exp04_report_path: Union[str, Path],
    *,
    expected_exp04_report_sha256: Optional[str],
    extension_bundle: Dict[str, Any],
    project_root: Optional[Union[str, Path]] = None,
    custom_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    repeats: int = 5,
    sigma: float = 0.01,
    seed: int = 42,
    min_omori_aftershocks: int = MIN_OMORI_AFTERSHOCKS,
    allow_lf_crlf_config_normalization: bool = True,
) -> Dict[str, Any]:
    """Fail-closed Reproduction freeze gate blocking overrides, tuning, or non-EXTENSION inputs."""
    require(
        custom_configs is None,
        "EXP05_CONFIG_OVERRIDE_FORBIDDEN: Custom model configs or parameter tuning on EXTENSION are forbidden",
    )
    require(
        isinstance(expected_exp04_report_sha256, str)
        and re.fullmatch(r"[a-f0-9]{64}", expected_exp04_report_sha256.strip()) is not None,
        "EXP05_EXP04_TRUSTED_SHA_REQUIRED",
    )
    trusted_sha = expected_exp04_report_sha256.strip()

    report_path = Path(exp04_report_path).resolve()
    require(
        report_path.is_file() and not report_path.is_symlink(),
        f"EXP05_EXP04_REPORT_NOT_FOUND:{exp04_report_path}",
    )
    raw_bytes = report_path.read_bytes()
    actual_sha = sha256_bytes(raw_bytes)
    require(
        actual_sha == trusted_sha,
        f"EXP05_EXP04_REPORT_SHA_MISMATCH:{actual_sha}!={trusted_sha}",
    )
    normalized_lf_sha = sha256_bytes(raw_bytes.replace(b"\r\n", b"\n"))

    exp04_report = read_json_safe(raw_bytes)
    canonical_report_sha = sha256_bytes(canonical(exp04_report).encode("utf-8"))

    require(exp04_report.get("task_id") == "EXP-04", f"EXP05_EXP04_GATE_FAILED:task_id={exp04_report.get('task_id')}")
    require(exp04_report.get("status") == "PASSED", f"EXP05_EXP04_GATE_FAILED:status={exp04_report.get('status')}")
    require(
        exp04_report.get("evaluation_scope") == "OFFICIAL_RESEARCH",
        f"EXP05_EXP04_GATE_FAILED:scope={exp04_report.get('evaluation_scope')}",
    )
    require(
        exp04_report.get("scientific_approval") is False,
        f"EXP05_EXP04_GATE_FAILED:scientific_approval={exp04_report.get('scientific_approval')}",
    )
    _verify_exp04_upstream_cross_checks(exp04_report)
    require(exp04_report.get("frozen_rule_published") is True, "EXP05_FROZEN_RULE_NOT_PUBLISHED")

    frozen_rule = exp04_report.get("frozen_extension_rule")
    require(isinstance(frozen_rule, dict), "EXP05_FROZEN_RULE_MISSING")
    require(
        frozen_rule.get("rule_version") == FROZEN_EXTENSION_RULE_VERSION,
        f"EXP05_FROZEN_RULE_VERSION_MISMATCH:{frozen_rule.get('rule_version')}!={FROZEN_EXTENSION_RULE_VERSION}",
    )
    require(
        frozen_rule.get("frozen_from_split") == "REPRODUCTION",
        f"EXP05_FROZEN_RULE_INVALID_SPLIT:{frozen_rule.get('frozen_from_split')}",
    )
    frozen_from_dataset_id = frozen_rule.get("frozen_from_dataset_id")
    frozen_from_collection_id = frozen_rule.get("frozen_from_collection_id")
    require(bool(frozen_from_dataset_id) and bool(frozen_from_collection_id), "EXP05_FROZEN_LINEAGE_MISSING")

    guardrails = frozen_rule.get("extension_guardrails") or {}
    required_true_guardrails = (
        "no_parameter_override_on_extension",
        "no_accuracy_or_f1_metric",
        "preserve_failed_windows_in_denominator",
        "preserve_honest_nulls_with_reason_codes",
        "retrospective_candidate_only",
    )
    for g_key in required_true_guardrails:
        require(guardrails.get(g_key) is True, f"EXP05_FROZEN_GUARDRAILS_INVALID:{g_key}={guardrails.get(g_key)}")
    require(
        guardrails.get("scientific_approval") is False,
        f"EXP05_FROZEN_GUARDRAILS_INVALID:scientific_approval={guardrails.get('scientific_approval')}",
    )

    # Verify frozen evaluation parameters (strictly no override on stability or Omori settings)
    eval_params = exp04_report.get("evaluation_parameters") or {}
    require(
        not isinstance(repeats, bool)
        and eval_params.get("stability_repeats") == int(repeats)
        and not isinstance(sigma, bool)
        and math.isclose(float(eval_params.get("perturbation_sigma", -1.0)), float(sigma), rel_tol=0.0, abs_tol=1e-12)
        and not isinstance(seed, bool)
        and eval_params.get("random_seed") == int(seed)
        and type(min_omori_aftershocks) is int
        and type(eval_params.get("min_omori_aftershocks")) is int
        and eval_params["min_omori_aftershocks"] == min_omori_aftershocks,
        "EXP05_EVAL_PARAM_OVERRIDE_FORBIDDEN: Evaluation parameters must match frozen EXP-04 settings",
    )
    require(
        min_omori_aftershocks == MIN_OMORI_AFTERSHOCKS,
        f"EXP05_UNSUPPORTED_MIN_OMORI_AFTERSHOCKS: v1 evaluator only supports min_omori_aftershocks={MIN_OMORI_AFTERSHOCKS} (got {min_omori_aftershocks})",
    )

    # Verify live algorithm configs strictly match EXP-04 frozen pins
    root = Path(project_root).resolve() if project_root is not None else PROJECT_ROOT
    live_configs, live_pins = load_algorithm_configs(project_root=root)
    locked_configs = frozen_rule.get("locked_algorithm_configs") or {}
    for algo in ALGORITHM_ORDER:
        locked = locked_configs.get(algo)
        live = live_pins.get(algo)
        require(isinstance(locked, dict) and isinstance(live, dict), f"EXP05_CONFIG_FREEZE_MISSING:{algo}")
        require(
            live.get("model_config_version") == locked.get("model_config_version")
            and live.get("algorithm_version") == locked.get("algorithm_version")
            and live.get("canonical_config_sha256") == locked.get("canonical_config_sha256"),
            f"EXP05_CONFIG_FREEZE_MISMATCH:{algo}:canonical_or_version",
        )
        cfg_file = root / DEFAULT_MODEL_CONFIG_PATHS[algo]
        require(
            _config_bytes_match_pin(
                cfg_file,
                str(live.get("file_bytes_sha256")),
                str(locked.get("file_bytes_sha256")),
                allow_lf_crlf_normalization=allow_lf_crlf_config_normalization,
            ),
            f"EXP05_CONFIG_FREEZE_MISMATCH:{algo}:file_bytes_sha256",
        )

    # Verify EXTENSION EXPORTED split & feature contract integrity
    require(
        extension_bundle.get("mode") == MODE_OFFICIAL
        and extension_bundle.get("is_official") is True
        and extension_bundle.get("is_collection") is True,
        "EXP05_REQUIRES_OFFICIAL_COLLECTION",
    )
    ext_manifest = extension_bundle.get("manifest") or {}
    require(
        ext_manifest.get("dataset_status") == "EXPORTED",
        f"EXP05_REQUIRES_EXPORTED_SPLIT:{ext_manifest.get('dataset_status')}",
    )
    require(
        ext_manifest.get("dataset_split") == "EXTENSION" and extension_bundle.get("dataset_split") == "EXTENSION",
        f"EXP05_REQUIRES_EXTENSION_SPLIT:{ext_manifest.get('dataset_split')}",
    )
    ext_dataset_id = ext_manifest.get("dataset_id")
    ext_collection_id = extension_bundle.get("collection_id") or ext_manifest.get("collection_id")
    require(
        bool(ext_dataset_id)
        and bool(ext_collection_id)
        and ext_dataset_id != frozen_from_dataset_id
        and ext_collection_id != frozen_from_collection_id,
        "EXP05_REQUIRES_DISTINCT_DATASET_ID: EXTENSION must be distinct from frozen REPRODUCTION split",
    )

    locked_contract = frozen_rule.get("locked_feature_contract") or {}
    for contract_key in ("window_model_version", "scaling_config_version", "feature_version"):
        require(
            ext_manifest.get(contract_key) == locked_contract.get(contract_key),
            f"EXP05_FEATURE_CONTRACT_MISMATCH:{contract_key}:{ext_manifest.get(contract_key)}!={locked_contract.get(contract_key)}",
        )

    gold_pin_verified = False
    frozen_pub = frozen_rule.get("gold_publication_id")
    ext_pub = ext_manifest.get("gold_publication_id")
    frozen_snap = frozen_rule.get("gold_snapshot_id")
    ext_snap = ext_manifest.get("gold_snapshot_id")
    if frozen_pub is not None or ext_pub is not None:
        require(
            frozen_pub == ext_pub and str(frozen_snap) == str(ext_snap),
            f"EXP05_GOLD_PUBLICATION_MISMATCH:{ext_pub}/{ext_snap}!={frozen_pub}/{frozen_snap}",
        )
        gold_pin_verified = True

    ext_mc = _extract_mc_value(ext_manifest)
    frozen_mc = _safe_float(locked_contract.get("mc_value"))
    mc_drift = round(ext_mc - frozen_mc, 6) if frozen_mc is not None else None

    return {
        "gate_status": "PASSED",
        "exp04_report_path": str(report_path),
        "exp04_report_sha256": actual_sha,
        "exp04_report_normalized_lf_sha256": normalized_lf_sha,
        "exp04_report_canonical_sha256": canonical_report_sha,
        "frozen_rule_version": frozen_rule["rule_version"],
        "frozen_from_split": frozen_rule["frozen_from_split"],
        "frozen_from_dataset_id": frozen_from_dataset_id,
        "frozen_from_collection_id": frozen_from_collection_id,
        "extension_dataset_id": ext_dataset_id,
        "extension_collection_id": ext_collection_id,
        "gold_pin_verified": gold_pin_verified,
        "parameter_tuning_applied_on_extension": False,
        "config_freeze_verified": True,
        "evaluation_param_freeze_verified": True,
        "feature_contract_freeze_verified": True,
        "exp02_cross_verify_passed": True,
        "exp03_cross_verify_passed": True,
        "reproduction_mc_value": frozen_mc,
        "extension_mc_value": ext_mc,
        "mc_drift": mc_drift,
        "exp04_report": exp04_report,
        "frozen_rule": frozen_rule,
        "configs": live_configs,
        "config_pins": live_pins,
        "evaluation_parameters": dict(eval_params),
    }


def verify_official_result_bundle(
    bundle_dir: Path,
    *,
    expected_dataset_id: str,
    expected_algorithm_name: str,
    expected_model_config_version: str,
    expected_mainshock_count: int,
    expected_feature_row_count: int,
    expected_code_version: Optional[str] = None,
    expected_shard_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Read back an MLI-03 official bundle and verify checksums, grain, and primary digests."""
    root = Path(bundle_dir).resolve()
    require(root.is_dir(), f"EXP05_BUNDLE_DIR_MISSING:{bundle_dir}")
    require(
        not (root / "checksums.sha256").exists(),
        f"IMP_UNSAFE_ARTIFACT_METADATA_FORBIDDEN:{root / 'checksums.sha256'}",
    )

    success_path = root / "_SUCCESS.json"
    require(success_path.is_file(), f"EXP05_SUCCESS_MARKER_MISSING:{success_path}")
    success_bytes = success_path.read_bytes()
    success_sha = sha256_bytes(success_bytes)
    success = read_json_safe(success_bytes)

    require(success.get("status") == "COMPLETED", f"EXP05_BUNDLE_NOT_COMPLETED:{success.get('status')}")
    require(success.get("dataset_id") == expected_dataset_id, "EXP05_BUNDLE_DATASET_MISMATCH")
    require(success.get("algorithm_name") == expected_algorithm_name, "EXP05_BUNDLE_ALGO_MISMATCH")
    require(success.get("model_config_version") == expected_model_config_version, "EXP05_BUNDLE_CONFIG_VER_MISMATCH")
    if expected_code_version:
        require(success.get("code_version") == expected_code_version, "EXP05_BUNDLE_CODE_VERSION_MISMATCH")

    checksums = success.get("checksums") or {}
    for fname in STANDARD_BUNDLE_FILES:
        fpath = root / fname
        require(fpath.is_file(), f"EXP05_BUNDLE_FILE_MISSING:{fpath}")
        require(
            sha256_bytes(fpath.read_bytes()) == checksums.get(fname),
            f"EXP05_BUNDLE_CHECKSUM_MISMATCH:{fname}",
        )

    exp_cfg = read_json_safe((root / "experiment_config.json").read_bytes())
    require(
        exp_cfg.get("experiment_run_id") == success.get("experiment_run_id"),
        "EXP05_BUNDLE_RUN_ID_MISMATCH",
    )
    if expected_shard_id is not None:
        scope = exp_cfg.get("collection_scope") or {}
        require(scope.get("shard_id") == expected_shard_id, "EXP05_BUNDLE_SHARD_SCOPE_MISMATCH")

    mem_table = pq.read_table(root / "memberships.parquet")
    sum_table = pq.read_table(root / "sequence_summary.parquet")
    require(
        mem_table.num_rows == int(success["membership_row_count"]) == int(expected_feature_row_count),
        f"EXP05_BUNDLE_MEMBERSHIP_COUNT_MISMATCH:{mem_table.num_rows}!={expected_feature_row_count}",
    )
    require(
        sum_table.num_rows == int(success["summary_row_count"]) == int(expected_mainshock_count),
        f"EXP05_BUNDLE_SUMMARY_COUNT_MISMATCH:{sum_table.num_rows}!={expected_mainshock_count}",
    )

    mem_rows = mem_table.to_pylist()
    sum_rows = sum_table.to_pylist()
    sum_mids = {str(r["mainshock_event_id"]) for r in sum_rows}
    mem_mids = {str(r["mainshock_event_id"]) for r in mem_rows}
    require(
        len(sum_mids) == int(expected_mainshock_count) and mem_mids == sum_mids,
        "EXP05_BUNDLE_MAINSHOCK_GRAIN_MISMATCH",
    )
    ds_ids = {str(r["dataset_id"]) for r in mem_rows} | {str(r["dataset_id"]) for r in sum_rows}
    require(ds_ids == {expected_dataset_id}, "EXP05_BUNDLE_LINEAGE_MISMATCH")

    post_aftershock_count = sum(
        1
        for r in mem_rows
        if r.get("is_sequence_member") is True and r.get("event_role_candidate") == "POST"
    )
    summary_aftershock_count = sum(
        int(r.get("aftershock_count") or 0)
        for r in sum_rows
        if r.get("window_result_status") == "SUCCESS"
    )
    require(
        post_aftershock_count == summary_aftershock_count,
        f"EXP05_BUNDLE_AFTERSHOCK_SUMMARY_MISMATCH:{post_aftershock_count}!={summary_aftershock_count}",
    )

    return {
        "bundle_dir": str(root),
        "experiment_run_id": success["experiment_run_id"],
        "success_marker_sha256": success_sha,
        "membership_row_count": mem_table.num_rows,
        "summary_row_count": sum_table.num_rows,
        "primary_membership_sha256": compute_primary_membership_digest(mem_rows),
        "primary_window_summary_sha256": compute_primary_summary_digest(sum_rows),
        "mainshock_ids": sum_mids,
        "post_aftershock_count": post_aftershock_count,
        "artifact_checksums": dict(checksums),
    }


def _verify_algorithm_output_and_shards(
    *,
    algo: str,
    full_dir: Path,
    shard_root_dir: Path,
    extension_bundle: Dict[str, Any],
    model_config_version: str,
    code_version: Optional[str],
) -> Dict[str, Any]:
    """Verify full-split bundle and disjoint union across all WHOLE_WINDOW_SHARD sibling bundles."""
    manifest = extension_bundle["manifest"]
    dataset_id = manifest["dataset_id"]
    full_info = verify_official_result_bundle(
        full_dir,
        expected_dataset_id=dataset_id,
        expected_algorithm_name=algo,
        expected_model_config_version=model_config_version,
        expected_mainshock_count=int(manifest["mainshock_count"]),
        expected_feature_row_count=int(manifest["feature_row_count"]),
        expected_code_version=code_version,
    )

    shard_verifications: Dict[str, Dict[str, Any]] = {}
    shard_mainshocks_seen: set[str] = set()
    total_shard_memberships = 0
    total_shard_summaries = 0
    for shard_id, shard_bundle in sorted(extension_bundle["shards"].items()):
        shard_dir = shard_root_dir / shard_id
        s_mainshocks = shard_bundle["mainshocks_table"].num_rows
        s_rows = shard_bundle["features_table"].num_rows
        s_info = verify_official_result_bundle(
            shard_dir,
            expected_dataset_id=dataset_id,
            expected_algorithm_name=algo,
            expected_model_config_version=model_config_version,
            expected_mainshock_count=s_mainshocks,
            expected_feature_row_count=s_rows,
            expected_code_version=code_version,
            expected_shard_id=shard_id,
        )
        overlap = shard_mainshocks_seen & s_info["mainshock_ids"]
        require(not overlap, f"EXP05_SHARD_OVERLAPPING_MAINSHOCKS:{algo}:{overlap}")
        shard_mainshocks_seen.update(s_info["mainshock_ids"])
        total_shard_memberships += s_info["membership_row_count"]
        total_shard_summaries += s_info["summary_row_count"]
        shard_verifications[shard_id] = {
            "experiment_run_id": s_info["experiment_run_id"],
            "success_marker_sha256": s_info["success_marker_sha256"],
            "membership_row_count": s_info["membership_row_count"],
            "summary_row_count": s_info["summary_row_count"],
            "post_aftershock_count": s_info["post_aftershock_count"],
        }

    require(
        shard_mainshocks_seen == full_info["mainshock_ids"]
        and total_shard_memberships == full_info["membership_row_count"]
        and total_shard_summaries == full_info["summary_row_count"],
        f"EXP05_SHARD_UNION_MISMATCH:{algo}",
    )

    return {
        "experiment_run_id": full_info["experiment_run_id"],
        "full_bundle_success_sha256": full_info["success_marker_sha256"],
        "primary_membership_sha256": full_info["primary_membership_sha256"],
        "primary_window_summary_sha256": full_info["primary_window_summary_sha256"],
        "membership_row_count": full_info["membership_row_count"],
        "summary_row_count": full_info["summary_row_count"],
        "post_aftershock_count": full_info["post_aftershock_count"],
        "physical_bundle_verified": True,
        "physical_shards_verified": True,
        "shard_count": len(shard_verifications),
        "shard_verifications": shard_verifications,
    }


def _summarize_failures(algo_detail: Dict[str, Any]) -> str:
    fail_cnt = int(algo_detail.get("failure_windows", 0))
    total_cnt = int(algo_detail.get("total_windows", 0))
    reasons = algo_detail.get("failure_reason_counts") or {}
    if fail_cnt == 0:
        return f"0/{total_cnt} windows failed"
    parts = [f"{k}={v}" for k, v in sorted(reasons.items())]
    return f"{fail_cnt}/{total_cnt} ({'; '.join(parts)})"


def _assess_generalization(
    algo: str,
    repro: Dict[str, Any],
    ext: Dict[str, Any],
    deltas: Dict[str, Any],
) -> Tuple[str, str]:
    """Derive factual cross-period assessment strictly from observed metrics and reason codes."""
    fail_delta = deltas.get("failure_rate")
    dbcv_ext = _safe_float(ext.get("mean_dbcv_score"))
    jacc_ext = _safe_float(ext.get("mean_jaccard_stability"))
    omori_r2_ext = _safe_float(ext.get("mean_omori_r2"))
    noise_ext = _safe_float(ext.get("mean_noise_ratio"))
    runtime_ratio = deltas.get("runtime_ratio")

    observations: List[str] = []
    if algo == "WINDOW":
        observations.append("0% failure rate across both splits by counting rule; retains full background (0% noise filtering).")
        if omori_r2_ext is not None:
            observations.append(f"Omori R^2={omori_r2_ext:.4f} across {ext.get('omori_fitted_windows')}/{ext.get('total_windows')} windows.")
        return "STABLE_UNFILTERED_BASELINE", " ".join(observations)

    if fail_delta is not None and abs(fail_delta) <= 0.15:
        observations.append(f"Failure rate delta={fail_delta:+.4f}.")
    elif fail_delta is not None:
        observations.append(f"Notable failure rate shift delta={fail_delta:+.4f}.")
    if noise_ext is not None:
        observations.append(f"Noise ratio={noise_ext:.4f}.")
    if dbcv_ext is not None:
        observations.append(f"DBCV={dbcv_ext:.4f} (n={ext.get('dbcv_computed_windows')}).")
    else:
        observations.append(f"DBCV=NULL ({ext.get('dbcv_reason_counts')}).")
    if jacc_ext is not None:
        observations.append(f"Jaccard stability={jacc_ext:.4f}.")
    if runtime_ratio is not None and runtime_ratio > 3.0:
        observations.append(f"Runtime scaled {runtime_ratio:.2f}x on denser mega-windows.")

    if fail_delta is not None and abs(fail_delta) <= 0.15 and (jacc_ext is None or jacc_ext >= 0.80):
        status = "GENERALIZES_WITH_TRADEOFFS"
    elif jacc_ext is not None and jacc_ext < 0.70:
        status = "STABILITY_DEGRADED"
    else:
        status = "SHIFTED_ON_EXTENSION"
    return status, " ".join(observations)


def compare_reproduction_vs_extension(
    *,
    exp04_report: Dict[str, Any],
    ext_eval_results: Dict[str, Any],
    extension_bundle: Dict[str, Any],
    bundle_verifications: Dict[str, Dict[str, Any]],
    freeze_gate_result: Dict[str, Any],
    adaptive_official_report: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Compare REPRODUCTION and EXTENSION distributions and algorithm metrics without accuracy/F1."""
    repro_profile = extract_reproduction_distribution_profile(exp04_report)
    ext_profile = compute_split_distribution_profile(extension_bundle)

    repro_dist = repro_profile["window_candidate_distribution"]
    ext_dist = ext_profile["window_candidate_distribution"]
    mean_ratio = (
        round(float(ext_dist["mean"]) / float(repro_dist["mean"]), 3)
        if repro_dist.get("mean") and ext_dist.get("mean")
        else None
    )
    max_ratio = (
        round(float(ext_dist["max"]) / float(repro_dist["max"]), 3)
        if repro_dist.get("max") and ext_dist.get("max")
        else None
    )

    data_drift = {
        "reproduction": repro_profile,
        "extension": ext_profile,
        "shifts": {
            "mc_value_delta": _compute_delta(ext_profile["mc_value"], repro_profile["mc_value"], 4),
            "mainshock_count_delta": int(ext_profile["mainshock_count"]) - int(repro_profile["mainshock_count"]),
            "feature_row_count_delta": int(ext_profile["feature_row_count"]) - int(repro_profile["feature_row_count"]),
            "feature_row_ratio": round(ext_profile["feature_row_count"] / max(1, repro_profile["feature_row_count"]), 4),
            "mean_candidates_per_window_ratio": mean_ratio,
            "median_candidates_per_window_delta": _compute_delta(ext_dist["median"], repro_dist["median"], 2),
            "max_candidates_per_window_delta": (
                int(ext_dist["max"]) - int(repro_dist["max"])
                if ext_dist.get("max") is not None and repro_dist.get("max") is not None
                else None
            ),
            "max_candidates_per_window_ratio": max_ratio,
            "windows_gt_10k_delta": int(ext_dist["windows_gt_10k"]) - int(repro_dist["windows_gt_10k"]),
            "windows_gt_50k_delta": int(ext_dist["windows_gt_50k"]) - int(repro_dist["windows_gt_50k"]),
        },
    }

    repro_details = exp04_report["evaluation_summary"]["algorithm_details"]
    ext_details = ext_eval_results["algorithm_details"]

    algorithm_comparisons: Dict[str, Dict[str, Any]] = {}
    decision_table: List[Dict[str, Any]] = []
    failure_cases_summary: Dict[str, Dict[str, Any]] = {}

    for algo in ALGORITHM_ORDER:
        r_det = repro_details[algo]
        e_det = ext_details[algo]
        ver = bundle_verifications[algo]

        # Enforce denominator preservation on EXTENSION (no silent dropping of failed windows)
        ext_total = int(e_det["total_windows"])
        ext_succ = int(e_det["success_windows"])
        ext_fail = int(e_det["failure_windows"])
        require(
            ext_total == int(ext_profile["mainshock_count"]) and (ext_succ + ext_fail == ext_total),
            f"EXP05_DENOMINATOR_MISMATCH:{algo}:{ext_succ}+{ext_fail}!={ext_total}",
        )

        # Cross-check exact primary digest parity between bundles on disk & evaluation summary
        require(
            ver["primary_membership_sha256"] == e_det["primary_membership_sha256"],
            f"EXP05_PRIMARY_MEMBERSHIP_PARITY_MISMATCH:{algo}",
        )
        require(
            ver["primary_window_summary_sha256"] == e_det["primary_window_summary_sha256"],
            f"EXP05_PRIMARY_SUMMARY_PARITY_MISMATCH:{algo}",
        )
        require(
            int(ver["post_aftershock_count"]) == int(e_det["total_aftershocks"]),
            f"EXP05_AFTERSHOCK_COUNT_PARITY_MISMATCH:{algo}:{ver['post_aftershock_count']}!={e_det['total_aftershocks']}",
        )

        r_runtime = _safe_float(r_det.get("runtime_seconds"))
        e_runtime = _safe_float(e_det.get("runtime_seconds"))
        deltas = {
            "failure_rate": _compute_delta(e_det.get("failure_rate"), r_det.get("failure_rate"), 4),
            "mean_noise_ratio": _compute_delta(e_det.get("mean_noise_ratio"), r_det.get("mean_noise_ratio"), 4),
            "mean_dbcv_score": _compute_delta(e_det.get("mean_dbcv_score"), r_det.get("mean_dbcv_score"), 4),
            "mean_omori_p": _compute_delta(e_det.get("mean_omori_p"), r_det.get("mean_omori_p"), 4),
            "mean_omori_r2": _compute_delta(e_det.get("mean_omori_r2"), r_det.get("mean_omori_r2"), 4),
            "mean_jaccard_stability": _compute_delta(e_det.get("mean_jaccard_stability"), r_det.get("mean_jaccard_stability"), 4),
            "mean_ari_stability": _compute_delta(e_det.get("mean_ari_stability"), r_det.get("mean_ari_stability"), 4),
            "runtime_seconds": _compute_delta(e_runtime, r_runtime, 2),
            "runtime_ratio": (
                round(e_runtime / r_runtime, 3)
                if e_runtime is not None and r_runtime is not None and r_runtime > 0
                else None
            ),
        }

        repro_slice = {
            "model_config_version": r_det.get("model_config_version"),
            "total_windows": r_det.get("total_windows"),
            "success_windows": r_det.get("success_windows"),
            "failure_windows": r_det.get("failure_windows"),
            "failure_rate": r_det.get("failure_rate"),
            "failure_reason_counts": r_det.get("failure_reason_counts", {}),
            "total_aftershocks": r_det.get("total_aftershocks"),
            "mean_noise_ratio": r_det.get("mean_noise_ratio"),
            "mean_dbcv_score": r_det.get("mean_dbcv_score"),
            "dbcv_computed_windows": r_det.get("dbcv_computed_windows"),
            "dbcv_reason_counts": r_det.get("dbcv_reason_counts", {}),
            "mean_omori_p": r_det.get("mean_omori_p"),
            "mean_omori_r2": r_det.get("mean_omori_r2"),
            "omori_fitted_windows": r_det.get("omori_fitted_windows"),
            "omori_reason_counts": r_det.get("omori_reason_counts", {}),
            "mean_jaccard_stability": r_det.get("mean_jaccard_stability"),
            "mean_ari_stability": r_det.get("mean_ari_stability"),
            "stability_computed_windows": r_det.get("stability_computed_windows"),
            "stability_reason_counts": r_det.get("stability_reason_counts", {}),
            "runtime_seconds": r_runtime,
        }
        ext_slice = {
            "model_config_version": e_det.get("model_config_version"),
            "total_windows": ext_total,
            "success_windows": ext_succ,
            "failure_windows": ext_fail,
            "failure_rate": e_det.get("failure_rate"),
            "failure_reason_counts": e_det.get("failure_reason_counts", {}),
            "total_aftershocks": e_det.get("total_aftershocks"),
            "mean_noise_ratio": e_det.get("mean_noise_ratio"),
            "mean_dbcv_score": e_det.get("mean_dbcv_score"),
            "dbcv_computed_windows": e_det.get("dbcv_computed_windows"),
            "dbcv_reason_counts": e_det.get("dbcv_reason_counts", {}),
            "mean_omori_p": e_det.get("mean_omori_p"),
            "mean_omori_r2": e_det.get("mean_omori_r2"),
            "omori_fitted_windows": e_det.get("omori_fitted_windows"),
            "omori_reason_counts": e_det.get("omori_reason_counts", {}),
            "mean_jaccard_stability": e_det.get("mean_jaccard_stability"),
            "mean_ari_stability": e_det.get("mean_ari_stability"),
            "stability_computed_windows": e_det.get("stability_computed_windows"),
            "stability_reason_counts": e_det.get("stability_reason_counts", {}),
            "runtime_seconds": e_runtime,
        }

        status_label, obs_summary = _assess_generalization(algo, repro_slice, ext_slice, deltas)
        algorithm_comparisons[algo] = {
            "reproduction": repro_slice,
            "extension": ext_slice,
            "deltas": deltas,
            "generalization_status": status_label,
            "observation_summary": obs_summary,
            "bundle_verification": {
                "experiment_run_id": ver["experiment_run_id"],
                "full_bundle_success_sha256": ver["full_bundle_success_sha256"],
                "primary_membership_sha256": ver["primary_membership_sha256"],
                "primary_window_summary_sha256": ver["primary_window_summary_sha256"],
                "shard_count": ver["shard_count"],
                "physical_bundle_verified": ver["physical_bundle_verified"],
                "physical_shards_verified": ver["physical_shards_verified"],
                "primary_membership_parity_verified": True,
                "shard_verifications": ver["shard_verifications"],
            },
        }
        decision_table.append({
            "Algorithm": algo,
            "Model Config": e_det.get("model_config_version"),
            "Repro Success/Total": f"{r_det['success_windows']}/{r_det['total_windows']}",
            "Ext Success/Total": f"{ext_succ}/{ext_total}",
            "Failure Rate Delta": f"{deltas['failure_rate']:+.4f}" if deltas["failure_rate"] is not None else "N/A",
            "Ext Noise Ratio": f"{e_det['mean_noise_ratio']:.4f}" if _safe_float(e_det.get("mean_noise_ratio")) is not None else "N/A",
            "Ext Mean DBCV": f"{e_det['mean_dbcv_score']:.4f}" if _safe_float(e_det.get("mean_dbcv_score")) is not None else "N/A",
            "Ext Mean Omori p": f"{e_det['mean_omori_p']:.4f}" if _safe_float(e_det.get("mean_omori_p")) is not None else "N/A",
            "Ext Jaccard": f"{e_det['mean_jaccard_stability']:.4f}" if _safe_float(e_det.get("mean_jaccard_stability")) is not None else "N/A",
            "Ext Failure Cases": _summarize_failures(e_det),
            "Assessment": status_label,
        })
        failure_cases_summary[algo] = {
            "reproduction_failures": r_det.get("failure_windows", 0),
            "reproduction_failure_reasons": r_det.get("failure_reason_counts", {}),
            "extension_failures": ext_fail,
            "extension_failure_reasons": e_det.get("failure_reason_counts", {}),
            "extension_dbcv_null_reasons": e_det.get("dbcv_reason_counts", {}),
            "extension_omori_null_reasons": e_det.get("omori_reason_counts", {}),
            "extension_stability_null_reasons": e_det.get("stability_reason_counts", {}),
        }

    repro_ms = (exp04_report.get("exp03_cross_verification") or {}).get("multi_sequence_reconciliation")
    ext_ms = (adaptive_official_report or {}).get("multi_sequence_reconciliation")
    adaptive_multi_sequence_summary = {
        "reproduction": repro_ms,
        "extension": ext_ms,
        "extension_case_results": (adaptive_official_report or {}).get("case_results"),
        "extension_adaptive_param_stats": (adaptive_official_report or {}).get("adaptive_param_stats"),
    }

    guardrails = {
        "is_temporal_generalization_check": True,
        "is_supervised_accuracy": False,
        "no_accuracy_or_f1_metric": True,
        "parameter_tuning_applied_on_extension": False,
        "preserve_failed_windows_in_denominator": True,
        "preserve_honest_nulls_with_reason_codes": True,
        "retrospective_candidate_only": True,
        "earthquake_prediction_claimed": False,
        "scientific_approval": False,
    }

    summary = {
        "freeze_gate": {
            "gate_status": freeze_gate_result["gate_status"],
            "frozen_rule_version": freeze_gate_result["frozen_rule_version"],
            "exp04_report_sha256": freeze_gate_result["exp04_report_sha256"],
            "exp04_report_normalized_lf_sha256": freeze_gate_result["exp04_report_normalized_lf_sha256"],
            "frozen_from_dataset_id": freeze_gate_result["frozen_from_dataset_id"],
            "extension_dataset_id": freeze_gate_result["extension_dataset_id"],
            "gold_pin_verified": freeze_gate_result["gold_pin_verified"],
            "parameter_tuning_applied_on_extension": freeze_gate_result["parameter_tuning_applied_on_extension"],
            "config_freeze_verified": freeze_gate_result["config_freeze_verified"],
            "evaluation_param_freeze_verified": freeze_gate_result["evaluation_param_freeze_verified"],
            "feature_contract_freeze_verified": freeze_gate_result["feature_contract_freeze_verified"],
            "exp02_cross_verify_passed": freeze_gate_result.get("exp02_cross_verify_passed", False),
            "exp03_cross_verify_passed": freeze_gate_result.get("exp03_cross_verify_passed", False),
            "mc_drift": freeze_gate_result["mc_drift"],
        },
        "data_drift": data_drift,
        "algorithm_comparisons": algorithm_comparisons,
        "adaptive_multi_sequence_summary": adaptive_multi_sequence_summary,
        "decision_table": decision_table,
        "failure_cases_summary": failure_cases_summary,
        "guardrails": guardrails,
    }
    assert_no_forbidden_metrics(summary)
    validate_safe_metadata(summary)
    return summary


def run_extension_bundles_and_evaluate(
    *,
    bundle_dir: Union[str, Path],
    exp04_report_path: Union[str, Path],
    expected_exp04_report_sha256: Optional[str],
    output_dir: Union[str, Path],
    manifest_sha256: Optional[str] = None,
    checksums_sha256: Optional[str] = None,
    handoff_path: Optional[Union[str, Path]] = None,
    expected_handoff_sha256: Optional[str] = None,
    custom_configs: Optional[Dict[str, Dict[str, Any]]] = None,
    repeats: int = 5,
    sigma: float = 0.01,
    seed: int = 42,
    min_omori_aftershocks: int = MIN_OMORI_AFTERSHOCKS,
    code_version: Optional[str] = None,
    allow_overwrite: bool = False,
    is_retry: bool = False,
    project_root: Optional[Union[str, Path]] = None,
    allow_lf_crlf_config_normalization: bool = True,
    progress_callback: Optional[Any] = None,
) -> Dict[str, Any]:
    """Verify frozen REPRODUCTION gate, execute all 4 frozen algorithms into MLI-03 bundles, and compare splits."""
    # Enforce no parameter override before touching disk or datasets
    require(
        custom_configs is None,
        "EXP05_CONFIG_OVERRIDE_FORBIDDEN: Custom model configs or parameter tuning on EXTENSION are forbidden",
    )

    root = Path(project_root).resolve() if project_root is not None else PROJECT_ROOT
    ext_bundle_path = Path(bundle_dir).resolve()
    out_root = Path(output_dir).resolve()

    extension_bundle = load_feature_bundle(
        ext_bundle_path,
        mode=MODE_OFFICIAL,
        manifest_sha256=manifest_sha256,
        checksums_sha256=checksums_sha256,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
    )

    gate_result = verify_reproduction_freeze_gate(
        exp04_report_path=exp04_report_path,
        expected_exp04_report_sha256=expected_exp04_report_sha256,
        extension_bundle=extension_bundle,
        project_root=root,
        custom_configs=custom_configs,
        repeats=repeats,
        sigma=sigma,
        seed=seed,
        min_omori_aftershocks=min_omori_aftershocks,
        allow_lf_crlf_config_normalization=allow_lf_crlf_config_normalization,
    )

    configs = gate_result["configs"]
    config_pins = gate_result["config_pins"]
    out_root.mkdir(parents=True, exist_ok=True)

    bundle_verifications: Dict[str, Dict[str, Any]] = {}
    adaptive_official_report: Optional[Dict[str, Any]] = None

    for algo in ALGORITHM_ORDER:
        subdir = ALGO_OUTPUT_SUBDIRS[algo]
        algo_out = out_root / subdir
        shard_out = out_root / f"{subdir}_shards"
        cfg = configs[algo]
        if progress_callback:
            progress_callback(f"bundle_start:{algo}", {"output_dir": str(algo_out)})

        if algo == "HDBSCAN_ADAPTIVE":
            adaptive_res = run_adaptive_experiment(
                bundle=extension_bundle,
                config=cfg,
                model_config_version=cfg["model_config_version"],
                output_dir=algo_out,
                mode=MODE_OFFICIAL,
                code_version=code_version,
                random_seed=seed,
                execution_id=f"exp05_official_hdbscan_adaptive_seed{seed}",
                allow_overwrite=allow_overwrite,
                is_retry=is_retry,
                shard_output_dir=shard_out,
            )
            adaptive_official_report = generate_official_adaptive_report(
                bundle=extension_bundle,
                result=adaptive_res,
                output_path=out_root / "hdbscan_adaptive_report.json",
                code_version=code_version,
                random_seed=seed,
                model_config=cfg,
                model_config_pin=config_pins[algo],
            )
        else:
            run_baseline_experiment(
                bundle=extension_bundle,
                algorithm_name=algo,
                config=cfg,
                model_config_version=cfg["model_config_version"],
                output_dir=algo_out,
                mode=MODE_OFFICIAL,
                code_version=code_version,
                random_seed=seed,
                execution_id=f"exp05_official_{subdir}_seed{seed}",
                allow_overwrite=allow_overwrite,
                is_retry=is_retry,
                shard_output_dir=shard_out,
            )

        bundle_verifications[algo] = _verify_algorithm_output_and_shards(
            algo=algo,
            full_dir=algo_out,
            shard_root_dir=shard_out,
            extension_bundle=extension_bundle,
            model_config_version=cfg["model_config_version"],
            code_version=code_version,
        )
        gc.collect()

    ext_eval_results = compare_all_models_on_official(
        bundle_dir=ext_bundle_path,
        expected_split="EXTENSION",
        manifest_sha256=manifest_sha256,
        checksums_sha256=checksums_sha256,
        handoff_path=handoff_path,
        expected_handoff_sha256=expected_handoff_sha256,
        model_configs=configs,
        config_pins=config_pins,
        repeats=repeats,
        sigma=sigma,
        seed=seed,
        project_root=root,
        progress_callback=progress_callback,
    )

    comparison = compare_reproduction_vs_extension(
        exp04_report=gate_result["exp04_report"],
        ext_eval_results=ext_eval_results,
        extension_bundle=extension_bundle,
        bundle_verifications=bundle_verifications,
        freeze_gate_result=gate_result,
        adaptive_official_report=adaptive_official_report,
    )

    return {
        "freeze_gate": gate_result,
        "extension_evaluation": ext_eval_results,
        "bundle_verifications": bundle_verifications,
        "adaptive_official_report": adaptive_official_report,
        "comparison": comparison,
        "extension_bundle_dataset": ext_eval_results["dataset_info"],
    }
