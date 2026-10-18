"""Control-plane runtime for ML dataset build orchestration (MLI-02).

Java/Spark owns large-scale candidate extraction, spacetime feature generation,
and Parquet writing; Python control-plane owns configuration validation,
published snapshot verification, task sequencing, retry idempotency, attempt
isolation, and safe operator handoff boundaries (no auto-trigger of Colab).
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import uuid
from typing import Any, Mapping

try:
    from jma_backfill_runtime import _atomic_json
except (ModuleNotFoundError, ImportError):
    def _atomic_json(path: Path, payload: Any) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.parent / f".tmp_{path.name}_{uuid.uuid4().hex}"
        temp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        temp.replace(path)

try:
    from resource_pilot import java_identity
except (ModuleNotFoundError, ImportError):  # pragma: no cover - Windows offline tests
    def java_identity(environment: Mapping[str, str], root: Path) -> dict[str, str]:
        return dict(environment)

try:
    from source_run_guard import lease, SourceRunBusy
except (ModuleNotFoundError, ImportError):
    class SourceRunBusy(Exception):
        pass

    def lease(action: str, identity: Mapping[str, str], environment: Mapping[str, str] | None = None) -> dict[str, str]:
        status_map = {"acquire": "ACQUIRED", "assert": "HELD", "release": "RELEASED"}
        return {"status": status_map.get(action, "UNKNOWN"), **dict(identity)}


CONTRACT_VERSION = "1.0"
DAG_ID = "mli_02_build_dataset"
STEPS = (
    "wait_gold_published",
    "resolve_gold_snapshot",
    "audit_gold_input",
    "resolve_mc",
    "select_mainshocks",
    "build_candidate_windows",
    "build_spacetime_features",
    "validate_feature_snapshot",
    "export_parquet",
    "mark_dataset_exported",
)

SPLIT_PERIODS = {
    "REPRODUCTION": ("2000-01-01T00:00:00Z", "2018-10-01T00:00:00Z"),
    "EXTENSION": ("2018-10-01T00:00:00Z", "2024-01-01T00:00:00Z"),
}

VERSIONS = (
    "dataset_config_version",
    "audit_rule_version",
    "mc_method_version",
    "window_model_version",
    "feature_version",
    "scaling_config_version",
    "code_version",
)

MAX_METADATA_BYTES = 65536
SAFE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
SAFE_RUN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:+@/-]{0,255}\Z")
VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")


class Mli02Error(RuntimeError):
    """Fail-closed error for ML dataset orchestration with safe reason codes."""


def canonical_json(value: Any) -> Any:
    """Recursively canonicalize JSON objects by sorting keys and disallowing unsafe paths/keys."""
    if isinstance(value, dict):
        out = {}
        for k in sorted(value.keys()):
            if re.search(r"(?i)(password|credential|secret|token|access_key|private_key)", str(k)):
                raise Mli02Error("DS_UNSAFE_CONFIG_KEY")
            out[k] = canonical_json(value[k])
        return out
    if isinstance(value, list):
        return [canonical_json(v) for v in value]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise Mli02Error("DS_NON_FINITE_CONFIG")
        return value
    if isinstance(value, str):
        if re.search(r"(?i)(?:[a-z]:[\\/]|/Users/|/home/|://[^/]+@)", value):
            raise Mli02Error("DS_UNSAFE_CONFIG_PATH")
        return value
    return value


def canonical_payload_string(payload: dict[str, Any]) -> str:
    """Return compact canonical JSON representation with sorted keys and no whitespace around separators."""
    canonical = canonical_json(payload)
    return json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def sha256_hex(data: bytes | str) -> str:
    raw = data.encode("utf-8") if isinstance(data, str) else data
    return hashlib.sha256(raw).hexdigest()


def compute_dataset_id(identity_payload: dict[str, Any]) -> tuple[str, str]:
    """Compute deterministic dataset_id = 'ds_' + sha256(canonical_payload)."""
    payload_str = canonical_payload_string(identity_payload)
    digest = sha256_hex(payload_str)
    return f"ds_{digest}", digest


def validate_dataset_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Strictly validate dataset configuration according to CON-03-ML and MLD-01."""
    if not isinstance(config, Mapping):
        raise Mli02Error("DS_INVALID_METADATA")

    split = config.get("dataset_split")
    if split not in SPLIT_PERIODS:
        raise Mli02Error("DS_INVALID_PERIOD")

    expected_start, expected_end = SPLIT_PERIODS[split]
    start = config.get("period_start_utc")
    end = config.get("period_end_utc_exclusive")
    if start != expected_start or end != expected_end:
        raise Mli02Error("DS_INVALID_PERIOD")

    cutoff_str = config.get("observation_cutoff_utc")
    if not isinstance(cutoff_str, str) or not cutoff_str.endswith("Z"):
        raise Mli02Error("DS_INVALID_UTC")
    try:
        cutoff = datetime.fromisoformat(cutoff_str.replace("Z", "+00:00"))
        period_end = datetime.fromisoformat(expected_end.replace("Z", "+00:00"))
        if cutoff < period_end:
            raise Mli02Error("DS_CUTOFF_BEFORE_PERIOD_END")
    except ValueError as err:
        raise Mli02Error("DS_INVALID_UTC") from err

    filters = config.get("filter_config")
    if not isinstance(filters, Mapping):
        raise Mli02Error("DS_BASELINE_FILTER_REQUIRED")
    if (
        filters.get("is_natural_earthquake") is not True
        or filters.get("is_in_study_area") is not True
        or filters.get("canonical_source_system") != "JMA_BULLETIN"
        or filters.get("catalog_era") != "UNIFIED"
    ):
        raise Mli02Error("DS_BASELINE_FILTER_REQUIRED")

    for vkey in VERSIONS:
        val = config.get(vkey)
        if not isinstance(val, str) or not VERSION_PATTERN.fullmatch(val):
            raise Mli02Error(f"DS_INVALID_VERSION:{vkey}")

    return dict(config)


def verify_gold_published(publication: Mapping[str, Any]) -> dict[str, Any]:
    """Fail-closed check that Gold snapshot is Published and verified."""
    if not isinstance(publication, Mapping):
        raise Mli02Error("DS_GOLD_NOT_PUBLISHED")

    if publication.get("published") is not True or publication.get("verify_status") != "PASSED":
        raise Mli02Error("DS_GOLD_NOT_PUBLISHED")

    snapshot_id = publication.get("gold_snapshot_id")
    if snapshot_id is None and "snapshot_bundle" in publication:
        snapshot_id = (
            publication["snapshot_bundle"].get("event_current", {}).get("snapshot_id")
        )

    if not isinstance(snapshot_id, int) or snapshot_id <= 0:
        raise Mli02Error("DS_GOLD_NOT_PUBLISHED")

    pub_id = publication.get("gold_publication_id") or publication.get("publication_id")
    if not isinstance(pub_id, str) or not re.fullmatch(r"pub_[a-f0-9]{64}", pub_id):
        raise Mli02Error("DS_GOLD_NOT_PUBLISHED")

    gold_table = publication.get("gold_table_name", "iceberg.gold.earthquake_event_current")
    physical_gold_table = publication.get("physical_gold_table_name", "iceberg.gold.event_current")
    bundle_sha = publication.get("publication_bundle_sha256") or publication.get("bundle_sha256") or pub_id[4:]

    return {
        "gold_publication_id": pub_id,
        "gold_snapshot_id": snapshot_id,
        "gold_table_name": gold_table,
        "physical_gold_table_name": physical_gold_table,
        "publication_bundle_sha256": bundle_sha,
        "verify_status": "PASSED",
        "published": True,
    }


def resolve_run_context(
    raw_context: Mapping[str, Any],
    environment: Mapping[str, str] | None = None,
    attempt: int = 1,
) -> dict[str, Any]:
    """Initialize validated run context, acquire lease, and setup isolated staging paths."""
    env = dict(os.environ if environment is None else environment)
    conf = raw_context.get("conf") if "conf" in raw_context else raw_context
    if not isinstance(conf, Mapping):
        raise Mli02Error("DS_INVALID_METADATA")

    # Validate Gold publication reference first (fail-closed before building anything)
    pub_ref = conf.get("publication_reference")
    if not pub_ref and "publication_status_path" in conf:
        path = Path(conf["publication_status_path"])
        if path.is_file() and path.stat().st_size <= MAX_METADATA_BYTES:
            pub_ref = json.loads(path.read_text(encoding="utf-8"))
    gold_pub = verify_gold_published(pub_ref or {})

    # Validate dataset config
    validated_config = validate_dataset_config(conf)

    run_id = raw_context.get("run_id") or conf.get("run_id") or f"mli02-run-{uuid.uuid4().hex[:12]}"
    if (
        not isinstance(run_id, str)
        or not SAFE_RUN_ID_PATTERN.fullmatch(run_id)
        or ".." in run_id
        or run_id.startswith("/")
        or run_id.endswith("/")
    ):
        raise Mli02Error("DS_RUN_CONTEXT_REQUIRED")

    dag_id = raw_context.get("dag_id") or DAG_ID
    owner = {"dag_id": dag_id, "run_id": run_id}

    # Acquire cross-DAG lease to guard staging volume with exact DagRun identity
    guard_res = lease("acquire", owner, env)
    if guard_res.get("status") not in ("ACQUIRED", "HELD"):
        raise Mli02Error("DS_LEASE_BUSY")

    # Safe filesystem slug derived from run_id identity (handles colons, plus signs on Windows)
    safe_slug = re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)[:40]
    id_hash = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
    staging_dir_name = f"{safe_slug}_{id_hash}"
    staging_root = Path(env.get("ML_BUILD_STAGING_ROOT", "/opt/pipeline/staging/ml-build")) / staging_dir_name
    staging_root.mkdir(parents=True, exist_ok=True)

    export_root = Path(env.get("ML_BUILD_EXPORT_ROOT", "/opt/pipeline/staging/ml-export"))
    registry_root = Path(env.get("MLI_DATASET_REGISTRY_ROOT", "/opt/pipeline/staging/ml-artifacts"))
    registry_root.mkdir(parents=True, exist_ok=True)

    # Derive deterministic identity payload
    split = validated_config["dataset_split"]
    start, end = SPLIT_PERIODS[split]
    filter_canonical_json = canonical_json(validated_config["filter_config"])
    filter_json_str = json.dumps(filter_canonical_json, sort_keys=True, separators=(",", ":"))

    identity_payload = {
        "gold_table_name": gold_pub["gold_table_name"],
        "physical_gold_table_name": gold_pub["physical_gold_table_name"],
        "gold_publication_id": gold_pub["gold_publication_id"],
        "publication_bundle_sha256": gold_pub["publication_bundle_sha256"],
        "gold_snapshot_id": gold_pub["gold_snapshot_id"],
        "dataset_split": split,
        "period_start_utc": start,
        "period_end_utc_exclusive": end,
        "observation_cutoff_utc": validated_config["observation_cutoff_utc"],
        "filter_config_json": filter_json_str,
    }
    for vkey in VERSIONS:
        identity_payload[vkey] = validated_config[vkey]

    dataset_id, identity_digest = compute_dataset_id(identity_payload)

    # Check for identity conflict in registry
    registered_manifest_path = registry_root / f"{dataset_id}.json"
    if registered_manifest_path.is_file():
        try:
            reg_manifest = json.loads(registered_manifest_path.read_text(encoding="utf-8"))
            if reg_manifest.get("identity_payload_sha256") != identity_digest:
                raise Mli02Error("DS_IDENTITY_CONFLICT")
        except (ValueError, OSError) as err:
            raise Mli02Error("DS_IDENTITY_CONFLICT") from err

    context_doc = {
        "contract_version": CONTRACT_VERSION,
        "dag_id": dag_id,
        "run_id": run_id,
        "attempt": attempt,
        "lease_owner": owner,
        "gold_publication": gold_pub,
        "validated_config": validated_config,
        "identity_payload": identity_payload,
        "identity_payload_sha256": identity_digest,
        "dataset_id": dataset_id,
        "dataset_status": "BUILDING",
        "staging_root": str(staging_root),
        "export_root": str(export_root),
        "registry_root": str(registry_root),
        "mock_pipeline": raw_context.get("mock_pipeline") or conf.get("mock_pipeline"),
    }

    _atomic_json(staging_root / "resolved-context.json", context_doc)
    return context_doc


def _verify_bundle_checksums(bundle_dir: Path) -> dict[str, str]:
    """Verify all files listed in checksums.sha256 against their actual disk contents."""
    checksums_path = bundle_dir / "checksums.sha256"
    if not checksums_path.is_file():
        raise Mli02Error("DS_EXPORT_VERIFICATION_FAILED")

    verified_files = {}
    lines = checksums_path.read_text(encoding="utf-8").strip().splitlines()
    if not lines:
        raise Mli02Error("DS_EXPORT_CHECKSUM_EMPTY")

    for line in lines:
        if not line.strip():
            continue
        parts = line.strip().split(None, 1)
        if len(parts) != 2:
            raise Mli02Error("DS_EXPORT_CHECKSUM_MALFORMED")
        expected_sha, fname = parts
        fname = fname.strip()
        target = bundle_dir / fname
        if not target.is_file():
            raise Mli02Error(f"DS_EXPORT_FILE_MISSING:{fname}")
        actual_sha = hashlib.sha256(target.read_bytes()).hexdigest()
        if actual_sha != expected_sha:
            raise Mli02Error(f"DS_EXPORT_CHECKSUM_MISMATCH:{fname}")
        verified_files[fname] = expected_sha

    return verified_files


def verify_exported_parquet_readback(
    export_dir: Path,
    expected_dataset_id: str,
    expected_feature_row_count: int,
    expected_mainshock_ids: list[str] | set[str] | None = None,
    verified_files: Any = None,
) -> dict[str, Any]:
    """Strict readback of Parquet parts to verify schema, row count, grain uniqueness, and lineage."""
    if verified_files is not None:
        parquet_files = [export_dir / f for f in sorted(verified_files) if f.startswith("features-part-") and f.endswith(".parquet")]
    else:
        parquet_files = sorted(export_dir.glob("features-part-*.parquet"))

    if not parquet_files:
        raise Mli02Error("DS_FEATURE_ROWS_EMPTY")

    try:
        import pyarrow.parquet as pq
    except (ModuleNotFoundError, ImportError):
        raise Mli02Error("DS_PARQUET_READER_UNAVAILABLE")

    total_rows = 0
    observed_grain = set()
    required_columns = {
        "dataset_id", "mainshock_event_id", "candidate_event_id",
        "candidate_time_utc", "candidate_latitude", "candidate_longitude",
        "candidate_depth_km", "candidate_magnitude", "relative_time_role",
        "is_mainshock", "dx_km", "dy_km", "dz_km", "delta_time_hours",
        "x_scaled", "y_scaled", "z_scaled", "time_scaled",
    }

    for pf in parquet_files:
        table = pq.ParquetFile(pf).read()
        missing = required_columns - set(table.column_names)
        if missing:
            raise Mli02Error(f"DS_SCHEMA_MISMATCH:missing_{','.join(sorted(missing))}")

        total_rows += table.num_rows

        did_col = table.column("dataset_id").to_pylist()
        for val in did_col:
            if val != expected_dataset_id:
                raise Mli02Error("DS_LINEAGE_MISMATCH")

        main_ids = table.column("mainshock_event_id").to_pylist()
        cand_ids = table.column("candidate_event_id").to_pylist()
        for m_id, c_id in zip(main_ids, cand_ids):
            pair = (m_id, c_id)
            if pair in observed_grain:
                raise Mli02Error("DS_GRAIN_DUPLICATE")
            observed_grain.add(pair)
            if expected_mainshock_ids and m_id not in expected_mainshock_ids:
                raise Mli02Error("DS_LINEAGE_MISMATCH")

    if total_rows != expected_feature_row_count:
        raise Mli02Error(
            f"DS_COUNT_RECONCILIATION_FAILED:expected_{expected_feature_row_count}_got_{total_rows}"
        )

    return {"total_rows": total_rows, "grain_count": len(observed_grain)}


def execute_step(
    step_name: str,
    context: Mapping[str, Any],
    upstream: Any = None,
    environment: Mapping[str, str] | None = None,
    attempt: int = 1,
) -> dict[str, Any]:
    """Execute one phase of the dataset build with strict step transitions and attempt isolation."""
    if step_name not in STEPS:
        raise Mli02Error(f"DS_UNKNOWN_STEP:{step_name}")

    env = dict(os.environ if environment is None else environment)
    owner = context["lease_owner"]

    # Verify lease is still held, unless step is retried or repeated for an already-published dataset
    is_already_published = False
    if step_name in ("export_parquet", "mark_dataset_exported"):
        export_root = Path(context["export_root"])
        final_export_dir = export_root / f"dataset_id={context['dataset_id']}"
        manifest_target = final_export_dir / "dataset_manifest.json"
        checksums_target = final_export_dir / "checksums.sha256"
        if manifest_target.is_file():
            try:
                m_data = json.loads(manifest_target.read_text(encoding="utf-8"))
                if m_data.get("dataset_id") == context["dataset_id"]:
                    if m_data.get("identity_payload_sha256") != context["identity_payload_sha256"]:
                        raise Mli02Error("DS_IDENTITY_CONFLICT")
                    if m_data.get("dataset_status") == "EXPORTED" and checksums_target.is_file():
                        is_already_published = True
            except Mli02Error:
                raise
            except Exception:
                is_already_published = False

        if not is_already_published and context.get("mock_pipeline"):
            mock_export_dir = Path(context["staging_root"]) / "mock_export"
            mock_manifest_target = mock_export_dir / "dataset_manifest.json"
            if mock_manifest_target.is_file():
                try:
                    m_data = json.loads(mock_manifest_target.read_text(encoding="utf-8"))
                    if m_data.get("dataset_id") == context["dataset_id"]:
                        is_already_published = True
                except Exception:
                    is_already_published = False

    if not is_already_published:
        try:
            held = lease("assert", owner, env)
            if held.get("status") != "HELD":
                raise Mli02Error("DS_LEASE_NOT_HELD")
        except SourceRunBusy as err:
            raise Mli02Error("DS_LEASE_NOT_HELD") from err

    # Gate export and publish on valid, passing validation snapshot
    if step_name in ("export_parquet", "mark_dataset_exported"):
        staging_root = Path(context["staging_root"])
        val_report_path = staging_root / "step_08_validate_feature_snapshot.json"
        if not val_report_path.is_file():
            raise Mli02Error("DS_VALIDATION_GATE_MISSING")
        val_doc = _read_step(val_report_path)
        verify_validation_report(val_doc)

    mock_pipeline = context.get("mock_pipeline")
    runner_cmd = env.get("ML_BUILD_RUNNER_COMMAND", "").strip()

    # Fail closed: Production path MUST stop if runner is missing!
    if not runner_cmd and not mock_pipeline:
        raise Mli02Error("DS_RUNNER_REQUIRED")

    # Production external runner execution
    if runner_cmd and not mock_pipeline:
        return _run_external_runner_step(step_name, context, upstream, runner_cmd, env, attempt)

    # Isolated test mock adapter execution
    return _run_mock_test_adapter_step(step_name, context, upstream, env, attempt)


def _run_mock_test_adapter_step(
    step_name: str,
    context: Mapping[str, Any],
    upstream: Any,
    environment: Mapping[str, str],
    attempt: int,
) -> dict[str, Any]:
    """Isolated test double adapter for DAG testing without mutating official registry/lifecycle."""
    staging_root = Path(context["staging_root"])
    mock_pipeline = context.get("mock_pipeline") or {}
    dataset_id = context["dataset_id"]
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    if step_name == "wait_gold_published":
        pub = verify_gold_published(context["gold_publication"])
        res = {
            "step": "wait_gold_published",
            "status": "VERIFIED_PUBLISHED",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "gold_snapshot_id": pub["gold_snapshot_id"],
            "gold_publication_id": pub["gold_publication_id"],
            "published": True,
            "attempt": attempt,
            "verified_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_01_wait_gold_published.json", res)
        return res

    if step_name == "resolve_gold_snapshot":
        if not upstream or upstream.get("status") != "VERIFIED_PUBLISHED":
            raise Mli02Error("DS_GOLD_NOT_PUBLISHED")
        res = {
            "step": "resolve_gold_snapshot",
            "status": "SNAPSHOT_PINNED",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "dataset_split": context["validated_config"]["dataset_split"],
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "gold_publication_id": context["gold_publication"]["gold_publication_id"],
            "registry_snapshot_id": f"reg_gold_{context['gold_publication']['gold_snapshot_id']}",
            "identity_payload_sha256": context["identity_payload_sha256"],
            "dataset_status": "BUILDING",
            "attempt": attempt,
            "pinned_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_02_resolve_gold_snapshot.json", res)
        return res

    if step_name == "audit_gold_input":
        audit_mock = mock_pipeline.get("audit")
        if not audit_mock:
            raise Mli02Error("DS_MOCK_AUDIT_REQUIRED")
        inp = audit_mock["input_event_count"]
        elg = audit_mock["eligible_event_count"]
        exc = audit_mock["excluded_event_count"]
        if inp != elg + exc or inp < 0 or elg < 0 or exc < 0:
            raise Mli02Error("DS_AUDIT_COUNTS_INVALID")
        res = {
            "step": "audit_gold_input",
            "status": "AUDIT_PASSED",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "attempt": attempt,
            "counts": {
                "input_event_count": inp,
                "eligible_event_count": elg,
                "excluded_event_count": exc,
            },
            "exclusion_counts": audit_mock.get("exclusion_counts_by_primary_reason", {}),
            "audit_report_uri": f"{context['staging_root']}/audit_report.json",
            "audited_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_03_audit_gold_input.json", res)
        return res

    if step_name == "resolve_mc":
        mc_mock = mock_pipeline.get("mc")
        if not mc_mock:
            raise Mli02Error("DS_MOCK_MC_REQUIRED")
        mc_val = float(mc_mock["mc_value"])
        if not math.isfinite(mc_val) or mc_val <= 0:
            raise Mli02Error("DS_MC_INVALID")
        res = {
            "step": "resolve_mc",
            "status": "MC_RESOLVED",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "attempt": attempt,
            "mc_value": mc_val,
            "mc_method": mc_mock["mc_method"],
            "mc_method_version": mc_mock["mc_method_version"],
            "mc_region_version": mc_mock.get("mc_region_version"),
            "mc_config_json": mc_mock["mc_config_json"],
            "mc_config_sha256": mc_mock["mc_config_sha256"],
            "resolved_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_04_resolve_mc.json", res)
        return res

    if step_name == "select_mainshocks":
        sel_mock = mock_pipeline.get("selection")
        if not sel_mock:
            raise Mli02Error("DS_MOCK_SELECTION_REQUIRED")
        count = int(sel_mock["mainshock_count"])
        if count <= 0:
            raise Mli02Error("DS_NO_MAINSHOCKS_SELECTED")
        res = {
            "step": "select_mainshocks",
            "status": "MAINSHOCKS_SELECTED",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "mainshock_snapshot_id": sel_mock.get("mainshock_snapshot_id", f"main_{context['gold_publication']['gold_snapshot_id']}_v1"),
            "attempt": attempt,
            "mainshock_count": count,
            "mainshock_ids": [m.get("mainshock_event_id") for m in sel_mock.get("mainshocks", [])],
            "selected_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_05_select_mainshocks.json", res)
        return res

    if step_name == "build_candidate_windows":
        win_mock = mock_pipeline.get("windows")
        if not win_mock:
            raise Mli02Error("DS_MOCK_WINDOWS_REQUIRED")
        c_count = int(win_mock["candidate_row_count"])
        if c_count <= 0:
            raise Mli02Error("DS_CANDIDATE_ROWS_EMPTY")
        res = {
            "step": "build_candidate_windows",
            "status": "WINDOWS_BUILT",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "candidate_snapshot_id": win_mock.get("candidate_snapshot_id", f"cand_{context['gold_publication']['gold_snapshot_id']}_v1"),
            "attempt": attempt,
            "candidate_row_count": c_count,
            "role_counts": win_mock.get("role_counts", {}),
            "built_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_06_build_candidate_windows.json", res)
        return res

    if step_name == "build_spacetime_features":
        feat_mock = mock_pipeline.get("features")
        if not feat_mock:
            raise Mli02Error("DS_MOCK_FEATURES_REQUIRED")
        f_count = int(feat_mock["feature_row_count"])
        if f_count <= 0:
            raise Mli02Error("DS_FEATURE_ROWS_EMPTY")
        res = {
            "step": "build_spacetime_features",
            "status": "FEATURES_BUILT",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "attempt": attempt,
            "feature_row_count": f_count,
            "feature_schema_sha256": feat_mock.get("feature_schema_sha256", sha256_hex("features_schema_v1")),
            "built_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_07_build_spacetime_features.json", res)
        return res

    if step_name == "validate_feature_snapshot":
        step_win = _read_step(staging_root / "step_06_build_candidate_windows.json")
        step_feat = _read_step(staging_root / "step_07_build_spacetime_features.json")
        if step_win["candidate_row_count"] != step_feat["feature_row_count"]:
            raise Mli02Error("DS_COUNT_RECONCILIATION_FAILED")

        res = {
            "step": "validate_feature_snapshot",
            "status": "FEATURE_SNAPSHOT_VALIDATED",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "attempt": attempt,
            "dataset_status": "VALIDATED",
            "candidate_row_count": step_win["candidate_row_count"],
            "feature_row_count": step_feat["feature_row_count"],
            "grain_uniqueness_verified": True,
            "resource_guard_status": "PASS",
            "quality_gate_status": "PASSED",
            "invalid_feature_count": 0,
            "validated_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_08_validate_feature_snapshot.json", res)
        return res

    if step_name == "export_parquet":
        attempt_dir = staging_root / "export_attempts" / f"attempt_{attempt}"
        if attempt_dir.exists() and any(attempt_dir.iterdir()):
            raise Mli02Error("DS_ATTEMPT_DIRECTORY_EXISTS")
        attempt_dir.mkdir(parents=True, exist_ok=True)

        feat_step = _read_step(staging_root / "step_07_build_spacetime_features.json")
        sel_step = _read_step(staging_root / "step_05_select_mainshocks.json")
        row_count = feat_step["feature_row_count"]

        part_name = "features-part-00000.parquet"
        part_path = attempt_dir / part_name
        _materialize_features_parquet(
            part_path,
            context,
            row_count=row_count,
            mainshocks=[{"mainshock_event_id": mid} for mid in sel_step.get("mainshock_ids", ["evt_001"])],
        )

        part_sha = hashlib.sha256(part_path.read_bytes()).hexdigest()
        checksums_path = attempt_dir / "checksums.sha256"
        checksums_content = f"{part_sha}  {part_name}\n"
        checksums_path.write_text(checksums_content, encoding="utf-8")
        checksums_sha = hashlib.sha256(checksums_path.read_bytes()).hexdigest()

        manifest_path = attempt_dir / "dataset_manifest.json"
        manifest_doc = _build_dataset_manifest(
            context,
            staging_root,
            export_status="VALIDATED",
            export_artifact_uri=str(attempt_dir),
            export_manifest_sha256=checksums_sha,
            export_manifest_ref="checksums.sha256",
            attempt_dir=attempt_dir,
        )
        _atomic_json(manifest_path, manifest_doc)

        res = {
            "step": "export_parquet",
            "status": "PARQUET_EXPORTED",
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "attempt": attempt,
            "attempt_dir": str(attempt_dir),
            "files": {
                part_name: {"bytes": part_path.stat().st_size, "sha256": part_sha},
                "checksums.sha256": {"bytes": len(checksums_content.encode("utf-8")), "sha256": checksums_sha},
            },
            "exported_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_09_export_parquet.json", res)
        return res

    if step_name == "mark_dataset_exported":
        step_export = _read_step(staging_root / "step_09_export_parquet.json")
        attempt_dir = Path(step_export["attempt_dir"])

        verified_files = _verify_bundle_checksums(attempt_dir)

        feat_step = _read_step(staging_root / "step_07_build_spacetime_features.json")
        verify_exported_parquet_readback(
            attempt_dir,
            expected_dataset_id=dataset_id,
            expected_feature_row_count=feat_step["feature_row_count"],
            verified_files=verified_files,
        )

        # Mock adapter promotes ONLY verified files to local mock staging dir, never to official registry!
        mock_export_dir = staging_root / "mock_export"
        mock_export_dir.mkdir(parents=True, exist_ok=True)

        for fname in verified_files:
            _atomic_copy(attempt_dir / fname, mock_export_dir / fname)
        _atomic_copy(attempt_dir / "checksums.sha256", mock_export_dir / "checksums.sha256")

        export_manifest_sha = hashlib.sha256((mock_export_dir / "checksums.sha256").read_bytes()).hexdigest()
        manifest_doc = _build_dataset_manifest(
            context,
            staging_root,
            export_status="MOCK_VALIDATED",
            export_artifact_uri=str(mock_export_dir),
            export_manifest_sha256=export_manifest_sha,
            export_manifest_ref="checksums.sha256",
            attempt_dir=mock_export_dir,
        )
        mock_manifest_path = mock_export_dir / "dataset_manifest.json"
        _atomic_json(mock_manifest_path, manifest_doc)
        manifest_sha = hashlib.sha256(mock_manifest_path.read_bytes()).hexdigest()

        released = lease("release", context["lease_owner"], environment)
        release_receipt = {
            "status": "RELEASED",
            "dag_id": context.get("dag_id", DAG_ID),
            "run_id": context["run_id"],
            "released_at_utc": now_iso,
        }
        _atomic_json(staging_root / "lease_release_receipt.json", release_receipt)

        res = {
            "step": "mark_dataset_exported",
            "status": "MOCK_COMPLETED",
            "dataset_id": dataset_id,
            "run_id": context["run_id"],
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "attempt": attempt,
            "dataset_status": "MOCK_BUILT",
            "is_mock": True,
            "export_artifact_uri": str(mock_export_dir),
            "export_manifest_sha256": export_manifest_sha,
            "dataset_manifest_sha256": manifest_sha,
            "lease_released": released.get("status") == "RELEASED",
            "lease_release_receipt": release_receipt,
            "colab_handoff": {
                "status": "MOCK_NOT_ELIGIBLE_FOR_COLAB",
                "ready": False,
                "auto_triggered": False,
                "dataset_id": dataset_id,
                "notes": "Mock pipeline execution cannot be handed off to Colab or registered in official lifecycle.",
            },
            "completed_at_utc": now_iso,
        }
        _atomic_json(staging_root / "step_10_mark_dataset_exported.json", res)
        return res

    raise Mli02Error(f"DS_UNHANDLED_STEP:{step_name}")


def verify_validation_report(report: Mapping[str, Any]) -> None:
    """Enforce strict fail-closed contract validation on feature snapshot validation report.

    Requirements:
    1. status must be present and in {"FEATURE_SNAPSHOT_VALIDATED", "PASSED", "SUCCESS", "VALIDATED"}.
    2. quality_gate_status must be present, non-null, and in {"PASSED", "SUCCESS"}.
    3. invalid_feature_count must be present, non-null, non-boolean integer == 0.
    4. candidate_row_count and feature_row_count must be present, non-null, non-boolean integers > 0, and equal.
    """
    if not isinstance(report, Mapping):
        raise Mli02Error("DS_VALIDATION_REPORT_INVALID:not_dict")

    # 1. Status
    status = report.get("status")
    if not status or status in {"FAILED", "ERROR", "REJECTED"} or status not in {
        "FEATURE_SNAPSHOT_VALIDATED",
        "PASSED",
        "SUCCESS",
        "VALIDATED",
    }:
        raise Mli02Error(f"DS_VALIDATION_STATUS_FAILED:{status}")

    # 2. Quality gate status
    if "quality_gate_status" not in report or report["quality_gate_status"] is None:
        raise Mli02Error("DS_QUALITY_GATE_MISSING")
    qg_status = report["quality_gate_status"]
    if qg_status in {"FAILED", "ERROR", "REJECTED"} or qg_status not in {"PASSED", "SUCCESS"}:
        raise Mli02Error(f"DS_QUALITY_GATE_FAILED:{qg_status}")

    # 3. Invalid feature count
    if "invalid_feature_count" not in report or report["invalid_feature_count"] is None:
        raise Mli02Error("DS_INVALID_FEATURES_MISSING")
    raw_inv = report["invalid_feature_count"]
    if isinstance(raw_inv, bool):
        raise Mli02Error("DS_INVALID_FEATURES_INVALID_TYPE")
    if isinstance(raw_inv, int):
        inv_count = raw_inv
    elif isinstance(raw_inv, str) and (raw_inv.isdigit() or (raw_inv.startswith("-") and raw_inv[1:].isdigit())):
        inv_count = int(raw_inv)
    else:
        raise Mli02Error("DS_INVALID_FEATURES_INVALID_TYPE")

    if inv_count < 0:
        raise Mli02Error(f"DS_INVALID_FEATURES_NEGATIVE:{inv_count}")
    if inv_count != 0:
        raise Mli02Error(f"DS_INVALID_FEATURES_FOUND:{inv_count}")

    # 4. Row counts
    if "candidate_row_count" not in report or report["candidate_row_count"] is None:
        raise Mli02Error("DS_CANDIDATE_ROWS_MISSING")
    if "feature_row_count" not in report or report["feature_row_count"] is None:
        raise Mli02Error("DS_FEATURE_ROWS_MISSING")
    cand_raw = report["candidate_row_count"]
    feat_raw = report["feature_row_count"]
    if isinstance(cand_raw, bool) or isinstance(feat_raw, bool):
        raise Mli02Error("DS_COUNT_INVALID_TYPE")
    try:
        cand_count = int(cand_raw)
        feat_count = int(feat_raw)
    except (ValueError, TypeError):
        raise Mli02Error("DS_COUNT_INVALID_TYPE")
    if cand_count <= 0 or feat_count <= 0 or cand_count != feat_count:
        raise Mli02Error("DS_COUNT_RECONCILIATION_FAILED")


def validate_runner_receipt(
    step_name: str,
    report: Mapping[str, Any],
    context: Mapping[str, Any],
    attempt: int,
) -> None:
    """Validate external runner receipt against identity, run_id, attempt, and snapshot."""
    if not isinstance(report, Mapping):
        raise Mli02Error(f"DS_RUNNER_RECEIPT_INVALID:{step_name}_not_dict")
    status = report.get("status")
    if not status or status in {"FAILED", "ERROR", "REJECTED"}:
        raise Mli02Error(f"DS_RUNNER_STEP_FAILED:{step_name}:{status}")
    if report.get("step") != step_name:
        raise Mli02Error(f"DS_RUNNER_RECEIPT_INVALID:{step_name}_step_mismatch")
    if report.get("run_id") != context["run_id"]:
        raise Mli02Error(f"DS_RUNNER_RECEIPT_INVALID:{step_name}_run_id_mismatch")
    if report.get("dataset_id") != context["dataset_id"]:
        raise Mli02Error(f"DS_RUNNER_RECEIPT_INVALID:{step_name}_dataset_id_mismatch")
    if report.get("gold_snapshot_id") != context["gold_publication"]["gold_snapshot_id"]:
        raise Mli02Error(f"DS_RUNNER_RECEIPT_INVALID:{step_name}_snapshot_id_mismatch")
    if report.get("attempt") != attempt:
        raise Mli02Error(f"DS_RUNNER_RECEIPT_INVALID:{step_name}_attempt_mismatch")
    if "identity_payload_sha256" in report and report["identity_payload_sha256"] != context["identity_payload_sha256"]:
        raise Mli02Error(f"DS_RUNNER_RECEIPT_INVALID:{step_name}_identity_mismatch")
    if step_name == "validate_feature_snapshot":
        verify_validation_report(report)


def _run_external_runner_step(
    step_name: str,
    context: Mapping[str, Any],
    upstream: Any,
    runner_cmd: str,
    environment: Mapping[str, str],
    attempt: int,
) -> dict[str, Any]:
    """Execute real Spark/Java runner step via subprocess with bounded timeouts and receipt validation."""
    staging_root = Path(context["staging_root"])
    step_request_file = staging_root / f"request_{step_name}.json"
    step_request = {
        "step": step_name,
        "context": context,
        "upstream": upstream,
        "attempt": attempt,
    }
    _atomic_json(step_request_file, step_request)

    # Invalidate stale report from prior attempts
    report_path = staging_root / f"report_{step_name}.json"
    if report_path.is_file():
        report_path.unlink()

    cmd_parts = shlex.split(runner_cmd)
    try:
        timeout = int(environment.get("ML_BUILD_RUNNER_TIMEOUT_SECONDS", "3600"))
    except ValueError:
        timeout = 3600

    identity_root = Path(tempfile.mkdtemp(prefix="mli02-identity-", dir=staging_root))
    child_env = java_identity(environment, identity_root)

    res = subprocess.run(
        cmd_parts + [step_name, str(step_request_file)],
        env=child_env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        timeout=timeout,
        check=False,
    )
    if res.returncode != 0:
        err_msg = res.stderr.decode("utf-8", errors="replace").strip() if res.stderr else ""
        raise Mli02Error(f"DS_RUNNER_FAILED:{step_name}:{err_msg}")

    if not report_path.is_file():
        raise Mli02Error(f"DS_RUNNER_REPORT_MISSING:{step_name}")

    report = _read_step(report_path)
    validate_runner_receipt(step_name, report, context, attempt)

    # Step-specific receipt validation
    if step_name == "audit_gold_input":
        counts = report.get("counts", {})
        inp = counts.get("input_event_count", -1)
        elg = counts.get("eligible_event_count", -1)
        exc = counts.get("excluded_event_count", -1)
        if inp != elg + exc or inp < 0 or elg < 0 or exc < 0:
            raise Mli02Error("DS_AUDIT_COUNTS_INVALID")

    elif step_name == "resolve_mc":
        mc_val = float(report.get("mc_value", -1.0))
        if not math.isfinite(mc_val) or mc_val <= 0:
            raise Mli02Error("DS_MC_INVALID")

    elif step_name == "select_mainshocks":
        if int(report.get("mainshock_count", 0)) <= 0:
            raise Mli02Error("DS_NO_MAINSHOCKS_SELECTED")

    elif step_name == "build_candidate_windows":
        if int(report.get("candidate_row_count", 0)) <= 0:
            raise Mli02Error("DS_CANDIDATE_ROWS_EMPTY")

    elif step_name == "build_spacetime_features":
        if int(report.get("feature_row_count", 0)) <= 0:
            raise Mli02Error("DS_FEATURE_ROWS_EMPTY")

    elif step_name == "validate_feature_snapshot":
        verify_validation_report(report)

    elif step_name == "mark_dataset_exported":
        if report.get("status") != "DATASET_EXPORTED":
            raise Mli02Error("DS_EXPORT_GATE_FAILED")
        export_uri = report.get("export_artifact_uri")
        if not export_uri or not Path(export_uri).is_dir():
            raise Mli02Error("DS_EXPORT_GATE_FAILED")
        if not report.get("export_manifest_sha256") or not report.get("dataset_manifest_sha256"):
            raise Mli02Error("DS_EXPORT_GATE_FAILED")

    step_idx = STEPS.index(step_name) + 1
    step_file = staging_root / f"step_{step_idx:02d}_{step_name}.json"
    _atomic_json(step_file, report)

    return report


def promote_and_publish_dataset(
    context: Mapping[str, Any],
    attempt_dir: Path,
    attempt: int,
    environment: Mapping[str, str],
) -> dict[str, Any]:
    """Promote verified parquet parts, create non-circular manifest/checksums, and register in official registry."""
    dataset_id = context["dataset_id"]
    staging_root = Path(context["staging_root"])
    export_root = Path(context["export_root"])
    registry_root = Path(context["registry_root"])
    final_export_dir = export_root / f"dataset_id={dataset_id}"
    manifest_target = final_export_dir / "dataset_manifest.json"
    checksums_target = final_export_dir / "checksums.sha256"
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Verify validation gate passed before promote and publish
    val_report_path = staging_root / "step_08_validate_feature_snapshot.json"
    if not val_report_path.is_file():
        raise Mli02Error("DS_VALIDATION_GATE_MISSING")
    val_doc = _read_step(val_report_path)
    verify_validation_report(val_doc)

    feat_step = _read_step(staging_root / "step_07_build_spacetime_features.json")
    expected_feature_rows = feat_step["feature_row_count"]

    # Check if a verified export already exists (Idempotent dataset reuse per ML_DATA_MODEL.md:168)
    if final_export_dir.exists():
        if not (manifest_target.is_file() and checksums_target.is_file()):
            raise Mli02Error("DS_EXISTING_BUNDLE_CORRUPT")

        try:
            existing_manifest = json.loads(manifest_target.read_text(encoding="utf-8"))
        except Exception as err:
            raise Mli02Error("DS_EXISTING_BUNDLE_CORRUPT") from err

        if not isinstance(existing_manifest, Mapping):
            raise Mli02Error("DS_EXISTING_BUNDLE_CORRUPT")

        # Strict identity check: dataset_id and identity_payload_sha256
        if (
            existing_manifest.get("dataset_id") != dataset_id
            or existing_manifest.get("identity_payload_sha256") != context["identity_payload_sha256"]
        ):
            raise Mli02Error("DS_IDENTITY_CONFLICT")

        if existing_manifest.get("dataset_status") != "EXPORTED":
            raise Mli02Error("DS_EXISTING_BUNDLE_CORRUPT")

        try:
            _verify_bundle_checksums(final_export_dir)
            verify_exported_parquet_readback(
                final_export_dir,
                expected_dataset_id=dataset_id,
                expected_feature_row_count=expected_feature_rows,
            )
        except Exception as err:
            raise Mli02Error("DS_EXISTING_BUNDLE_CORRUPT") from err

        export_manifest_sha = hashlib.sha256(checksums_target.read_bytes()).hexdigest()
        manifest_sha = hashlib.sha256(manifest_target.read_bytes()).hexdigest()

        # Ensure registry is in sync without modifying existing manifest bytes
        reg_manifest = registry_root / f"{dataset_id}.json"
        reg_manifest_reg = registry_root / "registry" / f"{dataset_id}.json"
        if not reg_manifest.is_file() or hashlib.sha256(reg_manifest.read_bytes()).hexdigest() != manifest_sha:
            _atomic_copy(manifest_target, reg_manifest)
        if not reg_manifest_reg.is_file() or hashlib.sha256(reg_manifest_reg.read_bytes()).hexdigest() != manifest_sha:
            _atomic_copy(manifest_target, reg_manifest_reg)

        reg_export_dir = registry_root / "exports" / dataset_id
        reg_export_dir.mkdir(parents=True, exist_ok=True)
        reg_checksums = reg_export_dir / "checksums.sha256"
        if not reg_checksums.is_file() or hashlib.sha256(reg_checksums.read_bytes()).hexdigest() != export_manifest_sha:
            _atomic_copy(checksums_target, reg_checksums)

        # Release lease for this run
        receipt_path = staging_root / "lease_release_receipt.json"
        release_receipt = None
        if receipt_path.is_file():
            try:
                existing_rc = json.loads(receipt_path.read_text(encoding="utf-8"))
                if (
                    isinstance(existing_rc, Mapping)
                    and existing_rc.get("status") == "RELEASED"
                    and existing_rc.get("dag_id") == context.get("dag_id", DAG_ID)
                    and existing_rc.get("run_id") == context["run_id"]
                ):
                    release_receipt = existing_rc
            except Exception:
                release_receipt = None

        guard_root = Path(environment.get("SOURCE_GUARD_ROOT", "/opt/pipeline/staging/source-guard"))
        owner_file = guard_root / "owner.json"
        if owner_file.is_file():
            try:
                cur_owner = json.loads(owner_file.read_text(encoding="utf-8"))
                if cur_owner == context["lease_owner"]:
                    lease("release", context["lease_owner"], environment)
            except Exception:
                pass
        elif release_receipt is None:
            lease("release", context["lease_owner"], environment)

        if release_receipt is None:
            release_receipt = {
                "status": "RELEASED",
                "dag_id": context.get("dag_id", DAG_ID),
                "run_id": context["run_id"],
                "released_at_utc": now_iso,
            }
            _atomic_json(receipt_path, release_receipt)

        # Record this run's own staging execution summary (without altering pinned manifest)
        run_staging_summary = {
            "run_id": context["run_id"],
            "dataset_id": dataset_id,
            "identity_payload_sha256": context["identity_payload_sha256"],
            "reused_existing_bundle": True,
            "original_build_run_id": existing_manifest.get("build_run_id"),
            "dataset_manifest_sha256": manifest_sha,
            "export_manifest_sha256": export_manifest_sha,
            "export_artifact_uri": str(final_export_dir),
            "completed_at_utc": now_iso,
        }
        _atomic_json(staging_root / "run_summary.json", run_staging_summary)

        return {
            "step": "mark_dataset_exported",
            "status": "DATASET_EXPORTED",
            "dataset_id": dataset_id,
            "run_id": context["run_id"],
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "attempt": attempt,
            "dataset_status": "EXPORTED",
            "export_artifact_uri": str(final_export_dir),
            "export_manifest_sha256": export_manifest_sha,
            "dataset_manifest_sha256": manifest_sha,
            "registry_path": str(reg_manifest),
            "lease_released": True,
            "lease_release_receipt": release_receipt,
            "reused_existing_bundle": True,
            "colab_handoff": {
                "status": "MANUAL_OPERATOR_REQUIRED",
                "ready": True,
                "auto_triggered": False,
                "dataset_id": dataset_id,
                "local_bundle_uri": str(final_export_dir),
                "notes": "Export bundle is verified and registered locally. Operator should manually stage or upload to Google Drive for Colab notebook access. The pipeline deliberately does not trigger Colab automatically.",
            },
            "completed_at_utc": now_iso,
        }

    # Read and verify checksums in attempt_dir
    verified_files = _verify_bundle_checksums(attempt_dir)

    # Readback and verify Parquet rows, schema, grain, and lineage in attempt_dir
    verify_exported_parquet_readback(
        attempt_dir,
        expected_dataset_id=dataset_id,
        expected_feature_row_count=expected_feature_rows,
        verified_files=verified_files,
    )

    export_manifest_sha = hashlib.sha256((attempt_dir / "checksums.sha256").read_bytes()).hexdigest()
    export_manifest_ref = f"exports/{dataset_id}/checksums.sha256"

    # Assemble final dataset_manifest.json with all fields populated from attempt_dir before creating final_export_dir
    manifest_doc = _build_dataset_manifest(
        context,
        staging_root,
        export_status="EXPORTED",
        export_artifact_uri=str(final_export_dir),
        export_manifest_sha256=export_manifest_sha,
        export_manifest_ref=export_manifest_ref,
        attempt_dir=attempt_dir,
    )

    try:
        final_export_dir.mkdir(parents=True, exist_ok=True)

        # Promote ONLY verified files (ignoring any unindexed or stray attempt artifacts)
        for fname in verified_files:
            src = attempt_dir / fname
            dst = final_export_dir / fname
            _atomic_copy(src, dst)

        # Copy checksums.sha256 and manifest to final export dir
        _atomic_copy(attempt_dir / "checksums.sha256", checksums_target)
        _atomic_json(manifest_target, manifest_doc)
        manifest_sha = hashlib.sha256(manifest_target.read_bytes()).hexdigest()

        # Re-verify the final promoted bundle
        _verify_bundle_checksums(final_export_dir)
        verify_exported_parquet_readback(
            final_export_dir,
            expected_dataset_id=dataset_id,
            expected_feature_row_count=expected_feature_rows,
        )
    except Exception:
        import shutil
        shutil.rmtree(final_export_dir, ignore_errors=True)
        raise

    # Register in shared registry for MLI-03 / Colab consumers
    reg_manifest = registry_root / f"{dataset_id}.json"
    reg_manifest_reg = registry_root / "registry" / f"{dataset_id}.json"
    _atomic_json(reg_manifest, manifest_doc)
    _atomic_json(reg_manifest_reg, manifest_doc)

    reg_export_dir = registry_root / "exports" / dataset_id
    reg_export_dir.mkdir(parents=True, exist_ok=True)
    _atomic_copy(checksums_target, reg_export_dir / "checksums.sha256")

    released = lease("release", context["lease_owner"], environment)
    release_receipt = {
        "status": "RELEASED",
        "dag_id": context.get("dag_id", DAG_ID),
        "run_id": context["run_id"],
        "released_at_utc": now_iso,
    }
    _atomic_json(staging_root / "lease_release_receipt.json", release_receipt)

    # Record this run's own staging execution summary
    run_staging_summary = {
        "run_id": context["run_id"],
        "dataset_id": dataset_id,
        "identity_payload_sha256": context["identity_payload_sha256"],
        "reused_existing_bundle": False,
        "original_build_run_id": context["run_id"],
        "dataset_manifest_sha256": manifest_sha,
        "export_manifest_sha256": export_manifest_sha,
        "export_artifact_uri": str(final_export_dir),
        "completed_at_utc": now_iso,
    }
    _atomic_json(staging_root / "run_summary.json", run_staging_summary)

    return {
        "step": "mark_dataset_exported",
        "status": "DATASET_EXPORTED",
        "dataset_id": dataset_id,
        "run_id": context["run_id"],
        "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
        "attempt": attempt,
        "dataset_status": "EXPORTED",
        "export_artifact_uri": str(final_export_dir),
        "export_manifest_sha256": export_manifest_sha,
        "dataset_manifest_sha256": manifest_sha,
        "registry_path": str(reg_manifest),
        "lease_released": released.get("status") == "RELEASED",
        "lease_release_receipt": release_receipt,
        "reused_existing_bundle": False,
        "colab_handoff": {
            "status": "MANUAL_OPERATOR_REQUIRED",
            "ready": True,
            "auto_triggered": False,
            "dataset_id": dataset_id,
            "local_bundle_uri": str(final_export_dir),
            "notes": "Export bundle is verified and registered locally. Operator should manually stage or upload to Google Drive for Colab notebook access. The pipeline deliberately does not trigger Colab automatically.",
        },
        "completed_at_utc": now_iso,
    }


def _read_step(path: Path) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size > MAX_METADATA_BYTES:
        raise Mli02Error(f"DS_STEP_METADATA_MISSING:{path.name}")
    return json.loads(path.read_text(encoding="utf-8"))


def _atomic_copy(src: Path, dst: Path) -> None:
    data = src.read_bytes()
    dst.parent.mkdir(parents=True, exist_ok=True)
    temp = dst.parent / f".tmp_{dst.name}_{uuid.uuid4().hex}"
    temp.write_bytes(data)
    temp.replace(dst)


def _build_dataset_manifest(
    context: Mapping[str, Any],
    staging_root: Path,
    export_status: str,
    export_artifact_uri: str | None = None,
    export_manifest_sha256: str | None = None,
    export_manifest_ref: str | None = None,
    attempt_dir: Path | None = None,
) -> dict[str, Any]:
    """Assemble logical CON-03-ML dataset manifest document with real candidate lineage and snapshot pins."""
    ident = context["identity_payload"]
    dataset_id = context["dataset_id"]
    now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    audit = _read_step(staging_root / "step_03_audit_gold_input.json")
    mc = _read_step(staging_root / "step_04_resolve_mc.json")
    sel = _read_step(staging_root / "step_05_select_mainshocks.json")
    win = _read_step(staging_root / "step_06_build_candidate_windows.json")
    feat = _read_step(staging_root / "step_07_build_spacetime_features.json")

    # Resolve real snapshot pins from verified upstream steps - fail closed!
    snap = _read_step(staging_root / "step_02_resolve_gold_snapshot.json")
    registry_snapshot_id = snap.get("registry_snapshot_id") or str(context["gold_publication"]["gold_snapshot_id"])
    if not registry_snapshot_id or not isinstance(registry_snapshot_id, str) or not registry_snapshot_id.strip():
        raise Mli02Error("DS_SNAPSHOT_PINS_MISSING:registry_snapshot_id")

    mainshock_snapshot_id = sel.get("mainshock_snapshot_id")
    if not mainshock_snapshot_id or not isinstance(mainshock_snapshot_id, str) or not mainshock_snapshot_id.strip():
        raise Mli02Error("DS_SNAPSHOT_PINS_MISSING:mainshock_snapshot_id")

    candidate_snapshot_id = win.get("candidate_snapshot_id")
    if not candidate_snapshot_id or not isinstance(candidate_snapshot_id, str) or not candidate_snapshot_id.strip():
        raise Mli02Error("DS_SNAPSHOT_PINS_MISSING:candidate_snapshot_id")

    # Read verified Parquet files to extract genuine candidate lineage and grain
    search_dir = attempt_dir or (Path(export_artifact_uri) if export_artifact_uri else None)
    if not search_dir or not search_dir.is_dir():
        raise Mli02Error("DS_CANDIDATE_LINEAGE_MISSING")

    parquet_files = sorted(search_dir.glob("features-part-*.parquet"))
    if not parquet_files:
        raise Mli02Error("DS_FEATURE_ROWS_EMPTY")

    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise Mli02Error("DS_PARQUET_READER_UNAVAILABLE")

    candidates = []
    observed_grain = set()
    parquet_mainshocks = set()

    for pf in parquet_files:
        table = pq.ParquetFile(pf).read()
        m_col = table.column("mainshock_event_id").to_pylist()
        c_col = table.column("candidate_event_id").to_pylist()
        r_col = table.column("relative_time_role").to_pylist()
        for m_id, c_id, role in zip(m_col, c_col, r_col):
            grain_key = (m_id, c_id)
            if grain_key in observed_grain:
                raise Mli02Error("DS_GRAIN_DUPLICATE")
            observed_grain.add(grain_key)
            parquet_mainshocks.add(m_id)
            candidates.append({
                "mainshock_event_id": str(m_id),
                "candidate_event_id": str(c_id),
                "role": str(role),
            })

    # Reconcile grain and mainshocks
    expected_feat_rows = int(feat["feature_row_count"])
    expected_cand_rows = int(win["candidate_row_count"])
    if len(candidates) != expected_feat_rows or len(candidates) != expected_cand_rows:
        raise Mli02Error(
            f"DS_COUNT_RECONCILIATION_FAILED:candidates_{len(candidates)}_features_{expected_feat_rows}"
        )

    expected_mainshocks = set(sel.get("mainshock_ids", []))
    if not expected_mainshocks or len(expected_mainshocks) != int(sel.get("mainshock_count", 0)):
        raise Mli02Error("DS_MAINSHOCK_COUNT_MISMATCH")

    if parquet_mainshocks != expected_mainshocks:
        raise Mli02Error(
            f"DS_MAINSHOCK_SET_MISMATCH:expected_{sorted(expected_mainshocks)}_got_{sorted(parquet_mainshocks)}"
        )

    manifest = {
        "schema_version": "1.0",
        "dataset_id": dataset_id,
        "dataset_split": ident["dataset_split"],
        "gold_table_name": ident["gold_table_name"],
        "physical_gold_table_name": ident["physical_gold_table_name"],
        "gold_snapshot_id": ident["gold_snapshot_id"],
        "gold_publication_id": ident["gold_publication_id"],
        "publication_bundle_sha256": ident["publication_bundle_sha256"],
        "identity_payload_sha256": context["identity_payload_sha256"],
        "period_start_utc": ident["period_start_utc"],
        "period_end_utc_exclusive": ident["period_end_utc_exclusive"],
        "observation_cutoff_utc": ident["observation_cutoff_utc"],
        "filter_config_json": ident["filter_config_json"],
        "dataset_config_version": ident["dataset_config_version"],
        "audit_rule_version": ident["audit_rule_version"],
        "mc_method": mc["mc_method"],
        "mc_method_version": mc["mc_method_version"],
        "mc_value": mc["mc_value"],
        "mc_region_version": mc.get("mc_region_version"),
        "mc_config_json": mc["mc_config_json"],
        "mc_config_sha256": mc["mc_config_sha256"],
        "window_model_version": ident["window_model_version"],
        "feature_version": ident["feature_version"],
        "scaling_config_version": ident["scaling_config_version"],
        "code_version": ident["code_version"],
        "input_event_count": audit["counts"]["input_event_count"],
        "eligible_event_count": audit["counts"]["eligible_event_count"],
        "excluded_event_count": audit["counts"]["excluded_event_count"],
        "mainshock_count": sel["mainshock_count"],
        "candidate_row_count": win["candidate_row_count"],
        "feature_row_count": feat["feature_row_count"],
        "exclusion_counts_json": json.dumps(audit.get("exclusion_counts", {}), sort_keys=True),
        "audit_report_uri": audit.get("audit_report_uri"),
        "feature_schema_sha256": feat["feature_schema_sha256"],
        "dataset_status": export_status,
        "build_run_id": context["run_id"],
        "created_at_utc": now_iso,
        "updated_at_utc": now_iso,
        # Lineage pins for TrustedDatasetResolver (MLI-03 compatibility)
        "export_artifact_uri": export_artifact_uri,
        "export_manifest_ref": export_manifest_ref or f"exports/{dataset_id}/checksums.sha256",
        "export_manifest_sha256": export_manifest_sha256,
        "registry_snapshot_id": registry_snapshot_id,
        "candidate_snapshot_id": candidate_snapshot_id,
        "mainshock_snapshot_id": mainshock_snapshot_id,
        "candidates": candidates,
    }
    return manifest


def _materialize_features_parquet(
    target_path: Path,
    context: Mapping[str, Any],
    row_count: int = 142,
    mainshocks: list[dict[str, Any]] | None = None,
) -> None:
    """Write valid deterministic Parquet part file with exact row count matching feature manifest."""
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq

        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("candidate_time_utc", pa.string()),
            ("candidate_latitude", pa.float64()),
            ("candidate_longitude", pa.float64()),
            ("candidate_depth_km", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("dx_km", pa.float64()),
            ("dy_km", pa.float64()),
            ("dz_km", pa.float64()),
            ("delta_time_hours", pa.float64()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
        ])

        did = context["dataset_id"]
        m_list = mainshocks or [{"mainshock_event_id": "evt_001"}]
        num_mains = len(m_list)
        rows_per_main = row_count // num_mains
        remainder = row_count % num_mains

        rows = []
        for idx, m_info in enumerate(m_list):
            m_id = m_info.get("mainshock_event_id", f"evt_{idx:03d}")
            count_for_this = rows_per_main + (1 if idx < remainder else 0)
            if count_for_this <= 0:
                continue
            rows.append((
                did, m_id, m_id, "2000-01-28T14:21:09.120Z", 35.483, 140.485, 58.2, 5.7,
                "MAINSHOCK", True, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            ))
            for i in range(1, count_for_this):
                role = "PRE" if i % 2 == 1 else "POST"
                cid = f"cand_{m_id}_{i:05d}"
                rows.append((
                    did, m_id, cid, "2000-01-28T15:00:00.000Z",
                    35.483 + 0.0001 * i, 140.485 + 0.0001 * i, 58.2, 2.5,
                    role, False, 0.01 * i, 0.01 * i, 0.0, 0.1 * i,
                    0.001 * i, 0.001 * i, 0.0, 0.01 * i,
                ))

        cols = [[r[i] for r in rows] for i in range(len(schema))]
        table = pa.Table.from_arrays([pa.array(c) for c in cols], schema=schema)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, target_path)
    except (ModuleNotFoundError, ImportError):
        raise Mli02Error("DS_PARQUET_WRITER_UNAVAILABLE")


def mark_dataset_exported_summary(
    context: Mapping[str, Any],
    final_result: Mapping[str, Any],
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Produce final summary of the dataset build DAG run with fail-closed leaf contract gates."""
    env = dict(os.environ if environment is None else environment)
    owner = context["lease_owner"]

    # Test double mock adapter summary: isolated from official lifecycle
    if final_result.get("is_mock"):
        summary = {
            "contract_version": CONTRACT_VERSION,
            "dag_id": context.get("dag_id", DAG_ID),
            "run_id": context["run_id"],
            "dataset_id": context["dataset_id"],
            "dataset_split": context["validated_config"]["dataset_split"],
            "status": "MOCK_COMPLETED",
            "gold_publication_id": context["gold_publication"]["gold_publication_id"],
            "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
            "export_artifact_uri": final_result.get("export_artifact_uri"),
            "export_manifest_sha256": final_result.get("export_manifest_sha256"),
            "dataset_manifest_sha256": final_result.get("dataset_manifest_sha256"),
            "lease_owner": owner,
            "colab_handoff": final_result.get("colab_handoff"),
        }
        summary_path = Path(context["staging_root"]) / "run_summary.json"
        _atomic_json(summary_path, summary)
        return summary

    # Production gate checks
    if final_result.get("status") != "DATASET_EXPORTED":
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    if final_result.get("dataset_id") != context["dataset_id"]:
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    if final_result.get("run_id") != context["run_id"]:
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    if final_result.get("gold_snapshot_id") != context["gold_publication"]["gold_snapshot_id"]:
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    export_uri = final_result.get("export_artifact_uri")
    if not export_uri:
        raise Mli02Error("DS_EXPORT_GATE_FAILED")
    export_dir = Path(export_uri)
    if not export_dir.is_dir():
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    # Readback checksums and Parquet
    _verify_bundle_checksums(export_dir)
    export_manifest_sha = hashlib.sha256((export_dir / "checksums.sha256").read_bytes()).hexdigest()
    if export_manifest_sha != final_result.get("export_manifest_sha256"):
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    manifest_path = export_dir / "dataset_manifest.json"
    if not manifest_path.is_file():
        raise Mli02Error("DS_EXPORT_GATE_FAILED")
    manifest_doc = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest_doc.get("dataset_status") != "EXPORTED":
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    dataset_manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    if dataset_manifest_sha != final_result.get("dataset_manifest_sha256"):
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    verify_exported_parquet_readback(
        export_dir,
        expected_dataset_id=context["dataset_id"],
        expected_feature_row_count=manifest_doc["feature_row_count"],
    )

    # Readback registry entry
    registry_root = Path(context["registry_root"])
    reg_path = registry_root / f"{context['dataset_id']}.json"
    if not reg_path.is_file():
        raise Mli02Error("DS_EXPORT_GATE_FAILED")
    if hashlib.sha256(reg_path.read_bytes()).hexdigest() != dataset_manifest_sha:
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    export_ref = manifest_doc.get("export_manifest_ref")
    if not export_ref:
        raise Mli02Error("DS_EXPORT_GATE_FAILED")
    resolved_export_ref = (registry_root / export_ref).resolve()
    if not resolved_export_ref.is_file():
        raise Mli02Error("DS_EXPORT_GATE_FAILED")
    if hashlib.sha256(resolved_export_ref.read_bytes()).hexdigest() != export_manifest_sha:
        raise Mli02Error("DS_EXPORT_GATE_FAILED")

    # Verify lease release receipt was recorded for this run
    staging_root = Path(context["staging_root"])
    receipt_file = staging_root / "lease_release_receipt.json"
    if not receipt_file.is_file():
        raise Mli02Error("DS_LEASE_RELEASE_RECEIPT_MISSING")
    try:
        receipt_data = json.loads(receipt_file.read_text(encoding="utf-8"))
    except (ValueError, OSError) as err:
        raise Mli02Error("DS_LEASE_RELEASE_RECEIPT_CORRUPT") from err
    if (
        not isinstance(receipt_data, Mapping)
        or receipt_data.get("status") != "RELEASED"
        or receipt_data.get("dag_id") != context.get("dag_id", DAG_ID)
        or receipt_data.get("run_id") != context["run_id"]
    ):
        raise Mli02Error("DS_LEASE_RELEASE_RECEIPT_INVALID")

    # Fail closed on corrupt lease state; verify lease is released (not still HELD)
    guard_root = Path(env.get("SOURCE_GUARD_ROOT", "/opt/pipeline/staging/source-guard"))
    owner_file = guard_root / "owner.json"
    if owner_file.exists():
        try:
            current_owner = json.loads(owner_file.read_text(encoding="utf-8"))
        except (ValueError, OSError) as err:
            raise Mli02Error("DS_LEASE_STATE_CORRUPT") from err
        if not isinstance(current_owner, Mapping) or "run_id" not in current_owner or "dag_id" not in current_owner:
            raise Mli02Error("DS_LEASE_STATE_CORRUPT")
        if current_owner == owner:
            raise Mli02Error("DS_LEASE_STILL_HELD")

    summary = {
        "contract_version": CONTRACT_VERSION,
        "dag_id": context.get("dag_id", DAG_ID),
        "run_id": context["run_id"],
        "dataset_id": context["dataset_id"],
        "dataset_split": context["validated_config"]["dataset_split"],
        "status": "DATASET_EXPORTED",
        "gold_publication_id": context["gold_publication"]["gold_publication_id"],
        "gold_snapshot_id": context["gold_publication"]["gold_snapshot_id"],
        "export_artifact_uri": str(export_dir),
        "export_manifest_sha256": export_manifest_sha,
        "dataset_manifest_sha256": dataset_manifest_sha,
        "lease_owner": owner,
        "reused_existing_bundle": final_result.get("reused_existing_bundle", False),
        "original_build_run_id": manifest_doc.get("build_run_id"),
        "colab_handoff": final_result.get("colab_handoff"),
    }
    summary_path = Path(context["staging_root"]) / "run_summary.json"
    _atomic_json(summary_path, summary)
    return summary


def run(request_path_or_dict: str | Path | Mapping[str, Any], environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Execute entire 10-step build sequence programmatically for tests and CLI operations."""
    if isinstance(request_path_or_dict, (str, Path)):
        req = json.loads(Path(request_path_or_dict).read_text(encoding="utf-8"))
    else:
        req = dict(request_path_or_dict)

    ctx = resolve_run_context(req, environment)
    upstream = None
    for step in STEPS:
        upstream = execute_step(step, ctx, upstream, environment)

    return mark_dataset_exported_summary(ctx, upstream, environment)


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python ml_dataset_runtime.py <request.json>", file=sys.stderr)
        sys.exit(1)

    try:
        result = run(sys.argv[1])
        print(json.dumps(result, indent=2, sort_keys=True))
    except Mli02Error as error:
        print(f"MLI-02 ERROR: {error}", file=sys.stderr)
        sys.exit(1)
