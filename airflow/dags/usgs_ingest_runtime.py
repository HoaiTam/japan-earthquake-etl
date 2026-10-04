"""Runtime boundary for the USG-04 Airflow task group.

The DAG deliberately keeps the HTTP client and Bronze writer outside Airflow.
An executable configured by ``USGS_INGEST_RUNNER_COMMAND`` receives a small
JSON context file for each phase and returns one JSON summary line. This keeps
payload bytes out of XCom/logs while allowing the Java USG-02/USG-03
implementation to be invoked from the deployment-specific runner.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
from typing import Any, Mapping


UTC = timezone.utc
PHASES = ("fetch", "validate", "upload", "verify")
SAFE_RUN_ID = re.compile(r"[^A-Za-z0-9._~-]+")
PUBLIC_RESULT_KEYS = {
    "phase",
    "status",
    "run_id",
    "bronze_status",
    "valid",
    "verified",
    "manifest_uri",
    "raw_object_uri",
    "record_count_estimate",
    "sha256",
    "artifact_uri",
    "quarantine_uri",
    "idempotent_reuse",
}


class UsgsRunnerError(RuntimeError):
    """Raised when the configured USGS phase runner cannot complete safely."""


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _as_utc(value: Any) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if hasattr(value, "in_timezone"):
        value = value.in_timezone("UTC")
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _conf_value(context: Mapping[str, Any], key: str, default: Any = None) -> Any:
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None)
    if conf is None and isinstance(dag_run, Mapping):
        conf = dag_run.get("conf")
    if not isinstance(conf, Mapping):
        conf = {}
    return conf.get(key, default)


def _int_env(environment: Mapping[str, str], key: str, default: int) -> int:
    value = environment.get(key)
    if value in (None, ""):
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise UsgsRunnerError(f"{key} must be an integer") from exc


def _bool_value(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _safe_run_id(run_id: str) -> str:
    safe = SAFE_RUN_ID.sub("-", run_id).strip("-")
    return safe[:180] or "airflow-run"


def resolve_run_context(
    context: Mapping[str, Any], environment: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """Resolve one deterministic UTC window from Airflow's run context."""

    env = os.environ if environment is None else environment
    seed_value = env.get("USGS_SEED_START_UTC", "2023-01-01T00:00:00Z")
    seed_start = _as_utc(seed_value)
    overlap_days = _int_env(env, "PIPELINE_OVERLAP_DAYS", 3)
    is_backfill = _bool_value(_conf_value(context, "is_backfill", False))
    requested_start = _conf_value(context, "window_start_utc")
    requested_end = _conf_value(context, "window_end_utc")
    if requested_start is not None or requested_end is not None:
        if not is_backfill:
            raise UsgsRunnerError(
                "explicit USGS window requires is_backfill=true"
            )
        if requested_start is None or requested_end is None:
            raise UsgsRunnerError(
                "window_start_utc and window_end_utc must be supplied together"
            )
        if not str(requested_start).endswith("Z") or not str(requested_end).endswith("Z"):
            raise UsgsRunnerError("explicit USGS window must use UTC Z timestamps")
        target_start = _as_utc(requested_start)
        target_end = _as_utc(requested_end)
        query_start = target_start
    else:
        run_at = _as_utc(context.get("data_interval_end") or context.get("logical_date"))
        current_day_start = run_at.replace(hour=0, minute=0, second=0, microsecond=0)
        target_end = current_day_start
        target_start = target_end - timedelta(days=1)
        query_start = max(seed_start, target_end - timedelta(days=overlap_days))
    if query_start < seed_start:
        raise UsgsRunnerError("USGS window starts before USGS_SEED_START_UTC")
    if not target_start < target_end:
        raise UsgsRunnerError("USGS target window must be non-empty")

    run_id = str(context.get("run_id") or "manual-usgs-run")
    logical_run_key = (
        f"USGS|{_iso(query_start)}|{_iso(target_end)}|"
        f"{'backfill' if is_backfill else 'daily'}"
    )
    return {
        "dag_id": str(context.get("dag_id") or "usg_04_usgs_ingest"),
        "run_id": run_id,
        "run_id_path": _safe_run_id(run_id),
        "window_start_utc": _iso(query_start),
        "window_end_utc": _iso(target_end),
        "target_window_start_utc": _iso(target_start),
        "target_window_end_utc": _iso(target_end),
        "processing_date": target_start.date().isoformat(),
        "is_backfill": is_backfill,
        "logical_run_key": logical_run_key,
        "config_version": env.get("CONFIG_VERSION", "1"),
        "revision_overlap_days": overlap_days,
        "explicit_window": requested_start is not None,
    }


def _staging_root(environment: Mapping[str, str]) -> Path:
    return Path(environment.get("USGS_STAGING_ROOT", "/opt/pipeline/staging/usgs"))


def _write_context_file(
    phase: str,
    run_context: Mapping[str, Any],
    upstream: Mapping[str, Any] | None,
    environment: Mapping[str, str],
) -> Path:
    root = _staging_root(environment) / str(run_context["run_id_path"])
    root.mkdir(parents=True, exist_ok=True)
    target = root / f"{phase}-input.json"
    payload = {
        "phase": phase,
        "run_context": dict(run_context),
        "upstream": dict(upstream or {}),
    }
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=root, prefix=f".{phase}-", delete=False
    ) as temporary:
        json.dump(payload, temporary, sort_keys=True, separators=(",", ":"))
        temporary.write("\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, target)
    return target


def _public_result(phase: str, result: Mapping[str, Any]) -> dict[str, Any]:
    if result.get("phase") not in (None, phase):
        raise UsgsRunnerError(f"runner returned phase {result.get('phase')!r}, expected {phase!r}")
    public = {key: value for key, value in result.items() if key in PUBLIC_RESULT_KEYS}
    public["phase"] = phase
    if public.get("status") not in (None, "ok"):
        raise UsgsRunnerError(f"USGS {phase} phase returned status {public['status']!r}")
    public.setdefault("status", "ok")
    return public


def _dry_run_result(phase: str, run_context: Mapping[str, Any], context_file: Path) -> dict[str, Any]:
    result: dict[str, Any] = {
        "phase": phase,
        "status": "ok",
        "run_id": run_context["run_id"],
        "artifact_uri": f"dry-run://usgs/{run_context['run_id_path']}/{phase}",
    }
    if phase in {"validate", "upload", "verify"}:
        result["bronze_status"] = "BronzeReady"
        result["valid"] = True
    if phase == "verify":
        result["verified"] = True
        result["manifest_uri"] = f"dry-run://usgs/{run_context['run_id_path']}/manifest.json"
        result["record_count_estimate"] = 0
    result["context_file"] = str(context_file)
    return result


def execute_phase(
    phase: str,
    run_context: Mapping[str, Any],
    upstream: Mapping[str, Any] | None = None,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Run one external USGS phase and return only safe summary metadata."""

    if phase not in PHASES:
        raise UsgsRunnerError(f"unsupported USGS phase: {phase}")
    env = os.environ if environment is None else environment
    context_file = _write_context_file(phase, run_context, upstream, env)
    if _bool_value(env.get("USGS_INGEST_DRY_RUN"), False):
        return _dry_run_result(phase, run_context, context_file)

    command = env.get("USGS_INGEST_RUNNER_COMMAND", "").strip()
    if not command:
        raise UsgsRunnerError(
            "USGS_INGEST_RUNNER_COMMAND is required unless USGS_INGEST_DRY_RUN=true"
        )
    argv = shlex.split(command)
    timeout = _int_env(env, "USGS_INGEST_RUNNER_TIMEOUT_SECONDS", 3600)
    argv.extend(["--phase", phase, "--context-file", str(context_file)])
    try:
        completed = subprocess.run(
            argv,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            env=dict(env),
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise UsgsRunnerError(f"USGS {phase} runner did not complete") from exc
    if completed.returncode != 0:
        stderr_tail = completed.stderr.strip().splitlines()[-1:] or ["no error detail"]
        raise UsgsRunnerError(f"USGS {phase} runner failed: {stderr_tail[0][:500]}")

    lines = [line.strip() for line in completed.stdout.splitlines() if line.strip()]
    if not lines:
        raise UsgsRunnerError(f"USGS {phase} runner returned no JSON summary")
    try:
        result = json.loads(lines[-1])
    except json.JSONDecodeError as exc:
        raise UsgsRunnerError(f"USGS {phase} runner returned invalid JSON summary") from exc
    if not isinstance(result, Mapping):
        raise UsgsRunnerError(f"USGS {phase} runner summary must be a JSON object")
    return _public_result(phase, result)


def require_bronze_ready(result: Mapping[str, Any]) -> dict[str, Any]:
    """Enforce the publish gate before any future Silver task is scheduled."""

    if result.get("bronze_status") != "BronzeReady" or result.get("verified") is not True:
        raise UsgsRunnerError("Bronze verification did not produce BronzeReady")
    return {
        "run_id": result.get("run_id"),
        "bronze_status": result["bronze_status"],
        "verified": True,
        "manifest_uri": result.get("manifest_uri"),
    }


def write_run_summary(
    run_context: Mapping[str, Any], phases: Mapping[str, Mapping[str, Any]], environment: Mapping[str, str] | None = None
) -> str:
    """Write a small run summary outside XCom and return its local URI."""

    env = os.environ if environment is None else environment
    root = _staging_root(env) / str(run_context["run_id_path"])
    root.mkdir(parents=True, exist_ok=True)
    target = root / "run_summary.json"
    summary = {
        "dag_id": run_context["dag_id"],
        "run_id": run_context["run_id"],
        "status": "BronzeReady",
        "logical_run_key": run_context["logical_run_key"],
        "window_start_utc": run_context["window_start_utc"],
        "window_end_utc": run_context["window_end_utc"],
        "processing_date": run_context["processing_date"],
        "is_backfill": run_context["is_backfill"],
        "phases": {name: dict(value) for name, value in phases.items()},
    }
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=root, prefix=".run-summary-", delete=False
    ) as temporary:
        json.dump(summary, temporary, sort_keys=True, indent=2)
        temporary.write("\n")
        temporary_path = Path(temporary.name)
    os.replace(temporary_path, target)
    return str(target)
