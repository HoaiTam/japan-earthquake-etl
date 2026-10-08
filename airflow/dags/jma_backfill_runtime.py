"""JMA orchestration metadata only. Download/validation/storage remain in Java."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
from typing import Any, Mapping
from urllib.parse import urlsplit


class JmaRunnerError(RuntimeError):
    """Invalid scope, runner failure or an incomplete Bronze handoff."""


PUBLIC_KEYS = {
    "run_id", "year", "segment", "attempt", "config_version", "processing_date", "status", "bronze_status",
    "verified", "reason", "download_status", "download_reused", "publication_reused",
    "catalog_release", "sha256", "record_count_estimate", "manifest_key", "manifest_uri",
    "raw_object_key", "raw_object_uri",
    "manifest_sha256", "readiness_decision",
}


def _boolean(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise JmaRunnerError(f"{name} must be a JSON boolean")
    return value


def _year(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1984 <= value <= 2023:
        raise JmaRunnerError("year must be an integer within pinned baseline 1984..2023")
    return value


def _root(env: Mapping[str, str]) -> Path:
    return Path(env.get("JMA_STAGING_ROOT", "/opt/pipeline/staging/jma"))


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".jma-", delete=False) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(value, temporary, sort_keys=True, separators=(",", ":"))
            temporary.write("\n")
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def resolve_plan(context: Mapping[str, Any], environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Require explicit years/range, pin exact CSV bytes, and default to non-writing preview."""
    env = os.environ if environment is None else environment
    dag_run = context.get("dag_run")
    conf = dag_run.get("conf", {}) if isinstance(dag_run, Mapping) else getattr(dag_run, "conf", {})
    if not isinstance(conf, Mapping):
        raise JmaRunnerError("dag_run.conf must be an object")
    if "years" in conf:
        if "start_year" in conf or "end_year" in conf:
            raise JmaRunnerError("use years OR start_year/end_year, not both")
        if not isinstance(conf["years"], list) or not conf["years"]:
            raise JmaRunnerError("years must be a non-empty list")
        years = sorted(set(_year(value) for value in conf["years"]))
    elif "start_year" in conf and "end_year" in conf:
        start, end = _year(conf["start_year"]), _year(conf["end_year"])
        if start > end:
            raise JmaRunnerError("start_year must not exceed end_year")
        years = list(range(start, end + 1))
    else:
        raise JmaRunnerError("explicit years or start_year/end_year is required; no full-baseline default")
    preview = _boolean(conf.get("preview", True), "preview")
    force_download = _boolean(conf.get("force_download", False), "force_download")
    inventory_path = Path(env.get("JMA_INVENTORY_PATH", "/opt/pipeline/config/jma/hypocenter_archives_v1.csv"))
    inventory_bytes = inventory_path.read_bytes()
    rows = list(csv.DictReader(io.StringIO(inventory_bytes.decode("utf-8"))))
    archives = []
    for year in years:
        selected = [row for row in rows if row.get("year") == str(year)]
        expected = {"jan-sep", "oct-dec"} if year == 1997 else {"full-year"}
        if len(selected) != len(expected) or {row.get("segment") for row in selected} != expected:
            raise JmaRunnerError(f"inventory must resolve all exact segments for year {year}")
        for row in sorted(selected, key=lambda item: item["native_start_jst"]):
            archives.append({"year": year, "segment": row["segment"], "archive_name": row["archive_name"],
                             "source_url": row["source_url"], "native_start_jst": row["native_start_jst"],
                             "native_end_jst": row["native_end_jst"], "catalog_era": row["catalog_era"]})
    run_id = str(context.get("run_id") or "")
    if not run_id or any(ord(char) < 32 for char in run_id):
        raise JmaRunnerError("run_id is required without control characters")
    # Hash prevents sanitized IDs colliding and keeps staging paths bounded and traversal-free.
    run_id_path = "jma-" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    logical = context.get("logical_date") or context.get("data_interval_end")
    if logical is None:
        logical = dag_run.get("start_date") if isinstance(dag_run, Mapping) else getattr(dag_run, "start_date", None)
    if logical is None:
        raise JmaRunnerError("a stable logical_date/data_interval_end/dag_run.start_date is required")
    logical_at = datetime.fromisoformat(str(logical).replace("Z", "+00:00"))
    if logical_at.tzinfo is None:
        raise JmaRunnerError("logical_date must have a timezone")
    def utc(value: str) -> str:
        return datetime.fromisoformat(value).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    plan = {
        "preview": preview, "force_download": force_download, "years": years, "archives": archives,
        "run_context": {
            "run_id": run_id, "run_id_path": run_id_path, "dag_id": "jma_04_year_backfill",
            "processing_date": logical_at.astimezone(timezone.utc).date().isoformat(),
            "window_start_utc": min(utc(item["native_start_jst"]) for item in archives),
            "window_end_utc": max(utc(item["native_end_jst"]) for item in archives),
            "is_backfill": True, "config_version": env.get("CONFIG_VERSION", "1"),
            "inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
            "logical_run_key": "JMA_BULLETIN|" + ",".join(map(str, years)) + "|backfill",
        },
    }
    target = _root(env) / "runs" / run_id_path / "plan.json"
    if target.exists() and json.loads(target.read_text()) != plan:
        raise JmaRunnerError("run scope changed: use a new run_id")
    _atomic_json(target, plan)
    return plan


def _entry_root(plan: Mapping[str, Any], archive: Mapping[str, Any], env: Mapping[str, str]) -> Path:
    if archive not in plan["archives"]:
        raise JmaRunnerError("archive is outside the resolved plan")
    run_path = plan["run_context"]["run_id_path"]
    if not re.fullmatch(r"jma-[0-9a-f]{32}", run_path):
        raise JmaRunnerError("unsafe run path")
    year = _year(archive["year"])
    segment = archive["segment"]
    if segment not in ({"jan-sep", "oct-dec"} if year == 1997 else {"full-year"}):
        raise JmaRunnerError("unsafe segment")
    return _root(env) / "runs" / run_path / f"year={year}" / f"segment={segment}"


def _artifact_uri(value: Any, key: str) -> bool:
    if not isinstance(value, str):
        return False
    try:
        uri = urlsplit(value)
        return (uri.scheme in {"s3", "file"} and not uri.username and not uri.query and not uri.fragment
                and (uri.scheme != "s3" or bool(uri.netloc)) and uri.path.endswith("/" + key))
    except ValueError:
        return False


def _ready(result: Mapping[str, Any]) -> bool:
    release = str(result.get("catalog_release", ""))
    manifest_key, raw_key = result.get("manifest_key"), result.get("raw_object_key")
    if (not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._~-]*", release) or ".." in release
            or not isinstance(manifest_key, str) or not isinstance(raw_key, str)
            or ".." in raw_key or "\\" in raw_key or any(ord(char) < 32 for char in raw_key)
            or not raw_key.startswith(f"bronze/jma/year={result.get('year')}/catalog_release={release}/")
            or not raw_key.endswith("/archive.zip") or manifest_key != raw_key.removesuffix("archive.zip") + "manifest.json"
            or not _artifact_uri(result.get("manifest_uri"), manifest_key)
            or not _artifact_uri(result.get("raw_object_uri"), raw_key)):
        return False
    return (result.get("status") == "BronzeReady" and result.get("bronze_status") == "BronzeReady"
            and result.get("verified") is True and bool(re.fullmatch(r"[0-9a-f]{64}", str(result.get("sha256", ""))))
            and isinstance(result.get("record_count_estimate"), int)
            and not isinstance(result.get("record_count_estimate"), bool)
            and result["record_count_estimate"] >= 0)


def _public_result(result: Mapping[str, Any]) -> dict[str, Any]:
    public = {key: value for key, value in result.items() if key in PUBLIC_KEYS}
    for name in ("manifest", "raw_object"):
        key = public.get(name + "_key")
        if (not isinstance(key, str) or ".." in key
                or not re.fullmatch(r"bronze/[A-Za-z0-9=._~/-]+", key)):
            public.pop(name + "_key", None)
            public.pop(name + "_uri", None)
        elif name + "_uri" in public and not _artifact_uri(public[name + "_uri"], key):
            public.pop(name + "_uri", None)
    if "reason" in public and not re.fullmatch(r"[A-Z][A-Z0-9_]{0,79}", str(public["reason"])):
        public["reason"] = "RUNNER_FAILED"
    return public


def execute_archive(plan: Mapping[str, Any], archive: Mapping[str, Any], attempt: int = 1,
                    environment: Mapping[str, str] | None = None, phase: str = "ingest") -> dict[str, Any]:
    env = os.environ if environment is None else environment
    if plan["preview"]:
        raise JmaRunnerError("preview cannot invoke an ingest runner")
    if phase not in {"ingest", "probe"}:
        raise JmaRunnerError("unsupported archive phase")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise JmaRunnerError("attempt must be a positive integer")
    root = _entry_root(plan, archive, env)
    identity = {"run_id": plan["run_context"]["run_id"], "year": archive["year"],
                "segment": archive["segment"], "attempt": attempt}
    target = root / "result.json"
    # Invalidate prior success before starting: killed/timeout attempts cannot leave a stale Ready summary.
    _atomic_json(target, {**identity, "status": "RUNNING", "verified": False})
    context_file = root / f"attempt-{attempt}-input.json"
    _atomic_json(context_file, {"phase": phase, "run_context": plan["run_context"], "archive": archive,
                                "force_download": plan["force_download"], "attempt": attempt})
    failure = {**identity, "status": "FAILED", "verified": False, "reason": "RUNNER_FAILED"}
    try:
        command = env.get("JMA_INGEST_RUNNER_COMMAND", "").strip()
        if not command:
            raise JmaRunnerError("JMA_INGEST_RUNNER_COMMAND is required")
        timeout = int(env.get("JMA_INGEST_RUNNER_TIMEOUT_SECONDS", "900"))
        if not 1 <= timeout <= 3600:
            raise JmaRunnerError("runner timeout must be within 1..3600 seconds")
        completed = subprocess.run(shlex.split(command) + ["--context-file", str(context_file)],
                                   check=False, capture_output=True, text=True, timeout=timeout, env=dict(env))
        if completed.returncode != 0:
            raise JmaRunnerError("runner failed")
        result = json.loads(completed.stdout.strip().splitlines()[-1])
        if not isinstance(result, dict) or any(result.get(key) != value for key, value in identity.items()):
            raise JmaRunnerError("runner identity differs")
        public = _public_result(result)
        needs_ingest = (phase == "probe" and public.get("status") == "NeedsIngest"
                        and public.get("verified") is False and public.get("readiness_decision")
                        in {"CHECKSUM_AUDIT", "CHANGED_OR_UNINITIALIZED"})
        if not _ready(public) and not needs_ingest:
            # Preserve only safe failure metadata; never trust a partial Ready/status from a runner.
            public.update(status="FAILED", verified=False)
            public.pop("bronze_status", None)
        _atomic_json(target, public)
        return public
    except (OSError, ValueError, IndexError, JmaRunnerError, subprocess.TimeoutExpired):
        _atomic_json(target, failure)
        return failure


def write_run_summary(plan: Mapping[str, Any], environment: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Read only planned result paths; missing/unfinished archives fail closed after all_done."""
    env = os.environ if environment is None else environment
    entries = []
    for archive in plan["archives"]:
        if plan["preview"]:
            result = {"year": archive["year"], "segment": archive["segment"], "status": "PREVIEW", "verified": False}
        else:
            try:
                result = json.loads((_entry_root(plan, archive, env) / "result.json").read_text())
                if (not isinstance(result, dict) or result.get("run_id") != plan["run_context"]["run_id"]
                        or result.get("year") != archive["year"] or result.get("segment") != archive["segment"]):
                    raise ValueError("summary identity differs")
                result = _public_result(result)
            except (OSError, ValueError):
                result = {"year": archive["year"], "segment": archive["segment"], "status": "FAILED",
                          "verified": False, "reason": "MISSING_OR_INVALID_RESULT"}
            if not _ready(result):
                result.update(status="FAILED", verified=False)
                result.pop("bronze_status", None)
        entries.append(result)
    years = []
    for year in plan["years"]:
        segments = [item for item in entries if item["year"] == year]
        ready_count = sum(_ready(item) for item in segments)
        status = "PREVIEW" if plan["preview"] else ("BronzeReady" if ready_count == len(segments)
                                                    else "PARTIAL" if ready_count else "FAILED")
        years.append({"year": year, "status": status, "segments": segments})
    all_ready = bool(entries) and all(_ready(item) for item in entries)
    summary = {"run_context": plan["run_context"], "preview": plan["preview"], "years": years,
               "status": "PREVIEW" if plan["preview"] else "BronzeReady" if all_ready else "FAILED",
               "verified": all_ready, "planned_archives": len(entries),
               "ready_archives": sum(_ready(item) for item in entries)}
    path = _root(env) / "runs" / plan["run_context"]["run_id_path"] / "run_summary.json"
    _atomic_json(path, summary)
    return {"status": summary["status"], "preview": summary["preview"], "verified": all_ready,
            "summary_uri": path.as_uri() if path.is_absolute() else path.absolute().as_uri(),
            "planned_archives": len(entries), "ready_archives": summary["ready_archives"]}


def require_complete(summary: Mapping[str, Any]) -> dict[str, Any]:
    if summary.get("preview") is True and summary.get("status") == "PREVIEW" and summary.get("verified") is False:
        return dict(summary)  # Successful preview is explicitly NOT a BronzeReady handoff.
    if (summary.get("preview") is not False or summary.get("status") != "BronzeReady"
            or summary.get("verified") is not True or not summary.get("planned_archives")
            or summary.get("planned_archives") != summary.get("ready_archives")):
        raise JmaRunnerError("JMA backfill is incomplete; inspect run_summary and rerun exact failed years")
    return dict(summary)
