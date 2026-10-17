"""ORC-02 bounded live QA. CLI schedules tasks; Java validates source/MinIO bytes.

Preserve services, volumes, existing data and original pause states. Never mark
Silver/Gold Published. The contention test uses a deliberately held QA lease.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import time
import uuid
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from airflow.api.common.trigger_dag import trigger_dag
from airflow.utils.types import DagRunTriggeredByType

sys.path.insert(0, "/opt/airflow/dags")
from source_run_guard import lease
from source_schedule_runtime import profile

DAG = "orc_02_daily_sources"
BACKFILL = "jma_04_year_backfill"
SAFE_REASONS = {
    "AIRFLOW_CLI_FAILED", "DAILY_TRIGGER_FAILED", "DAG_RUN_FAILED", "SOURCE_TASKS_NOT_EXECUTED",
    "DAG_TIMEOUT", "READINESS_REPORT_FAILED", "QA_REQUIRES_DEFAULT_BOUNDED_PROFILE", "DAG_IMPORT_ERRORS",
    "UNKNOWN_PAUSE_STATE", "DAGS_NOT_SERIALIZED", "EXISTING_ACTIVE_RUN", "NO_SCHEDULED_INTERVAL_EVIDENCE",
    "INTERVAL_OR_NO_CHANGE_FAILED", "RERUN_READBACK_CHANGED", "BACKFILL_NOT_BLOCKED_BEFORE_INGEST",
    "FOREIGN_CLEANUP_REMOVED_LEASE", "SCHEDULED_RUN_FAILED_RECOVER_REQUIRED", "SCHEDULED_SUCCESS_INVALID_REPORT",
}


class QaFailure(RuntimeError):
    def __init__(self, reason, dag_id=None, run_id=None):
        super().__init__(reason)
        self.dag_id, self.run_id = dag_id, run_id


def failure_details(dag_id, run_id):
    """Read only scoped safe metadata; never inspect exception text, XCom/env or private logs."""
    if dag_id not in {DAG, BACKFILL} or not isinstance(run_id, str) or not re.fullmatch(
            r"scheduled__[0-9T:.+\-Z]{10,80}|orc02-[0-9]{8}T[0-9]{6}Z-[a-f0-9]{12}-(first|rerun|contention)", run_id):
        return {}
    details = {"dag_id": dag_id, "run_id": run_id}
    try:
        states = task_states(dag_id, run_id)
        allowed = {"resolve_daily", "acquire_source_lease", "jma_readiness", "usgs_bronze",
                   "sources_ready", "real_etl", "release_source_lease", "completion_gate"}
        if dag_id == BACKFILL:
            allowed = {"jma_year_backfill." + name for name in ("resolve_plan", "acquire_source_lease", "select_archives",
                "ingest_archive", "run_summary", "bronze_ready_gate", "release_source_lease", "completion_gate")}
        details["failed_tasks"] = sorted(name for name, value in states.items() if name in allowed and value == "failed")
        if dag_id == DAG and "jma_readiness" in details["failed_tasks"]:
            slug = "jma-" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
            base = Path(os.environ.get("JMA_STAGING_ROOT", "/opt/pipeline/staging/jma")) / "runs" / slug
            for year, segment in ((1997, "jan-sep"), (1997, "oct-dec"), (2000, "full-year"), (2023, "full-year")):
                path = base / f"year={year}" / f"segment={segment}" / "result.json"
                if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
                    continue
                result = json.loads(path.read_text())
                if result.get("status") != "FAILED":
                    continue
                reason = result.get("reason")
                safe = isinstance(reason, str) and (reason in {"PUBLICATION_OR_STATE_FAILED", "RUNNER_FAILED", "DOWNLOAD_FAILED", "JMA_INTERRUPTED"}
                    or re.fullmatch(r"JMA_HEAD_(CONNECT_TIMEOUT|TIMEOUT|DNS_FAILED|CONNECTION_FAILED|TLS_FAILED|IO_FAILED|HTTP_([1-5][0-9]{2}|INVALID))", reason)
                    or reason in {"JMA_SOURCE_PROBE_OR_CACHE_FAILED", "JMA_PUBLICATION_STORE_OPEN_FAILED", "JMA_PUBLICATION_WRITE_FAILED", "JMA_PUBLICATION_READBACK_FAILED"})
                details.update(archive_year=year, archive_segment=segment, runner_reason=reason if safe else "RUNNER_FAILED")
                count = result.get("http_head_attempts")
                if type(count) is int and 0 <= count <= 5:
                    details["http_head_attempts"] = count
                break
    except Exception:
        details["diagnostics_available"] = False  # Keep original failure; never echo a secondary exception.
    return details


def scheduled_for_interval(runs, expected_end):
    for item in runs:
        if not item["run_id"].startswith("scheduled__"):
            continue
        try:
            matches = datetime.fromisoformat(item.get("data_interval_end", "")) == expected_end
        except (TypeError, ValueError):
            matches = False
        if matches:
            if item["state"] == "failed":
                raise QaFailure("SCHEDULED_RUN_FAILED_RECOVER_REQUIRED", DAG, item["run_id"])
            return item["run_id"]
    return None


def cli(*args):
    result = subprocess.run(["airflow", *args], capture_output=True, text=True, timeout=60)
    if result.returncode: raise RuntimeError("AIRFLOW_CLI_FAILED")
    return result.stdout


def state(dag_id, run_id):
    # Read-only harness metadata query, never used inside SDK tasks. Avoid
    # spawning the heavy Airflow CLI on every polling interval.
    with create_session() as session:
        return session.query(DagRun.state).filter_by(dag_id=dag_id, run_id=run_id).scalar()


def dagruns(dag_id):
    with create_session() as session:
        return [{"run_id": row.run_id, "state": row.state,
                 "data_interval_end": row.data_interval_end.isoformat() if row.data_interval_end else None} for row in session.query(DagRun)
                .filter_by(dag_id=dag_id).order_by(DagRun.id.desc()).limit(100).all()]


def task_states(dag_id, run_id):
    with create_session() as session:
        return {row.task_id: row.state for row in session.query(TaskInstance)
                .filter_by(dag_id=dag_id, run_id=run_id).all()}


def trigger_daily(run_id, logical):
    # Airflow 3.3.2 CLI sets run_after=NOW even when --logical-date is historic.
    # Use the native trigger API with BOTH clocks; timetable still resolves the
    # interval and the real scheduler/LocalExecutor runs all tasks. No SQL edits.
    result = trigger_dag(dag_id=DAG, triggered_by=DagRunTriggeredByType.TEST,
                         run_id=run_id, logical_date=logical, run_after=logical,
                         conf={}, replace_microseconds=False)
    if result is None: raise RuntimeError("DAILY_TRIGGER_FAILED")


def wait(dag_id, run_id, expected="success"):
    deadline = time.monotonic() + 900
    while time.monotonic() < deadline:
        current = state(dag_id, run_id)
        if current in {"success", "failed"}:
            if current != expected: raise QaFailure("DAG_RUN_FAILED", dag_id, run_id)
            if dag_id == DAG and expected == "success":
                tasks = task_states(dag_id, run_id)
                required = {"resolve_daily", "acquire_source_lease", "jma_readiness", "usgs_bronze",
                            "sources_ready", "real_etl", "release_source_lease", "completion_gate"}
                if set(tasks) != required or any(value != "success" for value in tasks.values()):
                    raise RuntimeError("SOURCE_TASKS_NOT_EXECUTED")
            return current
        time.sleep(3)
    raise QaFailure("DAG_TIMEOUT", dag_id, run_id)


def summary(run_id):
    slug = "daily-" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    path = Path(os.environ["SOURCE_READINESS_ROOT"]) / slug / "run_summary.json"
    report = json.loads(path.read_text())
    if (report.get("status") != "SourcesReady" or report.get("verified") is not True
            or report.get("published") is not False or len(report.get("input_manifests", [])) != 5):
        raise RuntimeError("READINESS_REPORT_FAILED")
    return report


def main():
    token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]
    root = Path(os.environ["SOURCE_READINESS_ROOT"]) / "qa" / token
    root.mkdir(parents=True, exist_ok=False)
    previous = {}; reports = {}; held = {"dag_id": "orc02_runtime_qa_holder", "run_id": token}
    held_acquired = False
    phase = "profile"
    print("event=source_readiness_qa_started evidence_id=" + token, flush=True)
    try:
        settings = profile()
        if os.environ.get("ORC07_DAILY_MODE", "disabled") != "disabled":
            raise QaFailure("QA_REQUIRES_DEFAULT_BOUNDED_PROFILE", DAG)
        if (settings["mode"] != "multi-source" or settings["years"] != [1997, 2000, 2023]
                or settings["audit_weekday"] != 6 or os.environ.get("PIPELINE_OVERLAP_DAYS") != "3"):
            raise RuntimeError("QA_REQUIRES_DEFAULT_BOUNDED_PROFILE")
        deadline = time.monotonic() + 180
        phase = "dag-discovery"
        while time.monotonic() < deadline:
            if json.loads(cli("dags", "list-import-errors", "--output", "json")):
                raise RuntimeError("DAG_IMPORT_ERRORS")
            dags = json.loads(cli("dags", "list", "--output", "json"))
            for name in (DAG, BACKFILL):
                entry = next((item for item in dags if item.get("dag_id") == name), None)
                if entry and name not in previous:
                    value = entry.get("is_paused")
                    if str(value).lower() not in {"true", "false"}: raise RuntimeError("UNKNOWN_PAUSE_STATE")
                    previous[name] = str(value).lower() == "true"
            if len(previous) == 2: break
            time.sleep(3)
        if len(previous) != 2: raise RuntimeError("DAGS_NOT_SERIALIZED")
        # Do not interfere with another operator's queued/running run.
        phase = "active-run-check"
        for name in (DAG, BACKFILL, "usg_04_usgs_ingest"):
            active = dagruns(name)
            if any(item.get("state") in {"queued", "running"} for item in active):
                raise QaFailure("EXISTING_ACTIVE_RUN", name, next(item["run_id"] for item in active if item.get("state") in {"queued", "running"}))
        # Enable the real daily timetable once. Airflow creates the latest complete
        # scheduled interval (catchup=False), bounded to one USGS three-day query.
        before = {item["run_id"] for item in dagruns(DAG)}
        current = datetime.now(timezone.utc)
        minute, hour = map(int, settings["cron"].split()[:2])
        expected_end = current.replace(hour=hour - 7, minute=minute, second=0, microsecond=0)
        if expected_end > current: expected_end -= timedelta(days=1)
        phase = "scheduled"
        scheduled = scheduled_for_interval(dagruns(DAG), expected_end)
        if scheduled:
            try:
                candidate = summary(scheduled)
                if candidate["profile"] != settings or datetime.fromisoformat(candidate["data_interval_end"]) != expected_end:
                    raise ValueError()
            except (OSError, ValueError, KeyError, RuntimeError):
                raise QaFailure("SCHEDULED_SUCCESS_INVALID_REPORT", DAG, scheduled) from None
        cli("dags", "unpause", DAG, "--yes")
        deadline = time.monotonic() + 180
        while scheduled is None and time.monotonic() < deadline:
            runs = dagruns(DAG)
            scheduled = scheduled_for_interval([item for item in runs if item["run_id"] not in before], expected_end)
            if scheduled: break
            time.sleep(3)
        if not scheduled: raise RuntimeError("NO_SCHEDULED_INTERVAL_EVIDENCE")
        wait(DAG, scheduled)
        reports["scheduled"] = summary(scheduled)
        (root / "scheduled.json").write_text(json.dumps(reports["scheduled"], indent=2) + "\n")
        print("event=source_readiness_qa_run phase=scheduled state=success", flush=True)
        # Pin an old interval for repeatable small-source acceptance. The manual
        # interval is inferred by the actual CronDataIntervalTimetable, not now().
        for phase in ("first", "rerun"):
            run_id = "orc02-" + token + "-" + phase
            offset = int(hashlib.sha256(token.encode()).hexdigest()[:12], 16) % 3_600_000_000 + 120_000_000
            # Unique logical dates within the SAME cron interval. Airflow 3.3.2
            # enforces (dag_id, logical_date) uniqueness even with different run IDs.
            logical = datetime(2023, 1, 4, 0, 15, tzinfo=timezone.utc) + timedelta(
                microseconds=offset + (phase == "rerun"))
            trigger_daily(run_id, logical)
            wait(DAG, run_id)
            report = summary(run_id)
            if (report["usgs_context"]["window_start_utc"] != "2023-01-01T00:00:00Z"
                    or report["usgs_context"]["window_end_utc"] != "2023-01-04T00:00:00Z"
                    or report["usgs_context"]["processing_date"] != "2023-01-03"
                    or report["jma_changed_years"] or any(item["ingest_invoked"] for item in report["jma"]["archives"])):
                raise RuntimeError("INTERVAL_OR_NO_CHANGE_FAILED")
            reports[phase] = report
            (root / (phase + ".json")).write_text(json.dumps(report, indent=2) + "\n")
            print("event=source_readiness_qa_run phase=" + phase + " state=success", flush=True)
        def pins(report):
            return [(item["year"], item["segment"], item["manifest_uri"], item["manifest_sha256"],
                     item["sha256"], item["record_count_estimate"]) for item in report["jma"]["archives"]]
        if pins(reports["first"]) != pins(reports["rerun"]):
            raise RuntimeError("RERUN_READBACK_CHANGED")
        phase = "contention"
        cli("dags", "pause", DAG, "--yes")
        lease("acquire", held); held_acquired = True
        blocked_id = "orc02-" + token + "-contention"
        cli("dags", "unpause", BACKFILL, "--yes")
        cli("dags", "trigger", BACKFILL, "--run-id", blocked_id,
            "--conf", json.dumps({"years": [2023], "preview": False}))
        wait(BACKFILL, blocked_id, "failed")
        blocked_tasks = task_states(BACKFILL, blocked_id)
        if (blocked_tasks.get("jma_year_backfill.acquire_source_lease") != "failed"
                or blocked_tasks.get("jma_year_backfill.ingest_archive") != "upstream_failed"):
            raise RuntimeError("BACKFILL_NOT_BLOCKED_BEFORE_INGEST")
        if json.loads((Path(os.environ["SOURCE_GUARD_ROOT"]) / "owner.json").read_text()) != held:
            raise RuntimeError("FOREIGN_CLEANUP_REMOVED_LEASE")
        reports["contention"] = {"run_id": blocked_id, "state": "failed", "holder_preserved": True}
        print("event=source_readiness_qa_contention expected_failure=true holder_preserved=true", flush=True)
    except Exception as error:
        reason = str(error) if isinstance(error, RuntimeError) and str(error) in SAFE_REASONS else "QA_UNEXPECTED_FAILURE"
        details = failure_details(getattr(error, "dag_id", None), getattr(error, "run_id", None))
        failure = {"status": "FAILED", "reason": reason, "phase": phase, "completed_phases": list(reports), **details}
        (root / "failure.json").write_text(json.dumps(failure, indent=2) + "\n")
        print("event=source_readiness_qa_failed evidence_id=" + token + " phase=" + phase + " reason=" + reason, flush=True)
        if details:
            print(json.dumps({"event": "source_readiness_qa_failure_details", **details}, sort_keys=True), flush=True)
        raise SystemExit(1) from None
    finally:
        cleanup_failed = []
        if held_acquired:
            try:
                if lease("release", held)["status"] != "RELEASED":
                    cleanup_failed.append("QA_LEASE_RELEASE_FAILED")
            except Exception:
                cleanup_failed.append("QA_LEASE_RELEASE_FAILED")
        for name, paused in previous.items():
            try:
                cli("dags", "pause" if paused else "unpause", name, "--yes")
            except Exception:
                cleanup_failed.append("PAUSE_STATE_RESTORE_FAILED")
        if cleanup_failed:
            failure_path = root / "failure.json"
            failure = json.loads(failure_path.read_text()) if failure_path.exists() else {
                "status": "FAILED", "reason": "QA_CLEANUP_FAILED", "phase": "cleanup", "completed_phases": list(reports)}
            failure["cleanup_failed"] = sorted(set(cleanup_failed))
            failure_path.write_text(json.dumps(failure, indent=2) + "\n")
            print("event=source_readiness_qa_cleanup_failed evidence_id=" + token + " reason=QA_CLEANUP_FAILED", flush=True)
            raise SystemExit(1) from None
    evidence = {"qa_version": "orc-02-v1", "status": "VERIFIED", "evidence_id": token,
                "daily_run_ids": {phase: reports[phase]["run_id"] for phase in ("scheduled", "first", "rerun")},
                "contention": reports["contention"], "pause_states_restored": True, "published": False}
    (root / "workflow-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("event=source_readiness_qa_passed evidence_id=" + token, flush=True)


if __name__ == "__main__": main()
