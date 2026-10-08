"""ORC-02 bounded live QA. CLI schedules tasks; Java validates source/MinIO bytes.

Preserve services, volumes, existing data and original pause states. Never mark
Silver/Gold Published. The contention test uses a deliberately held QA lease.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
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
        return [{"run_id": row.run_id, "state": row.state} for row in session.query(DagRun)
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
            if current != expected: raise RuntimeError("UNEXPECTED_DAG_STATE")
            if dag_id == DAG and expected == "success":
                tasks = task_states(dag_id, run_id)
                required = {"resolve_daily", "acquire_source_lease", "jma_readiness", "usgs_bronze",
                            "sources_ready", "release_source_lease", "completion_gate"}
                if set(tasks) != required or any(value != "success" for value in tasks.values()):
                    raise RuntimeError("SOURCE_TASKS_NOT_EXECUTED")
            return current
        time.sleep(3)
    raise RuntimeError("DAG_TIMEOUT")


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
    print("event=source_readiness_qa_started evidence_id=" + token, flush=True)
    try:
        settings = profile()
        if (settings["mode"] != "multi-source" or settings["years"] != [1997, 2000, 2023]
                or settings["audit_weekday"] != 6 or os.environ.get("PIPELINE_OVERLAP_DAYS") != "3"):
            raise RuntimeError("QA_REQUIRES_DEFAULT_BOUNDED_PROFILE")
        deadline = time.monotonic() + 180
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
        for name in (DAG, BACKFILL, "usg_04_usgs_ingest"):
            active = dagruns(name)
            if any(item.get("state") in {"queued", "running"} for item in active):
                raise RuntimeError("EXISTING_ACTIVE_RUN")
        # Enable the real daily timetable once. Airflow creates the latest complete
        # scheduled interval (catchup=False), bounded to one USGS three-day query.
        before = {item["run_id"] for item in dagruns(DAG)}
        current = datetime.now(timezone.utc)
        minute, hour = map(int, settings["cron"].split()[:2])
        expected_end = current.replace(hour=hour - 7, minute=minute, second=0, microsecond=0)
        if expected_end > current: expected_end -= timedelta(days=1)
        scheduled = None
        for item in dagruns(DAG):
            if item["run_id"].startswith("scheduled__") and item.get("state") == "success":
                try: candidate = summary(item["run_id"])
                except (OSError, ValueError, RuntimeError): continue
                if (candidate["profile"] == settings and datetime.fromisoformat(candidate["data_interval_end"]) == expected_end):
                    scheduled = item["run_id"]; break
        cli("dags", "unpause", DAG, "--yes")
        deadline = time.monotonic() + 180
        while scheduled is None and time.monotonic() < deadline:
            runs = dagruns(DAG)
            scheduled = next((item["run_id"] for item in runs if item["run_id"] not in before
                              and item["run_id"].startswith("scheduled__")), None)
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
    except Exception:
        (root / "failure.json").write_text(json.dumps({"status": "FAILED", "completed_phases": list(reports)}) + "\n")
        print("event=source_readiness_qa_failed evidence_id=" + token + " reason=QA_FAILED", flush=True)
        raise SystemExit(1) from None
    finally:
        if held_acquired and lease("release", held)["status"] != "RELEASED":
            raise RuntimeError("QA_LEASE_RELEASE_FAILED")
        for name, paused in previous.items():
            cli("dags", "pause" if paused else "unpause", name, "--yes")
    evidence = {"qa_version": "orc-02-v1", "status": "VERIFIED", "evidence_id": token,
                "daily_run_ids": {phase: reports[phase]["run_id"] for phase in ("scheduled", "first", "rerun")},
                "contention": reports["contention"], "pause_states_restored": True, "published": False}
    (root / "workflow-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("event=source_readiness_qa_passed evidence_id=" + token, flush=True)


if __name__ == "__main__": main()
