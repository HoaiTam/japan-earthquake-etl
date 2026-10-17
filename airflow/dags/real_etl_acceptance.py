"""Opt-in ORC-07 real QA. Runs new isolated DagRuns; never clears an existing run."""
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess
import uuid

import pendulum
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.session import create_session
from jma_backfill_runtime import _atomic_json
from source_run_guard import lease
import real_etl_runtime as runtime


def dag_test(dag, logical, conf=None):
    # DAG.test clears by logical date internally: refuse ANY existing logical date first.
    # No clear flags, mark-success patterns, pause/unpause or API configuration changes.
    with create_session() as session:
        runtime.require(session.query(DagRun).filter(DagRun.dag_id == dag.dag_id,
                        DagRun.logical_date == logical).first() is None, "QA_LOGICAL_DATE_ALREADY_EXISTS")
        runtime.require(session.query(DagRun).filter(DagRun.dag_id == dag.dag_id,
                        DagRun.state.in_(["running", "queued"])).first() is None, "QA_EXISTING_ACTIVE_RUN")
    run = dag.test(logical_date=logical, run_conf=conf)
    runtime.require(str(run.state) == "success", "QA_DAG_FAILED_LEASE_MAY_BE_RETAINED")
    with create_session() as session:
        states = {row.task_id: str(row.state) for row in session.query(TaskInstance).filter_by(dag_id=dag.dag_id, run_id=run.run_id).all()}
    runtime.require(states and all(value == "success" for value in states.values()), "QA_TASKS_NOT_ALL_SUCCESS")
    return run, states


def run():
    env = os.environ
    token = uuid.uuid4().hex; folder = runtime.root(env) / "qa" / token
    folder.mkdir(parents=True)
    _atomic_json(runtime.root(env) / "last-qa.json", {"evidence_key": token, "status": "RUNNING"})
    seed_id = "orc07-seed-" + token; namespace = env["ICEBERG_CATALOG_NAME"] + ".gold_qa_" + token
    owner = {"dag_id": "orc07_acceptance", "run_id": seed_id}
    lease("acquire", owner, env)
    seed = folder / "seed-request.json"
    with (folder / "seed.private.log").open("wb") as output:
        os.chmod(output.name, 0o600)
        child = subprocess.run([os.environ["JAVA_HOME"] + "/bin/java", "-Xmx384m", "-cp",
            "/opt/spark/jobs/japan-earthquake-etl-runner.jar:/opt/spark/jars/*:/opt/spark/jobs/iceberg/*", "ie212.earthquake.spark.usgs.RealEtlSeedJob",
            "/opt/pipeline/orc07-captures/usgs-2023-09.geojson", "/opt/pipeline/orc07-captures/jma-2023.zip",
            str(seed), seed_id, namespace], stdout=output, stderr=subprocess.STDOUT, timeout=300, check=False)
    runtime.require(child.returncode == 0, "QA_SEED_FAILED_LEASE_RETAINED")
    runtime.require(lease("release", owner, env)["status"] == "RELEASED", "QA_SEED_RELEASE_FAILED")
    key = "qa-" + token; request_file = runtime.root(env) / "requests" / (key + ".json")
    _atomic_json(request_file, json.loads(seed.read_text()))
    env["ORC07_DAILY_REQUEST_KEY"] = key
    env["ORC07_DAILY_REQUEST_SHA256"] = hashlib.sha256(request_file.read_bytes()).hexdigest()
    # Production daily DAG, real sources/writers, historical eligible UTC interval.
    # Unique sub-minute logical date selects the same closed 2023-10-01 schedule boundary.
    import orc_02_daily_sources
    daily_dag = orc_02_daily_sources.daily_sources()
    logical = pendulum.datetime(2023, 10, 1, 0, 15, 1 + int(token[:2], 16) % 58,
                                microsecond=int(token[2:8], 16) % 1000000, tz="UTC")
    daily_run, daily_tasks = dag_test(daily_dag, logical)
    op = runtime.operation("orc_02_daily_sources", daily_run.run_id)
    daily_context = json.loads((runtime.root(env) / op / "context.json").read_text())
    daily_summary = json.loads((runtime.root(env) / op / "summary.json").read_text())
    runtime.require(daily_context["trigger"] == "daily" and daily_summary["published"] is True, "QA_DAILY_PUBLICATION_REQUIRED")
    # Resume EXACT operation, not a new logical DagRun or a task clear.
    daily_owner = {"dag_id": "orc_02_daily_sources", "run_id": daily_run.run_id}
    lease("acquire", daily_owner, env)
    replay = runtime.execute(daily_context, env)
    runtime.require(replay == {k: v for k, v in daily_summary.items() if k != "receipts"}, "QA_RERUN_CHANGED")
    runtime.require(runtime.release(daily_context, replay, env)["status"] == "RELEASED", "QA_RERUN_RELEASE_FAILED")
    finish_manual(folder, token, daily_context, daily_run.run_id, daily_tasks, daily_summary)


def finish_manual(folder, token, daily_context, daily_run_id, daily_tasks, daily_summary, recovered=None):
    env = os.environ
    # Manual uses the SAME exact source pins/adapters, separate isolated Gold namespace.
    manual = deepcopy(daily_context["resolver_request"])
    manual["namespace"] = env["ICEBERG_CATALOG_NAME"] + ".gold_qa_" + uuid.uuid4().hex
    manual["baseline"] = None; manual["baseline_snapshots"] = {name: 0 for name in runtime.TABLES}
    manual_key = "manual-" + token; manual_file = runtime.root(env) / "requests" / (manual_key + ".json")
    _atomic_json(manual_file, manual)
    conf = {"request_key": manual_key, "request_sha256": hashlib.sha256(manual_file.read_bytes()).hexdigest(),
            "window_start_utc": "2023-09-01T00:00:00Z", "window_end_utc": "2023-10-01T00:00:00Z"}
    import orc_07_real_etl
    manual_dag = orc_07_real_etl.real_etl()
    manual_logical = pendulum.now("UTC")
    manual_run, manual_tasks = dag_test(manual_dag, manual_logical, conf)
    manual_op = runtime.operation("orc_07_real_etl", manual_run.run_id)
    manual_summary = json.loads((runtime.root(env) / manual_op / "summary.json").read_text())
    runtime.require(manual_summary["published"] is True, "QA_MANUAL_PUBLICATION_REQUIRED")
    def projection(summary):
        return {k: v for k, v in summary.items() if k != "receipts"}
    report = {"task": "ORC-07", "status": "PASSED", "backend": "MinIO-Iceberg-Trino-Airflow-DAG.test",
              "evidence_id": token, "utc_target_month": "2023-09", "daily_run_id": daily_run_id,
              "daily_tasks": daily_tasks, "daily": projection(daily_summary), "rerun_identical": True,
              "manual_run_id": manual_run.run_id, "manual_tasks": manual_tasks, "manual": projection(manual_summary),
              "scheduled_run": False, "production_namespace_modified": False}
    if recovered is not None: report["partial_recovery"] = recovered
    _atomic_json(folder / "acceptance.json", report)
    _atomic_json(runtime.root(env) / "last-qa.json", {"evidence_key": token, "status": "PASSED"})
    print("event=orc07_acceptance_passed evidence_id=" + token)


def recover_existing(key):
    """Explicit operator-confirmed local OOM recovery; never clears failed DagRuns."""
    runtime.require(re.fullmatch(r"[a-f0-9]{32}", key), "EXACT_QA_EVIDENCE_REQUIRED")
    env = os.environ; folder = runtime.root(env) / "qa" / key
    proof = json.loads((folder / "recovery-confirmation.json").read_text())
    runtime.require(proof["state"] == "OWN_LOCAL_OOM_STOP_CONFIRMED" and proof["trino_phase_started"] is False, "OPERATOR_STOP_CONFIRMATION_REQUIRED")
    op = proof["operation_id"]; daily_op = proof["daily_operation_id"]
    runtime.require(all(re.fullmatch(r"orc07-[a-f0-9]{40}", value) for value in (op, daily_op)), "EXACT_QA_OPERATION_REQUIRED")
    base = runtime.root(env) / op; context = json.loads((base / "context.json").read_text())
    runtime.validate_context(context)
    runtime.require(proof["owner"] == {"dag_id": context["dag_id"], "run_id": context["run_id"]}, "EXACT_RECOVERY_OWNER_REQUIRED")
    # Existing owned lock is required: no expired lease takeover or foreign cleanup.
    lease("assert", proof["owner"], env)
    resumed = runtime.execute(context, env)
    runtime.require(all(resumed["snapshots"][name] == value for name, value in proof["committed_snapshots"].items()), "PARTIAL_COMMIT_WAS_REPLACED")
    runtime.require(runtime.release(context, resumed, env)["status"] == "RELEASED", "RECOVERY_RELEASE_FAILED")
    recovered = {"operation_id": op, "original_dag_run_id": context["run_id"], "original_dag_state": "failed",
                 "backend_status": resumed["status"], "preserved_snapshots": proof["committed_snapshots"], "final_snapshots": resumed["snapshots"]}
    _atomic_json(folder / "partial-recovery.json", recovered)
    # Actual final-binary replay of daily operation, not a caller-provided passed flag.
    daily_base = runtime.root(env) / daily_op; daily_context = json.loads((daily_base / "context.json").read_text())
    daily_summary = json.loads((daily_base / "summary.json").read_text())
    with create_session() as session:
        state = session.query(DagRun.state).filter_by(dag_id=daily_context["dag_id"], run_id=daily_context["run_id"]).scalar()
        states = {row.task_id: str(row.state) for row in session.query(TaskInstance).filter_by(dag_id=daily_context["dag_id"], run_id=daily_context["run_id"]).all()}
    runtime.require(str(state) == "success" and len(states) == 8 and all(s == "success" for s in states.values()), "DAILY_DAG_EVIDENCE_REQUIRED")
    lease("acquire", {"dag_id": daily_context["dag_id"], "run_id": daily_context["run_id"]}, env)
    replay = runtime.execute(daily_context, env)
    runtime.require(replay == {k: v for k, v in daily_summary.items() if k != "receipts"}, "QA_RERUN_CHANGED")
    runtime.require(runtime.release(daily_context, replay, env)["status"] == "RELEASED", "QA_RERUN_RELEASE_FAILED")
    # Preserve failed Airflow run; a new independent DagRun must pass end-to-end.
    finish_manual(folder, key, daily_context, daily_context["run_id"], states, daily_summary, recovered)


def verify_existing(key):
    """Post-hardening readback: final binary verifies/publishes the SAME pinned bundles.

    GoldPublisher's completed-journal route never moves aliases backwards. No
    source ingest, Gold write, new snapshots or task clear happens in this check.
    """
    runtime.require(re.fullmatch(r"[a-f0-9]{32}", key), "EXACT_QA_EVIDENCE_REQUIRED")
    folder = runtime.root(os.environ) / "qa" / key
    report = json.loads((folder / "acceptance.json").read_text())
    runtime.require(report["status"] == "PASSED", "COMPLETED_QA_EVIDENCE_REQUIRED")
    checked = []
    for kind in ("daily", "manual"):
        item = report[kind]; op = item["operation_id"]
        runtime.require(re.fullmatch(r"orc07-[a-f0-9]{40}", op), "EXACT_QA_OPERATION_REQUIRED")
        base = runtime.root(os.environ) / op
        context = json.loads((base / "context.json").read_text()); runtime.validate_context(context)
        owner = {"dag_id": context["dag_id"], "run_id": context["run_id"]}
        lease("acquire", owner)
        _atomic_json(base / "execution.json", {"state": "VERIFYING", "context_sha256": context["context_sha256"]})
        upstream = json.loads((base / "gold-receipt.json").read_text())
        for phase in ("verify", "publish"):
            result = runtime.validate_receipt(phase, context, upstream, runtime.invoke(phase, context, upstream, os.environ))
            runtime.require(result == json.loads((base / (phase + "-receipt.json")).read_text()), "FINAL_BINARY_RECEIPT_CHANGED")
            upstream = result
        summary = json.loads((base / "summary.json").read_text())
        _atomic_json(base / "execution.json", {"state": "COMPLETE", "context_sha256": context["context_sha256"], "summary_sha256": runtime.digest(summary)})
        runtime.require(runtime.release(context, {k: v for k, v in summary.items() if k != "receipts"})["status"] == "RELEASED", "FINAL_BINARY_RELEASE_FAILED")
        checked.append({"operation_id": op, "snapshots": item["snapshots"], "publication_id": item["publication_id"],
                        "verify_receipt_sha256": runtime.digest(json.loads((base / "verify-receipt.json").read_text())),
                        "publish_receipt_sha256": runtime.digest(upstream)})
    binary = hashlib.sha256()
    with Path("/opt/spark/jobs/japan-earthquake-etl-runner.jar").open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""): binary.update(block)
    _atomic_json(folder / "final-binary-verification.json", {"status": "PASSED", "runner_sha256": binary.hexdigest(),
                 "backend": "MinIO-Trino-Java17", "new_gold_snapshots": False, "operations": checked})
    print("event=orc07_final_binary_verified evidence_id=" + key)


if __name__ == "__main__":
    try:
        if os.environ.get("ORC07_RECOVER_EVIDENCE_KEY"): recover_existing(os.environ["ORC07_RECOVER_EVIDENCE_KEY"])
        elif os.environ.get("ORC07_VERIFY_EVIDENCE_KEY"): verify_existing(os.environ["ORC07_VERIFY_EVIDENCE_KEY"])
        else: run()
    except BaseException:
        raise SystemExit("ORC07_ACCEPTANCE_FAILED inspect scoped QA/phase logs; lease may be retained") from None
