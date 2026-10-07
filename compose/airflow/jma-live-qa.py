"""JMA-05 live orchestration only; archive/readback/count validation runs in Java."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
from datetime import datetime, timezone

DAG = "jma_04_year_backfill"
YEARS = [1997, 2000, 2023]  # Fixed QA scope, never a 40-year/range default.
TIMEOUT = 1200


def cli(*arguments):
    result = subprocess.run(["airflow", *arguments], capture_output=True, text=True, timeout=45, check=False)
    if result.returncode:
        raise RuntimeError("AIRFLOW_CLI_FAILED")  # Do not echo arbitrary stderr/credentials.
    return result.stdout


def summary_path(run_id):
    slug = "jma-" + hashlib.sha256(run_id.encode()).hexdigest()[:32]
    return Path(os.environ.get("JMA_STAGING_ROOT", "/opt/pipeline/staging/jma")) / "runs" / slug / "run_summary.json"


def wait_run(run_id):
    deadline = time.monotonic() + TIMEOUT
    while time.monotonic() < deadline:
        state = cli("dags", "state", DAG, run_id).strip().splitlines()[-1].split(",", 1)[0].strip()
        if state == "success":
            return
        if state == "failed":
            raise RuntimeError("JMA_DAG_FAILED")
        time.sleep(3)
    raise RuntimeError("JMA_DAG_TIMEOUT")


def verify(run_id, report, baseline=None):
    args = [os.environ["JAVA_HOME"] + "/bin/java", "-cp", "/opt/pipeline/lib/usgs-ingest-runner.jar",
            "ie212.earthquake.spark.jma.JmaBronzeQaVerifier",
            "--summary", str(summary_path(run_id)), "--catalog", "/opt/airflow/smoke/dat01-catalog.json",
            "--inventory", os.environ["JMA_INVENTORY_PATH"], "--report", str(report)]
    if baseline:
        args += ["--baseline", str(baseline)]
    result = subprocess.run(args, capture_output=True, text=True, timeout=180, check=False)
    if result.returncode:
        raise RuntimeError("JMA_JAVA_READBACK_FAILED")


def main():
    token = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12]
    root = Path(os.environ.get("JMA_STAGING_ROOT", "/opt/pipeline/staging/jma")) / "qa" / token
    root.mkdir(parents=True, exist_ok=False)
    runs = {phase: "jma05-" + token + "-" + phase for phase in ("preview", "first", "rerun")}
    print("event=jma_qa_started evidence_id=" + token, flush=True)
    paused = None
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if json.loads(cli("dags", "list-import-errors", "--output", "json")):
                raise RuntimeError("DAG_IMPORT_ERRORS")
            dags = json.loads(cli("dags", "list", "--output", "json"))
            entry = next((item for item in dags if item.get("dag_id") == DAG), None)
            if entry:
                value = entry.get("is_paused")
                if value not in (True, False, "True", "False", "true", "false"):
                    raise RuntimeError("UNKNOWN_DAG_PAUSE_STATE")
                paused = str(value).lower() == "true"
                break
            time.sleep(3)
        if paused is None:
            raise RuntimeError("JMA_DAG_NOT_SERIALIZED")
        # max_active_runs=1 serializes QA and other operators' scoped manual runs.
        cli("dags", "unpause", DAG, "--yes")
        for phase, run_id in runs.items():
            conf = {"years": YEARS, "preview": phase == "preview", "force_download": False}
            cli("dags", "trigger", DAG, "--run-id", run_id, "--conf", json.dumps(conf))
            print("event=jma_qa_run phase=" + phase + " run_id=" + run_id, flush=True)
            wait_run(run_id)
            if phase == "preview":
                summary = json.loads(summary_path(run_id).read_text())
                if (summary.get("status") != "PREVIEW" or summary.get("verified") is not False
                        or summary.get("planned_archives") != 4 or summary.get("ready_archives") != 0):
                    raise RuntimeError("PREVIEW_GATE_MISMATCH")
            else:
                verify(run_id, root / (phase + "-readback.json"),
                       root / "first-readback.json" if phase == "rerun" else None)
    except Exception:
        # Failed runs and existing objects remain intact for diagnosis; no false success report.
        (root / "workflow-failure.json").write_text(json.dumps({"status": "FAILED", "runs": runs}) + "\n")
        print("event=jma_qa_failed evidence_id=" + token + " reason=WORKFLOW_OR_READBACK_FAILED", flush=True)
        raise SystemExit(1) from None
    finally:
        if paused is not None:
            try:
                cli("dags", "pause" if paused else "unpause", DAG, "--yes")
            except Exception:
                (root / "workflow-failure.json").write_text(json.dumps({"status": "FAILED", "runs": runs,
                                                                       "reason": "PAUSE_RESTORE_FAILED"}) + "\n")
                print("event=jma_qa_failed reason=PAUSE_RESTORE_FAILED", flush=True)
                raise SystemExit(1) from None
    # Commit the workflow QA evidence only after restoring the original pause state.
    evidence = {"qa_version": "jma-05-v1", "status": "VERIFIED", "years": YEARS,
                "dag_id": DAG, "airflow_run_states": {run: "success" for run in runs.values()},
                "reports": ["first-readback.json", "rerun-readback.json"],
                "preview_verified": True, "rerun_verified": True, "pause_state_restored": True}
    (root / "workflow-evidence.json").write_text(json.dumps(evidence, indent=2) + "\n")
    print("event=jma_qa_passed evidence_id=" + token + " archives=4 rerun_verified=true", flush=True)


if __name__ == "__main__":
    main()
