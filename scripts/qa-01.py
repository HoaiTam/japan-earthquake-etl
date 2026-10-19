#!/usr/bin/env python3
"""Collect one existing scheduled daily run. Never builds, starts or triggers ETL."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[1]
SERVICES = {"airflow-scheduler", "airflow-postgres", "minio", "iceberg-rest", "trino"}


def require(value, reason):
    if not value:
        raise RuntimeError("QA01_" + reason)


def execute(argv, timeout=300):
    return subprocess.run(argv, cwd=ROOT, capture_output=True, timeout=timeout, check=False)


def collect(run_id, env_file, compose_file):
    require(re.fullmatch(r"scheduled__[0-9T:.+Z-]{10,80}", run_id), "SCHEDULED_RUN_REQUIRED")
    compose = ["docker", "compose", "--env-file", env_file, "-f", compose_file]
    health = execute(compose + ["ps", "--format", "{{.Service}}:{{.State}}:{{.Health}}"], timeout=20)
    require(health.returncode == 0, "DOCKER_UNAVAILABLE")
    lines = health.stdout.decode().splitlines()
    require(all(name + ":running:healthy" in lines for name in SERVICES), "STACK_NOT_HEALTHY")
    folder = ROOT / "staging/qa-01" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:12])
    folder.mkdir(mode=0o700, parents=True)
    result = execute(compose + ["exec", "-T", "airflow-scheduler", "python",
                     "/opt/airflow/dags/qa_01_daily_e2e.py", "--run-id", run_id])
    require(len(result.stdout) <= 2 * 1024 * 1024 and len(result.stderr) <= 2 * 1024 * 1024, "OUTPUT_LIMIT")
    private = folder / "collector.private.log"
    with private.open("xb") as output:
        os.chmod(private, 0o600)
        output.write(result.stdout + result.stderr)
    reports = [line.removeprefix(b"QA01_REPORT=") for line in result.stdout.splitlines() if line.startswith(b"QA01_REPORT=")]
    require(len(reports) == 1, "COLLECTOR_REPORT_MISSING")
    report = json.loads(reports[0])
    require(report.get("task_id") == "QA-01" and report.get("report_version") == "qa-01-v1"
            and report.get("status") in {"PASSED", "NOT_READY", "FAILED"}, "REPORT_INVALID")
    if report["status"] == "PASSED":
        require(result.returncode == 0 and report.get("runtime_complete") is True
                and report.get("scheduled_run") is True and report.get("run", {}).get("run_id") == run_id
                and report.get("ml_upstream_eligible") is True and len(report.get("sql_readback", [])) == 17, "REPORT_INVALID")
    else:
        require(result.returncode != 0 and report.get("runtime_complete") is False, "REPORT_EXIT_MISMATCH")
    report["collected_at_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report["collector_sha256"] = hashlib.sha256((ROOT / "airflow/dags/qa_01_daily_e2e.py").read_bytes()).hexdigest()
    target = folder / "report.json"
    target.write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
    text = ["# QA-01 daily E2E", "", "- Status: `" + report["status"] + "`", "- Requested run: `" + run_id + "`",
            "- Reason: `" + report.get("reason", "ALL_READBACK_CHECKS_PASSED") + "`",
            "- ML upstream eligible: `" + str(report.get("ml_upstream_eligible", False)) + "`", "",
            "Full exact pins, counts, snapshots and SQL: [report.json](./report.json).",
            "Private collector log is local only; do not commit it.", ""]
    (folder / "report.md").write_text("\n".join(text))
    print("event=qa01_collected status=" + report["status"] + " report=" + str(target.relative_to(ROOT)))
    return 0 if report["status"] == "PASSED" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--compose-file", default="compose.yaml")
    args = parser.parse_args()
    return collect(args.run_id, args.env_file, args.compose_file)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        reason = str(error)
        raise SystemExit(reason if re.fullmatch(r"QA01_[A-Z0-9_]+", reason) else "QA01_CONTROL_FAILED") from None
