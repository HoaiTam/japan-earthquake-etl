#!/usr/bin/env python3
"""Explicit four-container stop/measure/restore session; never ETL, compose, build or pull."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import signal
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("light_rollout", ROOT / "scripts/research-rollout.py")
r = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(r)
TARGETS = ("spark-worker", "spark-master", "airflow-api-server", "airflow-dag-processor")
KEPT = (*r.SERVICES, "airflow-postgres")


def command(args, timeout=30):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=timeout).stdout


def rows(names=(*KEPT, *TARGETS)):
    result = json.loads(command(["docker", "inspect", *["japan-earthquake-etl-" + s + "-1" for s in names]]))
    r.require(len(result) == len(names), "LIGHT_EXACT_CONTAINER_COUNT_REQUIRED")
    return {row["Name"].removeprefix("/japan-earthquake-etl-").removesuffix("-1"): row for row in result}


def pin(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def snapshot(current):
    # Do not persist environment values, command lines, logs or credentials.
    return {name: {"id": row["Id"], "image": row["Image"], "config_sha256": pin(row["Config"]),
                   "host_config_sha256": pin(row["HostConfig"]), "mounts_sha256": pin(sorted(row["Mounts"], key=lambda mount: mount["Destination"])),
                   "state": row["State"]["Status"], "health": row["State"].get("Health", {}).get("Status"),
                   "started_at": row["State"]["StartedAt"], "restarts": row["RestartCount"]}
            for name, row in current.items()}


def healthy(current):
    return all(row["State"]["Status"] == "running" and row["State"].get("Health", {}).get("Status") == "healthy" for row in current.values())


def project(current, deployment):
    r.require(set(current) == set((*KEPT, *TARGETS)), "LIGHT_EXACT_CONTAINER_SET_REQUIRED")
    for row in current.values():
        labels = row["Config"]["Labels"]
        r.require(labels.get("com.docker.compose.project") == "japan-earthquake-etl"
                  and r.safe(labels["com.docker.compose.project.working_dir"]) == deployment, "LIGHT_FOREIGN_PROJECT_REJECTED")


def identity_unchanged(before, after, names):
    for name in names:
        r.require(name in before and name in after, "LIGHT_EXACT_CONTAINER_SET_REQUIRED")
        r.require(all(before[name][field] == after[name][field] for field in
                      ("id", "image", "config_sha256", "host_config_sha256", "mounts_sha256")), "LIGHT_RUNTIME_OR_CONFIG_DRIFT")


def unchanged(before, after):
    r.require(set(before) == set(after) == set((*KEPT, *TARGETS)), "LIGHT_EXACT_CONTAINER_SET_REQUIRED")
    identity_unchanged(before, after, (*KEPT, *TARGETS))
    for name in KEPT:
        fields = ("state", "health", "started_at", "restarts")
        r.require(all(before[name][field] == after[name][field] for field in fields), "LIGHT_RUNTIME_OR_CONFIG_DRIFT")


def spark_idle():
    script = '''import json,urllib.request
with urllib.request.urlopen("http://spark-master:8080/json/",timeout=10) as response:
    raw=response.read(1024*1024+1)
assert len(raw)<=1024*1024
row=json.loads(raw)
assert row.get("status")=="ALIVE" and isinstance(row.get("activeapps"),list) and not row["activeapps"]
assert isinstance(row.get("activedrivers"),list) and not row["activedrivers"]
assert isinstance(row.get("workers"),list) and len(row["workers"])==1
print("LIGHT_SPARK_IDLE")
'''
    r.require(command(["docker", "exec", "japan-earthquake-etl-airflow-scheduler-1", "python3", "-c", script], 20).strip() == "LIGHT_SPARK_IDLE", "LIGHT_SPARK_NOT_IDLE")


def host_sample():
    if sys.platform != "darwin": return {"status": "UNAVAILABLE_NOT_DARWIN"}
    try:
        first = command(["vm_stat"], 5); started = time.monotonic(); time.sleep(10); second = command(["vm_stat"], 5)
        elapsed = time.monotonic() - started
        size = int(re.search(r"page size of (\d+) bytes", second).group(1))
        def number(text, key): return int(re.search(r"^" + key + r":\s+(\d+)", text, re.M).group(1))
        delta = {key: number(second, key) - number(first, key) for key in ("Compressions", "Decompressions", "Swapins", "Swapouts")}
        r.require(all(n >= 0 for n in delta.values()), "LIGHT_HOST_COUNTER_RESET")
        return {"status": "OBSERVED", "seconds": round(elapsed, 3), "page_bytes": size, "delta_pages": delta,
                "mib_per_second": {key: round(n * size / 1048576 / elapsed, 3) for key, n in delta.items()}, "root_cause_proven": False}
    except (subprocess.SubprocessError, ValueError, AttributeError, OSError):
        return {"status": "UNAVAILABLE_NO_RESOURCE_CLAIM"}


def validate_runtime(deployment, runtime):
    expected = r.load(ROOT / "docs/evidence/DAT-03_CONTINUATION.json")["checkpoint_recovery"]
    r.require(runtime == deployment / "staging/dat03-continuation" / expected["attempt_id"] / "runtime", "LIGHT_EXACT_RETAINED_RUNTIME_REQUIRED")
    r.require(r.profile.sha(runtime / "report.json") == expected["report_sha256"]
              and r.profile.sha(runtime / "acceptance-recovery.json") == expected["acceptance_sha256"], "LIGHT_RETAINED_RUNTIME_PIN_CHANGED")


def restore(before):
    """Start only the exact approved old containers, never recreate a missing/replaced one."""
    current = rows(TARGETS); identity_unchanged(before, snapshot(current), TARGETS)
    stopped = [before[name]["id"] for name in TARGETS if current[name]["State"]["Status"] == "exited"]
    r.require(all(current[name]["State"]["Status"] in ("running", "exited") for name in TARGETS), "LIGHT_RESTORE_STATE_UNKNOWN")
    if stopped: command(["docker", "start", *stopped], 90)
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        current = rows(TARGETS); identity_unchanged(before, snapshot(current), TARGETS)
        if healthy(current):
            try:
                spark_idle(); return snapshot(rows())
            except (ValueError, subprocess.SubprocessError):
                pass  # Worker registration can lag its healthy web UI.
        time.sleep(5)
    raise ValueError("LIGHT_RESTORE_HEALTH_TIMEOUT_REQUIRES_OPERATOR")


def run(deployment, runtime):
    deployment = r.safe(deployment); runtime = r.safe(runtime); started = time.monotonic()
    r.require((deployment / "compose.yaml").is_file(), "LIGHT_DEPLOYMENT_REQUIRED")
    validate_runtime(deployment, runtime)
    current = rows(); project(current, deployment); r.require(healthy(current), "LIGHT_INITIAL_HEALTH_REQUIRED")
    r.services(); r.idle(); r.trino_queries_idle(); spark_idle()
    before = snapshot(current)
    staging = r.safe(deployment / "staging"); r.require(staging.is_dir(), "LIGHT_EXISTING_STAGING_REQUIRED")
    folder = r.safe(staging / ("dat03-light-session-" + uuid.uuid4().hex)); folder.mkdir(mode=0o700)
    r.save(folder / "before.json", {"version": "dat03-four-container-session-v1", "containers": before,
                                    "approved_targets": list(TARGETS), "kept": list(KEPT), "controller_sha256": r.profile.sha(Path(__file__)),
                                    "query_seconds": 90, "copies": 32, "session_budget_seconds": 7200})
    r.save(folder / "host-before.json", host_sample())
    print("event=dat03_light_prepared folder=" + str(folder), flush=True)
    results = []; failure = None; restored = None; stop_attempted = False
    try:
        # Fresh quiescence immediately before the one permitted stop command.
        unchanged(before, snapshot(rows())); r.idle(); r.trino_queries_idle(); spark_idle()
        r.save(folder / "stop-intent.json", {"container_ids": [before[name]["id"] for name in TARGETS], "automatic_retry": False})
        stop_attempted = True
        command(["docker", "stop", "--time", "30", *[before[name]["id"] for name in TARGETS]], 150)
        stopped = rows(); unchanged(before, snapshot(stopped))
        r.require(all(stopped[name]["State"]["Status"] == "exited" for name in TARGETS), "LIGHT_STOP_NOT_CONFIRMED")
        r.save(folder / "stopped.json", snapshot(stopped)); print("event=dat03_light_four_stopped", flush=True)
        time.sleep(20); r.save(folder / "host-light.json", host_sample())
        for strategy in ("full", "buckets-v1"):
            r.require(time.monotonic() - started < 7200 - 1500, "LIGHT_SESSION_BUDGET_RESTORE_RESERVED")
            result = r.probe(deployment, runtime, 32, strategy, 90); results.append(result)
            r.save(folder / (strategy + ".json"), result)
            # A timed-out server query with confirmed idle postflight is terminal.
            # Unknown client outcome, service drift, or other failures are never retried.
            if result["status"] not in ("STRESS_PROBE_PASSED", "BLOCKED_TIME", "BLOCKED_MEMORY") or result["postflight_status"] != "PASSED_UNCHANGED":
                raise ValueError("LIGHT_PROBE_OUTCOME_REQUIRES_READONLY_REVIEW")
        r.save(folder / "host-light-after.json", host_sample())
    except BaseException as error:
        failure = type(error).__name__  # Never persist a command payload or server error.
    finally:
        if stop_attempted:
            try:
                restored = restore(before); r.save(folder / "restored.json", restored)
                unchanged(before, restored); r.require(all(restored[name]["state"] == "running" and restored[name]["health"] == "healthy" for name in before), "LIGHT_FINAL_HEALTH_REQUIRED")
                print("event=dat03_light_four_restored healthy=true", flush=True)
            except BaseException as error:
                r.save(folder / "restoration-failed.json", {"error_type": type(error).__name__, "operator_attention_required": True})
                print("event=dat03_light_restore_failed operator_attention_required=true folder=" + str(folder), flush=True)
                raise
    if not stop_attempted:
        r.save(folder / "pre-stop-failed.json", {"error_type": failure, "stop_or_start_performed": False})
        raise ValueError("LIGHT_PREFLIGHT_FAILED_NO_CONTAINER_MUTATION")
    r.save(folder / "host-restored.json", host_sample())
    receipt = {"version": "dat03-four-container-session-v1", "four_original_containers_restored_healthy": restored is not None,
               "other_five_unchanged": True, "failure_type": failure, "probe_statuses": [x["status"] for x in results],
               "wall_seconds": round(time.monotonic() - started, 3), "source_or_lake_writes": False,
               "dag_lease_or_credential_mutations": False, "build_pull_recreate": False, "full_capacity_accepted": False}
    r.save(folder / "receipt.json", receipt)
    print("event=dat03_light_complete full_capacity_accepted=false folder=" + str(folder), flush=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-root", type=Path, required=True); parser.add_argument("--runtime-root", type=Path, required=True)
    parser.add_argument("--operator-approved-four-container-session", action="store_true"); args = parser.parse_args()
    r.require(args.operator_approved_four_container_session, "LIGHT_EXPLICIT_OPERATOR_APPROVAL_REQUIRED")
    def interrupted(_signal, _frame): raise SystemExit("LIGHT_INTERRUPTED_RESTORE_REQUIRED")
    signal.signal(signal.SIGTERM, interrupted)
    result = run(args.deployment_root, args.runtime_root)
    if result["failure_type"] or result["probe_statuses"] != ["STRESS_PROBE_PASSED", "STRESS_PROBE_PASSED"]: raise SystemExit(2)


if __name__ == "__main__": main()
