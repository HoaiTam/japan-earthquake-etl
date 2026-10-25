#!/usr/bin/env python3
"""DAT-03 block 1: read-only recovery preflight. Never resume ETL, publish or release a lease."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dat03_e2e", Path(__file__).with_name("research-e2e.py"))
e2e = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(e2e)
VERSION = "dat03-recovery-preflight-v1"
COMPLETED = ("2000-01", "2011-04")
INCOMPLETE = "2016-04"
SERVICES = ("airflow-scheduler", "minio", "iceberg-rest", "trino")


def retained(folder, pins):
    """Validate immutable failure/input/completed proofs before any Docker call."""
    folder = e2e.safe(folder)
    for name in ("request", "execution"):
        e2e.require(e2e.sha(e2e.safe(folder / (name + ".json"))) == pins[name + "_sha256"], "DAT03_RECOVERY_ORIGINAL_CHANGED")
    request = e2e.load(folder / "request.json", 65536)
    execution = e2e.load(folder / "execution.json", 65536)
    e2e.validate_inputs(folder, request)
    e2e.require(type(execution.get("exit_code")) is int and execution["exit_code"] != 0
                and execution.get("lease_held_at_execution_end") is True, "DAT03_RECOVERY_FAILED_EXECUTION_REQUIRED")
    e2e.require(not any((folder / n).exists() for n in ("acceptance.json", "report.json", "progress-2016-04.json", "progress-2023-12.json")),
                "DAT03_RECOVERY_UNEXPECTED_COMPLETION")
    e2e.require(set(pins["completed_progress_sha256"]) == set(COMPLETED), "DAT03_RECOVERY_COMPLETED_SET_CHANGED")
    for month in COMPLETED:
        file = e2e.safe(folder / ("progress-" + month + ".json"))
        e2e.require(e2e.sha(file) == pins["completed_progress_sha256"][month], "DAT03_RECOVERY_PROGRESS_CHANGED")
        row = e2e.load(file)
        e2e.require(row.get("month") == month and row.get("rerun_identical") is True and row.get("retention_verified") is True
                    and row.get("first") == row.get("rerun"), "DAT03_RECOVERY_COMPLETED_CASE_INVALID")
        e2e.publication(row["first"], request["namespace"], folder)
    op = "orc07-" + hashlib.sha256(("dat03_bounded_e2e|" + request["run_id"] + "-" + INCOMPLETE).encode()).hexdigest()[:40]
    context_file = e2e.safe(folder / op / "context.json")
    e2e.require(e2e.sha(context_file) == pins["incomplete_context_file_sha256"], "DAT03_RECOVERY_CONTEXT_CHANGED")
    commit_file = e2e.safe(folder / op / "gold.json")
    e2e.require(e2e.sha(commit_file) == pins["incomplete_commit_file_sha256"], "DAT03_RECOVERY_COMMIT_CHANGED")
    commit = e2e.load(commit_file)
    actual = {t: v["snapshot_id"] for t, v in commit["tables"].items()}
    e2e.require(set(actual) == e2e.TABLES and all(type(v) is int and 0 < v <= 9223372036854775807 for v in actual.values())
                and actual == pins["incomplete_snapshots"], "DAT03_RECOVERY_SNAPSHOTS_CHANGED")
    # All paths Java will read locally must be regular metadata, not ancestor symlinks.
    for month in (*COMPLETED, INCOMPLETE):
        operation = "orc07-" + hashlib.sha256(("dat03_bounded_e2e|" + request["run_id"] + "-" + month).encode()).hexdigest()[:40]
        for name in ("context", *(e2e.PHASES[:4] if month == INCOMPLETE else e2e.PHASES)):
            e2e.load(folder / operation / (name + ".json"))
    return request


def prepare(deployment, original, evidence):
    deployment, original = e2e.safe(deployment), e2e.safe(original)
    partial = e2e.load(evidence)
    e2e.require(partial["version"] == "dat03-bounded-e2e-partial-v1" and partial["status"] == "FAILED_AT_2016_04_VERIFY",
                "DAT03_RECOVERY_EXPECTED_FAILURE_REQUIRED")
    pins = {"request_sha256": partial["request_sha256"], "execution_sha256": partial["execution_sha256"],
            "completed_progress_sha256": {r["month"]: r["progress_sha256"] for r in partial["completed_cases"]}}
    request = e2e.load(original / "request.json", 65536)
    op = "orc07-" + hashlib.sha256(("dat03_bounded_e2e|" + request["run_id"] + "-" + INCOMPLETE).encode()).hexdigest()[:40]
    pins["incomplete_context_file_sha256"] = e2e.sha(e2e.safe(original / op / "context.json"))
    pins["incomplete_commit_file_sha256"] = e2e.sha(e2e.safe(original / op / "gold.json"))
    pins["incomplete_snapshots"] = partial["incomplete_case"]["snapshots"]
    request = retained(original, pins)
    e2e.require(request["run_id"] == partial["run_id"] and request["namespace"] == partial["namespace"]
                and request["runner_sha256"] == partial["runner_sha256"] and request["lease_owner"] == partial["lease_retained"], "DAT03_RECOVERY_RUN_CHANGED")
    relative = original.relative_to(deployment / "staging/dat03-e2e")
    e2e.require(str(relative) == request["run_id"][6:] + "/runtime", "DAT03_RECOVERY_HOST_RUNTIME_REQUIRED")
    jar = e2e.safe(ROOT / "spark/target/japan-earthquake-etl-runner.jar")
    e2e.require(jar.is_file() and jar.stat().st_size <= 64 * 1024**2, "DAT03_RECOVERY_PREBUILT_JAR_REQUIRED")
    e2e.require(e2e.sha(jar) != request["runner_sha256"], "DAT03_RECOVERY_SEPARATE_RUNNER_REQUIRED")
    folder = e2e.safe(deployment / "staging/dat03-recovery" / uuid.uuid4().hex)
    folder.mkdir(parents=True, mode=0o700)
    shutil.copyfile(jar, folder / "preflight.jar")
    shutil.copyfile(Path(__file__), folder / "preflight.py")
    shutil.copyfile(Path(__file__).with_name("research-e2e.py"), folder / "research-e2e.py")
    e2e.save(folder / "preflight-request.json", {"version": VERSION, **pins,
        "original_root": "/opt/pipeline/staging/dat03-e2e/" + request["run_id"][6:], "preflight_runner_sha256": e2e.sha(jar)})
    return folder


def assert_owner(owner):
    # The shared acquire/release helper opens mutex a+ and may mkdir. Preflight
    # must not create even a replacement lock/root when retained state is absent.
    import fcntl
    guard = e2e.safe(Path(os.environ.get("SOURCE_GUARD_ROOT", "/opt/pipeline/staging/source-guard")))
    with e2e.safe(guard / "mutex.lock").open("rb") as mutex:
        fcntl.flock(mutex, fcntl.LOCK_SH)
        try:
            e2e.require(e2e.load(guard / "owner.json", 65536) == owner, "DAT03_RECOVERY_LEASE_NOT_HELD")
        finally:
            fcntl.flock(mutex, fcntl.LOCK_UN)


def quiescent(owner):
    sys.path.insert(0, "/opt/airflow/dags")
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from sqlalchemy import select
    assert_owner(owner)  # Never acquire/release/take over or create missing state.
    with create_session() as session:
        active = session.scalars(select(DagRun).where(DagRun.state.in_(["running", "queued"]))).first()
    java = []
    for proc in Path("/proc").iterdir():
        if proc.name.isdigit():
            try:
                command = (proc / "comm").read_text().strip()
            except FileNotFoundError:
                continue
            if command == "java":
                java.append(proc.name)
    assert_idle(active is not None, java)


def assert_idle(active, java):
    e2e.require(not active, "DAT03_RECOVERY_ACTIVE_DAGRUN")
    e2e.require(not java, "DAT03_RECOVERY_ACTIVE_JAVA")


def validate_plan(plan, pins, request):
    e2e.require(plan.get("version") == VERSION and plan.get("status") == "READY_FOR_PUBLICATION_RECOVERY"
                and plan.get("run_id") == request["run_id"] and plan.get("namespace") == request["namespace"]
                and plan.get("original_runner_sha256") == request["runner_sha256"] and plan.get("month") == INCOMPLETE
                and all(plan.get(k) == pins[k] for k in ("preflight_runner_sha256", "request_sha256", "execution_sha256"))
                and all(plan.get(k) is False for k in ("recovered", "acceptance_passed", "research_ready", "can_release_lease"))
                and plan.get("next_phases") == ["verify", "publish"] and plan.get("do_not_replay") == list(e2e.PHASES[:4])
                and plan.get("remaining_case") == "2023-12", "DAT03_RECOVERY_PLAN_INVALID")
    e2e.require(set(plan["snapshots"]) == e2e.TABLES and len(plan["checks"]) == 13, "DAT03_RECOVERY_THIRTEEN_METADATA_CHECKS_REQUIRED")
    e2e.require(plan["snapshots"] == pins["incomplete_snapshots"], "DAT03_RECOVERY_SNAPSHOTS_CHANGED")
    absent = plan["checks"]["publication_absent"]
    e2e.require(type(absent.get("actual")) is int and type(absent.get("expected")) is int
                and absent["actual"] == absent["expected"] == 0, "DAT03_RECOVERY_PARTIAL_PUBLICATION")
    for table, snapshot in plan["snapshots"].items():
        e2e.require(type(snapshot) is int and 0 < snapshot <= 9223372036854775807, "DAT03_RECOVERY_PIN_INVALID")
        for kind, expected in (("current", snapshot), ("identity", 1)):
            row = plan["checks"][kind + ":" + table]
            e2e.require(type(row.get("actual")) is int and type(row.get("expected")) is int
                        and row["actual"] == row["expected"] == expected, "DAT03_RECOVERY_CHECK_FAILED")
        e2e.require(plan["checks"]["current:" + table]["sql"] == "SELECT snapshot_id FROM " + request["namespace"]
                    + '."' + table + '$refs" WHERE name=\'main\'', "DAT03_RECOVERY_SQL_CHANGED")


def worker(folder):
    folder = e2e.safe(folder)
    e2e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-recovery/[a-f0-9]{32}", str(folder)), "DAT03_RECOVERY_EXACT_ROOT_REQUIRED")
    pins = e2e.load(folder / "preflight-request.json", 65536)
    original = e2e.safe(Path(pins["original_root"]))
    e2e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-e2e/[a-f0-9]{32}", str(original)), "DAT03_RECOVERY_EXACT_ROOT_REQUIRED")
    e2e.require(pins["version"] == VERSION and e2e.sha(e2e.safe(folder / "preflight.jar")) == pins["preflight_runner_sha256"], "DAT03_RECOVERY_RUNNER_CHANGED")
    e2e.require(not any((folder / n).exists() for n in ("plan.json", "execution.json", "stdout.private", "stderr.private")), "DAT03_RECOVERY_ATTEMPT_EXISTS")
    request = retained(original, pins)
    quiescent(request["lease_owner"])
    command = ["java", "-Xmx256m", "-cp", str(folder / "preflight.jar") + ":/opt/spark/jars/*:/opt/spark/jobs/iceberg/*",
               "ie212.earthquake.spark.usgs.ResearchRecoveryPreflight", str(folder / "preflight-request.json")]
    with (folder / "stdout.private").open("xb") as out, (folder / "stderr.private").open("xb") as err:
        os.chmod(folder / "stdout.private", 0o600); os.chmod(folder / "stderr.private", 0o600)
        child = subprocess.run(command, stdout=out, stderr=err, timeout=180, check=False)
    e2e.save(folder / "execution.json", {"exit_code": child.returncode, "readonly": True, "lease_released": False})
    e2e.require(child.returncode == 0, "DAT03_RECOVERY_PREFLIGHT_FAILED_LEASE_RETAINED")
    quiescent(request["lease_owner"])
    retained(original, pins)
    plan = e2e.load(folder / "stdout.private")
    validate_plan(plan, pins, request)
    plan["quiescence_checked_before_and_after"] = True
    e2e.save(folder / "plan.json", plan)
    print("event=dat03_recovery_preflight_passed readonly=true lease_released=false", flush=True)


def health():
    value = subprocess.run(["docker", "inspect", *["japan-earthquake-etl-" + s + "-1" for s in SERVICES]],
                           check=True, capture_output=True, text=True, timeout=20)
    containers = json.loads(value.stdout)
    e2e.require(len(containers) == len(SERVICES) and all(c["State"]["Status"] == "running"
                and c["State"].get("Health", {}).get("Status") == "healthy" for c in containers), "DAT03_RECOVERY_SERVICES_NOT_READY")
    return {c["Name"]: {"started_at": c["State"]["StartedAt"], "restart_count": c["RestartCount"],
                        "oom_killed": c["State"]["OOMKilled"]} for c in containers}


TRANSFER = r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]); assert re.fullmatch(r"/opt/pipeline/staging/dat03-recovery/[a-f0-9]{32}",str(p))
files=json.loads(sys.stdin.buffer.readline(16385)); assert isinstance(files,list) and len(files)==4
assert {f['name'] for f in files}=={'preflight.jar','preflight.py','research-e2e.py','preflight-request.json'}
assert not any(q.is_symlink() for q in (p,*p.parents))
p.parent.mkdir(parents=True,exist_ok=True); p.mkdir(mode=0o700)
for f in files:
    assert type(f['bytes']) is int and 0<f['bytes']<=64*1024**2
    h=hashlib.sha256(); remaining=f['bytes']
    with (p/f['name']).open('xb') as out:
        os.chmod(p/f['name'],0o600)
        while remaining:
            b=sys.stdin.buffer.read(min(remaining,1024**2)); assert b
            h.update(b); out.write(b); remaining-=len(b)
    assert h.hexdigest()==f['sha256']
assert not sys.stdin.buffer.read(1)
'''


def check(deployment, folder):
    deployment, folder = e2e.safe(deployment), e2e.safe(folder)
    relative = folder.relative_to(deployment / "staging/dat03-recovery")
    e2e.require(re.fullmatch(r"[a-f0-9]{32}", str(relative)) and not (folder / "runtime").exists(), "DAT03_RECOVERY_FRESH_ATTEMPT_REQUIRED")
    before = health(); inside = "/opt/pipeline/staging/dat03-recovery/" + str(relative)
    paths = [e2e.safe(folder / name) for name in ("preflight.jar", "preflight.py", "research-e2e.py", "preflight-request.json")]
    files = [{"name": p.name, "bytes": p.stat().st_size, "sha256": e2e.sha(p)} for p in paths]
    with subprocess.Popen(["docker", "exec", "-i", e2e.CONTAINER, "python3", "-c", TRANSFER, inside], stdin=subprocess.PIPE) as child:
        child.stdin.write((json.dumps(files) + "\n").encode())
        for path in paths:
            with path.open("rb") as source:
                shutil.copyfileobj(source, child.stdin, 1024**2)
        child.stdin.close(); e2e.require(child.wait(timeout=120) == 0, "DAT03_RECOVERY_STAGE_FAILED")
    try:
        subprocess.run(["docker", "exec", e2e.CONTAINER, "python3", inside + "/preflight.py", "worker", "--folder", inside], check=True, timeout=210)
        after = health(); e2e.require(before == after, "DAT03_RECOVERY_SERVICE_RESTARTED")
        e2e.save(folder / "service-state.json", {"before": before, "after": after})
    finally:
        subprocess.run(["docker", "cp", e2e.CONTAINER + ":" + inside, str(folder / "runtime")], check=True, timeout=120)
    print("event=dat03_recovery_ready plan=" + str(folder / "runtime/plan.json"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("prepare", "check", "worker"))
    parser.add_argument("--deployment-root", type=Path)
    parser.add_argument("--original-root", type=Path)
    parser.add_argument("--evidence", type=Path, default=ROOT / "docs/evidence/DAT-03_BOUNDED_E2E_PARTIAL.json")
    parser.add_argument("--folder", type=Path)
    args = parser.parse_args()
    if args.mode == "prepare":
        e2e.require(args.deployment_root is not None and args.original_root is not None, "DAT03_RECOVERY_EXPLICIT_PATHS_REQUIRED")
        print(prepare(args.deployment_root, args.original_root, args.evidence))
    elif args.mode == "check":
        e2e.require(args.deployment_root is not None and args.folder is not None, "DAT03_RECOVERY_EXPLICIT_PATHS_REQUIRED")
        check(args.deployment_root, args.folder)
    else:
        e2e.require(args.folder is not None, "DAT03_RECOVERY_EXPLICIT_PATHS_REQUIRED")
        worker(args.folder)


if __name__ == "__main__":
    main()
