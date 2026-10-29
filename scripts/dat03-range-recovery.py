#!/usr/bin/env python3
"""Explicit range Verify/Publish recovery and pinned resume bridge. No ETL replay or quarantine."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
import zipfile

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("range_repair_worker", HERE / (
    "range-worker.py" if (HERE / "range-worker.py").is_file() else "dat03-range-worker.py"))
w = importlib.util.module_from_spec(spec); spec.loader.exec_module(w)
c, e = w.c, w.e
VERSION = "dat03-range-publication-repair-v1"
ENTRY = "ie212/earthquake/spark/usgs/ResearchRangePublicationRecovery.class"
CLASS = ENTRY[:-6].replace("/", ".")
CODE = ("controller.py", "range-worker.py", "research-catalog.py", "research-e2e.py", "runner.jar")
ID = r"[a-f0-9]{32}"
PIN = r"[a-f0-9]{64}"


def compatible(original, recovery):
    with zipfile.ZipFile(e.safe(original)) as a, zipfile.ZipFile(e.safe(recovery)) as b:
        e.require(len(a.namelist()) == len(set(a.namelist())) and len(b.namelist()) == len(set(b.namelist()))
                  and set(b.namelist()) - set(a.namelist()) == {ENTRY}
                  and set(a.namelist()) < set(b.namelist()), "RANGE_REPAIR_EXACT_CODE_SET")
        total = 0
        for item in a.infolist():
            total += item.file_size
            e.require(0 <= item.file_size <= 16*1024**2 and total <= 128*1024**2
                      and b.getinfo(item.filename).file_size == item.file_size
                      and a.read(item.filename) == b.read(item.filename), "RANGE_REPAIR_ORIGINAL_CODE_CHANGED")
        e.require(0 < b.getinfo(ENTRY).file_size <= 1024**2, "RANGE_REPAIR_ENTRY_BOUND")


def failure_names(failed, previous, pending):
    e.require(re.fullmatch(ID, failed) and re.fullmatch(r"20\d{2}-\d{2}", previous)
              and re.fullmatch(r"20\d{2}-\d{2}", pending), "RANGE_REPAIR_FAILURE_ID")
    return {f"sessions/{failed}/{n}" for n in ("intent.json", "execution.json", "stdout.private", "stderr.private")} | {
        f"months/{pending}/{n}.json" for n in ("context", "readiness", "bronze", "silver", "gold")} | {
        f"months/{previous}/{n}.json" for n in ("record", "checkpoint")} | {"bronze-pins.json"}


def snapshot(folder, failed, pin):
    """Read-only inspection on the owner's machine. Never prints private log contents."""
    r = w.validate(folder)
    e.require(re.fullmatch(ID, failed) and e.sha(folder / "request.json") == pin, "RANGE_REPAIR_TRUSTED_REQUEST_REQUIRED")
    e.require(folder.name == r["run_id"][12:], "RANGE_REPAIR_ROOT_IDENTITY_CHANGED")
    c.quiet({"dag_id": w.DAG, "run_id": r["run_id"]})
    sessions = folder / "sessions"
    e.require((sessions / failed).is_dir(), "RANGE_REPAIR_FAILED_SESSION_MISSING")
    for prior in sessions.iterdir():
        if prior.name != failed:
            w.acceptance(folder, r, prior)  # Any second unresolved session is a hard stop.
    old = sessions / failed
    x = c.load(old / "execution.json", 65536)
    intent = c.load(old / "intent.json", 65536)
    e.require(type(x["exit_code"]) is int and x["exit_code"] == 1 and x["timed_out"] is False
              and intent["request_sha256"] == pin and intent["version"] == w.VERSION
              and not (old / "acceptance.json").exists() and not (old / "result.json").exists(),
              "RANGE_REPAIR_KNOWN_TERMINAL_FAILURE_REQUIRED")
    err = e.safe(old / "stderr.private")
    e.require(0 < err.stat().st_size <= 16*1024**2
              and b"java.io.IOException: TRINO_QUERY_FAILED" in err.read_bytes(), "RANGE_REPAIR_TRINO_FAILURE_REQUIRED")
    planned = w.months(r); count = 0; previous = None; gap = False
    for month in planned:
        cp = folder / f"months/{month}/checkpoint.json"
        if not cp.exists(): gap = True; continue
        e.require(not gap, "RANGE_REPAIR_NONCONTIGUOUS_PREFIX")
        value = c.load(cp, 65536)
        e.require(value["month"] == month and value["status"] == "PUBLISHED"
                  and value["version"] == w.VERSION and value["request_identity_sha256"] == e.digest(r)
                  and value["previous_checkpoint_sha256"] == previous
                  and value["record_sha256"] == e.sha(folder / f"months/{month}/record.json"), "RANGE_REPAIR_PREFIX_CHANGED")
        count += 1; previous = e.sha(cp)
    e.require(0 < count < len(planned), "RANGE_REPAIR_PENDING_PREFIX_REQUIRED")
    ctx = c.load(folder / f"months/{planned[count]}/context.json", 65536)
    out = e.safe(old / "stdout.private")
    e.require(0 < out.stat().st_size <= 16*1024**2, "RANGE_REPAIR_EVIDENCE_BOUND")
    phases = re.findall(rb"event=dat03_catalog_phase month=(\d{4}-\d{2}) phase=([a-z]+)", out.read_bytes())
    e.require(phases and phases[-1] == (planned[count].encode(), b"verify"), "RANGE_REPAIR_VERIFY_FAILURE_REQUIRED")
    targets = ctx["resolver_request"]["target_months"]
    e.require(isinstance(targets, list) and 1 <= len(targets) <= 12
              and targets == planned[count:count+len(targets)], "RANGE_REPAIR_PENDING_SCOPE_CHANGED")
    names = failure_names(failed, planned[count-1], planned[count])
    for name in names:
        p = e.safe(folder / name)
        e.require(p.is_file() and 0 < p.stat().st_size <= 16*1024**2, "RANGE_REPAIR_EVIDENCE_BOUND")
    return {"initial_completed_months": count, "target_months": targets,
            "failed_files": {n: e.sha(folder / n) for n in sorted(names)}}


# Only pinned original controller/support is imported to inspect its own retained state.
INSPECT = r'''
import hashlib,importlib.util,json,sys
from pathlib import Path
p=Path(sys.argv[1]);pins=json.loads(sys.argv[2])
for n,h in pins.items():
 f=p/n;assert not any(x.is_symlink() for x in (f,*f.parents))
 assert hashlib.file_digest(f.open('rb'),'sha256').hexdigest()==h
sys.path.insert(0,str(p))
s=importlib.util.spec_from_file_location('original_range',p/'controller.py');w=importlib.util.module_from_spec(s);s.loader.exec_module(w)
namespace={'w':w,'c':w.c,'e':w.e,'re':__import__('re'),'ID':r'[a-f0-9]{32}'}
exec(sys.argv[3],namespace)
print(json.dumps(namespace['snapshot'](p,sys.argv[4],pins['request.json'])))
'''


def inspect_runtime(folder, failed, pin):
    import inspect
    e.require(re.fullmatch(PIN, pin) and e.sha(folder / "request.json") == pin, "RANGE_REPAIR_TRUSTED_REQUEST_REQUIRED")
    pins = {n: e.sha(folder / n) for n in ("request.json", "controller.py", "research-catalog.py", "research-e2e.py")}
    code = inspect.getsource(failure_names) + "\n" + inspect.getsource(snapshot)
    inside = "/opt/pipeline/staging/dat03-range/" + folder.name
    p = subprocess.run(["docker", "exec", e.CONTAINER, "python3", "-B", "-c", INSPECT,
                        inside, json.dumps(pins), code, failed], capture_output=True, text=True, timeout=120)
    if p.returncode != 0:
        reasons = re.findall(r"(?:RuntimeError|ValueError): ([A-Z][A-Z0-9_]{2,100})(?:\n|$)", p.stderr)
        raise RuntimeError("RANGE_REPAIR_INSPECTION_FAILED_LEASE_RETAINED" + (":" + reasons[-1] if reasons else ""))
    e.require(0 < len(p.stdout) <= 65536, "RANGE_REPAIR_INSPECTION_BOUND")
    return json.loads(p.stdout)


def prepare(deployment, folder, failed, pin, compiled):
    deployment, folder, compiled = map(e.safe, (deployment, folder, compiled))
    e.require(folder.parent == deployment / "staging/dat03-range" and re.fullmatch(ID, folder.name), "RANGE_REPAIR_HOST_ROOT_REQUIRED")
    w.validate(folder)
    view = inspect_runtime(folder, failed, pin)
    e.require(shutil.disk_usage(deployment).free >= 4*1024**3, "RANGE_REPAIR_PHYSICAL_DISK_RESERVE")
    attempt = deployment / "staging/dat03-range-repair" / uuid.uuid4().hex
    attempt.mkdir(parents=True, mode=0o700)
    with zipfile.ZipFile(folder / "runner.jar") as original, zipfile.ZipFile(compiled) as new:
        e.require(ENTRY not in original.namelist() and new.namelist().count(ENTRY) == 1, "RANGE_REPAIR_BUILD_ENTRY_FIRST")
        with zipfile.ZipFile(attempt / "runner.jar", "x") as out:
            for item in original.infolist(): out.writestr(item, original.read(item.filename))
            out.writestr(new.getinfo(ENTRY), new.read(ENTRY))
    os.chmod(attempt / "runner.jar", 0o600)
    compatible(folder / "runner.jar", attempt / "runner.jar")
    for source, name in ((Path(__file__), "controller.py"), (folder / "controller.py", "range-worker.py"),
                          (folder / "research-catalog.py", "research-catalog.py"), (folder / "research-e2e.py", "research-e2e.py")):
        w.copy_file(source, attempt / name)
    q = {"version": VERSION, "range_root": "/opt/pipeline/staging/dat03-range/" + folder.name,
         "range_request_sha256": pin, "failed_session": failed, "recovery_session": uuid.uuid4().hex,
         **view, "code": {n: e.sha(attempt / n) for n in CODE}, "runner_sha256": e.sha(attempt / "runner.jar")}
    e.save(attempt / "recovery-request.json", q)
    validate(attempt)
    return attempt


def validate(attempt):
    attempt = e.safe(attempt); q = c.load(attempt / "recovery-request.json", 65536)
    e.require(set(q) == {"version", "range_root", "range_request_sha256", "failed_session", "recovery_session",
                        "initial_completed_months", "target_months", "failed_files", "code", "runner_sha256"}
              and q["version"] == VERSION and re.fullmatch(r"/opt/pipeline/staging/dat03-range/" + ID, q["range_root"])
              and re.fullmatch(ID, q["failed_session"]) and re.fullmatch(ID, q["recovery_session"])
              and q["failed_session"] != q["recovery_session"] and re.fullmatch(PIN, q["range_request_sha256"])
              and type(q["initial_completed_months"]) is int and q["initial_completed_months"] > 0
              and set(q["code"]) == set(CODE) and q["runner_sha256"] == q["code"]["runner.jar"], "RANGE_REPAIR_REQUEST_INVALID")
    for n, pin in q["code"].items():
        e.require(re.fullmatch(PIN, pin) and e.sha(e.safe(attempt / n)) == pin, "RANGE_REPAIR_CODE_CHANGED")
    e.require(isinstance(q["target_months"], list) and 1 <= len(q["target_months"]) <= 12, "RANGE_REPAIR_TARGET_BOUND")
    from datetime import datetime
    first = datetime.strptime(q["target_months"][0], "%Y-%m")
    previous = f"{first.year-1}-12" if first.month == 1 else f"{first.year}-{first.month-1:02d}"
    e.require(set(q["failed_files"]) == failure_names(q["failed_session"], previous, q["target_months"][0])
              and all(re.fullmatch(PIN, v) for v in q["failed_files"].values()), "RANGE_REPAIR_FAILURE_PIN_SET_CHANGED")
    return q


def unchanged(folder, q):
    for n, pin in q["failed_files"].items():
        e.require(e.sha(e.safe(folder / n)) == pin, "RANGE_REPAIR_FAILED_EVIDENCE_CHANGED")


def accepted_recovery(attempt, folder, base):
    q = validate(attempt)
    e.require(q["range_root"] == str(folder) and e.sha(folder / "request.json") == q["range_request_sha256"], "RANGE_REPAIR_BASE_CHANGED")
    unchanged(folder, q)
    session = folder / "sessions" / q["recovery_session"]
    measured = w.acceptance(folder, base, session)
    a = c.load(session / "acceptance.json", 65536)
    e.require(a["recovery_request_sha256"] == e.sha(attempt / "recovery-request.json"), "RANGE_REPAIR_ACCEPTANCE_CHANGED")
    validate_measurement(attempt, q, measured)
    resolution = c.load(folder / "session-resolutions" / (q["failed_session"] + ".json"), 65536)
    e.require(resolution == resolution_value(attempt, q, session), "RANGE_REPAIR_RESOLUTION_CHANGED")
    return measured


def validate_measurement(attempt, q, measured):
    e.require(measured["recovery_request_sha256"] == e.sha(attempt / "recovery-request.json")
              and measured["recovery_runner_sha256"] == q["runner_sha256"]
              and measured["recovered_session"] == q["failed_session"]
              and measured["completed_months"] == q["initial_completed_months"] + len(q["target_months"]), "RANGE_REPAIR_ACCEPTANCE_CHANGED")
    checks = measured["fresh_current_pin_checks"]
    e.require(set(checks) == e.TABLES and all(type(checks[t]["actual"]) is int
              and checks[t]["actual"] == checks[t]["expected"] == measured["current_snapshots"][t]
              for t in e.TABLES), "RANGE_REPAIR_FRESH_PIN_CHECKS_CHANGED")


def resolution_value(attempt, q, session):
    return {"version": VERSION, "failed_session": q["failed_session"], "recovery_session": q["recovery_session"],
            "attempt": str(attempt), "request_sha256": e.sha(attempt / "recovery-request.json"),
            "acceptance_sha256": e.sha(session / "acceptance.json"), "failed_files": q["failed_files"]}


def resume_worker(attempt, folder, base, seconds, max_months):
    q = validate(attempt)
    accepted_recovery(attempt, folder, base)
    original_acceptance = w.acceptance
    def acceptance(root, request, prior):
        if prior.name == q["failed_session"]:
            w.acceptance = original_acceptance
            try: return accepted_recovery(attempt, root, request)
            finally: w.acceptance = acceptance
        return original_acceptance(root, request, prior)
    w.acceptance = acceptance
    try: w.worker(folder, seconds, max_months)
    finally: w.acceptance = original_acceptance


def worker(attempt, mode, seconds=6000, max_months=12):
    import fcntl
    attempt = e.safe(attempt)
    e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-range-repair/" + ID, str(attempt)), "RANGE_REPAIR_RUNTIME_REQUIRED")
    q = validate(attempt); folder = e.safe(q["range_root"])
    # OS advisory lock, not a second source lease. No stale-lock deletion/takeover.
    lock = e.safe(folder / "range-recovery-control.lock")
    with lock.open("a+b") as handle:
        os.chmod(lock, 0o600)
        try: fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError: raise RuntimeError("RANGE_REPAIR_CONTROLLER_ALREADY_ACTIVE") from None
        worker_locked(attempt, mode, seconds, max_months)


def worker_locked(attempt, mode, seconds, max_months):
    q = validate(attempt); folder = e.safe(q["range_root"]); base = w.validate(folder)
    e.require(e.sha(folder / "request.json") == q["range_request_sha256"], "RANGE_REPAIR_BASE_CHANGED")
    compatible(folder / "runner.jar", attempt / "runner.jar"); unchanged(folder, q)
    owner = {"dag_id": w.DAG, "run_id": base["run_id"]}
    if mode in ("resume", "report"):
        measured = accepted_recovery(attempt, folder, base)
        if mode == "report":
            print(json.dumps({k: measured[k] for k in ("status", "owner", "completed_months", "required_months", "research_ready", "official_exported")})); return
        # The frozen controller/JAR/request are NOT replaced. Only the explicitly
        # resolved failure gets an acceptance adapter, scoped to this pinned attempt.
        resume_worker(attempt, folder, base, seconds, max_months)
        return
    c.quiet(owner)
    if mode == "finalize":
        session = folder / "sessions" / q["recovery_session"]
        proof = c.load(session / "validation.json", 65536); x = c.load(session / "execution.json", 65536)
        e.require(proof == {"result_sha256": e.sha(session / "result.json"), "execution_sha256": e.sha(session / "execution.json"),
                            "request_sha256": e.sha(attempt / "recovery-request.json")}
                  and type(x["exit_code"]) is int and x["exit_code"] == 0 and x["timed_out"] is False, "RANGE_REPAIR_VALIDATION_REQUIRED")
        measured = w.result(folder, base, session)
        validate_measurement(attempt, q, measured)
        sys.path.insert(0, "/opt/airflow/dags"); from source_run_guard import lease
        c.quiet(owner); unchanged(folder, q)
        released = lease("release", owner)
        e.require(released == {"status": "RELEASED", **owner}, "RANGE_REPAIR_OWN_RELEASE_UNCONFIRMED")
        e.save(session / "acceptance.json", {"version": w.VERSION, "status": "CHECKPOINT_ACCEPTED",
               "request_sha256": q["range_request_sha256"], "result_sha256": e.sha(session / "result.json"),
               "execution_sha256": e.sha(session / "execution.json"), "lease_release": released,
               "recovery_request_sha256": e.sha(attempt / "recovery-request.json"), "research_ready": False})
        (folder / "session-resolutions").mkdir(exist_ok=True, mode=0o700)
        e.save(folder / "session-resolutions" / (q["failed_session"] + ".json"), resolution_value(attempt, q, session))
        accepted_recovery(attempt, folder, base)
        print("event=dat03_range_repair_accepted completed=" + str(measured["completed_months"])); return
    e.require(mode in ("preflight", "run") and shutil.disk_usage(folder).free >= 4*1024**3, "RANGE_REPAIR_MODE_OR_DISK")
    view = snapshot(folder, q["failed_session"], q["range_request_sha256"])
    e.require(all(view[k] == q[k] for k in view), "RANGE_REPAIR_RETAINED_STATE_CHANGED")
    deps = Path("/opt/spark/jobs/iceberg")
    intent = c.load(folder / "sessions" / q["failed_session"] / "intent.json", 65536)
    e.require(set(intent["iceberg_dependencies"]) == {"iceberg-spark-runtime-3.5_2.12-1.10.1.jar", "iceberg-aws-bundle-1.10.1.jar"}
              and all(e.sha(e.safe(deps / n)) == pin for n, pin in intent["iceberg_dependencies"].items()), "RANGE_REPAIR_DEPENDENCIES_CHANGED")
    output = attempt / "preflight" if mode == "preflight" else folder / "sessions" / q["recovery_session"]
    if mode == "run":
        check = c.load(attempt / "preflight.json", 65536)
        e.require(check["status"] == "READY_FOR_PUBLICATION_RECOVERY"
                  and check["request_sha256"] == e.sha(attempt / "recovery-request.json"), "RANGE_REPAIR_PREFLIGHT_REQUIRED")
    output.mkdir(mode=0o700)
    e.save(output / "intent.json", {"version": VERSION, "recovery_request_sha256": e.sha(attempt / "recovery-request.json"), "failed_session": q["failed_session"]})
    command = ["java", "-Xmx512m", "-XX:MaxDirectMemorySize=128m", "-cp",
               str(attempt / "runner.jar") + ":/opt/spark/jars/*:/opt/spark/jobs/iceberg/*", CLASS,
               str(attempt / "recovery-request.json"), mode]
    start = time.monotonic(); timed_out = False
    with (output / "stdout.private").open("xb") as out, (output / "stderr.private").open("xb") as err:
        os.chmod(out.name, 0o600); os.chmod(err.name, 0o600)
        child = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
        try: code = child.wait(timeout=6000)
        except subprocess.TimeoutExpired:
            timed_out = True; os.killpg(child.pid, signal.SIGTERM)
            try: code = child.wait(timeout=20)
            except subprocess.TimeoutExpired: os.killpg(child.pid, signal.SIGKILL); code = child.wait(timeout=10)
    e.save(output / "execution.json", {"exit_code": code, "timed_out": timed_out, "wall_seconds": round(time.monotonic()-start, 3)})
    e.require(code == 0 and not timed_out, "RANGE_REPAIR_FAILED_LEASE_RETAINED")
    c.quiet(owner); unchanged(folder, q)
    if mode == "run":
        w.result(folder, base, output)
        e.save(output / "validation.json", {"result_sha256": e.sha(output / "result.json"), "execution_sha256": e.sha(output / "execution.json"),
                                           "request_sha256": e.sha(attempt / "recovery-request.json")})
    print("event=dat03_range_repair_validated mode=" + mode)


STAGE = r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]);assert re.fullmatch(r'/opt/pipeline/staging/dat03-range-repair/[a-f0-9]{32}',str(p))
assert not any(x.is_symlink() for x in (p,*p.parents));p.mkdir(parents=True,mode=0o700)
items=json.loads(sys.stdin.buffer.readline(65537))
assert len(items)==6 and {x['name'] for x in items}=={'controller.py','range-worker.py','research-catalog.py','research-e2e.py','runner.jar','recovery-request.json'}
assert all(type(x['bytes']) is int and 0<x['bytes']<=64*1024**2 for x in items)
for item in items:
 data=sys.stdin.buffer.read(item['bytes']);assert len(data)==item['bytes'] and hashlib.sha256(data).hexdigest()==item['sha256']
 with (p/item['name']).open('xb') as f:os.chmod(f.name,0o600);f.write(data)
assert not sys.stdin.buffer.read(1)
'''
BOOT = r'''
import hashlib,json,runpy,sys
from pathlib import Path
p=Path(sys.argv[1]);f=p/'recovery-request.json'
assert not any(x.is_symlink() for x in (f,*f.parents))
assert hashlib.file_digest(f.open('rb'),'sha256').hexdigest()==sys.argv[2]
r=json.loads(f.read_bytes())
for n,h in r['code'].items():
 f=p/n;assert not any(x.is_symlink() for x in (f,*f.parents))
 assert hashlib.file_digest(f.open('rb'),'sha256').hexdigest()==h
sys.path.insert(0,str(p));sys.argv=[str(p/'controller.py'),'worker','--attempt',str(p),'--worker-mode',*sys.argv[3:]]
runpy.run_path(str(p/'controller.py'),run_name='__main__')
'''


def run(deployment, attempt, mode, seconds=6000, max_months=12):
    deployment, attempt = map(e.safe, (deployment, attempt)); q = validate(attempt)
    e.require(attempt.parent == deployment / "staging/dat03-range-repair" and re.fullmatch(ID, attempt.name), "RANGE_REPAIR_HOST_ATTEMPT_REQUIRED")
    e.require(type(seconds) is int and 3600 <= seconds <= 6600 and type(max_months) is int and 1 <= max_months <= 12, "RANGE_REPAIR_BUDGET_INVALID")
    if mode != "report":
        e.require(shutil.disk_usage(deployment).free >= 4*1024**3, "RANGE_REPAIR_PHYSICAL_DISK_RESERVE")
    rollout = c.module("research-rollout"); before = rollout.services()
    inside = "/opt/pipeline/staging/dat03-range-repair/" + attempt.name
    if mode == "preflight":
        listing = [{"name": n, "bytes": (attempt/n).stat().st_size, "sha256": e.sha(attempt/n)} for n in (*CODE, "recovery-request.json")]
        with subprocess.Popen(["docker", "exec", "-i", e.CONTAINER, "python3", "-c", STAGE, inside], stdin=subprocess.PIPE) as proc:
            proc.stdin.write((json.dumps(listing)+"\n").encode())
            for item in listing:
                with (attempt/item["name"]).open("rb") as source: shutil.copyfileobj(source, proc.stdin, 1024**2)
            proc.stdin.close(); e.require(proc.wait(timeout=120) == 0, "RANGE_REPAIR_STAGE_FAILED_DO_NOT_OVERWRITE")
    command = ["docker", "exec", e.CONTAINER, "python3", "-B", "-c", BOOT, inside, e.sha(attempt / "recovery-request.json")]
    subprocess.run([*command, mode, "--seconds", str(seconds), "--max-months", str(max_months)], check=True,
                   timeout=seconds+400 if mode == "resume" else 6400)
    e.require(rollout.services() == before, "RANGE_REPAIR_SERVICES_CHANGED_LEASE_RETAINED")
    if mode == "run": subprocess.run([*command, "finalize"], check=True, timeout=120)
    if mode in ("preflight", "run"):
        name = "preflight.json" if mode == "preflight" else "accepted-progress.json"
        if mode == "preflight":
            subprocess.run(["docker", "cp", e.CONTAINER+":"+inside+"/preflight.json", str(attempt / name)], check=True, timeout=60)
        else:
            proof = subprocess.run([*command, "report"], check=True, capture_output=True, text=True, timeout=120)
            e.save(attempt / name, json.loads(proof.stdout))
    print("RANGE_REPAIR_HOST_POSTFLIGHT_PASSED; no service lifecycle/data cleanup performed")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=("prepare", "preflight", "run", "resume", "report", "worker"))
    p.add_argument("--deployment-root", type=Path); p.add_argument("--folder", type=Path)
    p.add_argument("--failed-session"); p.add_argument("--request-sha256"); p.add_argument("--attempt", type=Path)
    p.add_argument("--runner", type=Path, default=HERE.parent / "spark/target/japan-earthquake-etl-runner.jar")
    p.add_argument("--worker-mode", choices=("preflight", "run", "finalize", "resume", "report"))
    p.add_argument("--seconds", type=int, default=6000); p.add_argument("--max-months", type=int, default=12)
    a = p.parse_args()
    if a.mode == "prepare":
        e.require(all(x is not None for x in (a.deployment_root, a.folder, a.failed_session, a.request_sha256)), "RANGE_REPAIR_ARGUMENTS_REQUIRED")
        print(prepare(a.deployment_root, a.folder, a.failed_session, a.request_sha256, a.runner))
    elif a.mode == "worker": worker(a.attempt, a.worker_mode, a.seconds, a.max_months)
    else: run(a.deployment_root, a.attempt, a.mode, a.seconds, a.max_months)


if __name__ == "__main__": main()
