#!/usr/bin/env python3
"""Explicit DAT-03 publication-only recovery; never retry ETL or change a failed execution."""
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
SPEC = importlib.util.spec_from_file_location("catalog_recovery_support", HERE / "research-catalog.py")
c = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(c)
e = c.e
LEGACY_VERSION = "dat03-catalog-publication-recovery-v1"
VERSION = "dat03-catalog-publication-recovery-v2"
SQL_PROFILE = "dat03-select-poll-300s-v1"
TRANSPORT = "ie212/earthquake/spark/gold/TrinoSqlClient.class"
ENTRY = "ie212/earthquake/spark/usgs/ResearchCatalogRecoveryJob.class"
CLASS = "ie212.earthquake.spark.usgs.ResearchCatalogRecoveryJob"
CODE = {"controller.py": "recovery_controller_sha256", "research-catalog.py": "catalog_controller_sha256", "research-e2e.py": "support_sha256", "runner.jar": "runner_sha256"}


def failure_names(failed, initial, targets, related=()):
    e.require(re.fullmatch(r"[a-f0-9]{32}", failed) and type(initial) is int and 0 < initial < 288
              and isinstance(targets, list) and 1 <= len(targets) <= 12
              and targets == c.module_months()[initial:initial+len(targets)], "RECOVERY_EXACT_PREFIX_REQUIRED")
    e.require(isinstance(related, (tuple, list)) and len(related) <= 2 and len(set(related)) == len(related)
              and failed not in related and all(isinstance(sid, str) and re.fullmatch(r"[a-f0-9]{32}", sid) for sid in related), "RECOVERY_RELATED_FAILURES_INVALID")
    return {f"sessions/{sid}/{name}" for sid in (failed, *related) for name in ("intent.json", "execution.json", "stdout.private", "stderr.private")} | {
        f"months/{targets[0]}/{name}.json" for name in ("context", "readiness", "bronze", "silver", "gold")} | {
        f"months/{c.module_months()[initial-1]}/{name}.json" for name in ("record", "checkpoint")} | {"bronze-pins.json"}


def compatible(original, recovery, sql_profile=None):
    e.require(sql_profile in (None, SQL_PROFILE), "RECOVERY_SQL_PROFILE_INVALID")
    with zipfile.ZipFile(e.safe(original)) as a, zipfile.ZipFile(e.safe(recovery)) as b:
        e.require(len(a.namelist()) == len(set(a.namelist())) and len(b.namelist()) == len(set(b.namelist()))
                  and set(b.namelist()) - set(a.namelist()) == {ENTRY} and set(a.namelist()) <= set(b.namelist()), "RECOVERY_EXACT_JAR_ENTRY_SET_REQUIRED")
        total = 0
        for item in a.infolist():
            e.require(item.file_size <= 16*1024**2 and b.getinfo(item.filename).file_size <= 16*1024**2, "RECOVERY_JAR_BOUND")
            total += item.file_size + b.getinfo(item.filename).file_size
            e.require(total <= 256*1024**2 and (a.read(item.filename) == b.read(item.filename)
                      or (sql_profile == SQL_PROFILE and item.filename == TRANSPORT)), "RECOVERY_DATA_PLANE_CHANGED")
        e.require(0 < b.getinfo(ENTRY).file_size <= 1024**2, "RECOVERY_JAR_BOUND")


def validate(attempt, catalog=None):
    attempt = e.safe(attempt); request = c.load(attempt / "recovery-request.json", 65536)
    fields = {"version", "catalog_root", "continuation_root", "catalog_request_sha256", "continuation_request_sha256", "base_runner_sha256",
              "failed_session", "recovery_session", "initial_completed_months", "target_months", "failed_files", *CODE.values()}
    v2 = request.get("version") == VERSION
    if v2: fields |= {"sql_read_profile", "transport_sha256", "related_failed_sessions"}
    e.require(set(request) == fields and request["version"] in (VERSION, LEGACY_VERSION)
              and re.fullmatch(r"/opt/pipeline/staging/dat03-catalog/[a-f0-9]{32}", request["catalog_root"])
              and re.fullmatch(r"/opt/pipeline/staging/dat03-catalog-continuation/[a-f0-9]{32}", request["continuation_root"])
              and re.fullmatch(r"[a-f0-9]{32}", request["recovery_session"])
              and request["failed_session"] != request["recovery_session"], "RECOVERY_REQUEST_INVALID")
    related = request["related_failed_sessions"] if v2 else ()
    e.require(set(request["failed_files"]) == failure_names(request["failed_session"], request["initial_completed_months"], request["target_months"], related)
              and all(isinstance(pin, str) and re.fullmatch(r"[a-f0-9]{64}", pin) for pin in request["failed_files"].values()), "RECOVERY_FAILURE_PIN_SET_CHANGED")
    for name, key in CODE.items():
        e.require(e.sha(e.safe(attempt / name)) == request[key], "RECOVERY_CODE_CHANGED")
    if v2:
        with zipfile.ZipFile(attempt / "runner.jar") as jar:
            e.require(request["sql_read_profile"] == SQL_PROFILE and re.fullmatch(r"[a-f0-9]{64}", request["transport_sha256"])
                      and e.hashlib.sha256(jar.read(TRANSPORT)).hexdigest() == request["transport_sha256"], "RECOVERY_SQL_PROFILE_CHANGED")
    if catalog is not None:
        base = c.validate(catalog)
        e.require(e.sha(catalog / "request.json") == request["catalog_request_sha256"]
                  and Path(request["catalog_root"]).name == catalog.name, "RECOVERY_CATALOG_CHANGED")
        return request, base
    return request


def prepare(deployment, catalog, continuation, observation, failed, related=()):
    deployment, catalog, continuation = map(e.safe, (deployment, catalog, continuation))
    e.require(catalog.parent == deployment / "staging/dat03-catalog" and continuation.parent == deployment / "staging/dat03-catalog-continuation", "RECOVERY_EXACT_HOST_ROOTS_REQUIRED")
    extra = c.continuation_inputs(catalog, continuation); view = c.observation_view(observation)
    e.require(c.validate(view) == c.validate(catalog), "RECOVERY_OBSERVATION_INPUTS_CHANGED")
    initial = len(list((view / "months").glob("*/checkpoint.json")))
    targets = c.load(view / f"months/{c.module_months()[initial]}/context.json")["resolver_request"]["target_months"]
    execution = c.load(view / f"sessions/{failed}/execution.json", 65536)
    e.require(type(execution["exit_code"]) is int and execution["exit_code"] != 0 and execution["timed_out"] is False
              and not (view / f"sessions/{failed}/acceptance.json").exists()
              and not (view / f"sessions/{failed}/result.json").exists(), "RECOVERY_TERMINAL_FAILURE_REQUIRED")
    names = failure_names(failed, initial, targets, related)
    for sid in related:
        prior = c.load(view / f"sessions/{sid}/execution.json", 65536)
        intent = c.load(view / f"sessions/{sid}/intent.json", 65536)
        e.require(type(prior["exit_code"]) is int and prior["exit_code"] != 0 and prior["timed_out"] is False
                  and intent.get("failed_session") == failed and not (view / f"sessions/{sid}/result.json").exists()
                  and not (view / f"sessions/{sid}/acceptance.json").exists(), "RECOVERY_RELATED_TERMINAL_FAILURE_REQUIRED")
    attempt = deployment / "staging/dat03-catalog-recovery" / uuid.uuid4().hex; attempt.mkdir(parents=True, mode=0o700)
    with zipfile.ZipFile(continuation / "runner.jar") as base, zipfile.ZipFile(c.ROOT / "spark/target/japan-earthquake-etl-runner.jar") as compiled:
        e.require(ENTRY not in base.namelist() and compiled.namelist().count(ENTRY) == 1, "RECOVERY_ENTRY_REQUIRED")
        with zipfile.ZipFile(attempt / "runner.jar", "x") as out:
            for item in base.infolist(): out.writestr(item, (compiled if item.filename == TRANSPORT else base).read(item.filename))
            out.writestr(compiled.getinfo(ENTRY), compiled.read(ENTRY))
    compatible(continuation / "runner.jar", attempt / "runner.jar", SQL_PROFILE)
    for source, name in ((Path(__file__), "controller.py"), (HERE / "research-catalog.py", "research-catalog.py"), (HERE / "research-e2e.py", "research-e2e.py")):
        with e.safe(source).open("rb") as src, (attempt / name).open("xb") as dst: shutil.copyfileobj(src, dst)
    request = {"version": VERSION, "catalog_root": "/opt/pipeline/staging/dat03-catalog/"+catalog.name,
               "continuation_root": "/opt/pipeline/staging/dat03-catalog-continuation/"+continuation.name,
               "catalog_request_sha256": e.sha(catalog / "request.json"), "continuation_request_sha256": e.sha(continuation / "continuation-request.json"),
               "base_runner_sha256": extra["runner_sha256"], "failed_session": failed, "recovery_session": uuid.uuid4().hex,
               "initial_completed_months": initial, "target_months": targets,
               "failed_files": {name: e.sha(view / name) for name in sorted(names)},
               "sql_read_profile": SQL_PROFILE, "related_failed_sessions": list(related)}
    with zipfile.ZipFile(attempt / "runner.jar") as jar: request["transport_sha256"] = e.hashlib.sha256(jar.read(TRANSPORT)).hexdigest()
    for name, key in CODE.items(): request[key] = e.sha(attempt / name)
    e.save(attempt / "recovery-request.json", request); validate(attempt, catalog); return attempt


def unchanged(catalog, request):
    for name, pin in request["failed_files"].items():
        e.require(e.sha(e.safe(catalog / name)) == pin, "RECOVERY_FAILED_EVIDENCE_CHANGED")


def worker(attempt, mode):
    request = validate(attempt); catalog = e.safe(request["catalog_root"]); _, base = validate(attempt, catalog)
    continuation = e.safe(request["continuation_root"])
    e.require(e.sha(continuation / "continuation-request.json") == request["continuation_request_sha256"]
              and c.continuation_inputs(catalog, continuation)["runner_sha256"] == request["base_runner_sha256"], "RECOVERY_BASE_RUNNER_CHANGED")
    compatible(continuation / "runner.jar", attempt / "runner.jar", request.get("sql_read_profile")); unchanged(catalog, request)
    owner = {"dag_id": c.DAG, "run_id": base["run_id"]}; c.quiet(owner)
    session = catalog / "sessions" / request["recovery_session"]
    if mode == "finalize":
        proof = c.load(session / "validation.json", 65536); measurement = c.load(session / "execution.json", 65536)
        e.require(proof == {"status": "VALIDATED_LEASE_HELD", "result_sha256": e.sha(session / "result.json"),
                           "execution_sha256": e.sha(session / "execution.json"), "recovery_request_sha256": e.sha(attempt / "recovery-request.json")}
                  and measurement["exit_code"] == 0 and measurement["timed_out"] is False, "RECOVERY_FINALIZATION_NOT_VALIDATED")
        result = c.session_result(catalog, base, session)
        e.require(result["completed_months"] == request["initial_completed_months"]+len(request["target_months"])
                  and result["recovered_session"] == request["failed_session"]
                  and result["session_runner_sha256"] == request["runner_sha256"]
                  and result["recovery_request_sha256"] == e.sha(attempt / "recovery-request.json"), "RECOVERY_RESULT_CHANGED")
        resolved = [request["failed_session"], *request.get("related_failed_sessions", [])]
        e.require(result.get("resolved_failed_sessions") == resolved and result.get("sql_read_profile") == SQL_PROFILE, "RECOVERY_RELATED_RESULT_CHANGED")
        sys.path.insert(0, "/opt/airflow/dags"); from source_run_guard import lease
        c.quiet(owner); unchanged(catalog, request)
        release = lease("release", owner); e.require(release == {"status": "RELEASED", **owner}, "RECOVERY_OWN_RELEASE_UNCONFIRMED")
        e.save(session / "acceptance.json", {"version": c.VERSION, "status": "CHECKPOINT_ACCEPTED", "request_sha256": e.sha(catalog / "request.json"),
               "result_sha256": e.sha(session / "result.json"), "execution_sha256": e.sha(session / "execution.json"), "lease_release": release,
               "recovery_request_sha256": e.sha(attempt / "recovery-request.json"), "recovered_session": request["failed_session"],
               "resolved_failed_sessions": resolved, "research_ready": False, "official_exported": False})
        resolutions = catalog / "session-resolutions"; resolutions.mkdir(exist_ok=True, mode=0o700)
        for sid in resolved:
            e.save(resolutions / (sid+".json"), {"version": VERSION, "recovered_session": sid,
                   "primary_failed_session": request["failed_session"], "resolved_failed_sessions": resolved,
                   "recovery_session": request["recovery_session"], "recovery_request_sha256": e.sha(attempt / "recovery-request.json"),
                   "acceptance_sha256": e.sha(session / "acceptance.json"), "failed_files": request["failed_files"]})
        print("event=dat03_catalog_recovery_accepted completed="+str(result["completed_months"]), flush=True); return
    e.require(mode in ("preflight", "run") and shutil.disk_usage(catalog).free >= 4*1024**3, "RECOVERY_MODE_OR_DISK_INVALID")
    if mode == "run":
        check = c.load(attempt / "preflight.json")
        e.require(check["status"] == "READY_FOR_PUBLICATION_RECOVERY" and len(check["checks"]) == 13, "RECOVERY_PREFLIGHT_REQUIRED")
        if request.get("version") == VERSION:
            probe = check.get("sql_read_probe", {}).get("primary_provenance:2", {})
            e.require(check.get("sql_read_profile") == SQL_PROFILE and type(probe.get("actual")) is int
                      and probe["actual"] == probe.get("expected") == 0, "RECOVERY_SQL_READ_PROBE_REQUIRED")
        session.mkdir(mode=0o700)
        e.save(session / "intent.json", {"version": VERSION, "recovery_request_sha256": e.sha(attempt / "recovery-request.json"), "failed_session": request["failed_session"]})
    output = attempt if mode == "preflight" else session
    command = ["java", "-Xmx512m", "-XX:MaxDirectMemorySize=128m", "-cp", str(attempt / "runner.jar")+":/opt/spark/jars/*:/opt/spark/jobs/iceberg/*", CLASS, str(attempt / "recovery-request.json"), mode]
    started = time.monotonic(); timed_out = False
    with (output / "stdout.private").open("xb") as out, (output / "stderr.private").open("xb") as err:
        os.chmod(out.name, 0o600); os.chmod(err.name, 0o600)
        child = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
        try: code = child.wait(timeout=6000)
        except subprocess.TimeoutExpired:
            timed_out = True; os.killpg(child.pid, signal.SIGTERM)
            try: code = child.wait(timeout=20)
            except subprocess.TimeoutExpired: os.killpg(child.pid, signal.SIGKILL); code = child.wait(timeout=10)
    import resource
    e.save(output / "execution.json", {"exit_code": code, "timed_out": timed_out, "wall_seconds": round(time.monotonic()-started, 3),
           "child_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss, "lease_retained_at_execution_end": True})
    e.require(code == 0 and not timed_out, "RECOVERY_FAILED_LEASE_RETAINED")
    c.quiet(owner); unchanged(catalog, request)
    if mode == "run":
        c.session_result(catalog, base, session)
        e.save(session / "validation.json", {"status": "VALIDATED_LEASE_HELD", "result_sha256": e.sha(session / "result.json"),
               "execution_sha256": e.sha(session / "execution.json"), "recovery_request_sha256": e.sha(attempt / "recovery-request.json")})
    print("event=dat03_catalog_recovery_worker_passed mode="+mode, flush=True)


STAGE = r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-catalog-recovery/[a-f0-9]{32}",str(p))
assert not any(x.is_symlink() for x in (p,*p.parents));p.mkdir(parents=True,mode=0o700)
listing=json.loads(sys.stdin.buffer.readline(65537));assert len(listing)==5 and {x['name'] for x in listing}=={'runner.jar','controller.py','research-catalog.py','research-e2e.py','recovery-request.json'}
for item in listing:
 assert 0<item['bytes']<=64*1024**2
 data=sys.stdin.buffer.read(item['bytes']);assert len(data)==item['bytes'] and hashlib.sha256(data).hexdigest()==item['sha256']
 with (p/item['name']).open('xb') as f:os.chmod(f.name,0o600);f.write(data)
assert not sys.stdin.buffer.read(1)
'''
BOOTSTRAP = r'''
import hashlib,json,runpy,sys
from pathlib import Path
p=Path(sys.argv[1]);f=p/'recovery-request.json';assert hashlib.file_digest(f.open('rb'),'sha256').hexdigest()==sys.argv[2]
r=json.loads(f.read_bytes())
for name,key in {'controller.py':'recovery_controller_sha256','research-catalog.py':'catalog_controller_sha256','research-e2e.py':'support_sha256','runner.jar':'runner_sha256'}.items():
 f=p/name;assert not any(x.is_symlink() for x in (f,*f.parents));assert hashlib.file_digest(f.open('rb'),'sha256').hexdigest()==r[key]
sys.path.insert(0,str(p));sys.argv=[str(p/'controller.py'),'worker','--attempt',str(p),'--worker-mode',sys.argv[3]]
runpy.run_path(str(p/'controller.py'),run_name='__main__')
'''


def run(deployment, attempt, mode):
    deployment, attempt = map(e.safe, (deployment, attempt)); request = validate(attempt)
    e.require(attempt.parent == deployment / "staging/dat03-catalog-recovery" and re.fullmatch(r"[a-f0-9]{32}", attempt.name), "RECOVERY_HOST_ATTEMPT_REQUIRED")
    e.require(shutil.disk_usage(deployment).free >= 4*1024**3, "RECOVERY_HOST_DISK_RESERVE")
    rollout = c.module("research-rollout"); before = rollout.services(); e.save(attempt / (mode+"-services-before.json"), before)
    inside = "/opt/pipeline/staging/dat03-catalog-recovery/"+attempt.name
    if mode == "preflight":
        listing = [{"name": name, "bytes": (attempt/name).stat().st_size, "sha256": e.sha(attempt/name)} for name in (*CODE, "recovery-request.json")]
        with subprocess.Popen(["docker", "exec", "-i", e.CONTAINER, "python3", "-c", STAGE, inside], stdin=subprocess.PIPE) as child:
            child.stdin.write((json.dumps(listing)+"\n").encode())
            for item in listing:
                with (attempt/item["name"]).open("rb") as src: shutil.copyfileobj(src, child.stdin)
            child.stdin.close(); e.require(child.wait(timeout=120) == 0, "RECOVERY_STAGE_FAILED")
    succeeded = False
    try:
        command = ["docker", "exec", e.CONTAINER, "python3", "-B", "-c", BOOTSTRAP, inside, e.sha(attempt / "recovery-request.json")]
        subprocess.run([*command, mode], check=True, timeout=6300)
        after = rollout.services(); e.save(attempt / (mode+"-services-after.json"), after)
        e.require(before == after, "RECOVERY_SERVICES_CHANGED_LEASE_RETAINED")
        if mode == "preflight":
            subprocess.run(["docker", "cp", e.CONTAINER+":"+inside+"/preflight.json", str(attempt/"preflight.json")], check=True, timeout=60)
        else:
            subprocess.run([*command, "finalize"], check=True, timeout=120)
        succeeded = True
    finally:
        if mode == "run":
            catalog = deployment / "staging/dat03-catalog" / Path(request["catalog_root"]).name
            target = catalog / ("runtime-observation-"+uuid.uuid4().hex)
            snapshot, measurement = c.observations().capture(catalog, target, e.CONTAINER, request["catalog_root"], c.files(), lambda: {
                "version": VERSION, "host_command_succeeded": succeeded, "recovery_request_sha256": e.sha(attempt/"recovery-request.json"),
                "research_ready": False, "host_disk_free_bytes_after": shutil.disk_usage(deployment).free})
            if succeeded:
                progress = c.module("research-catalog-progress").report(catalog, target, request["recovery_session"])
                c.observations().publish_latest(catalog, target, progress)
                e.save(attempt / "accepted-progress.json", progress)
            print(json.dumps({"event": "dat03_catalog_recovery_observation", "observation": target.name, "host_command_succeeded": succeeded, **measurement}), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("mode", choices=("prepare", "preflight", "run", "worker"))
    p.add_argument("--deployment-root", type=Path); p.add_argument("--attempt", type=Path); p.add_argument("--catalog", type=Path)
    p.add_argument("--continuation", type=Path); p.add_argument("--observation", type=Path); p.add_argument("--failed-session")
    p.add_argument("--related-failed-session", action="append", default=[])
    p.add_argument("--worker-mode", choices=("preflight", "run", "finalize")); a = p.parse_args()
    if a.mode == "prepare":
        e.require(all(x is not None for x in (a.deployment_root,a.catalog,a.continuation,a.observation,a.failed_session)), "RECOVERY_PREPARATION_ARGUMENTS_REQUIRED")
        print(prepare(a.deployment_root,a.catalog,a.continuation,a.observation,a.failed_session,a.related_failed_session))
    elif a.mode == "worker": worker(e.safe(a.attempt), a.worker_mode)
    else: run(a.deployment_root,a.attempt,a.mode)


if __name__ == "__main__": main()
