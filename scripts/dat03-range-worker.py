#!/usr/bin/env python3
"""Isolated DAT-03 month ranges. No source fetch, service lifecycle or catalog takeover."""
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

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("range_catalog", HERE / "research-catalog.py")
c = importlib.util.module_from_spec(spec)
spec.loader.exec_module(c)
e = c.e
VERSION = "dat03-range-worker-v1"
DAG = "dat03_range_worker"
CHECK = "ie212.earthquake.spark.usgs.ResearchRangeJob"
ASSIGNMENTS = {"HoaiTam": ("2008-06", "2013-08"), "ThanhTris": ("2013-08", "2018-10"),
               "rosy179": ("2018-10", "2024-01")}
CONFIGS = {"baseline.csv": "baseline_inventory_sha256", "boundary.csv": "boundary_inventory_sha256",
           "plan.json": "plan_sha256", "usgs-summary.json": "usgs_summary_sha256"}
CODE = {"runner.jar": "runner_sha256", "controller.py": "controller_sha256",
        "research-e2e.py": "support_sha256", "research-catalog.py": "catalog_controller_sha256"}


def payload_names():
    return set(CONFIGS) | {p for name in c.NAMES for p in c.source_files(name)}


def copy_file(source, target):
    source, target = e.safe(source), e.safe(target)
    e.require(source.is_file() and 0 < source.stat().st_size <= 64 * 1024**2, "RANGE_COPY_BOUND")
    target.parent.mkdir(parents=True, exist_ok=True)
    with source.open("rb") as src, target.open("xb") as dst:
        os.chmod(target, 0o600)
        shutil.copyfileobj(src, dst, 1024**2)
    e.require(e.sha(source) == e.sha(target), "RANGE_COPY_CHANGED")


def pack_inputs(catalog, output):
    """Only explicit public source bytes + provenance/config, never capacity logs or credentials."""
    catalog, output = e.safe(catalog), e.safe(output)
    request = c.validate(catalog)
    e.require(not output.exists(), "RANGE_NEW_RELEASE_DIRECTORY_REQUIRED")
    output.mkdir(parents=True, mode=0o700)
    for name in sorted(payload_names()):
        copy_file(catalog / name, output / name)
    manifest = {"version": "dat03-range-inputs-v1", "source_cutoff_utc": request["source_cutoff_utc"],
                "catalog_request_sha256": e.sha(catalog / "request.json"), "sources": request["sources"],
                "files": {n: {"sha256": e.sha(output / n), "bytes": (output / n).stat().st_size}
                          for n in sorted(payload_names())}, "research_ready": False}
    e.save(output / "manifest.json", manifest)
    pin = e.sha(output / "manifest.json")
    verify_inputs(output, pin)
    return {"status": "FROZEN_INPUTS_PACKED", "manifest_sha256": pin, "files": len(manifest["files"]),
            "bytes": sum(p["bytes"] for p in manifest["files"].values()), "output": str(output)}


def verify_inputs(root, pin):
    root = e.safe(root)
    e.require(isinstance(pin, str) and re.fullmatch(r"[a-f0-9]{64}", pin)
              and e.sha(root / "manifest.json") == pin, "RANGE_TRUSTED_INPUT_PIN_REQUIRED")
    m = c.load(root / "manifest.json")
    e.require(m["version"] == "dat03-range-inputs-v1" and m["research_ready"] is False
              and set(m["files"]) == payload_names(), "RANGE_EXACT_PAYLOAD_REQUIRED")
    for name, p in m["files"].items():
        f = e.safe(root / name)
        e.require(type(p["bytes"]) is int and 0 < p["bytes"] <= 32 * 1024**2
                  and f.is_file() and f.stat().st_size == p["bytes"] and e.sha(f) == p["sha256"], "RANGE_INPUT_CHANGED")
    e.require([p["name"] for p in m["sources"]] == list(c.NAMES), "RANGE_EXACT_37_SOURCES_REQUIRED")
    from datetime import datetime
    cutoff = datetime.fromisoformat(m["source_cutoff_utc"].replace("Z", "+00:00"))
    plan, summary = c.load(root / "plan.json"), c.load(root / "usgs-summary.json")
    e.require([b["month"] for b in plan["batches"]] == c.module_months()
              and [p["month"] for p in summary["captures"]] == [f"2023-{i:02d}" for i in range(1, 13)], "RANGE_PLAN_OR_USGS_SET_CHANGED")
    for p in m["sources"]:
        meta_name, raw_name = c.source_files(p["name"])
        meta = c.load(root / meta_name, 65536)
        e.require(m["files"][meta_name]["sha256"] == p["metadata_sha256"]
                  and m["files"][raw_name]["sha256"] == p["raw_sha256"] == meta["sha256"]
                  and m["files"][raw_name]["bytes"] == p["bytes"] == meta["bytes"]
                  and datetime.fromisoformat(meta["retrieved_at_utc"].replace("Z", "+00:00")) <= cutoff, "RANGE_SOURCE_PROVENANCE_CHANGED")
        if p["name"].startswith("usgs-"):
            row = next(x for x in summary["captures"] if x["month"] == p["name"][5:])
            e.require(row["raw_sha256"] == p["raw_sha256"] and row["capture_sha256"] == p["metadata_sha256"]
                      and type(row["check"]["feature_count"]) is int and 0 <= row["check"]["feature_count"] < 20000,
                      "RANGE_USGS_SUMMARY_CHANGED")
    return m


def months(request):
    e.require(request["owner"] in ASSIGNMENTS and
              (request["start_month"], request["end_month_exclusive"]) == ASSIGNMENTS[request["owner"]], "RANGE_ASSIGNMENT_CHANGED")
    return [m for m in c.module_months() if request["start_month"] <= m < request["end_month_exclusive"]]


def validate(root):
    root = e.safe(root)
    r = c.load(root / "request.json", 65536)
    e.require(set(r) == {"version", "run_id", "namespace", "owner", "start_month", "end_month_exclusive",
                        "input_manifest_sha256", "source_cutoff_utc", "sources", *CONFIGS.values(), *CODE.values()}
              and r["version"] == VERSION and re.fullmatch(r"dat03-range-[a-f0-9]{32}", r["run_id"])
              and r["namespace"] == "iceberg.dat03_range_" + r["run_id"][12:], "RANGE_REQUEST_INVALID")
    months(r)
    m = verify_inputs(root, r["input_manifest_sha256"])
    e.require(r["sources"] == m["sources"] and r["source_cutoff_utc"] == m["source_cutoff_utc"], "RANGE_FROZEN_PINS_CHANGED")
    for name, field in {**CONFIGS, **CODE}.items():
        e.require(e.sha(e.safe(root / name)) == r[field], "RANGE_CODE_OR_CONFIG_CHANGED")
    return r


def prepare(deployment, inputs, pin, owner, runner):
    e.require(owner in ASSIGNMENTS, "RANGE_OWNER_INVALID")
    deployment, inputs, runner = map(e.safe, (deployment, inputs, runner))
    m = verify_inputs(inputs, pin)
    import zipfile
    with zipfile.ZipFile(runner) as jar:
        e.require(CHECK.replace(".", "/") + ".class" in jar.namelist(), "RANGE_BUILD_RUNNER_FIRST")
    e.require(shutil.disk_usage(deployment).free >= 4*1024**3, "RANGE_HOST_DISK_RESERVE")
    key = uuid.uuid4().hex
    folder = deployment / "staging/dat03-range" / key
    folder.mkdir(parents=True, mode=0o700)
    for n in payload_names() | {"manifest.json"}:
        copy_file(inputs / n, folder / n)
    for src, name in ((runner, "runner.jar"), (Path(__file__), "controller.py"),
                      (HERE / "research-catalog.py", "research-catalog.py"), (HERE / "research-e2e.py", "research-e2e.py")):
        copy_file(src, folder / name)
    r = {"version": VERSION, "run_id": "dat03-range-" + key, "namespace": "iceberg.dat03_range_" + key,
         "owner": owner, "start_month": ASSIGNMENTS[owner][0], "end_month_exclusive": ASSIGNMENTS[owner][1],
         "input_manifest_sha256": pin, "source_cutoff_utc": m["source_cutoff_utc"], "sources": m["sources"],
         **{field: e.sha(folder / name) for name, field in {**CONFIGS, **CODE}.items()}}
    e.save(folder / "request.json", r)
    validate(folder)
    java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if os.environ.get("JAVA_HOME") else "java"
    checked = subprocess.run([java, "-Xmx256m", "-cp", str(folder / "runner.jar"),
                              "ie212.earthquake.spark.usgs.ResearchRangeInputCheck", str(folder)],
                             check=True, capture_output=True, text=True, timeout=120)
    e.require(checked.stdout.strip() == "RANGE_ORIGINAL_37_SOURCES_VERIFIED; no Bronze/Silver/Gold writes; research_ready=false",
              "RANGE_ORIGINAL_SOURCE_CHECK_FAILED")
    return folder


def result(folder, request, session):
    r = c.load(session / "result.json")
    count = r["completed_months"]
    planned = months(request)
    e.require(type(count) is int and 0 <= count <= len(planned) and r["required_months"] == len(planned)
              and r["pending_month"] is None and r["request_identity_sha256"] == e.digest(request)
              and r["status"] == ("RANGE_PUBLISHED" if count == len(planned) else "CHECKPOINTED")
              and r["full_research_catalog"] is False and r["official_exported"] is False and r["research_ready"] is False
              and r["session_runner_sha256"] == request["runner_sha256"]
              and [p["month"] for p in r["months"]] == planned[:count], "RANGE_RESULT_INVALID")
    previous = None
    for p in r["months"]:
        path = folder / "months" / p["month"]
        e.require(c.load(path / "checkpoint.json") == p and p["previous_checkpoint_sha256"] == previous
                  and p["request_identity_sha256"] == e.digest(request) and p["status"] == "PUBLISHED"
                  and e.sha(path / "record.json") == p["record_sha256"], "RANGE_CHECKPOINT_CHANGED")
        record = c.load(path / "record.json")
        ctx = record["context"]["resolver_request"]
        pub = record["publication"]
        e.require(ctx["namespace"] == request["namespace"] and ctx["source_cutoff_utc"] == request["source_cutoff_utc"]
                  and p["month"] in ctx["target_months"] and set(ctx["target_months"]) <= set(planned[:count])
                  and pub["published"] is True and pub["verify_status"] == "PASSED"
                  and pub["snapshot_bundle"] == record["commit"]["tables"]
                  and p["snapshots"] == {t: v["snapshot_id"] for t, v in pub["snapshot_bundle"].items()}
                  and all(v["actual"] == v["expected"] for v in pub["checks"].values()), "RANGE_PUBLICATION_CHANGED")
        previous = e.sha(path / "checkpoint.json")
    pins = r["months"][-1]["snapshots"] if count else {t: 0 for t in e.TABLES}
    e.require(set(pins) == e.TABLES and pins == r["current_snapshots"], "RANGE_SNAPSHOTS_CHANGED")
    return r


def worker(folder, seconds, max_months):
    """Failed sessions retain their lease and cannot be automatically resumed."""
    folder = e.safe(folder)
    e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-range/[a-f0-9]{32}", str(folder)), "RANGE_RUNTIME_ROOT_REQUIRED")
    e.require(3600 <= seconds <= 6600 and 1 <= max_months <= 12, "RANGE_BUDGET_INVALID")
    # The lifecycle engine is extracted below, not the catalog worker with its
    # full-catalog path/size assumptions.
    execute(folder, seconds, max_months)


def acceptance(folder, request, session):
    folder, session = e.safe(folder), e.safe(session)
    e.require(session.parent == folder / "sessions" and re.fullmatch(r"[a-f0-9]{32}", session.name), "RANGE_SESSION_INVALID")
    a = c.load(session / "acceptance.json", 65536)
    x = c.load(session / "execution.json", 65536)
    e.require(a["status"] == "CHECKPOINT_ACCEPTED" and a["request_sha256"] == e.sha(folder / "request.json")
              and a["result_sha256"] == e.sha(session / "result.json") and a["execution_sha256"] == e.sha(session / "execution.json")
              and type(x["exit_code"]) is int and x["exit_code"] == 0 and x["timed_out"] is False
              and a["lease_release"] == {"status": "RELEASED", "dag_id": DAG, "run_id": request["run_id"]}, "RANGE_ACCEPTANCE_INVALID")
    return result(folder, request, session)


def execute(folder, seconds, max_months):
    import resource
    r = validate(folder)
    c.quiet()
    sessions = folder / "sessions"
    sessions.mkdir(exist_ok=True, mode=0o700)
    accepted_months = 0
    for prior in sessions.iterdir():
        accepted_months = max(accepted_months, acceptance(folder, r, prior)["completed_months"])
    e.require(accepted_months > 0 or max_months == 1, "RANGE_FIRST_SESSION_ONE_MONTH_REQUIRED")
    deps = Path("/opt/spark/jobs/iceberg")
    jars = [deps / n for n in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar", "iceberg-aws-bundle-1.10.1.jar")]
    e.require(all(p.is_file() for p in jars) and shutil.disk_usage(folder).free >= 4*1024**3, "RANGE_DEPS_OR_DISK_RESERVE")
    session = sessions / uuid.uuid4().hex
    session.mkdir(mode=0o700)
    e.save(session / "intent.json", {"version": VERSION, "request_sha256": e.sha(folder / "request.json"),
                                    "seconds": seconds, "max_months": max_months,
                                    "iceberg_dependencies": {j.name: e.sha(j) for j in jars}})
    sys.path.insert(0, "/opt/airflow/dags")
    from source_run_guard import lease
    owner = {"dag_id": DAG, "run_id": r["run_id"]}
    lease("acquire", owner)
    command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m",
               "--jars", ",".join(map(str, jars)), "--conf", "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m",
               "--class", CHECK, str(folder / "runner.jar"), str(folder / "request.json"), str(session), str(seconds), str(max_months)]
    started = time.monotonic()
    timed_out = False
    print("event=dat03_range_started session=" + session.name + " owner=" + r["owner"], flush=True)
    with (session / "stdout.private").open("xb") as out, (session / "stderr.private").open("xb") as err:
        os.chmod(session / "stdout.private", 0o600); os.chmod(session / "stderr.private", 0o600)
        child = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
        try: code = child.wait(timeout=seconds + 120)
        except subprocess.TimeoutExpired:
            timed_out = True
            os.killpg(child.pid, signal.SIGTERM)
            try: code = child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL); code = child.wait(timeout=10)
    e.save(session / "execution.json", {"exit_code": code, "timed_out": timed_out,
                                      "wall_seconds": round(time.monotonic() - started, 3),
                                      "child_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss})
    e.require(code == 0 and not timed_out, "RANGE_FAILED_LEASE_RETAINED")
    measured = result(folder, r, session)
    c.quiet(owner)
    validate(folder)
    released = lease("release", owner)
    e.require(released == {"status": "RELEASED", **owner}, "RANGE_OWN_RELEASE_UNCONFIRMED")
    e.save(session / "acceptance.json", {"version": VERSION, "status": "CHECKPOINT_ACCEPTED",
                                        "request_sha256": e.sha(folder / "request.json"),
                                        "result_sha256": e.sha(session / "result.json"),
                                        "execution_sha256": e.sha(session / "execution.json"),
                                        "lease_release": released, "research_ready": False})
    print(json.dumps({"event": "dat03_range_accepted", "session": session.name,
                      "completed_months": measured["completed_months"], "required_months": measured["required_months"]}), flush=True)


def frozen_files():
    return payload_names() | set(CODE) | {"manifest.json", "request.json"}


# The host validates the exact file set before streaming; the receiver enforces
# flat inputs/ filenames, exclusive creation and byte hashes, never tar extraction.
TRANSFER = r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-range/[a-f0-9]{32}",str(p))
assert not any(q.is_symlink() for q in (p,*p.parents))
header=sys.stdin.buffer.readline(65537);assert len(header)<=65536
files=json.loads(header);assert len(files)==84 and len({f['name'] for f in files})==84
expected={'baseline.csv','boundary.csv','plan.json','usgs-summary.json','runner.jar','controller.py','research-catalog.py','research-e2e.py','manifest.json','request.json'}
for n in ['h'+str(y) for y in range(2000,2025)]+['usgs-2023-%02d'%m for m in range(1,13)]:
    expected.update(('inputs/'+n+'.json','inputs/'+n+('.zip' if n.startswith('h') else '.geojson')))
assert {f['name'] for f in files}==expected
assert all(re.fullmatch(r'(?:inputs/)?[a-zA-Z0-9_.-]+',f['name']) and '..' not in f['name']
           and type(f['bytes']) is int and 0<f['bytes']<=64*1024**2
           and re.fullmatch(r'[a-f0-9]{64}',f['sha256']) for f in files)
assert sum(f['bytes'] for f in files)<=256*1024**2
p.parent.mkdir(parents=True,exist_ok=True);p.mkdir(mode=0o700);(p/'inputs').mkdir(mode=0o700)
for f in files:
    h=hashlib.sha256();remaining=f['bytes']
    with (p/f['name']).open('xb') as out:
        os.chmod(p/f['name'],0o600)
        while remaining:
            b=sys.stdin.buffer.read(min(remaining,1024**2));assert b
            out.write(b);h.update(b);remaining-=len(b)
    assert h.hexdigest()==f['sha256']
assert not sys.stdin.buffer.read(1)
print('RANGE_EXACT_FILES_STAGED')
'''

BOOTSTRAP = r'''
import hashlib,json,runpy,sys
from pathlib import Path
p=Path(sys.argv[1]);pins=json.loads(sys.argv[2])
assert not any(q.is_symlink() for q in (p,*p.parents))
for n,h in pins.items():
    f=p/n;assert f.is_file() and not f.is_symlink()
    assert hashlib.file_digest(f.open('rb'),'sha256').hexdigest()==h
sys.path.insert(0,str(p));sys.argv=[str(p/'controller.py'),*sys.argv[3:]]
runpy.run_path(str(p/'controller.py'),run_name='__main__')
'''


def run(deployment, folder, seconds, max_months, resume):
    deployment, folder = e.safe(deployment), e.safe(folder)
    e.require(folder.parent == deployment / "staging/dat03-range" and re.fullmatch(r"[a-f0-9]{32}", folder.name), "RANGE_EXACT_HOST_ROOT_REQUIRED")
    e.require(type(seconds) is int and 3600 <= seconds <= 6600 and type(max_months) is int and 1 <= max_months <= 12, "RANGE_BUDGET_INVALID")
    e.require(shutil.disk_usage(deployment).free >= 4*1024**3, "RANGE_HOST_DISK_RESERVE")
    validate(folder)
    rollout = c.module("research-rollout")
    before = rollout.services(); rollout.idle(); rollout.trino_queries_idle()
    inside = "/opt/pipeline/staging/dat03-range/" + folder.name
    if not resume:
        listing = [{"name": n, "bytes": (folder/n).stat().st_size, "sha256": e.sha(folder/n)} for n in sorted(frozen_files())]
        with subprocess.Popen(["docker", "exec", "-i", e.CONTAINER, "python3", "-c", TRANSFER, inside], stdin=subprocess.PIPE) as proc:
            proc.stdin.write((json.dumps(listing) + "\n").encode())
            for item in listing:
                with (folder/item["name"]).open("rb") as src: shutil.copyfileobj(src, proc.stdin, 1024**2)
            proc.stdin.close()
            e.require(proc.wait(timeout=120) == 0, "RANGE_STAGE_FAILED_DO_NOT_OVERWRITE")
    pins = {n: e.sha(folder/n) for n in ("request.json", "controller.py", "research-catalog.py", "research-e2e.py")}
    # No automatic retry: unknown outcome must first be inspected in retained
    # runtime sessions. A timeout never releases the lease from the host.
    subprocess.run(["docker", "exec", e.CONTAINER, "python3", "-B", "-X", "pycache_prefix=/dev/null", "-c", BOOTSTRAP,
                    inside, json.dumps(pins), "worker", "--folder", inside, "--seconds", str(seconds),
                    "--max-months", str(max_months)], check=True, timeout=seconds+300)
    e.require(rollout.services() == before, "RANGE_SERVICE_CHANGED_REVIEW_REQUIRED")
    rollout.idle(); rollout.trino_queries_idle()
    print("RANGE_HOST_POSTFLIGHT_PASSED; keep Docker volume and all source/checkpoint artifacts")


def report(folder, session):
    folder = e.safe(folder)
    e.require(re.fullmatch(r"[a-f0-9]{32}", session), "RANGE_EXACT_SESSION_REQUIRED")
    request = validate(folder)
    if str(folder).startswith("/opt/pipeline/staging/dat03-range/"):
        r = acceptance(folder, request, folder / "sessions" / session)
        return {"version": VERSION, "session": session, "request_sha256": e.sha(folder / "request.json"),
                "acceptance_sha256": e.sha(folder / "sessions" / session / "acceptance.json"),
                "result_sha256": e.sha(folder / "sessions" / session / "result.json"),
                **{k: r[k] for k in ("completed_months", "required_months", "status", "owner", "current_snapshots")},
                "research_ready": False, "official_exported": False, "portable_gold_exported": False}
    e.require(folder.parent.name == "dat03-range" and re.fullmatch(r"[a-f0-9]{32}", folder.name), "RANGE_HOST_FOLDER_REQUIRED")
    pins = {n: e.sha(folder/n) for n in ("request.json", "controller.py", "research-catalog.py", "research-e2e.py")}
    inside = "/opt/pipeline/staging/dat03-range/" + folder.name
    p = subprocess.run(["docker", "exec", e.CONTAINER, "python3", "-B", "-X", "pycache_prefix=/dev/null", "-c", BOOTSTRAP,
                        inside, json.dumps(pins), "report", "--folder", inside, "--session", session],
                       capture_output=True, text=True, check=True, timeout=120)
    e.require(len(p.stdout) <= 65536, "RANGE_REPORT_BOUND")
    proof = json.loads(p.stdout)
    e.require(proof["request_sha256"] == e.sha(folder / "request.json") and proof["session"] == session
              and proof["owner"] == request["owner"], "RANGE_REPORT_IDENTITY_CHANGED")
    dest = folder / ("receipt-" + session + ".json")
    if dest.exists(): e.require(c.load(dest) == proof, "RANGE_RECEIPT_CHANGED")
    else: e.save(dest, proof)
    return proof


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=("pack-inputs", "verify-inputs", "prepare", "worker", "run", "resume", "report"))
    p.add_argument("--catalog", type=Path); p.add_argument("--output", type=Path)
    p.add_argument("--inputs", type=Path); p.add_argument("--input-sha256")
    p.add_argument("--owner", choices=ASSIGNMENTS); p.add_argument("--deployment-root", type=Path)
    p.add_argument("--folder", type=Path); p.add_argument("--session")
    p.add_argument("--runner", type=Path, default=HERE.parent / "spark/target/japan-earthquake-etl-runner.jar")
    p.add_argument("--seconds", type=int, default=6000); p.add_argument("--max-months", type=int, default=1)
    a = p.parse_args()
    if a.mode == "pack-inputs": print(json.dumps(pack_inputs(a.catalog, a.output)))
    elif a.mode == "verify-inputs":
        verify_inputs(a.inputs, a.input_sha256); print("FROZEN_37_INPUTS_VERIFIED_NOT_RUNTIME_ACCEPTANCE")
    elif a.mode == "prepare": print(prepare(a.deployment_root, a.inputs, a.input_sha256, a.owner, a.runner))
    elif a.mode == "worker": worker(a.folder, a.seconds, a.max_months)
    elif a.mode in ("run", "resume"): run(a.deployment_root, a.folder, a.seconds, a.max_months, a.mode == "resume")
    else:
        e.require(a.session and re.fullmatch(r"[a-f0-9]{32}", a.session), "RANGE_EXACT_SESSION_REQUIRED")
        print(json.dumps(report(a.folder, a.session)))


if __name__ == "__main__": main()
