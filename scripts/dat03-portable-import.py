#!/usr/bin/env python3
"""DAT-03 part1: checkpointed local PREPARED inputs. No Gold/ML publication or service mutation."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time
import uuid

HERE = Path(__file__).resolve().parent
CONTAINER = "japan-earthquake-etl-airflow-scheduler-1"
CLASS = "ie212.earthquake.spark.gold.PortableCatalogImportJob"


def module(name):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), HERE / (name + ".py"))
    result = importlib.util.module_from_spec(spec); spec.loader.exec_module(result); return result


p = module("dat03-portable-capacity")
require, safe, sha, save = p.require, p.safe, p.sha, p.save


def load(path, limit=8 * 1024**2): return p.load(path, limit)


def accepted_sessions(root, inputs, exclude=None):
    prior = sorted((root / "host-sessions").iterdir()) if (root / "host-sessions").exists() else []
    if exclude is not None: prior = [folder for folder in prior if folder.name != exclude]
    require((prior or exclude is not None) and all((folder / "acceptance.json").is_file() for folder in prior), "IMPORT_REQUIRES_ACCEPTED_PREVIOUS_SESSIONS")
    reports = []
    for folder in prior:
        accepted = load(folder / "acceptance.json")
        require(accepted["request_sha256"] == inputs["request_sha256"] and accepted["inputs_sha256"] == sha(root / "inputs.json")
                and accepted["candidate_sha256"] == sha(folder / "candidate.json") and accepted["preparation_lease_released"] is True,
                "IMPORT_PREVIOUS_ACCEPTANCE_CHANGED")
        reports.append(load(folder / "candidate.json")["report"])
    return reports


def verify_completion_reuse(previous, current):
    for report in previous:
        if report["status"] == "PREPARED":
            require(current["status"] == "PREPARED" and current["reused"] is True
                    and current["catalog_sha256"] == report["catalog_sha256"], "IMPORT_TRUSTED_COMPLETED_CATALOG_CHANGED")


def verify_host_capture(host, candidate):
    require(sha(host / "runtime/execution.json") == candidate["execution_sha256"]
            and sha(host / "metadata/session-report.json") == candidate["report_sha256"]
            and load(host / "metadata/session-report.json") == candidate["report"], "IMPORT_EXECUTION_REPORT_CAPTURE_CHANGED")
    if candidate["report"]["status"] != "PREPARED": return
    metadata = host / "metadata"
    require(sha(metadata / "catalog.json") == candidate["report"]["catalog_sha256"], "IMPORT_CATALOG_CAPTURE_CHANGED")
    catalog = load(metadata / "catalog.json")
    for ref in [catalog[key] for key in ("request", "coverage", "provenance", "reconciliation")] + catalog["checkpoints"]:
        path = safe(metadata / ref["relative_path"])
        require(path.is_relative_to(metadata) and path.stat().st_size == ref["byte_size"] and sha(path) == ref["sha256"], "IMPORT_METADATA_CAPTURE_CHANGED")


def progress_line(inside, session):
    """Optional observation must never interrupt the authoritative worker."""
    probe = 'import re,sys;from pathlib import Path;p=Path(sys.argv[1]);lines=p.read_text(errors="replace").splitlines() if p.exists() else [];v=[x for x in lines if re.fullmatch(r"event=portable_import_(?:batch batch=\\d+/\\d+ months=\\d{4}-\\d{2}\\.\\.\\d{4}-\\d{2}|global_checks phase=global_boundary_revision_source_link)",x)];print(v[-1] if v else "event=portable_import_initial_verification")'
    try:
        value = subprocess.run(["docker", "exec", CONTAINER, "python3", "-c", probe, inside + "/execution-sessions/" + session + "/stdout.private"], capture_output=True, text=True, timeout=20)
        return value.stdout.strip() if value.returncode == 0 else None
    except subprocess.TimeoutExpired:
        return None  # No retry, kill, clear, release or worker outcome inferred from this timeout.


def worker_batches(worker):
    """Validate the immutable readback checkpoint chain, not directory count/mtime."""
    f = module("verify-dat03-followup")
    folder = safe(worker["readback_root"])
    completed_path = f.pinned_file(folder / "completed.json", worker["completed_sha256"], 65536)
    completed = load(completed_path)
    expected = module("readback-dat03-worker").months_between(worker["start_month"], worker["end_month"])
    require(completed["months_verified"] == len(expected) and completed["gold_published"] is False
            and type(completed["batch_count"]) is int and 0 < completed["batch_count"] <= 32, "ACCEPTED_WORKER_READBACK_REQUIRED")
    previous = None; accepted = []; rows = 0; batches = []
    for ordinal in range(1, completed["batch_count"] + 1):
        plan_path = safe(folder / f"batch-{ordinal:02d}-plan.json"); receipt_path = safe(folder / f"batch-{ordinal:02d}-readback.json")
        cp_path = safe(folder / f"batch-{ordinal:02d}-checkpoint.json"); cp, receipt, plan = map(load, (cp_path, receipt_path, plan_path))
        require(cp["previous_checkpoint_sha256"] == previous and cp["plan_sha256"] == sha(plan_path)
                and cp["receipt_sha256"] == sha(receipt_path) and receipt["plan_sha256"] == cp["plan_sha256"]
                and cp["config_sha256"] == completed["config_sha256"] and receipt["gold_published"] is False
                and receipt["portable_import_ready"] is False and plan["owner"] == worker["owner"], "WORKER_CHECKPOINT_CHAIN_CHANGED")
        months = plan["affected_months"]; accepted.extend(months); rows += receipt["rows_decoded"]
        require(cp["accepted_readback_months"] == accepted and cp["rows_decoded"] == rows, "WORKER_CHECKPOINT_COUNTS_CHANGED")
        batches.append({"original_plan": plan, "original_plan_sha256": sha(plan_path), "months": months,
                        "owner": worker["owner"], "run_ids": [g["run_id"] for g in receipt["groups"]],
                        "observation_count": sum(g["current_observation_count"] for g in receipt["groups"]),
                        "canonical_count": sum(g["canonical_count"] for g in receipt["groups"]),
                        "source_link_count": sum(g["dataset_counts"]["source_link"] for g in receipt["groups"])})
        previous = sha(cp_path)
    require(previous == completed["last_checkpoint_sha256"] and accepted == expected
            and len(set(accepted)) == len(accepted) and rows == completed["rows_decoded"], "WORKER_EXACT_COVERAGE_REQUIRED")
    return batches


def prepare(config, root):
    f = module("verify-dat03-followup")
    require(config["version"] == "dat03-portable-import-preparation-v1", "IMPORT_CONFIG_VERSION_REQUIRED")
    root = safe(root); require(re.fullmatch(r"[a-f0-9]{32}", root.name) and not root.exists(), "NEW_EXACT_IMPORT_ROOT_REQUIRED")
    require(shutil.disk_usage(root.parent).free >= 10 * 1024**3, "IMPORT_INITIAL_DISK_RESERVE_10GIB_REQUIRED")
    catalog, observation = safe(config["catalog"]), safe(config["observation"])
    f.pinned_file(observation / "observation.json", config["observation_sha256"], 8 * 1024**2)
    progress = module("research-catalog-progress"); accepted = progress.report(catalog, observation, config["baseline_session_id"])
    require(accepted["completed_months"] == 163 and accepted["last_published_month"] == "2013-07", "EXACT_BASELINE_163_ACCEPTANCE_REQUIRED")
    c = progress.c; view = c.observation_view(observation); source_request = c.validate(view)
    record = c.load(view / "months/2013-07/record.json")
    require(record["commit_journal_sha256"] == record["publication"]["bundle_sha256"], "BASELINE_RAW_COMMIT_PIN_REQUIRED")
    workers = config["workers"]
    require([w["owner"] for w in workers] == ["ThanhTris", "rosy179"], "ORDERED_WORKERS_REQUIRED")
    batches = [b for w in workers for b in worker_batches(w)]
    actual = [m for b in batches for m in b["months"]]
    require(actual == module("readback-dat03-worker").months_between("2013-08", "2023-12") and len(set(actual)) == 125, "EXACT_WORKER_125_MONTHS_REQUIRED")
    bronze = f.pinned_file(config["bronze_plan"], config["bronze_plan_sha256"], 4 * 1024**2)
    require(load(bronze)["version"] == "dat03-bronze-relocation-plan-v1", "ORIGINAL_TO_PHYSICAL_PLAN_REQUIRED")
    runner = safe(config["runner"]); require(runner.is_file() and runner.stat().st_size <= 64 * 1024**2, "PREBUILT_RUNNER_REQUIRED")
    # Verify content once per archive; all records retain original bundle bytes and member names.
    archives = {}
    for b in batches:
        for a in b["original_plan"]["archives"]:
            if a["sha256"] not in archives:
                path = f.pinned_file(a["path"], a["sha256"], 1024**3); archives[a["sha256"]] = path
    require(len(archives) <= 8, "IMPORT_ARCHIVE_LIMIT")
    root.mkdir(mode=0o700); (root / "inputs").mkdir(); inside = "/opt/pipeline/staging/dat03-portable-import/" + root.name
    assets = []
    def asset(path, name):
        path = safe(path); assets.append({"path": str(path), "name": name, "bytes": path.stat().st_size, "sha256": sha(path)})
    def copied(path, name):
        dest = root / name; shutil.copyfile(path, dest); os.chmod(dest, 0o600); asset(dest, name); return {"relative_path": name, "sha256": sha(dest)}
    baseline_ref = root / "inputs/baseline-acceptance.json"; save(baseline_ref, accepted); asset(baseline_ref, "inputs/baseline-acceptance.json")
    baseline = {"namespace": record["commit"]["namespace"], "gold_run_id": record["commit"]["gold_run_id"],
                "bundle_sha256": record["commit_journal_sha256"], "tables": record["commit"]["tables"], "completed_months": 163,
                "acceptance_file_sha256": sha(baseline_ref)}
    request_batches = []
    for ordinal, b in enumerate(batches, 1):
        plan = b.pop("original_plan")
        for a in plan["archives"]: a["path"] = inside + "/archives/" + a["sha256"] + ".zip"
        name = f"inputs/worker-{ordinal:02d}.json"; target = root / name; save(target, plan); asset(target, name)
        request_batches.append({**b, "plan_file": name, "plan_sha256": sha(target)})
    bronze_ref = copied(bronze, "inputs/bronze-plan.json"); copied(runner, "runner.jar"); copied(Path(__file__), "controller.py")
    copied(HERE / "dat03-portable-capacity.py", "dat03-portable-capacity.py")
    for digest, path in archives.items(): asset(path, "archives/" + digest + ".zip")
    now = datetime.now(timezone.utc)
    request = {"version": "dat03-portable-import-v1", "mode": "PREPARE_ONLY", "run_id": "dat03-import-" + root.name,
               "processing_date": now.date().isoformat(), "processed_at_utc": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
               "source_cutoff_utc": source_request["source_cutoff_utc"], "code_sha256": sha(root / "runner.jar"),
               "baseline": baseline, "baseline_acceptance": {"relative_path": "inputs/baseline-acceptance.json", "sha256": sha(baseline_ref)},
               "bronze_plan": bronze_ref, "worker_batches": request_batches}
    request["run_context"] = {"run_id": request["run_id"], "window_start_utc": "2000-01-01T00:00:00Z", "window_end_utc": "2024-01-01T00:00:00Z",
        "processing_date": request["processing_date"], "is_backfill": True, "config_version": request["version"],
        "input_manifest_uris": ["s3://japan-earthquake/silver/gold-commits/" + baseline["gold_run_id"] + "/commit.json"]
        + ["s3://japan-earthquake/silver/bundles/" + run + "/manifest.json" for b in request_batches for run in b["run_ids"]]}
    save(root / "request.json", request); asset(root / "request.json", "request.json")
    deps = {name: sha(runner.parent / "iceberg" / name) for name in p.DEPS}
    save(root / "inputs.json", {"version": "dat03-portable-import-files-v1", "inside": inside, "files": assets,
                              "request_sha256": sha(root / "request.json"), "dependencies": deps})
    print(json.dumps({"folder": str(root), "request_sha256": sha(root / "request.json"), "baseline_months": 163,
                      "worker_months": 125, "worker_batches": len(batches), "gold_writes": False}), flush=True)
    return root


def worker(root, expected, session, seconds):
    root = safe(root); require(re.fullmatch(r"/opt/pipeline/staging/dat03-portable-import/[a-f0-9]{32}", str(root))
                              and re.fullmatch(r"[a-f0-9]{32}", session) and type(seconds) is int and 600 <= seconds <= 6000, "BOUNDED_IMPORT_WORKER_REQUIRED")
    require(sha(root / "inputs.json") == expected, "IMPORT_INPUT_LIST_CHANGED"); inputs = load(root / "inputs.json")
    for item in inputs["files"]:
        path = safe(root / item["name"]); require(path.is_relative_to(root) and path.stat().st_size == item["bytes"]
                                                and sha(path) == item["sha256"], "IMPORT_INPUT_CHANGED")
    for name in p.DEPS: require(sha(Path("/opt/spark/jobs/iceberg") / name) == inputs["dependencies"][name], "IMPORT_RUNTIME_DEPENDENCY_CHANGED")
    request = load(root / "request.json"); require(sha(root / "request.json") == inputs["request_sha256"], "IMPORT_REQUEST_CHANGED")
    require(not (Path(os.environ["SOURCE_GUARD_ROOT"]) / "owner.json").exists(), "IMPORT_SOURCE_LEASE_NOT_IDLE")
    owner = {"run_id": request["run_id"], "session": session, "inputs_sha256": expected}
    save(root / "preparation-owner.json", owner)
    sessions = root / "execution-sessions"; sessions.mkdir(exist_ok=True); attempt = sessions / session; attempt.mkdir(mode=0o700)
    command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m",
               "--conf", "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m", "--jars",
               ",".join("local:/opt/spark/jobs/iceberg/" + n for n in p.DEPS), "--class", CLASS, "local:" + str(root / "runner.jar"),
               str(root / "request.json"), inputs["request_sha256"], session, str(seconds)]
    started = time.monotonic(); reason = None
    with (attempt / "stdout.private").open("xb") as out, (attempt / "stderr.private").open("xb") as err:
        os.chmod(out.name, 0o600); os.chmod(err.name, 0o600)
        child = subprocess.Popen(command, stdout=out, stderr=err, start_new_session=True)
        save(attempt / "active.json", {"pid": child.pid, "run_id": request["run_id"]})
        while child.poll() is None:
            if (root / ("stop-" + session + ".json")).exists(): reason = "HOST_DISK_RESERVE"; break
            if time.monotonic() - started > seconds + 90: reason = "SESSION_DEADLINE"; break
            time.sleep(2)
        if reason:
            os.killpg(child.pid, signal.SIGTERM)
            try: child.wait(timeout=20)
            except subprocess.TimeoutExpired: os.killpg(child.pid, signal.SIGKILL); child.wait(timeout=10)
        code = child.wait()
    execution = {"exit_code": code, "timed_out": reason == "SESSION_DEADLINE", "stop_reason": reason,
                 "wall_seconds": round(time.monotonic() - started, 3), "lake_writes": False, "automatic_retry": False}
    save(attempt / "execution.json", execution); print(json.dumps(execution), flush=True)
    require(code == 0 and reason is None, "IMPORT_FAILED_OWN_PREPARATION_LEASE_RETAINED")
    report = root / "prepared/sessions" / (session + ".json"); value = load(report)
    require(value["status"] in ("PREPARED", "CHECKPOINTED") and value["request_sha256"] == inputs["request_sha256"]
            and value["gold_published"] is False and value["official_export_ready"] is False, "IMPORT_REPORT_NOT_ACCEPTED")
    save(attempt / "candidate.json", {"session": session, "report_sha256": sha(report), "execution_sha256": sha(attempt / "execution.json"), "report": value})


TRANSFER = r'''
import hashlib,json,os,re,sys
from pathlib import Path
root=Path(sys.argv[1]);assert re.fullmatch(r"/opt/pipeline/staging/dat03-portable-import/[a-f0-9]{32}",str(root))
assert not any(p.is_symlink() for p in (root,*root.parents))
files=json.loads(sys.stdin.buffer.readline());assert 0<len(files)<=64 and len({f["name"] for f in files})==len(files)
root.parent.mkdir(exist_ok=True);root.mkdir(mode=0o700)
for f in files:
 assert re.fullmatch(r"(?:inputs/[a-z0-9-]+\.json|archives/[a-f0-9]{64}\.zip|(?:request|inputs)\.json|runner\.jar|controller\.py|dat03-portable-capacity\.py)",f["name"])
 assert type(f["bytes"]) is int and 0<f["bytes"]<=1024**3
 p=root/f["name"];p.parent.mkdir(exist_ok=True);left=f["bytes"];digest=hashlib.sha256()
 with p.open("xb") as stream:
  os.chmod(stream.name,0o600)
  while left:
   b=sys.stdin.buffer.read(min(left,1024**2));assert b;left-=len(b);digest.update(b);stream.write(b)
 assert digest.hexdigest()==f["sha256"]
assert not sys.stdin.buffer.read(1)
'''


def run(root, seconds, resume=False):
    root = safe(root); require(type(seconds) is int and 600 <= seconds <= 6000, "BOUNDED_IMPORT_BUDGET_REQUIRED")
    inputs = load(root / "inputs.json"); inside = inputs["inside"]
    require(re.fullmatch(r"[a-f0-9]{32}", root.name) and inside == "/opt/pipeline/staging/dat03-portable-import/" + root.name, "IMPORT_RUNTIME_ROOT_CHANGED")
    require(shutil.disk_usage(root).free >= 10 * 1024**3, "IMPORT_INITIAL_DISK_RESERVE_10GIB_REQUIRED")
    r = module("research-rollout"); before = r.services(); r.idle(); r.trino_queries_idle()
    previous_reports = []
    if not resume:
        files = inputs["files"] + [{"name": "inputs.json", "path": str(root / "inputs.json"), "sha256": sha(root / "inputs.json"), "bytes": (root / "inputs.json").stat().st_size}]
        with subprocess.Popen(["docker", "exec", "-i", CONTAINER, "python3", "-c", TRANSFER, inside], stdin=subprocess.PIPE) as child:
            child.stdin.write((json.dumps([{k: v for k, v in item.items() if k != "path"} for item in files]) + "\n").encode())
            for item in files:
                require(sha(item["path"]) == item["sha256"], "HOST_IMPORT_INPUT_CHANGED")
                with safe(item["path"]).open("rb") as stream: shutil.copyfileobj(stream, child.stdin, 1024**2)
            child.stdin.close(); require(child.wait(timeout=180) == 0, "IMPORT_STAGE_FAILED_NO_RETRY")
        save(root / "staged.json", {"inputs_sha256": sha(root / "inputs.json"), "inside": inside})
    else:
        require(load(root / "staged.json")["inputs_sha256"] == sha(root / "inputs.json"), "IMPORT_STAGED_INPUT_CHANGED")
        previous_reports = accepted_sessions(root, inputs)
    r.idle(); r.trino_queries_idle(); session = uuid.uuid4().hex
    host = root / "host-sessions" / session; host.mkdir(parents=True, mode=0o700)
    save(host / "intent.json", {"session": session, "request_sha256": inputs["request_sha256"], "inputs_sha256": sha(root / "inputs.json"), "services_before": before})
    bootstrap = 'import hashlib,runpy,sys;from pathlib import Path;p=Path(sys.argv[1]);assert hashlib.sha256((p/"controller.py").read_bytes()).hexdigest()==sys.argv[2];assert hashlib.sha256((p/"dat03-portable-capacity.py").read_bytes()).hexdigest()==sys.argv[6];sys.argv=[str(p/"controller.py"),"worker","--folder",str(p),"--inputs-sha256",sys.argv[3],"--session",sys.argv[4],"--seconds",sys.argv[5]];runpy.run_path(str(p/"controller.py"),run_name="__main__")'
    with (host / "worker.private").open("xb") as log:
        os.chmod(log.name, 0o600)
        child = subprocess.Popen(["docker", "exec", CONTAINER, "python3", "-c", bootstrap, inside, sha(root / "controller.py"), sha(root / "inputs.json"), session, str(seconds), sha(root / "dat03-portable-capacity.py")], stdout=log, stderr=subprocess.STDOUT)
        disk_stop = False; previous_progress = None; started = time.monotonic()
        while child.poll() is None:
            require(time.monotonic() - started < seconds + 300, "IMPORT_HOST_DEADLINE_UNKNOWN_OUTCOME_KEEP_LEASE")
            if shutil.disk_usage(root).free < 4 * 1024**3 and not disk_stop:
                stop = 'from pathlib import Path;import sys,json;p=Path(sys.argv[1]);assert p.parent.name==sys.argv[2];p.write_text(json.dumps({"session":sys.argv[3]}))'
                subprocess.run(["docker", "exec", CONTAINER, "python3", "-c", stop, inside + "/stop-" + session + ".json", root.name, session], check=True, timeout=20); disk_stop = True
            # Read filtered progress only; private SQL/SDK logs are never sent to the terminal.
            progress = progress_line(inside, session)
            if progress and progress != previous_progress:
                previous_progress = progress; print(progress, flush=True)
            time.sleep(15)
        code = child.wait()
    require(code == 0 and not disk_stop, "IMPORT_FAILED_CHECKPOINT_AND_OWN_LEASE_RETAINED")
    capture(root, session, before, previous_reports)


def capture(root, session, before, previous_reports=None):
    """Accept an existing successful worker only; never launches/resumes ETL."""
    root = safe(root); inputs = load(root / "inputs.json"); inside = inputs["inside"]
    require(re.fullmatch(r"[a-f0-9]{32}", root.name) and inside == "/opt/pipeline/staging/dat03-portable-import/" + root.name
            and re.fullmatch(r"[a-f0-9]{32}", session), "EXACT_EXISTING_IMPORT_SESSION_REQUIRED")
    require(load(root / "staged.json")["inputs_sha256"] == sha(root / "inputs.json"), "IMPORT_STAGED_INPUT_CHANGED")
    host = root / "host-sessions" / session
    require(host.is_dir() and (host / "worker.private").is_file() and not (host / "acceptance.json").exists(), "EXISTING_UNACCEPTED_HOST_SESSION_REQUIRED")
    require(not (host / "runtime").exists(), "PARTIAL_CAPTURE_REQUIRES_REVIEW_NO_OVERWRITE")
    r = module("research-rollout"); r.idle(); r.trino_queries_idle(); require(r.services() == before, "IMPORT_SERVICES_CHANGED")
    subprocess.run(["docker", "cp", CONTAINER + ":" + inside + "/execution-sessions/" + session, str(host / "runtime")], check=True, timeout=120)
    execution = load(host / "runtime/execution.json")
    require(execution["exit_code"] == 0 and execution["timed_out"] is False and execution["stop_reason"] is None, "IMPORT_FAILED_CHECKPOINT_AND_OWN_LEASE_RETAINED")
    candidate = load(host / "runtime/candidate.json"); save(host / "candidate.json", candidate)
    require(candidate["session"] == session and candidate["report"]["request_sha256"] == inputs["request_sha256"]
            and candidate["report"]["gold_published"] is False and candidate["report"]["official_export_ready"] is False, "IMPORT_CANDIDATE_IDENTITY_CHANGED")
    verify_completion_reuse(previous_reports or [], candidate["report"])
    metadata = host / "metadata"; metadata.mkdir(mode=0o700)
    names = ["request.json", "coverage.json", "provenance.json"] + ["checkpoints/%02d.json" % i for i in range(1, candidate["report"].get("completed_batches", 0) + 1)]
    if candidate["report"]["status"] == "PREPARED": names += ["catalog.json", "reconciliation.json"]
    for name in names:
        target = metadata / name; target.parent.mkdir(exist_ok=True)
        subprocess.run(["docker", "cp", CONTAINER + ":" + inside + "/prepared/" + name, str(target)], check=True, timeout=30)
        require(target.stat().st_size <= 8 * 1024**2, "IMPORT_METADATA_CAPTURE_LIMIT")
    subprocess.run(["docker", "cp", CONTAINER + ":" + inside + "/prepared/sessions/" + session + ".json", str(metadata / "session-report.json")], check=True, timeout=30)
    if candidate["report"]["status"] == "PREPARED":
        for ref in load(metadata / "catalog.json")["checkpoints"]:
            name = ref["relative_path"]
            require(re.fullmatch(r"checkpoints/[0-9]{2}\.json", name), "BOUNDED_CHECKPOINT_CAPTURE_REQUIRED")
            target = metadata / name; target.parent.mkdir(exist_ok=True)
            if not target.exists(): subprocess.run(["docker", "cp", CONTAINER + ":" + inside + "/prepared/" + name, str(target)], check=True, timeout=30)
    verify_host_capture(host, candidate)
    after = r.services(); r.idle(); r.trino_queries_idle(); require(before == after, "IMPORT_SERVICES_CHANGED")
    finalize = 'import json,sys;from pathlib import Path;p=Path(sys.argv[1]);owner=p/"preparation-owner.json";v=json.loads(owner.read_bytes());assert v=={"run_id":sys.argv[2],"session":sys.argv[3],"inputs_sha256":sys.argv[4]};owner.unlink();print("OWN_PREPARATION_LEASE_RELEASED")'
    done = subprocess.run(["docker", "exec", CONTAINER, "python3", "-c", finalize, inside, load(root / "request.json")["run_id"], session, sha(root / "inputs.json")], capture_output=True, text=True, check=True, timeout=20)
    require(done.stdout.strip() == "OWN_PREPARATION_LEASE_RELEASED", "IMPORT_PREPARATION_RELEASE_NOT_CONFIRMED")
    save(host / "acceptance.json", {"version": "dat03-portable-import-session-v1", "status": candidate["report"]["status"],
         "request_sha256": inputs["request_sha256"], "inputs_sha256": sha(root / "inputs.json"), "candidate_sha256": sha(host / "candidate.json"),
         "host_controller_sha256": sha(Path(__file__)),
         "preparation_lease_released": True, "services_unchanged": True, "gold_published": False, "official_export_ready": False})
    print(json.dumps({"event": "portable_import_session_accepted", "folder": str(root), "session": session, **candidate["report"]}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("mode", choices=("prepare", "run", "resume", "worker", "capture"))
    parser.add_argument("--config", type=Path); parser.add_argument("--config-sha256"); parser.add_argument("--folder", type=Path, required=True)
    parser.add_argument("--seconds", type=int, default=6000); parser.add_argument("--inputs-sha256"); parser.add_argument("--session")
    parser.add_argument("--preflight", type=Path); parser.add_argument("--preflight-sha256")
    args = parser.parse_args()
    if args.mode == "prepare":
        f = module("verify-dat03-followup"); prepare(load(f.pinned_file(args.config, args.config_sha256, 65536)), args.folder)
    elif args.mode == "worker": worker(args.folder, args.inputs_sha256, args.session, args.seconds)
    elif args.mode == "capture":
        require(re.fullmatch(r"[a-f0-9]{32}", args.session or ""), "EXACT_EXISTING_IMPORT_SESSION_REQUIRED")
        root = safe(args.folder); intent = root / "host-sessions" / str(args.session) / "intent.json"
        if intent.is_file():
            value = load(intent); require(value["session"] == args.session and value["inputs_sha256"] == sha(root / "inputs.json")
                                         and value["request_sha256"] == load(root / "inputs.json")["request_sha256"], "IMPORT_HOST_INTENT_CHANGED")
        else:
            # Legacy attempts require an explicit pre-existing externally pinned services receipt.
            f = module("verify-dat03-followup"); value = load(f.pinned_file(args.preflight, args.preflight_sha256, 65536))
            require(value["unchanged"] is True and value["services_before"] == value["services_after"], "TRUSTED_PREEXISTING_SERVICES_RECEIPT_REQUIRED")
        capture(root, args.session, value["services_before"], accepted_sessions(root, load(root / "inputs.json"), args.session))
    else: run(args.folder, args.seconds, args.mode == "resume")


if __name__ == "__main__": main()
