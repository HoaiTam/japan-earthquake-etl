#!/usr/bin/env python3
"""DAT-03 explicit block-2 continuation; no fresh source ingestion or deployment changes."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("dat03_recovery", Path(__file__).with_name("research-recovery.py"))
recovery = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(recovery)
e2e = recovery.e2e
VERSION = "dat03-bounded-continuation-v1"
TRANSPORT = "ie212/earthquake/spark/gold/TrinoSqlClient.class"
NAMES = ("recovery.jar", "continuation.py", "research-e2e.py", "research-recovery.py", "preflight-request.json", "continuation-request.json", "compatibility.json")


def compatibility(original, current):
    changed = {}; matched = 0
    with zipfile.ZipFile(original) as a, zipfile.ZipFile(current) as b:
        e2e.require(len(a.namelist()) == len(set(a.namelist())) and len(b.namelist()) == len(set(b.namelist())), "DAT03_DUPLICATE_JAR_ENTRY")
        for entry in a.infolist():
            if entry.is_dir() or entry.filename == "META-INF/MANIFEST.MF":
                continue
            e2e.require(entry.filename in b.namelist(), "DAT03_RECOVERY_CODE_ENTRY_MISSING")
            before, after = a.read(entry), b.read(entry.filename)
            if before == after:
                matched += 1; continue
            e2e.require(entry.filename == TRANSPORT, "DAT03_RECOVERY_ETL_CODE_CHANGED")
            changed[entry.filename] = {"original_sha256": e2e.hashlib.sha256(before).hexdigest(), "recovery_sha256": e2e.hashlib.sha256(after).hexdigest()}
    e2e.require(set(changed) == {TRANSPORT}, "DAT03_RECOVERY_EXACT_TRANSPORT_PATCH_REQUIRED")
    return {"version": "dat03-code-compatibility-v1", "matched_original_entries": matched, "allowed_differences": changed}


def original_files(request):
    files = ["runner.jar", "controller.py", "request.json", "execution.json", "baseline.csv", "boundary.csv"]
    files += [f"inputs/h{y}{suffix}" for y in e2e.YEARS for suffix in (".zip", "-capture.json")]
    files += ["inputs/usgs-capture.json", "inputs/usgs-december2023.geojson"]
    for month in (*recovery.COMPLETED, recovery.INCOMPLETE):
        op = "orc07-" + e2e.hashlib.sha256(("dat03_bounded_e2e|" + request["run_id"] + "-" + month).encode()).hexdigest()[:40]
        files += [op + "/context.json"] + [op + "/" + p + ".json" for p in (e2e.PHASES[:4] if month == recovery.INCOMPLETE else e2e.PHASES)]
        if month in recovery.COMPLETED:
            files += ["progress-" + month + ".json"]
    return files


def copy_original(original, folder, request):
    for name in original_files(request):
        source = e2e.safe(original / name); target = e2e.safe(folder / name)
        e2e.require(source.is_file() and 0 < source.stat().st_size <= 64 * 1024**2, "DAT03_CONTINUATION_BOUNDED_COPY_REQUIRED")
        target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with source.open("rb") as reader, target.open("xb") as writer:
            os.chmod(target, 0o600); shutil.copyfileobj(reader, writer, 1024**2)
        e2e.require(e2e.sha(source) == e2e.sha(target), "DAT03_CONTINUATION_COPY_CHANGED")


def checkpoint_files(request):
    files = ["continuation-request.json", "compatibility.json", "recovery-execution.json", "live-preflight.json", "source-pins.json", "progress-2016-04.json"]
    for month in ("2016-04", "2023-12"):
        op = "orc07-" + e2e.hashlib.sha256(("dat03_bounded_e2e|" + request["run_id"] + "-" + month).encode()).hexdigest()[:40]
        files += [op + "/context.json"] + [op + "/" + p + ".json" for p in (e2e.PHASES if month == "2016-04" else e2e.PHASES[:4])]
    return files


def checkpoint_manifest(folder, request):
    folder = e2e.safe(folder); config, _ = inputs(folder)
    execution = e2e.load(folder / "recovery-execution.json", 65536)
    e2e.require(type(execution.get("exit_code")) is int and execution["exit_code"] != 0 and execution.get("lease_retained") is True
                and not any((folder / name).exists() for name in ("report.json", "acceptance-recovery.json", "progress-2023-12.json")), "DAT03_CHECKPOINT_FAILED_ATTEMPT_REQUIRED")
    progress = e2e.load(folder / "progress-2016-04.json")
    e2e.require(progress["month"] == "2016-04" and progress["first"] == progress["rerun"]
                and progress["rerun_identical"] is True and progress["retention_verified"] is True, "DAT03_CHECKPOINT_PROGRESS_CHANGED")
    e2e.publication(progress["first"], request["namespace"], folder)
    proof = compatibility(folder / "runner.jar", folder / "recovery.jar")
    e2e.require(proof == e2e.load(folder / "compatibility.json"), "DAT03_CHECKPOINT_CODE_CHANGED")
    for name in checkpoint_files(request):
        e2e.load(folder / name)
    return {name: e2e.sha(e2e.safe(folder / name)) for name in checkpoint_files(request)}


def copy_checkpoint(source, target, request, checkpoint):
    e2e.require(set(checkpoint["files_sha256"]) == set(checkpoint_files(request)), "DAT03_CHECKPOINT_EXACT_FILES_REQUIRED")
    for name, pin in checkpoint["files_sha256"].items():
        file = e2e.safe(source / name); e2e.load(file)
        e2e.require(e2e.sha(file) == pin, "DAT03_CHECKPOINT_FILE_CHANGED")
        destinations = [target / "checkpoint" / name]
        if name == "source-pins.json" or name == "progress-2016-04.json" or name.startswith("orc07-"):
            destinations.append(target / name)
        for destination in destinations:
            destination = e2e.safe(destination); destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            if destination.exists():
                e2e.require(e2e.sha(destination) == pin, "DAT03_CHECKPOINT_OVERLAP_CHANGED"); continue
            with file.open("rb") as reader, destination.open("xb") as writer:
                os.chmod(destination, 0o600); shutil.copyfileobj(reader, writer, 1024**2)


def prepare(deployment, original, preflight, checkpoint_root=None):
    deployment, original, preflight = map(e2e.safe, (deployment, original, preflight))
    evidence = e2e.load(ROOT / "docs/evidence/DAT-03_RECOVERY_PREFLIGHT.json")
    pins = e2e.load(preflight / "preflight-request.json", 65536)
    e2e.require(e2e.sha(preflight / "preflight-request.json") == evidence["preflight_request_sha256"]
                and e2e.sha(preflight / "preflight.jar") == evidence["preflight_runner_sha256"], "DAT03_CONTINUATION_PREFLIGHT_EVIDENCE_CHANGED")
    request = recovery.retained(original, pins)
    e2e.require(str(original.relative_to(deployment / "staging/dat03-e2e")) == request["run_id"][6:] + "/runtime", "DAT03_CONTINUATION_ORIGINAL_ROOT_CHANGED")
    jar = e2e.safe(ROOT / "spark/target/japan-earthquake-etl-runner.jar")
    proof = compatibility(original / "runner.jar", jar)
    with zipfile.ZipFile(preflight / "preflight.jar") as reference:
        e2e.require(e2e.hashlib.sha256(reference.read(TRANSPORT)).hexdigest() == proof["allowed_differences"][TRANSPORT]["recovery_sha256"],
                    "DAT03_CONTINUATION_UNREVIEWED_TRANSPORT_CHANGE")
    folder = e2e.safe(deployment / "staging/dat03-continuation" / uuid.uuid4().hex); folder.mkdir(parents=True, mode=0o700)
    sources = {"recovery.jar": jar, "continuation.py": Path(__file__), "research-e2e.py": Path(__file__).with_name("research-e2e.py"),
               "research-recovery.py": Path(__file__).with_name("research-recovery.py"), "preflight-request.json": preflight / "preflight-request.json"}
    for name, source in sources.items():
        shutil.copyfile(source, folder / name)
    e2e.save(folder / "compatibility.json", proof)
    config = {"version": VERSION, "recovery_runner_sha256": e2e.sha(jar),
        "controller_sha256": e2e.sha(folder / "continuation.py"), "e2e_controller_sha256": e2e.sha(folder / "research-e2e.py"),
        "recovery_controller_sha256": e2e.sha(folder / "research-recovery.py"), "preflight_request_sha256": e2e.sha(folder / "preflight-request.json"),
        "compatibility_sha256": e2e.sha(folder / "compatibility.json")}
    if checkpoint_root is not None:
        checkpoint_root = e2e.safe(checkpoint_root)
        relative = checkpoint_root.relative_to(deployment / "staging/dat03-continuation")
        e2e.require(re.fullmatch(r"[a-f0-9]{32}/runtime", str(relative)), "DAT03_CHECKPOINT_EXACT_ROOT_REQUIRED")
        config["checkpoint"] = {"version": "dat03-2023-publication-checkpoint-v1", "root": "/opt/pipeline/staging/dat03-continuation/" + relative.parts[0],
                                "files_sha256": checkpoint_manifest(checkpoint_root, request)}
    e2e.save(folder / "continuation-request.json", config)
    return folder


def inputs(folder):
    folder = e2e.safe(folder); request = e2e.load(folder / "continuation-request.json", 65536)
    e2e.require(request["version"] == VERSION, "DAT03_CONTINUATION_VERSION_REQUIRED")
    for name, field in (("recovery.jar", "recovery_runner_sha256"), ("continuation.py", "controller_sha256"),
                        ("research-e2e.py", "e2e_controller_sha256"), ("research-recovery.py", "recovery_controller_sha256"),
                        ("preflight-request.json", "preflight_request_sha256"), ("compatibility.json", "compatibility_sha256")):
        e2e.require(e2e.sha(e2e.safe(folder / name)) == request[field], "DAT03_CONTINUATION_CODE_PIN_CHANGED")
    return request, e2e.load(folder / "preflight-request.json", 65536)


def validate_report(folder):
    config, pins = inputs(folder); request = e2e.load(folder / "request.json", 65536)
    e2e.validate_inputs(folder, request)
    for name in ("request", "execution"):
        e2e.require(e2e.sha(folder / (name + ".json")) == pins[name + "_sha256"], "DAT03_CONTINUATION_ORIGINAL_EVIDENCE_CHANGED")
    for month in recovery.COMPLETED:
        e2e.require(e2e.sha(folder / ("progress-" + month + ".json")) == pins["completed_progress_sha256"][month], "DAT03_CONTINUATION_COMPLETED_PROGRESS_CHANGED")
    e2e.require(compatibility(folder / "runner.jar", folder / "recovery.jar") == e2e.load(folder / "compatibility.json"), "DAT03_CONTINUATION_COMPATIBILITY_CHANGED")
    report = e2e.load(folder / "report.json"); e2e.validate(request, report, folder)
    e2e.require(report.get("recovery") == {"version": VERSION, "runner_sha256": config["recovery_runner_sha256"],
                "compatibility_sha256": config["compatibility_sha256"], "preflight_request_sha256": config["preflight_request_sha256"]}, "DAT03_CONTINUATION_PROVENANCE_CHANGED")
    e2e.require({t: p["snapshot_id"] for t, p in report["cases"][2]["first"]["commit"]["tables"].items()} == pins["incomplete_snapshots"],
                "DAT03_CONTINUATION_REPLAYED_COMMIT")
    before = e2e.load(folder / "live-preflight.json")
    expected = pins["incomplete_snapshots"]
    if "checkpoint" in config:
        checkpoint = config["checkpoint"]
        for name, pin in checkpoint["files_sha256"].items():
            e2e.require(e2e.sha(e2e.safe(folder / "checkpoint" / name)) == pin, "DAT03_CHECKPOINT_FILE_CHANGED")
        e2e.require(set(checkpoint["files_sha256"]) == set(checkpoint_files(request))
                    and report["cases"][2] == e2e.load(folder / "checkpoint/progress-2016-04.json"), "DAT03_CHECKPOINT_PROGRESS_CHANGED")
        op = report["cases"][3]["first"]["operation_id"]
        e2e.require(report["cases"][3]["first"]["commit"] == e2e.load(folder / "checkpoint" / op / "gold.json"), "DAT03_CHECKPOINT_REPLAYED_COMMIT")
        expected = {t: p["snapshot_id"] for t, p in report["cases"][3]["first"]["commit"]["tables"].items()}
    e2e.require(before["snapshots"] == expected and before["research_ready"] is False, "DAT03_CONTINUATION_PREFLIGHT_CHANGED")
    return request, report


def worker(folder):
    import resource
    folder = e2e.safe(folder)
    e2e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-continuation/[a-f0-9]{32}", str(folder)), "DAT03_CONTINUATION_EXACT_ROOT_REQUIRED")
    config, pins = inputs(folder); original = e2e.safe(Path(pins["original_root"]))
    request = recovery.retained(original, pins); recovery.quiescent(request["lease_owner"])
    e2e.require(not any((folder / name).exists() for name in ("recovery-execution.json", "report.json", "stdout.private", "stderr.private", "request.json")),
                "DAT03_CONTINUATION_EXISTING_ATTEMPT")
    copy_original(original, folder, request); recovery.retained(folder, pins)
    if "checkpoint" in config:
        checkpoint = config["checkpoint"]
        source = e2e.safe(Path(checkpoint["root"]))
        e2e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-continuation/[a-f0-9]{32}", str(source))
                    and checkpoint["version"] == "dat03-2023-publication-checkpoint-v1"
                    and checkpoint_manifest(source, request) == checkpoint["files_sha256"], "DAT03_CHECKPOINT_CHANGED")
        copy_checkpoint(source, folder, request, checkpoint)
    deps = Path("/opt/spark/jobs/iceberg"); jars = [deps / n for n in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar", "iceberg-aws-bundle-1.10.1.jar")]
    e2e.require(all(j.is_file() for j in jars), "DAT03_CONTINUATION_PINNED_DEPS_REQUIRED")
    command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m", "--jars", ",".join(map(str, jars)),
               "--conf", "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m", "--class", "ie212.earthquake.spark.usgs.ResearchBoundedContinuationJob",
               str(folder / "recovery.jar"), str(folder / "continuation-request.json")]
    started = time.monotonic(); free = shutil.disk_usage(folder).free
    with (folder / "stdout.private").open("xb") as out, (folder / "stderr.private").open("xb") as err:
        os.chmod(folder / "stdout.private", 0o600); os.chmod(folder / "stderr.private", 0o600)
        try:
            child = subprocess.run(command, stdout=out, stderr=err, timeout=3600, check=False)
        except subprocess.TimeoutExpired:
            e2e.save(folder / "recovery-execution.json", {"status": "TIMEOUT", "lease_retained": True, "quiescence_verified": False})
            raise RuntimeError("DAT03_CONTINUATION_TIMEOUT_LEASE_RETAINED") from None
    measured = {"exit_code": child.returncode, "wall_seconds": round(time.monotonic() - started, 3), "child_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                "disk_free_bytes_before": free, "disk_free_bytes_after": shutil.disk_usage(folder).free, "lease_retained": True}
    e2e.save(folder / "recovery-execution.json", measured)
    e2e.require(child.returncode == 0, "DAT03_CONTINUATION_JAVA_FAILED_LEASE_RETAINED")
    recovery.quiescent(request["lease_owner"]); recovery.retained(original, pins)
    validate_report(folder)
    e2e.save(folder / "validation.json", {"status": "VALIDATED_LEASE_HELD", "report_sha256": e2e.sha(folder / "report.json"),
              "recovery_execution_sha256": e2e.sha(folder / "recovery-execution.json")})
    print("event=dat03_continuation_validated lease_released=false", flush=True)


def finalize(folder, report_sha):
    folder = e2e.safe(folder)
    e2e.require(re.fullmatch(r"/opt/pipeline/staging/dat03-continuation/[a-f0-9]{32}", str(folder)), "DAT03_CONTINUATION_EXACT_ROOT_REQUIRED")
    e2e.require(not (folder / "acceptance-recovery.json").exists(), "DAT03_CONTINUATION_FINALIZATION_EXISTS")
    request, report = validate_report(folder); config, pins = inputs(folder)
    recovery.quiescent(request["lease_owner"]); recovery.retained(e2e.safe(Path(pins["original_root"])), pins)
    measurement = e2e.load(folder / "recovery-execution.json"); validated = e2e.load(folder / "validation.json", 65536)
    completion_gate(measurement, validated, report_sha, e2e.sha(folder / "report.json"), e2e.sha(folder / "recovery-execution.json"))
    sys.path.insert(0, "/opt/airflow/dags"); from source_run_guard import lease
    release = lease("release", request["lease_owner"])
    e2e.require(release == {"status": "RELEASED", **request["lease_owner"]}, "DAT03_CONTINUATION_RELEASE_NOT_CONFIRMED")
    e2e.save(folder / "acceptance-recovery.json", {"version": VERSION, "status": "PASSED", "report_sha256": report_sha,
        "recovery_execution_sha256": validated["recovery_execution_sha256"], "request_sha256": pins["request_sha256"],
        "original_execution_sha256": pins["execution_sha256"], "recovery_runner_sha256": config["recovery_runner_sha256"],
        "continuation_request_sha256": e2e.sha(folder / "continuation-request.json"), "lease_release": release, "research_ready": False})
    print("event=dat03_continuation_passed namespace=" + request["namespace"], flush=True)


def completion_gate(measurement, validated, expected_report, actual_report, actual_execution):
    e2e.require(type(measurement.get("exit_code")) is int and measurement["exit_code"] == 0 and validated["status"] == "VALIDATED_LEASE_HELD"
                and expected_report == validated["report_sha256"] == actual_report
                and validated["recovery_execution_sha256"] == actual_execution, "DAT03_CONTINUATION_FINALIZATION_PIN_CHANGED")


TRANSFER = r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1]); assert re.fullmatch(r"/opt/pipeline/staging/dat03-continuation/[a-f0-9]{32}",str(p))
files=json.loads(sys.stdin.buffer.readline(16385)); assert isinstance(files,list) and len(files)==7
assert {f['name'] for f in files}=={'recovery.jar','continuation.py','research-e2e.py','research-recovery.py','preflight-request.json','continuation-request.json','compatibility.json'}
assert not any(q.is_symlink() for q in (p,*p.parents))
p.parent.mkdir(parents=True,exist_ok=True);p.mkdir(mode=0o700)
for f in files:
    assert type(f['bytes']) is int and 0<f['bytes']<=64*1024**2
    h=hashlib.sha256(); remaining=f['bytes']
    with (p/f['name']).open('xb') as out:
        os.chmod(p/f['name'],0o600)
        while remaining:
            b=sys.stdin.buffer.read(min(remaining,1024**2));assert b;out.write(b);h.update(b);remaining-=len(b)
    assert h.hexdigest()==f['sha256']
assert not sys.stdin.buffer.read(1)
'''


def run(deployment, folder):
    deployment, folder = map(e2e.safe, (deployment, folder))
    relative = folder.relative_to(deployment / "staging/dat03-continuation")
    e2e.require(re.fullmatch(r"[a-f0-9]{32}", str(relative)) and not (folder / "runtime").exists(), "DAT03_CONTINUATION_FRESH_ATTEMPT_REQUIRED")
    inputs(folder); before = recovery.health(); inside = "/opt/pipeline/staging/dat03-continuation/" + str(relative)
    paths = [e2e.safe(folder / name) for name in NAMES]
    files = [{"name": p.name, "bytes": p.stat().st_size, "sha256": e2e.sha(p)} for p in paths]
    with subprocess.Popen(["docker", "exec", "-i", e2e.CONTAINER, "python3", "-c", TRANSFER, inside], stdin=subprocess.PIPE) as child:
        child.stdin.write((json.dumps(files) + "\n").encode())
        for path in paths:
            with path.open("rb") as source:
                shutil.copyfileobj(source, child.stdin, 1024**2)
        child.stdin.close(); e2e.require(child.wait(timeout=120) == 0, "DAT03_CONTINUATION_STAGE_FAILED")
    try:
        subprocess.run(["docker", "exec", e2e.CONTAINER, "python3", inside + "/continuation.py", "worker", "--folder", inside], check=True)
        after = recovery.health(); e2e.require(before == after, "DAT03_CONTINUATION_SERVICE_RESTARTED_LEASE_RETAINED")
        e2e.save(folder / "service-state.json", {"before": before, "after": after})
        output = subprocess.run(["docker", "exec", e2e.CONTAINER, "python3", "-c",
            "import json,sys;from pathlib import Path;p=Path(sys.argv[1]);assert p.stat().st_size<65536;print(json.loads(p.read_text())['report_sha256'])", inside + "/validation.json"],
            check=True, capture_output=True, text=True, timeout=20)
        pin = output.stdout.strip(); e2e.require(re.fullmatch(r"[a-f0-9]{64}", pin), "DAT03_CONTINUATION_REPORT_PIN_REQUIRED")
        subprocess.run(["docker", "exec", e2e.CONTAINER, "python3", inside + "/continuation.py", "finalize", "--folder", inside, "--report-sha", pin], check=True, timeout=180)
    finally:
        subprocess.run(["docker", "cp", e2e.CONTAINER + ":" + inside, str(folder / "runtime")], check=True, timeout=120)


def verify(folder):
    folder = e2e.safe(folder); request, report = validate_report(folder)
    config, pins = inputs(folder); accepted = e2e.load(folder / "acceptance-recovery.json", 65536)
    execution = e2e.load(folder / "recovery-execution.json", 65536)
    e2e.require(accepted["version"] == VERSION and accepted["status"] == "PASSED" and accepted["research_ready"] is False
                and accepted["report_sha256"] == e2e.sha(folder / "report.json") and accepted["recovery_execution_sha256"] == e2e.sha(folder / "recovery-execution.json")
                and accepted["request_sha256"] == pins["request_sha256"] and accepted["original_execution_sha256"] == pins["execution_sha256"]
                and accepted["recovery_runner_sha256"] == config["recovery_runner_sha256"] and accepted["continuation_request_sha256"] == e2e.sha(folder / "continuation-request.json")
                and type(execution["exit_code"]) is int and execution["exit_code"] == 0
                and accepted["lease_release"] == {"status": "RELEASED", **request["lease_owner"]}, "DAT03_CONTINUATION_ACCEPTANCE_CHANGED")
    print("event=dat03_continuation_verified cases=4 research_ready=false")


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("mode", choices=("prepare", "run", "worker", "finalize", "verify"))
    parser.add_argument("--deployment-root", type=Path); parser.add_argument("--original-root", type=Path); parser.add_argument("--preflight-root", type=Path)
    parser.add_argument("--folder", type=Path); parser.add_argument("--report-sha"); parser.add_argument("--checkpoint-root", type=Path)
    args = parser.parse_args()
    if args.mode == "prepare":
        e2e.require(all(p is not None for p in (args.deployment_root, args.original_root, args.preflight_root)), "DAT03_CONTINUATION_PATHS_REQUIRED")
        print(prepare(args.deployment_root, args.original_root, args.preflight_root, args.checkpoint_root))
    elif args.mode == "run":
        e2e.require(args.deployment_root is not None and args.folder is not None, "DAT03_CONTINUATION_PATHS_REQUIRED"); run(args.deployment_root, args.folder)
    elif args.mode == "worker":
        worker(args.folder)
    elif args.mode == "finalize":
        e2e.require(args.folder is not None and args.report_sha is not None, "DAT03_CONTINUATION_PATHS_REQUIRED"); finalize(args.folder, args.report_sha)
    else:
        e2e.require(args.folder is not None, "DAT03_CONTINUATION_PATHS_REQUIRED"); verify(args.folder)


if __name__ == "__main__":
    main()
