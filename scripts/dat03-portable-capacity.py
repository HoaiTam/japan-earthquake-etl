#!/usr/bin/env python3
"""Single isolated read-only Spark attempt; pinned local dependencies, no automatic retry."""
import argparse
import hashlib
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

CONTAINER = "japan-earthquake-etl-airflow-scheduler-1"
CLASS = "ie212.earthquake.spark.gold.PortableGoldCapacityJob"
BRONZE_CLASS = "ie212.earthquake.spark.bronze.PortableBronzeProvenance"
DEPS = ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar", "iceberg-aws-bundle-1.10.1.jar")


def require(ok, reason):
    if not ok: raise ValueError(reason)


def safe(path):
    path = Path(path).absolute()
    require(".." not in path.parts and not any(p.is_symlink() for p in (path, *path.parents)), "SAFE_PATH_REQUIRED")
    return path


def sha(path):
    with safe(path).open("rb") as stream: return hashlib.file_digest(stream, "sha256").hexdigest()


def load(path, limit=65536):
    path = safe(path)
    require(path.is_file() and path.stat().st_size <= limit, "BOUNDED_METADATA_REQUIRED")
    def unique(pairs):
        result = {}
        for k, v in pairs:
            require(k not in result, "DUPLICATE_KEY"); result[k] = v
        return result
    return json.loads(path.read_bytes(), object_pairs_hook=unique,
                      parse_constant=lambda _: require(False, "NONFINITE_JSON"))


def save(path, value):
    with safe(path).open("x", encoding="utf-8") as stream:
        os.chmod(stream.name, 0o600)
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False); stream.write("\n")


def command(root):
    root = safe(root)
    require(re.fullmatch(r"/opt/pipeline/staging/dat03-portable-capacity/[a-f0-9]{32}", str(root)), "EXACT_RUNTIME_ROOT_REQUIRED")
    launch = load(root / "launch.json"); job = launch.get("job", "capacity")
    require(job in ("capacity", "bronze-provenance"), "READ_ONLY_JOB_REQUIRED")
    if job == "bronze-provenance":
        return ["java", "-Xmx768m", "-cp", str(root / "runner.jar"), BRONZE_CLASS,
                str(root / "request.json"), launch["request_sha256"], str(root / "result.json")]
    # All jars are SHA-checked and present in the same local[1] container.
    # local:/ avoids HTTP fetch/copies while the executor heartbeat is starting.
    return ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m",
            "--conf", "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m",
            "--jars", ",".join("local:/opt/spark/jobs/iceberg/" + name for name in DEPS),
            "--class", CLASS, "local:" + str(root / "runner.jar"), str(root / "request.json"),
            load(root / "launch.json")["request_sha256"], str(root / "result.json")]


def worker(root):
    root = safe(root); launch = load(root / "launch.json")
    require(launch["version"] == "dat03-portable-capacity-launch-v1" and launch["seconds"] == 360, "FIXED_READ_ONLY_DEADLINE_REQUIRED")
    for name in ("request.json", "runner.jar", "controller.py"):
        require(sha(root / name) == launch[name.replace(".json", "").replace(".jar", "").replace(".py", "") + "_sha256"], "LAUNCH_PIN_MISMATCH")
    require(set(launch["dependencies"]) == set(DEPS), "EXACT_DEPENDENCIES_REQUIRED")
    for name in DEPS:
        require(sha(Path("/opt/spark/jobs/iceberg") / name) == launch["dependencies"][name], "DEPENDENCY_PIN_MISMATCH")
    require(not (Path(os.environ["SOURCE_GUARD_ROOT"]) / "owner.json").exists(), "SOURCE_LEASE_NOT_IDLE")
    cmd = command(root); started = time.monotonic(); timed_out = False
    with (root / "stdout.private").open("xb") as out, (root / "stderr.private").open("xb") as err:
        os.chmod(out.name, 0o600); os.chmod(err.name, 0o600)
        child = subprocess.Popen(cmd, stdout=out, stderr=err, start_new_session=True)
        save(root / "active.json", {"pid": child.pid, "mode": "READ_ONLY", "deadline_seconds": 360})
        try: code = child.wait(timeout=360)
        except subprocess.TimeoutExpired:
            timed_out = True; os.killpg(child.pid, signal.SIGTERM)
            try: code = child.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL); code = child.wait(timeout=10)
    execution = {"exit_code": code, "timed_out": timed_out, "elapsed_seconds": round(time.monotonic() - started, 3),
                 "lake_writes": False, "automatic_retry": False, "launch_sha256": sha(root / "launch.json")}
    save(root / "execution.json", execution); print(json.dumps(execution), flush=True)
    require(code == 0 and not timed_out and (root / "result.json").is_file(), "CAPACITY_ATTEMPT_NOT_ACCEPTED")
    report = load(root / "result.json", 4 * 1024**2)
    if launch.get("job", "capacity") == "capacity":
        require(report["status"] == "READ_ONLY_FINGERPRINT_CAPACITY_PASSED" and report["gold_published"] is False
                and report["request_sha256"] == launch["request_sha256"], "CAPACITY_REPORT_MISMATCH")
    else:
        require(report["status"] == "ORIGINAL_TO_PHYSICAL_MAPPING_VERIFIED" and report["gold_published"] is False
                and report["plan_sha256"] == launch["request_sha256"], "PROVENANCE_REPORT_MISMATCH")


def run(request, expected, runner, output, job="capacity"):
    request, runner, output = map(safe, (request, runner, output))
    require(job in ("capacity", "bronze-provenance"), "READ_ONLY_JOB_REQUIRED")
    load(request, 4 * 1024**2); require(re.fullmatch(r"[a-f0-9]{64}", expected) and sha(request) == expected, "REQUEST_PIN_MISMATCH")
    require(runner.is_file() and runner.stat().st_size <= 64 * 1024**2, "BOUNDED_PREBUILT_RUNNER_REQUIRED")
    require(not output.exists() and shutil.disk_usage(output.parent).free >= 4 * 1024**3, "NEW_OUTPUT_AND_DISK_RESERVE_REQUIRED")
    spec = importlib.util.spec_from_file_location("rollout", Path(__file__).with_name("research-rollout.py"))
    r = importlib.util.module_from_spec(spec); spec.loader.exec_module(r)
    before = r.services(); r.idle(); r.trino_queries_idle()
    output.mkdir(mode=0o700); ident = uuid.uuid4().hex
    inside = "/opt/pipeline/staging/dat03-portable-capacity/" + ident
    for src, name in ((request, "request.json"), (runner, "runner.jar"), (Path(__file__), "controller.py")):
        with src.open("rb") as inp, (output / name).open("xb") as dest:
            os.chmod(dest.name, 0o600); shutil.copyfileobj(inp, dest)
    dependencies = {name: sha(runner.parent / "iceberg" / name) for name in DEPS}
    save(output / "launch.json", {"version": "dat03-portable-capacity-launch-v1", "seconds": 360,
         "request_sha256": expected, "runner_sha256": sha(output / "runner.jar"),
         "controller_sha256": sha(output / "controller.py"), "dependencies": dependencies, "profile": "local-uris-v1", "job": job})
    transfer = '''import json,sys,hashlib,os,re
from pathlib import Path
p=Path(sys.argv[1]); assert re.fullmatch(r"/opt/pipeline/staging/dat03-portable-capacity/[a-f0-9]{32}",str(p))
assert not any(q.is_symlink() for q in (p,*p.parents))
files=json.loads(sys.stdin.buffer.readline()); assert {x["name"] for x in files}=={"request.json","runner.jar","controller.py","launch.json"} and len(files)==4
p.parent.mkdir(exist_ok=True);p.mkdir(mode=0o700)
for f in files:
 assert type(f["bytes"]) is int and 0<f["bytes"]<=64*1024**2
 left=f["bytes"]; digest=hashlib.sha256()
 with (p/f["name"]).open("xb") as dest:
  os.chmod(dest.name,0o600)
  while left:
   b=sys.stdin.buffer.read(min(left,1024**2)); assert b; left-=len(b);digest.update(b);dest.write(b)
 assert digest.hexdigest()==f["sha256"]
assert not sys.stdin.buffer.read(1)
'''
    files = [{"name": n, "bytes": (output / n).stat().st_size, "sha256": sha(output / n)} for n in ("request.json", "runner.jar", "controller.py", "launch.json")]
    with subprocess.Popen(["docker", "exec", "-i", CONTAINER, "python3", "-c", transfer, inside], stdin=subprocess.PIPE) as child:
        child.stdin.write((json.dumps(files) + "\n").encode())
        for item in files:
            with (output / item["name"]).open("rb") as stream: shutil.copyfileobj(stream, child.stdin, 1024**2)
        child.stdin.close(); require(child.wait(timeout=120) == 0, "CAPACITY_TRANSFER_FAILED")
    bootstrap = 'import hashlib,runpy,sys;from pathlib import Path;p=Path(sys.argv[1]);assert hashlib.sha256((p/"controller.py").read_bytes()).hexdigest()==sys.argv[2];sys.argv=[str(p/"controller.py"),"worker","--folder",str(p)];runpy.run_path(str(p/"controller.py"),run_name="__main__")'
    try:
        result = subprocess.run(["docker", "exec", CONTAINER, "python3", "-c", bootstrap, inside, sha(output / "controller.py")], timeout=480)
    finally:
        subprocess.run(["docker", "cp", CONTAINER + ":" + inside, str(output / "runtime")], timeout=120, check=True)
    after = r.services(); r.idle(); r.trino_queries_idle()
    save(output / "postflight.json", {"services_before": before, "services_after": after, "unchanged": before == after,
                                    "worker_exit_code": result.returncode, "lake_writes": False})
    require(before == after and result.returncode == 0, "CAPACITY_NOT_ACCEPTED_NO_AUTOMATIC_RETRY")


def main():
    p = argparse.ArgumentParser(description=__doc__); p.add_argument("mode", choices=("run", "worker"))
    p.add_argument("--request", type=Path); p.add_argument("--request-sha256"); p.add_argument("--runner", type=Path)
    p.add_argument("--output", type=Path); p.add_argument("--folder", type=Path)
    p.add_argument("--job", choices=("capacity", "bronze-provenance"), default="capacity")
    args = p.parse_args()
    if args.mode == "worker":
        p.error("--folder required") if args.folder is None else worker(args.folder)
    else:
        if any(x is None for x in (args.request, args.request_sha256, args.runner, args.output)): p.error("exact inputs/output required")
        run(args.request, args.request_sha256, args.runner, args.output, args.job)


if __name__ == "__main__": main()
