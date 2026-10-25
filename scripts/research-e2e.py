#!/usr/bin/env python3
"""DAT-03 explicit bounded runtime; never build/start/reset services, alter DAGs, or release foreign leases."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]
CONTAINER = "japan-earthquake-etl-airflow-scheduler-1"
VERSION = "dat03-bounded-e2e-v1"
YEARS = (2000, 2011, 2016, 2023, 2024)
MONTHS = ("2000-01", "2011-04", "2016-04", "2023-12")
PHASES = ("readiness", "bronze", "silver", "gold", "verify", "publish")
TABLES = {"event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band"}
QUERY = "https://earthquake.usgs.gov/fdsnws/event/1/query?" + urlencode({
    "format": "geojson", "eventtype": "earthquake", "starttime": "2023-11-30T23:59:44Z",
    "endtime": "2024-01-01T00:00:15.999Z", "minlatitude": "20.0", "maxlatitude": "50.0",
    "minlongitude": "120.0", "maxlongitude": "155.0", "orderby": "time-asc", "limit": "20000", "offset": "1"})


def require(ok, reason):
    if not ok:
        raise RuntimeError(reason)


def sha(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def save(path, value):
    with path.open("x", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def safe(path):
    path = Path(path).absolute()
    require(not any(p.is_symlink() for p in (path, *path.parents)), "DAT03_SYMLINK_REJECTED")
    return path.resolve()


def load(path, limit=4 * 1024**2):
    path = safe(path)
    require(path.is_file() and 0 < path.stat().st_size <= limit, "DAT03_BOUNDED_METADATA_REQUIRED")
    return json.loads(path.read_text())


def validate_inputs(folder, request):
    require(set(request) == {"version", "namespace", "run_id", "lease_owner", "runner_sha256", "baseline_inventory_sha256",
                            "boundary_inventory_sha256", "jma_captures", "usgs_capture_sha256"}
            and request["version"] == VERSION and re.fullmatch(r"dat03-[a-f0-9]{32}", request["run_id"])
            and request["namespace"] == "iceberg.dat03_e2e_" + request["run_id"][6:]
            and request["lease_owner"] == {"dag_id": "dat03_bounded_e2e", "run_id": request["run_id"]}, "DAT03_REQUEST_INVALID")
    for file, field in (("runner.jar", "runner_sha256"), ("baseline.csv", "baseline_inventory_sha256"), ("boundary.csv", "boundary_inventory_sha256")):
        require(sha(safe(folder / file)) == request[field], "DAT03_INPUT_PIN_CHANGED")
    require([pin["year"] for pin in request["jma_captures"]] == list(YEARS), "DAT03_YEAR_SET_INVALID")
    for pin in request["jma_captures"]:
        meta = load(folder / f"inputs/h{pin['year']}-capture.json", 65536)
        raw = safe(folder / f"inputs/h{pin['year']}.zip")
        require(digest(meta) == pin["capture_sha256"] and meta["sha256"] == pin["sha256"] == sha(raw)
                and 0 < raw.stat().st_size == meta["bytes"] <= 32 * 1024**2 and meta["http_status"] == 200
                and meta["inventory_metadata_drift"] is False, "DAT03_JMA_PIN_CHANGED")
    meta = load(folder / "inputs/usgs-capture.json", 65536); raw = safe(folder / "inputs/usgs-december2023.geojson")
    require(digest(meta) == request["usgs_capture_sha256"] and meta["url"] == QUERY and meta["http_status"] == 200
            and meta["sha256"] == sha(raw) and 0 < raw.stat().st_size == meta["bytes"] <= 16 * 1024**2, "DAT03_USGS_PIN_CHANGED")


def capture(folder):
    folder = safe(folder)
    require(folder.is_relative_to(ROOT / "staging") and folder != ROOT / "staging", "DAT03_CAPTURE_ROOT_REQUIRED")
    if folder.exists():
        meta = load(folder / "capture.json", 65536); raw = safe(folder / "usgs-december2023.geojson")
        require(meta["url"] == QUERY and meta["http_status"] == 200 and meta["sha256"] == sha(raw)
                and 0 < raw.stat().st_size == meta["bytes"] <= 16 * 1024**2, "DAT03_CAPTURE_CHANGED")
        return meta
    require(shutil.disk_usage(ROOT).free >= 2 * 1024**3, "DAT03_CAPTURE_DISK_RESERVE")
    folder.mkdir(parents=True, mode=0o700)
    output = subprocess.run(["curl", "--fail", "--location", "--max-redirs", "3", "--proto", "=https", "--proto-redir", "=https",
        "--max-time", "90", "--max-filesize", str(16 * 1024**2), "--no-clobber", "--silent", "--show-error",
        "--dump-header", str(folder / "headers.txt"), "--output", str(folder / "usgs-december2023.geojson"),
        "--write-out", "%{http_code}\n%{url_effective}\n%{content_type}\n", QUERY], check=True, capture_output=True, text=True, timeout=100)
    status, url, content_type = output.stdout.strip().splitlines()
    require(status == "200" and url == QUERY and content_type.split(";")[0] in ("application/json", "application/geo+json"), "DAT03_USGS_HTTP_INVALID")
    file = folder / "usgs-december2023.geojson"
    meta = {"url": url, "http_status": 200, "content_type": content_type, "sha256": sha(file), "bytes": file.stat().st_size,
            "retrieved_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")}
    save(folder / "capture.json", meta)
    return meta


def prepare(deployment, profiles, usgs):
    deployment, profiles, usgs = safe(deployment), safe(profiles), safe(usgs)
    jar = ROOT / "spark/target/japan-earthquake-etl-runner.jar"
    require(jar.is_file() and not jar.is_symlink(), "DAT03_PREBUILT_JAR_REQUIRED")
    require(shutil.disk_usage(deployment).free >= 4 * 1024**3, "DAT03_E2E_DISK_RESERVE")
    key = uuid.uuid4().hex
    folder = deployment / "staging/dat03-e2e" / key
    safe(folder).mkdir(parents=True, mode=0o700)
    inputs = folder / "inputs"; inputs.mkdir(mode=0o700)
    shutil.copyfile(jar, folder / "runner.jar")
    shutil.copyfile(Path(__file__), folder / "controller.py")
    for source, target in (("hypocenter_archives_v1.csv", "baseline.csv"), ("research_boundary_archives_v1.csv", "boundary.csv")):
        shutil.copyfile(ROOT / "config/jma" / source, folder / target)
    captures = []
    for year in YEARS:
        source = safe(profiles / f"h{year}")
        meta = json.loads((source / "capture.json").read_text())
        raw = safe(source / f"h{year}.zip")
        require(raw.is_file() and meta["sha256"] == sha(raw) and meta["bytes"] == raw.stat().st_size
                and meta["http_status"] == 200 and not meta["inventory_metadata_drift"], "DAT03_JMA_PROFILE_PIN_CHANGED")
        shutil.copyfile(raw, inputs / raw.name)
        shutil.copyfile(source / "capture.json", inputs / f"h{year}-capture.json")
        captures.append({"year": year, "sha256": sha(raw), "capture_sha256": digest(meta)})
    meta = json.loads((usgs / "capture.json").read_text())
    require(meta["url"] == QUERY and meta["http_status"] == 200 and meta["sha256"] == sha(safe(usgs / "usgs-december2023.geojson")), "DAT03_USGS_PIN_CHANGED")
    shutil.copyfile(usgs / "usgs-december2023.geojson", inputs / "usgs-december2023.geojson")
    shutil.copyfile(usgs / "capture.json", inputs / "usgs-capture.json")
    request = {"version": VERSION, "namespace": "iceberg.dat03_e2e_" + key, "run_id": "dat03-" + key,
               "lease_owner": {"dag_id": "dat03_bounded_e2e", "run_id": "dat03-" + key}, "runner_sha256": sha(folder / "runner.jar"),
               "baseline_inventory_sha256": sha(folder / "baseline.csv"), "boundary_inventory_sha256": sha(folder / "boundary.csv"),
               "jma_captures": captures, "usgs_capture_sha256": digest(meta)}
    save(folder / "request.json", request)
    validate_inputs(folder, request)
    return folder


def publication(record, namespace, folder):
    context = record["context"]; unsigned = {k: v for k, v in context.items() if k != "context_sha256"}
    op = "orc07-" + hashlib.sha256((context["dag_id"] + "|" + context["run_id"]).encode()).hexdigest()[:40]
    require(digest(unsigned) == context["context_sha256"] and context["operation_id"] == record["operation_id"] == op
            and context["mode"] == "real" and context["contract_version"] == "orc-07-v1"
            and context["resolver_request"]["namespace"] == namespace, "DAT03_CONTEXT_CHANGED")
    require(len(record["receipts"]) == len(PHASES), "DAT03_RECEIPTS_INCOMPLETE")
    prior = None; artifacts = {}
    for phase, receipt in zip(PHASES, record["receipts"]):
        artifact = load(folder / op / (phase + ".json"))
        require(receipt["phase"] == phase and receipt["operation_id"] == op and receipt["context_sha256"] == context["context_sha256"]
                and receipt["contract_version"] == "orc-07-v1" and receipt["mode"] == "real" and receipt["status"] == "ok"
                and receipt["upstream_sha256"] == (None if prior is None else digest(prior))
                and receipt["artifact_sha256"] == digest(artifact)
                and receipt["artifact_uri"].endswith("/real-etl/" + op + "/" + phase + ".json"), "DAT03_PHASE_CHAIN_CHANGED")
        prior = receipt; artifacts[phase] = artifact
    commit, pub = record["commit"], record["publication"]
    require(artifacts["silver"] == record["silver"] and artifacts["gold"] == commit and artifacts["publish"] == pub
            and commit["namespace"] == pub["namespace"] == namespace and commit["gold_run_id"] == pub["gold_run_id"] == op
            and commit["status"] == "COMMITTED" and commit["published"] is False and pub["engine"] == "TRINO"
            and pub["identity_sha256"] == commit["identity_sha256"], "DAT03_COMMIT_CHANGED")
    # Preserve Java journal field order: the commit bundle hashes journal bytes, not sorted phase JSON.
    bundle_sha = hashlib.sha256(json.dumps(commit, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
    require(pub["bundle_sha256"] == bundle_sha and pub["publication_id"] == "pub_" + bundle_sha, "DAT03_PUBLICATION_PIN_CHANGED")
    require(set(commit["tables"]) == TABLES, "DAT03_SIX_TABLES_REQUIRED")
    checks = pub["checks"]
    blockers = {"required_fields", "bridge_lineage", "source_counts", "primary_provenance", "utc_jst", "dimensions", "aggregate_count",
                "changed_run_scope", "outside:event_current", "outside:event_source_bridge"} | {"unique:" + t for t in TABLES}
    require(blockers <= set(checks) and all(checks[name]["expected"] == 0 for name in blockers), "DAT03_BLOCKER_CHECKS_REQUIRED")
    for name, pin in commit["tables"].items():
        require(type(pin["snapshot_id"]) is int and 0 < pin["snapshot_id"] <= 9223372036854775807
                and type(pin["count"]) is int and pin["count"] >= 0 and pin["table"] == namespace + "." + name
                and checks["count:" + name]["actual"] == checks["count:" + name]["expected"] == pin["count"]
                and checks["identity:" + name]["actual"] == checks["identity:" + name]["expected"] == 1, "DAT03_SNAPSHOT_READBACK_CHANGED")
        require(checks["count:" + name]["sql"] == "SELECT count(*) FROM " + namespace + "." + name + " FOR VERSION AS OF " + str(pin["snapshot_id"]),
                "DAT03_SNAPSHOT_QUERY_CHANGED")
    snapshots = {name: pin["snapshot_id"] for name, pin in commit["tables"].items()}
    require(all(r["snapshots"] == snapshots for r in record["receipts"][3:])
            and record["receipts"][-1]["publication_id"] == pub["publication_id"] and record["receipts"][-1]["published"] is True,
            "DAT03_RECEIPT_SNAPSHOT_CHANGED")


def validate(request, report, folder):
    require(report.get("version") == VERSION and report.get("status") == "PASSED" and report.get("namespace") == request["namespace"]
            and re.fullmatch(r"iceberg\.dat03_e2e_[a-f0-9]{32}", report["namespace"])
            and report.get("run_id") == request["run_id"] and report.get("runner_sha256") == request["runner_sha256"]
            and report.get("research_ready") is False and report.get("full_research_catalog") is False
            and report.get("synthetic_data_used") is False, "DAT03_REPORT_IDENTITY_INVALID")
    require([c.get("month") for c in report.get("cases", [])] == list(MONTHS), "DAT03_CASES_INCOMPLETE")
    source = report["jma_bronze_inputs"]; usgs = report["usgs_bronze_input"]
    require(set(source) == {str(y) for y in YEARS} and usgs["source_system"] == "USGS"
            and usgs["sha256"] == load(folder / "inputs/usgs-capture.json", 65536)["sha256"], "DAT03_SOURCE_PINS_CHANGED")
    for pin in request["jma_captures"]:
        actual = source[str(pin["year"])]; capture_meta = load(folder / f"inputs/h{pin['year']}-capture.json", 65536)
        require(actual["source_system"] == "JMA_BULLETIN" and actual["sha256"] == pin["sha256"] and actual["year"] == pin["year"]
                and actual["segment"] == "full-year" and actual["catalog_release"] == capture_meta["catalog_release"], "DAT03_SOURCE_PINS_CHANGED")
    baseline = {name: 0 for name in TABLES}; cutoff = None
    for case in report["cases"]:
        require(case.get("rerun_identical") is True and case.get("retention_verified") is True and case.get("first") == case.get("rerun"), "DAT03_RERUN_INVALID")
        row = case["first"]
        publication(row, request["namespace"], folder)
        resolver = row["context"]["resolver_request"]
        expected_inputs = [source[case["month"][:4]]] if case["month"] != "2023-12" else [source["2023"], source["2024"], usgs]
        require(resolver["bronze_inputs"] == expected_inputs and resolver["target_months"] == [case["month"]]
                and resolver["baseline_snapshots"] == baseline and row["context"]["dag_id"] == "dat03_bounded_e2e"
                and row["context"]["run_id"] == request["run_id"] + "-" + case["month"], "DAT03_SCOPE_OR_BASELINE_CHANGED")
        if cutoff is None:
            cutoff = resolver["source_cutoff_utc"]
        require(resolver["source_cutoff_utc"] == cutoff, "DAT03_CUTOFF_CHANGED")
        require(row["context"]["resolver_request"]["resolver_version"] == "orc-06-v2"
                and row["context"]["resolver_request"]["namespace"] == request["namespace"]
                and row["silver"]["affected_months"] == [case["month"]]
                and row["silver"]["reconciliation"]["balanced"] is True
                and row["publication"]["verify_status"] == "PASSED" and row["publication"]["published"] is True
                and row["commit"]["tables"] == row["publication"]["snapshot_bundle"] and len(row["commit"]["tables"]) == 6,
                "DAT03_PUBLICATION_INVALID")
        checks = row["publication"]["checks"]
        require(len(checks) >= 36 and all(type(v.get("actual")) is int and type(v.get("expected")) is int
                and v["actual"] == v["expected"] for v in checks.values()), "DAT03_TRINO_CHECKS_INVALID")
        require([r["phase"] for r in row["receipts"]] == ["readiness", "bronze", "silver", "gold", "verify", "publish"], "DAT03_RECEIPTS_INCOMPLETE")
        baseline = {name: pin["snapshot_id"] for name, pin in row["commit"]["tables"].items()}
    require(report["final_snapshots"] == baseline, "DAT03_FINAL_SNAPSHOTS_CHANGED")


def worker(folder):
    # Runs inside the EXISTING Airflow container, no image/service/deployment changes.
    import resource
    sys.path.insert(0, "/opt/airflow/dags")
    from source_run_guard import lease
    from airflow.models import DagRun
    from airflow.utils.session import create_session
    from sqlalchemy import select
    folder = safe(folder); request = load(folder / "request.json", 65536)
    require(folder.is_relative_to(Path("/opt/pipeline/staging/dat03-e2e")) and request["version"] == VERSION, "DAT03_WORK_ROOT_REQUIRED")
    require(not (folder / "execution.json").exists() and not (folder / "report.json").exists(), "DAT03_EXISTING_EXECUTION_NO_AUTO_RETRY")
    validate_inputs(folder, request)
    with create_session() as session:
        active = session.scalars(select(DagRun).where(DagRun.state.in_(["running", "queued"]))).all()
        require(not active, "DAT03_ACTIVE_DAGRUNS")
    deps = Path("/opt/spark/jobs/iceberg")
    jars = [deps / name for name in ("iceberg-spark-runtime-3.5_2.12-1.10.1.jar", "iceberg-aws-bundle-1.10.1.jar")]
    require(all(j.is_file() for j in jars), "DAT03_PINNED_ICEBERG_JARS_MISSING")
    lease("acquire", request["lease_owner"])
    start = time.monotonic(); before = shutil.disk_usage(folder).free
    command = ["/opt/spark/bin/spark-submit", "--master", "local[1]", "--driver-memory", "768m", "--jars", ",".join(str(j) for j in jars),
               "--conf", "spark.driver.extraJavaOptions=-XX:MaxDirectMemorySize=256m", "--class", "ie212.earthquake.spark.usgs.ResearchBoundedE2EJob",
               str(folder / "runner.jar"), str(folder / "request.json")]
    with (folder / "stdout.private").open("xb") as out, (folder / "stderr.private").open("xb") as err:
        os.chmod(folder / "stdout.private", 0o600); os.chmod(folder / "stderr.private", 0o600)
        try:
            child = subprocess.run(command, stdout=out, stderr=err, timeout=3600, check=False)
        except subprocess.TimeoutExpired:
            save(folder / "execution.json", {"status": "TIMEOUT", "lease_retained": True, "quiescence_verified": False})
            raise RuntimeError("DAT03_TIMEOUT_LEASE_RETAINED") from None
    measured = {"exit_code": child.returncode, "wall_seconds": round(time.monotonic() - start, 3),
                "child_peak_rss_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                "disk_free_bytes_before": before, "disk_free_bytes_after": shutil.disk_usage(folder).free,
                "lease_held_at_execution_end": True}
    save(folder / "execution.json", measured)
    require(child.returncode == 0, "DAT03_JAVA_FAILED_LEASE_RETAINED")
    report = load(folder / "report.json"); validate(request, report, folder)
    release = lease("release", request["lease_owner"])
    require(release["status"] == "RELEASED", "DAT03_RELEASE_NOT_CONFIRMED")
    save(folder / "acceptance.json", {"status": "PASSED", "request_sha256": sha(folder / "request.json"), "report_sha256": sha(folder / "report.json"),
        "runner_sha256": request["runner_sha256"], "lease_release": release, "measurement": measured, "research_ready": False})
    print("event=dat03_bounded_e2e_passed namespace=" + request["namespace"], flush=True)


# Stream exact new files as the normal Airflow user into a fresh volume directory.
# No root/chown, tar extraction, broad copy, overwrite, or deployed runner replacement.
TRANSFER = r'''
import hashlib,json,os,re,sys
from pathlib import Path
p=Path(sys.argv[1])
assert re.fullmatch(r"/opt/pipeline/staging/dat03-e2e/[a-f0-9]{32}",str(p))
header=sys.stdin.buffer.readline(32769)
assert len(header)<=32768
files=json.loads(header); assert isinstance(files,list) and len(files)==17
names=[f["name"] for f in files]
expected={"runner.jar","controller.py","baseline.csv","boundary.csv","request.json","inputs/usgs-capture.json","inputs/usgs-december2023.geojson"}
expected|={"inputs/h"+str(y)+s for y in (2000,2011,2016,2023,2024) for s in (".zip","-capture.json")}
assert len(set(names))==len(names) and set(names)==expected
assert sum(f["bytes"] for f in files)<=160*1024**2
assert not any(q.is_symlink() for q in (p,*p.parents))
p.parent.mkdir(parents=True,exist_ok=True); p.mkdir(mode=0o700); (p/"inputs").mkdir(mode=0o700)
for f in files:
    assert type(f["bytes"]) is int and 0<f["bytes"]<=64*1024**2
    h=hashlib.sha256(); remaining=f["bytes"]
    with (p/f["name"]).open("xb") as out:
        while remaining:
            b=sys.stdin.buffer.read(min(remaining,1024**2)); assert b
            out.write(b); h.update(b); remaining-=len(b)
    assert h.hexdigest()==f["sha256"]
assert not sys.stdin.buffer.read(1)
print("event=dat03_exact_volume_artifacts_staged",flush=True)
'''


def stage_volume(folder, inside):
    request = load(folder / "request.json", 65536); validate_inputs(folder, request)
    paths = [folder / n for n in ("runner.jar", "controller.py", "baseline.csv", "boundary.csv", "request.json")]
    paths += sorted((folder / "inputs").iterdir())
    files = [{"name": str(p.relative_to(folder)), "bytes": safe(p).stat().st_size, "sha256": sha(p)} for p in paths]
    with subprocess.Popen(["docker", "exec", "-i", CONTAINER, "python3", "-c", TRANSFER, inside], stdin=subprocess.PIPE) as child:
        child.stdin.write((json.dumps(files) + "\n").encode())
        for path in paths:
            with path.open("rb") as stream:
                shutil.copyfileobj(stream, child.stdin, 1024**2)
        child.stdin.close(); require(child.wait(timeout=120) == 0, "DAT03_VOLUME_TRANSFER_FAILED")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("capture-usgs", "prepare", "run", "worker", "verify"))
    parser.add_argument("--deployment-root", type=Path)
    parser.add_argument("--profiles", type=Path, default=ROOT / "staging/dat03-research-profile-20261024")
    parser.add_argument("--usgs", type=Path, default=ROOT / "staging/dat03-usgs-december2023")
    parser.add_argument("--folder", type=Path)
    args = parser.parse_args()
    if args.mode == "capture-usgs":
        meta = capture(args.usgs); print("event=dat03_usgs_capture_pinned sha256=" + meta["sha256"])
    elif args.mode == "prepare":
        require(args.deployment_root is not None, "DAT03_DEPLOYMENT_ROOT_REQUIRED")
        print(prepare(args.deployment_root, args.profiles, args.usgs))
    elif args.mode == "worker":
        worker(args.folder)
    elif args.mode == "run":
        require(args.deployment_root is not None and args.folder is not None, "DAT03_EXACT_FOLDER_REQUIRED")
        folder, deployment = safe(args.folder), safe(args.deployment_root)
        relative = folder.relative_to(deployment / "staging/dat03-e2e")
        require(re.fullmatch(r"[a-f0-9]{32}", str(relative)), "DAT03_EXACT_FOLDER_REQUIRED")
        inside = "/opt/pipeline/staging/dat03-e2e/" + str(relative)
        services = ["airflow-scheduler", "minio", "iceberg-rest", "trino"]
        health = subprocess.run(["docker", "inspect", "--format", "{{.State.Status}} {{if .State.Health}}{{.State.Health.Status}}{{end}}",
                                 *["japan-earthquake-etl-" + s + "-1" for s in services]], check=True, capture_output=True, text=True, timeout=20)
        require(health.stdout.splitlines() == ["running healthy"] * len(services), "DAT03_EXISTING_SERVICES_NOT_READY")
        require(not (folder / "runtime").exists(), "DAT03_EXISTING_RUNTIME_NO_AUTO_RETRY")
        stage_volume(folder, inside)
        try:
            subprocess.run(["docker", "exec", CONTAINER, "python3", inside + "/controller.py", "worker", "--folder", inside], check=True)
        finally:
            # Preserve an independent host copy, including failure evidence. Never clear/retry/release here.
            subprocess.run(["docker", "cp", CONTAINER + ":" + inside, str(folder / "runtime")], check=True, timeout=120)
    else:
        require(args.folder is not None, "DAT03_EXACT_FOLDER_REQUIRED")
        folder = safe(args.folder); request = load(folder / "request.json", 65536)
        validate_inputs(folder, request)
        report = load(folder / "report.json"); validate(request, report, folder)
        acceptance = load(folder / "acceptance.json", 65536); execution = load(folder / "execution.json", 65536)
        require(acceptance["status"] == "PASSED" and acceptance["research_ready"] is False
                and acceptance["runner_sha256"] == request["runner_sha256"] and acceptance["measurement"] == execution
                and type(execution["exit_code"]) is int and execution["exit_code"] == 0
                and acceptance["report_sha256"] == sha(folder / "report.json") and acceptance["request_sha256"] == sha(folder / "request.json")
                and acceptance["lease_release"] == {"status": "RELEASED", **request["lease_owner"]}, "DAT03_ACCEPTANCE_PIN_CHANGED")
        print("event=dat03_bounded_e2e_verified namespace=" + request["namespace"])


if __name__ == "__main__":
    main()
