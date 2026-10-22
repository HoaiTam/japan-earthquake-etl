#!/usr/bin/env python3
"""Explicit local Source capture -> Silver component; no Docker, scheduler, Gold or auto-build."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("current_month_local_runtime", ROOT / "scripts/local-ml.py")
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)
USGS_KEYS = {"USGS_API_BASE_URL", "USGS_MIN_LATITUDE", "USGS_MAX_LATITUDE", "USGS_MIN_LONGITUDE",
             "USGS_MAX_LONGITUDE", "USGS_SEED_START_UTC", "PIPELINE_OVERLAP_DAYS", "USGS_MAX_WINDOW_DAYS",
             "USGS_REQUEST_LIMIT", "USGS_HTTP_TIMEOUT_MS", "USGS_HTTP_MAX_ATTEMPTS", "USGS_HTTP_INITIAL_BACKOFF_MS",
             "USGS_HTTP_MAX_BACKOFF_MS", "USGS_MAX_RESPONSE_BYTES", "USGS_EVENT_TYPE"}


def verify_runtime():
    if not runtime.RECEIPT.is_file():
        raise ValueError("PREPARE_REQUIRED: make prepare-current-month")
    receipt = json.loads(runtime.RECEIPT.read_text())
    if (runtime.source_digest() != receipt["source_sha256"] or runtime.digest(runtime.JAR) != receipt["jar_sha256"]
            or runtime.digest(runtime.CP) != receipt["classpath_sha256"]
            or any(not Path(p).is_file() or runtime.digest(Path(p)) != h for p, h in receipt["dependencies"].items())):
        raise ValueError("STALE_ARTIFACT: make prepare-current-month")


def environment():
    # Only public request configuration comes from the example; never load example
    # MinIO credentials or execute shell/runner strings. Explicit environment wins.
    env = {}
    for line in (ROOT / ".env.example").read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep and key in USGS_KEYS:
            env[key] = value.strip().strip('"').strip("'")
    env.update(os.environ)
    env.update(SPARK_LOCAL_IP="127.0.0.1", SPARK_LOCAL_HOSTNAME="localhost")
    return env


def scoped(path):
    absolute = path.absolute()
    staging = (ROOT / "staging").resolve()
    if not absolute.resolve().is_relative_to(staging) or absolute.resolve() == staging:
        raise ValueError("SCOPED_STAGING_ROOT_REQUIRED")
    for ancestor in (absolute, *absolute.parents):
        if ancestor.is_symlink():
            raise ValueError("SYMLINK_ROOT_FORBIDDEN")
    return absolute


def read_receipt(root, request_path):
    request = json.loads(request_path.read_text())
    context = request["run_context"]
    operation = "cm1-" + hashlib.sha256((context["dag_id"] + "|" + context["run_id"]).encode()).hexdigest()[:40]
    state = scoped(root / "silver/current-month/state" / operation)
    receipt_path = state / "receipt.json"
    if receipt_path.is_symlink() or (state / "_SUCCESS").is_symlink() or not receipt_path.is_file() or receipt_path.stat().st_size > 65536:
        raise ValueError("COMPONENT_RECEIPT_MISSING")
    raw = receipt_path.read_bytes()
    if (state / "_SUCCESS").read_bytes() != hashlib.sha256(raw).hexdigest().encode():
        raise ValueError("COMPONENT_MARKER_CHANGED")
    receipt = json.loads(raw)
    if (receipt.get("contract_version") != "current-month-source-silver-v1"
            or receipt.get("status") != "ObservedAsOfReady" or receipt.get("operation_id") != operation
            or receipt.get("coverage_mode") != "OBSERVED_AS_OF" or receipt.get("publication_quality") != "PROVISIONAL"
            or any(receipt.get(k) is not False for k in ("published", "research_ready", "ml_upstream_eligible"))):
        raise ValueError("COMPONENT_RECEIPT_IDENTITY_CHANGED")
    return receipt, receipt_path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--root", type=Path, default=ROOT / "staging/current-month-component")
    args = parser.parse_args(argv)
    root = scoped(args.root)
    if args.request.is_symlink() or not args.request.is_file() or args.request.stat().st_size > 32768:
        raise ValueError("BOUNDED_REQUEST_REQUIRED")
    env = environment()
    java_home = env.get("JAVA_HOME", "")
    java = Path(java_home) / "bin/java"
    if not java_home or not java.is_file():
        raise ValueError("JAVA_HOME_REQUIRED: JDK17/21")
    verify_runtime()
    logs = scoped(ROOT / "staging/current-month-logs")
    logs.mkdir(parents=True, exist_ok=True)
    log = logs / (uuid.uuid4().hex + ".private.log")
    command = [str(java), "-Xmx2g", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
               "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED", "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
               "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
               "-cp", str(runtime.JAR) + os.pathsep + runtime.CP.read_text().strip(),
               "ie212.earthquake.spark.CurrentMonthSourceSilverJob", "--local", str(args.request.absolute()), str(root)]
    with os.fdopen(os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as output:
        result = subprocess.run(command, cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
    if result.returncode:
        raise ValueError("CURRENT_MONTH_FAILED: retain artifacts; inspect " + str(log))
    receipt, receipt_path = read_receipt(root, args.request)
    print("event=current_month_component_passed evidence_root=" + str(root) + " private_log=" + str(log))
    print("status=ObservedAsOfReady coverage_mode=OBSERVED_AS_OF published=false research_ready=false ml_upstream_eligible=false")
    print("receipt=" + str(receipt_path) + " reconciliation=" + json.dumps(receipt["reconciliation"], sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError) as error:
        sys.exit(str(error))
