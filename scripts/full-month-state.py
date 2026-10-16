#!/usr/bin/env python3
"""ORC-06 control-plane only: explicit preparation/run and bounded public raw captures; no ETL in Python."""
import argparse
import importlib.util
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("local_runtime", ROOT / "scripts/local-ml.py")
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)
USGS_URL = ("https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&eventtype=earthquake"
            "&starttime=2023-08-31T23%3A59%3A44Z&endtime=2023-10-01T00%3A00%3A15.999Z"
            "&minlatitude=20.0&maxlatitude=50.0&minlongitude=120.0&maxlongitude=155.0&orderby=time-asc&limit=20000&offset=1")
JMA_URL = "https://www.data.jma.go.jp/eqev/data/bulletin/data/hypo/h2023.zip"


def verify_runtime():
    import json
    if not runtime.RECEIPT.is_file(): raise ValueError("PREPARE_REQUIRED: make prepare-full-month")
    receipt = json.loads(runtime.RECEIPT.read_text())
    if (runtime.source_digest() != receipt["source_sha256"] or runtime.digest(runtime.JAR) != receipt["jar_sha256"]
            or runtime.digest(runtime.CP) != receipt["classpath_sha256"]
            or any(not Path(p).is_file() or runtime.digest(Path(p)) != h for p, h in receipt["dependencies"].items())):
        raise ValueError("STALE_ARTIFACT: make prepare-full-month")


def capture(url, path):
    if path.is_symlink(): raise ValueError("UNSAFE_CAPTURE_PATH")
    if path.exists(): return
    temporary = path.with_suffix(path.suffix + ".partial")
    # No source payload parsing/filtering: only bounded streaming download of original bytes.
    # curl uses the host trust store; never disable TLS verification for Python CA issues.
    with temporary.open("xb"):
        pass
    result = subprocess.run(["curl", "--fail", "--silent", "--show-error", "--location",
                             "--proto", "=https", "--proto-redir", "=https", "--max-time", "60",
                             "--max-filesize", str(32 * 1024 * 1024), "--retry", "2",
                             "--output", str(temporary), url], capture_output=True, check=False)
    if result.returncode:
        raise ValueError("PUBLIC_CAPTURE_FAILED curl_exit=" + str(result.returncode) + "; retain scoped .partial; no Docker change")
    temporary.rename(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["live", "local"])
    parser.add_argument("--request", type=Path)
    parser.add_argument("--root", type=Path, default=ROOT / "staging/orc06-full-month")
    args = parser.parse_args()
    task_root = args.root.absolute()
    if (not task_root.resolve().is_relative_to((ROOT / "staging").resolve())
            or task_root.resolve() == (ROOT / "staging").resolve() or task_root.is_symlink()):
        parser.error("root phải là thư mục scoped bên trong staging")
    java_home = os.environ.get("JAVA_HOME", "")
    java = Path(java_home) / "bin/java"
    if not java_home or not java.is_file(): parser.error("JAVA_HOME phải trỏ JDK17/21")
    verify_runtime()
    task_root.mkdir(parents=True, exist_ok=True)
    for child in ("captures", "qa", "bronze", "silver", "java-run.log"):
        if (task_root / child).is_symlink(): parser.error("Không dùng symlink cho artifact runtime")
    if args.action == "live":
        captures = task_root / "captures"; captures.mkdir(exist_ok=True)
        usgs, jma = captures / "usgs-2023-09.geojson", captures / "jma-2023.zip"
        capture(USGS_URL, usgs); capture(JMA_URL, jma)
        job = "ie212.earthquake.spark.usgs.FullMonthAcceptanceJob"
        parameters = [str(usgs), str(jma), str(task_root / "qa")]
    else:
        if not args.request: parser.error("local cần --request JSON")
        job = "ie212.earthquake.spark.FullMonthStateJob"
        parameters = ["--local", str(args.request.absolute()), str(task_root / "bronze"), str(task_root / "silver")]
    command = [str(java), "-Xmx2g", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
               "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED", "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
               "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
               "-cp", str(runtime.JAR) + os.pathsep + runtime.CP.read_text().strip(), job, *parameters]
    environment = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1", SPARK_LOCAL_HOSTNAME="localhost")
    log_path = task_root / "java-run.log"
    with log_path.open("ab") as output:
        result = subprocess.run(command, cwd=ROOT, env=environment, stdout=output, stderr=subprocess.STDOUT, check=False)
    if result.returncode: raise ValueError("MONTH_STATE_FAILED: inspect scoped java-run.log; no cleanup/Gold write")
    print("event=full_month_state_passed action=" + args.action + " evidence_root=" + str(task_root))


if __name__ == "__main__":
    try: main()
    except ValueError as error: sys.exit(str(error))
