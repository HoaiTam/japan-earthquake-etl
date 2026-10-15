#!/usr/bin/env python3
"""Explicit Java/Spark local runtime. No Docker start/build, lake write or implicit compilation."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "spark/target"
JAR = TARGET / "japan-earthquake-etl.jar"
CP = TARGET / "local-feature-classpath.txt"
RECEIPT = TARGET / "local-ml-prepared.json"


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_digest():
    paths = [ROOT / "pom.xml", ROOT / "spark/pom.xml"] + sorted((ROOT / "spark/src/main").rglob("*"))
    records = [(str(p.relative_to(ROOT)), digest(p)) for p in paths if p.is_file()]
    return hashlib.sha256(json.dumps(records, separators=(",", ":")).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["prepare", "features", "pilot"])
    parser.add_argument("--input", type=Path, default=ROOT / "staging/ml-window-inputs/reproduction-v1")
    parser.add_argument("--scaling", type=Path, default=ROOT / "config/ml/feature_scaling_global_dev_v1.json")
    parser.add_argument("--selection", type=Path, default=ROOT / "config/ml/dat02_reproduction_v1.json")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--trino", default="http://127.0.0.1:8081")
    args = parser.parse_args()
    java_home = os.environ.get("JAVA_HOME")
    if not java_home or not (Path(java_home) / "bin/java").is_file():
        parser.error("JAVA_HOME phải trỏ tới JDK 17 hoặc 21; không tự chọn Java hệ thống")
    env = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1", SPARK_LOCAL_HOSTNAME="localhost")
    if args.action == "prepare":
        subprocess.run([str(ROOT / "mvnw"), "--batch-mode", "--no-transfer-progress", "-pl", "spark", "-am",
                        "-DskipTests", "package", "org.apache.maven.plugins:maven-dependency-plugin:3.9.0:build-classpath",
                        "-Dmdep.includeScope=test", "-Dmdep.outputFile=target/local-feature-classpath.txt"], cwd=ROOT, env=env, check=True)
        dependencies = [Path(p) for p in CP.read_text().strip().split(os.pathsep)]
        receipt = {"source_sha256": source_digest(), "jar_sha256": digest(JAR), "classpath_sha256": digest(CP),
                   "dependencies": {str(p): digest(p) for p in dependencies}}
        RECEIPT.write_text(json.dumps(receipt, sort_keys=True) + "\n")
        print("event=local_ml_prepared jar_sha256=" + digest(JAR))
        return
    if not RECEIPT.is_file():
        parser.error("Chưa prepare; chạy make prepare-ml-local một lần")
    receipt = json.loads(RECEIPT.read_text())
    if (source_digest() != receipt["source_sha256"] or digest(JAR) != receipt["jar_sha256"]
            or digest(CP) != receipt["classpath_sha256"]
            or any(not Path(p).is_file() or digest(Path(p)) != h for p, h in receipt["dependencies"].items())):
        parser.error("Artifact/classpath cũ hoặc thay đổi; chạy lại make prepare-ml-local, không sửa receipt")
    native_sha = json.loads((args.input / "source-input.json").read_text())["native_report_sha256"]
    job = "LocalFeatureJob" if args.action == "features" else "PilotExportJob"
    output = args.output or ROOT / ("staging/ml-features" if args.action == "features" else "staging/ml-pilot")
    command = [str(Path(java_home) / "bin/java"), "-Xmx2g", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
               "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED", "--add-exports=java.base/sun.security.action=ALL-UNNAMED",
               "--add-opens=java.base/java.nio=ALL-UNNAMED", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
               "-cp", str(JAR) + os.pathsep + CP.read_text().strip(), "ie212.earthquake.spark.ml." + job,
               str(args.input), native_sha, str(args.scaling), str(output), args.trino]
    if args.action == "pilot":
        command.append(str(args.selection))
    result = subprocess.run(command, cwd=ROOT, env=env, check=False)
    if result.returncode:
        raise SystemExit("LOCAL_ML_JOB_FAILED: xem mã lỗi Java phía trên; không tự build/start/cleanup hoặc overwrite bundle")


if __name__ == "__main__":
    main()
