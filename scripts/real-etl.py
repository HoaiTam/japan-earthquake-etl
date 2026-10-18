#!/usr/bin/env python3
"""Explicit ORC-07 preparation and bounded QA. QA never builds, starts or initializes services."""
import argparse
import importlib.util
import json
import os
import re
from pathlib import Path
import subprocess
import uuid

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("docker_lifecycle", ROOT / "scripts/docker-runtime.py")
lifecycle = importlib.util.module_from_spec(spec); spec.loader.exec_module(lifecycle)
IMAGE = "japan-earthquake-etl/real-etl:orc07-java17"


def run():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "qa", "verify", "recover"))
    parser.add_argument("--evidence", default="")
    parser.add_argument("--captures", type=Path, default=ROOT / "staging/orc06-full-month/captures")
    args = parser.parse_args(); env = dict(os.environ)
    compose = ["docker", "compose", "--env-file", str(ROOT / ".env"), "-f", str(ROOT / "compose.yaml"), "--profile", "real-etl"]
    fingerprint = lifecycle.source_sha(ROOT)
    folder = ROOT / "staging" / ("orc07-host-" + uuid.uuid4().hex); folder.mkdir(mode=0o700)
    log = folder / "private.log"
    with log.open("wb") as output:
        os.chmod(log, 0o600)
        if args.action == "prepare":
            result = subprocess.run(compose + ["build", "--build-arg", "PIPELINE_BUILD_SHA=" + fingerprint, "real-etl-qa"],
                                    cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
            if result.returncode or lifecycle.source_sha(ROOT) != fingerprint:
                raise RuntimeError("ORC07_PREPARE_FAILED inspect " + str(log.relative_to(ROOT)))
        else:
            captures = args.captures.resolve()
            if args.captures.is_symlink() or any(not (captures / name).is_file() or (captures / name).is_symlink()
                    or not 0 < (captures / name).stat().st_size < 32 * 1024 * 1024
                    for name in ("usgs-2023-09.geojson", "jma-2023.zip")):
                raise RuntimeError("ORC07_PUBLIC_CAPTURES_REQUIRED run ORC-06 QA first")
            # Make exports command-line variables. Compose treats a non-dot relative
            # bind source as a named volume, so normalize BEFORE even `compose ps`.
            env["ORC07_CAPTURE_ROOT"] = str(captures)
            image = subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True, check=False, timeout=20)
            if image.returncode or json.loads(image.stdout)[0].get("Config", {}).get("Labels", {}).get(lifecycle.BUILD_LABEL) != fingerprint:
                raise RuntimeError("ORC07_IMAGE_STALE make prepare-real-etl-runtime")
            healthy = subprocess.run(compose + ["ps", "--format", "{{.Service}}:{{.State}}:{{.Health}}"],
                                     capture_output=True, check=True, timeout=20, env=env)
            lines = healthy.stdout.decode().splitlines()
            if any(name + ":running:healthy" not in lines for name in ("minio", "airflow-postgres", "airflow-api-server", "iceberg-rest", "trino")):
                raise RuntimeError("ORC07_FOUNDATION_NOT_HEALTHY start services separately")
            extra = []
            if args.action in {"verify", "recover"}:
                if not re.fullmatch(r"[a-f0-9]{32}", args.evidence): raise RuntimeError("ORC07_EXACT_EVIDENCE_KEY_REQUIRED")
                name = "ORC07_VERIFY_EVIDENCE_KEY" if args.action == "verify" else "ORC07_RECOVER_EVIDENCE_KEY"
                extra = ["-e", name + "=" + args.evidence]
            result = subprocess.run(compose + ["run", "--rm", "--no-deps", "--pull", "never", *extra, "real-etl-qa"],
                                    cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT, check=False)
            if result.returncode: raise RuntimeError("ORC07_QA_FAILED_LEASE_MAY_BE_RETAINED inspect " + str(log.relative_to(ROOT)))
    print("event=orc07_" + args.action + "_passed log=" + str(log.relative_to(ROOT)))


if __name__ == "__main__":
    try: run()
    except subprocess.TimeoutExpired:
        raise SystemExit("ORC07_DOCKER_TIMEOUT no automatic build/start/cleanup") from None
    except subprocess.CalledProcessError:
        raise SystemExit("ORC07_DOCKER_PREFLIGHT_COMMAND_FAILED no automatic build/start/cleanup") from None
    except Exception as error:
        message = str(error)
        raise SystemExit(message if message.startswith("ORC07_") else "ORC07_CONTROL_FAILED no automatic cleanup") from None
