#!/usr/bin/env python3
"""Explicit Docker lifecycle and read-only smoke preflight (standard library only).

Docker diagnostics/config/inspect may contain secrets: never echo captured output.
Smoke checks do not build, pull, start, migrate, release leases or delete data.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
BUILD_LABEL = "com.japan-earthquake-etl.build-input-sha256"
BUILD_SPECS = {
    "minio": ("compose/minio", "compose/minio/Dockerfile", {
        "MINIO_VERSION": "RELEASE.2025-10-15T17-29-55Z", "MINIO_COMMIT": "9e49d5e7a648f00e26f2246f4dc28e6b07f8c84a"}),
    "minio-init": ("compose/minio-client", "compose/minio-client/Dockerfile", {
        "MC_VERSION": "RELEASE.2025-08-13T08-35-41Z", "MC_COMMIT": "7394ce0dd2a80935aded936b09fa12cbb3cb8096"}),
    "airflow-api-server": (".", "compose/airflow/Dockerfile", {}),
    "spark-master": (".", "compose/spark/Dockerfile", {}),
    "spark-gold-qa": (".", "compose/spark/Dockerfile.gold-qa", {}),
}
BUILD_SERVICES = tuple(BUILD_SPECS)
FOUNDATION = ("minio", "airflow-postgres", "airflow-api-server", "airflow-scheduler",
              "airflow-dag-processor", "spark-master", "spark-worker", "iceberg-rest", "trino")
AIRFLOW = ("minio", "airflow-postgres", "airflow-api-server", "airflow-scheduler", "airflow-dag-processor")
QUERY = ("minio", "iceberg-rest", "trino")
# (healthy services, completed init services, one-off images, needs mounted host JAR)
COMPONENTS = {
    "foundation": (FOUNDATION, ("minio-init", "airflow-init"), ("minio-smoke", "airflow-smoke", "spark-client", "trino-smoke"), False),
    "minio": (("minio",), ("minio-init",), ("minio-smoke",), False),
    "airflow": (AIRFLOW, ("minio-init", "airflow-init"), ("airflow-smoke",), False),
    "usgs": (AIRFLOW, ("minio-init", "airflow-init"), ("usgs-live-smoke",), False),
    "jma": (AIRFLOW, ("minio-init", "airflow-init"), ("jma-live-qa",), False),
    "source": (AIRFLOW, ("minio-init", "airflow-init"), ("source-readiness-qa",), False),
    "observability": (AIRFLOW, ("minio-init", "airflow-init"), (), False),
    "spark": (("spark-master", "spark-worker"), ("airflow-init",), ("spark-client",), False),
    "query": (QUERY, ("minio-init",), ("trino-smoke",), False),
    "silver": (("minio", "spark-master", "spark-worker"), ("minio-init", "airflow-init"), ("spark-silver-integration",), False),
    "resource": (("minio", "spark-master", "spark-worker"), ("minio-init", "airflow-init"), ("spark-resource-pilot",), False),
    "backfill": (("minio",), ("minio-init", "airflow-init"), ("airflow-api-server",), False),
    "gold": (QUERY, ("minio-init", "airflow-init"), ("spark-gold-qa",), True),
    "ml": (QUERY, ("minio-init", "airflow-init"), ("mainshock-window-qa",), True),
}
ARTIFACTS = ("spark/target/japan-earthquake-etl-runner.jar",
             "spark/target/iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar",
             "spark/target/iceberg/iceberg-aws-bundle-1.10.1.jar")


class RuntimeFailure(Exception):
    """Messages are controlled reason codes, never Docker stderr/env values."""


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(262144), b""):
            digest.update(block)
    return digest.hexdigest()


def source_sha(root=ROOT):
    files = [root / name for name in ("pom.xml", "spark/pom.xml", ".dockerignore", "compose.yaml",
             "mvnw", ".mvn/wrapper/maven-wrapper.properties",
             "compose/airflow/usgs-runner.sh", "compose/airflow/jma-runner.sh", "compose/airflow/bronze-reuse-runner.sh",
             "compose/minio/Dockerfile", "compose/minio/.dockerignore",
             "compose/minio-client/Dockerfile", "compose/minio-client/.dockerignore", "compose/airflow/Dockerfile",
             "compose/spark/Dockerfile", "compose/spark/Dockerfile.gold-qa")]
    for name in ("spark/src", "tests/fixtures", "config/jma"):
        if not (root / name).is_dir() or (root / name).is_symlink():
            raise RuntimeFailure("UNSAFE_OR_MISSING_BUILD_INPUT")
        for path in (root / name).rglob("*"):
            if path.is_symlink():
                raise RuntimeFailure("UNSAFE_OR_MISSING_BUILD_INPUT")
            if path.is_file():
                files.append(path)
    digest = hashlib.sha256()
    for path in sorted(files):
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(root.resolve()):
            raise RuntimeFailure("UNSAFE_OR_MISSING_BUILD_INPUT")
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(sha(path).encode() + b"\n")
    return digest.hexdigest()


def artifact_receipt(root=ROOT):
    path = root / "staging/docker-runtime/java-artifacts.json"
    if not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(root.resolve()) or path.stat().st_size > 65536:
        raise RuntimeFailure("JAVA_ARTIFACTS_UNPREPARED_RUN_MAKE_DOCKER_BUILD")
    receipt = json.loads(path.read_text())
    if receipt.get("source_sha256") != source_sha(root):
        raise RuntimeFailure("JAVA_ARTIFACTS_STALE_RUN_MAKE_DOCKER_BUILD")
    for name in ARTIFACTS:
        file = root / name
        if not file.is_file() or file.is_symlink() or not file.resolve().is_relative_to(root.resolve()) or receipt.get("files", {}).get(name) != sha(file):
            raise RuntimeFailure("JAVA_ARTIFACTS_CHANGED_RUN_MAKE_DOCKER_BUILD")
    return receipt


def package(root=ROOT):
    before = source_sha(root)
    result = subprocess.run([str(root / "mvnw"), "--batch-mode", "--no-transfer-progress", "clean", "verify"], cwd=root)
    if result.returncode:
        raise RuntimeFailure("MAVEN_VERIFY_FAILED_NO_NEW_ARTIFACT_RECEIPT")
    if source_sha(root) != before:
        raise RuntimeFailure("BUILD_INPUT_CHANGED_DURING_PACKAGE")
    record_artifacts(root, before)
    print("Java artifacts verified and fingerprinted; no Docker services changed.")


def record_artifacts(root, fingerprint):
    if source_sha(root) != fingerprint:
        raise RuntimeFailure("BUILD_INPUT_CHANGED_BEFORE_ARTIFACT_RECEIPT")
    receipt = {"source_sha256": fingerprint, "files": {name: sha(root / name) for name in ARTIFACTS}}
    directory = root / "staging/docker-runtime"
    if not directory.resolve().is_relative_to(root.resolve()):
        raise RuntimeFailure("UNSAFE_ARTIFACT_RECEIPT_PATH")
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=directory, delete=False) as output:
        json.dump(receipt, output, sort_keys=True)
        temporary = Path(output.name)
    temporary.replace(directory / "java-artifacts.json")


class Runtime:
    def __init__(self, root=ROOT, environment=None, timeout=20):
        self.root = root
        self.env = dict(os.environ if environment is None else environment)
        self.deadline = time.monotonic() + timeout
        self.compose = ["docker", "compose", "--env-file", self.env.get("ENV_FILE", str(root / ".env")),
                        "-f", self.env.get("COMPOSE_FILE", str(root / "compose.yaml")), "--profile", "*"]
        self.config = self.json_call(self.compose + ["config", "--format", "json"])
        self.services = self.config["services"]
        self.validate_build_contexts()
        lines = self.call(self.compose + ["config", "--hash", "*"]).splitlines()
        self.hashes = dict(line.split() for line in lines if line.strip())
        self.fingerprint = source_sha(root)

    def validate_build_contexts(self):
        # A custom Compose file must not stamp another checkout's code as this source.
        if self.services["minio-smoke"]["image"] != self.services["minio-init"]["image"]:
            raise RuntimeFailure("UNSUPPORTED_MINIO_CLIENT_IMAGE_MUST_MATCH_INIT")
        for service in (*BUILD_SERVICES, "minio-smoke"):
            directory, dockerfile, expected_args = BUILD_SPECS["minio-init" if service == "minio-smoke" else service]
            build = self.services[service].get("build", {})
            context = Path(build.get("context", ""))
            context = context if context.is_absolute() else self.root / context
            expected = self.root / directory
            actual_file = Path(build.get("dockerfile", "Dockerfile"))
            if not actual_file.is_absolute():
                actual_file = context / actual_file
            if context.resolve() != expected.resolve() or actual_file.resolve() != (self.root / dockerfile).resolve() or build.get("dockerfile_inline") or build.get("target") or build.get("additional_contexts"):
                raise RuntimeFailure("UNSUPPORTED_BUILD_CONTEXT_CANNOT_VERIFY_SOURCE: " + service)
            args = {key: value for key, value in build.get("args", {}).items() if key != "PIPELINE_BUILD_SHA"}
            if args != expected_args:
                raise RuntimeFailure("UNSUPPORTED_BUILD_ARGUMENTS_CANNOT_VERIFY_SOURCE: " + service)

    def call(self, command):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeFailure("DOCKER_COMMAND_BUDGET_EXHAUSTED")
        try:
            result = subprocess.run(command, cwd=self.root, env=self.env, capture_output=True, text=True, timeout=remaining)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeFailure("DOCKER_COMMAND_UNAVAILABLE_OR_TIMEOUT") from None
        if result.returncode:
            raise RuntimeFailure("DOCKER_COMMAND_FAILED_CHECK_DOCKER_AND_CONFIG")
        return result.stdout

    def json_call(self, command):
        return json.loads(self.call(command))

    def images(self, services):
        names = sorted({self.services[name]["image"] for name in services})
        try:
            found = self.json_call(["docker", "image", "inspect", *names])
        except RuntimeFailure as error:
            if str(error) == "DOCKER_COMMAND_FAILED_CHECK_DOCKER_AND_CONFIG":
                raise RuntimeFailure("IMAGE_MISSING_RUN_MAKE_DOCKER_BUILD") from None
            raise
        # Docker returns one inspect result per requested reference, in argument order.
        # RepoDigests omit tags, so matching `repo:tag@sha256` against them is wrong.
        if len(found) != len(names):
            raise RuntimeFailure("IMAGE_INSPECT_RESULT_CONFLICT")
        by_reference = dict(zip(names, found))
        result = {}
        built_names = {self.services[name]["image"] for name in BUILD_SERVICES}
        for service in services:
            image = self.services[service]["image"]
            item = by_reference.get(image)
            if item is None:
                raise RuntimeFailure("IMAGE_MISSING_RUN_MAKE_DOCKER_BUILD")
            if image in built_names and (item.get("Config", {}).get("Labels") or {}).get(BUILD_LABEL) != self.fingerprint:
                raise RuntimeFailure("IMAGE_UNPREPARED_OR_STALE_RUN_MAKE_DOCKER_BUILD")
            result[service] = item["Id"]
        return result

    def containers(self, services):
        ids = self.call(self.compose + ["ps", "-a", "-q", *services]).split()
        if not ids:
            return {}
        result = {}
        for item in self.json_call(["docker", "inspect", *ids]):
            labels = item.get("Config", {}).get("Labels", {})
            service = labels.get("com.docker.compose.service")
            if service not in services or labels.get("com.docker.compose.project") != self.config["name"] or labels.get("com.docker.compose.oneoff", "false").lower() != "false":
                continue
            if service in result:
                raise RuntimeFailure("MULTIPLE_SERVICE_CONTAINERS_NOT_SUPPORTED")
            result[service] = item
        return result

    def current(self, service, item, image):
        labels = item.get("Config", {}).get("Labels", {})
        return bool(self.hashes.get(service)) and labels.get("com.docker.compose.config-hash") == self.hashes[service] and item.get("Image") == image

    def check(self, component, runner_jar=None, client=None):
        healthy, inits, clients, mounted = COMPONENTS[component]
        if client:
            if client not in ("spark-gold-qa", "gold-verification-qa", "dataset-manifest-qa", "gold-input-audit-qa", "mainshock-window-qa") or component not in ("gold", "ml"):
                raise RuntimeFailure("UNSUPPORTED_QA_CLIENT")
            clients = (client,)
        required = tuple(dict.fromkeys((*healthy, *inits, *clients)))
        images = self.images(required)
        containers = self.containers((*healthy, *inits))
        for service in (*healthy, *inits):
            item = containers.get(service)
            if item is None or not self.current(service, item, images[service]):
                raise RuntimeFailure("SERVICE_MISSING_OR_OUTDATED_RUN_MAKE_DOCKER_UP: " + service)
            state = item.get("State", {})
            if service in inits:
                ready = state.get("Status") == "exited" and state.get("ExitCode") == 0
            else:
                ready = state.get("Status") == "running" and not state.get("Paused") and not state.get("Restarting") and state.get("Health", {}).get("Status") == "healthy"
            if not ready:
                raise RuntimeFailure("SERVICE_NOT_READY_CHECK_LOGS: " + service)
        if mounted or runner_jar:
            receipt = artifact_receipt(self.root)
            if runner_jar and sha(Path(runner_jar)) != receipt["files"][ARTIFACTS[0]]:
                raise RuntimeFailure("CUSTOM_RUNNER_MUST_MATCH_VERIFIED_PACKAGE")
        if mounted:
            for name in clients:
                binds = {item["target"]: item for item in self.services[name].get("volumes", []) if item.get("type") == "bind"}
                for target, source in (("/opt/spark/jobs/japan-earthquake-etl-runner.jar", ARTIFACTS[0]), ("/opt/spark/jobs/iceberg", "spark/target/iceberg")):
                    entry = binds.get(target, {})
                    if not entry.get("read_only") or Path(entry.get("source", "")).resolve() != (self.root / source).resolve():
                        raise RuntimeFailure("QA_ARTIFACT_MOUNT_NOT_VERIFIED: " + name)
        print("Docker preflight passed: " + component + "; no build/pull/start/init performed.")

    def mutate(self, arguments):
        if self.deadline <= time.monotonic():
            raise RuntimeFailure("DOCKER_START_BUDGET_EXHAUSTED_NO_AUTOMATIC_CLEANUP")
        remaining = max(1, int(self.deadline - time.monotonic()))
        try:
            result = subprocess.run(self.compose + arguments, cwd=self.root, env=self.env, timeout=remaining)
        except (OSError, subprocess.TimeoutExpired):
            raise RuntimeFailure("DOCKER_START_TIMEOUT_CHECK_STATE_NO_AUTOMATIC_CLEANUP") from None
        if result.returncode:
            raise RuntimeFailure("DOCKER_START_FAILED_CHECK_STATE_NO_AUTOMATIC_CLEANUP")

    def start_group(self, services):
        if not services:
            return
        remaining = max(1, int(self.deadline - time.monotonic()))
        self.mutate(["up", "-d", "--no-deps", "--no-build", "--pull", "never", "--wait", "--wait-timeout", str(remaining), *services])

    def up(self, component):
        healthy, inits, _, _ = COMPONENTS[component]
        # Staging ownership needs Airflow init/Postgres for Spark/Gold as well.
        healthy = tuple(dict.fromkeys((*healthy, *( ("airflow-postgres",) if "airflow-init" in inits else ()))))
        images = self.images((*healthy, *inits))
        existing = self.containers((*healthy, *inits))
        def ready(service):
            item = existing.get(service, {})
            state = item.get("State", {})
            return self.current(service, item, images[service]) and state.get("Status") == "running" and not state.get("Paused") and not state.get("Restarting") and state.get("Health", {}).get("Status") == "healthy"
        # Never run Compose up on an already ready group: exited init must not rerun.
        self.start_group([name for name in ("minio", "airflow-postgres") if name in healthy and not ready(name)])
        for service in inits:
            item = self.containers((service,)).get(service, {})
            current = self.current(service, item, images[service])
            state = item.get("State", {})
            if current and state.get("Status") == "exited" and state.get("ExitCode") == 0:
                print("Reuse completed init: " + service)
                continue
            if state.get("Status") in ("running", "restarting"):
                if not current:
                    raise RuntimeFailure("INIT_IN_PROGRESS_WITH_OLD_CONFIGURATION: " + service)
            else:
                self.mutate(["up", "-d", "--no-deps", "--no-build", "--pull", "never", service])
            previous = None
            while True:
                item = self.containers((service,)).get(service, {})
                status = item.get("State", {}).get("Status")
                if status != previous:
                    print("Init " + service + ": " + str(status), flush=True)
                    previous = status
                if status == "exited":
                    if item["State"].get("ExitCode") != 0 or not self.current(service, item, images[service]):
                        raise RuntimeFailure("INIT_FAILED_CHECK_LOGS: " + service)
                    break
                time.sleep(min(1, max(0, self.deadline - time.monotonic())))
        # Containers may have changed while init ran; inspect again before starting consumers.
        existing = self.containers(healthy)
        self.start_group([name for name in healthy if name not in ("minio", "airflow-postgres") and not ready(name)])
        self.check(component)


def build(environment=None):
    runtime = Runtime(environment=environment, timeout=60)
    before = runtime.fingerprint
    # Build is explicit and may take minutes. Nothing is started or initialized.
    command = runtime.compose + ["build", "--build-arg", "PIPELINE_BUILD_SHA=" + before, *BUILD_SERVICES]
    if subprocess.run(command, cwd=runtime.root, env=runtime.env).returncode:
        raise RuntimeFailure("DOCKER_BUILD_FAILED")
    if source_sha(runtime.root) != before:
        raise RuntimeFailure("BUILD_INPUT_CHANGED_DURING_DOCKER_BUILD")
    runtime.deadline = time.monotonic() + 60
    runtime.images(BUILD_SERVICES)
    export_artifacts(runtime)
    if subprocess.run(runtime.compose + ["pull", "airflow-postgres", "airflow-init", "iceberg-rest", "trino"], cwd=runtime.root, env=runtime.env).returncode:
        raise RuntimeFailure("DEPENDENCY_IMAGE_PULL_FAILED")
    print("Docker images prepared; run make docker-up. No services started.")


def export_artifacts(runtime):
    """Copy verified Java17 build outputs from a stopped, network-isolated container.

    No host JDK/Maven is needed by docker-build; the builders already ran verify.
    Remove only our newly-created export container, never a service or volume.
    """
    directory = runtime.root / "staging/docker-runtime"
    if not directory.resolve().is_relative_to(runtime.root.resolve()):
        raise RuntimeFailure("UNSAFE_ARTIFACT_EXPORT_PATH")
    directory.mkdir(parents=True, exist_ok=True)
    image = runtime.services["spark-master"]["image"]
    name = "ie212-artifact-export-" + uuid.uuid4().hex
    container = runtime.call(["docker", "create", "--name", name, "--network", "none", "--entrypoint", "/bin/true", image]).strip()
    if not re.fullmatch(r"[a-f0-9]{64}", container):
        raise RuntimeFailure("INVALID_ARTIFACT_EXPORT_CONTAINER_ID")
    try:
        with tempfile.TemporaryDirectory(prefix="export-", dir=directory) as output:
            root = Path(output)
            runtime.call(["docker", "cp", container + ":/opt/spark/jobs/.", str(root)])
            # Final destinations stay inside this checkout, not a user-provided broad root.
            for relative in (*ARTIFACTS, "spark/target/japan-earthquake-etl.jar"):
                suffix = relative.removeprefix("spark/target/")
                source, target = root / suffix, runtime.root / relative
                if not source.is_file() or source.is_symlink() or target.is_symlink() or not target.resolve().is_relative_to(runtime.root.resolve()):
                    raise RuntimeFailure("UNSAFE_OR_MISSING_EXPORTED_ARTIFACT")
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
            record_artifacts(runtime.root, runtime.fingerprint)
    finally:
        # A failed/timed-out export may leave this exact stopped container for diagnosis.
        runtime.deadline = time.monotonic() + 10
        runtime.call(["docker", "rm", container])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check", "up", "build", "package"))
    parser.add_argument("--component", choices=COMPONENTS, default="foundation")
    parser.add_argument("--runner-jar", type=Path)
    parser.add_argument("--client")
    args = parser.parse_args()
    try:
        if args.action == "package":
            package()
        elif args.action == "build":
            build()
        else:
            variable = "WAIT_TIMEOUT" if args.action == "up" else "DOCKER_CHECK_TIMEOUT"
            seconds = int(os.environ.get(variable, "300" if args.action == "up" else "20"))
            if not 1 <= seconds <= 3600:
                raise RuntimeFailure("INVALID_DOCKER_TIMEOUT")
            runtime = Runtime(timeout=seconds)
            if args.action == "up":
                runtime.up(args.component)
            else:
                runtime.check(args.component, args.runner_jar, args.client)
    except RuntimeFailure as error:
        print("Docker runtime: " + str(error), flush=True)
        raise SystemExit(1) from None
    except Exception:
        print("Docker runtime failed; diagnostic values redacted. Check config/artifacts and Docker Desktop.", flush=True)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
