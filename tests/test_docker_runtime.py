"""Offline lifecycle regression: no real Docker, source API or lake writes."""
import contextlib
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("docker_runtime", REPO / "scripts/docker-runtime.py")
runtime = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runtime)


class DockerDouble:
    def __init__(self, root):
        self.commands = []
        self.config = {"name": "isolated-test", "services": {}}
        self.images = {}
        self.containers = {}
        fingerprint = runtime.source_sha(root)
        names = set(runtime.FOUNDATION) | {"minio-init", "airflow-init"}
        for _, _, clients, _ in runtime.COMPONENTS.values():
            names.update(clients)
        names.update(runtime.BUILD_SERVICES)
        qa_clients = ("spark-gold-qa", "gold-verification-qa", "dataset-manifest-qa", "gold-input-audit-qa", "mainshock-window-qa", "mli-03-import-results", "qa-02-rerun-revision", "qa-03-jma-historical-backfill", "qa-04-failure-recovery")
        names.update(qa_clients)
        for name in sorted(names):
            if name.startswith("airflow-") and name not in ("airflow-postgres", "airflow-init") or name in ("usgs-live-smoke", "jma-live-qa", "source-readiness-qa"):
                image = "local-airflow:verified"
            elif name.startswith("spark-") and name != "spark-gold-qa" or name in ("mainshock-window-qa", "gold-input-audit-qa"):
                image = "local-spark:verified"
            elif name == "minio":
                image = "local-minio:verified"
            elif name in ("minio-init", "minio-smoke"):
                image = "local-mc:verified"
            elif name in ("spark-gold-qa", "gold-verification-qa", "dataset-manifest-qa", "mli-03-import-results", "qa-03-jma-historical-backfill", "qa-04-failure-recovery"):
                image = "local-gold:verified"
            elif name == "trino-smoke":
                image = "trino:test@sha256:" + "a" * 64
            else:
                image = name + ":test"
            self.config["services"][name] = {"image": image}
            if name in qa_clients:
                self.config["services"][name]["volumes"] = [
                    {"type": "bind", "target": "/opt/spark/jobs/japan-earthquake-etl-runner.jar", "source": str(root / runtime.ARTIFACTS[0]), "read_only": True},
                    {"type": "bind", "target": "/opt/spark/jobs/iceberg", "source": str(root / "spark/target/iceberg"), "read_only": True}]
            self.images.setdefault(image, {"Id": "sha256:" + image, "RepoTags": [image],
                "RepoDigests": [], "Config": {"Labels": {runtime.BUILD_LABEL: fingerprint}}})
        self.hashes = {name: "hash-" + name for name in names}
        for name in (*runtime.BUILD_SERVICES, "minio-smoke"):
            directory, file, args = runtime.BUILD_SPECS["minio-init" if name == "minio-smoke" else name]
            self.config["services"][name]["build"] = {"context": str(root / directory), "dockerfile": str(root / file), "args": dict(args)}
        for name in (*runtime.FOUNDATION, "minio-init", "airflow-init"):
            self.ready(name)

    def ready(self, name):
        init = name.endswith("-init")
        self.containers[name] = {"Id": name, "Image": self.images[self.config["services"][name]["image"]]["Id"],
            "Config": {"Labels": {"com.docker.compose.project": self.config["name"],
                "com.docker.compose.service": name, "com.docker.compose.oneoff": "False",
                "com.docker.compose.config-hash": self.hashes[name]}},
            "State": {"Status": "exited" if init else "running", "ExitCode": 0,
                      "Health": {"Status": "healthy"}}}

    def run(self, command, **kwargs):
        self.commands.append(command)
        if command[0] != "docker":
            raise AssertionError("Only controlled Docker commands expected")
        if command[1:3] == ["image", "inspect"]:
            if any(name not in self.images for name in command[3:]):
                return subprocess.CompletedProcess(command, 1, "", "Error: No such image: DO_NOT_PRINT_CREDENTIAL")
            output = json.dumps(list({name: self.images[name] for name in command[3:]}.values()))
        elif command[1] == "inspect":
            output = json.dumps([self.containers[name] for name in command[2:]])
        elif command[1] == "create":
            self.export_container = "d" * 64
            output = self.export_container
        elif command[1] == "cp":
            if command[2] != self.export_container + ":/opt/spark/jobs/.":
                raise AssertionError("Only own export container may be copied")
            root = Path(command[3])
            for name in (*runtime.ARTIFACTS, "spark/target/japan-earthquake-etl.jar"):
                path = root / name.removeprefix("spark/target/")
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"verified-docker-java17-artifact")
            output = ""
        elif command[1] == "rm":
            if command[2:] != [self.export_container]:
                raise AssertionError("Only exact newly-created export container may be removed")
            output = ""
        elif "config" in command:
            output = "\n".join(name + " " + value for name, value in self.hashes.items()) if "--hash" in command else json.dumps(self.config)
        elif "ps" in command:
            selected = command[command.index("-q") + 1:]
            output = "\n".join(name for name in selected if name in self.containers)
        elif "up" in command:
            for name in command[command.index("up") + 1:]:
                if name in self.config["services"]:
                    self.ready(name)
            output = ""
        elif "build" in command or "pull" in command:
            output = ""
        else:
            raise AssertionError("Unexpected Docker operation")
        return subprocess.CompletedProcess(command, 0, output, "")


class DockerRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="docker runtime with spaces ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ("pom.xml", "spark/pom.xml", ".dockerignore", "compose.yaml", "mvnw", ".mvn/wrapper/maven-wrapper.properties",
                     "compose/airflow/usgs-runner.sh", "compose/airflow/jma-runner.sh", "compose/airflow/bronze-reuse-runner.sh", "compose/airflow/mli-import-runner.sh", "compose/minio/Dockerfile",
                     "compose/minio/.dockerignore", "compose/minio-client/Dockerfile", "compose/minio-client/.dockerignore",
                     "compose/airflow/Dockerfile", "compose/spark/Dockerfile", "compose/spark/Dockerfile.gold-qa",
                     "spark/src/main/java/Example.java", "tests/fixtures/example.json", "config/jma/inventory.csv"):
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("fixture " + name)
        self.docker = DockerDouble(self.root)
        self.mock = patch.object(runtime.subprocess, "run", side_effect=self.docker.run)
        self.mock.start()
        self.addCleanup(self.mock.stop)
        self.env = {"ENV_FILE": str(self.root / ".env private"), "COMPOSE_FILE": str(self.root / "compose custom.yaml")}

    def context(self, timeout=20):
        return runtime.Runtime(self.root, self.env, timeout)

    def artifacts(self):
        receipt = {"source_sha256": runtime.source_sha(self.root), "files": {}}
        for name in runtime.ARTIFACTS:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"fixture-artifact")
            receipt["files"][name] = runtime.sha(path)
        path = self.root / "staging/docker-runtime/java-artifacts.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(receipt))
        return path

    def assert_read_only(self):
        for command in self.docker.commands:
            self.assertFalse(any(word in command for word in ("up", "build", "pull", "restart", "stop", "rm", "down", "run", "exec")), command)

    def test_all_component_preflights_are_read_only_and_support_custom_paths(self):
        self.artifacts()
        for component in runtime.COMPONENTS:
            with self.subTest(component=component):
                self.context().check(component)
        self.assert_read_only()
        self.assertIn(str(self.root / ".env private"), self.docker.commands[0])
        self.assertIn(str(self.root / "compose custom.yaml"), self.docker.commands[0])

    def test_mli_import_client_is_preflighted_with_verified_runtime_mounts(self):
        self.artifacts()
        self.context().check("ml", client="mli-03-import-results")
        self.assert_read_only()

    def test_mli_smokes_run_the_same_preflighted_service_without_build_or_start(self):
        makefile = (REPO / "Makefile").read_text(encoding="utf-8")
        for name in ("smoke-mli-import", "smoke-mli-import-acceptance"):
            target = re.search(r"(?ms)^" + name + r":.*?(?=^[A-Za-z0-9_.-]+:|\Z)", makefile).group()
            self.assertIn("--client mli-03-import-results", target)
            self.assertIn("--no-deps --pull never", target)
            self.assertIn("mli-03-import-results", target)
            self.assertNotIn("airflow-dag-processor python", target)
            self.assertNotRegex(target, r"--build|docker-up|docker-build")

    def test_mli_dag_acceptance_preflights_current_airflow_and_never_clears_runs(self):
        makefile = (REPO / "Makefile").read_text(encoding="utf-8")
        target = re.search(r"(?ms)^smoke-mli-import-dag:.*?(?=^[A-Za-z0-9_.-]+:|\Z)", makefile).group()
        self.assertIn("--component airflow", target)
        self.assertIn("--client mli-03-import-results", target)
        self.assertIn("exec -T airflow-dag-processor", target)
        self.assertNotRegex(target, r"--build|docker-up|docker-build|dags clear|tasks clear|--mark-success")

    def test_qa02_client_is_preflighted_with_verified_runtime_mounts(self):
        self.artifacts()
        self.context().check("ml", client="qa-02-rerun-revision")
        self.assert_read_only()

    def test_qa03_client_is_preflighted_with_gold_runtime_dependencies(self):
        self.artifacts()
        self.context().check("gold", client="qa-03-jma-historical-backfill")
        self.assert_read_only()

    def test_qa04_client_is_preflighted_with_ml_runtime_dependencies(self):
        self.artifacts()
        self.context().check("ml", client="qa-04-failure-recovery")
        self.assert_read_only()

    def test_qa04_smoke_target_uses_supported_client_and_qa_runner_service(self):
        makefile = (REPO / "Makefile").read_text(encoding="utf-8")
        target = re.search(r"(?ms)^smoke-qa-04:.*?(?=^[A-Za-z0-9_.-]+:|\Z)", makefile)
        self.assertIsNotNone(target)
        self.assertIn("--component ml --client qa-04-failure-recovery", target.group(0))
        self.assertIn("qa-04-failure-recovery", target.group(0))
        self.assertNotIn("airflow-smoke", target.group(0))

    def test_usgs_does_not_require_unrelated_spark_or_trino(self):
        for name in ("spark-master", "spark-worker", "trino", "iceberg-rest"):
            self.docker.containers.pop(name)
        self.context().check("usgs")
        self.assert_read_only()

    def test_missing_unhealthy_stopped_or_changed_service_fails_without_starting(self):
        original = copy.deepcopy(self.docker.containers["airflow-scheduler"])
        mutations = (None, {"State": {"Status": "running", "Health": {"Status": "starting"}}},
                     {"State": {"Status": "running", "Paused": True, "Health": {"Status": "healthy"}}},
                     {"State": {"Status": "running", "Restarting": True, "Health": {"Status": "healthy"}}},
                     {"State": {"Status": "exited", "ExitCode": 0}}, {"Image": "old-image"},
                     {"Config": {"Labels": {**original["Config"]["Labels"], "com.docker.compose.config-hash": "old-config"}}})
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                if mutation is None:
                    self.docker.containers.pop("airflow-scheduler", None)
                else:
                    self.docker.containers["airflow-scheduler"] = {**copy.deepcopy(original), **mutation}
                with self.assertRaises(runtime.RuntimeFailure):
                    self.context().check("usgs")
        self.assert_read_only()

    def test_init_must_be_exited_zero_and_match_current_config(self):
        for status, code in (("running", 0), ("exited", 1), ("created", 0)):
            self.docker.containers["minio-init"]["State"] = {"Status": status, "ExitCode": code}
            with self.assertRaisesRegex(runtime.RuntimeFailure, "SERVICE_NOT_READY"):
                self.context().check("usgs")
        self.docker.ready("minio-init")
        self.docker.containers["minio-init"]["Config"]["Labels"]["com.docker.compose.config-hash"] = "old-config"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "OUTDATED"):
            self.context().check("usgs")
        self.assert_read_only()

    def test_missing_image_and_missing_or_stale_build_label_fail(self):
        image = "local-airflow:verified"
        original = copy.deepcopy(self.docker.images[image])
        self.docker.images.pop(image)
        with self.assertRaisesRegex(runtime.RuntimeFailure, "IMAGE_MISSING"):
            self.context().check("usgs")
        for label in (None, {}, {runtime.BUILD_LABEL: "old-source"}):
            self.docker.images[image] = {**original, "Config": {"Labels": label}}
            with self.assertRaisesRegex(runtime.RuntimeFailure, "IMAGE_UNPREPARED_OR_STALE"):
                self.context().check("usgs")
        self.assert_read_only()

    def test_digest_pinned_image_with_no_tags_is_supported(self):
        image = "trino:test@sha256:" + "a" * 64
        self.docker.images[image]["RepoTags"] = None
        self.docker.images[image]["RepoDigests"] = ["trino@sha256:" + "a" * 64]
        self.context().check("query")

    def test_ml_requires_current_verified_host_artifacts(self):
        with self.assertRaisesRegex(runtime.RuntimeFailure, "UNPREPARED"):
            self.context().check("ml")
        receipt = self.artifacts()
        self.context().check("ml")
        (self.root / runtime.ARTIFACTS[0]).write_bytes(b"changed-artifact")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "ARTIFACTS_CHANGED"):
            self.context().check("ml")
        self.artifacts()
        data = json.loads(receipt.read_text()); data["source_sha256"] = "old-source"
        receipt.write_text(json.dumps(data))
        with self.assertRaisesRegex(runtime.RuntimeFailure, "ARTIFACTS_STALE"):
            self.context().check("ml")
        self.assert_read_only()

    def test_custom_silver_runner_must_match_verified_package(self):
        self.artifacts()
        custom = self.root / "custom-runner.jar"
        custom.write_bytes(b"fixture-artifact")
        self.context().check("silver", custom)
        custom.write_bytes(b"other-source-build")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "CUSTOM_RUNNER"):
            self.context().check("silver", custom)

    def test_custom_compose_cannot_certify_a_different_build_context(self):
        self.docker.config["services"]["spark-master"]["build"]["context"] = str(self.root / "another-checkout")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "UNSUPPORTED_BUILD_CONTEXT"):
            self.context()
        self.assert_read_only()

    def test_actual_qa_client_and_mounts_are_checked_not_just_shared_image(self):
        self.artifacts()
        self.context().check("ml", client="gold-input-audit-qa")
        self.docker.config["services"]["gold-input-audit-qa"]["volumes"][0]["source"] = str(self.root / "old-runner.jar")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "MOUNT_NOT_VERIFIED"):
            self.context().check("ml", client="gold-input-audit-qa")
        self.assert_read_only()

    def test_custom_build_args_cannot_change_reviewed_minio_source(self):
        self.docker.config["services"]["minio"]["build"]["args"]["MINIO_COMMIT"] = "another-source"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "UNSUPPORTED_BUILD_ARGUMENTS"):
            self.context()
        self.assert_read_only()

    def test_mc_source_pins_and_context_cannot_be_overridden(self):
        for service in ("minio-init", "minio-smoke"):
            original = copy.deepcopy(self.docker.config["services"][service]["build"])
            for field in ("MC_COMMIT", "MC_VERSION"):
                self.docker.config["services"][service]["build"]["args"][field] = "unreviewed"
                with self.assertRaisesRegex(runtime.RuntimeFailure, "UNSUPPORTED_BUILD_ARGUMENTS"):
                    self.context()
                self.docker.config["services"][service]["build"] = copy.deepcopy(original)
            self.docker.config["services"][service]["build"]["context"] = str(self.root / "another-client")
            with self.assertRaisesRegex(runtime.RuntimeFailure, "UNSUPPORTED_BUILD_CONTEXT"):
                self.context()
            self.docker.config["services"][service]["build"] = original
        self.assert_read_only()

    def test_mc_smoke_cannot_use_a_different_image_from_init(self):
        self.docker.config["services"]["minio-smoke"]["image"] = "quay.io/minio/mc:old"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "MINIO_CLIENT_IMAGE"):
            self.context()
        self.assert_read_only()

    def test_mc_missing_and_stale_images_fail_preflight_without_pulling(self):
        original = copy.deepcopy(self.docker.images["local-mc:verified"])
        self.docker.images.pop("local-mc:verified")
        with self.assertRaisesRegex(runtime.RuntimeFailure, "IMAGE_MISSING"):
            self.context().check("minio")
        for label in ({}, {runtime.BUILD_LABEL: "old-source"}):
            self.docker.images["local-mc:verified"] = {**original, "Config": {"Labels": label}}
            with self.assertRaisesRegex(runtime.RuntimeFailure, "IMAGE_UNPREPARED_OR_STALE"):
                self.context().check("minio")
        self.assert_read_only()

    def test_mc_dockerfile_and_context_allowlist_invalidate_fingerprint(self):
        before = runtime.source_sha(self.root)
        for name in ("compose/minio-client/Dockerfile", "compose/minio-client/.dockerignore", "compose/minio/.dockerignore"):
            path = self.root / name
            original = path.read_bytes()
            path.write_bytes(original + b"changed")
            self.assertNotEqual(before, runtime.source_sha(self.root))
            path.write_bytes(original)

    def test_mc_new_image_requires_explicit_init_not_silently_reused(self):
        self.docker.containers["minio-init"]["Image"] = "old-mc-image"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "OUTDATED"):
            self.context().check("minio")
        self.assert_read_only()
        self.context().up("minio")
        changes = [command for command in self.docker.commands if "up" in command]
        self.assertEqual(1, len(changes))
        self.assertEqual("minio-init", changes[0][-1])
        self.assertIn("--no-build", changes[0])
        self.assertIn("never", changes[0])

    def test_fingerprint_tracks_build_inputs_but_not_secret_or_docs(self):
        before = runtime.source_sha(self.root)
        (self.root / ".env").write_text("DO_NOT_PRINT_CREDENTIAL")
        (self.root / "README.md").write_text("docs changed")
        self.assertEqual(before, runtime.source_sha(self.root))
        (self.root / "spark/src/main/java/Example.java").write_text("changed-code")
        self.assertNotEqual(before, runtime.source_sha(self.root))
        with self.assertRaisesRegex(runtime.RuntimeFailure, "STALE"):
            self.context().check("usgs")

    def test_unsafe_artifact_and_build_input_symlinks_are_rejected(self):
        self.artifacts()
        path = self.root / runtime.ARTIFACTS[0]
        path.unlink(); path.symlink_to(self.root / "README.md")
        with self.assertRaises(runtime.RuntimeFailure):
            runtime.artifact_receipt(self.root)
        source = self.root / "spark/src/linked-directory"
        source.symlink_to(self.root / "config", target_is_directory=True)
        with self.assertRaises(runtime.RuntimeFailure):
            runtime.source_sha(self.root)

    def test_docker_timeout_and_error_do_not_expose_captured_diagnostics(self):
        for outcome in (subprocess.TimeoutExpired("docker", 1, output="DO_NOT_PRINT_CREDENTIAL"),
                        subprocess.CompletedProcess([], 1, "DO_NOT_PRINT_CREDENTIAL", "DO_NOT_PRINT_CREDENTIAL")):
            with patch.object(runtime.subprocess, "run", side_effect=outcome if isinstance(outcome, Exception) else lambda *a, **k: outcome):
                with self.assertRaises(runtime.RuntimeFailure) as result:
                    self.context(timeout=1)
                self.assertNotIn("DO_NOT_PRINT_CREDENTIAL", str(result.exception))

    def test_daemon_unavailable_is_not_reported_as_missing_image(self):
        context = self.context()
        for message in ("Cannot connect to the Docker daemon at unix:///DO_NOT_PRINT_CREDENTIAL",
                        "error during connect: DO_NOT_PRINT_CREDENTIAL",
                        "permission denied while trying to connect to the Docker daemon socket: DO_NOT_PRINT_CREDENTIAL"):
            with self.subTest(message=message), patch.object(runtime.subprocess, "run", return_value=
                    subprocess.CompletedProcess([], 1, "", message)):
                with self.assertRaisesRegex(runtime.RuntimeFailure, "DOCKER_DAEMON_UNAVAILABLE") as result:
                    context.images(("minio",))
                self.assertNotIn("DO_NOT_PRINT_CREDENTIAL", str(result.exception))
                self.assertNotIn("MAKE_DOCKER_BUILD", str(result.exception))
        self.assert_read_only()

    def test_unknown_inspect_failure_does_not_claim_image_is_missing(self):
        context = self.context()
        with patch.object(runtime.subprocess, "run", return_value=
                subprocess.CompletedProcess([], 1, "", "unexpected DO_NOT_PRINT_CREDENTIAL")):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "DOCKER_COMMAND_FAILED_CHECK") as result:
                context.images(("minio",))
            self.assertNotIn("DO_NOT_PRINT_CREDENTIAL", str(result.exception))
        self.assert_read_only()

    def test_cli_redacts_unexpected_diagnostics_and_exits_nonzero(self):
        output = io.StringIO()
        with patch.object(sys, "argv", ["docker-runtime.py", "check"]), patch.object(runtime, "Runtime", side_effect=ValueError("DO_NOT_PRINT_CREDENTIAL")), contextlib.redirect_stdout(output):
            with self.assertRaises(SystemExit) as result:
                runtime.main()
        self.assertEqual(1, result.exception.code)
        self.assertNotIn("DO_NOT_PRINT_CREDENTIAL", output.getvalue())
        self.assertIn("redacted", output.getvalue())

    def test_repeated_docker_up_reuses_init_and_ready_services_without_mutations(self):
        self.context().up("airflow")
        self.context().up("airflow")
        self.assert_read_only()

    def test_start_after_stop_does_not_rerun_completed_inits(self):
        self.docker.containers["airflow-scheduler"]["State"]["Status"] = "exited"
        self.context().up("airflow")
        changes = [command for command in self.docker.commands if "up" in command]
        self.assertEqual(1, len(changes))
        self.assertIn("airflow-scheduler", changes[0])
        self.assertNotIn("minio-init", changes[0]); self.assertNotIn("airflow-init", changes[0])
        self.assertIn("--no-deps", changes[0]); self.assertIn("--no-build", changes[0])
        self.assertEqual("never", changes[0][changes[0].index("--pull") + 1])

    def test_cold_start_orders_bases_then_init_then_consumers_without_build_pull(self):
        self.artifacts(); self.docker.containers.clear()
        self.context().up("foundation")
        changes = [command for command in self.docker.commands if "up" in command]
        self.assertEqual(4, len(changes))
        self.assertIn("minio", changes[0]); self.assertIn("airflow-postgres", changes[0])
        self.assertEqual("minio-init", changes[1][-1]); self.assertEqual("airflow-init", changes[2][-1])
        self.assertIn("airflow-api-server", changes[3]); self.assertIn("trino", changes[3])
        self.assertTrue(all("--no-deps" in command and "--no-build" in command for command in changes))
        self.assertFalse(any("build" in command or "pull" in command or "down" in command for command in changes))

    def test_old_init_config_reruns_only_in_explicit_startup(self):
        self.docker.containers["minio-init"]["Config"]["Labels"]["com.docker.compose.config-hash"] = "old-config"
        self.context().up("airflow")
        changes = [command for command in self.docker.commands if "up" in command]
        self.assertEqual(1, len(changes)); self.assertEqual("minio-init", changes[0][-1])

    def test_running_old_init_is_not_replaced_or_taken_over(self):
        item = self.docker.containers["airflow-init"]
        item["State"]["Status"] = "running"
        item["Config"]["Labels"]["com.docker.compose.config-hash"] = "old-config"
        with self.assertRaisesRegex(runtime.RuntimeFailure, "INIT_IN_PROGRESS"):
            self.context().up("airflow")
        self.assert_read_only()

    def test_package_records_checksums_only_after_success_and_stable_source(self):
        def maven(command, **kwargs):
            for name in runtime.ARTIFACTS:
                path = self.root / name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(b"verified")
            return subprocess.CompletedProcess(command, 0)
        with patch.object(runtime.subprocess, "run", side_effect=maven):
            runtime.package(self.root)
        runtime.artifact_receipt(self.root)
        stamp = self.root / "staging/docker-runtime/java-artifacts.json"
        before = stamp.read_bytes()
        with patch.object(runtime.subprocess, "run", return_value=subprocess.CompletedProcess([], 1)):
            with self.assertRaises(runtime.RuntimeFailure):
                runtime.package(self.root)
        self.assertEqual(before, stamp.read_bytes())
        def changed(command, **kwargs):
            result = maven(command, **kwargs)
            (self.root / "spark/src/main/java/Example.java").write_text("changed-during-package")
            return result
        with patch.object(runtime.subprocess, "run", side_effect=changed):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "CHANGED_DURING"):
                runtime.package(self.root)
        self.assertEqual(before, stamp.read_bytes())

    def test_export_uses_stopped_network_isolated_container_and_no_host_maven(self):
        context = self.context()
        runtime.export_artifacts(context)
        runtime.artifact_receipt(self.root)
        create = next(command for command in self.docker.commands if command[1] == "create")
        self.assertIn("--network", create); self.assertIn("none", create)
        self.assertIn("/bin/true", create)
        remove = next(command for command in self.docker.commands if command[1] == "rm")
        self.assertEqual(["docker", "rm", "d" * 64], remove)
        self.assertFalse(any(word in command for command in self.docker.commands for word in ("start", "run", "exec", "--volumes", "-v")))

    def test_explicit_build_exports_artifacts_and_pulls_dependencies_without_starting_services(self):
        context = self.context()
        with patch.object(runtime, "Runtime", return_value=context):
            runtime.build(self.env)
        runtime.artifact_receipt(self.root)
        commands = self.docker.commands
        build = next(command for command in commands if "build" in command)
        self.assertIn("PIPELINE_BUILD_SHA=" + runtime.source_sha(self.root), build)
        self.assertTrue(all(service in build for service in runtime.BUILD_SERVICES))
        self.assertTrue(any("pull" in command for command in commands))
        pull = next(command for command in commands if "pull" in command)
        self.assertNotIn("minio-init", pull)
        self.assertNotIn("minio-smoke", pull)
        self.assertFalse(any(word in command for command in commands for word in ("up", "start", "run", "exec", "down")))

    def test_failed_explicit_build_does_not_export_start_or_record_artifacts(self):
        context = self.context()
        original = self.docker.run
        def failed(command, **kwargs):
            if "build" in command:
                return subprocess.CompletedProcess(command, 1)
            return original(command, **kwargs)
        with patch.object(runtime, "Runtime", return_value=context), patch.object(runtime.subprocess, "run", side_effect=failed):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "BUILD_FAILED"):
                runtime.build(self.env)
        self.assertFalse((self.root / "staging/docker-runtime/java-artifacts.json").exists())
        self.assertFalse(any(word in command for command in self.docker.commands for word in ("create", "cp", "rm", "up", "run", "start", "pull")))

    def test_failed_export_never_records_a_success_receipt(self):
        original = self.docker.run
        def failed(command, **kwargs):
            if command[1] == "cp":
                return subprocess.CompletedProcess(command, 1, "", "DO_NOT_PRINT_CREDENTIAL")
            return original(command, **kwargs)
        with patch.object(runtime.subprocess, "run", side_effect=failed):
            with self.assertRaises(runtime.RuntimeFailure):
                runtime.export_artifacts(self.context())
        self.assertFalse((self.root / "staging/docker-runtime/java-artifacts.json").exists())
        self.assertIn(["docker", "rm", "d" * 64], self.docker.commands)

    def test_running_current_init_is_waited_not_restarted(self):
        self.docker.containers["airflow-init"]["State"]["Status"] = "running"
        original = self.docker.run
        reads = 0
        def completed(command, **kwargs):
            nonlocal reads
            if command[1] == "inspect" and command[-1] == "airflow-init":
                reads += 1
                if reads >= 2:
                    self.docker.ready("airflow-init")
            return original(command, **kwargs)
        with patch.object(runtime.subprocess, "run", side_effect=completed):
            self.context().up("airflow")
        self.assert_read_only()

    def test_failed_init_prevents_starting_consumers(self):
        self.docker.containers.clear()
        original = self.docker.run
        def failed(command, **kwargs):
            result = original(command, **kwargs)
            if "up" in command and command[-1] == "minio-init":
                self.docker.containers["minio-init"]["State"]["ExitCode"] = 1
            return result
        with patch.object(runtime.subprocess, "run", side_effect=failed):
            with self.assertRaisesRegex(runtime.RuntimeFailure, "INIT_FAILED"):
                self.context().up("airflow")
        changes = [command for command in self.docker.commands if "up" in command]
        self.assertFalse(any("airflow-api-server" in command for command in changes))

    def test_expired_startup_budget_prevents_new_mutations(self):
        context = self.context()
        context.deadline = 0
        before = len(self.docker.commands)
        with self.assertRaisesRegex(runtime.RuntimeFailure, "BUDGET_EXHAUSTED"):
            context.mutate(["up", "minio"])
        self.assertEqual(before, len(self.docker.commands))


class SmokeCommandContractTests(unittest.TestCase):
    def test_per_run_mli_request_is_not_exported_into_compose_preflight_config(self):
        result = subprocess.run(["make", "--no-print-directory", "-s", "-o", "check-config-local", "smoke-mli-import-dag",
            "MLI_IMPORT_REQUEST=/opt/pipeline/staging/mli-03-qa/unit/import-request.json",
            "RUNTIME_CHECK=python3 -c 'import os; assert not os.environ.get(\"MLI_IMPORT_REQUEST\")'",
            "COMPOSE=true"], cwd=REPO, capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)

    def test_airflow_api_cold_probe_retains_real_readiness(self):
        compose = (REPO / "compose.yaml").read_text()
        api = compose.split("\n  airflow-api-server:\n", 1)[1].split("\n  airflow-scheduler:", 1)[0]
        self.assertIn("http://localhost:8080/api/v2/monitor/health", api)
        self.assertIn("--fail", api)
        self.assertIn("timeout: 15s", api)
        self.assertIn("start_period: 240s", api)
        self.assertNotIn("disable:", api)
        self.assertNotIn("exit 0", api)

    def test_make_shell_smokes_have_exactly_one_config_validation(self):
        targets = ("smoke-foundation", "smoke-minio", "smoke-airflow", "smoke-spark",
                   "smoke-query", "smoke-usgs-live", "smoke-jma-live")
        for target in targets:
            with self.subTest(target=target):
                result = subprocess.run(["make", "-n", target], cwd=REPO, capture_output=True, text=True)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertNotIn("./scripts/check-config.sh", result.stdout)
                self.assertIn("./scripts/" + target + ".sh", result.stdout)
                script = (REPO / "scripts" / (target + ".sh")).read_text()
                self.assertEqual(1, script.count('"$project_root/scripts/check-config.sh" --require-local'))
                self.assertLess(script.index('"$project_root/scripts/check-config.sh" --require-local'),
                                script.index('docker-runtime.py" check --component'))

    def test_all_shell_smokes_stop_on_invalid_config_before_docker(self):
        with tempfile.TemporaryDirectory(prefix="foundation config gate ") as directory:
            root = Path(directory)
            scripts = root / "scripts"; scripts.mkdir()
            tools = root / "bin"; tools.mkdir()
            (root / ".env").touch()
            check = scripts / "check-config.sh"
            check.write_text("#!/bin/sh\nprintf 'CONFIG_REJECTED\\n'\nexit 7\n")
            check.chmod(0o700)
            docker = tools / "docker"
            docker.write_text('#!/bin/sh\nprintf "unexpected\\n" >> "$MOCK_DOCKER_LOG"\nexit 99\n')
            docker.chmod(0o700)
            log = root / "docker.commands"
            env = {**os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"],
                   "MOCK_DOCKER_LOG": str(log)}
            for target in ("foundation", "minio", "airflow", "spark", "query", "usgs-live", "jma-live"):
                path = scripts / ("smoke-" + target + ".sh")
                path.write_text((REPO / "scripts" / path.name).read_text())
                with self.subTest(target=target):
                    result = subprocess.run(["sh", str(path)], env=env, capture_output=True, text=True)
                    self.assertEqual(7, result.returncode, result.stderr)
                    self.assertIn("CONFIG_REJECTED", result.stdout)
                    self.assertFalse(log.exists())

    def test_usgs_and_jma_wrappers_run_only_client_after_preflight_and_stop_on_failure(self):
        # Execute real shell wrappers; only their config/preflight/Docker boundaries are doubled.
        with tempfile.TemporaryDirectory(prefix="smoke shell with spaces ") as directory:
            root = Path(directory); scripts = root / "scripts"; scripts.mkdir()
            tools = root / "bin"; tools.mkdir()
            (root / ".env").touch()
            for name in ("check-config.sh", "check-real-sample-catalog.sh"):
                path = scripts / name; path.write_text("#!/bin/sh\nexit 0\n"); path.chmod(0o700)
            guard = tools / "python3"
            guard.write_text('#!/bin/sh\ncase "$*" in *"docker-runtime.py check --component "*) ;; *) exit 91 ;; esac\n[ "$MOCK_PREFLIGHT_FAIL" = 0 ] || exit 1\nprintf "preflight-passed\\n"\n')
            guard.chmod(0o700)
            docker = tools / "docker"
            docker.write_text('#!/bin/sh\nprintf "%s\\n" "$*" >> "$MOCK_DOCKER_LOG"\n')
            docker.chmod(0o700)
            log = root / "docker.commands"
            env = {**os.environ, "PATH": str(tools) + os.pathsep + os.environ["PATH"], "MOCK_DOCKER_LOG": str(log)}
            for name in ("smoke-usgs-live.sh", "smoke-jma-live.sh"):
                path = scripts / name; path.write_text((REPO / "scripts" / name).read_text())
                for fail in ("0", "1"):
                    with self.subTest(script=name, failed_preflight=fail):
                        if log.exists(): log.unlink()
                        result = subprocess.run(["sh", str(path)], env={**env, "MOCK_PREFLIGHT_FAIL": fail}, capture_output=True, text=True)
                        self.assertEqual(int(fail), result.returncode, result.stderr)
                        if fail == "1":
                            self.assertFalse(log.exists())
                        else:
                            commands = log.read_text().splitlines()
                            self.assertEqual(1, len(commands))
                            self.assertIn("run --rm --no-deps --pull never", commands[0])
                            self.assertNotIn("up -d", commands[0])

    def test_every_make_smoke_is_runtime_only_and_has_preflight(self):
        targets = re.findall(r"^(smoke-[\w-]+|resume-ml-window):", (REPO / "Makefile").read_text(), re.M)
        for target in targets:
            result = subprocess.run(["make", "-n", target], cwd=REPO, capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stderr)
            commands = result.stdout
            self.assertFalse(re.search(r"\b(?:mvnw|clean verify)\b|\b(?:up -d|build (?:minio|spark|airflow)|restart)\b", commands), target)
            if target not in ("smoke-foundation", "smoke-minio", "smoke-airflow", "smoke-spark", "smoke-query", "smoke-usgs-live", "smoke-jma-live"):
                self.assertIn("scripts/docker-runtime.py check", commands, target)
            if " run " in commands:
                self.assertIn("--no-deps", commands, target); self.assertIn("--pull never", commands, target)

    def test_shell_smokes_cannot_bootstrap_build_or_call_full_static_maven_checks(self):
        for path in (REPO / "scripts").glob("smoke-*.sh"):
            text = path.read_text()
            self.assertIn("docker-runtime.py\" check --component", text, path.name)
            self.assertNotIn("up -d", text, path.name)
            self.assertNotIn("build spark", text, path.name)
            self.assertNotIn("run --rm --no-deps minio-init", text, path.name)
            self.assertNotIn("scripts/check-spark.sh", text, path.name)
            self.assertNotIn("scripts/check-foundation.sh", text, path.name)
            self.assertIn("--pull never", text, path.name)

    def test_existing_unit_tests_keep_maven_and_ci_does_not_require_docker_runtime(self):
        text = (REPO / "Makefile").read_text()
        self.assertRegex(text, r"test-java:[^\n]*\n\t@./mvnw")
        self.assertIn("test-docker-runtime", text.split("ci-docs:", 1)[1].split("ci-java:", 1)[0])
        for path in ("compose/minio/Dockerfile", "compose/minio-client/Dockerfile", "compose/airflow/Dockerfile", "compose/spark/Dockerfile", "compose/spark/Dockerfile.gold-qa"):
            self.assertIn("LABEL " + runtime.BUILD_LABEL + "=$PIPELINE_BUILD_SHA", (REPO / path).read_text())

    def test_docker_build_does_not_require_a_host_java_package(self):
        result = subprocess.run(["make", "-n", "docker-build"], cwd=REPO, capture_output=True, text=True)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertNotIn("docker-runtime.py package", result.stdout)
        self.assertNotIn("./mvnw", result.stdout)
        self.assertIn("docker-runtime.py build", result.stdout)

    def test_mc_source_build_has_version_commit_gate_shell_tls_and_no_mutable_source(self):
        dockerfile = (REPO / "compose/minio-client/Dockerfile").read_text()
        for required in ('git clone --depth 1 --branch "$MC_VERSION" https://github.com/minio/mc.git',
                         'test "$(git rev-parse HEAD)" = "$MC_COMMIT"',
                         'test "$(git tag --points-at HEAD)" = "$MC_VERSION"',
                         'MC_RELEASE=RELEASE go run buildscripts/gen-ldflags.go 2025-08-13T08:35:41Z',
                         '/out/mc --version | grep -F "mc version $MC_VERSION (commit-id=$MC_COMMIT)"',
                         'FROM alpine:3.22.1', 'ca-certificates', 'USER mc:mc',
                         '/src/LICENSE', '/src/CREDITS', '-mod=readonly', 'GOTOOLCHAIN=local'):
            self.assertIn(required, dockerfile)
        for unsafe in ('@latest', ':latest', 'FROM scratch', 'GOSUMDB=off', '--insecure'):
            self.assertNotIn(unsafe, dockerfile)
        self.assertEqual('*\n!Dockerfile\n', (REPO / 'compose/minio-client/.dockerignore').read_text())


class FoundationStartupLogTests(unittest.TestCase):
    def test_current_boot_log_must_be_readable_nonempty_and_without_fatal_markers(self):
        source = (REPO / "scripts/smoke-foundation.sh").read_text()
        function = re.search(r"(?ms)^check_service_health\(\) \{.*?^\}", source).group(0)
        prelude = r'''
set -eu
failed=0
network_name=foundation_pipeline
container_id() { printf 'fixture-container\n'; }
report_pass() { printf '[PASS] %s\n' "$1"; }
report_fail() { printf '[FAIL] %s\n' "$1"; failed=1; }
docker() {
    case "$*" in
        *State.Health*) printf 'healthy\n' ;;
        *State.StartedAt*) printf '2026-10-15T08:08:00Z\n' ;;
        *NetworkSettings*) printf 'foundation_pipeline\n' ;;
        *) return 90 ;;
    esac
}
compose() {
    [ "$*" = 'logs --no-color --since 2026-10-15T08:08:00Z --tail=200 minio' ] || return 91
    case "$MOCK_LOG_MODE" in
        warning) printf 'WARNING: harmless startup warning\n' ;;
        empty) : ;;
        unreadable) printf 'DO_NOT_PRINT_CREDENTIAL\n' >&2; return 1 ;;
        fatal) printf 'FATAL: startup failed\n' ;;
        panic) printf 'PANIC: startup failed\n' ;;
    esac
}
'''
        for mode in ("warning", "empty", "unreadable", "fatal", "panic"):
            with self.subTest(mode=mode):
                result = subprocess.run(["sh", "-c", prelude + function +
                    '\ncheck_service_health minio\nexit "$failed"\n'],
                    env={**os.environ, "MOCK_LOG_MODE": mode}, capture_output=True, text=True)
                self.assertEqual(0 if mode == "warning" else 1, result.returncode, result.stderr)
                self.assertNotIn("DO_NOT_PRINT_CREDENTIAL", result.stdout + result.stderr)
                if mode == "unreadable":
                    self.assertIn("startup log could not be read", result.stdout)


if __name__ == "__main__":
    unittest.main()
