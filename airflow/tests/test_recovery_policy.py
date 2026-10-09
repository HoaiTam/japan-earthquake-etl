"""Offline control-plane tests; real Java/Spark/MinIO evidence is recorded separately."""
from copy import deepcopy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parents[1] / "dags"))
import backfill_runtime as runtime
import recovery_qa
import runtime_profile as profile
import staging_maintenance
import resource_pilot
from types import SimpleNamespace
from source_run_guard import lease, SourceRunBusy
from test_backfill_runtime import base

REPO = Path(__file__).parents[2]


class RecoveryPolicyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.env = {"DATA_BUCKET": "lake", "BRONZE_PREFIX": "bronze", "CONFIG_VERSION": "1",
                    "RUN_SUMMARY_ROOT": self.temp.name + "/summary",
                    "BACKFILL_STAGING_ROOT": self.temp.name + "/backfill",
                    "SOURCE_GUARD_ROOT": self.temp.name + "/guard"}

    def verifier(self, command, request, target, env):
        return {"contract_version": "orc-03-v1", "scope_sha256": request["scope_sha256"],
                "status": "BronzeVerified", "verified": True, "bronze_inputs": deepcopy(request["bronze_inputs"]),
                "observability": {"version": "orc-04-v1", "bronze_inputs": [
                    {**pin, "record_count_estimate": 2, "raw_object_uri": pin["manifest_uri"].replace(
                        "manifest.json", "response.geojson" if pin["source_system"] == "USGS" else "archive.zip")}
                    for pin in request["bronze_inputs"]]}}

    def recovered(self):
        with patch.object(runtime, "invoke", side_effect=self.verifier) as verifier:
            result = recovery_qa.run(self.env, base("reuse"))
        self.assertEqual(2, verifier.call_count)
        return result

    def test_failure_resume_rerun_preserves_inputs_and_counts(self):
        result = self.recovered()
        self.assertEqual("BronzeVerified", result["status"])
        for key in ("contention_blocked", "rerun_unchanged", "lease_released", "failure_before_store_call"):
            self.assertTrue(result[key])
        self.assertFalse(result["lake_writes"]); self.assertFalse(result["published"])
        self.assertEqual([2, 2], [row["input"] for row in result["sources"]])
        events = list(Path(result["summary_path"]).parent.joinpath("events").glob("*.json"))
        self.assertTrue(any(json.loads(path.read_text())["status"] == "FAILED" for path in events))

    def test_actual_verifier_failure_stays_failed_and_releases_own_lease(self):
        with patch.object(runtime, "invoke", side_effect=runtime.BackfillError("SOURCE_NOT_READY")):
            with self.assertRaises(Exception): recovery_qa.run(self.env, base("reuse"))
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())
        states = list(Path(self.env["RUN_SUMMARY_ROOT"]).glob("qa/*/run_summary.json"))
        self.assertEqual("FAILED", json.loads(states[0].read_text())["status"])

    def test_stale_lease_never_expires_or_is_stolen(self):
        original = {"dag_id": "jma_04_year_backfill", "run_id": "stale-but-not-proven-dead"}
        lease("acquire", original, self.env)
        before = (Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").read_bytes()
        with self.assertRaises(Exception): recovery_qa.run(self.env, base("reuse"))
        self.assertEqual(before, (Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").read_bytes())
        self.assertEqual("NOT_OWNER", lease("release", {"dag_id": "other", "run_id": "other"}, self.env)["status"])
        with self.assertRaises(SourceRunBusy): lease("assert", {"dag_id": "other", "run_id": "other"}, self.env)

    def test_maintenance_is_preview_and_retains_audit(self):
        result = self.recovered()
        root = self.env["RUN_SUMMARY_ROOT"] + "/qa"
        before = {str(path): path.read_bytes() for path in Path(root).rglob("*.json")}
        report = staging_maintenance.preview(root, "orc_03_backfill", result["run_id"], self.env)
        self.assertTrue(report["terminal"]); self.assertFalse(report["automatic_cleanup_allowed"])
        self.assertFalse(report["mutation_performed"])
        self.assertEqual(before, {str(path): path.read_bytes() for path in Path(root).rglob("*.json")})
        lease("acquire", {"dag_id": "busy", "run_id": "active"}, self.env)
        self.assertTrue(staging_maintenance.preview(root, "orc_03_backfill", result["run_id"], self.env)["source_lease_present"])

    def test_maintenance_missing_run_creates_nothing(self):
        with self.assertRaises(Exception):
            staging_maintenance.preview(self.temp.name + "/absent", "orc_03_backfill", "none", self.env)
        self.assertFalse(Path(self.temp.name + "/absent").exists())

    def test_profile_caps_legacy_concurrency_and_rejects_bad_heap(self):
        self.assertEqual(1, profile.archive_concurrency({"JMA_BACKFILL_MAX_CONCURRENCY": "4"}))
        self.assertEqual("384m", profile.source_heap({}))
        for value in ("2g", "0", "512m -Dsecret=value", "-Xmx512m"):
            with self.assertRaises(ValueError): profile.source_heap({"SOURCE_RUNNER_HEAP": value})
        for value in ("0", "5", "bad"):
            with self.assertRaises(ValueError): profile.archive_concurrency({"JMA_BACKFILL_MAX_CONCURRENCY": value})

    def test_all_java_wrappers_reject_heap_injection_before_launch(self):
        for name in ("usgs-runner.sh", "jma-runner.sh", "bronze-reuse-runner.sh"):
            # USGS/JMA check absent packaged JAR first on host; still never launch Java.
            result = subprocess.run(["sh", str(REPO / "compose/airflow" / name)], capture_output=True,
                                    env={"PATH": "/usr/bin:/bin", "JAVA_HOME": "/not-a-jdk",
                                         "SOURCE_RUNNER_HEAP": "384m -Dsecret=sentinel"})
            self.assertEqual(2, result.returncode)
            self.assertNotIn(b"sentinel", result.stderr)

    def test_deployment_and_retry_boundaries_are_explicit(self):
        compose = (REPO / "compose.yaml").read_text()
        self.assertIn('AIRFLOW__CORE__PARALLELISM: "1"', compose)
        self.assertIn('AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: "1"', compose)
        self.assertIn("SPARK_DAEMON_MEMORY: 256m", compose)
        for name in ("usg_04_usgs_ingest.py", "jma_04_year_backfill.py", "orc_02_daily_sources.py"):
            source = (REPO / "airflow/dags" / name).read_text()
            self.assertIn("MUTATING_RETRIES", source)
            self.assertIn("max_active_tasks=1", source)
        self.assertEqual(0, profile.MUTATING_RETRIES); self.assertEqual(1, profile.VERIFY_RETRIES)
        self.assertIn('task_id="verify", retries=VERIFY_RETRIES',
                      (REPO / "airflow/dags/usg_04_usgs_ingest.py").read_text())

    def test_shell_spark_heap_guards_accept_legacy_and_reject_oom_sizes(self):
        source = (REPO / "scripts/check-config.sh").read_text()
        function = source.split("validate_spark_memory() {", 1)[1].split("\n}", 1)[0]
        shell = ('validate_spark_memory() {' + function + '\n}\n'
                 'sample=$1; failed=0; read_value() { printf "%s" "$sample"; }; '
                 'report_error() { failed=1; }; validate_spark_memory ignored "$2"; exit "$failed"')
        for value, key, expected in (("512m", "SPARK_DRIVER_MEMORY", 0), ("1g", "SPARK_DRIVER_MEMORY", 0),
                                     ("2g", "SPARK_EXECUTOR_MEMORY", 0), ("2g", "SPARK_DRIVER_MEMORY", 1),
                                     ("4g", "SPARK_EXECUTOR_MEMORY", 1), ("128m", "SPARK_DRIVER_MEMORY", 1),
                                     ("000512m", "SPARK_DRIVER_MEMORY", 1), ("9999999999g", "SPARK_DRIVER_MEMORY", 1)):
            with self.subTest(value=value, key=key):
                self.assertEqual(expected, subprocess.run(["sh", "-c", shell, "test", value, key]).returncode)

    def test_resource_probe_requires_idle_cluster_and_releases_lease_on_failure(self):
        with patch.object(resource_pilot, "urlopen") as api, patch.object(resource_pilot, "memory", return_value={}), \
                patch.object(resource_pilot.subprocess, "run") as submit:
            api.return_value.__enter__.return_value.read.return_value = b'{"activeapps":[{}],"workers":[]}'
            with self.assertRaisesRegex(runtime.BackfillError, "RESOURCE_PILOT_CLUSTER_BUSY"):
                resource_pilot.run(self.env, base("reuse"))
            submit.assert_not_called()
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())
        with patch.object(resource_pilot, "memory", side_effect=OSError("cgroup unavailable")):
            with self.assertRaises(OSError): resource_pilot.run(self.env, base("reuse"))
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())

    def test_resource_probe_cgroup_oom_blocks_success_even_with_zero_process_exit(self):
        def submit(*args, **kwargs):
            kwargs["stdout"].write(json.dumps({"task": "ORC-05", "lake_writes": False, "published": False,
                                               "master": "spark://spark-master:7077"}).encode())
            return SimpleNamespace(returncode=0)
        with patch.object(resource_pilot, "urlopen") as api, patch.object(resource_pilot, "java_identity", return_value=self.env), \
                patch.object(resource_pilot, "memory", side_effect=[{"events": {"oom_kill": 0}}, {"events": {"oom_kill": 1}}]), \
                patch.object(resource_pilot.subprocess, "run", side_effect=submit):
            api.return_value.__enter__.return_value.read.return_value = b'{"activeapps":[],"workers":[{"state":"ALIVE","coresfree":1,"memoryfree":512}]}'
            with self.assertRaisesRegex(runtime.BackfillError, "RESOURCE_PILOT_OOM"):
                resource_pilot.run(self.env, base("reuse"))
        self.assertFalse(list(Path(self.env["BACKFILL_STAGING_ROOT"]).rglob("resource_report.json")))
        self.assertFalse((Path(self.env["SOURCE_GUARD_ROOT"]) / "owner.json").exists())


if __name__ == "__main__": unittest.main()
