"""SEC-01 offline redaction, runtime gate and MinIO stdin regressions."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
def load(name, file):
    spec = importlib.util.spec_from_file_location(name, ROOT / file)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module
runtime = load("sec_runtime_test", "scripts/security-runtime.py")
audit = load("sec_audit_test", "scripts/audit-security.py")


class RuntimeSecurityTest(unittest.TestCase):
    def fixture(self):
        expected = {"ports": [{"target": 8080, "host_ip": "127.0.0.1", "published": "8080"}],
                    "environment": {"MINIO_SECRET_KEY": "synthetic-pipeline-secret"}}
        container = {"Config": {"Env": ["MINIO_SECRET_KEY=synthetic-pipeline-secret"]},
                     "HostConfig": {"PortBindings": {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8080"}]}}}
        return expected, container

    def test_deployed_loopback_role_matches(self):
        expected, container = self.fixture()
        findings, ports = runtime.inspect_container("airflow-api-server", container, expected, ["admin", "synthetic-root-secret"])
        self.assertEqual([], findings); self.assertEqual(1, len(ports))

    def test_public_bind_rejected(self):
        expected, container = self.fixture()
        container["HostConfig"]["PortBindings"]["8080/tcp"][0]["HostIp"] = "0.0.0.0"
        findings, _ = runtime.inspect_container("airflow-api-server", container, expected, ["admin", "synthetic-root-secret"])
        self.assertIn("unexpected_deployed_port", {row["category"] for row in findings})

    def test_trino_logging_mount_must_be_deployed_readonly(self):
        expected, container = self.fixture()
        expected["volumes"] = [{"source": "/reviewed/log.properties", "target": "/etc/trino/log.properties"}]
        roots = ["admin", "synthetic-root-secret"]
        findings, _ = runtime.inspect_container("trino", container, expected, roots)
        self.assertIn("trino_logging_config_not_deployed", {row["category"] for row in findings})
        container["Mounts"] = [{"Source": "/reviewed/log.properties", "Destination": "/etc/trino/log.properties", "RW": False}]
        self.assertEqual([], runtime.inspect_container("trino", container, expected, roots)[0])
        container["Mounts"][0]["RW"] = True
        self.assertTrue(runtime.inspect_container("trino", container, expected, roots)[0])

    def test_trino_logger_fix_keeps_runtime_info(self):
        text = (ROOT / "trino/log.properties").read_text()
        rules = [line.strip() for line in text.splitlines() if line.strip() and not line.startswith("#")]
        self.assertEqual(["io.trino.bootstrap.catalog=WARN"], rules)

    def test_credential_drift_redacted(self):
        expected, container = self.fixture()
        container["Config"]["Env"] = ["MINIO_SECRET_KEY=unexpected-private-value"]
        findings, _ = runtime.inspect_container("airflow-api-server", container, expected, ["admin", "synthetic-root-secret"])
        self.assertEqual("deployed_credential_config_mismatch", findings[0]["category"])
        self.assertNotIn("unexpected-private-value", repr(findings))

    def test_root_value_in_disguised_consumer_rejected(self):
        expected, container = self.fixture()
        container["Config"]["Env"].append("CUSTOM_ENV=synthetic-root-secret")
        findings, _ = runtime.inspect_container("airflow-api-server", container, expected, ["admin", "synthetic-root-secret"])
        self.assertIn("root_credential_in_consumer", {row["category"] for row in findings})

    def test_runtime_exception_never_echoes_secret(self):
        with patch.object(runtime, "call", side_effect=ValueError("DO_NOT_ECHO_PRIVATE")):
            result = runtime.audit_runtime(ROOT, audit.scan_text)
        self.assertFalse(result["passed"]); self.assertNotIn("DO_NOT_ECHO_PRIVATE", repr(result))
        self.assertEqual("config", result["pending"][0]["phase"])

    def test_report_true_without_probe_checks_rejected(self):
        self.assertFalse(runtime.valid_permission_evidence({"passed": True, "checks": []}, {"prefixes": ["bronze", "silver", "warehouse"]}))

    def test_current_secret_parser_does_not_return_placeholders(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / ".env").write_text('MINIO_SECRET_KEY="synthetic-private-secret"\nMINIO_ACCESS_KEY=change-me-placeholder\n')
            self.assertEqual(["synthetic-private-secret"], audit.configured_secrets(root))

    def test_notebook_and_pilot_ml_scan_scope(self):
        for name in ("notebooks/run.ipynb", "staging/ml-pilot/manifest.json", "experiments/config.json"):
            self.assertTrue(audit.is_ml_artifact(name))
        self.assertFalse(audit.is_ml_artifact("docs/task/tasks/DAT-02.md"))

    def test_incomplete_cfg_or_artifact_gate_cannot_enable_done(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / ".env").write_text("MINIO_SECRET_KEY=synthetic-private-secret\n")
            for cfg_code in (0, 1):
                with self.subTest(cfg_code=cfg_code):
                    calls = []
                    module = types.SimpleNamespace(audit_runtime=lambda *args, **kwargs:
                        calls.append(kwargs["permissions"]) or {"findings": [], "pending": [], "coverage": {}})
                    spec = types.SimpleNamespace(loader=types.SimpleNamespace(exec_module=lambda module: None))
                    def command(args, **kwargs):
                        if "check-ignore" in args:
                            return types.SimpleNamespace(returncode=0)
                        if str(args[0]).endswith("check-config.sh"):
                            return types.SimpleNamespace(returncode=cfg_code)
                        return types.SimpleNamespace(returncode=0, stdout=b'{"services": {}}')
                    with patch.object(audit, "git", return_value=b""), patch.object(audit.subprocess, "run", side_effect=command), \
                            patch.object(audit.importlib.util, "spec_from_file_location", return_value=spec), \
                            patch.object(audit.importlib.util, "module_from_spec", return_value=module):
                        report = audit.audit(root, include_history=False, runtime=True, permissions=True)
                    self.assertEqual([False], calls)  # history omitted => incomplete preflight
                    self.assertFalse(report["can_mark_done"])
                    self.assertIn("actual_notebook_export_import_acceptance_not_verified", {row["reason"] for row in report["pending"]})
                    if cfg_code:
                        self.assertIn("local_config_validation_failed", {row["category"] for row in report["findings"]})

    def test_permission_failure_can_never_be_passed(self):
        context = {"prefixes": ["bronze", "silver", "warehouse"]}
        required = {"bronze_delete_denied", "bronze_still_readable", "anonymous_read_denied",
                    "anonymous_list_denied", "outside_prefix_read_denied", "outside_prefix_list_denied",
                    "outside_prefix_write_denied", "mutable_probe_cleanup"}
        required |= {"pipeline_" + op + "_" + p for p in context["prefixes"] for op in ("read", "list")}
        keys = [p + "/_security/sec-01-" + "a" * 32 + "/probe.txt" for p in context["prefixes"]]
        evidence = {"passed": True, "checks": [{"check": c, "passed": True} for c in sorted(required)],
                    "created_keys": keys, "retained_keys": keys[:1],
                    "payload_sha256": hashlib.sha256(b"SEC-01 harmless scoped permission probe\n").hexdigest()}
        evidence["checks"].append({"check": "mutable_probe_cleanup", "passed": True})
        self.assertTrue(runtime.valid_permission_evidence(evidence, context))
        changed = copy.deepcopy(evidence); changed["checks"][0]["passed"] = False
        self.assertFalse(runtime.valid_permission_evidence(changed, context))
        changed = copy.deepcopy(evidence); changed["created_keys"][1] = "unrelated/real-data"
        self.assertFalse(runtime.valid_permission_evidence(changed, context))
        changed = copy.deepcopy(evidence); changed["checks"].append(changed["checks"][0])
        self.assertFalse(runtime.valid_permission_evidence(changed, context))
        changed = copy.deepcopy(evidence); changed["created_keys"][1] = changed["created_keys"][1].replace("a" * 32, "b" * 32)
        self.assertFalse(runtime.valid_permission_evidence(changed, context))

    def test_bootstrap_passwords_only_on_stdin(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); executable = root / "mc"; log = root / "calls.jsonl"
            executable.write_text("#!" + sys.executable + "\n" +
                "import os,sys,json\nargs=sys.argv[1:]\nrow={'args':args}\n"
                "if args[:2]==['alias','import']: row['stdin']=json.load(sys.stdin)\n"
                "if args[:3]==['admin','user','add']: row['stdin']=sys.stdin.read().splitlines()\n"
                "with open(os.environ['MOCK_MC_LOG'],'a') as f: f.write(json.dumps(row)+'\\n')\n"
                "if args[:3]==['admin','user','add'] and os.environ.get('MOCK_MC_FAIL'): "
                "print('DO_NOT_ECHO_PASSWORD',file=sys.stderr);sys.exit(1)\n")
            executable.chmod(0o700)
            source = (ROOT / "compose/minio/init.sh").read_text().replace("/tmp/", str(root) + "/")
            script = root / "init.sh"; script.write_text(source)
            env = {**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                   "MOCK_MC_LOG": str(log), "MINIO_ENDPOINT": "http://minio:9000",
                   "MINIO_ROOT_USER": "synthetic-root-user", "MINIO_ROOT_PASSWORD": 'synthetic-root-\\quoted"',
                   "MINIO_ACCESS_KEY": "synthetic-pipeline-user", "MINIO_SECRET_KEY": 'synthetic-pipeline-\\quoted"',
                   "DATA_BUCKET": "synthetic-bucket", "BRONZE_PREFIX": "bronze", "SILVER_PREFIX": "silver",
                   "WAREHOUSE_PATH": "s3://synthetic-bucket/warehouse"}
            result = subprocess.run(["sh", str(script)], env=env, capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stderr)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
            imported = next(row["stdin"] for row in calls if row["args"][:2] == ["alias", "import"])
            self.assertEqual(env["MINIO_ROOT_PASSWORD"], imported["secretKey"])
            user = next(row for row in calls if row["args"][:3] == ["admin", "user", "add"])
            self.assertEqual(["admin", "user", "add", "bootstrap"], user["args"])
            self.assertEqual([env["MINIO_ACCESS_KEY"], env["MINIO_SECRET_KEY"]], user["stdin"])
            for row in calls:
                self.assertNotIn(env["MINIO_ROOT_PASSWORD"], repr(row["args"]))
                self.assertNotIn(env["MINIO_SECRET_KEY"], repr(row["args"]))
            self.assertNotIn(env["MINIO_ROOT_PASSWORD"], result.stdout + result.stderr)
            failed = subprocess.run(["sh", str(script)], env={**env, "MOCK_MC_FAIL": "1"}, capture_output=True, text=True)
            self.assertNotEqual(0, failed.returncode); self.assertNotIn("DO_NOT_ECHO_PASSWORD", failed.stderr)


if __name__ == "__main__":
    unittest.main()
