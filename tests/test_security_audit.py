import importlib.util
import hashlib
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("security_audit", Path(__file__).resolve().parents[1] / "scripts/audit-security.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


class SecurityAuditTest(unittest.TestCase):
    def test_tokens_redacted_to_categories(self):
        value = "ghp_" + "x" * 40
        result = audit.scan_text(value)
        self.assertEqual(["github_token"], result)
        self.assertNotIn(value, repr(result))

    def test_key_and_credential_uri(self):
        self.assertIn("private_key", audit.scan_text("-----BEGIN " + "PRIVATE KEY-----"))
        self.assertIn("credential_uri", audit.scan_text("postgresql://user:" + "real-value@host/db"))

    def test_placeholders_interpolation_and_fixture_uri(self):
        for text in ("postgresql://${USER}:${PASSWORD}@db/name", "https://user:change-me-pass@host", "https://fixture:fixture@host"):
            self.assertEqual([], audit.scan_text(text))

    def test_ml_account_and_query_boundary(self):
        text = "/home/" + "person/result.parquet"
        self.assertEqual([], audit.scan_text(text))
        self.assertIn("personal_account_path", audit.scan_text(text, ml=True))
        self.assertIn("credential_query", audit.scan_text("https://drive.test/file?" + "token=value", ml=True))
        self.assertEqual([], audit.scan_text("/home/iceberg/warehouse", ml=True))

    def test_allowed_loopback_ports(self):
        findings, ports = audit.inspect_compose({"services": {"trino": {"ports": [{"host_ip": "127.0.0.1", "target": 8080}]}}})
        self.assertEqual([], findings)
        self.assertEqual(1, len(ports))

    def test_public_bind_internal_port_and_unknown_format_rejected(self):
        for port in ({"host_ip": "0.0.0.0", "target": 8080}, {"host_ip": "127.0.0.1", "target": 9000}, "8080:8080"):
            findings, _ = audit.inspect_compose({"services": {"minio": {"ports": [port]}}})
            self.assertEqual(1, len(findings))

    def test_root_credential_consumer_boundary(self):
        findings, _ = audit.inspect_compose({"services": {"spark-worker": {"environment": {"MINIO_ROOT_PASSWORD": "${PLACEHOLDER}"}}}})
        self.assertEqual("root_credential_consumer", findings[0]["category"])
        findings, _ = audit.inspect_compose({"services": {"minio-init": {"environment": {"MINIO_ROOT_PASSWORD": "${PLACEHOLDER}"}}}})
        self.assertEqual([], findings)

    def test_triage_is_content_bound_and_cannot_exempt_tokens(self):
        data = b"synthetic fixture\n"
        checksum = hashlib.sha256(data).hexdigest()
        allow = [{"normalized_sha256": checksum, "category": "credential_uri"},
                 {"normalized_sha256": checksum, "category": "github_token"}]
        self.assertEqual(([], ["credential_uri"]), audit.triage(["credential_uri"], data, allow))
        self.assertEqual((["credential_uri"], []), audit.triage(["credential_uri"], data + b"changed", allow))
        self.assertEqual((["github_token"], []), audit.triage(["github_token"], data, allow))


if __name__ == "__main__":
    unittest.main()
