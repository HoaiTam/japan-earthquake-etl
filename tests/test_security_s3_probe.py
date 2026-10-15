"""SEC-01 permission tests use an in-memory fake, never Docker/network."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("sec_probe", ROOT / "scripts/security-s3-probe.py")
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class ClientError(Exception):
    def __init__(self, code="AccessDenied", status=403):
        self.response = {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status}}


class FakeS3:
    def __init__(self, objects, anonymous=False, failure=None):
        self.objects, self.anonymous, self.failure = objects, anonymous, failure

    def authorize(self, operation, key):
        forbidden = self.anonymous or not key.startswith(("bronze/", "silver/", "warehouse/"))
        forbidden |= operation == "delete" and key.startswith("bronze/")
        if self.failure == "allow_outside" and key.startswith("_security-denied/"):
            forbidden = operation != "put"
        if self.failure == "allow_bronze_delete" and operation == "delete":
            forbidden = False
        if forbidden:
            if self.failure == "wrong_denial":
                raise ClientError("SignatureDoesNotMatch", 403)
            if self.failure == "network":
                raise TimeoutError("DO_NOT_ECHO_SECRET")
            raise ClientError()

    def put_object(self, Bucket, Key, Body):
        self.authorize("put", Key)
        self.objects[Key] = Body

    def get_object(self, Bucket, Key):
        self.authorize("get", Key)
        if Key not in self.objects:
            raise ClientError("NoSuchKey", 404)
        payload = b"wrong payload" if self.failure == "wrong_payload" else self.objects[Key]
        return {"Body": io.BytesIO(payload)}

    def list_objects_v2(self, Bucket, Prefix, MaxKeys):
        self.authorize("list", Prefix)
        assert MaxKeys == 1
        return {"Contents": [{"Key": key} for key in self.objects if key.startswith(Prefix)][:MaxKeys]}

    def delete_object(self, Bucket, Key):
        self.authorize("delete", Key)
        if self.failure == "cleanup" and Key.startswith("silver/"):
            raise TimeoutError("DO_NOT_ECHO_SECRET")
        self.objects.pop(Key, None)


class PermissionProbeTest(unittest.TestCase):
    def run_probe(self, failure=None):
        objects = {"silver/business-record": b"KEEP"}
        boto = types.ModuleType("boto3")
        boto.client = lambda *args, **kwargs: FakeS3(objects, kwargs["aws_access_key_id"] == "anonymous", failure)
        core = types.ModuleType("botocore"); core.UNSIGNED = "UNSIGNED"
        config = types.ModuleType("botocore.config"); config.Config = lambda **kwargs: kwargs
        exceptions = types.ModuleType("botocore.exceptions"); exceptions.ClientError = ClientError
        output = io.StringIO()
        modules = {"boto3": boto, "botocore": core, "botocore.config": config, "botocore.exceptions": exceptions}
        with patch.dict("sys.modules", modules), patch.dict("os.environ", {
                "MINIO_ACCESS_KEY": "synthetic-key", "MINIO_SECRET_KEY": "synthetic-secret"}), contextlib.redirect_stdout(output):
            probe.run({"endpoint": "http://minio:9000", "bucket": "test", "prefixes": ["bronze", "silver", "warehouse"]})
        self.assertNotIn("DO_NOT_ECHO_SECRET", output.getvalue())
        self.assertEqual(b"KEEP", objects["silver/business-record"])
        return json.loads(output.getvalue()), objects

    def test_allow_deny_and_scoped_cleanup(self):
        report, objects = self.run_probe()
        self.assertTrue(report["passed"]); self.assertEqual(15, len(report["checks"]))
        self.assertEqual(3, len(report["created_keys"]))
        self.assertEqual(report["created_keys"][:1], report["retained_keys"])
        self.assertEqual({"silver/business-record", report["retained_keys"][0]}, set(objects))

    def test_signature_error_is_not_permission_deny(self):
        report, _ = self.run_probe("wrong_denial")
        self.assertFalse(report["passed"])

    def test_network_failure_is_redacted_and_cleans_mutable_keys(self):
        report, objects = self.run_probe("network")
        self.assertFalse(report["passed"])
        self.assertEqual(2, len(objects))

    def test_unexpected_outside_write_is_recorded_not_deleted(self):
        report, objects = self.run_probe("allow_outside")
        self.assertFalse(report["passed"])
        outside = next(key for key in report["retained_keys"] if key.startswith("_security-denied/"))
        self.assertIn(outside, report["created_keys"]); self.assertIn(outside, objects)

    def test_bronze_delete_success_is_a_failure(self):
        report, _ = self.run_probe("allow_bronze_delete")
        self.assertFalse(report["passed"])

    def test_cleanup_failure_is_recorded(self):
        report, objects = self.run_probe("cleanup")
        self.assertFalse(report["passed"])
        self.assertTrue(any(key.startswith("silver/_security/") for key in report["retained_keys"]))
        self.assertEqual(3, len(objects))

    def test_readback_mismatch_is_a_failure(self):
        report, _ = self.run_probe("wrong_payload")
        self.assertFalse(report["passed"])


if __name__ == "__main__":
    unittest.main()
