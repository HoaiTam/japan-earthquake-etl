"""Offline, labeled synthetic security regressions; never actual cloud proof."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import security_cloud as c
spec = importlib.util.spec_from_file_location("cloud_scan_test", ROOT / "scripts/audit-security.py")
scanner = importlib.util.module_from_spec(spec); spec.loader.exec_module(scanner)
spec_cli = importlib.util.spec_from_file_location("cloud_cli_test", ROOT / "scripts/audit-security-cloud.py")
cli = importlib.util.module_from_spec(spec_cli); spec_cli.loader.exec_module(cli)


class CloudTest(unittest.TestCase):
    def inspect(self, name, data):
        return c.inspect_bytes(name, data, c.digest(data), scanner.scan_text)

    def test_clean_notebook_and_actual_output_scanned(self):
        nb = {"nbformat": 4, "cells": [{"source": ["print(1)"], "outputs": [{"text": ["safe output"]}]}]}
        self.assertTrue(self.inspect("test.ipynb", json.dumps(nb).encode())["passed"])
        nb["cells"][0]["outputs"][0]["text"] = ["/" + "Users/person/private"]
        result = self.inspect("test.ipynb", json.dumps(nb).encode())
        self.assertFalse(result["passed"])
        self.assertNotIn("/Users/person/private", json.dumps(result))

    def test_oauth_json_escaped_keys_and_bearer_redacted(self):
        for raw in (b'{"refresh_\\u0074oken":"synthetic-sensitive"}',
                    json.dumps({"value": "Bearer " + "x" * 40}).encode()):
            result = self.inspect("test.json", raw)
            self.assertFalse(result["passed"])
            self.assertNotIn("synthetic-sensitive", json.dumps(result))

    def test_wrong_pin_and_bad_json_fail_closed(self):
        self.assertFalse(c.inspect_bytes("a.json", b"{}", "0" * 64, scanner.scan_text)["passed"])
        self.assertFalse(self.inspect("a.json", b'{"x":1,"x":2}')["passed"])

    def test_nested_zip_payloads_and_unsafe_member(self):
        inner = io.BytesIO()
        with zipfile.ZipFile(inner, "w") as z:
            z.writestr("data.json", json.dumps({"access_token": "synthetic-sensitive"}))
        outer = io.BytesIO()
        with zipfile.ZipFile(outer, "w") as z:
            z.writestr("pilot.zip", inner.getvalue())
        self.assertFalse(self.inspect("handoff.zip", outer.getvalue())["passed"])
        bad = io.BytesIO()
        with zipfile.ZipFile(bad, "w") as z:
            z.writestr("../escape.json", "{}")
        self.assertTrue(self.inspect("bad.zip", bad.getvalue())["pending"])

    def test_zip_symlink_duplicate_and_binary_unknown_rejected(self):
        entry = zipfile.ZipInfo("link"); entry.external_attr = 0o120777 << 16
        raw = io.BytesIO()
        with zipfile.ZipFile(raw, "w") as z:
            z.writestr(entry, "target")
        self.assertFalse(self.inspect("bad.zip", raw.getvalue())["passed"])
        self.assertFalse(self.inspect("weights.pkl", b"binary")["passed"])
        import warnings
        raw = io.BytesIO()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(raw, "w") as z:
                z.writestr("a.json", "{}"); z.writestr("a.json", "{}")
        self.assertFalse(self.inspect("duplicate.zip", raw.getvalue())["passed"])

    def test_symlink_path_and_missing_file_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve(); (root / "real.json").write_text("{}")
            (root / "link.json").symlink_to(root / "real.json")
            self.assertFalse(c.inspect_file(root / "link.json", c.digest(b"{}"), scanner.scan_text)["passed"])
            self.assertFalse(c.inspect_file(root / "absent", c.digest(b"{}"), scanner.scan_text)["passed"])

    def test_acl_users_only_not_identity_approval(self):
        result = c.permission_summary({"id": "synthetic_object", "mimeType": "folder"},
                                      [{"type": "user", "role": "owner", "emailAddress": "synthetic@example.invalid"}])
        self.assertTrue(result["restricted_to_users"])
        self.assertFalse(result["passed"])
        self.assertNotIn("synthetic@example.invalid", json.dumps(result))

    def test_anyone_domain_group_are_findings(self):
        for kind in ("anyone", "domain", "group"):
            result = c.permission_summary({"id": "synthetic_object", "mimeType": "folder"},
                                          [{"type": kind, "role": "reader"}])
            self.assertTrue(result["findings"])
            self.assertFalse(result["passed"])

    def test_permissions_pagination_and_repeated_token_fail_closed(self):
        calls = []
        def pages(endpoint, params):
            calls.append((endpoint, params))
            return {"permissions": [{"type": "user"}], **({"nextPageToken": "next"} if len(calls) == 1 else {})}
        self.assertEqual(len(c.permissions_for(pages, "synthetic_id_1234")), 2)
        self.assertEqual(calls[1][1]["pageToken"], "next")
        with self.assertRaises(ValueError):
            c.permissions_for(lambda *args: {"permissions": [], "nextPageToken": "loop"}, "synthetic_id_1234")

    def test_empty_acl_missing_identity_or_role_are_pending(self):
        for perms in ([], [{"type": "user", "role": "unknown"}],
                      [{"type": "user", "role": "reader", "deleted": True}]):
            self.assertFalse(c.permission_summary({"id": "test", "mimeType": "folder"}, perms)["passed"])

    def test_offline_scope_cannot_mark_overall_done(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp).resolve() / "safe.json"; path.write_text("{}")
            report = cli.collect({"version": "sec-01-cloud-scope-v1", "artifacts": [{"path": str(path), "sha256": c.digest(b"{}")} ]})
            self.assertTrue(report["scoped_bytes_passed"])
            self.assertFalse(report["can_mark_done"])
            self.assertTrue(report["pending"])

    def test_not_reading_drive_when_offline(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp).resolve() / "safe.json"; path.write_text("{}")
            with patch.object(cli, "DriveReader", side_effect=AssertionError("must not connect")):
                cli.collect({"version": "sec-01-cloud-scope-v1", "artifacts": [{"path": str(path), "sha256": c.digest(b"{}")} ]})

    def test_png_compressed_metadata_is_scanned(self):
        try:
            from PIL import Image, PngImagePlugin
        except ImportError:
            self.skipTest("Pillow unavailable; real PNG audit returns pending")
        raw = io.BytesIO(); info = PngImagePlugin.PngInfo()
        info.add_text("comment", "/" + "Users/synthetic/private", zip=True)
        Image.new("RGB", (1, 1)).save(raw, format="PNG", pnginfo=info)
        self.assertFalse(self.inspect("plot.png", raw.getvalue())["passed"])

    def test_budget_limit_fails_closed(self):
        from unittest.mock import patch
        with patch.object(c.h, "TOTAL_LIMIT", 1):
            self.assertFalse(self.inspect("a.json", b"{}")["passed"])

    def test_configured_secret_match_without_named_token(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp).resolve() / "safe.json"; path.write_text('{"value":"synthetic-private-value"}')
            with patch.object(cli.scanner, "configured_secrets", return_value=["synthetic-private-value"]):
                report = cli.collect({"version": "sec-01-cloud-scope-v1", "artifacts": [{"path": str(path), "sha256": c.digest(path.read_bytes())}]})
            self.assertTrue(report["findings"])
            self.assertNotIn("synthetic-private-value", json.dumps(report))

    def test_receipt_is_not_authenticated_identity(self):
        raw = json.dumps({"version": "dat03-team-receipt-v1", "status": "TEAM_PILOT_VERIFIED",
            "recipient": "rosy179", "recipient_self_reported": True,
            "pilot_id": c.PILOT_ID, "handoff_sha256": c.HANDOFF_SHA,
            "feature_count": 279, "mainshock_count": 2, "coverage_status": "PARTIAL",
            "allowed_usage": "DEVELOPMENT_ONLY", "research_ready": False,
            "scientific_approval": False}).encode()
        self.assertFalse(c.receipt_summary(raw, c.HANDOFF_SHA, c.PILOT_ID)["identity_authenticated"])
        with self.assertRaises(ValueError):
            c.receipt_summary(raw, "0" * 64, c.PILOT_ID)
        with self.assertRaises(ValueError):
            c.receipt_summary(b'{"version":"dat03-range-worker-v1"}', c.HANDOFF_SHA, c.PILOT_ID)

    def test_redirect_rejected_before_forwarding_auth(self):
        with self.assertRaises(ValueError):
            cli.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://other.invalid")


if __name__ == "__main__":
    unittest.main()
