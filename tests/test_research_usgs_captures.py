"""Offline control-plane guards only. Mock bytes are never research/source evidence."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("research_usgs", ROOT / "scripts/capture-research-usgs.py")
c = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(c)


class ResearchUsgsCapturesTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup); self.root = Path(temp.name).resolve()

    def curl(self, args, **kwargs):
        Path(args[args.index("--output") + 1]).write_bytes(b"not-a-real-event-payload")
        Path(args[args.index("--dump-header") + 1]).write_text("HTTP/2 200\r\n\r\n")
        self.assertIn("--no-clobber", args); self.assertNotIn("--retry", args)
        self.assertNotIn("--location", args)
        self.assertEqual("60", args[args.index("--max-time") + 1])
        return subprocess.CompletedProcess(args, 0, "200\n" + args[-1] + "\napplication/json\n")

    def test_exact_twelve_half_open_utc_queries_with_seed_and_halo(self):
        self.assertEqual(12, len(c.MONTHS))
        for month in c.MONTHS:
            q = parse_qs(urlparse(c.scope(month)["url"]).query)
            self.assertEqual(11, len(q)); self.assertEqual(["1"], q["offset"]); self.assertEqual(["20000"], q["limit"])
            self.assertNotIn("minmagnitude", q); self.assertNotIn("updatedafter", q)
        self.assertEqual("2023-01-01T00:00:00Z", c.scope("2023-01")["window_start_utc"])
        self.assertEqual("2024-01-01T00:00:16Z", c.scope("2023-12")["window_end_utc_exclusive"])
        self.assertEqual(["2024-01-01T00:00:15.999Z"], parse_qs(urlparse(c.scope("2023-12")["url"]).query)["endtime"])
        for month in ("2022-12", "2024-01", "2023-1", "2023-13"):
            with self.assertRaises(ValueError): c.scope(month)

    def test_capture_never_requeries_or_overwrites_a_verified_existing_pin(self):
        with patch.object(c.subprocess, "run", side_effect=self.curl):
            original = c.capture_one(self.root, "2023-01")
        with patch.object(c.subprocess, "run", side_effect=AssertionError("unexpected network")):
            self.assertEqual(original, c.capture_one(self.root, "2023-01"))
        (self.root / "2023-01/response.geojson").write_bytes(b"tamper")
        with self.assertRaisesRegex(ValueError, "PIN_CHANGED"): c.capture_one(self.root, "2023-01")

    def test_reuse_december_preserves_original_retrieval_and_has_no_network(self):
        source = self.root / "old"; source.mkdir(); raw = source / "usgs-december2023.geojson"; raw.write_bytes(b"old-public-bytes")
        meta = {"url": c.scope("2023-12")["url"], "http_status": 200, "content_type": "application/json", "bytes": raw.stat().st_size,
                "sha256": c.r.profile.sha(raw), "retrieved_at_utc": "2026-10-24T14:19:35Z"}
        c.r.save(source / "capture.json", meta)
        with patch.object(c.subprocess, "run", side_effect=AssertionError("unexpected network")):
            result = c.capture_one(self.root, "2023-12", source)
        self.assertEqual(meta["retrieved_at_utc"], result["retrieved_at_utc"])
        self.assertEqual(meta["sha256"], result["sha256"])
        self.assertEqual(c.r.profile.sha(source / "capture.json"), result["origin_capture_receipt_sha256"])
        self.assertEqual("REUSED_PUBLIC_CAPTURE", result["capture_method"])

    def test_incomplete_capture_and_symlink_are_preserved_not_regenerated(self):
        folder = self.root / "2023-01"; folder.mkdir()
        with self.assertRaises(ValueError): c.capture_one(self.root, "2023-01")
        self.assertEqual([], list(folder.iterdir()))
        folder.rmdir(); folder.symlink_to(self.root)
        with self.assertRaisesRegex(ValueError, "SYMLINK"): c.capture_one(self.root, "2023-01")

    def test_http_failure_or_wrong_redirect_keeps_bytes_without_success_receipt(self):
        def wrong(args, **kwargs):
            result = self.curl(args, **kwargs)
            return subprocess.CompletedProcess(args, 0, result.stdout.replace("200\n", "206\n", 1))
        with patch.object(c.subprocess, "run", side_effect=wrong):
            with self.assertRaisesRegex(ValueError, "HTTP_INVALID"): c.capture_one(self.root, "2023-01")
        self.assertTrue((self.root / "2023-01/response.geojson").is_file())
        self.assertFalse((self.root / "2023-01/capture.json").exists())

    def test_disk_gate_precedes_directory_and_network(self):
        with patch.object(c.shutil, "disk_usage", return_value=type("Usage", (), {"free": 1024**3})()), patch.object(
                c.subprocess, "run", side_effect=AssertionError("unexpected network")):
            with self.assertRaisesRegex(ValueError, "DISK_RESERVE"): c.capture_one(self.root, "2023-01")
        self.assertFalse((self.root / "2023-01").exists())

    def test_java_check_is_bounded_and_must_not_promote_source_gates(self):
        with patch.object(c.subprocess, "run", side_effect=self.curl): meta = c.capture_one(self.root, "2023-01")
        jar = self.root / "runner.jar"; jar.write_bytes(b"fixture-code"); pin = c.r.profile.sha(jar)
        checked = dict(c.scope("2023-01"), version="dat03-usgs-capture-check-v1", status="CAPTURE_STRUCTURE_VERIFIED",
                       raw_sha256=meta["sha256"], feature_count=0, bronze_ready=False, coverage_verified=False, research_ready=False)
        checked.pop("url")
        with patch.object(c.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(checked))) as call:
            self.assertEqual(checked, c.validate(self.root / "2023-01", "2023-01", "java-test", jar, pin))
            self.assertEqual(45, call.call_args.kwargs["timeout"]); self.assertIn("-Xmx256m", call.call_args.args[0])
        checked["coverage_verified"] = True
        with patch.object(c.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, json.dumps(checked))):
            with self.assertRaisesRegex(ValueError, "JAVA_CHECK_INVALID"): c.validate(self.root / "2023-01", "2023-01", "java-test", jar, pin)


if __name__ == "__main__": unittest.main()
