"""Offline DAT-03 capture/profile orchestration guards. No JMA/Docker calls."""
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("research_profiles", ROOT / "scripts/profile-research-archives.py")
profile = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(profile)


class ResearchProfilesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.folder = Path(self.temp.name)
        self.row = profile.inventory([2024])[0]

    def capture(self, status="200", url=None, content_type="application/zip", length=3):
        def curl(args, **kwargs):
            Path(args[args.index("--output") + 1]).write_bytes(b"zip")
            Path(args[args.index("--dump-header") + 1]).write_text(
                "HTTP/1.1 200 Connection established\r\n\r\nHTTP/2 200\r\n"
                f"Content-Type: {content_type}\r\nContent-Length: {length}\r\n"
                "Last-Modified: Thu, 03 Sep 2026 01:45:34 GMT\r\n\r\n")
            self.assertIn("--max-filesize", args)
            self.assertIn("--no-clobber", args)
            return subprocess.CompletedProcess(args, 0, status + "\n" + (url or self.row["source_url"]) + "\n")
        with patch.object(profile.subprocess, "run", side_effect=curl):
            return profile.capture_one(self.folder, self.row)

    def test_inventory_keeps_baseline_and_explicit_boundary_separate(self):
        rows = profile.inventory()
        self.assertEqual(list(range(2000, 2025)), [int(r["year"]) for r in rows])
        self.assertEqual("dat03-boundary-1.0", rows[-1]["inventory_version"])
        self.assertTrue(all(r["inventory_version"] == "1.0" for r in rows[:-1]))
        for years in ([], [2024, 2024], [1999], [2025]):
            with self.assertRaises(ValueError):
                profile.inventory(years)

    def test_root_cannot_be_workspace_staging_root_or_symlink(self):
        for path in (ROOT, ROOT / "staging", self.folder):
            with self.assertRaises(ValueError):
                profile.safe_root(path)
        link = self.folder / "link"
        link.symlink_to(ROOT / "staging", target_is_directory=True)
        with self.assertRaises(ValueError):
            profile.safe_root(link / "profile")
        self.assertEqual(ROOT / "staging/dat03-test-only", profile.safe_root(ROOT / "staging/dat03-test-only"))

    def test_capture_is_pinned_not_source_or_bronze_proof(self):
        capture = self.capture()
        self.assertEqual(profile.sha(self.folder / "h2024/h2024.zip"), capture["sha256"])
        self.assertTrue(capture["inventory_metadata_drift"])  # three-byte test artifact != observed real size
        self.assertFalse(capture["bronze_ready"])
        self.assertFalse(capture["source_authenticity_verified"])
        self.assertEqual("2026-09-03T01:45:34Z", capture["last_modified_utc"])
        with patch.object(profile.subprocess, "run", side_effect=AssertionError("unexpected network")):
            self.assertEqual(capture, profile.capture_one(self.folder, self.row))

    def test_capture_tamper_or_inventory_change_cannot_silently_repin(self):
        self.capture()
        (self.folder / "h2024/h2024.zip").write_bytes(b"bad")
        with self.assertRaisesRegex(ValueError, "PIN_MISMATCH"):
            profile.capture_one(self.folder, self.row)
        with self.assertRaisesRegex(ValueError, "PIN_MISMATCH"):
            profile.capture_one(self.folder, dict(self.row, inventory_version="other"))

    def test_incomplete_capture_is_preserved_not_redownloaded(self):
        (self.folder / "h2024").mkdir()
        with patch.object(profile.subprocess, "run", side_effect=AssertionError("unexpected network")):
            with self.assertRaisesRegex(ValueError, "INCOMPLETE_CAPTURE"):
                profile.capture_one(self.folder, self.row)

    def test_disk_reserve_uses_hard_bound_before_any_capture_write_or_network(self):
        usage = type("Usage", (), {"free": profile.RESERVE_BYTES + profile.MAX_BYTES - 1})()
        with patch.object(profile.shutil, "disk_usage", return_value=usage), patch.object(
                profile.subprocess, "run", side_effect=AssertionError("unexpected network")):
            with self.assertRaisesRegex(ValueError, "DISK_RESERVE"):
                profile.capture_one(self.folder, self.row)
        self.assertFalse((self.folder / "h2024").exists())

    def test_invalid_http_type_status_redirect_or_length_fail_without_receipt(self):
        for kwargs in ({"status": "206"}, {"url": "https://example.invalid/h2024.zip"},
                       {"content_type": "text/html"}, {"length": 2}):
            with self.subTest(kwargs=kwargs), tempfile.TemporaryDirectory() as temp:
                self.folder = Path(temp)
                with self.assertRaises(ValueError):
                    self.capture(**kwargs)
                self.assertFalse((self.folder / "h2024/capture.json").exists())

    def test_capture_symlink_file_is_rejected(self):
        self.capture()
        archive = self.folder / "h2024/h2024.zip"
        archive.rename(self.folder / "original.zip")
        archive.symlink_to(self.folder / "original.zip")
        with self.assertRaisesRegex(ValueError, "INCOMPLETE_CAPTURE"):
            profile.verified_capture(self.folder / "h2024", self.row)

    def test_profile_uses_exact_java_pin_and_reuse_checks_profile_hash(self):
        self.capture()
        jar = self.folder / "runner.jar"
        jar.write_bytes(b"test-runner")
        def java(args, **kwargs):
            self.assertEqual("java-test", args[0])
            self.assertIn("-Xmx512m", args)
            self.assertEqual(profile.PROFILE_CLASS, args[4])
            profile.write_new(Path(args[-1]), {"archive_sha256": profile.sha(self.folder / "h2024/h2024.zip")})
            return subprocess.CompletedProcess(args, 0)
        with patch.object(profile.sys, "platform", "linux"), patch.object(profile.subprocess, "run", side_effect=java):
            report, metrics = profile.profile_one(self.folder, self.row, "java-test", jar)
        self.assertFalse(metrics["full_etl_benchmark"])
        with patch.object(profile.subprocess, "run", side_effect=AssertionError("unexpected rerun")):
            self.assertEqual((report, metrics), profile.profile_one(self.folder, self.row, "java-test", jar))
        output = self.folder / "h2024" / f"profile-{profile.sha(jar)}.json"
        output.write_text("{}")
        with self.assertRaisesRegex(ValueError, "PIN_MISMATCH"):
            profile.profile_one(self.folder, self.row, "java-test", jar)

    def test_existing_summary_or_receipt_is_not_overwritten(self):
        target = self.folder / "existing.json"
        profile.write_new(target, {"pin": "first"})
        with self.assertRaises(FileExistsError):
            profile.write_new(target, {"pin": "second"})
        self.assertEqual({"pin": "first"}, json.loads(target.read_text()))

    def test_real_2016_evidence_recovers_exact_eighteen_hashes_not_dropped_rows(self):
        old = json.loads((ROOT / "docs/evidence/DAT-03_JMA_2016_PROFILE.json").read_text())
        new = json.loads((ROOT / "docs/evidence/DAT-03_JMA_2016_PROFILE_V12.json").read_text())
        fixture = json.loads((ROOT / "tests/fixtures/jma/depth-blank-2016-v1.json").read_text())
        self.assertEqual(old["archive_sha256"], new["archive_sha256"])
        self.assertEqual(old["archive_sha256"], fixture["archive_sha256"])
        self.assertEqual(18, old["counts"]["parser_rejected"])
        self.assertEqual(0, new["counts"]["parser_rejected"])
        self.assertEqual(old["counts"]["parser_accepted"] + 18, new["counts"]["parser_accepted"])
        self.assertEqual(old["counts"]["physical_records"], new["counts"]["physical_records"])
        def identity(record):
            return record["physical_line"], record["record_sha256"], record["native_depth_token"], record["location_precision"]
        expected = {identity(r): r["expected_depth_km"] for r in fixture["records"]}
        actual = {identity(r): r["depth_km"] for r in new["depth_blank_encoding_observations"]}
        self.assertEqual(18, len(actual))
        self.assertEqual(expected, actual)

    def test_real_profile_metadata_has_25_pinned_archives_and_no_false_gates(self):
        report = json.loads((ROOT / "docs/evidence/DAT-03_RESEARCH_ARCHIVE_PROFILES.json").read_text())
        self.assertEqual(list(range(2000, 2025)), [r["year"] for r in report["archives"]])
        for flag in ("research_ready", "coverage_verified", "gold_published", "source_authenticity_verified", "full_etl_benchmark"):
            self.assertIs(report[flag], False)
        physical = 0
        months = {}
        for item in report["archives"]:
            row = item["profile"]
            self.assertEqual(row["archive_sha256"], item["archive_sha256"])
            self.assertEqual(64, len(item["profile_sha256"]))
            self.assertEqual("slv-03-v1.2", row["parser_version"])
            counts = row["counts"]
            self.assertEqual(counts["physical_records"], counts["parser_accepted"] + counts["parser_rejected"])
            self.assertEqual(counts["parser_accepted"], sum(row["accepted_rows_by_utc_month"].values()))
            physical += counts["physical_records"]
            for month, count in row["accepted_rows_by_utc_month"].items():
                months[month] = months.get(month, 0) + count
        self.assertEqual(physical, report["totals"]["physical_records"])
        for name, start, end, expected_months in (("REPRODUCTION", "2000-01", "2018-10", 225), ("EXTENSION", "2018-10", "2024-01", 63)):
            expected = {m: n for m, n in months.items() if start <= m < end}
            period = report["accepted_utc_periods"][name]
            self.assertEqual(expected_months, len(expected))
            self.assertEqual(expected, period["months"])
            self.assertEqual(sum(expected.values()), period["rows"])
        boundary = report["archives"][-1]
        self.assertEqual("UTC_BOUNDARY_ONLY", boundary["role"])
        self.assertEqual(186, boundary["profile"]["accepted_rows_by_utc_month"]["2023-12"])


if __name__ == "__main__":
    unittest.main()
