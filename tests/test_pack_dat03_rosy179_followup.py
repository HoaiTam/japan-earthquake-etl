"""Unit tests for DAT-03 follow-up metadata packaging and verification (rosy179)."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/pack-dat03-rosy179-followup.py"
spec = importlib.util.spec_from_file_location("pack_rosy_followup", SCRIPT_PATH)
pack_rosy_followup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pack_rosy_followup)

DEFAULT_REQUEST_PATH = pack_rosy_followup.DEFAULT_REQUEST_PATH
DEFAULT_OUTPUT_ZIP = pack_rosy_followup.DEFAULT_OUTPUT_ZIP
DEFAULT_RAW_DIR = pack_rosy_followup.DEFAULT_RAW_DIR
extract_identity_payload_digest = pack_rosy_followup.extract_identity_payload_digest
verify_followup_archive = pack_rosy_followup.verify_followup_archive
pack_followup_archive = pack_rosy_followup.pack_followup_archive


class TestDat03Rosy179Followup(unittest.TestCase):
    def setUp(self) -> None:
        self.request_data = json.loads(DEFAULT_REQUEST_PATH.read_text(encoding="utf-8"))

    def test_zip_archive_exists_and_valid(self) -> None:
        self.assertTrue(DEFAULT_OUTPUT_ZIP.is_file(), f"Archive not found: {DEFAULT_OUTPUT_ZIP}")
        with zipfile.ZipFile(DEFAULT_OUTPUT_ZIP, "r") as zf:
            self.assertIsNone(zf.testzip())
            names = zf.namelist()
            self.assertEqual(len(names), 48)
            self.assertIn("manifest.json", names)

    def test_all_24_pinned_objects_match(self) -> None:
        pinned = self.request_data.get("objects", [])
        self.assertEqual(len(pinned), 24)

        with zipfile.ZipFile(DEFAULT_OUTPUT_ZIP, "r") as zf:
            for item in pinned:
                rel_key = item["uri"].replace("s3://japan-earthquake/", "")
                self.assertIn(rel_key, zf.namelist())
                data = zf.read(rel_key)
                expected_sha = item["sha256"]

                if item["kind"] == "gold_identity":
                    internal_sha, recomputed = extract_identity_payload_digest(data)
                    self.assertEqual(internal_sha, expected_sha)
                    self.assertEqual(recomputed, expected_sha)
                elif item["kind"] == "gold_commit":
                    raw_sha = hashlib.sha256(data).hexdigest()
                    self.assertEqual(raw_sha, expected_sha)

    def test_all_23_publications_valid(self) -> None:
        pub_uris = self.request_data.get("unpinned_publication_uris", [])
        self.assertEqual(len(pub_uris), 23)

        with zipfile.ZipFile(DEFAULT_OUTPUT_ZIP, "r") as zf:
            for uri in pub_uris:
                rel_key = uri.replace("s3://japan-earthquake/", "")
                self.assertIn(rel_key, zf.namelist())
                data = zf.read(rel_key)
                pub_json = json.loads(data.decode("utf-8"))
                self.assertEqual(pub_json.get("verify_status"), "PASSED")
                self.assertTrue(pub_json.get("published"))
                self.assertIsNotNone(pub_json.get("identity_sha256"))
                self.assertIsNotNone(pub_json.get("bundle_sha256"))

    def test_manifest_metadata(self) -> None:
        with zipfile.ZipFile(DEFAULT_OUTPUT_ZIP, "r") as zf:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
            self.assertEqual(manifest.get("owner"), "rosy179")
            self.assertEqual(manifest.get("total_objects"), 47)
            self.assertEqual(len(manifest.get("objects", [])), 47)

    def test_extract_identity_payload_digest_logic(self) -> None:
        # Valid synthetic identity
        raw_payload = b'{"commit_id":"test-123","event_count":100}'
        raw_hash = hashlib.sha256(raw_payload).hexdigest()
        full_bytes = raw_payload[:-1] + f',\"identity_sha256\":\"{raw_hash}\"}}'.encode("ascii")

        internal, recomputed = extract_identity_payload_digest(full_bytes)
        self.assertEqual(internal, raw_hash)
        self.assertEqual(recomputed, raw_hash)

        # Invalid: missing field
        with self.assertRaises(ValueError) as ctx:
            extract_identity_payload_digest(b'{"no_identity":true}')
        self.assertIn("MISSING_IDENTITY_SHA256", str(ctx.exception))

        # Invalid: malformed suffix
        with self.assertRaises(ValueError) as ctx:
            extract_identity_payload_digest(b'{"identity_sha256":"abc","extra":1}')
        self.assertIn("INVALID_SERIALIZATION_SUFFIX", str(ctx.exception))

    def test_verify_followup_archive_tamper_detection(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_zip = Path(tmpdir) / "tampered.zip"
            # Create a zip with tampered content
            with zipfile.ZipFile(bad_zip, "w") as zf:
                zf.writestr("manifest.json", json.dumps({"owner": "rosy179"}))
                zf.writestr("silver/gold-commits/fake/identity.json", b'{"identity_sha256":"wrong"}')

            with self.assertRaises(ValueError):
                verify_followup_archive(zip_path=bad_zip, request_file=DEFAULT_REQUEST_PATH)

    def test_pack_followup_archive_roundtrip(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            out_zip = Path(tmpdir) / "test_followup.zip"
            res = pack_followup_archive(
                request_file=DEFAULT_REQUEST_PATH,
                output_zip=out_zip,
                source_dir=DEFAULT_RAW_DIR,
                overwrite=True,
            )
            self.assertEqual(res["status"], "SUCCESS")
            self.assertEqual(res["object_count"], 47)
            self.assertTrue(out_zip.is_file())
            sha_file = Path(tmpdir) / f"{out_zip.name}.sha256"
            self.assertTrue(sha_file.is_file())

            # Verify the newly packaged zip
            verify_res = verify_followup_archive(zip_path=out_zip, request_file=DEFAULT_REQUEST_PATH)
            self.assertEqual(verify_res["status"], "VERIFIED")
            self.assertEqual(verify_res["pinned_matched"], 24)
            self.assertEqual(verify_res["pubs_matched"], 23)


if __name__ == "__main__":
    unittest.main()
