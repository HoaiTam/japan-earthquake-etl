"""Unit tests for DAT-03 follow-up metadata packaging and verification."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = ROOT / "scripts/pack-dat03-followup-lineage.py"
spec = importlib.util.spec_from_file_location("pack_followup", SCRIPT_PATH)
pack_followup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pack_followup)

RUN_ID = pack_followup.RUN_ID
EXPECTED_COMMIT_SHA = pack_followup.EXPECTED_COMMIT_SHA
EXPECTED_IDENTITY_PAYLOAD_SHA = pack_followup.EXPECTED_IDENTITY_PAYLOAD_SHA
extract_identity_payload_digest = pack_followup.extract_identity_payload_digest

ZIP_PATH = ROOT / "staging/dat03-lineage/ThanhTris-dat03-lineage-followup.zip"


class TestDat03FollowupPackage(unittest.TestCase):
    def test_zip_archive_exists_and_valid(self) -> None:
        self.assertTrue(ZIP_PATH.is_file(), f"ZIP archive not found at {ZIP_PATH}")
        with zipfile.ZipFile(ZIP_PATH, "r") as zf:
            members = set(zf.namelist())
            expected_members = {
                f"silver/gold-commits/{RUN_ID}/commit.json",
                f"silver/gold-commits/{RUN_ID}/identity.json",
                f"silver/gold-verification/{RUN_ID}/publication.json",
                "manifest.json",
            }
            self.assertEqual(members, expected_members)

    def test_commit_json_checksum(self) -> None:
        with zipfile.ZipFile(ZIP_PATH, "r") as zf:
            commit_bytes = zf.read(f"silver/gold-commits/{RUN_ID}/commit.json")
            sha = hashlib.sha256(commit_bytes).hexdigest()
            self.assertEqual(sha, EXPECTED_COMMIT_SHA)

    def test_identity_json_payload_digest(self) -> None:
        with zipfile.ZipFile(ZIP_PATH, "r") as zf:
            identity_bytes = zf.read(f"silver/gold-commits/{RUN_ID}/identity.json")
            internal_sha, recomputed = extract_identity_payload_digest(identity_bytes)
            self.assertEqual(recomputed, EXPECTED_IDENTITY_PAYLOAD_SHA)
            self.assertEqual(internal_sha, EXPECTED_IDENTITY_PAYLOAD_SHA)

    def test_publication_json_fields(self) -> None:
        with zipfile.ZipFile(ZIP_PATH, "r") as zf:
            pub_bytes = zf.read(f"silver/gold-verification/{RUN_ID}/publication.json")
            pub = json.loads(pub_bytes.decode("utf-8"))
            self.assertEqual(pub.get("verify_status"), "PASSED")
            self.assertTrue(pub.get("published"))
            self.assertEqual(pub.get("bundle_sha256"), EXPECTED_COMMIT_SHA)
            self.assertEqual(pub.get("identity_sha256"), EXPECTED_IDENTITY_PAYLOAD_SHA)

    def test_manifest_matches_contents(self) -> None:
        with zipfile.ZipFile(ZIP_PATH, "r") as zf:
            manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
            self.assertEqual(manifest.get("owner"), "ThanhTris")
            self.assertEqual(manifest.get("run_id"), RUN_ID)
            self.assertEqual(manifest.get("object_count"), 3)


if __name__ == "__main__":
    unittest.main()
