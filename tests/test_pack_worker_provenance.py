"""Unit tests for DAT-03 worker provenance packaging and verification tool."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/pack-worker-provenance.py"

spec = importlib.util.spec_from_file_location("pack_worker_provenance", MODULE_PATH)
provenance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(provenance)


class TestPackWorkerProvenance(unittest.TestCase):
    def test_spec_definitions(self):
        self.assertIn("ThanhTris", provenance.WORKER_PROVENANCE_SPECS)
        self.assertIn("rosy179", provenance.WORKER_PROVENANCE_SPECS)
        self.assertEqual(provenance.WORKER_PROVENANCE_SPECS["ThanhTris"]["total_required_objects"], 75)
        self.assertEqual(provenance.WORKER_PROVENANCE_SPECS["rosy179"]["total_required_objects"], 83)

        # ThanhTris: 37 Bronze, 19 Silver, 19 registry
        self.assertEqual(provenance.WORKER_PROVENANCE_SPECS["ThanhTris"]["bronze_manifest_count"], 37)
        self.assertEqual(provenance.WORKER_PROVENANCE_SPECS["ThanhTris"]["silver_manifest_count"], 19)
        self.assertEqual(provenance.WORKER_PROVENANCE_SPECS["ThanhTris"]["registry_json_count"], 19)

        # rosy179: 37 Bronze, 23 Silver, 23 registry
        self.assertEqual(provenance.WORKER_PROVENANCE_SPECS["rosy179"]["bronze_manifest_count"], 37)
        self.assertEqual(provenance.WORKER_PROVENANCE_SPECS["rosy179"]["silver_manifest_count"], 23)
        self.assertEqual(provenance.WORKER_PROVENANCE_SPECS["rosy179"]["registry_json_count"], 23)

    def test_check_raw_path_safety(self):
        # Safe relative paths
        self.assertTrue(provenance.check_raw_path_safety("bronze/manifest_01.json")[0])
        self.assertTrue(provenance.check_raw_path_safety("silver/bundles/run=1/manifest.json")[0])

        # Absolute paths
        safe, reason = provenance.check_raw_path_safety("/etc/passwd")
        self.assertFalse(safe)
        self.assertEqual(reason, "ABSOLUTE_PATH_LEADING_SLASH")

        safe, reason = provenance.check_raw_path_safety("\\windows\\system32")
        self.assertFalse(safe)
        self.assertEqual(reason, "ABSOLUTE_PATH_LEADING_SLASH")

        safe, reason = provenance.check_raw_path_safety("C:/secret.json")
        self.assertFalse(safe)
        self.assertEqual(reason, "ABSOLUTE_PATH_DRIVE_LETTER")

        # Path traversal
        safe, reason = provenance.check_raw_path_safety("bronze/../../secret.json")
        self.assertFalse(safe)
        self.assertEqual(reason, "PATH_TRAVERSAL_DOT_DOT")

        # Empty segments / double slashes
        safe, reason = provenance.check_raw_path_safety("bronze//manifest.json")
        self.assertFalse(safe)
        self.assertEqual(reason, "EMPTY_PATH_SEGMENT_OR_CONSECUTIVE_SLASH")

    def test_generate_plan_rosy179(self):
        plan = provenance.generate_expected_provenance_manifest("rosy179")
        self.assertEqual(plan["owner"], "rosy179")
        self.assertEqual(plan["month_count"], 63)
        self.assertEqual(plan["total_required_objects"], 83)
        self.assertEqual(len(plan["required_objects"]), 83)
        self.assertEqual(plan["counts_by_category"]["bronze_manifest"], 37)
        self.assertEqual(plan["counts_by_category"]["silver_bundle_manifest"], 23)
        self.assertEqual(plan["counts_by_category"]["registry_json"], 23)

    def test_generate_plan_thanhtris(self):
        plan = provenance.generate_expected_provenance_manifest("ThanhTris")
        self.assertEqual(plan["owner"], "ThanhTris")
        self.assertEqual(plan["month_count"], 62)
        self.assertEqual(plan["total_required_objects"], 75)
        self.assertEqual(len(plan["required_objects"]), 75)
        self.assertEqual(plan["counts_by_category"]["bronze_manifest"], 37)
        self.assertEqual(plan["counts_by_category"]["silver_bundle_manifest"], 19)
        self.assertEqual(plan["counts_by_category"]["registry_json"], 19)

    def test_pack_and_verify_roundtrip_rosy179(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "rosy179-dat03-provenance-metadata.zip"

            # Create mock 83 objects
            plan = provenance.generate_expected_provenance_manifest("rosy179")
            expected_checksums = {}
            for item in plan["required_objects"]:
                fpath = src_dir / item["path"]
                fpath.parent.mkdir(parents=True, exist_ok=True)
                content = json.dumps({"object": item["path"], "status": "COMMITTED"}).encode("utf-8")
                fpath.write_bytes(content)
                expected_checksums[item["path"]] = provenance.compute_sha256(content)

            # Pack
            res_pack = provenance.pack_provenance_archive("rosy179", src_dir, zip_out)
            self.assertEqual(res_pack["status"], "PASS")
            self.assertEqual(res_pack["total_objects"], 83)
            self.assertTrue(zip_out.is_file())

            # 1. Verify self-integrity
            res_verify = provenance.verify_provenance_archive("rosy179", zip_out)
            self.assertEqual(res_verify["status"], "PASS")
            self.assertEqual(res_verify["verification_level"], "self_integrity_only")
            self.assertEqual(res_verify["verified_objects"], 83)
            self.assertEqual(res_verify["bronze_manifests"], 37)
            self.assertEqual(res_verify["silver_manifests"], 23)
            self.assertEqual(res_verify["registry_records"], 23)

            # 2. Verify trusted provenance with complete external mapping
            res_trusted = provenance.verify_provenance_archive(
                owner="rosy179",
                zip_path=zip_out,
                expected_checksums=expected_checksums,
                require_trusted=True,
            )
            self.assertEqual(res_trusted["status"], "PASS")
            self.assertEqual(res_trusted["verification_level"], "trusted_provenance")
            self.assertEqual(res_trusted["trusted_objects_verified"], 83)

    def test_duplicate_zip_member_same_bytes_rejected(self):
        """Regression test for Review P1/P2: ZIP with duplicate member name having same bytes fails before reading."""
        import warnings
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            zip_out = tmp / "dup_same.zip"

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with zipfile.ZipFile(zip_out, "w") as zf:
                    zf.writestr("manifest.json", json.dumps({"owner": "rosy179", "objects": []}))
                    zf.writestr("item_01.json", b"same_content")
                    zf.writestr("item_01.json", b"same_content")  # Duplicate member

            res = provenance.verify_provenance_archive("rosy179", zip_out)
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["reason"], "DUPLICATE_ZIP_MEMBER")
            self.assertIn("item_01.json", res["duplicate_members"])

    def test_duplicate_zip_member_diff_bytes_rejected(self):
        """Regression test for Review P1/P2: ZIP with duplicate member name having different bytes fails before reading."""
        import warnings
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            zip_out = tmp / "dup_diff.zip"

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                with zipfile.ZipFile(zip_out, "w") as zf:
                    zf.writestr("manifest.json", json.dumps({"owner": "rosy179", "objects": []}))
                    zf.writestr("item_01.json", b"content_a")
                    zf.writestr("item_01.json", b"content_b")  # Duplicate member with different bytes

            res = provenance.verify_provenance_archive("rosy179", zip_out)
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["reason"], "DUPLICATE_ZIP_MEMBER")
            self.assertIn("item_01.json", res["duplicate_members"])

    def test_zip_leading_slash_member_rejected_raw(self):
        """Regression test for Review P1/P2: ZIP member starting with '/' must fail before normalization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            zip_out = tmp / "leading_slash.zip"

            with zipfile.ZipFile(zip_out, "w") as zf:
                zf.writestr("manifest.json", json.dumps({"owner": "rosy179", "objects": []}))
                zf.writestr("/etc/passwd", b"root:x:0:0")

            res = provenance.verify_provenance_archive("rosy179", zip_out)
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["reason"], "UNSAFE_ZIP_MEMBER")
            self.assertEqual(res["error"], "ABSOLUTE_PATH_LEADING_SLASH")

    def test_checkpoint_schema_uri_sha256_kind_trusted_verification(self):
        """Regression test for Review P1: Verifier must natively support checkpoint schema with uri/sha256/kind."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            src_dir = tmp / "staging"
            zip_out = tmp / "rosy179-provenance.zip"
            checkpoint_file = tmp / "rosy179-checkpoint-pinned.json"

            # Create mock 83 objects
            plan = provenance.generate_expected_provenance_manifest("rosy179")
            checkpoint_objects = []
            for item in plan["required_objects"]:
                fpath = src_dir / item["path"]
                fpath.parent.mkdir(parents=True, exist_ok=True)
                content = json.dumps({"object": item["path"], "owner": "rosy179"}).encode("utf-8")
                fpath.write_bytes(content)
                sha = provenance.compute_sha256(content)

                # Format matching exact checkpoint format: uri, sha256, kind
                checkpoint_objects.append({
                    "uri": f"s3://japan-earthquake/{item['path']}",
                    "sha256": sha,
                    "kind": item["category"],
                })

            checkpoint_file.write_text(json.dumps({"objects": checkpoint_objects}, indent=2), encoding="utf-8")

            # Pack
            res_pack = provenance.pack_provenance_archive("rosy179", src_dir, zip_out)
            self.assertEqual(res_pack["status"], "PASS")

            # Verify with checkpoint file containing uri/sha256/kind schema
            res_trusted = provenance.verify_provenance_archive(
                owner="rosy179",
                zip_path=zip_out,
                expected_checksums=checkpoint_file,
                require_trusted=True,
            )
            self.assertEqual(res_trusted["status"], "PASS")
            self.assertEqual(res_trusted["verification_level"], "trusted_provenance")
            self.assertEqual(res_trusted["trusted_objects_verified"], 83)

    def test_missing_trusted_checksum_key_fails_closed(self):
        """Regression test for Review P1: Verification with missing keys in external checksums must fail-closed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            src_dir = tmp / "staging"
            zip_out = tmp / "test.zip"

            plan = provenance.generate_expected_provenance_manifest("ThanhTris")
            for item in plan["required_objects"]:
                fpath = src_dir / item["path"]
                fpath.parent.mkdir(parents=True, exist_ok=True)
                fpath.write_bytes(b"content")

            provenance.pack_provenance_archive("ThanhTris", src_dir, zip_out)

            # Incomplete trusted mapping: only 1 key instead of 75
            incomplete_map = {"bronze/manifest_01.json": provenance.compute_sha256(b"content")}
            res = provenance.verify_provenance_archive(
                owner="ThanhTris",
                zip_path=zip_out,
                expected_checksums=incomplete_map,
                require_trusted=True,
            )
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["reason"], "INCOMPLETE_TRUSTED_CHECKSUMS")
            self.assertEqual(res["missing_count"], 74)

    def test_category_breakdown_mismatch_rejected(self):
        """Regression test for Review P1: ZIP with incorrect category breakdown fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            zip_out = tmp / "breakdown.zip"

            manifest_items = []
            # 41 bronze, 21 silver, 21 reg (the old wrong spec for rosy179)
            with zipfile.ZipFile(zip_out, "w") as zf:
                for i in range(83):
                    cat = "bronze_manifest" if i < 41 else ("silver_bundle_manifest" if i < 62 else "registry_json")
                    fname = f"item_{i:02d}.json"
                    manifest_items.append({
                        "category": cat,
                        "path": fname,
                        "sha256": provenance.compute_sha256(b"content"),
                    })
                    zf.writestr(fname, b"content")
                zf.writestr("manifest.json", json.dumps({"owner": "rosy179", "objects": manifest_items}))

            res = provenance.verify_provenance_archive("rosy179", zip_out)
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["reason"], "CATEGORY_BREAKDOWN_MISMATCH")

    def test_require_trusted_without_file_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            src_dir = tmp / "staging"
            zip_out = tmp / "test.zip"

            plan = provenance.generate_expected_provenance_manifest("ThanhTris")
            for item in plan["required_objects"]:
                fpath = src_dir / item["path"]
                fpath.parent.mkdir(parents=True, exist_ok=True)
                fpath.write_bytes(b"content")

            provenance.pack_provenance_archive("ThanhTris", src_dir, zip_out)

            # Passing require_trusted=True but expected_checksums=None
            res = provenance.verify_provenance_archive(
                owner="ThanhTris",
                zip_path=zip_out,
                expected_checksums=None,
                require_trusted=True,
            )
            self.assertEqual(res["status"], "FAIL")
            self.assertIn("REQUIRED_TRUSTED_CHECKSUMS_MISSING", res["reason"])

    def test_actual_rosy179_zip_file_if_present(self):
        """Test verification against actual delivered rosy179 zip if present in repo staging."""
        actual_zip = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-dat03-provenance-metadata.zip"
        req_file = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-required-provenance.json"

        if actual_zip.is_file() and req_file.is_file():
            # 1. Self integrity
            res_self = provenance.verify_provenance_archive("rosy179", actual_zip)
            self.assertEqual(res_self["status"], "PASS")
            self.assertEqual(res_self["verified_objects"], 83)
            self.assertEqual(res_self["bronze_manifests"], 37)
            self.assertEqual(res_self["silver_manifests"], 23)
            self.assertEqual(res_self["registry_records"], 23)
            self.assertEqual(res_self["zip_sha256"], "f818ba4697d3915376420ad66cd040c542853333a31bc2b4e4a5d0b4115d859e")

            # 2. Trusted provenance
            res_trusted = provenance.verify_provenance_archive(
                owner="rosy179",
                zip_path=actual_zip,
                expected_checksums=req_file,
                require_trusted=True,
            )
            self.assertEqual(res_trusted["status"], "PASS")
            self.assertEqual(res_trusted["verification_level"], "trusted_provenance")
            self.assertEqual(res_trusted["trusted_objects_verified"], 83)


if __name__ == "__main__":
    unittest.main()
