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

    def test_one_file_repetition_bypass_rejected(self):
        """Regression test for Review P1: Manifest repeating same file 75 or 83 times must fail-closed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            zip_out = tmp / "bypass.zip"

            # Create ZIP with only 1 file
            manifest_items = []
            dummy_sha = provenance.compute_sha256(b"{}")
            for i in range(75):
                cat = "bronze_manifest" if i < 37 else ("silver_bundle_manifest" if i < 56 else "registry_json")
                manifest_items.append({
                    "category": cat,
                    "path": "same.json",
                    "archive_path": "same.json",
                    "sha256": dummy_sha,
                })

            manifest_content = {
                "owner": "ThanhTris",
                "objects": manifest_items,
            }
            with zipfile.ZipFile(zip_out, "w") as zf:
                zf.writestr("manifest.json", json.dumps(manifest_content))
                zf.writestr("same.json", b"{}")

            res = provenance.verify_provenance_archive("ThanhTris", zip_out)
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["reason"], "DUPLICATE_OBJECT_PATH")
            self.assertIn("duplicate_paths", res)

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

    def test_unsafe_path_traversal_rejected(self):
        """Regression test for Review P1: Objects with unsafe relative paths must fail."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            zip_out = tmp / "unsafe.zip"

            manifest_items = []
            for i in range(75):
                cat = "bronze_manifest" if i < 37 else ("silver_bundle_manifest" if i < 56 else "registry_json")
                path = f"../secret/file_{i}.json" if i == 0 else f"file_{i}.json"
                manifest_items.append({
                    "category": cat,
                    "path": path,
                    "sha256": "a" * 64,
                })

            with zipfile.ZipFile(zip_out, "w") as zf:
                zf.writestr("manifest.json", json.dumps({"owner": "ThanhTris", "objects": manifest_items}))
                for item in manifest_items:
                    zf.writestr(item["path"], b"abc")

            res = provenance.verify_provenance_archive("ThanhTris", zip_out)
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["reason"], "UNSAFE_OBJECT_PATH")

    def test_category_breakdown_mismatch_rejected(self):
        """Regression test for Review P1: ZIP with incorrect category breakdown fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            zip_out = tmp / "breakdown.zip"

            manifest_items = []
            # 41 bronze, 21 silver, 21 reg (the old wrong spec for rosy179)
            for i in range(83):
                cat = "bronze_manifest" if i < 41 else ("silver_bundle_manifest" if i < 62 else "registry_json")
                manifest_items.append({
                    "category": cat,
                    "path": f"item_{i:02d}.json",
                    "sha256": provenance.compute_sha256(b"content"),
                })

            with zipfile.ZipFile(zip_out, "w") as zf:
                zf.writestr("manifest.json", json.dumps({"owner": "rosy179", "objects": manifest_items}))
                for item in manifest_items:
                    zf.writestr(item["path"], b"content")

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
