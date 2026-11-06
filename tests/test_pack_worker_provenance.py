"""Unit tests for DAT-03 worker provenance packaging and verification tool."""

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

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

    def test_generate_plan_rosy179(self):
        plan = provenance.generate_expected_provenance_manifest("rosy179")
        self.assertEqual(plan["owner"], "rosy179")
        self.assertEqual(plan["month_count"], 63)
        self.assertEqual(plan["total_required_objects"], 83)
        self.assertEqual(len(plan["required_objects"]), 83)
        self.assertEqual(plan["counts_by_category"]["bronze_manifest"], 41)
        self.assertEqual(plan["counts_by_category"]["silver_bundle_manifest"], 21)
        self.assertEqual(plan["counts_by_category"]["registry_json"], 21)

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
            for item in plan["required_objects"]:
                fpath = src_dir / item["path"]
                fpath.parent.mkdir(parents=True, exist_ok=True)
                with open(fpath, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"object": item["path"], "status": "COMMITTED"}))

            # Pack
            res_pack = provenance.pack_provenance_archive("rosy179", src_dir, zip_out)
            self.assertEqual(res_pack["status"], "PASS")
            self.assertEqual(res_pack["total_objects"], 83)
            self.assertTrue(zip_out.is_file())

            # Verify
            res_verify = provenance.verify_provenance_archive("rosy179", zip_out)
            self.assertEqual(res_verify["status"], "PASS")
            self.assertEqual(res_verify["verified_objects"], 83)
            self.assertEqual(res_verify["bronze_manifests"], 41)
            self.assertEqual(res_verify["silver_manifests"], 21)
            self.assertEqual(res_verify["registry_records"], 21)

    def test_pack_missing_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "test.zip"

            # Create only 10 objects instead of 83
            plan = provenance.generate_expected_provenance_manifest("rosy179")
            for item in plan["required_objects"][:10]:
                fpath = src_dir / item["path"]
                fpath.parent.mkdir(parents=True, exist_ok=True)
                with open(fpath, "w", encoding="utf-8") as f:
                    f.write("test_content")

            res = provenance.pack_provenance_archive("rosy179", src_dir, zip_out)
            self.assertEqual(res["status"], "FAIL")
            self.assertEqual(res["error"], "INCOMPLETE_PROVENANCE_OBJECTS")
            self.assertEqual(res["missing_count"], 73)
            self.assertFalse(zip_out.exists())

    def test_verify_owner_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "test.zip"

            # Create valid 75 objects for ThanhTris
            plan = provenance.generate_expected_provenance_manifest("ThanhTris")
            for item in plan["required_objects"]:
                fpath = src_dir / item["path"]
                fpath.parent.mkdir(parents=True, exist_ok=True)
                with open(fpath, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"test": 1}))

            provenance.pack_provenance_archive("ThanhTris", src_dir, zip_out)

            # Attempt to verify ThanhTris zip as rosy179
            res = provenance.verify_provenance_archive("rosy179", zip_out)
            self.assertEqual(res["status"], "FAIL")
            self.assertIn("OWNER_MISMATCH", res["reason"])


if __name__ == "__main__":
    unittest.main()
