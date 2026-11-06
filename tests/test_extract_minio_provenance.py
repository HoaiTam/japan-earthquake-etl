"""Unit tests for extract-minio-provenance.py (DAT-03)."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/extract-minio-provenance.py"

spec = importlib.util.spec_from_file_location("extract_minio_provenance", MODULE_PATH)
extractor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(extractor)

# Also load pack-worker-provenance for end-to-end verification of extracted archive
pack_spec = importlib.util.spec_from_file_location("pack_worker_provenance", ROOT / "scripts/pack-worker-provenance.py")
pack_provenance = importlib.util.module_from_spec(pack_spec)
pack_spec.loader.exec_module(pack_provenance)


class TestExtractMinioProvenance(unittest.TestCase):
    def test_parse_uri_or_key_and_category(self):
        uri, key = extractor.parse_uri_or_key("s3://japan-earthquake/bronze/jma/year=2000/manifest.json")
        self.assertEqual(key, "bronze/jma/year=2000/manifest.json")
        self.assertEqual(extractor.resolve_category(key), "bronze_manifest")

        uri, key = extractor.parse_uri_or_key("silver/bundles/run=1/manifest.json")
        self.assertEqual(key, "silver/bundles/run=1/manifest.json")
        self.assertEqual(extractor.resolve_category(key), "silver_bundle_manifest")

        uri, key = extractor.parse_uri_or_key("silver/full-month/m=2020-01/registry.json")
        self.assertEqual(key, "silver/full-month/m=2020-01/registry.json")
        self.assertEqual(extractor.resolve_category(key), "registry_json")

    def test_unsafe_s3_key_rejected(self):
        with self.assertRaises(ValueError):
            extractor.parse_uri_or_key("s3://japan-earthquake/../secret/file.txt")
        with self.assertRaises(ValueError):
            extractor.parse_uri_or_key("bronze/../../etc/passwd")
        # Leading slash rejected before normalization
        with self.assertRaises(ValueError):
            extractor.parse_uri_or_key("/etc/passwd")

    def test_load_mapping_formats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)

            # Format 1: checksums dict
            p1 = tmp / "format1.json"
            dummy_sha = "a" * 64
            p1.write_text(json.dumps({
                "checksums": {"bronze/manifest_01.json": dummy_sha}
            }), encoding="utf-8")
            items1 = extractor.load_expected_mapping(p1)
            self.assertEqual(len(items1), 1)
            self.assertEqual(items1[0]["archive_path"], "bronze/manifest_01.json")

            # Format 2: objects list with s3_uri
            p2 = tmp / "format2.json"
            p2.write_text(json.dumps({
                "objects": [{"s3_uri": "s3://japan-earthquake/silver/bundles/01/manifest.json", "sha256": dummy_sha}]
            }), encoding="utf-8")
            items2 = extractor.load_expected_mapping(p2)
            self.assertEqual(len(items2), 1)
            self.assertEqual(items2[0]["category"], "silver_bundle_manifest")

            # Format 3: exact checkpoint schema with uri, sha256, kind
            p3 = tmp / "checkpoint.json"
            p3.write_text(json.dumps({
                "objects": [
                    {
                        "uri": "s3://japan-earthquake/silver/full-month/m=2020-01/registry.json",
                        "sha256": dummy_sha,
                        "kind": "registry_json",
                    }
                ]
            }), encoding="utf-8")
            items3 = extractor.load_expected_mapping(p3)
            self.assertEqual(len(items3), 1)
            self.assertEqual(items3[0]["category"], "registry_json")
            self.assertEqual(items3[0]["kind"], "registry_json")
            self.assertEqual(items3[0]["uri"], "s3://japan-earthquake/silver/full-month/m=2020-01/registry.json")

            # Format 4: flat dict
            p4 = tmp / "format4.json"
            p4.write_text(json.dumps({
                "silver/full-month/reg_01.json": dummy_sha
            }), encoding="utf-8")
            items4 = extractor.load_expected_mapping(p4)
            self.assertEqual(len(items4), 1)
            self.assertEqual(items4[0]["category"], "registry_json")

    def test_duplicate_key_in_mapping_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            p = tmp / "dup.json"
            p.write_text(json.dumps({
                "objects": [
                    {"uri": "s3://japan-earthquake/bronze/manifest_01.json", "sha256": "a" * 64},
                    {"uri": "s3://japan-earthquake/bronze/manifest_01.json", "sha256": "a" * 64},
                ]
            }), encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                extractor.load_expected_mapping(p)
            self.assertIn("DUPLICATE_KEY_IN_MAPPING", str(ctx.exception))

    def test_immutability_guard_rejects_overwrite_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            out_zip = tmp / "test.zip"
            out_zip.write_text("existing", encoding="utf-8")

            mapping_file = tmp / "map.json"
            mapping_file.write_text(json.dumps({}), encoding="utf-8")

            with self.assertRaises(FileExistsError):
                extractor.extract_and_pack_provenance(
                    owner="rosy179",
                    source_mapping_path=mapping_file,
                    output_zip=out_zip,
                    overwrite=False,
                )

    def test_extract_and_pack_mock_roundtrip_rosy179(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            mapping_file = tmp / "requirements.json"
            out_zip = tmp / "rosy179-provenance.zip"
            out_manifest = tmp / "required-provenance.json"

            # Create mock requirements for rosy179 with exact checkpoint uri/sha256/kind format
            mock_objects = []
            mock_bodies = {}

            # 37 bronze
            for i in range(1, 38):
                key = f"bronze/jma/year=2000/run={i:02d}/manifest.json"
                body = f"bronze_content_{i}".encode("utf-8")
                sha = hashlib.sha256(body).hexdigest()
                mock_objects.append({
                    "uri": f"s3://japan-earthquake/{key}",
                    "sha256": sha,
                    "kind": "bronze_manifest",
                })
                mock_bodies[key] = body

            # 23 silver bundles
            for i in range(1, 24):
                key = f"silver/bundles/run={i:02d}/manifest.json"
                body = f"silver_content_{i}".encode("utf-8")
                sha = hashlib.sha256(body).hexdigest()
                mock_objects.append({
                    "uri": f"s3://japan-earthquake/{key}",
                    "sha256": sha,
                    "kind": "silver_bundle_manifest",
                })
                mock_bodies[key] = body

            # 23 registry
            for i in range(1, 24):
                key = f"silver/full-month/m=2020-{i:02d}/registry.json"
                body = f"reg_content_{i}".encode("utf-8")
                sha = hashlib.sha256(body).hexdigest()
                mock_objects.append({
                    "uri": f"s3://japan-earthquake/{key}",
                    "sha256": sha,
                    "kind": "registry_json",
                })
                mock_bodies[key] = body

            self.assertEqual(len(mock_objects), 83)
            mapping_file.write_text(json.dumps({"objects": mock_objects}), encoding="utf-8")

            # Mock S3 client
            mock_s3 = MagicMock()
            def get_obj(Bucket, Key):
                if Key not in mock_bodies:
                    raise KeyError(f"Key not found: {Key}")
                m = MagicMock()
                m["Body"].read.return_value = mock_bodies[Key]
                return m
            mock_s3.get_object.side_effect = get_obj

            res = extractor.extract_and_pack_provenance(
                owner="rosy179",
                source_mapping_path=mapping_file,
                output_zip=out_zip,
                output_manifest=out_manifest,
                s3_client=mock_s3,
                overwrite=True,
            )

            self.assertEqual(res["status"], "PASS")
            self.assertEqual(res["total_objects"], 83)
            self.assertTrue(out_zip.is_file())
            self.assertTrue(out_manifest.is_file())

            # Now verify with pack_provenance tool in both modes
            # 1. Self integrity
            verify_res = pack_provenance.verify_provenance_archive("rosy179", out_zip)
            self.assertEqual(verify_res["status"], "PASS")
            self.assertEqual(verify_res["verification_level"], "self_integrity_only")
            self.assertEqual(verify_res["bronze_manifests"], 37)
            self.assertEqual(verify_res["silver_manifests"], 23)
            self.assertEqual(verify_res["registry_records"], 23)

            # 2. Trusted provenance using the checkpoint file directly!
            trusted_res = pack_provenance.verify_provenance_archive(
                owner="rosy179",
                zip_path=out_zip,
                expected_checksums=mapping_file,
                require_trusted=True,
            )
            self.assertEqual(trusted_res["status"], "PASS")
            self.assertEqual(trusted_res["verification_level"], "trusted_provenance")
            self.assertEqual(trusted_res["trusted_objects_verified"], 83)

    def test_extract_sha_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            mapping_file = tmp / "requirements.json"
            out_zip = tmp / "mismatch.zip"

            # Create 83 objects with 1 mismatched
            mock_data = {}
            for i in range(1, 38):
                mock_data[f"bronze/item_{i:02d}.json"] = "a" * 64
            for i in range(1, 24):
                mock_data[f"silver/bundles/item_{i:02d}.json"] = "b" * 64
            for i in range(1, 24):
                mock_data[f"silver/full-month/item_{i:02d}.json"] = "c" * 64

            mapping_file.write_text(json.dumps(mock_data), encoding="utf-8")

            mock_s3 = MagicMock()
            m = MagicMock()
            m["Body"].read.return_value = b"corrupted_content"
            mock_s3.get_object.return_value = m

            with self.assertRaises(ValueError) as ctx:
                extractor.extract_and_pack_provenance(
                    owner="rosy179",
                    source_mapping_path=mapping_file,
                    output_zip=out_zip,
                    s3_client=mock_s3,
                )
            self.assertIn("SHA_MISMATCH", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
