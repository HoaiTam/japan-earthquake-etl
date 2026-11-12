"""Unit tests for DAT-03 worker supplemental lineage packaging and verification tool."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts/pack-worker-lineage.py"

spec = importlib.util.spec_from_file_location("pack_worker_lineage", MODULE_PATH)
lineage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lineage)


class TestPackWorkerLineage(unittest.TestCase):
    def test_spec_definitions(self):
        self.assertIn("ThanhTris", lineage.WORKER_LINEAGE_SPECS)
        self.assertIn("rosy179", lineage.WORKER_LINEAGE_SPECS)
        # ThanhTris: 19 runs * 6 phases = 114
        self.assertEqual(lineage.WORKER_LINEAGE_SPECS["ThanhTris"]["run_count"], 19)
        self.assertEqual(lineage.WORKER_LINEAGE_SPECS["ThanhTris"]["phases_per_run"], 6)
        self.assertEqual(lineage.WORKER_LINEAGE_SPECS["ThanhTris"]["total_required_objects"], 114)

        # rosy179: 23 runs * 6 phases = 138
        self.assertEqual(lineage.WORKER_LINEAGE_SPECS["rosy179"]["run_count"], 23)
        self.assertEqual(lineage.WORKER_LINEAGE_SPECS["rosy179"]["phases_per_run"], 6)
        self.assertEqual(lineage.WORKER_LINEAGE_SPECS["rosy179"]["total_required_objects"], 138)

    def test_check_raw_path_safety(self):
        self.assertTrue(lineage.check_raw_path_safety("silver/real-etl/run1/gold.json")[0])
        safe, reason = lineage.check_raw_path_safety("/etc/passwd")
        self.assertFalse(safe)
        self.assertEqual(reason, "ABSOLUTE_PATH_LEADING_SLASH")

        safe, reason = lineage.check_raw_path_safety("C:/secret.json")
        self.assertFalse(safe)
        self.assertEqual(reason, "ABSOLUTE_PATH_DRIVE_LETTER")

        safe, reason = lineage.check_raw_path_safety("silver/../../secret.json")
        self.assertFalse(safe)
        self.assertEqual(reason, "PATH_TRAVERSAL_DOT_DOT")

        safe, reason = lineage.check_raw_path_safety("silver//real-etl/run1.json")
        self.assertFalse(safe)
        self.assertEqual(reason, "EMPTY_PATH_SEGMENT_OR_CONSECUTIVE_SLASH")

    def test_pack_and_verify_roundtrip_synthetic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"
            spec_out = tmp_path / "lineage_spec.json"

            # Create mock runs for rosy179 (23 runs * 6 phases)
            for r in range(1, 24):
                run_id = f"orc07-test-run-{r:02d}"
                for phase in lineage.REQUIRED_PHASES:
                    p = src_dir / "silver" / "real-etl" / run_id / f"{phase}.json"
                    p.parent.mkdir(parents=True, exist_ok=True)
                    p.write_bytes(json.dumps({"run": run_id, "phase": phase, "status": "ok"}).encode("utf-8"))

            res_pack = lineage.pack_lineage_archive(
                owner="rosy179",
                source_input=src_dir,
                output_zip=zip_out,
                output_spec_file=spec_out,
                overwrite=True,
            )
            self.assertEqual(res_pack["status"], "PASS")
            self.assertEqual(res_pack["total_objects"], 138)
            self.assertEqual(res_pack["run_count"], 23)

            # Verify self integrity
            res_verify = lineage.verify_lineage_archive("rosy179", zip_out)
            self.assertEqual(res_verify["status"], "PASS")
            self.assertEqual(res_verify["verified_objects"], 138)
            self.assertEqual(res_verify["verified_runs"], 23)

            # Verify trusted mode
            res_trusted = lineage.verify_lineage_archive(
                owner="rosy179",
                zip_path=zip_out,
                expected_checksums=spec_out,
                require_trusted=True,
            )
            self.assertEqual(res_trusted["status"], "PASS")
            self.assertEqual(res_trusted["verification_level"], "trusted_lineage")
            self.assertEqual(res_trusted["trusted_objects_verified"], 138)

    def test_missing_phase_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"

            # 23 runs but missing publish phase in run 1
            for r in range(1, 24):
                run_id = f"orc07-test-run-{r:02d}"
                phases = list(lineage.REQUIRED_PHASES)
                if r == 1:
                    phases.remove("publish")
                for phase in phases:
                    p = src_dir / "silver" / "real-etl" / run_id / f"{phase}.json"
                    p.parent.mkdir(parents=True, exist_ok=True)
                    p.write_bytes(b"{}")

            with self.assertRaises(ValueError) as ctx:
                lineage.pack_lineage_archive(
                    owner="rosy179",
                    source_input=src_dir,
                    output_zip=zip_out,
                )
            self.assertIn("MISSING_PHASES", str(ctx.exception))

    def test_run_count_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"

            # Only 22 runs instead of 23
            for r in range(1, 23):
                run_id = f"orc07-test-run-{r:02d}"
                for phase in lineage.REQUIRED_PHASES:
                    p = src_dir / "silver" / "real-etl" / run_id / f"{phase}.json"
                    p.parent.mkdir(parents=True, exist_ok=True)
                    p.write_bytes(b"{}")

            with self.assertRaises(ValueError) as ctx:
                lineage.pack_lineage_archive(
                    owner="rosy179",
                    source_input=src_dir,
                    output_zip=zip_out,
                )
            self.assertIn("RUN_COUNT_MISMATCH", str(ctx.exception))

    def test_actual_rosy179_lineage_zip_if_present(self):
        """Test verification against actual generated rosy179 lineage zip if present."""
        actual_zip = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-dat03-lineage-metadata.zip"
        req_file = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-required-lineage.json"

        if actual_zip.is_file() and req_file.is_file():
            # 1. Self integrity
            res_self = lineage.verify_lineage_archive("rosy179", actual_zip)
            self.assertEqual(res_self["status"], "PASS")
            self.assertEqual(res_self["verified_objects"], 138)
            self.assertEqual(res_self["verified_runs"], 23)

            # 2. Trusted mode
            res_trusted = lineage.verify_lineage_archive(
                owner="rosy179",
                zip_path=actual_zip,
                expected_checksums=req_file,
                require_trusted=True,
            )
            self.assertEqual(res_trusted["status"], "PASS")
            self.assertEqual(res_trusted["verification_level"], "trusted_lineage")
            self.assertEqual(res_trusted["trusted_objects_verified"], 138)

    def test_symlink_file_rejected_in_pack(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"

            target_file = src_dir / "silver" / "real-etl" / "orc07-test-run-01" / "readiness.json"
            target_file.parent.mkdir(parents=True, exist_ok=True)
            target_file.write_bytes(b"{}")

            has_real_symlink = False
            link_file = src_dir / "silver" / "real-etl" / "orc07-test-run-01" / "bronze.json"
            try:
                link_file.symlink_to(target_file)
                has_real_symlink = True
            except OSError:
                pass

            if has_real_symlink:
                with self.assertRaises(ValueError) as ctx:
                    lineage.pack_lineage_archive(owner="rosy179", source_input=src_dir, output_zip=zip_out)
                self.assertIn("SYMLINK_REJECTED", str(ctx.exception))
            else:
                with patch("pathlib.Path.is_symlink", return_value=True):
                    with self.assertRaises(ValueError) as ctx:
                        lineage.pack_lineage_archive(owner="rosy179", source_input=src_dir, output_zip=zip_out)
                    self.assertIn("SYMLINK_REJECTED", str(ctx.exception))

    def test_symlink_parent_rejected_in_pack(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"
            src_dir.mkdir(parents=True, exist_ok=True)

            with patch("pathlib.Path.is_symlink", return_value=True):
                with self.assertRaises(ValueError) as ctx:
                    lineage.pack_lineage_archive(owner="rosy179", source_input=src_dir, output_zip=zip_out)
                self.assertIn("SYMLINK_REJECTED", str(ctx.exception))

    def test_path_escapes_source_root_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"

            f = src_dir / "silver" / "real-etl" / "run1" / "readiness.json"
            f.parent.mkdir(parents=True, exist_ok=True)
            f.write_bytes(b"{}")

            with patch("pathlib.Path.is_relative_to", return_value=False):
                with self.assertRaises(ValueError) as ctx:
                    lineage.pack_lineage_archive(owner="rosy179", source_input=src_dir, output_zip=zip_out)
                self.assertIn("PATH_ESCAPES_SOURCE_ROOT", str(ctx.exception))

    def test_symlink_zip_entry_rejected_in_verify(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bad_zip = tmp_path / "symlink_entry.zip"

            with zipfile.ZipFile(bad_zip, "w") as zf:
                info = zipfile.ZipInfo("silver/real-etl/run1/gold.json")
                info.external_attr = 0o120777 << 16
                zf.writestr(info, "target/path")

            res = lineage.verify_lineage_archive("rosy179", bad_zip)
            self.assertEqual(res["status"], "FAIL")
            self.assertIn("SYMLINK_ZIP_ENTRY_REJECTED", res["reason"])

    def test_invalid_namespace_layout_rejected_in_verify(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bad_zip = tmp_path / "bad_layout.zip"

            objects = []
            with zipfile.ZipFile(bad_zip, "w") as zf:
                for r in range(1, 24):
                    op = f"orc07-run-{r:02d}"
                    for phase in lineage.REQUIRED_PHASES:
                        if r == 1 and phase == "readiness":
                            name = "other/unrelated/silver/real-etl/orc07-run-01/readiness.json"
                        else:
                            name = f"silver/real-etl/{op}/{phase}.json"
                        zf.writestr(name, b"{}")
                        objects.append({
                            "path": name,
                            "sha256": lineage.compute_sha256(b"{}"),
                            "operation_id": op,
                            "phase": phase,
                        })
                manifest = {
                    "owner": "rosy179",
                    "objects": objects,
                }
                zf.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))

            res = lineage.verify_lineage_archive("rosy179", bad_zip)
            self.assertEqual(res["status"], "FAIL")
            self.assertIn("INVALID_NAMESPACE_LAYOUT", res["reason"])

    def test_invalid_operation_id_format_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            bad_zip = tmp_path / "bad_op.zip"

            objects = []
            with zipfile.ZipFile(bad_zip, "w") as zf:
                for r in range(1, 24):
                    op = f"orc07-run-{r:02d}" if r > 1 else "orc07-run!invalid"
                    for phase in lineage.REQUIRED_PHASES:
                        name = f"silver/real-etl/{op}/{phase}.json"
                        zf.writestr(name, b"{}")
                        objects.append({
                            "path": name,
                            "sha256": lineage.compute_sha256(b"{}"),
                            "operation_id": op,
                            "phase": phase,
                        })
                manifest = {
                    "owner": "rosy179",
                    "objects": objects,
                }
                zf.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))

            res = lineage.verify_lineage_archive("rosy179", bad_zip)
            self.assertEqual(res["status"], "FAIL")
            self.assertIn("INVALID_OPERATION_ID_FORMAT", res["reason"])

    def test_manifest_field_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            objects = []
            bad_zip = tmp_path / "mismatch.zip"
            with zipfile.ZipFile(bad_zip, "w") as zf:
                for r in range(1, 24):
                    op = f"orc07-run-{r:02d}"
                    for phase in lineage.REQUIRED_PHASES:
                        name = f"silver/real-etl/{op}/{phase}.json"
                        zf.writestr(name, b"{}")
                        objects.append({
                            "path": name,
                            "sha256": lineage.compute_sha256(b"{}"),
                            "operation_id": op,
                            "phase": phase,
                        })
                # Manifest has mismatched phase
                objects_mismatched = [dict(o) for o in objects]
                objects_mismatched[0]["phase"] = "wrong_phase"
                manifest = {"owner": "rosy179", "objects": objects_mismatched}
                zf.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))

            res = lineage.verify_lineage_archive("rosy179", bad_zip)
            self.assertEqual(res["status"], "FAIL")
            self.assertIn("MANIFEST_PHASE_MISMATCH", res["reason"])

    def test_frontier_114_spec_extraction(self):
        prov_zip = ROOT / "staging/dat03-range-handoff-rosy179/rosy179-dat03-provenance-metadata.zip"
        if prov_zip.is_file():
            with tempfile.TemporaryDirectory() as tmpdir:
                spec_out = Path(tmpdir) / "frontier114.json"
                res = lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    output_spec_file=spec_out,
                )
                self.assertEqual(res["status"], "PASS")
                self.assertEqual(res["total_frontier_objects"], 114)
                self.assertEqual(res["silver_dataset_manifests"], 92)
                self.assertEqual(res["gold_parent_commits"], 22)
                self.assertTrue(spec_out.is_file())

                with open(spec_out, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.assertEqual(data["version"], "dat03-frontier-114-spec-v1")
                self.assertEqual(data["summary"]["total_frontier_objects"], 114)
                self.assertEqual(data["summary"]["silver_dataset_manifest_count"], 92)
                self.assertEqual(data["summary"]["gold_parent_commit_count"], 22)
                self.assertEqual(len(data["frontier_objects"]), 114)
                self.assertEqual(len(data["checksums"]), 114)


if __name__ == "__main__":
    unittest.main()
