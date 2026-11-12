"""Unit tests for DAT-03 worker supplemental lineage packaging and verification tool."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
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


if __name__ == "__main__":
    unittest.main()
