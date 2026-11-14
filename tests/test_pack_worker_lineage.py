"""Unit tests for DAT-03 worker supplemental lineage packaging and verification tool."""

from __future__ import annotations

import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch
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
            tmp_path = Path(tmpdir).resolve()
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
            tmp_path = Path(tmpdir).resolve()
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
            tmp_path = Path(tmpdir).resolve()
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
            tmp_path = Path(tmpdir).resolve()
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
                link_file.write_bytes(b"{}")

            if has_real_symlink:
                with self.assertRaises(ValueError) as ctx:
                    lineage.pack_lineage_archive(owner="rosy179", source_input=src_dir, output_zip=zip_out)
                self.assertIn("SYMLINK_REJECTED", str(ctx.exception))
            else:
                orig_is_symlink = Path.is_symlink
                def mock_is_symlink(self):
                    return Path(self) == Path(link_file) or orig_is_symlink(self)
                with patch.object(Path, "is_symlink", mock_is_symlink):
                    with self.assertRaises(ValueError) as ctx:
                        lineage.pack_lineage_archive(owner="rosy179", source_input=src_dir, output_zip=zip_out)
                    self.assertIn("SYMLINK_REJECTED", str(ctx.exception))

    def test_symlink_parent_rejected_in_pack(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"
            src_dir.mkdir(parents=True, exist_ok=True)

            parent_dir = src_dir / "silver" / "real-etl"
            parent_dir.mkdir(parents=True, exist_ok=True)
            orig_is_symlink = Path.is_symlink
            def mock_parent_symlink(self):
                return Path(self) == Path(parent_dir) or orig_is_symlink(self)
            with patch.object(Path, "is_symlink", mock_parent_symlink):
                with self.assertRaises(ValueError) as ctx:
                    lineage.pack_lineage_archive(owner="rosy179", source_input=src_dir, output_zip=zip_out)
                self.assertIn("SYMLINK_REJECTED", str(ctx.exception))

    def test_path_escapes_source_root_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
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
            tmp_path = Path(tmpdir).resolve()
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
            tmp_path = Path(tmpdir).resolve()
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
            tmp_path = Path(tmpdir).resolve()
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
            tmp_path = Path(tmpdir).resolve()
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
                spec_out = Path(tmpdir).resolve() / "frontier114.json"
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


    def _create_synthetic_provenance_zip(
        self,
        zip_path: Path,
        num_runs: int = 23,
        num_registries: Optional[int] = None,
        duplicate_member: bool = False,
        conflicting_sha: bool = False,
        duplicate_frontier_obj: bool = False,
        invalid_uri: bool = False,
        invalid_sha: bool = False,
        missing_dataset: bool = False,
        invalid_commit_key: bool = False,
        invalid_commit_sha: bool = False,
        include_bronze: bool = True,
    ) -> str:
        if num_registries is None:
            num_registries = num_runs

        with zipfile.ZipFile(zip_path, "w") as zf:
            for r in range(1, num_runs + 1):
                run_id = f"orc07-mock-run-{r:02d}"
                datasets = {}
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    if missing_dataset and r == 1 and ds == "source_link":
                        continue
                    uri = f"s3://japan-earthquake/silver/bundles/{run_id}/{ds}/manifest.json"
                    sha = lineage.compute_sha256(f"{run_id}-{ds}".encode("utf-8"))
                    if invalid_uri and r == 1 and ds == "source_observation":
                        uri = "https://untrusted.com/manifest.json"
                    if invalid_sha and r == 1 and ds == "source_observation":
                        sha = "invalid-not-64-hex"
                    datasets[ds] = {
                        "manifest_uri": uri,
                        "sha256": sha,
                        "record_count": 10,
                    }
                bm = {"run_id": run_id, "datasets": datasets}
                zf.writestr(f"silver/bundles/{run_id}/manifest.json", json.dumps(bm).encode("utf-8"))

            for r in range(1, num_registries + 1):
                run_id = f"orc07-mock-run-{r:02d}"
                reg: dict = {"run_id": run_id}
                if r > 1:
                    parent_run = f"orc07-mock-run-{r-1:02d}"
                    commit_key = f"gold-commits/{parent_run}/commit.json"
                    commit_sha = lineage.compute_sha256(f"parent-{parent_run}".encode("utf-8"))
                    if invalid_commit_key and r == 2:
                        commit_key = "invalid/layout/commit.json"
                    if invalid_commit_sha and r == 2:
                        commit_sha = "short-sha"
                    if conflicting_sha and r == 3:
                        parent_run = "orc07-mock-run-01"
                        commit_key = f"gold-commits/{parent_run}/commit.json"
                        commit_sha = lineage.compute_sha256(b"conflict-sha")
                    elif duplicate_frontier_obj and r == 3:
                        parent_run = "orc07-mock-run-01"
                        commit_key = f"gold-commits/{parent_run}/commit.json"
                        commit_sha = lineage.compute_sha256(f"parent-{parent_run}".encode("utf-8"))
                    reg["parent"] = {
                        "run_id": parent_run,
                        "commit_key": commit_key,
                        "commit_sha256": commit_sha,
                    }
                else:
                    reg["parent"] = None
                zf.writestr(f"silver/full-month/{run_id}/registry.json", json.dumps(reg).encode("utf-8"))

            if include_bronze:
                for b in range(1, 38):
                    zf.writestr(f"bronze/orc07-bronze-{b:02d}/manifest.json", b"{}")

            if duplicate_member:
                zf.writestr("silver/bundles/orc07-mock-run-01/manifest.json", b"{}")

        return lineage.compute_file_sha256(zip_path)

    def test_frontier_spec_synthetic_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            spec_out = tmp_path / "frontier.json"
            prov_sha = self._create_synthetic_provenance_zip(prov_zip)

            res = lineage.extract_frontier_spec(
                owner="rosy179",
                provenance_zip=prov_zip,
                output_spec_file=spec_out,
                expected_provenance_sha=prov_sha,
            )
            self.assertEqual(res["status"], "PASS")
            self.assertEqual(res["total_frontier_objects"], 114)
            self.assertEqual(res["silver_dataset_manifests"], 92)
            self.assertEqual(res["gold_parent_commits"], 22)
            self.assertTrue(res["provenance_pin_verified"])

            with open(spec_out, "r", encoding="utf-8") as f:
                data = json.load(f)

            self.assertEqual(data["tier_classification"]["tier_1_first_hop_provenance"]["status"], "INPUT_PIN_VERIFIED")
            self.assertEqual(data["tier_classification"]["tier_1_first_hop_provenance"]["total_first_hop_objects"], 83)
            self.assertEqual(data["tier_classification"]["tier_2_supplemental_phase_evidence"]["status"], "NOT_CHECKED")
            self.assertEqual(data["tier_classification"]["tier_3_transitive_closure_frontier"]["status"], "SPECIFIED_PENDING_BYTES")
            self.assertEqual(data["tier_classification"]["tier_3_transitive_closure_frontier"]["gate_status"], "CONG_1_TRANSITIVE_CLOSURE_OPEN")

    def test_frontier_spec_wrong_sha_pin_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    expected_provenance_sha="0" * 64,
                )
            self.assertIn("PROVENANCE_SHA_MISMATCH", str(ctx.exception))

    def test_frontier_spec_checkpoint_pin_verification(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            prov_sha = self._create_synthetic_provenance_zip(prov_zip)

            # 1. Matching checkpoint passes
            cp_file = tmp_path / "checkpoint.json"
            cp_file.write_text(json.dumps({"packages": {"prov.zip": {"sha256": prov_sha}}}), encoding="utf-8")
            res = lineage.extract_frontier_spec(
                owner="rosy179",
                provenance_zip=prov_zip,
                provenance_checkpoint=cp_file,
            )
            self.assertEqual(res["status"], "PASS")
            self.assertTrue(res["provenance_pin_verified"])

            # 2. Mismatching checkpoint fails
            cp_file_bad = tmp_path / "checkpoint_bad.json"
            cp_file_bad.write_text(json.dumps({"packages": {"prov.zip": {"sha256": "1" * 64}}}), encoding="utf-8")
            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    provenance_checkpoint=cp_file_bad,
                )
            self.assertIn("PROVENANCE_SHA_MISMATCH", str(ctx.exception))

    def test_frontier_spec_missing_pin_fails_without_allow_unverified(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="ThanhTris",
                    provenance_zip=prov_zip,
                )
            self.assertIn("PROVENANCE_INPUT_PIN_REQUIRED", str(ctx.exception))

    def test_frontier_spec_empty_zip_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            empty_zip = tmp_path / "empty.zip"
            with zipfile.ZipFile(empty_zip, "w") as zf:
                pass

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=empty_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("BUNDLE_MANIFEST_COUNT_MISMATCH", str(ctx.exception))

    def test_frontier_spec_missing_bundle_count_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, num_runs=20)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("BUNDLE_MANIFEST_COUNT_MISMATCH", str(ctx.exception))

    def test_frontier_spec_missing_registries_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, num_runs=23, num_registries=20)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("REGISTRY_COUNT_MISMATCH", str(ctx.exception))

    def test_frontier_spec_missing_datasets_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, missing_dataset=True)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("MISSING_DATASETS", str(ctx.exception))

    def test_frontier_spec_duplicate_zip_members_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, duplicate_member=True)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("DUPLICATE_ZIP_MEMBER_NAMES", str(ctx.exception))

    def test_frontier_spec_conflicting_sha_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, conflicting_sha=True)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("CONFLICTING_SHA", str(ctx.exception))

    def test_frontier_spec_duplicate_frontier_object_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, duplicate_frontier_obj=True)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("DUPLICATE_FRONTIER_OBJECT", str(ctx.exception))

    def test_frontier_spec_invalid_dataset_uri_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, invalid_uri=True)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("INVALID_DATASET_MANIFEST_URI", str(ctx.exception))

    def test_frontier_spec_invalid_dataset_sha_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, invalid_sha=True)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("INVALID_DATASET_SHA256", str(ctx.exception))

    def test_frontier_spec_invalid_commit_key_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, invalid_commit_key=True)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("INVALID_COMMIT_KEY", str(ctx.exception))

    def test_frontier_spec_invalid_commit_sha_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "prov.zip"
            self._create_synthetic_provenance_zip(prov_zip, invalid_commit_sha=True)

            with self.assertRaises(ValueError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    allow_unverified_provenance=True,
                )
            self.assertIn("INVALID_COMMIT_SHA256", str(ctx.exception))

    def test_frontier_spec_existing_file_fails_without_overwrite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            out_file = tmp_path / "existing_spec.json"
            out_file.write_text("DO_NOT_OVERWRITE", encoding="utf-8")

            prov_zip = tmp_path / "prov.zip"
            prov_sha = self._create_synthetic_provenance_zip(prov_zip)

            # 1. Fail without overwrite
            with self.assertRaises(FileExistsError) as ctx:
                lineage.extract_frontier_spec(
                    owner="rosy179",
                    provenance_zip=prov_zip,
                    output_spec_file=out_file,
                    expected_provenance_sha=prov_sha,
                    overwrite=False,
                )
            self.assertIn("OUTPUT_FILE_ALREADY_EXISTS", str(ctx.exception))
            self.assertEqual(out_file.read_text(encoding="utf-8"), "DO_NOT_OVERWRITE")

            # 2. Succeed with overwrite
            res = lineage.extract_frontier_spec(
                owner="rosy179",
                provenance_zip=prov_zip,
                output_spec_file=out_file,
                expected_provenance_sha=prov_sha,
                overwrite=True,
            )
            self.assertEqual(res["status"], "PASS")
            self.assertNotEqual(out_file.read_text(encoding="utf-8"), "DO_NOT_OVERWRITE")

    def test_frontier_spec_symlink_output_rejected_and_target_untouched(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            target_file = tmp_path / "important_checkpoint.json"
            target_file.write_text("ORIGINAL_CHECKPOINT_DATA", encoding="utf-8")
            link_file = tmp_path / "symlink_spec.json"

            prov_zip = tmp_path / "prov.zip"
            prov_sha = self._create_synthetic_provenance_zip(prov_zip)

            orig_is_symlink = Path.is_symlink
            orig_os_islink = os.path.islink

            def mock_is_symlink(p):
                if Path(p) == Path(link_file):
                    return True
                return orig_is_symlink(p)

            def mock_os_islink(p):
                if Path(p) == Path(link_file):
                    return True
                return orig_os_islink(p)

            with patch.object(Path, "is_symlink", mock_is_symlink), patch("os.path.islink", mock_os_islink):
                with self.assertRaises(ValueError) as ctx:
                    lineage.extract_frontier_spec(
                        owner="rosy179",
                        provenance_zip=prov_zip,
                        output_spec_file=link_file,
                        expected_provenance_sha=prov_sha,
                        overwrite=True,
                    )
                self.assertIn("SYMLINK_REJECTED", str(ctx.exception))

            # Crucial check: verify destination was NOT modified
            self.assertEqual(target_file.read_text(encoding="utf-8"), "ORIGINAL_CHECKPOINT_DATA")

    def test_frontier_spec_symlink_parent_directory_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            symlink_dir = tmp_path / "sym_dir"
            out_file = symlink_dir / "spec.json"

            prov_zip = tmp_path / "prov.zip"
            prov_sha = self._create_synthetic_provenance_zip(prov_zip)

            orig_is_symlink = Path.is_symlink
            orig_os_islink = os.path.islink

            def mock_is_symlink(p):
                if Path(p) == Path(symlink_dir):
                    return True
                return orig_is_symlink(p)

            def mock_os_islink(p):
                if Path(p) == Path(symlink_dir):
                    return True
                return orig_os_islink(p)

            with patch.object(Path, "is_symlink", mock_is_symlink), patch("os.path.islink", mock_os_islink):
                with self.assertRaises(ValueError) as ctx:
                    lineage.extract_frontier_spec(
                        owner="rosy179",
                        provenance_zip=prov_zip,
                        output_spec_file=out_file,
                        expected_provenance_sha=prov_sha,
                        overwrite=True,
                    )
                self.assertIn("SYMLINK_REJECTED", str(ctx.exception))

    def test_pack_lineage_existing_files_fail_without_overwrite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"
            spec_out = tmp_path / "lineage_spec.json"

            # Create mock runs for rosy179
            for r in range(1, 24):
                run_id = f"orc07-test-run-{r:02d}"
                for phase in lineage.REQUIRED_PHASES:
                    p = src_dir / "silver" / "real-etl" / run_id / f"{phase}.json"
                    p.parent.mkdir(parents=True, exist_ok=True)
                    p.write_bytes(b"{}")

            # 1. Existing output_zip fails
            zip_out.write_bytes(b"DUMMY_ZIP")
            with self.assertRaises(FileExistsError) as ctx:
                lineage.pack_lineage_archive(
                    owner="rosy179",
                    source_input=src_dir,
                    output_zip=zip_out,
                    overwrite=False,
                )
            self.assertIn("OUTPUT_ZIP_ALREADY_EXISTS", str(ctx.exception))
            self.assertEqual(zip_out.read_bytes(), b"DUMMY_ZIP")

            # 2. Existing output_spec_file fails
            zip_out.unlink()
            spec_out.write_text("DUMMY_SPEC", encoding="utf-8")
            with self.assertRaises(FileExistsError) as ctx:
                lineage.pack_lineage_archive(
                    owner="rosy179",
                    source_input=src_dir,
                    output_zip=zip_out,
                    output_spec_file=spec_out,
                    overwrite=False,
                )
            self.assertIn("OUTPUT_FILE_ALREADY_EXISTS", str(ctx.exception))
            self.assertEqual(spec_out.read_text(encoding="utf-8"), "DUMMY_SPEC")

    def test_pack_lineage_symlink_output_spec_rejected_and_target_untouched(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "staging"
            zip_out = tmp_path / "lineage.zip"
            target_checkpoint = tmp_path / "target_checkpoint.json"
            target_checkpoint.write_text("CHECKPOINT_DO_NOT_TOUCH", encoding="utf-8")
            symlink_spec = tmp_path / "symlink_spec.json"

            for r in range(1, 24):
                run_id = f"orc07-test-run-{r:02d}"
                for phase in lineage.REQUIRED_PHASES:
                    p = src_dir / "silver" / "real-etl" / run_id / f"{phase}.json"
                    p.parent.mkdir(parents=True, exist_ok=True)
                    p.write_bytes(b"{}")

            orig_is_symlink = Path.is_symlink
            orig_os_islink = os.path.islink

            def mock_is_symlink(p):
                if Path(p) == Path(symlink_spec):
                    return True
                return orig_is_symlink(p)

            def mock_os_islink(p):
                if Path(p) == Path(symlink_spec):
                    return True
                return orig_os_islink(p)

            with patch.object(Path, "is_symlink", mock_is_symlink), patch("os.path.islink", mock_os_islink):
                with self.assertRaises(ValueError) as ctx:
                    lineage.pack_lineage_archive(
                        owner="rosy179",
                        source_input=src_dir,
                        output_zip=zip_out,
                        output_spec_file=symlink_spec,
                        overwrite=True,
                    )
                self.assertIn("SYMLINK_REJECTED", str(ctx.exception))

            self.assertEqual(target_checkpoint.read_text(encoding="utf-8"), "CHECKPOINT_DO_NOT_TOUCH")

    def test_pack_frontier_metadata_roundtrip_synthetic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            zip_out = tmp_path / "frontier.zip"
            req_file = tmp_path / "req.json"

            objects = []
            # 92 silver manifests
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    f = src_dir / rel
                    f.parent.mkdir(parents=True, exist_ok=True)
                    content = json.dumps({"dataset": ds, "run": run_id}).encode("utf-8")
                    f.write_bytes(content)
                    sha = lineage.compute_sha256(content)
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": sha,
                        "kind": "silver_dataset_manifest",
                    })

            # 22 gold commits
            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                f = src_dir / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                content = json.dumps({"commit": f"commit-{parent_run}"}).encode("utf-8")
                f.write_bytes(content)
                sha = lineage.compute_sha256(content)
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": sha,
                    "kind": "gold_commit",
                })

            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            res = lineage.pack_frontier_metadata(
                owner="rosy179",
                output_zip=zip_out,
                request_file=req_file,
                source_dir=src_dir,
                overwrite=True,
            )
            self.assertEqual(res["status"], "PASS")
            self.assertEqual(res["total_objects"], 114)
            self.assertTrue(zip_out.is_file())
            self.assertTrue(Path(res["sha256_file"]).is_file())

            # Verify contents
            with zipfile.ZipFile(zip_out, "r") as zf:
                znames = zf.namelist()
                self.assertIn("manifest.json", znames)
                self.assertIn("README.md", znames)
                self.assertEqual(len(znames), 116)

    def test_pack_frontier_metadata_sha_mismatch_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            zip_out = tmp_path / "frontier.zip"
            req_file = tmp_path / "req.json"

            objects = []
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    f = src_dir / rel
                    f.parent.mkdir(parents=True, exist_ok=True)
                    content = b"original"
                    f.write_bytes(content)
                    sha = lineage.compute_sha256(content)
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": sha,
                        "kind": "silver_dataset_manifest",
                    })

            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                f = src_dir / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                content = b"original-commit"
                f.write_bytes(content)
                sha = lineage.compute_sha256(content)
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": sha,
                    "kind": "gold_commit",
                })

            # Tamper 1 file
            tampered_file = src_dir / "silver/bundles/orc07-run-01/source_observation/manifest.json"
            tampered_file.write_bytes(b"tampered")

            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            with self.assertRaises(ValueError) as ctx:
                lineage.pack_frontier_metadata(
                    owner="rosy179",
                    output_zip=zip_out,
                    request_file=req_file,
                    source_dir=src_dir,
                )
            self.assertIn("SHA_MISMATCH", str(ctx.exception))

    def test_pack_frontier_metadata_missing_file_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            zip_out = tmp_path / "frontier.zip"
            req_file = tmp_path / "req.json"

            objects = []
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    f = src_dir / rel
                    f.parent.mkdir(parents=True, exist_ok=True)
                    f.write_bytes(b"data")
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": lineage.compute_sha256(b"data"),
                        "kind": "silver_dataset_manifest",
                    })

            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                f = src_dir / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                f.write_bytes(b"commit")
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": lineage.compute_sha256(b"commit"),
                    "kind": "gold_commit",
                })

            # Remove 1 file
            (src_dir / "silver/bundles/orc07-run-01/source_observation/manifest.json").unlink()

            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            with self.assertRaises(FileNotFoundError) as ctx:
                lineage.pack_frontier_metadata(
                    owner="rosy179",
                    output_zip=zip_out,
                    request_file=req_file,
                    source_dir=src_dir,
                )
            self.assertIn("FRONTIER_FILE_NOT_FOUND", str(ctx.exception))

    def test_pack_frontier_metadata_s3_mocked_client(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            zip_out = tmp_path / "frontier_s3.zip"
            req_file = tmp_path / "req_s3.json"

            objects = []
            mock_storage: Dict[str, bytes] = {}

            # 92 silver manifests
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    content = json.dumps({"dataset": ds, "run": run_id}).encode("utf-8")
                    mock_storage[rel] = content
                    sha = lineage.compute_sha256(content)
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": sha,
                        "kind": "silver_dataset_manifest",
                    })

            # 22 gold commits
            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                content = json.dumps({"commit": f"commit-{parent_run}"}).encode("utf-8")
                mock_storage[rel] = content
                sha = lineage.compute_sha256(content)
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": sha,
                    "kind": "gold_commit",
                })

            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            class MockBody:
                def __init__(self, data: bytes):
                    self._data = data
                def read(self) -> bytes:
                    return self._data

            class MockS3Client:
                def get_object(self, Bucket: str, Key: str):
                    if Key not in mock_storage:
                        raise KeyError(f"NoSuchKey: {Key}")
                    return {"Body": MockBody(mock_storage[Key])}

            captured_kwargs = {}
            class MockBoto3Module:
                def client(self, service_name: str, **kwargs):
                    if service_name != "s3":
                        raise ValueError(f"Unknown service: {service_name}")
                    captured_kwargs.update(kwargs)
                    return MockS3Client()

            with patch.object(lineage, "boto3", MockBoto3Module()):
                with patch.dict(os.environ, {"MINIO_ACCESS_KEY": "test-ak", "MINIO_SECRET_KEY": "test-sk"}):
                    res = lineage.pack_frontier_metadata(
                        owner="rosy179",
                        output_zip=zip_out,
                        request_file=req_file,
                        source_dir=None,
                        endpoint_url="http://custom-endpoint:9000",
                        overwrite=True,
                    )
                    self.assertEqual(res["status"], "PASS")
                    self.assertEqual(res["total_objects"], 114)
                    self.assertTrue(zip_out.is_file())
                    self.assertTrue(Path(res["sha256_file"]).is_file())
                    self.assertEqual(captured_kwargs.get("aws_access_key_id"), "test-ak")
                    self.assertEqual(captured_kwargs.get("aws_secret_access_key"), "test-sk")
                    self.assertEqual(captured_kwargs.get("endpoint_url"), "http://custom-endpoint:9000")

            # Also verify direct s3_client parameter injection without env credentials
            zip_out_direct = tmp_path / "frontier_direct.zip"
            res_direct = lineage.pack_frontier_metadata(
                owner="rosy179",
                output_zip=zip_out_direct,
                request_file=req_file,
                source_dir=None,
                s3_client=MockS3Client(),
                overwrite=True,
            )
            self.assertEqual(res_direct["status"], "PASS")
            self.assertTrue(zip_out_direct.is_file())

    def test_pack_frontier_metadata_sha_file_exists_fails_without_overwrite(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            zip_out = tmp_path / "frontier.zip"
            sha_file = tmp_path / "frontier.zip.sha256"
            sha_file.write_text("OLD_CHECKSUM", encoding="utf-8")
            req_file = tmp_path / "req.json"

            objects = []
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    f = src_dir / rel
                    f.parent.mkdir(parents=True, exist_ok=True)
                    f.write_bytes(b"data")
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": lineage.compute_sha256(b"data"),
                        "kind": "silver_dataset_manifest",
                    })

            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                f = src_dir / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                f.write_bytes(b"commit")
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": lineage.compute_sha256(b"commit"),
                    "kind": "gold_commit",
                })

            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            # Must fail with FileExistsError and leave sha_file untouched
            with self.assertRaises(FileExistsError) as ctx:
                lineage.pack_frontier_metadata(
                    owner="rosy179",
                    output_zip=zip_out,
                    request_file=req_file,
                    source_dir=src_dir,
                    overwrite=False,
                )
            self.assertIn("SHA256_FILE_ALREADY_EXISTS", str(ctx.exception))
            self.assertEqual(sha_file.read_text(encoding="utf-8"), "OLD_CHECKSUM")

    def test_pack_frontier_metadata_symlink_output_rejected_and_target_untouched(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            target_checkpoint = tmp_path / "target_checkpoint.json"
            target_checkpoint.write_text("IMPORTANT_CHECKPOINT_DATA", encoding="utf-8")
            symlink_zip = tmp_path / "symlink.zip"
            req_file = tmp_path / "req.json"

            objects = []
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    f = src_dir / rel
                    f.parent.mkdir(parents=True, exist_ok=True)
                    f.write_bytes(b"data")
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": lineage.compute_sha256(b"data"),
                        "kind": "silver_dataset_manifest",
                    })

            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                f = src_dir / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                f.write_bytes(b"commit")
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": lineage.compute_sha256(b"commit"),
                    "kind": "gold_commit",
                })

            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            orig_is_symlink = Path.is_symlink
            orig_os_islink = os.path.islink

            def mock_is_symlink(p):
                if Path(p) == Path(symlink_zip):
                    return True
                return orig_is_symlink(p)

            def mock_os_islink(p):
                if Path(p) == Path(symlink_zip):
                    return True
                return orig_os_islink(p)

            with patch.object(Path, "is_symlink", mock_is_symlink), patch("os.path.islink", mock_os_islink):
                with self.assertRaises(ValueError) as ctx:
                    lineage.pack_frontier_metadata(
                        owner="rosy179",
                        output_zip=symlink_zip,
                        request_file=req_file,
                        source_dir=src_dir,
                        overwrite=True,
                    )
                self.assertIn("SYMLINK_REJECTED", str(ctx.exception))

            self.assertEqual(target_checkpoint.read_text(encoding="utf-8"), "IMPORTANT_CHECKPOINT_DATA")

    def test_pack_frontier_metadata_duplicate_objects_in_request_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            zip_out = tmp_path / "frontier.zip"
            req_file = tmp_path / "req_dup.json"

            # 1 object repeated 114 times
            single_obj = {
                "uri": "s3://japan-earthquake/silver/bundles/orc07-run-01/source_observation/manifest.json",
                "sha256": "a" * 64,
                "kind": "silver_dataset_manifest",
            }
            objects = [dict(single_obj) for _ in range(114)]
            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            with self.assertRaises(ValueError) as ctx:
                lineage.pack_frontier_metadata(
                    owner="rosy179",
                    output_zip=zip_out,
                    request_file=req_file,
                    source_dir=src_dir,
                )
            self.assertIn("DUPLICATE_FRONTIER_REQUEST_OBJECT", str(ctx.exception))

    def test_pack_frontier_metadata_composition_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            zip_out = tmp_path / "frontier.zip"
            req_file = tmp_path / "req_comp.json"

            objects = []
            # 93 silver manifests instead of 92
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": lineage.compute_sha256(rel.encode("utf-8")),
                        "kind": "silver_dataset_manifest",
                    })

            # Add 1 extra manifest from run 24
            objects.append({
                "uri": "s3://japan-earthquake/silver/bundles/orc07-run-24/source_observation/manifest.json",
                "sha256": "c" * 64,
                "kind": "silver_dataset_manifest",
            })

            # 21 gold commits instead of 22
            for r in range(1, 22):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": lineage.compute_sha256(rel.encode("utf-8")),
                    "kind": "gold_commit",
                })

            # Total is 114 (93 manifests + 21 commits)
            self.assertEqual(len(objects), 114)
            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            with self.assertRaises(ValueError) as ctx:
                lineage.pack_frontier_metadata(
                    owner="rosy179",
                    output_zip=zip_out,
                    request_file=req_file,
                    source_dir=src_dir,
                )
            self.assertIn("SILVER_MANIFEST_COUNT_MISMATCH", str(ctx.exception))

    def test_pack_frontier_metadata_invalid_sha_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            zip_out = tmp_path / "frontier.zip"
            req_file = tmp_path / "req_bad_sha.json"

            objects = []
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    sha = "invalid-not-64-hex" if (r == 1 and ds == "source_observation") else ("a" * 64)
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": sha,
                        "kind": "silver_dataset_manifest",
                    })

            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": "b" * 64,
                    "kind": "gold_commit",
                })

            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            with self.assertRaises(ValueError) as ctx:
                lineage.pack_frontier_metadata(
                    owner="rosy179",
                    output_zip=zip_out,
                    request_file=req_file,
                    source_dir=src_dir,
                )
            self.assertIn("INVALID_FRONTIER_SHA256", str(ctx.exception))

    def test_pack_frontier_metadata_kind_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_minio"
            zip_out = tmp_path / "frontier.zip"
            req_file = tmp_path / "req_bad_kind.json"

            objects = []
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    kind = "gold_commit" if (r == 1 and ds == "source_observation") else "silver_dataset_manifest"
                    objects.append({
                        "uri": f"s3://japan-earthquake/{rel}",
                        "sha256": "a" * 64,
                        "kind": kind,
                    })

            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                objects.append({
                    "uri": f"s3://japan-earthquake/{rel}",
                    "sha256": "b" * 64,
                    "kind": "gold_commit",
                })

            req_file.write_text(json.dumps({"owner": "rosy179", "object_count": 114, "objects": objects}), encoding="utf-8")

            with self.assertRaises(ValueError) as ctx:
                lineage.pack_frontier_metadata(
                    owner="rosy179",
                    output_zip=zip_out,
                    request_file=req_file,
                    source_dir=src_dir,
                )
            self.assertIn("KIND_MISMATCH", str(ctx.exception))

    def test_pack_frontier_metadata_boto3_missing_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            zip_out = tmp_path / "frontier.zip"
            req_file = ROOT / "config/research/dat03_lineage_rosy179_metadata.json"

            with patch.object(lineage, "boto3", None):
                with self.assertRaises(RuntimeError) as ctx:
                    lineage.pack_frontier_metadata(
                        owner="rosy179",
                        output_zip=zip_out,
                        request_file=req_file,
                        source_dir=None,
                    )
                self.assertIn("boto3 is required to interact with MinIO/S3", str(ctx.exception))


    def test_pack_frontier_metadata_missing_credentials_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            zip_out = tmp_path / "frontier.zip"
            req_file = ROOT / "config/research/dat03_lineage_rosy179_metadata.json"

            def mock_env_get(key, default=None):
                if key in ("MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
                    return None
                return default

            with patch.object(os.environ, "get", mock_env_get):
                with patch("boto3.Session") as mock_session_cls:
                    mock_sess = MagicMock()
                    mock_sess.get_credentials.return_value = None
                    mock_session_cls.return_value = mock_sess
                    with self.assertRaises(RuntimeError) as ctx:
                        lineage.pack_frontier_metadata(
                            owner="rosy179",
                            output_zip=zip_out,
                            request_file=req_file,
                            source_dir=None,
                        )
                    self.assertIn("MISSING_S3_CREDENTIALS", str(ctx.exception))
                    self.assertNotIn("change-me", str(ctx.exception))
                    self.assertNotIn("pipeline-access-key", str(ctx.exception))
                    self.assertNotIn("pipeline-secret-key", str(ctx.exception))

    def test_pack_frontier_metadata_incomplete_credentials_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            zip_out = tmp_path / "frontier.zip"
            req_file = ROOT / "config/research/dat03_lineage_rosy179_metadata.json"

            def mock_env_get(key, default=None):
                if key == "MINIO_ACCESS_KEY":
                    return "some-access-key"
                if key in ("MINIO_SECRET_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
                    return None
                return default

            with patch.object(os.environ, "get", mock_env_get):
                with self.assertRaises(RuntimeError) as ctx:
                    lineage.pack_frontier_metadata(
                        owner="rosy179",
                        output_zip=zip_out,
                        request_file=req_file,
                        source_dir=None,
                    )
                self.assertIn("INCOMPLETE_S3_CREDENTIALS", str(ctx.exception))

    def test_pack_frontier_cli_rejects_secret_flags(self):
        stderr_buf = io.StringIO()
        with patch("sys.stderr", stderr_buf):
            with self.assertRaises(SystemExit):
                lineage.main([
                    "pack-frontier",
                    "--owner", "rosy179",
                    "--output-zip", "dummy.zip",
                    "--access-key", "attempted-key",
                ])
        self.assertIn("unrecognized arguments", stderr_buf.getvalue())

    def test_pack_frontier_metadata_category_normalization_with_spec_categories(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            src_dir = tmp_path / "mock_storage"
            zip_out = tmp_path / "frontier.zip"
            req_file = tmp_path / "req_spec_categories.json"

            objects = []
            for r in range(1, 24):
                run_id = f"orc07-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    rel = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    f = src_dir / rel
                    f.parent.mkdir(parents=True, exist_ok=True)
                    f.write_bytes(b"content")
                    objects.append({
                        "s3_uri": f"s3://japan-earthquake/{rel}",
                        "sha256": lineage.compute_sha256(b"content"),
                        "category": "silver_dataset_manifest",
                    })

            for r in range(1, 23):
                parent_run = f"orc07-run-{r:02d}"
                rel = f"silver/gold-commits/{parent_run}/commit.json"
                f = src_dir / rel
                f.parent.mkdir(parents=True, exist_ok=True)
                f.write_bytes(b"commit-content")
                objects.append({
                    "s3_uri": f"s3://japan-earthquake/{rel}",
                    "sha256": lineage.compute_sha256(b"commit-content"),
                    "category": "gold_parent_commit",
                })

            req_file.write_text(json.dumps({
                "owner": "rosy179",
                "frontier_objects": objects,
            }), encoding="utf-8")

            res = lineage.pack_frontier_metadata(
                owner="rosy179",
                output_zip=zip_out,
                request_file=req_file,
                source_dir=src_dir,
                overwrite=True,
            )
            self.assertEqual(res["status"], "PASS")
            self.assertEqual(res["total_objects"], 114)
            self.assertTrue(zip_out.is_file())

    def test_frontier_spec_to_pack_frontier_e2e(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir).resolve()
            prov_zip = tmp_path / "mock_provenance.zip"
            src_dir = tmp_path / "mock_storage"
            spec_file = tmp_path / "extracted_spec.json"
            frontier_zip = tmp_path / "packaged_frontier.zip"

            prov_sha = self._create_synthetic_provenance_zip(prov_zip)

            # Populate mock_storage matching the files created by _create_synthetic_provenance_zip
            for r in range(1, 24):
                run_id = f"orc07-mock-run-{r:02d}"
                for ds in ("source_observation", "reject_record", "source_link", "canonical_membership"):
                    ds_key = f"silver/bundles/{run_id}/{ds}/manifest.json"
                    ds_file = src_dir / ds_key
                    ds_file.parent.mkdir(parents=True, exist_ok=True)
                    ds_file.write_bytes(f"{run_id}-{ds}".encode("utf-8"))

                if r > 1:
                    parent_run = f"orc07-mock-run-{r-1:02d}"
                    p_key = f"silver/gold-commits/{parent_run}/commit.json"
                    p_file = src_dir / p_key
                    p_file.parent.mkdir(parents=True, exist_ok=True)
                    p_file.write_bytes(f"parent-{parent_run}".encode("utf-8"))

            # Step 1: extract_frontier_spec
            spec_res = lineage.extract_frontier_spec(
                owner="rosy179",
                provenance_zip=prov_zip,
                output_spec_file=spec_file,
                expected_provenance_sha=prov_sha,
                overwrite=True,
            )
            self.assertEqual(spec_res["status"], "PASS")
            self.assertEqual(spec_res["total_frontier_objects"], 114)
            self.assertTrue(spec_file.is_file())

            # Step 2: feed spec_file directly into pack_frontier_metadata
            pack_res = lineage.pack_frontier_metadata(
                owner="rosy179",
                output_zip=frontier_zip,
                request_file=spec_file,
                source_dir=src_dir,
                overwrite=True,
            )
            self.assertEqual(pack_res["status"], "PASS")
            self.assertEqual(pack_res["total_objects"], 114)
            self.assertTrue(frontier_zip.is_file())
            self.assertTrue(Path(pack_res["sha256_file"]).is_file())


if __name__ == "__main__":
    unittest.main()
