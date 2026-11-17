"""Offline unit tests for DEMO-01 demo package manifest and preflight verifier."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("demo_preflight", ROOT / "scripts" / "demo-preflight.py")
preflight = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(preflight)


def _valid_v4_receipt(recipient: str = "ThanhTris") -> dict:
    gold_pin = {
        "gold_publication_id": "pub_832ce36ac875f546528718c3545442a77a0097f7573379f30ed92f2a43c481f4",
        "gold_snapshot_id": "7580315846938041321",
        "physical_gold_table_name": "iceberg.dat03_unified_20261114.event_current",
        "publication_bundle_sha256": "832ce36ac875f546528718c3545442a77a0097f7573379f30ed92f2a43c481f4",
    }
    return {
        "version": "dat03-team-research-v1",
        "status": "TEAM_RESEARCH_VERIFIED",
        "recipient": recipient,
        "recipient_self_reported": True,
        "all_shards_verified": True,
        "scientific_approval": False,
        "handoff_sha256": "6cf0af7c2a3d8e8ec4c993f84ed6d86cdc417324102133dd3068eb2ebc40d297",
        "machine_fingerprint_sha256": "f1aee22f4cd8ff91143cefd3d33cb97b4b338c8547dd46aa4531fe22d601a164",
        "python_version": "3.12.10",
        "verified_at_utc": "2026-11-16T11:52:22.110127+00:00",
        "collections": {
            "REPRODUCTION": {
                "status": "COLLECTION_VERIFIED",
                "all_shards_verified": True,
                "dataset_split": "REPRODUCTION",
                "dataset_id": "ds_907fe748fafdfc0f072ce4db4e1cf0aa619ed18877129e17c8440b43062c3d87",
                "collection_id": "col_bd10c69365da841b2cf6a62172e2350a797001eac04ce4bc6f2277a4d150ad98",
                "manifest_sha256": "d3ee1334bfc50005a527f7b32a8988fcb1bdd7659a7dd4e84de326a7735139ae",
                "checksums_sha256": "4768e500754c569bc951f23e2ac14fa9b9703ae69fbd5f78de06eacbb72f7ae9",
                "shard_count": 2,
                "feature_row_count": 156710,
                "mainshock_count": 194,
                "timestamp_reader_profile": "prepared-spark-utc-int96-v1",
                "timestamps_losslessly_normalized": True,
                "gold_pin": copy.deepcopy(gold_pin),
            },
            "EXTENSION": {
                "status": "COLLECTION_VERIFIED",
                "all_shards_verified": True,
                "dataset_split": "EXTENSION",
                "dataset_id": "ds_5259d2fe13e027245559e1ec4a1e696b5d94d0b67f44d5ec80738c9e984978be",
                "collection_id": "col_0dc6dc2fd0a60c2890f68863b8561813ecd1d5e1941a8995be1d58e192fe0906",
                "manifest_sha256": "fd1908f77ffcc70e77250ef834883d0ab8bbaf38e40962958a5c296f90d4aad1",
                "checksums_sha256": "753506e4039130e12f88746c5f95122a66a3bac36342634d082f89acf5ad89d8",
                "shard_count": 4,
                "feature_row_count": 233664,
                "mainshock_count": 45,
                "timestamp_reader_profile": "prepared-spark-utc-int96-v1",
                "timestamps_losslessly_normalized": True,
                "gold_pin": copy.deepcopy(gold_pin),
            },
        },
    }


class DemoPreflightTests(unittest.TestCase):
    def test_repo_manifest_passes_offline_pending_official_gates(self):
        report = preflight.run_preflight()
        self.assertEqual(report["task_id"], "DEMO-01")
        self.assertEqual(report["overall_status"], "PREPARED_PENDING_OFFICIAL_GATES")
        self.assertTrue(report["demo_package_prepared"])
        self.assertFalse(report["official_release_ready"])
        self.assertFalse(report["depends_on_power_bi"])
        self.assertFalse(report["stage_40_year_rerun_required"])
        self.assertEqual(report["verified_evidence_count"], 11)
        self.assertEqual(len(report["fallback_modes_verified"]), 4)
        self.assertEqual(
            [g["task_id"] for g in report["pending_official_gates"]],
            ["EXP-02", "EXP-03", "EXP-04", "MLI-04", "MLQ-01"],
        )
        self.assertEqual(report["local_research_receipt"]["status"], "NOT_REQUESTED")
        self.assertFalse(report["local_research_receipt"]["proves_kit_on_disk"])

    def _run_with_mutated_manifest(
        self,
        mutate_fn,
        local_receipt_path: Path | None = None,
        expected_receipt_sha256: str | None = None,
    ):
        base = json.loads(preflight.DEFAULT_MANIFEST.read_text(encoding="utf-8"))
        mutated = copy.deepcopy(base)
        mutate_fn(mutated)
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "manifest.json"
            manifest_path.write_text(json.dumps(mutated, ensure_ascii=False), encoding="utf-8")
            return preflight.run_preflight(
                manifest_path=manifest_path,
                root=ROOT,
                local_receipt_path=local_receipt_path,
                expected_receipt_sha256=expected_receipt_sha256,
            )

    # --- P1 regressions: official-readiness bypass prevention ---

    def test_clearing_pending_gates_while_ready_false_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_MISSING_REQUIRED_PENDING_GATES:EMPTY"):
            self._run_with_mutated_manifest(lambda m: m.__setitem__("pending_official_gates", []))

    def test_clearing_pending_gates_and_setting_ready_true_fails_closed(self):
        def mutate(m):
            m["pending_official_gates"] = []
            m["official_release_ready"] = True

        with self.assertRaisesRegex(ValueError, "DEMO_OFFICIAL_RELEASE_NOT_ALLOWED_BEFORE_GATES"):
            self._run_with_mutated_manifest(mutate)

    def test_dropping_single_required_pending_gate_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_MISSING_REQUIRED_PENDING_GATES:MLQ-01"):
            self._run_with_mutated_manifest(
                lambda m: m.__setitem__(
                    "pending_official_gates",
                    [g for g in m["pending_official_gates"] if g["task_id"] != "MLQ-01"],
                )
            )

    def test_setting_mli04_official_parity_true_in_prep_manifest_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_MLI04_PARITY_MUST_BE_FALSE_BEFORE_OFFICIAL_IMPORT"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["static_report_and_ml_sql"].__setitem__(
                    "official_sql_parity_verified", True
                )
            )

    # --- P2 regressions: fail-closed --local-receipt verification ---

    def test_valid_local_receipt_and_expected_sha_pass(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt_path = Path(tmp) / "ThanhTris-research-receipt.json"
            payload = json.dumps(_valid_v4_receipt(), sort_keys=True, indent=2).encode("utf-8")
            receipt_path.write_bytes(payload)
            expected_sha = hashlib.sha256(payload).hexdigest()

            unpinned_report = preflight.run_preflight(local_receipt_path=receipt_path)
            self.assertEqual(unpinned_report["local_research_receipt"]["status"], "TEAM_RESEARCH_VERIFIED")
            self.assertEqual(
                unpinned_report["local_research_receipt"]["verification_scope"],
                "SCHEMA_AND_V4_PINS_VERIFIED_SELF_REPORTED_LOCAL_FILE",
            )
            self.assertFalse(unpinned_report["local_research_receipt"]["proves_kit_on_disk"])
            self.assertEqual(unpinned_report["local_research_receipt"]["receipt_sha256"], expected_sha)

            pinned_report = preflight.run_preflight(
                local_receipt_path=receipt_path,
                expected_receipt_sha256=expected_sha,
            )
            self.assertEqual(
                pinned_report["local_research_receipt"]["verification_scope"],
                "EXTERNAL_RECEIPT_SHA_AND_PINS_VERIFIED",
            )
            self.assertEqual(pinned_report["local_research_receipt"]["recipient"], "ThanhTris")

    def test_missing_local_receipt_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "missing-receipt.json"
            with self.assertRaisesRegex(FileNotFoundError, "DEMO_RECEIPT_MISSING"):
                preflight.run_preflight(local_receipt_path=missing)

    def test_spoofed_four_field_receipt_with_zero_handoff_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            spoofed = Path(tmp) / "spoofed-receipt.json"
            spoofed.write_text(
                json.dumps(
                    {
                        "status": "TEAM_RESEARCH_VERIFIED",
                        "recipient": "ThanhTris",
                        "all_shards_verified": True,
                        "handoff_sha256": "0" * 64,
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_"):
                preflight.run_preflight(local_receipt_path=spoofed)

    def test_wrong_handoff_sha_in_complete_receipt_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt_path = Path(tmp) / "wrong-handoff.json"
            data = _valid_v4_receipt()
            data["handoff_sha256"] = "0" * 64
            receipt_path.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_HANDOFF_SHA_MISMATCH"):
                preflight.run_preflight(local_receipt_path=receipt_path)

    def test_malformed_json_receipt_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "bad.json"
            bad.write_text("{broken json", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_INVALID_JSON"):
                preflight.run_preflight(local_receipt_path=bad)

    def test_failed_status_or_unverified_shards_in_receipt_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            failed = Path(tmp) / "failed.json"
            data = _valid_v4_receipt()
            data["status"] = "FAILED"
            failed.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_STATUS_NOT_VERIFIED"):
                preflight.run_preflight(local_receipt_path=failed)

            unverified = Path(tmp) / "unverified.json"
            data2 = _valid_v4_receipt()
            data2["all_shards_verified"] = False
            unverified.write_text(json.dumps(data2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_ALL_SHARDS_NOT_VERIFIED"):
                preflight.run_preflight(local_receipt_path=unverified)

    def test_tampered_collection_count_or_gold_pin_in_receipt_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            tampered_count = Path(tmp) / "tampered-count.json"
            data = _valid_v4_receipt()
            data["collections"]["EXTENSION"]["feature_row_count"] = 233663
            tampered_count.write_text(json.dumps(data), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_COLLECTION_MISMATCH:EXTENSION.feature_row_count"):
                preflight.run_preflight(local_receipt_path=tampered_count)

            tampered_gold = Path(tmp) / "tampered-gold.json"
            data2 = _valid_v4_receipt()
            data2["collections"]["REPRODUCTION"]["gold_pin"]["gold_snapshot_id"] = "123"
            tampered_gold.write_text(json.dumps(data2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_GOLD_PIN_MISMATCH:REPRODUCTION"):
                preflight.run_preflight(local_receipt_path=tampered_gold)

    def test_expected_receipt_sha_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt_path = Path(tmp) / "receipt.json"
            receipt_path.write_text(json.dumps(_valid_v4_receipt()), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_SHA_MISMATCH"):
                preflight.run_preflight(local_receipt_path=receipt_path, expected_receipt_sha256="a" * 64)

    # --- P2 regressions: required pin set completeness & authoritative evidence cross-checks ---

    def test_manifest_missing_extension_collection_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:DAT-03.collections_set_mismatch"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["official_paired_release_v4"]["collections"].pop("EXTENSION")
            )

    def test_manifest_and_receipt_both_missing_extension_collection_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt_path = Path(tmp) / "partial-receipt.json"
            receipt = _valid_v4_receipt()
            receipt["collections"].pop("EXTENSION")
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:DAT-03.collections_set_mismatch"):
                self._run_with_mutated_manifest(
                    lambda m: m["representative_evidence"]["official_paired_release_v4"]["collections"].pop("EXTENSION"),
                    local_receipt_path=receipt_path,
                )

    def test_manifest_and_receipt_both_empty_collections_fail_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt_path = Path(tmp) / "empty-receipt.json"
            receipt = _valid_v4_receipt()
            receipt["collections"] = {}
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:DAT-03.collections_set_mismatch"):
                self._run_with_mutated_manifest(
                    lambda m: m["representative_evidence"]["official_paired_release_v4"].__setitem__("collections", {}),
                    local_receipt_path=receipt_path,
                )

    def test_receipt_missing_collection_fails_closed_even_without_manifest_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            receipt_path = Path(tmp) / "partial-receipt.json"
            receipt = _valid_v4_receipt()
            receipt["collections"].pop("EXTENSION")
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_RECEIPT_COLLECTIONS_SET_MISMATCH"):
                preflight.run_preflight(local_receipt_path=receipt_path)

    def test_qa01_missing_one_or_all_snapshots_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:QA-01.snapshot_ids_set_mismatch"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["daily_operational_v2"]["snapshot_ids"].pop("dim_depth_band")
            )
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:QA-01.snapshot_ids_set_mismatch"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["daily_operational_v2"].__setitem__("snapshot_ids", {})
            )

    def test_snapshot_ids_reject_float_or_overflow_int64(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_INVALID_INT64_SNAPSHOT"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["daily_operational_v2"]["snapshot_ids"].__setitem__(
                    "event_current", 1543775439517699328.0
                )
            )
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_INVALID_INT64_SNAPSHOT"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["official_paired_release_v4"]["gold_pin"].__setitem__(
                    "gold_snapshot_id", "9223372036854775808"
                )
            )

    def test_manifest_tampered_gold_pin_fails_closed_against_authoritative_evidence(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:DAT-03.gold_pin.gold_snapshot_id"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["official_paired_release_v4"]["gold_pin"].__setitem__(
                    "gold_snapshot_id", "123"
                )
            )

        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:DAT-03.gold_pin.gold_publication_id"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["official_paired_release_v4"]["gold_pin"].__setitem__(
                    "gold_publication_id", "pub_" + ("0" * 64)
                )
            )

        def mutate_pub_bundle(m):
            gold = m["representative_evidence"]["official_paired_release_v4"]["gold_pin"]
            gold["publication_bundle_sha256"] = "0" * 64
            gold["gold_publication_id"] = "pub_" + ("0" * 64)

        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:DAT-03.gold_pin.publication_bundle_sha256"):
            self._run_with_mutated_manifest(mutate_pub_bundle)

        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:DAT-03.gold_pin.physical_gold_table_name"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["official_paired_release_v4"]["gold_pin"].__setitem__(
                    "physical_gold_table_name", "iceberg.other_ns.event_current"
                )
            )

        with tempfile.TemporaryDirectory() as tmp:
            receipt_path = Path(tmp) / "dual-tampered.json"
            receipt = _valid_v4_receipt()
            receipt["collections"]["REPRODUCTION"]["gold_pin"]["gold_snapshot_id"] = "123"
            receipt["collections"]["EXTENSION"]["gold_pin"]["gold_snapshot_id"] = "123"
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:DAT-03.gold_pin.gold_snapshot_id"):
                self._run_with_mutated_manifest(
                    lambda m: m["representative_evidence"]["official_paired_release_v4"]["gold_pin"].__setitem__(
                        "gold_snapshot_id", "123"
                    ),
                    local_receipt_path=receipt_path,
                )

    # --- Existing pin, path, scientific boundary, and secret checks ---

    def test_missing_required_file_fails_closed(self):
        with self.assertRaisesRegex(FileNotFoundError, "DEMO_MISSING_FILE"):
            self._run_with_mutated_manifest(
                lambda m: m["required_repo_files"].append("docs/does_not_exist_demo.md")
            )

    def test_path_traversal_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PATH_ESCAPES_ROOT"):
            self._run_with_mutated_manifest(
                lambda m: m["required_repo_files"].append("../outside.txt")
            )

    def test_mismatched_run_id_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:QA-01.run_id"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["daily_operational_v2"].__setitem__("run_id", "wrong-run")
            )

    def test_mismatched_snapshot_id_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_PIN_MISMATCH:QA-01.snapshot.event_current"):
            self._run_with_mutated_manifest(
                lambda m: m["representative_evidence"]["daily_operational_v2"]["snapshot_ids"].__setitem__(
                    "event_current", "999"
                )
            )

    def test_missing_scientific_boundary_statement_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_BOUNDARY_STATEMENT_INCOMPLETE"):
            self._run_with_mutated_manifest(
                lambda m: m["scientific_boundary"].__setitem__("statement", "Du doan dong dat thanh cong")
            )

    def test_secret_pattern_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "DEMO_SECRET_PATTERN_DETECTED"):
            self._run_with_mutated_manifest(
                lambda m: m.__setitem__("leaked", "MINIO_ROOT_PASSWORD=supersecretvalue")
            )


if __name__ == "__main__":
    unittest.main()
