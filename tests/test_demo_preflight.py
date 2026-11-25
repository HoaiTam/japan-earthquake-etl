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


class DemoOfficialEvidenceTests(unittest.TestCase):
    # Known digest for the EXP-03 cohort pinned by the MLI-04 collector.
    ADAPTIVE_COHORT_SHA256 = "22e84855e4409ae0b0e1696c6ed508af3b7b667952a4f80b7fd50221f88868b0"

    def _pending_v2_manifest(self):
        manifest = json.loads(preflight.CURRENT_MANIFEST.read_bytes())
        for task in ("MLI-04", "MLQ-01"):
            manifest["official_evidence"].pop(task, None)
        manifest.update(
            package_status=preflight.PREPARED_PENDING_STATUS,
            official_release_ready=False,
            pending_official_gates=[
                {"task_id": "MLI-04", "reason": "Chua co official SQL parity tren exact snapshots"},
                {"task_id": "MLQ-01", "reason": "Chua co receipt nghiem thu ML E2E"},
            ],
        )
        return manifest

    def test_v2_repo_manifest_passes_official_ready_without_mocks(self):
        report = preflight.run_preflight(preflight.CURRENT_MANIFEST)
        self.assertEqual("PASS_OFFICIAL_READY", report["overall_status"])
        self.assertTrue(report["official_release_ready"])
        self.assertEqual([], report["pending_official_gates"])
        self.assertEqual(17, report["verified_evidence_count"])
        mli04 = next(row for row in report["verified_runs"]
                     if row["task_id"] == "MLI-04" and row["status"] == "VERIFIED")
        self.assertEqual(8, len(mli04["run_ids"]))
        self.assertEqual(self.ADAPTIVE_COHORT_SHA256, mli04["adaptive_cohort_sha256"])
        mlq = next(row for row in report["verified_runs"] if row["task_id"] == "MLQ-01")
        self.assertEqual(4, len(mlq["run_ids"]))
        self.assertEqual({"WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"}, set(mlq["algorithms"]))
        self.assertEqual(5, len(mlq["trace_query_ids"]))
        self.assertEqual(5, len(set(mlq["trace_query_ids"])))
        self.assertEqual("WINDOW", mlq["extension"]["algorithm_name"])
        self.assertEqual("shard_00000", mlq["extension"]["shard_id"])
        self.assertTrue(mlq["reject_cases"])
        self.assertTrue(mlq["rerun_runs"])

    def test_v2_pending_copy_keeps_two_runtime_gates_and_blocks_false_ready(self):
        pending_manifest = self._pending_v2_manifest()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "manifest.json"
            path.write_text(json.dumps(pending_manifest), encoding="utf-8")
            report = preflight.run_preflight(path)
            self.assertEqual("PREPARED_PENDING_OFFICIAL_GATES", report["overall_status"])
            self.assertEqual(["MLI-04", "MLQ-01"], [g["task_id"] for g in report["pending_official_gates"]])
            self.assertEqual(15, report["verified_evidence_count"])
            self.assertFalse(report["official_release_ready"])

            false_ready = copy.deepcopy(pending_manifest)
            false_ready.update(official_release_ready=True, pending_official_gates=[],
                               package_status="PASS_OFFICIAL_READY")
            path.write_text(json.dumps(false_ready), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "READINESS_MISMATCH"):
                preflight.run_preflight(path)

    def test_v2_keeps_old_manifests_pending_without_extension_pin(self):
        manifest = self._pending_v2_manifest()
        del manifest["official_evidence"]["EXP-05"]
        result = self._run_ready_fixture(
            manifest, {pin["path"]: preflight._pinned_official_json(ROOT, pin)
                       for pin in manifest["official_evidence"].values()},
        )
        self.assertEqual("PREPARED_PENDING_OFFICIAL_GATES", result["overall_status"])
        self.assertFalse(result["official_release_ready"])

        manifest, documents = self._ready_fixture()
        del manifest["official_evidence"]["EXP-05"]
        with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_EXTENSION_PIN"):
            self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_tampered_hash_false_ready_and_missing_source(self):
        base = json.loads(preflight.CURRENT_MANIFEST.read_bytes())
        for change, error in [
            (lambda m: m["official_evidence"]["EXP-04"].update(sha256_lf="0" * 64), "SHA_MISMATCH"),
            (lambda m: m["official_evidence"]["MLI-04"].update(sha256_lf="0" * 64), "SHA_MISMATCH"),
            (lambda m: m["official_evidence"]["MLQ-01"].update(sha256_lf="0" * 64), "SHA_MISMATCH"),
            (lambda m: m.update(official_release_ready=False,
                                package_status=preflight.PREPARED_PENDING_STATUS), "READINESS_MISMATCH"),
            (lambda m: m["official_evidence"]["EXP-03"].update(path="docs/evidence/EXP-03-local-smoke.json"),
             "SOURCE_MISMATCH"),
        ]:
            m = copy.deepcopy(base)
            change(m)
            with tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "manifest.json"
                path.write_text(json.dumps(m), encoding="utf-8")
                with self.assertRaisesRegex(ValueError, error):
                    preflight.run_preflight(path)



    def _ready_fixture(self, extension_algo="WINDOW", extension_shard="shard_00000"):
        manifest = self._pending_v2_manifest()
        refs = manifest["official_evidence"]
        docs = {task: preflight._pinned_official_json(ROOT, pin) for task, pin in refs.items()}
        sql_rows = []
        for task in ("EXP-02", "EXP-03"):
            shards = docs[task]["shard_verifications"]
            if task == "EXP-03":
                shards = {"HDBSCAN_ADAPTIVE": shards}
            for algo, items in shards.items():
                for shard, pin in items.items():
                    rid = pin["experiment_run_id"]
                    counts = {"membership_rows": pin["membership_row_count"], "summary_rows": pin["summary_row_count"]}
                    sql_rows.append({"experiment_run_id": rid, "algorithm_name": algo, "shard_id": shard,
                                     "experiment_status": "CANDIDATE", "success_marker_sha256": pin["success_marker_sha256"],
                                     "expected_counts": counts, "actual_counts": counts, "query_ids": ["q1", "q2", "q3", "q4"],
                                     "snapshots": {"experiment_snapshot_id": 41, "membership_snapshot_id": 42,
                                                   "summary_snapshot_id": 43}})
                    if algo == "HDBSCAN_ADAPTIVE":
                        sql_rows[-1]["adaptive_cohort_sha256"] = self.ADAPTIVE_COHORT_SHA256
        dataset = docs["EXP-04"]["dataset"]
        refs["MLI-04"] = {"path": "mli04-evidence.json", "sha256_lf": "a" * 64}
        refs["MLQ-01"] = {"path": "mlq01-evidence.json", "sha256_lf": "b" * 64}
        docs["MLI-04"] = {"schema_version": "mli04-readback-v1", "task_id": "MLI-04",
                          "status": "VERIFIED", "missing_run_ids": [], "scientific_approval": False,
                          "namespace": "iceberg.ml_qa_53cf813a988340fa9ebd45a3dfebbe7b",
                          "dataset_id": dataset["dataset_id"], "collection_id": dataset["collection_id"],
                          "exp04_sha256_lf": refs["EXP-04"]["sha256_lf"], "runs": sql_rows}
        representative = {}
        for row in sql_rows:
            representative.setdefault(row["algorithm_name"], row)
        rep_runs = {}
        for row in representative.values():
            idx = len(rep_runs) + 1
            rep_runs[row["experiment_run_id"]] = {
                **row["snapshots"],
                "trace": {
                    "query_id": f"20261124_120000_0000{idx}_v3i3r",
                    "dataset_id": dataset["dataset_id"],
                    "experiment_run_id": row["experiment_run_id"],
                    "gold_snapshot_id": dataset["gold_snapshot_id"],
                    **row["snapshots"],
                    "mainshock_event_id": f"main_{idx}",
                    "candidate_event_id": f"post_{idx}",
                    "lineage_check": "PASS",
                },
            }
        algo, shard = extension_algo, extension_shard
        extension_data = docs["EXP-05"]["dataset"]
        pin = docs["EXP-05"]["algorithm_comparisons"][algo]["bundle_verification"]["shard_verifications"][shard]
        model_version = docs["EXP-05"]["algorithm_config_pins"][algo]["model_config_version"]
        # Synthetic marker and pin. Keep insertion order different from Java TreeMap order.
        checksums = {
            "sequence_summary.parquet": "5" * 64,
            "requirements-lock.txt": "4" * 64,
            "optional-model-artifacts/weights.json": "6" * 64,
            "memberships.parquet": "3" * 64,
            "experiment_metrics.json": "2" * 64,
            "experiment_config.json": "1" * 64,
        }
        marker = {
            "schema_version": "1.0", "status": "COMPLETED",
            "dataset_id": extension_data["dataset_id"], "experiment_run_id": pin["experiment_run_id"],
            "algorithm_name": algo, "model_config_version": model_version, "checksums": checksums,
        }
        marker_bytes = (json.dumps(marker) + "\n").encode("utf-8")
        pin["success_marker_sha256"] = hashlib.sha256(marker_bytes).hexdigest()
        canonical_checksums = (
            "{experiment_config.json=" + "1" * 64 + ", experiment_metrics.json=" + "2" * 64
            + ", memberships.parquet=" + "3" * 64 + ", optional-model-artifacts/weights.json=" + "6" * 64
            + ", requirements-lock.txt=" + "4" * 64 + ", sequence_summary.parquet=" + "5" * 64 + "}"
        )
        fingerprint = hashlib.sha256("\n".join((
            marker["dataset_id"], marker["experiment_run_id"], algo, model_version, canonical_checksums,
        )).encode("utf-8")).hexdigest()
        extension = {key: extension_data[key] for key in ("dataset_id", "collection_id", "gold_snapshot_id")}
        extension.update(algorithm_name=algo, shard_id=shard, success_marker_path="extension/_SUCCESS.json",
                         experiment_run_id=pin["experiment_run_id"], success_marker_sha256=pin["success_marker_sha256"],
                         exp05_sha256_lf=refs["EXP-05"]["sha256_lf"],
                         import_receipt={"path": "extension-import.json", "sha256_lf": "c" * 64},
                         trace={
                             "query_id": "20261124_120000_00009_v3i3r",
                             "dataset_id": extension_data["dataset_id"],
                             "experiment_run_id": pin["experiment_run_id"],
                             "gold_snapshot_id": extension_data["gold_snapshot_id"],
                             "experiment_snapshot_id": 51,
                             "membership_snapshot_id": 52,
                             "summary_snapshot_id": 53,
                             "mainshock_event_id": "ext_main_1",
                             "candidate_event_id": "ext_post_1",
                             "lineage_check": "PASS",
                         })
        adaptive_row = representative["HDBSCAN_ADAPTIVE"]
        adaptive_rid = adaptive_row["experiment_run_id"]
        adaptive_pin = docs["EXP-03"]["shard_verifications"][adaptive_row["shard_id"]]
        rerun_verification = {
            "dataset_id": dataset["dataset_id"],
            "namespace": "iceberg.ml_qa_53cf813a988340fa9ebd45a3dfebbe7b",
            "before_rerun": {
                "experiment_run": {
                    "snapshot_query_id": "20261124_120100_00010_v3i3r",
                    "count_query_id": "20261124_120100_00011_v3i3r",
                    "snapshots": [[41, None]],
                    "run_row_counts": {adaptive_rid: 1},
                },
                "sequence_membership": {
                    "snapshot_query_id": "20261124_120100_00012_v3i3r",
                    "count_query_id": "20261124_120100_00013_v3i3r",
                    "snapshots": [[42, None]],
                    "run_row_counts": {adaptive_rid: adaptive_pin["membership_row_count"]},
                },
                "sequence_summary": {
                    "snapshot_query_id": "20261124_120100_00014_v3i3r",
                    "count_query_id": "20261124_120100_00015_v3i3r",
                    "snapshots": [[43, None]],
                    "run_row_counts": {adaptive_rid: adaptive_pin["summary_row_count"]},
                },
            },
            "after_rerun": {
                "experiment_run": {
                    "snapshot_query_id": "20261124_120200_00020_v3i3r",
                    "count_query_id": "20261124_120200_00021_v3i3r",
                    "snapshots": [[41, None]],
                    "run_row_counts": {adaptive_rid: 1},
                },
                "sequence_membership": {
                    "snapshot_query_id": "20261124_120200_00022_v3i3r",
                    "count_query_id": "20261124_120200_00023_v3i3r",
                    "snapshots": [[42, None]],
                    "run_row_counts": {adaptive_rid: adaptive_pin["membership_row_count"]},
                },
                "sequence_summary": {
                    "snapshot_query_id": "20261124_120200_00024_v3i3r",
                    "count_query_id": "20261124_120200_00025_v3i3r",
                    "snapshots": [[43, None]],
                    "run_row_counts": {adaptive_rid: adaptive_pin["summary_row_count"]},
                },
            },
        }
        reject_verification = {
            "namespace": "iceberg.ml_qa_53cf813a988340fa9ebd45a3dfebbe7b",
            "cases": [
                {
                    "case_name": "checksum",
                    "experiment_run_id": "exp_qa_reject_checksum",
                    "expected_reason_code": "IMP_CHECKSUM_MISMATCH",
                    "actual_reason_code": "IMP_CHECKSUM_MISMATCH",
                    "experiment_status": "REJECTED",
                    "candidate_created": False,
                    "candidate_row_count": 0,
                    "query_id": "20261124_120300_00030_v3i3r",
                }
            ],
        }
        docs["MLQ-01"] = {"task_id": "MLQ-01", "status": "PASSED", "scientific_approval": False,
                          "mli04_sha256_lf": refs["MLI-04"]["sha256_lf"], "dataset_id": dataset["dataset_id"],
                          "gold_snapshot_id": dataset["gold_snapshot_id"],
                          "checks": dict.fromkeys(("gold_to_report_lineage", "count_parity", "reject_without_candidate",
                                                   "rerun_no_duplicate", "extension_rule_preserved"), True),
                          "runs": rep_runs, "extension": extension,
                          "reject_verification": reject_verification,
                          "rerun_verification": rerun_verification}
        manifest.update(package_status="PASS_OFFICIAL_READY", official_release_ready=True, pending_official_gates=[])
        by_path = {value["path"]: docs[task] for task, value in refs.items()}
        by_path["extension/_SUCCESS.json"] = marker_bytes
        by_path["extension-import.json"] = {
            "experiment_run_id": extension["experiment_run_id"],
            "dataset_id": extension["dataset_id"],
            "algorithm_name": algo,
            "namespace": "iceberg.ml_qa_53cf813a988340fa9ebd45a3dfebbe7b",
            "experiment_status": "CANDIDATE",
            "result_bundle_sha256": fingerprint,
            "collection_scope": {
                "scope": "WHOLE_WINDOW_SHARD", "collection_id": extension["collection_id"], "shard_id": shard,
            },
            "experiment_snapshot_id": 51, "membership_snapshot_id": 52, "summary_snapshot_id": 53,
            "readback": {"sequence_membership": pin["membership_row_count"],
                         "sequence_summary": pin["summary_row_count"], "experiment_run": 1},
        }
        return manifest, by_path

    def _run_ready_fixture(self, manifest, by_path):
        from unittest.mock import patch
        resolve_repo_path = preflight._resolve_repo_path
        with tempfile.TemporaryDirectory() as folder:
            temp_root = Path(folder)
            path = temp_root / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            relative_marker = "extension/_SUCCESS.json"
            if relative_marker in by_path:
                marker = temp_root / relative_marker
                marker.parent.mkdir()
                marker.write_bytes(by_path[relative_marker])

            def resolve(root, relative):
                return resolve_repo_path(temp_root if relative == relative_marker else root, relative)

            with (patch.object(preflight, "_pinned_official_json", side_effect=lambda root, pin: by_path[pin["path"]]),
                  patch.object(preflight, "_resolve_repo_path", side_effect=resolve)):
                return preflight.run_preflight(path)

    def test_v2_accepts_four_representative_runs_and_pinned_extension(self):
        manifest, documents = self._ready_fixture()
        result = self._run_ready_fixture(manifest, documents)
        self.assertEqual("PASS_OFFICIAL_READY", result["overall_status"])
        mli04 = next(row for row in result["verified_runs"]
                     if row["task_id"] == "MLI-04" and row["status"] == "VERIFIED")
        self.assertEqual(self.ADAPTIVE_COHORT_SHA256, mli04["adaptive_cohort_sha256"])
        mlq = next(row for row in result["verified_runs"] if row["task_id"] == "MLQ-01")
        self.assertEqual(4, len(mlq["run_ids"]))
        self.assertEqual({"WINDOW", "DBSCAN", "HDBSCAN_GLOBAL", "HDBSCAN_ADAPTIVE"}, set(mlq["algorithms"]))
        self.assertEqual("52", mlq["extension"]["snapshots"]["membership_snapshot_id"])
        self.assertEqual(
            documents["extension-import.json"]["result_bundle_sha256"], mlq["extension"]["result_bundle_sha256"],
        )

    def test_v2_rejects_unverified_adaptive_cohorts_in_official_readback(self):
        expected, wrong = self.ADAPTIVE_COHORT_SHA256, "0" * 64
        for digests in [(None, None), (wrong, wrong), (expected, wrong), (wrong, expected), (None, expected)]:
            with self.subTest(digests=digests):
                manifest, documents = self._ready_fixture()
                adaptive = [row for row in documents["mli04-evidence.json"]["runs"]
                            if row["algorithm_name"] == "HDBSCAN_ADAPTIVE"]
                self.assertEqual(2, len(adaptive))
                for row, digest in zip(adaptive, digests):
                    if digest is None:
                        del row["adaptive_cohort_sha256"]
                    else:
                        row["adaptive_cohort_sha256"] = digest
                with self.assertRaisesRegex(ValueError, "DEMO_MLI04_ADAPTIVE_COHORT_MISMATCH"):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_missing_or_wrong_import_fingerprint(self):
        for value in (None, "not-a-bundle-sha", "0" * 64):
            with self.subTest(value=value):
                manifest, documents = self._ready_fixture()
                receipt = documents["extension-import.json"]
                if value is None:
                    del receipt["result_bundle_sha256"]
                else:
                    receipt["result_bundle_sha256"] = value
                with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_EXTENSION_BUNDLE_FINGERPRINT"):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_requires_exactly_one_registry_readback_row(self):
        for count in (None, 0, 2, True):
            with self.subTest(count=count):
                manifest, documents = self._ready_fixture()
                readback = documents["extension-import.json"]["readback"]
                if count is None:
                    del readback["experiment_run"]
                else:
                    readback["experiment_run"] = count
                with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_EXTENSION_IMPORT"):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_mutating_even_the_success_marker_line_endings(self):
        manifest, documents = self._ready_fixture()
        key = "extension/_SUCCESS.json"
        documents[key] = documents[key].replace(b"\n", b"\r\n")
        with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_EXTENSION_MARKER_SHA"):
            self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_one_algorithm_and_missing_adaptive(self):
        for retained in (1, 3):
            with self.subTest(retained=retained):
                manifest, documents = self._ready_fixture()
                mlq = documents["mlq01-evidence.json"]
                valid_runs = {row["experiment_run_id"]: row["algorithm_name"] for row in documents["mli04-evidence.json"]["runs"]}
                baseline = [(rid, pins) for rid, pins in mlq["runs"].items() if valid_runs[rid] != "HDBSCAN_ADAPTIVE"]
                mlq["runs"] = dict(baseline[:retained])
                with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_ALGORITHM_COVERAGE"):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_adaptive_or_other_non_window_extension_algorithms(self):
        for algo in ("HDBSCAN_ADAPTIVE", "DBSCAN", "HDBSCAN_GLOBAL"):
            with self.subTest(algo=algo):
                manifest, documents = self._ready_fixture(extension_algo=algo)
                with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_EXTENSION_PIN"):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_snapshot_drift_and_wrong_extension_lineage(self):
        for target, field, value, error in (
            ("run", "membership_snapshot_id", 44, "DEMO_MLQ01_RUN_MISMATCH"),
            ("extension", "gold_snapshot_id", "44", "DEMO_MLQ01_EXTENSION_PIN"),
            ("extension", "exp05_sha256_lf", "0" * 64, "DEMO_MLQ01_EXTENSION_PIN"),
            ("extension", "experiment_run_id", "wrong-run", "DEMO_MLQ01_EXTENSION_PIN"),
            ("extension", "success_marker_sha256", "0" * 64, "DEMO_MLQ01_EXTENSION_PIN"),
            ("receipt", "experiment_status", "IMPORT_VALIDATING", "DEMO_MLQ01_EXTENSION_IMPORT"),
            ("receipt", "membership_snapshot_id", 0, "DEMO_PIN_INVALID_INT64_SNAPSHOT"),
        ):
            with self.subTest(target=target, field=field):
                manifest, documents = self._ready_fixture()
                mlq = documents["mlq01-evidence.json"]
                fields = {"run": next(iter(mlq["runs"].values())),
                          "extension": mlq["extension"], "receipt": documents["extension-import.json"]}
                fields[target][field] = value
                with self.assertRaisesRegex(ValueError, error):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_mlq01_missing_or_tampered_trace_proof(self):
        for mutate, error in (
            (lambda mlq: next(iter(mlq["runs"].values())).pop("trace"), "DEMO_MLQ01_TRACE_INVALID"),
            (lambda mlq: mlq["extension"].pop("trace"), "DEMO_MLQ01_TRACE_INVALID"),
            (lambda mlq: next(iter(mlq["runs"].values()))["trace"].__setitem__("lineage_check", "FAIL"),
             "DEMO_MLQ01_TRACE_INVALID"),
            (lambda mlq: next(iter(mlq["runs"].values()))["trace"].__setitem__("query_id", " "),
             "DEMO_MLQ01_TRACE_INVALID"),
            (lambda mlq: mlq["extension"]["trace"].__setitem__(
                "query_id", next(iter(mlq["runs"].values()))["trace"]["query_id"]),
             "DEMO_MLQ01_TRACE_DUPLICATE_QUERY_ID"),
            (lambda mlq: mlq["extension"]["trace"].__setitem__(
                "candidate_event_id", mlq["extension"]["trace"]["mainshock_event_id"]),
             "DEMO_MLQ01_TRACE_INVALID"),
            (lambda mlq: mlq["extension"]["trace"].__setitem__("gold_snapshot_id", "44"),
             "DEMO_MLQ01_TRACE_INVALID"),
            (lambda mlq: next(iter(mlq["runs"].values()))["trace"].__setitem__("membership_snapshot_id", 99),
             "DEMO_MLQ01_TRACE_INVALID"),
        ):
            with self.subTest(error=error):
                manifest, documents = self._ready_fixture()
                mutate(documents["mlq01-evidence.json"])
                with self.assertRaisesRegex(ValueError, error):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_mlq01_missing_or_invalid_reject_verification(self):
        for mutate in (
            lambda mlq: mlq.pop("reject_verification"),
            lambda mlq: mlq["reject_verification"].pop("namespace"),
            lambda mlq: mlq["reject_verification"].__setitem__("cases", []),
            lambda mlq: mlq["reject_verification"].__setitem__("namespace", "iceberg.gold_prod"),
            lambda mlq: mlq["reject_verification"].__setitem__("namespace", "iceberg.dat03_unified_20261114"),
            lambda mlq: mlq["reject_verification"].__setitem__("namespace", "iceberg.ml"),
            lambda mlq: mlq["reject_verification"].__setitem__("namespace", "iceberg.gold_qa_12345678"),
            lambda mlq: mlq["reject_verification"].__setitem__("namespace", "iceberg.mlq_qa"),
            lambda mlq: mlq["reject_verification"]["cases"][0].__setitem__("candidate_created", True),
            lambda mlq: mlq["reject_verification"]["cases"][0].__setitem__("experiment_status", "CANDIDATE"),
            lambda mlq: mlq["reject_verification"]["cases"][0].__setitem__("candidate_row_count", 1),
            lambda mlq: mlq["reject_verification"]["cases"][0].__setitem__("query_id", ""),
            lambda mlq: mlq["reject_verification"]["cases"][0].__setitem__("actual_reason_code", "OTHER"),
            lambda mlq: mlq["reject_verification"]["cases"][0].__setitem__(
                "experiment_run_id", next(iter(mlq["runs"].keys()))),
        ):
            with self.subTest():
                manifest, documents = self._ready_fixture()
                mutate(documents["mlq01-evidence.json"])
                with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_REJECT_EVIDENCE_INVALID"):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_rejects_mlq01_missing_or_drifted_rerun_verification(self):
        def get_rid(mlq):
            return next(iter(mlq["rerun_verification"]["before_rerun"]["experiment_run"]["run_row_counts"]))

        def swap_two_query_ids_across_all_tables(mlq):
            for tbl in preflight.ML_TABLES:
                mlq["rerun_verification"]["before_rerun"][tbl]["snapshot_query_id"] = "q_reused_a"
                mlq["rerun_verification"]["before_rerun"][tbl]["count_query_id"] = "q_reused_b"
                mlq["rerun_verification"]["after_rerun"][tbl]["snapshot_query_id"] = "q_reused_b"
                mlq["rerun_verification"]["after_rerun"][tbl]["count_query_id"] = "q_reused_a"

        for mutate in (
            lambda mlq: mlq.pop("rerun_verification"),
            lambda mlq: mlq["rerun_verification"].pop("namespace"),
            lambda mlq: mlq["rerun_verification"].__setitem__("namespace", " "),
            lambda mlq: mlq["rerun_verification"].__setitem__(
                "namespace", "iceberg.ml_qa_ffffffffffffffffffffffffffffffff"),
            lambda mlq: mlq["rerun_verification"].__setitem__("namespace", "iceberg.dat03_unified_20261114"),
            swap_two_query_ids_across_all_tables,
            lambda mlq: mlq["rerun_verification"]["before_rerun"]["experiment_run"].__setitem__(
                "count_query_id",
                mlq["rerun_verification"]["before_rerun"]["experiment_run"]["snapshot_query_id"]),
            lambda mlq: mlq["rerun_verification"]["after_rerun"]["sequence_membership"].__setitem__(
                "snapshot_query_id",
                mlq["rerun_verification"]["before_rerun"]["experiment_run"]["count_query_id"]),
            lambda mlq: mlq["rerun_verification"]["after_rerun"]["experiment_run"]["snapshots"].append([99, 41]),
            lambda mlq: mlq["rerun_verification"]["after_rerun"]["sequence_membership"]["run_row_counts"].__setitem__(
                get_rid(mlq), 99999),
            lambda mlq: (
                mlq["rerun_verification"]["before_rerun"]["experiment_run"]["run_row_counts"].__setitem__(
                    get_rid(mlq), 2),
                mlq["rerun_verification"]["after_rerun"]["experiment_run"]["run_row_counts"].__setitem__(
                    get_rid(mlq), 2),
            ),
            lambda mlq: mlq["rerun_verification"]["after_rerun"]["experiment_run"].__setitem__(
                "snapshot_query_id",
                mlq["rerun_verification"]["before_rerun"]["experiment_run"]["snapshot_query_id"]),
            lambda mlq: mlq["rerun_verification"]["after_rerun"]["sequence_summary"].__setitem__(
                "count_query_id",
                mlq["rerun_verification"]["before_rerun"]["sequence_summary"]["count_query_id"]),
            lambda mlq: (
                mlq["rerun_verification"]["before_rerun"]["sequence_membership"].__setitem__("snapshots", [[99, None]]),
                mlq["rerun_verification"]["after_rerun"]["sequence_membership"].__setitem__("snapshots", [[99, None]]),
            ),
        ):
            with self.subTest():
                manifest, documents = self._ready_fixture()
                mutate(documents["mlq01-evidence.json"])
                with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_RERUN_EVIDENCE_INVALID"):
                    self._run_ready_fixture(manifest, documents)

    def test_v2_enforces_namespace_parity_two_level_runs_and_real_mli03_mli04_evidence(self):
        # 1. Missing or Gold MLI-04 namespace fails closed.
        for bad_ns in (None, "iceberg.dat03_unified_20261114", "iceberg.gold"):
            manifest, documents = self._ready_fixture()
            if bad_ns is None:
                documents["mli04-evidence.json"].pop("namespace")
            else:
                documents["mli04-evidence.json"]["namespace"] = bad_ns
            with self.assertRaisesRegex(ValueError, "DEMO_MLI04_NOT_VERIFIED"):
                self._run_ready_fixture(manifest, documents)

        # 2. Gold namespace in extension import receipt fails closed.
        manifest, documents = self._ready_fixture()
        documents["extension-import.json"]["namespace"] = "iceberg.dat03_unified_20261114"
        with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_EXTENSION_IMPORT"):
            self._run_ready_fixture(manifest, documents)

        # 3. EXTENSION rerun must match extension-import receipt namespace, not REPRODUCTION MLI-04 namespace.
        manifest, documents = self._ready_fixture()
        mlq = documents["mlq01-evidence.json"]
        ext = mlq["extension"]
        ext_rid = ext["experiment_run_id"]
        ext_counts = documents[manifest["official_evidence"]["EXP-05"]["path"]][
            "algorithm_comparisons"]["WINDOW"]["bundle_verification"]["shard_verifications"][ext["shard_id"]]
        ext_ns = "iceberg.ml_qa_eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
        documents["extension-import.json"]["namespace"] = ext_ns
        mlq["rerun_verification"] = {
            "dataset_id": ext["dataset_id"],
            "namespace": ext_ns,
            "before_rerun": {
                "experiment_run": {"snapshot_query_id": "q_b1", "count_query_id": "q_b2",
                                   "snapshots": [[51, None]], "run_row_counts": {ext_rid: 1}},
                "sequence_membership": {"snapshot_query_id": "q_b3", "count_query_id": "q_b4",
                                        "snapshots": [[52, None]],
                                        "run_row_counts": {ext_rid: ext_counts["membership_row_count"]}},
                "sequence_summary": {"snapshot_query_id": "q_b5", "count_query_id": "q_b6",
                                     "snapshots": [[53, None]],
                                     "run_row_counts": {ext_rid: ext_counts["summary_row_count"]}},
            },
            "after_rerun": {
                "experiment_run": {"snapshot_query_id": "q_a1", "count_query_id": "q_a2",
                                   "snapshots": [[51, None]], "run_row_counts": {ext_rid: 1}},
                "sequence_membership": {"snapshot_query_id": "q_a3", "count_query_id": "q_a4",
                                        "snapshots": [[52, None]],
                                        "run_row_counts": {ext_rid: ext_counts["membership_row_count"]}},
                "sequence_summary": {"snapshot_query_id": "q_a5", "count_query_id": "q_a6",
                                     "snapshots": [[53, None]],
                                     "run_row_counts": {ext_rid: ext_counts["summary_row_count"]}},
            },
        }
        self.assertEqual("PASS_OFFICIAL_READY", self._run_ready_fixture(manifest, documents)["overall_status"])
        mlq["rerun_verification"]["namespace"] = documents["mli04-evidence.json"]["namespace"]
        with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_RERUN_EVIDENCE_INVALID"):
            self._run_ready_fixture(manifest, documents)

        # 4. Deleting run-level snapshot pin while keeping trace intact triggers DEMO_MLQ01_RUN_MISMATCH.
        manifest, documents = self._ready_fixture()
        next(iter(documents["mlq01-evidence.json"]["runs"].values())).pop("experiment_snapshot_id")
        with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_RUN_MISMATCH"):
            self._run_ready_fixture(manifest, documents)

        # 5. Real merged MLI-04 & MLI-03 evidence files match namespace, snapshots, and rerun checks.
        manifest, documents = self._ready_fixture()
        real_mli04_path = "docs/evidence/MLI-04_OFFICIAL_READBACK_20261124.json"
        real_mli03_path = "docs/evidence/MLI-03_ADAPTIVE_REPRODUCTION_20261124.json"
        mli04_bytes = (ROOT / real_mli04_path).read_bytes().replace(b"\r\n", b"\n")
        mli03_bytes = (ROOT / real_mli03_path).read_bytes().replace(b"\r\n", b"\n")
        mli04_sha = hashlib.sha256(mli04_bytes).hexdigest()
        mli03_sha = hashlib.sha256(mli03_bytes).hexdigest()
        real_mli04 = json.loads(mli04_bytes)
        real_mli03 = json.loads(mli03_bytes)
        manifest["official_evidence"]["MLI-04"] = {"path": real_mli04_path, "sha256_lf": mli04_sha}
        documents[real_mli04_path] = real_mli04
        documents[real_mli03_path] = real_mli03
        mlq = documents["mlq01-evidence.json"]
        mlq["mli04_sha256_lf"] = mli04_sha
        rep_by_algo = {}
        for row in real_mli04["runs"]:
            rep_by_algo.setdefault(row["algorithm_name"], row)
        mlq["runs"] = {
            row["experiment_run_id"]: {
                **row["snapshots"],
                "trace": {
                    "query_id": f"20261124_130000_0000{idx + 1}_v3i3r",
                    "dataset_id": real_mli04["dataset_id"],
                    "experiment_run_id": row["experiment_run_id"],
                    "gold_snapshot_id": mlq["gold_snapshot_id"],
                    **row["snapshots"],
                    "mainshock_event_id": f"main_real_{idx}",
                    "candidate_event_id": f"post_real_{idx}",
                    "lineage_check": "PASS",
                },
            }
            for idx, row in enumerate(rep_by_algo.values())
        }
        mlq["rerun_verification"] = {"path": real_mli03_path, "sha256_lf": mli03_sha}
        self.assertEqual("PASS_OFFICIAL_READY", self._run_ready_fixture(manifest, documents)["overall_status"])

        documents[real_mli03_path] = copy.deepcopy(real_mli03)
        documents[real_mli03_path]["runs"][0]["receipt"]["namespace"] = "iceberg.ml_qa_ffffffffffffffffffffffffffffffff"
        with self.assertRaisesRegex(ValueError, "DEMO_MLQ01_RERUN_EVIDENCE_INVALID"):
            self._run_ready_fixture(manifest, documents)

    def test_v2_accepts_pinned_external_mlq01_evidence_and_blocks_sha_drift(self):
        manifest, documents = self._ready_fixture()
        mlq = documents["mlq01-evidence.json"]
        ext_trace = mlq["extension"].pop("trace")
        reject_doc = mlq.pop("reject_verification")
        rerun_doc = mlq.pop("rerun_verification")
        documents["ext-trace.json"] = ext_trace
        documents["mlq-reject.json"] = reject_doc
        documents["mlq-rerun.json"] = rerun_doc
        mlq["extension"]["trace"] = {"path": "ext-trace.json", "sha256_lf": "d" * 64}
        mlq["reject_verification"] = {"path": "mlq-reject.json", "sha256_lf": "e" * 64}
        mlq["rerun_verification"] = {"path": "mlq-rerun.json", "sha256_lf": "f" * 64}
        result = self._run_ready_fixture(manifest, documents)
        self.assertEqual("PASS_OFFICIAL_READY", result["overall_status"])

        with tempfile.TemporaryDirectory() as folder:
            temp_root = Path(folder)
            pinned = temp_root / "mlq-rerun.json"
            payload = (json.dumps(rerun_doc) + "\n").encode("utf-8")
            pinned.write_bytes(payload)
            digest = hashlib.sha256(payload).hexdigest()
            loaded = preflight._resolve_receipt_section(
                temp_root, {"path": "mlq-rerun.json", "sha256_lf": digest}, "DEMO_MLQ01_RERUN_EVIDENCE_INVALID"
            )
            self.assertEqual(rerun_doc["dataset_id"], loaded["dataset_id"])
            with self.assertRaisesRegex(ValueError, "DEMO_OFFICIAL_SHA_MISMATCH"):
                preflight._resolve_receipt_section(
                    temp_root, {"path": "mlq-rerun.json", "sha256_lf": "0" * 64}, "DEMO_MLQ01_RERUN_EVIDENCE_INVALID"
                )


if __name__ == "__main__":
    unittest.main()
