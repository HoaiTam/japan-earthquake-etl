"""EXP-01: Unit and contract tests for the shared experiment harness.

Tests offline without network, MinIO, Docker or live cloud APIs.
Covers:
- Mode-aware bundle loader (explicit DEVELOPMENT vs OFFICIAL).
- Fail-fast validation of checksums, schemas, non-finite values before fitting.
- Output skeleton conformance for memberships and sequence summary.
- Result writer separation (pilot manifest vs official _SUCCESS.json).
- Colab path adapter with local smoke gate enforcement.
"""
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("experiment_harness", ROOT / "notebooks/experiment_harness.py")
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)

spec_pilot = importlib.util.spec_from_file_location("pilot_bundle", ROOT / "notebooks/pilot_bundle.py")
pilot = importlib.util.module_from_spec(spec_pilot)
spec_pilot.loader.exec_module(pilot)

MID = "evt_" + "1" * 32
POST_EVT = "evt_" + "2" * 32


def create_pilot_fixture(root: Path) -> str:
    """Create a minimal valid synthetic DAT-02 pilot bundle."""
    centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
    selection = {"source_dataset_id": "ds_fixture", "native_report_sha256": "f" * 64, "mainshock_ids": [MID]}
    scaling = {"mode": "GLOBAL", "space_scale_km": 50.0, "depth_scale_km": 30.0, "time_scale_hours": 720.0}
    payload = {
        "source_dataset_id": "ds_fixture",
        "native_report_sha256": "f" * 64,
        "scaling_config_sha256": harness.sha256_bytes(harness.canonical(scaling).encode()),
        "selection_config_sha256": harness.sha256_bytes(harness.canonical(selection).encode()),
    }
    pid = "pilot_" + harness.sha256_bytes(harness.canonical(payload).encode())

    main_row = {
        "schema_version": "fixture-v1",
        "dataset_id": "ds_fixture",
        "pilot_id": pid,
        "mainshock_event_id": MID,
        "selection_rule_version": "fixture-v1",
        "window_model_version": "fixture-v1",
        "resource_guard_status": "PASS",
        "resource_reason_code": None,
        "latitude": 35.0,
        "longitude": 140.0,
        "depth_km": 80.0,
        "magnitude": 6.0,
        "mc_value": 1.0,
        "pre_window_hours": 24.0,
        "post_window_hours": 720.0,
        "search_radius_km": 50.0,
        "event_time_utc": centre,
        "created_at_utc": centre,
        "candidate_count": 2,
    }

    own_row = {
        "schema_version": "fixture-v1",
        "dataset_id": "ds_fixture",
        "pilot_id": pid,
        "mainshock_event_id": MID,
        "candidate_event_id": MID,
        "relative_time_role": "MAINSHOCK",
        "window_model_version": "fixture-v1",
        "feature_version": "relative-4d-v1",
        "scaling_config_version": "fixture-v1",
        "candidate_latitude": 35.0,
        "candidate_longitude": 140.0,
        "candidate_depth_km": 80.0,
        "candidate_magnitude": 6.0,
        "mainshock_magnitude": 6.0,
        "mc_value": 1.0,
        "dx_km": 0.0,
        "dy_km": 0.0,
        "dz_km": 0.0,
        "delta_time_hours": 0.0,
        "distance_3d_km": 0.0,
        "x_scaled": 0.0,
        "y_scaled": 0.0,
        "z_scaled": 0.0,
        "time_scaled": 0.0,
        "space_scale_km": 50.0,
        "depth_scale_km": 30.0,
        "time_scale_hours": 720.0,
        "is_mainshock": True,
        "candidate_time_utc": centre,
        "created_at_utc": centre,
    }

    post_row = dict(
        own_row,
        candidate_event_id=POST_EVT,
        is_mainshock=False,
        relative_time_role="POST",
        candidate_magnitude=3.5,
        candidate_time_utc=centre + timedelta(hours=2),
        delta_time_hours=2.0,
        time_scaled=2.0 / 720.0,
        dx_km=10.0,
        dy_km=5.0,
        dz_km=-2.0,
        distance_3d_km=11.3578,
        x_scaled=10.0 / 50.0,
        y_scaled=5.0 / 50.0,
        z_scaled=-2.0 / 30.0,
    )

    arrow_types = {
        "string": pa.string(),
        "double": pa.float64(),
        "long": pa.int64(),
        "timestamp": pa.timestamp("us", tz="UTC"),
        "boolean": pa.bool_(),
    }

    pq.write_table(
        pa.Table.from_pylist([own_row, post_row], schema=pa.schema([(k, arrow_types[t]) for k, t in pilot.FEATURE_TYPES.items()])),
        root / "features-part-00000.parquet",
    )
    pq.write_table(
        pa.Table.from_pylist([main_row], schema=pa.schema([(k, arrow_types[t]) for k, t in pilot.MAIN_TYPES.items()])),
        root / "mainshocks.parquet",
    )

    cov = {
        "research_ready": False,
        "status": "PARTIAL",
        "windows": [
            {
                "mainshock_event_id": MID,
                "candidate_count": 2,
                "required_start_utc": "2000-06-30T00:00:00Z",
                "required_end_utc_exclusive": "2000-07-31T00:00:00.000001Z",
                "status": "PARTIAL",
                "available_ranges": [{"start_utc": "2000-06-30T00:00:00Z", "end_utc_exclusive": "2000-07-05T00:00:00Z"}],
                "missing_ranges": [{"start_utc": "2000-07-05T00:00:00Z", "end_utc_exclusive": "2000-07-31T00:00:00.000001Z"}],
            }
        ],
    }
    (root / "coverage_report.json").write_text(harness.canonical(cov) + "\n", encoding="utf-8")

    table_names = ["event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band"]
    pins = {t: {"table": "iceberg.fixture." + t, "snapshot_id": 9223372036854775700 + i} for i, t in enumerate(table_names)}

    manifest = {
        "manifest_version": "dat-02-pilot-v1",
        "pilot_id": pid,
        "artifact_kind": "DEVELOPMENT_PILOT",
        "allowed_usage": "DEVELOPMENT_ONLY",
        "research_ready": False,
        "source_dataset_status": "BUILDING",
        "source_dataset_id": "ds_fixture",
        "identity_payload": payload,
        "scaling_config_json": harness.canonical(scaling),
        "selection_config_json": harness.canonical(selection),
        "vector_columns": harness.VECTOR_COLUMNS,
        "feature_version": "relative-4d-v1",
        "scaling_config_version": "fixture-v1",
        "feature_count": 2,
        "mainshock_count": 1,
        "invalid_count": 0,
        "selected_mainshock_ids": [MID],
        "coverage_status": "PARTIAL",
        "feature_schema": [{"name": k, "type": t} for k, t in pilot.FEATURE_TYPES.items()],
        "mainshock_schema": [{"name": k, "type": t} for k, t in pilot.MAIN_TYPES.items()],
        "coverage_sha256": harness.sha256_bytes((root / "coverage_report.json").read_bytes()),
        "source_manifest": {
            "dataset_id": "ds_fixture",
            "dataset_status": "BUILDING",
            "gold_publication_id": "pub_fixture",
            "gold_snapshot_id": pins["event_current"]["snapshot_id"],
            "physical_gold_table_name": pins["event_current"]["table"],
        },
        "gold_publication": {"publication_id": "pub_fixture", "published": True, "verify_status": "PASSED", "snapshot_bundle": pins},
    }
    manifest["schema_sha256"] = harness.sha256_bytes(harness.canonical(manifest["feature_schema"]).encode())
    (root / "pilot_manifest.json").write_text(harness.canonical(manifest) + "\n", encoding="utf-8")

    names = sorted(p.name for p in root.iterdir() if p.name != "checksums.sha256")
    checksums_txt = "".join(f"{harness.sha256_bytes((root / n).read_bytes())}  {n}\n" for n in names)
    (root / "checksums.sha256").write_text(checksums_txt, encoding="utf-8")
    return pid


def create_official_fixture(root: Path) -> str:
    """Create a minimal valid synthetic MLD-05 official dataset export bundle."""
    centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
    ds_id = "ds_official_1234567890abcdef"

    own_row = {
        "dataset_id": ds_id,
        "mainshock_event_id": MID,
        "candidate_event_id": MID,
        "relative_time_role": "MAINSHOCK",
        "is_mainshock": True,
        "x_scaled": 0.0,
        "y_scaled": 0.0,
        "z_scaled": 0.0,
        "time_scaled": 0.0,
        "candidate_magnitude": 6.0,
        "candidate_time_utc": centre,
    }
    post_row = {
        "dataset_id": ds_id,
        "mainshock_event_id": MID,
        "candidate_event_id": POST_EVT,
        "relative_time_role": "POST",
        "is_mainshock": False,
        "x_scaled": 0.2,
        "y_scaled": 0.1,
        "z_scaled": -0.05,
        "time_scaled": 0.005,
        "candidate_magnitude": 3.5,
        "candidate_time_utc": centre + timedelta(hours=1),
    }

    schema = pa.schema([
        ("dataset_id", pa.string()),
        ("mainshock_event_id", pa.string()),
        ("candidate_event_id", pa.string()),
        ("relative_time_role", pa.string()),
        ("is_mainshock", pa.bool_()),
        ("x_scaled", pa.float64()),
        ("y_scaled", pa.float64()),
        ("z_scaled", pa.float64()),
        ("time_scaled", pa.float64()),
        ("candidate_magnitude", pa.float64()),
        ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
    ])
    pq.write_table(pa.Table.from_pylist([own_row, post_row], schema=schema), root / "features-part-00000.parquet")

    manifest = {
        "schema_version": "1.0",
        "dataset_id": ds_id,
        "dataset_status": "EXPORTED",
        "dataset_split": "REPRODUCTION",
        "research_ready": True,
        "feature_row_count": 2,
        "mainshock_count": 1,
        "feature_version": "relative-4d-v1",
        "period_start_utc": "2000-01-01T00:00:00Z",
        "period_end_utc_exclusive": "2000-12-31T23:59:59Z",
    }
    (root / "dataset_manifest.json").write_text(harness.canonical(manifest) + "\n", encoding="utf-8")

    names = sorted(p.name for p in root.iterdir() if p.name != "checksums.sha256")
    checksums_txt = "".join(f"{harness.sha256_bytes((root / n).read_bytes())}  {n}\n" for n in names)
    (root / "checksums.sha256").write_text(checksums_txt, encoding="utf-8")
    return ds_id


class ExperimentHarnessTest(unittest.TestCase):
    def setUp(self):
        self.temp_pilot = tempfile.TemporaryDirectory()
        self.pilot_dir = Path(self.temp_pilot.name)
        self.pid = create_pilot_fixture(self.pilot_dir)

        self.temp_official = tempfile.TemporaryDirectory()
        self.official_dir = Path(self.temp_official.name)
        self.ds_id = create_official_fixture(self.official_dir)

        self.temp_out = tempfile.TemporaryDirectory()
        self.out_dir = Path(self.temp_out.name)

    def tearDown(self):
        self.temp_pilot.cleanup()
        self.temp_official.cleanup()
        self.temp_out.cleanup()

    # -------------------------------------------------------------------------
    # Loader Tests: Pilot & Official
    # -------------------------------------------------------------------------
    def test_load_development_pilot_succeeds(self):
        bundle = harness.load_feature_bundle(self.pilot_dir, mode=harness.MODE_DEVELOPMENT)
        self.assertEqual(bundle["mode"], harness.MODE_DEVELOPMENT)
        self.assertFalse(bundle["is_official"])
        self.assertEqual(bundle["bundle_id"], self.pid)
        self.assertEqual(bundle["features_table"].num_rows, 2)

    def test_corrupted_checksum_fails_before_fit(self):
        (self.pilot_dir / "coverage_report.json").write_text("{\"corrupted\": true}\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_CHECKSUM_MISMATCH:coverage_report.json"):
            harness.load_feature_bundle(self.pilot_dir, mode=harness.MODE_DEVELOPMENT)

    def test_nonfinite_feature_fails_before_fit(self):
        table = pq.read_table(self.pilot_dir / "features-part-00000.parquet")
        rows = table.to_pylist()
        rows[1]["x_scaled"] = float("nan")
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), self.pilot_dir / "features-part-00000.parquet")
        # Update checksum to isolate non-finite check
        names = sorted(p.name for p in self.pilot_dir.iterdir() if p.name != "checksums.sha256")
        (self.pilot_dir / "checksums.sha256").write_text(
            "".join(f"{harness.sha256_bytes((self.pilot_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "PILOT_NONFINITE_VALUE"):
            harness.load_feature_bundle(self.pilot_dir, mode=harness.MODE_DEVELOPMENT)

    def test_mode_separation_development_rejects_official_dataset(self):
        with self.assertRaisesRegex(ValueError, "DEVELOPMENT_LOADER_REJECTS_OFFICIAL_DATASET"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_DEVELOPMENT)

    def test_mode_separation_official_rejects_pilot_bundle(self):
        with self.assertRaisesRegex(ValueError, "OFFICIAL_LOADER_REJECTS_PILOT_BUNDLE"):
            harness.load_feature_bundle(self.pilot_dir, mode=harness.MODE_OFFICIAL)

    def test_load_official_dataset_succeeds(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        self.assertEqual(bundle["mode"], harness.MODE_OFFICIAL)
        self.assertTrue(bundle["is_official"])
        self.assertEqual(bundle["bundle_id"], self.ds_id)
        self.assertEqual(bundle["features_table"].num_rows, 2)

    def test_official_dataset_not_exported_is_rejected(self):
        manifest_path = self.official_dir / "dataset_manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["dataset_status"] = "BUILDING"
        manifest_path.write_text(harness.canonical(manifest) + "\n", encoding="utf-8")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text(
            "".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8"
        )
        with self.assertRaisesRegex(ValueError, "EXP_DATASET_NOT_EXPORTED"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    # -------------------------------------------------------------------------
    # Output Skeleton Builders
    # -------------------------------------------------------------------------
    def test_output_skeleton_conforms_to_schema(self):
        candidates = [
            {"candidate_event_id": MID, "delta_time_hours": 0.0},
            {"candidate_event_id": POST_EVT, "delta_time_hours": 3.0},
        ]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="run_123",
            dataset_id="ds_fixture",
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="HDBSCAN_GLOBAL",
            model_config_version="v1",
            cluster_labels=[0, 0],
            probabilities=[1.0, 0.95],
            normalized_distances=[0.0, 0.25],
        )
        self.assertEqual(len(mem_rows), 2)
        self.assertEqual(mem_rows[0]["event_role_candidate"], "MAINSHOCK")
        self.assertEqual(mem_rows[1]["event_role_candidate"], "POST")
        self.assertTrue(mem_rows[1]["is_sequence_member"])
        self.assertEqual(mem_rows[1]["multi_sequence_status"], "UNIQUE")
        self.assertEqual(mem_rows[1]["resolution_rank"], 1)

        summary_row = harness.build_sequence_summary_row(
            experiment_run_id="run_123",
            dataset_id="ds_fixture",
            mainshock_event_id=MID,
            candidate_count=2,
            algorithm_name="HDBSCAN_GLOBAL",
            model_config_version="v1",
            membership_rows=mem_rows,
            window_result_status="SUCCESS",
            runtime_seconds=0.15,
        )
        self.assertEqual(summary_row["candidate_count"], 2)
        self.assertEqual(summary_row["sequence_member_count"], 2)
        self.assertEqual(summary_row["aftershock_count"], 1)
        self.assertTrue(summary_row["productive_sequence_detected"])
        self.assertAlmostEqual(summary_row["mean_membership_probability"], 0.975, places=3)

    # -------------------------------------------------------------------------
    # Result Writer Separation (Pilot Manifest vs Official _SUCCESS.json)
    # -------------------------------------------------------------------------
    def test_development_result_writer_never_produces_success_marker(self):
        bundle = harness.load_feature_bundle(self.pilot_dir, mode=harness.MODE_DEVELOPMENT)
        candidates = [
            {"candidate_event_id": MID, "delta_time_hours": 0.0},
            {"candidate_event_id": POST_EVT, "delta_time_hours": 3.0},
        ]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="pilot_run_temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="pilot_run_temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_count=2,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        res = harness.write_experiment_result(
            output_dir=self.out_dir,
            bundle=bundle,
            algorithm_name="WINDOW",
            algorithm_version="1.0.0",
            model_config_version="dev_v1",
            model_config={"param": "test"},
            memberships=mem_rows,
            summaries=[sum_row],
            metrics={"time_sec": 0.05},
            mode=harness.MODE_DEVELOPMENT,
        )
        self.assertEqual(res["mode"], harness.MODE_DEVELOPMENT)
        self.assertFalse(res["has_success_marker"])
        self.assertFalse((self.out_dir / "_SUCCESS.json").exists())
        self.assertTrue((self.out_dir / "pilot_result_manifest.json").exists())

        pilot_manifest = json.loads((self.out_dir / "pilot_result_manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(pilot_manifest["artifact_kind"], "PILOT_EXPERIMENT_RESULT")
        self.assertEqual(pilot_manifest["allowed_usage"], "DEVELOPMENT_ONLY")
        self.assertFalse(pilot_manifest["research_ready"])

    def test_official_result_writer_produces_success_marker_with_checksums(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [
            {"candidate_event_id": MID, "delta_time_hours": 0.0},
            {"candidate_event_id": POST_EVT, "delta_time_hours": 3.0},
        ]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="exp_run_temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="HDBSCAN_GLOBAL",
            model_config_version="v1",
            cluster_labels=[0, 0],
            probabilities=[1.0, 0.9],
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="exp_run_temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_count=2,
            algorithm_name="HDBSCAN_GLOBAL",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        res = harness.write_experiment_result(
            output_dir=self.out_dir,
            bundle=bundle,
            algorithm_name="HDBSCAN_GLOBAL",
            algorithm_version="1.0.0",
            model_config_version="official_v1",
            model_config={"min_cluster_size": 2},
            memberships=mem_rows,
            summaries=[sum_row],
            metrics={"time_sec": 0.1},
            mode=harness.MODE_OFFICIAL,
        )
        self.assertEqual(res["mode"], harness.MODE_OFFICIAL)
        self.assertTrue(res["has_success_marker"])
        success_path = self.out_dir / "_SUCCESS.json"
        self.assertTrue(success_path.exists())

        success_marker = json.loads(success_path.read_text(encoding="utf-8"))
        self.assertEqual(success_marker["status"], "COMPLETED")
        self.assertEqual(success_marker["membership_row_count"], 2)
        self.assertEqual(success_marker["summary_row_count"], 1)
        self.assertIn("memberships.parquet", success_marker["checksums"])
        self.assertIn("sequence_summary.parquet", success_marker["checksums"])

    # -------------------------------------------------------------------------
    # Colab Path Adapter & Local Smoke Gate
    # -------------------------------------------------------------------------
    def test_colab_adapter_gate_passes_with_passed_local_report(self):
        report_file = self.out_dir / "local-smoke-report.json"
        report_data = {
            "local_smoke_status": "PASSED",
            "colab_migration_allowed": True,
            "pilot_id": self.pid,
        }
        report_file.write_text(harness.canonical(report_data) + "\n", encoding="utf-8")

        adapter = harness.ColabPathAdapter()
        verified = adapter.verify_local_smoke_gate(report_file)
        self.assertEqual(verified["local_smoke_status"], "PASSED")

    def test_colab_adapter_gate_blocks_when_smoke_failed_or_missing(self):
        adapter = harness.ColabPathAdapter()
        with self.assertRaisesRegex(ValueError, "COLAB_GATE_BLOCKED"):
            adapter.verify_local_smoke_gate(self.out_dir / "nonexistent-report.json")

        failed_report = self.out_dir / "failed-smoke-report.json"
        failed_report.write_text(harness.canonical({"local_smoke_status": "FAILED", "colab_migration_allowed": False}) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "COLAB_GATE_BLOCKED"):
            adapter.verify_local_smoke_gate(failed_report)

    def test_colab_adapter_path_resolution_and_unsafe_metadata(self):
        adapter = harness.ColabPathAdapter()
        in_p, out_p = adapter.resolve_paths(
            input_bundle="ml-pilot/pilot_1",
            output_dir="ml-output",
            drive_mount="/content/drive/MyDrive",
            is_colab=True,
        )
        self.assertEqual(str(in_p).replace("\\", "/"), "/content/drive/MyDrive/ml-pilot/pilot_1")
        self.assertEqual(str(out_p).replace("\\", "/"), "/content/drive/MyDrive/ml-output")

        # Rejection of credential injection in path
        with self.assertRaisesRegex(ValueError, "EXP_UNSAFE_METADATA"):
            adapter.resolve_paths(input_bundle="s3://user:secret_token@bucket/data", is_colab=True)

    def test_incomplete_output_never_writes_success_marker(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [
            {"candidate_event_id": MID, "delta_time_hours": 0.0},
        ]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="run_incomplete",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        # Invalid summary with illegal algorithm name that triggers an error during write
        with self.assertRaises(Exception):
            harness.write_experiment_result(
                output_dir=self.out_dir,
                bundle=bundle,
                algorithm_name="ILLEGAL_ALGO",
                algorithm_version="1.0.0",
                model_config_version="v1",
                model_config={},
                memberships=mem_rows,
                summaries=[],
                metrics={},
                mode=harness.MODE_OFFICIAL,
            )
        self.assertFalse((self.out_dir / "_SUCCESS.json").exists())

    def test_notebook_restart_run_all_passes(self):
        import importlib.util
        runner_spec = importlib.util.spec_from_file_location("inspect_runner", ROOT / "scripts/inspect-exp01-notebook.py")
        runner_mod = importlib.util.module_from_spec(runner_spec)
        runner_spec.loader.exec_module(runner_mod)

        notebook_file = ROOT / "notebooks/exp_01_harness_smoke.ipynb"
        report = runner_mod.run_notebook_cells(notebook_file, {})
        self.assertTrue(report["source_notebook_outputs_cleared"])
        if report.get("runner_mode") == "nbclient":
            self.assertEqual(report["notebook_status"], "RESTART_RUN_ALL_PASSED")
            self.assertEqual(report["local_smoke_status"], "PASSED")
            self.assertTrue(report["fresh_kernel_acceptance"])
            self.assertTrue(report["colab_migration_allowed"])
        else:
            self.assertEqual(report["notebook_status"], "FALLBACK_EXEC_PASSED")
            self.assertEqual(report["local_smoke_status"], "FALLBACK_EXEC_WARNING")
            self.assertFalse(report["fresh_kernel_acceptance"])
            self.assertFalse(report["colab_migration_allowed"])

    def test_official_loader_rejects_empty_checksums(self):
        (self.official_dir / "checksums.sha256").write_text("", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_EMPTY_CHECKSUMS"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_unindexed_parquet_or_extra_files(self):
        (self.official_dir / "unindexed.parquet").write_bytes(b"dummy")
        with self.assertRaisesRegex(ValueError, "EXP_OFFICIAL_CHECKSUM_SET_MISMATCH"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_missing_required_id_columns(self):
        broken_schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        row1 = {
            "dataset_id": "ds_official_1234567890abcdef",
            "mainshock_event_id": MID,
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        row2 = dict(row1, is_mainshock=False, relative_time_role="POST")
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=broken_schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_MISSING_REQUIRED_COL:candidate_event_id"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_lineage_mismatch(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row1 = {
            "dataset_id": "wrong_ds_lineage",
            "mainshock_event_id": MID,
            "candidate_event_id": MID,
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        row2 = dict(row1, candidate_event_id=POST_EVT, is_mainshock=False, relative_time_role="POST", x_scaled=0.1)
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_DATASET_LINEAGE_MISMATCH"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_missing_self_event(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row1 = {
            "dataset_id": "ds_official_1234567890abcdef",
            "mainshock_event_id": MID,
            "candidate_event_id": POST_EVT,
            "relative_time_role": "POST",
            "is_mainshock": False,
            "x_scaled": 0.1,
            "y_scaled": 0.1,
            "z_scaled": 0.1,
            "time_scaled": 0.1,
            "candidate_magnitude": 3.0,
            "candidate_time_utc": centre + timedelta(hours=1),
        }
        row2 = dict(row1, candidate_event_id="post_event_2", time_scaled=0.2)
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_MISSING_SELF_EVENT"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_nonzero_self_event_vector(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row1 = {
            "dataset_id": "ds_official_1234567890abcdef",
            "mainshock_event_id": MID,
            "candidate_event_id": MID,
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": 0.5,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        row2 = dict(row1, candidate_event_id=POST_EVT, is_mainshock=False, relative_time_role="POST", x_scaled=0.1)
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_NONZERO_SELF_VECTOR"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_duplicate_grain(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row = {
            "dataset_id": "ds_official_1234567890abcdef",
            "mainshock_event_id": MID,
            "candidate_event_id": MID,
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        pq.write_table(pa.Table.from_pylist([row, row], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_DUPLICATE_GRAIN"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_writer_synchronizes_effective_run_id_into_parquet(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [
            {"candidate_event_id": MID, "delta_time_hours": 0.0},
            {"candidate_event_id": POST_EVT, "delta_time_hours": 2.0},
        ]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="stale_run_id",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="stale_run_id",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_count=2,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        res = harness.write_experiment_result(
            output_dir=self.out_dir,
            bundle=bundle,
            algorithm_name="WINDOW",
            algorithm_version="1.0.0",
            model_config_version="v1",
            model_config={"param": "sync_test"},
            memberships=mem_rows,
            summaries=[sum_row],
            metrics={},
            mode=harness.MODE_OFFICIAL,
            random_seed=42,
        )
        effective_id = res["run_id"]
        self.assertNotEqual(effective_id, "stale_run_id")

        read_mem = pq.read_table(self.out_dir / "memberships.parquet")
        read_sum = pq.read_table(self.out_dir / "sequence_summary.parquet")
        for rid in read_mem["experiment_run_id"].to_pylist():
            self.assertEqual(rid, effective_id)
        for rid in read_sum["experiment_run_id"].to_pylist():
            self.assertEqual(rid, effective_id)

    def test_official_writer_different_seed_different_run_id(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [{"candidate_event_id": MID, "delta_time_hours": 0.0}]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_count=1,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        dir1 = self.out_dir / "seed1"
        dir2 = self.out_dir / "seed2"
        res1 = harness.write_experiment_result(
            output_dir=dir1, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
            model_config_version="v1", model_config={}, memberships=mem_rows, summaries=[sum_row],
            metrics={}, mode=harness.MODE_OFFICIAL, random_seed=1,
        )
        res2 = harness.write_experiment_result(
            output_dir=dir2, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
            model_config_version="v1", model_config={}, memberships=mem_rows, summaries=[sum_row],
            metrics={}, mode=harness.MODE_OFFICIAL, random_seed=999,
        )
        self.assertNotEqual(res1["run_id"], res2["run_id"])

    def test_official_writer_rejects_empty_summary_or_mismatched_windows(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [{"candidate_event_id": MID, "delta_time_hours": 0.0}]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        with self.assertRaisesRegex(ValueError, "EXP_MISSING_SUMMARY_ROWS"):
            harness.write_experiment_result(
                output_dir=self.out_dir, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
                model_config_version="v1", model_config={}, memberships=mem_rows, summaries=[],
                metrics={}, mode=harness.MODE_OFFICIAL,
            )

        mismatched_sum = harness.build_sequence_summary_row(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id="different_mainshock_id",
            candidate_count=1,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        with self.assertRaisesRegex(ValueError, "EXP_SUMMARY_WINDOW_MISMATCH"):
            harness.write_experiment_result(
                output_dir=self.out_dir, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
                model_config_version="v1", model_config={}, memberships=mem_rows, summaries=[mismatched_sum],
                metrics={}, mode=harness.MODE_OFFICIAL,
            )

    def test_official_writer_omits_checksums_sha256_file(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [{"candidate_event_id": MID, "delta_time_hours": 0.0}]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_count=1,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        res = harness.write_experiment_result(
            output_dir=self.out_dir, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
            model_config_version="v1", model_config={}, memberships=mem_rows, summaries=[sum_row],
            metrics={}, mode=harness.MODE_OFFICIAL,
        )
        self.assertTrue(res["has_success_marker"])
        self.assertFalse((self.out_dir / "checksums.sha256").exists())

    def test_official_writer_includes_runtime_environment_json(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [{"candidate_event_id": MID, "delta_time_hours": 0.0}]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_count=1,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        harness.write_experiment_result(
            output_dir=self.out_dir, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
            model_config_version="v1", model_config={}, memberships=mem_rows, summaries=[sum_row],
            metrics={}, mode=harness.MODE_OFFICIAL,
        )
        config_data = json.loads((self.out_dir / "experiment_config.json").read_text(encoding="utf-8"))
        self.assertIn("runtime_environment_json", config_data)
        self.assertTrue(isinstance(config_data["runtime_environment_json"], str))
        self.assertIn("python_version", config_data["runtime_environment_json"])

    def test_official_loader_rejects_null_required_id_column(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row1 = {
            "dataset_id": "ds_official_1234567890abcdef",
            "mainshock_event_id": MID,
            "candidate_event_id": None,
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        row2 = dict(row1, candidate_event_id=POST_EVT, is_mainshock=False, relative_time_role="POST", x_scaled=0.1)
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_NULL_REQUIRED_ID_COL:candidate_event_id"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_empty_required_id_column(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row1 = {
            "dataset_id": "ds_official_1234567890abcdef",
            "mainshock_event_id": MID,
            "candidate_event_id": "   ",
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        row2 = dict(row1, candidate_event_id=POST_EVT, is_mainshock=False, relative_time_role="POST", x_scaled=0.1)
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_EMPTY_REQUIRED_ID_COL:candidate_event_id"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_invalid_id_column_type(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.int64()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row1 = {
            "dataset_id": 12345,
            "mainshock_event_id": MID,
            "candidate_event_id": MID,
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": 0.0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        row2 = dict(row1, candidate_event_id=POST_EVT, is_mainshock=False, relative_time_role="POST", x_scaled=0.1)
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_INVALID_ID_COL_TYPE:dataset_id"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_non_floating_vector_column_type(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.int64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row1 = {
            "dataset_id": "ds_official_1234567890abcdef",
            "mainshock_event_id": MID,
            "candidate_event_id": MID,
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": 0,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        row2 = dict(row1, candidate_event_id=POST_EVT, is_mainshock=False, relative_time_role="POST", x_scaled=1)
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_INVALID_VECTOR_COL_TYPE:x_scaled"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_loader_rejects_null_vector_column(self):
        centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
        schema = pa.schema([
            ("dataset_id", pa.string()),
            ("mainshock_event_id", pa.string()),
            ("candidate_event_id", pa.string()),
            ("relative_time_role", pa.string()),
            ("is_mainshock", pa.bool_()),
            ("x_scaled", pa.float64()),
            ("y_scaled", pa.float64()),
            ("z_scaled", pa.float64()),
            ("time_scaled", pa.float64()),
            ("candidate_magnitude", pa.float64()),
            ("candidate_time_utc", pa.timestamp("us", tz="UTC")),
        ])
        row1 = {
            "dataset_id": "ds_official_1234567890abcdef",
            "mainshock_event_id": MID,
            "candidate_event_id": MID,
            "relative_time_role": "MAINSHOCK",
            "is_mainshock": True,
            "x_scaled": None,
            "y_scaled": 0.0,
            "z_scaled": 0.0,
            "time_scaled": 0.0,
            "candidate_magnitude": 6.0,
            "candidate_time_utc": centre,
        }
        row2 = dict(row1, candidate_event_id=POST_EVT, is_mainshock=False, relative_time_role="POST", x_scaled=0.1)
        pq.write_table(pa.Table.from_pylist([row1, row2], schema=schema), self.official_dir / "features-part-00000.parquet")
        names = sorted(p.name for p in self.official_dir.iterdir() if p.name != "checksums.sha256")
        (self.official_dir / "checksums.sha256").write_text("".join(f"{harness.sha256_bytes((self.official_dir / n).read_bytes())}  {n}\n" for n in names), encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "EXP_NULL_VECTOR_COL:x_scaled"):
            harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)

    def test_official_writer_rejects_lineage_mismatch_memberships_list(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [{"candidate_event_id": MID, "delta_time_hours": 0.0}]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="temp",
            dataset_id="foreign_dataset_id",
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_count=1,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        with self.assertRaisesRegex(ValueError, "EXP_DATASET_LINEAGE_MISMATCH"):
            harness.write_experiment_result(
                output_dir=self.out_dir, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
                model_config_version="v1", model_config={}, memberships=mem_rows, summaries=[sum_row],
                metrics={}, mode=harness.MODE_OFFICIAL,
            )

    def test_official_writer_rejects_lineage_mismatch_memberships_table(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [{"candidate_event_id": MID, "delta_time_hours": 0.0}]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="temp",
            dataset_id="foreign_dataset_id",
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        mem_table = pa.Table.from_pylist(mem_rows, schema=harness.MEMBERSHIP_SCHEMA)
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_count=1,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        with self.assertRaisesRegex(ValueError, "EXP_DATASET_LINEAGE_MISMATCH"):
            harness.write_experiment_result(
                output_dir=self.out_dir, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
                model_config_version="v1", model_config={}, memberships=mem_table, summaries=[sum_row],
                metrics={}, mode=harness.MODE_OFFICIAL,
            )

    def test_official_writer_rejects_lineage_mismatch_summaries_list(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [{"candidate_event_id": MID, "delta_time_hours": 0.0}]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="temp",
            dataset_id="foreign_dataset_id",
            mainshock_event_id=MID,
            candidate_count=1,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        with self.assertRaisesRegex(ValueError, "EXP_DATASET_LINEAGE_MISMATCH"):
            harness.write_experiment_result(
                output_dir=self.out_dir, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
                model_config_version="v1", model_config={}, memberships=mem_rows, summaries=[sum_row],
                metrics={}, mode=harness.MODE_OFFICIAL,
            )

    def test_official_writer_rejects_lineage_mismatch_summaries_table(self):
        bundle = harness.load_feature_bundle(self.official_dir, mode=harness.MODE_OFFICIAL)
        candidates = [{"candidate_event_id": MID, "delta_time_hours": 0.0}]
        mem_rows = harness.build_membership_rows(
            experiment_run_id="temp",
            dataset_id=bundle["source_dataset_id"],
            mainshock_event_id=MID,
            candidate_records=candidates,
            algorithm_name="WINDOW",
            model_config_version="v1",
        )
        sum_row = harness.build_sequence_summary_row(
            experiment_run_id="temp",
            dataset_id="foreign_dataset_id",
            mainshock_event_id=MID,
            candidate_count=1,
            algorithm_name="WINDOW",
            model_config_version="v1",
            membership_rows=mem_rows,
        )
        sum_table = pa.Table.from_pylist([sum_row], schema=harness.SEQUENCE_SUMMARY_SCHEMA)
        with self.assertRaisesRegex(ValueError, "EXP_DATASET_LINEAGE_MISMATCH"):
            harness.write_experiment_result(
                output_dir=self.out_dir, bundle=bundle, algorithm_name="WINDOW", algorithm_version="1.0.0",
                model_config_version="v1", model_config={}, memberships=mem_rows, summaries=sum_table,
                metrics={}, mode=harness.MODE_OFFICIAL,
            )


if __name__ == "__main__":
    unittest.main()
