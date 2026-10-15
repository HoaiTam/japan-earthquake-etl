"""Small generated synthetic fixtures: never depend on staging, Docker, APIs or real catalogs."""
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("pilot_bundle", ROOT / "notebooks/pilot_bundle.py")
pilot = importlib.util.module_from_spec(spec); spec.loader.exec_module(pilot)
MID, POST = "evt_" + "a" * 32, "evt_" + "b" * 32


def save(root, name, value):
    (root / name).write_text(pilot.canonical(value) + "\n")


def rehash(root):
    names = sorted(p.name for p in root.iterdir() if p.name != "checksums.sha256")
    (root / "checksums.sha256").write_text("".join(pilot.sha((root / n).read_bytes()) + "  " + n + "\n" for n in names))


def fixture(root):
    centre = datetime(2000, 7, 1, tzinfo=timezone.utc)
    selection = {"source_dataset_id": "ds_fixture", "native_report_sha256": "b" * 64, "mainshock_ids": [MID]}
    scaling = {"mode": "GLOBAL", "space_scale_km": 50., "depth_scale_km": 30., "time_scale_hours": 720.}
    payload = {"source_dataset_id": "ds_fixture", "native_report_sha256": "b" * 64,
               "scaling_config_sha256": pilot.sha(pilot.canonical(scaling).encode()),
               "selection_config_sha256": pilot.sha(pilot.canonical(selection).encode())}
    pid = "pilot_" + pilot.sha(pilot.canonical(payload).encode())
    def row(types):
        defaults = {"string": "fixture-v1", "double": 1., "long": 1, "timestamp": centre, "boolean": True}
        return {k: defaults[t] for k, t in types.items()}
    main = row(pilot.MAIN_TYPES)
    main.update(dataset_id="ds_fixture", pilot_id=pid, mainshock_event_id=MID, depth_km=80., magnitude=6.,
                latitude=35., longitude=140., mc_value=1., candidate_count=2, resource_guard_status="PASS", resource_reason_code=None)
    own = row(pilot.FEATURE_TYPES)
    own.update(dataset_id="ds_fixture", pilot_id=pid, mainshock_event_id=MID, candidate_event_id=MID,
               candidate_latitude=35., candidate_longitude=140., candidate_depth_km=80., candidate_magnitude=6.,
               mainshock_magnitude=6., mc_value=1., relative_time_role="MAINSHOCK", feature_version="relative-4d-v1",
               space_scale_km=50., depth_scale_km=30., time_scale_hours=720.)
    own.update({k: 0. for k in pilot.VECTOR + ["dx_km", "dy_km", "dz_km", "distance_3d_km", "delta_time_hours"]})
    post = dict(own, candidate_event_id=POST, is_mainshock=False, relative_time_role="POST", candidate_magnitude=3.,
                candidate_time_utc=centre + timedelta(hours=1), delta_time_hours=1., time_scaled=1. / 720.)
    arrow_types = {"string": pa.string(), "double": pa.float64(), "long": pa.int64(), "timestamp": pa.timestamp("us", tz="UTC"), "boolean": pa.bool_()}
    for name, rows, types in [("features-part-00000.parquet", [own, post], pilot.FEATURE_TYPES), ("mainshocks.parquet", [main], pilot.MAIN_TYPES)]:
        pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(k, arrow_types[t]) for k, t in types.items()])), root / name)
    cov = {"research_ready": False, "status": "PARTIAL", "windows": [{"mainshock_event_id": MID, "candidate_count": 2,
           "required_start_utc": "2000-06-30T23:00:00Z", "required_end_utc_exclusive": "2000-07-01T01:00:00.000001Z", "status": "PARTIAL",
           "available_ranges": [{"start_utc": "2000-06-30T23:00:00Z", "end_utc_exclusive": "2000-07-01T00:30:00Z"}],
           "missing_ranges": [{"start_utc": "2000-07-01T00:30:00Z", "end_utc_exclusive": "2000-07-01T01:00:00.000001Z"}]}]}
    save(root, "coverage_report.json", cov)
    table_names = ["event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band"]
    pins = {t: {"table": "iceberg.fixture." + t, "snapshot_id": 9223372036854775700 + i} for i, t in enumerate(table_names)}
    manifest = {"manifest_version": "dat-02-pilot-v1", "pilot_id": pid, "artifact_kind": "DEVELOPMENT_PILOT", "allowed_usage": "DEVELOPMENT_ONLY",
                "research_ready": False, "source_dataset_status": "BUILDING", "source_dataset_id": "ds_fixture", "identity_payload": payload,
                "scaling_config_json": pilot.canonical(scaling), "selection_config_json": pilot.canonical(selection), "vector_columns": pilot.VECTOR,
                "feature_version": "relative-4d-v1", "scaling_config_version": "fixture-v1", "feature_count": 2, "mainshock_count": 1,
                "invalid_count": 0, "selected_mainshock_ids": [MID], "coverage_status": "PARTIAL",
                "feature_schema": [{"name": k, "type": t} for k, t in pilot.FEATURE_TYPES.items()],
                "mainshock_schema": [{"name": k, "type": t} for k, t in pilot.MAIN_TYPES.items()],
                "coverage_sha256": pilot.sha((root / "coverage_report.json").read_bytes()),
                "source_manifest": {"dataset_id": "ds_fixture", "dataset_status": "BUILDING", "gold_publication_id": "pub_fixture",
                                    "gold_snapshot_id": pins["event_current"]["snapshot_id"], "physical_gold_table_name": pins["event_current"]["table"]},
                "gold_publication": {"publication_id": "pub_fixture", "published": True, "verify_status": "PASSED", "snapshot_bundle": pins}}
    manifest["schema_sha256"] = pilot.sha(pilot.canonical(manifest["feature_schema"]).encode())
    save(root, "pilot_manifest.json", manifest); rehash(root)


class PilotBundleTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name); fixture(self.root)
    def tearDown(self): self.temp.cleanup()
    def mutate_features(self, change):
        path = self.root / "features-part-00000.parquet"
        table = pq.read_table(path); rows = table.to_pylist(); change(rows)
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path); rehash(self.root)
    def test_valid_offline_inspection_preserves_large_snapshot_integer(self):
        bundle = pilot.load_bundle(self.root)
        self.assertEqual(bundle["summary"]["status"], "PILOT_INSPECTION_PASSED")
        self.assertEqual(bundle["manifest"]["source_manifest"]["gold_snapshot_id"], 9223372036854775700)
    def test_checksum_tampering_and_extra_files_rejected(self):
        (self.root / "coverage_report.json").write_text("{}");
        with self.assertRaisesRegex(ValueError, "CHECKSUM_MISMATCH"): pilot.load_bundle(self.root)
    def test_unindexed_file_rejected(self):
        (self.root / "_SUCCESS.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "FILE_SET"): pilot.load_bundle(self.root)
    def test_schema_error_even_with_updated_checksums(self):
        path = self.root / "features-part-00000.parquet"; table = pq.read_table(path)
        table = table.set_column(table.column_names.index("x_scaled"), "x_scaled", pa.array(["0", "0"]))
        pq.write_table(table, path); rehash(self.root)
        with self.assertRaisesRegex(ValueError, "SCHEMA_TYPE"): pilot.load_bundle(self.root)
    def test_nan_even_with_valid_checksums(self):
        self.mutate_features(lambda rows: rows[1].update(x_scaled=float("nan")))
        with self.assertRaisesRegex(ValueError, "NONFINITE"): pilot.load_bundle(self.root)
    def test_duplicate_grain(self):
        self.mutate_features(lambda rows: rows.__setitem__(1, dict(rows[0])))
        with self.assertRaisesRegex(ValueError, "DUPLICATE_GRAIN"): pilot.load_bundle(self.root)
    def test_missing_self(self):
        self.mutate_features(lambda rows: rows[0].update(candidate_event_id="evt_" + "c" * 32, is_mainshock=False, relative_time_role="POST"))
        with self.assertRaisesRegex(ValueError, "MISSING_SELF"): pilot.load_bundle(self.root)
    def test_nonzero_self(self):
        self.mutate_features(lambda rows: rows[0].update(x_scaled=1.))
        with self.assertRaisesRegex(ValueError, "NONZERO_SELF"): pilot.load_bundle(self.root)
    def test_partial_cannot_claim_complete_or_research_ready(self):
        path = self.root / "pilot_manifest.json"; manifest = json.loads(path.read_text()); manifest["research_ready"] = True
        save(self.root, path.name, manifest); rehash(self.root)
        with self.assertRaisesRegex(ValueError, "DEVELOPMENT_ONLY"): pilot.load_bundle(self.root)
    def test_coverage_gap_detected(self):
        path = self.root / "coverage_report.json"; coverage = json.loads(path.read_text()); coverage["windows"][0]["missing_ranges"] = []
        save(self.root, path.name, coverage)
        manifest = json.loads((self.root / "pilot_manifest.json").read_text()); manifest["coverage_sha256"] = pilot.sha(path.read_bytes())
        save(self.root, "pilot_manifest.json", manifest); rehash(self.root)
        with self.assertRaisesRegex(ValueError, "COVERAGE_STATUS"): pilot.load_bundle(self.root)
    def test_symlink_rejected(self):
        original = self.root / "checksums.sha256"; target = self.root.parent / (self.root.name + "-checksums")
        target.write_bytes(original.read_bytes()); original.unlink(); original.symlink_to(target)
        try:
            with self.assertRaisesRegex(ValueError, "UNSAFE"): pilot.load_bundle(self.root)
        finally: target.unlink()
    def test_archive_contains_only_verified_files_and_refuses_overwrite(self):
        import zipfile
        archive_spec = importlib.util.spec_from_file_location("pack_pilot", ROOT / "scripts/pack-pilot.py")
        module = importlib.util.module_from_spec(archive_spec); archive_spec.loader.exec_module(module)
        output = self.root.parent / (self.root.name + ".zip")
        try:
            first = module.pack(self.root, output); second = module.pack(self.root, output)
            self.assertEqual(first, second)
            with zipfile.ZipFile(output) as archive:
                self.assertEqual(len(archive.namelist()), 5)
                self.assertNotIn(".env", archive.namelist())
            output.write_bytes(b"different")
            with self.assertRaisesRegex(ValueError, "NO_OVERWRITE"): module.pack(self.root, output)
        finally:
            if output.exists(): output.unlink()


if __name__ == "__main__": unittest.main()
