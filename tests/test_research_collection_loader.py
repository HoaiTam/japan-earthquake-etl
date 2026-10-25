"""Synthetic offline collection fixtures; never evidence of real research/source coverage."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "notebooks"))
import research_collection as collection
import experiment_harness as harness

spec = importlib.util.spec_from_file_location("collection_fixture_support", ROOT / "tests/test_experiment_harness.py")
support = importlib.util.module_from_spec(spec)
spec.loader.exec_module(support)


def write_json(path, value):
    path.write_text(harness.canonical(value), encoding="utf-8")


def reindex(root):
    data = {p.relative_to(root).as_posix(): collection._hash(p) for p in root.rglob("*") if p.is_file()
            and p.name not in ("dataset_manifest.json", "checksums.sha256", "export-checksums.sha256")}
    (root / "export-checksums.sha256").write_text("".join(f"{v}  {k}\n" for k, v in sorted(data.items())))
    manifest = json.loads((root / "dataset_manifest.json").read_bytes())
    manifest["export_manifest_sha256"] = collection._hash(root / "export-checksums.sha256")
    write_json(root / "dataset_manifest.json", manifest)
    data.update({n: collection._hash(root / n) for n in ("dataset_manifest.json", "export-checksums.sha256")})
    (root / "checksums.sha256").write_text("".join(f"{v}  {k}\n" for k, v in sorted(data.items())))
    return {"manifest_sha256": collection._hash(root / "dataset_manifest.json"), "checksums_sha256": collection._hash(root / "checksums.sha256")}


def fixture(root, pilot_root):
    support.create_pilot_fixture(pilot_root)
    pilot_manifest = json.loads((pilot_root / "pilot_manifest.json").read_bytes())
    seed = pq.ParquetFile(pilot_root / "features-part-00000.parquet").read().drop(["pilot_id"])
    main_seed = pq.ParquetFile(pilot_root / "mainshocks.parquet").read().drop(["pilot_id"])
    dataset = "ds_" + "1"*64
    plan = {"dataset_id": dataset, "collection_id": "col_" + "2"*64, "plan_sha256": "2"*64,
            "full_mainshock_assignment_verified": True, "split_windows": False, "shards": [],
            "mainshock_count": 2, "feature_row_count": 4, "global_mc": {"scope": "FULL_PINNED_SPLIT", "mc_value": 1.0},
            "coverage": {"status": "COMPLETE", "required_month_count": 225}}
    for n in range(2):
        name, mid = f"shard_{n:05d}", f"main-{n}"
        folder = root / name; folder.mkdir()
        rows = copy.deepcopy(seed.to_pylist())
        for row in rows:
            if row["candidate_event_id"] == row["mainshock_event_id"]:
                row["candidate_event_id"] = mid
            row.update(dataset_id=dataset, mainshock_event_id=mid, schema_version="1.0")
        pq.write_table(pa.Table.from_pylist(rows, schema=seed.schema), folder / "features.parquet")
        main = main_seed.to_pylist()
        main[0].update(dataset_id=dataset, mainshock_event_id=mid, schema_version="1.0")
        pq.write_table(pa.Table.from_pylist(main, schema=main_seed.schema), folder / "mainshocks.parquet")
        plan["shards"].append({"shard_id": name, "mainshock_count": 1, "feature_row_count": 2,
                               "mainshocks": [{"mainshock_event_id": mid, "candidate_row_count": 2}]})
    write_json(root / "collection-preparation.json", {"status": "RESEARCH_COLLECTION_PREPARED", "plan": plan})
    manifest = {"export_version": collection.VERSION, "dataset_status": "EXPORTED", "schema_version": "1.0", "dataset_id": dataset,
                "collection_plan": plan, "mainshock_count": 2, "feature_row_count": 4, "dataset_split": "REPRODUCTION",
                "feature_version": "relative-4d-v1", "scaling_config_version": "fixture-v1", "window_model_version": "fixture-v1",
                "feature_schema": [entry for entry in pilot_manifest["feature_schema"] if entry["name"] != "pilot_id"]}
    write_json(root / "dataset_manifest.json", manifest)
    return reindex(root)


class CollectionLoaderTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.root = self.base / "bundle"; self.root.mkdir()
        pilot = self.base / "pilot"; pilot.mkdir()
        self.pins = fixture(self.root, pilot)

    def load(self):
        return collection.ResearchCollection(self.root, **self.pins)

    def test_all_shards_verified_but_only_selected_whole_window_is_loaded(self):
        loaded = self.load()
        self.assertEqual(4, loaded.verify_all()["feature_row_count"])
        shard = loaded.load_shard("shard_00001")
        self.assertEqual(2, shard["features_table"].num_rows)
        self.assertEqual(4, shard["manifest"]["feature_row_count"])
        self.assertEqual("WHOLE_WINDOW_SHARD", shard["collection_scope"]["scope"])
        with self.assertRaisesRegex(ValueError, "UNKNOWN_SHARD"):
            loaded.load_shard("shard_00002")

    def test_external_pins_and_exact_file_set_are_required(self):
        with self.assertRaisesRegex(ValueError, "EXTERNAL_PIN_MISMATCH"):
            collection.ResearchCollection(self.root, **{**self.pins, "manifest_sha256": "0"*64})
        (self.root / "extra").mkdir()
        with self.assertRaisesRegex(ValueError, "EXTRA_DIRECTORY"):
            self.load()

    def test_corrupt_file_is_not_regenerated_even_after_loader_construction(self):
        loaded = self.load()
        (self.root / "shard_00000/features.parquet").write_bytes(b"corrupt")
        with self.assertRaisesRegex(ValueError, "CHECKSUM_MISMATCH"):
            loaded.load_shard("shard_00000")

    def test_prepared_or_partial_cannot_be_treated_as_official(self):
        manifest = json.loads((self.root / "dataset_manifest.json").read_bytes())
        manifest["dataset_status"] = "BUILDING"
        write_json(self.root / "dataset_manifest.json", manifest); self.pins = reindex(self.root)
        with self.assertRaisesRegex(ValueError, "OFFICIAL_EXPORTED_REQUIRED"):
            self.load()

    def test_reassignment_and_nonfinite_feature_fail_with_otherwise_valid_checksums(self):
        file = self.root / "shard_00000/features.parquet"
        table = pq.ParquetFile(file).read(); rows = table.to_pylist(); rows[1]["mainshock_event_id"] = "foreign"
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), file); self.pins = reindex(self.root)
        with self.assertRaisesRegex(ValueError, "FEATURE_LINEAGE_CHANGED"):
            self.load().load_shard("shard_00000")
        rows[1]["mainshock_event_id"] = "main-0"; rows[1]["x_scaled"] = float("inf")
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), file); self.pins = reindex(self.root)
        with self.assertRaisesRegex(ValueError, "NONFINITE_FEATURE"):
            self.load().load_shard("shard_00000")

    def test_result_scope_is_bound_to_config_and_retry_cannot_switch_shards(self):
        loaded = self.load(); shard = loaded.load_shard("shard_00000"); records = shard["features_table"].to_pylist()
        mem = harness.build_membership_rows("temp", shard["source_dataset_id"], "main-0", records, "WINDOW", "v1")
        summary = harness.build_sequence_summary_row("temp", shard["source_dataset_id"], "main-0", 2, "WINDOW", "v1", mem)
        args = dict(output_dir=self.base / "result", bundle=shard, algorithm_name="WINDOW", algorithm_version="1.0.0",
                    model_config_version="v1", model_config={}, memberships=mem, summaries=[summary], metrics={}, mode=harness.MODE_OFFICIAL)
        first = harness.write_experiment_result(**args)
        config = json.loads((self.base / "result/experiment_config.json").read_bytes())
        self.assertEqual(shard["collection_scope"], config["collection_scope"])
        self.assertTrue(harness.write_experiment_result(**args, experiment_run_id=first["run_id"], is_retry=True)["is_reused"])
        args["bundle"] = loaded.load_shard("shard_00001")
        with self.assertRaisesRegex(ValueError, "COMPLETED_BUNDLE_CONFLICT"):
            harness.write_experiment_result(**args, experiment_run_id=first["run_id"], is_retry=True)

    def test_boolean_global_mc_is_not_a_numeric_completeness_value(self):
        manifest=json.loads((self.root/"dataset_manifest.json").read_bytes())
        plan=manifest["collection_plan"];plan["global_mc"]["mc_value"]=True
        write_json(self.root/"dataset_manifest.json",manifest)
        write_json(self.root/"collection-preparation.json",{"status":"RESEARCH_COLLECTION_PREPARED","plan":plan})
        self.pins=reindex(self.root)
        with self.assertRaisesRegex(ValueError,"GLOBAL_MC_REQUIRED"):self.load()

    def test_unknown_split_with_missing_month_count_is_not_complete(self):
        manifest=json.loads((self.root/"dataset_manifest.json").read_bytes());manifest["dataset_split"]="UNKNOWN"
        plan=manifest["collection_plan"];plan["coverage"].pop("required_month_count")
        write_json(self.root/"dataset_manifest.json",manifest)
        write_json(self.root/"collection-preparation.json",{"status":"RESEARCH_COLLECTION_PREPARED","plan":plan})
        self.pins=reindex(self.root)
        with self.assertRaisesRegex(ValueError,"COMPLETE_SPLIT_REQUIRED"):self.load()

    def test_float_mainshock_counter_is_rejected_even_if_python_equality_matches(self):
        path=self.root/"shard_00000/mainshocks.parquet";table=pq.ParquetFile(path).read()
        table=table.set_column(table.schema.get_field_index("candidate_count"),"candidate_count",pa.array([2.0],type=pa.float64()))
        pq.write_table(table,path);self.pins=reindex(self.root)
        with self.assertRaisesRegex(ValueError,"SCHEMA_TYPE_MISMATCH"):self.load().load_shard("shard_00000")


if __name__ == "__main__":
    unittest.main()
