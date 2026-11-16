"""Metadata-only mapping tests; fake bytes must still pass Java readback separately."""
import base64
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("portable_plan", ROOT / "scripts/plan-dat03-portable-silver.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class PortablePlanTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def save(self, name, value):
        p = self.root / name; p.write_text(json.dumps(value))
        return str(p), m.f.file_sha(p)

    def fixture(self, months=None):
        run = "fixture-run"; prefix = "silver/bundles/" + run + "/"
        bundle = {"run_id": run, "context": {"quality_passed": True, "reconciliation_balanced": True, "affected_months": months or ["2023-01"]}, "datasets": {}}
        metadata, inventory = {}, []
        for name in ("source_observation", "reject_record", "source_link", "canonical_membership"):
            uri = m.a.PREFIX + prefix + name + "/part.parquet"
            part = {"uri": uri, "sha256": "a" * 64, "byte_size": 4}
            mf = json.dumps({"files": [part]}).encode()
            metadata[prefix + name + "/manifest.json"] = mf
            bundle["datasets"][name] = {"manifest_uri": m.a.PREFIX + prefix + name + "/manifest.json", "sha256": m.a.digest(mf)}
            inventory.append({**part, "sources": [{"archive_sha256": "b" * 64, "member": name + ".parquet"}]})
        raw = json.dumps(bundle).encode(); metadata[prefix + "manifest.json"] = raw
        archive = self.root / "metadata.zip"
        with zipfile.ZipFile(archive, "w") as z:
            for name, data in metadata.items(): z.writestr(name, data)
        roots, roots_sha = self.save("roots.json", {"objects": [{"uri": m.a.PREFIX + prefix + "manifest.json", "sha256": m.a.digest(raw)}]})
        inv, inv_sha = self.save("inventory.json", {"version": "dat03-payload-byte-inventory-v1", "owner": "fixture", "payload_bytes_available": True, "missing_references": 0, "objects": inventory})
        fp, fp_sha = self.save("followup.json", {"metadata_archives": [{"path": str(archive), "sha256": m.f.file_sha(archive)}]})
        return {"version": "dat03-portable-preparation-v1", "followup_plan": fp, "followup_plan_sha256": fp_sha,
                "roots": roots, "roots_sha256": roots_sha, "inventory": inv, "inventory_sha256": inv_sha,
                "run_ids": [run], "payload_archives": [{"path": "not-read-by-metadata-planner.zip", "sha256": "b" * 64}]}

    def prepare(self, config):
        with patch.object(m.f, "verify", return_value={"owner": "fixture"}):
            return m.prepare(config)

    def test_preserves_original_manifest_bytes_and_returns_read_only_plan(self):
        plan = self.prepare(self.fixture())
        self.assertEqual(plan["mode"], "READ_ONLY_NO_OBJECT_STORE_OR_GOLD_WRITES")
        self.assertEqual(len(plan["groups"][0]["datasets"]), 4)
        raw = base64.b64decode(plan["groups"][0]["bundle_base64"])
        self.assertEqual(m.a.digest(raw), plan["groups"][0]["bundle_sha256"])

    def test_missing_pinned_bundle_fails(self):
        config = self.fixture(); config["run_ids"] = ["missing"]
        with self.assertRaisesRegex(ValueError, "PINNED_BUNDLE_ROOT_REQUIRED"): self.prepare(config)

    def test_more_than_twelve_months_fails(self):
        config = self.fixture([f"2023-{i:02d}" for i in range(1, 13)] + ["2024-01"])
        with self.assertRaisesRegex(ValueError, "MAX_TWELVE_AFFECTED_MONTHS"): self.prepare(config)

    def test_duplicate_run_fails(self):
        config = self.fixture(); config["run_ids"] *= 2
        with self.assertRaisesRegex(ValueError, "DISTINCT_RUNS_REQUIRED"): self.prepare(config)

    def test_inventory_pin_cannot_be_replaced_by_matching_status_flag(self):
        config = self.fixture(); config["inventory_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "INPUT_PIN_MISMATCH"): self.prepare(config)

    def test_missing_archive_mapping_fails(self):
        config = self.fixture(); config["payload_archives"] = []
        with self.assertRaisesRegex(ValueError, "PAYLOAD_ARCHIVE_MAPPING_REQUIRED"): self.prepare(config)


if __name__ == "__main__": unittest.main()
