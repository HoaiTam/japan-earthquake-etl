"""Metadata preparation guards; no network or runtime calls."""
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("relocation", Path(__file__).resolve().parents[1] / "scripts/prepare-dat03-bronze-relocation.py")
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


class BronzeRelocationPlanTest(unittest.TestCase):
    def test_relocation_retains_both_identities_without_marking_import_ready(self):
        config = {"version": "dat03-bronze-relocation-preparation-v1", "catalog_request_sha256": "a" * 64,
                  "observation_manifest_sha256": "b" * 64, "bronze_pins_sha256": "c" * 64,
                  "workers": [{"owner": "ThanhTris", "roots_sha256": "d" * 64, "followup_plan_sha256": "e" * 64}]}
        with patch.object(m, "physical_pins", return_value=[{"manifest_uri": "physical"}]), patch.object(m, "worker_sources", return_value=[{"manifest_uri": "original"}]):
            result = m.prepare(config)
        self.assertNotEqual(result["original_sources"][0]["manifest_uri"], result["physical_bronze_inputs"][0]["manifest_uri"])
        self.assertNotIn("portable_import_ready", result)

    def test_partial_worker_set_cannot_be_truncated(self):
        config = {"version": "dat03-bronze-relocation-preparation-v1", "workers": [{"owner": "rosy179"}]}
        with patch.object(m, "physical_pins", return_value=[{}, {}]), patch.object(m, "worker_sources", return_value=[{}]):
            with self.assertRaisesRegex(ValueError, "COMPLETE_WORKER_SOURCE_SET_REQUIRED"): m.prepare(config)

    def test_duplicate_workers_fail_before_loading_archives(self):
        config = {"version": "dat03-bronze-relocation-preparation-v1", "workers": [{"owner": "rosy179"}] * 2}
        with patch.object(m, "physical_pins", return_value=[{}]):
            with self.assertRaisesRegex(ValueError, "DISTINCT_APPROVED_WORKERS_REQUIRED"): m.prepare(config)

    def test_selected_cas_blob_must_match_the_trusted_descriptor(self):
        with tempfile.TemporaryDirectory(dir=Path(tempfile.gettempdir()).resolve()) as tmp:
            root = Path(tmp); request = b'{}'; (root / "request.json").write_bytes(request)
            raw = b'[{"manifest_uri":"s3://lake/bronze/a/manifest.json"}]'; digest = hashlib.sha256(raw).hexdigest()
            blob = root / "host-observations-v1/blobs" / digest[:2] / digest; blob.parent.mkdir(parents=True); blob.write_bytes(raw)
            manifest = {"catalog_request_sha256": hashlib.sha256(request).hexdigest(), "files": {"bronze-pins.json": {"sha256": digest, "bytes": len(raw)}}}
            path = root / "observation.json"; path.write_text(json.dumps(manifest))
            config = {"catalog": str(root), "catalog_request_sha256": manifest["catalog_request_sha256"],
                      "observation_manifest": str(path), "observation_manifest_sha256": m.f.file_sha(path), "bronze_pins_sha256": digest}
            self.assertEqual(len(m.physical_pins(config)), 1)
            blob.write_bytes(raw + b' ')
            with self.assertRaises(ValueError): m.physical_pins(config)


if __name__ == "__main__": unittest.main()
