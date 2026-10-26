"""Tiny Parquet fixtures for partial catalog transport, never official research data."""
from datetime import datetime, timezone
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("prefix_kit", ROOT / "scripts/verify-catalog-prefix.py")
v = importlib.util.module_from_spec(spec); spec.loader.exec_module(v)


class CatalogPrefixKitTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve(); self.path = self.root / "catalog.zip"
        self.obs = ["a", "b"]; self.member = [("a", "ca"), ("b", "cb")]
        self.time = datetime(2000, 1, 2, tzinfo=timezone.utc)

    def build(self, mutate=None, extra=None):
        raw = {}
        tables = {
            "source_observation": pa.table({"source_observation_id": self.obs,
                "event_time_utc": pa.array([self.time] * len(self.obs), type=pa.timestamp('ms', tz='UTC')),
                "is_current_source_revision": [True] * len(self.obs), "source_system": ["JMA_BULLETIN"] * len(self.obs)}),
            "canonical_membership": pa.table({"source_observation_id": [x[0] for x in self.member],
                                               "canonical_event_id": [x[1] for x in self.member]}),
            "source_link": pa.table({"source_link_id": pa.array([], type=pa.string())}),
        }
        files = {}
        for kind, table in tables.items():
            name = "data/2000-01/" + kind + "-0.parquet"; output = io.BytesIO()
            pq.write_table(table, output); raw[name] = output.getvalue()
            files[name] = {"dataset": kind, "bytes": len(raw[name]), "sha256": v.sha(raw[name])}
        m = {"version": v.VERSION, "artifact_kind": "PARTIAL_PROCESSED_CATALOG", "allowed_usage": "EXPLORATORY_ONLY",
             "research_ready": False, "official_exported": False, "warehouse_backup": False, "contains_gold_tables": False,
             "completed_months": 1, "expected_rows": 2, "canonical_by_month": {"2000-01": 2}, "files": files,
             "batches": [{"batch_start": "2000-01", "affected_months": ["2000-01"],
                          "canonical_by_month": {"2000-01": 2}, "files": list(files)}]}
        if mutate: mutate(m)
        with zipfile.ZipFile(self.path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            for name, data in raw.items(): archive.writestr(name, data)
            archive.writestr("manifest.json", json.dumps(m))
            if extra: archive.writestr(extra, b"fixture")
        return v.sha(self.path.read_bytes())

    def test_offline_success(self):
        result = v.verify(self.path, self.build())
        self.assertEqual(2, result["unique_canonical_ids"])
        self.assertFalse(result["official_exported"])

    def test_fresh_process_without_repo_imports(self):
        pin = self.build()
        result = subprocess.run([sys.executable, str(ROOT / 'scripts/verify-catalog-prefix.py'), str(self.path),
                                 '--zip-sha256', pin], cwd=self.root, capture_output=True, text=True, timeout=20)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertEqual('PARTIAL_PROCESSED_CATALOG_VERIFIED', json.loads(result.stdout)['status'])

    def test_wrong_external_pin(self):
        self.build()
        with self.assertRaisesRegex(ValueError, 'ZIP_PIN_CHANGED'): v.verify(self.path, '0' * 64)

    def test_extra_file_rejected(self):
        pin = self.build(extra='unexpected.txt')
        with self.assertRaisesRegex(ValueError, 'EXACT_FILE_SET'): v.verify(self.path, pin)

    def test_traversal_rejected(self):
        pin = self.build(extra='../outside')
        with self.assertRaisesRegex(ValueError, 'UNSAFE_MEMBER'): v.verify(self.path, pin)

    def test_official_flag_rejected(self):
        pin = self.build(lambda m: m.update(official_exported=True))
        with self.assertRaisesRegex(ValueError, 'PARTIAL_SCOPE'): v.verify(self.path, pin)

    def test_member_hash_rejected(self):
        pin = self.build(lambda m: m['files'][next(iter(m['files']))].update(sha256='0' * 64))
        with self.assertRaisesRegex(ValueError, 'MEMBER_HASH'): v.verify(self.path, pin)

    def test_duplicate_observation_rejected(self):
        self.obs = ['a', 'a']; pin = self.build()
        with self.assertRaises(Exception): v.verify(self.path, pin)

    def test_duplicate_canonical_rejected(self):
        self.member = [('a', 'ca'), ('b', 'ca')]; pin = self.build()
        with self.assertRaises(Exception): v.verify(self.path, pin)

    def test_membership_join_mismatch_rejected(self):
        self.member = [('a', 'ca'), ('foreign', 'cb')]; pin = self.build()
        with self.assertRaisesRegex(ValueError, 'MEMBERSHIP_COVERAGE'): v.verify(self.path, pin)

    def test_month_boundary_rejected(self):
        self.time = datetime(1999, 12, 31, tzinfo=timezone.utc); pin = self.build()
        with self.assertRaisesRegex(ValueError, 'OUTSIDE_BATCH_MONTH'): v.verify(self.path, pin)

    def test_repeated_batch_rejected(self):
        pin = self.build(lambda m: m['batches'].append(m['batches'][0].copy()))
        with self.assertRaisesRegex(ValueError, 'EXACT_PARQUET_ASSIGNMENT'): v.verify(self.path, pin)


if __name__ == '__main__': unittest.main()
