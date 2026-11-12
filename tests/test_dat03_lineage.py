"""Offline tests: trusted frontier, no ETL/network/credentials."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("lineage", ROOT / "scripts/audit-dat03-lineage.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class LineageTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)

    def archive(self, name, values):
        p = self.root / name
        with zipfile.ZipFile(p, "w") as z:
            for k, v in values:
                z.writestr(k, v)
        return p

    def run_audit(self, value, extra=(), object_bytes=None):
        data = json.dumps(value).encode() if object_bytes is None else object_bytes
        path = "silver/full-month/run/registry.json"
        roots = {"objects": [{"uri": m.PREFIX + path, "sha256": m.digest(data), "kind": "registry_json"}]}
        r = self.root / "roots.json"
        r.write_text(json.dumps(roots))
        z = self.archive("objects.zip", [(path, data), *extra])
        return m.audit(r, m.digest(r.read_bytes()), [z])

    def test_missing_next_hop_is_not_gold_acceptance(self):
        result = self.run_audit({"datasets": {"obs": {"manifest_uri": m.PREFIX + "silver/obs/manifest.json", "sha256": "a" * 64}}, "bundle_version": "slv-09-bundle-v1"})
        self.assertEqual(result["roots_matched"], 1)
        self.assertEqual(len(result["next_required_objects"]), 1)
        self.assertFalse(result["portable_import_ready"])
        self.assertFalse(result["official_export_ready"])

    def test_relative_parent_resolves_under_silver_not_bucket_root(self):
        result = self.run_audit({"active_inputs": [], "parent": {"commit_key": "gold-commits/p/commit.json", "commit_sha256": "a" * 64, "registry_sha256": "b" * 64, "run_id": "p"}})
        uris = {r["uri"] for r in result["next_required_objects"]}
        self.assertIn(m.PREFIX + "silver/gold-commits/p/commit.json", uris)
        self.assertNotIn(m.PREFIX + "gold-commits/p/commit.json", uris)

    def test_wrong_root_pin_fails(self):
        p = self.root / "r.json"; p.write_text('{"objects":[]}')
        with self.assertRaisesRegex(ValueError, "ROOT_PIN"):
            m.audit(p, "0" * 64, [])

    def test_tampered_child_fails(self):
        with self.assertRaisesRegex(ValueError, "OBJECT_SHA"):
            self.run_audit({"raw_object_uri": m.PREFIX + "bronze/raw", "sha256": "a" * 64}, [("bronze/raw", b"wrong")])

    def test_conflicting_metadata_sha_fails(self):
        with self.assertRaisesRegex(ValueError, "CONFLICTING_REFERENCE_SHA"):
            self.run_audit({"bundle_version": "slv-09-bundle-v1", "datasets": {
                "a": {"manifest_uri": m.PREFIX + "silver/x", "sha256": "a" * 64},
                "b": {"manifest_uri": m.PREFIX + "silver/x", "sha256": "b" * 64}}})

    def test_duplicate_json_keys_rejected(self):
        with self.assertRaisesRegex(ValueError, "DUPLICATE_JSON"):
            self.run_audit({}, object_bytes=b'{"x":1,"x":2}')

    def test_unsafe_zip_name_rejected(self):
        p = self.archive("unsafe.zip", [("../secret", b"bad")])
        with self.assertRaisesRegex(ValueError, "UNSAFE_KEY"):
            m.load_archives([p])

    def test_duplicate_zip_name_rejected(self):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            p = self.archive("dup.zip", [("silver/a", b"a"), ("silver/a", b"a")])
        with self.assertRaisesRegex(ValueError, "DUPLICATE_ZIP"):
            m.load_archives([p])

    def test_container_indexes_differ_but_object_conflicts_fail(self):
        a = self.archive("a.zip", [("manifest.json", b"a"), ("silver/object", b"data")])
        b = self.archive("b.zip", [("manifest.json", b"b"), ("silver/object", b"data")])
        objects, _ = m.load_archives([a, b])
        self.assertEqual(objects, {"silver/object": b"data"})
        c = self.archive("c.zip", [("silver/object", b"changed")])
        with self.assertRaisesRegex(ValueError, "CONFLICTING_OBJECT_BYTES"):
            m.load_archives([a, c])

    def test_snapshot_reference_is_not_portable_snapshot(self):
        result = self.run_audit({"commit_version": "gld-03-v1", "gold_run_id": "run", "identity_sha256": "a" * 64, "tables": {"event_current": {"snapshot_id": 123}}})
        self.assertEqual(len(result["snapshot_references"]), 1)
        self.assertEqual(len(result["unpinned_publication_uris"]), 1)
        self.assertFalse(result["portable_import_ready"])

    def test_metadata_transport_wrapper_keeps_conflict_guard(self):
        a = self.archive("wrap.zip", [("metadata/silver/a", b"a")])
        objects, _ = m.load_archives([a])
        self.assertEqual(objects, {"silver/a": b"a"})
        b = self.archive("plain.zip", [("silver/a", b"changed")])
        with self.assertRaisesRegex(ValueError, "CONFLICTING_OBJECT_BYTES"):
            m.load_archives([a, b])

    def test_wrong_size_fails_even_with_matching_hash(self):
        with self.assertRaisesRegex(ValueError, "OBJECT_SIZE"):
            self.run_audit({"raw_object_uri": m.PREFIX + "bronze/raw", "sha256": m.digest(b"a"), "content_length_bytes": 2}, [("bronze/raw", b"a")])

    def test_foreign_bucket_fails(self):
        with self.assertRaisesRegex(ValueError, "FOREIGN_OR_INVALID_URI"):
            self.run_audit({"raw_object_uri": "s3://foreign/key", "sha256": "a" * 64})

    def test_oversized_entry_fails(self):
        p = self.archive("big.zip", [("silver/a", b"a" * (2 * 1024**2 + 1))])
        with self.assertRaisesRegex(ValueError, "DECODED_LIMIT"):
            m.load_archives([p])

    def test_symlink_entry_fails(self):
        p = self.root / "link.zip"
        info = zipfile.ZipInfo("silver/a")
        info.create_system = 3
        info.external_attr = (0o120777 << 16)
        with zipfile.ZipFile(p, "w") as z:
            z.writestr(info, b"target")
        with self.assertRaisesRegex(ValueError, "UNSAFE_ZIP_ENTRY"):
            m.load_archives([p])

    def test_duplicate_roots_fail(self):
        p = self.root / "roots.json"
        item = {"uri": m.PREFIX + "silver/a", "sha256": "a" * 64, "kind": "registry_json"}
        p.write_text(json.dumps({"objects": [item, item]}))
        z = self.archive("empty.zip", [])
        with self.assertRaisesRegex(ValueError, "DUPLICATE_ROOT"):
            m.audit(p, m.digest(p.read_bytes()), [z])

    def test_committed_worker_request_inventories_are_consistent(self):
        for owner, part, expected in [("ThanhTris", "metadata", 18),
                                      ("ThanhTris", "payload", 119),
                                      ("rosy179", "metadata", 114)]:
            p = ROOT / "config/research" / f"dat03_lineage_{owner}_{part}.json"
            value = m.decode(p.read_bytes())
            self.assertEqual(value["owner"], owner)
            self.assertEqual(value["object_count"], expected)
            self.assertEqual(len(value["objects"]), expected)
            names = set()
            for item in value["objects"]:
                name = m.key(item["uri"])
                self.assertNotIn(name, names)
                names.add(name)
                self.assertRegex(item["sha256"], m.SHA)
                if item["kind"] == "gold_commit":
                    self.assertTrue(name.startswith("silver/gold-commits/"))
            if part == "payload":
                self.assertEqual(sum(i["byte_size"] for i in value["objects"]), 522659149)


if __name__ == "__main__":
    unittest.main()
