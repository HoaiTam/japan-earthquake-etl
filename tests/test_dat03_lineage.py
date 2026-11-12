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

    def _make_identity(self, commit_version="gld-03-v1", namespace="iceberg.test", run_id="r1", scope=None, data=None):
        import hashlib
        if scope is None:
            scope = {"context": {"run_id": run_id, "window_start_utc": "2018-01-01T00:00:00Z"}}
        if data is None:
            data = {"event_current": {"count": 10, "sha256": "0" * 64, "schema": "{}"}}
        payload_dict = {"commit_version": commit_version, "namespace": namespace, "scope": scope, "data": data}
        payload_bytes = json.dumps(payload_dict, separators=(",", ":")).encode("utf-8")
        sha = hashlib.sha256(payload_bytes).hexdigest()
        full_bytes = payload_bytes[:-1] + f',\"identity_sha256\":\"{sha}\"}}'.encode("utf-8")
        return full_bytes, sha

    def test_gold_identity_recompute_payload_sha_positive(self):
        full_bytes, sha = self._make_identity()
        payload_sha, raw_sha = m.verify_gold_identity(full_bytes, sha)
        self.assertEqual(payload_sha, sha)
        self.assertEqual(raw_sha, m.digest(full_bytes))
        self.assertNotEqual(payload_sha, raw_sha)

    def test_gold_identity_tampered_namespace_fails(self):
        full_bytes, sha = self._make_identity(namespace="iceberg.authentic")
        tampered = full_bytes.replace(b"iceberg.authentic", b"iceberg.tampered_")
        with self.assertRaisesRegex(ValueError, "PAYLOAD_DECLARED_SHA_MISMATCH"):
            m.verify_gold_identity(tampered, sha)

    def test_gold_identity_tampered_scope_fails(self):
        full_bytes, sha = self._make_identity(run_id="run_orig")
        tampered = full_bytes.replace(b"run_orig", b"run_fake")
        with self.assertRaisesRegex(ValueError, "PAYLOAD_DECLARED_SHA_MISMATCH"):
            m.verify_gold_identity(tampered, sha)

    def test_gold_identity_tampered_data_fails(self):
        full_bytes, sha = self._make_identity()
        tampered = full_bytes.replace(b'"count":10', b'"count":99')
        with self.assertRaisesRegex(ValueError, "PAYLOAD_DECLARED_SHA_MISMATCH"):
            m.verify_gold_identity(tampered, sha)

    def test_gold_identity_duplicate_keys_fails(self):
        full_bytes, sha = self._make_identity()
        dup = full_bytes.replace(b'"commit_version":"gld-03-v1"', b'"commit_version":"gld-03-v1","commit_version":"gld-03-v1"')
        with self.assertRaisesRegex(ValueError, "DUPLICATE_JSON_KEY"):
            m.verify_gold_identity(dup, sha)

    def test_gold_identity_missing_sha_fails(self):
        full_bytes, sha = self._make_identity()
        no_sha = full_bytes.replace(f',\"identity_sha256\":\"{sha}\"'.encode("ascii"), b"")
        with self.assertRaisesRegex(ValueError, "MISSING_IDENTITY_SHA"):
            m.verify_gold_identity(no_sha, sha)

    def test_gold_identity_invalid_serialization_suffix_fails(self):
        full_bytes, sha = self._make_identity()
        # Trailing space breaks strict serialization contract
        broken = full_bytes + b" "
        with self.assertRaisesRegex(ValueError, "INVALID_IDENTITY_SERIALIZATION"):
            m.verify_gold_identity(broken, sha)

    def test_gold_identity_declared_sha_mismatch_fails(self):
        full_bytes, sha = self._make_identity()
        wrong_expected = "f" * 64
        with self.assertRaisesRegex(ValueError, "OBJECT_SHA_MISMATCH"):
            m.verify_gold_identity(full_bytes, wrong_expected)

    def test_gold_identity_in_audit_flow(self):
        full_bytes, sha = self._make_identity(run_id="run123")
        path = "silver/gold-commits/run123/identity.json"
        roots = {"objects": [{"uri": m.PREFIX + path, "sha256": sha, "kind": "gold_identity"}]}
        r = self.root / "roots_id.json"
        r.write_text(json.dumps(roots))
        z = self.archive("ident.zip", [(path, full_bytes)])
        res = m.audit(r, m.digest(r.read_bytes()), [z])
        self.assertEqual(res["roots_matched"], 1)

    def test_gold_identity_tampered_fails_audit(self):
        full_bytes, sha = self._make_identity(run_id="run123")
        tampered = full_bytes.replace(b"run123", b"run999")
        path = "silver/gold-commits/run123/identity.json"
        roots = {"objects": [{"uri": m.PREFIX + path, "sha256": sha, "kind": "gold_identity"}]}
        r = self.root / "roots_id_bad.json"
        r.write_text(json.dumps(roots))
        z = self.archive("ident_bad.zip", [(path, tampered)])
        with self.assertRaisesRegex(ValueError, "PAYLOAD_DECLARED_SHA_MISMATCH"):
            m.audit(r, m.digest(r.read_bytes()), [z])

    def test_generate_evidence_empty_probe_does_not_certify_success(self):
        gen_spec = importlib.util.spec_from_file_location("gen_evidence", ROOT / "scripts/generate-dat03-thanh-tris-evidence.py")
        gen_mod = importlib.util.module_from_spec(gen_spec)
        gen_spec.loader.exec_module(gen_mod)

        probe_data = {"identities": [], "publications": []}
        req_data = {
            "objects": [{"uri": m.PREFIX + "silver/gold-commits/r1/identity.json", "sha256": "a" * 64}],
            "unpinned_publication_uris": [m.PREFIX + "silver/gold-verification/r1/publication.json"],
        }
        evidence = gen_mod.build_evidence(probe_data, req_data, None, strict=False)
        self.assertIn("0/1 MATCHED", evidence)
        self.assertNotIn("1/1 MATCHED", evidence)
        self.assertIn("NOT_SUPPLIED", evidence)
        with self.assertRaisesRegex(ValueError, "STRICT_CHECK_FAILED"):
            gen_mod.build_evidence(probe_data, req_data, None, strict=True)

    def test_generate_evidence_probe_non_object_fails(self):
        gen_spec = importlib.util.spec_from_file_location("gen_evidence", ROOT / "scripts/generate-dat03-thanh-tris-evidence.py")
        gen_mod = importlib.util.module_from_spec(gen_spec)
        gen_spec.loader.exec_module(gen_mod)

        with self.assertRaisesRegex(ValueError, "PROBE_DATA_MUST_BE_OBJECT"):
            gen_mod.build_evidence([], {}, None)

    def test_generate_evidence_mismatch_status(self):
        gen_spec = importlib.util.spec_from_file_location("gen_evidence", ROOT / "scripts/generate-dat03-thanh-tris-evidence.py")
        gen_mod = importlib.util.module_from_spec(gen_spec)
        gen_spec.loader.exec_module(gen_mod)

        probe_data = {
            "identities": [{
                "uri": m.PREFIX + "silver/gold-commits/r1/identity.json",
                "internal_identity_sha256": "b" * 64,
                "payload_sha256": "b" * 64,
                "payload_matches_expected": False,
                "valid_serialization": True,
            }],
            "publications": [],
        }
        req_data = {
            "objects": [{"uri": m.PREFIX + "silver/gold-commits/r1/identity.json", "sha256": "a" * 64}],
            "unpinned_publication_uris": [],
        }
        evidence = gen_mod.build_evidence(probe_data, req_data, None, strict=False)
        self.assertIn("MISMATCH", evidence)
        self.assertIn("0/1 MATCHED", evidence)

    def test_pack_frontier_metadata_overwrite_guard(self):
        pack_spec = importlib.util.spec_from_file_location("pack_meta", ROOT / "scripts/pack-dat03-frontier-metadata.py")
        pack_mod = importlib.util.module_from_spec(pack_spec)
        pack_spec.loader.exec_module(pack_mod)

        existing_file = self.root / "existing.zip"
        existing_file.write_bytes(b"dummy")

        import sys
        old_argv = sys.argv
        try:
            sys.argv = ["pack-dat03-frontier-metadata.py", "--output", str(existing_file)]
            with self.assertRaises(FileExistsError):
                pack_mod.main()
        finally:
            sys.argv = old_argv


if __name__ == "__main__":
    unittest.main()
