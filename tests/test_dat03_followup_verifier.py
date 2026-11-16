"""Deterministic follow-up acceptance fixtures; no network, SDK, or Spark."""
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("followup", ROOT / "scripts/verify-dat03-followup.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def encoded(value):
    return json.dumps(value, separators=(",", ":")).encode()


class FollowupVerifierTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def fixture(self, change=None):
        run = "run-final"
        identity = {"commit_version": "gld-03-v1", "namespace": "iceberg.fixture", "scope": {}, "data": {}}
        identity_sha = m.lineage.digest(encoded(identity))
        identity["identity_sha256"] = identity_sha
        tables = {name: {"table": "iceberg.fixture." + name, "snapshot_id": 9007199254740993}
                  for name in ("event_current", "event_source_bridge", "dim_date", "dim_region", "dim_magnitude_band", "dim_depth_band")}
        commit = {"commit_version": "gld-03-v1", "gold_run_id": run, "identity_sha256": identity_sha,
                  "namespace": "iceberg.fixture", "scope": {}, "tables": tables}
        raw_commit = encoded(commit)
        pub = {"gold_run_id": run, "identity_sha256": identity_sha, "bundle_sha256": m.lineage.digest(raw_commit),
               "namespace": "iceberg.fixture", "verify_status": "PASSED", "published": True,
               "snapshot_bundle": copy.deepcopy(tables), "checks": {"count": {"actual": 0, "expected": 0}}}
        if change:
            change(commit, pub)
        raw_commit = encoded(commit)
        data_path = self.root / "data.zip"
        with zipfile.ZipFile(data_path, "w") as z:
            z.writestr("2018-09/gold.json", raw_commit)
            z.writestr("2018-09/publish.json", encoded(pub))
        prefix = "silver/gold-commits/" + run + "/"
        pub_key = "silver/gold-verification/" + run + "/publication.json"
        meta = self.root / "metadata.zip"
        with zipfile.ZipFile(meta, "w") as z:
            z.writestr(prefix + "commit.json", raw_commit)
            z.writestr(prefix + "identity.json", encoded(identity))
            z.writestr(pub_key, encoded(pub))
        request = {"version": "dat03-followup-original-metadata-request-v1", "owner": "fixture",
                   "head_run_id": run, "object_count": 2,
                   "head_phase_gold_member": "2018-09/gold.json", "head_phase_publish_member": "2018-09/publish.json",
                   "head_phase_gold_sha256": m.lineage.digest(raw_commit), "head_phase_publish_sha256": m.lineage.digest(encoded(pub)),
                   "basis_data_archive_sha256": m.file_sha(data_path),
                   "objects": [{"uri": m.lineage.PREFIX + prefix + "commit.json", "kind": "gold_commit", "sha256": m.lineage.digest(raw_commit)},
                               {"uri": m.lineage.PREFIX + prefix + "identity.json", "kind": "gold_identity", "sha256": identity_sha}],
                   "unpinned_publication_uris": [m.lineage.PREFIX + pub_key]}
        request_path = self.root / "request.json"
        request_path.write_bytes(encoded(request))
        return {"version": "dat03-followup-verification-plan-v1", "owner": "fixture",
                "request": str(request_path), "request_sha256": m.file_sha(request_path),
                "metadata_archives": [{"path": str(meta), "sha256": m.file_sha(meta)}],
                "data_archive": str(data_path), "data_archive_sha256": m.file_sha(data_path)}

    def test_valid_receipt_keeps_all_acceptance_flags_closed(self):
        r = m.verify(self.fixture())
        self.assertEqual((r["pinned_objects_matched"], r["publication_receipts_matched"], r["snapshot_references_matched"]), (2, 1, 6))
        for flag in ("full_transitive_closure_verified", "portable_import_ready", "official_export_ready", "research_ready", "ml_upstream_eligible"):
            self.assertFalse(r[flag])
        identity = next(x for x in r["objects"] if x["kind"] == "gold_identity")
        self.assertNotEqual(identity["raw_file_sha256"], identity["identity_payload_sha256"])

    def test_wrong_request_pin(self):
        p = self.fixture(); p["request_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "INPUT_PIN_MISMATCH"):
            m.verify(p)

    def test_wrong_metadata_archive_pin(self):
        p = self.fixture(); p["metadata_archives"][0]["sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "INPUT_PIN_MISMATCH"):
            m.verify(p)

    def test_wrong_data_archive_pin(self):
        p = self.fixture(); p["data_archive_sha256"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "INPUT_PIN_MISMATCH"):
            m.verify(p)

    def test_owner_mismatch(self):
        p = self.fixture(); p["owner"] = "someone-else"
        with self.assertRaisesRegex(ValueError, "OWNER_MISMATCH"):
            m.verify(p)

    def test_publication_phase_cannot_be_replaced_by_self_hash(self):
        p = self.fixture()
        path = Path(p["metadata_archives"][0]["path"])
        with zipfile.ZipFile(path) as z:
            entries = {n: z.read(n) for n in z.namelist()}
        name = next(n for n in entries if n.endswith("publication.json"))
        pub = json.loads(entries[name]); pub["event_count"] = 999
        entries[name] = encoded(pub)
        with zipfile.ZipFile(path, "w") as z:
            for n, raw in entries.items():
                z.writestr(n, raw)
        p["metadata_archives"][0]["sha256"] = m.file_sha(path)
        with self.assertRaisesRegex(ValueError, "PUBLICATION_PHASE_CONTENT_MISMATCH"):
            m.verify(p)

    def test_not_passed_is_not_accepted(self):
        p = self.fixture(lambda c, p: p.update(verify_status="FAILED"))
        with self.assertRaisesRegex(ValueError, "PUBLICATION_NOT_PASSED"):
            m.verify(p)

    def test_failed_recorded_check(self):
        p = self.fixture(lambda c, p: p["checks"]["count"].update(actual=1))
        with self.assertRaisesRegex(ValueError, "RECORDED_CHECK_FAILED"):
            m.verify(p)

    def test_snapshot_mismatch(self):
        p = self.fixture(lambda c, p: p["snapshot_bundle"]["event_current"].update(snapshot_id=1))
        with self.assertRaisesRegex(ValueError, "SIX_SNAPSHOT_BUNDLE_MISMATCH"):
            m.verify(p)

    def test_float_snapshot_is_rejected(self):
        def change(c, p):
            c["tables"]["event_current"]["snapshot_id"] = 9007199254740992.0
            p["snapshot_bundle"] = copy.deepcopy(c["tables"])
            p["bundle_sha256"] = m.lineage.digest(encoded(c))
        with self.assertRaisesRegex(ValueError, "INVALID_SNAPSHOT_ID"):
            m.verify(self.fixture(change))

    def test_empty_recorded_checks_is_not_a_pass(self):
        p = self.fixture(lambda c, p: p.update(checks={}))
        with self.assertRaisesRegex(ValueError, "RECORDED_CHECKS_REQUIRED"):
            m.verify(p)

    def test_no_archives_fails_closed(self):
        p = self.fixture(); p["metadata_archives"] = []
        with self.assertRaisesRegex(ValueError, "ARCHIVE_COUNT_LIMIT"):
            m.verify(p)

    def test_head_only_followup_may_reuse_archive_with_older_phases(self):
        p = self.fixture()
        data_path = Path(p["data_archive"])
        with zipfile.ZipFile(data_path, "a") as z:
            z.writestr("2018-08/publish.json", encoded({"gold_run_id": "previous-run"}))
        p["data_archive_sha256"] = m.file_sha(data_path)
        request_path = Path(p["request"])
        request = json.loads(request_path.read_bytes())
        request["basis_data_archive_sha256"] = p["data_archive_sha256"]
        request_path.write_bytes(encoded(request))
        p["request_sha256"] = m.file_sha(request_path)
        receipt = m.verify(p)
        self.assertEqual(receipt["publication_receipts_matched"], 1)
        self.assertEqual(receipt["other_phase_groups_not_verified_by_this_receipt"], 1)


if __name__ == "__main__":
    unittest.main()
