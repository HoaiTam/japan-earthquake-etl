"""Byte inventory fixtures only: no network, ETL or Parquet semantic claims."""
import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import warnings
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("payload_inventory", ROOT / "scripts/inventory-dat03-payloads.py")
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def sha(data):
    return hashlib.sha256(data).hexdigest()


class PayloadInventoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.data = b"PAR1fixture-byte-identity-onlyPAR1"
        self.obj = {"uri": m.PREFIX + "silver/bundles/r/source_link/part-0.parquet",
                    "sha256": sha(self.data), "kind": "silver_payload", "byte_size": len(self.data)}

    def request(self, objects=None, raw=None):
        p = self.root / "request.json"
        p.write_bytes(raw if raw is not None else json.dumps({"owner": "fixture",
                      "objects": [self.obj] if objects is None else objects}).encode())
        return p, sha(p.read_bytes())

    def archive(self, items, name="input.zip"):
        p = self.root / name
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            with zipfile.ZipFile(p, "w", zipfile.ZIP_DEFLATED) as z:
                for key, data in items:
                    z.writestr(key, data)
        return p, sha(p.read_bytes())

    def run_inventory(self, items=(), objects=None):
        p, pin = self.request(objects)
        return m.inventory(p, pin, [self.archive(items)])

    def test_renamed_payload_reuses_content_not_physical_key_or_gold(self):
        r = self.run_inventory([("months/2023-01/source_link/a.parquet", self.data)])
        self.assertEqual(r["matched_references"], 1)
        self.assertTrue(r["payload_bytes_available"])
        self.assertEqual(r["objects"][0]["sources"][0]["member"], "months/2023-01/source_link/a.parquet")
        for flag in ("s3_keys_verified", "parquet_semantics_verified", "full_transitive_closure_verified",
                     "portable_import_ready", "official_export_ready"):
            self.assertIs(r[flag], False)

    def test_missing_or_changed_payload_is_not_declared_lost(self):
        for items in ([], [("months/a.parquet", b"x" * len(self.data))]):
            r = self.run_inventory(items)
            self.assertEqual(r["missing_references"], 1)
            self.assertEqual(r["objects"][0]["status"], "NOT_BYTE_IDENTICAL_IN_SCANNED_ARCHIVES")
            self.assertEqual(r["delta_objects"], [self.obj])
            self.assertIn("not data lost", r["note"])

    def test_repeated_empty_payload_content_deduplicates_transfer_budget(self):
        other = {**self.obj, "uri": self.obj["uri"].replace("/r/", "/r2/")}
        r = self.run_inventory([], [self.obj, other])
        self.assertEqual(r["expected_references"], 2)
        self.assertEqual(r["expected_unique_contents"], 1)
        self.assertEqual(r["missing_reference_bytes"], len(self.data) * 2)
        self.assertEqual(r["missing_unique_bytes"], len(self.data))
        r = self.run_inventory([("empty.parquet", self.data)], [self.obj, other])
        self.assertEqual(r["matched_references"], 2)

    def test_exact_path_tamper_fails_even_when_same_bytes_exist_elsewhere(self):
        name = self.obj["uri"][len(m.PREFIX):]
        for bad in (b"bad", b"x" * len(self.data)):
            with self.assertRaisesRegex(ValueError, "EXACT_PATH_(SIZE|SHA)_MISMATCH"):
                self.run_inventory([(name, bad), ("copy.parquet", self.data)])

    def test_metadata_wrapper_keeps_exact_path_guard(self):
        name = "metadata/" + self.obj["uri"][len(m.PREFIX):]
        with self.assertRaisesRegex(ValueError, "EXACT_PATH_SHA_MISMATCH"):
            self.run_inventory([(name, b"x" * len(self.data))])

    def test_external_request_and_archive_pins_required(self):
        p, pin = self.request()
        z, zpin = self.archive([])
        with self.assertRaisesRegex(ValueError, "REQUEST_PIN_MISMATCH"):
            m.inventory(p, "0" * 64, [(z, zpin)])
        with self.assertRaisesRegex(ValueError, "ARCHIVE_PIN_MISMATCH"):
            m.inventory(p, pin, [(z, "0" * 64)])

    def test_duplicate_or_conflicting_objects_rejected(self):
        with self.assertRaisesRegex(ValueError, "DUPLICATE_OBJECT_URI"):
            self.run_inventory(objects=[self.obj, self.obj])
        bad = {**self.obj, "uri": self.obj["uri"].replace("/r/", "/r2/"), "byte_size": len(self.data) + 1}
        with self.assertRaisesRegex(ValueError, "CONFLICTING_CONTENT_SIZE"):
            self.run_inventory(objects=[self.obj, bad])

    def test_json_duplicate_keys_foreign_uri_and_invalid_size_rejected(self):
        p, pin = self.request(raw=b'{"objects":[],"objects":[]}')
        with self.assertRaisesRegex(ValueError, "DUPLICATE_JSON_KEY"):
            m.load_request(p, pin)
        for bad, error in (({**self.obj, "uri": "s3://foreign/a.parquet"}, "FOREIGN_URI"),
                           ({**self.obj, "byte_size": True}, "INVALID_OBJECT_SIZE"),
                           ({**self.obj, "kind": "gold_commit"}, "UNSUPPORTED_PAYLOAD")):
            with self.assertRaisesRegex(ValueError, error):
                self.run_inventory(objects=[bad])

    def test_empty_request_not_success(self):
        with self.assertRaisesRegex(ValueError, "INVALID_REQUEST"):
            self.run_inventory(objects=[])

    def test_zip_duplicates_and_unsafe_paths_rejected(self):
        with self.assertRaisesRegex(ValueError, "DUPLICATE_ZIP_MEMBER"):
            self.run_inventory([("a.parquet", self.data), ("a.parquet", self.data)])
        for name in ("../secret", "/absolute", "a//b", "a\\b", "C:/foo"):
            with self.assertRaisesRegex(ValueError, "UNSAFE_PATH"):
                self.run_inventory([(name, b"bad")])

    def test_zip_symlink_and_special_entry_rejected(self):
        for mode in (0o120777, 0o010644):
            info = zipfile.ZipInfo("link.parquet")
            info.create_system = 3
            info.external_attr = mode << 16
            with self.assertRaisesRegex(ValueError, "(UNSAFE|SPECIAL)_ZIP_MEMBER"):
                self.run_inventory([(info, self.data)])

    def test_directory_entries_safe_but_not_candidates(self):
        r = self.run_inventory([("months/", b""), ("months/data.parquet", self.data)])
        self.assertEqual(r["matched_references"], 1)
        self.assertEqual(r["archives"][0]["parquet_members"], 1)

    def test_archive_symlink_rejected_before_read(self):
        p, pin = self.request()
        z, zpin = self.archive([])
        link = self.root / "symlink.zip"
        link.symlink_to(z)
        with self.assertRaisesRegex(ValueError, "SYMLINK_PATH"):
            m.inventory(p, pin, [(link, zpin)])

    def test_limits_and_duplicate_archives_rejected(self):
        p, pin = self.request()
        z, zpin = self.archive([("a.parquet", self.data)])
        for attr, value, code in (("MAX_ENTRIES", 0, "ENTRY_COUNT_LIMIT"),
                                  ("MAX_MEMBER_BYTES", len(self.data), "MEMBER_SIZE_LIMIT"),
                                  ("MAX_DECODED_BYTES", 0, "DECODED_BYTE_LIMIT"),
                                  ("MAX_ARCHIVE_BYTES", 0, "ARCHIVE_SIZE_LIMIT")):
            # Member cap must remain large enough to validate request first.
            items = [("large.txt", self.data + b"x")] if attr == "MAX_MEMBER_BYTES" else [("a.parquet", self.data)]
            z, zpin = self.archive(items)
            with patch.object(m, attr, value), self.assertRaisesRegex(ValueError, code):
                m.inventory(p, pin, [(z, zpin)])
        with self.assertRaisesRegex(ValueError, "DUPLICATE_ARCHIVE"):
            m.inventory(p, pin, [(z, zpin), (z, zpin)])

    def test_cli_immutable_output_and_partial_exit(self):
        p, pin = self.request()
        z, zpin = self.archive([])
        output = self.root / "report.json"
        args = ["inventory", "--request", str(p), "--request-sha256", pin,
                "--archive", str(z), "--archive-sha256", zpin, "--output", str(output)]
        with patch("sys.argv", args), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(m.main(), 2)
        original = output.read_bytes()
        with patch("sys.argv", args), self.assertRaisesRegex(ValueError, "OUTPUT_EXISTS"):
            m.main()
        self.assertEqual(output.read_bytes(), original)
        link = self.root / "out-link.json"
        link.symlink_to(output)
        args[-1] = str(link)
        with patch("sys.argv", args), self.assertRaisesRegex(ValueError, "SYMLINK_PATH"):
            m.main()
        self.assertEqual(output.read_bytes(), original)

    def test_full_content_still_no_official_acceptance(self):
        r = self.run_inventory([("file.parquet", self.data)])
        self.assertEqual(r["delta_objects"], [])
        self.assertEqual(r["missing_unique_bytes"], 0)
        self.assertFalse(r["official_export_ready"])

    def test_committed_delta_preserves_original_pins_and_unique_budget(self):
        full = json.loads((ROOT / "config/research/dat03_lineage_ThanhTris_payload.json").read_bytes())
        delta = json.loads((ROOT / "config/research/dat03_lineage_ThanhTris_payload_delta.json").read_bytes())
        original = {o["uri"]: o for o in full["objects"]}
        self.assertEqual(delta["basis_payload_request_sha256"], sha(
            (ROOT / "config/research/dat03_lineage_ThanhTris_payload.json").read_bytes()))
        self.assertEqual(delta["object_count"], len(delta["objects"]))
        self.assertEqual(delta["object_count"], 19)
        for obj in delta["objects"]:
            self.assertEqual(obj, original[obj["uri"]])
            self.assertIn("/reject_record/", obj["uri"])
        contents = {(o["sha256"], o["byte_size"]) for o in delta["objects"]}
        self.assertEqual(len(contents), delta["unique_content_count"])
        self.assertEqual(sum(size for _, size in contents), delta["unique_content_bytes"])
        self.assertEqual(delta["unique_content_bytes"], 595)

    def test_committed_rosy_payload_request_has_pinned_manifest_scope(self):
        path = ROOT / "config/research/dat03_lineage_rosy179_payload.json"
        request = json.loads(path.read_bytes())
        self.assertEqual(request["owner"], "rosy179")
        self.assertEqual(request["object_count"], 144)
        self.assertEqual(len(request["objects"]), 144)
        _, objects, by_key = m.load_request(path, sha(path.read_bytes()))
        self.assertEqual(len(by_key), 144)
        self.assertEqual(sum(o["byte_size"] for o in objects), 635408427)
        self.assertEqual(len({(o["sha256"], o["byte_size"]) for o in objects}), 104)
        summary = json.loads((ROOT / "docs/evidence/DAT-03_BYTE_AVAILABILITY_20261114.json").read_bytes())
        self.assertEqual(request["basis_audit_sha256"], summary["rosy_combined_audit_sha256"])
        self.assertEqual(request["basis_roots_sha256"], summary["rosy_root_sha256"])

    def test_followup_requests_include_current_heads_without_reasking_parents(self):
        heads = {
            "rosy179": ("orc07-22b5e7647a3df9ed83370139e11c48048845ee8f", 24, 23, 138),
            "ThanhTris": ("orc07-5679c9c378edacb4e5f804613c8475351ce326b4", 2, 1, 114),
        }
        for owner, (head, count, publications, snapshots) in heads.items():
            with self.subTest(owner=owner):
                req = json.loads((ROOT / f"config/research/dat03_lineage_{owner}_followup_20261114.json").read_bytes())
                self.assertEqual(req["owner"], owner)
                self.assertEqual(req["object_count"], count)
                self.assertEqual(len(req["objects"]), count)
                self.assertEqual(len({o["uri"] for o in req["objects"]}), count)
                self.assertEqual(len(req["unpinned_publication_uris"]), publications)
                self.assertEqual(len(set(req["unpinned_publication_uris"])), publications)
                self.assertEqual(len(req["snapshot_references"]), snapshots)
                self.assertEqual(req["head_run_id"], head)
                prefix = m.PREFIX + "silver/gold-commits/" + head + "/"
                keyed = {o["uri"]: o for o in req["objects"]}
                self.assertEqual(keyed[prefix + "commit.json"]["kind"], "gold_commit")
                self.assertEqual(keyed[prefix + "identity.json"]["kind"], "gold_identity")
                self.assertTrue(all(m.SHA.fullmatch(o["sha256"]) for o in req["objects"]))
                head_tables = {o["table"] for o in req["snapshot_references"]
                               if o["commit_uri"] == prefix + "commit.json"}
                self.assertEqual(head_tables, {"event_current", "event_source_bridge", "dim_date",
                                               "dim_region", "dim_magnitude_band", "dim_depth_band"})
                old = json.loads((ROOT / f"config/research/dat03_lineage_{owner}_metadata.json").read_bytes())
                if owner == "rosy179":
                    parents = {o["uri"].removesuffix("commit.json") + "identity.json"
                               for o in old["objects"] if o["kind"] == "gold_commit"}
                    self.assertEqual(len(parents), 22)
                    self.assertTrue(parents.issubset(keyed))
                else:
                    self.assertTrue(set(keyed).isdisjoint({o["uri"] for o in old["objects"]}))

    def test_byte_availability_evidence_binds_requests_and_keeps_acceptance_closed(self):
        summary = json.loads((ROOT / "docs/evidence/DAT-03_BYTE_AVAILABILITY_20261114.json").read_bytes())
        self.assertEqual(summary["task_status"], "In Progress")
        self.assertEqual(summary["assignee"], "HoaiTam")
        self.assertEqual(summary["reviewer"], "unassigned")
        self.assertEqual(summary["new_live_trino_queries"], 0)
        for flag in ("s3_keys_verified", "parquet_semantics_verified", "full_transitive_closure_verified",
                     "portable_import_ready", "official_export_ready", "research_ready",
                     "three_machine_receipts_verified", "etl_replayed", "docker_changed", "drive_written"):
            self.assertIs(summary[flag], False, flag)
        for worker in summary["workers"]:
            for field in ("payload_request", "followup_request"):
                pin = worker[field]
                self.assertEqual(sha((ROOT / pin["path"]).read_bytes()), pin["sha256"])
            self.assertEqual(worker["payload_expected_references"], worker["matched_references_after_reuse"])
            self.assertEqual(worker["missing_references_after_reuse"], 0)
            self.assertEqual(worker["shared_raw_source_matched_references"], 37)
        reuse = summary["byte_reuse"]
        self.assertEqual(reuse["references_resolved"], 42)
        for flag in ("bytes_regenerated", "original_s3_keys_restored", "shared_to_other_machines"):
            self.assertIs(reuse[flag], False)

    def test_reused_content_digest_resolves_both_worker_deltas_not_source_keys(self):
        summary = json.loads((ROOT / "docs/evidence/DAT-03_BYTE_AVAILABILITY_20261114.json").read_bytes())
        reuse = summary["byte_reuse"]
        requests = (("config/research/dat03_lineage_ThanhTris_payload_delta.json", 19),
                    ("config/research/dat03_lineage_rosy179_payload.json", 23))
        for path, expected in requests:
            data = json.loads((ROOT / path).read_bytes())
            matches = [o for o in data["objects"] if o["sha256"] == reuse["sha256"]
                       and o["byte_size"] == reuse["byte_size"]]
            self.assertEqual(len(matches), expected)
            self.assertTrue(all("/reject_record/" in o["uri"] for o in matches))

    def test_snapshot_reference_pins_keep_exact_int64_values(self):
        expected = {
            "rosy179": "d84d17f903c5796f758f9c42929c67a3cc1579da41a9b374bf65f7719e4595d3",
            "ThanhTris": "010792a3a2fb227dcb219ea5971435e067efecc658599873563ccf6752e65b8a",
        }
        for owner, digest in expected.items():
            req = json.loads((ROOT / f"config/research/dat03_lineage_{owner}_followup_20261114.json").read_bytes())
            refs = req["snapshot_references"]
            encoded = json.dumps(refs, sort_keys=True, separators=(",", ":")).encode()
            self.assertEqual(req["snapshot_reference_encoding"], "json-sort-keys-compact-utf8-v1")
            self.assertEqual(sha(encoded), digest)
            self.assertEqual(req["snapshot_reference_sha256"], digest)
            self.assertTrue(any(o["snapshot_id"] > 2**53 for o in refs))
            self.assertTrue(all(type(o["snapshot_id"]) is int
                                and 0 < o["snapshot_id"] <= 2**63 - 1 for o in refs))


if __name__ == "__main__":
    unittest.main()
