"""Offline fixture regression for the DAT-03 rosy179 follow-up packer."""
from __future__ import annotations

from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import Mock, patch
import warnings
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("pack_rosy_followup", ROOT / "scripts/pack-dat03-rosy179-followup.py")
packer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(packer)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class FollowupTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "objects"
        self.output = self.root / "followup.zip"
        self.sidecar = self.root / "followup.zip.sha256"
        self.request = self.root / "request.json"
        self.pins, self.pubs = [], []
        self.start = "orc07-" + f"{0:040x}"
        self.head = "orc07-" + f"{22:040x}"
        self.head_commit = json.dumps({"commit_version": "gld-03-v1", "gold_run_id": self.head}).encode()
        self.head_commit_key = f"silver/gold-commits/{self.head}/commit.json"
        self.write_object(self.head_commit_key, self.head_commit)

        for n in range(23):
            run = "orc07-" + f"{n:040x}"
            key = f"silver/gold-commits/{run}/identity.json"
            payload = json.dumps({"commit_version": "gld-03-v1", "gold_run_id": run}, separators=(",", ":")).encode()
            pin = digest(payload)
            self.write_object(key, payload[:-1] + b',"identity_sha256":"' + pin.encode() + b'"}')
            self.pins.append({"uri": f"s3://japan-earthquake/{key}", "kind": "gold_identity", "sha256": pin})
            publication = f"silver/gold-verification/{run}/publication.json"
            self.write_object(publication, json.dumps({
                "gold_run_id": run, "verify_status": "PASSED", "published": True,
                "identity_sha256": pin, "bundle_sha256": digest(self.head_commit) if run == self.head else "a" * 64,
            }).encode())
            self.pubs.append(f"s3://japan-earthquake/{publication}")
        self.pins.append({"uri": f"s3://japan-earthquake/{self.head_commit_key}", "kind": "gold_commit",
                           "sha256": digest(self.head_commit)})
        self.request_doc = {"version": "dat03-followup-original-metadata-request-v1", "owner": "rosy179",
                            "object_count": 24, "head_run_id": self.head, "objects": self.pins,
                            "unpinned_publication_uris": self.pubs}
        self.write_request()
        self.unsafe_client = self.enterContext(
            patch.object(packer, "get_s3_client", side_effect=AssertionError("Unexpected S3 client creation"))
        )

    def write_object(self, key, data):
        dest = self.source / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)

    def write_request(self):
        self.request.write_text(json.dumps(self.request_doc), encoding="utf-8")

    def pack(self, **kwargs):
        return packer.pack_followup_archive(self.request, self.output, self.source, **kwargs)

    def verify(self):
        return packer.verify_followup_archive(self.output, self.request)

    def cli(self, *args):
        output = io.StringIO()
        with redirect_stdout(output):
            result = packer.main(["--request-file", str(self.request), "--output-zip", str(self.output), *args])
        self.assertEqual(result, 0)
        return json.loads(output.getvalue())

    def members(self):
        with zipfile.ZipFile(self.output) as archive:
            return [(info, archive.read(info)) for info in archive.infolist()]

    def replace_members(self, members):
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Duplicate name:.*", category=UserWarning)
            with zipfile.ZipFile(self.output, "w") as archive:
                for name, data in members:
                    archive.writestr(name, data)

    def alter_manifest(self, edit):
        members = self.members()
        info, data = members[-1]
        self.assertEqual(info.filename, "manifest.json")
        manifest = json.loads(data)
        edit(manifest)
        self.replace_members([*members[:-1], (info, json.dumps(manifest).encode())])

    def mutate_publication(self, **updates):
        key = self.pubs[0].removeprefix("s3://japan-earthquake/")
        value = json.loads((self.source / key).read_bytes())
        value.update(updates)
        self.write_object(key, json.dumps(value).encode())

    def symlink(self, link, target, *, directory=False):
        try:
            link.symlink_to(target, target_is_directory=directory)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"Symlink creation unavailable: {type(exc).__name__}")

    def test_offline_round_trip_and_scope(self):
        packed = self.pack()
        verified = self.verify()
        self.assertEqual((packed["status"], packed["object_count"]), ("SUCCESS", 47))
        self.assertEqual((verified["status"], verified["pinned_matched"], verified["pubs_matched"],
                          verified["total_entries"]), ("VERIFIED", 24, 23, 48))
        self.assertFalse(verified["publication_authenticity_verified"])
        self.assertEqual(verified["publication_verification"], "PACKAGE_SELF_CHECK_ONLY")
        self.assertEqual(packed["sha256"], verified["sha256"])
        self.assertEqual(self.cli("--verify")["sha256"], packed["sha256"])
        self.assertEqual(self.sidecar.read_text(), f'{packed["sha256"]}  {self.output.name}\n')
        self.unsafe_client.assert_not_called()

    def test_request_checked_before_fetch(self):
        for wrong_owner in (None, "other"):
            with self.subTest(owner=wrong_owner):
                self.request_doc["owner"] = wrong_owner
                self.write_request()
                with patch.object(packer, "fetch_bytes", side_effect=AssertionError("read too early")):
                    with self.assertRaisesRegex(ValueError, "INVALID_REQUEST_OWNER"):
                        self.pack()

    def test_duplicate_pin_rejected(self):
        self.pins[1] = self.pins[0]
        self.write_request()
        with self.assertRaisesRegex(ValueError, "DUPLICATE_REQUEST_OBJECT"):
            self.pack()

    def test_duplicate_publication_rejected(self):
        self.pubs[1] = self.pubs[0]
        self.write_request()
        with self.assertRaisesRegex(ValueError, "DUPLICATE_REQUEST_OBJECT"):
            self.pack()

    def test_foreign_bucket_rejected(self):
        self.pins[0]["uri"] = self.pins[0]["uri"].replace("japan-earthquake", "other")
        self.write_request()
        with self.assertRaisesRegex(ValueError, "FOREIGN_OR_INVALID_URI"):
            self.pack()

    def test_unsafe_request_key_rejected(self):
        self.pins[0]["uri"] = self.pins[0]["uri"].replace("/gold-commits/", "/../gold-commits/")
        self.write_request()
        with self.assertRaisesRegex(ValueError, "UNSAFE_OR_UNEXPECTED_OBJECT_PATH"):
            self.pack()

    def test_wrong_commit_pin_rejected_before_publish(self):
        self.pins[-1]["sha256"] = "f" * 64
        self.write_request()
        with self.assertRaisesRegex(ValueError, "COMMIT_SHA_MISMATCH"):
            self.pack()
        self.assertFalse(self.output.exists())
        self.assertFalse(self.sidecar.exists())

    def test_wrong_identity_pin_rejected(self):
        self.pins[0]["sha256"] = "f" * 64
        self.write_request()
        with self.assertRaisesRegex(ValueError, "OBJECT_SHA_MISMATCH"):
            self.pack()

    def test_duplicate_member_cannot_replace_required_member(self):
        self.pack()
        entries = self.members()
        self.replace_members([entries[0], entries[0], *entries[2:]])
        with patch.object(zipfile.ZipFile, "read", side_effect=AssertionError("read before preflight")):
            with self.assertRaisesRegex(ValueError, "DUPLICATE_OR_ALIASED_ZIP_MEMBER"):
                self.verify()

    def test_missing_or_extra_objects_rejected(self):
        self.pack()
        entries = self.members()
        foreign = (f"silver/gold-commits/orc07-{'e' * 40}/identity.json", b"{}")
        for members in (entries[1:], [*entries, foreign], [foreign, *entries[1:]]):
            with self.subTest(count=len(members)):
                self.replace_members(members)
                with self.assertRaisesRegex(ValueError, "ZIP_OBJECT_SET_MISMATCH"):
                    self.verify()

    def test_unsafe_zip_name_and_symlink_metadata_rejected(self):
        self.pack()
        members = self.members()
        for name in ("../manifest.json", "/manifest.json", "C:/manifest.json", r"silver\alias"):
            with self.subTest(name=name):
                self.replace_members([*members[:-1], (name, b"{}")])
                with self.assertRaisesRegex(ValueError, "UNSAFE_ZIP_MEMBER_PATH|DUPLICATE_OR_ALIASED_ZIP_MEMBER"):
                    self.verify()
        info = zipfile.ZipInfo(members[0][0].filename)
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        self.replace_members([(info, b"alias"), *members[1:]])
        with self.assertRaisesRegex(ValueError, "UNSAFE_ZIP_MEMBER_TYPE"):
            self.verify()

    def test_encrypted_zip_member_rejected_before_read(self):
        self.pack()
        members = [item[0] for item in self.members()]
        members[0].flag_bits |= 1
        with patch.object(zipfile.ZipFile, "infolist", return_value=members), \
             patch.object(zipfile.ZipFile, "read", side_effect=AssertionError("read before preflight")):
            with self.assertRaisesRegex(ValueError, "UNSAFE_ZIP_MEMBER_TYPE"):
                self.verify()

    def test_individual_and_total_limits_checked_before_decompression(self):
        self.pack()
        for bound in ("MAX_OBJECT", "MAX_EXPANDED"):
            with self.subTest(limit=bound), patch.object(packer, bound, 16), \
                 patch.object(zipfile.ZipFile, "read", side_effect=AssertionError("read before preflight")):
                with self.assertRaisesRegex(ValueError, "ZIP_EXPANDED_SIZE_LIMIT"):
                    self.verify()
        with patch.object(packer, "MAX_ARCHIVE", 16):
            with self.assertRaisesRegex(ValueError, "FILE_SIZE_LIMIT"):
                self.verify()

    def test_manifest_malformed_inventory_or_duplicate_keys_rejected(self):
        self.pack()
        entries = self.members()
        for manifest in (b"[]", b'{"owner":"rosy179","owner":"rosy179"}', b'{"objects":{}}', b"{"):
            with self.subTest(manifest=manifest):
                self.replace_members([*entries[:-1], (entries[-1][0], manifest)])
                with self.assertRaises(ValueError):
                    self.verify()

    def test_manifest_entries_bound_to_bytes_and_requested_metadata(self):
        self.pack()
        entries = self.members()
        for field, value in (("sha256", "f" * 64), ("size", 0), ("status", "NOT_CHECKED"),
                             ("uri", "s3://other/unchanged"), ("expected_sha256", "0" * 64)):
            with self.subTest(field=field):
                self.replace_members(entries)
                self.alter_manifest(lambda m: m["objects"][0].update({field: value}))
                with self.assertRaisesRegex(ValueError, "MANIFEST_OBJECT_MISMATCH"):
                    self.verify()

    def test_manifest_duplicate_entry_rejected(self):
        self.pack()
        self.alter_manifest(lambda m: m["objects"].__setitem__(1, m["objects"][0]))
        with self.assertRaisesRegex(ValueError, "MANIFEST_OBJECT_SET_MISMATCH"):
            self.verify()

    def test_legacy_inventory_still_verifies_but_must_bind_exact_bytes(self):
        self.pack()

        def compact(manifest):
            common = ("uri", "relative_path", "kind", "size", "sha256")
            manifest["objects"] = [dict(((k, item[k]) for k in common), expected_sha256=item.get("expected_sha256"))
                                   for item in manifest["objects"]]

        self.alter_manifest(compact)
        self.assertEqual(self.verify()["manifest_schema"], "LEGACY_MINIMAL")
        self.alter_manifest(lambda m: m["objects"][0].update(sha256="f" * 64))
        with self.assertRaisesRegex(ValueError, "MANIFEST_OBJECT_MISMATCH"):
            self.verify()

    def test_publication_false_or_wrong_lineage_rejected(self):
        for patch_data, error in (({"published": False}, "UNPUBLISHED_PUBLICATION"),
                                  ({"published": True, "gold_run_id": self.head}, "PUBLICATION_LINEAGE_MISMATCH")):
            with self.subTest(patch=patch_data):
                self.mutate_publication(**patch_data)
                with self.assertRaisesRegex(ValueError, error):
                    self.pack()

    def test_changed_publication_without_matching_manifest_rejected(self):
        self.pack()
        members = self.members()
        self.mutate_publication(event_count=10)
        path = self.pubs[0].removeprefix("s3://japan-earthquake/")
        self.replace_members([(info, (self.source / path).read_bytes() if info.filename == path else data)
                              for info, data in members])
        # Recompute only the total, not the original publication's recorded SHA/size.
        updated_members = self.members()
        self.alter_manifest(lambda m: m.update(total_bytes=sum(len(data) for _, data in updated_members[:-1])))
        with self.assertRaisesRegex(ValueError, "MANIFEST_OBJECT_MISMATCH"):
            self.verify()

    def test_cli_default_reuse_then_refuse_replacement(self):
        first = self.cli("--source-dir", str(self.source))
        before = self.output.read_bytes(), self.sidecar.read_bytes()
        self.assertEqual(self.cli("--source-dir", str(self.source))["status"], "REUSED")
        self.mutate_publication(event_count=10)
        with self.assertRaisesRegex(FileExistsError, "OUTPUT_ALREADY_EXISTS"):
            self.cli("--source-dir", str(self.source))
        self.assertEqual(before, (self.output.read_bytes(), self.sidecar.read_bytes()))
        self.assertEqual(self.verify()["sha256"], first["sha256"])

    def test_explicit_overwrite_replaces_valid_pair(self):
        first = self.pack()
        self.mutate_publication(event_count=10)
        new = self.cli("--source-dir", str(self.source), "--overwrite")
        self.assertEqual(new["status"], "SUCCESS")
        self.assertNotEqual(first["sha256"], new["sha256"])
        self.assertEqual(self.verify()["sha256"], new["sha256"])
        self.assertEqual(self.sidecar.read_text(), f'{new["sha256"]}  {self.output.name}\n')

    def test_failed_replacement_restores_previous_zip_and_sidecar(self):
        self.pack()
        before = self.output.read_bytes(), self.sidecar.read_bytes()
        self.mutate_publication(event_count=10)
        real_replace = os.replace

        def fail_sidecar(src, dst):
            if dst == self.sidecar:
                raise OSError("fixture sidecar write failure")
            return real_replace(src, dst)

        with patch.object(packer.os, "replace", side_effect=fail_sidecar):
            with self.assertRaisesRegex(OSError, "fixture sidecar write failure"):
                self.pack(overwrite=True)
        self.assertEqual(before, (self.output.read_bytes(), self.sidecar.read_bytes()))
        self.assertFalse(self.output.with_suffix(".zip.lock").exists())

    def test_conflicting_sidecar_creation_does_not_leave_partial_archive(self):
        real_link = os.link

        def fail_sidecar(src, dst):
            if dst == self.sidecar:
                raise FileExistsError("fixture concurrent writer")
            return real_link(src, dst)

        with patch.object(packer.os, "link", side_effect=fail_sidecar):
            with self.assertRaisesRegex(FileExistsError, "fixture concurrent writer"):
                self.pack()
        self.assertFalse(self.output.exists())
        self.assertFalse(self.sidecar.exists())

    def test_incomplete_or_bad_existing_pair_is_preserved(self):
        self.output.write_bytes(b"preexisting")
        with self.assertRaisesRegex(FileExistsError, "OUTPUT_PAIR_INCOMPLETE"):
            self.pack(overwrite=True)
        self.sidecar.write_bytes(b"wrong")
        with self.assertRaisesRegex(ValueError, "OUTPUT_CHECKSUM_MISMATCH"):
            self.pack(overwrite=True)
        self.assertEqual((self.output.read_bytes(), self.sidecar.read_bytes()), (b"preexisting", b"wrong"))

    def test_missing_local_data_never_falls_back_to_s3(self):
        (self.source / self.head_commit_key).unlink()
        with self.assertRaises(FileNotFoundError):
            self.pack()
        self.unsafe_client.assert_not_called()
        self.assertFalse(self.output.exists())

    def test_local_size_is_bounded(self):
        self.write_object(self.head_commit_key, b"x" * (packer.MAX_OBJECT + 1))
        with self.assertRaisesRegex(ValueError, "FILE_SIZE_LIMIT"):
            packer.fetch_bytes(self.head_commit_key, source_dir=self.source)

    def test_file_or_parent_symlink_rejected(self):
        item = self.source / self.head_commit_key
        item.unlink()
        target = self.root / "separate.json"
        target.write_bytes(self.head_commit)
        self.symlink(item, target)
        with self.assertRaisesRegex(ValueError, "SYMLINK_REJECTED"):
            packer.fetch_bytes(self.head_commit_key, source_dir=self.source)
        alias = self.root / "aliased-source"
        self.symlink(alias, self.source, directory=True)
        with self.assertRaisesRegex(ValueError, "SYMLINK_REJECTED"):
            packer.fetch_bytes(self.head_commit_key, source_dir=alias)

    def test_output_symlink_is_not_followed(self):
        target = self.root / "unrelated.zip"
        target.write_bytes(b"keep")
        self.symlink(self.output, target)
        with self.assertRaisesRegex(ValueError, "SYMLINK_REJECTED"):
            self.pack(overwrite=True)
        self.assertEqual(target.read_bytes(), b"keep")

    def test_explicit_s3_uses_mock_and_closes_bounded_streams(self):
        bodies = []

        def get_object(*, Bucket, Key):
            self.assertEqual(Bucket, "japan-earthquake")
            data = (self.source / Key).read_bytes()
            body = io.BytesIO(data)
            bodies.append(body)
            return {"Body": body, "ContentLength": len(data)}

        client = Mock()
        client.get_object.side_effect = get_object
        self.unsafe_client.side_effect = None
        self.unsafe_client.return_value = client
        self.assertEqual(self.cli("--s3")["object_count"], 47)
        self.assertEqual(client.get_object.call_count, 47)
        self.assertTrue(all(body.closed for body in bodies))

    def test_s3_checks_header_and_body_length(self):
        for length, data, error in ((packer.MAX_OBJECT + 1, b"123", "S3_OBJECT_SIZE_LIMIT"),
                                    (8, b"123", "S3_OBJECT_LENGTH_MISMATCH"),
                                    (1, b"123", "S3_OBJECT_LENGTH_MISMATCH")):
            with self.subTest(length=length):
                body = Mock(wraps=io.BytesIO(data))
                client = Mock()
                client.get_object.return_value = {"Body": body, "ContentLength": length}
                with self.assertRaisesRegex(ValueError, error):
                    packer.fetch_bytes(self.head_commit_key, s3_client=client)
                body.close.assert_called_once()
                if length > packer.MAX_OBJECT:
                    body.read.assert_not_called()
                else:
                    body.read.assert_called_once_with(length + 1)


class CredentialsTest(unittest.TestCase):
    def test_missing_or_cross_provider_partial_pair_rejected(self):
        for env in ({}, {"MINIO_ACCESS_KEY": "fake-a", "AWS_SECRET_ACCESS_KEY": "fake-b"},
                    {"MINIO_ACCESS_KEY": "fake-a", "AWS_ACCESS_KEY_ID": "fake-b", "AWS_SECRET_ACCESS_KEY": "fake-c"}):
            with self.subTest(keys=list(env)), patch.dict(os.environ, env, clear=True), patch.object(packer, "boto3") as sdk:
                with self.assertRaisesRegex(RuntimeError, "MISSING_OR_INCOMPLETE_S3_CREDENTIALS"):
                    packer.get_s3_client()
                sdk.client.assert_not_called()

    def test_minio_never_uses_aws_token(self):
        env = {"MINIO_ACCESS_KEY": "fake-a", "MINIO_SECRET_KEY": "fake-b", "AWS_SESSION_TOKEN": "fake-c"}
        with patch.dict(os.environ, env, clear=True), patch.object(packer, "boto3") as sdk:
            packer.get_s3_client("http://localhost:9000")
        kwargs = sdk.client.call_args.kwargs
        self.assertEqual(kwargs["aws_access_key_id"], "fake-a")
        self.assertEqual(kwargs["aws_secret_access_key"], "fake-b")
        self.assertNotIn("aws_session_token", kwargs)

    def test_complete_aws_pair_gets_its_token(self):
        env = {"AWS_ACCESS_KEY_ID": "fake-a", "AWS_SECRET_ACCESS_KEY": "fake-b", "AWS_SESSION_TOKEN": "fake-c"}
        with patch.dict(os.environ, env, clear=True), patch.object(packer, "boto3") as sdk:
            packer.get_s3_client()
        kwargs = sdk.client.call_args.kwargs
        self.assertEqual((kwargs["aws_access_key_id"], kwargs["aws_secret_access_key"], kwargs["aws_session_token"]),
                         ("fake-a", "fake-b", "fake-c"))


if __name__ == "__main__":
    unittest.main()
