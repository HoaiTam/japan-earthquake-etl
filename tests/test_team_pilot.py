"""Synthetic kit regressions only; no source data, Docker, network or official lifecycle."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))
from test_pilot_bundle import fixture


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


packer = module("team_packer", ROOT / "scripts/pack-team-pilot.py")
verifier = module("team_verify", ROOT / "scripts/verify-team-pilot.py")


class TeamPilotTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.bundle = self.root / "source"
        self.bundle.mkdir()
        fixture(self.bundle)
        self.archive = self.root / "pilot.zip"
        self.result = packer.pack(self.bundle, self.archive)
        self.kit = self.root / "kit"
        with zipfile.ZipFile(self.archive) as archive:
            archive.extractall(self.kit)

    def verify(self, **kwargs):
        return verifier.verify(self.kit, kwargs.pop("pin", self.result["handoff_sha256"]),
                               kwargs.pop("recipient", "HoaiTam"), **kwargs)

    def test_valid_portable_kit_and_self_reported_receipt(self):
        output = self.root / "owner-receipt.json"
        result = self.verify(receipt=output)
        self.assertEqual("TEAM_PILOT_VERIFIED", result["status"])
        self.assertEqual(2, result["feature_count"])
        self.assertEqual("PARTIAL", result["coverage_status"])
        self.assertFalse(result["research_ready"])
        self.assertTrue(result["recipient_self_reported"])
        self.assertEqual(result, json.loads(output.read_text()))

    def test_deterministic_pack_and_existing_identical_bytes_reused(self):
        result = packer.pack(self.bundle, self.archive)
        self.assertTrue(result["reused"])
        self.assertEqual(self.result["zip_sha256"], result["zip_sha256"])
        other = packer.pack(self.bundle, self.root / "other.zip")
        self.assertEqual(result["zip_sha256"], other["zip_sha256"])

    def test_archive_exact_allowlist_and_no_absolute_paths(self):
        with zipfile.ZipFile(self.archive) as archive:
            self.assertEqual(verifier.FILES | {"handoff.json"}, set(archive.namelist()))
            self.assertTrue(all(not Path(n).is_absolute() and ".." not in Path(n).parts for n in archive.namelist()))

    def test_external_pin_required_and_mismatch_rejected(self):
        for pin, error in (("", "EXTERNAL_PIN_REQUIRED"), ("0" * 64, "HANDOFF_PIN_MISMATCH")):
            with self.subTest(pin=pin), self.assertRaisesRegex(ValueError, error):
                self.verify(pin=pin)

    def test_manifest_tampering_rejected(self):
        with (self.kit / "handoff.json").open("ab") as output:
            output.write(b" ")
        with self.assertRaisesRegex(ValueError, "HANDOFF_PIN_MISMATCH"):
            self.verify()

    def test_file_tampering_rejected_before_import_or_loader(self):
        (self.kit / "tools/pilot_bundle.py").write_text("raise AssertionError('must not execute')")
        with self.assertRaisesRegex(ValueError, "FILE_CHECKSUM_MISMATCH"):
            self.verify()

    def test_extra_file_and_symlink_rejected(self):
        extra = self.kit / ".env"
        extra.write_text("not a real secret")
        with self.assertRaisesRegex(ValueError, "FILE_SET_MISMATCH"):
            self.verify()
        extra.unlink()
        extra.symlink_to(self.root / "outside")
        with self.assertRaisesRegex(ValueError, "SYMLINK_REJECTED"):
            self.verify()

    def test_pilot_cannot_be_relabelled_research_even_with_new_pin(self):
        path = self.kit / "handoff.json"
        metadata = json.loads(path.read_text())
        metadata["research_ready"] = True
        path.write_text(json.dumps(metadata))
        with self.assertRaisesRegex(ValueError, "DEVELOPMENT_ONLY_REQUIRED"):
            self.verify(pin=verifier.sha(path.read_bytes()))

    def test_unsupported_recipient_rejected(self):
        with self.assertRaisesRegex(ValueError, "RECIPIENT_REQUIRED"):
            self.verify(recipient="unknown")

    def test_receipt_must_be_outside_immutable_kit(self):
        with self.assertRaisesRegex(ValueError, "RECEIPT_MUST_BE_OUTSIDE_KIT"):
            self.verify(receipt=self.kit / "receipt.json")

    def test_receipt_and_different_zip_cannot_be_overwritten(self):
        receipt = self.root / "receipt.json"
        self.verify(receipt=receipt)
        with self.assertRaises(FileExistsError):
            self.verify(receipt=receipt)
        self.archive.write_bytes(b"existing other release")
        with self.assertRaisesRegex(ValueError, "CONFLICT_NO_OVERWRITE"):
            packer.pack(self.bundle, self.archive)

    def test_changed_source_extra_file_rejected(self):
        (self.bundle / "private.log").write_text("fixture")
        with self.assertRaises(ValueError):
            packer.pack(self.bundle, self.root / "rejected.zip")

    def test_changed_bytes_after_first_validation_cannot_be_rehashed_into_valid_kit(self):
        original = packer.load_bundle
        def race(path):
            result = original(path)
            if Path(path) == self.bundle:
                with (self.bundle / "features-part-00000.parquet").open("ab") as output:
                    output.write(b"changed-after-validation")
            return result
        output = self.root / "race.zip"
        with patch.object(packer, "load_bundle", side_effect=race), self.assertRaises(ValueError):
            packer.pack(self.bundle, output)
        self.assertFalse(output.exists())

    def test_symlink_zip_and_receipt_rejected(self):
        link = self.root / "link.zip"
        link.symlink_to(self.archive)
        with self.assertRaisesRegex(ValueError, "OUTPUT_SYMLINK_REJECTED"):
            packer.pack(self.bundle, link)
        receipt = self.root / "receipt.json"
        receipt.symlink_to(self.root / "outside")
        with self.assertRaisesRegex(ValueError, "RECEIPT_SYMLINK_REJECTED"):
            self.verify(receipt=receipt)


if __name__ == "__main__":
    unittest.main()
