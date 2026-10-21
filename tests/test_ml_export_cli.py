import hashlib
import importlib.util
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock

spec = importlib.util.spec_from_file_location("ml_export_verify",Path(__file__).resolve().parents[1] / "scripts/verify-ml-export.py")
verify = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verify)


class ExportHandoffTest(unittest.TestCase):
    def test_pins_are_required_before_any_notebook_dependency(self):
        with self.assertRaisesRegex(ValueError,"CHECKSUM_PINS_REQUIRED"):
            verify.verify(Path("missing"),"bad","bad")

    def test_tampered_manifest_is_rejected_before_loader(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); (root/"dataset_manifest.json").write_text("corrupt"); (root/"checksums.sha256").write_text("index")
            loader=Mock()
            with self.assertRaisesRegex(ValueError,"PINNED_CHECKSUM_MISMATCH"):
                verify.verify(root,"0"*64,"0"*64,loader)
            loader.assert_not_called()

    def test_exact_pin_verification_delegates_to_official_loader_without_fitting(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); (root/"dataset_manifest.json").write_text("manifest"); (root/"checksums.sha256").write_text("index")
            pins=[hashlib.sha256((root/name).read_bytes()).hexdigest() for name in ("dataset_manifest.json","checksums.sha256")]
            loader=Mock(return_value={"source_dataset_id":"ds_fixture","features_table":SimpleNamespace(num_rows=2)})
            report=verify.verify(root,*pins,loader)
            loader.assert_called_once_with(root,mode="OFFICIAL")
            self.assertEqual(report["feature_row_count"],2); self.assertFalse(report["scientific_approval"])

    def test_bundle_symlink_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp); (root/"link").symlink_to(root,target_is_directory=True)
            with self.assertRaisesRegex(ValueError,"EXACT_BUNDLE_DIRECTORY"):
                verify.verify(root/"link","a"*64,"b"*64)
