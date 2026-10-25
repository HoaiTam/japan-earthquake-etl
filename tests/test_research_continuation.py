"""Synthetic continuation/code-compatibility guards. No Docker, source or SQL execution."""
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import warnings
import zipfile

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("continuation", ROOT / "scripts/research-continuation.py")
c = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(c)


class ResearchContinuationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup); self.root = Path(self.temp.name).resolve()

    def jar(self, name, entries):
        file = self.root / name
        with zipfile.ZipFile(file, "w") as out:
            for key, value in entries.items():
                out.writestr(key, value)
        return file

    def test_only_transport_patch_changes_original_code_and_extra_recovery_is_allowed(self):
        a = self.jar("old.jar", {"core.class": b"same", c.TRANSPORT: b"old", "schema.json": b"same"})
        b = self.jar("new.jar", {"core.class": b"same", c.TRANSPORT: b"new", "schema.json": b"same", "Recovery.class": b"new"})
        proof = c.compatibility(a, b)
        self.assertEqual(2, proof["matched_original_entries"]); self.assertEqual({c.TRANSPORT}, set(proof["allowed_differences"]))

    def test_changed_etl_or_schema_resource_fails(self):
        a = self.jar("old.jar", {"core.class": b"same", c.TRANSPORT: b"old", "schema.json": b"same"})
        for key in ("core.class", "schema.json"):
            values = {"core.class": b"same", c.TRANSPORT: b"new", "schema.json": b"same"}; values[key] = b"changed"
            b = self.jar("new.jar", values)
            with self.assertRaisesRegex(RuntimeError, "ETL_CODE_CHANGED"):
                c.compatibility(a, b)

    def test_missing_original_entry_is_not_compatible(self):
        a = self.jar("old.jar", {"core.class": b"same", c.TRANSPORT: b"old"}); b = self.jar("new.jar", {c.TRANSPORT: b"new"})
        with self.assertRaisesRegex(RuntimeError, "ENTRY_MISSING"):
            c.compatibility(a, b)

    def test_duplicate_class_cannot_hide_modified_bytecode(self):
        a = self.jar("old.jar", {c.TRANSPORT: b"old"}); b = self.jar("new.jar", {c.TRANSPORT: b"new"})
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with zipfile.ZipFile(b, "a") as out:
                out.writestr(c.TRANSPORT, b"hidden")
        with self.assertRaisesRegex(RuntimeError, "DUPLICATE_JAR_ENTRY"):
            c.compatibility(a, b)

    def test_unchanged_transport_cannot_claim_the_recovery_patch(self):
        a = self.jar("old.jar", {c.TRANSPORT: b"same"})
        with self.assertRaisesRegex(RuntimeError, "EXACT_TRANSPORT_PATCH_REQUIRED"):
            c.compatibility(a, a)

    def test_original_copy_inventory_excludes_private_logs_and_future_progress(self):
        files = c.original_files({"run_id": "dat03-" + "a" * 32})
        self.assertEqual(39, len(files)); self.assertEqual(len(files), len(set(files)))
        self.assertIn("execution.json", files); self.assertNotIn("stdout.private", files); self.assertNotIn("report.json", files)
        self.assertNotIn("progress-2016-04.json", files); self.assertNotIn("progress-2023-12.json", files)

    def test_copy_is_exact_and_never_overwrites_a_previous_attempt(self):
        original, target = self.root / "original", self.root / "attempt"; original.mkdir(); target.mkdir()
        request = {"run_id": "dat03-" + "a" * 32}
        for name in c.original_files(request):
            file = original / name; file.parent.mkdir(parents=True, exist_ok=True); file.write_bytes(name.encode())
        c.copy_original(original, target, request)
        for name in c.original_files(request):
            self.assertEqual((original / name).read_bytes(), (target / name).read_bytes())
        with self.assertRaises(FileExistsError):
            c.copy_original(original, target, request)

    def test_copy_rejects_symlink_without_reading_new_source(self):
        original, target = self.root / "original", self.root / "attempt"; original.mkdir(); target.mkdir()
        (self.root / "outside").write_bytes(b"secret"); (original / "runner.jar").symlink_to(self.root / "outside")
        with self.assertRaisesRegex(RuntimeError, "SYMLINK_REJECTED"):
            c.copy_original(original, target, {"run_id": "dat03-" + "a" * 32})
        self.assertEqual([], list(target.iterdir()))

    def test_existing_runtime_blocks_before_any_docker_call(self):
        folder = self.root / "staging/dat03-continuation" / ("a" * 32); (folder / "runtime").mkdir(parents=True)
        with patch.object(c.recovery, "health", side_effect=AssertionError("Docker forbidden")), self.assertRaisesRegex(RuntimeError, "FRESH_ATTEMPT_REQUIRED"):
            c.run(self.root, folder)

    def test_completion_gate_requires_exact_report_and_successful_child(self):
        validation = {"status": "VALIDATED_LEASE_HELD", "report_sha256": "a" * 64, "recovery_execution_sha256": "b" * 64}
        c.completion_gate({"exit_code": 0}, validation, "a" * 64, "a" * 64, "b" * 64)
        for bad in (True, 1, "0", None):
            with self.assertRaisesRegex(RuntimeError, "FINALIZATION_PIN_CHANGED"):
                c.completion_gate({"exit_code": bad}, validation, "a" * 64, "a" * 64, "b" * 64)
        with self.assertRaisesRegex(RuntimeError, "FINALIZATION_PIN_CHANGED"):
            c.completion_gate({"exit_code": 0}, validation, "a" * 64, "changed", "b" * 64)
        with self.assertRaisesRegex(RuntimeError, "FINALIZATION_PIN_CHANGED"):
            c.completion_gate({"exit_code": 0}, validation, "a" * 64, "a" * 64, "changed")

    def test_checkpoint_exact_inventory_never_copies_failed_logs_or_new_completion(self):
        files = c.checkpoint_files({"run_id": "dat03-" + "a" * 32})
        self.assertEqual(18, len(files)); self.assertEqual(len(files), len(set(files)))
        self.assertIn("progress-2016-04.json", files)
        self.assertNotIn("progress-2023-12.json", files)
        self.assertNotIn("stderr.private", files)
        op = "orc07-" + c.e2e.hashlib.sha256(("dat03_bounded_e2e|dat03-" + "a" * 32 + "-2023-12").encode()).hexdigest()[:40]
        self.assertIn(op + "/gold.json", files)
        self.assertNotIn(op + "/verify.json", files)
        self.assertNotIn(op + "/publish.json", files)

    def test_checkpoint_copies_exact_metadata_and_rejects_overlap_or_pin_drift(self):
        source, target = self.root / "source", self.root / "target"; source.mkdir(); target.mkdir()
        request = {"run_id": "dat03-" + "a" * 32}; pins = {}
        for name in c.checkpoint_files(request):
            file = source / name; file.parent.mkdir(parents=True, exist_ok=True); c.e2e.save(file, {"name": name}); pins[name] = c.e2e.sha(file)
        checkpoint = {"files_sha256": pins}
        c.copy_checkpoint(source, target, request, checkpoint)
        c.copy_checkpoint(source, target, request, checkpoint)  # identical metadata only
        for name, pin in pins.items():
            self.assertEqual(pin, c.e2e.sha(target / "checkpoint" / name))
        (target / "source-pins.json").write_text('{}')
        with self.assertRaisesRegex(RuntimeError, "OVERLAP_CHANGED"):
            c.copy_checkpoint(source, target, request, checkpoint)
        (source / "source-pins.json").write_text('{}')
        with self.assertRaisesRegex(RuntimeError, "FILE_CHANGED"):
            c.copy_checkpoint(source, target, request, checkpoint)
        with self.assertRaisesRegex(RuntimeError, "EXACT_FILES_REQUIRED"):
            c.copy_checkpoint(source, target, request, {"files_sha256": {"../outside": "a" * 64}})


if __name__ == "__main__":
    unittest.main()
