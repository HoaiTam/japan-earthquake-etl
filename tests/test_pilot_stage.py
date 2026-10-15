import hashlib
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("stage_windows", ROOT / "scripts/stage-ml-windows.py")
stage = importlib.util.module_from_spec(spec); spec.loader.exec_module(stage)


class StageWindowTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.source = "/opt/pipeline/staging/mainshock-window-qa/mld03-qa-" + "a" * 32 + "/mld-03-report.json"
        parent = str(Path(self.source).parent)
        part = "windows/mainshock_candidate_snapshot.parquet/part-00000.parquet"
        window = {"parquet_files": [{"path": part, "bytes": 4, "sha256": hashlib.sha256(b"part").hexdigest()}]}
        handoff = {"dataset_status": "BUILDING"}
        native = {"status": "VERIFIED_PUBLISHED_JMA_WINDOWS", "research_ready": False,
                  "datasets": [{"manifest": {"dataset_split": "REPRODUCTION"}, "window": window, "window_manifest": handoff}]}
        native_bytes = json.dumps(native).encode()
        self.files = {self.source: native_bytes, parent + "/reproduction/" + part: b"part",
                      parent + "/reproduction/window-runtime-report.json": json.dumps(window).encode(),
                      parent + "/reproduction/window-manifest.json": json.dumps(handoff).encode()}
        self.evidence = self.root / "evidence.json"
        self.evidence.write_text(json.dumps({"native_report_path": self.source, "native_report_sha256": hashlib.sha256(native_bytes).hexdigest()}))
        self.calls = []
    def tearDown(self): self.temp.cleanup()
    def read(self, command, **kwargs):
        self.calls.append(command)
        self.assertIn("exec", command); self.assertNotIn("up", command); self.assertNotIn("build", command)
        return SimpleNamespace(returncode=0, stdout=self.files[command[-2]], stderr=b"")
    def test_read_only_stage_and_retry_reuse_exact_files(self):
        with patch.object(stage.subprocess, "run", side_effect=self.read):
            first = stage.stage(self.evidence, self.root / "input", "REPRODUCTION")
            second = stage.stage(self.evidence, self.root / "input", "REPRODUCTION")
        self.assertFalse(first["reused"]); self.assertTrue(second["reused"])
        self.assertEqual(first["files"], 5); self.assertEqual(len(self.calls), 8)
    def test_changed_bytes_rejected_without_overwriting_existing_input(self):
        with patch.object(stage.subprocess, "run", side_effect=self.read):
            stage.stage(self.evidence, self.root / "input", "REPRODUCTION")
            file = self.root / "input/reproduction/window-manifest.json"; file.write_text("tampered")
            with self.assertRaisesRegex(ValueError, "EXISTING_INPUT_CONFLICT"):
                stage.stage(self.evidence, self.root / "input", "REPRODUCTION")
            self.assertEqual(file.read_text(), "tampered")
    def test_receipt_or_remote_part_tamper_rejected(self):
        self.files[self.source] = b"{}"
        with patch.object(stage.subprocess, "run", side_effect=self.read):
            with self.assertRaisesRegex(ValueError, "RECEIPT_CHANGED"): stage.stage(self.evidence, self.root / "input", "REPRODUCTION")
        self.assertFalse((self.root / "input").exists())
    def test_unsafe_remote_receipt_rejected_before_docker(self):
        self.evidence.write_text(json.dumps({"native_report_path": "/etc/passwd"}))
        with patch.object(stage.subprocess, "run") as run:
            with self.assertRaisesRegex(ValueError, "EXACT_RECEIPT"): stage.stage(self.evidence, self.root / "input", "REPRODUCTION")
            run.assert_not_called()


if __name__ == "__main__": unittest.main()
