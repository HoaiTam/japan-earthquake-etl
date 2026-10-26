"""Small synthetic archives exercise verifier safety; never upload these fixtures."""
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("source_verifier", ROOT / "scripts/verify-jma-source-handoff.py")
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class SourceHandoffTest(unittest.TestCase):
    def test_make_target_is_independent_of_qa_01(self):
        command = subprocess.check_output(['make', '-n', 'test-jma-source-handoff'], cwd=ROOT, text=True)
        self.assertIn("-p 'test_jma_source_handoff.py'", command)
        self.assertNotIn('test_current_month_cli.py', command)
        qa = subprocess.check_output(['make', '-n', 'test-qa-01'], cwd=ROOT, text=True)
        self.assertIn('test_current_month_cli.py', qa)
        self.assertNotIn('test_jma_source_handoff.py', qa)

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir="/private/tmp" if Path("/private/tmp").exists() else None)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ("README.md", "verify_sources.py"):
            (self.root / name).write_text("fixture\n")
        self.archives = []
        for year in verifier.YEARS:
            relative = f"sources/h{year}/h{year}.zip"
            path = self.root / relative
            path.parent.mkdir(parents=True)
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr(f"h{year}", b"fixture\n")
            self.archives.append({"year": year, "path": relative, "member": f"h{year}",
                                  "role": "BOUNDARY_SOURCE_ONLY" if year == 2024 else "RESEARCH_SOURCE",
                                  "bytes": path.stat().st_size, "sha256": verifier.sha(path), "member_bytes": 8})
        self.seal()

    def seal(self):
        paths = sorted(p for p in self.root.rglob("*") if p.is_file()
                       and p.name not in {"manifest.json", "checksums.sha256"})
        files = {p.relative_to(self.root).as_posix(): {"bytes": p.stat().st_size, "sha256": verifier.sha(p)} for p in paths}
        (self.root / "checksums.sha256").write_text("".join(f"{entry['sha256']}  {name}\n" for name, entry in sorted(files.items())))
        path = self.root / "checksums.sha256"
        files[path.name] = {"bytes": path.stat().st_size, "sha256": verifier.sha(path)}
        self.manifest = {"version": "dat03-jma-source-handoff-v1", "artifact_kind": "PINNED_JMA_SOURCE_ARCHIVES",
                         "source_system": "JMA_BULLETIN", "research_ready": False, "gold_published": False,
                         "bronze_ready": False, "coverage_verified": False, "archives": self.archives,
                         "files": files, "compressed_archive_bytes": sum(a["bytes"] for a in self.archives)}
        self.repin()

    def repin(self):
        path = self.root / "manifest.json"
        path.write_text(json.dumps(self.manifest, sort_keys=True))
        self.pin = verifier.sha(path)

    def test_success_source_only(self):
        result = verifier.verify(self.root, self.pin)
        self.assertEqual(result["archives"], 25)
        self.assertFalse(result["research_ready"])

    def test_bad_external_pin(self):
        with self.assertRaisesRegex(ValueError, "PIN_MISMATCH"):
            verifier.verify(self.root, "0" * 64)

    def test_archive_tamper(self):
        (self.root / self.archives[0]["path"]).write_bytes(b"changed")
        with self.assertRaisesRegex(ValueError, "FILE_CHANGED"):
            verifier.verify(self.root, self.pin)

    def test_extra_file(self):
        (self.root / ".env").write_text("fixture")
        with self.assertRaisesRegex(ValueError, "EXACT_FILE_SET"):
            verifier.verify(self.root, self.pin)

    def test_symlink(self):
        path = self.root / "README.md"
        path.unlink()
        path.symlink_to(self.root / "verify_sources.py")
        with self.assertRaisesRegex(ValueError, "SYMLINK"):
            verifier.verify(self.root, self.pin)

    def test_research_flag_rejected(self):
        self.manifest["research_ready"] = True
        self.repin()
        with self.assertRaisesRegex(ValueError, "SOURCE_ONLY"):
            verifier.verify(self.root, self.pin)

    def test_missing_year(self):
        self.manifest["archives"] = self.archives[:-1]
        self.repin()
        with self.assertRaisesRegex(ValueError, "EXACT_YEARS"):
            verifier.verify(self.root, self.pin)

    def test_wrong_zip_member(self):
        path = self.root / self.archives[0]["path"]
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("../outside", b"fixture\n")
        self.archives[0].update(bytes=path.stat().st_size, sha256=verifier.sha(path))
        self.seal()
        with self.assertRaisesRegex(ValueError, "ZIP_LAYOUT"):
            verifier.verify(self.root, self.pin)


if __name__ == "__main__":
    unittest.main()
