"""DOC-01 link checker and runnable onboarding command regressions, offline."""
import importlib.util
from pathlib import Path
import re
import tempfile
import unittest

ROOT = Path(__file__).parents[1]
spec = importlib.util.spec_from_file_location("doc_links", ROOT / "scripts/check-docs.py")
docs = importlib.util.module_from_spec(spec); spec.loader.exec_module(docs)


class DocumentationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def check(self, source, other="# Đúng `pin`\n"):
        file = self.root / "README.md"; file.write_text(source)
        (self.root / "other.md").write_text(other)
        return docs.check([file], self.root)

    def test_relative_root_anchor_and_unicode_links(self):
        count, errors = self.check("[local](other.md#đúng-pin) [root](/other.md#đúng-pin) [encoded](other.md#%C4%91%C3%BAng-pin)")
        self.assertEqual(3, count); self.assertEqual([], errors)

    def test_missing_target_anchor_and_path_escape_fail(self):
        _, errors = self.check("[x](missing.md) [y](other.md#absent) [z](../outside.md)")
        self.assertEqual(3, len(errors))
        self.assertEqual("missing heading anchor", errors[1][3])

    def test_code_examples_are_not_real_links(self):
        count, errors = self.check("```markdown\n[x](missing.md)\n```\n`[x](also-missing.md)`\n~~~\n[x](none.md)\n~~~\n")
        self.assertEqual(0, count); self.assertEqual([], errors)

    def test_external_links_are_not_fetched(self):
        count, errors = self.check("[x](https://example.invalid/not-fetched) [y](mailto:team@example.invalid)")
        self.assertEqual(2, count); self.assertEqual([], errors)

    def test_reference_and_image_links_and_spaces(self):
        (self.root / "image.png").write_bytes(b"fixture")
        (self.root / "has space.md").write_text("# Header\n")
        _, errors = self.check('[ref][id] ![image](image.png) [space](<has space.md#header>)\n[id]: other.md#đúng-pin\n')
        self.assertEqual([], errors)
        self.assertTrue(self.check("[missing][unknown]")[1])

    def test_duplicate_headings_and_explicit_anchor(self):
        _, errors = self.check('[x](other.md#same-1) [y](other.md#custom)', '# Same\n# Same\n<a id="custom"></a>\n')
        self.assertEqual([], errors)

    def test_local_only_target_is_not_portable_repository_documentation(self):
        self.check("[x](other.md)")
        _, errors = docs.check([self.root / "README.md"], self.root, {self.root / "README.md"})
        self.assertEqual("ignored/local-only target", errors[0][3])

    def test_source_symlink_is_not_read(self):
        (self.root / "target.md").write_text("private local document")
        source = self.root / "README.md"; source.symlink_to(self.root / "target.md")
        _, errors = docs.check([source], self.root)
        self.assertEqual("unsafe source path", errors[0][3])

    def test_onboarding_make_targets_exist(self):
        makefile = (ROOT / "Makefile").read_text()
        targets = set(re.findall(r"^([A-Za-z0-9_-]+):", makefile, re.M))
        for name in ("docs/runbooks/DOC-01_TEAM_ONBOARDING.md", "docs/LOCAL_OPERATIONS_RUNBOOK.md"):
            text = (ROOT / name).read_text()
            for target in re.findall(r"\bmake ([a-z][a-z0-9-]*)", text):
                self.assertIn(target, targets, name + ": make " + target)
            for script in re.findall(r"\bpython (scripts/[a-z0-9-]+\.py)", text):
                self.assertTrue((ROOT / script).is_file(), script)

    def test_onboarding_does_not_offer_destructive_recovery(self):
        for name in ("DOC-01_TEAM_ONBOARDING.md", "MLI-02_DATASET_BUILD.md"):
            source = (ROOT / "docs/runbooks" / name).read_text()
            for block in re.findall(r"```[^\n]*\n(.*?)```", source, re.S):
                self.assertNotRegex(block, r"\brm\s+-[a-z]*f|docker compose down -v|git reset --hard")


if __name__ == "__main__": unittest.main()
