"""Offline regression checks for Java Docker builder packaging, not ETL data."""

from pathlib import Path
import subprocess
import tempfile
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CHECKER = PROJECT_ROOT / "scripts/check-java-build-inputs.sh"
FIXTURE_COPY = "COPY tests/fixtures ./tests/fixtures"
INVENTORY_COPY = "COPY config/jma ./config/jma"
VERIFY = "RUN mvn --batch-mode --no-transfer-progress clean verify"


class JavaBuildInputsTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="java build inputs ")
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        for relative_path in (
            ".dockerignore",
            "compose/spark/Dockerfile",
            "compose/airflow/Dockerfile",
        ):
            target = self.root / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text((PROJECT_ROOT / relative_path).read_text())
        for relative_path in (
            "pom.xml",
            "spark/pom.xml",
            "tests/fixtures/cases.json",
            "tests/fixtures/usgs/success.geojson",
            "tests/fixtures/jma/archives/success.zip",
            "config/jma/hypocenter_archives_v1.csv",
        ):
            target = self.root / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.touch()

    def run_check(self, *args, cwd=None):
        return subprocess.run(
            ["sh", str(CHECKER), *map(str, args)],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=False,
        )

    def change_dockerfile(self, original, replacement, service="spark"):
        path = self.root / "compose" / service / "Dockerfile"
        content = path.read_text()
        self.assertIn(original, content)
        path.write_text(content.replace(original, replacement, 1))

    def assert_rejected(self, expected):
        result = self.run_check(self.root)
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn(expected, result.stderr)

    def test_repository_passes_from_another_working_directory(self):
        result = self.run_check(cwd=self.root)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_root_path_with_spaces_passes(self):
        result = self.run_check(self.root)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_original_spark_packaging_bug_is_rejected(self):
        self.change_dockerfile(FIXTURE_COPY, "")
        self.change_dockerfile(INVENTORY_COPY, "")
        self.assert_rejected("compose/spark/Dockerfile must COPY")

    def test_each_builder_requires_each_copy(self):
        for service in ("spark", "airflow"):
            for directive in (FIXTURE_COPY, INVENTORY_COPY):
                with self.subTest(service=service, directive=directive):
                    path = self.root / "compose" / service / "Dockerfile"
                    original = path.read_text()
                    self.change_dockerfile(directive, "", service)
                    self.assert_rejected(f"compose/{service}/Dockerfile must COPY")
                    path.write_text(original)

    def test_copy_after_verify_is_rejected(self):
        self.change_dockerfile(FIXTURE_COPY, "")
        self.change_dockerfile(VERIFY, VERIFY + "\n" + FIXTURE_COPY)
        self.assert_rejected("compose/spark/Dockerfile must COPY")

    def test_runtime_copy_does_not_satisfy_builder(self):
        self.change_dockerfile(INVENTORY_COPY, "")
        self.change_dockerfile("USER root", "USER root\n" + INVENTORY_COPY)
        self.assert_rejected("compose/spark/Dockerfile must COPY")

    def test_commented_copy_does_not_count(self):
        self.change_dockerfile(FIXTURE_COPY, "# " + FIXTURE_COPY)
        self.assert_rejected("compose/spark/Dockerfile must COPY")

    def test_verify_in_runtime_stage_does_not_count(self):
        self.change_dockerfile(VERIFY, "")
        self.change_dockerfile("USER root", "USER root\n" + VERIFY)
        self.assert_rejected("compose/spark/Dockerfile must COPY")

    def test_missing_context_allowlist_is_rejected(self):
        path = self.root / ".dockerignore"
        path.write_text(path.read_text().replace("!tests/fixtures/**\n", ""))
        self.assert_rejected("missing Java build context rule: !tests/fixtures/**")

    def test_missing_inputs_are_rejected(self):
        for relative_path in (
            "tests/fixtures/jma/archives/success.zip",
            "config/jma/hypocenter_archives_v1.csv",
        ):
            with self.subTest(relative_path=relative_path):
                path = self.root / relative_path
                path.unlink()
                self.assert_rejected(f"missing Java build input: {relative_path}")
                path.touch()

    def test_missing_dockerfile_is_rejected(self):
        (self.root / "compose/spark/Dockerfile").unlink()
        self.assert_rejected("missing Java builder Dockerfile: compose/spark/Dockerfile")

    def test_extra_arguments_are_rejected(self):
        result = self.run_check(self.root, self.root)
        self.assertEqual(2, result.returncode)
        self.assertIn("Usage:", result.stderr)


if __name__ == "__main__":
    unittest.main()
